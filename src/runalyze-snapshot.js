import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";

// Runalyze hat keine öffentliche API, nur ein MCP. Der Worker kann es deshalb nicht selbst
// abfragen: Eine Sitzung mit Runalyze-MCP (z. B. der Coaching-Task) schickt Prognose und
// Rennen per PUT /api/runalyze hierher (Bearer DASHBOARD_TOKEN), das Dashboard liest sie aus dem KV.
//
// Body: {
//   fetchedAt: ISO-Zeitstempel,
//   vdot: number,                                          // effectiveVO2max aus get_calculations (optional)
//   prognosis: [{ distanceKm, seconds }],                  // aus get_prognosis
//   races: [{ date, name, distanceKm, officialDistanceKm, officialTimeSec }]  // aus get_historical_races
//   marathonShape: number (0-100, aus get_calculations; optional)
//   runs: [{ date, distanceKm, durationSec, type, decouplingPct }]  // Läufe der letzten 8 Wochen: type = Art in Runalyze
//                                                                   // (get_activities), decouplingPct = aerobic_decoupling_pace (get_activity_details)
// }

export const RUNALYZE_KV_KEY = "dashboard:runalyze";
const MAX_BODY_BYTES = 50_000;

function finitePositive(v) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? n : null;
}

export function validateSnapshot(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const prognosis = (Array.isArray(body.prognosis) ? body.prognosis : [])
    .map((p) => ({ distanceKm: finitePositive(p?.distanceKm), seconds: finitePositive(p?.seconds) }))
    .filter((p) => p.distanceKm && p.seconds);
  const races = (Array.isArray(body.races) ? body.races : [])
    .map((r) => ({
      date: /^\d{4}-\d{2}-\d{2}$/.test(String(r?.date)) ? String(r.date) : null,
      name: r?.name ? String(r.name).slice(0, 120) : null,
      officialDistanceKm: finitePositive(r?.officialDistanceKm ?? r?.distanceKm),
      officialTimeSec: finitePositive(r?.officialTimeSec),
    }))
    .filter((r) => r.date && r.officialDistanceKm && r.officialTimeSec);
  // Läufe mit der in Runalyze gesetzten Art ("Langer Lauf", "Intervalltraining" ...) und dem Pace-Decoupling.
  const runs = (Array.isArray(body.runs) ? body.runs : [])
    .map((r) => {
      const dec = Number(r?.decouplingPct);
      return {
        date: /^\d{4}-\d{2}-\d{2}$/.test(String(r?.date)) ? String(r.date) : null,
        distanceKm: finitePositive(r?.distanceKm),
        durationSec: finitePositive(r?.durationSec),
        type: r?.type ? String(r.type).slice(0, 60) : null,
        decouplingPct: r?.decouplingPct != null && r.decouplingPct !== "" && Number.isFinite(dec) && Math.abs(dec) < 100 ? dec : null,
      };
    })
    .filter((r) => r.date && r.distanceKm)
    .slice(0, 80);
  const vdot = finitePositive(body?.vdot);
  const ms = Number(body?.marathonShape);
  const marathonShape = body?.marathonShape != null && body.marathonShape !== "" && Number.isFinite(ms) && ms >= 0 && ms <= 100 ? ms : null;
  if (vdot != null && (vdot < 15 || vdot > 90)) return { error: "vdot außerhalb 15–90" };
  if (!prognosis.length && !races.length && vdot == null) return { error: "weder prognosis, races noch vdot enthalten" };
  return { value: { fetchedAt: new Date(fetchedAt).toISOString(), vdot, prognosis, races, runs, marathonShape } };
}

// Art des Laufs aus der Runalyze-Bezeichnung, die der Nutzer selbst setzt: Rennen, lang, Intensität (Intervall, Tempo, Schwelle) oder sonstiges.
export function runKindFromType(type) {
  const t = String(type ?? "");
  if (/wettkampf|race|rennen/i.test(t)) return "race";
  if (/lang/i.test(t)) return "long";
  if (/intervall|tempo|schwelle/i.test(t)) return "intensity";
  return "other";
}

// Bestzeit je Distanz = schnellstes Rennen, dessen offizielle Distanz höchstens 5 % abweicht.
// Die tatsächliche Distanz bleibt sichtbar, damit z. B. 5,02 km nicht als exakt 5 km durchgeht.
export function bestForDistance(races, distanceKm, tolerance = 0.05) {
  const hits = races.filter((r) => Math.abs(r.officialDistanceKm - distanceKm) / distanceKm <= tolerance);
  if (!hits.length) return null;
  return hits.reduce((a, b) => (b.officialTimeSec < a.officialTimeSec ? b : a));
}

export async function readRunalyzeSnapshot(env) {
  return readKvJson(env, RUNALYZE_KV_KEY).catch(() => null);
}

export async function handleRunalyzeSnapshotRequest(req, env, isAuthorized) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  if (req.method !== "PUT") return json({ ok: false, error: "Nur PUT erlaubt" }, 405, { ...headers, allow: "PUT" });
  if (!env?.KV) return json({ ok: false, error: "KV nicht verfügbar" }, 503, headers);
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return json({ ok: false, error: "Body zu groß" }, 413, headers);
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    return json({ ok: false, error: "Kein gültiges JSON" }, 400, headers);
  }
  const { value, error } = validateSnapshot(body);
  if (error) return json({ ok: false, error }, 400, headers);
  await writeKvJson(env, RUNALYZE_KV_KEY, value);
  return json({ ok: true, prognosis: value.prognosis.length, races: value.races.length, runs: value.runs.length }, 200, headers);
}
