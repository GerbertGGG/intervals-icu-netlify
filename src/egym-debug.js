import { json } from "./http-helpers.js";
import { isAuthorized, timingSafeEqual } from "./dashboard.js";
import { isoDateBerlin } from "./date-utils.js";
import { hasEgymCredentials, fetchEgymWorkouts, fetchEgymStrength, fetchEgymBioAge } from "./egym-client.js";

// Geschuetzter Debug-Endpoint fuer die EGYM-Anbindung: zeigt die Rohdaten der drei Endpunkte (ohne Zugangsdaten),
// damit das Kraft-Widget auf echten Feldnamen aufgebaut werden kann. Standard: Zaehlung plus je wenige Beispiele,
// mit ?full=1 die komplette Antwort. ?days=N (Standard 28, max 180) bestimmt den Zeitraum.
const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

const settle = async (fn) => {
  try {
    return { ok: true, data: await fn() };
  } catch (e) {
    return { ok: false, error: String(e?.message ?? e) };
  }
};

// Listen stecken je nach Endpunkt direkt oder unter einem Schluessel (z. B. strengthMeasurements); beides abdecken.
function sample(data, full) {
  if (full) return data;
  if (Array.isArray(data)) return { count: data.length, sample: data.slice(0, 2) };
  if (data && typeof data === "object") {
    const out = { keys: Object.keys(data) };
    for (const [k, v] of Object.entries(data)) out[k] = Array.isArray(v) ? { count: v.length, sample: v.slice(0, 2) } : v;
    return out;
  }
  return data;
}

// Nur hier zusaetzlich ?token=<DASHBOARD_TOKEN>: der Aufruf vom Handy-Browser kann keinen Header setzen.
// Das Token landet damit im Browserverlauf; nach dem Debuggen am besten neu setzen.
function authorizedForDebug(req, env, params) {
  if (isAuthorized(req, env)) return true;
  const given = params.get("token") || "";
  return given.length > 0 && timingSafeEqual(given, env.DASHBOARD_TOKEN);
}

// Kompakte Uebersicht je Workout (Datum, Uebung, Quelle, Geraet, Satzzahl): zeigt, was das Kraft-Widget als Kraft-Einheit zaehlt
function workoutSummary(data) {
  const list = Array.isArray(data) ? data : data?.workouts;
  if (!Array.isArray(list)) return null;
  return list.map((w) => ({
    at: w.completedAt,
    exercises: (w.exercises ?? []).map((ex) => ({ name: ex.name, source: ex.source?.code ?? null, category: ex.exercise?.category?.code ?? null, machineBased: ex.exercise?.machineBased ?? null, at: ex.completedAt ?? null, duration: ex.attributes?.duration ?? null, sets: Array.isArray(ex.attributes?.sets_of_reps_and_weight_or_duration_and_weight) ? ex.attributes.sets_of_reps_and_weight_or_duration_and_weight.length : 0 })),
  }));
}

export async function handleEgymDebugRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  const params = new URL(req.url).searchParams;
  if (!authorizedForDebug(req, env, params)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  if (!hasEgymCredentials(env)) return json({ ok: false, error: "EGYM_USERNAME und EGYM_PASSWORD nicht gesetzt" }, 503, headers);
  const full = params.get("full") === "1";
  const days = Math.min(180, Math.max(1, Math.floor(Number(params.get("days"))) || 28));
  const to = isoDateBerlin();
  const from = addDays(to, -days);
  const [workouts, strength, bioAge] = await Promise.all([
    settle(() => fetchEgymWorkouts(env, from, to)),
    settle(() => fetchEgymStrength(env, from, to)),
    settle(() => fetchEgymBioAge(env)),
  ]);
  const view = (r) => (r.ok ? { ok: true, data: sample(r.data, full) } : r);
  return json({ ok: true, from, to, workouts: view(workouts),
    workoutSummary: workouts.ok ? workoutSummary(workouts.data) : null, strength: view(strength), bioAge: view(bioAge) }, 200, headers);
}
