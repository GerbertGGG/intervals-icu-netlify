import { readKvJson, writeKvJson, hasKv } from "./kv.js";
import { fetchIntervalsActivityIntervals, fetchIntervalsActivityStreams } from "./intervals-client.js";
import { activityDay, isBike } from "./activity-utils.js";
import { FLAT_M_PER_KM, HEAT_C, swimTestFromIntervals, swimTestFromText } from "./season-block.js";

// Eingaben fuer die Abschlussziele des Trainingsblocks (season-block.js): lockere Laeufe und Rollen-Einheiten mit
// Decoupling, Schwimmtests, FTP-Test. Welche Einheit vergleichbar ist, entscheidet season-block.js; hier werden nur
// die Rohwerte gesammelt. Decoupling: Runalyze (lange Laeufe) vor Intervals vor eigener Rechnung aus den Zeitreihen.

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const MAX_FETCH = 6; // je Aufruf hoechstens so viele Einheiten nachladen (Rest beim naechsten Mal, Ergebnis in KV)
const DEC_CACHE = "icu:decoupling:v1:";
const SWIM_CACHE = "icu:swimtest:v1:";
const EASY_NAME = /long\s*slow|\blsr\b|\blit\b|locker|grundlage|langer\s*lauf|\bz2\b|ga\s*1|ausdauer|endurance/i;
const HARD_NAME = /\b(hit|hiit|sit|vo2|ftp|test|sprint|pyramid|sweet\s*spot|schwelle|threshold|tempo|intervall|fatmax|eb)\b/i;
const HEAT_TEXT = /hitze|heiß|heiss|\bhot\b/i;

// Aerobe Entkopplung (Pa:HR bzw. Pw:HR): Effizienz (Tempo bzw. Leistung je Herzschlag) der ersten gegen die zweite Haelfte.
// Nur Proben mit Puls und Bewegung; mindestens 20 Minuten. Positiv = zweite Haelfte schlechter.
export function decouplingFromStreams(streams, field) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const hr = get("heartrate"), out = get(field);
  if (!Array.isArray(hr) || !Array.isArray(out)) return null;
  const minOut = field === "velocity_smooth" ? 1 : 10;
  const pts = [];
  for (let i = 0; i < Math.min(hr.length, out.length); i++) if (hr[i] > 40 && out[i] >= minOut) pts.push([out[i], hr[i]]);
  if (pts.length < 1200) return null;
  const half = Math.floor(pts.length / 2);
  const ef = (xs) => xs.reduce((a, p) => a + p[0], 0) / xs.reduce((a, p) => a + p[1], 0);
  const e1 = ef(pts.slice(0, half)), e2 = ef(pts.slice(half));
  return e1 > 0 ? Math.round(((e1 - e2) / e1) * 1000) / 10 : null;
}

async function cached(env, key, budget, load) {
  if (hasKv(env)) {
    const hit = await readKvJson(env, key).catch(() => null);
    if (hit) return hit.v;
  }
  if (budget.left <= 0) return undefined;
  budget.left--;
  let v;
  try { v = await load(); } catch (err) { console.warn("block input", key, err); return undefined; }
  if (hasKv(env)) await writeKvJson(env, key, { v: v ?? null }).catch(() => {});
  return v ?? null;
}

const hasHrOf = (a) => a?.has_heartrate === true || num(a?.average_heartrate) != null;

async function decouplingOf(env, a, field, budget) {
  const own = num(a?.decoupling);
  if (own != null) return own;
  if (!hasHrOf(a) || a?.id == null) return null;
  const v = await cached(env, DEC_CACHE + a.id, budget, async () => decouplingFromStreams(await fetchIntervalsActivityStreams(env, a.id, ["heartrate", field]), field));
  return v ?? null;
}

const easyRun = (r, a) => r.kind === "base" || r.kind === "long" || (r.kind === "unknown" && EASY_NAME.test(`${a?.name ?? ""} ${a?.description ?? ""}`));
const heatOf = (a) => {
  const t = num(a?.average_weather_temp) ?? num(a?.average_feels_like);
  return (t != null && t >= HEAT_C) || HEAT_TEXT.test(`${a?.name ?? ""} ${a?.description ?? ""}`) || (a?.tags ?? []).some((x) => HEAT_TEXT.test(String(x)));
};

// runs: Laufdatensaetze (buildRunRecord) des Fensters, acts: Roh-Aktivitaeten; rzRuns: Runalyze-Laeufe (decouplingPct je Tag)
export async function loadBlockSessionInputs(env, { acts, runs, rzRuns, from, to, swimDistM = 500 }) {
  const budget = { left: MAX_FETCH };
  const byId = new Map(acts.map((a) => [a?.id, a]));
  const rzDec = new Map((rzRuns ?? []).filter((r) => Number.isFinite(r.decouplingPct)).map((r) => [r.date, r.decouplingPct]));
  const inWin = (d) => d >= from && d <= to;

  const runOut = [];
  for (const r of [...runs].filter((x) => inWin(x.date)).sort((x, y) => y.date.localeCompare(x.date))) {
    const a = byId.get(r.id);
    if (!a) continue;
    const easy = easyRun(r, a);
    const km = (num(a.distance) ?? 0) / 1000;
    const treadmill = /virtual|treadmill/i.test(String(a.type ?? "")) || a.trainer === true;
    const rec = { date: r.date, durationMin: r.movingTimeMin, paceSecPerKm: r.paceSecPerKm, elevPerKm: km > 0 && num(a.total_elevation_gain) != null ? Math.round((num(a.total_elevation_gain) / km) * 10) / 10 : null, heat: heatOf(a), easy, treadmill, decoupling: null };
    // Decoupling nur fuer lockere Laeufe (Pulsregel), Zeitreihen nur fuer die, die sonst vergleichbar waeren
    if (easy) rec.decoupling = rzDec.get(r.date) ?? (!treadmill && !rec.heat && rec.durationMin >= 55 && (rec.elevPerKm == null || rec.elevPerKm <= FLAT_M_PER_KM) ? await decouplingOf(env, a, "velocity_smooth", budget) : num(a.decoupling));
    runOut.push(rec);
  }

  const rides = [];
  for (const a of acts.filter((x) => isBike(x) && inWin(activityDay(x))).sort((x, y) => activityDay(y).localeCompare(activityDay(x)))) {
    const indoor = a.trainer === true || /virtual/i.test(String(a.type ?? ""));
    const name = `${a.name ?? ""}`;
    const rec = {
      date: activityDay(a), durationMin: Math.round((num(a.moving_time) ?? 0) / 60), watts: num(a.icu_average_watts) ?? num(a.average_watts),
      vi: num(a.icu_variability_index), intensity: num(a.icu_intensity), indoor, easy: !HARD_NAME.test(name), hasHr: hasHrOf(a), tempC: num(a.average_temp), decoupling: null,
    };
    if (indoor && rec.easy && rec.durationMin >= 65) rec.decoupling = await decouplingOf(env, a, "watts", budget);
    rides.push(rec);
  }
  const ftpAct = acts.filter((x) => isBike(x) && inWin(activityDay(x)) && /\bftp\b/i.test(String(x.name ?? ""))).sort((x, y) => activityDay(x).localeCompare(activityDay(y)))[0];

  const swimTests = [];
  for (const a of acts.filter((x) => /swim/i.test(String(x.type ?? "")) && inWin(activityDay(x)))) {
    const text = `${a.name ?? ""}\n${a.description ?? ""}`;
    let t = swimTestFromText(text, swimDistM);
    if (!t && (num(a.distance) ?? 0) >= swimDistM * 2 && a.id != null) {
      t = await cached(env, `${SWIM_CACHE}${swimDistM}:${a.id}`, budget, async () => swimTestFromIntervals(await fetchIntervalsActivityIntervals(env, a.id), swimDistM));
    }
    if (t) swimTests.push({ date: activityDay(a), ...t });
  }

  return { runs: runOut, rides, swimTests, ftpTest: ftpAct ? { date: activityDay(ftpAct) } : null };
}
