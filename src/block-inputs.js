import { readKvJson, writeKvJson, hasKv } from "./kv.js";
import { fetchIntervalsActivityIntervals, fetchIntervalsActivityStreams } from "./intervals-client.js";
import { activityDay, isBike } from "./activity-utils.js";
import { FLAT_M_PER_KM, HEAT_C, swimTestFromIntervals, swimTestFromText } from "./season-block.js";

// Eingaben fuer die Abschlussziele des Trainingsblocks (season-block.js): lockere Laeufe und Rollen-Einheiten mit
// Decoupling, Schwimmtests, FTP-Test. Welche Einheit vergleichbar ist, entscheidet season-block.js; hier werden nur
// die Rohwerte gesammelt. Decoupling Laufen: Runalyze (lange Laeufe) vor eigener Rechnung aus den Zeitreihen (Intervals
// nie, s. decouplingOf); Rad: Intervals vor eigener Rechnung.

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const MAX_FETCH = 6; // je Aufruf hoechstens so viele Einheiten nachladen (Rest beim naechsten Mal, Ergebnis in KV)
const DEC_CACHE = "icu:decoupling:v2:"; // v2: Auf-/Abwaermzeit aus Intervals bleibt draussen
const SWIM_CACHE = "icu:swimtest:v1:";
const EASY_NAME = /long\s*slow|\blsr\b|\blit\b|locker|grundlage|langer\s*lauf|\bz2\b|ga\s*1|ausdauer|endurance/i;
const HARD_NAME = /\b(hit|hiit|sit|vo2|ftp|test|sprint|pyramid|sweet\s*spot|schwelle|threshold|tempo|intervall|fatmax|eb)\b/i;
const HEAT_TEXT = /hitze|heiß|heiss|\bhot\b/i;

// Aerobe Entkopplung (Pa:HR bzw. Pw:HR): Effizienz (Tempo bzw. Leistung je Herzschlag) der ersten gegen die zweite Haelfte.
// Nur Proben mit Puls und Bewegung; mindestens 20 Minuten. Positiv = zweite Haelfte schlechter.
// maxSec: nur die ersten maxSec Sekunden (Zeitstrahl "time", sonst eine Probe je Sekunde).
// warmupSec/cooldownSec: wie bei Intervals (icu_warmup_time, icu_cooldown_time) vorne bzw. hinten ausgelassen.
export function decouplingFromStreams(streams, field, maxSec = null, { warmupSec = 0, cooldownSec = 0 } = {}) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const hr = get("heartrate"), out = get(field), time = get("time");
  if (!Array.isArray(hr) || !Array.isArray(out)) return null;
  const minOut = field === "velocity_smooth" ? 1 : 10;
  const n = Math.min(hr.length, out.length);
  const tOf = (i) => (Array.isArray(time) && Number.isFinite(time[i]) ? time[i] : i);
  const endSec = cooldownSec > 0 ? tOf(n - 1) - cooldownSec : Infinity;
  const pts = [];
  for (let i = 0; i < n; i++) {
    const t = tOf(i);
    if ((maxSec != null && t >= maxSec) || t > endSec) break;
    if (t < warmupSec) continue;
    if (hr[i] > 40 && out[i] >= minOut) pts.push([out[i], hr[i]]);
  }
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

// firstMin: nur die ersten firstMin Minuten zaehlen (Ziel "der ersten 60 min"); bei laengeren Einheiten passt der
// Intervals-Wert ueber die ganze Einheit nicht, dann aus den Zeitreihen. Beim Laufen (velocity_smooth) nie den
// Intervals-Wert: ohne Laufleistung liefert Intervals dort nichts Brauchbares.
async function decouplingOf(env, a, field, budget, firstMin = null) {
  const cut = firstMin != null && (num(a?.moving_time) ?? 0) > (firstMin + 1) * 60;
  const own = field === "velocity_smooth" ? null : num(a?.decoupling);
  if (own != null && !cut) return own;
  if (!hasHrOf(a) || a?.id == null) return null;
  const key = DEC_CACHE + a.id + (cut ? `:${firstMin}` : "");
  const skip = { warmupSec: num(a?.icu_warmup_time) ?? 0, cooldownSec: num(a?.icu_cooldown_time) ?? 0 };
  const v = await cached(env, key, budget, async () => decouplingFromStreams(await fetchIntervalsActivityStreams(env, a.id, ["time", "heartrate", field]), field, cut ? firstMin * 60 : null, skip));
  return v ?? null;
}

const easyRun = (r, a) => r.kind === "base" || r.kind === "long" || (r.kind === "unknown" && EASY_NAME.test(`${a?.name ?? ""} ${a?.description ?? ""}`));
const heatOf = (a) => {
  const t = num(a?.average_weather_temp) ?? num(a?.average_feels_like);
  return (t != null && t >= HEAT_C) || HEAT_TEXT.test(`${a?.name ?? ""} ${a?.description ?? ""}`) || (a?.tags ?? []).some((x) => HEAT_TEXT.test(String(x)));
};

// runs: Laufdatensaetze (buildRunRecord) des Fensters, acts: Roh-Aktivitaeten; rzRuns: Runalyze-Laeufe (decouplingPct je Tag)
// bike: Radziel aus parseBlockTargets (minMin, firstMin) – Decoupling nur fuer Rollen-Einheiten, die lang genug sind
export async function loadBlockSessionInputs(env, { acts, runs, rzRuns, from, to, swimDistM = 500, bike = null }) {
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
    if (easy) rec.decoupling = rzDec.get(r.date) ?? (!treadmill && !rec.heat && rec.durationMin >= 55 && (rec.elevPerKm == null || rec.elevPerKm <= FLAT_M_PER_KM) ? await decouplingOf(env, a, "velocity_smooth", budget) : null);
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
    if (indoor && rec.easy && rec.durationMin >= (bike?.minMin ?? 75) - 5) rec.decoupling = await decouplingOf(env, a, "watts", budget, bike?.firstMin ?? null);
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
