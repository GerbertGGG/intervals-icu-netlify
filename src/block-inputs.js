import { readKvJson, writeKvJson, hasKv } from "./kv.js";
import { fetchIntervalsActivityIntervals, fetchIntervalsActivityStreams } from "./intervals-client.js";
import { activityDay, isBike } from "./activity-utils.js";
import { FLAT_M_PER_KM, HEAT_C, swimTestFromIntervals, swimTestFromText } from "./season-block.js";
import { TEST_RULES, TEST_FALLBACK, classifyRuns, classifyRide, rideWindowOf, rideWindowStats, isTestRide } from "./session-tests.js";

// Eingaben fuer die Abschlussziele des Trainingsblocks (season-block.js): lockere Laeufe und Rollen-Einheiten mit
// Decoupling, Schwimmtests, FTP-Test. Welche Einheit vergleichbar ist, entscheidet season-block.js; hier werden nur
// die Rohwerte gesammelt. Decoupling Laufen: Runalyze (lange Laeufe) vor eigener Rechnung aus den Zeitreihen (Intervals
// nie, s. decouplingOf). Blocktest, Trendlauf und Rad-Vergleichseinheit erkennt session-tests.js (category, testReason).

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const MAX_FETCH = 6; // je Aufruf hoechstens so viele Einheiten nachladen (Rest beim naechsten Mal, Ergebnis in KV)
const DEC_CACHE = "icu:decoupling:v2:"; // v2: Auf-/Abwaermzeit aus Intervals bleibt draussen
const SWIM_CACHE = "icu:swimtest:v1:";
const EASY_NAME = /long\s*slow|\blsr\b|\blit\b|locker|grundlage|langer\s*lauf|\bz2\b|ga\s*1|ausdauer|endurance/i;
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

// Lauf-Auswertung aus den Zeitreihen (Methode wie im Coaching vereinbart): Fenster 0–T ab Start, in zwei Haelften nach Zeit
// geteilt; Effizienz je Haelfte = (Distanzzuwachs / Zeit) / mittlere HF (nur Proben mit HF). Decoupling % = (Eff1 / Eff2 − 1) × 100.
// Gehpausen und Stopps bleiben drin (sie gehoeren zur Einheit). Fenster 60, 75, 90 … min bis zur Gesamtdauer; stabile Dauer =
// laengstes Fenster mit Decoupling ≤ stableMax (0 = schon 0–60 darueber). Dazu Pace 0–60 und Minuten ueber dem HF-Deckel.
const EF_WINDOW = [5, 35]; // Minuten
const EF_MIN_SEC = 15 * 60;
export function analyzeRunStreams(streams, { capHr = null, stableMax = 5, firstMin = 60, stepMin = 15 } = {}) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const hr = get("heartrate"), time = get("time"), vel = get("velocity_smooth");
  let dist = get("distance");
  if (!Array.isArray(hr) || !hr.length) return null;
  const n = hr.length;
  const tOf = (i) => (Array.isArray(time) && Number.isFinite(time[i]) ? time[i] : i);
  if (!Array.isArray(dist) || dist.length < n) {
    if (!Array.isArray(vel)) return null;
    dist = [0];
    for (let i = 1; i < n; i++) dist.push(dist[i - 1] + (Number(vel[i]) || 0) * Math.max(0, tOf(i) - tOf(i - 1)));
  }
  const durSec = tOf(n - 1);
  let over = 0;
  if (capHr != null) for (let i = 1; i < n; i++) if (hr[i] > capHr) over += Math.min(10, Math.max(0, tOf(i) - tOf(i - 1)));
  const idxAt = (sec) => { let i = 0; while (i < n - 1 && tOf(i + 1) <= sec) i++; return i; };
  const half = (a, b) => {
    const i = idxAt(a), j = idxAt(b), secs = tOf(j) - tOf(i);
    const hs = []; for (let k = i; k <= j; k++) if (hr[k] > 40) hs.push(hr[k]);
    if (secs <= 0 || hs.length < 60) return null;
    return ((dist[j] - dist[i]) / secs) / (hs.reduce((x, y) => x + y, 0) / hs.length);
  };
  const decAt = (T) => { const e1 = half(0, T / 2), e2 = half(T / 2, T); return e1 && e2 ? Math.round((e1 / e2 - 1) * 1000) / 10 : null; };
  const windows = [];
  for (let m = firstMin; m * 60 <= durSec + 30; m += stepMin) { const v = decAt(Math.min(m * 60, durSec)); if (v != null) windows.push({ min: m, value: v }); }
  const dec60 = windows.find((x) => x.min === firstMin)?.value ?? null;
  const ok = windows.filter((x) => x.value <= stableMax);
  const d60 = dist[idxAt(firstMin * 60)];
  // Aerobe Effizienz (EF) im Standardfenster Minute 5–35, kuerzere Laeufe Minute 5 bis Ende (mind. 15 min):
  // Tempo in m/min je Herzschlag, ohne den Pulsanstieg aus dem Stand. Trend der Grundlage zwischen den Blocktests.
  const efEnd = Math.min(EF_WINDOW[1] * 60, durSec);
  const efRaw = efEnd >= EF_MIN_SEC ? half(EF_WINDOW[0] * 60, efEnd) : null;
  return {
    ef: efRaw ? Math.round(efRaw * 60 * 1000) / 1000 : null, efToMin: efRaw ? Math.round(efEnd / 60) : null,
    durationMin: Math.round(durSec / 60),
    dec60, windows,
    stableMin: dec60 == null ? null : dec60 > stableMax ? 0 : Math.max(...ok.map((x) => x.min)),
    pace60: durSec >= firstMin * 60 - 30 && d60 > 0 ? Math.round(firstMin * 60 / (d60 / 1000)) : null,
    capHr, overCapMin: capHr != null ? Math.round(over / 60) : null,
  };
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

// Unklarer Lauf ohne Stichwort im Namen zaehlt als locker, wenn der Puls es zeigt: mindestens 80 % der Zeit in HF-Zone 1–2.
const EASY_HR_SHARE = 0.8;
const easyByHr = (a) => {
  const z = Array.isArray(a?.icu_hr_zone_times) ? a.icu_hr_zone_times.map((x) => num(x) ?? 0) : [];
  const total = z.reduce((n, x) => n + x, 0);
  return total > 0 && (z[0] + (z[1] ?? 0)) / total >= EASY_HR_SHARE;
};
export const easyRun = (r, a) => r.kind === "base" || r.kind === "long" || (r.kind === "unknown" && (EASY_NAME.test(`${a?.name ?? ""} ${a?.description ?? ""}`) || easyByHr(a)));
const heatOf = (a) => {
  const t = num(a?.average_weather_temp) ?? num(a?.average_feels_like);
  return (t != null && t >= HEAT_C) || HEAT_TEXT.test(`${a?.name ?? ""} ${a?.description ?? ""}`) || (a?.tags ?? []).some((x) => HEAT_TEXT.test(String(x)));
};

// runs: Laufdatensaetze (buildRunRecord) des Fensters, acts: Roh-Aktivitaeten; rzRuns: Runalyze-Laeufe (decouplingPct je Tag)
const RUN_CACHE = "icu:runanalysis:v2:"; // v2: EF Minute 5–35
const RIDE_TEST_CACHE = "icu:ridetest:v1:"; // Messfenster der Rad-Vergleichseinheit (session-tests.js)
const STREAM_MIN = 20; // kuerzere Laeufe brauchen keine Zeitreihen
// testCfg: Termine, HF-Deckel und Vergleichsleistung fuer die Test-Erkennung (testConfigOf); jede Einheit bekommt category und testReason
export async function loadBlockSessionInputs(env, { acts, runs, rzRuns, from, to, swimDistM = 500, run = null, hrCap = null, testCfg = null }) {
  const tcfg = testCfg ?? { ...TEST_FALLBACK };
  const budget = { left: MAX_FETCH };
  const byId = new Map(acts.map((a) => [a?.id, a]));
  const rzDec = new Map((rzRuns ?? []).filter((r) => Number.isFinite(r.decouplingPct)).map((r) => [r.date, r.decouplingPct]));
  const inWin = (d) => d >= from && d <= to;
  const runClass = classifyRuns(acts.filter((a) => inWin(activityDay(a))), tcfg);

  const runOut = [];
  for (const r of [...runs].filter((x) => inWin(x.date)).sort((x, y) => y.date.localeCompare(x.date))) {
    const a = byId.get(r.id);
    if (!a) continue;
    const easy = easyRun(r, a);
    const km = (num(a.distance) ?? 0) / 1000;
    const treadmill = /virtual|treadmill/i.test(String(a.type ?? "")) || a.trainer === true;
    const rec = { date: r.date, durationMin: r.movingTimeMin, paceSecPerKm: r.paceSecPerKm, elevPerKm: km > 0 && num(a.total_elevation_gain) != null ? Math.round((num(a.total_elevation_gain) / km) * 10) / 10 : null, heat: heatOf(a), easy, treadmill, decoupling: null };
    // Lockere Laeufe (Pulsregel) bekommen die Auswertung aus den Zeitreihen: EF Minute 5–35, Minuten ueber dem HF-Deckel,
    // Fenster und stabile Dauer – im Blocktest-Modus immer, sonst wenn der Block einen HF-Deckel hat. Im Blocktest-Modus
    // wird das Decoupling ueber den ganzen Lauf nicht mehr gebraucht; sonst hat es Vorrang beim Abrufbudget (Blockziel).
    const cls = runClass.get(a.id);
    rec.id = a.id; rec.name = a.name ?? null; rec.category = cls?.category ?? null; rec.testReason = cls?.testReason ?? null;
    if (cls?.testDate) rec.testDate = cls.testDate;
    rec.test = rec.category === "runBlocktest";
    const analyse = async () => {
      if (!(easy || rec.category) || !hasHrOf(a) || a.id == null || (rec.durationMin ?? 0) < STREAM_MIN) return;
      const opts = { capHr: hrCap, stableMax: run?.stableMax ?? 5 };
      const v = await cached(env, `${RUN_CACHE}${a.id}:${hrCap ?? "-"}:${opts.stableMax}`, budget, async () => analyzeRunStreams(await fetchIntervalsActivityStreams(env, a.id, ["time", "heartrate", "distance", "velocity_smooth"]), opts));
      if (v) rec.analysis = v;
    };
    if (Array.isArray(run?.tests)) {
      await analyse();
      runOut.push(rec);
      continue;
    }
    // Decoupling nur fuer lockere Laeufe (Pulsregel), Zeitreihen nur fuer die, die sonst vergleichbar waeren
    if (easy) rec.decoupling = rzDec.get(r.date) ?? (!treadmill && !rec.heat && rec.durationMin >= 55 && (rec.elevPerKm == null || rec.elevPerKm <= FLAT_M_PER_KM) ? await decouplingOf(env, a, "velocity_smooth", budget) : null);
    if (hrCap != null) await analyse();
    runOut.push(rec);
  }

  const rides = [];
  for (const a of acts.filter((x) => isBike(x) && inWin(activityDay(x))).sort((x, y) => activityDay(y).localeCompare(activityDay(x)))) {
    const indoor = a.trainer === true || /virtual/i.test(String(a.type ?? ""));
    const rec = {
      date: activityDay(a), durationMin: Math.round((num(a.moving_time) ?? 0) / 60), watts: num(a.icu_average_watts) ?? num(a.average_watts),
      vi: num(a.icu_variability_index), intensity: num(a.icu_intensity), indoor, hasHr: hasHrOf(a), decoupling: null,
    };
    // Rad-Vergleichseinheit (nur Ride/VirtualRide): Messfenster nach dem Einrollen aus den Zeitreihen, nur wenn der Hauptteil lang genug ist
    const win = rideWindowOf(a);
    let stats = null;
    if (isTestRide(a) && (win.forced || win.mainSec >= TEST_RULES.bikeWindowMin * 60 - TEST_RULES.bikeMainSlackSec) && a.id != null) {
      stats = await cached(env, `${RIDE_TEST_CACHE}${a.id}:${win.warmSec}`, budget, async () => rideWindowStats(await fetchIntervalsActivityStreams(env, a.id, ["time", "heartrate", "watts"]), win.warmSec, TEST_RULES.bikeWindowMin * 60)) ?? null;
    }
    const c = classifyRide(a, stats, tcfg);
    rec.category = c?.category ?? null; rec.testReason = c?.testReason ?? `${a.type ?? "Rad"}: keine Vergleichseinheit (nur Ride/VirtualRide)`;
    if (c?.category) Object.assign(rec, { baseline: c.baseline, decoupling: c.decoupling, decSource: c.decSource, mainWatts: c.mainWatts, mainHr: c.mainHr ?? null, vi: c.vi, noWarmup: c.noWarmup, warmupAssumed: c.warmupAssumed, mainMin: c.mainMin });
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
