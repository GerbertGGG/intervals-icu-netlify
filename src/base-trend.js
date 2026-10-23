// "Greift die Grundlage?": Frühindikator zwischen den Blocktests. Laufen = Aerobe Effizienz (EF, m/min je Herzschlag,
// Minute 5–35, block-inputs.js) der lockeren Läufe auf flacher Strecke; Rad = Puls im Messfenster bei fester Leistung
// (Vergleichseinheit). Steigt die EF bzw. sinkt der Puls bei gleicher Leistung über Wochen, wirkt die Grundlage.
// Einzelne Einheiten schwanken (Tageszeit, Wind, Müdigkeit): verglichen werden Mediane aus je 3 Einheiten, Veränderungen
// unter 3 % (EF) bzw. 3 Schlägen (Puls) sind Rauschen. Trend, keine Entscheidung – das Blockziel misst nur der Blocktest.

import { FLAT_M_PER_KM, isBikeCompare } from "./season-block.js";

export const TREND_RULES = { n: 3, efNoisePct: 3, hrNoise: 3, keep: 10 };

const median = (a) => { const s = [...a].sort((x, y) => x - y), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const r3 = (v) => Math.round(v * 1000) / 1000;

// Erste n gegen letzte n (ohne Überlappung, also erst ab 2n Einheiten). dir: "better" | "worse" | "flat"
function compare(values, better, noise, pct) {
  const T = TREND_RULES;
  if (values.length < 2 * T.n) return null;
  const first = median(values.slice(0, T.n)), last = median(values.slice(-T.n));
  const delta = pct ? ((last - first) / first) * 100 : last - first;
  const dir = Math.abs(delta) < noise ? "flat" : (delta > 0) === (better === "up") ? "better" : "worse";
  return { first: r3(first), last: r3(last), delta: Math.round(delta * 10) / 10, dir };
}

// sessions: Ergebnis von loadBlockSessionInputs ({runs, rides}); bike: Radziel aus parseBlockTargets (feste Leistung)
export function buildBaseTrend(sessions, bike = null) {
  const T = TREND_RULES;
  const runs = (sessions?.runs ?? [])
    .filter((r) => r.easy && !r.treadmill && Number.isFinite(r.analysis?.ef) && (r.elevPerKm == null || r.elevPerKm <= FLAT_M_PER_KM))
    .sort((a, b) => a.date.localeCompare(b.date))
    .map((r) => ({ date: r.date, ef: r.analysis.ef, toMin: r.analysis.efToMin, durationMin: r.durationMin, heat: Boolean(r.heat) }));
  const rides = bike
    ? (sessions?.rides ?? [])
      .filter((r) => Number.isFinite(r.mainHr) && isBikeCompare(r, bike))
      .sort((a, b) => a.date.localeCompare(b.date))
      .map((r) => ({ date: r.date, hr: r.mainHr, watts: r.mainWatts ?? null, decoupling: r.decoupling ?? null, noWarmup: Boolean(r.noWarmup), baseline: Boolean(r.baseline) }))
    : [];
  // Fahrten ohne Einrollen verschieben den Puls im Fenster, Ausgangswerte (baseline) zaehlen nicht: im Vergleich nur die anderen
  const warm = rides.filter((r) => !r.noWarmup && !r.baseline);
  if (!runs.length && !rides.length) return null;
  return {
    runs: runs.slice(-T.keep), runTrend: compare(runs.filter((r) => !r.heat).map((r) => r.ef), "up", T.efNoisePct, true),
    rides: rides.slice(-T.keep), rideTrend: compare(warm.map((r) => r.hr), "down", T.hrNoise, false),
    watts: bike?.watts ?? null, n: T.n,
  };
}
