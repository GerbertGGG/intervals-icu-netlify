// Kennzahlen für Dashboard-Seite und Widget an EINER Stelle, damit beide dasselbe zeigen: Frische (TSB), ACWR und die
// Erholungswerte gegen die eigene Basis. Hier wird nichts entschieden: Das Urteil über den Tag fällt der Coach in der
// Morgen-Auswertung (save_coach), das Dashboard zeigt nur die Werte, auf die er schaut.

// tsb: alte Punktgrenzen, nur noch fuers Widget. tsbPct: Frische in % der Fitness (CTL) wie die Formzonen von Intervals.icu:
// unter −30 % hohes Risiko, −30 bis −10 % produktives Training, −10 bis +5 % neutral, +5 bis +25 % frisch, darueber Uebergang
// (Fitness sinkt). Ein negativer TSB in Belastungswochen ist gewollt und keine Warnung.
const THRESHOLDS = { tsb: { ok: -10, warn: -25 }, tsbPct: { risk: -30, optimal: -10, fresh: 5, transition: 25 }, acwr: { lo: 0.8, hi: 1.3 } };

const FEEL_METRICS = [
  { key: "sleepQuality", label: "Schlaf" },
  { key: "fatigue", label: "Ermüdung" },
  { key: "soreness", label: "Muskelkater" },
  { key: "mood", label: "Stimmung" },
  { key: "motivation", label: "Motivation" },
];

function median(a) {
  const s = [...a].sort((x, y) => x - y);
  const m = s.length >> 1;
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2;
}

function quantile(a, q) {
  const s = [...a].sort((x, y) => x - y);
  const p = (s.length - 1) * q;
  const lo = Math.floor(p);
  const hi = Math.ceil(p);
  return s[lo] + (s[hi] - s[lo]) * (p - lo);
}

// Erwartet Wellness, in der heute schon mit dem absolvierten Load gerechnet ist (live-load.js).
// Zukunftstage (projizierte Werte) zählen nie.
export function computeLoad(wellness, todayIso = null) {
  const usable = todayIso ? wellness.filter((x) => x.date <= todayIso) : wellness;
  const w = [...usable].reverse().find((x) => x.ctl != null && x.atl != null) ?? null;
  const tsb = w ? w.ctl - w.atl : null;
  const acwr = w && w.ctl > 0 ? w.atl / w.ctl : null;
  const tsbPct = w && w.ctl > 0 ? (tsb / w.ctl) * 100 : null;
  const { tsbPct: z, acwr: a } = THRESHOLDS;
  const zone = tsbPct == null ? null : tsbPct < z.risk ? "risk" : tsbPct < z.optimal ? "optimal" : tsbPct <= z.fresh ? "neutral" : tsbPct <= z.transition ? "fresh" : "transition";
  return {
    date: w?.date ?? null,
    ctl: w?.ctl ?? null,
    atl: w?.atl ?? null,
    tsb,
    tsbPct,
    tsbZone: zone,
    acwr,
    tsbCls: { risk: "bad", optimal: "ok", neutral: "none", fresh: "ok", transition: "none" }[zone] ?? "none",
    acwrCls: acwr == null ? "none" : acwr < a.lo ? "warn" : acwr <= a.hi ? "ok" : "bad",
  };
}


// Basis der Körperwerte: Median der 28 Tage VOR heute (heute zählt nicht mit), ohne Krankheitstage und die 3 Tage danach,
// nur Tage mit Wert, ab 5 Werten. Ausreißer (HRV unter 70 % bzw. Ruhepuls über +8 des Medians, z. B. eine Krankheitsnacht
// ohne Kalendereintrag) bleiben draußen. Dazu der 7-Tage-Schnitt gegen die 28 Tage vor dieser Woche.
export const BASE_RULES = { baseDays: 28, minN: 5, sickTail: 3, trendDays: 7, trendMinN: 4, outlier: { hrv: 0.7, restingHR: 8 } };

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

function sickSetOf(sickDays) {
  const set = new Set();
  for (const d of sickDays ?? []) for (let k = 0; k <= BASE_RULES.sickTail; k++) set.add(addDays(d, k));
  return set;
}

function computeBody(wellness, todayIso, sickDays = []) {
  const R = BASE_RULES, skip = sickSetOf(sickDays);
  const past = wellness.filter((w) => w.date >= addDays(todayIso, -R.baseDays) && w.date < todayIso && !skip.has(w.date));
  const keep = { hrv: (v, m) => v >= m * R.outlier.hrv, restingHR: (v, m) => v <= m + R.outlier.restingHR };
  const base = (key, list = past) => {
    let v = list.map((w) => w[key]).filter((x) => x != null);
    if (keep[key] && v.length >= R.minN) { const m = median(v); v = v.filter((x) => keep[key](x, m)); }
    return v.length >= R.minN ? median(v) : null;
  };
  const prior = wellness.filter((w) => w.date >= addDays(todayIso, -(R.trendDays - 1) - R.baseDays) && w.date <= addDays(todayIso, -R.trendDays) && !skip.has(w.date));
  const recent = wellness.filter((w) => w.date > addDays(todayIso, -R.trendDays) && w.date <= todayIso);
  const mean7 = (key) => { const v = recent.map((w) => w[key]).filter((x) => x != null); return v.length >= R.trendMinN ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  return {
    hrv: { mean7: mean7("hrv"), base7: base("hrv", prior) },
    resting: { mean7: mean7("restingHR"), base7: base("restingHR", prior) },
    sleepMedian: base("sleepHours"), hrvMedian: base("hrv"), restingMedian: base("restingHR"),
  };
}

// Langzeitbasis: Die 28-Tage-Basis wandert bei schleichender Ermüdung über Wochen mit. Zweiter, fester Bezug: Median der
// Tage 29–90 vor heute (ohne Krankheitstage, mind. 20 Werte) gegen den 7-Tage-Schnitt (mind. 4 Werte). Nur Werte, kein Urteil.
export const DRIFT_RULES = { from: 90, to: 29, minN: 20, recentMinN: 4 };
export function computeDrift(longWellness, todayIso, sickDays = []) {
  const R = DRIFT_RULES, skip = sickSetOf(sickDays);
  if (!Array.isArray(longWellness) || !longWellness.length) return null;
  const anchorDays = longWellness.filter((w) => w.date >= addDays(todayIso, -R.from) && w.date <= addDays(todayIso, -R.to) && !skip.has(w.date));
  const recent = longWellness.filter((w) => w.date > addDays(todayIso, -7) && w.date <= todayIso && !skip.has(w.date));
  const med = (list, k) => { const v = list.map((w) => w[k]).filter((x) => x != null); return v.length >= R.minN ? median(v) : null; };
  const avg = (list, k) => { const v = list.map((w) => w[k]).filter((x) => x != null); return v.length >= R.recentMinN ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  const hrvA = med(anchorDays, "hrv"), rhrA = med(anchorDays, "restingHR");
  const hrv7 = hrvA != null ? avg(recent, "hrv") : null, rhr7 = rhrA != null ? avg(recent, "restingHR") : null;
  if (hrv7 == null && rhr7 == null) return null;
  return { hrv7, hrvAnchor: hrvA, rhr7, rhrAnchor: rhrA };
}

// Erholungswerte von heute als Fakten: Körperwerte gegen die eigene Basis, Gefühlswerte gegen den üblichen Bereich der
// letzten 8 Wochen (Band = 25–75 %-Quantil ab 5 Werten, Skala ab 1 = bestmöglich). Die Gefühlswerte trägt der Nutzer
// immer VOR dem Lauf ein: Sie beschreiben den Ausgangszustand, nicht die Reaktion auf die Einheit des Tages. Fehlt der
// heutige Eintrag, steht der von gestern da (entryDate). Keine Ampel, keine Vorgabe: das entscheidet der Coach.
// ctx: { sickDays: [date], longWellness: [{date,hrv,restingHR}] }
export function computeRecovery(wellness, todayIso, ctx = {}) {
  const yIso = addDays(todayIso, -1);
  const entry = (d) => wellness.find((w) => w.date === d && FEEL_METRICS.some((m) => w[m.key] != null)) ?? null;
  const src = entry(todayIso) ?? entry(yIso);
  const past = wellness.filter((w) => w.date < (src?.date ?? todayIso));
  const items = FEEL_METRICS.map((m) => {
    const v = src ? src[m.key] ?? null : null;
    const base = past.map((w) => w[m.key]).filter((x) => x != null);
    const max = Math.max(2, ...wellness.map((w) => w[m.key] ?? 0));
    return { ...m, v, max, band: base.length >= 5 ? [quantile(base, 0.25), quantile(base, 0.75)] : null, med: base.length >= 5 ? median(base) : null };
  });
  const body = computeBody(wellness, todayIso, ctx.sickDays);
  const drift = computeDrift(ctx.longWellness, todayIso, ctx.sickDays);
  if (drift) body.drift = drift;
  const today = wellness.find((w) => w.date === todayIso);
  const sick = (ctx.sickDays ?? []).includes(todayIso) || (ctx.sickDays ?? []).includes(yIso);
  return { items, body, sick, entryDate: src?.date ?? null, sleepHours: today?.sleepHours ?? null, sleepScore: today?.sleepScore ?? null, hrv: today?.hrv ?? null, restingHR: today?.restingHR ?? null };
}

export function buildSummary(wellness, todayIso, ctx = {}) {
  return { thresholds: THRESHOLDS, load: computeLoad(wellness, todayIso), recovery: computeRecovery(wellness, todayIso, ctx) };
}
