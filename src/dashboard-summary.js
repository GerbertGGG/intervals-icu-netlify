// Einschätzungen für Dashboard-Seite und Widget an EINER Stelle, damit beide dasselbe zeigen:
// Frische (TSB), ACWR und "Bereit für Training?". Alle Grenzen sind eigene Standardwerte, keine
// persönlich kalibrierten oder medizinischen Vorgaben.

export const THRESHOLDS = { tsb: { ok: -10, warn: -25 }, acwr: { lo: 0.8, hi: 1.3 } };

export const READY_METRICS = [
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

export function computeLoad(wellness) {
  const w = [...wellness].reverse().find((x) => x.ctl != null && x.atl != null) ?? null;
  const tsb = w ? w.ctl - w.atl : null;
  const acwr = w && w.ctl > 0 ? w.atl / w.ctl : null;
  const { tsb: t, acwr: a } = THRESHOLDS;
  return {
    date: w?.date ?? null,
    ctl: w?.ctl ?? null,
    atl: w?.atl ?? null,
    tsb,
    acwr,
    tsbCls: tsb == null ? "none" : tsb >= t.ok ? "ok" : tsb >= t.warn ? "warn" : "bad",
    acwrCls: acwr == null ? "none" : acwr < a.lo ? "warn" : acwr <= a.hi ? "ok" : "bad",
  };
}

// Heutiger Eintrag gegen den eigenen üblichen Bereich der letzten 8 Wochen. Skala ab 1 = bestmöglich:
// ein Schritt schlechter als der Median = Achtung, zwei oder mehr = deutlich. Ohne heutigen Eintrag
// gibt es bewusst keine Einschätzung, fehlende Werte zählen nie als "gut".
export function computeReadiness(wellness, todayIso, load) {
  const today = wellness.find((w) => w.date === todayIso);
  const past = wellness.filter((w) => w.date < todayIso);
  const items = READY_METRICS.map((m) => {
    const v = today ? today[m.key] : null;
    const base = past.map((w) => w[m.key]).filter((x) => x != null);
    const max = Math.max(2, ...wellness.map((w) => w[m.key] ?? 0));
    const band = base.length >= 5 ? [quantile(base, 0.25), quantile(base, 0.75)] : null;
    if (v == null) return { ...m, v: null, max, band, cls: "none", text: "fehlt" };
    if (!band) return { ...m, v, max, band, cls: "none", text: "kein Vergleich" };
    const delta = v - median(base);
    return { ...m, v, max, band, cls: delta >= 2 ? "bad" : delta >= 1 ? "warn" : "ok", text: delta >= 2 ? "deutlich schlechter" : delta >= 1 ? "etwas schlechter" : "wie üblich" };
  });
  const hasScales = items.some((i) => i.v != null);
  const nBad = items.filter((i) => i.cls === "bad").length;
  const nWarn = items.filter((i) => i.cls === "warn").length;
  let verdict;
  if (!hasScales) verdict = { cls: "none", text: "Heute noch nichts eingetragen", sub: "Ohne Eintrag gebe ich keine Einschätzung ab." };
  else if (nBad >= 1 || nWarn >= 3) verdict = { cls: "bad", text: "Eher ruhig angehen", sub: "Mehrere Werte liegen schlechter als üblich." };
  else if (nWarn >= 1 || (load?.tsb != null && load.tsb < THRESHOLDS.tsb.ok)) verdict = { cls: "warn", text: "Mit Vorsicht", sub: "Einzelne Werte oder die Belastung sind auffällig." };
  else verdict = { cls: "ok", text: "Bereit", sub: "Die Werte liegen im üblichen Bereich." };
  return { verdict, items, sleepHours: today?.sleepHours ?? null, hrv: today?.hrv ?? null, restingHR: today?.restingHR ?? null };
}

// Farbe allein aus dem Wert: Skala ab 1 = bestmöglich, niedrig ist gut. Untere Drittel-Grenze grün,
// mittleres gelb, oberes rot. Die Skala reicht mindestens bis 4 (Intervals-Standard).
export const SCALE_MIN_MAX = 4;
export function scaleValueCls(v, max) {
  if (v == null) return "none";
  const pos = (v - 1) / (Math.max(SCALE_MIN_MAX, max ?? 0) - 1);
  return pos <= 1 / 3 + 0.01 ? "ok" : pos <= 2 / 3 + 0.01 ? "warn" : "bad";
}

export function buildSummary(wellness, todayIso) {
  const load = computeLoad(wellness);
  return { thresholds: THRESHOLDS, load, readiness: computeReadiness(wellness, todayIso, load) };
}
