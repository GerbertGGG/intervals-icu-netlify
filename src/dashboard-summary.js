// Einschätzungen für Dashboard-Seite und Widget an EINER Stelle, damit beide dasselbe zeigen:
// Frische (TSB), ACWR und "Bereit für Training?". Alle Grenzen sind eigene Standardwerte, keine
// persönlich kalibrierten oder medizinischen Vorgaben.

const THRESHOLDS = { tsb: { ok: -10, warn: -25 }, acwr: { lo: 0.8, hi: 1.3 } };

const READY_METRICS = [
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

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

// Körperwerte (objektiv) gegen die eigenen Mediane der 14 Tage VOR heute (heute zählt nicht mit, damit ein
// auffälliger Wert seinen eigenen Vergleichswert nicht verschiebt). Fehlende Werte = "none", nie "gut".
const BODY_LIMITS = { sleepHours: { ok: 7, warn: 6 }, hrv: { bad: 0.7, warn: 0.85, up: 1.05 }, restingHR: { warn: 3, bad: 6 } };

function computeBody(wellness, todayIso) {
  const today = wellness.find((w) => w.date === todayIso);
  const past = wellness.filter((w) => w.date >= addDays(todayIso, -14) && w.date < todayIso);
  const med = (key) => { const v = past.map((w) => w[key]).filter((x) => x != null); return v.length ? median(v) : null; };
  const hrvMedian = med("hrv"), restingMedian = med("restingHR"), sleepMedian = med("sleepHours");
  const L = BODY_LIMITS;
  const h = today?.sleepHours ?? null, hrv = today?.hrv ?? null, rhr = today?.restingHR ?? null;
  const q = hrv != null && hrvMedian > 0 ? hrv / hrvMedian : null;
  return {
    sleep: { cls: h == null ? "none" : h >= L.sleepHours.ok ? "ok" : h >= L.sleepHours.warn ? "warn" : "bad" },
    hrv: { cls: q == null ? "none" : q < L.hrv.bad ? "bad" : q < L.hrv.warn ? "warn" : "ok", trend: q == null ? null : q < L.hrv.warn ? "down" : q > L.hrv.up ? "up" : "flat" },
    resting: { cls: rhr == null || restingMedian == null ? "none" : rhr - restingMedian >= L.restingHR.bad ? "bad" : rhr - restingMedian >= L.restingHR.warn ? "warn" : "ok" },
    sleepMedian, hrvMedian, restingMedian,
  };
}

// Heutiger Eintrag gegen den eigenen üblichen Bereich der letzten 8 Wochen. Skala ab 1 = bestmöglich:
// Auffällig ist ein Wert nur, wenn er über dem üblichen Bereich (Quartile) liegt: ein Schritt schlechter als
// der Median = Achtung, zwei oder mehr = deutlich. Ohne heutigen Eintrag gibt es bewusst keine Einschätzung,
// fehlende Werte zählen nie als "gut". Das Urteil beachtet auch die Körperwerte (Schlaf, HRV, Ruhepuls).
// Die Gefühlswerte (Muskelkater, Motivation, ...) trägt der Nutzer immer VOR dem Lauf ein: sie beschreiben den
// Ausgangszustand, nicht die Reaktion auf die Einheit des Tages, und dürfen nicht als Lauffolge gelesen werden.
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
    const delta = v - median(base), out = v > band[1];
    const cls = out && delta >= 2 ? "bad" : out && delta >= 1 ? "warn" : "ok";
    return { ...m, v, max, band, med: median(base), cls, text: cls === "bad" ? "deutlich schlechter" : cls === "warn" ? "etwas schlechter" : "wie üblich" };
  });
  const body = computeBody(wellness, todayIso);
  const bodyCls = [body.sleep.cls, body.hrv.cls, body.resting.cls];
  const hasScales = items.some((i) => i.v != null);
  const nBad = items.filter((i) => i.cls === "bad").length;
  const nWarn = items.filter((i) => i.cls === "warn").length;
  const bodyBad = bodyCls.filter((c) => c === "bad").length;
  const bodyWarn = bodyCls.filter((c) => c === "warn").length;
  const f = (x) => String(Math.round(x * 10) / 10).replace(".", ",");
  const reasons = [
    ...items.filter((i) => i.cls === "warn" || i.cls === "bad").map((i) => `${i.label} ${i.v} (üblich ${f(i.med)})`),
    body.sleep.cls === "warn" || body.sleep.cls === "bad" ? `Schlaf ${f(today.sleepHours)} h (Ziel ab ${BODY_LIMITS.sleepHours.ok} h)` : null,
    body.hrv.cls === "warn" || body.hrv.cls === "bad" ? `HRV ${f(today.hrv)} (Ø ${f(body.hrvMedian)})` : null,
    body.resting.cls === "warn" || body.resting.cls === "bad" ? `Ruhepuls ${f(today.restingHR)} (Ø ${f(body.restingMedian)})` : null,
    load?.tsb != null && load.tsb < THRESHOLDS.tsb.ok ? `Form (TSB) ${f(load.tsb)}` : null,
  ].filter(Boolean);
  let verdict;
  if (!hasScales) verdict = { cls: "none", text: "Heute noch nichts eingetragen", sub: "Ohne Eintrag gebe ich keine Einschätzung ab." };
  else if (nBad >= 1 || bodyBad >= 2 || nWarn + bodyWarn + bodyBad >= 3) verdict = { cls: "bad", text: "Eher ruhig angehen", sub: "Mehrere Werte liegen schlechter als üblich." };
  // Ein einzelner knapper Warnwert (z. B. Motivation einen Schritt über dem Median) ist normales Rauschen: erst zwei Warnungen, ein deutlich schlechter Körperwert oder niedrige Form zählen.
  else if (nWarn + bodyWarn + bodyBad >= 2 || bodyBad >= 1 || (load?.tsb != null && load.tsb < THRESHOLDS.tsb.ok)) verdict = { cls: "warn", text: "Mit Vorsicht", sub: "Mehrere Werte oder die Belastung sind auffällig." };
  else verdict = { cls: "ok", text: "Bereit", sub: "Die Werte liegen im üblichen Bereich." };
  verdict.reasons = reasons;
  return { verdict, items, body, sleepHours: today?.sleepHours ?? null, hrv: today?.hrv ?? null, restingHR: today?.restingHR ?? null };
}

export function buildSummary(wellness, todayIso) {
  const load = computeLoad(wellness, todayIso);
  return { thresholds: THRESHOLDS, load, readiness: computeReadiness(wellness, todayIso, load) };
}
