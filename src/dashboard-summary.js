// Einschätzungen für Dashboard-Seite und Widget an EINER Stelle, damit beide dasselbe zeigen:
// Frische (TSB), ACWR und "Bereit für Training?". Die Erholungsampel folgt den Regeln des Coachings (BODY_RULES),
// damit Dashboard und Morgen-Update dasselbe Urteil fällen. Keine medizinischen Vorgaben.

// tsb: alte Punktgrenzen, nur noch fuers Widget. tsbPct: Frische in % der Fitness (CTL) wie die Formzonen von Intervals.icu:
// unter −30 % hohes Risiko, −30 bis −10 % produktives Training, −10 bis +5 % neutral, +5 bis +25 % frisch, darueber Uebergang
// (Fitness sinkt). Ein negativer TSB in Belastungswochen ist gewollt und keine Warnung.
const THRESHOLDS = { tsb: { ok: -10, warn: -25 }, tsbPct: { risk: -30, optimal: -10, fresh: 5, transition: 25 }, acwr: { lo: 0.8, hi: 1.3 } };

const READY_METRICS = [
  { key: "sleepQuality", label: "Schlaf", soft: true },
  { key: "fatigue", label: "Ermüdung" },
  { key: "soreness", label: "Muskelkater" },
  { key: "mood", label: "Stimmung", soft: true },
  { key: "motivation", label: "Motivation", soft: true },
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

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

// Körperwerte (objektiv) nach den Regeln des Coachings: Basis = Median der 28 Tage VOR heute (heute zählt nicht mit),
// ohne Krankheitstage und die 3 Tage danach, nur Tage mit Wert. Unter 5 Werten keine Aussage, unter 10 "Basis dünn".
// Einzelne Tage streuen stark (HRV bei dir 22–53), deshalb zählt ein leichter Ausschlag nur mit Bestätigung:
//   HRV: Einbruch ≥ 20 % unter Basis zählt allein. 12–20 % unter Basis zählt nur, wenn Ruhepuls, Schlaf oder ein Trend
//        ihn bestätigen. Trend: 7-Tage-Schnitt ≥ 7 % unter dem Median der 4 Wochen davor und heute noch ≥ 5 % darunter.
//   Ruhepuls: ≥ 8 über Basis = rot, ≥ 6 zählt allein, 4–5 nur mit Bestätigung. Trend: 7-Tage-Schnitt ≥ 3 über dem
//        Median der 4 Wochen davor und heute noch ≥ 2 darüber.
//   Schlaf: Schlaf-Score < 80 an 2 Tagen in Folge (ohne Score: unter 6 h).
// Gelb = ein Signal, Rot = Ruhepuls ≥ 8 über Basis oder zwei Signale. Zeigen HRV und Ruhepuls nur über den Trend aus, ist
// das eine Beobachtung (dieselbe Woche), also ein Signal. Ein unbestätigter Einzelwert bleibt "ok" mit
// single = true (Hinweis, kein Signal). Fehlende Werte = "none", nie "gut".
export const BODY_RULES = {
  baseDays: 28, minN: 5, thinN: 10, sickTail: 3,
  restingHR: { mild: 4, warn: 6, bad: 8 },
  hrvDrop: 0.88, hrvStrong: 0.8, hrvUp: 1.05,
  trend: { days: 7, minN: 4, hrv: 0.93, resting: 3, hrvToday: 0.95, restingToday: 2 },
  sleepScore: 80, sleepHoursMin: 6,
};

function sickSetOf(sickDays) {
  const set = new Set();
  for (const d of sickDays ?? []) for (let k = 0; k <= BODY_RULES.sickTail; k++) set.add(addDays(d, k));
  return set;
}

function computeBody(wellness, todayIso, sickDays = []) {
  const R = BODY_RULES, T = R.trend, skip = sickSetOf(sickDays);
  const by = Object.fromEntries(wellness.map((w) => [w.date, w]));
  const today = by[todayIso], yday = by[addDays(todayIso, -1)];
  const past = wellness.filter((w) => w.date >= addDays(todayIso, -R.baseDays) && w.date < todayIso && !skip.has(w.date));
  const base = (key, list = past) => { const v = list.map((w) => w[key]).filter((x) => x != null); return { med: v.length >= R.minN ? median(v) : null, n: v.length }; };
  // Trend-Basis: die 28 Tage VOR der Trendwoche, sonst steckt ein langsamer Abfall schon in der Basis und hebt sich auf
  const prior = wellness.filter((w) => w.date >= addDays(todayIso, -(T.days - 1) - R.baseDays) && w.date <= addDays(todayIso, -T.days) && !skip.has(w.date));
  // 7-Tage-Schnitt bis einschließlich heute (Krankheitstage zählen mit: danach soll die Ampel vorsichtig bleiben)
  const recent = wellness.filter((w) => w.date > addDays(todayIso, -T.days) && w.date <= todayIso);
  const mean7 = (key) => { const v = recent.map((w) => w[key]).filter((x) => x != null); return v.length >= T.minN ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  const hrvB = base("hrv"), rhrB = base("restingHR"), sleepB = base("sleepHours");
  const h = today?.sleepHours ?? null, score = today?.sleepScore ?? null, hrv = today?.hrv ?? null, rhr = today?.restingHR ?? null;
  const q = hrv != null && hrvB.med > 0 ? hrv / hrvB.med : null;
  const diff = rhr != null && rhrB.med != null ? rhr - rhrB.med : null;
  const hrvT = base("hrv", prior).med, rhrT = base("restingHR", prior).med;
  const hrv7 = hrvT > 0 ? mean7("hrv") : null, rhr7 = rhrT != null ? mean7("restingHR") : null;
  const sleepCls = score != null
    ? (score < R.sleepScore && (yday?.sleepScore ?? 100) < R.sleepScore ? "warn" : "ok")
    : h == null ? "none" : h < R.sleepHoursMin ? "warn" : "ok";
  const hrvTrend = hrv7 != null && hrv7 <= hrvT * T.hrv && (hrv == null || hrv <= hrvT * T.hrvToday);
  const rhrTrend = rhr7 != null && rhr7 >= rhrT + T.resting && (rhr == null || rhr >= rhrT + T.restingToday);
  const hrvMild = q != null && q <= R.hrvDrop, rhrMild = diff != null && diff >= R.restingHR.mild;
  // Bestätigung eines leichten Ausschlags: ein anderes Körpersignal oder ein Trend zeigt in dieselbe Richtung
  const hrvDay = (q != null && q <= R.hrvStrong) || (hrvMild && (rhrMild || rhrTrend || sleepCls === "warn"));
  const rhrDay = (diff != null && diff >= R.restingHR.warn) || (rhrMild && (hrvMild || hrvTrend || sleepCls === "warn"));
  const hrvOk = hrvDay || hrvTrend, rhrOk = rhrDay || rhrTrend;
  const hrvCls = hrvOk ? "warn" : q == null && hrv7 == null ? "none" : "ok";
  const rhrCls = diff != null && diff >= R.restingHR.bad ? "bad" : rhrOk ? "warn" : diff == null && rhr7 == null ? "none" : "ok";
  return {
    sleep: { cls: sleepCls, score, hours: h },
    hrv: { cls: hrvCls, trend: q == null ? null : q <= R.hrvDrop ? "down" : q > R.hrvUp ? "up" : "flat", mean7: hrv7, base7: hrvT, low7: hrvTrend, trendOnly: hrvTrend && !hrvDay, single: hrvMild && !hrvOk, thin: hrvB.n < R.thinN, n: hrvB.n },
    resting: { cls: rhrCls, mean7: rhr7, base7: rhrT, high7: rhrTrend, trendOnly: rhrTrend && !rhrDay && rhrCls === "warn", single: rhrMild && rhrCls === "ok", thin: rhrB.n < R.thinN, n: rhrB.n },
    sleepMedian: sleepB.med, hrvMedian: hrvB.med, restingMedian: rhrB.med,
  };
}

// Gefühlswerte gegen den eigenen üblichen Bereich der letzten 8 Wochen. Skala ab 1 = bestmöglich: auffällig ist ein Wert nur,
// wenn er über dem üblichen Bereich (Quartile) liegt: ein Schritt schlechter als der Median = Achtung, zwei oder mehr = deutlich.
// Die Gefühlswerte trägt der Nutzer immer VOR dem Lauf ein: sie beschreiben den Ausgangszustand, nicht die Reaktion auf die
// Einheit des Tages, und dürfen nicht als Lauffolge gelesen werden. Fehlt der heutige Eintrag, gilt der von gestern früh.
// Urteil (Coaching-Regeln): Krankheit (Kalender) = kein Training. Rot = Ruhepuls ≥ 8 über Basis, zwei gelbe Körpersignale,
// Ermüdung/Muskelkater deutlich schlechter oder drei Warnungen insgesamt. Gelb = ein gelbes Körpersignal, zwei
// Warnungen, Schlafqualität/Stimmung/Motivation deutlich schlechter oder Hüfte/Leiste/Knie in den letzten 3 Tagen erwähnt.
// Die eingetragene Schlafqualität macht allein höchstens gelb und zählt nicht zusätzlich, wenn der Schlaf-Score schon
// meldet (Schlaf zählt einmal). Eine einzelne knappe Gefühls-Warnung ist Rauschen. Die Trainingslast (TSB) gehört nicht ins Erholungsurteil.
// ctx: { sickDays: [date], hipFlags: [{date}] }
export function computeReadiness(wellness, todayIso, load = null, ctx = {}) {
  const yIso = addDays(todayIso, -1);
  const entry = (d) => wellness.find((w) => w.date === d && READY_METRICS.some((m) => w[m.key] != null)) ?? null;
  const todayE = entry(todayIso), ydayE = todayE ? null : entry(yIso);
  const src = todayE ?? ydayE;
  const past = wellness.filter((w) => w.date < (src?.date ?? todayIso));
  const items = READY_METRICS.map((m) => {
    const v = src ? src[m.key] : null;
    const base = past.map((w) => w[m.key]).filter((x) => x != null);
    const max = Math.max(2, ...wellness.map((w) => w[m.key] ?? 0));
    const band = base.length >= 5 ? [quantile(base, 0.25), quantile(base, 0.75)] : null;
    if (v == null) return { ...m, v: null, max, band, cls: "none", text: "fehlt" };
    if (!band) return { ...m, v, max, band, cls: "none", text: "kein Vergleich" };
    const delta = v - median(base), out = v > band[1];
    const cls = out && delta >= 2 ? "bad" : out && delta >= 1 ? "warn" : "ok";
    return { ...m, v, max, band, med: median(base), cls, text: cls === "bad" ? "deutlich schlechter" : cls === "warn" ? "etwas schlechter" : "wie üblich" };
  });
  const body = computeBody(wellness, todayIso, ctx.sickDays);
  const today = wellness.find((w) => w.date === todayIso);
  const f = (x) => String(Math.round(x * 10) / 10).replace(".", ",");
  const bodyReason = (label, v, b, med) => [v != null && med != null ? `${label} ${f(v)} (Basis ${f(med)})` : null, b.low7 || b.high7 ? `7-Tage-Schnitt ${f(b.mean7)} (Wochen davor ${f(b.base7)})` : null].filter(Boolean).join(", ");
  const bodyWarn = [body.sleep, body.hrv, body.resting].filter((b) => b.cls === "warn").length - (body.hrv.trendOnly && body.resting.trendOnly ? 1 : 0);
  const bodyBad = body.resting.cls === "bad";
  // Schlaf zählt einmal: meldet schon der Schlaf-Score, zählt die eingetragene Schlafqualität nicht noch einmal
  const counted = items.filter((i) => !(i.key === "sleepQuality" && body.sleep.cls === "warn"));
  const hardBad = counted.filter((i) => i.cls === "bad" && !i.soft).length;
  const softBad = counted.filter((i) => i.cls === "bad" && i.soft).length;
  const subjWarn = counted.filter((i) => i.cls === "warn").length + softBad;
  const sick = (ctx.sickDays ?? []).includes(todayIso) || (ctx.sickDays ?? []).includes(yIso);
  const hip = (ctx.hipFlags ?? []).filter((x) => x.date >= addDays(todayIso, -2) && x.date <= todayIso);
  const reasons = [
    sick ? "Krankheit im Kalender" : null,
    ...hip.slice(0, 1).map((x) => `Hüfte/Leiste/Knie erwähnt (${x.date.slice(8, 10)}.${x.date.slice(5, 7)}.)`),
    body.resting.cls === "warn" || body.resting.cls === "bad" ? bodyReason("Ruhepuls", today?.restingHR, body.resting, body.restingMedian) : null,
    body.hrv.cls === "warn" ? bodyReason("HRV", today?.hrv, body.hrv, body.hrvMedian) : null,
    body.sleep.cls === "warn" ? (body.sleep.score != null ? `Schlaf-Score ${body.sleep.score} (2. Tag unter ${BODY_RULES.sleepScore})` : `Schlaf ${f(body.sleep.hours)} h`) : null,
    ...items.filter((i) => i.cls === "warn" || i.cls === "bad").map((i) => `${i.label} ${i.v} (üblich ${f(i.med)})`),
  ].filter(Boolean);
  const hasAny = Boolean(src) || [body.sleep, body.hrv, body.resting].some((b) => b.cls !== "none");
  let verdict;
  if (sick) verdict = { cls: "bad", sick: true, text: "Krank: kein Training", sub: "Krankheit geht vor Plan. Danach locker und kurz einsteigen, nichts nachholen." };
  else if (!hasAny) verdict = { cls: "none", text: "Keine Werte", sub: "Weder Uhr- noch Gefühlswerte für heute – keine Einschätzung." };
  else if (bodyBad || bodyWarn >= 2 || hardBad >= 1 || bodyWarn + subjWarn + hardBad >= 3) verdict = { cls: "bad", text: "Rot: nur locker und kurz", sub: "Qualität und Zusatzeinheiten streichen, lockere Grundlage kurz halten – oder frei." };
  else if (bodyWarn >= 1 || subjWarn >= 2 || softBad >= 1 || hip.length) verdict = { cls: "warn", text: "Gelb: heute nur locker", sub: "Qualität streichen, die lockere Grundlage bleibt." };
  else verdict = { cls: "ok", text: "Grün: Qualität möglich", sub: "Erholungswerte im üblichen Bereich." };
  verdict.reasons = reasons;
  if (ydayE) verdict.note = "Gefühlswerte von gestern früh (heute noch nicht eingetragen).";
  // Unbestätigte Einzelwerte: sichtbar machen, aber nicht werten
  const single = [body.hrv.single ? `HRV ${f(today.hrv)} (Basis ${f(body.hrvMedian)})` : null, body.resting.single ? `Ruhepuls ${f(today.restingHR)} (Basis ${f(body.restingMedian)})` : null].filter(Boolean);
  if (single.length) verdict.watch = `Einzelwert, allein kein Signal: ${single.join(" · ")}`;
  const thin = [body.hrv.thin && body.hrv.n >= BODY_RULES.minN ? "HRV" : null, body.resting.thin && body.resting.n >= BODY_RULES.minN ? "Ruhepuls" : null].filter(Boolean);
  if (thin.length) verdict.thin = `Basis noch dünn: ${thin.join(", ")}`;
  return { verdict, items, body, entryDate: src?.date ?? null, sleepHours: today?.sleepHours ?? null, sleepScore: today?.sleepScore ?? null, hrv: today?.hrv ?? null, restingHR: today?.restingHR ?? null };
}

export function buildSummary(wellness, todayIso, ctx = {}) {
  const load = computeLoad(wellness, todayIso);
  return { thresholds: THRESHOLDS, load, readiness: computeReadiness(wellness, todayIso, load, ctx) };
}
