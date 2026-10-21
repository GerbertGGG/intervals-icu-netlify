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
  // Stimmung und Motivation spiegeln oft Job und Alltag, nicht die Erholung: sie zählen schwächer (weak)
  { key: "mood", label: "Stimmung", soft: true, weak: true },
  { key: "motivation", label: "Motivation", soft: true, weak: true },
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
  trend: { days: 7, minN: 4, hrv: 0.93, resting: 3, hrvToday: 0.95, restingToday: 2, recoverDays: 3 },
  sleepScore: 80, sleepHoursMin: 6,
  // Ausreißer bleiben aus der Basis (z. B. eine Krankheitsnacht ohne Kalendereintrag): HRV unter 70 % bzw. Ruhepuls über +8
  outlier: { hrv: 0.7, restingHR: 8 },
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
  const keep = { hrv: (v, m) => v >= m * R.outlier.hrv, restingHR: (v, m) => v <= m + R.outlier.restingHR };
  const base = (key, list = past) => {
    let v = list.map((w) => w[key]).filter((x) => x != null);
    if (keep[key] && v.length >= R.minN) { const m = median(v); v = v.filter((x) => keep[key](x, m)); }
    return { med: v.length >= R.minN ? median(v) : null, n: v.length };
  };
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
  // Erholung: Liegen die letzten 3 Tage schon besser als der 7-Tage-Schnitt, zieht nur ein vergangenes Tief den Schnitt
  // herunter; dann zählt der Trend nicht mehr (z. B. HRV 37/38/38, danach 44/46/43)
  const last3 = (key) => { const v = wellness.filter((w) => w.date > addDays(todayIso, -T.recoverDays) && w.date <= todayIso).map((w) => w[key]).filter((x) => x != null); return v.length >= 2 ? v.reduce((a, b) => a + b, 0) / v.length : null; };
  const hrv3 = last3("hrv"), rhr3 = last3("restingHR");
  const hrvRecovering = hrv7 != null && hrv3 != null && hrv3 > hrv7, rhrRecovering = rhr7 != null && rhr3 != null && rhr3 < rhr7;
  const hrvTrend = hrv7 != null && hrv7 <= hrvT * T.hrv && (hrv == null || hrv <= hrvT * T.hrvToday) && !hrvRecovering;
  const rhrTrend = rhr7 != null && rhr7 >= rhrT + T.resting && (rhr == null || rhr >= rhrT + T.restingToday) && !rhrRecovering;
  const hrvMild = q != null && q <= R.hrvDrop, rhrMild = diff != null && diff >= R.restingHR.mild;
  // Bestätigung eines leichten Ausschlags: ein anderes Körpersignal oder ein Trend zeigt in dieselbe Richtung
  const hrvDay = (q != null && q <= R.hrvStrong) || (hrvMild && (rhrMild || rhrTrend || sleepCls === "warn"));
  const rhrDay = (diff != null && diff >= R.restingHR.warn) || (rhrMild && (hrvMild || hrvTrend || sleepCls === "warn"));
  const hrvOk = hrvDay || hrvTrend, rhrOk = rhrDay || rhrTrend;
  const hrvCls = hrvOk ? "warn" : q == null && hrv7 == null ? "none" : "ok";
  const rhrCls = diff != null && diff >= R.restingHR.bad ? "bad" : rhrOk ? "warn" : diff == null && rhr7 == null ? "none" : "ok";
  return {
    sleep: { cls: sleepCls, score, hours: h },
    hrv: { cls: hrvCls, trend: q == null ? null : q <= R.hrvDrop ? "down" : q > R.hrvUp ? "up" : "flat", mean7: hrv7, base7: hrvT, low7: hrvTrend, recovering: hrvRecovering, trendOnly: hrvTrend && !hrvDay, single: hrvMild && !hrvOk, thin: hrvB.n < R.thinN, n: hrvB.n },
    resting: { cls: rhrCls, mean7: rhr7, base7: rhrT, high7: rhrTrend, recovering: rhrRecovering, trendOnly: rhrTrend && !rhrDay && rhrCls === "warn", single: rhrMild && rhrCls === "ok", thin: rhrB.n < R.thinN, n: rhrB.n },
    sleepMedian: sleepB.med, hrvMedian: hrvB.med, restingMedian: rhrB.med,
  };
}

// Langzeitbasis: Die 28-Tage-Basis wandert bei schleichender Ermüdung über 4–6 Wochen mit, der Trend (7 Tage gegen die
// 4 Wochen davor) dann auch. Deshalb ein zweiter, fester Bezug: Median der Tage 29–90 vor heute (ohne Krankheitstage,
// mind. 20 Werte). Abstand = 7-Tage-Schnitt (ohne Krankheitstage, mind. 4 Werte) HRV ≤ 92 % bzw. Ruhepuls ≥ +3 davon.
export const DRIFT_RULES = { from: 90, to: 29, minN: 20, recentMinN: 4, hrv: 0.92, resting: 3 };
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
  const hrvLow = hrv7 != null && hrv7 <= hrvA * R.hrv, rhrHigh = rhr7 != null && rhr7 >= rhrA + R.resting;
  return { cls: hrvLow || rhrHigh ? "warn" : "ok", hrv7, hrvAnchor: hrvA, rhr7, rhrAnchor: rhrA, hrvLow, rhrHigh };
}

// Leistungssignal: Puls bei lockerem Tempo. Letzter lockerer Lauf (Grundlage/lang, mind. 30 min, Puls) der 3 Tage VOR heute
// gegen den Median der lockeren Läufe der 6 Wochen davor (mind. 4). Effizienz = Tempo je Herzschlag (m/min ÷ HF).
// Liegt sie ≥ 6 % darunter, ist das eine Warnung (Hügel, Wind und Hitze verfälschen sie, deshalb allein kein Gelb).
// Heutige Läufe zählen nicht: die Ampel beschreibt den Zustand vor dem Training.
// since: erst Läufe ab diesem Tag (Blockstart): Mit einem neuen HF-Deckel läuft man langsamer, die Effizienz fällt dann allein
// wegen der geringeren Intensität – ein Vergleich mit den Läufen vor dem Block wäre ein Fehlalarm.
export const PERF_RULES = { lookback: 3, baseDays: 42, minN: 4, minMin: 30, drop: 0.94 };
export function computeEasyRunSignal(runs, todayIso, since = null) {
  const P = PERF_RULES;
  const ef = (r) => (r.avgHr > 0 && r.paceSecPerKm > 0 ? (60000 / r.paceSecPerKm) / r.avgHr : null);
  const easy = (runs ?? []).filter((r) => ["base", "long"].includes(r.kind) && (r.movingTimeMin ?? 0) >= P.minMin && ef(r) != null && r.date < todayIso && (since == null || r.date >= since));
  const last = easy.filter((r) => r.date >= addDays(todayIso, -P.lookback)).sort((a, b) => b.date.localeCompare(a.date))[0];
  if (!last) return null;
  const base = easy.filter((r) => r.date < last.date && r.date >= addDays(last.date, -P.baseDays)).map(ef);
  if (base.length < P.minN) return null;
  const q = ef(last) / median(base);
  const dm = `${last.date.slice(8, 10)}.${last.date.slice(5, 7)}.`;
  return { cls: q <= P.drop ? "warn" : "ok", ratio: Math.round(q * 1000) / 1000, date: last.date, text: q <= P.drop ? `Puls bei lockerem Tempo erhöht (Lauf ${dm}: ${Math.round((1 - q) * 100)} % weniger Tempo je Herzschlag als üblich)` : null };
}

// Gefühl in den Qualitätseinheiten (Rad-HIIT, Hügelsprints; nur feel und RPE, kein Puls): Wird feel über die letzten 3
// Einheiten um 2 Stufen schlechter (Intervals: 1 = stark, 5 = schwach), ist das ein Warnsignal – vor Landsberg lief es 2, 2, 3, 4, 5.
// Zählt nur, solange die letzte dieser Einheiten höchstens 7 Tage zurückliegt.
export const QUALITY_RULES = { n: 3, worse: 2, maxAgeDays: 7 };
export function computeQualityFeelSignal(sessions, todayIso) {
  const Q = QUALITY_RULES;
  const list = (sessions ?? []).filter((x) => Number.isFinite(x.feel) && x.date < todayIso).sort((a, b) => a.date.localeCompare(b.date)).slice(-Q.n);
  if (list.length < Q.n || list[list.length - 1].date < addDays(todayIso, -Q.maxAgeDays)) return null;
  const d = list[list.length - 1].feel - list[0].feel;
  return { cls: d >= Q.worse ? "warn" : "ok", feels: list.map((x) => x.feel), text: d >= Q.worse ? `Gefühl in den Qualitätseinheiten kippt (feel ${list.map((x) => x.feel).join(" → ")})` : null };
}

// Puls bei fester Leistung (Rad-Vergleichseinheit, Messfenster nach dem Einrollen): letzte Einheit der 7 Tage vor heute gegen den
// Median der 3 davor. Mindestens 5 Schläge mehr bei gleicher Leistung = Warnsignal (Ermüdung oder beginnender Infekt).
// Einheiten ohne Einrollen zählen nicht (der Puls aus dem Stand verschiebt das Fenster).
export const FIXED_POWER_RULES = { n: 3, up: 5, maxAgeDays: 7 };
export function computeFixedPowerSignal(rides, todayIso) {
  const F = FIXED_POWER_RULES;
  const list = (rides ?? []).filter((r) => Number.isFinite(r.mainHr) && !r.noWarmup && r.date < todayIso).sort((a, b) => a.date.localeCompare(b.date));
  const last = list[list.length - 1];
  if (!last || last.date < addDays(todayIso, -F.maxAgeDays) || list.length < F.n + 1) return null;
  const ref = median(list.slice(-(F.n + 1), -1).map((r) => r.mainHr));
  const d = last.mainHr - ref;
  return { cls: d >= F.up ? "warn" : "ok", hr: last.mainHr, ref, date: last.date, text: d >= F.up ? `Puls bei fester Leistung ${Math.round(d)} höher als üblich (${last.mainHr} statt ${ref}${last.mainWatts ? ` bei ${last.mainWatts} W` : ""})` : null };
}

// Umfang je Ampelfarbe: Im Grundlagenblock ist fast alles locker, "nur locker" ändert dort kaum etwas. Die Ampel sagt
// deshalb auch, wie viel: grün = voller Wochenrahmen, gelb = etwa 30 % kürzer und keine lange Einheit, rot = frei.
export const DOSE = {
  ok: { factor: 1, text: "voller Umfang laut Wochenrahmen" },
  warn: { factor: 0.7, text: "etwa 30 % kürzer, lange Einheit verschieben" },
  bad: { factor: 0, text: "frei – oder höchstens 30 min locker Rad/Schwimmen" },
  sick: { factor: 0, text: "kein Training" },
  none: { factor: null, text: "ohne Werte keine Vorgabe" },
};

// Gefühlswerte gegen den eigenen üblichen Bereich der letzten 8 Wochen. Skala ab 1 = bestmöglich: auffällig ist ein Wert nur,
// wenn er über dem üblichen Bereich (Quartile) liegt: ein Schritt schlechter als der Median = Achtung, zwei oder mehr = deutlich.
// Die Gefühlswerte trägt der Nutzer immer VOR dem Lauf ein: sie beschreiben den Ausgangszustand, nicht die Reaktion auf die
// Einheit des Tages, und dürfen nicht als Lauffolge gelesen werden. Fehlt der heutige Eintrag, gilt der von gestern früh,
// grün gibt es dann aber nicht (höchstens gelb, bis eingetragen ist).
// Urteil (Coaching-Regeln): Krankheit (Kalender) = kein Training. Rot = Ruhepuls ≥ 8 über Basis, zwei gelbe Körpersignale,
// Ermüdung deutlich schlechter oder drei Warnungen insgesamt. Gelb = ein gelbes Körpersignal (auch Abstand zur Langzeitbasis),
// zwei Warnungen, Schlafqualität oder Muskelkater deutlich schlechter.
// Stimmung und Motivation zählen schwächer: "etwas schlechter" ist nur ein Hinweis, "deutlich schlechter" eine Warnung
// (allein kein Gelb). Das Leistungssignal (Puls bei lockerem Tempo erhöht, ctx.perf) ist ebenfalls eine Warnung.
// Je Sportart: Hüfte/Leiste/Knie in den letzten 3 Tagen oder Muskelkater deutlich schlechter = kein Lauf, Rad und
// Schwimmen nach der Ampel; Muskelkater etwas schlechter = Lauf kurz. Die eingetragene Schlafqualität macht allein höchstens
// gelb und zählt nicht zusätzlich, wenn der Schlaf-Score schon meldet (Schlaf zählt einmal). Die Trainingslast (TSB) gehört
// nicht ins Erholungsurteil.
// ctx: { sickDays: [date], hipFlags: [{date}], longWellness: [{date,hrv,restingHR}], perf: {cls,text}|null,
//   signals: [{cls,text}] (Gefühl in Qualitätseinheiten, Puls bei fester Leistung; je eine Warnung wie perf) }
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
  // Langzeitbasis zählt nur, wenn der Trend den Abfall nicht schon meldet (sonst dieselbe Beobachtung doppelt)
  const drift = computeDrift(ctx.longWellness, todayIso, ctx.sickDays);
  const driftWarn = drift?.cls === "warn" && !body.hrv.low7 && !body.resting.high7;
  if (drift) body.drift = { ...drift, counted: driftWarn };
  const bodyWarn = [body.sleep, body.hrv, body.resting].filter((b) => b.cls === "warn").length - (body.hrv.trendOnly && body.resting.trendOnly ? 1 : 0) + (driftWarn ? 1 : 0);
  const bodyBad = body.resting.cls === "bad";
  // Schlaf zählt einmal: meldet schon der Schlaf-Score, zählt die eingetragene Schlafqualität nicht noch einmal
  const counted = items.filter((i) => !(i.key === "sleepQuality" && body.sleep.cls === "warn"));
  const is = (key, cls) => counted.some((i) => i.key === key && i.cls === cls);
  const fatigueBad = is("fatigue", "bad"), soreBad = is("soreness", "bad"), soreWarn = is("soreness", "warn");
  const sleepQBad = is("sleepQuality", "bad");
  const weakBad = counted.filter((i) => i.weak && i.cls === "bad").length;
  const weakWarn = counted.filter((i) => i.weak && i.cls === "warn");
  // Leistungssignale (Puls bei lockerem Tempo, Gefühl in Qualitätseinheiten, Puls bei fester Leistung): je eine Warnung
  const perfs = [ctx.perf, ...(ctx.signals ?? [])].filter((x) => x?.cls === "warn");
  const perfWarn = perfs.length;
  const subjWarn = counted.filter((i) => !i.weak && i.cls === "warn").length + (sleepQBad ? 1 : 0) + weakBad + perfWarn;
  const sick = (ctx.sickDays ?? []).includes(todayIso) || (ctx.sickDays ?? []).includes(yIso);
  const hip = (ctx.hipFlags ?? []).filter((x) => x.date >= addDays(todayIso, -2) && x.date <= todayIso);
  const reasons = [
    sick ? "Krankheit im Kalender" : null,
    ...hip.slice(0, 1).map((x) => `Hüfte/Leiste/Knie erwähnt (${x.date.slice(8, 10)}.${x.date.slice(5, 7)}.)`),
    body.resting.cls === "warn" || body.resting.cls === "bad" ? bodyReason("Ruhepuls", today?.restingHR, body.resting, body.restingMedian) : null,
    body.hrv.cls === "warn" ? bodyReason("HRV", today?.hrv, body.hrv, body.hrvMedian) : null,
    driftWarn ? `Abstand zur Langzeitbasis (${[drift.hrvLow ? `HRV 7 Tage ${f(drift.hrv7)} statt ${f(drift.hrvAnchor)}` : null, drift.rhrHigh ? `Ruhepuls 7 Tage ${f(drift.rhr7)} statt ${f(drift.rhrAnchor)}` : null].filter(Boolean).join(", ")})` : null,
    body.sleep.cls === "warn" ? (body.sleep.score != null ? `Schlaf-Score ${body.sleep.score} (2. Tag unter ${BODY_RULES.sleepScore})` : `Schlaf ${f(body.sleep.hours)} h`) : null,
    ...items.filter((i) => (i.cls === "warn" && !i.weak) || i.cls === "bad").map((i) => `${i.label} ${i.v} (üblich ${f(i.med)})`),
    ...perfs.map((x) => x.text),
  ].filter(Boolean);
  const hasAny = Boolean(src) || [body.sleep, body.hrv, body.resting].some((b) => b.cls !== "none");
  let verdict;
  if (sick) verdict = { cls: "bad", sick: true, text: "Krank: kein Training", sub: "Krankheit geht vor Plan. Danach locker und kurz einsteigen, nichts nachholen." };
  else if (!hasAny) verdict = { cls: "none", text: "Keine Werte", sub: "Weder Uhr- noch Gefühlswerte für heute – keine Einschätzung." };
  else if (bodyBad || bodyWarn >= 2 || fatigueBad || bodyWarn + subjWarn + (soreBad ? 1 : 0) >= 3) verdict = { cls: "bad", text: "Rot: frei oder kurz und locker", sub: "Qualität und lange Einheiten streichen; wenn überhaupt, 30 min locker Rad oder Schwimmen." };
  else if (bodyWarn >= 1 || subjWarn >= 2 || sleepQBad || soreBad) verdict = { cls: "warn", text: "Gelb: locker und kürzer", sub: "Qualität streichen, Umfang etwa 30 % kürzer, die lange Einheit verschieben." };
  else verdict = { cls: "ok", text: "Grün: Qualität möglich", sub: "Erholungswerte im üblichen Bereich." };
  // Ohne heutigen Eintrag kein Grün: der Zustand von heute früh ist noch nicht bekannt
  if (verdict.cls === "ok" && !todayE) verdict = { cls: "warn", missing: true, text: "Gelb: Gefühlswerte fehlen", sub: "Heute noch nichts eingetragen – erst eintragen, dann gilt die Ampel. Bis dahin locker." };
  verdict.reasons = reasons;
  if (ydayE) verdict.note = "Gefühlswerte von gestern früh (heute noch nicht eingetragen).";
  verdict.dose = DOSE[verdict.sick ? "sick" : verdict.cls] ?? DOSE.none;
  // Je Sportart: Laufen hat eigene Bremsen (Hüfte/Leiste/Knie, Muskelkater), Rad und Schwimmen folgen der Ampel
  const general = { ok: "nach Wochenrahmen", warn: "locker und kürzer", bad: verdict.sick ? "kein Training" : "frei oder 30 min locker", none: "keine Einschätzung" }[verdict.cls];
  const run = verdict.cls === "bad" ? { cls: "bad", text: verdict.sick ? "kein Training" : "kein Lauf" }
    : hip.length ? { cls: "bad", text: "kein Lauf (Hüfte/Leiste/Knie erwähnt)" }
    : soreBad ? { cls: "bad", text: "kein Lauf (Muskelkater deutlich)" }
    : soreWarn ? { cls: "warn", text: "nur kurz und locker (Muskelkater)" }
    : { cls: verdict.cls, text: general };
  verdict.sports = { run, bike: { cls: verdict.cls, text: general }, swim: { cls: verdict.cls, text: general } };
  // Unbestätigte Einzelwerte: sichtbar machen, aber nicht werten
  const single = [body.hrv.single ? `HRV ${f(today.hrv)} (Basis ${f(body.hrvMedian)})` : null, body.resting.single ? `Ruhepuls ${f(today.restingHR)} (Basis ${f(body.restingMedian)})` : null, ...weakWarn.map((i) => `${i.label} ${i.v} (üblich ${f(i.med)})`)].filter(Boolean);
  if (single.length) verdict.watch = `Einzelwert, allein kein Signal: ${single.join(" · ")}`;
  const thin = [body.hrv.thin && body.hrv.n >= BODY_RULES.minN ? "HRV" : null, body.resting.thin && body.resting.n >= BODY_RULES.minN ? "Ruhepuls" : null].filter(Boolean);
  if (thin.length) verdict.thin = `Basis noch dünn: ${thin.join(", ")}`;
  return { verdict, items, body, entryDate: src?.date ?? null, sleepHours: today?.sleepHours ?? null, sleepScore: today?.sleepScore ?? null, hrv: today?.hrv ?? null, restingHR: today?.restingHR ?? null };
}

export function buildSummary(wellness, todayIso, ctx = {}) {
  const load = computeLoad(wellness, todayIso);
  return { thresholds: THRESHOLDS, load, readiness: computeReadiness(wellness, todayIso, load, ctx) };
}
