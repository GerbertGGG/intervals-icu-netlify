// Einschätzungen für Dashboard-Seite und Widget an EINER Stelle, damit beide dasselbe zeigen:
// Frische (TSB), ACWR und "Bereit für Training?". Die Erholungsampel folgt den Regeln des Coachings (BODY_RULES),
// damit Dashboard und Morgen-Update dasselbe Urteil fällen. Keine medizinischen Vorgaben.

// tsb: alte Punktgrenzen, nur noch fuers Widget. tsbPct: Frische in % der Fitness (CTL) wie die Formzonen von Intervals.icu:
// unter −30 % hohes Risiko, −30 bis −10 % produktives Training, −10 bis +5 % neutral, +5 bis +25 % frisch, darueber Uebergang
// (Fitness sinkt). Ein negativer TSB in Belastungswochen ist gewollt und keine Warnung.
const THRESHOLDS = { tsb: { ok: -10, warn: -25 }, tsbPct: { risk: -30, optimal: -10, fresh: 5, transition: 25 }, acwr: { lo: 0.8, hi: 1.3 } };

const READY_METRICS = [
  { key: "sleepQuality", label: "Schlaf" },
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
// Gelb: Ruhepuls ≥ 4 über Basis, HRV ≥ 12 % unter Basis, Schlaf-Score < 80 an 2 Tagen in Folge (ohne Score: unter 6 h).
// Rot: Ruhepuls ≥ 8 über Basis oder zwei gelbe Signale. Fehlende Werte = "none", nie "gut".
export const BODY_RULES = { baseDays: 28, minN: 5, thinN: 10, restingHR: { warn: 4, bad: 8 }, hrvDrop: 0.88, hrvUp: 1.05, sleepScore: 80, sleepHoursMin: 6, sickTail: 3 };

function sickSetOf(sickDays) {
  const set = new Set();
  for (const d of sickDays ?? []) for (let k = 0; k <= BODY_RULES.sickTail; k++) set.add(addDays(d, k));
  return set;
}

function computeBody(wellness, todayIso, sickDays = []) {
  const R = BODY_RULES, skip = sickSetOf(sickDays);
  const by = Object.fromEntries(wellness.map((w) => [w.date, w]));
  const today = by[todayIso], yday = by[addDays(todayIso, -1)];
  const past = wellness.filter((w) => w.date >= addDays(todayIso, -R.baseDays) && w.date < todayIso && !skip.has(w.date));
  const base = (key) => { const v = past.map((w) => w[key]).filter((x) => x != null); return { med: v.length >= R.minN ? median(v) : null, n: v.length }; };
  const hrvB = base("hrv"), rhrB = base("restingHR"), sleepB = base("sleepHours");
  const h = today?.sleepHours ?? null, score = today?.sleepScore ?? null, hrv = today?.hrv ?? null, rhr = today?.restingHR ?? null;
  const q = hrv != null && hrvB.med > 0 ? hrv / hrvB.med : null;
  const diff = rhr != null && rhrB.med != null ? rhr - rhrB.med : null;
  const sleepCls = score != null
    ? (score < R.sleepScore && (yday?.sleepScore ?? 100) < R.sleepScore ? "warn" : "ok")
    : h == null ? "none" : h < R.sleepHoursMin ? "warn" : "ok";
  return {
    sleep: { cls: sleepCls, score, hours: h },
    hrv: { cls: q == null ? "none" : q <= R.hrvDrop ? "warn" : "ok", trend: q == null ? null : q <= R.hrvDrop ? "down" : q > R.hrvUp ? "up" : "flat", thin: hrvB.n < R.thinN, n: hrvB.n },
    resting: { cls: diff == null ? "none" : diff >= R.restingHR.bad ? "bad" : diff >= R.restingHR.warn ? "warn" : "ok", thin: rhrB.n < R.thinN, n: rhrB.n },
    sleepMedian: sleepB.med, hrvMedian: hrvB.med, restingMedian: rhrB.med,
  };
}

// Gefühlswerte gegen den eigenen üblichen Bereich der letzten 8 Wochen. Skala ab 1 = bestmöglich: auffällig ist ein Wert nur,
// wenn er über dem üblichen Bereich (Quartile) liegt: ein Schritt schlechter als der Median = Achtung, zwei oder mehr = deutlich.
// Die Gefühlswerte trägt der Nutzer immer VOR dem Lauf ein: sie beschreiben den Ausgangszustand, nicht die Reaktion auf die
// Einheit des Tages, und dürfen nicht als Lauffolge gelesen werden. Fehlt der heutige Eintrag, gilt der von gestern früh.
// Urteil (Coaching-Regeln): Krankheit (Kalender) = kein Training. Rot = Ruhepuls ≥ 8 über Basis, zwei gelbe Körpersignale,
// Ermüdung/Muskelkater/Schlaf deutlich schlechter oder drei Warnungen insgesamt. Gelb = ein gelbes Körpersignal, zwei
// Warnungen, Stimmung/Motivation deutlich schlechter oder Hüfte/Leiste/Knie in den letzten 3 Tagen erwähnt. Eine einzelne
// knappe Gefühls-Warnung ist Rauschen. Die Trainingslast (TSB) gehört nicht ins Erholungsurteil.
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
  const bodyWarn = [body.sleep, body.hrv, body.resting].filter((b) => b.cls === "warn").length;
  const bodyBad = body.resting.cls === "bad";
  const hardBad = items.filter((i) => i.cls === "bad" && !i.soft).length;
  const softBad = items.filter((i) => i.cls === "bad" && i.soft).length;
  const subjWarn = items.filter((i) => i.cls === "warn").length + softBad;
  const sick = (ctx.sickDays ?? []).includes(todayIso) || (ctx.sickDays ?? []).includes(yIso);
  const hip = (ctx.hipFlags ?? []).filter((x) => x.date >= addDays(todayIso, -2) && x.date <= todayIso);
  const reasons = [
    sick ? "Krankheit im Kalender" : null,
    ...hip.slice(0, 1).map((x) => `Hüfte/Leiste/Knie erwähnt (${x.date.slice(8, 10)}.${x.date.slice(5, 7)}.)`),
    body.resting.cls === "warn" || body.resting.cls === "bad" ? `Ruhepuls ${f(today.restingHR)} (Basis ${f(body.restingMedian)})` : null,
    body.hrv.cls === "warn" ? `HRV ${f(today.hrv)} (Basis ${f(body.hrvMedian)})` : null,
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
  const thin = [body.hrv.thin && body.hrv.n >= BODY_RULES.minN ? "HRV" : null, body.resting.thin && body.resting.n >= BODY_RULES.minN ? "Ruhepuls" : null].filter(Boolean);
  if (thin.length) verdict.thin = `Basis noch dünn: ${thin.join(", ")}`;
  return { verdict, items, body, entryDate: src?.date ?? null, sleepHours: today?.sleepHours ?? null, sleepScore: today?.sleepScore ?? null, hrv: today?.hrv ?? null, restingHR: today?.restingHR ?? null };
}

export function buildSummary(wellness, todayIso, ctx = {}) {
  const load = computeLoad(wellness, todayIso);
  return { thresholds: THRESHOLDS, load, readiness: computeReadiness(wellness, todayIso, load, ctx) };
}
