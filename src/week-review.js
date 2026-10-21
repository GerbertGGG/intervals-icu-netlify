// Montagsbilanz als Faktenliste: Was ist in der letzten vollen Woche passiert, im Vergleich zu den bis zu 4 vollen Wochen
// davor? Gefühlswerte (vor dem Training eingetragen, also Ausgangszustand), Session-RPE und Monotonie, Laufumfang, längster
// Lauf, Minuten über dem HF-Deckel, Wochenrahmen des Coaches (Ist gegen Zeitziel und TSS-Grenze), Kraft, Krankheit und
// Beschwerden. Keine Stufe und kein Vorschlag: Wie es in der neuen Woche weitergeht, entscheidet der Coach.

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);

// rpeCoverage: Session-RPE erst ab 70 % der Einheiten mit RPE, sonst rechnet die Monotonie mit dem Tages-TSS
export const REVIEW_RULES = { rpeCoverage: 0.7 };
const SPORTS = ["run", "bike", "swim", "strength", "other"];

// input: { weeks (buildWeeks), wellness, sessions: [{date, sport, minutes, rpe}], daily: [{date, load}], sickDays, hipFlags,
//   runs: [{date, movingTimeMin}], capRuns: [{date, overCapMin}]|null, strength: {count, goal, unit}|null (Kraft der letzten Woche) }
export function buildWeekReview(input) {
  const R = REVIEW_RULES;
  const full = (input.weeks ?? []).filter((w) => w.complete);
  const W = full[full.length - 1];
  if (!W) return null;
  const from = W.weekStart, to = addDays(from, 6);
  const days = Array.from({ length: 7 }, (_, i) => addDays(from, i));
  const prior = full.slice(-5, -1);
  const inWeek = (d) => d >= from && d <= to;
  const priorFrom = prior[0]?.weekStart ?? from;
  const inPrior = (d) => d >= priorFrom && d < from;

  // Gefühlswerte (1 = bestmöglich): Wochenschnitt gegen die 4 Wochen davor
  const feel = {};
  for (const [k, label] of [["fatigue", "Ermüdung"], ["soreness", "Muskelkater"], ["stress", "Stress"]]) {
    const cur = mean((input.wellness ?? []).filter((w) => inWeek(w.date) && w[k] != null).map((w) => w[k]));
    const ref = mean((input.wellness ?? []).filter((w) => inPrior(w.date) && w[k] != null).map((w) => w[k]));
    if (cur != null) feel[k] = { label, mean: r1(cur), ref: r1(ref) };
  }

  // Session-RPE (RPE × Minuten) und Monotonie (Foster): Tagesschnitt ÷ Standardabweichung über 7 Tage, Ruhetage = 0
  const ses = (input.sessions ?? []).filter((x) => inWeek(x.date) && (x.minutes ?? 0) >= 10);
  const withRpe = ses.filter((x) => x.rpe > 0);
  const coverage = ses.length ? withRpe.length / ses.length : null;
  const srpe = withRpe.reduce((a, x) => a + x.rpe * x.minutes, 0);
  const priorSes = (input.sessions ?? []).filter((x) => inPrior(x.date) && x.rpe > 0 && (x.minutes ?? 0) >= 10);
  const srpeRef = prior.length && priorSes.length ? priorSes.reduce((a, x) => a + x.rpe * x.minutes, 0) / prior.length : null;
  const useRpe = coverage != null && coverage >= R.rpeCoverage;
  const perDay = days.map((d) => (useRpe ? withRpe.filter((x) => x.date === d).reduce((a, x) => a + x.rpe * x.minutes, 0) : (input.daily ?? []).find((x) => x.date === d)?.load ?? 0));
  const m = mean(perDay), sd = Math.sqrt(mean(perDay.map((v) => (v - m) ** 2)));
  const monotony = m > 0 && sd > 0 ? Math.round((m / sd) * 100) / 100 : null;

  // Laufen: Umfang gegen den Schnitt der 4 Wochen davor, längster Lauf, HF-Deckel
  const runRef = prior.length ? mean(prior.map((w) => w.runMin)) : null;
  const longest = (rs) => rs.reduce((a, r) => Math.max(a, r.movingTimeMin ?? 0), 0) || null;
  const capOver = input.capRuns ? input.capRuns.filter((r) => inWeek(r.date)).reduce((a, r) => a + (r.overCapMin ?? 0), 0) : null;

  // Wochenrahmen des Coaches: Ist gegen Zeitziel und TSS-Grenze
  const frame = SPORTS.filter((k) => W.bySport[k]?.targetMin > 0 || W.bySport[k]?.targetLoad > 0).map((k) => {
    const b = W.bySport[k];
    return { sport: k, minutes: b.minutes, targetMin: b.targetMin, load: b.load, targetLoad: b.targetLoad, over: b.targetLoad > 0 && b.load > b.targetLoad };
  });
  const tMin = frame.reduce((a, f) => a + (f.targetMin ?? 0), 0), dMin = frame.reduce((a, f) => a + (f.targetMin ? f.minutes : 0), 0);

  return {
    weekStart: from, weekEnd: to, feel,
    rpe: { coverage: coverage != null ? Math.round(coverage * 100) / 100 : null, srpe: withRpe.length ? Math.round(srpe) : null, ref: srpeRef != null ? Math.round(srpeRef) : null, monotony, basis: useRpe ? "rpe" : "tss" },
    run: { minutes: W.runMin, ref: runRef != null ? Math.round(runRef) : null, ratio: runRef > 0 ? Math.round((W.runMin / runRef) * 100) / 100 : null,
      longest: longest((input.runs ?? []).filter((r) => inWeek(r.date))), longestRef: longest((input.runs ?? []).filter((r) => inPrior(r.date))), capOver },
    frame, frameShare: tMin > 0 ? Math.round((dMin / tMin) * 100) / 100 : null,
    strength: input.strength ?? null,
    sickDays: (input.sickDays ?? []).filter(inWeek),
    hipDays: [...new Set((input.hipFlags ?? []).filter((x) => inWeek(x.date)).map((x) => x.date))],
  };
}
