// Montagsbilanz: Wie wurde die letzte volle Woche verkraftet, und wie stark darf der Wochenrahmen der laufenden Woche steigen?
// Grundlage sind nicht die Umfänge allein, sondern die Reaktion darauf: Ampeltage, Gefühlswerte im Wochenschnitt (vor dem
// Training eingetragen, also Ausgangszustand), Session-RPE und Monotonie, Laufumfang gegen den Schnitt (Stoßbelastung),
// Einhaltung des HF-Deckels, Beschwerden und Krankheit. Faustregeln, kein Trainingsplan.
//
// Stufe (für alle Sportarten, Laufen kann strenger sein):
//   Entlastungswoche (jede 4. Blockwoche)                                  → Entlastung, etwa −35 %
//   Krankheit, ein roter Tag                                                → zurücknehmen, etwa −25 %
//   ≥ 2 gelbe Tage, Ermüdung/Muskelkater ≥ 0,5 schlechter als in den 4 Wochen davor, Monotonie > 2,
//     Rahmen unter 75 % erfüllt oder TSS-Grenze überschritten              → halten
//   sonst                                                                   → steigern, +5–10 %
// Laufen extra: Beschwerden (Hüfte/Leiste/Knie) → zurücknehmen; Laufminuten > 15 % über dem 4-Wochen-Schnitt oder
//   > 10 min über dem HF-Deckel → halten; Laufen steigt höchstens 10 %.
//
// Vorschlag: Gab es letzte Woche einen Rahmen (Zeitziele), dann Zeitziel je Sportart × Faktor. Ohne Rahmen keine Minuten je
// Sportart (das Ist einer Krankheits-, Wettkampf- oder Übergangswoche ist keine Basis), sondern die Wochen-TSS gesamt aus der
// Fitness: 7 × CTL am Sonntag hält die Fitness, × Faktor ergibt den Vorschlag.

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const dm = (iso) => `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);
const r5 = (v) => Math.round(v / 5) * 5;

export const REVIEW_RULES = {
  feelDelta: 0.5, monotony: 2, rpeCoverage: 0.7, runJump: 1.15, runMaxUp: 0.1, capOverMin: 10, frameMin: 0.75, warnDays: 2,
  factor: { up: [1.05, 1.1], hold: [1, 1], down: [0.75, 0.75], deload: [0.65, 0.65] },
};
const LEVEL_TEXT = { up: "steigern (+5–10 %)", hold: "halten", down: "zurücknehmen (etwa −25 %)", deload: "Entlastung (etwa −35 %)" };
const SPORTS = ["run", "bike", "swim"];

// input: { today, weeks (buildWeeks), wellness, sessions: [{date, sport, minutes, rpe}], daily: [{date, load}],
//   ampel: (date) => cls, sickDays, hipFlags, runs: [{date, movingTimeMin}], capRuns: [{date, overCapMin}]|null,
//   block: {status, weekNo}|null }
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
  const signals = [];
  const add = (level, text, sport = null) => signals.push({ level, text, sport });

  // Ampeltage
  const ampel = { ok: 0, warn: 0, bad: 0, none: 0 };
  const ampelDays = days.map((d) => { const c = input.ampel?.(d) ?? "none"; ampel[c] = (ampel[c] ?? 0) + 1; return { date: d, cls: c }; });
  const sickDays = (input.sickDays ?? []).filter(inWeek);
  const sickText = sickDays.length ? `krank (${sickDays.length} Tag${sickDays.length === 1 ? "" : "e"})` : null;
  const redText = ampel.bad ? `${ampel.bad} rote${ampel.bad === 1 ? "r" : ""} Tag${ampel.bad === 1 ? "" : "e"}` : null;
  // Krankheit und rote Tage sind meist dasselbe Ereignis: ein Grund statt zwei
  if (sickText || redText) add("down", [sickText, redText].filter(Boolean).join(", "));
  else if (ampel.warn >= R.warnDays) add("hold", `${ampel.warn} gelbe Tage`);

  // Gefühlswerte (1 = bestmöglich): Wochenschnitt gegen die 4 Wochen davor
  const feel = {};
  for (const [k, label] of [["fatigue", "Ermüdung"], ["soreness", "Muskelkater"], ["stress", "Stress"]]) {
    const cur = mean((input.wellness ?? []).filter((w) => inWeek(w.date) && w[k] != null).map((w) => w[k]));
    const ref = mean((input.wellness ?? []).filter((w) => inPrior(w.date) && w[k] != null).map((w) => w[k]));
    if (cur == null) continue;
    const delta = ref != null ? cur - ref : null;
    feel[k] = { label, mean: r1(cur), ref: r1(ref), delta: r1(delta) };
    if (k !== "stress" && delta != null && delta >= R.feelDelta) add("hold", `${label} im Schnitt ${String(r1(cur)).replace(".", ",")} statt ${String(r1(ref)).replace(".", ",")}`);
  }

  // Session-RPE (RPE × Minuten) und Monotonie (Foster): Tagesschnitt ÷ Standardabweichung über 7 Tage, Ruhetage = 0.
  // Ohne RPE bei mind. 70 % der Einheiten rechnet die Monotonie mit dem Tages-TSS.
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
  const strain = monotony != null ? Math.round(perDay.reduce((a, b) => a + b, 0) * monotony) : null;
  if (monotony != null && monotony > R.monotony) add("hold", `Monotonie ${String(monotony).replace(".", ",")} (jeder Tag ähnlich belastet)`);

  // Laufen: Umfang gegen den Schnitt der 4 Wochen davor, längster Lauf, HF-Deckel, Beschwerden
  const runRef = prior.length ? mean(prior.map((w) => w.runMin)) : null;
  const runRatio = runRef > 0 ? W.runMin / runRef : null;
  if (runRatio != null && runRatio > R.runJump) add("hold", `Laufumfang ${Math.round((runRatio - 1) * 100)} % über dem Schnitt`, "run");
  const longest = (rs) => rs.reduce((a, r) => Math.max(a, r.movingTimeMin ?? 0), 0) || null;
  const longRun = longest((input.runs ?? []).filter((r) => inWeek(r.date)));
  const longRef = longest((input.runs ?? []).filter((r) => inPrior(r.date)));
  const capOver = input.capRuns ? input.capRuns.filter((r) => inWeek(r.date)).reduce((a, r) => a + (r.overCapMin ?? 0), 0) : null;
  if (capOver != null && capOver > R.capOverMin) add("hold", `${capOver} min über dem HF-Deckel`, "run");
  const hipDays = [...new Set((input.hipFlags ?? []).filter((x) => inWeek(x.date)).map((x) => x.date))];
  if (hipDays.length) add("down", `Hüfte/Leiste/Knie erwähnt (${hipDays.map(dm).join(", ")})`, "run");

  // Wochenrahmen: Zeit erfüllt? TSS-Grenze eingehalten?
  const frame = SPORTS.filter((k) => W.bySport[k].targetMin > 0 || W.bySport[k].targetLoad > 0).map((k) => {
    const b = W.bySport[k];
    return { sport: k, minutes: b.minutes, targetMin: b.targetMin, share: b.targetMin > 0 ? Math.round((b.minutes / b.targetMin) * 100) / 100 : null, load: b.load, targetLoad: b.targetLoad, over: b.targetLoad > 0 && b.load > b.targetLoad };
  });
  const tMin = frame.reduce((a, f) => a + (f.targetMin ?? 0), 0), dMin = frame.reduce((a, f) => a + (f.targetMin ? f.minutes : 0), 0);
  const frameShare = tMin > 0 ? dMin / tMin : null;
  if (frameShare != null && frameShare < R.frameMin) add("hold", `Rahmen nur zu ${Math.round(frameShare * 100)} % erfüllt`);
  for (const f of frame.filter((x) => x.over)) add("hold", `TSS-Grenze überschritten (${f.load} statt ≤ ${f.targetLoad})`, f.sport);

  // Stufe: die strengste Regel gewinnt; Entlastungswoche geht vor
  const rank = { up: 0, hold: 1, down: 2, deload: 3 };
  const pick = (xs) => xs.reduce((a, s) => (rank[s.level] > rank[a] ? s.level : a), "up");
  const curWeekNo = input.block?.status === "active" && input.block.weekNo != null ? input.block.weekNo : null;
  const deload = curWeekNo != null && curWeekNo % 4 === 0;
  const general = deload ? "deload" : pick(signals.filter((s) => !s.sport));
  const sportLevel = (k) => (deload ? "deload" : pick([{ level: general }, ...signals.filter((s) => s.sport === k)]));

  // Vorschlag: mit Rahmen Zeitziel je Sportart × Faktor (Laufen höchstens +10 %), ohne Rahmen Wochen-TSS gesamt = 7 × CTL × Faktor
  const hasFrame = frame.some((f) => f.targetMin > 0);
  const range = (base, level, run = false) => {
    let [lo, hi] = R.factor[level];
    if (run && level === "up") hi = Math.min(hi, 1 + R.runMaxUp);
    return { from: r5(base * lo), to: r5(base * hi) };
  };
  const ctl = [...(input.wellness ?? [])].filter((w) => w.date <= to && w.ctl != null).sort((a, b) => a.date.localeCompare(b.date)).pop()?.ctl ?? null;
  const proposal = hasFrame
    ? frame.filter((f) => f.targetMin > 0).map((f) => { const level = sportLevel(f.sport); return { sport: f.sport, unit: "min", basis: "frame", level, base: f.targetMin, ...range(f.targetMin, level, f.sport === "run") }; })
    : ctl > 0 ? [{ sport: "all", unit: "tss", basis: "ctl", level: general, ctl: Math.round(ctl), base: Math.round(ctl * 7), ...range(ctl * 7, general) }] : [];
  const runLevel = sportLevel("run");
  return {
    weekStart: from, weekEnd: to,
    level: general, text: LEVEL_TEXT[general], hasFrame, runLevel, runText: runLevel !== general ? LEVEL_TEXT[runLevel] : null,
    deload, curWeekNo,
    reasons: deload ? [`Woche ${curWeekNo} ist Entlastungswoche`] : signals.map((s) => (s.sport === "run" ? `Laufen: ${s.text}` : s.text)),
    ampel, ampelDays, feel,
    rpe: { coverage: coverage != null ? Math.round(coverage * 100) / 100 : null, srpe: withRpe.length ? Math.round(srpe) : null, ref: srpeRef != null ? Math.round(srpeRef) : null, monotony, strain, basis: useRpe ? "rpe" : "tss" },
    run: { minutes: W.runMin, ref: runRef != null ? Math.round(runRef) : null, ratio: runRatio != null ? Math.round(runRatio * 100) / 100 : null, longest: longRun, longestRef: longRef, capOver },
    frame, frameShare: frameShare != null ? Math.round(frameShare * 100) / 100 : null,
    hipDays, sickDays, proposal,
  };
}
