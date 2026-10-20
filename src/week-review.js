// Montagsbilanz: Wie wurde die letzte volle Woche verkraftet, und wie stark darf der Wochenrahmen der laufenden Woche steigen?
// Regel wie im Block-Eintrag ("Steigerung nach der Vorwoche"): Signale in vier Gruppen – Erholung (Ampeltage), Befinden
// (Gefühlswerte im Wochenschnitt, vor dem Training eingetragen, also Ausgangszustand; Monotonie), Ausführung (Rahmen, TSS-Grenze,
// HF-Deckel, Laufumfang, Kraft) und Warnzeichen (Hüfte/Leiste/Knie, Krankheit). Faustregeln, kein Trainingsplan.
//
// Stufe:
//   Entlastungswoche (jede 4. Blockwoche)                                  → Entlastung, 60–70 % der Vorwoche
//   Krankheit, ein roter Tag, Schmerz                                       → reduzieren, etwa −25 %
//     (Krankheit in einer Woche ohne Rahmen nur halten: dort war nichts geplant, und der Einbruch steckt schon in der CTL)
//   deutlich unter Rahmen (< 75 % erfüllt) oder Signale in ≥ 2 Gruppen gelb → halten (0 %)
//   Signale in genau einer Gruppe gelb ("zäh")                              → leicht steigern, +3–5 %
//   alles grün                                                              → steigern, +8–10 %
// Nie mehr als +10 % pro Woche; in einer Woche mit Blocktest höchstens +3 %. Gelb sind u. a. ≥ 2 gelbe Ampeltage,
// Ermüdung/Muskelkater ≥ 0,5 schlechter als in den 4 Wochen davor, Monotonie > 2, TSS-Grenze überschritten, Kraft unter Soll.
// Laufen extra: Beschwerden → reduzieren; Laufminuten > 15 % über dem 4-Wochen-Schnitt oder > 10 min über dem HF-Deckel → gelb.
//
// Vorschlag: Gab es letzte Woche einen Rahmen (Zeitziele), dann Zeitziel je Sportart × Faktor. Ohne Rahmen letzte Woche, aber
// mit Rahmen für die laufende Woche (vom Coach gesetzt): dieser je Sportart, die Stufe nimmt ihn nur zurück, steigert ihn nicht;
// dazu die Gegenprobe der TSS-Grenzen gegen die Erhaltungslast (7 × CTL). Ohne beides keine Minuten je Sportart (das Ist einer
// Krankheits-, Wettkampf- oder Übergangswoche ist keine Basis), sondern die Wochen-TSS gesamt aus der Fitness.

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const dm = (iso) => `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);
const r5 = (v) => Math.round(v / 5) * 5;

export const REVIEW_RULES = {
  feelDelta: 0.5, monotony: 2, rpeCoverage: 0.7, runJump: 1.15, runMaxUp: 0.1, capOverMin: 10, frameMin: 0.75, warnDays: 2,
  testWeekMax: 1.03, longRunDays: 30, longRunUp: 0.1,
  factor: { up: [1.08, 1.1], small: [1.03, 1.05], hold: [1, 1], down: [0.75, 0.75], deload: [0.6, 0.7] },
};
const LEVEL_TEXT = { up: "steigern (+8–10 %)", small: "leicht steigern (+3–5 %)", hold: "halten (0 %)", down: "reduzieren (etwa −25 %)", deload: "Entlastung (60–70 %)" };
export const REVIEW_GROUPS = [["erholung", "Erholung"], ["befinden", "Befinden"], ["ausfuehrung", "Ausführung"], ["warnzeichen", "Warnzeichen"]];
const SPORTS = ["run", "bike", "swim", "strength", "other"];

// input: { today, weeks (buildWeeks), wellness, sessions: [{date, sport, minutes, rpe}], daily: [{date, load}],
//   ampel: (date) => cls, sickDays, hipFlags, runs: [{date, movingTimeMin, distanceKm}], capRuns: [{date, overCapMin}]|null,
//   block: {status, weekNo}|null, testDates: [date] (Blocktests), strength: {count, goal, unit}|null (Kraft der letzten Woche) }
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
  // level: "warn" (gelb, zählt je Gruppe), "hold" (0 %) oder "down" (reduzieren)
  const add = (level, group, text, sport = null) => signals.push({ level, group, text, sport });

  // Ampeltage
  const ampel = { ok: 0, warn: 0, bad: 0, none: 0 };
  const ampelDays = days.map((d) => { const c = input.ampel?.(d) ?? "none"; ampel[c] = (ampel[c] ?? 0) + 1; return { date: d, cls: c }; });
  const sickDays = (input.sickDays ?? []).filter(inWeek);
  const sickText = sickDays.length ? `krank (${sickDays.length} Tag${sickDays.length === 1 ? "" : "e"})` : null;
  const redText = ampel.bad ? `${ampel.bad} rote${ampel.bad === 1 ? "r" : ""} Tag${ampel.bad === 1 ? "" : "e"}` : null;
  // Krankheit und rote Tage sind meist dasselbe Ereignis: ein Grund statt zwei
  const lastFramed = SPORTS.some((k) => W.bySport[k]?.targetMin > 0);
  if (sickText || redText) add(lastFramed ? "down" : "hold", sickText ? "warnzeichen" : "erholung", [sickText, redText].filter(Boolean).join(", "));
  else if (ampel.warn >= R.warnDays) add("warn", "erholung", `${ampel.warn} gelbe Tage`);

  // Gefühlswerte (1 = bestmöglich): Wochenschnitt gegen die 4 Wochen davor
  const feel = {};
  for (const [k, label] of [["fatigue", "Ermüdung"], ["soreness", "Muskelkater"], ["stress", "Stress"]]) {
    const cur = mean((input.wellness ?? []).filter((w) => inWeek(w.date) && w[k] != null).map((w) => w[k]));
    const ref = mean((input.wellness ?? []).filter((w) => inPrior(w.date) && w[k] != null).map((w) => w[k]));
    if (cur == null) continue;
    const delta = ref != null ? cur - ref : null;
    feel[k] = { label, mean: r1(cur), ref: r1(ref), delta: r1(delta) };
    if (k !== "stress" && delta != null && delta >= R.feelDelta) add("warn", "befinden", `${label} im Schnitt ${String(r1(cur)).replace(".", ",")} statt ${String(r1(ref)).replace(".", ",")}`);
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
  if (monotony != null && monotony > R.monotony) add("warn", "befinden", `Monotonie ${String(monotony).replace(".", ",")} (jeder Tag ähnlich belastet)`);

  // Laufen: Umfang gegen den Schnitt der 4 Wochen davor, längster Lauf, HF-Deckel, Beschwerden
  const runRef = prior.length ? mean(prior.map((w) => w.runMin)) : null;
  const runRatio = runRef > 0 ? W.runMin / runRef : null;
  if (runRatio != null && runRatio > R.runJump) add("warn", "ausfuehrung", `Laufumfang ${Math.round((runRatio - 1) * 100)} % über dem Schnitt`, "run");
  const longest = (rs) => rs.reduce((a, r) => Math.max(a, r.movingTimeMin ?? 0), 0) || null;
  const longRun = longest((input.runs ?? []).filter((r) => inWeek(r.date)));
  const longRef = longest((input.runs ?? []).filter((r) => inPrior(r.date)));
  const capOver = input.capRuns ? input.capRuns.filter((r) => inWeek(r.date)).reduce((a, r) => a + (r.overCapMin ?? 0), 0) : null;
  if (capOver != null && capOver > R.capOverMin) add("warn", "ausfuehrung", `${capOver} min über dem HF-Deckel`, "run");
  const hipDays = [...new Set((input.hipFlags ?? []).filter((x) => inWeek(x.date)).map((x) => x.date))];
  if (hipDays.length) add("down", "warnzeichen", `Hüfte/Leiste/Knie erwähnt (${hipDays.map(dm).join(", ")})`, "run");

  // Wochenrahmen: Zeit erfüllt? TSS-Grenze eingehalten?
  const frame = SPORTS.filter((k) => W.bySport[k]?.targetMin > 0 || W.bySport[k]?.targetLoad > 0).map((k) => {
    const b = W.bySport[k];
    return { sport: k, minutes: b.minutes, targetMin: b.targetMin, share: b.targetMin > 0 ? Math.round((b.minutes / b.targetMin) * 100) / 100 : null, load: b.load, targetLoad: b.targetLoad, over: b.targetLoad > 0 && b.load > b.targetLoad };
  });
  const tMin = frame.reduce((a, f) => a + (f.targetMin ?? 0), 0), dMin = frame.reduce((a, f) => a + (f.targetMin ? f.minutes : 0), 0);
  const frameShare = tMin > 0 ? dMin / tMin : null;
  if (frameShare != null && frameShare < R.frameMin) add("hold", "ausfuehrung", `Rahmen nur zu ${Math.round(frameShare * 100)} % erfüllt`);
  for (const f of frame.filter((x) => x.over)) add("warn", "ausfuehrung", `TSS-Grenze überschritten (${f.load} statt ≤ ${f.targetLoad})`, f.sport);
  const st = input.strength;
  if (st?.goal > 0 && st.count < st.goal) add("warn", "ausfuehrung", `Kraft ${st.count} von ${st.goal} ${st.unit ?? "Sätzen"}`);

  // Stufe: reduzieren und halten gewinnen; sonst zählen die Gruppen mit gelbem Signal (eine = zäh, zwei und mehr = halten).
  // Entlastungswoche geht vor.
  const levelOf = (xs) => {
    if (xs.some((x) => x.level === "down")) return "down";
    if (xs.some((x) => x.level === "hold")) return "hold";
    const groups = new Set(xs.filter((x) => x.level === "warn").map((x) => x.group)).size;
    return groups >= 2 ? "hold" : groups === 1 ? "small" : "up";
  };
  const curWeekNo = input.block?.status === "active" && input.block.weekNo != null ? input.block.weekNo : null;
  const deload = curWeekNo != null && curWeekNo % 4 === 0;
  const generalSignals = signals.filter((x) => !x.sport);
  const general = deload ? "deload" : levelOf(generalSignals);
  const sportLevel = (k) => (deload ? "deload" : levelOf([...generalSignals, ...signals.filter((x) => x.sport === k)]));
  // Blocktest in der laufenden Woche: höchstens +3 %
  const curFrom = addDays(to, 1), curTo = addDays(to, 7);
  const testWeek = (input.testDates ?? []).some((d) => d >= curFrom && d <= curTo);
  const groups = REVIEW_GROUPS.map(([key, label]) => {
    const xs = signals.filter((x) => x.group === key);
    return { key, label, cls: xs.some((x) => x.level === "down") ? "bad" : xs.length ? "warn" : "ok", texts: xs.map((x) => (x.sport === "run" ? `Laufen: ${x.text}` : x.text)) };
  });

  // Vorschlag: mit Rahmen Zeitziel je Sportart × Faktor (Laufen höchstens +10 %), sonst der Rahmen der laufenden Woche
  // (höchstens Faktor 1), ohne beides Wochen-TSS gesamt = 7 × CTL × Faktor
  const hasFrame = frame.some((f) => f.targetMin > 0);
  const cur = (input.weeks ?? []).find((w) => w.weekStart === addDays(to, 1));
  const curFrame = hasFrame ? [] : SPORTS.filter((k) => cur?.bySport?.[k]?.targetMin > 0).map((k) => ({ sport: k, targetMin: cur.bySport[k].targetMin, targetLoad: cur.bySport[k].targetLoad }));
  const range = (base, level, run = false, capOne = false) => {
    let [lo, hi] = R.factor[level];
    if (run) hi = Math.min(hi, 1 + R.runMaxUp);
    if (testWeek) hi = Math.min(hi, R.testWeekMax);
    if (capOne) hi = Math.min(hi, 1);
    lo = Math.min(lo, hi);
    return { from: r5(base * lo), to: r5(base * hi) };
  };
  const ctl = [...(input.wellness ?? [])].filter((w) => w.date <= to && w.ctl != null).sort((a, b) => a.date.localeCompare(b.date)).pop()?.ctl ?? null;
  const proposal = hasFrame
    ? frame.filter((f) => f.targetMin > 0).map((f) => { const level = sportLevel(f.sport); return { sport: f.sport, unit: "min", basis: "frame", level, base: f.targetMin, prevMin: f.minutes, ...range(f.targetMin, level, f.sport === "run") }; })
    : curFrame.length ? curFrame.map((f) => { const level = sportLevel(f.sport); const rg = range(f.targetMin, level, false, true); return { sport: f.sport, unit: "min", basis: "current", level, base: f.targetMin, prevMin: W.bySport[f.sport]?.minutes ?? 0, ...rg, maxLoad: f.targetLoad > 0 ? r5(f.targetLoad * Math.min(1, R.factor[level][1])) : null }; })
    : ctl > 0 ? [{ sport: "all", unit: "tss", basis: "ctl", level: general, ctl: Math.round(ctl), base: Math.round(ctl * 7), ...range(ctl * 7, general) }] : [];
  // Gegenprobe: TSS-Grenzen der laufenden Woche gegen die Erhaltungslast (7 × CTL), gerade wenn die Vorwoche keine Basis ist
  const capTss = SPORTS.reduce((a, k) => a + (cur?.bySport?.[k]?.targetLoad > 0 ? cur.bySport[k].targetLoad : 0), 0);
  const check = capTss > 0 && ctl > 0 ? { ctl: Math.round(ctl), keep: Math.round(ctl * 7), capTss, ratio: Math.round((capTss / (ctl * 7)) * 100) / 100 } : null;
  // Laufregel: kein Lauf mehr als 10 % länger (Distanz) als der längste der letzten 30 Tage
  const since30 = addDays(input.today ?? addDays(to, 1), -R.longRunDays);
  const longest30 = (input.runs ?? []).filter((r) => r.date >= since30 && r.date <= (input.today ?? to)).reduce((a, r) => Math.max(a, r.distanceKm ?? 0), 0) || null;
  const runLevel = sportLevel("run");
  return {
    weekStart: from, weekEnd: to,
    level: general, text: LEVEL_TEXT[general], hasFrame, runLevel, runText: runLevel !== general ? LEVEL_TEXT[runLevel] : null,
    deload, curWeekNo, testWeek, groups, check,
    reasons: deload ? [`Woche ${curWeekNo} ist Entlastungswoche`] : [...signals.map((x) => (x.sport === "run" ? `Laufen: ${x.text}` : x.text)), ...(testWeek ? ["Testwoche: höchstens +3 %"] : [])],
    ampel, ampelDays, feel,
    rpe: { coverage: coverage != null ? Math.round(coverage * 100) / 100 : null, srpe: withRpe.length ? Math.round(srpe) : null, ref: srpeRef != null ? Math.round(srpeRef) : null, monotony, strain, basis: useRpe ? "rpe" : "tss" },
    run: { minutes: W.runMin, ref: runRef != null ? Math.round(runRef) : null, ratio: runRatio != null ? Math.round(runRatio * 100) / 100 : null, longest: longRun, longestRef: longRef, capOver,
      longest30Km: longest30 != null ? r1(longest30) : null, maxNextKm: longest30 != null ? r1(longest30 * (1 + R.longRunUp)) : null },
    frame, frameShare: frameShare != null ? Math.round(frameShare * 100) / 100 : null,
    hipDays, sickDays, proposal,
  };
}
