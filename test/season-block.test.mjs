import assert from "node:assert/strict";
import { resolveSeasonBlock, parseBlockWeeks } from "../src/season-block.js";

const ev = { category: "SEASON_START", name: "Block Grundlage (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: "Ziel ist es Decoupling unter 8% zu druecken\n\nCheckpoint 3 / Blockabschluss nach Woche 12" };
assert.equal(parseBlockWeeks(ev.name), 12);
let b = resolveSeasonBlock([ev], "2026-10-05");
assert.equal(b.status, "upcoming"); assert.equal(b.daysToStart, 14); assert.equal(b.end, "2027-01-10");
assert.equal(b.name, "Block Grundlage"); assert.match(b.goal, /Decoupling/); assert.equal(b.notes.length, 1);
b = resolveSeasonBlock([ev], "2026-11-02");
assert.equal(b.status, "active"); assert.equal(b.weekNo, 3);
assert.equal(resolveSeasonBlock([{ category: "WORKOUT", name: "Block x" }], "2026-10-05"), null);
console.log("season-block ok");

import { buildBlockGoals, parseBlockTargets } from "../src/season-block.js";
const text = "Ziel ist es bei langen Läufen das Decoupling im Mittel unter 8% zu drücken\n\nCheckpoint 3: Decoupling ≤8% Kraft-Konsistenz ACWR-Korridor gehalten";
const t = parseBlockTargets(text);
assert.equal(t.decoupling.max, 8); assert.ok(t.strength && t.acwr);
const blk = resolveSeasonBlock([{ ...ev, description: text }], "2026-11-16"); // Woche 5
const input = {
  longRuns: [{ date: "2026-10-25", decoupling: 9.5 }, { date: "2026-11-08", decoupling: 6 }, { date: "2026-10-01", decoupling: 20 }],
  strength: [{ date: "2026-10-20" }, { date: "2026-10-20" }, { date: "2026-10-22" }, { date: "2026-11-03" }, { date: "2026-11-04" }, { date: "2026-11-10" }],
  strengthSource: { egym: true, egymAt: 1 },
  acwrDays: [{ date: "2026-10-20", acwr: 1.0 }, { date: "2026-10-21", acwr: 1.5 }, { date: "2026-10-22", acwr: 0.9 }, { date: "2026-10-23", acwr: 1.1 }],
};
const g = Object.fromEntries(buildBlockGoals(blk, input, "2026-11-16").map((x) => [x.key, x]));
assert.equal(g.decoupling.value, 7.8); assert.equal(g.decoupling.count, 2); assert.equal(g.decoupling.status, "ok"); // Lauf vor Blockstart zählt nicht
assert.equal(g.strength.of, 4); assert.equal(g.strength.value, 2); // ohne Satzzahlen: Woche 1 und Woche 3 mit je 2 Kraft-Tagen
const sg = Object.fromEntries(buildBlockGoals(blk, { ...input, strength: [{ date: "2026-10-20", sets: 40 }, { date: "2026-10-22", sets: 25 }, { date: "2026-10-28", sets: 30 }, { date: "2026-11-03", sets: 70 }], weeklyGoalSets: 60 }, "2026-11-16").map((x) => [x.key, x]));
// Kalenderwoche fuer die Uhr: Mo 16.11. zaehlt nur ab Montag, nicht die Blockwoche ab dem Starttag
assert.deepEqual(Object.fromEntries(buildBlockGoals(blk, { ...input, strength: [{ date: "2026-11-15", sets: 30 }, { date: "2026-11-16", sets: 12 }], weeklyGoalSets: 60 }, "2026-11-16").map((x) => [x.key, x])).strength.calWeek, { start: "2026-11-16", count: 12 });
assert.equal(sg.strength.goalSets, 60); assert.equal(sg.strength.value, 2); assert.equal(sg.strength.of, 4); assert.match(sg.strength.target, /60 Sätze.*75 %/);
assert.equal(g.strength.egym, true);
assert.equal(g.acwr.value, 75); assert.equal(g.acwr.status, "warn");
assert.equal(g.marathonShape, undefined);
const base = Object.fromEntries(buildBlockGoals(resolveSeasonBlock([{ ...ev, description: text }], "2026-10-05"), input, "2026-10-05").map((x) => [x.key, x]));
assert.equal(base.decoupling.status, "base"); assert.equal(base.decoupling.value, 20);
console.log("block goals ok");

import { buildWidgetBlock } from "../src/widget.js";
const wd = buildWidgetBlock({ generatedAt: "t", today: "2026-11-16", seasonBlock: { ...blk, goals: Object.values(g) }, sources: {} });
assert.equal(wd.block.status, "active"); assert.equal(wd.block.weekNo, 5); assert.equal(wd.block.goals.length, 3);
assert.equal(wd.block.goals.find((x) => x.key === "strength").weeks.length <= 6, true);
assert.equal(buildWidgetBlock({ generatedAt: "t", today: "2026-11-16", seasonBlock: null, sources: {} }).block, null);
console.log("block widget ok");

// ---------- Block A: Abschlussziele und Leitplanken ----------
import { buildBlockGoalsFull, swimTestFromText, swimTestFromIntervals } from "../src/season-block.js";
import { decouplingFromStreams } from "../src/block-inputs.js";
const blockA = "Block A – Abschlussziele\n\n1. Lauf: aerobe Stabilität\nDecoupling ≤5 % im Mittel der letzten 3 vergleichbaren Läufe (≥60 min, Z2, flach, kein Hitzetag). Die Pace ist dabei nicht langsamer als bei den ersten Vergleichsläufen des Blocks.\n\n2. Rad (Rolle): aerobe Stabilität\nDecoupling (Leistung zu Puls) ≤5 % im Mittel der letzten 3 vergleichbaren Einheiten: 75 min Z2 bei fester Leistung, mit Ventilator und gleicher Raumtemperatur. Die Z2-Leistung leitest du aus einem FTP-Test in Woche 1 ab.\n\n3. Schwimmen: Dauer mit sauberer Technik\n2× 500 m mit maximal 1 min Pause. Die zweiten 500 m sind höchstens 5 s pro 100 m langsamer als die ersten.\n\nLeitplanken\n\nKraft: ≥60 Sätze pro Woche in 8 von 9 Belastungswochen\nRamp Rate in Belastungswochen ≤5, keine ungeplante Pause über 7 Tage\n\nWoche 12: Sind alle Ziele und Leitplanken erfüllt, geht es mit Block B weiter. Fehlt ein Ziel, verlängerst du die betroffene Sportart um 3–4 Wochen. Wurden die Leitplanken gerissen, klärst du zuerst die Ursache, bevor du verlängerst.\n\nWenn du lieber bei 8 % bleiben willst, ist das als Muss-Kriterium in Ordnung. Dann nimm 5 % als Zielwert, den du anpeilst.";
const ta = parseBlockTargets(blockA);
assert.deepEqual(ta.run, { max: 5, must: 8, lastN: 3, minMin: 60, pace: true });
assert.equal(ta.bike.minMin, 75); assert.equal(ta.bike.must, 8); assert.equal(ta.bike.ftpTest, true);
assert.deepEqual(ta.swim, { reps: 2, distM: 500, maxPauseSec: 60, maxSlowerPer100: 5 });
assert.deepEqual(ta.strength, { need: 8, of: 9, sets: 60 });
assert.equal(ta.ramp.max, 5); assert.equal(ta.pause.maxDays, 7); assert.deepEqual(ta.decision, { week: 12, extend: "3–4" });
assert.equal(ta.decoupling, null); assert.equal(ta.acwr, null);

const evA = { ...ev, description: blockA };
const evB = { category: "SEASON_START", name: "Block B", start_date_local: "2027-01-11T00:00:00" };
const bA = resolveSeasonBlock([evA, evB], "2027-01-08"); // Woche 12
assert.equal(bA.weekNo, 12); assert.deepEqual(bA.next, { name: "Block B", start: "2027-01-11" });
const easyRun = (date, dec, pace, extra = {}) => ({ date, durationMin: 75, decoupling: dec, paceSecPerKm: pace, elevPerKm: 4, heat: false, easy: true, treadmill: false, ...extra });
const ride = (date, dec, extra = {}) => ({ date, durationMin: 75, decoupling: dec, watts: 145, vi: 1.01, intensity: 62, indoor: true, easy: true, hasHr: true, tempC: 22, ...extra });
// Kraft: 60 Sätze in allen Wochen ausser Woche 2 (Belastung) und den Entlastungswochen 4/8/12
const strengthA = [];
for (let i = 0; i < 12; i++) { const d = new Date(Date.parse("2026-10-20") + i * 7 * 86400000).toISOString().slice(0, 10); if (i !== 1 && (i + 1) % 4 !== 0) strengthA.push({ date: d, sets: 60 }); }
const allDays = Array.from({ length: 81 }, (_, i) => new Date(Date.parse("2026-10-19") + i * 86400000).toISOString().slice(0, 10)).filter((_, i) => i % 3 === 0);
const ctlDays = Array.from({ length: 82 }, (_, i) => ({ date: new Date(Date.parse("2026-10-18") + i * 86400000).toISOString().slice(0, 10), ctl: 22 + i * 0.4 })); // 2,8 je Woche
const inputA = {
  runs: [easyRun("2026-10-25", 9, 420), easyRun("2026-11-01", 8, 418), easyRun("2026-11-08", 7, 415), easyRun("2026-12-20", 5, 410), easyRun("2026-12-27", 4.5, 412), easyRun("2027-01-03", 5.5, 414),
    easyRun("2026-12-30", 1, 400, { heat: true }), easyRun("2026-12-31", 1, 400, { elevPerKm: 20 }), easyRun("2027-01-02", 1, 400, { durationMin: 45 })],
  rides: [ride("2026-12-01", 6), ride("2026-12-15", 7), ride("2027-01-05", 6.5), ride("2027-01-06", 0, { vi: 1.2 }), ride("2027-01-07", null, { hasHr: false })],
  swimTests: [{ date: "2026-11-20", t1Sec: 600, t2Sec: 640, pauseSec: 50 }, { date: "2027-01-02", t1Sec: 580, t2Sec: 600, pauseSec: 55 }],
  ftpTest: { date: "2026-10-21" }, ftp: 240,
  strength: strengthA, weeklyGoalSets: 60, ctlDays, activityDays: allDays, plannedBreaks: [],
};
const ra = buildBlockGoalsFull(bA, inputA, "2027-01-08");
const ga = Object.fromEntries(ra.goals.map((x) => [x.key, x]));
assert.deepEqual(ra.goals.map((x) => x.key), ["runDecoupling", "bikeDecoupling", "swimTest", "strength", "ramp", "pause"]);
assert.equal(ga.runDecoupling.count, 6); assert.equal(ga.runDecoupling.value, 5); assert.equal(ga.runDecoupling.status, "ok");
assert.deepEqual(ga.runDecoupling.pace, { first: 418, last: 412, ok: true }); assert.equal(ga.runDecoupling.pass, true);
assert.match(ga.runDecoupling.hints[0], /Hitze/); assert.match(ga.runDecoupling.hints[0], /nicht flach/); assert.match(ga.runDecoupling.hints[0], /unter 60 min/);
assert.equal(ga.bikeDecoupling.count, 3); assert.equal(ga.bikeDecoupling.value, 6.5); assert.equal(ga.bikeDecoupling.status, "warn"); assert.equal(ga.bikeDecoupling.pass, true); // Muss ≤8 erfüllt
assert.ok(ga.bikeDecoupling.hints.some((h) => /ohne Puls/.test(h))); assert.ok(ga.bikeDecoupling.hints.some((h) => /FTP-Test am 21\.10\./.test(h)));
assert.equal(ga.swimTest.value, 4); assert.equal(ga.swimTest.status, "ok"); assert.equal(ga.swimTest.tests[0].pass, false);
assert.equal(ga.strength.group, "leitplanke"); assert.equal(ga.strength.of, 9); assert.equal(ga.strength.value, 8); assert.equal(ga.strength.status, "warn"); // 1 von 1 erlaubter Fehlwoche
assert.equal(ga.strength.weeks.filter((x) => !x.load).length, 3);
assert.equal(ga.ramp.status, "ok"); assert.equal(ga.ramp.value, 2.8);
assert.equal(ga.pause.status, "ok"); assert.equal(ga.pause.value, 2);
assert.equal(ra.decision.due, true); assert.equal(ra.decision.state, "next"); assert.match(ra.decision.text, /Block B \(ab 11\.01\.\)/);
// Lauf langsamer -> Ziel verfehlt, Leitplanke Ramp gerissen -> erst Ursache klären
const rb = buildBlockGoalsFull(bA, { ...inputA, runs: inputA.runs.map((r) => (r.date > "2026-12-01" ? { ...r, paceSecPerKm: 430 } : r)), ctlDays: ctlDays.map((x, i) => ({ ...x, ctl: 22 + i })) }, "2027-01-08");
assert.equal(rb.goals[0].status, "warn"); assert.equal(rb.goals[0].pass, false);
assert.equal(rb.decision.state, "cause"); assert.match(rb.decision.text, /Ramp Rate.*Lauf verlängern/);
const rc = buildBlockGoalsFull(bA, { ...inputA, swimTests: [] }, "2027-01-08");
assert.equal(rc.decision.state, "extend"); assert.equal(rc.decision.text, "Schwimmen um 3–4 Wochen verlängern.");
// Pause: 10 Tage ohne Training -> gerissen, ausser als geplante Pause eingetragen
const gap = allDays.filter((d) => d < "2026-11-10" || d > "2026-11-20");
assert.equal(buildBlockGoalsFull(bA, { ...inputA, activityDays: gap }, "2027-01-08").goals.find((x) => x.key === "pause").status, "bad");
assert.equal(buildBlockGoalsFull(bA, { ...inputA, activityDays: gap, plannedBreaks: [{ from: "2026-11-11", to: "2026-11-19" }] }, "2027-01-08").goals.find((x) => x.key === "pause").status, "ok");
// vor dem Block: Ausgangswerte, keine Entscheidung
const up = buildBlockGoalsFull(resolveSeasonBlock([evA, evB], "2026-10-12"), { ...inputA, runs: [easyRun("2026-09-20", 7, 420)] }, "2026-10-12");
assert.equal(up.goals[0].status, "base"); assert.equal(up.decision, null);
// Schwimmtest aus Text und aus Intervallen
assert.deepEqual(swimTestFromText("2x500: 9:40 / 9:52, Pause 45 s", 500), { t1Sec: 580, t2Sec: 592, pauseSec: 45, source: "text" });
assert.deepEqual(swimTestFromText("Test 2× 500 m 10:05 10:20 Pause 1:00", 500), { t1Sec: 605, t2Sec: 620, pauseSec: 60, source: "text" });
assert.equal(swimTestFromText("Schwimmbad 9:40", 500), null);
assert.deepEqual(swimTestFromIntervals({ icu_intervals: [{ type: "WORK", distance: 200, moving_time: 240, start_time: 0, end_time: 240 }, { type: "WORK", distance: 500, moving_time: 590, start_time: 300, end_time: 890 }, { type: "WORK", distance: 500, moving_time: 605, start_time: 940, end_time: 1545 }] }, 500), { t1Sec: 590, t2Sec: 605, pauseSec: 50, source: "intervalle" });
// Decoupling aus Zeitreihen: zweite Hälfte gleiche Leistung bei 5 % mehr Puls
const n2 = 1800;
assert.equal(decouplingFromStreams([{ type: "heartrate", data: Array.from({ length: n2 * 2 }, (_, i) => (i < n2 ? 130 : 136.5)) }, { type: "watts", data: Array(n2 * 2).fill(150) }], "watts"), 4.8);
assert.equal(decouplingFromStreams([{ type: "heartrate", data: [130] }, { type: "watts", data: [150] }], "watts"), null);
console.log("block A ok");
