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
assert.deepEqual(b.rules, []);
import { parseBlockRules } from "../src/season-block.js";
assert.deepEqual(parseBlockRules("Ziel\n\nRegeln\n- Laufen: HF nicht über 125\n• Keine Racepace-Intervalle\n\nLeitplanken\nKraft"), ["Laufen: HF nicht über 125", "Keine Racepace-Intervalle"]);
assert.deepEqual(parseBlockRules("Regeln:<br />\n\n1. Mehr Grundlage<br />\n2) Rad Z2\n\nWoche 12"), ["Mehr Grundlage", "Rad Z2"]);
assert.deepEqual(resolveSeasonBlock([{ ...ev, description: ev.description + "\n\nRegeln\n- Locker laufen" }], "2026-11-02").rules, ["Locker laufen"]);
assert.deepEqual(parseBlockRules("WOCHENRAHMEN\n- Zeit = Ziel\n\nFIXPUNKTE (meine Zusagen)\n- Lauf-Grundlage unter HF 125, Gehpausen erlaubt\n- Kraft ≥60 Sätze pro Woche\nBei jedem Blocktest entscheide ich neu.\n\nLEITPLANKEN\n- Kraft"), ["Lauf-Grundlage unter HF 125, Gehpausen erlaubt", "Kraft ≥60 Sätze pro Woche"]);
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
// Neue Form: Listenpunkte ohne Leerzeile, "Blocktest" bzw. "Vergleichseinheiten", Muss/Ziel im Absatz, FTP nur als Referenz
const blockA2 = "Block A – Grundlage (12 Wochen, ab 19.10.2026)\n\nABSCHLUSSZIELE\n1. Lauf: aerobe Stabilität Decoupling der ersten 60 min im Blocktest, Mittel der letzten 3 Tests: Muss ≤8 %, Ziel ≤5 %. Pace der ersten 60 min nicht langsamer als im Basistest. Blocktest = langer Lauf am Sonntag der 3. Belastungswoche (Basistest 25.10., dann 08.11., 06.12., 03.01.). HF-Band 120–125, gleiche flache Runde.\n2. Rad (Rolle): aerobe Stabilität Decoupling (Leistung zu Puls) ≤5 % im Mittel der letzten 3 Vergleichseinheiten: 75 min bei fester Leistung, mit Ventilator und gleicher Raumtemperatur. Referenz: FTP 252 W (Rampentest 12.10.). Feste Leistung einmal wählen (ca. 165–170 W) und im ganzen Block beibehalten.\n3. Schwimmen: Dauer mit sauberer Technik 2× 500 m mit maximal 1 min Pause. Die zweiten 500 m höchstens 5 s pro 100 m langsamer als die ersten.";
const ta2 = parseBlockTargets(blockA2);
assert.deepEqual(ta2.run, { max: 5, must: 8, lastN: 3, minMin: 60, pace: true, tests: ["10-25", "11-08", "12-06", "01-03"], stableMax: 5, band: [120, 125] });
assert.deepEqual(ta2.bike, { max: 5, must: null, lastN: 3, minMin: 75, firstMin: null, ftpTest: false });
// Radziel "der ersten 60 min … ab 60 min": Mindestdauer 60, ausgewertet werden nur die ersten 60 min
const ta3 = parseBlockTargets("2. Rad (Rolle): aerobe Stabilität Decoupling (Leistung zu Puls) der ersten 60 min ≤5 % im Mittel der letzten 3 Vergleichseinheiten. Vergleichseinheit = jede Grundlagen-Ausfahrt ab 60 min, deren erste 60 min bei fester Leistung laufen. Danach frei.");
assert.deepEqual(ta3.bike, { max: 5, must: null, lastN: 3, minMin: 60, firstMin: 60, ftpTest: false });
assert.deepEqual(ta2.swim, { reps: 2, distM: 500, maxPauseSec: 60, maxSlowerPer100: 5 });
assert.equal(ta2.decoupling, null);

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
// nur die ersten maxSec Sekunden: danach steigender Puls zaehlt nicht
assert.equal(decouplingFromStreams([{ type: "heartrate", data: Array.from({ length: n2 * 3 }, (_, i) => (i < n2 ? 130 : i < n2 * 2 ? 136.5 : 160)) }, { type: "watts", data: Array(n2 * 3).fill(150) }], "watts", n2 * 2), 4.8);
// Auf-/Abwaermzeit wie bei Intervals auslassen: kalter Start (Puls 100) und Ausrollen (Puls 90) zaehlen nicht
const warm = [{ type: "heartrate", data: Array.from({ length: 600 + n2 * 2 + 300 }, (_, i) => (i < 600 ? 100 : i < 600 + n2 ? 130 : i < 600 + n2 * 2 ? 136.5 : 90)) }, { type: "watts", data: Array(600 + n2 * 2 + 300).fill(150) }];
assert.equal(decouplingFromStreams(warm, "watts", null, { warmupSec: 600, cooldownSec: 300 }), 4.8);
assert.notEqual(decouplingFromStreams(warm, "watts"), 4.8);
console.log("block A ok");

// ---------- Leitplanke Intensität: Rad-HIIT und Hügelsprints je Belastungswoche ab Woche 2 ----------
import { intensityKindOf, checkBlockText } from "../src/season-block.js";
const lpText = "Block A\n\nLEITPLANKEN\n- Kraft ≥60 Sätze in 8 von 9 Belastungswochen\n- Ramp Rate (CTL) in Belastungswochen ≤5\n- Intensität: Rad-HIIT und Hügelsprints je 1× pro Woche in 6 von 8 Belastungswochen ab Woche 2 (Krankheitswochen zählen nicht als verfehlt)";
const tl = parseBlockTargets(lpText);
assert.deepEqual(tl.intensity, { need: 6, of: 8, fromWeek: 2, kinds: ["bikeHiit", "hillSprints"], sickOk: true });
assert.equal(tl.strength.need, 8); assert.equal(tl.strength.of, 9); assert.equal(tl.ramp.max, 5);
assert.equal(intensityKindOf("bike", "Rad-HIIT 30/30"), "bikeHiit");
assert.equal(intensityKindOf("bike", "Rolle 3x 4min"), "bikeHiit");
assert.equal(intensityKindOf("bike", "Zwift", 300), "bikeHiit");
assert.equal(intensityKindOf("bike", "FTP Test", 392), null);
assert.equal(intensityKindOf("bike", "Vergleichstest", 0), null);
assert.equal(intensityKindOf("run", "Lockerer Lauf + Hügelsprints 4x8s"), "hillSprints");
assert.equal(intensityKindOf("run", "Bobingen Laufen"), null);
const bL = resolveSeasonBlock([{ ...ev, description: lpText }, evB], "2027-01-08");
// Belastungswochen ab Woche 2: 2,3,5,6,7,9,10,11. Woche 3 nur HIIT (verfehlt), Woche 6 krank ohne Einheit (zählt nicht)
const wk = (n) => addDaysT("2026-10-19", (n - 1) * 7);
const addDaysT = (iso, n) => new Date(new Date(iso + "T00:00:00Z").getTime() + n * 86400000).toISOString().slice(0, 10);
const ints = [2, 3, 5, 7, 9, 10, 11].flatMap((n) => [{ date: wk(n), kind: "bikeHiit" }, ...(n === 3 ? [] : [{ date: addDaysT(wk(n), 2), kind: "hillSprints" }])]);
let gi = buildBlockGoalsFull(bL, { intensity: ints, sickDays: [addDaysT(wk(6), 1)] }, "2027-01-08").goals.find((x) => x.key === "intensity");
assert.equal(gi.group, "leitplanke"); assert.equal(gi.of, 7); assert.equal(gi.value, 6); assert.equal(gi.status, "ok");
assert.equal(gi.weeks.filter((x) => !x.load).length, 4); // Woche 1 und Entlastungswochen 4/8/12
// ohne Krankmeldung zählt Woche 6 als verfehlt: 2 von 2 erlaubten Fehlwochen
gi = buildBlockGoalsFull(bL, { intensity: ints }, "2027-01-08").goals.find((x) => x.key === "intensity");
assert.equal(gi.of, 8); assert.equal(gi.status, "warn");
// drei Fehlwochen -> gerissen, Entscheidung "Ursache klären" nur mit Abschlusszielen; hier Status bad
gi = buildBlockGoalsFull(bL, { intensity: ints.filter((x) => x.date !== wk(10)) }, "2027-01-08").goals.find((x) => x.key === "intensity");
assert.equal(gi.status, "bad");
console.log("block intensity ok");
// Woche 1 läuft: kein "offen", sondern der Grund, warum sie nicht zählt
assert.match(buildBlockGoalsFull(resolveSeasonBlock([{ ...ev, description: lpText }], "2026-10-20"), {}, "2026-10-20").goals.find((x) => x.key === "intensity").note, /^Diese Woche: Woche 1, zählt erst ab Woche 2$/);
console.log("block intensity week 1 ok");

// ---------- Block A wie im Kalender (Stand 20.10.2026): Blocktests, Rad-Messfenster, Woche-12-Entscheidung ----------
import { analyzeRunStreams, rideWindowFromStreams } from "../src/block-inputs.js";
import { buildTodayPlan, parseDayRules } from "../src/today-plan.js";
const real = "Block A – Grundlage (12 Wochen, ab 19.10.2026)\n\nABSCHLUSSZIELE\n\n1. Lauf: aerobe Stabilität\na) Basis: Decoupling der ersten 60 min im Blocktest, Mittel der letzten 3 Tests: Muss ≤8 %, Ziel ≤5 %. Pace der ersten 60 min nicht langsamer als im Basistest.\nb) Haltbarkeit: Stabile Dauer im letzten Blocktest länger als im Basistest.\nStabile Dauer = längstes Fenster ab Start (0–60, 0–75, 0–90 min … in 15-min-Schritten) mit Decoupling ≤5 %.\nBlocktest = langer Lauf am Sonntag der 3. Belastungswoche (Basistest 25.10., dann 08.11., 06.12., 03.01.). Volle geplante Länge, HF-Band 120–125, gleiche flache Runde in Bobingen.\nZusätzlich zählt jeder Grundlagenlauf ab 60 min unter HF 125 als Trend (nicht fürs Ziel).\n\n2. Rad (Rolle): aerobe Stabilität\nDecoupling (Leistung zu Puls) im 60-min-Messfenster ≤5 % im Mittel der letzten 3 Vergleichseinheiten.\nVergleichseinheit = 10 min Einrollen (ca. 5 min 110–130 W, 5 min 140–160 W), danach mindestens 60 min bei fester Leistung 165 W = Messfenster.\n\n3. Schwimmen: Dauer mit sauberer Technik\n2× 500 m mit maximal 1 min Pause. Die zweiten 500 m höchstens 5 s pro 100 m langsamer als die ersten.\n\nLAUFREGELN\n- Nie an zwei Tagen hintereinander laufen.\n\nFIXPUNKTE (meine Zusagen)\n- Mindestens 1 Ruhetag pro Woche, kein fester Wochentag.\n- Lauf-Grundlage unter HF 125, Gehpausen erlaubt\n- Intensität ab Woche 2:\n  Rad-HIIT 1× pro Woche (30/30 oder 3–4× 4 min)\n  Kein HIIT und keine Sprints in den 2 Tagen vor einem Blocktest.\n\nLEITPLANKEN\n- Kraft ≥60 Sätze in 8 von 9 Belastungswochen\n- Ramp Rate (CTL) in Belastungswochen ≤5\n- Intensität: Rad-HIIT und Hügelsprints je 1× pro Woche in 6 von 8 Belastungswochen ab Woche 2 (Krankheitswochen zählen nicht als verfehlt)\n\nWOCHE 12\nAlle Ziele und Leitplanken erfüllt: weiter mit Block B. Fehlt ein Ziel: betroffene Sportart um 3–4 Wochen verlängern. Leitplanken gerissen: erst Ursache klären, dann verlängern.";
const tr = parseBlockTargets(real);
assert.deepEqual(tr.decision, { week: 12, extend: "3–4" }); // nicht "ab Woche 2:" aus der Intensitätszeile
assert.equal(tr.bike.minMin, 60); assert.equal(tr.bike.firstMin, 60); assert.equal(tr.bike.warmupMin, 10); assert.equal(tr.bike.watts, 165);
assert.deepEqual(tr.run.tests, ["10-25", "11-08", "12-06", "01-03"]); assert.equal(tr.hrCap, 125);
assert.deepEqual(parseDayRules(real), { noRunBackToBack: true, restDay: true, preTestDays: 2 });
const evR = { category: "SEASON_START", name: "Block Grundlage (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: real };
// Woche 2: keine Entscheidung fällig, auch wenn noch nichts erfüllt ist
const w2 = buildBlockGoalsFull(resolveSeasonBlock([evR], "2026-10-27"), {}, "2026-10-27");
assert.equal(w2.decision.week, 12); assert.equal(w2.decision.due, false);
// Blocktests: nur Läufe am Termin (bis 2 Tage später) zählen fürs Ziel, andere Grundlagenläufe nur als Trend
const an = (dec60, stableMin, pace60, extra = {}) => ({ dec60, stableMin, pace60, windows: [{ min: 60, value: dec60 }], overCapMin: 0, ...extra });
const lr = (date, dur, a, extra = {}) => ({ date, durationMin: dur, easy: true, treadmill: false, heat: false, elevPerKm: 3, name: "Langer Lauf", analysis: a, ...extra });
const runsR = [lr("2026-10-25", 95, an(6, 0, 430)), lr("2026-10-29", 70, an(3, 70, 425)), lr("2026-11-09", 105, an(5, 75, 428)), lr("2026-11-08", 50, an(1, 60, 400))];
const bR = resolveSeasonBlock([evR], "2026-11-12");
const gR = Object.fromEntries(buildBlockGoalsFull(bR, { runs: runsR }, "2026-11-12").goals.map((x) => [x.key, x]));
assert.deepEqual(gR.runDecoupling.tests.map((t) => [t.date, t.label]), [["2026-10-25", "Basistest"], ["2026-11-09", "Test 1"]]); // 08.11. verschoben auf 09.11. (längster)
assert.equal(gR.runDecoupling.value, 5.5); assert.equal(gR.runDecoupling.pass, false); assert.match(gR.runDecoupling.note, /erst 2 von 3 Tests.*nächster Test 06\.12\./);
assert.equal(gR.runDecoupling.pace.ok, true); // 428 vs 430: nicht langsamer
assert.match(gR.runDecoupling.hints[0], /Trend.*1 Grundlagenlauf ab 60 min.*3 %/);
assert.equal(gR.runStable.value, 75); assert.equal(gR.runStable.baseValue, 0); assert.equal(gR.runStable.status, "ok");
// Kurs (Trend, keine Entscheidung): Decoupling 6 → Ø 5,5 bei Ziel 5 und Muss 8 = knapp; Hauptkurve Pace 0–60 im HF-Band
assert.deepEqual([gR.runDecoupling.course.state, gR.runDecoupling.course.from, gR.runDecoupling.course.to, gR.runDecoupling.course.dir], ["close", 6, 5.5, "better"]);
assert.equal(gR.runDecoupling.course.main, "Pace 0–60 7:10 → 7:08 /km"); assert.equal(gR.runDecoupling.course.mainDir, "better");
assert.equal(gR.runDecoupling.course.nextText, "nächster Blocktest 06.12.");
assert.deepEqual([gR.runStable.course.state, gR.runStable.course.from, gR.runStable.course.to], ["on", 0, 75]);
assert.equal(buildBlockGoalsFull(bR, { runs: runsR.slice(0, 1) }, "2026-11-12").goals.find((x) => x.key === "runDecoupling").course.state, "open"); // nur der Basistest
// Pace mehr als 5 s/km langsamer als im Basistest: verfehlt
const slow = Object.fromEntries(buildBlockGoalsFull(bR, { runs: runsR.map((r) => (r.date === "2026-11-09" ? { ...r, analysis: an(5, 75, 440) } : r)) }, "2026-11-12").goals.map((x) => [x.key, x]));
assert.equal(slow.runDecoupling.pace.ok, false); assert.equal(slow.runDecoupling.status, "warn");
// Rad: nur Fahrten mit 165 W im Messfenster sind Vergleichseinheiten; Geraetetemperatur kommt nicht vor
const rides = [{ date: "2026-10-26", durationMin: 72, decoupling: 4, mainWatts: 165, vi: 1.01, intensity: 66, indoor: true, easy: true, hasHr: true }, { date: "2026-10-28", durationMin: 30, decoupling: 1, mainWatts: 130, vi: 1.01, intensity: 50, indoor: true, easy: true, hasHr: true }, { date: "2026-10-30", durationMin: 75, decoupling: 9, mainWatts: 140, vi: 1.01, intensity: 56, indoor: true, easy: true, hasHr: true }];
const gb = buildBlockGoalsFull(bR, { rides }, "2026-11-12").goals.find((x) => x.key === "bikeDecoupling");
assert.equal(gb.count, 1); assert.equal(gb.value, 4); assert.equal(gb.course.state, "open"); assert.ok(gb.hints.some((h) => /Messfenster: 60 min nach 10 min Einrollen, Leistung 165 W/.test(h)));
assert.equal(/°C|Temperatur/.test(JSON.stringify(gb)), false);
// Zeitreihen Lauf: konstante Pace, Puls ab Minute 70 steigt -> 0–60 stabil, 0–75 ok, 0–90 drüber
{
  const n = 100 * 60, time = Array.from({ length: n }, (_, i) => i), velocity_smooth = Array(n).fill(2.5);
  const heartrate = time.map((t) => (t < 70 * 60 ? 122 : 122 + (t - 70 * 60) / 60 * 1.5));
  const a = analyzeRunStreams([{ type: "time", data: time }, { type: "heartrate", data: heartrate }, { type: "velocity_smooth", data: velocity_smooth }], { capHr: 125 });
  assert.equal(a.dec60, 0); assert.equal(a.pace60, 400); assert.equal(a.durationMin, 100);
  assert.deepEqual(a.windows.map((x) => x.min), [60, 75, 90]);
  assert.ok(a.windows[2].value > 5 && a.windows[1].value <= 5); assert.equal(a.stableMin, 75);
  assert.equal(a.overCapMin, 28); // ab Minute 72 über 125
  assert.deepEqual([a.ef, a.efToMin], [1.23, 35]); // EF Minute 5–35: 150 m/min ÷ 122
  const short = analyzeRunStreams([{ type: "time", data: time.slice(0, 20 * 60) }, { type: "heartrate", data: heartrate.slice(0, 20 * 60) }, { type: "velocity_smooth", data: velocity_smooth.slice(0, 20 * 60) }]);
  assert.equal(short.efToMin, 20); // kürzer als 35 min: Minute 5 bis Ende
  assert.equal(analyzeRunStreams([{ type: "time", data: time.slice(0, 14 * 60) }, { type: "heartrate", data: heartrate.slice(0, 14 * 60) }, { type: "velocity_smooth", data: velocity_smooth.slice(0, 14 * 60) }]).ef, null);
}
// Zeitreihen Rad: 10 min Einrollen bei 130 W, dann 60 min 165 W mit 5 % mehr Puls in der zweiten Hälfte
{
  const n = 70 * 60, time = Array.from({ length: n }, (_, i) => i);
  const watts = time.map((t) => (t < 600 ? 130 : 165)), heartrate = time.map((t) => (t < 600 ? 100 : t < 600 + 1800 ? 130 : 136.5));
  const st = [{ type: "time", data: time }, { type: "watts", data: watts }, { type: "heartrate", data: heartrate }];
  assert.deepEqual(rideWindowFromStreams(st, { warmupMin: 10, windowMin: 60, durationSec: n }), { decoupling: 4.8, mainWatts: 165, mainHr: 133, noWarmup: false }); // Puls im Fenster: Hälfte 130, Hälfte 136,5
  assert.equal(rideWindowFromStreams(st, { warmupMin: 10, windowMin: 60, durationSec: 3600 }).noWarmup, true); // 60-min-Fahrt ohne Einrollen
}
// Tagesentscheidung: Ampel plus Regeln aus dem Block
{
  const blk = { status: "active", weekNo: 1, rulesText: real };
  const daily = [{ date: "2026-10-19", load: 40, minutes: { bike: 60 } }, { date: "2026-10-20", load: 0, minutes: {} }];
  const base = { today: "2026-10-20", daily, block: blk, hrCap: 125, testDates: ["2026-10-25", "2026-11-08"], band: [120, 125], intensity: { fromWeek: 2, kinds: ["bikeHiit", "hillSprints"] }, intensitySessions: [] };
  const p1 = buildTodayPlan({ ...base, verdict: { cls: "ok", sub: "", reasons: [] } });
  assert.equal(p1.title, "Locker nach Wochenrahmen"); // Woche 1: noch keine Intensität
  assert.ok(p1.lines.some((l) => /Laufen möglich: locker unter HF 125/.test(l.text)));
  assert.ok(p1.lines.some((l) => /Keine Intensität in Woche 1/.test(l.text)));
  assert.ok(p1.lines.some((l) => /Blocktest So 25\.10\. \(in 5 Tagen\): HF-Band 120–125/.test(l.text)));
  // gestern gelaufen -> heute kein Lauf
  const p2 = buildTodayPlan({ ...base, daily: [{ date: "2026-10-19", load: 40, minutes: { run: 60 } }], verdict: { cls: "ok", sub: "", reasons: [] } });
  assert.ok(p2.lines.some((l) => l.cls === "bad" && /Kein Lauf heute/.test(l.text)));
  // Woche 2, zwei Tage vor dem Test: kein HIIT; drei Tage vorher und grün: Qualität möglich
  const w2b = { ...base, block: { ...blk, weekNo: 2 }, testDates: ["2026-11-08"] };
  assert.ok(buildTodayPlan({ ...w2b, today: "2026-11-06", verdict: { cls: "ok" } }).lines.some((l) => /Kein HIIT, keine Sprints: Blocktest am So 08\.11\./.test(l.text)));
  assert.equal(buildTodayPlan({ ...w2b, today: "2026-11-04", verdict: { cls: "ok" } }).title, "Qualität möglich");
  assert.equal(buildTodayPlan({ ...w2b, today: "2026-11-04", verdict: { cls: "warn" } }).title, "Heute locker und kürzer");
  // Testtag bei Rot: verschieben; Krankheit: kein Training
  assert.equal(buildTodayPlan({ ...base, today: "2026-10-25", verdict: { cls: "bad" } }).title, "Blocktest verschieben");
  assert.equal(buildTodayPlan({ ...base, verdict: { cls: "bad", sick: true } }).title, "Kein Training");
  // Ruhetag: Mo trainiert, Di frei -> erledigt
  const p3 = buildTodayPlan({ ...base, today: "2026-10-21", daily: [...daily, { date: "2026-10-21", load: 0, minutes: {} }], verdict: { cls: "ok" } });
  assert.ok(p3.lines.some((l) => l.cls === "ok" && /Ruhetag diese Woche: Di 20\.10\.$/.test(l.text)));
  // Umfang in Minuten aus dem Wochenrahmen: vor dem Training offen, nach einem Lauf heute erledigt (20.10.: Rad 60 Mo, Lauf 34 Di)
  const frame = { run: 90, bike: 105, swim: 30, strength: null };
  const pre = buildTodayPlan({ ...base, frame, verdict: { cls: "ok", dose: { text: "voller Umfang laut Wochenrahmen" } } });
  assert.equal(pre.lines[0].text, "Offen diese Woche: Lauf 90 min, Rad 45 min, Schwimmen 30 min (noch 5 Tage).");
  assert.equal(pre.lines.some((l) => /voller Umfang/.test(l.text)), false);
  const ranToday = [daily[0], { date: "2026-10-20", load: 26, minutes: { run: 34 } }];
  const post = buildTodayPlan({ ...base, frame, daily: ranToday, verdict: { cls: "ok", dose: { text: "voller Umfang laut Wochenrahmen" } } });
  assert.equal(post.title, "Heute erledigt");
  assert.equal(post.lines[0].text, "Heute erledigt: Lauf 34 min. Heute kein weiteres Training nötig.");
  assert.equal(post.lines[1].text, "Offen diese Woche: Lauf 56 min, Rad 45 min, Schwimmen 30 min (noch 5 Tage).");
  assert.equal(post.lines.some((l) => /schon gelaufen/.test(l.text)), false);
  const warn = buildTodayPlan({ ...base, frame, verdict: { cls: "warn", dose: { text: "x" } } });
  assert.match(warn.lines[0].text, /^Heute etwa 30 % kürzer, lange Einheit verschieben\. Offen diese Woche: Lauf 90 min/);
  assert.equal(buildTodayPlan({ ...base, verdict: { cls: "ok", dose: { text: "x" } } }).lines[0].text, "Kein Wochenrahmen eingetragen – keine Minutenvorgabe.");
  // Passt der Basistest (mind. 60 min) noch in den Laufrahmen? 56 min offen -> Zeitrahmen überziehen, Test nicht kürzen
  const fit = buildTodayPlan({ ...base, frame, frameLoad: { run: 90, bike: 75 }, testMin: 60, daily: ranToday, verdict: { cls: "ok" } });
  assert.ok(fit.lines.some((l) => l.cls === "warn" && /Blocktest So 25\.10\. braucht mindestens 60 min, im Laufrahmen sind noch 56 min offen.*TSS-Grenze 90 bleibt/.test(l.text)));
  assert.equal(buildTodayPlan({ ...base, frame, testMin: 60, verdict: { cls: "ok" } }).lines.some((l) => /braucht mindestens/.test(l.text)), false); // 90 min offen
  // Rad-Vergleichseinheit (~48 TSS) passt nicht mehr unter die Grenze 75, wenn schon 43 verbraucht sind
  const bikeDaily = [{ date: "2026-10-19", load: 43, sports: { bike: 43 }, minutes: { bike: 60 } }];
  const cmp = (extra) => buildTodayPlan({ ...base, frame, frameLoad: { run: 90, bike: 75 }, compareTss: 48, daily: bikeDaily, verdict: { cls: "ok" }, ...extra });
  assert.ok(cmp({}).lines.some((l) => /Rad-Vergleichseinheit \(~48 TSS\) passt nicht mehr unter die Rad-Grenze \(offen 32 TSS\)/.test(l.text)));
  assert.equal(cmp({ compareDone: true }).lines.some((l) => /Vergleichseinheit/.test(l.text)), false);
  // Testtag bei Gelb: kein "kürzer, lange Einheit verschieben", sondern Test in voller Länge und notieren
  const tday = buildTodayPlan({ ...base, today: "2026-10-25", frame, verdict: { cls: "warn", sub: "Qualität streichen, Umfang etwa 30 % kürzer, die lange Einheit verschieben." } });
  assert.equal(tday.lines.some((l) => /lange Einheit verschieben/.test(l.text)), false);
  assert.match(tday.sub, /voller Länge.*Gelb notieren/);
  assert.ok(tday.lines.some((l) => /Blocktest heute: volle geplante Länge.*Ampel gelb notieren/.test(l.text)));
}
// Block-Eintrag prüfen: was gelesen wurde und Widersprüche (Deckel, Testband, fehlende Testtermine)
{
  const chk = checkBlockText(real);
  assert.deepEqual([chk.hrCap, chk.band, chk.tests.length, chk.hints], [125, [120, 125], 4, []]);
  const bad = real.replace("Basistest 25.10., dann 08.11., 06.12., 03.01.", "Basistest in Woche 1").replace("HF-Band 120–125", "HF-Band 120–128").replace("Gehpausen erlaubt", "Gehpausen erlaubt, Hügelsprints bis HF 128");
  const c2 = checkBlockText(bad);
  assert.deepEqual(c2.band, [120, 128]); assert.equal(c2.tests.length, 0);
  assert.ok(c2.hints.some((h) => /Verschiedene HF-Grenzen im Text: 125, 128/.test(h)));
  assert.ok(c2.hints.some((h) => /Testband 120–128 reicht über den HF-Deckel 125/.test(h)));
  assert.ok(c2.hints.some((h) => /Keine Blocktest-Termine gefunden/.test(h)));
  assert.equal(parseBlockTargets(real + "\n- Rad: Vergleichseinheit inkl. Einrollen ca. 70 min / ~48 TSS.").compareTss, 48);
  // Deckel aus der Zeile "Lauf-Grundlage unter HF …", nicht aus dem früheren Trend-Satz "Grundlagenlauf … unter HF 125"
  const c128 = real.replace("Lauf-Grundlage unter HF 125", "Lauf-Grundlage unter HF 128");
  assert.equal(parseBlockTargets(c128).hrCap, 128);
  assert.ok(checkBlockText(c128).hints.some((h) => /Verschiedene HF-Grenzen im Text: 125, 128 – als Deckel gezählt wird 128/.test(h)));
  assert.equal(parseBlockTargets("Lauf: HF-Deckel 128\nGrundlagenlauf unter HF 125").hrCap, 128);
}
// Was die restlichen Tests fürs Blockziel brauchen: gezählt wird das Mittel der letzten 3 (08.11., 06.12., 03.01.)
{
  const only = buildBlockGoalsFull(bR, { runs: runsR.slice(0, 1) }, "2026-11-01").goals.find((x) => x.key === "runDecoupling");
  assert.ok(only.hints.some((h) => /Fürs Ziel zählen die Tests am 08\.11\., 06\.12\., 03\.01\. – der Basistest nicht/.test(h)));
  // Test 1 mit 5 %: die restlichen 2 brauchen im Mittel ≤ 5 % (Ziel) bzw. ≤ 9,5 % (Muss)
  const need = gR.runDecoupling.hints.find((h) => /restlichen/.test(h));
  assert.equal(need, "Die restlichen 2 Tests brauchen im Mittel ≤ 5 % fürs Ziel, ≤ 9,5 % fürs Muss.");
}
// Ramp Rate bei niedriger CTL: Hinweis, wie viel Last die Grenze erlaubt
{
  const ctlDays = Array.from({ length: 30 }, (_, i) => ({ date: new Date(Date.parse("2026-10-19") + i * 86400000).toISOString().slice(0, 10), ctl: 20 }));
  const ramp = buildBlockGoalsFull(bR, { ctlDays }, "2026-11-12").goals.find((x) => x.key === "ramp");
  assert.match(ramp.hints[0], /Bei CTL 20 erlaubt Ramp 5 etwa das 2,6-Fache der Erhaltungslast/);
}
console.log("block A calendar ok");
