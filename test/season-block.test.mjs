import assert from "node:assert/strict";
import { resolveSeasonBlock, parseBlockWeeks, parseStrengthSets, parseStrengthTarget, activeBlockStrengthSets, activeBlockStrengthTarget } from "../src/season-block.js";

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
assert.equal(wd.block.status, "active"); assert.equal(wd.block.weekNo, 5); assert.deepEqual(wd.block.goals.map((x) => x.key), ["decoupling", "strength"]); // ACWR nur noch als Hinweis im Rahmen-Widget
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
assert.equal(ta.ramp.max, 5); assert.equal(ta.pause.maxDays, 7); assert.equal("decision" in ta, false);
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
// Kraft: 60 Sätze in allen Wochen ausser Woche 2 (Belastung) und den Entlastungswochen 4/8/12 (vom Coach markiert, deloadWeeks)
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
  deloadWeeks: ["2026-11-09", "2026-12-07", "2027-01-04"],
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
assert.equal("decision" in ra, false);
// Lauf langsamer -> Ziel verfehlt (pass false), Leitplanke Ramp gerissen
const rb = buildBlockGoalsFull(bA, { ...inputA, runs: inputA.runs.map((r) => (r.date > "2026-12-01" ? { ...r, paceSecPerKm: 430 } : r)), ctlDays: ctlDays.map((x, i) => ({ ...x, ctl: 22 + i })) }, "2027-01-08");
assert.equal(rb.goals[0].status, "warn"); assert.equal(rb.goals[0].pass, false);
assert.equal(rb.goals.find((x) => x.key === "ramp").status, "bad");
// Pause: 10 Tage ohne Training -> gerissen, ausser als geplante Pause eingetragen
const gap = allDays.filter((d) => d < "2026-11-10" || d > "2026-11-20");
assert.equal(buildBlockGoalsFull(bA, { ...inputA, activityDays: gap }, "2027-01-08").goals.find((x) => x.key === "pause").status, "bad");
assert.equal(buildBlockGoalsFull(bA, { ...inputA, activityDays: gap, plannedBreaks: [{ from: "2026-11-11", to: "2026-11-19" }] }, "2027-01-08").goals.find((x) => x.key === "pause").status, "ok");
// vor dem Block: Ausgangswerte
const up = buildBlockGoalsFull(resolveSeasonBlock([evA, evB], "2026-10-12"), { ...inputA, runs: [easyRun("2026-09-20", 7, 420)] }, "2026-10-12");
assert.equal(up.goals[0].status, "base");
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
const deloadL = [wk(4), wk(8), wk(12)]; // vom Coach markierte Entlastungswochen
const ints = [2, 3, 5, 7, 9, 10, 11].flatMap((n) => [{ date: wk(n), kind: "bikeHiit" }, ...(n === 3 ? [] : [{ date: addDaysT(wk(n), 2), kind: "hillSprints" }])]);
// Keine Pflichtquote mehr: je Woche nur der Stand, kein "unter Soll" und kein Rot. Ohne Wochenumfang (Referenz) zählt die Intensität.
let gi = buildBlockGoalsFull(bL, { intensity: ints, sickDays: [addDaysT(wk(6), 1)], deloadWeeks: deloadL }, "2027-01-08").goals.find((x) => x.key === "intensity");
assert.equal(gi.group, "leitplanke"); assert.equal(gi.status, "info"); assert.equal(gi.source, "blocktext");
// Woche 1 vor der Startwoche, 3 nur HIIT (zählt: eine Einheit reicht), 4/8/12 Entlastung, 6 krank ohne Einheit
assert.deepEqual(gi.weeks.map((x) => x.state), ["early", "done", "done", "deload", "done", "sick", "done", "deload", "done", "done", "done", "open"]);
assert.deepEqual([gi.value, gi.of, gi.withoutWeeks, gi.volumeWeeks], [7, 11, 0, 0]);
assert.equal(gi.target, "Rad-HIIT: 1× pro Woche · Hügelsprints: 1× pro Woche");
// Ohne Krankmeldung: Woche 6 "ohne Intensität" (grau), trotzdem kein Rot
gi = buildBlockGoalsFull(bL, { intensity: ints, deloadWeeks: deloadL }, "2027-01-08").goals.find((x) => x.key === "intensity");
assert.equal(gi.weeks[5].state, "none"); assert.equal(gi.status, "info"); assert.equal(gi.withoutWeeks, 1);
console.log("block intensity ok");
// Woche 1 läuft: kein "offen", sondern der Grund, warum sie nicht zählt
assert.match(buildBlockGoalsFull(resolveSeasonBlock([{ ...ev, description: lpText }], "2026-10-20"), {}, "2026-10-20").goals.find((x) => x.key === "intensity").note, /^Diese Woche: Woche 1, zählt erst ab Woche 2$/);
console.log("block intensity week 1 ok");

// Intensität aus dem Rahmen des Coaches (save_targets): je Woche der Rahmen, der in ihr galt; Umfang gegen die Referenzwoche
{
  const { intensityFromFrame } = await import("../src/season-block.js");
  const { DEFAULT_TARGET } = await import("../src/targets-snapshot.js");
  // Startwert: Rad-HIIT ab Woche 2, Hügelsprints; Leitplanke "Krankheitswochen zählen nicht" -> sickOk; keine Quote mehr
  assert.deepEqual(intensityFromFrame(DEFAULT_TARGET, tl.intensity), { fromWeek: 2, kinds: ["bikeHiit", "hillSprints"], sickOk: true, source: "rahmen" });
  assert.equal(intensityFromFrame(null, tl.intensity), tl.intensity); // ohne Rahmen: Blocktext
  const onlyBike = { intensity: [{ sport: "rad", text: "1× HIIT pro Woche ab Woche 3, z. B. 2× 8× 30/30" }, { sport: "lauf", text: "nur locker" }], guardrails: [] };
  assert.deepEqual(intensityFromFrame(onlyBike, tl.intensity), { fromWeek: 3, kinds: ["bikeHiit"], sickOk: false, source: "rahmen" });
  assert.equal(intensityFromFrame({ intensity: [{ sport: "lauf", text: "nur locker" }], guardrails: [] }, tl.intensity), null); // keine Art -> keine Kachel
  // Rahmen ab 26.10. (Woche 2): "Erst Umfang, dann Intensität", Rad frühestens ab Woche 3
  const fr2 = { ...DEFAULT_TARGET, default: undefined, validFrom: "2026-10-26",
    intensity: [{ sport: "lauf", text: "Erst wenn der Umfang steht: 1× Hügelsprints am Ende eines lockeren Laufs" }, { sport: "rad", text: "Erst wenn der Umfang steht: 1× HIIT pro Woche, frühestens ab Woche 3, z. B. 2× 8× 30/30" }],
    guardrails: ["Erst Umfang, dann Intensität: nie in derselben Woche Umfang steigern und Intensität einführen oder erhöhen", "Kein HIIT/Sprints 2 Tage vor Blocktest", "Krankheitswochen zählen nicht als verfehlt"] };
  const frames = [fr2, DEFAULT_TARGET]; // neueste zuerst wie targetsView().history
  // Umfang je Woche: Woche 3 +10 % (Umfangswoche, auch mit HIIT), Woche 5 +3 % (gehalten), Woche 7 −20 %, Rest gleich
  const vols = Array.from({ length: 12 }, (_, i) => ({ week: wk(i + 1), enduranceMin: i === 2 ? 330 : i === 4 ? 309 : i === 6 ? 240 : 300, referenceMin: 300 }));
  const dB = (today) => resolveSeasonBlock([{ ...ev, description: "Block A\n\nLEITPLANKEN\n- Ramp Rate ≤5" }, evB], today);
  gi = buildBlockGoalsFull(dB("2026-12-23"), { intensity: ints, deloadWeeks: deloadL, frame: fr2, frames, weekVolumes: vols, plannedTests: ["2026-11-08"], sickDays: [addDaysT(wk(6), 1)] }, "2026-12-23").goals.find((x) => x.key === "intensity");
  // Woche 2: Rahmen ab 26.10. (Rad frühestens ab Woche 3), Hügelsprints gemacht -> erledigt; Woche 3: Umfangswoche (trotz HIIT);
  // Woche 4 (Test 08.11.) ist Entlastung
  assert.deepEqual(gi.weeks.map((x) => x.state), ["early", "done", "volume", "deload", "done", "sick", "done", "deload", "done", "open"]);
  assert.deepEqual(gi.weeks.map((x) => x.frameFrom).slice(0, 2), ["2026-10-19", "2026-10-26"]);
  assert.deepEqual([gi.weeks[2].volumePct, gi.weeks[4].volumePct, gi.weeks[6].volumePct], [10, 3, -20]);
  assert.deepEqual([gi.status, gi.value, gi.of, gi.volumeWeeks, gi.source], ["info", 4, 9, 1, "rahmen"]);
  // Zieltext komplett aus dem Rahmen: Intensität je Sportart plus die Leitplanke, kein "ab Woche 2" aus dem Code
  assert.deepEqual(gi.targetLines.map((l) => l.label), ["Lauf", "Rad", "Leitplanke"]);
  assert.match(gi.target, /^Lauf: Erst wenn der Umfang steht: 1× Hügelsprints.* · Rad: Erst wenn .* · Leitplanke: Erst Umfang, dann Intensität: nie in derselben Woche/);
  assert.equal(/Belastungswochen|unter Soll/.test(JSON.stringify(gi)), false);
  // Testwoche ohne Intensität: zählt nicht (hell); ohne Krankmeldung, Woche 6 mit Test
  gi = buildBlockGoalsFull(dB("2026-12-23"), { intensity: ints, deloadWeeks: deloadL, frame: fr2, frames, weekVolumes: vols, plannedTests: [addDaysT(wk(6), 6)] }, "2026-12-23").goals.find((x) => x.key === "intensity");
  assert.equal(gi.weeks[5].state, "test"); assert.equal(gi.weeks[5].test, true);
  // Blocktext ohne Intensitäts-Zeile und ohne Rahmen: keine Kachel
  const bN = resolveSeasonBlock([{ ...ev, description: "Block A\n\nLEITPLANKEN\n- Ramp Rate ≤5" }, evB], "2027-01-08");
  assert.equal(buildBlockGoalsFull(bN, { intensity: ints }, "2027-01-08").goals.find((x) => x.key === "intensity"), undefined);
  gi = buildBlockGoalsFull(bN, { intensity: ints, deloadWeeks: deloadL, frame: DEFAULT_TARGET }, "2027-01-08").goals.find((x) => x.key === "intensity");
  assert.deepEqual([gi.status, gi.value, gi.of, gi.withoutWeeks, gi.note], ["info", 7, 11, 1, "Diese Woche: Entlastungswoche, zählt nicht"]);
  assert.match(gi.target, /^Lauf: 1× Hügelsprints .* · Rad: 1× HIIT pro Woche ab Woche 2/); // Startwert-Rahmen, ohne Leitplanke zur Intensität
  console.log("block intensity from frame ok");

  // Widgets: Rahmen (Parameter "rahmen") mit dem Stand der laufenden Blockwoche; Block-Widget ohne Ramp Rate/ACWR
  const { buildWidgetRahmen, buildWidgetBlock: wBlock } = await import("../src/widget.js");
  const { targetsView } = await import("../src/targets-snapshot.js");
  const bW = resolveSeasonBlock([{ ...ev, description: lpText }, evB], "2026-11-25"); // Woche 6, Mittwoch
  const dW = { generatedAt: "t", today: "2026-11-25", sources: {}, targets: targetsView(null, "2026-11-25"), nextTest: { date: "2026-11-28", daysToGo: 3 }, loadHint: null,
    seasonBlock: { ...bW, goals: buildBlockGoalsFull(bW, { intensity: [...ints, { date: "2026-11-24", kind: "bikeHiit" }], deloadWeeks: deloadL, frame: DEFAULT_TARGET, ctlDays: [] }, "2026-11-25").goals } };
  assert.ok(dW.seasonBlock.goals.some((x) => x.key === "ramp")); // im Dashboard-JSON noch da …
  assert.equal(wBlock(dW).block.goals.some((x) => x.key === "ramp" || x.key === "acwr"), false); // … im Widget nicht
  const wr = buildWidgetRahmen(dW);
  assert.deepEqual(wr.frame.intensity.map((x) => [x.sport, x.week]), [["lauf", { count: 0, skip: null }], ["rad", { count: 1, skip: null }]]);
  assert.deepEqual([wr.frame.notAllowed[0], wr.frame.guardrails.length, wr.frame.maxIncreasePct, wr.nextTest.daysToGo, wr.loadHint], ["Sweet Spot", 3, 10, 3, null]);
  // Entlastungswoche: zählt nicht; ohne Block kein Wochenstand, Rahmen trotzdem da
  const dD = { ...dW, today: "2026-12-09", seasonBlock: { ...bW, goals: buildBlockGoalsFull(bW, { intensity: ints, deloadWeeks: deloadL, frame: DEFAULT_TARGET }, "2026-12-09").goals } }; // Woche 8 = Entlastung
  assert.deepEqual(buildWidgetRahmen(dD).frame.intensity.map((x) => x.week.skip), ["Entlastung", "Entlastung"]);
  assert.deepEqual(buildWidgetRahmen({ ...dW, seasonBlock: null }).frame.intensity.map((x) => x.week), [null, null]);
  assert.equal(wr.next, null);
  // geplanter Rahmen (validFrom in der Zukunft): ab wann und warum, ohne Vorspann
  const up = { ...dW, targets: { ...dW.targets, upcoming: [{ validFrom: "2026-11-30", reason: "Gerberts Entscheidung im Chat am 23.10.2026: erst Umfang, dann Intensität." }] } };
  assert.deepEqual(buildWidgetRahmen(up).next, { validFrom: "2026-11-30", reason: "erst Umfang, dann Intensität." });
  console.log("widget rahmen ok");
}

// ---------- Block A wie im Kalender (Stand 20.10.2026): Blocktests, Rad-Messfenster, Woche-12-Entscheidung ----------
import { analyzeRunStreams, rideWindowFromStreams } from "../src/block-inputs.js";
const real = "Block A – Grundlage (12 Wochen, ab 19.10.2026)\n\nABSCHLUSSZIELE\n\n1. Lauf: aerobe Stabilität\na) Basis: Decoupling der ersten 60 min im Blocktest, Mittel der letzten 3 Tests: Muss ≤8 %, Ziel ≤5 %. Pace der ersten 60 min nicht langsamer als im Basistest.\nb) Haltbarkeit: Stabile Dauer im letzten Blocktest länger als im Basistest.\nStabile Dauer = längstes Fenster ab Start (0–60, 0–75, 0–90 min … in 15-min-Schritten) mit Decoupling ≤5 %.\nBlocktest = langer Lauf am Sonntag der 3. Belastungswoche (Basistest 25.10., dann 08.11., 06.12., 03.01.). Volle geplante Länge, HF-Band 120–125, gleiche flache Runde in Bobingen.\nZusätzlich zählt jeder Grundlagenlauf ab 60 min unter HF 125 als Trend (nicht fürs Ziel).\n\n2. Rad (Rolle): aerobe Stabilität\nDecoupling (Leistung zu Puls) im 60-min-Messfenster ≤5 % im Mittel der letzten 3 Vergleichseinheiten.\nVergleichseinheit = 10 min Einrollen (ca. 5 min 110–130 W, 5 min 140–160 W), danach mindestens 60 min bei fester Leistung 165 W = Messfenster.\n\n3. Schwimmen: Dauer mit sauberer Technik\n2× 500 m mit maximal 1 min Pause. Die zweiten 500 m höchstens 5 s pro 100 m langsamer als die ersten.\n\nLAUFREGELN\n- Nie an zwei Tagen hintereinander laufen.\n\nFIXPUNKTE (meine Zusagen)\n- Mindestens 1 Ruhetag pro Woche, kein fester Wochentag.\n- Lauf-Grundlage unter HF 125, Gehpausen erlaubt\n- Intensität ab Woche 2:\n  Rad-HIIT 1× pro Woche (30/30 oder 3–4× 4 min)\n  Kein HIIT und keine Sprints in den 2 Tagen vor einem Blocktest.\n\nLEITPLANKEN\n- Kraft ≥60 Sätze in 8 von 9 Belastungswochen\n- Ramp Rate (CTL) in Belastungswochen ≤5\n- Intensität: Rad-HIIT und Hügelsprints je 1× pro Woche in 6 von 8 Belastungswochen ab Woche 2 (Krankheitswochen zählen nicht als verfehlt)\n\nWOCHE 12\nAlle Ziele und Leitplanken erfüllt: weiter mit Block B. Fehlt ein Ziel: betroffene Sportart um 3–4 Wochen verlängern. Leitplanken gerissen: erst Ursache klären, dann verlängern.";
const tr = parseBlockTargets(real);
assert.equal(tr.bike.minMin, 60); assert.equal(tr.bike.firstMin, 60); assert.equal(tr.bike.warmupMin, 10); assert.equal(tr.bike.watts, 165);
assert.deepEqual(tr.run.tests, ["10-25", "11-08", "12-06", "01-03"]); assert.equal(tr.hrCap, 125);
const evR = { category: "SEASON_START", name: "Block Grundlage (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: real };
// Blocktests legt der Nutzer fest: nur als "Blocktest"/"Basistest" benannte Läufe (r.test aus dem gekoppelten Workout oder
// der Name) zählen fürs Ziel – auch ein langer Lauf am Termin nicht automatisch; andere Grundlagenläufe nur als Trend
const an = (dec60, stableMin, pace60, extra = {}) => ({ dec60, stableMin, pace60, windows: [{ min: 60, value: dec60 }], overCapMin: 0, ...extra });
const lr = (date, dur, a, extra = {}) => ({ date, durationMin: dur, easy: true, treadmill: false, heat: false, elevPerKm: 3, name: "Langer Lauf", analysis: a, ...extra });
const runsR = [lr("2026-10-25", 95, an(6, 0, 430), { name: "Basistest" }), lr("2026-10-29", 70, an(3, 70, 425)), lr("2026-11-09", 105, an(5, 75, 428), { test: true }), lr("2026-11-08", 50, an(1, 60, 400)), lr("2026-11-01", 100, an(9, 0, 420))];
const bR = resolveSeasonBlock([evR], "2026-11-12");
const gR = Object.fromEntries(buildBlockGoalsFull(bR, { runs: runsR }, "2026-11-12").goals.map((x) => [x.key, x]));
assert.deepEqual(gR.runDecoupling.tests.map((t) => [t.date, t.label]), [["2026-10-25", "Basistest"], ["2026-11-09", "Test 1"]]); // der Lauf am Termin 08.11. ist nicht benannt
assert.equal(gR.runDecoupling.value, 5.5); assert.equal(gR.runDecoupling.pass, false); assert.match(gR.runDecoupling.note, /erst 2 von 3 Tests.*nächster Test 06\.12\./);
assert.equal(gR.runDecoupling.pace.ok, true); // 428 vs 430: nicht langsamer
assert.match(gR.runDecoupling.hints[0], /Trend.*2 Grundlagenläufe ab 60 min.*6 %/); // 29.10. (3 %) und der unbenannte lange Lauf 01.11. (9 %)
assert.equal(gR.runStable.value, 75); assert.equal(gR.runStable.baseValue, 0); assert.equal(gR.runStable.status, "ok");
// Verlauf (Trend, keine Stufe): Decoupling 6 → Ø 5,5; Hauptkurve Pace 0–60 im HF-Band
assert.deepEqual([gR.runDecoupling.course.open, gR.runDecoupling.course.from, gR.runDecoupling.course.to, gR.runDecoupling.course.dir], [false, 6, 5.5, "better"]);
assert.equal("state" in gR.runDecoupling.course, false);
assert.equal(gR.runDecoupling.course.main, "Pace 0–60 7:10 → 7:08 /km"); assert.equal(gR.runDecoupling.course.mainDir, "better");
assert.equal(gR.runDecoupling.course.nextText, "nächster Blocktest 06.12.");
assert.deepEqual([gR.runStable.course.from, gR.runStable.course.to, gR.runStable.course.dir], [0, 75, "better"]);
assert.equal(buildBlockGoalsFull(bR, { runs: runsR.slice(0, 1) }, "2026-11-12").goals.find((x) => x.key === "runDecoupling").course.open, true); // nur der Basistest
// Pace mehr als 5 s/km langsamer als im Basistest: verfehlt
const slow = Object.fromEntries(buildBlockGoalsFull(bR, { runs: runsR.map((r) => (r.date === "2026-11-09" ? { ...r, analysis: an(5, 75, 440) } : r)) }, "2026-11-12").goals.map((x) => [x.key, x]));
assert.equal(slow.runDecoupling.pace.ok, false); assert.equal(slow.runDecoupling.status, "warn");
// Rad: nur Fahrten mit 165 W im Messfenster sind Vergleichseinheiten; Geraetetemperatur kommt nicht vor
const rides = [{ date: "2026-10-26", durationMin: 72, decoupling: 4, mainWatts: 165, vi: 1.01, intensity: 66, indoor: true, easy: true, hasHr: true }, { date: "2026-10-28", durationMin: 30, decoupling: 1, mainWatts: 130, vi: 1.01, intensity: 50, indoor: true, easy: true, hasHr: true }, { date: "2026-10-30", durationMin: 75, decoupling: 9, mainWatts: 140, vi: 1.01, intensity: 56, indoor: true, easy: true, hasHr: true }];
const gb = buildBlockGoalsFull(bR, { rides }, "2026-11-12").goals.find((x) => x.key === "bikeDecoupling");
assert.equal(gb.count, 1); assert.equal(gb.value, 4); assert.equal(gb.course.open, true); assert.ok(gb.hints.some((h) => /Messfenster: 60 min nach 10 min Einrollen, Leistung 165 W/.test(h)));
assert.equal(/°C|Temperatur/.test(JSON.stringify(gb)), false);
// Fahrt ohne Einrollen (Protokoll verlangt 10 min): zählt nicht fürs Ziel, nur als Orientierung
const gnw = buildBlockGoalsFull(bR, { rides: [{ ...rides[0], date: "2026-10-19", decoupling: 9.8, noWarmup: true }, rides[0]] }, "2026-11-12").goals.find((x) => x.key === "bikeDecoupling");
assert.equal(gnw.count, 1); assert.equal(gnw.value, 4); assert.deepEqual(gnw.orientation, { date: "2026-10-19", value: 9.8 });
assert.ok(gnw.hints.some((h) => /1× ohne Einrollen, zählt nicht fürs Ziel \(zuletzt 19\.10\.: 9,8 %\)/.test(h)));
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
  // Fensterbeginn aus dem Warmup-Block in Intervals (icu_warmup_time): wie 10 min Einrollen; startSec 0 = ohne Einrollen
  assert.deepEqual(rideWindowFromStreams(st, { startSec: 600, windowMin: 60, durationSec: n }), { decoupling: 4.8, mainWatts: 165, mainHr: 133, noWarmup: false });
  assert.equal(rideWindowFromStreams(st, { startSec: 0, windowMin: 60, durationSec: n }).noWarmup, true);
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
  assert.equal(c2.testsByName, true); // ohne Termine: Tests werden über den Namen erkannt, keine Warnung
  assert.equal(c2.hints.length, 2);
  // Deckel aus der Zeile "Lauf-Grundlage unter HF …", nicht aus dem früheren Trend-Satz "Grundlagenlauf … unter HF 125"
  const c128 = real.replace("Lauf-Grundlage unter HF 125", "Lauf-Grundlage unter HF 128");
  assert.equal(parseBlockTargets(c128).hrCap, 128);
  assert.ok(checkBlockText(c128).hints.some((h) => /Verschiedene HF-Grenzen im Text: 125, 128 – als Deckel gezählt wird 128/.test(h)));
  assert.equal(parseBlockTargets("Lauf: HF-Deckel 128\nGrundlagenlauf unter HF 125").hrCap, 128);
}
// Was der nächste Test fürs Blockziel braucht: gezählt wird das Mittel der letzten 3 Tests
{
  const only = buildBlockGoalsFull(bR, { runs: runsR.slice(0, 1) }, "2026-11-01").goals.find((x) => x.key === "runDecoupling");
  assert.ok(only.hints.includes("Fürs Ziel zählt das Mittel der letzten 3 Tests – bisher 1."));
  // Basistest 6 %, Test 1 5 %: der nächste braucht ≤ 4 % (Ziel 5) bzw. ≤ 13 % (Muss 8)
  assert.ok(gR.runDecoupling.hints.includes("Der nächste Test braucht ≤ 4 % fürs Ziel, ≤ 13 % fürs Muss (Mittel der letzten 3)."));
  // Ohne Termine im Text: Blocktest-Modus bleibt an, nächster Test aus dem Kalender (geplantes Workout "Blocktest")
  const noDates = { ...evR, description: real.replace("(Basistest 25.10., dann 08.11., 06.12., 03.01.)", "") };
  const bN = resolveSeasonBlock([noDates], "2026-11-12");
  const gN = buildBlockGoalsFull(bN, { runs: runsR, plannedTests: ["2026-11-15"] }, "2026-11-12").goals.find((x) => x.key === "runDecoupling");
  assert.equal(gN.count, 2); assert.equal(gN.nextTest, "2026-11-15");
}
// Ramp Rate bei niedriger CTL: Hinweis, wie viel Last die Grenze erlaubt
{
  const ctlDays = Array.from({ length: 30 }, (_, i) => ({ date: new Date(Date.parse("2026-10-19") + i * 86400000).toISOString().slice(0, 10), ctl: 20 }));
  const ramp = buildBlockGoalsFull(bR, { ctlDays }, "2026-11-12").goals.find((x) => x.key === "ramp");
  assert.match(ramp.hints[0], /Bei CTL 20 erlaubt Ramp 5 etwa das 2,6-Fache der Erhaltungslast/);
}
console.log("block A calendar ok");

// Satz-Ziel aus dem Blocktext: Zeile mit "Kraft", nur der laufende Block zählt (Kraft-Karte)
assert.equal(parseStrengthSets("FIXPUNKTE\n- Lauf 3× pro Woche\n- Kraft ≥70 Sätze pro Woche"), 70);
assert.equal(parseStrengthSets("Kraft 2× pro Woche"), null);
{
  const ev = (start, desc) => ({ category: "SEASON_START", name: "Block B (8 Wochen)", start_date_local: start + "T00:00:00", description: desc });
  assert.equal(activeBlockStrengthSets([ev("2026-10-05", "Ziel\n\nLEITPLANKEN\n- Kraft ≥75 Sätze in 7 von 8 Belastungswochen")], "2026-10-22"), 75);
  assert.equal(activeBlockStrengthSets([ev("2026-11-02", "Ziel\n\n- Kraft ≥75 Sätze pro Woche")], "2026-10-22"), null);
  const tg = parseBlockTargets("Ziel\n\n- Kraft ≥75 Sätze pro Woche");
  assert.equal(tg.strength.sets, 75);
}
// Leitplanke über zwei Wochen geht der Zahl aus den Standardregeln vor; Fenster gleitend (Block A)
{
  const txt = "Es gelten meine Standardregeln (Ruhetag, Kraft ≥60 Sätze).\n\nLEITPLANKEN\n- Kraft ≥90 Sätze in 2 Wochen\n- Ramp Rate (CTL) in Belastungswochen ≤5";
  assert.deepEqual(parseStrengthTarget(txt), { sets: 90, weeks: 2 });
  assert.equal(parseStrengthSets(txt), 90);
  assert.deepEqual(parseStrengthTarget("Kraft ≥75 Sätze in 7 von 8 Belastungswochen"), { sets: 75, weeks: 1 });
  const ev = { category: "SEASON_START", name: "Block A (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: txt };
  assert.deepEqual(activeBlockStrengthTarget([ev], "2026-10-22"), { sets: 90, weeks: 2, start: "2026-10-19" });
  const blkA = resolveSeasonBlock([ev], "2026-11-04");
  const strength = [{ date: "2026-10-14", sets: 60 }, { date: "2026-10-21", sets: 30 }, { date: "2026-10-28", sets: 40 }, { date: "2026-11-03", sets: 20 }];
  const g = buildBlockGoals(blkA, { strength }, "2026-11-04").find((x) => x.key === "strength");
  assert.equal(g.group, "leitplanke");
  assert.equal(g.periodWeeks, 2);
  assert.equal(g.weekGoal, 90);
  assert.match(g.target, /≥ 90 Sätze je 2 Wochen \(ab Blockstart\)/);
  // Feste Abschnitte ab Blockstart: 19.10.–01.11. (30 + 40), 02.11.–15.11. (läuft); die Woche vor dem Block zählt nicht
  assert.deepEqual(g.weeks.map((w) => [w.start, w.end, w.count, w.done, w.hit]), [["2026-10-19", "2026-11-01", 70, true, false], ["2026-11-02", "2026-11-15", 20, false, false]]);
  assert.equal(g.status, "bad");
  assert.deepEqual(g.calWeek, { start: "2026-11-02", end: "2026-11-15", count: 20 });
}

// "6 von 8 Belastungswochen" in der Intensitäts-Zeile gehört nicht zur Kraft (echter Block A: Kraft ≥90 Sätze in 2 Wochen)
{
  const txt = "LEITPLANKEN\nKraft ≥90 Sätze in 2 Wochen, d. h. 3× Krafttraining à 30 Sätze im 14-Tage-Fenster.\nRamp Rate (CTL) in Belastungswochen ≤5.\nIntensität: Rad-HIIT und Hügelsprints je 1× pro Woche in 6 von 8 Belastungswochen ab Woche 2. Krankheitswochen zählen nicht als verfehlt.";
  const tk = parseBlockTargets(txt);
  assert.deepEqual(tk.strength, { sets: 90, periodWeeks: 2 });
  assert.equal(tk.intensity.need, 6);
  const ev = { category: "SEASON_START", name: "Block A (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: txt };
  const g = buildBlockGoals(resolveSeasonBlock([ev], "2026-10-22"), { strength: [{ date: "2026-10-20", sets: 30 }] }, "2026-10-22").find((x) => x.key === "strength");
  assert.match(g.target, /≥ 90 Sätze je 2 Wochen \(ab Blockstart\)/);
  assert.equal(g.note, "Dieser Abschnitt: 30 von 90 Sätzen");
}

// Abschlussziele mit Leerzeile nach der Überschrift ("1. Lauf: …\n\na) Basis: Decoupling …") und Terminen im Ablauf
// ("Basistest So 25.10., dann So 08.11."): Lauf, Rad und Schwimmen werden trotzdem erkannt
{
  const txt = "ABLAUF\n\nBlocktests (langer Lauf, Sonntag vor der Entlastungswoche): Basistest So 25.10., dann So 08.11., So 06.12., So 03.01.\n\nABSCHLUSSZIELE\n\n1. Lauf: aerobe Stabilität und Haltbarkeit\n\na) Basis: Decoupling der ersten 60 min im Blocktest, Mittel der letzten 3 Tests: Muss ≤8 %, Ziel ≤5 %. Pace der ersten 60 min gegenüber dem Basistest mindestens stabil.\n\nb) Haltbarkeit: Die stabile Dauer (längstes Fenster 0–60, 0–75, 0–90 … mit Decoupling ≤5 %) ist im letzten Blocktest länger als im Basistest.\n\n2. Rad (Rolle): aerobe Stabilität\n\nDecoupling (Leistung zu Puls) im 60-min-Messfenster ≤5 % im Mittel der letzten 3 Vergleichseinheiten.\n\n3. Schwimmen: Dauer mit stabiler Technik\n\n2× 500 m mit maximal 1 min Pause. Die zweiten 500 m höchstens 5 s pro 100 m langsamer als die ersten.\n\nHF-DECKEL\n\nLauf-Grundlage unter HF 128.";
  const tg = parseBlockTargets(txt);
  assert.equal(tg.run.max, 5);
  assert.equal(tg.run.must, 8);
  assert.deepEqual(tg.run.tests, ["10-25", "11-08", "12-06", "01-03"]);
  assert.equal(tg.bike.max, 5);
  assert.equal(tg.bike.firstMin, 60);
  assert.deepEqual(tg.swim, { reps: 2, distM: 500, maxPauseSec: 60, maxSlowerPer100: 5 });
  assert.equal(tg.decoupling, null);
  assert.equal(tg.hrCap, 128);
}

// Satz-Ziel aus dem Rahmen des Coaches (strengthSets2w): laufende Woche plus Vorwoche, geht dem Blocktext vor
{
  const ev = { category: "SEASON_START", name: "Block A (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: "Standardregeln (Kraft ≥60 Sätze)" };
  const blkA = resolveSeasonBlock([ev], "2026-11-04");
  const strength = [{ date: "2026-10-14", sets: 60 }, { date: "2026-10-21", sets: 30 }, { date: "2026-10-28", sets: 40 }, { date: "2026-11-03", sets: 20 }];
  const g = buildBlockGoals(blkA, { strength, frame: { strengthSets2w: 90 } }, "2026-11-04").find((x) => x.key === "strength");
  assert.equal(g.label, "Kraft"); assert.equal(g.weekGoal, 90); assert.equal(g.source, "rahmen"); assert.equal(g.rollWeeks, 2);
  assert.match(g.target, /≥ 90 Sätze in 2 Wochen/);
  // Woche 19.10. mit Vorwoche: 60 + 30, Woche 26.10.: 30 + 40, Woche 02.11. (läuft): 40 + 20
  assert.deepEqual(g.weeks.map((w) => [w.start, w.count, w.done, w.hit]), [["2026-10-19", 90, true, true], ["2026-10-26", 70, true, false], ["2026-11-02", 60, false, false]]);
  assert.equal(g.status, "bad");
  assert.deepEqual(g.calWeek, { start: "2026-10-26", end: "2026-11-08", count: 60 });
  assert.match(g.note, /Diese Woche und Vorwoche: 60 von 90 Sätzen/);
  // Heute (23.10.): 60 Sätze aus der Vorwoche, diese Woche noch keine
  const now = buildBlockGoals(resolveSeasonBlock([ev], "2026-10-23"), { strength: strength.slice(0, 1), frame: { strengthSets2w: 90 } }, "2026-10-23").find((x) => x.key === "strength");
  assert.equal(now.note, "Diese Woche und Vorwoche: 60 von 90 Sätzen");
}

// Block-Eintrag Stand 23.10.2026: Testband im Abschnitt BLOCKTEST LAUF, Einrollen und 165 W im Abschnitt RAD-VERGLEICHSEINHEIT.
// Rad zählt nur mit Warmup-Block (icu_warmup_time ≥ 300 s) und Messfenster ≥ 55 min bei 160–170 W; 19.10. ohne Einrollen = Orientierung
{
  const { rideMeasureIssue } = await import("../src/season-block.js");
  const txt = "Block A – Grundlage (12 Wochen, ab 19.10.2026)\n\nWochenziel, Steigerung, Intensität und Leitplanken stehen im aktuellen Rahmen im Dashboard. Sie ändern sich nur nach Absprache mit dem Coach, mit Grund. Hier stehen nur die festen Teile: Ziele, Tests, Protokolle.\n\nABLAUF\nBlocktests (langer Lauf): Basistest Fr 23.10., dann So 08.11., So 06.12., So 03.01. Termine fest, unabhängig von Entlastungswochen.\nTypische Woche: 2× Rad, 2× Lauf, 1× Schwimmen (meist Mittwoch), Kraft. Ein zweites Schwimmen ist Bonus.\n\nABSCHLUSSZIELE\nLauf: aerobe Stabilität und Haltbarkeit\na) Basis: Decoupling der ersten 60 min im Blocktest, Mittel der letzten 3 Tests: Muss ≤8 %, Ziel ≤5 %. Pace der ersten 60 min gegenüber dem Basistest mindestens stabil bzw. kein klar negativer Trend.\nb) Haltbarkeit: Die stabile Dauer (längstes Fenster 0–60, 0–75, 0–90 … mit Decoupling ≤5 %) ist im letzten Blocktest länger als im Basistest.\nJeder Grundlagenlauf ab 60 min unter HF 128 zählt als Trendindikator, nicht direkt für das Abschlussziel.\n\nRad (Rolle): aerobe Stabilität\nDecoupling (Leistung zu Puls) im 60-min-Messfenster ≤5 % im Mittel der letzten 3 Vergleichseinheiten.\nReferenz: FTP 252 W (Rampentest 12.10.). Erster Wert 19.10. (ohne Einrollen, nach Krankheit): 9,8 %, nur Orientierung.\n\nSchwimmen: Dauer mit stabiler Technik\n2× 500 m mit maximal 1 min Pause. Die zweiten 500 m höchstens 5 s pro 100 m langsamer als die ersten.\n\nHF-DECKEL\nLauf-Grundlage unter HF 128.\n\nBLOCKTEST LAUF (Protokoll)\nLanger Lauf in der vollen geplanten Länge, ruhig anlaufen, kein separates Einlaufen. Die ganze Zeit HF-Band 120–128.\nGleiche flache Runde in Bobingen, gleiche Tageszeit, gleiche Schuhe, gleiche Mahlzeit vorher, kein Alkohol am Vorabend, kein Hitzetag.\nAmpel rot oder krank: 1–2 Tage verschieben. Ampel gelb: laufen und notieren.\n\nRAD-VERGLEICHSEINHEIT (Protokoll)\n10 min Einrollen (ca. 5 min 110–130 W, 5 min 140–160 W) als „Warmup“, danach mindestens 60 min bei 165 W als „Main Set“ = Messfenster. Danach frei.\nVentilator von Beginn an gleiche Stufe, gleiche Raumtemperatur, ähnliche Tageszeit. 165 W bleibt im ganzen Block. Nicht direkt nach Krankheit.\n\nWOCHE 12\nAlle Kernziele erreicht: Block B ab 11.01.2027.\nKnapp verfehlt, aber klar positive Entwicklung: Block B kann beginnen, die Fähigkeit wird weiter beobachtet.\nDeutlich verfehlt oder keine positive Entwicklung: Grundlage der betroffenen Sportart um 3–4 Wochen verlängern, dann neu bewerten.\n\nAUSBLICK BLOCK B\nLauf-Haltbarkeit: stabile Dauer ≥90 min. Erstmals Sweet Spot/Schwelle und Koppelläufe.";
  const t23 = parseBlockTargets(txt);
  assert.deepEqual(t23.run.band, [120, 128]); assert.equal(t23.hrCap, 128);
  assert.deepEqual(checkBlockText(txt).band, [120, 128]); assert.deepEqual(checkBlockText(txt).hints, []);
  assert.deepEqual([t23.bike.warmupMin, t23.bike.watts, t23.bike.firstMin], [10, 165, 60]);
  // "Testband" und Bindestrich gehen auch
  assert.deepEqual(parseBlockTargets(txt.replace("HF-Band 120–128", "Testband 118-126")).run.band, [118, 126]);
  const cfg = t23.bike;
  const r19 = { date: "2026-10-19", durationMin: 60, decoupling: 9.803871, watts: 165, mainWatts: 165, warmupSec: 0, windowMin: 60, noWarmup: true, vi: 1, intensity: 65, indoor: true, easy: true, hasHr: true };
  const ok = { ...r19, date: "2026-10-26", durationMin: 72, decoupling: 4.2, warmupSec: 600, windowMin: 62, noWarmup: false };
  assert.equal(rideMeasureIssue(r19, cfg), "ohne Einrollen");
  assert.equal(rideMeasureIssue(ok, cfg), null);
  assert.equal(rideMeasureIssue({ ...ok, windowMin: 50 }, cfg), "Messfenster unter 55 min");
  assert.equal(rideMeasureIssue({ ...ok, mainWatts: 172 }, cfg), "nicht 160–170 W");
  assert.equal(rideMeasureIssue({ ...ok, warmupSec: 240, noWarmup: false }, cfg), "ohne Einrollen");
  const b23 = resolveSeasonBlock([{ category: "SEASON_START", name: "Block Grundlage (12 Wochen)", start_date_local: "2026-10-19T00:00:00", description: txt }], "2026-10-23");
  let gb = buildBlockGoalsFull(b23, { rides: [r19] }, "2026-10-23").goals.find((x) => x.key === "bikeDecoupling");
  assert.deepEqual([gb.value, gb.count, gb.status, gb.orientation], [null, 0, "none", { date: "2026-10-19", value: 9.8 }]);
  assert.equal(gb.note, "Noch keine Vergleichseinheit mit Einrollen – bewertet wird ab der ersten.");
  assert.equal(/von 3/.test(JSON.stringify(gb)), false);
  // ab der ersten echten Vergleichseinheit wird bewertet
  gb = buildBlockGoalsFull(b23, { rides: [r19, ok, { ...ok, date: "2026-10-28", windowMin: 45, decoupling: 1 }] }, "2026-10-30").goals.find((x) => x.key === "bikeDecoupling");
  assert.deepEqual([gb.value, gb.count, gb.status], [4.2, 1, "ok"]); assert.match(gb.note, /erst 1 von 3/);
  assert.ok(gb.hints.some((h) => /nicht vergleichbar: 1× Messfenster unter 55 min/.test(h)));
  console.log("block A 23.10. ok");
}
