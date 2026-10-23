// Testet src/week-totals.js (Ausdauerstunden ohne Kraft, Kraftsätze in 2 Wochen). Ausführen: node test/week-totals.test.mjs
import assert from "node:assert/strict";
import { computeWeekTotals, currentWeekTotals, enduranceDiscipline, enduranceWeeks, strengthSetsOf, weekTotalsOldest, withReference } from "../src/week-totals.js";
import { DEFAULT_TARGET, appendTarget, targetOn, targetHistory, targetsView, validateTargets } from "../src/targets-snapshot.js";

const GOALS = { enduranceHours: 5, strengthSets2w: 90, streakThresholdPct: 90 }; // Startwert ohne gespeicherte Ziele
assert.deepEqual(["Run", "VirtualRun", "TrailRun", "Ride", "VirtualRide", "GravelRide", "MountainBikeRide", "Swim", "OpenWaterSwim"].map((type) => enduranceDiscipline({ type })),
  ["lauf", "lauf", "lauf", "rad", "rad", "rad", "rad", "schwimmen", "schwimmen"]);
assert.deepEqual(["WeightTraining", "Yoga", "Workout", "Walk", "", undefined].map((type) => enduranceDiscipline({ type })), [null, null, null, null, null, null]);

// Sätze: Zahl aus der Kopfzeile, sonst je "Wdh × kg" ein Satz
const egym = "30 Sätze · 25,4 t Volumen · Dauer geschätzt\n\nBack Extension: 20 × 47 kg, 20 × 50 kg, 20 × 50 kg";
assert.equal(strengthSetsOf({ description: egym }), 30);
assert.equal(strengthSetsOf({ description: "Back Extension: 20 × 47 kg, 20 × 50 kg\nLeg Press: 20 × 97,5 kg\nRotary Torso: Gerät genutzt" }), 3);
assert.equal(strengthSetsOf({ description: null }), 0);

// Echte Woche ab 19.10.2026 (Intervals) und die Vorwoche, Stand Fr 23.10.
const acts = [
  { start_date_local: "2026-10-22T19:09:52", type: "VirtualRide", moving_time: 2700 },
  { start_date_local: "2026-10-20T18:32:19", type: "Run", moving_time: 2046 },
  { start_date_local: "2026-10-19T11:31:41", type: "VirtualRide", moving_time: 3600 },
  { start_date_local: "2026-10-17T18:56:07", type: "WeightTraining", moving_time: 1920, description: egym },
  { start_date_local: "2026-10-16T16:25:13", type: "Run", moving_time: 1976 },
  { start_date_local: "2026-10-12T19:50:42", type: "WeightTraining", moving_time: 1920, description: "30 Sätze · 23,4 t Volumen · Dauer geschätzt" },
  { start_date_local: "2026-10-12T11:53:31", type: "VirtualRide", moving_time: 1347 },
  { start_date_local: "2026-10-21T10:00:00", type: "Yoga", moving_time: 3600 }, // zählt nicht
  { start_date_local: "2026-10-21T12:00:00", type: "Ride", moving_time: 9000, icu_ignore_time: true }, // ignoriert
  { start_date_local: "2026-10-24T09:00:00", type: "Run", moving_time: 3600 }, // Zukunft
];
const t = computeWeekTotals(acts, "2026-10-23");
assert.equal(t.weeks.length, 8);
const cur = t.weeks.at(-1), prev = t.weeks.at(-2);
assert.deepEqual([cur.week, cur.complete, prev.week, prev.complete], ["2026-10-19", false, "2026-10-12", true]);
assert.equal(cur.enduranceMin, 139); // 6300 s Rad + 2046 s Lauf = 2:19
assert.deepEqual(cur.byDisciplineMin, { lauf: 34, rad: 105, schwimmen: 0 });
assert.deepEqual(cur.byDiscipline, { lauf: 0.57, rad: 1.75, schwimmen: 0 });
assert.deepEqual([cur.strengthSets, cur.strengthSets2w, prev.strengthSets], [0, 60, 60]);
assert.equal(prev.enduranceMin, 55); // Kraft (2 × 32 min) taucht nicht in den Stunden auf
assert.equal(t.streakWeeksAbove4_5, 0);
assert.deepEqual(currentWeekTotals(t), { week: "2026-10-19", enduranceHours: 2.32, referenceHours: 0.3, byDiscipline: { lauf: 0.57, rad: 1.75, schwimmen: 0 }, strengthSets: 0, strengthSets2w: 60, streakWeeksAbove4_5: 0, streakWeeks: 0, goals: GOALS });
assert.equal(weekTotalsOldest("2026-10-23"), "2026-08-24");
// Referenzwoche: Mittel der letzten 3 Wochen (12.10.: 55 min, davor nichts) = 18 min
assert.equal(cur.referenceMin, 18);
{
  // ohne leichte Wochen (unter 70 % ihrer eigenen Referenz): 300, 300, 150 (leicht), 300 -> Referenz der 5. Woche (300+300)/2 bzw. ohne 150
  const r = withReference([300, 300, 300, 150, 300, 330].map((enduranceMin) => ({ enduranceMin })));
  assert.deepEqual(r.map((x) => [x.referenceMin, x.light]), [[null, false], [300, false], [300, false], [300, true], [300, false], [300, false]]);
  // alle drei leicht: Mittel aller drei
  assert.equal(withReference([600, 600, 600, 100, 100, 100, 100].map((enduranceMin) => ({ enduranceMin })))[6].referenceMin, 100);
  // Wochen ab dem ersten vollen Montag ab Ladebeginn, mit Referenz
  const ew = enduranceWeeks(acts, "2026-10-08", "2026-10-23");
  assert.deepEqual(ew.map((x) => [x.week, x.enduranceMin, x.referenceMin]), [["2026-10-12", 55, null], ["2026-10-19", 139, 55]]);
}

// Serie: volle Wochen ab 4,5 h bis zur letzten vollen Woche, die laufende zählt erst, wenn sie die Schwelle erreicht
const ride = (date, h) => ({ start_date_local: date + "T10:00:00", type: "Ride", moving_time: h * 3600 });
const s = computeWeekTotals([ride("2026-10-05", 4.5), ride("2026-10-12", 5), ride("2026-09-28", 4)], "2026-10-23");
assert.deepEqual([s.streakWeeksAbove4_5, s.streakCapped], [2, false]);
assert.equal(computeWeekTotals([ride("2026-10-05", 4.5), ride("2026-10-12", 5), ride("2026-10-19", 4.6)], "2026-10-23").streakWeeksAbove4_5, 3);
assert.equal(computeWeekTotals([ride("2026-10-05", 5)], "2026-10-23").streakWeeksAbove4_5, 0); // Lücke in der Vorwoche

// save_targets: Prüfung der Eingaben
const T0 = "2026-10-23", at = "2026-10-23T06:00:00Z";
const ok = validateTargets({ fetchedAt: at, enduranceHoursPerWeek: 5.5, strengthSets2w: 100, reason: "6 Wochen ohne gelbe Signale" }, T0).value;
assert.deepEqual(ok, { enduranceHoursPerWeek: 5.5, strengthSets2w: 100, streakThresholdPct: 90, maxIncreasePct: 10, intensity: DEFAULT_TARGET.intensity, guardrails: DEFAULT_TARGET.guardrails,
  notAllowed: DEFAULT_TARGET.notAllowed, changed: ["enduranceHoursPerWeek", "strengthSets2w"], reason: "6 Wochen ohne gelbe Signale", validFrom: "2026-10-19", fetchedAt: "2026-10-23T06:00:00.000Z" }); // Standard: Montag der laufenden Woche
for (const bad of [{ enduranceHoursPerWeek: 5.3 }, { enduranceHoursPerWeek: 0.75 }, { enduranceHoursPerWeek: 20.25 }, { strengthSets2w: 301 }, { strengthSets2w: 2.5 }, { reason: "" }, { reason: "r".repeat(201) },
  { validFrom: "2.11.2026" }, { streakThresholdPct: 40 }, { fetchedAt: "kaputt" }, { maxIncreasePct: 60 }, { maxIncreasePct: "viel" }, { intensity: "rad HIIT" },
  { intensity: [{ sport: "rudern", text: "x" }] }, { intensity: [{ sport: "rad", text: "x".repeat(161) }] }, { intensity: Array.from({ length: 7 }, () => ({ sport: "kraft", text: "x" })) },
  { guardrails: ["x".repeat(161)] }, { guardrails: Array(9).fill("x") }, { guardrails: [""] }, { notAllowed: ["x".repeat(81)] }, { notAllowed: Array(9).fill("x") }, { notAllowed: "Sweet Spot" }]) assert.ok(validateTargets({ fetchedAt: at, enduranceHoursPerWeek: 5, strengthSets2w: 90, reason: "x", ...bad }, T0).error, JSON.stringify(bad));
assert.equal(validateTargets({ fetchedAt: at, enduranceHoursPerWeek: "5,25", strengthSets2w: 0, reason: "x", streakThresholdPct: 80 }, T0).value.enduranceHoursPerWeek, 5.25);

// Ohne Eintrag gilt der Startwert, auch für Tage davor
assert.equal(targetOn(targetHistory(null), "2026-09-01"), DEFAULT_TARGET);
assert.deepEqual([targetsView(null, T0).current.enduranceHoursPerWeek, targetsView(null, T0).current.reason, targetsView(null, T0).previous], [5, "Startwert aus Blockplan A", null]);
assert.deepEqual(targetsView(null, T0).current, {
  enduranceHoursPerWeek: 5, strengthSets2w: 90, streakThresholdPct: 90, maxIncreasePct: 10,
  intensity: [
    { sport: "lauf", text: "1× Hügelsprints am Ende eines lockeren Laufs, 4× 8 s bis 8× 10 s; nicht in Entlastungswochen" },
    { sport: "rad", text: "1× HIIT pro Woche ab Woche 2, z. B. 2× 8× 30/30 oder 3–4× 4 min bei 265–270 W; in Entlastungswochen halbiert" },
  ],
  guardrails: ["Kein HIIT/Sprints 2 Tage vor Blocktest", "Hüft-/Leistenschmerz bei Sprints: sofort abbrechen, 1 Woche aussetzen", "Krankheitswochen zählen nicht als verfehlt"],
  notAllowed: ["Sweet Spot", "Renntempo-Läufe", "Koppelläufe", "andere Laufintervalle"],
  validFrom: "2026-10-19", reason: "Startwert aus Blockplan A", savedAt: null, default: true,
});

// Nur ein Feld ändern (Intensität Rad): alles andere kommt vom geltenden Eintrag, Lauf-Intensität bleibt
const rad = { sport: "rad", text: "1× HIIT pro Woche, z. B. 2× 8× 30/30" };
const firstSave = appendTarget(null, validateTargets({ fetchedAt: at, enduranceHoursPerWeek: 5.5, maxIncreasePct: 8, reason: "mehr Umfang" }, T0).value);
const onlyRad = validateTargets({ fetchedAt: at, intensity: [rad], reason: "HIIT kürzer" }, T0, firstSave).value;
assert.deepEqual(onlyRad.changed, ["intensity"]);
assert.deepEqual([onlyRad.enduranceHoursPerWeek, onlyRad.strengthSets2w, onlyRad.streakThresholdPct, onlyRad.maxIncreasePct], [5.5, 90, 90, 8]); // aus dem ersten Eintrag
assert.deepEqual(onlyRad.intensity, [DEFAULT_TARGET.intensity[0], rad]); // Lauf übernommen, Rad ersetzt
assert.deepEqual([onlyRad.guardrails, onlyRad.notAllowed], [DEFAULT_TARGET.guardrails, DEFAULT_TARGET.notAllowed]);
const radView = targetsView(appendTarget(firstSave, onlyRad), T0);
assert.deepEqual([radView.current.reason, radView.current.changed, radView.current.intensity.map((x) => x.sport), radView.history.map((t) => t.reason)],
  ["HIIT kürzer", ["intensity"], ["lauf", "rad"], ["HIIT kürzer", "mehr Umfang", "Startwert aus Blockplan A"]]);
// text "" streicht eine Sportart, Listen ersetzen als Ganzes, Schwimmen kommt dazu (Sortierung lauf, rad, schwimmen, kraft)
const edit = validateTargets({ fetchedAt: at, intensity: [{ sport: "lauf", text: "" }, { sport: "schwimmen", text: "1× 10× 100 m" }], notAllowed: ["Sweet Spot"], guardrails: [], reason: "x" }, T0, firstSave).value;
assert.deepEqual([edit.intensity.map((x) => x.sport), edit.notAllowed, edit.guardrails, edit.changed], [["rad", "schwimmen"], ["Sweet Spot"], [], ["intensity", "guardrails", "notAllowed"]]);
// short (Kurzfassung fürs Widget) wird mitgespeichert, zu lang ist ein Fehler
assert.deepEqual(validateTargets({ fetchedAt: at, intensity: [{ ...rad, short: " 1× HIIT ab Woche 2 " }], reason: "x" }, T0, firstSave).value.intensity[1], { ...rad, short: "1× HIIT ab Woche 2" });
assert.ok(validateTargets({ fetchedAt: at, intensity: [{ ...rad, short: "x".repeat(41) }], reason: "x" }, T0, firstSave).error);
// Ein gleicher Wert zählt nicht als geändert
assert.deepEqual(validateTargets({ fetchedAt: at, maxIncreasePct: 10, reason: "x" }, T0).value.changed, []);
// Alte Einträge ohne die neuen Felder (vor dieser Änderung gespeichert) erben sie
const legacy = { items: [{ enduranceHoursPerWeek: 6, strengthSets2w: 80, streakThresholdPct: 85, reason: "alt", validFrom: "2026-10-19", fetchedAt: at }] };
const lv = targetsView(legacy, T0).current;
assert.deepEqual([lv.enduranceHoursPerWeek, lv.maxIncreasePct, lv.intensity, lv.notAllowed, lv.changed], [6, 10, DEFAULT_TARGET.intensity, DEFAULT_TARGET.notAllowed, undefined]);
assert.equal(computeWeekTotals(acts, T0, legacy).weeks.at(-1).goalHours, 6);

// Ziel 5 -> 5,5 h ab Montag 02.11. (in der Zukunft): heute und alle vergangenen Wochen bleiben bei 5 h
const base = [...acts, ride("2026-10-05", 4.6), ride("2026-09-28", 4.8)]; // zwei Wochen ab 4,5 h vor der Vorwoche
const before = computeWeekTotals(base, T0);
const future = appendTarget(null, { ...ok, validFrom: "2026-11-02" });
const after = computeWeekTotals(base, T0, future);
assert.deepEqual(after, before); // nichts ändert sich vor dem 02.11.
assert.deepEqual(after.weeks.map((w) => w.goalHours), [5, 5, 5, 5, 5, 5, 5, 5]);
const v = targetsView(future, T0);
assert.deepEqual([v.current.enduranceHoursPerWeek, v.upcoming.map((t) => [t.validFrom, t.enduranceHoursPerWeek])], [5, [["2026-11-02", 5.5]]]);
// ... zwei Wochen später: KW 45 zählt gegen 5,5 h (Schwelle 4:57), die Wochen davor behalten 5 h
const later = [...base, ride("2026-10-26", 4.6), ride("2026-11-02", 4.6)];
const w2 = computeWeekTotals(later, "2026-11-08", future);
assert.deepEqual(w2.weeks.map((w) => [w.week, w.goalHours, w.streakMin, w.streakMet]).slice(-3), [
  ["2026-10-19", 5, 270, false], ["2026-10-26", 5, 270, true], ["2026-11-02", 5.5, 297, false]]); // 08.11. ist Sonntag
assert.equal(computeWeekTotals(later, "2026-11-08").weeks.at(-1).streakMet, true); // mit 5 h wäre KW 45 dabei gewesen
assert.equal(targetsView(future, "2026-11-08").previous.enduranceHoursPerWeek, 5);
// Schwelle je Eintrag: 80 % von 5,5 h = 4:24 -> die 4:36 aus KW 45 zählen
assert.equal(computeWeekTotals(later, "2026-11-08", appendTarget(null, { ...ok, validFrom: "2026-11-02", streakThresholdPct: 80 })).weeks.at(-1).streakMet, true);
// Neuester Eintrag gewinnt; ein späterer mit älterem validFrom überdeckt den früheren
const two = appendTarget(future, { ...ok, enduranceHoursPerWeek: 6, validFrom: "2026-10-19", reason: "korrigiert" });
assert.equal(targetOn(targetHistory(two), "2026-11-10").enduranceHoursPerWeek, 6);
assert.deepEqual(targetsView(two, T0).history.map((t) => t.enduranceHoursPerWeek), [6, 5.5, 5]); // neueste zuerst, Startwert zuletzt
console.log("week-totals tests ok");

// Stiller Hinweis Ramp Rate / ACWR (Karte "Aktueller Rahmen"): nur bei Ramp > 5 oder ACWR an mind. 3 der letzten 7 Tage > 1,3
const { buildLoadHint } = await import("../src/dashboard.js");
const wd = (date, ctl, atl, rampRate = null) => ({ date, ctl, atl, rampRate });
const calm = ["2026-10-17", "2026-10-18", "2026-10-19", "2026-10-20", "2026-10-21", "2026-10-22", "2026-10-23"].map((x) => wd(x, 40, 44, 3));
assert.equal(buildLoadHint(calm, T0), null);
assert.equal(buildLoadHint([...calm.slice(0, 5), wd("2026-10-22", 40, 54), wd("2026-10-23", 40, 56, 4)], T0), null); // nur 2 Tage über 1,3
const hi = buildLoadHint([wd("2026-10-15", 40, 60), wd("2026-10-16", 40, 60), ...calm.slice(0, 5), wd("2026-10-22", 40, 54), wd("2026-10-23", 40, 56, 5.6)], T0);
assert.deepEqual([hi.ramp, hi.acwrDays, hi.acwrPeak], [5.6, null, null]); // 15./16.10. liegen außerhalb der 7 Tage
const both = buildLoadHint([...calm.slice(0, 4), wd("2026-10-21", 40, 54), wd("2026-10-22", 40, 54), wd("2026-10-23", 40, 56, 6)], T0);
assert.deepEqual([both.ramp, both.acwrDays, both.acwrPeak], [6, 3, 1.4]);
console.log("load-hint tests ok");
