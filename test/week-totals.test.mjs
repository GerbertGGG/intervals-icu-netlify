// Testet src/week-totals.js (Ausdauerstunden ohne Kraft, Kraftsätze in 2 Wochen). Ausführen: node test/week-totals.test.mjs
import assert from "node:assert/strict";
import { computeWeekTotals, currentWeekTotals, enduranceDiscipline, strengthSetsOf, WEEK_GOALS, weekTotalsOldest } from "../src/week-totals.js";

assert.deepEqual(WEEK_GOALS, { enduranceHours: 5, strengthSets2w: 90, streakHours: 4.5 });
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
assert.deepEqual(currentWeekTotals(t), { week: "2026-10-19", enduranceHours: 2.32, byDiscipline: { lauf: 0.57, rad: 1.75, schwimmen: 0 }, strengthSets: 0, strengthSets2w: 60, streakWeeksAbove4_5: 0, goals: WEEK_GOALS });
assert.equal(weekTotalsOldest("2026-10-23"), "2026-08-24");

// Serie: volle Wochen ab 4,5 h bis zur letzten vollen Woche, die laufende zählt erst, wenn sie die Schwelle erreicht
const ride = (date, h) => ({ start_date_local: date + "T10:00:00", type: "Ride", moving_time: h * 3600 });
const s = computeWeekTotals([ride("2026-10-05", 4.5), ride("2026-10-12", 5), ride("2026-09-28", 4)], "2026-10-23");
assert.deepEqual([s.streakWeeksAbove4_5, s.streakCapped], [2, false]);
assert.equal(computeWeekTotals([ride("2026-10-05", 4.5), ride("2026-10-12", 5), ride("2026-10-19", 4.6)], "2026-10-23").streakWeeksAbove4_5, 3);
assert.equal(computeWeekTotals([ride("2026-10-05", 5)], "2026-10-23").streakWeeksAbove4_5, 0); // Lücke in der Vorwoche
console.log("week-totals tests ok");
