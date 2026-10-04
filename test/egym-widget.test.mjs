import test from "node:test";
import assert from "node:assert/strict";
import { buildWidgetKraft, strengthDays, strengthProgress } from "../src/egym-widget.js";

// Garmin-Tagesaktivitaet wie in der echten Antwort: zaehlt nicht als Krafttraining
const garmin = { code: "g1", completedAt: "2026-10-03T21:59:59Z", exercises: [{ name: "Daily routine", exercise: { machineBased: false }, attributes: { calories: { unit: "kcal", value: 69 } } }] };
const set = (reps, kg) => ({ reps: { value: reps, unit: "rep" }, weight: { value: kg, unit: "kg" } });
const gym = (code, at, sets) => ({ code, completedAt: at, exercises: [{ name: "Lat Pulldown", exercise: { machineBased: true }, attributes: { sets_of_reps_and_weight_or_duration_and_weight: sets } }] });

const m = (code, label, at, kg, region = "UPPER") => ({ createdAt: at, exercise: { code, label }, strength: { value: kg, createdAt: "1970-01-01T00:00:00Z" }, bodyRegion: region });

test("Garmin-Tagesaktivitaet ist keine Kraft-Einheit, Geraete-Saetze schon", () => {
  const days = strengthDays([garmin, gym("a", "2026-10-01T16:00:00Z", [set(10, 50), set(8, 60)])]);
  assert.deepEqual(days, [{ date: "2026-10-01", sets: 2, volumeKg: 980 }]);
});

test("doppelte Workout-Nummer: die vollstaendigere Variante gewinnt", () => {
  const days = strengthDays([gym("a", "2026-10-01T16:00:00Z", [set(10, 50)]), gym("a", "2026-10-01T16:00:00Z", [set(10, 50), set(10, 50)])]);
  assert.equal(days[0].sets, 2);
});

test("Fortschritt: letzter Test gegen den vorherigen je Geraet, 1970-Platzhalter ignoriert", () => {
  const p = strengthProgress([
    m("1000", "EGYM Lat Pulldown", "2026-08-20T08:00:00Z", 120),
    m("1000", "EGYM Lat Pulldown", "2026-09-28T08:47:10Z", 128),
    m("1003", "EGYM Abductor", "2026-09-28T08:44:06Z", 101, "CORE"),
  ]);
  const lat = p.items.find((i) => i.label === "Lat Pulldown");
  assert.equal(lat.diffKg, 8);
  assert.equal(lat.pct, 6.7);
  assert.equal(p.items[0].label, "Lat Pulldown");
  assert.equal(p.items.find((i) => i.label === "Abductor").diffKg, null);
  assert.deepEqual([p.machines, p.improved, p.declined], [2, 1, 0]);
});

test("Woche: Einheiten gegen Ziel, Serie, Wochentage", () => {
  // Heute Sonntag 04.10.2026, Woche ab Montag 28.09.
  const w = [gym("a", "2026-09-29T16:00:00Z", [set(10, 50)]), gym("b", "2026-10-02T16:00:00Z", [set(10, 50)]), gym("c", "2026-09-22T16:00:00Z", [set(10, 50)]), gym("d", "2026-09-24T16:00:00Z", [set(10, 50)]), garmin];
  const d = buildWidgetKraft({ workouts: { workouts: w }, strength: { strengthMeasurements: [] }, bioAge: null, today: "2026-10-04", env: {}, generatedAt: "x" });
  assert.equal(d.week.sessions, 2);
  assert.equal(d.week.goal, 2);
  assert.equal(d.week.goalSource, "default");
  assert.deepEqual(d.week.days, [false, true, false, false, true, false, false]);
  assert.equal(d.streakWeeks, 2);
  assert.equal(d.daysSince, 2);
  assert.equal(d.weeks.length, 6);
});

test("EGYM_WEEKLY_GOAL gilt, ohne Daten bleibt alles leer statt erfunden", () => {
  const d = buildWidgetKraft({ workouts: null, strength: null, bioAge: null, today: "2026-10-04", env: { EGYM_WEEKLY_GOAL: "3" }, failed: ["egymWorkouts"] });
  assert.equal(d.week.goal, 3);
  assert.equal(d.week.goalSource, "config");
  assert.equal(d.daysSince, null);
  assert.equal(d.progress, null);
  assert.equal(d.streakWeeks, 0);
});

test("Muskelalter aus der Bio-Age-Antwort", () => {
  const d = buildWidgetKraft({ workouts: [], strength: [], bioAge: { totalDetails: { totalBioAge: { value: 42 } }, muscleDetails: { muscleBioAge: { value: 36 }, upperBodyAge: { value: 51 }, coreAge: { value: 35 }, lowerBodyAge: { value: 21 } } }, today: "2026-10-04" });
  assert.deepEqual(d.bioAge, { total: 42, muscle: 36, upper: 51, core: 35, lower: 21 });
});
