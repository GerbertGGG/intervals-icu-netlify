import test from "node:test";
import assert from "node:assert/strict";
import { buildWidgetKraft, strengthDays, strengthProgress, machineSessions, machineInsights } from "../src/egym-widget.js";

// Garmin-Tagesaktivitaet wie in der echten Antwort: zaehlt nicht als Krafttraining
const garmin = { code: "g1", completedAt: "2026-10-03T21:59:59Z", exercises: [{ name: "Daily routine", exercise: { machineBased: false }, attributes: { calories: { unit: "kcal", value: 69 } } }] };
const set = (reps, kg) => ({ reps: { value: reps, unit: "rep" }, weight: { value: kg, unit: "kg" } });
const gym = (code, at, sets, durationSec) => ({ code, completedAt: at, exercises: [{ name: "Lat Pulldown", completedAt: at, exercise: { machineBased: true }, attributes: { sets_of_reps_and_weight_or_duration_and_weight: sets, ...(durationSec ? { duration: { unit: "sec", value: durationSec } } : {}) } }] });

const m = (code, label, at, kg, region = "UPPER") => ({ createdAt: at, exercise: { code, label }, strength: { value: kg, createdAt: "1970-01-01T00:00:00Z" }, bodyRegion: region });

test("Garmin-Tagesaktivitaet ist keine Kraft-Einheit, Geraete-Saetze schon", () => {
  const days = strengthDays([garmin, gym("a", "2026-10-01T16:00:00Z", [set(10, 50), set(8, 60)])]);
  assert.deepEqual(days, [{ date: "2026-10-01", sets: 2, volumeKg: 980, minutes: null, durationSource: null }]);
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

test("Dauer: EGYM-Angabe, sonst Saetze, sonst nur geschaetzte Spanne, sonst keine", () => {
  const d1 = strengthDays([gym("a", "2026-10-01T16:00:00Z", [set(10, 50)], 1500)]);
  assert.deepEqual([d1[0].minutes, d1[0].durationSource], [25, "egym"]);
  const withSetTime = { code: "s", completedAt: "2026-10-02T16:00:00Z", exercises: [{ exercise: { machineBased: true }, attributes: { sets_of_reps_and_weight_or_duration_and_weight: [{ ...set(10, 50), duration: { unit: "sec", value: 90 } }, { ...set(10, 50), duration: { unit: "sec", value: 90 } }] } }] };
  const d2 = strengthDays([withSetTime]);
  assert.deepEqual([d2[0].minutes, d2[0].durationSource], [3, "egym"]);
  const twoEx = { code: "t", completedAt: "2026-10-03T16:40:00Z", exercises: [
    { completedAt: "2026-10-03T16:00:00Z", exercise: { machineBased: true }, attributes: {} },
    { completedAt: "2026-10-03T16:40:00Z", exercise: { machineBased: true }, attributes: {} } ] };
  const d3 = strengthDays([twoEx]);
  assert.deepEqual([d3[0].minutes, d3[0].durationSource], [40, "estimate"]);
});

test("Woche: Minuten gegen Ziel 60, Serie, Wochentage", () => {
  // Heute Sonntag 04.10.2026, Woche ab Montag 28.09.
  const w = [gym("a", "2026-09-29T16:00:00Z", [set(10, 50)], 1800), gym("b", "2026-10-02T16:00:00Z", [set(10, 50)], 2100), gym("c", "2026-09-22T16:00:00Z", [set(10, 50)], 3600), gym("d", "2026-09-17T16:00:00Z", [set(10, 50)], 4000), garmin];
  const d = buildWidgetKraft({ workouts: { workouts: w }, strength: { strengthMeasurements: [] }, bioAge: null, today: "2026-10-04", env: {}, generatedAt: "x" });
  assert.equal(d.week.minutes, 65);
  assert.equal(d.week.goalMin, 60);
  assert.equal(d.week.goalSource, "default");
  assert.equal(d.week.sessions, 2);
  assert.deepEqual(d.week.days, [false, true, false, false, true, false, false]);
  assert.deepEqual(d.week.dayMinutes, [null, 30, null, null, 35, null, null]);
  assert.equal(d.streakWeeks, 3); // laufende Woche (65), 21.-27.09. (60), 14.-20.09. (67)
  assert.deepEqual(d.weeksHit, { hit: 2, of: 4 });
  assert.equal(d.daysSince, 2);
  assert.equal(d.weeks.length, 6);
});

test("EGYM_WEEKLY_GOAL_MIN gilt, ohne Daten bleibt alles leer statt erfunden", () => {
  const d = buildWidgetKraft({ workouts: null, strength: null, bioAge: null, today: "2026-10-04", env: { EGYM_WEEKLY_GOAL_MIN: "90" }, failed: ["egymWorkouts"] });
  assert.equal(d.week.goalMin, 90);
  assert.equal(d.week.goalSource, "config");
  assert.equal(d.week.minutes, 0);
  assert.equal(d.daysSince, null);
  assert.equal(d.progress, null);
  assert.equal(d.avg4Minutes, 0);
  assert.equal(d.volumeTrend, null);
  assert.equal(d.streakWeeks, 0);
});

test("Einheit ohne Dauer: Minuten unbekannt (null) statt 0", () => {
  const d = buildWidgetKraft({ workouts: [gym("a", "2026-09-29T16:00:00Z", [set(10, 50)])], today: "2026-10-04" });
  assert.equal(d.week.minutes, null);
  assert.equal(d.week.sessions, 1);
  assert.equal(d.week.minutesSource, null);
});

test("Muskelalter aus der Bio-Age-Antwort", () => {
  const d = buildWidgetKraft({ workouts: [], strength: [], bioAge: { totalDetails: { totalBioAge: { value: 42 } }, muscleDetails: { muscleBioAge: { value: 36 }, upperBodyAge: { value: 51 }, coreAge: { value: 35 }, lowerBodyAge: { value: 21 } } }, today: "2026-10-04" });
  assert.deepEqual(d.bioAge, { total: 42, muscle: 36, upper: 51, core: 35, lower: 21 });
});

// Geraete-Workout mit exerciseCode, damit Bestwerte und Koerperbereich gebildet werden koennen
const machine = (code, exCode, name, at, sets) => ({ code, completedAt: at, exercises: [{ name, exerciseCode: exCode, completedAt: at, exercise: { machineBased: true }, attributes: { sets_of_reps_and_weight_or_duration_and_weight: sets } }] });

test("Bestwert: bester Satz dieser Woche gegen alle fruehere Einheiten, geschaetzter 1RM nach Epley", () => {
  const sessions = machineSessions([
    machine("a", "1000", "EGYM Lat Pulldown", "2026-09-15T16:00:00Z", [set(10, 50)]),
    machine("b", "1000", "EGYM Lat Pulldown", "2026-09-29T16:00:00Z", [set(10, 55), set(8, 50)]),
    machine("c", "1003", "EGYM Abductor", "2026-09-29T16:00:00Z", [set(10, 40)]),
    machine("d", "1003", "EGYM Abductor", "2026-09-15T16:00:00Z", [set(10, 45)]),
  ], new Map([["1000", "UPPER"]]));
  const { records } = machineInsights(sessions, "2026-09-28");
  assert.equal(records.length, 1); // Abductor wurde schwaecher
  assert.equal(records[0].label, "Lat Pulldown");
  assert.equal(records[0].kg, 55);
  assert.equal(records[0].reps, 10);
  assert.equal(records[0].e1rm, 73.3); // 55 * (1 + 10/30)
  assert.equal(records[0].diffKg, 6.7); // gegen 50 * (1 + 10/30) = 66,7
  assert.equal(records[0].pct, 10);
});

test("Ohne fruehere Einheit gibt es keinen Bestwert", () => {
  const { records } = machineInsights(machineSessions([machine("a", "1000", "EGYM Lat Pulldown", "2026-09-29T16:00:00Z", [set(10, 55)])]), "2026-09-28");
  assert.deepEqual(records, []);
});

test("Verteilung nach Koerperbereich nur bei genug zugeordnetem Volumen", () => {
  const w = [machine("a", "1000", "Lat Pulldown", "2026-09-29T16:00:00Z", [set(10, 60)]), machine("b", "1003", "Abductor", "2026-09-30T16:00:00Z", [set(10, 20)]), machine("c", "1005", "Leg Press", "2026-10-01T16:00:00Z", [set(10, 20)])];
  const map = new Map([["1000", "UPPER"], ["1003", "CORE"], ["1005", "LOWER"]]);
  assert.deepEqual(machineInsights(machineSessions(w, map), "2026-09-28").regions, { UPPER: 60, CORE: 20, LOWER: 20 });
  assert.equal(machineInsights(machineSessions(w, new Map([["1003", "CORE"]])), "2026-09-28").regions, null); // 20 von 100: zu wenig
});

test("buildWidgetKraft liefert Bestwerte und Verteilung, Region ueber den Kraft-Test-Code", () => {
  const d = buildWidgetKraft({
    workouts: [machine("a", "1000", "EGYM Lat Pulldown", "2026-09-15T16:00:00Z", [set(10, 50)]), machine("b", "1000", "EGYM Lat Pulldown", "2026-09-29T16:00:00Z", [set(10, 55)])],
    strength: [{ createdAt: "2026-09-28T08:00:00Z", exercise: { code: "1000", label: "EGYM Lat Pulldown" }, strength: { value: 128 }, bodyRegion: "UPPER" }],
    today: "2026-10-04",
  });
  assert.equal(d.records[0].label, "Lat Pulldown");
  assert.deepEqual(d.regions, { UPPER: 100, CORE: 0, LOWER: 0 });
});
