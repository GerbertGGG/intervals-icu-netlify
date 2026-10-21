// Montagsbilanz (week-review.js): Faktenliste zur letzten vollen Woche, ohne Stufe und Vorschlag (das entscheidet der Coach).
// Ausführen: node test/week-review.test.mjs
import assert from "node:assert/strict";
import { buildWeekReview } from "../src/week-review.js";

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const sport = (minutes, load = minutes, targetMin = null, targetLoad = null) => ({ count: minutes ? 3 : 0, minutes, load, targetMin, targetLoad });
// 5 volle Wochen (Mo 14.09. – So 18.10.) plus die laufende ab 19.10.
const week = (start, runMin, extra = {}) => ({
  weekStart: start, complete: start < "2026-10-19", runMin, load: 300,
  bySport: { run: sport(runMin, runMin, extra.runTarget ?? null), bike: sport(150, extra.bikeLoad ?? 150, extra.bikeTarget ?? null, extra.bikeCap ?? null), swim: sport(60), strength: sport(40), other: sport(0) },
});
const starts = ["2026-09-14", "2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19"];
const weeks = starts.map((s, i) => week(s, i === 4 ? 150 : 140, i === 4 ? { runTarget: 200, bikeTarget: 150, bikeCap: 120 } : {}));
const days = Array.from({ length: 35 }, (_, i) => addDays("2026-09-14", i));
const wellness = days.map((date) => ({ date, fatigue: date >= "2026-10-12" ? 3 : 2, soreness: 2 }));
// jeden Tag trainiert, unterschiedlich lang, RPE bei allen Einheiten
const sessions = days.map((date, i) => ({ date, sport: i % 2 ? "bike" : "run", minutes: [30, 90, 45, 60, 20, 120, 40][i % 7], rpe: 3 }));
const runs = days.filter((_, i) => i % 2 === 0).map((date, i) => ({ date, movingTimeMin: [30, 45, 20, 40, 90][i % 5] }));
const base = { weeks, wellness, sessions, runs, daily: [], sickDays: [], hipFlags: [], capRuns: null };

{
  const r = buildWeekReview(base);
  assert.equal(r.weekStart, "2026-10-12");
  // keine Stufe, kein Vorschlag, keine Gründe
  for (const k of ["level", "proposal", "reasons", "groups", "ampel"]) assert.equal(r[k], undefined, k);
  assert.deepEqual(r.feel.fatigue, { label: "Ermüdung", mean: 3, ref: 2 });
  assert.equal(r.rpe.basis, "rpe"); assert.ok(r.rpe.monotony > 0);
  assert.equal(r.run.minutes, 150); assert.equal(r.run.ref, 140); assert.equal(r.run.ratio, 1.07);
  // Rahmen: Lauf 150 von 200, Rad 150 von 150 -> 300 von 350 min; Rad über der TSS-Grenze
  assert.equal(r.frameShare, 0.86);
  assert.equal(r.frame.find((f) => f.sport === "bike").over, true);
}
// Ohne RPE-Abdeckung rechnet die Monotonie mit dem Tages-TSS
{
  const daily = days.map((date, i) => ({ date, load: [30, 90, 45, 60, 20, 120, 40][i % 7] }));
  const r = buildWeekReview({ ...base, sessions: sessions.map((x) => ({ ...x, rpe: null })), daily });
  assert.equal(r.rpe.basis, "tss"); assert.equal(r.rpe.srpe, null); assert.ok(r.rpe.monotony > 0);
}
// Krankheit, Beschwerden, HF-Deckel und Kraft stehen als Fakten drin
{
  const r = buildWeekReview({ ...base, sickDays: ["2026-10-13", "2026-09-30"], hipFlags: [{ date: "2026-10-15" }, { date: "2026-10-15" }],
    capRuns: [{ date: "2026-10-14", overCapMin: 7 }, { date: "2026-10-16", overCapMin: 5 }, { date: "2026-10-05", overCapMin: 30 }], strength: { count: 30, goal: 60, unit: "Sätze" } });
  assert.deepEqual(r.sickDays, ["2026-10-13"]);
  assert.deepEqual(r.hipDays, ["2026-10-15"]);
  assert.equal(r.run.capOver, 12);
  assert.deepEqual(r.strength, { count: 30, goal: 60, unit: "Sätze" });
}
// Ohne volle Woche keine Bilanz
assert.equal(buildWeekReview({ ...base, weeks: [weeks[5]] }), null);

console.log("week-review ok");
