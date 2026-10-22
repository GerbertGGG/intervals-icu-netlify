// Testet src/experiment-snapshot.js (Experiment, Profil, Trainingstage). Ausführen: node test/experiment.test.mjs
import assert from "node:assert/strict";
import { validateExperiment, validateProfil, computeTrainingDays } from "../src/experiment-snapshot.js";

const base = { fetchedAt: "2026-10-22T06:00:00Z", title: " Trainingstage ", hypothesis: "Mehr Tage, gleiche Zeit.", status: "vorgeschlagen", startDate: null, minWeeks: 6, targetDaysPerWeek: 5, metrics: [], verdict: null };
const ok = validateExperiment(base).value;
assert.deepEqual(ok, { fetchedAt: "2026-10-22T06:00:00.000Z", title: "Trainingstage", hypothesis: "Mehr Tage, gleiche Zeit.", status: "vorgeschlagen", startDate: null, minWeeks: 6, targetDaysPerWeek: 5, metrics: [], verdict: null });
assert.equal(validateExperiment({ ...base, status: "laeuft" }).value.status, "läuft");
for (const bad of [
  { fetchedAt: "kaputt" }, { title: "" }, { title: "t".repeat(81) }, { hypothesis: "" }, { hypothesis: "h".repeat(201) },
  { status: "fertig" }, { startDate: "19.10.2026" }, { minWeeks: 0 }, { minWeeks: 2.5 }, { targetDaysPerWeek: 8 },
  { metrics: "lauf" }, { verdict: "v".repeat(201) },
  { metrics: [{ key: "lauf_hf130", label: "a", unit: "s/km", better: "schneller" }] },
  { metrics: [{ key: "lauf_hf130", label: "a", unit: "s/km", baseline: "schnell", better: "lower" }] },
  { metrics: [{ key: "lauf_hf130", label: "a", unit: "s/km", baselineDate: "gestern", better: "lower" }] },
  { metrics: [{ key: "lauf_hf130", label: "a", better: "lower" }, { key: "lauf_hf130", label: "b", better: "lower" }] },
  { metrics: [{ key: "lauf_hf130", label: "l".repeat(61), better: "lower" }] },
]) assert.ok(validateExperiment({ ...base, ...bad }).error, JSON.stringify(bad));
// Metrik: fehlendes better aus dem Key, fehlende Werte als null, Liste auch als JSON-Text
const m = validateExperiment({ ...base, metrics: JSON.stringify([{ key: "schwimm_amstueck", label: "Am Stück", unit: "m", baseline: 400, baselineDate: "2026-10-16" }]) }).value.metrics;
assert.deepEqual(m, [{ key: "schwimm_amstueck", label: "Am Stück", unit: "m", better: "higher", baseline: 400, baselineDate: "2026-10-16", current: null, currentDate: null }]);

const pr = { fetchedAt: "2026-10-22T06:00:00Z", updatedAt: "2026-10-20", highlights: [{ text: " Lange Läufe wirken ", strength: "Mittel" }] };
assert.deepEqual(validateProfil(pr).value, { fetchedAt: "2026-10-22T06:00:00.000Z", updatedAt: "2026-10-20", highlights: [{ text: "Lange Läufe wirken", strength: "mittel" }] });
assert.deepEqual(validateProfil({ ...pr, highlights: [] }).value.highlights, []);
for (const bad of [{ updatedAt: "heute" }, { highlights: null }, { highlights: Array(6).fill({ text: "x", strength: "mittel" }) },
  { highlights: [{ text: "x".repeat(141), strength: "mittel" }] }, { highlights: [{ text: "x", strength: "stark" }] }, { highlights: [{ text: " ", strength: "mittel" }] }])
  assert.ok(validateProfil({ ...pr, ...bad }).error, JSON.stringify(bad));

// Trainingstage: Do 22.10.2026 = KW 43 (laufende Woche); ab Mo 31.08. (KW 36) acht Wochen
const act = (d, o = {}) => ({ start_date_local: d + "T07:00:00", type: "Run", ...o });
const acts = [
  act("2026-10-19"), act("2026-10-19", { type: "Ride" }), act("2026-10-20", { type: "WeightTraining" }), act("2026-10-21", { icu_ignore_time: true }),
  act("2026-10-23"), // Zukunft zaehlt nicht
  act("2026-10-12"), act("2026-10-13"), act("2026-10-14", { type: "Swim" }), act("2026-10-16"), act("2026-10-18", { type: "WeightTraining" }),
  act("2026-08-30"), // vor dem Fenster
];
const td = computeTrainingDays(acts, "2026-10-22");
assert.equal(td.length, 8);
assert.deepEqual(td[0], { week: "2026-08-31", kw: 36, days: 0, complete: true });
assert.deepEqual(td[6], { week: "2026-10-12", kw: 42, days: 5, complete: true });
assert.deepEqual(td[7], { week: "2026-10-19", kw: 43, days: 2, complete: false });
// Jahreswechsel: Mo 28.12.2026 ist KW 53, Mo 04.01.2027 KW 1
assert.deepEqual(computeTrainingDays([], "2027-01-06").slice(-2).map((x) => x.kw), [53, 1]);
console.log("experiment tests ok");
