import test from "node:test";
import assert from "node:assert/strict";
import { buildWidgetTests } from "../src/widget.js";

const t = (o) => ({ id: "a", title: "Test A", kind: "test", discipline: "lauf", status: "laeuft", load: "mehr", checkDate: "2026-11-15", metrics: [{ key: "lauf_hf130", label: "Lauf HF 130", unit: "min/km", better: "lower", baseline: 6.1, current: 5.9 }], adherence: { rule: "r", minPct: 80, actualPct: 70 }, ...o });

test("Tests-Widget: laufende vor Urteil, Kandidaten nur gezählt, Tage bis Prüfdatum", () => {
  const w = buildWidgetTests({ generatedAt: "t", today: "2026-11-05", tests: { tests: [t({}), t({ id: "b", status: "kandidat" }), t({ id: "c", status: "urteil", checkDate: null })] }, sources: {} });
  assert.deepEqual(w.counts, { laeuft: 1, kandidat: 1, urteil: 1, gelernt: 0 });
  assert.equal(w.heavy, 1);
  assert.deepEqual(w.tests.map((x) => x.id), ["a", "c"]);
  assert.equal(w.tests[0].daysToCheck, 10);
  assert.equal(w.tests[0].metric.current, 5.9);
  assert.equal(w.tests[0].adherence.actualPct, 70);
});

test("Tests-Widget ohne Tests: leere Liste", () => {
  const w = buildWidgetTests({ generatedAt: "t", today: "2026-11-05", tests: null, sources: {} });
  assert.deepEqual(w.tests, []);
});
