import assert from "node:assert/strict";
import { __test } from "../src/index.js";

{
  const steadyContinuous = __test.normalizeKeyTypeMeta("steady_continuous", {
    activity: {
      name: "Steady Dauerlauf",
      tags: ["#key:steady_continuous"],
      icu_intervals: [
        { type: "WORK", moving_time: 1800, distance: 5000, average_speed: 3.5 },
      ],
    },
  });
  assert.equal(steadyContinuous.keyType, "steady");
  assert.equal(steadyContinuous.keySubtype, "continuous");
  assert.equal(__test.keyImpliesIntervalSignal(steadyContinuous), false);
  assert.equal(__test.shouldSearchIntervalsForRun({
    isKey: true,
    keyMeta: steadyContinuous,
    activity: {
      name: "Steady 3x10 min mit Pause",
      icu_intervals: [
        { type: "WORK", moving_time: 600, distance: 2000, average_speed: 3.4 },
        { type: "RECOVERY", moving_time: 120, distance: 250, average_speed: 2.0 },
      ],
    },
  }), false);
}

{
  const steadyIntervals = __test.normalizeKeyTypeMeta("steady_intervals", {
    activity: {
      name: "Steady 3x10 min",
      tags: ["#key:steady_intervals"],
    },
  });
  assert.equal(steadyIntervals.keyType, "steady");
  assert.equal(steadyIntervals.keySubtype, "intervals");
  assert.equal(__test.keyImpliesIntervalSignal(steadyIntervals), true);
  assert.equal(__test.shouldSearchIntervalsForRun({
    isKey: true,
    keyMeta: steadyIntervals,
    activity: { name: "Steady ohne Intervalldaten" },
  }), true);
}

console.log("steady continuous interval signal ok");
