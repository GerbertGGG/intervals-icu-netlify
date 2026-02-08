import assert from "node:assert/strict";
import { computeIntervalMetricsFromStreams } from "../src/index.js";

function addSegment({ time, hr, speed, startTime, durationSec, startHr, endHr, speedValue }) {
  for (let i = 0; i < durationSec; i++) {
    time.push(startTime + i);
    const pct = durationSec > 1 ? i / (durationSec - 1) : 0;
    hr.push(startHr + (endHr - startHr) * pct);
    speed.push(speedValue);
  }
  return startTime + durationSec;
}

function buildIntervals({ reps, workSec, recSec, workSpeed, recSpeed, workHrStart, workHrPeak, recHrEnd }) {
  const time = [];
  const hr = [];
  const speed = [];
  let t = 0;
  for (let i = 0; i < reps; i++) {
    t = addSegment({
      time,
      hr,
      speed,
      startTime: t,
      durationSec: workSec,
      startHr: workHrStart,
      endHr: workHrPeak,
      speedValue: workSpeed,
    });
    t = addSegment({
      time,
      hr,
      speed,
      startTime: t,
      durationSec: recSec,
      startHr: workHrPeak,
      endHr: recHrEnd,
      speedValue: recSpeed,
    });
  }
  return { time, hr, speed };
}

function runTest(name, fn) {
  try {
    fn();
    console.log(`✓ ${name}`);
  } catch (error) {
    console.error(`✗ ${name}`);
    throw error;
  }
}

runTest("VO2-touch 5x2' yields HRR60 median and count", () => {
  const { time, hr, speed } = buildIntervals({
    reps: 5,
    workSec: 120,
    recSec: 120,
    workSpeed: 5,
    recSpeed: 2.5,
    workHrStart: 150,
    workHrPeak: 182,
    recHrEnd: 135,
  });

  const streams = { time, heartrate: hr, velocity_smooth: speed };
  const metrics = computeIntervalMetricsFromStreams(streams, {
    intervalType: "vo2",
    activity: { tags: ["key:vo2"] },
  });

  assert.ok(metrics);
  assert.equal(metrics.HRR60_count, 5);
  assert.ok(metrics.HRR60_median != null && metrics.HRR60_median > 0);
});

runTest("Short strides keep HRR60 null due to duration guardrail", () => {
  const { time, hr, speed } = buildIntervals({
    reps: 6,
    workSec: 20,
    recSec: 60,
    workSpeed: 6,
    recSpeed: 3,
    workHrStart: 145,
    workHrPeak: 170,
    recHrEnd: 135,
  });

  const streams = { time, heartrate: hr, velocity_smooth: speed };
  const metrics = computeIntervalMetricsFromStreams(streams, {
    intervalType: "vo2",
    activity: { tags: ["key:interval"] },
  });

  assert.ok(metrics);
  assert.equal(metrics.HRR60_count, 0);
  assert.equal(metrics.HRR60_median, null);
});

runTest("Threshold intervals yield consistent HRR60 values", () => {
  const { time, hr, speed } = buildIntervals({
    reps: 3,
    workSec: 360,
    recSec: 120,
    workSpeed: 4.2,
    recSpeed: 2.6,
    workHrStart: 155,
    workHrPeak: 175,
    recHrEnd: 140,
  });

  const streams = { time, heartrate: hr, velocity_smooth: speed };
  const metrics = computeIntervalMetricsFromStreams(streams, {
    intervalType: "threshold",
    activity: { tags: ["key:interval"] },
  });

  assert.ok(metrics);
  assert.equal(metrics.HRR60_count, 3);
  assert.ok(metrics.HRR60_min != null && metrics.HRR60_max != null);
});

runTest("Falling HR without plateau still computes HRR60", () => {
  const { time, hr, speed } = buildIntervals({
    reps: 2,
    workSec: 180,
    recSec: 180,
    workSpeed: 4.8,
    recSpeed: 2.4,
    workHrStart: 150,
    workHrPeak: 178,
    recHrEnd: 120,
  });

  const streams = { time, heartrate: hr, velocity_smooth: speed };
  const metrics = computeIntervalMetricsFromStreams(streams, {
    intervalType: "vo2",
    activity: { tags: ["key:vo2"] },
  });

  assert.ok(metrics);
  assert.equal(metrics.HRR60_count, 2);
});

runTest("HR dropouts invalidate affected intervals", () => {
  const { time, hr, speed } = buildIntervals({
    reps: 2,
    workSec: 120,
    recSec: 120,
    workSpeed: 5,
    recSpeed: 2.5,
    workHrStart: 150,
    workHrPeak: 180,
    recHrEnd: 130,
  });

  const intervalEnd = 120;
  for (let i = intervalEnd + 10; i < intervalEnd + 20; i++) {
    const idx = time.indexOf(i);
    if (idx >= 0) hr[idx] = null;
  }

  const streams = { time, heartrate: hr, velocity_smooth: speed };
  const metrics = computeIntervalMetricsFromStreams(streams, {
    intervalType: "vo2",
    activity: { tags: ["key:vo2"] },
  });

  assert.ok(metrics);
  assert.equal(metrics.HRR60_count, 1);
});
