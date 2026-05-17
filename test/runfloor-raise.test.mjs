import assert from "node:assert/strict";
import { __test } from "../src/index.js";

function isoOffset(baseIso, days) {
  const ts = new Date(baseIso + "T00:00:00Z").getTime() + days * 86400000;
  return new Date(ts).toISOString().slice(0, 10);
}

function evaluate({
  phase = "BASE",
  floorTarget = 80,
  eventInDays = 40,
  previousState = {},
  dailyRunLoads = Array(21).fill(20),
  lifeEventEffect = { active: false, freezeFloorIncrease: false, category: null },
} = {}) {
  return __test.evaluateRunFloorState({
    todayISO: "2026-03-24",
    floorTarget,
    phase,
    eventInDays,
    eventDistance: "10k",
    eventDateISO: null,
    previousState,
    dailyRunLoads,
    lifeEventEffect,
    recentHolidayEvent: null,
  });
}

// 1) FULL Raise in BASE bei sauberem NORMAL-Fall
{
  const out = evaluate({
    phase: "BASE",
    floorTarget: 80,
    previousState: {
      floorTarget: 80,
      lastFloorIncreaseDate: "2026-03-01",
      lastDeloadCompletedISO: "2026-03-10",
    },
  });
  assert.equal(out.floorRaiseMode, "FULL");
  assert.equal(out.floorRaised, true);
  assert.equal(out.floorRaiseStep, 8);
  assert.equal(out.floorTarget, 88);
}

// 2) FULL Raise in BUILD bei sauberem NORMAL-Fall
{
  const out = evaluate({
    phase: "BUILD",
    floorTarget: 120,
    previousState: {
      floorTarget: 120,
      lastFloorIncreaseDate: "2026-03-01",
      lastDeloadCompletedISO: "2026-03-12",
    },
  });
  assert.equal(out.floorRaiseMode, "FULL");
  assert.equal(out.floorRaiseStep, 12);
  assert.equal(out.floorTarget, 132);
}

// 3) SOFT Raise trotz nicht perfektem, aber stabilem Zustand (YELLOW)
{
  const out = evaluate({
    phase: "BASE",
    floorTarget: 70, // floorDaily=10
    previousState: {
      floorTarget: 70,
      lastFloorIncreaseDate: "2026-03-10",
      lastDeloadCompletedISO: null,
    },
    dailyRunLoads: Array(21).fill(9.4), // BASE soft-dip yellow (>= 9.3)
  });
  assert.equal(out.floorLevel, "YELLOW");
  assert.equal(out.floorRaiseMode, "SOFT");
  assert.equal(out.floorRaiseStep, 6); // round(70 * 0.08) = round(5.6) = 6
  assert.equal(out.floorTarget, 76);
}

// 4) BLOCK bei RED
{
  const out = evaluate({
    phase: "BASE",
    floorTarget: 90,
    previousState: { floorTarget: 90, lastFloorIncreaseDate: "2026-03-01" },
    dailyRunLoads: Array(21).fill(0),
  });
  assert.equal(out.floorLevel, "RED");
  assert.equal(out.floorRaiseMode, "BLOCK");
  assert.equal(out.floorRaised, false);
  assert.equal(out.floorTarget, 90);
}

// 5) BLOCK bei daysToEvent <= 14
{
  const out = evaluate({
    phase: "BASE",
    floorTarget: 80,
    eventInDays: 14,
    previousState: {
      floorTarget: 80,
      lastFloorIncreaseDate: "2026-03-01",
      lastDeloadCompletedISO: "2026-03-10",
    },
  });
  assert.equal(out.floorRaiseMode, "BLOCK");
  assert.equal(out.floorRaised, false);
}

// 6) BLOCK bei sick/injured
{
  const out = evaluate({
    phase: "BASE",
    floorTarget: 80,
    lifeEventEffect: {
      active: true,
      category: "SICK",
      runFloorFactor: 0,
      freezeFloorIncrease: true,
      overlayMode: "LIFE_EVENT_STOP",
    },
  });
  assert.equal(out.floorRaiseMode, "BLOCK");
  assert.equal(out.floorRaised, false);
}

// 7) BLOCK bei POST_RACE
{
  const out = evaluate({
    phase: "BASE",
    floorTarget: 80,
    eventInDays: 200,
    previousState: {
      floorTarget: 80,
      lastEventDate: isoOffset("2026-03-24", -1),
      lastFloorIncreaseDate: "2026-03-01",
      lastDeloadCompletedISO: "2026-03-10",
    },
  });
  assert.equal(out.overlayMode, "POST_RACE_RAMP");
  assert.equal(out.floorRaiseMode, "BLOCK");
}

// 8) SOFT ohne deloadCompletedSinceLastRaise
{
  const out = evaluate({
    phase: "BUILD",
    floorTarget: 100,
    previousState: {
      floorTarget: 100,
      lastFloorIncreaseDate: "2026-03-10",
      lastDeloadCompletedISO: null,
    },
  });
  assert.equal(out.floorRaiseMode, "SOFT");
  assert.equal(out.floorRaiseStep, 8);
  assert.equal(out.floorTarget, 108);
}

// 9) FULL nur mit deloadCompletedSinceLastRaise
{
  const withoutDeloadCompletion = evaluate({
    phase: "BUILD",
    floorTarget: 100,
    previousState: {
      floorTarget: 100,
      lastFloorIncreaseDate: "2026-03-10",
      lastDeloadCompletedISO: "2026-03-05",
    },
  });
  assert.equal(withoutDeloadCompletion.floorRaiseMode, "SOFT");

  const withDeloadCompletion = evaluate({
    phase: "BUILD",
    floorTarget: 100,
    previousState: {
      floorTarget: 100,
      lastFloorIncreaseDate: "2026-03-10",
      lastDeloadCompletedISO: "2026-03-20",
    },
  });
  assert.equal(withDeloadCompletion.floorRaiseMode, "FULL");
}

// 10) FULL = immer 10% (kleiner Floor: round(30*0.10)=3)
{
  const out = evaluate({
    phase: "BUILD",
    floorTarget: 30,
    previousState: {
      floorTarget: 30,
      lastFloorIncreaseDate: "2026-03-01",
      lastDeloadCompletedISO: "2026-03-10",
    },
  });
  assert.equal(out.floorRaiseMode, "FULL");
  assert.equal(out.floorRaiseStep, 3); // round(30 * 0.10) = 3
  assert.equal(out.floorTarget, 33);
}

// 11) SOFT = immer 8% (kleiner Floor: round(30*0.08)=2)
{
  const out = evaluate({
    phase: "BUILD",
    floorTarget: 30,
    previousState: {
      floorTarget: 30,
      lastFloorIncreaseDate: "2026-03-10",
      lastDeloadCompletedISO: null,
    },
  });
  assert.equal(out.floorRaiseMode, "SOFT");
  assert.equal(out.floorRaiseStep, 2); // round(30 * 0.08) = round(2.4) = 2
  assert.equal(out.floorTarget, 32);
}

// 13) BLOCK wenn SOFT-Raise < 7 Tage nach letzter Erhöhung
{
  const out = evaluate({
    phase: "BUILD",
    floorTarget: 100,
    previousState: {
      floorTarget: 100,
      lastFloorIncreaseDate: "2026-03-20", // nur 4 Tage vor todayISO
      lastDeloadCompletedISO: null,
    },
  });
  assert.equal(out.floorRaiseMode, "BLOCK");
  assert.equal(out.floorRaised, false);
}

// 12) lastFloorIncreaseDate wird nur bei echter Erhöhung gesetzt
{
  const oldDate = "2026-03-20";
  const blocked = evaluate({
    phase: "BASE",
    floorTarget: 90,
    eventInDays: 10,
    previousState: {
      floorTarget: 90,
      lastFloorIncreaseDate: oldDate,
    },
  });
  assert.equal(blocked.floorRaised, false);
  assert.equal(blocked.lastFloorIncreaseDate, oldDate);

  const raised = evaluate({
    phase: "BASE",
    floorTarget: 90,
    previousState: {
      floorTarget: 90,
      lastFloorIncreaseDate: oldDate,
      lastDeloadCompletedISO: "2026-03-22",
    },
  });
  assert.equal(raised.floorRaised, true);
  assert.equal(raised.lastFloorIncreaseDate, "2026-03-24");
}
