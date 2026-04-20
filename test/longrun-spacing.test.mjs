import assert from "node:assert/strict";
import { __test } from "../src/index.js";

// 1) Kein echter Longrun (nur lange GA/Key-Läufe) -> kein Spacer
{
  const ctx = {
    activitiesAll: [
      { id: "run-ga-1", type: "Run", start_date_local: "2026-04-15T07:00:00Z", moving_time: 80 * 60, tags: [] },
      { id: "run-key-1", type: "Run", start_date_local: "2026-04-16T07:00:00Z", moving_time: 75 * 60, tags: ["key:steady"] },
    ],
  };
  const lastLongrun = __test.findLastTrueLongrunActivity(ctx, "2026-04-17");
  assert.equal(lastLongrun.found, false);
  assert.equal(lastLongrun.reason, "kein_longrun_gefunden");

  const decision = __test.evaluateDayBasedKeyDecision({
    dayIso: "2026-04-17",
    keyAllowedNow: true,
    lastKeyIso: "2026-04-14",
    lastLongrun,
  });
  assert.equal(decision.daysSinceLastLongrun, null);
  assert.equal(decision.blockedByLongrunSpacing, false);
  assert.equal(decision.lastLongrunFound, false);
  assert.equal(decision.lastLongrunReason, "kein_longrun_gefunden");
}

// 1b) Kein Lauf >= LONGRUN_MIN_SECONDS im 14T-Fenster -> expliziter 14T-Grund
{
  const ctx = {
    activitiesAll: [
      { id: "easy-1", type: "Run", start_date_local: "2026-04-15T07:00:00Z", moving_time: 45 * 60, tags: [] },
      { id: "easy-2", type: "Run", start_date_local: "2026-04-16T07:00:00Z", moving_time: 50 * 60, tags: [] },
    ],
  };
  const lastLongrun = __test.findLastTrueLongrunActivity(ctx, "2026-04-17");
  assert.equal(lastLongrun.found, false);
  assert.equal(lastLongrun.reason, "kein_longrun_14t");
}

// 2) Echter Longrun gestern -> Spacer aktiv
{
  const ctx = {
    activitiesAll: [
      { id: "lr-1", type: "Run", start_date_local: "2026-04-16T06:30:00Z", moving_time: 95 * 60, tags: ["key:longrun"] },
    ],
  };
  const lastLongrun = __test.findLastTrueLongrunActivity(ctx, "2026-04-17");
  assert.equal(lastLongrun.found, true);
  assert.equal(lastLongrun.activityId, "lr-1");

  const decision = __test.evaluateDayBasedKeyDecision({
    dayIso: "2026-04-17",
    keyAllowedNow: true,
    lastLongrun,
  });
  assert.equal(decision.daysSinceLastLongrun, 1);
  assert.equal(decision.blockedByLongrunSpacing, true);
  assert.equal(decision.lastLongrunReason, "key_type_longrun");
}

// 3) daysSinceLastLongrun=0 nur wenn heute echter Longrun
{
  const ctx = {
    activitiesAll: [
      { id: "today-lr", type: "Run", start_date_local: "2026-04-17T06:30:00Z", moving_time: 90 * 60, tags: ["key:longrun"] },
    ],
  };
  const lastLongrun = __test.findLastTrueLongrunActivity(ctx, "2026-04-17");
  const decision = __test.evaluateDayBasedKeyDecision({
    dayIso: "2026-04-17",
    keyAllowedNow: true,
    lastLongrun,
  });
  assert.equal(decision.daysSinceLastLongrun, 0);
  assert.equal(decision.lastLongrunDate, "2026-04-17");
}

console.log("longrun spacing guardrails ok");
