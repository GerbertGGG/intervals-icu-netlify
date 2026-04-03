import assert from "node:assert/strict";
import { __test } from "../src/index.js";

// Longrun must resolve to canonical progression target everywhere.
{
  const resolved = __test.buildResolvedSessionDecision({
    todaySessionType: "LONGRUN",
    todayDecisionCandidate: "Longrun wie im Wochenplan: Langer Lauf ~45′.",
    plannedSessionLabel: "Langer Lauf ~45′",
    longrunProgressionTargetMin: 50,
  });

  assert.equal(resolved.sessionType, "LONGRUN");
  assert.equal(resolved.longrunTargetMin, 50);
  assert.equal(resolved.todayDecision, "Longrun 50′");

  const report = [
    "🏃 HEUTE",
    `${resolved.todayDecision}.`,
    "⸻",
    "",
    "🗓 WOCHENPLAN",
    `→ Fr: Langer Lauf ~${resolved.longrunTargetMin}′`,
    "⸻",
    "",
    "🧾 BOTTOM LINE",
    "Heute Longrun sauber und kontrolliert absolvieren.",
  ].join("\n");

  assert.doesNotThrow(() => __test.validateResolvedDecisionRenderConsistency(report, resolved));
}

// Non-Longrun days must not keep stale Longrun labeling in today decision.
{
  const resolved = __test.buildResolvedSessionDecision({
    todaySessionType: "GA",
    todayDecisionCandidate: "Longrun wie im Wochenplan: Langer Lauf ~45′.",
    plannedSessionLabel: "easy / frei (30–60′ locker oder Ruhetag nach Gefühl)",
    longrunProgressionTargetMin: 50,
  });

  assert.equal(resolved.sessionType, "GA");
  assert.match(resolved.todayDecision, /Locker\/GA wie im Wochenplan/i);
  assert.doesNotMatch(resolved.todayDecision, /Longrun|Langer Lauf/i);
}

console.log("daily render structural consistency ok");
