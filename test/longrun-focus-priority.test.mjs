import assert from "node:assert/strict";
import { __test } from "../src/index.js";

// Wenn Key wegen Spacing gesperrt ist, aber Longrun-Fokus aktiv ist,
// soll aktiv der Longrun empfohlen werden (statt pauschalem LOW/Key-Spacer).
{
  const next = __test.buildNextRunRecommendation({
    runFloorState: { overlayMode: "NORMAL" },
    keySpacingOk: false,
    keyAllowedNow: true,
    keyDecision: { allowKey: false, blockedByKeySpacing: true, blockedByLongrunSpacing: false },
    nextAllowed: "2026-04-20",
    plannedSessionType: "LONGRUN",
    plannedSessionLabel: "Langer Lauf ca. 45′",
    longRunTargetMin: 47,
  });

  assert.equal(next, "Longrun wie im Wochenplan: Langer Lauf ca. 47′.");
  assert.equal(/Key-Spacer|Heute kein Key/i.test(next), false);
}

console.log("longrun focus priority ok");
