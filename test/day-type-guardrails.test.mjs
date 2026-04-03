import assert from "node:assert/strict";
import { __test } from "../src/index.js";

// 1) LONGRUN-Tag + explicitSession vorhanden + keyAllowedNow=false → kein Key-Text in HEUTE/BOTTOM LINE
{
  const ctx = __test.resolveTodaySessionContext(
    { sessionType: "LONGRUN" },
    {
      keyAllowedNow: false,
      hasConcreteKeySession: true,
      explicitSession: "steady konkret: 2×8–10′ steady, 3′ locker.",
    },
    "2026-04-04"
  );
  assert.equal(ctx.isTodayKey, false);
  assert.equal(ctx.todayCanUseExplicitKeySession, false);

  const todayLine = __test.buildNextRunRecommendation({
    keyAllowedNow: false,
    keySpacingOk: false,
    nextAllowed: "2026-04-04",
    keySuggestion: "Nächster Key frühestens 2026-04-04",
    explicitSession: null,
    plannedSessionType: "LONGRUN",
    plannedSessionLabel: "Langer Lauf ~45′",
  });
  assert.match(todayLine, /Longrun wie im Wochenplan/i);
  assert.doesNotMatch(todayLine, /Key wie im Wochenplan|2×8–10′ steady/i);

  const bottom = __test.resolveBottomLine({ todaySessionType: "LONGRUN" });
  assert.equal(bottom, "Heute Longrun sauber und kontrolliert absolvieren.");
}

// 2) KEY-Tag + explicitSession vorhanden + keyAllowedNow=true → Key-Text erscheint konsistent
{
  const ctx = __test.resolveTodaySessionContext(
    { sessionType: "KEY" },
    {
      keyAllowedNow: true,
      hasConcreteKeySession: true,
      explicitSession: "steady konkret: 2×8–10′ steady, 3′ locker.",
    },
    null
  );
  assert.equal(ctx.todayCanUseExplicitKeySession, true);

  const todayLine = __test.buildNextRunRecommendation({
    keyAllowedNow: true,
    keySpacingOk: true,
    keySuggestion: "Nächster Key: steady",
    explicitSession: "steady konkret: 2×8–10′ steady, 3′ locker.",
    plannedSessionType: "KEY",
    plannedSessionLabel: "Key: steady",
  });
  assert.match(todayLine, /Key wie im Wochenplan/i);
  assert.match(todayLine, /2×8–10′ steady/i);

  const bottom = __test.resolveBottomLine({
    todaySessionType: "KEY",
    explicitSessionShort: "steady konkret: 2×8–10′ steady, 3′ locker",
  });
  assert.match(bottom, /^Key heute:/);
}

// 3) LOW-Tag + pendingLever vorhanden → nur Zukunftshinweis, keine heutige Key-Ausführung
{
  const ctx = __test.resolveTodaySessionContext(
    { sessionType: "LOW" },
    {
      keyAllowedNow: false,
      hasConcreteKeySession: true,
      explicitSession: "steady konkret: 2×8–10′ steady, 3′ locker.",
    },
    "2026-04-04"
  );
  assert.equal(ctx.isTodayLow, true);
  assert.equal(ctx.todayCanUseExplicitKeySession, false);
  assert.equal(ctx.nextKeyHint, "Nächster Key frühestens ab 2026-04-04.");

  const bottom = __test.resolveBottomLine({ todaySessionType: "LOW" });
  assert.equal(bottom, "Heute locker bleiben / Erholung und Volumen sauber absichern.");
}

console.log("day-type guardrails ok");
