import assert from "node:assert/strict";
import { __test } from "../src/index.js";

{
  const keyMode = __test.resolveDayModeFromKeyDecision({ allowKey: true });
  assert.equal(keyMode, "KEY");

  const why = __test.buildWhyNarrative(["Volumen stabil", "Key-Abstand erfüllt"], { dayMode: keyMode });
  assert.match(why, /^Heute KEY,/);
  assert.doesNotMatch(why, /nächsten sinnvollen Reiz vor/i);

  const bottom = __test.resolveBottomLine({
    candidate: "",
    todayDecision: "Key heute: 3×8′ @ Schwelle.",
    dayMode: keyMode,
  });
  assert.match(bottom, /KEY-Reiz/i);
  assert.doesNotMatch(bottom, /nächsten Qualitätsreiz/i);
}

{
  const lowMode = __test.resolveDayModeFromKeyDecision({ allowKey: false });
  assert.equal(lowMode, "LOW");

  const why = __test.buildWhyNarrative(["Volumen noch nicht stabil"], { dayMode: lowMode });
  assert.match(why, /^Heute kontrolliert,/);

  const bottom = __test.resolveBottomLine({
    candidate: "",
    todayDecision: "30–45 min locker.",
    dayMode: lowMode,
  });
  assert.match(bottom, /nächsten Qualitätsreiz/i);
}

{
  const next = __test.buildNextRunRecommendation({
    keyDecision: { allowKey: true },
    keyAllowedNow: true,
    keySpacingOk: true,
    explicitSession: "Strides konkret: 4–6×8–10″ (Sekunden) Hill Sprints, volle 2–3′ Pause.",
  });
  assert.match(next, /8–10″ \(Sekunden\)/);
}

console.log("day-mode copy consistency ok");
