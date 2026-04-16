import assert from "node:assert/strict";
import { __test } from "../src/index.js";

{
  const keyMode = __test.resolveDayModeFromKeyDecision({ allowKey: true });
  assert.equal(keyMode, "KEY");

  const why = __test.buildWhyNarrative(["Volumen stabil", "Key-Abstand erfüllt"], { dayMode: keyMode });
  assert.match(why, /^Heute KEY,/);
  assert.match(why, /Key freigegeben/i);
  assert.match(why, /kurzer, konservativer Reiz/i);
  assert.match(why, /ergänzt die aktuelle Spezifik/i);
  assert.doesNotMatch(why, /Volumen noch nicht stabil/i);

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

{
  const keyRecommendations = __test.prependKeyRecommendationContext(
    ["Volumen über die Woche graduell steigern.", "Longrun stabil halten."],
    { dayMode: "KEY" }
  );
  assert.match(keyRecommendations[0], /^Heute: kurzer KEY-Reiz; insgesamt bleibt Volumen Priorität\./);
  assert.match(keyRecommendations[1], /Volumen über die Woche/i);
}

console.log("day-mode copy consistency ok");
