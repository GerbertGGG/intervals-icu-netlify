import assert from "node:assert/strict";
import { __test } from "../src/index.js";

{
  const keyMode = __test.resolveDayModeFromKeyDecision({ allowKey: true });
  assert.equal(keyMode, "KEY");

  const why = __test.buildWhyNarrative(["Volumen stabil", "Key-Abstand erfüllt"], {
    dayMode: keyMode,
    fatigueOverride: true,
    fatigueReasons: ["Ramp: 203% vs vorherige 7 Tage"],
  });
  assert.match(why, /^Heute KEY,/);
  assert.match(why, /Key freigegeben/i);
  assert.match(why, /Fatigue-Signal aktiv \(Ramp: 203% vs vorherige 7 Tage\), aber Key-Freigabe bleibt bestehen/i);
  assert.match(why, /kurzer, konservativer Reiz/i);
  assert.match(why, /ergänzt die aktuelle Spezifik/i);
  assert.doesNotMatch(why, /Volumen noch nicht stabil/i);

  const bottom = __test.resolveBottomLine({
    candidate: "",
    todayDecision: "Key heute: 3×8′ @ Schwelle.",
    dayMode: keyMode,
  });
  assert.match(bottom, /regulären Qualitätsreiz/i);
  assert.doesNotMatch(bottom, /nächsten Qualitätsreiz/i);
}

{
  const lowMode = __test.resolveDayModeFromKeyDecision({ allowKey: false });
  assert.equal(lowMode, "LOW");

  const why = __test.buildWhyNarrative(["Volumen noch nicht stabil"], {
    dayMode: lowMode,
    fatigueOverride: true,
    fatigueReasons: ["Ramp: 203% vs vorherige 7 Tage"],
  });
  assert.match(why, /^Heute kontrolliert,/);
  assert.doesNotMatch(why, /Fatigue-Signal aktiv/i);

  const bottom = __test.resolveBottomLine({
    candidate: "",
    todayDecision: "30–45 min locker.",
    dayMode: lowMode,
  });
  assert.match(bottom, /nächsten Qualitätsreiz/i);
}

{
  const keyDay = __test.buildRecommendationsAndBottomLine({
    dayMode: "KEY",
    keyAllowedNow: true,
    todayAction: "Key heute: 3×8′ @ Schwelle.",
    fatigue: { override: true, reasons: ["Ramp: 203% vs vorherige 7 Tage"] },
  });
  assert.match(
    keyDay.recommendations.join(" "),
    /Fatigue-Override aktiv → Umfang nach dem Key niedrig halten, kein zweiter harter Reiz heute\./i
  );
  assert.doesNotMatch(keyDay.recommendations.join(" "), /Evidenz: Fatigue-Override aktiv/i);
}

{
  const decision = __test.evaluateDayBasedKeyDecision({
    dayIso: "2026-04-21",
    keyAllowedNow: true,
    lastKeyIso: "2026-04-16",
    lastLongrun: { found: false, reason: "kein_longrun_14t" },
    fatigueOverride: true,
    fatigueGuard: "downscale",
  });
  assert.equal(decision.reason, "fatigue_guard_downscale");
  assert.equal(decision.finalDecision, "LOW");
}

{
  const next = __test.buildNextRunRecommendation({
    hasSpecific: true,
    specificOk: false,
    keySpacingOk: true,
    keyMode: "light",
    keyDecision: { allowKey: false, blockedByFatigue: false },
  });
  assert.match(next, /kontrolliert steady|Qualitätsanteil/i);
  assert.doesNotMatch(next, /^35–50 min locker\/steady/i);
}

{
  const bottom = __test.resolveBottomLine({
    candidate: "",
    todayDecision: "40–55 min locker mit 10–15′ kontrolliert steady.",
    dayMode: "LOW",
    keyMode: "light",
  });
  assert.match(bottom, /leichten Qualitätsreiz/i);
  assert.doesNotMatch(bottom, /nächsten Qualitätsreiz sauber vorbereiten/i);
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
  const next = __test.buildNextRunRecommendation({
    keyDecision: { allowKey: true },
    keyAllowedNow: true,
    keySpacingOk: true,
    block: "BASE",
    explicitSession: null,
    allowedKeyTypes: ["steady", "strides"],
    preferredKeyTypes: ["steady"],
    plannedKeyType: "steady",
  });
  assert.match(next, /^Key heute:/i);
  assert.doesNotMatch(next, /\bga\b|locker|kontrolliert/i);
  assert.match(next, /strides|x|×|@|interval|hill sprint/i);
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
