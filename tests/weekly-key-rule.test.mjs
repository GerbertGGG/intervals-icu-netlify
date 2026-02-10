import assert from "node:assert/strict";
import { getWeeklyKeySuggestion } from "../src/index.js";

const baseContext = {
  distance: "10k",
  dayIso: "2026-02-10",
  keyRules: {
    allowedKeyTypes: ["racepace"],
    preferredKeyTypes: ["racepace"],
    bannedKeyTypes: ["vo2_touch"],
  },
  intensitySelection: { keyType: "vo2_touch" },
  decisionKeyType: "vo2_touch",
  keySpacing: { ok: true, nextAllowedIso: null },
  keyHardDecision: { allowed: true, reason: "Key-Hard erlaubt" },
  guardrailHardActive: true,
  runfloorGap: true,
  driftWarning: true,
  negativeSignals: ["hrv_down"],
  workoutDebug: {
    chosenTemplateId: "RP1",
    adjustedReps: 4,
    adjustedRecSec: 120,
    scalingLevel: -1,
  },
};

{
  const suggestion = getWeeklyKeySuggestion(baseContext);
  assert.ok(!suggestion.keyLabel.startsWith("kein Key"), "Key muss vorgeschlagen werden, wenn 48h + <2/7T erfüllt sind.");
  assert.equal(suggestion.keyType, "racepace", "Nicht erlaubter Key-Typ darf nur skaliert, nicht unterdrückt werden.");
}

{
  const suggestion = getWeeklyKeySuggestion({
    ...baseContext,
    keySpacing: { ok: false, nextAllowedIso: "2026-02-12" },
  });
  assert.ok(suggestion.keyLabel.startsWith("kein Key"), "Bei Abstand <48h darf kein Key vorgeschlagen werden.");
}

{
  const suggestion = getWeeklyKeySuggestion({
    ...baseContext,
    keyHardDecision: { allowed: false, reason: "KEY_HARD-Limit (2/7T) erreicht." },
  });
  assert.ok(suggestion.keyLabel.startsWith("kein Key"), "Bei >=2 Keys in 7 Tagen darf kein Key vorgeschlagen werden.");
}

console.log("weekly-key-rule.test.mjs: ok");
