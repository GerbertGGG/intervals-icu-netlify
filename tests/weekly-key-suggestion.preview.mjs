import { getWeeklyKeySuggestion } from "../src/index.js";

const weeks = [40, 32, 24, 16, 8, 0];

const base = {
  distance: "5k",
  keyRules: { allowedKeyTypes: ["racepace"], bannedKeyTypes: [] },
  keyHardDecision: { allowed: true },
  keySpacing: { ok: true, nextAllowedIso: null },
  guardrailHardActive: false,
  decisionKeyType: "racepace",
  intensitySelection: { keyType: "racepace" },
  readinessTier: "MED",
  runfloorGap: false,
  driftWarning: false,
};

console.log("week,chosenTemplateId,progressionStep,reps,taperApplied,scalingLevel");
weeks.forEach((daysToRace, idx) => {
  const scalingLevel = daysToRace <= 16 ? -1 : 0;
  const suggestion = getWeeklyKeySuggestion({
    ...base,
    weekIndex: idx,
    previousStep: idx === 0 ? 0 : idx - 1,
    daysToRace,
    workoutDebug: {
      chosenTemplateId: "RP1",
      adjustedReps: idx < 3 ? 5 + idx : 5,
      adjustedRecSec: 120,
      scalingLevel,
    },
  });

  console.log([
    idx + 1,
    suggestion.templateId || "-",
    suggestion.progressionStep,
    suggestion.reps,
    suggestion.taperApplied,
    suggestion.scalingLevel,
  ].join(","));
});
