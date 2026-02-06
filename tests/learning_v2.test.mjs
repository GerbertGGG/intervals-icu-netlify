import assert from "node:assert/strict";
import {
  applyTextGate,
  buildLearningNarrative,
  computeLearningEvidence,
  computeLearningStats,
  decayWeight,
  deriveContextKey,
  deriveStrategyArm,
  formatContextSummary,
  normalizeOutcomeClass,
} from "../src/index.js";

function isoDaysAgo(days) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - days);
  return d.toISOString().slice(0, 10);
}

function makeEvent({
  day,
  strategyArm = "HOLD_ABSORB",
  outcomeClass = "GOOD",
  contextKey = "RFgap=F|stress=LOW|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW",
  learningEligible = true,
} = {}) {
  return {
    day,
    strategyArm,
    outcomeClass,
    contextKey,
    learningEligible,
  };
}

// deriveStrategyArm priority: red flag wins, run floor gap stress next
{
  const res = deriveStrategyArm({
    runFloorGap: true,
    lifeStress: "HIGH",
    hrvState: "LOW",
    driftState: "BAD",
    hadKey: true,
    freqNotRed: true,
    highMonotony: true,
    fatigueHigh: true,
    hasHardRedFlag: true,
  });
  assert.equal(res.strategyArm, "NEUTRAL");
  assert.equal(res.learningEligible, false);
}

{
  const res = deriveStrategyArm({
    runFloorGap: true,
    lifeStress: "HIGH",
    hrvState: "NORMAL",
    driftState: "OK",
    hadKey: false,
    freqNotRed: true,
    highMonotony: false,
    fatigueHigh: false,
    hasHardRedFlag: false,
  });
  assert.equal(res.strategyArm, "FREQ_UP");
  assert.equal(res.policyReason, "RUN_FLOOR_GAP_HIGH_STRESS");
}

// deriveContextKey stable buckets
{
  const key = deriveContextKey({
    runFloorGap: true,
    fatigueOverride: true,
    warningCount: 2,
    hrvDeltaPct: -10,
    driftSignal: "orange",
    recoverySignals: { sleepLow: true, sleepDeltaPct: -12 },
    monotony: 3,
  });
  assert.equal(
    key,
    "RFgap=T|stress=HIGH|hrv=LOW|drift=WARN|sleep=LOW|mono=HIGH"
  );
}

// Posterior mapping for GOOD/NEUTRAL/BAD
{
  const day = isoDaysAgo(0);
  const stats = computeLearningStats([
    makeEvent({ day, outcomeClass: "GOOD", strategyArm: "FREQ_UP" }),
    makeEvent({ day, outcomeClass: "NEUTRAL", strategyArm: "FREQ_UP" }),
    makeEvent({ day, outcomeClass: "BAD", strategyArm: "FREQ_UP" }),
  ], day);
  const arm = stats.armStats.FREQ_UP;
  assert.ok(arm.goodPosterior > 0.3);
  assert.ok(arm.badPosterior > 0.3);
}

// Recency weights: newer event should weigh more
{
  const endDay = isoDaysAgo(0);
  const oldDay = isoDaysAgo(30);
  const newDay = isoDaysAgo(1);
  const oldWeight = decayWeight(oldDay, endDay, 45);
  const newWeight = decayWeight(newDay, endDay, 45);
  assert.ok(newWeight > oldWeight);
}

// Recommendation fallback to global context
{
  const endDay = isoDaysAgo(0);
  const otherContext = "RFgap=F|stress=LOW|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW";
  const sparseContext = "RFgap=T|stress=HIGH|hrv=LOW|drift=BAD|sleep=LOW|mono=HIGH";
  const evidence = computeLearningEvidence([
    makeEvent({ day: endDay, strategyArm: "HOLD_ABSORB", outcomeClass: "GOOD", contextKey: otherContext }),
    makeEvent({ day: endDay, strategyArm: "PROTECT_DELOAD", outcomeClass: "BAD", contextKey: otherContext }),
  ], endDay, sparseContext);
  assert.equal(evidence.contextKey, "ALL");
  assert.ok(evidence.recommendation.globalFallback);
}

// Narrative includes context summary and confidence
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=T|stress=HIGH|hrv=LOW|drift=BAD|sleep=LOW|mono=HIGH";
  const evidence = computeLearningEvidence([
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey }),
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey }),
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey }),
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "NEUTRAL", contextKey }),
  ], endDay, contextKey);
  const narrative = buildLearningNarrative(evidence);
  assert.ok(narrative.includes("n_eff="));
  assert.ok(narrative.includes("Confidence"));
  assert.ok(narrative.includes("RunFloorGap"));
}

// Arm ohne Daten darf nicht empfohlen werden (exploreUntried=false)
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=F|stress=LOW|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW";
  const evidence = computeLearningEvidence([
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "BAD", contextKey }),
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "BAD", contextKey }),
  ], endDay, contextKey);
  assert.equal(evidence.recommendation.strategyArm, "FREQ_UP");
}

// nEffTotal groß + untried Arm (nEff=0) -> Empfehlung bleibt auf Arm mit Daten
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=T|stress=MED|hrv=LOW|drift=WARN|sleep=LOW|mono=HIGH";
  const events = Array.from({ length: 6 }, () =>
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "BAD", contextKey })
  );
  const evidence = computeLearningEvidence(events, endDay, contextKey);
  assert.equal(evidence.recommendation.strategyArm, "FREQ_UP");
  assert.ok(evidence.recommendation.nEffTotal > 0);
  assert.ok(evidence.recommendation.nEffArm > 0);
}

// Confidence mit n_eff > 0 ist > 0 und gerundet (nicht floor)
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=T|stress=MED|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW";
  const events = Array.from({ length: 10 }, () =>
    makeEvent({ day: endDay, strategyArm: "HOLD_ABSORB", outcomeClass: "GOOD", contextKey })
  );
  const evidence = computeLearningEvidence(events, endDay, contextKey);
  const confPct = Math.round((evidence.recommendation.confidenceArm || 0) * 100);
  assert.ok(confPct > 0);
  assert.ok(confPct >= 60);
}

// explorationNeed true wenn <2 Arms Daten haben, aber Confidence bleibt > 0
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=F|stress=LOW|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW";
  const evidence = computeLearningEvidence([
    makeEvent({ day: endDay, strategyArm: "HOLD_ABSORB", outcomeClass: "GOOD", contextKey }),
    makeEvent({ day: endDay, strategyArm: "HOLD_ABSORB", outcomeClass: "GOOD", contextKey }),
  ], endDay, contextKey);
  assert.ok(evidence.recommendation.explorationNeed);
  assert.ok(evidence.recommendation.confidenceArm > 0);
}

// OutcomeScore Mapping (>=2 GOOD, ==1 NEUTRAL, <=0 BAD)
{
  assert.equal(normalizeOutcomeClass(null, 2, null), "GOOD");
  assert.equal(normalizeOutcomeClass(null, 1, null), "NEUTRAL");
  assert.equal(normalizeOutcomeClass(null, 0, null), "BAD");
}

// formatContextSummary: max 3 Tags, ALL/LEGACY korrekt
{
  assert.equal(formatContextSummary("LEGACY"), "Legacy-Kontext");
  assert.equal(formatContextSummary("ALL"), "globaler Kontext");
  const summary = formatContextSummary("RFgap=T|stress=HIGH|hrv=LOW|drift=WARN|sleep=LOW|mono=HIGH");
  assert.ok(summary.split(",").length === 3);
  assert.ok(summary.includes("RunFloorGap"));
}

// RedFlag learningEligible=false wird aus Aggregation ausgeschlossen
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=F|stress=LOW|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW";
  const evidence = computeLearningEvidence([
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey, learningEligible: false }),
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey, learningEligible: true }),
  ], endDay, contextKey);
  assert.equal(evidence.sampleCount, 1);
  assert.equal(evidence.redFlagCount, 1);
}

// T1) Nur ein Arm mit Daten -> kein Vergleich, Level 1 Text
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=F|stress=LOW|hrv=NORMAL|drift=OK|sleep=OK|mono=LOW";
  const events = Array.from({ length: 4 }, () =>
    makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey })
  );
  const evidence = computeLearningEvidence(events, endDay, contextKey);
  const narrative = buildLearningNarrative(evidence);
  assert.ok(!narrative.includes("robuster als"));
  assert.ok(!narrative.includes("besser als"));
  assert.ok(!narrative.includes("als die Alternativen"));
  assert.ok(narrative.includes("gut verträglich gezeigt"));
  assert.ok(narrative.includes("zu wenig getestet, um einen Vergleich zu ziehen"));
}

// T1b) Text-Gate rewrite for comparisons when only one arm has data
{
  const gated = applyTextGate(
    "Wir entscheiden uns heute für Häufigkeit statt Tempo, weil du auf kumulierten Stress robuster reagierst als auf Intensität.",
    {
      recommendedArm: "FREQ_UP",
      nEffRec: 19.3,
      nEffSecond: 0,
      nArmsWithData: 1,
      confidenceRec: 0.76,
      confidenceContext: 0.76,
      explorationNeed: false,
      policyReason: "RUN_FLOOR_GAP_HIGH_STRESS",
    }
  );
  assert.ok(!gated.includes("robuster als"));
  assert.ok(gated.includes("Häufigkeit statt Tempo"));
  assert.ok(gated.includes("Runload unter dem Ziel"));
}

// T2) Zwei Arme mit Daten -> vorsichtiger Vergleich mit Hedge (Level 2)
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=T|stress=MED|hrv=LOW|drift=WARN|sleep=LOW|mono=HIGH";
  const events = [
    ...Array.from({ length: 4 }, () =>
      makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey })
    ),
    ...Array.from({ length: 4 }, () =>
      makeEvent({ day: endDay, strategyArm: "INTENSITY_SHIFT", outcomeClass: "BAD", contextKey })
    ),
  ];
  const evidence = computeLearningEvidence(events, endDay, contextKey);
  const narrative = buildLearningNarrative(evidence);
  assert.ok(narrative.includes("spricht derzeit mehr für"));
  assert.ok(narrative.includes("vorläufig"));
  assert.ok(!narrative.includes("robuster als"));
}

// T2b) Hohe Confidence -> starker Vergleich erlaubt (Level 3)
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=T|stress=MED|hrv=LOW|drift=WARN|sleep=LOW|mono=HIGH";
  const events = [
    ...Array.from({ length: 8 }, () =>
      makeEvent({ day: endDay, strategyArm: "FREQ_UP", outcomeClass: "GOOD", contextKey })
    ),
    ...Array.from({ length: 8 }, () =>
      makeEvent({ day: endDay, strategyArm: "INTENSITY_SHIFT", outcomeClass: "BAD", contextKey })
    ),
  ];
  const evidence = computeLearningEvidence(events, endDay, contextKey);
  const narrative = buildLearningNarrative(evidence);
  assert.ok(narrative.includes("robuster als"));
}

// T3) recommendedArm == NEUTRAL -> keine robuste/bewährt Sprache
{
  const endDay = isoDaysAgo(0);
  const contextKey = "RFgap=T|stress=HIGH|hrv=LOW|drift=BAD|sleep=LOW|mono=HIGH";
  const events = Array.from({ length: 4 }, () =>
    makeEvent({ day: endDay, strategyArm: "NEUTRAL", outcomeClass: "GOOD", contextKey })
  );
  const evidence = computeLearningEvidence(events, endDay, contextKey);
  const narrative = buildLearningNarrative(evidence);
  assert.ok(!narrative.includes("robust"));
  assert.ok(!narrative.includes("bewährt"));
  assert.ok(!narrative.includes("stabiler"));
  assert.ok(narrative.includes("konservativ") || narrative.includes("stabilisieren"));
}

// T4) Confidence NaN -> formatPct 0%, kein NaN im Text
{
  const evidence = {
    recommendation: {
      strategyArm: "FREQ_UP",
      confidenceArm: Number.NaN,
      confidenceContext: Number.NaN,
      nEffArm: 4.2,
      nEffTotal: 4.2,
      explorationNeed: false,
      contextSummary: "RunFloorGap ja, Stress LOW, HRV NORMAL",
      contextKey: "LEGACY",
      globalFallback: false,
    },
    arms: {
      FREQ_UP: { nEff: Number.NaN },
    },
  };
  const narrative = buildLearningNarrative(evidence);
  assert.ok(!narrative.includes("NaN"));
  assert.ok(narrative.includes("Confidence=0%"));
}

// T5) Global fallback suffix max einmal
{
  const evidence = {
    recommendation: {
      strategyArm: "FREQ_UP",
      confidenceArm: 0.5,
      confidenceContext: 0.5,
      nEffArm: 4,
      nEffTotal: 4,
      explorationNeed: false,
      contextSummary: "globaler Kontext",
      contextKey: "ALL",
      globalFallback: true,
    },
    arms: {
      FREQ_UP: { nEff: 4 },
    },
  };
  const narrative = buildLearningNarrative(evidence);
  const suffix = "(basierend auf globalen Daten)";
  const count = narrative.split(suffix).length - 1;
  assert.ok(count <= 1);
}

console.log("learning_v2 tests passed");
