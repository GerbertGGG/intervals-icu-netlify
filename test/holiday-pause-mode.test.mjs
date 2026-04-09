import assert from "node:assert/strict";
import { __test } from "../src/index.js";

function makeCommentsInput(todayIso) {
  const eventDistance = "10k";
  const blockState = { block: "BASE", weeksToEvent: 14, eventDistance };
  const runFloorState = {
    overlayMode: "LIFE_EVENT_HOLIDAY",
    effectiveFloorTarget: 77,
    floorTarget: 135,
    floorLevel: "YELLOW",
    stabilityOK: false,
    avg7: 60,
    decisionText: "holiday_pause",
  };
  const distanceDiagnostics = {
    readiness: 45,
    primaryGap: "base",
    secondaryGap: "robustness",
    snapshot: { runsCount: 0 },
    components: {
      base: { interpretation: "Basis limitiert." },
      specificity: { interpretation: "Spezifik neutral." },
      longrun: { interpretation: "Longrun neutral." },
      robustness: { interpretation: "Robustheit limitiert." },
      execution: { interpretation: "Execution neutral." },
    },
    scores: { base: 45, specificity: 50, longrun: 50, robustness: 40, execution: 55 },
    strengths: ["longrun", "execution"],
  };
  return {
    perRunInfo: [],
    trend: { dv: 0, dEF: 0, dVDOT: 0 },
    motor: { score: 0, status: "neutral", text: "neutral" },
    benchReports: [],
    robustness: { strengthMinutes7d: 0, strengthPolicy: { target: 45, minutes7d: 0 } },
    modeInfo: { nextEvent: { start_date_local: `${todayIso}T08:00:00Z`, distance_type: eventDistance } },
    blockState,
    keyRules: { plannedPrimaryType: "steady" },
    keyCompliance: {
      keyAllowedNow: false,
      plannedKeyType: "steady",
      maxKeysPerWeek: 1,
      suggestion: "Nächster Key: steady",
      intensityDistribution: { hasData: false, targets: { easyMin: 0.72, midMax: 0.2, hardMax: 0.16 }, lookbackDays: 28 },
      keySpacingOk: true,
      actual7Raw: 0,
      actual7: 0,
    },
    keySpacing: { ok: true },
    todayIso,
    policy: {},
    loads7: { runTotal7: 0 },
    runFloorEwma10: 60,
    runFloorState,
    specificOk: false,
    specificValue: 0,
    aerobicOk: true,
    aerobicFloor: 0,
    aerobicFloorActive: false,
    fatigue: { override: false, monotony: 1.0, acwr: 1.0, runDist14dRatio: 1.0 },
    longRunSummary: { longRun14d: { minutes: 0 }, plan: { targetMin: 45 }, longestRun30d: { minutes: 0, windowDays: 30 } },
    distanceDiagnostics,
    gapRecommendations: { primaryFocus: ["Basis sichern"], secondaryFocus: [] },
    bikeSubFactor: 0.6,
    weeksToEvent: blockState.weeksToEvent,
    eventDistance,
  };
}

{
  const comments = __test.buildComments(makeCommentsInput("2026-04-09"), { verbosity: "debug" });
  assert.match(comments, /Urlaub aktiv/i);
  assert.match(comments, /Trainingsziele pausiert/i);
  assert.doesNotMatch(comments, /FOKUS\n/i);
  assert.doesNotMatch(comments, /TRAININGSSTAND\n/i);
  assert.doesNotMatch(comments, /EMPFEHLUNGEN\n/i);
  assert.doesNotMatch(comments, /NÄCHSTE SCHRITTE\n/i);

  assert.doesNotMatch(comments, /Nächster Key: aktuell offen/i);
  assert.doesNotMatch(comments, /Longrun: .*später prüfen/i);
  assert.doesNotMatch(comments, /Strength offen:/i);
}

console.log("holiday pause mode ok");
