import assert from 'node:assert/strict';
import { __test } from '../src/index.js';

function makeCommentsInput({
  todayIso,
  blockState,
  keyRules,
  keyCompliance,
  runFloorState,
  distanceDiagnostics,
  longRunSummary,
  runFloorEwma10,
  eventDistance,
}) {
  return {
    perRunInfo: [],
    trend: { dv: 0, dEF: 0, dVDOT: 0 },
    motor: { score: 0, status: 'neutral', text: 'neutral' },
    benchReports: [],
    robustness: { strengthMinutes7d: 20, strengthPolicy: { target: 60, minutes7d: 20 } },
    modeInfo: { nextEvent: { start_date_local: `${todayIso}T08:00:00Z` } },
    blockState,
    keyRules,
    keyCompliance,
    keySpacing: { ok: true },
    todayIso,
    policy: {},
    loads7: { runTotal7: 100 },
    runFloorEwma10,
    runFloorState,
    specificOk: false,
    specificValue: 0,
    aerobicOk: true,
    aerobicFloor: 0,
    aerobicFloorActive: false,
    fatigue: { override: false, monotony: 1.2, acwr: 1.0, runDist14dRatio: 1.0 },
    longRunSummary,
    distanceDiagnostics,
    gapRecommendations: { primaryFocus: ['Basis sichern', 'Frequenz stabilisieren'] },
    bikeSubFactor: 0,
    weeksToEvent: blockState.weeksToEvent,
    eventDistance,
  };
}

// E2E Golden 1: Instabile Basis / RED / avg7=0 / Ziel 2–3 Läufe
{
  const todayIso = '2026-01-05';
  const eventDistance = '10k';
  const ctx = {
    activitiesAll: [],
    distanceDiagnostics: { primaryGap: 'base', secondaryGap: 'specificity' },
  };
  const blockState = { block: 'BASE', weeksToEvent: 20, eventDistance };
  const keyRules = { plannedPrimaryType: 'steady' };
  const keyCompliance = {
    keyAllowedNow: true,
    plannedKeyType: 'steady',
    maxKeysPerWeek: 2,
    suggestion: 'Starte mit 2–3 Läufen/Woche',
    intensityDistribution: {
      hasData: true,
      easyShare: 0.85,
      midShare: 0.1,
      hardShare: 0.05,
      targets: { easyMin: 0.72, midMax: 0.2, hardMax: 0.16 },
      lookbackDays: 7,
    },
    keySpacingOk: true,
    actual7Raw: 0,
  };
  const runFloorState = {
    overlayMode: 'NORMAL',
    effectiveFloorTarget: 8,
    floorTarget: 10,
    floorLevel: 'RED',
    stabilityOK: false,
    avg7: 0,
    decisionText: __test.resolveRunFloorDecisionText({ overlayMode: 'NORMAL', stabilityWarn: false, avg7: 0, stabilityOK: false }),
  };
  const distanceDiagnostics = {
    readiness: 38,
    primaryGap: 'base',
    secondaryGap: 'specificity',
    snapshot: { runsCount: 1 },
    components: {
      base: { interpretation: 'Basis aktuell klar defizitär — zuerst Frequenz/Kontinuität stabilisieren.' },
      specificity: { interpretation: 'Spezifität ist aktuell ausbaufähig.' },
      longrun: { interpretation: 'Longrun-Aufbau läuft.' },
      robustness: { interpretation: 'Robustheit okay.' },
      execution: { interpretation: 'Ausführung solide.' },
    },
    scores: { base: 30, specificity: 45, longrun: 50, robustness: 55, execution: 60 },
    strengths: ['execution'],
  };
  const weeklyFocus = __test.buildWeeklyFocus(ctx, todayIso, blockState, keyCompliance, runFloorState, [], null);
  const preview = __test.buildWeekPreview(ctx, todayIso, { blockState, keyCompliance, runFloorState, distanceDiagnostics });
  const comments = __test.buildComments(
    makeCommentsInput({
      todayIso,
      blockState,
      keyRules,
      keyCompliance,
      runFloorState,
      distanceDiagnostics,
      longRunSummary: { longRun14d: { minutes: 45 }, plan: { targetMin: 45 }, longestRun30d: { minutes: 45, windowDays: 14 } },
      runFloorEwma10: 5,
      eventDistance,
    }),
    { verbosity: 'debug' }
  );

  const hardRuns = preview.days.filter((d) => ['GA', 'KEY', 'LONGRUN'].includes(d.sessionType));
  const keys = hardRuns.filter((d) => d.sessionType === 'KEY');
  const longruns = hardRuns.filter((d) => d.sessionType === 'LONGRUN');
  const low = preview.days.filter((d) => d.sessionType === 'LOW');

  assert.equal(/Fokus: (Basis|Frequenz)/.test(weeklyFocus), true);
  assert.equal(/Fokus: Spezifik/.test(weeklyFocus), false);
  assert.equal(hardRuns.length <= 3, true);
  assert.equal(low.length >= 1, true);
  assert.equal(keys.length, 1);
  assert.equal(longruns.length, 1);
  assert.equal(runFloorState.decisionText === 'stabilize_base' || runFloorState.decisionText === 'rebuild', true);
  assert.equal(/Ja – im Plan/.test(comments), false);
}

// E2E Golden 2: Stabile Woche / genug Frequenz / steady-Key erlaubt
{
  const todayIso = '2026-01-12';
  const eventDistance = '10k';
  const ctx = {
    activitiesAll: [
      { type: 'Run', start_date_local: '2026-01-11T08:00:00Z', moving_time: 3300 },
      { type: 'Run', start_date_local: '2026-01-10T08:00:00Z', moving_time: 3600 },
      { type: 'Run', start_date_local: '2026-01-09T08:00:00Z', moving_time: 2700 },
      { type: 'Run', start_date_local: '2026-01-08T08:00:00Z', moving_time: 2700 },
    ],
    distanceDiagnostics: { primaryGap: 'specificity', secondaryGap: 'base' },
  };
  const blockState = { block: 'BUILD', weeksToEvent: 8, eventDistance };
  const keyRules = { plannedPrimaryType: 'steady' };
  const keyCompliance = {
    keyAllowedNow: true,
    plannedKeyType: 'steady',
    maxKeysPerWeek: 2,
    suggestion: '2–3 Läufe/Woche',
    freqOk: false,
    explicitSession: "2x10' steady",
    intensityDistribution: {
      hasData: true,
      easyShare: 0.74,
      midShare: 0.14,
      hardShare: 0.12,
      targets: { easyMin: 0.72, midMax: 0.2, hardMax: 0.16 },
      lookbackDays: 7,
    },
    keySpacingOk: true,
    actual7Raw: 1,
    focusHits: 1,
    focusTarget: 2,
  };
  const runFloorState = {
    overlayMode: 'NORMAL',
    effectiveFloorTarget: 12,
    floorTarget: 12,
    floorLevel: 'GREEN',
    stabilityOK: true,
    avg7: 12,
    decisionText: __test.resolveRunFloorDecisionText({ overlayMode: 'NORMAL', stabilityWarn: false, avg7: 12, stabilityOK: true }),
  };
  const distanceDiagnostics = {
    readiness: 72,
    primaryGap: 'specificity',
    secondaryGap: 'base',
    snapshot: { runsCount: 4 },
    components: {
      base: { interpretation: 'Basis vorhanden, aber klar unter Zielkorridor.' },
      specificity: { interpretation: 'Spezifität vorhanden, gezielt weiter schärfen.' },
      longrun: { interpretation: 'Longrun-Fähigkeit bestätigt; zuletzt kürzer, aktuell eher gehalten als ausgebaut.' },
      robustness: { interpretation: 'Robustheit gut ausgeprägt.' },
      execution: { interpretation: 'Ausführung prozessstabil.' },
    },
    scores: { base: 62, specificity: 58, longrun: 64, robustness: 72, execution: 75 },
    strengths: ['execution', 'robustness'],
  };
  const weekMemories = [{
    dateFrom: '2026-01-05',
    dateTo: '2026-01-11',
    runs: { count: 4, totalMinutes: 220, keyCount: 1, keyTypes: ['steady'], longestMinutes: 70 },
    strength: { totalMinutes: 50, sessionCount: 2 },
    efTrend: { pct: 2, confidence: 'high' },
  }];

  const weeklyFocus = __test.buildWeeklyFocus(ctx, todayIso, blockState, keyCompliance, runFloorState, weekMemories, null);
  const preview = __test.buildWeekPreview(ctx, todayIso, { blockState, keyCompliance, runFloorState, distanceDiagnostics });
  const comments = __test.buildComments(
    makeCommentsInput({
      todayIso,
      blockState,
      keyRules,
      keyCompliance,
      runFloorState,
      distanceDiagnostics,
      longRunSummary: { longRun14d: { minutes: 65 }, plan: { targetMin: 60 }, longestRun30d: { minutes: 70, windowDays: 14 } },
      runFloorEwma10: 12,
      eventDistance,
    }),
    { verbosity: 'debug' }
  );

  assert.equal(__test.inferKeyTypeFromExplicitSession("2x10' steady"), 'steady');
  assert.equal(/Fokus: Spezifik/.test(weeklyFocus), true);
  assert.equal(/Mindestziel/.test(comments), true);
  assert.equal(/Entwicklungsziel/.test(comments), true);
  assert.equal(/Longrun aktuell im Mindestzielbereich/.test(comments), true);
  assert.equal(/Longrun-Progression:/.test(comments), false);
  assert.equal(/BOTTOM LINE/.test(comments), true);
  assert.equal(preview.days.some((d) => d.sessionType === 'KEY'), true);
  assert.equal(preview.days.some((d) => d.sessionType === 'LONGRUN'), true);
}

console.log('e2e golden ok');
