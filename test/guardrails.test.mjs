import assert from 'node:assert/strict';
import { __test } from '../src/index.js';

// 0) Cloudflare-Subrequest-Limit Fehler darf nicht erneut retried werden
{
  assert.equal(
    __test.isWorkerSubrequestLimitError(new Error('Too many subrequests by single Worker invocation.')),
    true
  );
  assert.equal(
    __test.isWorkerSubrequestLimitError(new Error('network timeout')),
    false
  );
}

// 1) WeightTraining muss als strength zählen
{
  const d = __test.detectStrength({ type: 'WeightTraining', tags: [], name: 'Upper body' });
  assert.equal(d.matched, true);
  assert.equal(d.typeMatch, true);
}

// 2) TAPER darf freqOk nicht unnötig auf false setzen
{
  const keyRules = {
    expectedKeysPerWeek: 1,
    maxKeysPerWeek: 2,
    allowedKeyTypes: ['steady'],
    preferredKeyTypes: ['steady'],
    bannedKeyTypes: [],
  };
  const keyStats7 = { count: 0.5, list: ['steady'] };
  const keyStats14 = { count: 1, list: ['steady'] };
  const compliance = __test.evaluateKeyCompliance(keyRules, keyStats7, keyStats14, {
    overlayMode: 'TAPER',
    eventInDays: 7,
    block: 'RACE',
    eventDistance: '10k',
    keySpacing: { ok: true },
  });
  assert.equal(compliance.expected <= 0.5, true);
  assert.equal(compliance.freqOk, true);
}

// 3) Änderungen an Tags/Name müssen Scheduled-Signatur ändern
{
  const base = [{ id: 1, start_date_local: '2026-01-01T07:00:00', moving_time: 3600, icu_training_load: 50, tags: ['easy'], name: 'Morning Run' }];
  const changedTags = [{ ...base[0], tags: ['easy', 'shoe:a'] }];
  const changedName = [{ ...base[0], name: 'Morning Run Updated' }];
  const sigBase = __test.buildRunSignatureDescriptor(base).runIdsSignature;
  const sigTags = __test.buildRunSignatureDescriptor(changedTags).runIdsSignature;
  const sigName = __test.buildRunSignatureDescriptor(changedName).runIdsSignature;
  assert.notEqual(sigBase, sigTags);
  assert.notEqual(sigBase, sigName);
}

// 4) Phase und Overlay müssen getrennt sichtbar sein
{
  const line = __test.formatPhaseOverlayLine('RACE', 'TAPER');
  assert.match(line, /Phase RACE/);
  assert.match(line, /Overlay TAPER/);
}

console.log('guardrails ok');

// 5) 10k TAPER mit Verzögerung setzt genau einen Aktivierungs-Key näher am Rennen (3 Tage vorher)
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'RACE', weeksToEvent: 1, eventDate: '2026-01-10', eventDistance: '10k' },
    keyCompliance: { keyAllowedNow: true, plannedKeyType: 'steady', maxKeysPerWeek: 2 },
    runFloorState: { overlayMode: 'TAPER' },
  });
  const keys = week.days.filter((d) => d.sessionType === 'KEY');
  assert.equal(keys.length, 1);
  assert.equal(keys[0].date, '2026-01-07'); // 3 Tage vor Event (verzögert nach Laufpause)
  assert.match(keys[0].sessionLabel, /Aktivierungs-Key \(Taper\)/);
  assert.equal(keys[0].intensity, 'HIGH');
  const today = week.days.find((d) => d.date === '2026-01-05');
  assert.equal(today.sessionType, 'GA');
  assert.match(today.sessionLabel, /erst wieder reinkommen/);
  assert.match(today.note, /Tage ohne Lauf/);
}

// 6) TAPER-KEY wird nicht gesetzt, wenn keyAllowedNow=false
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'RACE', weeksToEvent: 1, eventDate: '2026-01-10', eventDistance: '10k' },
    keyCompliance: { keyAllowedNow: false, plannedKeyType: 'steady', maxKeysPerWeek: 2 },
    runFloorState: { overlayMode: 'TAPER' },
  });
  const keys = week.days.filter((d) => d.sessionType === 'KEY');
  assert.equal(keys.length, 0);
}

// 7) Bei Lauf in den letzten 24h bleibt der 10k-Taper-Key auf dem bevorzugten 4-Tage-Slot
{
  const week = __test.buildWeekPreview({ activitiesAll: [{ type: 'Run', start_date_local: '2026-01-04T07:00:00Z' }] }, '2026-01-05', {
    blockState: { block: 'RACE', weeksToEvent: 1, eventDate: '2026-01-10', eventDistance: '10k' },
    keyCompliance: { keyAllowedNow: true, plannedKeyType: 'steady', maxKeysPerWeek: 2 },
    runFloorState: { overlayMode: 'TAPER' },
  });
  const keys = week.days.filter((d) => d.sessionType === 'KEY');
  assert.equal(keys.length, 1);
  assert.equal(keys[0].date, '2026-01-06');
}

// 8) Optionale LOW-Slots dürfen Report-Frequenz (2–3 Läufe/Woche) nicht überschreiten
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'BASE', weeksToEvent: 20, eventDistance: '10k' },
    keyCompliance: {
      keyAllowedNow: true,
      plannedKeyType: 'steady',
      maxKeysPerWeek: 2,
      suggestion: 'Starte mit 2–3 Läufen/Woche',
    },
    runFloorState: { overlayMode: 'NORMAL' },
  });
  const runLike = week.days.filter((d) => ['GA', 'KEY', 'LONGRUN'].includes(d.sessionType));
  assert.equal(runLike.length <= 3, true);
  assert.equal(runLike.filter((d) => d.sessionType === 'KEY').length >= 1, true);
  assert.equal(runLike.filter((d) => d.sessionType === 'LONGRUN').length, 1);
  const nonCoreEasyRuns = runLike.filter((d) => d.sessionType === 'GA').length;
  assert.equal(nonCoreEasyRuns <= 1, true);
}

// 9) Bei hartem Frequenz-Cap (2 Läufe/Woche) bleiben Key + Longrun erhalten
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'BASE', weeksToEvent: 20, eventDistance: '10k' },
    keyCompliance: {
      keyAllowedNow: true,
      plannedKeyType: 'steady',
      maxKeysPerWeek: 2,
      suggestion: 'Diese Woche 2 Läufe/Woche',
    },
    runFloorState: { overlayMode: 'NORMAL' },
  });
  const runLike = week.days.filter((d) => ['GA', 'KEY', 'LONGRUN'].includes(d.sessionType));
  assert.equal(runLike.length <= 2, true);
  assert.equal(runLike.filter((d) => d.sessionType === 'KEY').length, 1);
  assert.equal(runLike.filter((d) => d.sessionType === 'LONGRUN').length, 1);
}

// 10) KeyType-Inferenz: easy/GA => null, echte steady-Texte => steady
{
  const inferred = __test.inferKeyTypeFromExplicitSession('ga konkret: 60–75′ GA1 locker');
  assert.equal(inferred, null);

  const steadyA = __test.inferKeyTypeFromExplicitSession("2x10' steady");
  const steadyB = __test.inferKeyTypeFromExplicitSession("40' steady");
  const steadyC = __test.inferKeyTypeFromExplicitSession('steady mit Endbeschleunigung');
  const steadyD = __test.inferKeyTypeFromExplicitSession('steady, aber kontrolliert');
  const steadyE = __test.inferKeyTypeFromExplicitSession('locker/steady');
  const gaWithStrides = __test.inferKeyTypeFromExplicitSession('GA locker mit Strides');
  assert.equal(steadyA, 'steady');
  assert.equal(steadyB, 'steady');
  assert.equal(steadyC, 'steady');
  assert.equal(steadyD, 'steady');
  assert.equal(steadyE, 'steady');
  assert.equal(gaWithStrides, null);
}

// 11) LOW/easy-frei bleibt optional und zählt nicht als harter Lauftag
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'BASE', weeksToEvent: 20, eventDistance: '10k' },
    keyCompliance: {
      keyAllowedNow: true,
      plannedKeyType: 'steady',
      maxKeysPerWeek: 2,
      suggestion: 'Starte mit 2–3 Läufen/Woche',
    },
    runFloorState: { overlayMode: 'NORMAL' },
  });
  const lowSlots = week.days.filter((d) => d.sessionType === 'LOW');
  assert.equal(lowSlots.length >= 1, true);
  const hardRuns = week.days.filter((d) => ['GA', 'KEY', 'LONGRUN'].includes(d.sessionType));
  assert.equal(hardRuns.length <= 3, true);
}

// 12) decisionText-Semantik: RED/instabil klingt nicht mehr nach normalem Build
{
  const unstable = __test.resolveRunFloorDecisionText({
    overlayMode: 'NORMAL',
    stabilityWarn: false,
    avg7: 0,
    stabilityOK: false,
  });
  assert.equal(unstable, 'stabilize_base');

  const stable = __test.resolveRunFloorDecisionText({
    overlayMode: 'NORMAL',
    stabilityWarn: false,
    avg7: 8,
    stabilityOK: true,
  });
  assert.equal(stable, 'rebuild');
}

// 13) Basis/Frequenz-Fokus bleibt textlich konsistent (kein impliziter Spezifik-Push in Bottom line)
{
  const out = __test.buildRecommendationsAndBottomLine({
    todayAction: '35–45′ locker',
    keyAllowedNow: true,
    explicitSessionShort: "2x10' steady",
    runFloorTarget: 10,
    runFloorEwma10: 6,
    distanceDiagnostics: { readiness: 40, primaryGap: 'base', secondaryGap: 'specificity' },
    gapRecommendations: { primaryFocus: ['Basis sichern', 'Frequenz stabilisieren'] },
    longRunDoneMin: 45,
    longRunTargetMin: 45,
    longRunDiagnosisTargetMin: 60,
  });
  const bottom = (out.bottomLine || []).join(' ');
  assert.equal(/Key \(wenn frisch\)/.test(bottom), false);
}

// 14) Longrun-Kommunikation enthält keine ungetrennte Zielbereich-vs-zu-kurz-Kollision
{
  const out = __test.buildRecommendationsAndBottomLine({
    longRunDoneMin: 45,
    longRunTargetMin: 45,
    longRunDiagnosisTargetMin: 60,
    blockLongRunNextWeekTargetMin: 50,
    longRunStepCapMin: 50,
  });
  const text = (out.recommendations || []).join(' | ');
  assert.equal(/Longrun aktuell im Mindestzielbereich/.test(text), true);
  assert.equal(/Longrun-Progression:/.test(text), false);
}

// 15) WHY-Narrativ normalisiert Rohtexte und vermeidet Komma-Aufzählung
{
  const text = __test.buildWhyNarrative(['krafttraining unter Soll (0′/20′)', 'ef leicht rückläufig']);
  assert.match(text, /Krafttraining unter Soll \(0 von 20 Minuten\)/);
  assert.match(text, /Effizienzfaktor leicht rückläufig/);
}

// 16) Renntag-Block zeigt 5k-Zieltempo nur bei plausibler Pace
{
  const block = __test.buildRaceDayPrepBlock({
    eventInDays: 1,
    eventDistance: '10k',
    vdotMed: 50,
    efMed: null,
  });
  assert.match(block, /Zieltempo: ~/);
  assert.match(block, /Gesamtzeit über 5 km/);

  const noLine = __test.buildRaceDayPrepBlock({
    eventInDays: 1,
    eventDistance: '10k',
    vdotMed: 90,
    efMed: null,
  });
  assert.equal(/Zieltempo: ~/.test(noLine), false);

  const efFallback = __test.buildRaceDayPrepBlock({
    eventInDays: 1,
    eventDistance: '10k',
    vdotMed: null,
    efMed: 0.05,
  });
  assert.match(efFallback, /Zieltempo: ~/);

  const noToday = __test.buildRaceDayPrepBlock({
    eventInDays: 0,
    eventDistance: '10k',
    vdotMed: 50,
    efMed: null,
  });
  assert.equal(noToday, '');

  const noPast = __test.buildRaceDayPrepBlock({
    eventInDays: -1,
    eventDistance: '10k',
    vdotMed: 50,
    efMed: null,
  });
  assert.equal(noPast, '');
}


// 15) Taper-Woche priorisiert Wochenfokus über Frequenz
{
  const ctx = { distanceDiagnostics: { primaryGap: 'execution' } };
  const weekly = __test.buildWeeklyFocus(
    ctx,
    '2026-01-05',
    { block: 'RACE', weeksToEvent: 1, eventDistance: '5k' },
    { plannedKeyType: 'schwelle' },
    { overlayMode: 'TAPER' },
    [],
    null
  );
  assert.equal(/Fokus: Taper/.test(weekly), true);
  assert.equal(/Frisch bleiben/.test(weekly), true);
}

// 16) Overlay-Taper senkt Kraft-Ziel in Raceweek
{
  const adjusted = __test.applyStrengthPolicyOverlay(
    { minRunfloor: 30, target: 60, max: 75, minutes7d: 10, score: 0, confidenceDelta: -3, belowRunfloor: true },
    { overlayMode: 'TAPER', weeksToEvent: 1 }
  );
  assert.equal(adjusted.target, 20);
  assert.equal(adjusted.max, 30);
}

// 17) 2. Key optional nur mit verfügbarer Progressionsvorlage
{
  const keyRules = {
    expectedKeysPerWeek: 1,
    maxKeysPerWeek: 2,
    allowedKeyTypes: ['steady'],
    preferredKeyTypes: ['steady'],
    bannedKeyTypes: [],
    plannedPrimaryType: 'steady',
  };
  const out = __test.evaluateKeyCompliance(keyRules, { count: 1, list: ['steady'] }, { count: 2, list: ['steady'] }, {
    block: 'BASE',
    eventDistance: '10k',
    dayIso: '2026-01-10',
    blockStartIso: '2026-01-01',
    weeksToEvent: 12,
    overlayMode: 'NORMAL',
    ctx: { activitiesAll: [] },
    keySpacing: { keySpacingNowOk: true, ok: true },
  });
  assert.equal(/2\. Key diese Woche optional\/erlaubt/.test(out.suggestion), false);
}


// 18) Manueller Blockstart-Override setzt Persistenzfelder konsistent
{
  const base = {
    startDate: '2026-03-16',
    blockStartEffective: '2026-03-16',
    blockStartPersisted: null,
    startWasReset: true,
    timeInBlockDays: 0,
    reasons: [],
  };
  const out = __test.applyManualBlockStartOverride(base, '2026-02-01', '2026-03-16');
  assert.equal(out.startDate, '2026-02-01');
  assert.equal(out.blockStartEffective, '2026-02-01');
  assert.equal(out.blockStartPersisted, '2026-02-01');
  assert.equal(out.startWasReset, false);
  assert.equal(out.timeInBlockDays > 0, true);
}

// 19) Robustness-Bewertung respektiert taper-reduziertes Kraftziel
{
  const snapshot = {
    eventDistance: '10k',
    block: 'RACE',
    runFloor: 12,
    runLoad7: 180,
    runsCount: 4,
    easyShare: 0.8,
    midShare: 0.12,
    hardShare: 0.06,
    keyCount: 1,
    keyTypes: ['steady'],
    longrunMin: 60,
    longrunSpecificMin: 0,
    strengthMin: 20,
    fatigueOverride: false,
    keySpacingOk: true,
    efTrend: 0,
    driftTrend: 0,
    executionScoreRaw: 80,
    executionScore: 80,
  };
  const ctx = {
    dayIso: '2026-03-16',
    activitiesAll: [
      { type: 'Run', start_date_local: '2026-03-10T08:00:00Z', moving_time: 2400 },
      { type: 'Run', start_date_local: '2026-03-12T08:00:00Z', moving_time: 2400 },
      { type: 'Run', start_date_local: '2026-03-14T08:00:00Z', moving_time: 3600 },
    ],
    strengthPolicy: { target: 20, max: 30 },
    keyCompliance: { focusHits: 1, preferredMissing: false, freqOk: true },
    runFloorState: { plannedDip: false },
  };
  const diag = __test.computeDistanceDiagnostics(snapshot, ctx);
  const constraints = diag?.components?.robustness?.constraints || [];
  assert.equal(constraints.some((line) => /Kraftumfang 0-10′\/Woche \(schwach\)/.test(line)), false);
}

// 20) Taper-Empfehlungen unterdrücken RunFloor-Volumen-Push
{
  const out = __test.buildRecommendationsAndBottomLine({
    runFloorEwma10: 93,
    runFloorTarget: 113,
    overlayMode: 'TAPER',
    taperPriorityWeek: true,
    todayAction: '30–40′ locker',
    keyAllowedNow: false,
  });
  const joined = (out?.recommendations || []).join('\n');
  assert.equal(/Volumen priorisieren/.test(joined), false);
  assert.equal(/Overlay: TAPER/.test(joined), true);
}
