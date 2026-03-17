import assert from 'node:assert/strict';
import { __test } from '../src/index.js';

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

// 5) TAPER erlaubt genau einen Aktivierungs-Key bevorzugt 4–5 Tage vor Event
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'RACE', weeksToEvent: 1, eventDate: '2026-01-10', eventDistance: '10k' },
    keyCompliance: { keyAllowedNow: true, plannedKeyType: 'steady', maxKeysPerWeek: 2 },
    runFloorState: { overlayMode: 'TAPER' },
  });
  const keys = week.days.filter((d) => d.sessionType === 'KEY');
  assert.equal(keys.length, 1);
  assert.equal(keys[0].date, '2026-01-06'); // 4 Tage vor Event (um 1 Tag verschoben nach Laufpause)
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

// 7) Bei Lauf in den letzten 24h bleibt Taper-Key auf bevorzugtem 5-Tage-Slot
{
  const week = __test.buildWeekPreview({ activitiesAll: [{ type: 'Run', start_date_local: '2026-01-04T07:00:00Z' }] }, '2026-01-05', {
    blockState: { block: 'RACE', weeksToEvent: 1, eventDate: '2026-01-10', eventDistance: '10k' },
    keyCompliance: { keyAllowedNow: true, plannedKeyType: 'steady', maxKeysPerWeek: 2 },
    runFloorState: { overlayMode: 'TAPER' },
  });
  const keys = week.days.filter((d) => d.sessionType === 'KEY');
  assert.equal(keys.length, 1);
  assert.equal(keys[0].date, '2026-01-05');
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
