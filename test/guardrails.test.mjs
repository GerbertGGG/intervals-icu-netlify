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
  assert.equal(keys[0].date, '2026-01-05'); // 5 Tage vor Event
  assert.match(keys[0].sessionLabel, /Aktivierungs-Key \(Taper\)/);
  assert.equal(keys[0].intensity, 'HIGH');
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
