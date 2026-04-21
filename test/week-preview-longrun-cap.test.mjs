import assert from 'node:assert/strict';
import { __test } from '../src/index.js';

// Regression: buildWeekPreview must use longRunStepCapMin for LONGRUN label.
{
  const week = __test.buildWeekPreview({ activitiesAll: [] }, '2026-01-05', {
    blockState: { block: 'BASE', weeksToEvent: 10, eventDistance: 'hm' },
    keyCompliance: {
      keyAllowedNow: false,
      plannedKeyType: 'steady',
      maxKeysPerWeek: 1,
    },
    runFloorState: { overlayMode: 'NORMAL', longRunStepCapMin: 53, longRunTargetMin: 90 },
  });

  const longrun = week.days.find((d) => d.sessionType === 'LONGRUN');
  assert.equal(Boolean(longrun), true);
  assert.equal(/53/.test(String(longrun?.sessionLabel || '')), true);
}

// Regression: longrun recommendation text must prefer step cap over target fallback.
{
  const next = __test.buildNextRunRecommendation({
    runFloorState: { overlayMode: 'NORMAL' },
    keyDecision: { blockedByLongrunSpacing: false, blockedByFatigue: false },
    keySpacingOk: true,
    keyAllowedNow: false,
    plannedSessionType: 'LONGRUN',
    plannedSessionLabel: 'Langer Lauf ~90′',
    longRunStepCapMin: 53,
    longRunTargetMin: 90,
  });

  assert.equal(next, 'Longrun wie im Wochenplan: Langer Lauf ~53′.');
}

console.log('week preview longrun cap regression ok');
