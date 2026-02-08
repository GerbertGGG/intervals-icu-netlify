# Workout Builder (Key-Reiz) – Logic + Templates + Output Examples

This document mirrors the workout-builder module used for the Daily Report. It includes pseudocode, template data structures, and example output strings.

## 1) Pseudocode (code-ready)

```
buildWorkout(decision, metrics):
  if guardrailSeverity == "hard" or readinessAmpel == 🔴:
     return NO_KEY

  readinessTier = tierFrom(readinessAmpel, readinessScore, fatigue)
  if readinessTier == BAD:
     return NO_KEY

  if !keyBudgetAvailable or !spacingOk:
     return NO_KEY

  if keyType == "vo2_touch" and guardrailSeverity == "soft":
     keyType = "racepace"
     reason += SOFT_GUARDRAIL_SWITCH_VO2_TO_RP

  scalingLevel = baselineFromTier(readinessTier)
  if runFloorGap: cap scalingLevel at 0 (reason FLOOR_GAP_CAPS_PROGRESSION)
  if hrvDeltaPct <= -12: cap scalingLevel at -1 (reason HRV_CAPS_WORKOUT)
  if driftPct >= 5.5 and confidence != high: cap scalingLevel at -1 (reason DRIFT_CAPS_WORKOUT)

  progressIndex = deriveProgressIndexFromHistory(familyKeyCount, readinessTier, drift/hrv)
  template = chooseTemplateByIndex(family, progressIndex)

  scaled = applyScaling(template, scalingLevel)
    - adjust reps first, then duration
    - keep intensity minutes within caps
    - if scalingLevel == -2 and intensityMinutes < 6: fallback to strides

  if runFloorGap and totalMinutes > 55:
     reduce reps until <= 55

  return plan + debug scaling note
```

## 2) Template data structures (JSON examples)

```json
{
  "racepace": [
    { "id": "RP1", "baseReps": 6, "baseWorkSec": 120, "baseRecSec": 120 },
    { "id": "RP2", "baseReps": 5, "baseWorkSec": 180, "baseRecSec": 120 },
    { "id": "RP3", "baseReps": 3, "baseWorkSec": 300, "baseRecSec": 180 }
  ],
  "vo2_touch": [
    { "id": "VO2_1", "baseReps": 10, "baseWorkSec": 60, "baseRecSec": 60 },
    { "id": "VO2_2", "baseReps": 8, "baseWorkSec": 90, "baseRecSec": 90 }
  ],
  "strides": { "id": "STRIDES", "baseReps": 8, "baseWorkSec": 20, "baseRecSec": 75 }
}
```

## 3) Output formatting (Daily Report)

The “Konkret” line is formatted as:

```
Konkret: 12′ EL, 6×2′ @ racepace (2′ trab), 8′ AL.
```

For strides:

```
Konkret: 12′ EL, 8×20s zügig/locker (75s trab), 8′ AL.
```

## 4) Example outputs (scenarios)

### Scenario A
**Input:** RACE, 🟠 score 60, runFloorGap true, HRV -14.5%, drift 5.9% (confidence not high), spacing ok  
**Expected:** racepace, scaled down (-1), no progression  
**Konkret (example):**
```
Konkret: 10′ EL, 5×2′ @ racepace (2′ trab), 8′ AL.
```

### Scenario B
**Input:** RACE, 🟢 score 75, no runFloorGap, guardrail none  
**Expected:** progress (+1)  
**Konkret (example):**
```
Konkret: 12′ EL, 7×2′ @ racepace (2′ trab), 10′ AL.
```

### Scenario C
**Input:** soft guardrail, wants vo2_touch  
**Expected:** switch to racepace  
**Konkret (example):**
```
Konkret: 12′ EL, 6×2′ @ racepace (2′ trab), 8′ AL.
```
