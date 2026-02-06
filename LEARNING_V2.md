# Learning v2 – Kontextbasiertes Strategie-Learning

## Ziel
Learning v2 ersetzt die alte Arm-Logik (frequency/intensity/neutral) durch coach-taugliche Strategien, die pro Kontext gelernt werden. Die Event-Pipeline bleibt erhalten, das Schema wird erweitert und Legacy-Events bleiben lesbar.

## Strategie-Arme (Decision)
- `FREQ_UP`: Häufiger, kürzer, locker
- `INTENSITY_SHIFT`: Qualitätsreiz statt mehr Umfang
- `VOLUME_ADJUST`: Umfang gezielt anpassen
- `HOLD_ABSORB`: Stabilisieren & absorbieren
- `PROTECT_DELOAD`: Schützen & deloaden
- `NEUTRAL`: keine klare Strategie

## Kontext-Slices
Kontext wird aus diskreten Buckets abgeleitet und als stabiler `contextKey` gespeichert:

```
RFgap=T|stress=HIGH|hrv=LOW|drift=WARN|sleep=LOW|mono=HIGH
```

Buckets:
- `stress`: LOW/MED/HIGH (Fatigue + Warnsignale)
- `hrv`: LOW/NORMAL/HIGH (Δ vs 7T)
- `drift`: OK/WARN/BAD (aus Drift-Signal)
- `sleep`: LOW/OK/HIGH (Sleep Δ vs 7T)
- `mono`: LOW/HIGH (Monotony > Limit)

Fehlende Signale werden mit `UNK` markiert.

## Outcome-Klassen
Outcomes sind 3-stufig:
- `GOOD`, `NEUTRAL`, `BAD`

Im aktuellen Score-System gilt: **höherer `outcomeScore` = besser**.
Mapping:
- `outcomeScore >= 2` → `GOOD`
- `outcomeScore == 1` → `NEUTRAL`
- `outcomeScore <= 0` → `BAD`

## Bayesian Tracking
Pro `(contextKey, strategyArm)` werden zwei Beta-Posterioren geführt:
- `p_good = P(outcome == GOOD)`
- `p_bad  = P(outcome == BAD)`

Utility:
```
utility = p_good_mean - λ * p_bad_mean
```
Standard: `λ = 1.5`

Recency-Decay:
```
w = exp(-ageDays / decayDays)
```

Confidence:
```
conf = n_eff / (n_eff + k)
```
Standard: `k = 6`

Fallback:
Wenn `n_eff < 3`, wird auf globalen Kontext (`ALL`) zurückgegriffen.

## Guardrails / Red Flags
Bei Red Flags gilt:
- `strategyArm = NEUTRAL`
- `learningEligible = false`

Diese Events werden **separat gezählt**, fließen aber nicht ins Learning ein.

## Migration (Legacy)
Legacy-Events bleiben lesbar:
- `decisionArm: frequency → FREQ_UP`
- `decisionArm: intensity → INTENSITY_SHIFT`
- `decisionArm: neutral → NEUTRAL`
- `contextKey = LEGACY`
- `outcomeGood → outcomeClass` (GOOD/BAD)

## Beispiel-Learning-Text
```
Learning today: In Situationen wie (RunFloorGap ja, Stress HIGH, HRV LOW) war Häufiger, kürzer, locker für dich stabiler (n_eff=4.2, Confidence=62%).
```
