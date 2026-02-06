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

UI/Texts zeigen maximal 3 Tags an; `LEGACY` → „Legacy-Kontext“, `ALL` → „globaler Kontext“.

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

Confidence (datenbasiert, nicht gleich Exploration):
```
conf_context = n_eff_total / (n_eff_total + k)
conf_arm = n_eff_arm / (n_eff_arm + k)
```
Standard: `k = 6`

Fallback:
Wenn `n_eff < 3`, wird auf globalen Kontext (`ALL`) zurückgegriffen.

## Exploration vs. konservativ
Exploration wird **nicht** durch Confidence ersetzt. Kriterien:
- `conf_context < 0.4` oder
- weniger als 2 Arms mit Daten oder
- Utility-Diff < 0.05

Wenn Exploration nötig ist und `exploreUntried=false`, bleibt die Empfehlung konservativ im datenbasierten Pool.

## Guardrails / Red Flags
Bei Red Flags gilt:
- `strategyArm = NEUTRAL`
- `learningEligible = false`

Diese Events werden **separat gezählt**, fließen aber nicht ins Learning ein.

## Schema (Events)
`learningEvent` wird erweitert um:
- `strategyArm`
- `contextKey`
- `outcomeClass`
- `learningEligible`
- `policyReason`
- optional `signalsSnapshot`

## Migration (Legacy)
Legacy-Events bleiben lesbar:
- `decisionArm: frequency → FREQ_UP`
- `decisionArm: intensity → INTENSITY_SHIFT`
- `decisionArm: neutral → NEUTRAL`
- `contextKey = LEGACY`
- `outcomeGood → outcomeClass` (GOOD/BAD)

## Beispiel-Learning-Text
Exploit (genug Daten):
```
Learning today: In Situationen wie (RunFloorGap ja, Stress HIGH, HRV LOW) war Häufiger, kürzer, locker für dich stabiler (n_eff=4.2, Confidence=62%).
```

Unsicher + konservativ:
```
Learning today: Wir sind noch unsicher in (RunFloorGap ja, Stress HIGH, HRV LOW); wir bleiben konservativ bei Häufiger, kürzer, locker (n_eff=1.1, Confidence=15%).
```

Exploration:
```
Learning today: Wir sind noch unsicher in (RunFloorGap ja, Stress HIGH, HRV LOW); wir testen Häufiger, kürzer, locker klein dosiert (n_eff=0.0, Confidence=0%).
```

```
Learning today: In Situationen wie (RunFloorGap ja, Stress HIGH, HRV LOW) war Häufiger, kürzer, locker für dich stabiler (n_eff=4.2, Confidence=62%).
```
