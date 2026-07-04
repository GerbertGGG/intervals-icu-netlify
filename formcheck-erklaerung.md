# Formcheck – wie die Berechnung funktioniert

Der „Formcheck“ ist die tägliche Ampel-Bewertung (🟢/🟡/🔴), die jede Nacht als
Notiz im intervals.icu-Kalender landet. Die Logik steckt in zwei Dateien:

- `src/form-analysis.js` – Datenaufbereitung + Ampel-Logik (`assessRecoveryStatus`)
- `src/recovery-note.js` – schreibt das Ergebnis als Kalender-Notiz

## 1. Datenbasis (`buildRecentFormAnalysis`, `src/form-analysis.js:680`)

Für die letzten **28 Tage** (`RECOVERY_NOTE_DAYS` in `recovery-note.js:4`) werden
Aktivitäten + Wellness-Daten (Ruhepuls, HRV, Schlaf, CTL/ATL) von intervals.icu
geladen und zu Wochen-Buckets sowie Trend-Zeitreihen aggregiert.

## 2. Die eigentliche Ampel-Logik (`assessRecoveryStatus`, `src/form-analysis.js`)

Es werden **6 unabhängige Red-Flag-Detektoren** geprüft. Jeder liefert nicht mehr
nur ja/nein, sondern `{ triggered, severity }` – `severity` ist ein auf 0-1
normierter Wert (0 an der Schwelle, 1 am jeweiligen Cap), berechnet über die
Helper `severity(value, threshold, cap)` (höher = schlechter) bzw.
`severityBelow(value, threshold, cap)` (niedriger = schlechter).

| Flag | Kategorie | Prüfung | Schwellwert | Cap (Severity = 1) |
|---|---|---|---|---|
| **load_spike** (`detectLoadSpike`) | load | „Jo-Jo“-Muster: letzte Woche springt stark hoch, *nachdem* die Vorwoche selbst ein Rückgang war (braucht ≥3 Wochen Daten) | Anstieg > **80 %** UND Vorwoche < Woche davor | 150 % Anstieg |
| **load_spike_immediate** (`detectLoadSpikeImmediate`) | load | Direkter Belastungssprung ohne Jo-Jo-Vorbedingung: letzte Woche vs. Ø der letzten 4 Wochen (braucht ≥4 Wochen Daten) | Verhältnis > **1,5** | Verhältnis 2,2 |
| **resting_hr_rising** (`detectRestingHrRising`) | recovery | Steigung (least-squares) des Ruhepuls-Trends über die 28 Tage | Steigung > **0,15 bpm/Tag** | 0,4 bpm/Tag |
| **sleep_debt** (`detectSleepDebt`) | recovery | Schlaf der letzten 7 Nächte (mind. 3 Nächte mit Daten nötig) | Ø < **6,5 h** ODER ≥2 Nächte < **5 h** | Ø 4,5 h bzw. 4 kurze Nächte |
| **hrv_drop** (`detectHrvDrop`) | recovery | (a) Trend-Delta (2. Hälfte vs. 1. Hälfte der 28 Tage) ODER (b) 3-Tage-Rolling-Average der letzten HRV-Werte vs. 28-Tage-Durchschnitt (ersetzt das frühere, rauschanfällige Einzeltag-Kriterium) | Trend-Delta < **−3** ODER Rolling-Ratio < **0,7** | Trend-Delta −8 bzw. Ratio 0,5 |
| **acute_overload** (`detectAcuteOverload`) | load | ATL/CTL-Verhältnis am jüngsten Tag mit vorhandenen Werten | Ratio > **1,3** | Ratio 1,8 |

Bei Flags mit zwei unabhängigen Kriterien (sleep_debt, hrv_drop) gilt
`triggered = A ODER B`, `severity = max(severityA, severityB)`.

Der zugrunde liegende Trend (`computeTrend`, `src/form-analysis.js:487`) ist eine
simple lineare Regression (Steigung/Tag) plus Vergleich 1. vs. 2. Hälfte des
Zeitraums – braucht mindestens 4 gültige Datenpunkte, sonst `null`.

## 3. Kategorie-Scores und Status-Ableitung

Statt die Flags nur zu zählen, wird pro Kategorie der **Maximalwert** (nicht die
Summe) der Severities gebildet – bewusst, damit z.B. sleep_debt, resting_hr_rising
und hrv_drop, die oft dieselbe Ursache haben, sich nicht gegenseitig hochpuschen:

```
recoveryScore = max(severity[sleep_debt], severity[resting_hr_rising], severity[hrv_drop])
loadScore     = max(severity[acute_overload], severity[load_spike], severity[load_spike_immediate])
combinedScore = recoveryScore * 0.6 + loadScore * 0.6
                + (recoveryScore > 0.3 UND loadScore > 0.3 ? 0.3 Bonus : 0)

combinedScore < 0.3  → grün
combinedScore < 0.7  → gelb
sonst                → rot
```

Die Gewichte/Schwellen (`RECOVERY_SCORE_WEIGHT`, `LOAD_SCORE_WEIGHT`,
`CORRELATION_BONUS_SCORE_THRESHOLD`, `CORRELATION_BONUS`,
`STATUS_GREEN_MAX_SCORE`, `STATUS_YELLOW_MAX_SCORE`) stehen als benannte
Konstanten am Anfang des Abschnitts in `src/form-analysis.js`.

Da ein einzelner Kategorie-Score maximal mit 0,6 gewichtet wird, kann eine
Kategorie allein (auch bei Severity 1,0) nie „rot“ auslösen – das braucht entweder
den Korrelations-Bonus (beide Kategorien gleichzeitig spürbar erhöht) oder eine
entsprechend hohe Kombination aus beiden Scores.

## 4. Text/Empfehlung (`buildAssessmentText`, `src/form-analysis.js`)

Nur die tatsächlich **ausgelösten** Flags (`triggered === true`) fließen in den
Text ein. Sie werden nach Kategorie unterschieden (`recovery`: sleep_debt,
resting_hr_rising, hrv_drop / `load`: acute_overload, load_spike,
load_spike_immediate) und daraus ein Satz gebaut, ob es eher an Erholung, an
Trainingsumfang oder an beidem liegt. Zusätzlich wird jedes Flag-Label je nach
Severity qualifiziert: `severity > 0.7` → „deutlich“, `severity < 0.3` →
„leicht“, dazwischen kein Zusatz. Empfehlungstexte pro Flag stehen fix in
`FLAG_INFO`.

## 5. Veröffentlichung (`src/recovery-note.js`)

`writeDailyRecoveryNote` ruft `buildRecentFormAnalysis` auf, baut daraus Titel
`Formcheck 🟢/🟡/🔴` + Beschreibungstext (`summary`/`recommendation`, unverändert
Strings) und schreibt das per `upsertIntervalsNote` als eigenes Kalender-Event
(`externalId: formcheck-<Datum>`, Farbe grün/orange/rot) – separat vom
wöchentlichen „Wochenvergleich“.

## Kurzfassung

Kein reines Zählen von Ja/Nein-Flags mehr, sondern ein gewichtetes
Severity-Scoring (0-1 pro Detektor) über 6 Red-Flag-Checks auf Ruhepuls, HRV,
Schlaf, Trainingslast-Sprüngen (Jo-Jo und direkt) und ATL/CTL-Verhältnis. Der
Maximalwert je Kategorie (recovery/load) bestimmt zusammen mit einem kleinen
Korrelations-Bonus die Ampelfarbe – ein einzelnes, knapp über der Schwelle
liegendes Symptom schlägt dadurch nicht mehr automatisch rot an.
