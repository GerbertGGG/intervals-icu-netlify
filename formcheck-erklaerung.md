# Formcheck – wie die Berechnung funktioniert

Der „Formcheck“ ist die tägliche Ampel-Bewertung (🟢/🟡/🔴), die jede Nacht als
Notiz im intervals.icu-Kalender landet. Die Logik steckt in zwei Dateien:

- `src/form-analysis.js` – Datenaufbereitung + Ampel-Logik (`assessRecoveryStatus`)
- `src/recovery-note.js` – schreibt das Ergebnis als Kalender-Notiz

## 1. Datenbasis (`buildRecentFormAnalysis`, `src/form-analysis.js:680`)

Für die letzten **28 Tage** (`RECOVERY_NOTE_DAYS` in `recovery-note.js:4`) werden
Aktivitäten + Wellness-Daten (Ruhepuls, HRV, Schlaf, CTL/ATL) von intervals.icu
geladen und zu Wochen-Buckets sowie Trend-Zeitreihen aggregiert.

## 2. Die eigentliche Ampel-Logik (`assessRecoveryStatus`, `src/form-analysis.js:643`)

Es werden **5 unabhängige Red-Flag-Detektoren** geprüft, jeder liefert nur ja/nein:

| Flag | Prüfung | Schwellwert |
|---|---|---|
| **load_spike** (`detectLoadSpike`) | „Jo-Jo“-Muster: letzte Woche springt stark hoch, *nachdem* die Vorwoche selbst ein Rückgang war (braucht ≥3 Wochen Daten) | Anstieg `(letzte−vorherige)/vorherige` > **80 %** UND Vorwoche < Woche davor |
| **resting_hr_rising** (`detectRestingHrRising`) | Steigung (least-squares) des Ruhepuls-Trends über die 28 Tage | Steigung > **0,15 bpm/Tag** |
| **sleep_debt** (`detectSleepDebt`) | Schlaf der letzten 7 Nächte (mind. 3 Nächte mit Daten nötig) | Ø < **6,5 h** ODER ≥2 Nächte < **5 h** |
| **hrv_drop** (`detectHrvDrop`) | (a) Trend-Delta (2. Hälfte vs. 1. Hälfte der 28 Tage) ODER (b) ein Einzeltag der letzten 7 liegt >30 % unter dem 28-Tage-Durchschnitt | Trend-Delta < **−3** ODER Tageswert < Ø·**0,7** |
| **acute_overload** (`detectAcuteOverload`) | ATL/CTL-Verhältnis am jüngsten Tag mit vorhandenen Werten | Ratio > **1,3** |

Der zugrunde liegende Trend (`computeTrend`, `src/form-analysis.js:487`) ist eine
simple lineare Regression (Steigung/Tag) plus Vergleich 1. vs. 2. Hälfte des
Zeitraums – braucht mindestens 4 gültige Datenpunkte, sonst `null`.

## 3. Status-Ableitung

```
0 Flags  → grün
1 Flag   → gelb
2+ Flags → rot
```

(`src/form-analysis.js:652`: `flags.length === 0 ? "grün" : flags.length === 1 ? "gelb" : "rot"`)

## 4. Text/Empfehlung (`buildAssessmentText`, `src/form-analysis.js:613`)

Die Flags werden nach Kategorie unterschieden (`recovery`: sleep_debt,
resting_hr_rising, hrv_drop / `load`: acute_overload, load_spike) und daraus ein
Satz gebaut, ob es eher an Erholung, an Trainingsumfang oder an beidem liegt.
Empfehlungstexte pro Flag stehen fix in `FLAG_INFO` (`src/form-analysis.js:525`).

## 5. Veröffentlichung (`src/recovery-note.js`)

`writeDailyRecoveryNote` ruft `buildRecentFormAnalysis` auf, baut daraus Titel
`Formcheck 🟢/🟡/🔴` + Beschreibungstext und schreibt das per `upsertIntervalsNote`
als eigenes Kalender-Event (`externalId: formcheck-<Datum>`, Farbe
grün/orange/rot) – separat vom wöchentlichen „Wochenvergleich“.

## Kurzfassung

Kein einzelner Score, sondern ein Regelwerk aus 5 harten Schwellwert-Checks auf
Ruhepuls, HRV, Schlaf, Trainingslast-Sprüngen und ATL/CTL-Verhältnis – die Anzahl
der ausgelösten Flags bestimmt die Ampelfarbe.
