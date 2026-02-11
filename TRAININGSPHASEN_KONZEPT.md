# Konzept: Trainingsphasen, Block-Logik und Progression (Ist-Stand)

## Zielbild
- Der Worker steuert Tagesempfehlungen über `BASE` → `BUILD` → `RACE` → `RESET`.
- Reize werden über **Zeit/Umfang** progressiv gesteuert (nicht primär über Pace-Erhöhung).
- Eventnähe, Lastsignale und Robustheit beeinflussen, ob progressiv, deloaded oder konservativ gearbeitet wird.

## Aktueller Stand im Code (`src/index.js`)

### 1) Block-Status + Transition
`determineBlockState(...)` entscheidet täglich anhand von:
- Event-Datum + Distanz (`weeksToEvent`, `eventDistance`)
- historischer Last (Run/Bike/Aerobic Floors)
- Fatigue-Signalen (Ramp, Monotony, Strain, ACWR)
- Key-Compliance inkl. Key-Spacing
- Robustheit (Kraft-/Stabi-Minuten)

Genutzte Blöcke:
- `BASE`
- `BUILD`
- `RACE`
- `RESET`

### 2) Moduslogik (EVENT/OPEN)
Über kommende Events wird ein Modus gewählt:
- `EVENT:RUN`
- `EVENT:BIKE`
- `OPEN`

Daraus folgen spezifische Floors/Policies und die spätere Tagesbewertung.

### 3) Run-Floor-State + Overlays
`computeRunFloorState(...)` ergänzt den Blockzustand um operative Overlays:
- `NORMAL`
- `DELOAD`
- `TAPER`
- `RECOVER_OVERLAY`

Diese Overlays beeinflussen u. a.:
- effektive Floor-Ziele
- Key-Caps (z. B. strenger bei Taper/Fatigue)
- Tagesempfehlung im Report

### 4) Key-Rules je Block + Distanz
`getKeyRules(block, eventDistance, weeksToEvent)` liefert u. a.:
- erlaubte Key-Typen
- bevorzugte Key-Typen
- erwartete/maximale Keys pro Woche
- Verbote je Phase

Die Regelbasis ist differenziert nach Distanz (`5k`, `10k`, `hm`, `m`) und Block.

### 5) Progressions-Engine
Die Progression wird durch diese Bausteine erzeugt:
- `PHASE_MAX_MINUTES` (Block-/Distanz-spezifische Obergrenzen pro Reiztyp)
- `computeProgressionTarget(...)` (Wochenziel in Minuten)
- Deload-Rhythmus via `PROGRESSION_DELOAD_EVERY_WEEKS` (aktuell 4)
- Race-spezifisches Budget (`RACEPACE_BUDGET_DAYS`)

Ausgabe im Daily-Report:
- Key-Format
- Wochenziel (Minuten)
- Block-Maximum
- Coaching-Notiz (z. B. Deload-Hinweis)

### 6) Coach-Hinweise im Tagesreport
`buildKeySuggestion(...)` + `buildProgressionSuggestion(...)` erzeugen konkrete Hinweise wie:
- nächster Key-Reiz
- Progressionsumfang dieser Woche
- Belastungs-/Sicherheitskontext

## Wichtige Leitplanken (derzeit)
- Fatigue/Overload begrenzt Intensitätsfreigaben (Key-Cap sinkt dynamisch).
- In Taper/Recover werden Key-Einheiten stark begrenzt oder deaktiviert.
- Progression in Deload-Wochen wird auf reduzierten Umfang gekappt.
- Distanz- und phasenspezifische Reiztypen werden bevorzugt, nicht nur „einheitsmäßig“ verteilt.

## Offene Weiterentwicklungen
1. `PHASE_MAX_MINUTES` aus Code in konfigurierbare Datenquelle (KV/JSON) auslagern.
2. Reiztyp → konkretes Workout-Template mappen (`3x10`, `5x3`, `2x20` etc.).
3. Athlete-Level (Einsteiger/Fortgeschritten/Elite) als Multiplikator integrieren.
4. Bike-spezifische Progressionslogik analog zur Run-Progression vertiefen.
5. Block-State-Persistenz robuster/transparent dokumentieren (inkl. Recovery nach Deploy/Reset).
