# Berechnungen im Worker – Prüfdokumentation

Stand: Code auf `main` bei Commit `8f4f1ee` (03.10.2026). Dieses Dokument beschreibt **nur, was im Code tatsächlich gerechnet wird**, mit Datei und Zeile, damit jede Formel gegen die Quelle geprüft werden kann. Wo ich beim Lesen etwas Auffälliges gefunden habe, steht ein Verweis **[P#]** und die Details stehen in Kapitel 12.

Alle Zahlenbeispiele wurden mit dem echten Code ausgeführt (`node`), nicht von Hand gerechnet.

**Nicht hier berechnet, sondern von Intervals.icu geliefert:** `icu_training_load` (Load/TSS je Aktivität), `ctl`, `atl`, `rampRate`, `decoupling`, HF-Zonen, FTP, Schwellenpace. Der Worker rechnet damit nur weiter. Intervals.icu definiert CTL/ATL als exponentiell gewichtete Mittel der Tageslast (üblich: 42 bzw. 7 Tage); das ist dort einstellbar und im Worker nicht nachgebaut.

**Nicht mehr im Code, aber noch in `doc.txt` beschrieben:** EF, „VDOT-like = EF·1200“, Drift, Motor-Index, RunFloor-TSS, GA/Key-Klassifikation, Daily-/Montags-Report. Ich habe keine dieser Berechnungen in `src/` gefunden (`grep` nach `bikeSubFactor`, `motor`, `velocity_smooth`, `runfloor` ohne Treffer). `doc.txt` ist an dieser Stelle veraltet **[P14]**.

---

## Inhalt

1. Grundlagen: Klassifikation, Datum, Rundung
2. VDOT (`src/vdot.js`)
3. Blockphasen, Zielrennen, Long-Run-Plan
4. Formcheck / Erholungs-Ampel (`src/form-analysis.js`)
5. Wochenvergleich (`src/weekly-progress.js`)
6. Dashboard: Belastung, Bereitschaft, Körperwerte
7. Dashboard: Wochen, Longruns, Rennplan, Triathlon
8. Ernährung (Yazio-Sync und Auswertung)
9. Widget (`src/widget.js`)
10. Wetter (`src/weather.js`)
11. Freitext-Parser (Heißhunger, Hüfte, Workout-Schritte)
12. Prüfhinweise (Auffälligkeiten)

---

## 1. Grundlagen

### 1.1 Aktivitäten klassifizieren (`src/activity-utils.js`)

| Funktion | Regel |
|---|---|
| `isRun` (10) | `type` (klein geschrieben) ist `run`/`running` oder **enthält** `run`, `laufen`, `treadmill` |
| `isTreadmill` (21) | `type == "virtualrun"` oder enthält `treadmill` |
| `isBike` (28) | `type` enthält `ride`, `bike`, `cycling`, `rad`, `velo` |
| `isRaceActivity` (37) | Tag beginnt mit `race:` **oder** `category` ∈ {`RACE`,`RACE_A`,`A_RACE`} **oder** Name/Titel enthält als ganzes Wort `race`, `wettkampf`, `competition` **[P9]** |
| `isIntervalActivity` (65) | irgendein Tag beginnt (ohne führendes `#`) mit `interval` |
| `hasIntervalTextSignal` (83) | Name+Beschreibung enthält Wort `intervall`/`interval` **oder** Muster `\d+\s*[x×]\s*\d+` (z. B. „5x1000“) |
| `isVdotExcluded` (104) | Tag ist (ohne `#`) genau `novdot` |
| `activityLoad` (5) | `icu_training_load`, sonst `training_load`, sonst `load`; nur Werte > 0, sonst 0 |
| `activityDay` (1) | erste 10 Zeichen von `start_date_local`, sonst `start_date` |

`sportOf` im Dashboard (`src/dashboard.js:115`) prüft in dieser Reihenfolge: Typ enthält `swim` → Schwimmen; `isBike` → Rad; `isRun` → Laufen; Typ enthält `weight`/`strength`/`kraft` → Kraft; sonst „other“.

### 1.2 Datum und Zeit (`src/date-utils.js`)

- Alle Tagesrechnungen laufen über UTC-Mitternacht (`T00:00:00Z`). Es gibt keine Sommerzeit-Korrektur, weil nur ganze Tage addiert werden.
- `weeksBetween(a, b) = (b − a) / (7·86400000 ms)` (Bruchteil, nicht gerundet).
- `daysBetween(a, b) = (b − a) / 86400000` (ebenfalls Bruchteil; bei reinen Datumswerten ganzzahlig).
- Berlin-Zeit (`isoDateBerlin`, 5) wird für „heute“ im Dashboard/Widget und für das Cron-Fenster verwendet. Der Sync (`src/index.js:163`) nimmt dagegen das UTC-Datum (`isoDate(new Date())`). Weil er nur zwischen 07:00 und 23:58 Berlin läuft (05:00–22:58 UTC), ist das UTC-Datum zu allen Sync-Zeiten identisch mit dem Berliner Datum.
- Woche = Montag–Sonntag (`mondayOf` in `dashboard.js:48`, `lastCompletedWeek` in `weekly-progress.js:33`). Die Formcheck-Wochen (4.2) sind dagegen **gleitende 7-Tage-Blöcke ab heute rückwärts**, nicht Kalenderwochen.

### 1.3 Zeitplan (`wrangler.toml`, `src/index.js`)

- Cron `*/15 5-22 * * *` und `58 21-22 * * *` (UTC).
- Jeder Tick im Fenster 07:00–23:59 Berlin: Yazio-Sync für heute (um 07:xx zusätzlich gestern).
- Intervals-Sync nur bei Minute :00/:30 und 07:00–21:00 Berlin, immer für „vor 2 Tagen bis heute“.
- Montag, erster Lauf des Tages (07:00–07:29): Wochenvergleich + E-Mail-Report. Täglich erster Lauf: Formcheck-Notiz.

---

## 2. VDOT (`src/vdot.js`)

### 2.1 Daniels-Formel (`computeVdotFromRaceTime`, Zeile 63)

Eingabe: Distanz `d` (m), Zeit `T` (s). Gültig nur bei `d ≥ 400` und `T ≥ 60`.

```
v   = d / T · 60                                   [m/min]
t   = T / 60                                       [min]
VO2 = −4,60 + 0,182258·v + 0,000104·v²
%VO2max = 0,8 + 0,1894393·e^(−0,012778·t) + 0,2989558·e^(−0,1932605·t)
VDOT = VO2 / %VO2max
```

Ergebnis wird verworfen (null), wenn `VDOT < 20` oder `> 90`; sonst auf 1 Nachkommastelle gerundet. Die Konstanten entsprechen den veröffentlichten Daniels/Gilbert-Gleichungen.

Beispiele (ausgeführt):

| Leistung | VDOT |
|---|---|
| 10 km in 50:00 | 40,0 |
| 5 km in 25:00 | 38,3 |
| Halbmarathon (21 097 m) in 2:00:00 | 36,5 |
| „Halbmarathon“ mit 21 300 m GPS-Distanz in 2:00:00 | 36,9 **[P8]** |

### 2.2 Renn-VDOT (`computeRaceVdot`, 81)

- Betrachtet Läufe, die `isRun`, `isRaceActivity` und nicht `#novdot` sind.
- Fenster: **180 Tage** bis einschließlich „heute“ (Ankertag).
- Voraussetzung je Lauf: Distanz ≥ 800 m, Zeit ≥ 60 s. Zeit = `moving_time`, sonst `elapsed_time`; Distanz = `distance`, sonst `icu_distance` (also die vom Gerät gemessene, nicht die offizielle Distanz) **[P8]**.
- Ergebnis: das **höchste** VDOT aller Rennen im Fenster (`best.vdot`), mit Renndatum.

### 2.3 Maximalpuls auflösen (`resolveMaxHrDetailed`, 497)

Reihenfolge, der erste Treffer gewinnt:

1. Umgebungsvariable `MAX_HR` oder `ATHLETE_MAX_HR`
2. KV-Cache (24 h) des Intervals-Run-Werts
3. Live-Abruf `sport-settings` → `max_hr` des Typs „Run“ (nur wenn > 100), danach 24 h gecacht
4. `_estimateMaxHrFromActivities` (243): höchster `max_heartrate` aller geladenen Aktivitäten × 1,05, nur wenn dieser Höchstwert > 100
5. Heuristik: höchster **Durchschnitts**puls × 1,2, nur wenn > 80

Die Quelle wird in `vdotDebug.resolvedMaxHrSource` ausgegeben.

### 2.4 Trainings-VDOT aus Puls und Pace (`_vdotDetailsFromTrainingActivity`, 256)

Je Lauf, mit `maxHr`:

```
hrPct     = avgHr / maxHr
%VO2max   = 1,154 · hrPct − 0,15
v         = dist / moving_time · 60        [m/min]
VO2       = −4,60 + 0,182258·v + 0,000104·v²
VDOT      = VO2 / %VO2max
```

Der Lauf wird **ausgeschlossen** (mit Grund), wenn eine dieser Bedingungen gilt (in dieser Prüfreihenfolge):

| Bedingung | Grund |
|---|---|
| Distanz < 2000 m | `distance<2000m` |
| Zeit < 600 s | `movingTime<600s` |
| `avgHr ≤ 0` | `avgHr<=0` |
| `maxHr ≤ 100` | `maxHr<=100` |
| `hrPct < 0,55` oder `> 0,87` | `hrPct<…` |
| `%VO2max ≤ 0,3` oder `≥ 1,0` | `pctVo2max…` |
| `VO2 ≤ 0` | `vo2<=0` |
| VDOT außerhalb 20–90 | `vdot out of range` |

Die Gerade `1,154·hrPct − 0,15` ist ein im Kommentar als „aus Daniels E/M/T-Ankerpunkten abgeleitet“ bezeichneter Näherungsansatz, keine Literaturformel. Beispiel: 65 % HFmax → 60 % VO2max; 80 % → 77,3 %; 87 % → 85,4 %. Verwendet wird **keine Ruhepuls-Reserve**, nur `avgHr/maxHr`.

### 2.5 Fenster-Schätzung (`estimateTrainingVdotForWindow`, 330)

- Nimmt Läufe in `[von, bis]`, die `isRun`, **nicht** Rennen, **nicht** Laufband/VirtualRun, **nicht** `#intervall*`-getaggt und nicht `#novdot` sind.
- Je Lauf der Wert aus 2.4; **Median** der gültigen Werte (bei gerader Anzahl Mittel der beiden mittleren), auf 1 Nachkommastelle.
- Mindestens `minCount = 2` gültige Läufe, sonst `null`.
- Das 28-Tage-Fenster (`computeTrainingVdotFromActivities`, 345) ist „Ankertag − 28 Tage bis Ankertag“.

### 2.6 Pace-Benchmarks (`computeVdotFromPaceBenchmarks`, 228; Abruf `intervals-client.js:357`)

- Abruf: bestes Segment über 1000/5000/10000/21097 m der letzten **56 Tage** (`activity-pace-curves`), 7 Tage in KV gecacht, wird nur bei Montag-Sync oder Schreibzugriff geholt.
- VDOT = **Maximum** der VDOTs über die vier Distanzen (mit 2.1 und der jeweiligen Segmentzeit) **[P7]**.

### 2.7 Korrekturfaktoren (`vdot.js` 6–38, 125, 150, 201)

Drei Größen, alle auf **[0,90 ; 1,10]** begrenzt (`CORRECTION_MIN/MAX_FACTOR`):

**a) Rennfaktor** (`updateRaceCorrectionFactor`, nur bei Schreibzugriff, einmal je Renndatum):

```
vorhergesagt = Trainings-VDOT (2.5) aus den 14 Tagen vor dem Rennen (Tag −14 bis −1)
roh          = clamp(Renn-VDOT / vorhergesagt, 0,9 ; 1,1)
neu          = clamp(alt·0,5 + roh·0,5, 0,9 ; 1,1)      (alt = 1 beim ersten Rennen)
```

Gibt es keinen vorhergesagten Wert, bleibt der Faktor unverändert, das Rennen gilt aber als verarbeitet. Weicht `roh` um mehr als 0,05 von 1 ab, wird `raceCorrectionWarning` gesetzt (nur Anzeige).

**b) Zerfall des Rennfaktors** (`applyCorrectionDecay`, 32), gerechnet beim Lesen:

```
Tage seit Rennen ≤ 90   → Faktor unverändert
90 < Tage < 180         → Faktor + (1 − Faktor)·(Tage − 90)/90
Tage ≥ 180              → 1
```

**c) Drift-Faktor** (`updatePaceBenchmarkDrift`, nur bei Schreibzugriff, einmal je Benchmark-Zeitstempel):

```
roh  = clamp(Pace-Benchmark-VDOT / HF-Trainings-VDOT, 0,9 ; 1,1)
neu  = clamp(alt·0,95 + roh·0,05, 0,9 ; 1,1)
```

**Kombination** (`combineCorrectionFactors`, 125): `clamp(Rennfaktor_zerfallen · Driftfaktor, 0,9 ; 1,1)`.

### 2.8 Aktueller VDOT (`computeAndPersistRealVdot`, 521)

1. `trainVdot` = HF-Trainings-VDOT (28 Tage). Gibt es keinen, wird der Pace-Benchmark-VDOT genommen.
2. Ist `Korrekturfaktor ≠ 1`: `trainVdot = round(trainVdot · Korrekturfaktor, 1)`. **Das gilt auch für den Pace-Benchmark-Fallback [P7].**
3. `aktuell = min(Renn-VDOT, trainVdot)`, wenn beide vorhanden; sonst der vorhandene. Quelle = `"training"` oder `"race"`. Konservative Auslegung: gelaufene Form kann die Rennform nicht übersteigen.
4. **Absturz-Bremse:** Fällt der Wert gegenüber dem gespeicherten um mehr als 8, wird `gespeichert − 8` verwendet. Einen Anstieg begrenzt nichts.
5. Ohne neue Daten wird der gespeicherte Wert zurückgegeben.
6. Rundung auf 1 Nachkommastelle; Speicherung nur bei `persistLatest` (letzter Tag eines Schreib-Syncs).

### 2.9 VDOT des heutigen Laufs (`todayRunVdot`, 591–634)

Median der Werte aus 2.4 für heutige Läufe (kein Rennen, kein Intervall, kein `#novdot`; Laufband ist hier **nicht** ausgeschlossen), multipliziert mit dem Korrekturfaktor, 1 Nachkommastelle. Hier genügt **ein** Lauf (im Fenster sind es zwei) **[P20]**. Der Wert wird seit Commit #948 nicht mehr in Intervals-Wellness geschrieben, bleibt aber im Sync-Ergebnis.

### 2.10 Pace-Zonen (`paceTargetsFromVdot`, 398)

Zielgeschwindigkeit je Zone aus `VO2_Ziel = Anteil · VDOT`, umgekehrt aus der Quadratformel:

```
a = 0,000104;  b = 0,182258;  c = −(4,60 + VO2_Ziel)
v = (−b + √(b² − 4ac)) / (2a)                [m/min]
Pace [s/km] = round(60000 / v)
```

| Zone | Anteil am VDOT |
|---|---|
| Easy (E) | 0,70 |
| Marathon (M) | 0,84 |
| Threshold (T) | 0,88 |
| Interval (I) | 0,975 |
| Repetition (R) | 1,05 |

Ergebnisse (ausgeführt):

| VDOT | E | M | T | I | R |
|---|---|---|---|---|---|
| 36,5 | 6:34 | 5:41 | 5:29 | 5:03 | 4:45 |
| 45 | 5:34 | 4:49 | 4:38 | 4:16 | 4:01 |
| 50 | 5:07 | 4:25 | 4:15 | 3:55 | 3:41 |

Zum Vergleich mit den Daniels-Tabellen (VDOT 50), aus dem Gedächtnis und bitte gegen die Originaltabelle prüfen: T ≈ 4:15/km, I ≈ 3:55/km und M ≈ 4:26/km passen; R liegt rechnerisch bei 3:41/km, die Tabelle nennt R über 400 m (etwa 1:30–1:32, also rund 3:45–3:50/km). Die Zonen sind eine gleichmäßige %VDOT-Näherung, nicht Daniels’ Tabellenwerte.

### 2.11 Rennzeit-Prognose (`predictRaceTimeSeconds`, 427)

Weil %VO2max von der Dauer abhängt, wird iteriert (12 Schritte):

```
t₀ = Distanz_km · 4                          [min, Startschätzung 4 min/km]
wiederhole 12×:
    %VO2max(t) = 0,8 + 0,1894393·e^(−0,012778·t) + 0,2989558·e^(−0,1932605·t)
    VO2   = VDOT · %VO2max(t)
    v     = Geschwindigkeit(VO2)               (Quadratformel wie 2.10)
    t     = Distanz / v
Zeit [s] = round(t · 60)
```

Distanzen: 5 000, 10 000, 21 097, 42 195 m. Beispiel VDOT 36,5 → 5 km 26:02, 10 km 54:04, HM 1:59:53, M 4:07:31 (ausgeführt; HM-Prognose passt zur Rückrechnung in 2.1).

### 2.12 Wochen-VDOT im Formcheck (`buildWeekVdot`, `form-analysis.js:505`)

Je 7-Tage-Block: bestes Renn-VDOT der Woche (Rennen, Distanz ≥ 800 m, Zeit ≥ 60 s) und Fenster-Schätzung (2.5, ohne Korrekturfaktor) → `min` beider, sonst der vorhandene **[P5]**.

---

## 3. Blockphasen, Zielrennen, Long-Run-Plan

### 3.1 Blocklängen je Distanz (`block-phase.js:22`, Wochen)

| Distanz | Base | Build | Race | Taper | Reset |
|---|---|---|---|---|---|
| 5 km | 10 | 8 | 6 | 1 | 2 |
| 10 km | 10 | 8 | 6 | 1 | 2 |
| HM | 12 | 8 | 8 | 2 | 3 |
| M | 16 | 10 | 8 | 2 | 4 |

- `planStartWeeks = Base + Build + Race + Taper` (HM: 30)
- `raceStartWeeks = Race + Taper` (HM: 10)
- Blockdauer in Tagen = `max(7, round(Wochen·7))`; Min = Max (feste Dauer). RACE = Race+Taper, RESET = Reset.
- Distanz-Erkennung `normalizeEventDistance` (29): Text mit `5k`/`10k`/`half|hm|halb`/`marathon|42`, sonst Zahl (< 1000 → km, sonst m) mit Toleranzbändern 4,9–5,1 / 9,5–10,5 / 20,5–21,5 / 41–43 km. Unbekannt → „10k“.

### 3.2 Zustandsautomat (`determineBlockState`, 169)

Eingabe: heute, Eventdatum, Distanz, Vorzustand (Block, Welle, Startdatum). `weeksToEvent = weeksBetween(heute, Event)`.

Entscheidungen in dieser Reihenfolge:

1. Kein gültiges Eventdatum → `BASE`, Welle 0.
2. War der gespeicherte Block `RACE` und `weeksToEvent > raceStartWeeks` → Zustand ungültig; Startdatum = heute, Rückfall `BUILD` falls `weeksToEvent ≤ 12`, sonst `BASE`.
3. `0 ≤ weeksToEvent ≤ 4` → `RACE` (Startdatum bleibt, wenn schon `RACE`).
4. `weeksToEvent < 0` → `BASE` („Event vorbei“, Startdatum heute).
5. `weeksToEvent > planStartWeeks` → `BASE` („freie Vorphase“).
6. Sonst: Welle = 1, wenn `weeksToEvent > 20`; Welle 2 bleibt Welle 2; bei `weeksToEvent ≤ 8` wird Welle 1 auf 0 gesetzt. Block = Vorblock, sonst `BUILD` (wenn `≤ raceStartWeeks`) oder `BASE`.
7. `weeksToEvent ≤ raceStartWeeks + 6` und Block `BASE` → erzwungen `BUILD` (HM: ab 16 Wochen).
8. `weeksToEvent ≤ raceStartWeeks` und Block ≠ `RACE` → `RACE`.
9. Ist die Mindestdauer des Blocks noch nicht erreicht → bleibt. Sonst Wechsel zu `nextSuggestedBlock`: `BASE→BUILD`, `BUILD→RACE` (wenn `weeksToEvent ≤ raceStartWeeks`, sonst `BASE`), alles andere → `BASE`. Wechselt der Block dabei zurück auf `BASE`, während Welle 1 aktiv ist, wird daraus Welle 2.

`weeksToEvent`-Plausibilitätsprüfung (`computeWeeksToEvent`, 125) berechnet im Fehlerfall exakt dieselbe Zahl neu und ändert nichts **[P19]**. Der Block wird nur vom Sync (`sync.js`) fortgeschrieben und in KV gespeichert; manuelle Overrides (`applyManualBlock*`) setzen Block bzw. Startdatum fest.

### 3.3 Zielrennen und Plan-Raster (`goal-race.js`, `computeGoalRaceInfo` 144)

Das Raster wird **rein datumsbasiert** aus dem Renntag zurückgerechnet:

```
raceBlockWeeks = Race + Taper
planStart  = Renntag − (Base + Build + raceBlockWeeks) Wochen
buildStart = Renntag − (Build + raceBlockWeeks) Wochen
raceStart  = Renntag − raceBlockWeeks Wochen
empfohlen: Renntag vorbei → RESET; ≥ raceStart → RACE; ≥ buildStart → BUILD; ≥ planStart → BASE; sonst null (freie Vorphase)
```

HM: Plan ab 30, BUILD ab 18, RACE ab 10 Wochen vor dem Rennen. Das Raster weicht systembedingt vom Zustandsautomaten aus 3.2 ab **[P4]**.

**Ziel aus dem Kalender** (`deriveAutoGoalFromRaces`, 50): nächster A-Rennen-Eintrag ab heute; Zielzeit = `time_target`, sonst `moving_time` des Eintrags (Sekunden, > 0). Hat der Eintrag keine Zielzeit, wird eine manuell gesetzte (`PUT /goal`) **nur dann** übernommen, wenn sie dasselbe Datum hat.

**A-Rennen-Erkennung** (`event-utils.js`): Kategorie ∈ {`RACE_A`, `A_RACE`, `A-RACE`, `RACE A`, `A`} oder kompakt `RACEA`/`ARACE`; alternativ ein Prioritätsfeld = „A“ **und** Typ enthält „RACE“.

**Prognose gegen Ziel** (in `computeGoalRaceInfo`): `predictedSecs` aus 2.11 für die Zieldistanz bei aktuellem VDOT; `gapSecs = predictedSecs − targetSecs`; `faster = gapSecs < 0`.

**Triathlon** (`block-phase.js:50`, `parseTriathlonEvent`): Format aus Name+Typ+Beschreibung per Regex, **erster Treffer** in der Reihenfolge Sprint, Olympisch, Mittel, Lang; Standard Olympisch **[P10]**.

| Format | Schwimmen | Rad | Lauf | Lauf-Distanz für Blocklänge |
|---|---|---|---|---|
| Sprint | 0,75 km | 20 km | 5 km | 5k |
| Olympisch | 1,5 | 40 | 10 | 10k |
| Mittel | 1,9 | 90 | 21,0975 | HM |
| Lang | 3,8 | 180 | 42,195 | M |

Zeitziele aus der Beschreibung: Wort `lauf|run`, `schwimm|swim`, `rad|bike` direkt gefolgt von `m:ss` oder `h:mm:ss`; zwei Teile werden als **Minuten:Sekunden** gelesen, drei als Stunden:Minuten:Sekunden. Watt: `rad|bike … <2–3 Ziffern> w|watt`.

### 3.4 Long-Run-Ziel (`long-run-plan.js`)

**Spitzenlauf** (`getPeakLongRunKm`, 74), `D` = Renndistanz in km:

| Bereich | generisch |
|---|---|
| `D > 35` | `min(32, 0,75·D)` |
| `18 ≤ D ≤ 25` | `min(20, 0,90·D)` |
| `8 ≤ D ≤ 12` | `min(16, 1,5·D)` |
| `D < 8` | `min(12, 2,2·D)` |
| dazwischen | `D < 18` → 10-km-Regel, sonst HM-Regel |

Ist der längste Lauf der letzten 28 Tage **länger** als der generische Wert, gilt `längster + 2 km`. Alles wird auf 0,5 km gerundet.
Ergebnisse (ausgeführt, ohne bzw. mit 20 km Ausgangslauf): 5 km → 11 / 22; 10 km → 15 / 22; HM → 19 / 22; Marathon → 31,5 / 31,5 (bei 20 km Ausgangslauf bleibt es beim Marathon bei 31,5, weil 20 nicht über dem generischen Wert liegt).

**Wochentabelle** (`buildLongRunProgressionWeeks`, 155):

```
Basis-Montag        = Montag der aktuellen Woche
Wochen bis Rennen   = floor(weeksBetween(heute, Rennen))
Aufbauwochen        = Wochen bis Rennen − Taperwochen    (≤ 0 → Basis halten)
Fortschrittswochen  = Aufbauwochen − floor(Aufbauwochen / 4)
Steigerung/Woche    = max(0, Spitze − Basis) / max(1, Fortschrittswochen)
                      (auf 2,5 km/Woche gedeckelt → timelineAmbitious = true)
Woche w = 1…Aufbau: w durch 4 teilbar → round½(Vorwoche · 0,75)
                    sonst            → round½(min(Spitze, Vorwoche + Steigerung))
Taperwoche k: round½(Spitze · Anteil[k])   Anteile: M [0,65; 0,45; 0,25], HM [0,6; 0,35], 5k/10k [0,5]
Taperwochen: M 3, HM 2, 10k 1, 5k 1
```

**Wichtig: Der Plan erreicht die Spitze nicht [P1].** Nach jeder Rücknahmewoche geht es von dem **reduzierten** Wert aus weiter. Ausgeführt mit Basis 10 km, HM, 12 Wochen bis zum Rennen (Spitze 19, 10 Aufbau- und 2 Taperwochen): Aufbau `11, 12, 13, 10, 11, 12, 13, 10, 11, 12`, Taper `11,5` und `6,5`. Der höchste Planwert ist 13 km statt 19 km. Beim Marathon (Basis 16, Spitze 31,5, 20 Wochen bis zum Rennen, 17 Aufbauwochen) steigt der Aufbau bis 19 km (Woche 3), endet aber bei 12,5 km; die erste Taperwoche springt dann auf `0,65·31,5 = 20,5` km und ist damit länger als jede Aufbauwoche nach der dritten.

Die Tabelle wird nur neu gebaut, wenn sich Renndatum oder Distanz gegenüber dem gespeicherten Plan geändert haben (`maybeRebuildLongRunPlanOnGoalChange`, 207). „Ist“-Wert = längster Lauf der Woche (`longestRunKmInActivities`/`…InRunRecords`), „Soll“ = Tabelleneintrag, der den Tag enthält (Montag bis Sonntag).

---

## 4. Formcheck / Erholungs-Ampel (`src/form-analysis.js`)

Quelle: `buildRecentFormAnalysis` (1222), Standardfenster **28 Tage** (`newest = heute`, `oldest = heute − 27`). Die tägliche Notiz (`recovery-note.js`) und `GET /api/analysis/recent-form` nutzen dieselbe Berechnung.

### 4.1 Trends (`computeTrend`, 725)

Für Ruhepuls, HRV und Schlafstunden: Punkte `(Tag-Index, Wert)`, nur gültige Werte; mindestens **4** Punkte, sonst `null`.

```
Steigung (bpm/Tag)  = Σ(x−x̄)(y−ȳ) / Σ(x−x̄)²              (lineare Regression)
half               = max(1, floor(n/2))
Δ                  = Ø(letzte half gültige Punkte) − Ø(erste half gültige Punkte)
```

Die Hälften beziehen sich auf die Reihenfolge der **gültigen** Punkte, nicht auf Kalenderhälften. Bei ungerader Zahl bleibt der mittlere Punkt außen vor.

### 4.2 Wochenblöcke (`buildWeekBuckets`, 637; `buildWeekSummary`, 663)

7-Tage-Blöcke rückwärts ab `newest`; ein Rest (`Tage mod 7`) bildet den ältesten, kürzeren Block. Je Block:

- `distanceKm`, `movingTimeMin`, `runSessionCount`: nur Läufe (`isRun`); Rad analog separat (`ride…`).
- `loadSum`: Summe von `activityLoad` **aller Sportarten**.
- **Easy-Pace:** nur Läufe, die kein Rennen/Laufband/Intervall/`#novdot` sind **und** `0,60 ≤ avgHr/maxHr ≤ 0,78`; Pace = Σ Zeit / Σ Distanz · 1000 (Gesamtgewichtung).
- **Monotonie/Strain** (`computeMonotonyStrain`, 651): `tägliche Last` je Tag des Blocks (Ruhetage = 0).

```
Monotonie = Mittelwert / Standardabweichung           (Standardabweichung der Grundgesamtheit, Teiler n)
Strain    = Wochenlast · Monotonie     mit Wochenlast = Mittelwert · Tageszahl
```

Es braucht mindestens 3 Trainingstage im Block und `Standardabweichung > 0`, sonst `null`.

### 4.3 Flags: Schwelle, Deckel, Schweregrad

Alle Detektoren liefern `triggered` (ja/nein) und `severity` (0–1). Zwei Hilfsfunktionen (798/805):

```
severity(v, schwelle, deckel)      = 0 wenn v ≤ schwelle, sonst min(1, (v − schwelle)/(deckel − schwelle))
severityBelow(v, schwelle, deckel) = 0 wenn v ≥ schwelle, sonst min(1, (schwelle − v)/(schwelle − deckel))
```

| Flag | Kategorie | Auslöser (`triggered`) | Schweregrad bei Deckel |
|---|---|---|---|
| `sleep_debt` (906) | Erholung | ≥ 3 Nächte mit Daten in den letzten 7 Tagen **und** (Ø Schlaf < 6,5 h **oder** ≥ 2 Nächte < 5 h) | Ø: 4,5 h → 1; Kurznächte: 4 → 1; Maximum beider |
| `resting_hr_rising` (894) | Erholung | Regressionssteigung > 0,15 bpm/Tag | 0,4 bpm/Tag → 1 |
| `hrv_drop` (940) | Erholung | `Δ` der HRV (4.1) < −3 **oder** 3-Tage-Ø / Fenster-Ø < 0,70 | Δ −8 → 1; Verhältnis 0,5 → 1; Maximum |
| `acute_overload` (978) | Last | ATL/CTL des letzten Tags mit beiden Werten und CTL > 0 **über 1,3** | 1,8 → 1 |
| `load_spike` (857) | Last | letzte Woche > 80 % über Vorwoche (km) **und** Vorwoche < vorvorige Woche; ≥ 3 Wochen, beide Referenzwochen > 0 km | +150 % → 1 |
| `load_spike_immediate` (877) | Last | letzte Woche > 1,5 × Ø der letzten 4 Wochen (inkl. der letzten, km); ≥ 4 Wochen | 2,2 × → 1 |
| `high_monotony` (1025) | Last | Monotonie (rollierend letzte 7 Kalendertage) > 2,0 **und** Wochenlast/(CTL·7) > 0,9, mit letztem gültigen CTL > 0 | Last-Verhältnis 1,3 → 1 |

Details zur HRV (`detectHrvDrop`): Fenster = alle `days` Tage; mindestens 4 gültige Werte; „rolling“ = **letzte 3 gültige** Werte (nicht 3 Kalendertage), Verhältnis = Ø(3) / Ø(alle inkl. dieser 3). Schwellen sind **absolute** HRV-Einheiten, passen also nur für das gleiche Messverfahren **[P11]**.

Die Last-Flags `load_spike*` beziehen sich auf **Lauf-Kilometer**, nicht auf Load. `acute_overload` und `high_monotony` nutzen Intervals-Load/ATL/CTL.

### 4.4 Gesamtbewertung (`assessRecoveryStatus`, 1131)

```
recoveryScore = max(Schweregrade von sleep_debt, resting_hr_rising, hrv_drop)
loadScore     = max(Schweregrade von acute_overload, load_spike, load_spike_immediate, high_monotony)
bonus         = 0,3 wenn recoveryScore > 0,3 UND loadScore > 0,3, sonst 0
combined      = 0,6·recoveryScore + 0,6·loadScore + bonus
Status        = grün (combined < 0,3) | gelb (< 0,7) | rot (≥ 0,7)
```

Folgerungen, die man prüfen sollte **[P17]**:

- Eine einzelne Kategorie erreicht höchstens 0,6, ist also höchstens **gelb**. **Rot setzt beide Kategorien voraus**, und zwar `recoveryScore + loadScore ≥ 0,667` (bei beiden > 0,3).
- Schon **ein** ausgelöstes Flag mit Schweregrad ≥ 0,5 macht den Status gelb (0,6·0,5 = 0,3 ist nicht `< 0,3`).
- Maximalwert insgesamt: 0,6 + 0,6 + 0,3 = 1,5.

### 4.5 Erholungskontext (`classifyRecoveryContext`, 1051)

Drei Marker (nur wenn der jeweilige Trend vorhanden ist): Ruhepuls-Δ ≤ +0,5 bpm, HRV-Δ ≥ −1, Schlaf-Δ ≥ −0,2 h gelten als „stabil“. Anzahl nicht stabiler Marker: 0 → „unauffällig“, ≥ 2 → „auffällig“, sonst „gemischt“; **keine** Marker → „gemischt“. Die Notiz „Belastungssprung erkannt, aber Erholungswerte unauffällig …“ erscheint nur bei einem Last-Flag und Kontext „unauffällig“. Der Kontext verändert Score und Status nicht.

### 4.6 Textbausteine

`severityQualifier`: Schweregrad > 0,7 → „deutlich“, < 0,3 → „leicht“, sonst nichts. Ursache im Satz: nur Erholungs-Flags → „unzureichende Erholung“; nur Last-Flags → „zu schnell gesteigertes Volumen“; beides → „Zusammenspiel“. Zielhinweis: „kombinierter Score < 0,30“.

### 4.7 Ernährungsübersicht im Formcheck (`buildNutritionSummary`, 565)

- Nur Tage mit `Calories > 0`. Trainingstag = Tag mit mindestens einer Aktivität mit `moving_time ≥ 1200 s`.
- Tagesbilanz = `Calories − CalorieGoal`; Mittelwerte gerundet.
- Gewicht = **letztes** im Fenster geloggtes Gewicht. `Protein g/kg = Ø Protein / Gewicht`; `Kohlenhydrate g/kg (Trainingstage) = Ø Carbs an Trainingstagen / Gewicht`.
- Hinweise: Protein < 1,4 g/kg; KH an Trainingstagen < 4 g/kg (Kommentar nennt 5–8 als Richtwert); letzte Woche Ø-Bilanz > +150 kcal („über Budget“) oder < −600 kcal („großes Defizit“); Ø-Carbs Trainingstage − Ruhetage < 20 g („keine Periodisierung“).

---

## 5. Wochenvergleich (`src/weekly-progress.js`)

### 5.1 Zeitraum

`lastCompletedWeek(heute)`: letzter **abgeschlossener** Montag–Sonntag vor „heute“ (sonntags ist es die Vorwoche, nicht die laufende). Vergleich mit der Woche davor. Aktivitäten werden ab `Vorwoche − 180 Tage` geladen (nur für den Max-Puls).

### 5.2 Kennzahlen je Woche (`buildWeekSnapshot`, 94)

- Summen über **alle** Aktivitäten: `loadSum` (gerundet), `movingTimeMin`, `distanceKm`, `sessionCount`, `runSessionCount`. `distanceKm` addiert Schwimm-, Rad- und Laufkilometer **zusammen** und steht im Bericht trotzdem neben „Umfang“ **[P21]**.
- `VDOT` = Fenster-Schätzung (2.5) über die 7 Tage, **ohne** Korrekturfaktor **[P5]**.
- `CTL`, `ATL`, `rampRate` = Intervals-Wellness vom Wochenende; wenn dort kein CTL steht, bis zu 3 Tage zurück. `TSB = CTL − ATL` (1 Nachkommastelle).

### 5.3 Vergleich (`compareSnapshots`, 113)

`d… = round1(aktuell − Vorwoche)`; `pctLoad = round1((aktuell − vor)/|vor|·100)` (nur wenn `vor ≠ 0`).

### 5.4 Urteil (`buildVerdict`, 143)

```
Signal 1: dVDOT  ≥ +0,3 → +1 | ≤ −0,3 → −1 | sonst 0
Signal 2: dCTL   ≥ +1   → +1 | ≤ −1   → −1 | sonst 0
Score = Summe; ohne beide Signale → UNKLAR; Score > 0 → BESSER; < 0 → SCHLECHTER; = 0 → STABIL
```

Zusätzliche Texte (ändern das Urteil nicht):

| Größe | Aussage |
|---|---|
| ΔATL | > +5: „deutlich mehr Reize“; < −5: „Müdigkeit sinkt“ |
| TSB | < −20 „stark negativ“; < −5 „moderat negativ“; ≤ +10 „ausgeglichen“; sonst „deutlich positiv“ **[P2]** |
| ΔTSB | ≥ +5 „frischer“; ≤ −5 „akkumuliert Müdigkeit“ |
| Ramp Rate | > 8/Woche „erhöhtes Verletzungsrisiko“ |
| Einheiten, Load/Einheit | Load je Einheit = `loadSum/sessionCount`; Meldung ab ±15 % |

### 5.5 Empfehlung (`buildRecommendation`, 262)

`highFatigue = TSB < −20 oder rampRate > 8`. Dann je Urteil: BESSER → mit `highFatigue` „Gas rausnehmen“, sonst „+5–10 % Umfang“; SCHLECHTER → mit `highFatigue` „−20 bis −30 %“, sonst „Umfang leicht anheben“; STABIL → „gezielter Impuls“; UNKLAR → „mehr Daten“. Die Zahlen sind Textvorgaben, keine Berechnung.

### 5.6 Prognose und Paces im Bericht

Bericht verwendet `realVdot ?? Wochen-VDOT` für Paces (2.10) und Rennzeiten (2.11); die Vorwoche zeigt Rennzeiten aus ihrem Wochen-VDOT. Zeitdifferenzen: `c.seconds − p.seconds`, formatiert als `±m:ss`.

---

## 6. Dashboard: Belastung, Bereitschaft, Körperwerte (`src/dashboard-summary.js`)

### 6.1 Belastung (`computeLoad`, 31)

```
Datensatz = letzter Eintrag mit CTL und ATL (nur Tage < heute, wenn heute noch keine Aktivität vorliegt)
TSB  = CTL − ATL
ACWR = ATL / CTL   (nur wenn CTL > 0)
TSB-Klasse:  ≥ −10 ok | ≥ −25 warn | sonst bad
ACWR-Klasse: < 0,8 warn | ≤ 1,3 ok | > 1,3 bad
```

Begründung im Code: Der Wellness-Eintrag von heute rechnet die **geplante** Einheit bereits ein. Der „ACWR“ ist hier das ATL/CTL-Verhältnis der Intervals-Kurven, nicht das klassische 7:28-Tage-Verhältnis der rollierenden Mittel **[P16]**. Die Frontend-Anzeige (`app.js:84`) wertet im Taper (0–14 Tage vor dem Rennen) ein „zu niedrig“ als ok **[P3]**.

### 6.2 Körperwerte (`computeBody`, 54)

Vergleichsbasis: **Median der 14 Tage vor heute** (ohne heute) je Messgröße.

| Größe | Klasse |
|---|---|
| Schlaf (h, heute) | ≥ 7 ok · ≥ 6 warn · sonst bad **[P12]** |
| HRV | `q = HRV_heute / Median` ; `q < 0,8` bad · `< 0,9` warn · sonst ok; Trend `down` (q < 0,9) / `up` (q > 1,05) / `flat` |
| Ruhepuls | `heute − Median` ≥ 6 bad · ≥ 3 warn · sonst ok |

Fehlende Werte ergeben `none` (nie „gut“).

### 6.3 Bereitschaft (`computeReadiness`, 74)

Fünf Skalen aus Intervals (`sleepQuality`, `fatigue`, `soreness`, `mood`, `motivation`; Werte ab 1, 1 = bestmöglich; 0/leer = „nicht erfasst“, siehe `scaleValue`, `dashboard.js:60`):

```
Basis  = alle Werte der Skala vor heute im geladenen Fenster (56 Tage); Band erst ab ≥ 5 Werten
Band   = [25 %-Quantil ; 75 %-Quantil]      (lineare Interpolation, `quantile`, 21)
out    = heute > 75 %-Quantil
delta  = heute − Median
Klasse: bad  = out UND delta ≥ 2
        warn = out UND delta ≥ 1
        ok   sonst
```

Gesamturteil, mit `nBad`/`nWarn` (Skalen) und `bodyBad`/`bodyWarn` (Körper: Schlaf, HRV, Ruhepuls):

```
kein Skalenwert heute                          → „Heute noch nichts eingetragen“ (none)
nBad ≥ 1  ODER  bodyBad ≥ 2  ODER  nWarn + bodyWarn + bodyBad ≥ 3   → „Eher ruhig angehen“ (bad)
nWarn + bodyWarn + bodyBad ≥ 2  ODER  bodyBad ≥ 1  ODER  TSB < −10  → „Mit Vorsicht“ (warn)
sonst                                                              → „Bereit“ (ok)
```

Wichtig: Der Körper-Teil wird **nur** bewertet, wenn mindestens eine Skala heute einen Wert hat (`hasScales`). Ohne Skalen gibt es keine Einschätzung, auch bei schlechtem Schlaf.

### 6.4 Schlafkonto (Frontend, `app.js:13`)

Gewichteter Schnitt der **letzten zwei Nächte** (letzte Nacht 60 %, davor 40 %); Ziel `SLEEP_TARGET_H = 7,5 h` je Nacht. Klasse: Ø ≥ Ziel − 0,25 ok · ≥ Ziel − 1 warn · sonst bad; fehlt eine Nacht, wird `ok` höchstens zu `warn` heruntergestuft. Woche (7 Nächte, ab 4 Werten): Ø ≥ Ziel − 0,25 ok · ≥ Ziel − 0,75 warn · sonst bad. Das Ziel 7,5 h ist ein anderer Wert als die 7 h in 6.2 und die 6,5 h in 4.3 **[P12]**.

---

## 7. Dashboard: Wochen, Longruns, Rennplan, Triathlon

### 7.1 Wochen und Tage (`dashboard.js`)

- `HISTORY_DAYS = 56`; Wochen von dem Montag an, der `heute − 55` enthält, bis heute. `complete = (Montag + 6) < heute`.
- Je Woche und Sportart: `count`, `minutes = moving_time/60` (gerundet), `km`, `load`; Laufumfang `km` zählt nur Läufe.
- Plan: nur Kalender-Einträge der Kategorie `WORKOUT`. `plannedKm = Σ (distance_target ?? distance)/1000`, `plannedLoad = Σ (icu_training_load ?? load_target)`.
- Tages-Heatmap: Tageslast = Σ `activityLoad`; leere Tage 0.

### 7.2 Einheitenart (`classifyRun`, 78) und Pulsregel

`intensity` bei Intervall-Tag/-Text oder Name mit `tempo|tdl|mit|schwelle|wettkampfspezifisch|fartlek|strides|steigerung`; `long` bei `long run|lsr|langer lauf` oder ≥ 14 km; `base` bei `grundlagen|easy|ga1|regeneration|recovery`; sonst `unknown`. **Puls und Decoupling werden nur bei `base` und `long` ausgegeben**, bei allem anderen `null`.

### 7.3 Longrun-Tracker (`buildFitness`, 284)

Quellen: Runalyze-Läufe der Art „lang“ (`/lang/i` im Typ) **und** Intervals-Läufe ab **15 km** (auch Rennen/Abbrüche, nicht Intensitätseinheiten); pro Tag gilt der Runalyze-Eintrag. `count16` = Anzahl ≥ 16 km; „längster“ = größte Distanz. Frontend-Ampel für Decoupling: < 5 % ok · < 8 % warn · sonst bad (`app.js:255`).

### 7.4 Halbmarathon-Schätzungen (`buildHmEstimates`, 316)

| Schlüssel | Berechnung |
|---|---|
| `runalyze` | Prognose aus Runalyze (Sekunden), unverändert |
| `vdot` | HM-Zeit (2.11) aus dem **Runalyze-VDOT** **[P6]** |
| `best-5`, `best-10` | VDOT aus der Bestzeit (Distanz ± 5 %) nach 2.1, davon HM-Zeit nach 2.11 |

Bestzeit je Distanz (`bestForDistance`, `runalyze-snapshot.js:69`): schnellstes Rennen, dessen offizielle Distanz höchstens 5 % abweicht; gerechnet wird mit der **tatsächlichen offiziellen Distanz** des Rennens.

### 7.5 Rennplan-Szenarien (`raceScenarios`, `app.js:168`)

```
Spanne unten (lo) = min der HM-Schätzungen „vdot“ und „best-10“
Spanne oben (hi)  = max derselben × 1,03, wenn count16 = 0 (kein Lauf ≥ 16 km in 8 Wochen), sonst × 1
Szenarien         = {Ziel, lo, hi}, dedupliziert, aufsteigend sortiert → A, B, C
Pace              = Sekunden / km
Splits            = Pace · Marke (5, 10, 15, 20 km, Ziel)
Verpflegung       = ein Gel alle 40 min (bei Zeit t = 40, 80, … min, solange t < Zielzeit − 10 min), Km = t / Pace
```

Mit nur 2 Szenarien heißen sie A/B, mit 3 A/B/C; „B realistisch“ steht im Hinweistext nur bei genau 3. Die Warnung „Ziel liegt unter der Spanne“ erscheint, wenn `Ziel < lo`.

### 7.6 Triathlon-Ziele (`triathlon-targets.js`)

```
Schwimmen: Eintrag: Pace/100 m = Zielzeit / (Schwimm-km · 10)
           Vorschlag: Pace/100 m = CSS · Faktor (Sprint 1,02 | Olymp. 1,04 | Mittel 1,06 | Lang 1,08), Zeit = Pace · km · 10
Rad:       Eintrag: Watt bzw. Zeit (Speed = km / (Sekunden/3600)), %FTP = Watt / FTP
           Vorschlag: Watt = FTP · IF (Sprint 0,85 | Olymp. 0,80 | Mittel 0,72 | Lang 0,68), Spanne ± 0,03 · FTP
Lauf:      nur wenn „Lauf …“ in der Beschreibung steht: Pace/km = Zielzeit / Lauf-km
```

Schwellen aus den Sport-Settings (`buildThresholds`, `dashboard.js:186`): `threshold_pace` in **m/s** → Lauf `round(1000 / v)` s/km, Schwimmen `round(100 / v)` s/100 m; Feldname laut Kommentar ungeprüft. Im Frontend: Summe der drei Zeitziele plus Warnung, wenn `Summe + 120 s > Gesamtziel`; Verpflegung Rad: ein Gel alle 40 min bis 10 min vor Ende.

### 7.7 Wochenstunden Kraft (`app.js:523`)

Zielmarke 60 min/Woche Kraft; „erreicht“ = abgeschlossene Wochen mit ≥ 60 min.

### 7.8 Verlauf der HM-Prognose (`runalyze-history.js`, `app.js:348`)

Je Tag ein Eintrag (VDOT-Rechnung, Runalyze-Prognosen für 5 km, 10 km, HM; Zuordnung der Prognose zur Distanz mit ≤ 1 % Abweichung), max. 400 Einträge. Veränderung = `Prognose_jetzt − Prognose_früher` (negativ = schneller). Eine Prognose „vor 7 Tagen“ = jüngster Eintrag, der ≥ 7 Tage älter ist.

---

## 8. Ernährung (Yazio-Sync und Dashboard)

### 8.1 Tagessummen (`yazio-client.js:176`)

- Einträge vom Typ `product`: `Menge (g) · Nährwert pro g` (Kalorien, Protein, Fett, Kohlenhydrate; Nährwerte je Produkt in KV gecacht).
- `simple_product`/Rezept-Portion mit eigenen `nutrients`: Werte direkt übernommen (nicht multipliziert).
- Einträge ohne beides werden übersprungen und gelistet (`skippedItems`).
- Wellness-Felder (`sync.js:112`): `Calories` gerundet auf kcal, Protein/Carbs/Fat auf 0,1 g.

### 8.2 Kalorienziel (`sync.js:129`)

```
CalorieGoal = round( Yazio-Diätziel_kcal + Σ Aktivitätskalorien des Tages )
```

Die Summe nutzt das Feld `calories` jeder Aktivität an diesem Tag (alle Sportarten, zu 100 %). Es gibt kein Mindest-/Höchstziel und keine Teilrückrechnung; das Diätziel ist der Boden **[P13]**.

### 8.3 Auswertung im Dashboard (`app.js:485`)

- Leere Tage (Wert ≤ 0) gelten als „keine Daten“ (`positive`, `dashboard.js:217`).
- Je abgeschlossenem Tag (heute zählt nicht): „über Ziel“ bei `kcal > 1,10 · Ziel`; „deutlich drunter“ bei `kcal < 0,75 · Ziel`; sonst „im Rahmen“. „Wenig Eiweiß“ bei `Protein·4 / kcal < 0,20`.
- Bilanz = Σ (kcal − Ziel) über abgeschlossene Tage mit Ziel; Schnitt = Mittel über abgeschlossene Tage.
- Makro-Balken: `Wert / Ziel`, Balken rot ab 110 %. Kalorienanteil je Makro = `Gramm · 4 (Eiweiß, Kohlenhydrate) bzw. 9 (Fett) / Summe dieser drei`. Die Summe aus 4/4/9 stimmt nicht exakt mit Yazios Kalorien überein (Ballaststoffe, Alkohol, Rundung).

---

## 9. Widget (`src/widget.js`)

- **Rennphase** (`racePhase`, 31): `daysToGo > 7` → normal; `0…7` → Taper; `−3…−1` → Erholung; sonst normal **[P3]**.
- **Wochenziel** (`weeklyGoal`, 41): Summe der geplanten Last der Woche aus dem Kalender; fehlt sie, der Wert `WEEKLY_TSS_GOAL`; sonst kein Ziel.
- **Wochenlast** (`buildWidget`, 47): Summe der Last über alle Sportarten der laufenden und der Vorwoche.
- **Fitness (CTL)** (`buildFitness`, 138): sieben Wochenpunkte bei `heute − 42, −35, …, −7, heute`; der letzte Punkt wird durch den **letzten vorhandenen CTL** ersetzt; `delta = CTL_heute − erster vorhandener Wochenpunkt` (gerundet), mit der Anzahl Wochen dazwischen.
- **Trainingsverteilung** (`buildWidgetTraining`, 108): Anteil je Sportart (Schwimmen/Rad/Lauf) = Minuten der **letzten bis zu vier abgeschlossenen Wochen** / Gesamtminuten, gerundet auf ganze Prozent; Soll nur, wenn `TRI_SPLIT_TARGET` gesetzt ist (Normierung auf 100 %). „Tage seit letzter Einheit“ aus der jüngsten Tagesaktivität der Sportart.
- **Cache:** das Dashboard wird 5 Minuten in KV gehalten (`CACHE_MS`), nur bei vollständigen Daten und gleichem Berliner Datum.

---

## 10. Wetter (`src/weather.js`)

- Bevorzugt Intervals-Wetter aus den Kartenpunkten: **Mittelwert** aller Temperatur- und Feuchtewerte über alle Zeitproben, auf 0,1 gerundet; beide Größen müssen vorhanden sein.
- Fallback Open-Meteo-Archiv am GPS-Startpunkt: Stundenwert zur lokalen Startstunde (`start_date_local`), sonst Tagesmittel aller Stunden. Koordinaten auf 3 Nachkommastellen (~111 m) gerundet.
- Laufband: kein Wetter. Ergebnisse werden je Aktivität dauerhaft in KV gecacht.

---

## 11. Freitext-Parser (`src/dashboard-parse.js`)

- **Heißhunger** (`parseCravings`, 9): jeder Eintrag beginnt mit „HH“. Uhrzeit `HH h[:|.|h]mm` (Stunde 0–24, Minute 0–59), `Stärke n`, `Auslöser …`, `davor …`; der Rest bis zum nächsten Komma ist „was“. Nicht lesbare Teile bleiben `null`, der Eintrag wird trotzdem gezählt.
- **Hüfte/Leiste/Knie** (`findHipFlags`, 46): Treffer auf Wörter wie `hüft*`, `leiste*`, `knie*`, `hip`, `groin`; Kniebeuge/Kniehebe-Wörter und Verneinungen in den 30 Zeichen davor (`kein`, `ohne`, `nicht`, `schmerzfrei` …) werden ausgelassen.
- **Workout-Schritte** (`parseWorkoutSteps`, 108): bevorzugt Intervals-`workout_doc`, sonst Beschreibung im Intervals-Format. Dauer: Zahl + Einheit (`h`, `min`/`m`, `s`, `km`, `mtr`); **Distanz wird mit 6:00 min/km (360 s/km) in Zeit umgerechnet**. Intensität: Prozentwert (bei Bereich der Mittelwert) oder Zone `Z1…Z7` → 55/70/85/98/110/125/150 %. Wiederholungen max. 50, Blöcke max. 200.

---

## 12. Prüfhinweise (Auffälligkeiten)

Sortiert nach Gewicht. „Beleg“ nennt, wie ich es festgestellt habe.

### Wahrscheinliche Fehler

**P1 – Long-Run-Plan erreicht die Spitze nie.** Nach jeder Rücknahmewoche (alle 4 Wochen, ×0,75) setzt die Steigerung vom reduzierten Wert neu an, während die Steigerung pro Woche so berechnet wird, als gäbe es keinen Verlust. Beleg: Ausführung mit Basis 10 km, HM, Spitze 19 km, 12 Wochen → höchste Planwert 13 km. Beim Marathon (Basis 16, Spitze 31,5) endet der Aufbau bei 12,5 km und die erste Taperwoche steht bei 20,5 km, also über der letzten Aufbauwoche. (`long-run-plan.js:181–195`). Naheliegende Korrektur: die Rücknahme nur für die Anzeige verwenden und den Aufbau von der letzten *Nicht-Rücknahme*-Woche fortsetzen. Nicht geändert, weil das die Planlogik ändert.

**P2 – Zwei verschiedene TSB-Skalen.** Wochenbericht: < −20 stark negativ, < −5 moderat, ≤ +10 ausgeglichen (`weekly-progress.js:199`). Dashboard/Widget: ≥ −10 ok, ≥ −25 warn, sonst bad (`dashboard-summary.js:5`). Ein TSB von −15 heißt im Bericht „moderat negativ, produktiv“, im Dashboard „belastet (warn)“.

**P3 – Zwei Taper-Definitionen.** Dashboard: Taper = 0–14 Tage vor dem Rennen (`app.js:10`), Widget: 0–7 Tage (`widget.js:30`).

**P4 – Zwei Phasenmodelle.** Das Datumsraster (3.3) und der Zustandsautomat (3.2) können verschieden entscheiden, z. B. HM: Raster BUILD ab 18 Wochen vor dem Rennen, Automat erzwingt BUILD spätestens ab 16 Wochen, außerdem gilt für `≤ 4` Wochen ein Sonderfall. Der Wochenbericht zeigt den **Automaten** (`Block: …`) und das **Raster** („Empfohlener Block“) nebeneinander.

**P5 – VDOT mit und ohne Korrekturfaktor.** Der aktuelle VDOT (2.8) enthält den Rennfaktor, der Wochen-VDOT (2.12, 5.2) nicht. Beide stehen im Wochenbericht, die Differenz kann also teilweise vom Faktor stammen. Die Dokumentation in `doc.txt` 6.6 erklärt nur Zeitfenster und Glättung, nicht den Faktor.

### Methodische Schwächen

**P6 – Zwei VDOT-Quellen.** Wochenbericht, Formcheck und Ziel-Prognose nutzen den Worker-eigenen VDOT (Puls+Pace). Der Rennplan im Dashboard (7.5) nutzt den **Runalyze-VDOT** aus dem Snapshot. Beide können mehrere VDOT-Punkte auseinanderliegen.

**P7 – Pace-Benchmark.** (a) Maximum über 1 km, 5 km, 10 km, HM: Die kürzeste Distanz liefert im Daniels-Modell tendenziell das höchste VDOT, das Maximum ist daher optimistisch. (b) Der Fallback (2.8 Schritt 2) multipliziert den Benchmark-VDOT mit einem Faktor, der selbst aus Benchmark/HF-VDOT gebildet wurde – das ist zirkulär, wenn der HF-VDOT fehlt.

**P8 – GPS-Distanz in der Renn-VDOT.** Die Rechnung nimmt die gemessene Distanz. Bei 21,3 km statt 21,0975 km ergibt dieselbe Zeit 36,9 statt 36,5 (ausgeführt). Der Dashboard-Weg (Bestzeiten, 7.4) nimmt dagegen die *offizielle* Distanz.

**P9 – Rennen-Erkennung über den Namen.** Jeder Titel mit dem ganzen Wort „race“, „wettkampf“ oder „competition“ gilt als Rennen (z. B. „Race Pace Training 3×3 km“) und geht dann als Renn-VDOT ein, wenn der Lauf `moving_time` und Distanz hat. Schutz bietet nur `#novdot`.

**P10 – Triathlon-Format per Regex.** `lang` trifft auch „langsam“, „lange“, „Belang“ in der Beschreibung; getestet wird erst nach Sprint/Olympisch/Mittel, deshalb entscheidet nur das **Fehlen** früherer Treffer. Zeiten mit zwei Teilen („Rad 3:00“) werden als Minuten:Sekunden gelesen.

**P11 – HRV-Schwellen absolut.** −3 bzw. −8 als HRV-Einheiten (7.4/4.3) passen zu rMSSD in einem üblichen Bereich; bei anderer Skala (SDNN, andere Uhr) verschieben sich die Auslöser. Das Verhältnis (0,7/0,5) ist skalenunabhängig.

**P12 – Uneinheitliche Schlafwerte.** Formcheck: Ø < 6,5 h oder 2 Nächte < 5 h; Körper-Ampel: ≥ 7 ok, ≥ 6 warn; Schlafkonto: Ziel 7,5 h/Nacht; Empfehlungstext: „mind. 7 h“.

**P13 – Kalorienziel addiert die volle Aktivitätskalorien.** `calories` aus Intervals enthält typischerweise den Grundumsatz-Anteil der Einheit mit; bei langen Einheiten wird das Ziel dadurch eher zu hoch.

**P16 – „ACWR“ ist ATL/CTL.** Das ist das Verhältnis zweier exponentiell gewichteter Kurven, nicht das rollierende 7:28-Verhältnis, auf das die üblichen Bänder 0,8–1,3 zurückgehen. Die Bänder sind hier „eigene Standardwerte“ (`dashboard-summary.js:1`).

**P17 – Formcheck-Skalierung.** Siehe 4.4: Rot nur mit beiden Kategorien; ein einzelnes Flag mit Schweregrad ≥ 0,5 macht gelb.

### Kleinigkeiten

**P14 – `doc.txt` veraltet** (EF, Drift, Motor, RunFloor-TSS, GA/Key, Daily-Report): nicht mehr im Code.
**P15 – Fest eingebautes Fallback-Ziel** `CONFIGURED_GOAL` (Halbmarathon 03.10.2026, 2:00:00) in `dashboard.js:26`; gilt, wenn Intervals kein A-Rennen kennt, und bleibt auch nach dem Renntermin im Code.
**P19 – Toter Code:** die Plausibilitätsprüfung in `computeWeeksToEvent` rechnet denselben Wert neu.
**P20 – Heutiger Lauf braucht 1 Lauf, das Fenster 2** (2.9 vs. 2.5).
**P21 – Wochenvergleich mischt Sportarten:** `distanceKm` im Bericht summiert Schwimmen, Rad und Laufen (`weekly-progress.js:48–70`), während `runSessionCount` nur Läufe zählt.

---

## Anhang: Wie ich die Beispiele erzeugt habe

`node` mit direktem Import der Funktionen `computeVdotFromRaceTime`, `paceTargetsFromVdot`, `predictRaceTimesFromVdot` (aus `src/vdot.js`) sowie `getPeakLongRunKm` und `buildLongRunProgressionWeeks` (aus `src/long-run-plan.js`). Keine Netzwerkzugriffe, keine Änderung am Code.
