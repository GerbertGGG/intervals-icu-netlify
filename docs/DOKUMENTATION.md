# Dokumentation: Datenfluss, Tasks und Berechnungen

Alles in einem Dokument.

- **Teil A** erklärt, woher die Daten kommen und wie die geplanten Tasks laufen.
- **Teil B** ist die Prüfdokumentation der Berechnungen im Worker, mit Datei und Zeile.

Die Garmin-Uhr steht in [../garmin/README.md](../garmin/README.md). Das Projekt heißt noch `intervals-icu-netlify`, ist aber ein **Cloudflare Worker** (`wrangler.toml`), kein Netlify-Projekt.

---

# Teil A: Datenfluss und geplante Tasks

## A1. Überblick

```
 Intervals.icu ─┐                                   ┌─ Dashboard   /dashboard/ (public/)
 Yazio ─────────┤                                   ├─ iOS-Widget  /api/widget (Scriptable)
 EGYM ──────────┼──►  Cloudflare Worker  ──────────►├─ Garmin-Uhr  /api/widget?view=watch
 Runalyze*  ────┤      (src/index.js)               └─ Claude      /mcp (Connector)
 Coach-Aussage ─┘      + KV-Speicher
                       + D1 (Export, nur lesen)

 Intervals.icu ──►  GitHub Action „Export nach D1“ (täglich) ──►  D1 training-export
                    (scripts/export-to-d1.mjs)
```

\* Runalyze hat keine öffentliche API, nur ein MCP. Der Worker kann es nicht selbst abfragen; Daten kommen über den Coach (siehe A2.4).

**Grundsatz** (aus `CLAUDE.md`): Das Dashboard zeigt, der Coach entscheidet. Der Worker rechnet keine Tagesentscheidung. Urteile (`go`, Wochenampel) kommen vom Coach und werden nur angezeigt.

## A2. Woher kommen die Daten?

### A2.1 Intervals.icu (live bei jedem Aufbau)

`src/intervals-client.js` ruft `https://intervals.icu/api/v1` ab, mit `ATHLETE_ID` und `INTERVALS_API_KEY`:

- **Wellness** (HRV, Ruhepuls, Schlaf, Gefühlswerte, CTL/ATL, dazu die Yazio-Felder, siehe A2.2)
- **Aktivitäten** (abgeschlossene Einheiten, `icu_training_load`)
- **Events** (Kalender: geplante Workouts, Rennen, Wochenziele als Kategorie „Target“)
- **Sport-Settings** (FTP, Schwellenwerte, Zonen)

`buildDashboard` (`src/dashboard.js`) holt alles parallel; scheitert eine Quelle, bleibt der Rest heil (`settle`). Load, CTL, ATL, Rampe usw. rechnet **Intervals**, der Worker rechnet damit weiter.

### A2.2 Yazio (Ernährung, Gewicht) → Intervals-Wellness

`src/yazio-client.js` nutzt die inoffizielle Yazio-API (`YAZIO_USERNAME`, `YAZIO_PASSWORD`; Token im KV `yazio:token`). `src/yazio-sync.js` schreibt Kalorien, Eiweiß, Kohlenhydrate, Fett und Kalorienziel **als Zusatzfelder in den Intervals-Wellness-Eintrag** des Tages. Das Dashboard liest sie von dort. Gewichtseinträge der letzten 90 Tage landen im KV (`yazio:weight`). Das Tagesziel holt der Worker beim Neuaufbau direkt aus Yazio (KV-Cache `yazio:goals`). Fehler sind „best effort“ und brechen nichts ab.

### A2.3 EGYM (Kraft)

`src/egym-client.js` nutzt die inoffizielle Netpulse-API (`EGYM_USERNAME`, `EGYM_PASSWORD`, optional `EGYM_BRAND`; Session im KV `egym:session`). `src/egym-intervals-sync.js` schreibt je Kraft-Tag eine manuelle Aktivität (Typ `WeightTraining`, `external_id` = `egym-<Datum>`, damit der Abgleich wiederholbar ist) nach Intervals. Zusätzlich hält `src/egym-widget.js` einen Cache der Kraft-Sätze (`widget:egym-kraft`). Wochenziel: `EGYM_WEEKLY_GOAL_SETS` (Standard 60).

### A2.4 Coach-Aussagen und Runalyze (geschrieben, nicht abgerufen)

Diese Daten erzeugt der **Coach** (siehe A3.2). Sie liegen als JSON im KV (`dashboard:*`) und werden validiert (`validate*`-Funktionen in `src/*-snapshot.js`):

| KV-Schlüssel | Inhalt | MCP-Tool | HTTP |
|---|---|---|---|
| `dashboard:coach` | Tagesaussage: `note`, `headline`, `text`, `go`, `blockers`, Rückfrage/Antwort, Idee der Woche, Wochenampel `week`, Wochenvorschlag `week_plan`. **`save_coach` ersetzt komplett** (nur `idea`, `week`, `week_plan` bleiben stehen, wenn sie fehlen); `decisions` wird abgelehnt („Entscheidungen bitte mit save_decisions speichern“), ebenso ein Aufruf ohne `text`, wenn heute schon ein Bericht gespeichert ist | `save_coach` | `PUT /api/coach` |
| `dashboard:decisions` | Entscheidungsliste „Was du entscheiden musst“ (max. 5): je Punkt `id` (kebab-case, stabil), `title`, `recommendation`, `reason`, `alternatives`, `deadline`, `ifNoAnswer`, Antwortfelder `answer`/`answeredAt` (aus dem Dashboard, bleiben per `id` erhalten). Ersetzt die ganze Liste, fehlende Punkte werden gelöscht, `[]` blendet die Karte aus. Fasst die Tagesaussage nie an. Das alte Feld `dashboard:coach.decisions` wird beim ersten Zugriff einmalig übernommen und entfernt | `save_decisions` | `PUT /api/decisions` (Antwort aus dem Dashboard: `POST /api/coach/decision` mit `id`, `answer`) |
| `dashboard:runalyze` | VDOT, Prognosen, Rennen, Läufe mit Decoupling | `save_runalyze_snapshot` | `PUT /api/runalyze` |
| `dashboard:runalyze-history` | Zeitverlauf der Runalyze-Werte (Tageseintrag, vom Cron) | – | – |
| `dashboard:studie` | Studien-Check der Woche | `save_studie` | `PUT /api/studie` |
| `dashboard:wissensbasis` | Wissensbasis-Stichpunkte | `save_wissensbasis` | `PUT /api/wissensbasis` |
| `dashboard:ernaehrung` | Coach-Aussage zu Ernährung und Gewichtsziel | `save_ernaehrung` | – |
| `dashboard:experiment`, `dashboard:profil` | laufendes Experiment, Profil-Punkte | `save_experiment`, `save_profil` | – |
| `dashboard:tests` | Tests des Coaches (laufend, Kandidaten, abgeschlossen), höchstens 40 | `save_test` | – |
| `dashboard:insights` | Erkenntnisse des Coaches | `save_insight`, `archive_insight` | – |
| `dashboard:targets` | Rahmen (Wochenstunden, Leitplanken), mit Historie | `save_targets` | – |

Drei Wege, wie ein Snapshot ins KV kommt (je nach dem, was die Coaching-Sitzung erreichen kann):

1. **MCP-Connector** (`/mcp`, `src/mcp-server.js`): die `save_*`-Tools. Das ist der normale Weg.
2. **HTTP-PUT** mit `Authorization: Bearer <DASHBOARD_TOKEN>`.
3. **GitHub-Fallback** (`src/github-snapshot.js`): Der Task committet `runalyze.json`, `studie.json`, `coach.json` auf den Branch **`snapshots`** (nicht `main`, damit kein Deploy ausgelöst wird). Der Cron holt sie über `raw.githubusercontent.com` ins KV. Ein älterer Stand überschreibt nie einen neueren (`fetchedAt`-Vergleich). Konfigurierbar über `GITHUB_REPO`, `SNAPSHOT_BRANCH`, `GITHUB_TOKEN` (nur bei privatem Repo).

Der Coach liest vor jedem Update mit `get_coach` den gesamten Stand zurück (letzte Aussage, Antwort auf die Rückfrage, Wochensummen, Rahmen, Ernährung …).

**Export für die Langzeitanalyse** (lesend, kein Snapshot; Details in Teil B, Abschnitt 13):

| MCP-Tool | HTTP | Inhalt |
|---|---|---|
| `get_run_export` | `GET /api/export/runs` | eine Zeile pro Lauf: Stammdaten, Wetter, Kennzahlen aus den Zeitreihen |
| `get_ride_export` | `GET /api/export/rides` | eine Zeile pro Radfahrt: Leistung, Puls, Einordnung (Intensität, Vergleichseinheit), Kennzahlen aus den Zeitreihen |
| `get_swim_export` | `GET /api/export/swims` | eine Zeile pro Schwimmeinheit: Distanz, Bewegungszeit, Pace/100 m, Schwimmtest 2× 500 m |
| `get_day_export` | `GET /api/export/days` | eine Zeile pro Tag: CTL/ATL, Load und Minuten je Sportart, Wellness, Gewicht, Ernährung, Kalender (Krankheit, OP, Urlaub …) |

Beide nehmen `from`, `to`, `cursor`, `limit` (Standard 200, max. 500) und `format` (MCP: `table` als Standard, `json`, `csv`; HTTP: JSON-Objekte als Standard, `table`, `csv`). Die MCP-Tools sind der Hauptweg, weil die Coaching-Sitzung den Worker per HTTP oft nicht erreicht; HTTP braucht `Authorization: Bearer <DASHBOARD_TOKEN>`. Der Worker liest dafür **nur aus D1**; gerechnet und geschrieben wird der Export täglich in GitHub Actions (A3.1a), weil die Auswertung der Zeitreihen die 10 ms CPU des kostenlosen Plans sprengt.

### A2.5 Antworten aus dem Dashboard

Das Dashboard schreibt zwei Dinge zurück: die **Antwort auf die Rückfrage** des Coaches (`POST /api/coach/answer`) und **angepasste Zeiten im Wochenvorschlag** (`POST /api/coach/plan`, baut danach den Cache sofort neu). Der Coach liest beides am nächsten Morgen.

Dazu kommt die **Entscheidung über Test-Kandidaten**: `POST /api/tests/:id/decision` (Bearer `DASHBOARD_TOKEN`) mit `{ action: "start" | "reject" | "later", startDate?, checkDate?, expectation?, adherence?, block? }`. Im Dashboard stehen auf jeder Kandidaten-Karte „Machen wir“, „Nicht jetzt“ und „Später (anderer Block)“. Der Endpunkt schreibt in `dashboard:tests` denselben Datensatz wie `save_test` (gleiche Validierung über `validateTest`, danach baut er den Cache neu) und setzt `decidedAt` selbst auf heute (Berlin). Nur Tests mit status `kandidat` lassen sich entscheiden (sonst 409).
- `start`: status `laeuft`, `startDate` (Standard nächster Montag, heute wenn Montag), `checkDate` (Standard: die Vorgabe des Tests, sonst Start + 8 Wochen), `expectation` (Pflicht: `confirmed`, `unclear`, `refuted`; vorbelegt aus dem Kandidaten), `adherence` und `block` optional; `proposedBy` bleibt. Die Antwort enthält die `warnings` (mehr als zwei laufende Tests mit Mehrbelastung, gleiche Messgröße); sie blockieren nicht.
- `reject`: status `verworfen`, `decidedAt` heute, der Test verschwindet aus der Ansicht.
- `later`: bleibt Kandidat, nur `block` (Pflicht, max. 20 Zeichen) ändert sich, `decidedAt` bleibt leer.
- Schutz: `save_test` mit status `kandidat` für einen Test mit `decidedAt` wird ignoriert (Antwort `ignored: true` mit Warnung); der Coach setzt eine Entscheidung Gerberts nicht zurück. Ein `save_test` mit älterem `fetchedAt` überschreibt die Freigabe ohnehin nicht. Der Coach liest die Entscheidung beim nächsten `get_coach` (`tests`).

## A3. Geplante Tasks

Es gibt **zwei getrennte Sorten**, die man nicht verwechseln darf: was im Repo läuft (Cron des Workers A3.1, GitHub Action A3.1a) und was bei Claude läuft (Morgen-Update A3.2, Monats-Review A3.3).

### A3.1 Cron des Workers (im Repo, `wrangler.toml` + `src/index.js`, `scheduled`)

Zwei Cron-Einträge (UTC):

- `*/15 3-22 * * *`: alle 15 Minuten, 03:00–22:45 UTC
- `58 21-22 * * *`: Minute 58 in den UTC-Stunden 21 und 22, damit einer der beiden Ticks unabhängig von Sommer-/Winterzeit auf **23:58 Berlin** fällt (letzter Yazio-Sync des Tages)

Jeder Tick macht:

| Schritt | Wann | Was |
|---|---|---|
| Snapshots aus GitHub holen, danach Runalyze-Tageseintrag | jeder Tick | `syncSnapshotsFromGithub` → `recordRunalyzeHistory` (A2.4, Weg 3) |
| Dashboard-Cache warm halten | jeder Tick | `warmDashboardCache`: baut den Stand aus allen Quellen neu auf, damit die Uhr sofort antwortet |
| Yazio → Intervals | 07:00–23:59 Berlin | `syncYazioRange` für heute; der erste Tick um 07:xx holt zusätzlich gestern nach |
| EGYM → Intervals, danach EGYM-Cache | 1× pro Stunde (Tick mit Minute < 15) im selben Fenster | `syncEgymToIntervals`, dann `loadWidgetKraft(env, true)` |

Fehler werden geloggt und stoppen die anderen Schritte nicht.

**Cache für Widget und Uhr** (`src/widget.js`): Der Aufbau aus allen Quellen dauert länger, als die Uhr wartet. Deshalb antwortet `/api/widget` sofort aus dem Cache von heute (`widget:dashboard-cache`, gilt 5 Minuten) und baut im Hintergrund neu; der Cron hält ihn warm. `GET /api/watch-status` (ohne Token) zeigt Abrufe der Uhr und das Alter des Caches, praktisch bei Fehlersuche.

### A3.1a GitHub Action „Export nach D1“ (im Repo, `.github/workflows/export-d1.yml`)

Rechnet den Export (Teil B 13) außerhalb des Workers und schreibt ihn in die D1-Datenbank `training-export`; der Worker liest nur.

- **Täglich** um 03:17 UTC (04:17/05:17 Berlin, vor dem Coaching-Task): die letzten 14 Tage bis gestern (`EXPORT_DAYS`, Standard 14). Gelesen wird der ganze Bereich, geschrieben nur, was neu ist oder sich geändert hat. So kommen neue Läufe, nachträglich geänderte Namen/Tags, Wetter aus dem nachhinkenden Open-Meteo-Archiv und geänderte Wellness-Werte nach. GitHub startet geplante Läufe oft einige Minuten später.
- **Von Hand** (Actions → „Export nach D1“ → „Run workflow“): `full` rechnet die ganze Historie (einmalig nach dem Einrichten und nach jedem Wechsel von `METHOD_VERSION`), `from`/`to` einen eigenen Bereich.
- Schreibt nur neue oder geänderte Zeilen (Hash je Zeile), löscht Läufe, die es im Bereich in Intervals nicht mehr gibt, und rechnet Zeitreihen nur neu, wenn nötig (Teil B 13.4).
- Logs und Job-Zusammenfassung enthalten nur Bereich, Zähler, HTTP-Status und Fehlercodes (`safeError`): keine Datenzeilen, Gesundheitswerte, Aktivitäts-IDs oder Antworttexte, auch nicht bei Fehlern, denn das Repo und damit die Logs sind öffentlich.

### A3.2 Morgen-Update (Coach bei Claude, **nicht** im Repo)

Das Morgen-Update läuft in Claude mit dem Skill **`triathlon-coach`**. Gerbert startet es von Hand, sobald er morgens alles eingetragen hat. Die geplante Aufgabe „Morgendliches Trainings-Update“ ist bewusst ausgeschaltet. Das Update:

1. liest per MCP `get_coach` (Vortag, Antwort auf die Rückfrage, Rahmen, Wochensummen, Tests, Wissensbasis),
2. liest Daten aus Intervals (`get_wellness`, `get_recent_activities`, `get_planned_workouts`), Yazio (`get_nutrition`) und Runalyze (dessen eigenes MCP),
3. bewertet und schreibt das Ergebnis zurück: `save_coach` (Tagesaussage, `go`, Rückfrage), montags zusätzlich Wochenampel und Wochenvorschlag, außerdem nach Bedarf `save_runalyze_snapshot`, `save_ernaehrung`, `save_targets`, `save_insight`, `save_decisions`, `save_test`.

Läuft eine Sitzung in einer Cloud-Umgebung, die den Worker per HTTP nicht erreicht (Egress-Policy), GitHub aber schon, greift der Branch-Fallback (A2.4, Weg 3).

Der Skill-Text liegt in Claude (Skill `triathlon-coach`), nicht im Repo. Die Werkzeugbeschreibungen in `src/mcp-server.js` hängen eng an ihm: Wer ein Feld in `get_coach` oder einem `save_*`-Tool ändert, muss den Skill mitziehen und umgekehrt.

### A3.3 Monatlicher Profil-Review (geplanter Task bei Claude, **nicht** im Repo)

Am 1. jedes Monats läuft eine geplante Aufgabe mit dem Skill **`athletenprofil`**. Dieser Skill ergänzt `triathlon-coach`: Er hält fest, was bei Gerbert nachweislich wirkt, und vergleicht sein Training mit der Wissensbasis. Bei Widerspruch zum Coach-Skill gilt das Profil.

1. Datenbasis ist der Trainings-Export in D1 (Teil B 13), gelesen über `get_run_export`, `get_ride_export`, `get_swim_export` und `get_day_export`. Die früheren Auswertungen aus Runalyze und der Ø-Puls-Regression sind überholt.
2. Nach der Methode im Skill neu rechnen (u. a. Pace im Pulsband pro Quartal, Rad-Puls bei fester Leistung, Schwimmtest, Koppelläufe), Erkenntnis-Karten und Selbstversuche (Tests) aktualisieren und das eigene Training gegen die Wissensbasis prüfen.
3. Ergebnis als Skill-Vorschlag zeigen. Speichert Gerbert ihn, gilt das als Zustimmung. Das Profil ändert nie Blockregeln oder Standardregeln.

### A3.4 Ablauf eines typischen Tages

```
 04:17/05:17  GitHub Action „Export nach D1“: letzte 14 Tage → D1
 05:00–07:00  Cron: Cache neu, Snapshots von GitHub (noch Stand von gestern)
 morgens      Morgen-Update von Hand starten: get_coach / Daten lesen → save_coach (+ weitere)
              → Cache neu → Uhr/Widget zeigen Urteil
 07:00–23:58  Cron alle 15 Min: Yazio → Intervals-Wellness; stündlich EGYM → Intervals
 tagsüber     Uhr holt alle 5 Min /api/widget?view=watch (Antwort aus dem Cache)
 abends       Rückfrage im Dashboard beantworten → Coach liest sie am nächsten Morgen
 am 1. im Monat  zusätzlich: Profil-Review (A3.3), Ergebnis als Skill-Vorschlag
```

## A4. Zugriff und Geheimnisse

| Zugang | Wofür | Prüfung |
|---|---|---|
| `DASHBOARD_TOKEN` | Dashboard, Widget, alle Schreib-Endpunkte | Bearer-Header (`isAuthorized`) |
| `WATCH_TOKEN` | nur `/api/widget?view=watch` | eigenes Token, öffnet das Dashboard nicht |
| MCP-OAuth | Claude-Connector `/mcp` | `src/mcp-oauth.js`: ein Nutzer, ein Client (`claude`), PKCE; braucht `MCP_CLIENT_SECRET`, `MCP_OWNER_PASSCODE`; Access-Token 1 h, Refresh-Token 60 Tage |
| `INTERVALS_API_KEY`, `ATHLETE_ID` | Intervals.icu | Secret |
| `YAZIO_*`, `EGYM_*` | Yazio, EGYM | Secrets |
| `GITHUB_TOKEN` | nur bei privatem Repo (Snapshot-Branch) | optional |
| D1-Binding `EXPORT_DB` | Export lesen (`src/export.js`) | `[[d1_databases]]` in `wrangler.toml` |
| Repo-Secrets `INTERVALS_API_KEY`, `ATHLETE_ID` | GitHub Action „Export nach D1“: Intervals lesen | GitHub → Settings → Secrets and variables → Actions |
| Repo-Secrets `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID` | GitHub Action: D1 schreiben | minimal „Account → D1 → Edit“, nur dieses Konto; optional zusätzlich „Workers KV Storage: Read“ für das Yazio-Gewicht |

Optionale Variablen: `WEEKLY_TSS_GOAL`, `TRI_SPLIT_TARGET`, `EGYM_WEEKLY_GOAL_SETS`, `GITHUB_REPO`, `SNAPSHOT_BRANCH` (Beschreibung in `wrangler.toml`). Secrets werden im Cloudflare-Dashboard bzw. mit `wrangler secret put` gesetzt, nie im Repo.

## A5. Wo schaue ich nach, wenn etwas fehlt?

- **Uhr zeigt „!“ oder alte Daten** → `GET /api/watch-status` (Cache-Alter, letzter Abruf), Fehlercodes in `garmin/README.md`.
- **Coach-Aussage fehlt heute** → Hat der Task `save_coach` geschrieben? Sonst im Branch `snapshots` nach `coach.json` schauen; der Cron übernimmt sie beim nächsten Tick. Die Aussage gilt als veraltet nach `COACH_MAX_AGE_HOURS` (36 h).
- **Kalorien/Eiweiß stimmen nicht** → kommen aus dem Intervals-Wellness-Eintrag; der Yazio-Abgleich läuft höchstens 15 Minuten verzögert (nur 07:00–23:59 Berlin).
- **Kraft fehlt** → stündlicher Abgleich; von Hand: `POST /api/egym-sync` (Bearer `DASHBOARD_TOKEN`) liefert das Ergebnis oder die genaue Fehlermeldung.
- **Export veraltet oder leer** → `syncedAt` in der Antwort von `get_run_export`/`get_day_export`; sonst GitHub → Actions → „Export nach D1“ (letzter Lauf, Zusammenfassung mit `pending`). „EXPORT_DB ist nicht eingerichtet“: D1-Block in `wrangler.toml` fehlt (Teil B 13.4).
- **Logs** → Cloudflare-Dashboard (Worker `intervals-icu-netlify`); Fehler der Cron-Schritte stehen mit `console.error` dort. Workers Logs ist eingeschaltet (`[observability]` in `wrangler.toml`): je Aufruf steht dort auch die CPU-Zeit (`cpuTimeMs`).

---

# Teil B: Berechnungen im Worker

Stand: Code auf `main` bei Commit `88d6dca` (05.11.2026). Dieses Dokument beschreibt **nur, was im Code tatsächlich gerechnet wird**, mit Datei und Funktion (Zeilenangaben gelten für diesen Commit), damit jede Formel gegen die Quelle geprüft werden kann. Offene Auffälligkeiten tragen einen Verweis **[P#]**, die Details stehen in Kapitel 12.

Die Zahlenbeispiele wurden mit dem echten Code ausgeführt (`node`), nicht von Hand gerechnet.

**Nicht hier berechnet, sondern von Intervals.icu geliefert:** `icu_training_load` (Load/TSS je Aktivität), `ctl`, `atl`, `rampRate`, `decoupling`, HF-Zonen, FTP, Schwellenpace. Der Worker rechnet damit nur weiter. Intervals.icu definiert CTL/ATL als exponentiell gewichtete Mittel der Tageslast (üblich: 42 bzw. 7 Tage); das ist dort einstellbar und im Worker nicht nachgebaut.

**Nicht mehr im Code (Stand 03.10.2026 noch beschrieben, seitdem entfernt):** eigener VDOT aus Puls und Pace mit Korrekturfaktoren, Renn-VDOT aus Intervals-Aktivitäten, Maximalpuls-Auflösung, Blockphasen-Automat und Plan-Raster (`determineBlockState`, `computeGoalRaceInfo`), Long-Run-Plan (`long-run-plan.js`), Formcheck mit Erholungs-Ampel (`form-analysis.js`), Wochenvergleich mit Urteil und Empfehlung (`weekly-progress.js`), das Kalorienziel „Diätziel + Aktivitätskalorien“ (`sync.js`). Ein Urteil über den Tag oder die Woche rechnet der Worker grundsätzlich nicht mehr; das macht der Coach (`CLAUDE.md`).

---

## Inhalt

1. Grundlagen: Klassifikation, Datum, Rundung
2. VDOT und Paces (`src/vdot.js`)
3. Zielrennen und Triathlon-Format
4. Formcheck (entfallen)
5. Wochenvergleich (entfallen)
6. Dashboard: Belastung, Erholungswerte, Montagsbilanz
7. Dashboard: Wochen, Longruns, Rennplan, Triathlon
8. Ernährung (Yazio-Sync und Auswertung)
9. Widget (`src/widget.js`)
10. Wetter (`src/weather.js`)
11. Freitext-Parser (Heißhunger, Hüfte, Workout-Schritte)
12. Prüfhinweise (Auffälligkeiten)
13. Export für die Langzeitanalyse (`src/export.js`)

---

## 1. Grundlagen

### 1.1 Aktivitäten klassifizieren (`src/activity-utils.js`)

| Funktion | Regel |
|---|---|
| `isRun` (19) | `type` (klein geschrieben) ist `run`/`running` oder **enthält** `run`, `laufen`, `treadmill` |
| `isTreadmill` (31) | `type == "virtualrun"`, enthält `treadmill` oder `trainer == true`; seit dem Export wieder im Code (nur `export-calc.js`) |
| `isBike` (49) | `type` enthält `ride`, `bike`, `cycling`, `rad`, `velo` |
| `isRaceActivity` (38) | Tag beginnt mit `race:` **oder** `category` ∈ {`RACE`,`RACE_A`,`A_RACE`} **oder** `sub_type == "RACE"`/`race == true`; Wörter im Namen zählen nicht (seit #1291); nur `export-calc.js` |
| `isIntervalActivity` (55) | irgendein Tag beginnt (ohne führendes `#`) mit `interval` |
| `hasIntervalTextSignal` (69) | Name+Beschreibung enthält Wort `intervall`/`interval` **oder** Muster `\d+\s*[x×]\s*\d+` (z. B. „5x1000“) |
| `activityLoad` (5) | `icu_training_load`, sonst `training_load`, sonst `load`; nur Werte > 0, sonst 0. Nur für CTL/ATL von heute (`live-load.js`), damit sie zur Fitness-Kurve von Intervals passen |
| `sessionLoad` (14) | Rad (`isBike`) mit `power_load` > 0 → `power_load`, sonst `activityLoad`. Für Wochen- und Tageslast, Wochenrahmen (TSS-Obergrenzen) und letzte Einheit: Intervals rechnet den Load bei Puls-Priorität als HRSS, die Obergrenzen sind aber in Leistungs-TSS geplant (19.10.2026: HRSS 26 statt 43, FTP-Test 14 statt 35) |
| `activityDay` (1) | erste 10 Zeichen von `start_date_local`, sonst `start_date` |

`sportOf` im Dashboard (`src/dashboard.js`, 191) prüft in dieser Reihenfolge: Typ enthält `swim` → Schwimmen; `isBike` → Rad; `isRun` → Laufen; Typ enthält `weight`/`strength`/`kraft` → Kraft; sonst „other“.

### 1.2 Datum und Zeit (`src/date-utils.js`)

- Alle Tagesrechnungen laufen über UTC-Mitternacht (`T00:00:00Z`). Es gibt keine Sommerzeit-Korrektur, weil nur ganze Tage addiert werden.
- `weeksBetween(a, b) = (b − a) / (7·86400000 ms)` (Bruchteil, nicht gerundet).
- `daysBetween(a, b) = (b − a) / 86400000` (ebenfalls Bruchteil; bei reinen Datumswerten ganzzahlig).
- Berlin-Zeit (`isoDateBerlin`, 5) wird für „heute“ im Dashboard/Widget und für das Cron-Fenster verwendet. Der Sync (`src/index.js`, 139) nimmt dagegen das UTC-Datum (`isoDate(new Date())`). Weil er nur zwischen 07:00 und 23:58 Berlin läuft (05:00–22:58 UTC), ist das UTC-Datum zu allen Sync-Zeiten identisch mit dem Berliner Datum.
- Woche = Montag–Sonntag (`mondayOf`, `dashboard.js`, 75; `mondayOnOrBefore`, `date-utils.js`).

### 1.3 Zeitplan

Siehe Teil A, Abschnitt A3.1.

---

## 2. VDOT und Paces (`src/vdot.js`)

**Grundsatz:** Der maßgebliche VDOT kommt aus **Runalyze** (`snapshot.vdot`, über `save_runalyze_snapshot`). Der Worker rechnet keinen eigenen VDOT aus Training, Puls oder Intervals-Rennen. `vdot.js` enthält nur noch drei Rechnungen nach Daniels: VDOT aus einer Rennzeit, Pace-Bereiche aus einem VDOT und Rennzeiten aus einem VDOT.

### 2.1 Daniels-Formel (`computeVdotFromRaceTime`, 6)

Eingabe: Distanz `d` (m), Zeit `T` (s). Gültig nur bei `d ≥ 400` und `T ≥ 60`.

```
v   = d / T · 60                                   [m/min]
t   = T / 60                                       [min]
VO2 = −4,60 + 0,182258·v + 0,000104·v²
%VO2max = 0,8 + 0,1894393·e^(−0,012778·t) + 0,2989558·e^(−0,1932605·t)
VDOT = VO2 / %VO2max
```

Ergebnis wird verworfen (null), wenn `VDOT < 20` oder `> 90`; sonst auf 1 Nachkommastelle gerundet.

| Leistung | VDOT |
|---|---|
| 10 km in 50:00 | 40,0 |
| 5 km in 25:00 | 38,3 |
| Halbmarathon (21 097 m) in 2:00:00 | 36,5 |

Verwendet wird sie nur für die Zusatzschätzungen „aus 5-km-/10-km-Bestzeit“ (7.4). Eingabe ist dort die **offizielle Renndistanz** aus Runalyze (`officialDistanceKm`), nicht die GPS-Distanz.

### 2.2 Pace-Bereiche (`paceTargetsFromVdot`, 51)

Wie in den Runalyze-Lauftabellen: Anteil der Geschwindigkeit bei vVO2max (die Geschwindigkeit, bei der die VO2 dem VDOT entspricht). vVO2max kommt aus der umgestellten VO2-Formel (`velocityFromVo2`):

```
a = 0,000104;  b = 0,182258;  c = −(4,60 + VDOT)
vVO2max = (−b + √(b² − 4ac)) / (2a)          [m/min]
Basis   = 60000 / vVO2max                    [s/km bei 100 %]
Pace    = Basis / (Anteil / 100)             je Grenze; Mitte = Basis / (Mittelwert der Grenzen / 100)
```

| Bereich | Anteil an vVO2max |
|---|---|
| Easy (E) | 59–74 % |
| Marathon (M) | 75–84 % |
| Threshold (T) | 88–92 % |
| Interval (I) | 95–100 % |
| Repetition (R) | 105–110 % |

Zwischen den Bereichen liegen bewusst Lücken, wie in der Tabelle. Ergebnisse (ausgeführt, langsam–schnell):

| VDOT | E | M | T | I | R |
|---|---|---|---|---|---|
| 36,5 | 8:23–6:41 | 6:36–5:53 | 5:37–5:23 | 5:12–4:57 | 4:43–4:30 |
| 45 | 7:05–5:39 | 5:34–4:58 | 4:45–4:32 | 4:24–4:11 | 3:59–3:48 |
| 50 | 6:30–5:11 | 5:07–4:34 | 4:21–4:10 | 4:02–3:50 | 3:39–3:29 |

### 2.3 Rennzeit-Prognose (`predictRaceTimesFromVdot`, 101)

Weil %VO2max von der Dauer abhängt, wird iteriert (`predictRaceTimeSeconds`, 12 Schritte):

```
t₀ = Distanz_km · 4                          [min, Startschätzung 4 min/km]
wiederhole 12×:
    %VO2max(t) = 0,8 + 0,1894393·e^(−0,012778·t) + 0,2989558·e^(−0,1932605·t)
    VO2   = VDOT · %VO2max(t)
    v     = Geschwindigkeit(VO2)               (umgestellte Formel wie 2.2)
    t     = Distanz / v
Zeit [s] = round(t · 60)
```

Distanzen: 3 000, 5 000, 10 000, 21 097, 42 195 m. Beispiel VDOT 36,5 → 3 km 15:11, 5 km 26:02, 10 km 54:04, HM 1:59:53, M 4:07:31 (ausgeführt; die HM-Zeit passt zur Rückrechnung in 2.1). Das ist eine Rechnung, keine Vorhersage: Sie unterstellt Ausdauer wie über die Ausgangsdistanz und ist für längere Strecken tendenziell zu optimistisch.

---

## 3. Zielrennen und Triathlon-Format

### 3.1 Distanz eines Kalendereintrags (`block-phase.js`, `normalizeEventDistance`, 3)

Text mit `5k`/`5 km`, `10k`/`10 km`, `half|hm|halb`, `marathon|42` (in dieser Reihenfolge), sonst eine Zahl (< 1000 → km, sonst m) mit den Bändern 4,9–5,1 / 9,5–10,5 / 20,5–21,5 / 41–43 km. Sonst `null`.

### 3.2 Triathlon-Format (`parseTriathlonEvent`, 31)

Intervals kennt keinen Triathlon als Rennart. Ein Eintrag gilt als Triathlon, wenn Name oder Typ `triathlon`, `tri` (als Wort), `ironman` oder `70.3` enthält. Das Format kommt per Regex aus Name, Typ und Beschreibung, **erster Treffer** in der Reihenfolge Sprint, Olympisch, Mittel, Lang; ohne Treffer Olympisch.

| Format | Muster | Schwimmen | Rad | Lauf | Lauf-Distanz |
|---|---|---|---|---|---|
| Sprint | `sprint` | 0,75 km | 20 km | 5 km | 5k |
| Olympisch | `olymp`, `kurzdistanz`, `standard` | 1,5 | 40 | 10 | 10k |
| Mittel | `mittel`, `70.3`, `half iron`, `halb iron` | 1,9 | 90 | 21,0975 | HM |
| Lang | `lang` oder `langdistanz` als ganzes Wort, `ironman`, `140.6`, `full iron` | 3,8 | 180 | 42,195 | M |

Zeitziele aus der Beschreibung: Wort `lauf|run`, `schwimm|swim`, `rad|bike` direkt gefolgt von `m:ss` oder `h:mm:ss`; zwei Teile werden als **Minuten:Sekunden** gelesen, drei als Stunden:Minuten:Sekunden. Watt: `rad|bike … <2–3 Ziffern> w|watt`.

### 3.3 Zielrennen (`goal-race.js`, `deriveAutoGoalFromRaces`, 22)

Ziel ist das nächste **A-Rennen** im Intervals-Kalender ab heute, bis 400 Tage voraus. Zielzeit = `time_target`, sonst `moving_time` des Eintrags (Sekunden, > 0). Beim Triathlon ist `distance` die Lauf-Distanz des Formats, die Zielzeit die Gesamtzeit. Ohne Eintrag gibt es kein Ziel (`null`).

**A-Rennen-Erkennung** (`event-utils.js`, `isARaceEvent`): Kategorie ∈ {`RACE_A`, `A_RACE`, `A-RACE`, `RACE A`, `A`} oder kompakt `RACEA`/`ARACE`; alternativ ein Prioritätsfeld = „A“ **und** Typ enthält „RACE“. **B-Rennen** (`isBRaceEvent`): Kategorie `B`, `RACE_B` o. ä.

**Kein Ziel ohne A-Rennen** (`buildDashboard`): Steht kein A-Rennen im Kalender, ist das Ziel leer (`source: "none"`, `none: true`): keine Renn-Kacheln, Zielpaces oder Prognosen. Eine Zielzeit gibt es nur aus dem Kalendereintrag. Das „letzte Rennen“ für die Erholungsphase (`findRecentRace`) kommt ebenfalls nur aus dem Kalender (A-Rennen der letzten 30 Tage). Die frühere feste Vorgabe Landsberg 03.10.2026 ist seit #1291 entfernt.

Keine Blockphasen mehr: Den Trainingsblock gibt der Saison-Eintrag „Block …“ im Kalender vor (7.9), nicht ein Rechenmodell aus dem Renndatum.

---

## 4. Formcheck (entfallen)

`form-analysis.js` mit Flags, Schweregraden und Erholungs-Ampel (grün/gelb/rot) gibt es nicht mehr. Der Worker liefert nur Erholungswerte gegen die eigene Basis (6.2). Was „gelb“ oder „rot“ ist, legt der Coach-Skill fest (Ruhepuls, HRV, Schlaf-Score gegen die Basis).

## 5. Wochenvergleich (entfallen)

`weekly-progress.js` mit Urteil (besser/schlechter), Empfehlung und TSB-Texten gibt es nicht mehr. An seine Stelle tritt die Montagsbilanz als Faktenliste (6.4); das Urteil fällt der Coach.

---

## 6. Dashboard: Belastung, Erholungswerte, Montagsbilanz (`src/dashboard-summary.js`)

### 6.1 Belastung (`computeLoad`)

```
Datensatz = letzter Eintrag mit CTL und ATL (nur Tage ≤ heute)
TSB   = CTL − ATL
TSB % = TSB / CTL × 100          (nur wenn CTL > 0)
Zone (wie Intervals.icu, in % der CTL):
  < −30 %  hohes Risiko (bad) | −30 bis −10 % produktiv belastet (ok) | −10 bis +5 % neutral
  +5 bis +25 % frisch (ok)    | > +25 % Übergang: Fitness sinkt
ACWR  = ATL / CTL                (nur noch fürs Widget)
```

Die Zonen hängen an der Fitness: Bei CTL 20 ist TSB −10 schon −50 %, bei CTL 40 nur −25 %. Feste Punktgrenzen (früher −10/−25) haben jede Belastungswoche als „belastet“ markiert, obwohl genau das der Trainingsbereich ist. Der ACWR erscheint auf der Seite nicht mehr: Er zeigt dieselbe Information wie der TSB (ATL gegen CTL) und hat bei jeder geplanten Entlastungswoche „zu niedrig“ gemeldet; das Steigerungsrisiko deckt die Leitplanke Ramp Rate (7.9) ab. Die alten Punktgrenzen sind seit #1291 entfernt. Das Widget bekommt dieselben %-Zonen (`tsbZones`, `tsbPct`, `tsbZone`), das Scriptable-Widget zeigt den TSB mit Prozentwert.

### 6.2 Erholungswerte (`computeRecovery`, `computeBody`, `computeDrift`)

**Grundsatz:** Das Dashboard bewertet den Tag nicht. Es gibt keine Ampel, keine Tagesentscheidung und keine Umfangsvorgabe mehr; das Urteil fällt der Coach in der Morgen-Auswertung (`save_coach`, Karte oben im Heute-Band). Der Worker rechnet nur die Werte, auf die der Coach schaut.

Körperwerte (Ruhepuls, HRV, Schlaf): Wert von heute gegen die **Basis = Median der 28 Tage vor heute** (ohne heute), **ohne Krankheitstage und die 3 Tage danach** (Kalender: Kategorie SICK/INJURED oder Notiz „krank“), nur Tage mit Wert, ab 5 Werten. Ausreißer (HRV unter 70 % bzw. Ruhepuls über +8 des Medians, z. B. eine Krankheitsnacht ohne Kalendereintrag) bleiben draußen. Dazu der **7-Tage-Schnitt** (heute und 6 Tage davor, mind. 4 Werte) gegen den Median der 28 Tage vor dieser Woche.

**Langzeitbasis** (`computeDrift`): 7-Tage-Schnitt (ohne Krankheitstage) gegen den Median der Tage 29–90 vor heute aus der 180-Tage-Wellness der Formkurve (mind. 20 Werte). Nur Werte, kein Signal.

Gefühlswerte (`sleepQuality`, `fatigue`, `soreness`, `mood`, `motivation`; 1 = bestmöglich): Sie werden **vor** dem Training eingetragen und beschreiben den Ausgangszustand. Fehlt der Eintrag von heute, steht der von gestern da (`entryDate`). Üblicher Bereich = 25–75 %-Quantil aller Werte vor dem Eintragstag im 56-Tage-Fenster (ab 5 Werten), dazu der Median. Farbe (`cls`, Widget und Dashboard) nur im Vergleich damit: im Bereich oder besser = ok, über dem Bereich und ≥ 1 schlechter als der Median = warn, ≥ 2 = bad. Das ist kein Urteil über den Tag.

`sick` = Krankheit im Kalender heute oder gestern (als Fakt im Heute-Band).

### 6.3 Heute-Band

Oben die Aussage des Coaches mit seinem Urteil (`go`: Hart erlaubt / Locker geplant (neutral, keine Warnfarbe) / Frei) und seinen Blockern (beides aus `save_coach`), dazu seine Rückfrage (ohne Aussage von heute: „Heute noch keine Coach-Auswertung“), daneben die geplante bzw. letzte Einheit. Unter der Karte die Idee der Woche und der **Wochenvorschlag** (`week_plan`, montags per `save_coach`, sichtbar bis Sonntag derselben Woche): Zeit und TSS-Rahmen je Sportart (Untergrenze 80 % der Obergrenze des Coaches, Schwimmen nur Zeit), darunter seine Tagesverteilung klein als „Vorschlag“. Kein Abhaken, kein Abgleich mit den Einheiten, keine Ampel; `{}` blendet die Karte aus. Der Kasten **Was du entscheiden musst** (`decisions` aus `save_coach`, aus dem Monats-Review) steht am Anfang des Bereichs Erkenntnisse: höchstens 5 Punkte nach Frist, je mit Empfehlung, Grund, Alternativen, Frist und „ohne Antwort“; er bleibt über die täglichen Updates stehen, `[]` blendet ihn aus, das Dashboard rechnet nichts. Gerbert antwortet je Punkt mit Ja, Nein, einer Alternative oder eigenem Text (`POST /api/coach/decision`, Feld `answer`/`answeredAt` am Punkt); der Coach liest die Antwort per `get_coach` und nimmt den Punkt beim nächsten `save_coach` aus der Liste (Punkte mit gleichem Titel behalten ihre Antwort). Die Antwort auf seine Rückfrage bleibt stehen, solange er dieselbe Frage erneut speichert. Unter den Erkenntnis-Karten steht die Karte **Wissensbasis** (`dashboard:wissensbasis`, `save_wissensbasis`, in `get_coach` unter `wissensbasis`): Stichpunkte des Coaches aus der Literatur in 1–12 einklappbaren Abschnitten (nur der erste offen), je Punkt Text, Badge „Evidenz: stark/mittel/schwach/Praxis“, Badge „passt: hoch/mittel/gering/unklar“ (nur Text, keine Ampelfarben), optional Quelle und „neu“. **Abgleich (alle Felder optional):** `consensus` (`einig`/`erklaert`/`umstritten`/`eine_seite`, Badge „Studienlage“ plus „geprüft TT.MM.“ aus `counterCheckedAt` (YYYY-MM-DD) bzw. „nicht gegengeprüft“), `practice` („Du“, max. 160), `match` (`passt`/`luecke`/`bewusst_anders`/`nicht_pruefbar`), `atYou` (`nicht_getestet`/`laeuft`/`gewirkt`/`nicht_gewirkt`/`offen`), `next` („Nächster Schritt“, max. 160, Vorschlag) und `link` (`{type: insight|experiment|targets, id (max 60)}`; bei `insight` zeigt die Karte den Titel aus den Erkenntnissen, sonst die id). Jeder Punkt ist eine Gegenüberstellung: links „Studie sagt“ (Text, Badges Evidenz/passt/Studienlage mit „geprüft TT.MM.“ bzw. „nicht gegengeprüft“, „neu“, Quelle), rechts „Du machst“ (`practice`, Badges „Abgleich: …“ und „Bei dir: …“), darunter eine Fußzeile mit „Nächster Schritt“ und dem Verweis (`link`: bei `insight` der Titel der Erkenntnis, sonst die id; bei Tests „zum Test: …“). Auf dem Handy liegen die beiden Hälften untereinander. Oben steht das Kreislauf-Band Studie → Abgleich → Selbstversuch → Ergebnis mit den Zählern Studien, Lücken, Tests laufen, Ergebnisse; Tippen filtert die Punkte, nochmal Tippen hebt auf (löst den früheren Umschalter „nur Lücken und Tests“ ab). Darunter eine Zeile mit gewirkt, nicht gewirkt und umstritten. Punkte mit Lücke oder laufendem Test haben einen Rahmen in der Akzentfarbe; keine Ampelfarben, alle Werte stehen als Text. Ungültiges wird mit Feldnamen abgelehnt, alte Stände ohne diese Felder bleiben gültig. Darunter „Noch offen“ (`open`), oben „Stand“ und ggf. „Studien-Ordner geprüft“. Ohne gespeicherten Stand ist die Karte ausgeblendet; ein älterer `fetchedAt` überschreibt nie einen neueren; das Dashboard bewertet nichts. Der Studien-Check der Woche (`save_studie`) bleibt davon getrennt. Darunter als Fakten: Schlaf-Score (sonst Schlafdauer), **Laufen 7 Tage** (Laufminuten der letzten 7 Tage gegen den Wochenschnitt der 4 Wochen davor, als Zahl), Minuten über dem HF-Deckel in der laufenden Woche (6.5), nächster Blocktest (`nextTest`: erster geplanter Test ab heute) bzw. Rennen. Der TSB steht unter der Formkurve.

### 6.3a Tests (`src/tests-snapshot.js`, `save_test`)

Löst den einzelnen Selbstversuch (`save_experiment`, jetzt veraltet, bleibt lesbar und funktionsfähig) ab: mehrere **Tests** laufen gleichzeitig, dazu Kandidaten und abgeschlossene Tests mit „Gelernt“. KV `dashboard:tests` = `{ fetchedAt, tests[] }`, höchstens 40; ist die Grenze erreicht, fällt zuerst der älteste Test mit `status = verworfen` weg, ohne verworfenen Test lehnt `save_test` ab. `save_test` schreibt **einen** Test je Aufruf (Upsert über `id`); ein älterer `fetchedAt` überschreibt nie den neueren Stand desselben Tests. Antwort: `{ ok, id, fetchedAt, warnings[] }`. `get_coach` liefert `tests` (ohne `verworfen`, sortiert `laeuft`, `kandidat`, `urteil`, `gelernt`, innerhalb nach `checkDate`; `[]` ohne Tests), `experiment` bleibt. Das Dashboard bewertet nichts, das Urteil steht nur im Test.

| Feld | Grenze | Pflicht |
|---|---|---|
| `id` | kebab-case, ≤ 60 | ja |
| `title` | ≤ 80 | ja |
| `kind` | `test` \| `paket` \| `begleiter` (Paket: Urteil gilt fürs Paket; Begleiter: prüft nur „stört nicht“, `blockGoal` dann „Begleiter: …“) | ja |
| `discipline` | `lauf` `rad` `schwimmen` `kraft` `erholung` `ernaehrung` `alle` | ja |
| `block`, `blockGoal`, `question`, `change` | ≤ 20, ≤ 140, ≤ 160, ≤ 160 | ja |
| `studyRefs`, `insightRefs` | je ≤ 6 ids (Wissensbasis-Punkte, Erkenntnis-Karten), je ≤ 60 | nein |
| `startDate`, `checkDate`, `decidedAt` | YYYY-MM-DD oder `null` | bei `laeuft` (`decidedAt` = Datum von Gerberts Ja) |
| `metrics` | 1–4 × `{ key ≤ 40 (frei), label ≤ 60, unit ≤ 20, better lower\|higher, baseline, baselineDate, current, currentDate, provisional }`; `better` fehlt nur bei den bekannten keys `lauf_hf130`, `rad_hf165w`, `schwimm_amstueck`, `schwimm_100m` (dann Standardrichtung); `current` liefert wie bisher der Coach | ja |
| `expectation` | `{ confirmed, unclear, refuted }`, je ≤ 120 | bei `laeuft` |
| `load` | `mehr` \| `neutral` | ja |
| `adherence` | `{ rule ≤ 120, minPct 0–100, actualPct 0–100 \| null }`; liegt `actualPct` unter `minPct`, ist `result` höchstens `unklar` (sonst Fehler) | nein |
| `abort` | ≤ 140 | nein |
| `status` | `kandidat` `laeuft` `urteil` `gelernt` `verworfen` | ja |
| `result` | `bestaetigt` `widerlegt` `unklar` `gestoert` `abgebrochen`; nur bei `urteil`/`gelernt` (dort Pflicht) | – |
| `confoundedWith` | Test-ids paralleler Tests | nein |
| `learned` | ≤ 200, nur bei `gelernt` (dort Pflicht) | – |
| `proposedBy` | `coach` \| `gerbert` | ja |
| `updatedAt` | setzt der Worker (= `fetchedAt`) | – |

**Warnungen** (kein Fehler, in der Antwort und als Hinweis über der Karte): mehr als zwei laufende Tests mit `load = mehr`; zwei laufende Tests (`test`/`paket`) teilen einen `metrics.key` ohne gegenseitiges `confoundedWith`; unbekannte `studyRefs`/`insightRefs` (gespeichert, aber gemeldet).

**Karte „Tests“** (`renderTests`, erscheint, sobald es Tests gibt; eine Karte „Experiment“ gibt es im Dashboard nicht mehr): vier Stationen Kandidaten → läuft → Urteil → Gelernt mit Anzahl (Tippen filtert, nochmal Tippen hebt auf), darunter „n laufen, davon m mit Mehrbelastung“ und die Warnungen. Je Test: Titel, Badges (Test/Paket/Begleiter, Disziplin, Block, Mehrbelastung), Frage, „Zahlt ein auf“, Gründe (links aufgelöste Wissensbasis-Punkte mit Evidenz, passt und Studienlage, rechts Erkenntnis-Karten mit Stärke; fehlende grau „nicht gefunden“), Änderung, Start und Prüfdatum mit „noch x Wochen“, Messgrößen (Ausgangswert → aktueller Wert, grau bei `provisional`; Veränderung erst ab Prüfdatum oder mit Ergebnis, nie farbig bewertet), Erwartung, Einhaltungsbalken mit Mindestquote, Ergebnis und „Gelernt“ hervorgehoben, „vermischt mit …“. Erkenntnis-Karten, die ein Test in `insightRefs` nennt, zeigen „Teil von Test …“.

**Verknüpfung mit der Wissensbasis:** Punkte haben ein optionales `id` (kebab-case, ≤ 60, eindeutig), `link.type` erlaubt zusätzlich `test`. Ist ein Punkt mit einem Test verknüpft (`link` am Punkt oder `studyRefs` eines Tests; verworfene Tests zählen nicht), zeigt das Dashboard „Du machst“ = `change` des Tests und „Bei dir“ aus dem Test und ignoriert gespeichertes `practice`/`atYou`; dazu der Link „zum Test“. Zuordnung: `kandidat` → „nicht getestet (Kandidat)“, `laeuft` → „Test läuft“, `bestaetigt` → „gewirkt“, `widerlegt` → „nicht gewirkt“, `unklar`/`gestoert`/`abgebrochen` → „offen“; bei `begleiter` statt „gewirkt“ „stört nicht“, statt „nicht gewirkt“ „stört“ (zählt nicht als gewirkt). Bei mehreren Tests gilt der laufende, sonst der mit Urteil, sonst der Kandidat. Ohne Test gelten die gespeicherten Felder, aber „gewirkt“ gibt es nie ohne Test-Ergebnis (Anzeige „offen“), und „Studienlage: einig“ zeigt das Dashboard nur mit `counterCheckedAt` (sonst „nur eine Seite“). Die Ableitung passiert beim Lesen (`applyTestsToWissensbasis`), gespeichert und von `get_coach` geliefert wird die Wissensbasis unverändert. `save_wissensbasis` meldet diese Fälle als `warnings`.

### 6.4 Montagsbilanz (`week-review.js`, `buildWeekReview`)

Faktenliste zur **letzten vollen Woche**, verglichen mit den bis zu 4 vollen Wochen davor. Keine Stufe, kein Vorschlag für die neue Woche: das entscheidet der Coach.

| Größe | Rechnung |
|---|---|
| Gefühl | Wochenschnitt Ermüdung, Muskelkater (und Stress, falls in Intervals erfasst) gegen den Schnitt der 4 Wochen davor |
| Session-RPE | Σ RPE × Minuten (Einheiten ab 10 min mit RPE); nur bei mindestens 70 % RPE-Abdeckung angezeigt |
| Monotonie | Tagesschnitt ÷ Standardabweichung über 7 Tage (Ruhetage = 0), aus Session-RPE, bei < 70 % RPE-Abdeckung aus dem Tages-TSS |
| Laufen | Wochenminuten gegen den 4-Wochen-Schnitt, längster Lauf gegen den längsten der 4 Wochen davor, Minuten über dem HF-Deckel (Blocktest-Modus) |
| Rahmen | Ist-Minuten gegen die Zeitziele des Coaches (Lauf, Rad, Schwimmen), TSS gegen die Obergrenzen |
| Kraft | Sätze der Blockwoche gegen das Wochenziel |
| Krank, Hüfte/Leiste/Knie | Tage mit Krankheit im Kalender bzw. Erwähnung in den Notizen |

### 6.5 Grundlagenläufe aus den Zeitreihen (`analyzeRunStreams`, `buildBaseRuns`)

Im Blocktest-Modus (7.9) bekommt jeder lockere Lauf ab 20 min mit Puls die Auswertung aus den Intervals-Zeitreihen (`time`, `heartrate`, `distance`, sonst `velocity_smooth` aufsummiert), gespeichert je Aktivität in KV (`icu:runanalysis:v3:<id>:<Deckel>:<Grenze>`). Nur Laufen, die Rad-Auswertung ist davon unberührt. Grenzwerte in `RUN_RULES` (`block-inputs.js`).

**1. Zeitachse = Bewegungszeit** (`runMovingAxis`): Intervals liefert `time` als vergangene Zeit. Eine Lücke über 5 s (Auto-Pause) zählt 1 s, Stehen (mindestens 5 s am Stück unter 0,5 m/s) zählt nicht. Alle Fenster und Paces laufen auf dieser Achse.

**2. GPS-Plausibilität** (`cleanRunDistance`), vor allen Berechnungen:

```
Tempo je Probe = ΔDistanz / ΔZeit
verdächtig: Tempo > GPS_MAX_RUN_SPEED (4,0 m/s) oder Sprung > 15 m in höchstens 1 s
Segment = verdächtige Proben mit Lücken ≤ 5 s
im Segment: ΔDistanz = Median-Tempo der 60 s davor und danach × ΔZeit, danach kumulierte Distanz neu
gps = { segments, correctedM }   (correctedM = entfernte Meter)
```

**3. Startausblendung** (`DECOUPLING_SKIP_START_S` = 300 s): Der Puls steigt in den ersten Minuten aus dem Stand, deshalb beginnen alle Fenster bei Minute 5.

```
Fenster 5–T (T = 60, 75, 90 … min bis zur Gesamtdauer), nach Zeit in zwei Hälften geteilt (5–32,5 und 32,5–60)
Effizienz je Hälfte = (Distanzzuwachs / Zeit) / mittlere HF (nur Proben mit HF > 40)
Decoupling % = (Eff_1 / Eff_2 − 1) × 100
stabile Dauer = längstes Fenster mit Decoupling ≤ 5 % (0 = schon 5–60 darüber, „unter 60 min“)
Pace 5–60 = 3300 s / bereinigte Distanz von Minute 5 bis 60
über Deckel = Sekunden mit HF > Deckel (je Probe höchstens 10 s), in Minuten
```

Gehpausen bleiben drin, Stehen und Auto-Pausen nicht. Die Karte „Grundlagenläufe ab 60 min“ zeigt je Lauf Dauer (statt km, die Pace ist durch den Deckel bewusst langsamer), Decoupling 5–60, stabile Dauer (Fensterreihe beim Darüberfahren), Pace 5–60, „GPS korrigiert: −x km“ (wenn Meter entfernt wurden), Minuten über dem Deckel und DFA α1 aus Runalyze (`dfaAlpha1` im Snapshot, gleicher Tag; nur als Hinweis ohne Ampel, weil der Runalyze-Wert beim Nutzer nicht stimmt – er entscheidet nichts). Blocktests sind markiert. Die Kennzahlen der letzten Einheit zeigen beim Lauf dieselben Werte; das Decoupling über den ganzen Lauf entfällt dort, weil es mit der Länge wächst.

---

## 7. Dashboard: Wochen, Longruns, Rennplan, Triathlon

### 7.1 Wochen und Tage (`dashboard.js`)

- `HISTORY_DAYS = 56`; Wochen von dem Montag an, der `heute − 55` enthält, bis heute. `complete = (Montag + 6) < heute`.
- Je Woche und Sportart: `count`, `minutes = moving_time/60` (gerundet), `km`, `load`; Laufumfang `km` zählt nur Läufe; dazu `runMin` (Bewegungszeit der Läufe) und `plannedRunMin` bzw. je Sportart `plannedMin` aus `moving_time` der geplanten Workouts. Dashboard und Widget zeigen den Umfang in Stunden (`runMin`, `minutes`), km nur noch bei Pace, Renndistanzen und langen Läufen.
- Plan: nur Kalender-Einträge der Kategorie `WORKOUT`. `plannedKm = Σ (distance_target ?? distance)/1000`, `plannedLoad = Σ (icu_training_load ?? load_target)`.
- Wochenziele des Coaches (`isWeekTarget`): Kalender-Einträge der Kategorie `TARGET` (in Intervals „Weekly“, jeden Montag je Sportart), außer einem Trainingsblock („Block …“). Je Woche und Sportart `targetMin = Σ time_target/60` (das **Ziel**), `targetLoad = Σ load_target` (die **Obergrenze**) und `targetLoadMin` (die **Untergrenze** des TSS-Korridors, `targetLoadFloor`): je Eintrag `TARGET_LOAD_FLOOR_SHARE` = 80 % von `load_target`, oder eine eigene Untergrenze aus der Beschreibung („min 220“, „mindestens 220“, „Untergrenze 220“; höchstens die Obergrenze; ohne `load_target` keine), dazu die Summen je Woche. Ohne Eintrag `null`. Gibt es Wochenziele, ersetzen sie überall das TSS-Soll aus den geplanten Workouts: Dashboard (Wochenkopf, Tabelle, Laufumfang-Marker), Widget (`week.targets` mit `lo`/`under` für die Untergrenze, `rings.tss` mit der Untergrenze als viertem Element, Training-Ansicht `targetMin`/`targetLoad`/`targetLoadMin`, Detail `tssWeeks[].goalMin`) und Uhr (noch ohne Untergrenze) (`rz` Minuten, `rt` TSS, je `[Sportart, erledigt, Vorgabe]` für Rad, Lauf, Schwimmen). Formprognose (`targetDailyLoad`): Wochensumme = Mitte der TSS-Korridore (`(targetLoadMin + targetLoad) / 2`, ohne Untergrenze die Obergrenze) plus Sportarten mit Zeitziel ohne Grenze (Zielminuten × TSS je Minute der Vorwochen); laufende Woche: Rest nach dem Erledigten gleichmäßig auf die Tage nach heute, danach dieselbe Woche je Tag /7. „Über der Obergrenze“ heißt `load > targetLoad` (rot), „unter der Untergrenze“ `load < targetLoadMin` (gedämpft, keine Wertung).
- Tages-Heatmap: Tageslast = Σ `activityLoad`; leere Tage 0.

### 7.2 Einheitenart (`classifyRun`, 138) und Pulsregel

Reihenfolge, der erste Treffer gewinnt:

1. `race`: `sub_type` RACE oder Rennflag von Intervals, Renneintrag im Kalender am selben Tag mit passender Distanz (± 15 %) oder Runalyze-Art „Wettkampf“ (gleicher Tag, Distanz ± 15 %). Wörter im Namen („HM Race Pace 3×3 km“) zählen nicht (seit #1291).
2. `intensity`: Intervall-Tag oder -Text (1.1), Name mit `tempo|tdl|schwelle|wettkampfspezifisch|fartlek` oder dem großgeschriebenen Kürzel `MIT`. Steigerungen/Strides machen aus einem lockeren Lauf keine Intensitätseinheit.
3. `long`: in Runalyze als „Langer Lauf“ eingetragen (gleicher Tag, Distanz ± 15 %).
4. `base`: Name oder Beschreibung mit `grundlagen|easy|ga1|ga 1|regeneration|recovery`.
5. sonst `unknown`.

**Puls und Decoupling werden nur bei `base` und `long` ausgegeben**, bei allem anderen `null`. Decoupling eines Laufs kommt nur aus Runalyze (derselbe Tag, Distanz ± 15 %), nie von Intervals.

### 7.3 Longrun-Tracker (`buildFitness`, `dashboard.js`, 579)

Wird nur noch fürs Widget und als Rückfall berechnet: Runalyze-Läufe der Art „lang“ und Intervals-Läufe ab **15 km**; `count16` = Anzahl ≥ 16 km. Die Seite zeigt stattdessen die Grundlagenläufe ab 60 min aus den Zeitreihen (6.5). Die frühere Warnung „kein Lauf ab 16 km … Zielpace“ ist entfallen.

### 7.3a Renn-Kacheln, VDOT und Prognosen

Nur mit einem kommenden A-Rennen im Kalender. Ohne Rennen ist das Ziel als `none` markiert (3.3): keine Renn-Kachel, kein Rennplan, kein VDOT mit Daniels-Paces (die E-Pace widerspricht im Grundlagenblock dem HF-Deckel), keine Prognosen.

### 7.4 Halbmarathon-Schätzungen (`buildHmEstimates`, 632)

| Schlüssel | Berechnung |
|---|---|
| `runalyze` | Prognose aus Runalyze (Sekunden), unverändert |
| `vdot` | HM-Zeit (2.3) aus dem **Runalyze-VDOT** |
| `best-5`, `best-10` | VDOT aus der Bestzeit (offizielle Distanz, ± 5 %) nach 2.1, davon HM-Zeit nach 2.3 |

Bestzeit je Distanz (`bestForDistance`, `runalyze-snapshot.js`, 72): schnellstes Rennen, dessen offizielle Distanz höchstens 5 % abweicht; gerechnet wird mit der **tatsächlichen offiziellen Distanz** des Rennens.

### 7.5 Rennplan-Szenarien (`raceScenarios`, `app.js`, 503)

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

Schwellen aus den Sport-Settings (`buildThresholds`, `dashboard.js`, 320): `threshold_pace` in **m/s** → Lauf `round(1000 / v)` s/km, Schwimmen `round(100 / v)` s/100 m; Feldname laut Kommentar ungeprüft. Im Frontend: Summe der drei Zeitziele plus Warnung, wenn `Summe + 120 s > Gesamtziel`; Verpflegung Rad: ein Gel alle 40 min bis 10 min vor Ende.

### 7.7 Kraft im Wochenrahmen

Die frühere Zielmarke „60 min Kraft pro Woche“ gibt es nicht mehr. Kraft zählt in Sätzen (`strengthSets2w`, 7.7a) oder, wenn der Coach `strengthSets2w` auf 0 setzt und in der Kraft-Zeile der Intensität „N×“ schreibt („Mindestens 1× pro Woche“), in Einheiten pro Woche (`kraftSessionGoal`, `targets-snapshot.js`). Kraft zählt nie zur Ausdauerzeit.

### 7.7a Wochenziele: Ausdauerstunden und Kraftsätze (`week-totals.js`)

Eine Rechnung für Dashboard-Karte „Woche“, Diagramm „Ausdauerzeit pro Woche“, Zeit-Widget und `get_coach` (`week_totals`).

- Ziele setzt der Coach per MCP-Tool `save_targets` (`targets-snapshot.js`, KV `dashboard:targets`): `enduranceHoursPerWeek` (1–20, 0,25er-Schritte), `strengthSets2w` (0–300), `streakThresholdPct` (Standard 90), `maxIncreasePct` (0–50, Standard 10), `intensity` ([{ sport: lauf|rad|schwimmen|kraft, text ≤ 160 }], max. 6), `guardrails` (max. 8 × 160 Zeichen), `notAllowed` (max. 8 × 80 Zeichen), `reason`, `validFrom` (Standard: Montag der laufenden Woche). Alle Wertfelder sind optional: Was fehlt, kommt vom Eintrag, der an `validFrom` gilt (`intensity` je Sportart, `text: ""` streicht eine; Listen als Ganzes); `changed` hält fest, welche Felder sich geändert haben. Ältere Einträge ohne die neuen Felder erben sie genauso. Jeder Aufruf hängt an die Historie an. Für einen Tag gilt der zuletzt gespeicherte Eintrag mit `validFrom` ≤ Tag, sonst der Startwert `DEFAULT_TARGET` (5 h, 90 Sätze, 90 %, +10 %, Intensität/Leitplanken/Nicht erlaubt aus Blockplan A, ab 19.10.2026, „Startwert aus Blockplan A“).
- Widget „Rahmen“ (Scriptable mittel, Parameter `rahmen`, `buildWidgetRahmen`): Intensität je Sportart mit dem Stand der laufenden Blockwoche aus der Intensitäts-Leitplanke (✓ erledigt / offen / zählt nicht), Nicht erlaubt, Leitplanken, nächster Blocktest, `loadHint` als stille Zeile. Das Block-Widget zeigt Ramp Rate und ACWR nicht mehr als Ziele.
- Karte „Aktueller Rahmen“ (Dashboard): zeigt `targets.current` (Wochenziel, Steigerung, Intensität je Sportart, Leitplanken, Nicht erlaubt), darunter „Geändert am … – Grund“, aufklappbar die Historie mit den geänderten Feldern. Keine Prüfung gegen den Rahmen. Ramp Rate und ACWR haben keine eigenen Kacheln mehr; `loadHint` (`buildLoadHint`) steht nur als stiller Hinweis in der Karte, wenn die letzte Ramp Rate aus Intervals über 5 liegt oder ATL/CTL an mind. 3 der letzten 7 Tage über 1,3.
- Jede Woche trägt das Ziel, das an ihrem Sonntag galt (laufende Woche: heute). Steigt das Ziel, bleiben vergangene Wochen beim alten.
- Ausdauer = Lauf-, Rad- und Schwimm-Typen (`enduranceDiscipline`: `swim` → schwimmen; `ride|bike|cycl|velomobile|handcycle` → rad; `run|treadmill` → lauf). WeightTraining, Yoga, Workout, Walk und Unbekanntes zählen nicht. Summe aus `moving_time`, Aktivitäten mit `icu_ignore_time` fallen weg.
- Woche = ISO-Woche Mo–So nach dem lokalen Startdatum (`start_date_local`, Europe/Berlin).
- Kraftsätze: Zahl vor „Sätze“ in der Beschreibung, sonst je „Wdh × kg“ in den Übungszeilen ein Satz. `strengthSets2w` = laufende Woche + Vorwoche (rollierend).
- Serie: volle Wochen in Folge mit Ausdauer ≥ `streakThresholdPct` % des Ziels der jeweiligen Woche (beim Startwert 4,5 h) bis zur letzten vollen Woche; die laufende zählt mit, sobald sie ihre Schwelle erreicht. Nur innerhalb der 8 gezeigten Wochen. Das Feld heißt aus Kompatibilität weiter `streakWeeksAbove4_5`.

### 7.8 Verlauf der HM-Prognose (`runalyze-history.js`; Anzeige `renderHmTrend`, `app.js`, 779)

Je Tag ein Eintrag (VDOT-Rechnung, Runalyze-Prognosen für 5 km, 10 km, HM; Zuordnung der Prognose zur Distanz mit ≤ 1 % Abweichung), max. 400 Einträge. Veränderung = `Prognose_jetzt − Prognose_früher` (negativ = schneller). Eine Prognose „vor 7 Tagen“ = jüngster Eintrag, der ≥ 7 Tage älter ist.

---

### 7.9 Trainingsblock: Abschlussziele und Leitplanken (`season-block.js`, `block-inputs.js`)

Der Block ist ein Saison-Eintrag im Intervals-Kalender („Block …“). Welche Ziele verfolgt werden, steht im Text der Beschreibung. Ein Absatz (durch Leerzeile getrennt) mit „Lauf“ + „Decoupling“ + „vergleichbar“ ergibt das Laufziel; „Rad“/„Rolle“ + „Decoupling“ + „vergleichbar“ ergibt das Radziel; „Schwimm…“ + „2× 500 m“ ergibt den Schwimmtest. Die Zahlen (≤ 5 %, letzte 3, ≥ 60 min, 75 min, 1 min Pause, 5 s pro 100 m, 8 von 9 Belastungswochen, Ramp Rate ≤ 5, Pause über 7 Tage) liest der Worker aus dem Text. Ein Satz mit „8 % … Muss-Kriterium“ setzt das Muss für beide Decoupling-Ziele. Beim Rad zählt eine Rolleneinheit ab der genannten Dauer minus 5 min; steht „der ersten 60 min“ im Text, wird bei längeren Einheiten nur das Decoupling der ersten 60 min gewertet (aus den Zeitreihen). Steht ein Messfenster mit Einrollen und fester Leistung im Text („10 min Einrollen … 60-min-Messfenster … fester Leistung 165 W“ bzw. „mindestens 60 min bei 165 W“ oder „Vergleichsleistung 165 W“, auch im eigenen Abschnitt „RAD-VERGLEICHSEINHEIT (Protokoll)“), erkennt `session-tests.js` die **Rad-Vergleichseinheit** (nur Ride/VirtualRide, nie Blocktest): Einrollen = Warmup-Block in Intervals (`icu_warmup_time` ≥ 300 s; kürzer: Fenster ab Minute 0, höchstens Ausgangswert); danach ≥ 60 min Hauptteil, im 60-min-Messfenster Ø-Leistung = Vergleichsleistung ± 5 W und VI ≤ 1,05 (aus den Zeitreihen, Cache `icu:ridetest:v1:`), damit fallen HIIT und Intervalle heraus. Decoupling: Intervals-Wert nur bei Gesamtdauer ≈ Einrollen + 60 min (± 5 min) und `icu_warmup_time` ≈ Einrolldauer, sonst selbst gerechnet (zwei Hälften, Effizienz = Ø-Watt / Ø-HF, (Eff1 / Eff2 − 1) × 100, Proben ohne HF ausgelassen); `decSource` sagt, welche Quelle. Override per Name: „Vergleich“ erzwingt die Vergleichseinheit, „Ausgangswert“ ebenso als baseline, „kein Test“ schließt aus. **baseline** (Fahrt ohne Einrollen oder „Ausgangswert“ im Namen, z. B. 19.10.2026: 9,8 %) wird angezeigt (hell vor den Säulen, Orientierung im Widget), zählt aber nicht ins Mittel der letzten 3 und nicht in den Puls-Trend. Nicht klassifizierte Fahrten (ältere Eingaben ohne `category`) prüft weiter `rideMeasureIssue` (Warmup-Block ≥ 300 s, Messfenster ≥ 55 min bei 160–170 W). Bewertet (Soll/Muss) wird erst ab der ersten echten Vergleichseinheit.

**Rad-Trendpunkt (Kategorie `trend`, `session-tests.js`: `rideTrendWindowOf`, `rideTrendStats`, `classifyRideTrend`).** Seit 03.11.2026 wertet der Coach jede lockere, gleichmäßige Rollenfahrt als Trendpunkt (Watt pro Puls und Decoupling), auch ohne Einrollen und ohne Testprotokoll; das Dashboard zeigt dieselben Zahlen. Ein Trendpunkt ist **kein Test**: er zählt nie fürs Blockziel, nicht ins Mittel der letzten 3 Vergleichseinheiten, nicht in `goalCourse` und nicht in den Puls-Trend; Wert, Status und `pass` der Rad-Karte bleiben gleich (Snapshot-Test in `season-block.test.mjs`). Eine Vergleichseinheit oder ein Ausgangswert kann zusätzlich Trendpunkt sein (z. B. 19.10.2026).

- **Bedingungen** (Grenzwerte in `TEST_RULES`): Ride/VirtualRide auf der Rolle (`trainer` = true oder VirtualRide); Bewegungszeit ≥ `trendMinMin` (45 min); kein Blocktest, Basistest, FTP- oder Rampentest im Namen; Pulsdaten vorhanden (`has_heartrate`/`average_heartrate`, im Fenster ≥ 60 Proben mit HF); im Messfenster VI ≤ `trendMaxVi` (1,05, damit fällt HIIT heraus) und Ø-Leistung = Vergleichsleistung aus dem Blocktext (`testConfigOf`, derzeit 165 W) ± `trendWattsTol` (10 W). „kein Test“ im Namen schließt auch den Trendpunkt aus. Jede Fahrt trägt den Grund (`testReason` bzw. `trendReason`).
- **Ablauf** (`ablauf`): `mit_einrollen`, wenn `icu_warmup_time` ≥ `trendWarmupSec` (300 s), sonst `ohne_einrollen`. Verglichen wird nur innerhalb derselben Gruppe.
- **Messfenster** (`windowStartS`): mit Einrollen ab Ende des Warmup-Blocks (`icu_warmup_time`), ohne Einrollen ab Minute 10 (`trendNoWarmupStartSec`), jeweils bis zum Ende der Zeitreihe. Gleiche Rechnung wie `rideWindowStats`.
- **Werte:** `ef` (Watt pro Puls) = Ø-Leistung im Fenster / Ø-HF im Fenster (nur Proben mit HF), 2 Nachkommastellen. `decoupling` = Fenster nach Zeit in zwei Hälften, Effizienz je Hälfte = Ø-Watt / Ø-HF, (Eff1 / Eff2 − 1) × 100, Proben ohne HF ausgelassen, 1 Nachkommastelle. Dazu `date`, `durationMin`, `avgWatts`, `avgHr`, `vi`, `windowMin`, `ablauf`, `windowStartS`. Handrechnung (Test): 55 min ohne Einrollen, Fenster 600–3316 s, 165 W, HF 100 bzw. 105 je Hälfte → EF 165 / 102,5 = 1,61, Decoupling (1,65 / 1,571 − 1) × 100 = 5,0 %.
- **Cache:** `icu:ridetrend:v1:<id>:<windowStartS>`. Die Zeitreihen werden je Fahrt einmal geladen und für Vergleichseinheit und Trendpunkt genutzt.
- **Anzeige:** in der Rad-Vergleichskarte unter den Säulen eine Reihe „Trendpunkte“, je Gruppe („ohne Einrollen“ / „mit Einrollen“) eine kleine Tabelle (Datum, min, W, W/Puls, Decoupling), höchstens 8 je Gruppe, älteste zuerst. Keine Farbe, keine Soll/Muss-Bewertung, kein Mittel. Beim Darüberfahren: Dauer, Leistung, Ø-HF, Fenster und Grund. Hinweiszeile: „Trend, kein Test. Bewertet wird nur am Blocktest.“
- **`get_coach`:** Feld `ride_trend` mit den letzten 8 Trendpunkten (beide Gruppen, älteste zuerst, `updatedAt`, `note`). Das Dashboard legt sie nach jeder Rechnung unter `icu:ridetrend:latest:v1` ab (geschrieben nur bei Änderung); `null`, solange noch keine abgelegt sind.

**Blocktest-Modus (Lauf):** Stehen Testtermine im Block-Eintrag (Zeile „Blocktests …: Basistest Fr 23.10., dann So 08.11. …“, sonst Lauf-Absatz), wird das Laufziel nur an Blocktests gemessen. Welche Einheit was ist, entscheidet `session-tests.js` aus der absolvierten Aktivität, nicht aus dem Namen (Grenzwerte in `TEST_RULES`, Fallback ohne Angaben im Text `TEST_FALLBACK` mit Warnung): **Lauf-Blocktest** = Sportart Run am Termin ±2 Tage, ≥ 60 min Bewegungszeit, bei mehreren der längste; **Automatisch:** jeder Run ab 65 min (`runAutoMin`) zählt ohne vorherige Entscheidung als Lauf-Blocktest: 5 min Einlaufen, Messfenster Minute 5–65 (Läufe von 60 bis 65 min, z. B. der Basistest 23.10., werden bis zum Ende gewertet); **Lauf-Trend** = Run ≥ 60 min mit Ø-HF unter dem HF-Deckel, kein Blocktest (zählt nicht fürs Ziel). **Rad-Vergleich:** jede Fahrt auf Ride/VirtualRide, die für Einrollen + Messfenster lang genug ist (10 + 60 min, 1 min Toleranz, ohne Cooldown), zählt automatisch: die ersten 10 min sind das Einrollen (`bikeAutoWarmupSec`, unabhängig vom Warmup-Block in Intervals), danach 60 min Messfenster; gewertet wird nur bei Ø 165 ±5 W und VI ≤ 1,05, sonst normale Einheit. Kürzere Fahrten ohne Warmup-Block bleiben höchstens Ausgangswert. Override per Name: „Blocktest“/„Basistest“ erzwingt den Blocktest (nur Lauf), „kein Test“ schließt aus. Jede Einheit trägt `category` und `testReason` (Dashboard: beim Darüberfahren). Ziel 1a: Ø Decoupling 5–65 der letzten 3 Tests (Ziel/Muss aus dem Text), Pace 5–65 des letzten Tests höchstens 5 s/km langsamer als im Basistest (darunter Rauschen). Ziel 1b: stabile Dauer (6.5) im letzten Test länger als im Basistest. Ohne Testtermine gilt die ältere Regel mit vergleichbaren Läufen (unten). Das HF-Band des Tests („HF-Band 120–128“ oder „Testband 120-128“, Binde- oder Halbgeviertstrich) steht im Lauf-Absatz oder in einem eigenen Abschnitt wie „BLOCKTEST LAUF (Protokoll)“; die Kopfzeile der Block-Karte zeigt es als „Testband 120–128“.

- **Fenster:** ab Blockstart. Vor dem Start zählen die letzten 8 Wochen als Ausgangswert (Status „Basis“, keine Entscheidung).
- **Lauf, vergleichbar:** locker (`base`/`long` oder Name mit Long Slow, LIT, locker, Grundlage, GA1, Z2), draußen (kein Laufband), mindestens 60 min, flach (≤ 10 Hm/km) und kein Hitzetag. Ein Hitzetag ist eine Wetter-Temperatur ≥ 22 °C von Intervals oder „Hitze“/„heiß“ in Name, Beschreibung oder Tags. Die Temperatur der Uhr zählt nicht, weil sie durch den Körper erwärmt wird.
- **Rad, vergleichbar:** auf der Rolle (`trainer` oder VirtualRide), mindestens 70 min (75 min − 5), gleichmäßige Leistung (VI ≤ 1,05), locker (IF ≤ 0,80) und kein Intensitätsname (HIT, VO2, FTP, SIT, Pyramide …). Ventilator und Raumtemperatur kann der Worker nicht prüfen. Er zeigt die Leistung der letzten 3 Einheiten; die Temperatur des Radcomputers nicht (der Sensor misst Körperwärme mit). Ein FTP-Test wird über „FTP“ im Namen einer Radeinheit erkannt.
- **Rad, Trendpunkte (`trend`):** lockere, gleichmäßige Rollenfahrten ab 45 min bei Vergleichsleistung ± 10 W und VI ≤ 1,05, getrennt nach `ablauf` (`mit_einrollen` / `ohne_einrollen`), Werte EF und Decoupling aus dem Fenster ab Ende Einrollen bzw. Minute 10 bis Ende. Nur Anzeige in der Rad-Karte und `get_coach.ride_trend`, nie Teil der Zielbewertung (siehe oben).
- **Decoupling:** Laufen: Runalyze-Wert des Tages (lange Läufe), sonst eine eigene Rechnung aus Tempo und Puls; den Intervals-Wert nie (Intervals liefert beim Laufen ohne Laufleistung nichts Brauchbares). Rad: der Wert von Intervals, sonst eine eigene Rechnung aus den Zeitreihen (`decouplingFromStreams`): Tempo bzw. Leistung je Herzschlag in der ersten gegen die zweite Hälfte, nur Proben mit Puls und Bewegung, mindestens 20 min. Auf- und Abwärmzeit der Aktivität (`icu_warmup_time`, `icu_cooldown_time`, in Intervals einstellbar) bleiben wie bei Intervals draußen. Das Ergebnis wird je Aktivität in KV gespeichert. Je Aufruf werden höchstens 6 Einheiten nachgeladen. Ohne Puls gibt es keinen Wert, das steht dann als Hinweis in der Karte.
- **Bewertung Lauf/Rad:** Ø der letzten 3 vergleichbaren Einheiten. ≤ Ziel (5 %) ist „im Soll“, ≤ Muss (8 %) ist „knapp“, darüber „unter Soll“. Pace beim Laufen: Ø der letzten 3 gegen Ø der ersten 3 Vergleichsläufe. Das wird erst geprüft, wenn es mehr als 3 gibt. Ist der Lauf langsamer, ist die Karte höchstens „knapp“, und das Ziel gilt als nicht erfüllt.
- **Schwimmtest:** aus dem Text der Einheit („2x500: 9:40 / 9:52, Pause 45 s“), sonst aus den Intervallen: zwei aufeinanderfolgende Abschnitte mit 500 m ± 10 %, Pause = Abstand dazwischen. Bestanden heißt: Pause ≤ 60 s und die zweiten 500 m höchstens 5 s je 100 m langsamer. Der Status richtet sich nach dem letzten Test. „Knapp“ heißt, ein früherer Test war bestanden.
- **Intensität (Leitplanke):** keine Pflichtquote, kein „unter Soll“, kein Rot (Status immer „Stand“). Regel im Rahmen: „Erst Umfang, dann Intensität“. Je Blockwoche gilt der Rahmen, der in ihr galt (`targets.history`, wie bei den Wochenzielen); der Zieltext kommt komplett daraus: Intensität je Sportart plus die Leitplanke mit „Umfang“ und „Intensität“. Was als Einheit zählt (eine Stelle, `intensityKindOf`): Rad mit HIIT, 30/30, VO2 oder 3–4× 4 min in Titel/Beschreibung (auch des gekoppelten Workouts) oder mind. 4 min in Zone 5 (kein FTP-/Rampentest); Lauf mit „Sprint“ oder „Hügel“. Welche Arten zählen und „ab Woche N“ liest `intensityFromFrame` aus dem Rahmen. Wochenstatus: **Umfangswoche** (Ausdauer der Woche mehr als 3 % über der Referenzwoche, hell mit Pfeil, „Umfang gesteigert – Intensität bewusst nicht“), sonst **Intensität erledigt** (mind. eine Einheit laut Rahmen, grün), **ohne Intensität** (grau); hell und nicht gezählt: Entlastung, vor der Startwoche, Krankheit, Testwoche ohne Intensität. Referenzwoche (`withReference` in `week-totals.js`, auch `referenceMin` in den Wochensummen und `referenceHours` in `get_coach.week_totals`): Mittel der Ausdauerzeit der letzten 3 Wochen ohne leichte Wochen (unter 70 % ihrer eigenen Referenz); dafür lädt das Dashboard Aktivitäten ab 6 Wochen vor Blockstart. Eine Quote im Blocktext („6 von 8 Belastungswochen“) wird nicht mehr ausgewertet. Nennt der Rahmen keine der beiden Arten, entfällt die Kachel.
- **Entlastungswochen:** kein fester Rhythmus. Eine Blockwoche ist Entlastung, wenn der Coach ihren Montag im Wochenvorschlag mit `deload: true` markiert hat (`coach.deloadWeeks`); Ramp Rate, Intensität und Kraft lassen nur diese Wochen aus. Die Zeitleiste markiert nur sie.
- **Kraft:** Hat der Coach im aktuellen Rahmen ein Satz-Ziel für 2 Wochen gesetzt (`save_targets`, `strengthSets2w`), gilt das: jede Blockwoche zählt mit ihrer Vorwoche (auch der vor Blockstart), wie bei `get_coach` und den Wochenzielen; im Soll, wenn jede abgeschlossene Belastungswoche das Ziel erreicht. Sonst Sätze je Woche aus dem Blocktext (EGYM). Entlastungswochen (siehe oben) zählen nicht. Bei „8 von 9“ ist eine Fehlwoche erlaubt („knapp“, wenn sie verbraucht ist). Eine zweite reißt die Leitplanke.
- **Kraft-Karte (Bereich Kraft, Kraft-Widget):** dasselbe Ziel aus dem Rahmen des Coaches (`strengthSets2w`): oben die Sätze der laufenden Woche plus Vorwoche, darunter jede frühere Woche mit ihrer Vorwoche (`rollSets`). Nur ohne Rahmen gilt der Blocktext, dann `EGYM_WEEKLY_GOAL_SETS`, zuletzt 60 Sätze pro Woche.
- **Ramp Rate:** CTL am Wochenende minus CTL am Vortag des Wochenstarts, je abgeschlossener Belastungswoche. Liegt ein Wert über 5, ist die Leitplanke gerissen.
- **Pause:** längste Lücke ohne Aktivität (alle Sportarten, EGYM-Tage zählen mit), der laufende Tag zählt nicht. Eine Notiz „Pause“/„Urlaub“ oder ein Urlaubseintrag im Kalender macht die Lücke zur geplanten Pause.
- **Verlauf** (`goalCourse`, je Abschlussziel): erster Messwert → letzter (Lauf/Rad: Ø der letzten 3) → Ziel, Richtung (besser/schlechter/gleich), nächste Messung. Nur der Trend, keine Stufe und keine Entscheidung. Hauptkurve ist die Leistung bei gleichem Puls: Lauf = Pace 5–60 im HF-Band (Basistest gegen letzten Test), Rad = mittlerer Puls im Messfenster bei fester Leistung (`mainHr` aus `rideWindowStats`, `session-tests.js`). `open` = noch kein Vergleich (unter 2 Messungen).
- **Woche 12:** Die Entscheidung zum Blockende (weiter, verlängern, Ursache klären) trifft der Coach. Das Dashboard zeigt dafür nur den Stand der Ziele (Status gegen Ziel und Muss aus dem Block-Eintrag, `pass`) und den Verlauf.


### 7.10 Letzte Einheit im Heute-Band (`buildLastActivity`, `session-profile.js`)

Ohne Plan für heute zeigt das Band die jüngste erledigte Einheit. Kennzahlen: Dauer, Distanz, Load, Ø Pace (Laufen /km, Schwimmen /100 m), Ø Leistung und NP (Rad, `icu_average_watts`, `icu_weighted_avg_watts`), Intensität (`icu_intensity`), RPE und Gefühl (`feel`: 1 stark … 5 schwach). **Puls und Decoupling nur bei lockeren Läufen** (`base`/`long`, Pulsregel 7.2); im Blocktest-Modus Decoupling 5–60, stabile Dauer und Minuten über dem HF-Deckel (6.5), sonst Decoupling aus Runalyze. Rad und Schwimmen ohne Puls.

Profil (Laufen und Rad): Zeitreihen von Intervals (`watts` bzw. `velocity_smooth`, Puls nur mit Freigabe) in höchstens 120 Abschnitte (mind. 10 s) gemittelt. Pace in s/km; langsamer als 15:00/km bleibt eine Lücke. Das Ergebnis wird je Aktivität in KV gespeichert (`icu:profile:v1:<id>`), auch „keine Zeitreihen“ (404). In der Grafik ist Leistung bzw. Pace die Fläche in Sportfarbe (Pace: schneller = höher, Skala ohne die äußersten 3 %), Puls die Linie mit eigener Skala rechts.

## 8. Ernährung (Yazio-Sync und Dashboard)

### 8.1 Tagessummen (`yazio-client.js`, `fetchYazioDailyNutrition`)

- Einträge vom Typ `product`: `Menge (g) · Nährwert pro g` (Kalorien, Protein, Fett, Kohlenhydrate; Nährwerte je Produkt in KV gecacht).
- `simple_product`/Rezept-Portion mit eigenen `nutrients`: Werte direkt übernommen (nicht multipliziert).
- Einträge ohne beides werden übersprungen und gelistet (`skippedItems`).
- Wellness-Felder (`yazio-sync.js`, `syncYazioRange`): `Calories` gerundet auf kcal, Protein/Carbs/Fat auf 0,1 g.

### 8.2 Kalorienziel (`yazio-sync.js`, `syncYazioRange`; `yazio-client.js`, `fetchYazioDailyGoalKcal`)

```
CalorieGoal = round( Yazio-Tagesziel )      (widgets/daily-summary → goals["energy.energy"])
```

Der Worker rechnet nichts dazu. Yazios Tagesziel enthält schon den Sport, den Yazio importiert (Garmin-Schätzung, eher großzügig). Ob gegessen wie geplant ein Defizit ergibt, zeigt der Gewichtstrend; das bewertet der Coach. Widget und Uhr holen das Ziel live (`fetchYazioDailyGoals`, 10 Minuten in KV gecacht), weil Yazio es im Lauf des Tages mit jedem importierten Training erhöht.

### 8.3 Auswertung im Dashboard (`renderNutrition`, `renderNutritionWeek`, `app.js`)

- Leere Tage (Wert ≤ 0) gelten als „keine Daten“ (`positive`, `dashboard.js`, 384).
- Je Tag: kcal, Ziel und Differenz als Zahlen, ohne Bewertung; „kein Tagebuch“ für abgeschlossene Tage ohne Eintrag.
- Bilanz = Σ (kcal − Ziel) über abgeschlossene Tage mit Ziel; Schnitt = Mittel über abgeschlossene Tage.
- Makro-Balken: `Wert / Ziel`, Balken rot ab 110 %. Kalorienanteil je Makro = `Gramm · 4 (Eiweiß, Kohlenhydrate) bzw. 9 (Fett) / Summe dieser drei`. Die Summe aus 4/4/9 stimmt nicht exakt mit Yazios Kalorien überein (Ballaststoffe, Alkohol, Rundung).

---

## 9. Widget (`src/widget.js`)

- **Rennphase** (`racePhaseFor`, `racePhase`, `phaseDaysFor`): Taper ab `taperFrom` Tagen vor dem Rennen bis zum Renntag, danach `recoveryDays` Tage Erholung; die Erholung nach dem letzten A-Rennen (`recentRace`, 3.3) hat Vorrang. Tage je Distanz: 5 km 3/3, 10 km 5/4, HM 7/7, Marathon 14/14; Triathlon Sprint 5/4, Olympisch 7/5, Mittel 14/7, Lang 14/14 **[P3]**.
- **Wochenziel** (`weeklyGoal`, 65): Summe der geplanten Last der Woche aus dem Kalender; fehlt sie, der Wert `WEEKLY_TSS_GOAL`; sonst kein Ziel.
- **Wochenlast** (`buildWidget`, 105): Summe der Last über alle Sportarten der laufenden und der Vorwoche.
- **Fitness (CTL)** (`buildFitness`, `widget.js`, 299): sieben Wochenpunkte bei `heute − 42, −35, …, −7, heute`; der letzte Punkt wird durch den **letzten vorhandenen CTL** ersetzt; `delta = CTL_heute − erster vorhandener Wochenpunkt` (gerundet), mit der Anzahl Wochen dazwischen.
- **Trainingsverteilung** (`buildWidgetTraining`, 252): Anteil je Sportart (Schwimmen/Rad/Lauf) = Minuten der **letzten bis zu vier abgeschlossenen Wochen** / Gesamtminuten, gerundet auf ganze Prozent; Soll nur, wenn `TRI_SPLIT_TARGET` gesetzt ist (Normierung auf 100 %). „Tage seit letzter Einheit“ aus der jüngsten Tagesaktivität der Sportart.
- **Kopfzeile** (`readiness.verdict`): die Kopfzeile des Coaches von heute, Farbe aus seinem Urteil `go` (hart → ok, locker → warn, frei → bad, ohne Urteil `cls: "coach"`), sonst „Noch keine Coach-Auswertung“ (`cls: "none"`). Seine Blocker (`coach.blockers`) und Rückfrage gehen mit. Keine eigene Ampel, keine Montagsbilanz, kein Befund. Körper- und Gefühlswerte nur als Zahlen.
- **Uhr** (`buildWidgetWatch`): `rd` = Urteil des Coaches (`go`) als ok/warn/bad, ohne Urteil `"none"`; `co = 1`, wenn er heute geschrieben hat; `bl` = Zahl seiner Blocker. Eine eigene Warnung gibt es nicht. Für die Karte nach Tageszeit: `sl` Schlaf heute (h), `hv` HRV der letzten 7 Tage (heute zuletzt), `hm` deren Basis (Median wie in der Erholung), `cl` Kopfzeile des Coaches (höchstens 90 Zeichen, nur noch für ältere Uhr-Versionen, die Uhr zeigt sie nicht mehr), `rv` Schlaf, HRV, Ruhepuls von heute je `[Wert, Basis, Vergleich]` mit dem Vergleich gegen den eigenen üblichen Bereich der Basistage (`body.band` = 10/25/75/90-%-Quantil der Basistage aus `computeBody`, ab 5 Werten): 1 = im Bereich oder besser, 2 = schlechter als der Bereich (unter 25 % bzw. beim Ruhepuls über 75 %), 3 = schlechter als 9 von 10 Basistagen, 0 = ohne Wert oder Bereich; kein Gesamturteil, `wd` Minuten je Tag Mo–So für Lauf, Rad, Schwimmen (erledigt aus den Tagesminuten, geplant ab heute aus dem Wochenplan, wenn die Sportart an dem Tag noch nicht erledigt ist), `wm` Ausdauer der Woche gegen das Ziel (wie die Wochenziele), `la` letzte Einheit nur von heute: Werte, Ende (`start_date` + Gesamtzeit) und `lw` = Wochen seit der letzten mindestens so langen Einheit derselben Sportart im geladenen Verlauf (erst ab 14 Tagen). Puls nur, wenn die letzte Einheit ihn im Dashboard zeigt.
- **Block-Widget:** je Abschlussziel die Richtung des Verlaufs (7.9, `course.dir`), nicht bei Ausgangswerten. Keine Woche-12-Entscheidung.
- **Block-Widget neu** (`buildWidgetKompass`, `view=kompass`): Block, Rahmen und Experiment in einer Antwort, nur zusammengesetzt aus `buildWidgetBlock`, `buildWidgetRahmen` und `buildWidgetExperiment`. Die Intensität des Rahmens steht dort in Kurzform: `intensity[].short` vom Coach (`save_targets`, max. 40 Zeichen), sonst der Text bis zum ersten Komma, Semikolon oder „z. B.“. Die Einzelansichten bleiben für ältere Skripte.
- **Scriptable-Aufteilung** (`public/dashboard/scriptable-widget.js`): Groß = Heute (Woche als ein Balken: Zeit gegen die Summe der Zeitziele, Rahmen gestrichelt), Mittel = Woche je Sportart (mit Zonenzeit), 6 Wochen (Zeit und TSS), Block, Ernährung, Kraft, VDOT; Klein = Schlaf und Form, Ernährung, Fitness. Alte Parameter führen auf die zusammengelegten Ansichten.
- **Cache:** das Dashboard wird 5 Minuten in KV gehalten (`CACHE_MS`), nur bei vollständigen Daten und gleichem Berliner Datum.

---

## 10. Wetter (`src/weather.js`)

Mit #976 entfernt, für den Export (13) wiederhergestellt; nur `export-calc.js` nutzt es, also nur die GitHub Action, nicht der Worker.

- Bevorzugt Intervals-Wetter aus den Kartenpunkten: **Mittelwert** aller Temperatur- und Feuchtewerte über alle Zeitproben, auf 0,1 gerundet; beide Größen müssen vorhanden sein.
- Fallback Open-Meteo-Archiv am GPS-Startpunkt: Stundenwert zur lokalen Startstunde (`start_date_local`), sonst Tagesmittel aller Stunden. Koordinaten auf 3 Nachkommastellen (~111 m) gerundet.
- Laufband: kein Wetter. Das Ergebnis steht je Lauf in D1 (13.4); Open-Meteo-Tage merkt sich die Action nur für die Dauer eines Laufs.
- Wind (`windMs`) nur aus Open-Meteo (`wind_speed_10m`, `wind_speed_unit=ms`); die Kartenpunkte von Intervals liefern keinen, dann `null`. Tageswerte im KV unter `weather:v2:<lat>:<lng>:<Datum>` (`openMeteoDay`, 61; in der Action ein Speicher nur für diesen Lauf), nur gespeichert, wenn das Archiv schon Werte hat (es hinkt einige Tage hinterher).

---

## 11. Freitext-Parser (`src/dashboard-parse.js`)

- **Heißhunger** (`parseCravings`, 12): jeder Eintrag beginnt mit „HH“ oder „Heißhunger“/„Heisshunger“. Uhrzeit `HH h[:|.|h]mm`, optional mit „h“ oder „Uhr“ dahinter (Stunde 0–24, Minute 0–59), `Stärke n`, `Auslöser …`, `davor …`; der Rest bis zum nächsten Komma ist „was“. Nicht lesbare Teile bleiben `null`, der Eintrag wird trotzdem gezählt.
- **Hüfte/Leiste/Knie** (`findHipFlags`, 50): Treffer auf Wörter wie `hüft*`, `leiste*`, `knie*`, `hip`, `groin`; Kniebeuge/Kniehebe-Wörter und Verneinungen in den 30 Zeichen davor (`kein`, `ohne`, `nicht`, `schmerzfrei` …) werden ausgelassen.
- **Workout-Schritte** (`parseWorkoutSteps`, 127): bevorzugt Intervals-`workout_doc`, sonst Beschreibung im Intervals-Format. Dauer: Zahl + Einheit (`h`, `min`/`m`, `s`, `km`, `mtr`); **Distanz wird mit 6:00 min/km (360 s/km) in Zeit umgerechnet**. Intensität: Prozentwert (bei Bereich der Mittelwert) oder Zone `Z1…Z7` → 55/70/85/98/110/125/150 %. Wiederholungen max. 50, Blöcke max. 200.

---

## 12. Prüfhinweise (Auffälligkeiten)

Neu geprüft am 05.11.2026 gegen Commit `88d6dca`. Die Liste vom 03.10.2026 (P1–P21) bezog sich zum großen Teil auf Code, der inzwischen entfernt ist; vier Punkte hat #1291 behoben. Die Nummern bleiben, damit ältere Verweise auffindbar sind.

### Offen

**P3 – Zwei Taper-Definitionen.** Dashboard: Taper = 0–14 Tage vor dem Rennen (`app.js`, `isTaper`). Widget: je Distanz bzw. Triathlon-Format (`widget.js`, `phaseDaysFor`: HM 7 Tage, Mitteldistanz 14 Tage). Für die Mitteldistanz stimmen beide überein.

**P16 – „ACWR“ ist ATL/CTL.** Das ist das Verhältnis zweier exponentiell gewichteter Kurven, nicht das rollierende 7:28-Verhältnis, auf das die üblichen Bänder 0,8–1,3 zurückgehen. Es erscheint nur noch im Widget und im stillen `loadHint` (7.7a).

**Kleinigkeit zu P10:** `mittel` im Triathlon-Muster hat weiter keine Wortgrenze und trifft auch „Mittelgebirge“. Weil Mittel vor Lang geprüft wird, kann das ein Langdistanz-Rennen mit diesem Wort in der Beschreibung als Mitteldistanz lesen. Für den Kalender heute ohne Folgen.

### Erledigt (Code entfernt oder geändert)

| Nr. | Damals | Heute |
|---|---|---|
| P1 | Long-Run-Plan erreicht nach jeder Rücknahmewoche (×0,75) die Spitze nicht | `long-run-plan.js` entfernt. Die Referenzwoche (`withReference`, 7.9) lässt leichte Wochen unter 70 % weg; den langen Lauf steuert der Coach. |
| P2 | Zwei TSB-Skalen (Punkte und %) | #1291: Punktgrenzen entfernt, Widget nutzt die %-Zonen (6.1). |
| P4 | Zwei Phasenmodelle (Automat und Raster) | Beide entfernt; der Block kommt aus dem Kalender (7.9). |
| P5–P8 | Eigener VDOT mit/ohne Korrekturfaktor, zwei VDOT-Quellen, Pace-Benchmark, GPS-Distanz im Renn-VDOT | Eigener VDOT entfernt; Runalyze ist die einzige Quelle (2). Bestzeit-Schätzungen nehmen die offizielle Distanz. |
| P9 | Rennen-Erkennung über Wörter im Namen („Race Pace“) | #1291: Nur noch Intervals-Rennfelder, Kalender und Runalyze-Art „Wettkampf“ (7.2, 1.1). |
| P10 | `lang` ohne Wortgrenze trifft „langsam“ | #1291: `\blang(distanz)?\b` (3.2). |
| P11, P12, P17 | Formcheck: absolute HRV-Schwellen, uneinheitliche Schlafwerte, ein Flag ≥ 0,5 macht gelb | `form-analysis.js` entfernt (4). |
| P13 | Kalorienziel = Diätziel + volle Aktivitätskalorien | Das Ziel ist Yazios eigenes Tagesziel (8.2). Dass Yazio den Sport großzügig obendrauf rechnet, behandelt der Coach-Skill (Gewichtstrend entscheidet). |
| P14 | `doc.txt` veraltet | `doc.txt` gibt es nicht mehr. |
| P15 | Fest eingebautes Fallback-Ziel Landsberg 03.10.2026 | #1291: entfernt; ohne A-Rennen kein Ziel (3.3). |
| P19, P20 | Toter Code in `computeWeeksToEvent`, 1 vs. 2 Läufe beim Trainings-VDOT | Funktionen entfernt. |
| P21 | Wochenvergleich addiert Schwimm-, Rad- und Laufkilometer | `weekly-progress.js` entfernt. Umfang über alle Sportarten nur in Zeit (7.7a), km nur je Sportart. |

---

## 13. Export für die Langzeitanalyse (`src/export.js`, `src/export-calc.js`, `scripts/export-sync.mjs`)

Der Coach soll über die ganze Trainingshistorie rechnen können. Der Export liefert dafür **eine Zeile pro Lauf** und **eine Zeile pro Tag**, nur Rohwerte und Kennzahlen, keine Bewertung (Grundsatz aus `CLAUDE.md`).

**Aufteilung** (der Worker bleibt im kostenlosen Plan, 13.4):

| Teil | Datei | Was |
|---|---|---|
| Rechnung | `src/export-calc.js` | Kennzahlen aus den Zeitreihen, Wetter, Zeilen (13.2, 13.3, 13.5); wiederverwendet `runMovingAxis`, `cleanRunDistance`, `classifyRun`, `isRun`, `sessionLoad`, `weather.js` |
| Schreiben | `scripts/export-sync.mjs`, `scripts/export-to-d1.mjs` | GitHub Action (A3.1a): Intervals abrufen, rechnen, Upsert nach D1 |
| Schema | `src/export-schema.js` | Spalten, D1-Tabellen, `METHOD_VERSION` (gemeinsam für Action und Worker) |
| Lesen | `src/export.js` | Worker: MCP `get_run_export`/`get_day_export` (`mcp-server.js`), HTTP `/api/export/runs|days` (`index.js`, `handleExportRequest`, 94); nur D1, kein Abruf, keine Auswertung |

### 13.1 Parameter und Antwort (`buildActivityExport` 51, `buildDayExport` 72)

- `from`/`to` (ISO-Datum): Standard erster Tag in der jeweiligen D1-Tabelle bis gestern (Berlin).
- `limit`: Standard 200, höchstens 500. `cursor`: Läufe `<startLocal>|<id>` der letzten Zeile (sortiert nach Startzeit), Tage das nächste Datum. `nextCursor = null` heißt letzte Seite.
- Antwort: `methodVersion` (aktuelle Methode im Code), `from`, `to`, `total` (Zeilen im Zeitraum), `count`, `pending` (Läufe im Zeitraum mit `streamStatus: pending`, also Zeitreihen beim letzten Lauf der Action nicht abrufbar; bei Tagen 0), `syncedAt`, `syncedFrom`, `syncedTo` (letzter Lauf der Action, `export_meta.lastSync`), `nextCursor`, `rows`.
- `format=table` liefert `columns` einmal und die Zeilen als Arrays, `csv` eine Kopfzeile (HTTP: `nextCursor`, `pending`, `methodVersion`, `syncedAt` in den Headern `x-next-cursor`, `x-pending`, `x-method-version`, `x-synced-at`; MCP: in einer ersten Textzeile).
- Zahlen gerundet, fehlende Werte `null`, `tags` als Liste, Wahrheitswerte als `true`/`false`. Jede Zeile trägt ihre `methodVersion`.

### 13.2 Zeile pro Lauf (`export-calc.js`: `runRow`, `baseRunRow` 194)

Alle Aktivitäten mit `isRun`, älteste zuerst.

| Spalte | Quelle |
|---|---|
| `id`, `date`, `startLocal`, `type`, `name`, `tags` | Intervals (`date` = `activityDay`, `startLocal` = `start_date_local`) |
| `treadmill` | `isTreadmill` |
| `race` | `isRaceActivity` (Tag `race:`, `category`, Name, `sub_type`, `race`), **oder ein Renn-Eintrag im Kalender am selben Tag** (`raceContext`: Kategorie `RACE`, `RACE_A`/`B`/`C`, `A_RACE` bzw. A-Rennen) **ohne Distanzabgleich**, damit auch ein abgebrochenes Rennen zählt (03.10.2026: Halbmarathon nach 15,2 km beendet, Eintrag 21,1 km), oder `classifyRun` = `race` (Dashboard-Regel mit Distanz ±15 %). Ohne Runalyze-Kontext |
| `interval` | `isIntervalActivity` oder `hasIntervalTextSignal` |
| `runClass` | `race`, wenn `race` gilt, sonst `classifyRun` (7.2) mit Kalender-Kontext |
| `distKm`, `movingMin`, `elevPerKm` | `distance`/1000, `moving_time`/60, `total_elevation_gain`/km |
| `avgHr`, `maxHr`, `rpe`, `feel`, `load` | `average_heartrate`, `max_heartrate`, `icu_rpe` (sonst `perceived_exertion`), `feel`, `sessionLoad` |
| `tempC`, `humidity`, `windMs`, `weatherSrc` | Abschnitt 10; `weatherSrc` = `intervals` \| `openmeteo` \| `null`. Nie die Uhr; Laufband ohne Wetter |
| `streamStatus` | `ok` \| `pending` (noch nicht berechnet) \| `none` (Zeitreihen 404 oder ohne Puls/Distanz) \| `skip` |
| `streamSkip` | Grund ohne Kennzahlen (`streamSkipOf`, 185): `treadmill`, `race`, `interval`, `short` (unter 20 min Bewegungszeit), `noHr`; dazu `gps` aus der Rechnung (Schutzregel 13.3): dann `streamStatus` `skip` und nur `gpsCorrectedM` |

### 13.3 Kennzahlen aus den Zeitreihen (`export-calc.js`: `analyzeExportStreams`, 52; Grenzwerte `EXPORT_RULES`, 19)

Nur Läufe im Freien, kein Rennen, kein Intervall, mindestens 20 min mit Puls. Abgerufen werden `time`, `heartrate`, `distance`, `velocity_smooth`, `altitude`, `fixed_altitude` (Höhe: `fixed_altitude`, sonst `altitude`).

```
Zeitachse = Bewegungszeit (runMovingAxis), Distanz GPS-bereinigt (cleanRunDistance), wie 6.5, mit EXPORT_RUN_RULES:
  Lücke bis 12 s mit mindestens 0,5 m/s Fortschritt = Aufzeichnungslücke, zählt mit ihrer echten Dauer
  (ältere Uhren mit intelligenter Aufzeichnung, z. B. 2022: Proben alle 5–10 s; als Auto-Pause gezählt landete die
  Strecke auf 1 s Bewegungszeit und die GPS-Bereinigung entfernte 60–70 % der Distanz). Längere Lücken oder Lücken
  ohne Fortschritt bleiben Pausen. Das Dashboard (6.5) rechnet unverändert ohne diese Regel (SPARSE_MAX_GAP_S = 0).
Schutzregel: entfernt die GPS-Bereinigung mehr als 10 % der Rohdistanz (GPS_MAX_SHARE), keine Kennzahlen:
  streamStatus skip, streamSkip gps, nur gpsCorrectedM
Raster je Sekunde Bewegungszeit: Distanz linear interpoliert, Puls/Höhe von der Probe davor (Puls nur > 40)
Unterbrechung = Probe, bei der vergangene Zeit verloren ging (Auto-Pause, Stehen)
Fenster: 30 s, gleitend in 5-s-Schritten, ab Sekunde 300; ungültig bei Unterbrechung im Fenster,
         Puls in weniger als 80 % der Sekunden oder ohne Distanzzuwachs
v = Distanzzuwachs / 30 s,  HF = Ø-Puls im Fenster
flach = |ΔHöhe / ΔDistanz| < 2 % (Höhe vorher ±15 s gemittelt)
```

| Spalte | Rechnung |
|---|---|
| `paceBand125_135`, `paceBand120_128` | Fenster mit HF im Band (Grenzen eingeschlossen): Pace = 1000 / Median(v) in s/km. `null`, wenn die Fenster zusammen unter 300 s abdecken |
| `secBand125_135`, `secBand120_128` | Sekunden im Band (Vereinigung der Fenstersekunden) |
| `paceBand125_135Flat`, `paceBand120_128Flat` | dasselbe nur aus flachen Fenstern; `null` ohne Höhendaten |
| `effAt130`, `effSlope`, `effR2`, `effN` | lineare Regression v (m/min) gegen HF über alle gültigen Fenster mit HF 110–150; `effAt130` = Vorhersage bei HF 130 (m/min), `effSlope` in m/min je Schlag, `effN` = Zahl der Fenster. Unter 20 Fenstern oder ohne Streuung im Puls `null` (nur `effN` bleibt) |
| `decoup` | wie 6.5, aber über den ganzen Lauf: Minute 5 bis Ende, zwei Hälften nach Zeit, Effizienz = (Distanzzuwachs / Zeit) / Ø-HF, `(Eff1 / Eff2 − 1) × 100` |
| `hrDriftBpm` | Ø-HF letztes minus erstes Drittel (Minute 5 bis Ende in Drittel geteilt), nur wenn die Pace der beiden Drittel unter 3 % abweicht, sonst `null` |
| `gradeShareFlat` | Anteil der gültigen Fenster mit Steigung unter 2 % (0–1); `null` ohne Höhendaten |
| `gpsCorrectedM` | von `cleanRunDistance` entfernte Meter |

Gegenprobe mit echten Daten (Fixture 23.10.2026): `decoup` 6,3 % und `gpsCorrectedM` 326 m wie `analyzeRunStreams` (6.5, Fenster 5–65 = ganzer Lauf); Pace im Band 451–460 s/km bei Pace 5–65 von 454 s/km.

### 13.4 GitHub Action, D1, Upsert (`syncExport` 106, `computeRunEntry` 247)

**Warum nicht im Worker.** Der Worker läuft im kostenlosen Plan (Commit `08d6aec`) mit 10 ms CPU je Aufruf, und zwar für den ganzen Aufruf samt Dashboard-Cache und Yazio-Sync. Gemessen (Node 22, V8 wie in Workers, nur Hauptthread, warm): Zeitreihen eines 60-min-Laufs parsen und auswerten 4,4 ms (im frischen Isolate ≈ 12 ms), ein ganzer Aufruf mit 2 Läufen 10–14 ms, die Aktivitätsliste der ganzen Historie (2500 Aktivitäten) allein 29 ms. Deshalb rechnet eine GitHub Action, und der Worker liest nur.

**D1** (Datenbank `training-export`, Binding `EXPORT_DB`, kostenloser Plan: 5 GB, 5 Mio. gelesene und 100 000 geschriebene Zeilen am Tag). Schema aus `schemaStatements` (`export-schema.js`, 38), legt die Action selbst an (`CREATE TABLE IF NOT EXISTS`, fehlende Spalten per `ALTER TABLE … ADD COLUMN`):

| Tabelle | Primärschlüssel | Spalten |
|---|---|---|
| `runs` | `id` (Intervals-Aktivitäts-ID) | alle Spalten aus 13.2/13.3, `methodVersion`, dazu `hash`, `syncedAt`; Index auf `startLocal, id` und `date` |
| `days` | `date` | alle Spalten aus 13.5, `methodVersion`, `hash`, `syncedAt` |
| `rides`, `swims` | `id` | Spalten aus 13.6 bzw. 13.8, `methodVersion`, `hash`, `syncedAt`; Index auf `startLocal, id` und `date` |
| `export_meta` | `key` | `lastSync`: Zeitpunkt, Bereich, `methodVersion`, Zahl der Läufe und `pending` des letzten Laufs |

`tags` steht als JSON-Text, Wahrheitswerte als 0/1; der Worker wandelt beim Lesen zurück (`decodeRow`).

**Ablauf je Lauf der Action** (`syncExport`):

1. Schema sicherstellen.
2. Kalender, Aktivitäten und Wellness des Bereichs aus Intervals (in 180-Tage-Scheiben); aus dem Kalender die Renntage (`raceContext`, wie `buildRaceDays` im Dashboard); dazu das Yazio-Gewicht aus dem KV (`yazio:weight`, über die Cloudflare-API, nur lesen, nur mit KV-Leserecht des Tokens).
3. Je Lauf: die gespeicherte Zeile wiederverwenden, wenn `methodVersion` gleich ist, `streamStatus` nicht `pending`, `streamSkip` gleich (ein gespeichertes `gps` zählt wie „kein Übersprung“) (Name oder Tags können ihn ändern, z. B. „Wettkampf“) und das Wetter nicht fehlt, solange der Lauf höchstens 14 Tage alt ist. Sonst neu rechnen (`computeRunEntry`): Karte (Startpunkt, Intervals-Wetter), sonst Open-Meteo, und ohne `streamSkip` die Zeitreihen. 404 bei Zeitreihen ergibt `none`; andere Abruffehler `pending`, der nächste Lauf der Action rechnet nach.
4. Zeile bilden und hashen; **Upsert nur für neue oder geänderte Zeilen** (`INSERT … ON CONFLICT DO UPDATE … WHERE hash IS NOT excluded.hash`, 40 Zeilen je Anweisung mit Literalen, weil D1 höchstens 100 gebundene Parameter je Anweisung erlaubt).
5. Läufe, die es im Bereich in Intervals nicht mehr gibt, aus D1 löschen.
6. Tage ebenso. Ein früher gespeichertes Yazio-Gewicht bleibt, wenn das KV (nur 90 Tage) keinen Wert mehr liefert.
7. `export_meta.lastSync` schreiben.

Eine neue Methode (`METHOD_VERSION` in `export-schema.js` erhöhen; 2 seit 03.11.2026: Aufzeichnungslücken, GPS-Schutzregel, Rennen ohne Distanzabgleich) rechnet jede ältere Zeile beim nächsten Lauf über ihren Bereich neu; für die ganze Historie die Action einmal mit `full` starten.

**Einrichtung** (einmalig):

1. D1-Datenbank `training-export` anlegen (`npx wrangler d1 create training-export`) und die `database_id` im `[[d1_databases]]`-Block von `wrangler.toml` eintragen (erledigt am 02.11.2026; die Action liest die ID ebenfalls von dort).
2. API-Token in Cloudflare (My Profile → API Tokens → Create Custom Token): Berechtigung **Account → D1 → Edit**, Account Resources **Include → nur dieses Konto**, keine Zonen-Rechte. „Edit“ ist nötig, weil die Action schreibt; „Read“ reicht nicht. Optional zusätzlich „Account → Workers KV Storage → Read“: dann liest die Action das Yazio-Gewicht (`yazio:weight`) aus dem KV, sonst kommt `weightKg` nur aus Intervals und das Log meldet „HTTP 403“.
3. Repo-Secrets `INTERVALS_API_KEY`, `ATHLETE_ID`, `CLOUDFLARE_API_TOKEN`, `CLOUDFLARE_ACCOUNT_ID`.
4. Nach dem Merge die Action einmal mit `full` starten.

**Minutenverbrauch.** Das Repo ist öffentlich; Standard-Runner sind für öffentliche Repos kostenlos und ohne Minutenkontingent. Geschätzt (abgerechnet wird je Job auf volle Minuten aufgerundet):

- täglich: Checkout und Node ≈ 10 s, Abrufe für 14 Tage, 1–2 neue Läufe mit Karte, Zeitreihen und Wetter, Upsert ≈ 20–40 s, also **1 Minute je Lauf, ≈ 31 Minuten im Monat**;
- ganze Historie einmalig: ≈ 1–1,5 s je Lauf (3 Abrufe und 150 ms Pause), bei 1000 Läufen ≈ 20–25 Minuten.

Wäre das Repo privat, lägen diese ≈ 31 Minuten im Monat weit unter den 2000 Gratisminuten. GitHub schaltet geplante Läufe in öffentlichen Repos nach 60 Tagen ohne Aktivität im Repo ab; dann die Action wieder einschalten.

### 13.5 Zeile pro Tag (`export-calc.js`: `buildDayRows`, 314)

Jeder Kalendertag im Zeitraum, auch ohne Daten.

| Spalte | Quelle |
|---|---|
| `ctl`, `atl`, `rampRate` | Intervals-Wellness |
| `loadRun`, `loadBike`, `loadSwim`, `loadStrength` | Σ `sessionLoad` je Sportart (Schwimmen = Typ mit `swim`, Rad `isBike`, Lauf `isRun`, Kraft `isStrengthActivity`) |
| `minRun`, `minBike`, `minSwim` | Σ `moving_time`/60; Schwimmen `swimTimeOf` (ohne nachgeladene Laps = `moving_time`) |
| `strengthSets` | Σ `strengthSetsOf` |
| `hrv`, `restHr`, `sleepH`, `sleepScore`, `sleepQuality`, `fatigue`, `soreness`, `mood`, `motivation` | Wellness wie `buildWellness` (HRV, Schlaf-Score > 0; Skalen ≥ 1; `sleepH` = `sleepSecs`/3600) |
| `sick` | `calendarByDay` (284): Kategorie `SICK`/`INJURED`, Notiz „krank“ oder Name mit „OP“, „Operation“, „Verletzung“; mehrtägige Einträge an jedem Tag (Ende exklusiv, höchstens 120 Tage); Kalender ab 60 Tage vor dem Bereich. Beispiel: `INJURED` „Hüft-OP“ 14.10.–11.11.**2024** (nicht 2023) → `sick` vom 14.10. bis 10.11.2024 |
| `calendar` | alle Nicht-Workout-Einträge des Tages als „Name (KATEGORIE)“, mit „; “ getrennt (Notiz, Krank, Verletzt, Urlaub, Rennen, Wochenziel …), mehrtägige an jedem Tag. Damit erkennt der Coach Phasen wie OP, Krankheit oder Urlaub |
| `weightKg` | Yazio (`yazio:weight`, nur letzte 90 Tage), sonst `weight` aus Intervals |
| `kcal`, `kcalGoal`, `protein`, `carbs` | Zusatzfelder `Calories`, `CalorieGoal`, `Protein`, `Carbs` der Wellness (Yazio-Sync) |

Ohne Training sind Load, Minuten und Sätze 0, fehlende Wellness-Werte `null`.

### 13.6 Zeile pro Radfahrt (`export-tri.js`: `rideRow` 177, `computeRideEntry` 140)

Alle Fahrten mit `isBike` (auch VirtualRide). Tabelle `rides`, MCP `get_ride_export`, HTTP `/api/export/rides`; Parameter und Antwort wie 13.1.

| Spalte | Quelle |
|---|---|
| `id`, `date`, `startLocal`, `endLocal`, `type`, `name`, `tags` | Intervals; `endLocal` = Start + `elapsed_time` |
| `trainer` | `trainer == true` oder Typ mit „virtual“ |
| `movingMin`, `distKm`, `elevPerKm` | wie beim Lauf |
| `avgPower`, `np`, `if`, `vi`, `ftp` | `icu_average_watts`, `icu_weighted_avg_watts`, `icu_intensity`/100 (Intervals liefert Prozent), `icu_variability_index` (sonst NP/Ø), `icu_ftp` (FTP zum Zeitpunkt der Fahrt) |
| `avgHr`, `maxHr`, `avgCadence`, `load`, `rpe`, `feel`, `warmupS` | `average_heartrate`, `max_heartrate`, `average_cadence`, `sessionLoad` (Rad: `power_load`), `icu_rpe`, `feel`, `icu_warmup_time` |
| `intensity` | `intensityKindOf("bike", Name/Beschreibung der Fahrt und des gekoppelten Workouts, Z5-Zeit)`: `bikeHiit` oder `null` |
| `testKind` | Rad-Vergleichseinheit wie im Dashboard (7.9): `rideWindowOf` → `rideWindowStats` → `classifyRide` mit der Vergleichsleistung aus `TEST_FALLBACK` (165 W; die Action kennt den Blocktext nicht). `compare`, `baseline` oder `null` |
| Wetter | wie beim Lauf, nur draußen (`trainer` false) |
| `streamStatus`, `streamSkip` | `rideSkipOf` (51): `noPower` (keine Ø-Leistung), `short` (unter 20 min), `interval` (`intensity`, Intervall-Tag oder -Text), `noHr` |

**Kennzahlen** (`analyzeRideStreams`, 61; Grenzwerte `RIDE_RULES`): Zeitreihen `time`, `watts`, `heartrate`, `distance`, `velocity_smooth`. Zeitachse ohne Stillstand (`runMovingAxis` auf der Distanz, Stehen unter 0,5 m/s). Einrollen: bei `compare`/`baseline` das Fenster des Dashboards, sonst `icu_warmup_time`, ohne Wert die ersten 10 min. Gleichmäßige 30-s-Fenster in 5-s-Schritten: keine Unterbrechung, Puls in mindestens 80 % der Sekunden, Variationskoeffizient der Leistung in den 2 min davor unter 10 % (der Puls läuft der Leistung nach).

| Spalte | Rechnung |
|---|---|
| `hrAt160_170`, `secAt160_170` | Median-Puls der Fenster mit Ø 160–170 W, Sekunden (Vereinigung); `null` unter 300 s. Hauptgröße für den Vergleich mit der Rad-Vergleichseinheit |
| `powerAt120`, `powerAt130`, `pwSlope`, `pwR2`, `pwN` | Regression Leistung gegen Puls über die Fenster mit HF 100–150; unter 20 Fenstern `null`. Bei ERG-Fahrten mit fester Leistung ohne Aussage (26.10.2026: R² 0,003) |
| `ef` | NP / Ø-Puls nach dem Einrollen |
| `decoup` | `decouplingFromStreams` (Pw:HR, 7.9) nach dem Einrollen bis vor dem Ausrollen |
| `decoup60` | erste 60 min nach dem Einrollen (`rideWindowStats`); bei `compare`/`baseline` genau der Wert des Dashboards (`classifyRide`: Intervals-Wert, wenn er das Messfenster deckt) |
| `mainHr` | Ø-Puls im Messfenster, nur bei `compare`/`baseline` |

Gegenprobe (echte Daten): 19.10.2026 `baseline`, Fenster 0–60 min ohne Einrollen, `decoup60` 9,8 (Intervals-Wert, deckt das Fenster), `mainHr` 101. 26.10.2026 „Blocktest“ `compare`, Einrollen 601 s aus `interval_summary`, `mainHr` 113, `decoup60` 4,7 (selbst gerechnet; Intervals 2,0 gilt für die ganze Fahrt), gleich dem Dashboard-Weg.

### 13.7 Koppelläufe (`export-tri.js`: `brickMap` 210)

Spalten in `runs`: `brick` (Lauf beginnt höchstens 30 min nach dem Ende einer Radfahrt; bis 5 min Überlappung der Uhren zählt als Abstand 0), `brickRideId`, `brickGapMin`, `brickRideMin`, `brickRideLoad` (bei mehreren Fahrten die zuletzt beendete). Ende einer Fahrt = Start + `elapsed_time`. Koppelläufe bekommen ihre Kennzahlen ganz normal (13.3), damit die Pace im Pulsband frisch gegen nach dem Rad vergleichbar ist.

### 13.8 Zeile pro Schwimmeinheit (`export-tri.js`: `swimRow` 261, `computeSwimEntry` 243)

Tabelle `swims`, MCP `get_swim_export`, HTTP `/api/export/swims`. Keine Zeitreihen-Kennzahlen; je Einheit einmal die Laps (`/activity/{id}/intervals`).

| Spalte | Quelle |
|---|---|
| `pool`, `poolLengthM` | `open` bei Typ OpenWaterSwim, `pool` bei `pool_length` > 0, sonst `null` |
| `distanceM`, `movingMin`, `pace100` | `distance`; Bewegungszeit `swimTimeOf` mit den Laps (Pausen am Beckenrand fallen raus); `pace100` = Bewegungszeit / (Distanz/100) |
| `avgHr`, `load`, `rpe`, `feel` | wie beim Lauf |
| `test500a`, `test500b`, `testPauseS` | `swimTestFromText` (Name/Beschreibung), sonst `swimTestFromIntervals` (zwei aufeinanderfolgende Intervalle mit 500 m ±10 %) |
| `testPassed` | wie das Blockziel (7.9): Pause ≤ 60 s und zweite 500 m höchstens 5 s/100 m langsamer |
| `lapsStatus` | `ok`, `none` (keine Laps, 404), `pending` (Abruf fehlgeschlagen) |

---

## Anhang: Wie ich die Beispiele erzeugt habe

`node` mit direktem Import von `computeVdotFromRaceTime`, `paceTargetsFromVdot` und `predictRaceTimesFromVdot` aus `src/vdot.js` (Commit `88d6dca`). Keine Netzwerkzugriffe, keine Änderung am Code.

## Gewicht und Ernährung (ernaehrung.js)

- `ernaehrung.weight`: Gewichtseinträge `{date, kg}` der letzten 90 Tage, neueste zuletzt. Quelle ist Yazio (Sync in `yazio-sync.js`, KV `yazio:weight`); gibt es dort keine, das `weight`-Feld aus Intervals-Wellness (`weightSource`: `yazio` oder `intervals`).
- `ernaehrung.weightTrend7d`: je Tag das Mittel aller Einträge der 7 Kalendertage bis einschließlich diesem Tag, vom ersten bis zum letzten Eintrag; Tage ohne Eintrag im Fenster fehlen. Reine Rechnung, keine Bewertung.
- `ernaehrung.coach`: Aussage aus `save_ernaehrung` (`headline`, `text`, `weekLine`, `goal`), das Ziel bleibt bei Updates ohne `goal` stehen. Das Dashboard zeigt sie nur an.
- Die Kalorienbalken der Karte kommen aus den Wellness-Feldern `Calories`/`CalorieGoal` (derselbe Yazio-Sync wie bei `get_nutrition`); 0 kcal gilt als „nicht eingetragen“.
