# Architektur: Datenquellen und geplante Tasks

Dieses Dokument beantwortet zwei Fragen: **Woher kommen die Daten?** und **Wie laufen die geplanten Tasks?** Es beschreibt den Stand des Codes. Wie die Zahlen gerechnet werden, steht in [BERECHNUNGEN.md](BERECHNUNGEN.md), die Uhr in [../garmin/README.md](../garmin/README.md).

Das Projekt heißt noch `intervals-icu-netlify`, ist aber ein **Cloudflare Worker** (`wrangler.toml`), kein Netlify-Projekt.

## 1. Überblick

```
 Intervals.icu ─┐                                   ┌─ Dashboard   /dashboard/ (public/)
 Yazio ─────────┤                                   ├─ iOS-Widget  /api/widget (Scriptable)
 EGYM ──────────┼──►  Cloudflare Worker  ──────────►├─ Garmin-Uhr  /api/widget?view=watch
 Runalyze*  ────┤      (src/index.js)               └─ Claude      /mcp (Connector)
 Coach-Aussage ─┘      + KV-Speicher
```

\* Runalyze hat keine öffentliche API, nur ein MCP. Der Worker kann es nicht selbst abfragen; Daten kommen über den Coach (siehe 2.4).

**Grundsatz** (aus `CLAUDE.md`): Das Dashboard zeigt, der Coach entscheidet. Der Worker rechnet keine Tagesentscheidung. Urteile (`go`, Wochenampel) kommen vom Coach und werden nur angezeigt.

## 2. Woher kommen die Daten?

### 2.1 Intervals.icu (live bei jedem Aufbau)

`src/intervals-client.js` ruft `https://intervals.icu/api/v1` ab, mit `ATHLETE_ID` und `INTERVALS_API_KEY`:

- **Wellness** (HRV, Ruhepuls, Schlaf, Gefühlswerte, CTL/ATL, dazu die Yazio-Felder, siehe 2.2)
- **Aktivitäten** (abgeschlossene Einheiten, `icu_training_load`)
- **Events** (Kalender: geplante Workouts, Rennen, Wochenziele als Kategorie „Target“)
- **Sport-Settings** (FTP, Schwellenwerte, Zonen)

`buildDashboard` (`src/dashboard.js`) holt alles parallel; scheitert eine Quelle, bleibt der Rest heil (`settle`). Load, CTL, ATL, Rampe usw. rechnet **Intervals**, der Worker rechnet damit weiter.

### 2.2 Yazio (Ernährung, Gewicht) → Intervals-Wellness

`src/yazio-client.js` nutzt die inoffizielle Yazio-API (`YAZIO_USERNAME`, `YAZIO_PASSWORD`; Token im KV `yazio:token`). `src/yazio-sync.js` schreibt Kalorien, Eiweiß, Kohlenhydrate, Fett und Kalorienziel **als Zusatzfelder in den Intervals-Wellness-Eintrag** des Tages. Das Dashboard liest sie von dort. Gewichtseinträge der letzten 90 Tage landen im KV (`yazio:weight`). Das Tagesziel holt der Worker beim Neuaufbau direkt aus Yazio (KV-Cache `yazio:goals`). Fehler sind „best effort“ und brechen nichts ab.

### 2.3 EGYM (Kraft)

`src/egym-client.js` nutzt die inoffizielle Netpulse-API (`EGYM_USERNAME`, `EGYM_PASSWORD`, optional `EGYM_BRAND`; Session im KV `egym:session`). `src/egym-intervals-sync.js` schreibt je Kraft-Tag eine manuelle Aktivität (Typ `WeightTraining`, `external_id` = `egym-<Datum>`, damit der Abgleich wiederholbar ist) nach Intervals. Zusätzlich hält `src/egym-widget.js` einen Cache der Kraft-Sätze (`widget:egym-kraft`). Wochenziel: `EGYM_WEEKLY_GOAL_SETS` (Standard 60).

### 2.4 Coach-Aussagen und Runalyze (geschrieben, nicht abgerufen)

Diese Daten erzeugt der **Coaching-Task** (siehe 3). Sie liegen als JSON im KV (`dashboard:*`) und werden validiert (`validate*`-Funktionen in `src/*-snapshot.js`):

| KV-Schlüssel | Inhalt | MCP-Tool | HTTP |
|---|---|---|---|
| `dashboard:coach` | Tagesaussage: `note`, `headline`, `text`, `go`, `blockers`, Rückfrage/Antwort, Idee der Woche, Wochenampel `week`, Wochenvorschlag `week_plan` | `save_coach` | `PUT /api/coach` |
| `dashboard:runalyze` | VDOT, Prognosen, Rennen, Läufe mit Decoupling | `save_runalyze_snapshot` | `PUT /api/runalyze` |
| `dashboard:runalyze-history` | Zeitverlauf der Runalyze-Werte (Tageseintrag, vom Cron) | – | – |
| `dashboard:studie` | Studien-Check der Woche | `save_studie` | `PUT /api/studie` |
| `dashboard:ernaehrung` | Coach-Aussage zu Ernährung und Gewichtsziel | `save_ernaehrung` | – |
| `dashboard:experiment`, `dashboard:profil` | laufendes Experiment, Profil-Punkte | `save_experiment`, `save_profil` | – |
| `dashboard:insights` | Erkenntnisse des Coaches | `save_insight`, `archive_insight` | – |
| `dashboard:targets` | Rahmen (Wochenstunden, Leitplanken), mit Historie | `save_targets` | – |

Drei Wege, wie ein Snapshot ins KV kommt (je nach dem, was die Coaching-Sitzung erreichen kann):

1. **MCP-Connector** (`/mcp`, `src/mcp-server.js`): die `save_*`-Tools. Das ist der normale Weg.
2. **HTTP-PUT** mit `Authorization: Bearer <DASHBOARD_TOKEN>`.
3. **GitHub-Fallback** (`src/github-snapshot.js`): Der Task committet `runalyze.json`, `studie.json`, `coach.json` auf den Branch **`snapshots`** (nicht `main`, damit kein Deploy ausgelöst wird). Der Cron holt sie über `raw.githubusercontent.com` ins KV. Ein älterer Stand überschreibt nie einen neueren (`fetchedAt`-Vergleich). Konfigurierbar über `GITHUB_REPO`, `SNAPSHOT_BRANCH`, `GITHUB_TOKEN` (nur bei privatem Repo).

Der Coach liest vor jedem Update mit `get_coach` den gesamten Stand zurück (letzte Aussage, Antwort auf die Rückfrage, Wochensummen, Rahmen, Ernährung …).

### 2.5 Antworten aus dem Dashboard

Das Dashboard schreibt zwei Dinge zurück: die **Antwort auf die Rückfrage** des Coaches (`POST /api/coach/answer`) und **angepasste Zeiten im Wochenvorschlag** (`POST /api/coach/plan`, baut danach den Cache sofort neu). Der Coach liest beides am nächsten Morgen.

## 3. Geplante Tasks

Es gibt **zwei getrennte Sorten**, die man nicht verwechseln darf.

### 3.1 Cron des Workers (im Repo, `wrangler.toml` + `src/index.js`, `scheduled`)

Zwei Cron-Einträge (UTC):

- `*/15 3-22 * * *`: alle 15 Minuten, 03:00–22:45 UTC
- `58 21-22 * * *`: Minute 58 in den UTC-Stunden 21 und 22, damit einer der beiden Ticks unabhängig von Sommer-/Winterzeit auf **23:58 Berlin** fällt (letzter Yazio-Sync des Tages)

Jeder Tick macht:

| Schritt | Wann | Was |
|---|---|---|
| Snapshots aus GitHub holen, danach Runalyze-Tageseintrag | jeder Tick | `syncSnapshotsFromGithub` → `recordRunalyzeHistory` (2.4, Weg 3) |
| Dashboard-Cache warm halten | jeder Tick | `warmDashboardCache`: baut den Stand aus allen Quellen neu auf, damit die Uhr sofort antwortet |
| Yazio → Intervals | 07:00–23:59 Berlin | `syncYazioRange` für heute; der erste Tick um 07:xx holt zusätzlich gestern nach |
| EGYM → Intervals, danach EGYM-Cache | 1× pro Stunde (Tick mit Minute < 15) im selben Fenster | `syncEgymToIntervals`, dann `loadWidgetKraft(env, true)` |

Fehler werden geloggt und stoppen die anderen Schritte nicht.

**Cache für Widget und Uhr** (`src/widget.js`): Der Aufbau aus allen Quellen dauert länger, als die Uhr wartet. Deshalb antwortet `/api/widget` sofort aus dem Cache von heute (`widget:dashboard-cache`, gilt 5 Minuten) und baut im Hintergrund neu; der Cron hält ihn warm. `GET /api/watch-status` (ohne Token) zeigt Abrufe der Uhr und das Alter des Caches, praktisch bei Fehlersuche.

### 3.2 Coaching-Task (geplanter Task bei Claude, **nicht** im Repo)

Jeden Morgen läuft ein geplanter Task in Claude mit dem Skill **`triathlon-coach`**. Er:

1. liest per MCP `get_coach` (Vortag, Antwort auf die Rückfrage, Rahmen, Wochensummen),
2. liest Daten aus Intervals (`get_wellness`, `get_recent_activities`, `get_planned_workouts`), Yazio (`get_nutrition`) und Runalyze (dessen eigenes MCP),
3. bewertet und schreibt das Ergebnis zurück: `save_coach` (Tagesaussage, `go`, Rückfrage), montags zusätzlich Wochenampel und Wochenvorschlag, sonntags der Studien-Check (`save_studie`), außerdem `save_runalyze_snapshot`, `save_ernaehrung`, `save_targets`, `save_insight`.

Der Task läuft in einer Cloud-Sitzung, die den Worker per HTTP nicht immer erreicht (Egress-Policy), GitHub aber schon. Darum gibt es den Branch-Fallback (2.4, Weg 3).

> **Offen / bitte ergänzen:** Uhrzeit, Wochentage und der Prompt des Tasks stehen in der Task-Konfiguration bei Claude, nicht im Repo. Ich habe sie nicht gesehen und deshalb nicht geraten. Wenn sie hier stehen sollen, bitte kurz die Task-Einstellungen nennen. Gleiches gilt für den Inhalt des Skills `triathlon-coach`.

### 3.3 Ablauf eines typischen Tages

```
 05:00–07:00  Cron: Cache neu, Snapshots von GitHub (noch Stand von gestern)
 morgens      Coaching-Task: get_coach / Daten lesen → save_coach (+ weitere)
              → Cron übernimmt ggf. den GitHub-Branch → Cache neu → Uhr/Widget zeigen Urteil
 07:00–23:58  Cron alle 15 Min: Yazio → Intervals-Wellness; stündlich EGYM → Intervals
 tagsüber     Uhr holt alle 5 Min /api/widget?view=watch (Antwort aus dem Cache)
 abends       Rückfrage im Dashboard beantworten → Coach liest sie am nächsten Morgen
```

## 4. Zugriff und Geheimnisse

| Zugang | Wofür | Prüfung |
|---|---|---|
| `DASHBOARD_TOKEN` | Dashboard, Widget, alle Schreib-Endpunkte | Bearer-Header (`isAuthorized`) |
| `WATCH_TOKEN` | nur `/api/widget?view=watch` | eigenes Token, öffnet das Dashboard nicht |
| MCP-OAuth | Claude-Connector `/mcp` | `src/mcp-oauth.js`: ein Nutzer, ein Client (`claude`), PKCE; braucht `MCP_CLIENT_SECRET`, `MCP_OWNER_PASSCODE`; Access-Token 1 h, Refresh-Token 60 Tage |
| `INTERVALS_API_KEY`, `ATHLETE_ID` | Intervals.icu | Secret |
| `YAZIO_*`, `EGYM_*` | Yazio, EGYM | Secrets |
| `GITHUB_TOKEN` | nur bei privatem Repo (Snapshot-Branch) | optional |

Optionale Variablen: `WEEKLY_TSS_GOAL`, `TRI_SPLIT_TARGET`, `EGYM_WEEKLY_GOAL_SETS`, `GITHUB_REPO`, `SNAPSHOT_BRANCH` (Beschreibung in `wrangler.toml`). Secrets werden im Cloudflare-Dashboard bzw. mit `wrangler secret put` gesetzt, nie im Repo.

## 5. Wo schaue ich nach, wenn etwas fehlt?

- **Uhr zeigt „!“ oder alte Daten** → `GET /api/watch-status` (Cache-Alter, letzter Abruf), Fehlercodes in `garmin/README.md`.
- **Coach-Aussage fehlt heute** → Hat der Task `save_coach` geschrieben? Sonst im Branch `snapshots` nach `coach.json` schauen; der Cron übernimmt sie beim nächsten Tick. Die Aussage gilt als veraltet nach `COACH_MAX_AGE_HOURS` (36 h).
- **Kalorien/Eiweiß stimmen nicht** → kommen aus dem Intervals-Wellness-Eintrag; der Yazio-Abgleich läuft höchstens 15 Minuten verzögert (nur 07:00–23:59 Berlin).
- **Kraft fehlt** → stündlicher Abgleich; von Hand: `POST /api/egym-sync` (Bearer `DASHBOARD_TOKEN`) liefert das Ergebnis oder die genaue Fehlermeldung.
- **Logs** → Cloudflare-Dashboard (Worker `intervals-icu-netlify`); Fehler der Cron-Schritte stehen mit `console.error` dort.
