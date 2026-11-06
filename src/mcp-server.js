// MCP (Model Context Protocol) endpoint: exposes read-only Intervals.icu data
// (planned workouts, past activities, wellness) and the Yazio nutrition diary as
// tools so a Claude chat can be added as a custom connector against this Worker,
// reusing the existing intervals-client.js / yazio-client.js fetchers instead of
// second integrations. get_coach liest die letzte Coach-Aussage samt Rückfrage und Antwort.
// get_run_export/get_day_export liefern den Export fuer die Langzeitanalyse aus D1 (export.js, gefuellt von GitHub Actions),
// get_export_stats dessen Zusammenfassung je Woche/Monat/Quartal/Jahr.
// Die einzigen schreibenden Tools sind save_runalyze_snapshot,
// save_studie, save_wissensbasis, save_coach, save_decisions, save_ernaehrung, save_experiment, save_test, save_profil, save_insight, archive_insight und save_targets: Sie legen das Dashboard-Snapshot im KV ab (Weg fuer Sitzungen, die weder den
// Worker per HTTP noch GitHub erreichen, aber diesen Connector nutzen).
import { json } from "./http-helpers.js";
import { isIsoDate, isoDate, isoDateBerlin, listIsoDaysInclusive } from "./date-utils.js";
import { isValidAccessToken } from "./mcp-oauth.js";
import { fetchIntervalsEvents, fetchIntervalsActivities, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { hasKv, readKvJson, writeKvJson } from "./kv.js";
import { RUNALYZE_KV_KEY, validateSnapshot } from "./runalyze-snapshot.js";
import { STUDIE_KV_KEY, validateStudie } from "./studie-snapshot.js";
import { saveWissensbasis, readWissensbasis, wissensbasisWarnings, wissenForCoach } from "./wissensbasis-snapshot.js";
import { readArchiv, archivWarnings, archivForCoach, visibleWissensbasis } from "./wissensbasis-archiv.js";
import { TESTS_KV_KEY, TEST_KIND, TEST_DISCIPLINE, TEST_LOAD, TEST_STATUS, TEST_RESULT, TEST_PROPOSED_BY, validateTest, upsertTest, resetGuard, testWarnings, refWarnings, readTests, testsForCoach, provenTests, ADOPTION_STATE } from "./tests-snapshot.js";
import { saveCoach, readCoach } from "./coach-snapshot.js";
import { saveDecisions, readDecisions } from "./decisions-snapshot.js";
import { EXPERIMENT_KV_KEY, PROFIL_KV_KEY, validateExperiment, validateProfil, readExperiment, readProfil } from "./experiment-snapshot.js";
import { INSIGHTS_KV_KEY, validateInsight, validateArchive, upsertInsight, archiveInsight, insightsForCoach, readInsights } from "./insight-snapshot.js";
import { computeWeekTotals, currentWeekTotals, weekTotalsOldest } from "./week-totals.js";
import { withSwimIntervals } from "./swim-time.js";
import { TARGETS_KV_KEY, COACH_HISTORY, validateTargets, appendTarget, readTargets, targetsView } from "./targets-snapshot.js";
import { ERNAEHRUNG_KV_KEY, validateErnaehrung, mergeErnaehrung, buildErnaehrung, WEIGHT_DAYS } from "./ernaehrung.js";
import { hasYazioCredentials, fetchYazioDailyNutrition, fetchYazioDailyGoalKcal } from "./yazio-client.js";
import { buildDayExport, buildActivityExport, buildExportStats, STATS_GROUPS, STATS_MAX_METRICS, RUN_COLUMNS, DAY_COLUMNS, RIDE_COLUMNS, SWIM_COLUMNS, toTable, toCsv, DEFAULT_LIMIT, MAX_LIMIT } from "./export.js";
import { warmDashboardCache } from "./widget.js";
import { readRideTrend } from "./block-inputs.js";
import { RIDE_TREND_NOTE } from "./session-tests.js";

// get_nutrition fetches one Yazio API round-trip per day in the range (Yazio has
// no range endpoint), sequentially to stay gentle on Yazio's rate limiting - so the
// range is capped to keep a single MCP call within the Worker's execution time.
const MAX_NUTRITION_DAYS = 31;

const PROTOCOL_VERSION = "2025-06-18";

const EXPORT_ARGS = {
      type: "object",
      properties: {
        from: { type: "string", description: "Startdatum YYYY-MM-DD, Standard: erster Tag in der Datenbank." },
        to: { type: "string", description: "Enddatum YYYY-MM-DD, Standard: gestern." },
        cursor: { type: "string", description: "nextCursor der vorigen Antwort, um weiterzublättern." },
        limit: { type: "number", description: `Zeilen je Seite, Standard ${DEFAULT_LIMIT}, max. ${MAX_LIMIT}.` },
        format: { type: "string", enum: ["table", "json", "csv"], description: "table (Standard): columns einmal, rows als Arrays in dieser Reihenfolge. json: rows als Objekte. csv: Text mit Kopfzeile." },
      },
    };

const TOOLS = [
  {
    name: "get_planned_workouts",
    description: "Geplante Workouts/Events aus dem Intervals.icu-Trainingskalender für einen Datumsbereich.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute." },
        newest: { type: "string", description: "Enddatum ISO YYYY-MM-DD, Standard: heute + 14 Tage." },
      },
    },
  },
  {
    name: "get_recent_activities",
    description: "Abgeschlossene Trainingsaktivitäten aus Intervals.icu für einen Datumsbereich.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute - 14 Tage." },
        newest: { type: "string", description: "Enddatum ISO YYYY-MM-DD, Standard: heute." },
      },
    },
  },
  {
    name: "get_wellness",
    description: "Tägliche Wellness-Daten (HRV, Ruhepuls, Schlaf, CTL/ATL/Form) aus Intervals.icu für einen Datumsbereich.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute - 14 Tage." },
        newest: { type: "string", description: "Enddatum ISO YYYY-MM-DD, Standard: heute." },
      },
    },
  },
  {
    name: "get_nutrition",
    description:
      "Ernährungstagebuch aus Yazio für einen Datumsbereich, Tag für Tag: Tagessummen (Kalorien, Protein, Fett, Kohlenhydrate, Kalorienziel) sowie die einzelnen gegessenen Gerichte/Lebensmittel mit Name, Mahlzeit und Nährwerten.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute - 14 Tage." },
        newest: { type: "string", description: `Enddatum ISO YYYY-MM-DD, Standard: heute. Bereich max. ${MAX_NUTRITION_DAYS} Tage.` },
      },
    },
  },
  {
    name: "save_runalyze_snapshot",
    description:
      "Schreibt den Runalyze-Snapshot (VDOT, Prognose, Rennen, Läufe mit Art und Decoupling) fürs Dashboard. fetchedAt bei jedem Aufruf auf die aktuelle Zeit setzen. Nur Werte aus Runalyze übergeben, nichts erfinden.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        vdot: { type: "number", description: "effectiveVO2max aus Runalyze get_calculations (optional)." },
        prognosis: {
          type: "array",
          description: "Aus get_prognosis: distance -> distanceKm, prognosis_seconds -> seconds.",
          items: { type: "object", properties: { distanceKm: { type: "number" }, seconds: { type: "number" } } },
        },
        races: {
          type: "array",
          description: "Aus get_historical_races: date, name, official_distance -> officialDistanceKm, official_time -> officialTimeSec.",
          items: {
            type: "object",
            properties: { date: { type: "string" }, name: { type: "string" }, officialDistanceKm: { type: "number" }, officialTimeSec: { type: "number" } },
          },
        },
        runs: {
          type: "array",
          description: "Läufe der letzten 8 Wochen aus get_activities (date, distance -> distanceKm, duration -> durationSec, type = vom Nutzer gesetzte Art, z. B. Langer Lauf) plus aerobic_decoupling_pace aus get_activity_details -> decouplingPct und dfa.avg -> dfaAlpha1 (beides nur bei Langen Läufen und Grundlagenläufen ab 60 min holen, sonst weglassen).",
          items: { type: "object", properties: { date: { type: "string" }, distanceKm: { type: "number" }, durationSec: { type: "number" }, type: { type: "string" }, decouplingPct: { type: "number" }, dfaAlpha1: { type: "number" } } },
        },
      },
      required: ["fetchedAt"],
    },
  },
  {
    name: "save_studie",
    description: "Schreibt den Studien-Check der Woche fürs Dashboard (nur mit belastbarer Quelle).",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        text: { type: "string", description: "Text des Abschnitts, max. 6000 Zeichen." },
        source: { type: "string", description: "Quelle (Autor/Journal/Jahr)." },
        sourceUrl: { type: "string", description: "https-Link zur Quelle (optional)." },
        week: { type: "string", description: "Datum YYYY-MM-DD (optional)." },
        title: { type: "string", description: "Titel (optional)." },
      },
      required: ["fetchedAt", "text", "source"],
    },
  },
  {
    name: "save_wissensbasis",
    description: "Schreibt die Wissensbasis des Coaches als Stichpunkte fürs Dashboard (Abschnitte mit Punkten, je Evidenz und Übertragbarkeit). Ersetzt den ganzen Stand. Optional adoption { rule (max 200, ein Satz), conditions (max 160, wann die Regel gilt) } bei Punkten mit Handlungsbezug (Knopf „Als Wissen übernehmen“ im Dashboard); Punkte ohne Handlungsbezug bekommen keins. Schreibrechte: Du schreibst nur rule und conditions und legst neue Punkte offen an. state (offen|uebernommen|abgelehnt) und decidedAt setzt nur Gerbert per Knopf; der Worker übernimmt beide per id aus dem gespeicherten Stand, auch wenn du sie sendest. Ändert sich rule/conditions eines entschiedenen Punkts, bleibt state stehen und das Dashboard fragt „Regeltext geändert – prüfen?“. Punkte mit adoption bekommen ohne id eine aus dem Text; bestehende Punkte werden per id, sonst per Text zugeordnet, sende id mit. Entschiedene Punkte liefert get_coach unter wissen. Punkte, die Gerbert im Dashboard archiviert hat (id siehe get_coach.wissensbasisArchiv), werden gespeichert, aber nicht angezeigt; die Antwort warnt dann je Punkt. Nicht neu vorschlagen.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601. Ein älterer Wert überschreibt nie einen neueren." },
        updatedAt: { type: "string", description: "Stand der Wissensbasis, YYYY-MM-DD." },
        folderCheckedAt: { type: "string", description: "Letzte Prüfung des Studien-Ordners, YYYY-MM-DD (optional)." },
        sections: {
          type: "array",
          description: "1–12 Abschnitte mit je 1–8 Punkten.",
          items: {
            type: "object",
            properties: {
              title: { type: "string", description: "Abschnittstitel, max. 60 Zeichen." },
              items: {
                type: "array",
                items: {
                  type: "object",
                  properties: {
                    text: { type: "string", description: "Stichpunkt, max. 220 Zeichen." },
                    evidence: { type: "string", enum: ["stark", "mittel", "schwach", "praxis"] },
                    fit: { type: "string", enum: ["hoch", "mittel", "gering", "unklar"], description: "Übertragbarkeit auf Gerbert." },
                    source: { type: "string", description: "Quelle, max. 80 Zeichen (optional)." },
                    isNew: { type: "boolean", description: "Zeigt „neu“ und zählt im Filter „neu“ (optional)." },
                    id: { type: "string", description: "Stabile id (kebab-case, max. 60), Ziel von studyRefs der Tests (optional). Ist ein Test verknüpft, leitet das Dashboard „Du machst“ und „Bei dir“ aus dem Test ab; practice/atYou werden dann ignoriert, „gewirkt“ gibt es nur über ein Test-Ergebnis." },
                    consensus: { type: "string", enum: ["einig", "erklaert", "umstritten", "eine_seite"], description: "Studienlage: einig / erklärt unterschiedlich / umstritten / nur eine Seite gefunden (optional)." },
                    counterCheckedAt: { type: "string", description: "Datum der letzten Suche nach Gegenbelegen, YYYY-MM-DD; fehlt = noch nicht gegengeprüft (optional)." },
                    practice: { type: "string", description: "„Du machst“: was Gerbert tut, max. 160 Zeichen (optional)." },
                    match: { type: "string", enum: ["passt", "luecke", "bewusst_anders", "nicht_pruefbar"], description: "Abgleich Studie ↔ Praxis (optional)." },
                    atYou: { type: "string", enum: ["nicht_getestet", "laeuft", "gewirkt", "nicht_gewirkt", "offen"], description: "Stand bei Gerbert (optional)." },
                    link: { type: "object", description: "Verweis auf Erkenntnis, Selbstversuch, Rahmen oder Test (optional).", properties: { type: { type: "string", enum: ["insight", "experiment", "targets", "test"] }, id: { type: "string", description: "max. 60 Zeichen." } }, required: ["type", "id"] },
                    next: { type: "string", description: "Nächster Schritt als Vorschlag, Gerbert entscheidet, max. 160 Zeichen (optional)." },
                    forYou: { type: "string", description: "„Für dich bedeutet das“: Studie sagt X, für dich heißt das Y, in Alltagssprache mit dem Grund aus Gerberts Situation, max. 200 Zeichen. Nur Text; wird nie aus einem Test abgeleitet oder überschrieben. Fehlt es, kommt eine Warnung." },
                    adoption: { type: "object", description: "Übernahme-Frage (optional, nur bei Handlungsbezug). state/decidedAt werden ignoriert, die setzt nur das Dashboard.", properties: { rule: { type: "string", description: "Regel, ein Satz, max. 200 Zeichen." }, conditions: { type: "string", description: "Wann die Regel gilt, max. 160 Zeichen." } }, required: ["rule", "conditions"] },
                    addedAt: { type: "string", description: "Datum, an dem der Punkt aufgenommen wurde, YYYY-MM-DD (optional); das Badge „neu“ zeigt es dann an. isNew nur bei Punkten der letzten Sonntags-Prüfung setzen." },
                  },
                  required: ["text", "evidence", "fit"],
                },
              },
            },
            required: ["title", "items"],
          },
        },
        open: { type: "array", items: { type: "string" }, description: "Offene Fragen / noch zu prüfen, max. 6 Einträge à 160 Zeichen (optional)." },
      },
      required: ["fetchedAt", "updatedAt", "sections"],
    },
  },
  {
    name: "save_coach",
    description:
      "Schreibt die Tagesaussage des Coaches fürs Dashboard (Karte) und Widget (Hinweiszeile). Bei jedem Update aufrufen, fetchedAt = jetzt. Das Widget zeigt sie nur 36 Stunden lang. Ersetzt die Tagesaussage komplett. Felder, die du weglässt, sind danach leer (Ausnahmen: idea, week, week_plan – dort bleibt das Gespeicherte stehen, wenn du sie weglässt). Fehlt text, obwohl heute schon ein Bericht gespeichert ist, wird der Aufruf abgelehnt. Für die Entscheidungsliste save_decisions verwenden.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        note: { type: "string", description: "Hinweis für heute, 1-2 Sätze, max. 160 Zeichen, Du-Form, mit Konsequenz (steht im Widget)." },
        headline: { type: "string", description: "Kurzurteil, max. 80 Zeichen (optional)." },
        go: { type: "string", enum: ["hart", "locker", "frei"], description: "Dein Urteil für heute, steht groß in Dashboard, Widget und Uhr: hart = Qualität/Intensität erlaubt (draufhauen ok), locker = nur locker, frei = Ruhetag oder kein Training. Bei jedem Morgen-Update setzen." },
        blockers: { type: "array", items: { type: "string" }, description: "Was heute gegen Intensität spricht, höchstens 3 Stichworte je max. 60 Zeichen (z. B. \"Hüfte zwickt\", \"HRV 20 % unter Basis\", \"Blocktest in 2 Tagen\"). Leer oder weglassen, wenn nichts dagegen spricht." },
        text: { type: "string", description: "Ganzer Bericht ohne Studien-Check, max. 8000 Zeichen (optional, fürs Dashboard)." },
        question: { type: "string", description: "Rückfrage an Gerbert, max. 300 Zeichen (optional). Steht im Dashboard mit Antwortfeld." },
        answer: { type: "string", description: "Seine Antwort auf question, max. 1000 Zeichen (optional, nur zusammen mit question, z. B. wenn er im Chat geantwortet hat)." },
        answeredAt: { type: "string", description: "Zeitpunkt der Antwort, ISO-8601 (optional, sonst jetzt)." },
        idea: {
          type: "object",
          description:
            "Idee der Woche, nur montags mitschicken: eine kreative, freiwillige Trainingsidee, steht als eigene kleine Karte im Dashboard (bis 8 Tage, ohne Abhaken). Weglassen = die gespeicherte Idee bleibt stehen. Leerer text = Idee entfernen. Vorher in get_coach unter ideas nachsehen, damit sie sich nicht wiederholt.",
          properties: {
            text: { type: "string", description: "Die Idee, 1-2 Sätze, max. 240 Zeichen, Du-Form." },
            angle: { type: "integer", minimum: 1, maximum: 8, description: "Blickwinkel 1-8 (optional)." },
            sport: { type: "string", enum: ["lauf", "rad", "schwimmen", "kraft", "alle"], description: "Sportart der Idee (optional, bestimmt das Icon)." },
          },
          required: ["text"],
        },
        week: {
          type: "object",
          description:
            "Deine Wochenampel zur letzten vollen Woche, nur montags mitschicken: steht in der Montagsbilanz des Dashboards (bis die nächste Woche abgeschlossen ist). gruen = Woche passt, gelb = im Auge behalten, rot = Warnsignal. Weglassen = die gespeicherte Ampel bleibt stehen. Leeres light = Ampel entfernen.",
          properties: {
            light: { type: "string", enum: ["gruen", "gelb", "rot"], description: "Ampelfarbe." },
            text: { type: "string", description: "Begründung in einem Satz, max. 160 Zeichen, Du-Form (optional)." },
          },
          required: ["light"],
        },
        week_plan: {
          type: "object",
          description:
            "Dein Wochenvorschlag, nur montags mitschicken (an anderen Tagen Fehler): steht als Karte \"Wochenvorschlag\" im Dashboard bis zum nächsten Montag, ohne Abhaken und ohne Abgleich mit den Einheiten. Weglassen = der gespeicherte Vorschlag bleibt stehen. Leeres Objekt {} = Karte ausblenden. deload: true markiert die Woche als Entlastung; nur so markierte Wochen lassen die Leitplanken im Trainingsblock aus.",
          properties: {
            week: { type: "string", description: "Montag der Woche, YYYY-MM-DD (optional, sonst der heutige Montag)." },
            basis: { type: "string", description: "Grundlage in einem kurzen Satz, max. 160 Zeichen, z. B. \"ca. 5 h verfügbar, Erholung grün → +8 %\"." },
            deload: { type: "boolean", description: "true = Entlastungswoche (nach Bedarf, kein fester Rhythmus)." },
            targets: {
              type: "array",
              description: "Wochenrahmen je Sportart (jede höchstens einmal).",
              items: {
                type: "object",
                properties: {
                  sport: { type: "string", enum: ["lauf", "rad", "schwimmen", "kraft"] },
                  time_min: { type: "number", description: "Zeit in Minuten." },
                  tss_max: { type: ["number", "null"], description: "TSS-Obergrenze (Untergrenze zeigt das Dashboard mit 80 %), null beim Schwimmen." },
                },
                required: ["sport", "time_min"],
              },
            },
            days: {
              type: "array",
              description: "Verteilung auf Tage (optional), steht klein als Vorschlag unter den Zielen.",
              items: {
                type: "object",
                properties: {
                  day: { type: "string", description: "Tag, z. B. \"Mo\", max. 12 Zeichen." },
                  text: { type: "string", description: "Einheit, z. B. \"Lauf locker 50 min\", max. 120 Zeichen." },
                },
                required: ["day", "text"],
              },
            },
          },
          required: ["targets"],
        },
      },
      required: ["fetchedAt", "note"],
    },
  },
  {
    name: "save_decisions",
    description:
      "Ersetzt nur die Entscheidungsliste (Karte \"Was du entscheiden musst\"). Die Tagesaussage (save_coach) bleibt unberührt. Immer die ganze Liste schicken; fehlende Punkte werden gelöscht. Antworten bleiben per id erhalten. Höchstens 5 Punkte, [] blendet die Karte aus. Fehlt id, erzeugt der Worker sie aus dem Titel und gibt sie in der Antwort zurück.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        decisions: {
          type: "array",
          description: "Alle offenen Punkte, höchstens 5 (nach Frist sortiert der Worker). Jeder Punkt muss mit Ja, Nein oder Option B beantwortbar sein. Erledigte Punkte weglassen.",
          items: {
            type: "object",
            properties: {
              id: { type: "string", description: "Stabile kebab-case-id (a-z, 0-9, Bindestrich, max. 60), über Updates gleich lassen. Fehlt sie, kommt sie aus dem Titel." },
              title: { type: "string", description: "Der Punkt als Entscheidungsfrage, max. 80 Zeichen." },
              recommendation: { type: "string", description: "Deine Empfehlung, max. 160 Zeichen." },
              reason: { type: "string", description: "Grund der Empfehlung, max. 200 Zeichen." },
              alternatives: { type: "array", items: { type: "string" }, description: "Alternativen (höchstens 3, je max. 80 Zeichen), z. B. \"Option B: Kraft erst ab Block C\"." },
              deadline: { type: "string", description: "Frist, YYYY-MM-DD." },
              ifNoAnswer: { type: "string", description: "Was passiert, wenn Gerbert nichts sagt, max. 160 Zeichen." },
              answer: { type: "string", description: "Nur setzen, wenn du Gerberts Antwort ausdrücklich ändern willst (max. 200 Zeichen, \"\" entfernt sie). Weglassen = die gespeicherte Antwort bleibt." },
              answeredAt: { type: "string", description: "Zeitpunkt der Antwort, ISO-8601 (optional, nur mit answer)." },
            },
            required: ["title", "recommendation", "reason", "deadline", "ifNoAnswer"],
          },
        },
      },
      required: ["fetchedAt", "decisions"],
    },
  },
  {
    name: "save_ernaehrung",
    description:
      "Schreibt die Ernährungs- und Gewichtsaussage des Coaches fürs Dashboard (Karte \"Ernährung & Gewicht\"); get_coach liefert sie unter ernaehrung.coach. fetchedAt = jetzt. Mindestens eines von headline, text, weekLine oder goal. Das Ziel (goal) bleibt gespeichert, wenn es fehlt; goal null entfernt es.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        headline: { type: "string", description: "Kurzaussage, max. 80 Zeichen (optional)." },
        text: { type: "string", description: "Tagesaussage, max. 2000 Zeichen (optional)." },
        weekLine: { type: "string", description: "Wochenzeile, max. 300 Zeichen (optional)." },
        goal: {
          type: ["object", "null"],
          description: "Gewichtsziel (optional). Weglassen = das gespeicherte Ziel bleibt, null = Ziel entfernen.",
          properties: {
            targetKg: { type: "number", description: "Zielgewicht in kg, 30 bis 300." },
            byDate: { type: "string", description: "Bis wann, YYYY-MM-DD." },
          },
          required: ["targetKg", "byDate"],
        },
      },
      required: ["fetchedAt"],
    },
  },
  {
    name: "save_test",
    description:
      "Schreibt EINEN Test fürs Dashboard (Ansicht \"Tests\"; Upsert über id, ein Test pro Aufruf). Mehrere Tests laufen gleichzeitig; dazu Kandidaten und abgeschlossene Tests mit „Gelernt“. kind: test = eine Änderung, paket = mehrere Änderungen (Urteil gilt fürs Paket), begleiter = läuft mit und prüft nur „stört nicht“ (blockGoal dann „Begleiter: …“). Gründe: studyRefs = ids von Wissensbasis-Punkten, insightRefs = ids von Erkenntnis-Karten (unbekannte ids werden gespeichert, aber als Warnung gemeldet). Das Urteil steht nur im Test; die Wissensbasis-Karte leitet „Du machst“ und „Bei dir“ daraus ab. Pflicht je status: laeuft braucht startDate, checkDate, decidedAt (Datum von Gerberts Ja) und expectation; urteil/gelernt brauchen result; gelernt braucht learned; result nur bei urteil/gelernt. Liegt actualPct der adherence unter minPct, ist result höchstens unklar. fetchedAt = jetzt; ein älterer Wert überschreibt nie einen neueren (gleicher Test). Antwort: { ok, id, fetchedAt, warnings[] } (mehr als zwei laufende Tests mit Mehrbelastung, gemeinsame Messgröße ohne gegenseitiges confoundedWith, unbekannte Verweise). Optional adoption { state offen|uebernommen|abgelehnt, rule, conditions, decidedAt } nur bei urteil/gelernt: Bei bestätigtem Test state offen mit Regel und Bedingungen setzen, Gerbert antwortet im Dashboard („Als Standard übernehmen?“); uebernommen nur bei result bestaetigt. Höchstens 40 Tests; status verworfen blendet aus und gibt Platz frei. get_coach liefert sie unter tests, die übernommenen unter proven.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        id: { type: "string", description: "Stabile id, kebab-case, max. 60 Zeichen (Upsert-Schlüssel)." },
        title: { type: "string", description: "Titel, max. 80 Zeichen." },
        kind: { type: "string", enum: TEST_KIND },
        discipline: { type: "string", enum: TEST_DISCIPLINE },
        block: { type: "string", description: "Block, in dem der Test läuft oder laufen soll, max. 20 Zeichen (z. B. „A“)." },
        blockGoal: { type: "string", description: "Worauf er einzahlt, max. 140 Zeichen; bei begleiter „Begleiter: …“." },
        question: { type: "string", description: "Frage in einem Satz, max. 160 Zeichen." },
        studyRefs: { type: "array", items: { type: "string" }, description: "ids von Wissensbasis-Punkten, max. 6 (optional)." },
        insightRefs: { type: "array", items: { type: "string" }, description: "ids von Erkenntnis-Karten, max. 6 (optional)." },
        change: { type: "string", description: "Was konkret anders ist, max. 160 Zeichen." },
        startDate: { type: ["string", "null"], description: "Start YYYY-MM-DD (Pflicht bei laeuft)." },
        checkDate: { type: ["string", "null"], description: "Prüfdatum YYYY-MM-DD aus der Messgröße (Pflicht bei laeuft)." },
        metrics: {
          type: "array",
          minItems: 1,
          maxItems: 4,
          description: "1–4 Messgrößen, key frei (max. 40). Bekannte keys lauf_hf130, rad_hf165w, schwimm_amstueck, schwimm_100m haben eine Standard-Richtung; der Coach liefert current selbst.",
          items: {
            type: "object",
            properties: {
              key: { type: "string", description: "Frei, max. 40 Zeichen." },
              label: { type: "string", description: "Anzeigename, max. 60 Zeichen." },
              unit: { type: "string", description: "Einheit, max. 20 Zeichen." },
              better: { type: "string", enum: ["lower", "higher"], description: "Pflicht bei eigenem key." },
              baseline: { type: ["number", "null"] },
              baselineDate: { type: ["string", "null"], description: "YYYY-MM-DD." },
              current: { type: ["number", "null"] },
              currentDate: { type: ["string", "null"], description: "YYYY-MM-DD." },
              provisional: { type: "boolean", description: "true = aktueller Wert nur vorläufig." },
            },
            required: ["key", "label"],
          },
        },
        expectation: {
          type: ["object", "null"],
          description: "Grenzen vorher festgelegt (Pflicht bei laeuft).",
          properties: {
            confirmed: { type: "string", description: "Wann bestätigt, max. 120 Zeichen." },
            unclear: { type: "string", description: "Wann unklar, max. 120 Zeichen." },
            refuted: { type: "string", description: "Wann widerlegt, max. 120 Zeichen." },
          },
          required: ["confirmed", "unclear", "refuted"],
        },
        load: { type: "string", enum: TEST_LOAD, description: "Mehrbelastung ja (mehr) oder nein (neutral)." },
        adherence: {
          type: ["object", "null"],
          description: "Einhaltung (optional); darunter ist das Ergebnis höchstens unklar.",
          properties: {
            rule: { type: "string", description: "Regel, max. 120 Zeichen." },
            minPct: { type: "number", minimum: 0, maximum: 100 },
            actualPct: { type: ["number", "null"], minimum: 0, maximum: 100 },
          },
          required: ["rule", "minPct"],
        },
        abort: { type: ["string", "null"], description: "Abbruchregel, max. 140 Zeichen (optional)." },
        status: { type: "string", enum: TEST_STATUS },
        result: { type: ["string", "null"], enum: [...TEST_RESULT, null], description: "Nur bei urteil/gelernt (dort Pflicht)." },
        confoundedWith: { type: "array", items: { type: "string" }, description: "ids parallel gelaufener Tests mit möglichem Einfluss (optional)." },
        learned: { type: ["string", "null"], description: "„Bei dir …“, max. 200 Zeichen (nur bei gelernt, dort Pflicht)." },
        adoption: {
          type: ["object", "null"],
          description: "Optional, nur bei urteil/gelernt: Übernahme als Standard. Bei bestätigtem Test state offen setzen; Gerbert antwortet im Dashboard (uebernommen/abgelehnt), das überschreibt der Coach nicht mehr mit offen.",
          properties: {
            state: { type: "string", enum: ADOPTION_STATE },
            rule: { type: "string", description: "Die Regel in einem Satz, max. 200 Zeichen." },
            conditions: { type: "string", description: "Unter welchen Bedingungen sie galt (Block, Umfang, Gewicht, Hüfte), max. 200 Zeichen." },
            decidedAt: { type: ["string", "null"], description: "Datum der Entscheidung YYYY-MM-DD (setzt sonst das Dashboard)." },
          },
          required: ["state", "rule", "conditions"],
        },
        proposedBy: { type: "string", enum: TEST_PROPOSED_BY },
        decidedAt: { type: ["string", "null"], description: "Datum von Gerberts Ja, YYYY-MM-DD (Pflicht bei laeuft)." },
      },
      required: ["fetchedAt", "id", "title", "kind", "discipline", "block", "blockGoal", "question", "change", "metrics", "load", "status", "proposedBy"],
    },
  },
  {
    name: "save_experiment",
    description:
      "Veraltet, bitte save_test nutzen (mehrere Tests gleichzeitig, Kandidaten, Gelerntes). Schreibt (überschreibt) das laufende Trainings-Experiment fürs Dashboard (Karte \"Experiment\"). Die Trainingstage je Woche rechnet der Worker selbst aus den Aktivitäten. fetchedAt = jetzt. Nur Messwerte übergeben, die es wirklich gibt; fehlt ein aktueller Wert, current und currentDate auf null setzen.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        title: { type: "string", description: "Name des Experiments, max. 80 Zeichen, z. B. \"Trainingstage\"." },
        hypothesis: { type: "string", description: "Hypothese in einem Satz, max. 200 Zeichen." },
        status: { type: "string", enum: ["vorgeschlagen", "läuft", "ausgewertet"] },
        startDate: { type: ["string", "null"], description: "Start YYYY-MM-DD, null solange es nicht läuft." },
        minWeeks: { type: "integer", minimum: 1, maximum: 52, description: "Mindestdauer in Wochen." },
        targetDaysPerWeek: { type: "integer", minimum: 1, maximum: 7, description: "Ziel: Trainingstage pro Woche (Ziellinie im Balkendiagramm)." },
        metrics: {
          type: "array",
          description:
            "Messgrößen, jede höchstens einmal: lauf_hf130 (Sekunden pro km bei Puls 130, lower), rad_hf165w (Ø-Puls bei 165 W, lower), schwimm_amstueck (Meter am Stück, higher), schwimm_100m (Sekunden pro 100 m, lower).",
          items: {
            type: "object",
            properties: {
              key: { type: "string", enum: ["lauf_hf130", "rad_hf165w", "schwimm_amstueck", "schwimm_100m"] },
              label: { type: "string", description: "Anzeigename, max. 60 Zeichen." },
              unit: { type: "string", description: "Einheit, max. 20 Zeichen (z. B. s/km, bpm, m, s/100 m)." },
              baseline: { type: ["number", "null"], description: "Ausgangswert." },
              baselineDate: { type: ["string", "null"], description: "Datum des Ausgangswerts, YYYY-MM-DD." },
              current: { type: ["number", "null"], description: "Aktueller Wert, null = noch keiner." },
              currentDate: { type: ["string", "null"], description: "Datum des aktuellen Werts, YYYY-MM-DD." },
              better: { type: "string", enum: ["lower", "higher"], description: "Welche Richtung besser ist." },
              provisional: { type: "boolean", description: "true = aktueller Wert ist noch nicht bewertbar (z. B. Quartal mit zu wenigen Läufen, direkt nach Krankheit). Das Dashboard zeigt ihn dann grau als „vorläufig“ statt die Veränderung grün/orange." },
            },
            required: ["key", "label", "unit", "better"],
          },
        },
        verdict: { type: ["string", "null"], description: "Dein Fazit, max. 200 Zeichen, z. B. \"stützt die Hypothese\" (null, solange offen)." },
      },
      required: ["fetchedAt", "title", "hypothesis", "status", "minWeeks", "targetDaysPerWeek", "metrics"],
    },
  },
  {
    name: "save_profil",
    description: "Schreibt (überschreibt) die wichtigsten Punkte aus dem Athletenprofil fürs Dashboard (Bereich \"Profil\"). fetchedAt = jetzt.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        updatedAt: { type: "string", description: "Stand des Profils, YYYY-MM-DD." },
        highlights: {
          type: "array",
          description: "Höchstens 5 Punkte.",
          items: {
            type: "object",
            properties: {
              text: { type: "string", description: "Der Punkt, max. 140 Zeichen." },
              strength: { type: "string", enum: ["mittel", "schwach"], description: "Wie gut der Punkt belegt ist." },
            },
            required: ["text", "strength"],
          },
        },
      },
      required: ["fetchedAt", "updatedAt", "highlights"],
    },
  },
  {
    name: "save_insight",
    description:
      "Legt eine Erkenntnis an oder aktualisiert sie (Upsert über id): eine Auffälligkeit, die du selbst in Gerberts Daten gefunden hast. Jede Karte hat vier Teile: Beobachtung (text) → Hypothese (hypothesis) → Umstellung (change) → Erwartung (expectation); ohne mögliche Umstellung keine Karte. Steht als Karte im Dashboard-Bereich \"Erkenntnisse\". Dieselbe id für dieselbe Erkenntnis verwenden, damit Status und Stärke ihren Verlauf behalten; vorher in get_coach unter insights nachsehen. fetchedAt = jetzt. Eine archivierte Erkenntnis wird durch Speichern wieder aktiv.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Stabile id in kebab-case, max. 60 Zeichen, z. B. \"trainingstage-vs-umfang\"." },
        title: { type: "string", description: "Kurzer Titel in Alltagssprache, max. 60 Zeichen (z. B. \"Mehr Laufminuten statt mehr Rad\")." },
        text: { type: "string", description: "BEOBACHTUNG: was in deinen Daten tatsächlich zu sehen war, mit Zahl und Stärke, Du-Form, max. 280 Zeichen. Nur Beobachtetes, keine Deutung (z. B. \"Wochen mit mehr Laufstunden waren danach bei gleichem Puls schneller, ca. 8 s/km je Stunde (schwach)\")." },
        hypothesis: { type: ["string", "null"], description: "HYPOTHESE: was daraus vermutet wird, ein Satz, max. 140 Zeichen (z. B. \"Dein Plateau kommt von zu wenig Laufminuten\")." },
        change: { type: ["string", "null"], description: "UMSTELLUNG: was Gerbert dafür konkret anders macht (Einheit, Menge, ab wann), max. 140 Zeichen. Nur was er entschieden hat oder als Vorschlag mit „Vorschlag:“ markiert." },
        expectation: { type: ["string", "null"], description: "ERWARTUNG: woran man bis wann sieht, ob es stimmt, mit Messgröße und Widerlegt-Schwelle, max. 160 Zeichen (z. B. \"Blocktest Mitte Jan ≤ 7:14/km im Band; ≥ 7:24 = widerlegt\")." },
        action: { type: ["string", "null"], description: "Veraltet: nur noch für Karten, die keine Umstellung haben (z. B. reine Messkorrektur), max. 160 Zeichen." },
        strength: { type: "string", enum: ["beobachtung", "schwach", "mittel", "stark"], description: "Wie gut die Erkenntnis belegt ist." },
        status: { type: "string", enum: ["neu", "bestätigt", "abgeschwächt", "widerlegt"] },
        discipline: { type: "string", enum: ["lauf", "rad", "schwimmen", "kraft", "erholung", "alle"] },
        evidence: { type: "string", description: "Datengrundlage, max. 200 Zeichen, z. B. \"107 Läufe, Jan 2025–Okt 2026\"." },
        chart: {
          type: ["object", "null"],
          description: "Kleine Grafik zur Erkenntnis (optional).",
          properties: {
            type: { type: "string", enum: ["line", "bar"] },
            unit: { type: "string", description: "Einheit, max. 20 Zeichen." },
            points: { type: "array", description: "1–30 Punkte, x = Beschriftung (max. 20 Zeichen, z. B. \"KW 42\" oder \"2026-10\").", items: { type: "object", properties: { x: { type: "string" }, y: { type: "number" } }, required: ["x", "y"] } },
            refLine: { type: ["number", "null"], description: "Referenzlinie (optional)." },
            better: { type: "string", enum: ["higher", "lower"] },
          },
          required: ["type", "unit", "points", "better"],
        },
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
      },
      required: ["id", "title", "text", "strength", "status", "discipline", "evidence", "fetchedAt"],
    },
  },
  {
    name: "archive_insight",
    description: "Blendet eine Erkenntnis aus (wird nicht gelöscht, steht im Dashboard unter \"Ältere Erkenntnisse\").",
    inputSchema: { type: "object", properties: { id: { type: "string", description: "id der Erkenntnis." } }, required: ["id"] },
  },
  {
    name: "save_targets",
    description:
      "Setzt Gerberts aktuellen Rahmen fürs Dashboard (Karten \"Woche\" und \"Aktueller Rahmen\") und get_coach: Ausdauerstunden je ISO-Woche (Laufen, Rad, Schwimmen; Kraft zählt nicht), Kraftsätze in 2 Wochen (laufende Woche plus Vorwoche), erlaubte Steigerung gegenüber der Referenzwoche, Intensität je Sportart, Leitplanken und was nicht erlaubt ist. Nur nach Gerberts Zustimmung ändern. Nur schicken, was sich ändert: Fehlende Felder werden vom bisher geltenden Eintrag übernommen; intensity ersetzt nur die geschickten Sportarten (text \"\" streicht eine), guardrails und notAllowed ersetzen die ganze Liste. Jeder Aufruf hängt einen Eintrag an die Historie an, nichts wird überschrieben; es gilt immer der zuletzt gespeicherte Eintrag mit validFrom <= heute, vergangene Wochen behalten ihr damaliges Ziel. fetchedAt = jetzt.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "ISO-Zeitstempel (jetzt)." },
        enduranceHoursPerWeek: { type: "number", description: "Ausdauerstunden je Woche, 1 bis 20 in Schritten von 0,25 (z. B. 5.5)." },
        strengthSets2w: { type: "integer", description: "Kraftsätze in 2 Wochen (rollierend), 0 bis 300." },
        reason: { type: "string", description: "Warum geändert, max. 200 Zeichen (steht im Dashboard)." },
        validFrom: { type: "string", description: "Gilt ab YYYY-MM-DD. Standard: Montag der laufenden Woche." },
        streakThresholdPct: { type: "number", description: "Für \"Wochen in Folge\": zählt eine Woche ab diesem Anteil ihres Ziels, 50 bis 100. Standard 90." },
        maxIncreasePct: { type: "number", description: "Erlaubte Steigerung gegenüber der Referenzwoche in Prozent, 0 bis 50. Standard 10." },
        intensity: {
          type: "array",
          maxItems: 6,
          description: "Intensität je Sportart, max. 6 Einträge. Ersetzt nur die geschickten Sportarten; text \"\" streicht eine. Beispiel: [{ sport: \"rad\", text: \"1× HIIT pro Woche, z. B. 2× 8× 30/30\" }].",
          items: {
            type: "object",
            properties: {
              sport: { type: "string", enum: ["lauf", "rad", "schwimmen", "kraft"] },
              text: { type: "string", description: "Max. 160 Zeichen." },
              short: { type: "string", description: "Optional: Kurzfassung für das iPhone-Widget, max. 40 Zeichen (z. B. \"1× HIIT ab Woche 2\"). Ohne short zeigt das Widget den Anfang von text." },
            },
            required: ["sport", "text"],
          },
        },
        guardrails: { type: "array", maxItems: 8, items: { type: "string" }, description: "Leitplanken, max. 8, je max. 160 Zeichen (z. B. \"Kein HIIT/Sprints 2 Tage vor Blocktest\"). Ersetzt die ganze Liste." },
        notAllowed: { type: "array", maxItems: 8, items: { type: "string" }, description: "Nicht erlaubt, max. 8, je max. 80 Zeichen (z. B. \"Sweet Spot\"). Ersetzt die ganze Liste." },
      },
      required: ["fetchedAt", "reason"],
    },
  },
  {
    name: "get_run_export",
    description:
      "Export für die Langzeitanalyse: eine Zeile pro Lauf (alle Läufe, älteste zuerst), nur Rohwerte und Kennzahlen, keine Bewertung. Liest aus der Export-Datenbank (D1), die eine GitHub Action täglich gegen 04:17/05:17 Berlin für die letzten 14 Tage nachführt; ganz neue Läufe von heute fehlen also noch. Antwort: methodVersion (aktuelle Methode), from, to, total (Läufe im Zeitraum), count, pending (Läufe im Zeitraum, deren Zeitreihen beim letzten Lauf der Action nicht abrufbar waren), syncedAt / syncedFrom / syncedTo (letzte Synchronisation), nextCursor (null = letzte Seite), columns, rows. " +
      "Spalten: id, date, startLocal (lokale Startzeit), type, name, tags, treadmill, race, interval (Intervall-Tag oder Textsignal wie 5x1000), runClass (base|long|intensity|race|unknown, Einheitsart des Dashboards), distKm, movingMin (Bewegungszeit), elevPerKm (Hm/km), avgHr, maxHr, rpe (1–10), feel (1 stark … 5 schwach), load (Intervals-Load). " +
      "Koppellauf: brick (Start höchstens 30 min nach dem Ende einer Radfahrt), brickRideId (id in get_ride_export), brickGapMin (Minuten zwischen Radende und Laufstart), brickRideMin, brickRideLoad; Koppelläufe haben ganz normale Kennzahlen (Pace im Band frisch gegen nach dem Rad). " +
      "Wetter: tempC, humidity (%), dewPointC (Taupunkt in °C aus tempC und humidity nach Magnus; fasst Temperatur und Feuchte in einer Zahl zusammen), windMs (m/s, nur aus Open-Meteo), weatherSrc (intervals|openmeteo|null). Nie die Temperatur der Uhr; Laufband ohne Wetter. " +
      "streamStatus: ok | pending | none (keine Zeitreihen/kein Puls) | skip; streamSkip: treadmill | race | interval | short (unter 20 min) | noHr | gps (GPS-Bereinigung hätte mehr als 10 % der Distanz entfernt; dann nur gpsCorrectedM). race = Tag/Intervals-Flag oder ein Renn-Eintrag (RACE, RACE_A/B/C, A_RACE) im Kalender am selben Tag, auch bei abgebrochenem Rennen. Kennzahlen nur bei ok, sonst null: " +
      "Zeitachse Bewegungszeit, GPS bereinigt, erste 5 min ausgelassen, gleitende 30-s-Fenster (5-s-Schritt) ohne Pause/Stehen. " +
      "paceBand125_135 / paceBand120_128 = Median-Pace in s/km der Fenster mit Ø-HF im Band (null unter 300 s im Band), secBand125_135 / secBand120_128 = Sekunden im Band, paceBand…Flat = dasselbe nur aus flachen Fenstern (Steigung < 2 %, null ohne Höhendaten). " +
      "effAt130 = vorhergesagte Geschwindigkeit in m/min bei HF 130 aus der Regression Geschwindigkeit gegen HF (Fenster mit HF 110–150), effSlope = m/min je Schlag, effR2, effN = Zahl der Fenster (unter 20: Regression null). " +
      "decoup = Pa:HR-Decoupling in % (Minute 5 bis Ende, erste gegen zweite Hälfte, positiv = zweite Hälfte schlechter). hrDriftBpm = Ø-HF letztes minus erstes Drittel, nur wenn die Pace beider Drittel < 3 % abweicht. gradeShareFlat = Anteil der Fenster mit Steigung < 2 % (0–1). gpsCorrectedM = durch die GPS-Bereinigung entfernte Meter. methodVersion = Methode, mit der die Zeile gerechnet wurde.",
    inputSchema: EXPORT_ARGS,
  },
  {
    name: "get_ride_export",
    description:
      "Export für die Langzeitanalyse: eine Zeile pro Radfahrt (Ride, VirtualRide, alle Rad-Typen), älteste zuerst, nur Rohwerte und Kennzahlen, keine Bewertung. Antwort und Parameter wie get_run_export (methodVersion, total, count, pending, syncedAt, nextCursor, columns, rows). " +
      "Spalten: id, date, startLocal, endLocal (Start + Gesamtzeit, für Koppelläufe), type, name, tags, trainer (Rolle/VirtualRide), movingMin, distKm, elevPerKm, avgPower (Ø W), np (Normalized Power), if (Intensitätsfaktor, z. B. 0,65), vi (NP/Ø), ftp (FTP in Intervals zum Zeitpunkt der Fahrt), avgHr, maxHr, avgCadence, load (Rad: power_load, wie im Wochenrahmen), rpe, feel, warmupS (icu_warmup_time). " +
      "intensity: bikeHiit (HIIT, 30/30, VO2, 3–4× 4 min oder ≥ 4 min Z5) oder null. testKind: compare (Rad-Vergleichseinheit wie im Dashboard: Einrollen + 60-min-Messfenster bei der Vergleichsleistung 165 ±5 W, VI ≤ 1,05), baseline (Ausgangswert, z. B. ohne Einrollen) oder null. " +
      "Wetter wie bei Läufen, nur draußen (tempC, humidity, dewPointC, windMs, weatherSrc). streamStatus ok | pending | none | skip; streamSkip: noPower | short (unter 20 min) | interval (Intensitätseinheit, Intervall-Tag/-Text) | noHr. " +
      "Kennzahlen nach dem Einrollen (icu_warmup_time, sonst 10 min; bei compare/baseline das Fenster des Dashboards), Zeitachse ohne Stillstand, nur gleichmäßige 30-s-Fenster (5-s-Schritt, Variationskoeffizient der Leistung in den 2 min davor < 10 %, weil der Puls nachläuft): " +
      "hrAt160_170 = Median-Puls der Fenster mit Ø 160–170 W (null unter 300 s), secAt160_170 = Sekunden; Hauptgröße für den Vergleich mit der Rad-Vergleichseinheit. powerAt120 / powerAt130 = vorhergesagte Watt bei HF 120/130 aus der Regression Leistung gegen Puls (Fenster mit HF 100–150), pwSlope (W je Schlag), pwR2, pwN (unter 20: Regression null). " +
      "ef = NP / Ø-Puls nach dem Einrollen. decoup = Pw:HR-Decoupling nach dem Einrollen über die ganze Fahrt (positiv = zweite Hälfte schlechter). decoup60 = Decoupling der ersten 60 min nach dem Einrollen; bei compare/baseline genau der Wert des Dashboards (Intervals-Wert, wenn er das Messfenster deckt). mainHr = Ø-Puls im Messfenster, nur bei compare/baseline. methodVersion.",
    inputSchema: EXPORT_ARGS,
  },
  {
    name: "get_swim_export",
    description:
      "Export für die Langzeitanalyse: eine Zeile pro Schwimmeinheit, älteste zuerst, nur Rohwerte, keine Zeitreihen-Kennzahlen. Antwort und Parameter wie get_run_export (pending = Einheiten, deren Laps noch nicht abrufbar waren). " +
      "Spalten: id, date, startLocal, type, name, pool (pool = Becken, open = Freiwasser, null = unbekannt), poolLengthM, distanceM, movingMin (Bewegungszeit ohne Pausen am Beckenrand, aus den Laps), pace100 (s/100 m auf die Bewegungszeit), avgHr, load, rpe, feel. " +
      "Schwimmtest 2× 500 m (aus dem Text „2x500: 9:40 / 9:52, Pause 45 s“, sonst aus zwei aufeinanderfolgenden Intervallen mit 500 m ±10 %): test500a, test500b (s), testPauseS, testPassed (Pause ≤ 60 s und die zweiten 500 m höchstens 5 s/100 m langsamer, wie im Blockziel). lapsStatus ok | none | pending. methodVersion.",
    inputSchema: EXPORT_ARGS,
  },
  {
    name: "get_day_export",
    description:
      "Export für die Langzeitanalyse: eine Zeile pro Kalendertag (auch Tage ohne Daten), nur Rohwerte, keine Basislinien und Bewertungen. Liest aus der Export-Datenbank (D1), täglich von einer GitHub Action nachgeführt. Antwort: methodVersion, from, to, total, count, pending (immer 0), syncedAt / syncedFrom / syncedTo, nextCursor (nächster Tag, null = letzte Seite), columns, rows. " +
      "Spalten: date, ctl, atl, rampRate (Intervals), loadRun, loadBike, loadSwim, loadStrength (Summe Load je Sportart, Rad mit power_load wie im Wochenrahmen), minRun, minBike, minSwim (Bewegungszeit in min, Schwimmen ohne Pausen soweit bekannt), strengthSets (Kraftsätze aus der Beschreibung), hrv, restHr, sleepH, sleepScore, sleepQuality (1 gut … 4 schlecht), fatigue, soreness, mood, motivation (Intervals-Skalen 1–4, vor dem Training eingetragen), sick (Kalender SICK/INJURED, Notiz „krank“ oder Name mit OP/Operation/Verletzung), calendar (alle Nicht-Workout-Kalendereinträge des Tages als „Name (KATEGORIE)“, mehrtägige an jedem Tag, mit „; “ getrennt, sonst null – daran Phasen wie OP, Krankheit, Urlaub erkennen), weightKg (Yazio, sonst Intervals), kcal, kcalGoal, protein, carbs (g, aus Yazio über die Intervals-Wellness; kcalGoal = Yazios Tagesziel inklusive des Sports, den Yazio von Garmin importiert), kcalActivities (Summe calories aller Aktivitäten des Tages, also die Sport-Schätzung von Uhr/Intervals, die Yazio aufschlägt; null ohne Wert), kjBike (Summe mechanische Arbeit der Radfahrten in kJ aus der Leistung, icu_joules; 1 kJ Arbeit ≈ 1 kcal Nahrungsenergie bei rund 24 % Wirkungsgrad; null ohne Leistung), methodVersion. Fehlende Werte sind null, Load/Minuten/Sätze ohne Training 0.",
    inputSchema: EXPORT_ARGS,
  },
  {
    name: "get_export_stats",
    description:
      "Zusammenfassung des Exports statt hunderter Zeilen: rechnet je Gruppe (Woche = ISO-Montag, Monat, Quartal, Jahr oder none = alles) für bis zu " + STATS_MAX_METRICS + " Zahlenspalten n (Zeilen mit Wert), mean, median, min, max und sd (Stichproben-Standardabweichung). Liest aus derselben D1-Datenbank wie get_run_export usw., nur Rechnung, keine Bewertung. " +
      "Für Langzeit-Trends (z. B. Pace im Pulsband je Quartal) zuerst dieses Tool nehmen; die Einzelzeilen nur holen, wenn Ausreißer, Wetter oder Kalender geprüft werden müssen – ein Mittel verdeckt Störfaktoren. " +
      "Antwort: methodVersion, table, groupBy, from, to, filter, min, max, metrics, rows (gelesene Zeilen), syncedAt, groups [{ key, from, to (erster/letzter Tag mit Zeile), rows, stats { Spalte: { n, mean, median, min, max, sd } } }].",
    inputSchema: {
      type: "object",
      properties: {
        table: { type: "string", enum: ["runs", "rides", "swims", "days"], description: "runs = get_run_export, rides = get_ride_export, swims = get_swim_export, days = get_day_export." },
        metrics: { type: "array", items: { type: "string" }, minItems: 1, maxItems: STATS_MAX_METRICS, description: "Zahlenspalten der Tabelle (Namen wie im jeweiligen Export), z. B. [\"paceBand120_128\", \"decoup\", \"dewPointC\"]." },
        groupBy: { type: "string", enum: STATS_GROUPS, description: "Gruppierung nach Datum, Standard month." },
        from: { type: "string", description: "Startdatum YYYY-MM-DD, Standard: erster Tag in der Tabelle." },
        to: { type: "string", description: "Enddatum YYYY-MM-DD, Standard: gestern." },
        filter: { type: "object", description: "Gleichheit auf Einordnungs-Spalten, Wert oder Liste (null = leer), z. B. { \"runClass\": [\"base\", \"long\"], \"treadmill\": false, \"brick\": false } oder { \"testKind\": \"compare\" }. Filterbar: Text- und Ja/Nein-Spalten außer id, name, tags, type, calendar, Datumswerten." },
        min: { type: "object", description: "Untergrenzen auf Zahlenspalten (>=), z. B. { \"movingMin\": 60 }." },
        max: { type: "object", description: "Obergrenzen auf Zahlenspalten (<=), z. B. { \"elevPerKm\": 10, \"dewPointC\": 12 }." },
      },
      required: ["table", "metrics"],
    },
  },
  {
    name: "get_coach",
    description:
      "Liest die letzte gespeicherte Coach-Aussage (note, headline, text) mit Rückfrage (question) und Gerberts Antwort (answer, answeredAt; null = nicht beantwortet), dazu die aktuelle Idee der Woche (idea: text, angle, sport, createdAt; null = keine), die Wochenampel (week: light, text, createdAt; null = keine), den Wochenvorschlag (week_plan: week, basis, deload, targets, days, createdAt; null = keiner), die markierten Entlastungswochen (deloadWeeks: Montage, neueste zuerst), die offenen Entscheidungen (decisions, eigener Speicher save_decisions: id, title, recommendation, reason, alternatives, deadline, ifNoAnswer, answer/answeredAt = Gerberts Antwort aus dem Dashboard, sonst fehlend; nach Frist sortiert; Morgen-Update fragt sie einzeln ab, kürzeste Frist zuerst), die bisherigen Ideen (ideas: date, text, angle, sport; max. 12, neueste zuerst), das laufende Experiment (experiment, wie bei save_experiment; null = keins), die Profil-Punkte (profil: updatedAt, highlights; null = keine), die aktiven Erkenntnisse (insights: id, title, discipline, strength, status, updatedAt, kurzer text = Beobachtung, hypothesis, change = Umstellung, expectation = Erwartung; neueste zuerst), die Wochensummen der laufenden ISO-Woche (week_totals: week, enduranceHours = Laufen+Rad+Schwimmen ohne Kraft, referenceHours = Referenzwoche (Mittel der Ausdauerzeit der letzten 3 Wochen ohne leichte Wochen unter 70 %; null ohne Vorwochen), byDiscipline { lauf, rad, schwimmen } in Stunden (Schwimmen nur Bewegungszeit ohne Pausen am Beckenrand), swimMovingHours = gezählte Schwimmzeit, swimElapsedHours = Gesamtzeit inkl. Pausen (nur Info), swimRestShare = Pausenanteil in % (null ohne Schwimmen), swimTimeEstimated: true nur, wenn eine Einheit ohne Bewegungszeit und Laps mit der Gesamtzeit zählt, strengthSets, strengthSets2w = laufende Woche plus Vorwoche, streakWeeksAbove4_5 = Wochen in Folge ab streakThresholdPct % des jeweiligen Wochenziels, goals; null = Intervals nicht erreichbar) und die Ernährung (ernaehrung: coach = Aussage aus save_ernaehrung mit headline, text, weekLine, goal {targetKg, byDate}; weight = Gewichtseinträge {date, kg} der letzten 90 Tage, neueste zuletzt; weightTrend7d = 7-Tage-Mittel pro Tag; weightSource = yazio oder intervals; null = nicht abrufbar) die letzten 8 Rad-Trendpunkte (ride_trend: points mit date, ablauf mit_einrollen|ohne_einrollen, durationMin, windowStartS, windowMin, avgWatts, avgHr, vi, ef = Watt pro Puls, decoupling, testReason; älteste zuerst; nur innerhalb desselben ablauf vergleichen; Trend, kein Test – zählt nicht fürs Blockziel; updatedAt = letzte Rechnung im Dashboard; null = noch keine) die Wissensbasis (wissensbasis: fetchedAt, updatedAt, folderCheckedAt, sections [{ title, items [{ text, evidence stark|mittel|schwach|praxis, fit hoch|mittel|gering|unklar, source, isNew, addedAt, forYou, practice, next u. a. }] }], open; Stand aus save_wissensbasis ohne die von Gerbert archivierten Punkte; null = keiner; wissensbasisArchiv: von Gerbert im Dashboard archivierte Punkte { id, title, source, archivedAt, reason }, neueste zuerst – diese nicht neu vorschlagen) und die von Gerbert entschiedenen Wissensbasis-Punkte (wissen: id, title, topic = Rubrik, state uebernommen|abgelehnt, rule, conditions, decidedAt, ggf. ruleChanged; übernommen = Regel, vor einem Rat zum Thema prüfen; abgelehnt = nicht empfehlen; offene Punkte stehen nur in wissensbasis mit adoption) und das bei Gerbert Geprüfte (proven: Tests mit adoption.state uebernommen – id, title, rule, conditions, decidedAt, verdict, metric; neueste zuerst; VOR der Literatur lesen, Gerbert ist die einzige betreute Person) und die Tests (tests: Liste aus save_test ohne verworfene, sortiert laeuft, kandidat, urteil, gelernt, innerhalb nach checkDate; Felder wie in save_test, dazu updatedAt) und den aktuellen Rahmen (targets: current mit enduranceHoursPerWeek, strengthSets2w, streakThresholdPct, maxIncreasePct, intensity [{ sport, text }], guardrails, notAllowed, validFrom, reason, savedAt; upcoming = geplante mit validFrom in der Zukunft; history = letzte 10 Einträge aus save_targets mit reason und changed, neueste zuerst; ohne Eintrag der Startwert). Vor jedem Morgen-Update aufrufen.",
    inputSchema: { type: "object", properties: {} },
  },
];

function resolveRange(args, { pastDefaultDays, futureDefaultDays }) {
  const oldest = isIsoDate(args?.oldest) ? args.oldest : isoDate(new Date(Date.now() - pastDefaultDays * 86400000));
  const newest = isIsoDate(args?.newest) ? args.newest : isoDate(new Date(Date.now() + futureDefaultDays * 86400000));
  return { oldest, newest };
}

async function saveSnapshot(env, args, validate, kvKey, merge) {
  if (!hasKv(env)) throw new Error("KV nicht verfügbar");
  const { value, error } = validate(args);
  if (error) throw new Error(error);
  await writeKvJson(env, kvKey, merge ? merge(await readKvJson(env, kvKey), value) : value);
  return { ok: true, fetchedAt: value.fetchedAt };
}

async function callTool(env, name, args) {
  if (name === "save_runalyze_snapshot") return saveSnapshot(env, args, validateSnapshot, RUNALYZE_KV_KEY);
  if (name === "save_studie") return saveSnapshot(env, args, validateStudie, STUDIE_KV_KEY);
  if (name === "save_wissensbasis") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    const { value, error } = await saveWissensbasis(env, args);
    if (error) throw new Error(error);
    const warnings = [...wissensbasisWarnings(value, testsForCoach(await readTests(env))), ...archivWarnings(value, await readArchiv(env))];
    return { ok: true, fetchedAt: value.fetchedAt, ...(warnings.length ? { warnings } : {}) };
  }
  if (name === "save_coach") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    return saveCoach(env, args);
  }
  if (name === "save_decisions") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    return saveDecisions(env, args);
  }
  if (name === "save_ernaehrung") return saveSnapshot(env, args, validateErnaehrung, ERNAEHRUNG_KV_KEY, mergeErnaehrung);
  if (name === "save_test") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    const { value, fetchedAt, error } = validateTest(args);
    if (error) throw new Error(error);
    const stored = await readTests(env);
    const guard = resetGuard(stored, value);
    if (guard) return { ok: true, id: value.id, ignored: true, warnings: [guard] };
    const up = upsertTest(stored, value, fetchedAt);
    if (up.error) throw new Error(up.error);
    await writeKvJson(env, TESTS_KV_KEY, up.value);
    const [wb, ins] = await Promise.all([readWissensbasis(env), readInsights(env)]);
    const studyIds = new Set((wb?.sections ?? []).flatMap((x) => (x.items ?? []).map((i) => i.id).filter(Boolean)));
    const insightIds = new Set((ins?.items ?? []).map((i) => i.id));
    const warnings = [...testWarnings(up.value.tests), ...refWarnings(value, studyIds, insightIds)];
    return { ok: true, id: value.id, fetchedAt, warnings };
  }
  if (name === "save_experiment") return saveSnapshot(env, args, validateExperiment, EXPERIMENT_KV_KEY);
  if (name === "save_profil") return saveSnapshot(env, args, validateProfil, PROFIL_KV_KEY);
  if (name === "save_targets") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    const today = isoDateBerlin();
    const prev = await readTargets(env);
    const { value, error } = validateTargets(args, today, prev);
    if (error) throw new Error(error);
    const stored = appendTarget(prev, value);
    await writeKvJson(env, TARGETS_KV_KEY, stored);
    return { ok: true, saved: value, ...targetsView(stored, today, COACH_HISTORY) };
  }
  if (name === "save_insight") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    const { value, error } = validateInsight(args);
    if (error) throw new Error(error);
    const stored = upsertInsight(await readInsights(env), value);
    await writeKvJson(env, INSIGHTS_KV_KEY, stored);
    const saved = stored.items[0];
    return { ok: true, id: saved.id, createdAt: saved.createdAt, updatedAt: saved.updatedAt, history: saved.history };
  }
  if (name === "archive_insight") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    const id = validateArchive(args);
    if (id.error) throw new Error(id.error);
    const r = archiveInsight(await readInsights(env), id.value);
    if (r.error) throw new Error(r.error);
    if (!r.already) await writeKvJson(env, INSIGHTS_KV_KEY, r.value);
    return { ok: true, id: id.value, archived: true };
  }
  if (name === "get_coach") {
    if (!hasKv(env)) throw new Error("KV nicht verfügbar");
    // week_totals: Ausdauerstunden und Kraftsätze der laufenden Woche, gleiche Rechnung wie das Dashboard (null, wenn Intervals ausfällt)
    const today = isoDateBerlin();
    const stored = await readTargets(env);
    const weekTotals = (async () => currentWeekTotals(computeWeekTotals(await withSwimIntervals(env, await fetchIntervalsActivities(env, weekTotalsOldest(today), today)), today, stored)))().catch(() => null);
    // ernaehrung: Coach-Aussage und Gewicht (Yazio, sonst Intervals-Wellness); Fehler machen daraus null, nie einen Fehler in get_coach
    const ernaehrung = buildErnaehrung(env, today, () => fetchIntervalsWellnessRange(env, isoDate(new Date(Date.parse(today) - (WEIGHT_DAYS - 1) * 86400000)), today)).catch(() => null);
    const coachRaw = await readCoach(env);
    const [decisions, experiment, profil, insights, week_totals, ernaehrungView, rideTrend, wissensbasis, testsStored, wbArchiv] = await Promise.all([readDecisions(env, coachRaw), readExperiment(env), readProfil(env), readInsights(env), weekTotals, ernaehrung, readRideTrend(env), readWissensbasis(env), readTests(env), readArchiv(env)]);
    const { decisions: _legacy, ...coach } = coachRaw ?? {};
    const hasCoach = Boolean(coachRaw);
    // targets: heute geltendes Wochenziel, geplante Änderungen und die letzten Einträge (save_targets), neueste zuerst
    const { current, upcoming, history } = targetsView(stored, today, COACH_HISTORY);
    // idea/ideas auch bei aelteren Eintraegen ohne diese Felder, damit der Coach immer dieselbe Form liest
    const extra = { decisions, wissensbasis: visibleWissensbasis(wissensbasis, wbArchiv) ?? null, wissen: wissenForCoach(wissensbasis), wissensbasisArchiv: archivForCoach(wbArchiv), tests: testsForCoach(testsStored), proven: provenTests(testsStored), experiment: experiment ?? null, profil: profil ?? null, insights: insightsForCoach(insights), week_totals, ernaehrung: ernaehrungView, targets: { current, upcoming, history },
      // ride_trend: letzte Rad-Trendpunkte wie in der Rad-Vergleichskarte (Dashboard legt sie ab), aelteste zuerst; null = noch keine
      ride_trend: rideTrend ? { points: rideTrend.points, updatedAt: rideTrend.updatedAt ?? null, note: RIDE_TREND_NOTE } : null };
    return hasCoach
      ? { ...coach, idea: coach.idea ?? null, ideas: coach.ideas ?? [], week: coach.week ?? null, week_plan: coach.week_plan ?? null, deloadWeeks: coach.deloadWeeks ?? [], ...extra }
      : { none: true, idea: null, ideas: [], week: null, week_plan: null, deloadWeeks: [], ...extra };
  }
  if (name === "get_export_stats") return JSON.stringify(await buildExportStats(env, args ?? {}));
  const EXPORT_TOOLS = { get_run_export: ["runs", RUN_COLUMNS], get_ride_export: ["rides", RIDE_COLUMNS], get_swim_export: ["swims", SWIM_COLUMNS], get_day_export: ["days", DAY_COLUMNS] };
  if (EXPORT_TOOLS[name]) {
    const [table, columns] = EXPORT_TOOLS[name];
    const result = table === "days" ? await buildDayExport(env, args) : await buildActivityExport(env, table, args);
    if (args?.format === "json") return JSON.stringify(result);
    if (args?.format === "csv") return `methodVersion=${result.methodVersion} pending=${result.pending} nextCursor=${result.nextCursor ?? ""}\n${toCsv(result.rows, columns)}`;
    // kompakt: ohne Einrueckung, Zeilen als Arrays
    return JSON.stringify(toTable(result, columns));
  }
  if (name === "get_planned_workouts") {
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 0, futureDefaultDays: 14 });
    return fetchIntervalsEvents(env, oldest, newest);
  }
  if (name === "get_recent_activities") {
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 14, futureDefaultDays: 0 });
    return fetchIntervalsActivities(env, oldest, newest);
  }
  if (name === "get_wellness") {
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 14, futureDefaultDays: 0 });
    return fetchIntervalsWellnessRange(env, oldest, newest);
  }
  if (name === "get_nutrition") {
    if (!hasYazioCredentials(env)) throw new Error("Yazio ist nicht konfiguriert (YAZIO_USERNAME/YAZIO_PASSWORD fehlen).");
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 14, futureDefaultDays: 0 });
    const days = listIsoDaysInclusive(oldest, newest);
    if (days.length > MAX_NUTRITION_DAYS) throw new Error(`Zeitraum zu groß: max. ${MAX_NUTRITION_DAYS} Tage.`);

    const results = [];
    for (const day of days) {
      const [nutrition, goalKcal] = await Promise.all([fetchYazioDailyNutrition(env, day), fetchYazioDailyGoalKcal(env, day)]);
      results.push({
        date: day,
        energyKcal: Math.round(nutrition.energyKcal),
        proteinG: Math.round(nutrition.proteinG),
        fatG: Math.round(nutrition.fatG),
        carbG: Math.round(nutrition.carbG),
        goalKcal,
        items: nutrition.items.map((i) => ({
          name: i.name,
          daytime: i.daytime,
          amountG: i.amountG ?? null,
          energyKcal: Math.round(i.energyKcal),
          proteinG: Math.round(i.proteinG * 10) / 10,
          fatG: Math.round(i.fatG * 10) / 10,
          carbG: Math.round(i.carbG * 10) / 10,
        })),
      });
    }
    return results;
  }
  throw new Error(`Unknown tool: ${name}`);
}

function rpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}

function rpcError(id, code, message) {
  return { jsonrpc: "2.0", id, error: { code, message } };
}

async function handleMcpRequestInner(req, url, env, info, ctx) {
  if (req.method === "GET") return new Response("MCP endpoint requires POST", { status: 405 });

  const authMatch = (req.headers.get("Authorization") || "").match(/^Bearer\s+(.+)$/i);
  const token = authMatch ? authMatch[1] : null;
  info.hasToken = Boolean(token);
  if (!(await isValidAccessToken(env, token))) {
    return json({ error: "unauthorized" }, 401, {
      "WWW-Authenticate": `Bearer resource_metadata="${url.origin}/.well-known/oauth-protected-resource"`,
    });
  }

  let body;
  try {
    body = await req.json();
  } catch {
    return json(rpcError(null, -32700, "Parse error"), 400);
  }

  const { id, method, params } = body || {};
  info.rpcMethod = method;

  if (method === "initialize") {
    return json(
      rpcResult(id, {
        protocolVersion: PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: "intervals-icu-mcp", version: "1.0.0" },
      }),
    );
  }

  if (method === "notifications/initialized" || method === "ping") {
    return new Response(null, { status: 202 });
  }

  if (method === "tools/list") {
    return json(rpcResult(id, { tools: TOOLS }));
  }

  if (method === "tools/call") {
    const toolName = params?.name;
    const args = params?.arguments || {};
    try {
      const data = await callTool(env, toolName, args);
      // Dashboard-Cache nach dem Schreiben neu aufbauen, damit Karte, Widget und Uhr die Liste sofort zeigen
      if (toolName === "save_decisions" && ctx?.waitUntil) ctx.waitUntil(warmDashboardCache(env).catch(() => {}));
      // Export-Tools liefern fertigen Text (kompakt), alle anderen ein Objekt
      return json(rpcResult(id, { content: [{ type: "text", text: typeof data === "string" ? data : JSON.stringify(data, null, 2) }] }));
    } catch (e) {
      return json(
        rpcResult(id, {
          content: [{ type: "text", text: `Error: ${String(e?.message ?? e)}` }],
          isError: true,
        }),
      );
    }
  }

  return json(rpcError(id, -32601, `Method not found: ${method}`));
}

// Diagnostic log line per MCP request (visible in Workers Logs): lets us see which
// RPC method the client sent and which HTTP status the Worker answered with, since
// the Cloudflare log list only shows "Ok" for the Worker outcome, not the status.
export async function handleMcpRequest(req, url, env, ctx = null) {
  const info = { httpMethod: req.method, hasToken: false, rpcMethod: null };
  let res;
  try {
    res = await handleMcpRequestInner(req, url, env, info, ctx);
    return res;
  } finally {
    console.log("mcp", JSON.stringify({ ...info, status: res?.status ?? "exception", protocol: req.headers.get("mcp-protocol-version"), ua: req.headers.get("user-agent") }));
  }
}
