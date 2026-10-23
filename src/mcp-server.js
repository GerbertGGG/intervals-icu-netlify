// MCP (Model Context Protocol) endpoint: exposes read-only Intervals.icu data
// (planned workouts, past activities, wellness) and the Yazio nutrition diary as
// tools so a Claude chat can be added as a custom connector against this Worker,
// reusing the existing intervals-client.js / yazio-client.js fetchers instead of
// second integrations. get_coach liest die letzte Coach-Aussage samt Rückfrage und Antwort.
// Die einzigen schreibenden Tools sind save_runalyze_snapshot,
// save_studie, save_coach, save_experiment, save_profil, save_insight, archive_insight und save_targets: Sie legen das Dashboard-Snapshot im KV ab (Weg fuer Sitzungen, die weder den
// Worker per HTTP noch GitHub erreichen, aber diesen Connector nutzen).
import { json } from "./http-helpers.js";
import { isIsoDate, isoDate, isoDateBerlin, listIsoDaysInclusive } from "./date-utils.js";
import { isValidAccessToken } from "./mcp-oauth.js";
import { fetchIntervalsEvents, fetchIntervalsActivities, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { hasKv, readKvJson, writeKvJson } from "./kv.js";
import { RUNALYZE_KV_KEY, validateSnapshot } from "./runalyze-snapshot.js";
import { STUDIE_KV_KEY, validateStudie } from "./studie-snapshot.js";
import { COACH_KV_KEY, validateCoach, mergeCoach, readCoach } from "./coach-snapshot.js";
import { EXPERIMENT_KV_KEY, PROFIL_KV_KEY, validateExperiment, validateProfil, readExperiment, readProfil } from "./experiment-snapshot.js";
import { INSIGHTS_KV_KEY, validateInsight, validateArchive, upsertInsight, archiveInsight, insightsForCoach, readInsights } from "./insight-snapshot.js";
import { computeWeekTotals, currentWeekTotals, weekTotalsOldest } from "./week-totals.js";
import { TARGETS_KV_KEY, COACH_HISTORY, validateTargets, appendTarget, readTargets, targetsView } from "./targets-snapshot.js";
import { hasYazioCredentials, fetchYazioDailyNutrition, fetchYazioDailyGoalKcal } from "./yazio-client.js";

// get_nutrition fetches one Yazio API round-trip per day in the range (Yazio has
// no range endpoint), sequentially to stay gentle on Yazio's rate limiting - so the
// range is capped to keep a single MCP call within the Worker's execution time.
const MAX_NUTRITION_DAYS = 31;

const PROTOCOL_VERSION = "2025-06-18";

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
    name: "save_coach",
    description:
      "Schreibt die Tagesaussage des Coaches fürs Dashboard (Karte) und Widget (Hinweiszeile). Bei jedem Update aufrufen, fetchedAt = jetzt. Das Widget zeigt sie nur 36 Stunden lang.",
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
    name: "save_experiment",
    description:
      "Schreibt (überschreibt) das laufende Trainings-Experiment fürs Dashboard (Karte \"Experiment\"). Die Trainingstage je Woche rechnet der Worker selbst aus den Aktivitäten. fetchedAt = jetzt. Nur Messwerte übergeben, die es wirklich gibt; fehlt ein aktueller Wert, current und currentDate auf null setzen.",
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
      "Legt eine Erkenntnis an oder aktualisiert sie (Upsert über id): eine Auffälligkeit, die du selbst in Gerberts Daten gefunden hast. Steht als Karte im Dashboard-Bereich \"Erkenntnisse\". Dieselbe id für dieselbe Erkenntnis verwenden, damit Status und Stärke ihren Verlauf behalten; vorher in get_coach unter insights nachsehen. fetchedAt = jetzt. Eine archivierte Erkenntnis wird durch Speichern wieder aktiv.",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "Stabile id in kebab-case, max. 60 Zeichen, z. B. \"trainingstage-vs-umfang\"." },
        title: { type: "string", description: "Titel, max. 60 Zeichen." },
        text: { type: "string", description: "Was auffällt und warum es zählt, Du-Form, max. 280 Zeichen." },
        action: { type: ["string", "null"], description: "Konsequenz oder Rat, max. 160 Zeichen (optional)." },
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
    name: "get_coach",
    description:
      "Liest die letzte gespeicherte Coach-Aussage (note, headline, text) mit Rückfrage (question) und Gerberts Antwort (answer, answeredAt; null = nicht beantwortet), dazu die aktuelle Idee der Woche (idea: text, angle, sport, createdAt; null = keine), die Wochenampel (week: light, text, createdAt; null = keine), den Wochenvorschlag (week_plan: week, basis, deload, targets, days, createdAt; null = keiner), die markierten Entlastungswochen (deloadWeeks: Montage, neueste zuerst), die bisherigen Ideen (ideas: date, text, angle, sport; max. 12, neueste zuerst), das laufende Experiment (experiment, wie bei save_experiment; null = keins), die Profil-Punkte (profil: updatedAt, highlights; null = keine), die aktiven Erkenntnisse (insights: id, title, discipline, strength, status, updatedAt, kurzer text; neueste zuerst), die Wochensummen der laufenden ISO-Woche (week_totals: week, enduranceHours = Laufen+Rad+Schwimmen ohne Kraft, referenceHours = Referenzwoche (Mittel der Ausdauerzeit der letzten 3 Wochen ohne leichte Wochen unter 70 %; null ohne Vorwochen), byDiscipline { lauf, rad, schwimmen } in Stunden, strengthSets, strengthSets2w = laufende Woche plus Vorwoche, streakWeeksAbove4_5 = Wochen in Folge ab streakThresholdPct % des jeweiligen Wochenziels, goals; null = Intervals nicht erreichbar) und den aktuellen Rahmen (targets: current mit enduranceHoursPerWeek, strengthSets2w, streakThresholdPct, maxIncreasePct, intensity [{ sport, text }], guardrails, notAllowed, validFrom, reason, savedAt; upcoming = geplante mit validFrom in der Zukunft; history = letzte 10 Einträge aus save_targets mit reason und changed, neueste zuerst; ohne Eintrag der Startwert). Vor jedem Morgen-Update aufrufen.",
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
  if (name === "save_coach") return saveSnapshot(env, args, validateCoach, COACH_KV_KEY, mergeCoach);
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
    const weekTotals = (async () => currentWeekTotals(computeWeekTotals(await fetchIntervalsActivities(env, weekTotalsOldest(today), today), today, stored)))().catch(() => null);
    const [coach, experiment, profil, insights, week_totals] = await Promise.all([readCoach(env), readExperiment(env), readProfil(env), readInsights(env), weekTotals]);
    // targets: heute geltendes Wochenziel, geplante Änderungen und die letzten Einträge (save_targets), neueste zuerst
    const { current, upcoming, history } = targetsView(stored, today, COACH_HISTORY);
    // idea/ideas auch bei aelteren Eintraegen ohne diese Felder, damit der Coach immer dieselbe Form liest
    const extra = { experiment: experiment ?? null, profil: profil ?? null, insights: insightsForCoach(insights), week_totals, targets: { current, upcoming, history } };
    return coach
      ? { ...coach, idea: coach.idea ?? null, ideas: coach.ideas ?? [], week: coach.week ?? null, week_plan: coach.week_plan ?? null, deloadWeeks: coach.deloadWeeks ?? [], ...extra }
      : { none: true, idea: null, ideas: [], week: null, week_plan: null, deloadWeeks: [], ...extra };
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

async function handleMcpRequestInner(req, url, env, info) {
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
      return json(rpcResult(id, { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] }));
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
export async function handleMcpRequest(req, url, env) {
  const info = { httpMethod: req.method, hasToken: false, rpcMethod: null };
  let res;
  try {
    res = await handleMcpRequestInner(req, url, env, info);
    return res;
  } finally {
    console.log("mcp", JSON.stringify({ ...info, status: res?.status ?? "exception", protocol: req.headers.get("mcp-protocol-version"), ua: req.headers.get("user-agent") }));
  }
}
