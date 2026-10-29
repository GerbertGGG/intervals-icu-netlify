import { json } from "./http-helpers.js";
import { hasYazioCredentials, fetchYazioDailyGoals } from "./yazio-client.js";
import { diffDays, isoDateBerlin } from "./date-utils.js";
import { activityDay, activityLoad, sessionLoad, isRun, isBike, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, fetchIntervalsSportSettings, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { resolveActiveGoalRace, DISTANCE_LABELS, DISTANCE_KM } from "./goal-race.js";
import { isARaceEvent, isBRaceEvent } from "./event-utils.js";
import { getEventDistanceFromEvent, parseTriathlonEvent } from "./block-phase.js";
import { resolveSeasonBlock, isSeasonBlockEvent, buildBlockGoalsFull, windowOf, parseBlockTargets, intensityKindOf, intensityFromFrame, checkBlockText, TEST_RE } from "./season-block.js";
import { buildBaseTrend } from "./base-trend.js";
import { testConfigOf, TEST_RULES } from "./session-tests.js";
import { loadBlockSessionInputs } from "./block-inputs.js";
import { readEgymStrengthDates, goalOf as egymGoalOf } from "./egym-widget.js";
import { buildTriathlonTargets } from "./triathlon-targets.js";
import { mustEnv } from "./kv.js";
import { computeVdotFromRaceTime, paceTargetsFromVdot, predictRaceTimesFromVdot } from "./vdot.js";
import { findHipFlags, parseCravings, parseWorkoutSteps } from "./dashboard-parse.js";
import { buildIntervalSessions } from "./interval-splits.js";
import { loadSessionProfile } from "./session-profile.js";
import { readStudie } from "./studie-snapshot.js";
import { buildErnaehrung } from "./ernaehrung.js";
import { readCoach, applyEditedPlan } from "./coach-snapshot.js";
import { readExperiment, readProfil, computeTrainingDays } from "./experiment-snapshot.js";
import { computeWeekTotals, enduranceSecOf, enduranceWeeks, strengthSetsOf } from "./week-totals.js";
import { withSwimIntervals } from "./swim-time.js";
import { readTargets, targetsView } from "./targets-snapshot.js";
import { readInsights, splitInsights } from "./insight-snapshot.js";
import { buildSummary } from "./dashboard-summary.js";
import { buildWeekReview } from "./week-review.js";
import { withActualToday } from "./live-load.js";
import { bestForDistance, readRunalyzeSnapshot, runKindFromType } from "./runalyze-snapshot.js";
import { readRunalyzeHistory, historyEntryFromSnapshot, upsertHistory } from "./runalyze-history.js";

// Read-only Endpunkt für das Trainings-Dashboard (public/dashboard/index.html).
// Die API-Schlüssel bleiben im Worker; der Browser bekommt nur diese aufbereitete JSON.
// Zugriff nur mit DASHBOARD_TOKEN (Header "Authorization: Bearer <token>").

const HISTORY_DAYS = 56;
// Wochen vor Blockstart, die fuer die Referenzwoche der ersten Blockwochen mitgeladen werden (Mittel aus 3 Wochen, leichte Wochen
// brauchen ihre eigene Referenz)
const BLOCK_LEAD_WEEKS = 6;
const LONGRUN_MIN_KM = 15;
// Formkurve (CTL/ATL) zeigt einen längeren Verlauf, damit die Tagesschwankungen wie in Intervals sichtbar werden
const FORM_DAYS = 180;
const PLAN_AHEAD_DAYS = 14;
const FORM_AHEAD_DAYS = 30; // Formprognose: so weit reicht die Vorschau auf CTL/ATL/TSB
const CTL_DAYS = 42, ATL_DAYS = 7; // Zeitkonstanten wie in intervals.icu

// Vom Nutzer vorgegeben (Halbmarathon Samstag 03.10.2026, Ziel < 2:00:00). Wird nur
// verwendet, wenn in Intervals.icu kein A-Rennen im Kalender steht.
const CONFIGURED_GOAL = { date: "2026-10-03", name: "Halbmarathon", distance: "hm", distanceKm: 21.0975, targetTimeSecs: 7200 };

export function timingSafeEqual(a, b) {
  const x = new TextEncoder().encode(String(a));
  const y = new TextEncoder().encode(String(b));
  let diff = x.length ^ y.length;
  for (let i = 0; i < Math.max(x.length, y.length); i++) diff |= (x[i] ?? 0) ^ (y[i] ?? 0);
  return diff === 0;
}

export function isAuthorized(req, env) {
  const expected = env?.DASHBOARD_TOKEN;
  if (!expected) return false;
  const header = req.headers.get("authorization") || "";
  const given = header.startsWith("Bearer ") ? header.slice(7).trim() : "";
  return given.length > 0 && timingSafeEqual(given, expected);
}

function addDays(iso, n) {
  return new Date(new Date(iso + "T00:00:00Z").getTime() + n * 86400000).toISOString().slice(0, 10);
}

function mondayOf(iso) {
  const d = new Date(iso + "T00:00:00Z");
  const dow = (d.getUTCDay() + 6) % 7;
  return addDays(iso, -dow);
}

function num(v) {
  const n = Number(v);
  return v != null && v !== "" && Number.isFinite(n) ? n : null;
}

// Wellness-Skalen laufen ab 1; 0/null bedeutet "nicht erfasst", nie "gut".
function scaleValue(v) {
  const n = num(v);
  return n != null && n >= 1 ? n : null;
}

function formatPace(secPerKm) {
  if (!Number.isFinite(secPerKm) || secPerKm <= 0) return null;
  const m = Math.floor(secPerKm / 60);
  const s = Math.round(secPerKm % 60);
  return s === 60 ? `${m + 1}:00` : `${m}:${String(s).padStart(2, "0")}`;
}

// "MIT" (Mitteltempo) nur als grossgeschriebenes Kuerzel: das Wort "mit" ("Dauerlauf mit Anna") zaehlt nicht.
// Steigerungen/Strides sind kurze Zusaetze zu lockeren Laeufen und machen daraus keine Intensitaetseinheit.
const TEMPO_PATTERN = /\b(tempo|tdl|schwelle|wettkampfspezifisch|fartlek)/i;
const MIT_PATTERN = /\bMIT\b/;
const RACE_PATTERN = /\b(wettkampf|rennen|race)\b/i;
const BASE_PATTERN = /\b(grundlagen|easy|ga1|ga 1|regeneration|recovery)/i;
const RACE_DISTANCE_TOLERANCE = 0.15;

// ctx.raceDays: Tage mit Renn-Eintrag im Intervals-Kalender, ctx.rzRuns: Laeufe aus dem Runalyze-Snapshot
// (dort setzt der Nutzer die Art "Wettkampf" selbst). Beide werden ueber Datum und Distanz zugeordnet.
// Lauf am selben Tag mit passender Distanz in Runalyze mit der angegebenen Art
function isRunalyzeKind(a, ctx, kind) {
  const km = (num(a?.distance) ?? 0) / 1000;
  const day = activityDay(a);
  return km > 0 && (ctx?.rzRuns ?? []).some((r) => r.date === day && runKindFromType(r.type) === kind && Math.abs(r.distanceKm - km) / km <= RACE_DISTANCE_TOLERANCE);
}

// Decoupling eines Laufs: nur der Runalyze-Wert desselben Laufs (gleicher Tag, Distanz passt). Intervals liefert
// beim Laufen ohne Laufleistung nichts Brauchbares, sein Wert wird deshalb nie genommen.
function runalyzeDecoupling(a, ctx) {
  const km = (num(a?.distance) ?? 0) / 1000;
  const day = activityDay(a);
  const hit = km > 0 ? (ctx?.rzRuns ?? []).find((r) => r.date === day && Number.isFinite(r.decouplingPct) && Math.abs(r.distanceKm - km) / km <= RACE_DISTANCE_TOLERANCE) : null;
  return hit ? hit.decouplingPct : null;
}

function isRaceRun(a, ctx) {
  if (String(a?.sub_type ?? "").toUpperCase() === "RACE" || a?.race === true) return true;
  const name = String(a?.name ?? "");
  if (RACE_PATTERN.test(name)) return true;
  const day = activityDay(a);
  const km = (num(a?.distance) ?? 0) / 1000;
  // Name nennt die Distanz ("Halbmarathon Berlin") und die gelaufene Strecke passt dazu; "Marathonpace"-Training bleibt aussen vor
  const named = /halbmarathon|halb\s*marathon/i.test(name) ? DISTANCE_KM.hm : /\bmarathon\b(?![\s-]*pace)/i.test(name) ? DISTANCE_KM.m : null;
  if (named && km > 0 && Math.abs(named - km) / named <= RACE_DISTANCE_TOLERANCE) return true;
  const close = (other) => km > 0 && other > 0 && Math.abs(other - km) / km <= RACE_DISTANCE_TOLERANCE;
  if (ctx?.raceDays?.get(day)?.some((evKm) => evKm == null || close(evKm))) return true;
  return isRunalyzeKind(a, ctx, "race");
}

// Bestimmt die Einheitsart, um die Pulsregel durchzusetzen: Puls gibt es nur bei
// "base" und "long". Rennen ("race") haben eine eigene Kategorie und gehen weder in den
// Longrun-Tracker noch in die Pulsauswertung. Alles Unklare wird wie eine Intensitaetseinheit behandelt.
export function classifyRun(a, ctx = null) {
  const text = `${a?.name ?? ""} ${a?.description ?? ""}`;
  if (isRaceRun(a, ctx)) return "race";
  if (isIntervalActivity(a) || hasIntervalTextSignal(a) || TEMPO_PATTERN.test(String(a?.name ?? "")) || MIT_PATTERN.test(String(a?.name ?? ""))) return "intensity";
  if (isRunalyzeKind(a, ctx, "long")) return "long";
  if (BASE_PATTERN.test(text)) return "base";
  return "unknown";
}

export function buildRunRecord(a, ctx = null) {
  const distanceM = num(a?.distance) ?? 0;
  const timeSecs = num(a?.moving_time) ?? 0;
  const pace = distanceM > 0 && timeSecs > 0 ? timeSecs / (distanceM / 1000) : null;
  const kind = classifyRun(a, ctx);
  const showHr = kind === "base" || kind === "long";
  return {
    id: a?.id ?? null,
    date: activityDay(a),
    name: a?.name ?? null,
    description: a?.description ?? null,
    kind,
    distanceKm: Math.round((distanceM / 1000) * 100) / 100,
    movingTimeMin: Math.round(timeSecs / 60),
    pace: formatPace(pace),
    paceSecPerKm: pace != null ? Math.round(pace) : null,
    load: activityLoad(a) || null,
    rpe: num(a?.icu_rpe),
    feel: num(a?.feel),
    // Harte Regel: bei Intervall-/Tempo-/unklaren Einheiten verlassen keine Pulswerte den Worker.
    avgHr: showHr ? num(a?.average_heartrate) : null,
    decoupling: showHr ? runalyzeDecoupling(a, ctx) : null,
    compliance: num(a?.compliance),
  };
}

const SPORTS = ["run", "bike", "swim", "strength", "other"];

// Kraft-Tage fuer das Block-Ziel: EGYM-Tage aus dem Cache, dazu Kraft-Einheiten aus Intervals von mind. 20 min an Tagen,
// die der Cache nicht kennt (Mobility, Dehnen, Yoga zaehlen nicht). Ist der Cache aelter als der Abgleich nach Intervals,
// kommt die heutige EGYM-Einheit nur ueber Intervals: Ihre Saetze stehen in der Beschreibung ("30 Saetze ..."), sonst
// zaehlte der Tag im Satz-Modus mit 0.
export function mergeStrengthDays(egymEntries, acts) {
  const egymSet = new Set(egymEntries.map((x) => x.date));
  const byDay = new Map();
  for (const a of acts ?? []) {
    if (sportOf(a) !== "strength" || (num(a?.moving_time) ?? 0) < 20 * 60 || /mobil|stretch|dehn|yoga/i.test(String(a?.name ?? ""))) continue;
    const date = activityDay(a);
    if (!date || egymSet.has(date)) continue;
    byDay.set(date, (byDay.get(date) ?? 0) + strengthSetsOf(a));
  }
  return [...egymEntries, ...[...byDay].map(([date, sets]) => (sets > 0 ? { date, sets } : { date }))];
}

export function sportOf(a) {
  const t = String(a?.type ?? "").toLowerCase();
  if (t.includes("swim")) return "swim";
  if (isBike(a)) return "bike";
  if (isRun(a)) return "run";
  if (t.includes("weight") || t.includes("strength") || t.includes("kraft")) return "strength";
  return "other";
}

function emptySports() {
  return Object.fromEntries(SPORTS.map((k) => [k, { count: 0, minutes: 0, km: 0, load: 0, plannedLoad: null, plannedKm: null, plannedMin: null, targetMin: null, targetLoad: null, targetLoadMin: null }]));
}

// Intensitaetsverteilung aus den Zonenzeiten, die Intervals.icu je Einheit mitliefert: Rad nach Power-Zonen,
// Laufen und Schwimmen nach Pace-Zonen. Pulszonen bleiben aussen vor (Pulsregel). Ohne Zonenzeiten null.
// Dreiteilung wie die Polarisation in Intervals (S1/S2/S3): Z1-Z2 locker, Z3-Z4 mittel, Z5+ hart, egal ob 5 oder 7 Zonen.
export function zoneBuckets(a) {
  const sport = sportOf(a);
  if (sport === "strength" || sport === "other") return null;
  // Intervals rechnet Pace-Zonen mit steigungskorrigiertem Tempo (GAP), wenn use_gap_zone_times gesetzt ist
  const raw = sport === "bike" ? a?.icu_zone_times : a?.use_gap_zone_times && Array.isArray(a?.gap_zone_times) ? a.gap_zone_times : a?.pace_zone_times;
  if (!Array.isArray(raw) || !raw.length) return null;
  const secs = raw.map((z) => Number(typeof z === "object" && z !== null ? z.secs : z) || 0);
  const [midFrom, hardFrom] = [2, 4];
  const out = { easy: 0, mid: 0, hard: 0 };
  secs.forEach((s, i) => { out[i < midFrom ? "easy" : i < hardFrom ? "mid" : "hard"] += s; });
  return out.easy + out.mid + out.hard > 0 ? out : null;
}

export function isWeekTarget(e) {
  return String(e?.category ?? "").toUpperCase() === "TARGET" && !isSeasonBlockEvent(e);
}

// TSS-Korridor des Wochenziels: load_target ist die Obergrenze, die Untergrenze TARGET_LOAD_FLOOR_SHARE davon. Steht in der
// Beschreibung des TARGET-Eintrags eine eigene Untergrenze ("min 220", "mindestens 220", "Untergrenze 220"), gilt die,
// höchstens bis zur Obergrenze. Ohne Obergrenze null.
export const TARGET_LOAD_FLOOR_SHARE = 0.8;
const FLOOR_RE = /(?:^|[^\p{L}\d])(?:min(?:destens)?|untergrenze)\.?\s*[:=]?\s*(\d{2,4})(?!\s*(?:min|minuten|h|std|km|m)\b)/iu;
export function targetLoadFloor(e) {
  const max = num(e?.load_target);
  if (max == null || !(max > 0)) return null;
  const m = String(e?.description ?? "").match(FLOOR_RE);
  return m ? Math.min(Number(m[1]), max) : max * TARGET_LOAD_FLOOR_SHARE;
}

function buildWeeks(todayIso, activities, events) {
  const firstMonday = mondayOf(addDays(todayIso, -(HISTORY_DAYS - 1)));
  const weeks = new Map();
  for (let d = firstMonday; d <= todayIso; d = addDays(d, 7)) {
    weeks.set(d, { weekStart: d, km: 0, runMin: 0, load: 0, runs: 0, plannedKm: null, plannedRunMin: null, plannedLoad: null, targetMin: null, targetLoad: null, targetLoadMin: null, bySport: emptySports(), intensity: { easy: 0, mid: 0, hard: 0 }, complete: addDays(d, 6) < todayIso });
  }
  for (const a of activities) {
    const w = weeks.get(mondayOf(activityDay(a)));
    if (!w) continue;
    const load = sessionLoad(a);
    const sp = w.bySport[sportOf(a)];
    sp.count += 1;
    sp.load += load;
    sp.minutes += (num(a?.moving_time) ?? 0) / 60;
    sp.km += (num(a?.distance) ?? 0) / 1000;
    const zt = zoneBuckets(a);
    if (zt) for (const k of Object.keys(zt)) w.intensity[k] += zt[k] / 60;
    w.load += load;
    if (isRun(a)) {
      w.km += (num(a?.distance) ?? 0) / 1000;
      w.runMin += (num(a?.moving_time) ?? 0) / 60;
      w.runs += 1;
    }
  }
  for (const e of events) {
    const day = String(e?.start_date_local || e?.start_date || "").slice(0, 10);
    const w = weeks.get(mondayOf(day));
    if (!w) continue;
    // Wochenziel des Coaches ("Weekly", Kategorie TARGET, je Sportart): Zeit ist das Ziel, TSS ein Korridor
    // (targetLoadMin bis targetLoad, siehe targetLoadFloor). Ein Trainingsblock kann ebenfalls TARGET sein ("Block …"), der zaehlt hier nicht.
    if (isWeekTarget(e)) {
      const sp = w.bySport[sportOf(e)], secs = num(e?.time_target), load = num(e?.load_target), floor = targetLoadFloor(e);
      if (secs != null && secs > 0) { sp.targetMin = (sp.targetMin ?? 0) + secs / 60; w.targetMin = (w.targetMin ?? 0) + secs / 60; }
      if (load != null && load > 0) { sp.targetLoad = (sp.targetLoad ?? 0) + load; w.targetLoad = (w.targetLoad ?? 0) + load; }
      if (floor != null) { sp.targetLoadMin = (sp.targetLoadMin ?? 0) + floor; w.targetLoadMin = (w.targetLoadMin ?? 0) + floor; }
      continue;
    }
    if (String(e?.category ?? "").toUpperCase() !== "WORKOUT") continue;
    const km = num(e?.distance_target ?? e?.distance);
    const load = num(e?.icu_training_load ?? e?.load_target);
    const secs = num(e?.moving_time);
    if (secs != null && secs > 0) {
      const spm = w.bySport[sportOf(e)];
      spm.plannedMin = (spm.plannedMin ?? 0) + secs / 60;
      if (sportOf(e) === "run") w.plannedRunMin = (w.plannedRunMin ?? 0) + secs / 60;
    }
    if (km != null) {
      w.plannedKm = (w.plannedKm ?? 0) + km / 1000;
      const spk = w.bySport[sportOf(e)];
      spk.plannedKm = (spk.plannedKm ?? 0) + km / 1000;
    }
    if (load != null) {
      w.plannedLoad = (w.plannedLoad ?? 0) + load;
      const sp = w.bySport[sportOf(e)];
      sp.plannedLoad = (sp.plannedLoad ?? 0) + load;
    }
  }
  const r1 = (v) => Math.round(v * 10) / 10;
  return [...weeks.values()].map((w) => ({
    ...w,
    km: r1(w.km),
    runMin: Math.round(w.runMin),
    plannedRunMin: w.plannedRunMin != null ? Math.round(w.plannedRunMin) : null,
    load: Math.round(w.load),
    plannedKm: w.plannedKm != null ? r1(w.plannedKm) : null,
    plannedLoad: w.plannedLoad != null ? Math.round(w.plannedLoad) : null,
    targetMin: w.targetMin != null ? Math.round(w.targetMin) : null,
    targetLoad: w.targetLoad != null ? Math.round(w.targetLoad) : null,
    targetLoadMin: w.targetLoadMin != null ? Math.round(w.targetLoadMin) : null,
    intensity: { easy: Math.round(w.intensity.easy), mid: Math.round(w.intensity.mid), hard: Math.round(w.intensity.hard) },
    bySport: Object.fromEntries(Object.entries(w.bySport).map(([k, v]) => [k, { count: v.count, minutes: Math.round(v.minutes), km: r1(v.km), load: Math.round(v.load), plannedLoad: v.plannedLoad != null ? Math.round(v.plannedLoad) : null, plannedKm: v.plannedKm != null ? r1(v.plannedKm) : null, plannedMin: v.plannedMin != null ? Math.round(v.plannedMin) : null, targetMin: v.targetMin != null ? Math.round(v.targetMin) : null, targetLoad: v.targetLoad != null ? Math.round(v.targetLoad) : null, targetLoadMin: v.targetLoadMin != null ? Math.round(v.targetLoadMin) : null }])),
  }));
}

// Rad und Schwimmen (Triathlon): Einheitenliste und FTP-Verlauf. Kein Puls: Die Pulsregel gilt
// hier vorsorglich mit, es gehen nur Leistung, Pace, RPE und Feel raus.
function findSettings(list, names) {
  return Array.isArray(list) ? list.find((s) => Array.isArray(s?.types) && names.some((n) => s.types.includes(n))) ?? null : null;
}

// Schwellen je Sportart aus den Intervals.icu-Sport-Einstellungen. threshold_pace ist dort in
// m/s hinterlegt (Feldname ungeprüft, sonst null); Laufen wird in s/km, Schwimmen in s/100 m
// umgerechnet. Alles Fehlende bleibt null.
function buildThresholds(settingsList) {
  const run = findSettings(settingsList, ["Run"]);
  const ride = findSettings(settingsList, ["Ride", "VirtualRide"]);
  const swim = findSettings(settingsList, ["Swim", "OpenWaterSwim"]);
  const secPer = (ms, meters) => (ms && ms > 0 ? Math.round(meters / ms) : null);
  return {
    run: { thresholdPaceSecPerKm: secPer(num(run?.threshold_pace), 1000), paceZonePct: zonePctFromBounds(run?.pace_zones), lthr: num(run?.lthr), maxHr: num(run?.max_hr) },
    bike: { ftp: num(ride?.ftp), indoorFtp: num(ride?.indoor_ftp), lthr: num(ride?.lthr), maxHr: num(ride?.max_hr) },
    swim: { thresholdPaceSecPer100m: secPer(num(swim?.threshold_pace), 100) },
  };
}

// Intervals.icu liefert Zonen als obere Grenzen in % der Schwellenpace (letzte oft offen, z. B. 999).
// Fuer die Hoehe der Workout-Bloecke zaehlt die Mitte der Zone; die erste (ohne untere Grenze) und die
// offene letzte Zone werden aus dem Nachbarn abgeleitet. Ohne Zonen null, dann gilt das Standardmodell.
export function zonePctFromBounds(bounds) {
  const b = (Array.isArray(bounds) ? bounds : []).map(Number);
  if (b.length < 2 || b.some((x) => !Number.isFinite(x) || x <= 0)) return null;
  const out = {};
  b.forEach((upper, i) => {
    const lower = i === 0 ? Math.max(upper - (b[1] - upper), 0) : b[i - 1];
    const top = upper > 200 ? lower + (lower - (i > 1 ? b[i - 2] : lower - 10)) : upper;
    out[i + 1] = Math.round(((lower + top) / 2) * 10) / 10;
  });
  return out;
}

function buildPlanned(events, todayIso, stepOpts = {}) {
  return events
    .filter((e) => String(e?.category ?? "").toUpperCase() === "WORKOUT")
    .map((e) => ({
      date: String(e?.start_date_local || e?.start_date || "").slice(0, 10),
      name: e?.name ?? null,
      description: e?.description ?? null,
      durationMin: num(e?.moving_time) != null ? Math.round(num(e.moving_time) / 60) : null,
      distanceKm: num(e?.distance_target ?? e?.distance) != null ? Math.round((num(e.distance_target ?? e.distance) / 1000) * 10) / 10 : null,
      load: num(e?.icu_training_load ?? e?.load_target),
      type: e?.type ?? null,
      sport: sportOf(e),
      tags: Array.isArray(e?.tags) ? e.tags.map(String) : [],
      steps: parseWorkoutSteps(e?.description, e?.workout_doc, { ...stepOpts, sport: sportOf(e) }),
    }))
    .filter((e) => e.date >= todayIso)
    .sort((a, b) => a.date.localeCompare(b.date));
}

// Geplante Einheiten der laufenden Woche (Mo-So), auch die vergangenen Tage: Grundlage der Wochenvorschau
// mit Plan gegen Ist. Ohne Workout-Schritte, die stehen nur in `planned` (ab heute).
function buildPlannedWeek(events, todayIso) {
  const start = mondayOf(todayIso), end = addDays(start, 6);
  return events
    .filter((e) => String(e?.category ?? "").toUpperCase() === "WORKOUT")
    .map((e) => ({
      date: String(e?.start_date_local || e?.start_date || "").slice(0, 10),
      name: e?.name ?? null,
      durationMin: num(e?.moving_time) != null ? Math.round(num(e.moving_time) / 60) : null,
      distanceKm: num(e?.distance_target ?? e?.distance) != null ? Math.round((num(e.distance_target ?? e.distance) / 1000) * 10) / 10 : null,
      load: num(e?.icu_training_load ?? e?.load_target) != null ? Math.round(num(e?.icu_training_load ?? e?.load_target)) : null,
      sport: sportOf(e),
    }))
    .filter((e) => e.date >= start && e.date <= end)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function positive(v) {
  const n = num(v);
  return n != null && n > 0 ? Math.round(n * 10) / 10 : null;
}

// CTL/ATL je Tag über FORM_DAYS; Tage der kurzen Wellness-Liste (inkl. Live-Wert für heute) haben Vorrang
function buildFormSeries(list, wellness) {
  const byDate = new Map();
  for (const w of list) {
    const date = String(w?.id ?? w?.date ?? "").slice(0, 10);
    if (date) byDate.set(date, { date, ctl: num(w?.ctl), atl: num(w?.atl) });
  }
  for (const w of wellness) if (w.date && (w.ctl != null || w.atl != null)) byDate.set(w.date, { date: w.date, ctl: w.ctl, atl: w.atl });
  return [...byDate.values()].sort((a, b) => a.date.localeCompare(b.date));
}

// Tageslast für die Prognose aus den Wochenzielen des Coaches (ohne Ziele null, dann gelten die geplanten Workouts).
// Wochensumme = Mitte der TSS-Korridore (Untergrenze bis Obergrenze, ohne Untergrenze die Obergrenze) plus Sportarten mit Zeitziel, aber ohne
// Grenze (meist Schwimmen): Zielminuten × deren TSS je Minute der letzten vollen Wochen. Laufende Woche: Rest nach dem
// bisher Erledigten gleichmäßig auf die Tage nach heute; spätere Wochen (noch ohne Ziel): dieselbe Woche wieder.
export function targetDailyLoad(weeks, todayIso) {
  const cur = weeks[weeks.length - 1];
  if (!cur || !(cur.targetLoad > 0 || cur.targetMin > 0)) return null;
  const past = weeks.slice(0, -1);
  let total = 0;
  for (const k of Object.keys(cur.bySport)) {
    const b = cur.bySport[k];
    if (b.targetLoad > 0) total += (b.targetLoad + (b.targetLoadMin ?? b.targetLoad)) / 2;
    else if (b.targetMin > 0) {
      const min = past.reduce((a, w) => a + w.bySport[k].minutes, 0), load = past.reduce((a, w) => a + w.bySport[k].load, 0);
      if (min > 0) total += b.targetMin * (load / min);
    }
  }
  const monday = mondayOf(todayIso), sunday = addDays(monday, 6), daysLeft = diffDays(todayIso, sunday);
  const rest = daysLeft > 0 ? Math.max(0, total - cur.load) / daysLeft : 0;
  return (date) => (date <= sunday ? rest : total / 7);
}

// Prognose von CTL/ATL ab dem letzten bekannten Wert: je Tag Last aus den Wochenzielen (targetLoad, siehe targetDailyLoad)
// oder ohne Ziele aus den geplanten Workouts (ohne Plan = Ruhetag), CTL += (Last - CTL) / 42, ATL += (Last - ATL) / 7.
// Heute zählt schon in den echten Werten, die Prognose beginnt morgen.
export function buildFormProjection(formSeries, events, todayIso, days = FORM_AHEAD_DAYS, targetLoad = null) {
  const base = [...formSeries].reverse().find((p) => p.date <= todayIso && p.ctl != null && p.atl != null);
  if (!base) return [];
  const load = new Map();
  for (const e of events) {
    if (String(e?.category ?? "").toUpperCase() !== "WORKOUT") continue;
    const day = String(e?.start_date_local || e?.start_date || "").slice(0, 10);
    const l = num(e?.icu_training_load ?? e?.load_target);
    if (day && l != null && l > 0) load.set(day, (load.get(day) ?? 0) + l);
  }
  const out = [];
  let ctl = base.ctl, atl = base.atl;
  for (let d = addDays(todayIso, 1); d <= addDays(todayIso, days); d = addDays(d, 1)) {
    const l = targetLoad ? targetLoad(d) : load.get(d) ?? 0;
    ctl += (l - ctl) / CTL_DAYS;
    atl += (l - atl) / ATL_DAYS;
    out.push({ date: d, ctl: Math.round(ctl * 10) / 10, atl: Math.round(atl * 10) / 10, plannedLoad: Math.round(l) });
  }
  return out;
}

function buildWellness(list) {
  return list
    .map((w) => ({
      date: String(w?.id ?? w?.date ?? "").slice(0, 10),
      ctl: num(w?.ctl),
      atl: num(w?.atl),
      rampRate: num(w?.rampRate),
      restingHR: scaleValue(w?.restingHR),
      sleepHours: num(w?.sleepSecs) != null && num(w.sleepSecs) > 0 ? Math.round((num(w.sleepSecs) / 3600) * 10) / 10 : null,
      sleepScore: num(w?.sleepScore) != null && num(w.sleepScore) > 0 ? num(w.sleepScore) : null,
      hrv: num(w?.hrv) != null && num(w.hrv) > 0 ? num(w.hrv) : null,
      weight: positive(w?.weight),
      sleepQuality: scaleValue(w?.sleepQuality),
      soreness: scaleValue(w?.soreness),
      fatigue: scaleValue(w?.fatigue),
      mood: scaleValue(w?.mood),
      motivation: scaleValue(w?.motivation),
      // Stress außerhalb des Trainings (Job, Alltag), nur für die Montagsbilanz
      stress: scaleValue(w?.stress),
      // Yazio-Werte stehen als Intervals-Zusatzfelder im Wellness-Eintrag (siehe sync.js). Der Sync
      // schreibt bei leerem Tagebuch 0: Nichts zu essen gibt es nicht, also zählt <= 0 als "keine Daten".
      calories: positive(w?.Calories),
      carbs: positive(w?.Carbs),
      protein: positive(w?.Protein),
      fat: positive(w?.Fat),
      calorieGoal: positive(w?.CalorieGoal),
    }))
    .filter((w) => /^\d{4}-\d{2}-\d{2}$/.test(w.date))
    .sort((a, b) => a.date.localeCompare(b.date));
}

// Tageswerte der Belastung für die Kalender-Heatmap (leere Tage = 0, keine Lücken).

// Stiller Hinweis in der Karte "Aktueller Rahmen" statt eigener Kachel: nur, wenn die Ramp Rate (CTL-Anstieg je Woche, Intervals)
// zuletzt über 5 lag oder der ACWR (ATL/CTL) an mind. 3 der letzten 7 Tage über 1,3. Nur Werte, keine Bewertung; sonst null.
export const LOAD_HINT = { rampMax: 5, acwrMax: 1.3, acwrMinDays: 3, days: 7 };
export function buildLoadHint(wellness, todayIso) {
  const from = addDays(todayIso, -(LOAD_HINT.days - 1));
  const recent = (wellness ?? []).filter((w) => w.date >= from && w.date <= todayIso);
  const ramp = [...recent].reverse().find((w) => w.rampRate != null)?.rampRate ?? null;
  const acwr = recent.filter((w) => w.ctl > 0 && w.atl != null).map((w) => ({ date: w.date, acwr: Math.round((w.atl / w.ctl) * 100) / 100 })).filter((x) => x.acwr > LOAD_HINT.acwrMax);
  const rampHit = ramp != null && ramp > LOAD_HINT.rampMax, acwrHit = acwr.length >= LOAD_HINT.acwrMinDays;
  if (!rampHit && !acwrHit) return null;
  return {
    ramp: rampHit ? Math.round(ramp * 10) / 10 : null,
    acwrDays: acwrHit ? acwr.length : null, acwrPeak: acwrHit ? Math.max(...acwr.map((x) => x.acwr)) : null,
    ...LOAD_HINT,
  };
}

function buildDaily(todayIso, activities) {
  const out = [];
  for (let d = addDays(todayIso, -(HISTORY_DAYS - 1)); d <= todayIso; d = addDays(d, 1)) out.push({ date: d, load: 0, sports: {}, minutes: {}, endurance: 0 });
  const byDate = new Map(out.map((x) => [x.date, x]));
  for (const a of activities) {
    const day = byDate.get(activityDay(a));
    if (!day) continue;
    const load = sessionLoad(a), secs = num(a?.moving_time) ?? 0;
    day.load += load;
    if (load) day.sports[sportOf(a)] = (day.sports[sportOf(a)] ?? 0) + load;
    // Minuten je Sportart (auch ohne Load, z. B. Kraft) fuer die Wochenkarte
    if (secs > 0) day.minutes[sportOf(a)] = (day.minutes[sportOf(a)] ?? 0) + secs / 60;
    // Ausdauerminuten wie in den Wochenzielen (week-totals.js, Schwimmen ohne Pausen) fuer den Wochenvergleich der Uhr
    day.endurance += enduranceSecOf(a) / 60;
  }
  const round = (o) => Object.fromEntries(Object.entries(o).map(([k, v]) => [k, Math.round(v)]));
  return out.map((x) => ({ ...x, load: Math.round(x.load), sports: round(x.sports), minutes: round(x.minutes), endurance: Math.round(x.endurance) }));
}

// Letzte erledigte Einheit fürs Heute-Band, wenn nichts geplant ist (Namen verlassen den Worker nicht).
export function pickLastActivity(activities, todayIso) {
  const done = activities.filter((a) => activityDay(a) && activityDay(a) <= todayIso && ((num(a?.moving_time) ?? 0) > 0 || activityLoad(a) > 0));
  return done.sort((x, y) => String(y?.start_date_local ?? "").localeCompare(String(x?.start_date_local ?? "")))[0] ?? null;
}

// Puls und Decoupling nur bei lockeren Läufen (Pulsregel); Rad und Schwimmen ohne Puls.
export const lastActivityHrAllowed = (a, ctx = null) => sportOf(a) === "run" && ["base", "long"].includes(classifyRun(a, ctx));

// Kennzahlen wie auf der Intervals-Karte: Dauer, Distanz, Load, Pace bzw. Leistung, IF, RPE, Gefühl.
export function buildLastActivity(activities, todayIso, ctx = null) {
  const a = pickLastActivity(activities, todayIso);
  if (!a) return null;
  const sport = sportOf(a), km = (num(a?.distance) ?? 0) / 1000, secs = num(a?.moving_time) ?? 0;
  const base = { date: activityDay(a), sport, durationMin: secs > 0 ? Math.round(secs / 60) : null, distanceKm: km > 0 ? Math.round(km * 10) / 10 : null, load: sessionLoad(a) ? Math.round(sessionLoad(a)) : null };
  const hr = lastActivityHrAllowed(a, ctx);
  // Fuer die Uhr: Startzeit (Ortszeit), Ende in Sekunden seit 1970 (aus start_date in UTC) und wie viele Tage die letzte
  // mindestens so lange Einheit derselben Sportart zurueckliegt (nur im geladenen Verlauf; ohne Treffer kein Wert)
  const start = String(a?.start_date_local ?? "").match(/T(\d{2}:\d{2})/)?.[1] ?? null;
  const startUtc = /Z$|[+-]\d{2}:?\d{2}$/.test(String(a?.start_date ?? "")) ? Date.parse(a.start_date) : NaN;
  const endAt = Number.isFinite(startUtc) && secs > 0 ? Math.round(startUtc / 1000 + (num(a?.elapsed_time) ?? secs)) : null;
  const longer = km > 0 ? activities.filter((x) => x !== a && sportOf(x) === sport && activityDay(x) < base.date && (num(x?.distance) ?? 0) / 1000 >= km).map(activityDay).sort().pop() : null;
  const pace = km > 0 && secs > 0 && (sport === "run" || sport === "swim") ? Math.round(sport === "swim" ? secs / (km * 10) : secs / km) : null;
  const extra = {
    paceSecPerKm: sport === "run" ? pace : null,
    paceSecPer100m: sport === "swim" ? pace : null,
    watts: sport === "bike" ? (num(a?.icu_average_watts) ?? num(a?.average_watts)) : null,
    normWatts: sport === "bike" ? num(a?.icu_weighted_avg_watts) : null,
    intensity: num(a?.icu_intensity) != null ? Math.round(num(a.icu_intensity)) : null,
    avgHr: hr ? num(a?.average_heartrate) : null,
    decoupling: hr ? runalyzeDecoupling(a, ctx) : null,
    rpe: num(a?.icu_rpe),
    feel: num(a?.feel),
    movingSecs: secs > 0 ? Math.round(secs) : null,
    start,
    endAt,
    longerAgoDays: longer ? diffDays(longer, base.date) : null,
  };
  return { ...base, ...Object.fromEntries(Object.entries(extra).filter(([, v]) => v != null)) };
}

// Heißhunger-Einträge und Hüft-/Leisten-/Knie-Hinweise aus den Freitexten. Die Rohtexte bleiben im Worker.
function buildInsights(rawWellness, activities) {
  const cravings = [];
  const hipFlags = [];
  for (const w of rawWellness) {
    const date = String(w?.id ?? w?.date ?? "").slice(0, 10);
    if (!/^\d{4}-\d{2}-\d{2}$/.test(date) || !w?.comments) continue;
    cravings.push(...parseCravings(date, w.comments));
    for (const f of findHipFlags(w.comments)) hipFlags.push({ date, source: "Wellness-Kommentar", ...f });
  }
  for (const a of activities) {
    for (const f of findHipFlags(`${a?.name ?? ""}. ${a?.description ?? ""}`)) hipFlags.push({ date: activityDay(a), source: "Einheit", ...f });
  }
  cravings.sort((a, b) => a.date.localeCompare(b.date) || (a.hour ?? 0) - (b.hour ?? 0));
  hipFlags.sort((a, b) => b.date.localeCompare(a.date));
  return { cravings, hipFlags };
}

// Fitness-Daten (Bereich 3). Puls nur aus Grundlagen- und Long-Slow-Läufen (Pulsregel).
function buildFitness(runs, snapshot, todayIso) {
  // Longrun-Tracker: Fuer die Langstrecke zaehlen die laengsten Laeufe der 8 Wochen, nicht der Wochenumfang.
  // Ein langer Lauf ist ein in Runalyze als "Langer Lauf" eingetragener Lauf (Art setzt der Nutzer selbst,
  // Runalyze liefert das Decoupling) oder ein Intervals-Lauf ab 15 km. Nur Datum, Distanz, Pace, Decoupling gehen raus.
  const since = addDays(todayIso, -(HISTORY_DAYS - 1));
  const rz = (snapshot?.runs ?? []).filter((r) => r.date >= since && runKindFromType(r.type) === "long");
  const rzRecords = rz.map((r) => {
    const pace = r.durationSec ? r.durationSec / r.distanceKm : null;
    return { date: r.date, distanceKm: Math.round(r.distanceKm * 10) / 10, paceSecPerKm: pace != null ? Math.round(pace) : null, pace: pace != null ? formatPace(pace) : null, decoupling: r.decouplingPct };
  });
  // Ergaenzung aus Intervals: Laeufe ab 15 km, die in Runalyze nicht als "Langer Lauf" stehen (andere Art, Snapshot veraltet).
  // Rennen und Rennabbrueche zaehlen mit, Intensitaetseinheiten nicht. Pro Tag gilt der Runalyze-Eintrag.
  const rzDays = new Set(rzRecords.map((r) => r.date));
  const icuRecords = (runs ?? [])
    .filter((r) => r.date >= since && r.distanceKm >= LONGRUN_MIN_KM && r.kind !== "intensity" && !rzDays.has(r.date))
    .map((r) => ({ date: r.date, distanceKm: Math.round(r.distanceKm * 10) / 10, paceSecPerKm: r.paceSecPerKm, pace: r.pace, decoupling: r.decoupling ?? null, race: r.kind === "race" }));
  const records = [...rzRecords, ...icuRecords];
  const byLength = [...records].sort((a, b) => b.distanceKm - a.distanceKm || b.date.localeCompare(a.date));
  return {
    longRunTracker: {
      source: "runalyze",
      minKm: LONGRUN_MIN_KM,
      longest: byLength[0] ?? null,
      count16: records.filter((r) => r.distanceKm >= 16).length,
      recent: byLength.slice(0, 6).sort((a, b) => b.date.localeCompare(a.date)),
    },
    longRuns: [...records].filter((r) => r.decoupling != null).sort((a, b) => a.date.localeCompare(b.date)).map((r) => ({ date: r.date, distanceKm: r.distanceKm, decoupling: r.decoupling })),
  };
}

// Grundlagenlaeufe aus den Zeitreihen (Blocktest-Modus, block-inputs.js): HF-Deckel-Einhaltung der laufenden Woche und
// die Blocktests und Trendlaeufe (category aus session-tests.js) mit Decoupling 5–60, stabiler Dauer, GPS-Korrektur, DFA alpha1 (Runalyze,
// gleicher Tag) und testReason.
function buildBaseRuns(runs, snapshot, targets, block, todayIso) {
  const cap = targets?.hrCap ?? null;
  const analysed = runs.filter((r) => (r.easy || r.category) && r.analysis);
  if (!analysed.length && cap == null) return null;
  const dow = (new Date(todayIso + "T00:00:00Z").getUTCDay() + 6) % 7, monday = addDays(todayIso, -dow);
  const wk = analysed.filter((r) => r.date >= monday && r.date <= todayIso && r.analysis.overCapMin != null);
  const dfa = new Map((snapshot?.runs ?? []).filter((x) => x.dfaAlpha1 != null).map((x) => [x.date, x.dfaAlpha1]));
  return {
    cap,
    week: { runs: wk.length, minutes: wk.reduce((a, r) => a + (r.durationMin ?? 0), 0), overMin: wk.reduce((a, r) => a + r.analysis.overCapMin, 0) },
    recent: analysed.filter((r) => r.category === "runBlocktest" || r.category === "runTrend").sort((a, b) => b.date.localeCompare(a.date)).slice(0, 8).map((r) => ({
      date: r.date, durationMin: r.durationMin, dec60: r.analysis.dec60, stableMin: r.analysis.stableMin, windows: r.analysis.windows, gps: r.analysis.gps ?? null,
      pace60: r.analysis.pace60 != null ? formatPace(r.analysis.pace60) : null, overCapMin: r.analysis.overCapMin, dfa: dfa.get(r.date) ?? null, test: r.category === "runBlocktest", category: r.category, testReason: r.testReason ?? null,
    })),
  };
}

// Halbmarathon-Zeiten aus verschiedenen Quellen für den Zielkorridor. Alles außer der Runalyze-Prognose
// ist eine Rechnung nach Daniels (VDOT-Modell), keine Vorhersage: Sie unterstellt Ausdauer wie über die
// Ausgangsdistanz und ist bei kurzen Distanzen deshalb tendenziell zu optimistisch.
function buildHmEstimates(snapshot, vdot, rows) {
  const hmSeconds = (v) => predictRaceTimesFromVdot(v)?.find((x) => x.key === "hm")?.seconds ?? null;
  const out = [];
  const prog = rows.find((r) => r.label === "Halbmarathon")?.prognosisSeconds;
  if (prog != null) out.push({ key: "runalyze", label: "Runalyze-Prognose", seconds: prog, kind: "prognosis" });
  if (vdot != null && hmSeconds(vdot)) out.push({ key: "vdot", label: `aus VDOT ${Math.round(vdot * 10) / 10}`, seconds: hmSeconds(vdot), kind: "calc" });
  for (const r of rows) {
    if (r.label === "Halbmarathon" || r.bestSeconds == null) continue;
    const v = computeVdotFromRaceTime(r.bestDistanceKm * 1000, r.bestSeconds);
    const sec = v != null ? hmSeconds(v) : null;
    if (sec) out.push({ key: `best-${r.distanceKm}`, label: `aus ${r.label}-Bestzeit`, seconds: sec, kind: "calc" });
  }
  return out;
}

// Dieselbe Rechnung für alle Standarddistanzen (5 km, 10 km, Halbmarathon, Marathon): aus dem aktuellen VDOT und
// aus dem VDOT der 10-km-Bestzeit. Je Distanz eine Liste wie in hmEstimates, damit das Dashboard zur Zieldistanz passt.
function buildRaceEstimates(vdot, rows, snapshot) {
  const best10 = rows.find((r) => r.label === "10 km");
  const vdot10 = best10?.bestSeconds != null ? computeVdotFromRaceTime(best10.bestDistanceKm * 1000, best10.bestSeconds) : null;
  const sources = [vdot != null ? { key: "vdot", label: `aus VDOT ${Math.round(vdot * 10) / 10}`, vdot } : null, vdot10 != null ? { key: "best-10", label: "aus 10-km-Bestzeit", vdot: vdot10 } : null].filter(Boolean);
  const dists = [{ key: "3k", km: 3, label: "3 km" }, { key: "5k", km: 5 }, { key: "10k", km: 10 }, { key: "hm", km: 21.0975 }, { key: "m", km: 42.195 }];
  return dists.map((d) => {
    const estimates = sources.map((s) => ({ key: s.key, label: s.label, seconds: predictRaceTimesFromVdot(s.vdot)?.find((x) => x.key === d.key)?.seconds ?? null, kind: "calc" })).filter((e) => e.seconds);
    const prog = snapshot?.prognosis?.find((p) => Math.abs(p.distanceKm - d.km) / d.km <= 0.01);
    if (prog?.seconds) estimates.unshift({ key: "runalyze", label: "Runalyze-Prognose", seconds: prog.seconds, kind: "prognosis" });
    return { key: d.key, km: d.km, label: d.label ?? DISTANCE_LABELS[d.key] ?? d.key, estimates };
  }).filter((d) => d.estimates.length);
}

function buildRunalyze(snapshot, history = []) {
  if (!snapshot) return null;
  const rows = [{ label: "5 km", km: 5 }, { label: "10 km", km: 10 }, { label: "Halbmarathon", km: 21.0975 }].map(({ label, km }) => {
    const best = bestForDistance(snapshot.races, km);
    const prog = snapshot.prognosis.find((p) => Math.abs(p.distanceKm - km) / km <= 0.01);
    return {
      label,
      distanceKm: km,
      bestSeconds: best?.officialTimeSec ?? null,
      bestDate: best?.date ?? null,
      bestDistanceKm: best?.officialDistanceKm ?? null,
      prognosisSeconds: prog?.seconds ?? null,
    };
  });
  // Runalyze liefert nur das VDOT (effektive VO2max), keine Trainingspaces: Die Paces werden
  // hier nach Daniels aus diesem VDOT berechnet (dieselbe Formel wie in vdot.js).
  const vdot = snapshot.vdot ?? null;
  const paces = vdot != null ? paceTargetsFromVdot(vdot) : null;
  return { fetchedAt: snapshot.fetchedAt, vdot, paces, rows, hmEstimates: buildHmEstimates(snapshot, vdot, rows), raceEstimates: buildRaceEstimates(vdot, rows, snapshot), hmHistory: upsertHistory(history, historyEntryFromSnapshot(snapshot)) };
}

async function settle(label, fn) {
  try {
    return { label, ok: true, value: await fn() };
  } catch (e) {
    return { label, ok: false, error: String(e?.message ?? e).slice(0, 300) };
  }
}

const eventDayOf = (e) => String(e?.start_date_local || e?.start_date || "").slice(0, 10);

// Renn-Eintraege (A/B/C) des Kalenders je Tag mit Zieldistanz in km, fuer die Zuordnung "Lauf = Rennen".
function buildRaceDays(events) {
  const map = new Map();
  for (const e of events) {
    const cat = String(e?.category ?? "").toUpperCase();
    if (!isARaceEvent(e) && !/^RACE(_[A-C])?$/.test(cat)) continue;
    const day = eventDayOf(e);
    const km = num(e?.distance_target ?? e?.distance);
    map.set(day, [...(map.get(day) ?? []), km != null && km > 0 ? km / 1000 : null]);
  }
  return map;
}

// Letztes A-Rennen bis heute (Kalender, sonst die konfigurierte Vorgabe), damit die Erholungsphase auch
// dann greift, wenn der Zielwettkampf nach dem Renntag schon aus "ab heute" herausgefallen ist.
function findRecentRace(events, todayIso) {
  const past = events
    .filter((e) => isARaceEvent(e) && eventDayOf(e) <= todayIso && diffDays(eventDayOf(e), todayIso) <= 30)
    .sort((a, b) => eventDayOf(b).localeCompare(eventDayOf(a)))[0];
  if (past) {
    const tri = parseTriathlonEvent(past);
    return { date: eventDayOf(past), name: past.name ?? null, distance: getEventDistanceFromEvent(past), triathlonFormat: tri?.format ?? null };
  }
  return CONFIGURED_GOAL.date <= todayIso ? { date: CONFIGURED_GOAL.date, name: CONFIGURED_GOAL.name, distance: CONFIGURED_GOAL.distance, triathlonFormat: null } : null;
}

// Kommende B-Rennen (Vorbereitungswettkaempfe) bis ~13 Monate voraus. Das A-Ziel bestimmt weiter das ganze Dashboard,
// B-Rennen erscheinen als Zwischenziele und werden nicht ausgeblendet.
const B_RACE_LOOKAHEAD_DAYS = 400;

function buildSupportRaces(events, todayIso, goalDate) {
  return events
    .filter((e) => isBRaceEvent(e) && eventDayOf(e) >= todayIso && eventDayOf(e) !== goalDate)
    .sort((a, b) => eventDayOf(a).localeCompare(eventDayOf(b)))
    .map((e) => {
      const tri = parseTriathlonEvent(e);
      const distance = getEventDistanceFromEvent(e);
      const secs = Number(e?.time_target ?? e?.moving_time ?? NaN);
      const day = eventDayOf(e);
      return {
        date: day,
        daysToGo: diffDays(todayIso, day),
        name: e?.name || (tri ? `Triathlon ${tri.label}` : DISTANCE_LABELS[distance] ?? "Rennen"),
        distance: distance ?? null,
        runKm: tri?.runKm ?? DISTANCE_KM[distance] ?? null,
        targetTimeSecs: Number.isFinite(secs) && secs > 0 ? secs : null,
        triathlonFormat: tri?.format ?? null,
      };
    });
}

export async function buildDashboard(env, todayIso = isoDateBerlin()) {
  mustEnv(env, "ATHLETE_ID");
  mustEnv(env, "INTERVALS_API_KEY");
  const oldest = addDays(todayIso, -(HISTORY_DAYS - 1));
  const newestEvents = addDays(todayIso, PLAN_AHEAD_DAYS);

  const [wellnessR, activitiesR, eventsR, formEventsR, bRacesR, goalR, settingsR, snapshot, runalyzeHistory, studie, coach, experiment, profil, coachInsights, storedTargets] = await Promise.all([
    settle("wellness", () => fetchIntervalsWellnessRange(env, oldest, todayIso)),
    settle("activities", () => fetchIntervalsActivities(env, oldest, todayIso)),
    settle("events", () => fetchIntervalsEvents(env, oldest, newestEvents)),
    settle("formEvents", () => fetchIntervalsEvents(env, todayIso, addDays(todayIso, FORM_AHEAD_DAYS))),
    settle("bRaces", () => fetchIntervalsEvents(env, todayIso, addDays(todayIso, B_RACE_LOOKAHEAD_DAYS))),
    settle("goal", () => resolveActiveGoalRace(env, todayIso)),
    settle("sportSettings", async () => {
      const list = await fetchIntervalsSportSettings(env);
      if (!list) throw new Error("sport-settings nicht abrufbar");
      return list;
    }),
    readRunalyzeSnapshot(env),
    readRunalyzeHistory(env),
    readStudie(env),
    readCoach(env),
    readExperiment(env),
    readProfil(env),
    readInsights(env),
    readTargets(env),
  ]);

  const activities = activitiesR.ok && Array.isArray(activitiesR.value) ? activitiesR.value : [];
  const events = eventsR.ok && Array.isArray(eventsR.value) ? eventsR.value : [];
  // Aktueller Rahmen des Coaches (save_targets): Karte "Aktueller Rahmen" und was als Intensitaet im Block zaehlt
  const targets = targetsView(storedTargets, todayIso), frame = targets.current;
  const wellness = wellnessR.ok ? withActualToday(buildWellness(wellnessR.value), activities, todayIso) : [];
  const fitnessR = await settle("fitness", () => fetchIntervalsWellnessRange(env, addDays(todayIso, -(FORM_DAYS - 1)), todayIso));
  const formSeries = buildFormSeries(fitnessR.ok && Array.isArray(fitnessR.value) ? fitnessR.value : [], wellness);
  const runCtx = { raceDays: buildRaceDays(events), rzRuns: snapshot?.runs ?? [] };
  const runs = activities.filter(isRun).map((a) => buildRunRecord(a, runCtx)).sort((a, b) => b.date.localeCompare(a.date));

  // Pace je Zone aus dem VDOT (Runalyze) fuer die Umrechnung von Distanz-Schritten geplanter Workouts
  const intervalSessions = await settle("intervalSplits", () => buildIntervalSessions(env, runs, todayIso, addDays(todayIso, -27)));
  const stepPaces = snapshot?.vdot ? Object.fromEntries((paceTargetsFromVdot(snapshot.vdot) ?? []).map((z) => [z.key, z.secPerKm])) : null;
  const goalFromCalendar = goalR.ok && goalR.value?.date ? goalR.value : null;
  const thresholds = buildThresholds(settingsR.ok ? settingsR.value : null);
  const tri = goalFromCalendar?.triathlon ?? null;
  // Triathlon: Gesamtzeit aus dem Eintrag, die Lauf-Ziele der Grafiken nur aus einer Lauf-Zeit in der Beschreibung ("Lauf 1:55:00").
  const goal = tri
    ? { date: goalFromCalendar.date, name: `Triathlon ${tri.label}`, targetTimeSecs: tri.runTargetSecs, totalTargetSecs: goalFromCalendar.targetTimeSecs ?? null, runKm: tri.runKm, triathlon: { format: tri.format, swimKm: tri.swimKm, bikeKm: tri.bikeKm, runKm: tri.runKm, targets: buildTriathlonTargets(tri, thresholds, goalFromCalendar.targetTimeSecs ?? null, { vdot: snapshot?.vdot ?? null, weightKg: [...wellness].reverse().find((w) => w.weight)?.weight ?? null }) }, source: "intervals" }
    : goalFromCalendar
      ? { date: goalFromCalendar.date, name: DISTANCE_LABELS[goalFromCalendar.distance] ?? "Rennen", distance: goalFromCalendar.distance ?? null, targetTimeSecs: goalFromCalendar.targetTimeSecs ?? (goalFromCalendar.distance === "hm" ? CONFIGURED_GOAL.targetTimeSecs : null), runKm: DISTANCE_KM[goalFromCalendar.distance] ?? null, source: "intervals" }
      : { ...CONFIGURED_GOAL, runKm: CONFIGURED_GOAL.distanceKm, source: "config", none: CONFIGURED_GOAL.date < todayIso };
  // none: kein kommendes Rennen im Kalender (die Vorgabe ist vorbei). Dann zeigt die Seite keine Renn-Kacheln, Zielpaces oder Prognosen.

  // Krankheit: Kalendereintraege der Kategorie SICK/INJURED oder eine Notiz "krank" (Ende im Kalender exklusiv).
  // Zaehlt fuer die Intensitaets-Leitplanke und die Erholungswerte (Basis ohne Krankheitstage).
  const sickDays = events.filter((e) => /SICK|INJURED/i.test(String(e?.category ?? "")) || (/NOTE/i.test(String(e?.category ?? "")) && /krank/i.test(String(e?.name ?? "")))).flatMap((e) => {
    const from = String(e.start_date_local ?? "").slice(0, 10), endRaw = String(e.end_date_local ?? "").slice(0, 10);
    const to = endRaw > from ? addDays(endRaw, -1) : from;
    const out = [];
    for (let d = from; d && d <= to && out.length < 60; d = addDays(d, 1)) out.push(d);
    return out;
  });
  const insights = buildInsights(wellnessR.ok && Array.isArray(wellnessR.value) ? wellnessR.value : [], activities);
  // Langzeitbasis der Erholungswerte (180 Tage Wellness aus der Formkurve)
  const longWellness = fitnessR.ok && Array.isArray(fitnessR.value) ? buildWellness(fitnessR.value).map((w) => ({ date: w.date, hrv: w.hrv, restingHR: w.restingHR })) : [];
  let blockTargets = null, blockSessions = {}, intensity = [], plannedTests = [], testCfg = null;
  // Name/Beschreibung der Einheit und des gekoppelten Workouts (Intensitaet)
  const evById = new Map(events.map((e) => [String(e?.id), e]));
  const textOf = (a) => { const e = evById.get(String(a?.paired_event_id)); return [a?.name, a?.description, e?.name, e?.description].filter(Boolean).join(" "); };

  // Trainingsblock aus dem Kalender samt Fortschritt der Ziele. Laeuft der Block schon laenger als die 56 Tage Historie,
  // werden Aktivitaeten und Wellness ab Blockstart nachgeladen (best effort, sonst zaehlt nur das Vorhandene).
  let seasonBlock = resolveSeasonBlock([...(bRacesR.ok && Array.isArray(bRacesR.value) ? bRacesR.value : []), ...events], todayIso);
  if (seasonBlock) {
    let blockActs = activities, blockWell = wellnessR.ok && Array.isArray(wellnessR.value) ? wellnessR.value : [];
    // Aktivitaeten ab BLOCK_LEAD_WEEKS vor Blockstart: Referenzwoche der ersten Blockwochen (Intensitaets-Kachel)
    const actsFrom = addDays(seasonBlock.start, -7 * BLOCK_LEAD_WEEKS);
    let actsCover = oldest;
    if (seasonBlock.status === "active" && actsFrom < oldest) {
      const till = addDays(oldest, -1);
      const [xa, xw] = await Promise.all([
        settle("blockActivities", () => fetchIntervalsActivities(env, actsFrom, till)),
        seasonBlock.start < oldest ? settle("blockWellness", () => fetchIntervalsWellnessRange(env, seasonBlock.start, till)) : { ok: false },
      ]);
      if (xa.ok && Array.isArray(xa.value)) {
        const have = new Set(activities.map((x) => x?.id));
        blockActs = [...xa.value.filter((x) => !have.has(x?.id)), ...activities];
        actsCover = actsFrom;
      }
      if (xw.ok && Array.isArray(xw.value)) blockWell = [...xw.value, ...blockWell];
    }
    const blockRuns = blockActs === activities ? runs : blockActs.filter(isRun).map((a) => buildRunRecord(a, runCtx));
    // Lange Laeufe wie im Longrun-Tracker: Art "Langer Lauf" und Decoupling aus Runalyze; Intervals nur als Ergaenzung fuer Tage ohne Runalyze-Wert.
    const rzLong = (snapshot?.runs ?? []).filter((r) => runKindFromType(r.type) === "long" && Number.isFinite(r.decouplingPct)).map((r) => ({ date: r.date, decoupling: r.decouplingPct }));
    const rzDays = new Set(rzLong.map((r) => r.date));
    const icuLong = blockRuns.filter((r) => r.kind === "long" && r.decoupling != null && !rzDays.has(r.date)).map((r) => ({ date: r.date, decoupling: r.decoupling }));
    // Kraft-Tag = Tag mit EGYM-Training oder einer Kraft-Einheit aus Intervals von mind. 20 min (Mobility, Dehnen, Yoga zaehlen nicht)
    const egymDays = await readEgymStrengthDates(env).catch(() => null);
    const egymEntries = egymDays?.days ?? (egymDays?.dates ?? []).map((date) => ({ date }));
    const strengthDays = mergeStrengthDays(egymEntries, blockActs);
    // Abschlussziele (Lauf/Rad vergleichbar, Schwimmtest): Rohwerte je Einheit, Decoupling notfalls aus den Zeitreihen (best effort)
    const blockText = [seasonBlock.goal, ...(seasonBlock.notes ?? [])].filter(Boolean).join("\n\n");
    const tg = parseBlockTargets(blockText);
    blockTargets = tg;
    const win = windowOf(seasonBlock, todayIso);
    // Tests erkennt session-tests.js aus der absolvierten Aktivitaet: Termine, HF-Deckel und Vergleichsleistung aus dem
    // Block-Eintrag (sonst Fallback mit Warnung). Geplante Tests = Termine aus dem Text, dazu Lauf-Workouts "Blocktest" (nur Anzeige)
    testCfg = testConfigOf(blockText, seasonBlock.start, tg, seasonBlock);
    plannedTests = [...new Set([
      ...events.filter((e) => /WORKOUT/i.test(String(e?.category ?? "")) && /run/i.test(String(e?.type ?? "")) && TEST_RE.test(`${e?.name ?? ""} ${e?.description ?? ""}`)).map((e) => eventDayOf(e)),
      ...(tg.run?.tests ? testCfg.testDates : []),
    ])].filter(Boolean).sort();
    const sessions = tg.run || tg.bike || tg.swim
      ? await loadBlockSessionInputs(env, { acts: blockActs, runs: blockRuns, rzRuns: snapshot?.runs ?? [], from: win.from, to: win.to, swimDistM: tg.swim?.distM ?? 500, run: tg.run ?? null, hrCap: tg.hrCap ?? null, testCfg }).catch((err) => { console.warn("block sessions", err); return {}; })
      : {};
    const dayOfWell = (x) => String(x?.id ?? x?.date ?? "").slice(0, 10);
    // Intensitaets-Leitplanke: Rad-HIIT und Huegelsprints aus Name/Beschreibung der Einheit und des gekoppelten Workouts;
    // was zaehlt, steht im Rahmen des Coaches (save_targets), sonst im Blocktext
    blockSessions = sessions;
    intensity = intensityFromFrame(frame, tg.intensity)
      ? blockActs.map((a) => {
          const sport = isBike(a) ? "bike" : isRun(a) ? "run" : null;
          const z5 = Array.isArray(a?.icu_zone_times) ? num(a.icu_zone_times.find((z) => z?.id === "Z5")?.secs) : null;
          return { date: activityDay(a), kind: sport ? intensityKindOf(sport, textOf(a), z5) : null, feel: num(a?.feel) };
        }).filter((x) => x.kind)
      : [];
    const { goals } = buildBlockGoalsFull(seasonBlock, {
      longRuns: [...rzLong, ...icuLong],
      strength: strengthDays,
      weeklyGoalSets: egymGoalOf(env).goal,
      strengthSource: { egym: Boolean(egymDays), egymAt: egymDays?.at ?? null },
      acwrDays: blockWell.map((x) => ({ date: dayOfWell(x), acwr: num(x?.ctl) > 0 ? num(x?.atl) / num(x.ctl) : null })),
      ...sessions,
      ftp: thresholds.bike.indoorFtp ?? thresholds.bike.ftp ?? null,
      ctlDays: blockWell.map((x) => ({ date: dayOfWell(x), ctl: num(x?.ctl) })),
      activityDays: [...blockActs.map((a) => activityDay(a)), ...egymEntries.map((x) => x.date)],
      intensity,
      plannedTests,
      sickDays,
      deloadWeeks: coach?.deloadWeeks ?? [],
      frame,
      // Intensitaet je Woche gegen den damaligen Rahmen und den Umfang gegen die Referenzwoche (week-totals.js)
      frames: targets.history,
      weekVolumes: enduranceWeeks(blockActs, actsCover, todayIso),
      plannedBreaks: events.filter((e) => /NOTE|HOLIDAY/i.test(String(e?.category ?? "")) && (/HOLIDAY/i.test(String(e?.category)) || /pause|urlaub/i.test(String(e?.name ?? "")))).map((e) => ({ from: String(e.start_date_local ?? "").slice(0, 10), to: String(e.end_date_local ?? e.start_date_local ?? "").slice(0, 10) })),
    }, todayIso);
    seasonBlock = { ...seasonBlock, goals, check: { ...checkBlockText(blockText, tg), testConfig: { ...testCfg, rules: TEST_RULES } } };
  }

  const summary = buildSummary(wellness, todayIso, { sickDays, longWellness });

  const daily = buildDaily(todayIso, activities);
  const weeks = applyEditedPlan(buildWeeks(todayIso, activities, events), null, coach?.week_plan).weeks;
  const baseRuns = buildBaseRuns(blockSessions.runs ?? [], snapshot, blockTargets, seasonBlock, todayIso);
  // Naechster Blocktest (Termin aus dem Block-Eintrag oder geplantes Workout "Blocktest") fuer Heute-Band und Grundlagenlaeufe;
  // ein Termin, zu dem schon ein Blocktest erkannt ist (auch verschoben, ±2 Tage), ist erledigt
  const doneTests = (blockSessions.runs ?? []).filter((r) => r.category === "runBlocktest");
  const nextTest = plannedTests.find((x) => x >= todayIso && !doneTests.some((r) => r.testDate === x || r.date === x)) ?? null;
  // Kraft der letzten vollen Woche (Blockwoche mit gleichem Start) für die Montagsbilanz; Entlastungswochen zählen nicht
  const strengthGoal = seasonBlock?.goals?.find((g) => g.key === "strength");
  const strengthWeek = strengthGoal?.weeks?.find((x) => x.start === addDays(mondayOf(todayIso), -7) && x.load !== false);

  // Montagsbilanz: Fakten zur letzten vollen Woche
  const weekReview = buildWeekReview({
    weeks, wellness, daily, runs,
    sessions: activities.map((a) => ({ date: activityDay(a), sport: sportOf(a), minutes: (num(a?.moving_time) ?? 0) / 60, rpe: num(a?.icu_rpe) })),
    sickDays, hipFlags: insights.hipFlags,
    strength: strengthWeek && strengthGoal.weekGoal ? { count: strengthWeek.count, goal: strengthGoal.weekGoal, unit: strengthGoal.countUnit } : null,
    capRuns: blockTargets?.hrCap != null ? (blockSessions.runs ?? []).filter((r) => r.easy && r.analysis?.overCapMin != null).map((r) => ({ date: r.date, overCapMin: r.analysis.overCapMin })) : null,
  });
  const targetLoad = targetDailyLoad(weeks, todayIso);
  const lastRaw = pickLastActivity(activities, todayIso);
  const lastActivity = buildLastActivity(activities, todayIso, runCtx);
  if (lastActivity && lastRaw) {
    const profile = await loadSessionProfile(env, lastRaw, lastActivity.sport, lastActivityHrAllowed(lastRaw, runCtx));
    if (profile) lastActivity.profile = profile;
  }
  // Letzte Einheit: im Blocktest-Modus kein Decoupling ueber den ganzen Lauf (waechst mit der Laenge), sondern Minute 5–60,
  // stabile Dauer und Minuten ueber dem HF-Deckel aus den Zeitreihen
  if (lastActivity?.sport === "run" && blockTargets?.run?.tests) {
    const ra = (blockSessions.runs ?? []).find((r) => r.id != null && r.id === lastRaw?.id)?.analysis;
    delete lastActivity.decoupling;
    if (ra?.dec60 != null) Object.assign(lastActivity, { dec60: ra.dec60, stableMin: ra.stableMin });
    if (ra?.overCapMin != null) Object.assign(lastActivity, { overCapMin: ra.overCapMin, capHr: ra.capHr });
  }
  // Schwimmeinheiten mit ihren Laps fuer die Wochensummen (Bewegungszeit ohne Pausen, swim-time.js)
  const weekActs = activitiesR.ok ? await withSwimIntervals(env, activities) : activities;
  return {
    generatedAt: new Date().toISOString(),
    today: todayIso,
    sources: {
      intervalsWellness: wellnessR.ok ? { ok: true } : { ok: false, error: wellnessR.error },
      intervalsActivities: activitiesR.ok ? { ok: true } : { ok: false, error: activitiesR.error },
      intervalsEvents: eventsR.ok ? { ok: true } : { ok: false, error: eventsR.error },
      intervalsSportSettings: settingsR.ok ? { ok: true } : { ok: false, error: settingsR.error },
    },
    goal: { ...goal, daysToGo: diffDays(todayIso, goal.date) },
    seasonBlock,
    recentRace: findRecentRace(events, todayIso),
    supportRaces: buildSupportRaces(bRacesR.ok && Array.isArray(bRacesR.value) ? bRacesR.value : events, todayIso, goal.date),
    wellness,
    formSeries,
    formProjection: buildFormProjection(formSeries, formEventsR.ok && Array.isArray(formEventsR.value) ? formEventsR.value : [], todayIso, FORM_AHEAD_DAYS, targetLoad),
    // Grundlage der Prognose: "target" = Wochenziele des Coaches, "plan" = geplante Workouts
    formProjectionSource: targetLoad ? "target" : "plan",
    weeks,
    summary,
    nextTest: nextTest ? { date: nextTest, daysToGo: diffDays(todayIso, nextTest) } : null,
    weekReview,
    baseRuns,
    baseTrend: buildBaseTrend(blockSessions, blockTargets?.bike ?? null),
    daily,
    lastActivity,
    ...insights,
    fitness: buildFitness(runs, snapshot, todayIso),
    intervalSessions: intervalSessions.ok ? intervalSessions.value : [],
    thresholds,
    runalyze: buildRunalyze(snapshot, runalyzeHistory),
    studie: studie ?? null,
    coach: coach ?? null,
    // Experiment und Profil schreibt der Coach (save_experiment, save_profil); das Dashboard zeigt sie nicht mehr, das Widget
    // "experiment" liest sie noch. Die Trainingstage rechnet der Worker live
    experiment: experiment ?? null,
    profil: profil ?? null,
    trainingDays: activitiesR.ok ? computeTrainingDays(activities, todayIso) : null,
    // Wochenziele (week-totals.js): Ausdauerstunden ohne Kraft und Kraftsätze in 2 Wochen, letzte 8 Wochen, je Woche gegen das
    // Ziel, das der Coach für sie gesetzt hat (save_targets); dieselbe Rechnung wie get_coach. targets: geltendes Ziel und Historie
    weekTotals: activitiesR.ok ? { ...applyEditedPlan([], computeWeekTotals(weekActs, todayIso, storedTargets), coach?.week_plan).weekTotals, targets } : null,
    // Aktueller Rahmen des Coaches (save_targets): Wochenziel, Steigerung, Intensität je Sportart, Leitplanken, Nicht erlaubt, Historie.
    // loadHint: Ramp Rate / ACWR nur, wenn auffällig hoch (sonst null)
    targets,
    loadHint: buildLoadHint(wellness, todayIso),
    // Erkenntnisse des Coaches (save_insight, archive_insight): aktive und archivierte, jeweils neueste zuerst
    insights: splitInsights(coachInsights),
    plannedWeek: buildPlannedWeek(events, todayIso),
    planned: buildPlanned(events, todayIso, { paces: stepPaces, zonePct: thresholds.run.paceZonePct, thresholdSecPerKm: thresholds.run.thresholdPaceSecPerKm }),
  };
}

export async function handleDashboardRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  const dashboard = await buildDashboard(env);
  // Tagesziele der Ernährung aus Yazio (best effort, in KV gecacht): fehlt der Zugang oder scheitert die Abfrage, bleibt es null.
  const nutritionGoals = hasYazioCredentials(env) ? await fetchYazioDailyGoals(env, dashboard.today).catch(() => null) : null;
  // Ernährung & Gewicht (ernaehrung.js): Coach-Aussage, Gewichtsverlauf; bei Fehler null, das Dashboard bleibt heil.
  const ernaehrung = await buildErnaehrung(env, dashboard.today, dashboard.wellness).catch(() => null);
  return json({ ...dashboard, nutritionGoals, ernaehrung }, 200, headers);
}
