import { json } from "./http-helpers.js";
import { hasYazioCredentials, fetchYazioDailyGoals } from "./yazio-client.js";
import { diffDays, isoDateBerlin } from "./date-utils.js";
import { activityDay, activityLoad, isRun, isBike, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, fetchIntervalsSportSettings, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { resolveActiveGoalRace, DISTANCE_LABELS, DISTANCE_KM } from "./goal-race.js";
import { isARaceEvent, isBRaceEvent } from "./event-utils.js";
import { getEventDistanceFromEvent, parseTriathlonEvent } from "./block-phase.js";
import { resolveSeasonBlock, buildBlockGoals } from "./season-block.js";
import { readEgymStrengthDates } from "./egym-widget.js";
import { buildTriathlonTargets } from "./triathlon-targets.js";
import { mustEnv } from "./kv.js";
import { computeVdotFromRaceTime, paceTargetsFromVdot, predictRaceTimesFromVdot } from "./vdot.js";
import { findHipFlags, parseCravings, parseWorkoutSteps } from "./dashboard-parse.js";
import { readStudie } from "./studie-snapshot.js";
import { buildSummary } from "./dashboard-summary.js";
import { withActualToday } from "./live-load.js";
import { bestForDistance, readRunalyzeSnapshot, runKindFromType } from "./runalyze-snapshot.js";
import { readRunalyzeHistory, historyEntryFromSnapshot, upsertHistory } from "./runalyze-history.js";

// Read-only Endpunkt für das Trainings-Dashboard (public/dashboard/index.html).
// Die API-Schlüssel bleiben im Worker; der Browser bekommt nur diese aufbereitete JSON.
// Zugriff nur mit DASHBOARD_TOKEN (Header "Authorization: Bearer <token>").

const HISTORY_DAYS = 56;
const PLAN_AHEAD_DAYS = 14;

// Vom Nutzer vorgegeben (Halbmarathon Samstag 03.10.2026, Ziel < 2:00:00). Wird nur
// verwendet, wenn in Intervals.icu kein A-Rennen im Kalender steht.
const CONFIGURED_GOAL = { date: "2026-10-03", name: "Halbmarathon", distance: "hm", distanceKm: 21.0975, targetTimeSecs: 7200 };

function timingSafeEqual(a, b) {
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
    decoupling: showHr ? num(a?.decoupling) : null,
    compliance: num(a?.compliance),
  };
}

const SPORTS = ["run", "bike", "swim", "strength", "other"];

export function sportOf(a) {
  const t = String(a?.type ?? "").toLowerCase();
  if (t.includes("swim")) return "swim";
  if (isBike(a)) return "bike";
  if (isRun(a)) return "run";
  if (t.includes("weight") || t.includes("strength") || t.includes("kraft")) return "strength";
  return "other";
}

function emptySports() {
  return Object.fromEntries(SPORTS.map((k) => [k, { count: 0, minutes: 0, km: 0, load: 0, plannedLoad: null, plannedKm: null }]));
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

function buildWeeks(todayIso, activities, events) {
  const firstMonday = mondayOf(addDays(todayIso, -(HISTORY_DAYS - 1)));
  const weeks = new Map();
  for (let d = firstMonday; d <= todayIso; d = addDays(d, 7)) {
    weeks.set(d, { weekStart: d, km: 0, load: 0, runs: 0, plannedKm: null, plannedLoad: null, bySport: emptySports(), intensity: { easy: 0, mid: 0, hard: 0 }, complete: addDays(d, 6) < todayIso });
  }
  for (const a of activities) {
    const w = weeks.get(mondayOf(activityDay(a)));
    if (!w) continue;
    const load = activityLoad(a);
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
      w.runs += 1;
    }
  }
  for (const e of events) {
    const day = String(e?.start_date_local || e?.start_date || "").slice(0, 10);
    const w = weeks.get(mondayOf(day));
    if (!w || String(e?.category ?? "").toUpperCase() !== "WORKOUT") continue;
    const km = num(e?.distance_target ?? e?.distance);
    const load = num(e?.icu_training_load ?? e?.load_target);
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
    load: Math.round(w.load),
    plannedKm: w.plannedKm != null ? r1(w.plannedKm) : null,
    plannedLoad: w.plannedLoad != null ? Math.round(w.plannedLoad) : null,
    intensity: { easy: Math.round(w.intensity.easy), mid: Math.round(w.intensity.mid), hard: Math.round(w.intensity.hard) },
    bySport: Object.fromEntries(Object.entries(w.bySport).map(([k, v]) => [k, { count: v.count, minutes: Math.round(v.minutes), km: r1(v.km), load: Math.round(v.load), plannedLoad: v.plannedLoad != null ? Math.round(v.plannedLoad) : null, plannedKm: v.plannedKm != null ? r1(v.plannedKm) : null }])),
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

function positive(v) {
  const n = num(v);
  return n != null && n > 0 ? Math.round(n * 10) / 10 : null;
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
      hrv: num(w?.hrv) != null && num(w.hrv) > 0 ? num(w.hrv) : null,
      weight: positive(w?.weight),
      sleepQuality: scaleValue(w?.sleepQuality),
      soreness: scaleValue(w?.soreness),
      fatigue: scaleValue(w?.fatigue),
      mood: scaleValue(w?.mood),
      motivation: scaleValue(w?.motivation),
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
function buildDaily(todayIso, activities) {
  const out = [];
  for (let d = addDays(todayIso, -(HISTORY_DAYS - 1)); d <= todayIso; d = addDays(d, 1)) out.push({ date: d, load: 0, sports: {} });
  const byDate = new Map(out.map((x) => [x.date, x]));
  for (const a of activities) {
    const day = byDate.get(activityDay(a));
    if (!day) continue;
    const load = activityLoad(a);
    day.load += load;
    if (load) day.sports[sportOf(a)] = (day.sports[sportOf(a)] ?? 0) + load;
  }
  return out.map((x) => ({ ...x, load: Math.round(x.load), sports: Object.fromEntries(Object.entries(x.sports).map(([k, v]) => [k, Math.round(v)])) }));
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
  // Ein langer Lauf zaehlt nur, wenn er in Runalyze als "Langer Lauf" eingetragen ist (Art setzt der Nutzer selbst,
  // Runalyze liefert das Decoupling). Nur Datum, Distanz, Pace, Decoupling gehen raus.
  const since = addDays(todayIso, -(HISTORY_DAYS - 1));
  const rz = (snapshot?.runs ?? []).filter((r) => r.date >= since && runKindFromType(r.type) === "long");
  const rzRecords = rz.map((r) => {
    const pace = r.durationSec ? r.durationSec / r.distanceKm : null;
    return { date: r.date, distanceKm: Math.round(r.distanceKm * 10) / 10, paceSecPerKm: pace != null ? Math.round(pace) : null, pace: pace != null ? formatPace(pace) : null, decoupling: r.decouplingPct };
  });
  const byLength = [...rzRecords].sort((a, b) => b.distanceKm - a.distanceKm || b.date.localeCompare(a.date));
  return {
    longRunTracker: {
      source: "runalyze",
      minKm: null,
      longest: byLength[0] ?? null,
      count16: rzRecords.filter((r) => r.distanceKm >= 16).length,
      recent: byLength.slice(0, 6).sort((a, b) => b.date.localeCompare(a.date)),
    },
    longRuns: [...rzRecords].filter((r) => r.decoupling != null).sort((a, b) => a.date.localeCompare(b.date)).map((r) => ({ date: r.date, distanceKm: r.distanceKm, decoupling: r.decoupling })),
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
function buildRaceEstimates(vdot, rows) {
  const best10 = rows.find((r) => r.label === "10 km");
  const vdot10 = best10?.bestSeconds != null ? computeVdotFromRaceTime(best10.bestDistanceKm * 1000, best10.bestSeconds) : null;
  const sources = [vdot != null ? { key: "vdot", label: `aus VDOT ${Math.round(vdot * 10) / 10}`, vdot } : null, vdot10 != null ? { key: "best-10", label: "aus 10-km-Bestzeit", vdot: vdot10 } : null].filter(Boolean);
  const dists = [{ key: "5k", km: 5 }, { key: "10k", km: 10 }, { key: "hm", km: 21.0975 }, { key: "m", km: 42.195 }];
  return dists.map((d) => {
    const estimates = sources.map((s) => ({ key: s.key, label: s.label, seconds: predictRaceTimesFromVdot(s.vdot)?.find((x) => x.key === d.key)?.seconds ?? null, kind: "calc" })).filter((e) => e.seconds);
    return { key: d.key, km: d.km, label: DISTANCE_LABELS[d.key] ?? d.key, estimates };
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
  return { fetchedAt: snapshot.fetchedAt, vdot, paces, rows, hmEstimates: buildHmEstimates(snapshot, vdot, rows), raceEstimates: buildRaceEstimates(vdot, rows), hmHistory: upsertHistory(history, historyEntryFromSnapshot(snapshot)) };
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

  const [wellnessR, activitiesR, eventsR, bRacesR, goalR, settingsR, snapshot, runalyzeHistory, studie] = await Promise.all([
    settle("wellness", () => fetchIntervalsWellnessRange(env, oldest, todayIso)),
    settle("activities", () => fetchIntervalsActivities(env, oldest, todayIso)),
    settle("events", () => fetchIntervalsEvents(env, oldest, newestEvents)),
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
  ]);

  const activities = activitiesR.ok && Array.isArray(activitiesR.value) ? activitiesR.value : [];
  const events = eventsR.ok && Array.isArray(eventsR.value) ? eventsR.value : [];
  const wellness = wellnessR.ok ? withActualToday(buildWellness(wellnessR.value), activities, todayIso) : [];
  const runCtx = { raceDays: buildRaceDays(events), rzRuns: snapshot?.runs ?? [] };
  const runs = activities.filter(isRun).map((a) => buildRunRecord(a, runCtx)).sort((a, b) => b.date.localeCompare(a.date));

  // Pace je Zone aus dem VDOT (Runalyze) fuer die Umrechnung von Distanz-Schritten geplanter Workouts
  const stepPaces = snapshot?.vdot ? Object.fromEntries((paceTargetsFromVdot(snapshot.vdot) ?? []).map((z) => [z.key, z.secPerKm])) : null;
  const goalFromCalendar = goalR.ok && goalR.value?.date ? goalR.value : null;
  const thresholds = buildThresholds(settingsR.ok ? settingsR.value : null);
  const tri = goalFromCalendar?.triathlon ?? null;
  // Triathlon: Gesamtzeit aus dem Eintrag, die Lauf-Ziele der Grafiken nur aus einer Lauf-Zeit in der Beschreibung ("Lauf 1:55:00").
  const goal = tri
    ? { date: goalFromCalendar.date, name: `Triathlon ${tri.label}`, targetTimeSecs: tri.runTargetSecs, totalTargetSecs: goalFromCalendar.targetTimeSecs ?? null, runKm: tri.runKm, triathlon: { format: tri.format, swimKm: tri.swimKm, bikeKm: tri.bikeKm, runKm: tri.runKm, targets: buildTriathlonTargets(tri, thresholds, goalFromCalendar.targetTimeSecs ?? null, { vdot: snapshot?.vdot ?? null, weightKg: [...wellness].reverse().find((w) => w.weight)?.weight ?? null }) }, source: "intervals" }
    : goalFromCalendar
      ? { date: goalFromCalendar.date, name: DISTANCE_LABELS[goalFromCalendar.distance] ?? "Rennen", distance: goalFromCalendar.distance ?? null, targetTimeSecs: goalFromCalendar.targetTimeSecs ?? (goalFromCalendar.distance === "hm" ? CONFIGURED_GOAL.targetTimeSecs : null), runKm: DISTANCE_KM[goalFromCalendar.distance] ?? null, source: "intervals" }
      : { ...CONFIGURED_GOAL, runKm: CONFIGURED_GOAL.distanceKm, source: "config" };

  // Trainingsblock aus dem Kalender samt Fortschritt der Ziele. Laeuft der Block schon laenger als die 56 Tage Historie,
  // werden Aktivitaeten und Wellness ab Blockstart nachgeladen (best effort, sonst zaehlt nur das Vorhandene).
  let seasonBlock = resolveSeasonBlock([...(bRacesR.ok && Array.isArray(bRacesR.value) ? bRacesR.value : []), ...events], todayIso);
  if (seasonBlock) {
    let blockActs = activities, blockWell = wellnessR.ok && Array.isArray(wellnessR.value) ? wellnessR.value : [];
    if (seasonBlock.status === "active" && seasonBlock.start < oldest) {
      const till = addDays(oldest, -1);
      const [xa, xw] = await Promise.all([settle("blockActivities", () => fetchIntervalsActivities(env, seasonBlock.start, till)), settle("blockWellness", () => fetchIntervalsWellnessRange(env, seasonBlock.start, till))]);
      if (xa.ok && Array.isArray(xa.value)) blockActs = [...xa.value, ...activities];
      if (xw.ok && Array.isArray(xw.value)) blockWell = [...xw.value, ...blockWell];
    }
    const blockRuns = blockActs === activities ? runs : blockActs.filter(isRun).map((a) => buildRunRecord(a, runCtx));
    // Lange Laeufe wie im Longrun-Tracker: Art "Langer Lauf" und Decoupling aus Runalyze; Intervals nur als Ergaenzung fuer Tage ohne Runalyze-Wert.
    const rzLong = (snapshot?.runs ?? []).filter((r) => runKindFromType(r.type) === "long" && Number.isFinite(r.decouplingPct)).map((r) => ({ date: r.date, decoupling: r.decouplingPct }));
    const rzDays = new Set(rzLong.map((r) => r.date));
    const icuLong = blockRuns.filter((r) => r.kind === "long" && r.decoupling != null && !rzDays.has(r.date)).map((r) => ({ date: r.date, decoupling: r.decoupling }));
    // Kraft-Tag = Tag mit EGYM-Training oder einer Kraft-Einheit aus Intervals von mind. 20 min (Mobility, Dehnen, Yoga zaehlen nicht)
    const egymDays = await readEgymStrengthDates(env).catch(() => null);
    const strengthDays = [...new Set([
      ...(egymDays?.dates ?? []),
      ...blockActs.filter((a) => sportOf(a) === "strength" && (num(a?.moving_time) ?? 0) >= 20 * 60 && !/mobil|stretch|dehn|yoga/i.test(String(a?.name ?? ""))).map((a) => activityDay(a)),
    ])].map((date) => ({ date }));
    const goals = buildBlockGoals(seasonBlock, {
      longRuns: [...rzLong, ...icuLong],
      strength: strengthDays,
      strengthSource: { egym: Boolean(egymDays), egymAt: egymDays?.at ?? null },
      acwrDays: blockWell.map((x) => ({ date: String(x?.id ?? x?.date ?? "").slice(0, 10), acwr: num(x?.ctl) > 0 ? num(x?.atl) / num(x.ctl) : null })),
    }, todayIso);
    seasonBlock = { ...seasonBlock, goals };
  }

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
    weeks: buildWeeks(todayIso, activities, events),
    summary: buildSummary(wellness, todayIso),
    daily: buildDaily(todayIso, activities),
    ...buildInsights(wellnessR.ok && Array.isArray(wellnessR.value) ? wellnessR.value : [], activities),
    fitness: buildFitness(runs, snapshot, todayIso),
    thresholds,
    runalyze: buildRunalyze(snapshot, runalyzeHistory),
    studie: studie ?? null,
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
  return json({ ...dashboard, nutritionGoals }, 200, headers);
}
