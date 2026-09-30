import { json } from "./http-helpers.js";
import { diffDays, isoDateBerlin } from "./date-utils.js";
import { activityDay, activityLoad, isRun, isBike, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, fetchIntervalsSportSettings, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { resolveActiveGoalRace } from "./goal-race.js";
import { mustEnv } from "./kv.js";
import { paceTargetsFromVdot } from "./vdot.js";
import { bestForDistance, readRunalyzeSnapshot } from "./runalyze-snapshot.js";

// Read-only Endpunkt für das Trainings-Dashboard (public/dashboard/index.html).
// Die API-Schlüssel bleiben im Worker; der Browser bekommt nur diese aufbereitete JSON.
// Zugriff nur mit DASHBOARD_TOKEN (Header "Authorization: Bearer <token>").

const HISTORY_DAYS = 56;
const PLAN_AHEAD_DAYS = 14;

// Vom Nutzer vorgegeben (Halbmarathon Samstag 03.10.2026, Ziel < 2:00:00). Wird nur
// verwendet, wenn in Intervals.icu kein A-Rennen im Kalender steht.
const CONFIGURED_GOAL = { date: "2026-10-03", name: "Halbmarathon", distanceKm: 21.0975, targetTimeSecs: 7200 };

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

const TEMPO_PATTERN = /\b(tempo|tdl|mit\b|schwelle|wettkampfspezifisch|fartlek|strides?|steigerung)/i;
const LONG_PATTERN = /\b(long\s*slow|long\s*run|lsr|langer?\s+lauf)/i;
const BASE_PATTERN = /\b(grundlagen|easy|ga1|ga 1|regeneration|recovery)/i;

// Bestimmt die Einheitsart, um die Pulsregel durchzusetzen: Puls gibt es nur bei
// "base" und "long". Alles Unklare wird wie eine Intensitätseinheit behandelt.
export function classifyRun(a) {
  const text = `${a?.name ?? ""} ${a?.description ?? ""}`;
  if (isIntervalActivity(a) || hasIntervalTextSignal(a) || TEMPO_PATTERN.test(String(a?.name ?? ""))) return "intensity";
  const distanceKm = (num(a?.distance) ?? 0) / 1000;
  if (LONG_PATTERN.test(text) || distanceKm >= 14) return "long";
  if (BASE_PATTERN.test(text)) return "base";
  return "unknown";
}

function buildRunRecord(a) {
  const distanceM = num(a?.distance) ?? 0;
  const timeSecs = num(a?.moving_time) ?? 0;
  const pace = distanceM > 0 && timeSecs > 0 ? timeSecs / (distanceM / 1000) : null;
  const kind = classifyRun(a);
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

export const SPORTS = ["run", "bike", "swim", "strength", "other"];

export function sportOf(a) {
  const t = String(a?.type ?? "").toLowerCase();
  if (t.includes("swim")) return "swim";
  if (isBike(a)) return "bike";
  if (isRun(a)) return "run";
  if (t.includes("weight") || t.includes("strength") || t.includes("kraft")) return "strength";
  return "other";
}

function emptySports() {
  return Object.fromEntries(SPORTS.map((k) => [k, { count: 0, minutes: 0, km: 0, load: 0, plannedLoad: null }]));
}

function buildWeeks(todayIso, activities, events) {
  const firstMonday = mondayOf(addDays(todayIso, -(HISTORY_DAYS - 1)));
  const weeks = new Map();
  for (let d = firstMonday; d <= todayIso; d = addDays(d, 7)) {
    weeks.set(d, { weekStart: d, km: 0, load: 0, runs: 0, plannedKm: null, plannedLoad: null, bySport: emptySports(), complete: addDays(d, 6) < todayIso });
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
    if (km != null) w.plannedKm = (w.plannedKm ?? 0) + km / 1000;
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
    bySport: Object.fromEntries(Object.entries(w.bySport).map(([k, v]) => [k, { count: v.count, minutes: Math.round(v.minutes), km: r1(v.km), load: Math.round(v.load), plannedLoad: v.plannedLoad != null ? Math.round(v.plannedLoad) : null }])),
  }));
}

// Rad und Schwimmen (Triathlon): Einheitenliste und FTP-Verlauf. Kein Puls: Die Pulsregel gilt
// hier vorsorglich mit, es gehen nur Leistung, Pace, RPE und Feel raus.
function buildTriSessions(activities) {
  return activities
    .filter((a) => ["bike", "swim"].includes(sportOf(a)))
    .map((a) => {
      const sport = sportOf(a);
      const distanceM = num(a?.distance) ?? 0;
      const secs = num(a?.moving_time) ?? 0;
      return {
        date: activityDay(a),
        sport,
        name: a?.name ?? null,
        description: a?.description ?? null,
        distanceKm: Math.round((distanceM / 1000) * 100) / 100,
        movingTimeMin: Math.round(secs / 60),
        load: activityLoad(a) || null,
        avgWatts: sport === "bike" ? num(a?.icu_average_watts) : null,
        normWatts: sport === "bike" ? num(a?.icu_weighted_avg_watts) : null,
        intensity: sport === "bike" ? num(a?.icu_intensity) : null,
        pace100m: sport === "swim" && distanceM > 0 && secs > 0 ? Math.round(secs / (distanceM / 100)) : null,
        rpe: num(a?.icu_rpe),
        feel: num(a?.feel),
      };
    })
    .sort((a, b) => b.date.localeCompare(a.date))
    .slice(0, 12);
}

function findSettings(list, names) {
  return Array.isArray(list) ? list.find((s) => Array.isArray(s?.types) && names.some((n) => s.types.includes(n))) ?? null : null;
}

function buildThresholds(settingsList, activities) {
  const ride = findSettings(settingsList, ["Ride", "VirtualRide"]);
  const swim = findSettings(settingsList, ["Swim", "OpenWaterSwim"]);
  const swimMs = num(swim?.threshold_pace);
  const trend = activities
    .filter((a) => sportOf(a) === "bike" && num(a?.icu_ftp) != null)
    .map((a) => ({ date: activityDay(a), ftp: num(a.icu_ftp) }))
    .sort((a, b) => a.date.localeCompare(b.date));
  return {
    ftp: num(ride?.ftp),
    indoorFtp: num(ride?.indoor_ftp),
    ftpTrend: trend,
    // threshold_pace ist bei Intervals.icu in m/s hinterlegt (Feldname ungeprüft, sonst null).
    swimThresholdPace100m: swimMs && swimMs > 0 ? Math.round(100 / swimMs) : null,
  };
}

function buildPlanned(events, todayIso) {
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
    }))
    .filter((e) => e.date >= todayIso)
    .sort((a, b) => a.date.localeCompare(b.date));
}

function buildWellness(list) {
  return list
    .map((w) => ({
      date: String(w?.id ?? w?.date ?? "").slice(0, 10),
      ctl: num(w?.ctl),
      atl: num(w?.atl),
      rampRate: num(w?.rampRate),
      restingHR: scaleValue(w?.restingHR),
      sleepQuality: scaleValue(w?.sleepQuality),
      soreness: scaleValue(w?.soreness),
      fatigue: scaleValue(w?.fatigue),
      mood: scaleValue(w?.mood),
      motivation: scaleValue(w?.motivation),
      comments: w?.comments ? String(w.comments) : null,
    }))
    .filter((w) => /^\d{4}-\d{2}-\d{2}$/.test(w.date))
    .sort((a, b) => a.date.localeCompare(b.date));
}

// Fitness-Daten (Bereich 3). Puls nur aus Grundlagen- und Long-Slow-Läufen (Pulsregel).
function buildFitness(runs) {
  const asc = [...runs].sort((a, b) => a.date.localeCompare(b.date));
  return {
    baseRuns: asc.filter((r) => (r.kind === "base" || r.kind === "long") && r.avgHr != null && r.paceSecPerKm != null)
      .map((r) => ({ date: r.date, name: r.name, kind: r.kind, avgHr: r.avgHr, paceSecPerKm: r.paceSecPerKm, pace: r.pace })),
    longRuns: asc.filter((r) => r.kind === "long" && r.decoupling != null)
      .map((r) => ({ date: r.date, name: r.name, distanceKm: r.distanceKm, decoupling: r.decoupling })),
  };
}

function buildRunalyze(snapshot) {
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
  return { fetchedAt: snapshot.fetchedAt, vdot, paces, rows };
}

async function settle(label, fn) {
  try {
    return { label, ok: true, value: await fn() };
  } catch (e) {
    return { label, ok: false, error: String(e?.message ?? e).slice(0, 300) };
  }
}

export async function buildDashboard(env, todayIso = isoDateBerlin()) {
  mustEnv(env, "ATHLETE_ID");
  mustEnv(env, "INTERVALS_API_KEY");
  const oldest = addDays(todayIso, -(HISTORY_DAYS - 1));
  const newestEvents = addDays(todayIso, PLAN_AHEAD_DAYS);

  const [wellnessR, activitiesR, eventsR, goalR, settingsR, snapshot] = await Promise.all([
    settle("wellness", () => fetchIntervalsWellnessRange(env, oldest, todayIso)),
    settle("activities", () => fetchIntervalsActivities(env, oldest, todayIso)),
    settle("events", () => fetchIntervalsEvents(env, oldest, newestEvents)),
    settle("goal", () => resolveActiveGoalRace(env, todayIso)),
    settle("sportSettings", async () => {
      const list = await fetchIntervalsSportSettings(env);
      if (!list) throw new Error("sport-settings nicht abrufbar");
      return list;
    }),
    readRunalyzeSnapshot(env),
  ]);

  const activities = activitiesR.ok && Array.isArray(activitiesR.value) ? activitiesR.value : [];
  const events = eventsR.ok && Array.isArray(eventsR.value) ? eventsR.value : [];
  const wellness = wellnessR.ok ? buildWellness(wellnessR.value) : [];
  const runs = activities.filter(isRun).map(buildRunRecord).sort((a, b) => b.date.localeCompare(a.date));

  const goalFromCalendar = goalR.ok && goalR.value?.date ? goalR.value : null;
  const goal = goalFromCalendar
    ? { date: goalFromCalendar.date, name: "Halbmarathon", targetTimeSecs: goalFromCalendar.targetTimeSecs ?? CONFIGURED_GOAL.targetTimeSecs, source: "intervals" }
    : { ...CONFIGURED_GOAL, source: "config" };

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
    wellness,
    weeks: buildWeeks(todayIso, activities, events),
    runs: runs.slice(0, 20),
    fitness: buildFitness(runs),
    triathlon: { thresholds: buildThresholds(settingsR.ok ? settingsR.value : null, activities), sessions: buildTriSessions(activities) },
    runalyze: buildRunalyze(snapshot),
    planned: buildPlanned(events, todayIso),
  };
}

export async function handleDashboardRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  return json(await buildDashboard(env), 200, headers);
}
