import { json } from "./http-helpers.js";
import { hasYazioCredentials, fetchYazioDailyGoals } from "./yazio-client.js";
import { diffDays, isoDateBerlin } from "./date-utils.js";
import { activityDay, activityLoad, isRun, isBike, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivities, fetchIntervalsActivityStreams, fetchIntervalsEvents, fetchIntervalsSportSettings, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { resolveActiveGoalRace } from "./goal-race.js";
import { buildTriathlonTargets } from "./triathlon-targets.js";
import { mustEnv, readKvJson, writeKvJson } from "./kv.js";
import { computeVdotFromRaceTime, paceTargetsFromVdot, predictRaceTimesFromVdot } from "./vdot.js";
import { findHipFlags, parseCravings, parseWorkoutSteps } from "./dashboard-parse.js";
import { readStudie } from "./studie-snapshot.js";
import { buildSummary } from "./dashboard-summary.js";
import { bestForDistance, readRunalyzeSnapshot } from "./runalyze-snapshot.js";
import { readRunalyzeHistory, historyEntryFromSnapshot, upsertHistory } from "./runalyze-history.js";

// Read-only Endpunkt für das Trainings-Dashboard (public/dashboard/index.html).
// Die API-Schlüssel bleiben im Worker; der Browser bekommt nur diese aufbereitete JSON.
// Zugriff nur mit DASHBOARD_TOKEN (Header "Authorization: Bearer <token>").

const HISTORY_DAYS = 56;
const PLAN_AHEAD_DAYS = 14;
const LONGRUN_MIN_KM = 12;

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

export function buildRunRecord(a) {
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
  return Object.fromEntries(SPORTS.map((k) => [k, { count: 0, minutes: 0, km: 0, load: 0, plannedLoad: null, plannedKm: null }]));
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
    run: { thresholdPaceSecPerKm: secPer(num(run?.threshold_pace), 1000), lthr: num(run?.lthr), maxHr: num(run?.max_hr) },
    bike: { ftp: num(ride?.ftp), indoorFtp: num(ride?.indoor_ftp), lthr: num(ride?.lthr), maxHr: num(ride?.max_hr) },
    swim: { thresholdPaceSecPer100m: secPer(num(swim?.threshold_pace), 100) },
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
      sport: sportOf(e),
      tags: Array.isArray(e?.tags) ? e.tags.map(String) : [],
      steps: parseWorkoutSteps(e?.description, e?.workout_doc),
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

// Decoupling (Pa:HR) aus den Rohdaten: Effizienz = Geschwindigkeit / Puls, erste gegen zweite Hälfte der
// bewegten Zeit. Positiv = Puls driftet gegen Pace. Intervals.icu liefert das Feld oft nicht (null).
export function computeDecoupling(hr, speed) {
  if (!Array.isArray(hr) || !Array.isArray(speed)) return null;
  const n = Math.min(hr.length, speed.length), pts = [];
  for (let i = 0; i < n; i++) if (hr[i] > 60 && speed[i] > 0.5) pts.push([hr[i], speed[i]]);
  if (pts.length < 600) return null; // unter ca. 10 min bewegter Zeit nicht aussagekräftig
  const eff = (a) => a.reduce((s, p) => s + p[1], 0) / a.reduce((s, p) => s + p[0], 0);
  const half = pts.length >> 1, e1 = eff(pts.slice(0, half)), e2 = eff(pts.slice(half));
  return Math.round(((e1 - e2) / e1) * 1000) / 10;
}

// Fehlendes Decoupling langer Grundlagen-/Long-Läufe selbst rechnen (Pulsregel: nur diese Arten) und je Aktivität
// im KV merken, damit die Streams nur einmal geholt werden. Best effort: bei Fehlern bleibt der Wert null.
async function fillDecoupling(env, runs) {
  const todo = runs.filter((r) => r.id != null && r.decoupling == null && (r.kind === "long" || r.kind === "base") && r.distanceKm >= LONGRUN_MIN_KM).slice(0, 8);
  await Promise.all(todo.map(async (r) => {
    const key = `dashboard:decoupling:${r.id}`;
    try {
      const cached = await readKvJson(env, key);
      if (cached && "value" in cached) { r.decoupling = cached.value; return; }
      const st = await fetchIntervalsActivityStreams(env, r.id, ["heartrate", "velocity_smooth"]);
      if (!st) return;
      const value = computeDecoupling(st.heartrate, st.velocity_smooth);
      r.decoupling = value;
      await writeKvJson(env, key, { value });
    } catch { /* bleibt null */ }
  }));
}

// Fitness-Daten (Bereich 3). Puls nur aus Grundlagen- und Long-Slow-Läufen (Pulsregel).
function buildFitness(runs) {
  const asc = [...runs].sort((a, b) => a.date.localeCompare(b.date));
  // Longrun-Tracker: Für die Langstrecke zählen die längsten Läufe der 8 Wochen, nicht der Wochenumfang.
  // Nur Datum, Distanz und Pace gehen raus (keine Namen, kein Puls).
  const long = runs.filter((r) => r.distanceKm >= LONGRUN_MIN_KM).sort((a, b) => b.distanceKm - a.distanceKm || b.date.localeCompare(a.date));
  const pick = (r) => ({ date: r.date, distanceKm: r.distanceKm, pace: r.pace, paceSecPerKm: r.paceSecPerKm, decoupling: r.decoupling });
  return {
    longRunTracker: { minKm: LONGRUN_MIN_KM, longest: long[0] ? pick(long[0]) : null, count16: runs.filter((r) => r.distanceKm >= 16).length, recent: long.slice(0, 6).sort((a, b) => b.date.localeCompare(a.date)).map(pick) },
    longRuns: asc.filter((r) => r.kind === "long" && r.decoupling != null)
      .map((r) => ({ date: r.date, distanceKm: r.distanceKm, decoupling: r.decoupling })),
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
  return { fetchedAt: snapshot.fetchedAt, vdot, paces, rows, hmEstimates: buildHmEstimates(snapshot, vdot, rows), hmHistory: upsertHistory(history, historyEntryFromSnapshot(snapshot)) };
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

  const [wellnessR, activitiesR, eventsR, goalR, settingsR, snapshot, runalyzeHistory, studie] = await Promise.all([
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
    readRunalyzeHistory(env),
    readStudie(env),
  ]);

  const activities = activitiesR.ok && Array.isArray(activitiesR.value) ? activitiesR.value : [];
  const events = eventsR.ok && Array.isArray(eventsR.value) ? eventsR.value : [];
  const wellness = wellnessR.ok ? buildWellness(wellnessR.value) : [];
  const runs = activities.filter(isRun).map(buildRunRecord).sort((a, b) => b.date.localeCompare(a.date));
  await fillDecoupling(env, runs);

  const goalFromCalendar = goalR.ok && goalR.value?.date ? goalR.value : null;
  const thresholds = buildThresholds(settingsR.ok ? settingsR.value : null);
  const tri = goalFromCalendar?.triathlon ?? null;
  // Triathlon: Gesamtzeit aus dem Eintrag, die Lauf-Ziele der Grafiken nur aus einer Lauf-Zeit in der Beschreibung ("Lauf 1:55:00").
  const goal = tri
    ? { date: goalFromCalendar.date, name: `Triathlon ${tri.label}`, targetTimeSecs: tri.runTargetSecs, totalTargetSecs: goalFromCalendar.targetTimeSecs ?? null, runKm: tri.runKm, triathlon: { swimKm: tri.swimKm, bikeKm: tri.bikeKm, runKm: tri.runKm, targets: buildTriathlonTargets(tri, thresholds, goalFromCalendar.targetTimeSecs ?? null) }, source: "intervals" }
    : goalFromCalendar
      ? { date: goalFromCalendar.date, name: "Halbmarathon", targetTimeSecs: goalFromCalendar.targetTimeSecs ?? CONFIGURED_GOAL.targetTimeSecs, runKm: CONFIGURED_GOAL.distanceKm, source: "intervals" }
      : { ...CONFIGURED_GOAL, runKm: CONFIGURED_GOAL.distanceKm, source: "config" };

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
    summary: buildSummary(wellness, todayIso),
    daily: buildDaily(todayIso, activities),
    ...buildInsights(wellnessR.ok && Array.isArray(wellnessR.value) ? wellnessR.value : [], activities),
    fitness: buildFitness(runs),
    thresholds,
    runalyze: buildRunalyze(snapshot, runalyzeHistory),
    studie: studie ?? null,
    planned: buildPlanned(events, todayIso),
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
