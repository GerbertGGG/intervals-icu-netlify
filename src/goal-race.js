import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";
import { isIsoDate, daysBetween, weeksBetween, isoDate } from "./date-utils.js";
import { normalizeEventDistance, getBlockLengthsWeeks, getEventDistanceFromEvent } from "./block-phase.js";
import { predictRaceTimesFromVdot } from "./vdot.js";
import { fetchIntervalsEvents } from "./intervals-client.js";
import { isARaceEvent } from "./event-utils.js";

const GOAL_RACE_KV_PREFIX = "goal:race:";

const DISTANCE_LABELS = { "5k": "5 km", "10k": "10 km", hm: "Halbmarathon", m: "Marathon" };

function goalRaceKvKey(env) {
  return `${GOAL_RACE_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}

export async function readGoalRace(env) {
  if (!hasKv(env)) return null;
  const raw = await readKvJson(env, goalRaceKvKey(env)).catch(() => null);
  if (!raw || !isIsoDate(raw.date)) return null;
  return raw;
}

export async function writeGoalRace(env, goal) {
  await writeKvJson(env, goalRaceKvKey(env), goal);
}

export async function deleteGoalRace(env) {
  if (!hasKv(env)) return;
  await env.KV.delete(goalRaceKvKey(env));
}

const GOAL_EVENT_LOOKAHEAD_DAYS = 365;

function eventDay(event) {
  return String(event?.start_date_local || event?.start_date || "").slice(0, 10);
}

// intervals.icu Events don't have a confirmed dedicated "target/goal time" field in
// the OpenAPI spec available in this environment (unreachable at implementation
// time) - moving_time is the best-effort candidate, mirroring the same fallback-chain
// pattern already used elsewhere in this codebase (see form-analysis.js) for fields
// whose exact naming can't be confirmed against the live API.
function extractEventTargetTimeSecs(event) {
  const secs = Number(event?.moving_time ?? event?.icu_target_time ?? event?.target_time_secs ?? NaN);
  return Number.isFinite(secs) && secs > 0 ? secs : null;
}

// Auto-derives the goal race straight from the athlete's own intervals.icu calendar:
// the nearest upcoming event marked as an A-race (same isARaceEvent detection the
// block-phase state machine already relies on in sync.js). Marking a race "A" in
// intervals.icu is then enough on its own - no manual PUT /goal call needed.
export function deriveAutoGoalFromRaces(races, todayIso) {
  const upcoming = (races || [])
    .map((e) => ({ e, day: eventDay(e) }))
    .filter((x) => isIsoDate(x.day) && x.day >= todayIso)
    .sort((a, b) => a.day.localeCompare(b.day));
  if (!upcoming.length) return null;

  const event = upcoming[0].e;
  const distance = getEventDistanceFromEvent(event);
  if (!distance) return null;

  const targetTimeSecs = extractEventTargetTimeSecs(event);
  return {
    date: upcoming[0].day,
    distance,
    targetTime: targetTimeSecs ? formatTime(targetTimeSecs) : null,
    targetTimeSecs,
    source: "auto",
  };
}

async function fetchUpcomingARaceEvents(env, todayIso) {
  const newest = isoDate(new Date(new Date(todayIso + "T00:00:00Z").getTime() + GOAL_EVENT_LOOKAHEAD_DAYS * 86400000));
  const events = await fetchIntervalsEvents(env, todayIso, newest);
  const list = Array.isArray(events) ? events : Array.isArray(events?.events) ? events.events : [];
  return list.filter((e) => isARaceEvent(e));
}

// The single entry point every goalInfo/long-run-plan consumer should use: prefers
// the auto-derived A-race calendar event, falling back to a manually set /goal only
// when no A-race event exists (e.g. the athlete hasn't added one to intervals.icu yet).
export async function resolveActiveGoalRace(env, todayIso) {
  const races = await fetchUpcomingARaceEvents(env, todayIso).catch(() => []);
  const auto = deriveAutoGoalFromRaces(races, todayIso);
  if (auto) return auto;
  return readGoalRace(env).catch(() => null);
}

function parseTargetTime(s) {
  if (!s) return null;
  const parts = String(s).split(":").map(Number);
  if (parts.some((p) => !Number.isFinite(p) || p < 0)) return null;
  if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
  if (parts.length === 2) return parts[0] * 60 + parts[1];
  if (parts.length === 1 && parts[0] > 0) return parts[0];
  return null;
}

function formatTime(secs) {
  if (!Number.isFinite(secs) || secs <= 0) return null;
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.round(secs % 60);
  if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  return `${m}:${String(s).padStart(2, "0")}`;
}

function addWeeksBefore(dateIso, weeks) {
  return isoDate(new Date(new Date(dateIso + "T00:00:00Z").getTime() - weeks * 7 * 86400000));
}

export function buildGoalRacePayload({ date, distance, targetTime }) {
  if (!isIsoDate(date)) return { ok: false, error: "Ungültiges Datum (YYYY-MM-DD)" };
  const distNorm = normalizeEventDistance(distance);
  if (!distNorm) return { ok: false, error: "Unbekannte Distanz (5k, 10k, hm, m)" };
  const targetTimeSecs = parseTargetTime(targetTime) ?? null;
  return {
    ok: true,
    goal: {
      date,
      distance: distNorm,
      targetTime: targetTimeSecs ? formatTime(targetTimeSecs) : null,
      targetTimeSecs,
    },
  };
}

export function computeGoalRaceInfo(goal, todayIso, currentVdot) {
  if (!goal || !isIsoDate(goal.date)) return null;
  const weeksToRace = weeksBetween(todayIso, goal.date);
  const daysToRace = Math.round(daysBetween(todayIso, goal.date));

  const lengths = getBlockLengthsWeeks(goal.distance);
  const raceBlockWeeks = (lengths.race || 0) + (lengths.taper || 0);
  const totalPlanWeeks = (lengths.base || 0) + (lengths.build || 0) + raceBlockWeeks;

  const schedule = {
    planStart: addWeeksBefore(goal.date, totalPlanWeeks),
    buildStart: addWeeksBefore(goal.date, (lengths.build || 0) + raceBlockWeeks),
    raceStart: addWeeksBefore(goal.date, raceBlockWeeks),
    raceDate: goal.date,
  };

  let recommendedBlock = null;
  if (daysToRace < 0) {
    recommendedBlock = "RESET";
  } else if (todayIso >= schedule.raceStart) {
    recommendedBlock = "RACE";
  } else if (todayIso >= schedule.buildStart) {
    recommendedBlock = "BUILD";
  } else if (todayIso >= schedule.planStart) {
    recommendedBlock = "BASE";
  } else {
    recommendedBlock = null; // Freie Vorphase
  }

  let prediction = null;
  if (Number.isFinite(currentVdot) && currentVdot > 0) {
    const racePredictions = predictRaceTimesFromVdot(currentVdot);
    const pred = racePredictions?.find((r) => r.key === goal.distance);
    if (pred) {
      const gapSecs =
        Number.isFinite(goal.targetTimeSecs) && Number.isFinite(pred.seconds)
          ? pred.seconds - goal.targetTimeSecs
          : null;
      prediction = {
        predictedTime: pred.time,
        predictedSecs: pred.seconds,
        targetTime: goal.targetTime,
        targetSecs: goal.targetTimeSecs,
        gapSecs,
        gapFormatted: gapSecs != null ? formatTime(Math.abs(gapSecs)) : null,
        faster: gapSecs != null ? gapSecs < 0 : null,
      };
    }
  }

  return {
    goal,
    distanceLabel: DISTANCE_LABELS[goal.distance] ?? goal.distance,
    weeksToRace: Math.round(weeksToRace * 10) / 10,
    daysToRace,
    isPast: daysToRace < 0,
    schedule,
    recommendedBlock,
    prediction,
  };
}
