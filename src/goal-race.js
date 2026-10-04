import { isIsoDate, isoDate } from "./date-utils.js";
import { getEventDistanceFromEvent, parseTriathlonEvent } from "./block-phase.js";
import { fetchIntervalsEvents } from "./intervals-client.js";
import { isARaceEvent } from "./event-utils.js";

export const DISTANCE_LABELS = { "5k": "5 km", "10k": "10 km", hm: "Halbmarathon", m: "Marathon" };
export const DISTANCE_KM = { "5k": 5, "10k": 10, hm: 21.0975, m: 42.195 };

const GOAL_EVENT_LOOKAHEAD_DAYS = 400;

function eventDay(event) {
  return String(event?.start_date_local || event?.start_date || "").slice(0, 10);
}

// Intervals.icu liefert die Zielzeit eines Rennens in Sekunden als time_target.
function extractEventTargetTimeSecs(event) {
  const secs = Number(event?.time_target ?? event?.moving_time ?? NaN);
  return Number.isFinite(secs) && secs > 0 ? secs : null;
}

// Naechstes kommendes A-Rennen aus dem Intervals-Kalender.
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
    // Triathlon: distance ist die Lauf-Distanz des Formats, targetTimeSecs die Gesamtzeit des Eintrags.
    triathlon: parseTriathlonEvent(event),
  };
}

async function fetchUpcomingARaceEvents(env, todayIso) {
  const newest = isoDate(new Date(new Date(todayIso + "T00:00:00Z").getTime() + GOAL_EVENT_LOOKAHEAD_DAYS * 86400000));
  const events = await fetchIntervalsEvents(env, todayIso, newest);
  const list = Array.isArray(events) ? events : Array.isArray(events?.events) ? events.events : [];
  return list.filter((e) => isARaceEvent(e));
}

function formatTime(secs) {
  if (!Number.isFinite(secs) || secs <= 0) return null;
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.round(secs % 60);
  if (h > 0) return `${h}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
  return `${m}:${String(s).padStart(2, "0")}`;
}

// Das Ziel-Rennen kommt aus dem A-Rennen im Intervals-Kalender; ohne Eintrag null.
export async function resolveActiveGoalRace(env, todayIso) {
  const races = await fetchUpcomingARaceEvents(env, todayIso).catch(() => []);
  return deriveAutoGoalFromRaces(races, todayIso);
}
