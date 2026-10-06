import { readKvJson, writeKvJson, hasKv } from "./kv.js";
import { fetchIntervalsActivityIntervals } from "./intervals-client.js";

// Echte Intervall-Splits einer Einheit aus Intervals.icu (GET /activity/{id}/intervals).
// Harte Regel wie bei den Laufdaten: Intervall-/Tempo-Einheiten liefern KEINE Pulswerte, hier gehen nur Distanz, Zeit und Pace raus.

const MAX_SESSIONS = 4;
const CACHE_PREFIX = "icu:intervals:v1:";

const num = (v) => (Number.isFinite(Number(v)) && v != null && v !== "" ? Number(v) : null);

function formatPace(secPerKm) {
  if (!Number.isFinite(secPerKm) || secPerKm <= 0) return null;
  const s = Math.round(secPerKm);
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, "0")}`;
}

const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;

// Aus dem IntervalsDTO nur die Arbeitsintervalle (type WORK). Pause/Erholung zaehlt nicht.
export function buildIntervalSession(dto) {
  const list = Array.isArray(dto?.icu_intervals) ? dto.icu_intervals : [];
  const reps = list
    .filter((i) => String(i?.type ?? "").toUpperCase() === "WORK")
    .map((i) => {
      const distanceM = num(i.distance), timeSec = num(i.moving_time) ?? num(i.elapsed_time);
      const speed = num(i.average_speed);
      const pace = distanceM > 0 && timeSec > 0 ? timeSec / (distanceM / 1000) : speed > 0 ? 1000 / speed : null;
      return { distanceM: distanceM != null ? Math.round(distanceM) : null, timeSec, paceSecPerKm: pace != null ? Math.round(pace) : null };
    })
    .filter((r) => r.paceSecPerKm != null);
  if (reps.length < 2) return null;
  const paces = reps.map((r) => r.paceSecPerKm);
  const half = Math.floor(paces.length / 2);
  // Fade: zweite Haelfte im Schnitt langsamer (positiv) oder schneller (negativ) als die erste; bei ungerader Anzahl zaehlt die mittlere Wdh. nicht
  const fade = mean(paces.slice(paces.length - half)) - mean(paces.slice(0, half));
  const totalM = reps.reduce((s, r) => s + (r.distanceM ?? 0), 0);
  return {
    reps: reps.map((r, idx) => ({ n: idx + 1, ...r, pace: formatPace(r.paceSecPerKm) })),
    count: reps.length,
    workKm: Math.round(totalM / 100) / 10,
    avgPaceSecPerKm: Math.round(mean(paces)),
    avgPace: formatPace(mean(paces)),
    spreadSecPerKm: Math.max(...paces) - Math.min(...paces),
    fadeSecPerKm: Math.round(fade),
  };
}

// Die juengsten Intensitaets-Laeufe: Intervall-Splits holen (KV-Cache je Aktivitaet, die Splits aendern sich nicht mehr), Fehler einzeln schlucken.
export async function buildIntervalSessions(env, runs, todayIso, sinceIso) {
  const picks = runs.filter((r) => r.kind === "intensity" && r.id != null && r.date >= sinceIso && r.date <= todayIso).slice(0, MAX_SESSIONS);
  const out = [];
  for (const r of picks) {
    let session = hasKv(env) ? await readKvJson(env, CACHE_PREFIX + r.id) : null;
    if (!session) {
      try {
        session = buildIntervalSession(await fetchIntervalsActivityIntervals(env, r.id));
      } catch (err) {
        console.warn("interval splits", r.id, err);
        continue;
      }
      if (session && hasKv(env)) await writeKvJson(env, CACHE_PREFIX + r.id, session).catch(() => {});
    }
    if (session) out.push({ date: r.date, name: r.name, ...session });
  }
  return out;
}
