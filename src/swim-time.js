import { readKvJson, writeKvJson, hasKv } from "./kv.js";
import { fetchIntervalsActivityIntervals } from "./intervals-client.js";

// Schwimmzeit fuers Wochenziel: nur die Bewegungszeit, Pausen am Beckenrand zaehlen nicht. Lauf und Rad bleiben bei moving_time.
// Die Laps holt der Worker einzeln (GET /activity/{id}/intervals); die Aktivitaetsliste liefert keine icu_intervals.

const num = (v) => (Number.isFinite(Number(v)) && v != null && v !== "" ? Number(v) : null);
const CACHE_PREFIX = "icu:swim-intervals:v1:";
// Liegen moving_time und die Summe der Nicht-Ruhe-Intervalle hoechstens so weit auseinander, gilt moving_time
export const SWIM_MOVING_TOLERANCE = 0.05;

// Ruhe-Intervall: Typ RECOVERY/REST bzw. ein Lap ohne Strecke (Garmin-Pause am Beckenrand)
const isRest = (i) => /^(recovery|rest)$/i.test(String(i?.type ?? "")) || !(num(i?.distance) > 0);

// Summe der Nicht-Ruhe-Intervalle in Sekunden (moving_time je Intervall, sonst elapsed_time); null ohne Intervalle
export function swimActiveSec(intervals) {
  const list = Array.isArray(intervals) ? intervals : [];
  if (!list.length) return null;
  return list.filter((i) => !isRest(i)).reduce((s, i) => s + (num(i.moving_time) ?? num(i.elapsed_time) ?? 0), 0);
}

// Gezaehlte Schwimmsekunden einer Aktivitaet: moving_time, wenn es zur Summe der Nicht-Ruhe-Intervalle passt (< 5 %),
// sonst diese Summe (nie mehr als moving_time). Ohne Intervalle moving_time, fehlt auch das elapsed_time (estimated: true).
// source: "moving_time" | "intervals" | "elapsed_time"; elapsedSec fuer den Pausenanteil (ohne elapsed_time = sec).
export function swimTimeOf(a) {
  const moving = num(a?.moving_time) > 0 ? num(a.moving_time) : null, elapsed = num(a?.elapsed_time) > 0 ? num(a.elapsed_time) : null;
  const active = swimActiveSec(a?.icu_intervals);
  let sec, source;
  // Die Intervall-Summe dient nur dazu, eine zu hohe moving_time (mit Pausen) zu senken; liegt sie darueber, gilt moving_time
  if (active > 0 && (moving == null || (moving > active && (moving - active) / active >= SWIM_MOVING_TOLERANCE))) [sec, source] = [active, "intervals"];
  else if (moving != null) [sec, source] = [moving, "moving_time"];
  else [sec, source] = [elapsed ?? 0, "elapsed_time"];
  return { sec, elapsedSec: elapsed ?? sec, source, estimated: source === "elapsed_time" };
}

const isSwim = (a) => /swim/i.test(String(a?.type ?? ""));

// Haengt an jede Schwimmeinheit ohne icu_intervals die Intervalle aus Intervals.icu an (nur type, distance, moving_time,
// elapsed_time). KV-Cache je Aktivitaet und Analyse-Stand (analyzed aendert sich, wenn Laps bearbeitet werden). Fehler einzeln
// schlucken: dann bleibt die Einheit ohne Intervalle und zaehlt mit moving_time.
export async function withSwimIntervals(env, activities) {
  return Promise.all((activities ?? []).map(async (a) => {
    if (!isSwim(a) || a?.icu_ignore_time || Array.isArray(a?.icu_intervals) || a?.id == null) return a;
    const key = `${CACHE_PREFIX}${a.id}:${a.analyzed ?? ""}`;
    let list = hasKv(env) ? await readKvJson(env, key).catch(() => null) : null;
    if (!Array.isArray(list)) {
      try {
        const dto = await fetchIntervalsActivityIntervals(env, a.id);
        list = (Array.isArray(dto?.icu_intervals) ? dto.icu_intervals : []).map((i) => ({ type: i?.type ?? null, distance: num(i?.distance), moving_time: num(i?.moving_time), elapsed_time: num(i?.elapsed_time) }));
      } catch (err) {
        console.warn("swim intervals", a.id, err);
        return a;
      }
      if (hasKv(env)) await writeKvJson(env, key, list).catch(() => {});
    }
    return { ...a, icu_intervals: list };
  }));
}
