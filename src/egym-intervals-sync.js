import { isoDateBerlin } from "./date-utils.js";
import { fetchEgymWorkouts, hasEgymCredentials } from "./egym-client.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, postManualActivities, updateIntervalsActivity } from "./intervals-client.js";
import { isStrengthExercise, secondsOf, setsOf, setVolumeKg, strengthDays, uniqueWorkouts, weightKgOf } from "./egym-widget.js";

// Schreibt die EGYM-Krafteinheiten als manuelle Aktivitaeten (Typ WeightTraining) nach Intervals.icu, damit Kraft dort
// im Kalender und in der Wochenlast steht. Je Kraft-Tag eine Aktivitaet; external_id "egym-<Datum>" macht den Abgleich
// wiederholbar: Intervals aktualisiert eine vorhandene Aktivitaet mit derselben external_id statt eine zweite anzulegen.
const SYNC_DAYS = 7;
const val = (a) => (a && typeof a === "object" ? a.value : a);

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

// "2026-10-01T18:05:00" in Berliner Ortszeit (start_date_local erwartet keine Zeitzone)
function localIso(ms) {
  const p = Object.fromEntries(new Intl.DateTimeFormat("en-GB", { timeZone: "Europe/Berlin", year: "numeric", month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", second: "2-digit", hourCycle: "h23" }).formatToParts(new Date(ms)).map((x) => [x.type, x.value]));
  return `${p.year}-${p.month}-${p.day}T${p.hour}:${p.minute}:${p.second}`;
}

const fmtNum = (x) => String(Math.round(x * 10) / 10).replace(".", ",");

// Reine Funktion: EGYM-Workouts in Intervals-Aktivitaeten (eine je Kraft-Tag, nur Tage mit Saetzen oder Geraet)
export function buildIntervalsActivities(workouts) {
  const days = new Map(strengthDays(workouts).map((d) => [d.date, d]));
  const byDay = new Map();
  for (const w of uniqueWorkouts(workouts)) {
    const at = Date.parse(w?.completedAt);
    if (!Number.isFinite(at)) continue;
    const date = isoDateBerlin(new Date(at));
    if (!days.has(date)) continue;
    for (const ex of (w.exercises ?? []).filter(isStrengthExercise)) {
      const cur = byDay.get(date) ?? { machines: new Map(), first: Infinity };
      const t = Date.parse(ex.completedAt ?? w.completedAt);
      if (Number.isFinite(t)) cur.first = Math.min(cur.first, t);
      const label = String(ex.name ?? ex.exercise?.label ?? "Übung").replace(/^EGYM\s+/i, "");
      cur.machines.set(label, [...(cur.machines.get(label) ?? []), ...setsOf(ex)]);
      byDay.set(date, cur);
    }
  }
  return [...byDay.entries()].sort(([a], [b]) => a.localeCompare(b)).map(([date, { machines, first }]) => {
    const d = days.get(date);
    const lines = [...machines.entries()].map(([label, sets]) => `${label}: ${sets.length ? sets.map((s) => { const kg = weightKgOf(s); return `${val(s.reps) ?? "?"}${kg ? ` × ${fmtNum(kg)} kg` : ""}`; }).join(", ") : "Gerät genutzt"}`);
    const estimated = d.minutes == null;
    const minutes = d.minutes ?? Math.max(1, d.sets * 2);
    const volume = d.volumeKg >= 1000 ? `${fmtNum(d.volumeKg / 1000)} t` : `${Math.round(d.volumeKg)} kg`;
    const head = `${d.sets} Sätze · ${volume} Volumen${d.durationSource === "estimate" || estimated ? " · Dauer geschätzt" : ""}`;
    return {
      external_id: `egym-${date}`,
      type: "WeightTraining",
      name: "Krafttraining (EGYM)",
      start_date_local: localIso(Number.isFinite(first) ? first : Date.parse(`${date}T12:00:00Z`)),
      moving_time: minutes * 60,
      description: `${head}\n\n${lines.join("\n")}`,
    };
  });
}

// Reine Funktion: welche EGYM-Aktivitaet zu welchem geplanten Krafttraining gehoert. Manuell angelegte Aktivitaeten
// koppelt Intervals nicht selbst mit dem Plan; je Tag nimmt eine EGYM-Aktivitaet das erste noch freie WeightTraining-Workout.
export function pairsForPlanned(activities, events) {
  const taken = new Set((activities ?? []).map((a) => a?.paired_event_id).filter(Boolean));
  const out = [];
  for (const a of activities ?? []) {
    if (!String(a?.external_id ?? "").startsWith("egym-") || a.paired_event_id || !a.id) continue;
    const date = String(a.start_date_local ?? "").slice(0, 10);
    const ev = (events ?? []).find((e) => e?.category === "WORKOUT" && e.type === "WeightTraining" && String(e.start_date_local ?? "").slice(0, 10) === date && !e.paired_activity_id && !taken.has(e.id));
    if (!ev) continue;
    taken.add(ev.id);
    out.push({ activityId: a.id, eventId: ev.id });
  }
  return out;
}

// Reine Funktion: Dauer, die in Intervals nicht zum EGYM-Stand passt. Der Bulk-Upsert uebernimmt bei einer vorhandenen
// Aktivitaet die Beschreibung, aber nicht die Dauer (ein Sync mitten im Training liess sonst z. B. 3 Minuten stehen).
export function durationFixes(existing, built) {
  const want = new Map((built ?? []).map((b) => [b.external_id, b.moving_time]));
  return (existing ?? [])
    .filter((a) => a?.id && want.has(a.external_id) && (a.moving_time !== want.get(a.external_id) || a.elapsed_time !== want.get(a.external_id)))
    .map((a) => ({ activityId: a.id, moving_time: want.get(a.external_id), elapsed_time: want.get(a.external_id) }));
}

// Nach dem Upsert: Dauer nachziehen und ungekoppelte EGYM-Aktivitaeten mit dem geplanten Krafttraining desselben Tages
// verbinden; Fehler nur protokollieren
async function reconcile(env, built, oldest, newest) {
  try {
    const [acts, events] = await Promise.all([fetchIntervalsActivities(env, oldest, newest), fetchIntervalsEvents(env, oldest, newest)]);
    const patches = new Map();
    for (const f of durationFixes(acts, built)) patches.set(f.activityId, { moving_time: f.moving_time, elapsed_time: f.elapsed_time });
    const pairs = pairsForPlanned(acts, events);
    for (const p of pairs) patches.set(p.activityId, { ...patches.get(p.activityId), paired_event_id: p.eventId });
    for (const [id, patch] of patches) await updateIntervalsActivity(env, id, patch);
    return { paired: pairs.length, updated: patches.size, minutes: Object.fromEntries(built.map((b) => [b.external_id.slice(5), Math.round(b.moving_time / 60)])) };
  } catch (e) {
    console.warn("egym reconcile failed", String(e?.message ?? e));
    return { paired: 0, updated: 0, reconcileError: String(e?.message ?? e) };
  }
}

// Letzte SYNC_DAYS Tage aus EGYM holen und nach Intervals schreiben; ohne Zugangsdaten tut es nichts
export async function syncEgymToIntervals(env, today = isoDateBerlin()) {
  if (!hasEgymCredentials(env) || !env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return { skipped: true, count: 0 };
  const r = await fetchEgymWorkouts(env, addDays(today, -SYNC_DAYS), today);
  const activities = buildIntervalsActivities(Array.isArray(r) ? r : r?.workouts ?? []);
  if (!activities.length) return { skipped: false, count: 0 };
  await postManualActivities(env, activities);
  const done = await reconcile(env, activities, activities[0].start_date_local.slice(0, 10), today);
  return { skipped: false, count: activities.length, ...done };
}
