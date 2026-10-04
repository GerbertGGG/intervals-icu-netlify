import { readKvJson, writeKvJson } from "./kv.js";
import { isoDateBerlin, mondayOnOrBefore } from "./date-utils.js";
import { hasEgymCredentials, fetchEgymWorkouts, fetchEgymStrength, fetchEgymBioAge } from "./egym-client.js";

// Kraft-Widget (Ansicht "kraft", mittleres Widget, und Karte im Dashboard): Trainingszeit gegen Wochenziel, Einheiten,
// Saetze und Volumen der letzten Wochen, Fortschritt aus den EGYM-Kraft-Tests (1RM je Geraet, Veraenderung zum
// vorherigen Test) und das Muskelalter.
// Daten kommen aus der EGYM-App (siehe egym-client.js). Die Garmin-Tagesaktivitaet ("Daily routine") zaehlt nicht
// als Krafttraining: Eine Kraft-Einheit ist ein Tag mit mindestens einer Uebung mit Saetzen (Wiederholungen und
// Gewicht) oder an einem Geraet. Das Wochenziel ist Zeit: mindestens 60 Minuten Krafttraining pro Woche (wie im
// Dashboard-Abschnitt Kraft), per EGYM_WEEKLY_GOAL_MIN aenderbar; goalSource sagt, was gilt.
// Die Dauer eines Tages kommt aus den Angaben von EGYM (Dauer der Uebungen, sonst der Saetze); fehlen sie, ist es nur
// die Spanne zwischen erster und letzter Uebung und als Schaetzung gekennzeichnet (durationSource), nie als EGYM-Wert.
const DEFAULT_WEEKLY_GOAL_MIN = 60;
const WEEKS = 6;
const LB_TO_KG = 0.45359237;
const SETS_KEY = "sets_of_reps_and_weight_or_duration_and_weight";

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const round1 = (x) => Math.round(x * 10) / 10;
const val = (a) => (a && typeof a === "object" ? a.value : a);

const setsOf = (ex) => {
  const s = ex?.attributes?.[SETS_KEY];
  return Array.isArray(s) ? s : [];
};
const isStrengthExercise = (ex) => setsOf(ex).length > 0 || ex?.exercise?.machineBased === true;

// Volumen eines Satzes in kg (Wiederholungen mal Gewicht); fehlen Angaben, zaehlt der Satz nur als Satz
function setVolumeKg(s) {
  const reps = Number(val(s?.reps));
  const wt = Number(val(s?.weight));
  if (!Number.isFinite(reps) || !Number.isFinite(wt) || reps <= 0 || wt <= 0) return 0;
  const unit = String(s?.weight?.unit ?? "kg").toLowerCase();
  return reps * wt * (unit === "lb" || unit === "lbs" ? LB_TO_KG : 1);
}

// Dieselbe Workout-Nummer kann mehrfach kommen (auch unvollstaendig): die Variante mit mehr Saetzen gewinnt
function uniqueWorkouts(list) {
  const richness = (w) => (w.exercises ?? []).reduce((a, ex) => a + setsOf(ex).length, 0);
  const byCode = new Map();
  for (const w of list ?? []) {
    const key = w?.code ?? `${w?.completedAt}`;
    const have = byCode.get(key);
    if (!have || richness(w) > richness(have)) byCode.set(key, w);
  }
  return [...byCode.values()];
}

// Dauer in Sekunden aus einem EGYM-Attribut {value, unit}; unbekannte Einheit zaehlt nicht
function secondsOf(attr) {
  const v = Number(val(attr));
  if (!Number.isFinite(v) || v <= 0) return 0;
  const unit = String(attr?.unit ?? "sec").toLowerCase();
  if (unit.startsWith("min")) return v * 60;
  if (unit === "h" || unit.startsWith("hour")) return v * 3600;
  return unit.startsWith("s") ? v : 0;
}

// Eine Zeile je Kraft-Tag: Datum (Berlin), Saetze, Volumen, Dauer in Minuten samt Herkunft ("egym" oder "estimate")
export function strengthDays(workouts) {
  const days = new Map();
  for (const w of uniqueWorkouts(workouts)) {
    const at = w?.completedAt;
    if (!at || !Number.isFinite(Date.parse(at))) continue;
    const exs = (w.exercises ?? []).filter(isStrengthExercise);
    if (!exs.length) continue;
    const date = isoDateBerlin(new Date(at));
    const d = days.get(date) ?? { date, sets: 0, volumeKg: 0, sec: 0, stamps: [] };
    for (const ex of exs) {
      const sets = setsOf(ex);
      d.sets += sets.length;
      d.volumeKg += sets.reduce((a, s) => a + setVolumeKg(s), 0);
      d.sec += secondsOf(ex.attributes?.duration) || sets.reduce((a, s) => a + secondsOf(s?.duration), 0);
      const t = Date.parse(ex.completedAt ?? at);
      if (Number.isFinite(t)) d.stamps.push(t);
    }
    days.set(date, d);
  }
  return [...days.values()].sort((a, b) => a.date.localeCompare(b.date)).map(({ stamps, sec, ...d }) => {
    if (sec > 0) return { ...d, minutes: Math.round(sec / 60), durationSource: "egym" };
    const span = stamps.length > 1 ? (Math.max(...stamps) - Math.min(...stamps)) / 60000 : 0;
    return span >= 1 ? { ...d, minutes: Math.round(span), durationSource: "estimate" } : { ...d, minutes: null, durationSource: null };
  });
}

// Zeitpunkt eines Kraft-Tests: aussen, bei Platzhalter 1970 der innere
function measuredAt(m) {
  const ok = (s) => typeof s === "string" && s && !s.startsWith("1970-");
  return ok(m?.createdAt) ? m.createdAt : ok(m?.strength?.createdAt) ? m.strength.createdAt : null;
}

// Je Geraet der letzte Test gegen den davor (die History liefert progress/amountDiff nicht, also selbst gerechnet).
// Mehrere Tests am selben Tag zaehlen als einer (der letzte).
export function strengthProgress(measurements) {
  const byEx = new Map();
  for (const m of measurements ?? []) {
    const at = measuredAt(m);
    const kg = Number(m?.strength?.value);
    const code = m?.exercise?.code;
    if (!at || !Number.isFinite(kg) || kg <= 0 || code == null) continue;
    const list = byEx.get(code) ?? [];
    list.push({ at, kg, label: m.exercise.label ?? String(code), region: m.bodyRegion ?? null });
    byEx.set(code, list);
  }
  const items = [];
  for (const list of byEx.values()) {
    list.sort((a, b) => a.at.localeCompare(b.at));
    const perDay = new Map(list.map((x) => [x.at.slice(0, 10), x]));
    const tests = [...perDay.values()];
    const last = tests[tests.length - 1];
    const prev = tests.length > 1 ? tests[tests.length - 2] : null;
    items.push({
      label: String(last.label).replace(/^EGYM\s+/i, ""),
      region: last.region,
      kg: last.kg,
      at: last.at.slice(0, 10),
      prevKg: prev?.kg ?? null,
      prevAt: prev?.at.slice(0, 10) ?? null,
      diffKg: prev ? round1(last.kg - prev.kg) : null,
      pct: prev ? round1(((last.kg - prev.kg) / prev.kg) * 100) : null,
    });
  }
  const compared = items.filter((i) => i.diffKg != null);
  return {
    machines: items.length,
    improved: compared.filter((i) => i.diffKg > 0).length,
    same: compared.filter((i) => i.diffKg === 0).length,
    declined: compared.filter((i) => i.diffKg < 0).length,
    // groesste Zuwaechse zuerst, ohne Vergleich ans Ende
    items: items.sort((a, b) => (b.pct ?? -Infinity) - (a.pct ?? -Infinity)),
    latestAt: items.reduce((a, i) => (i.at > a ? i.at : a), ""),
  };
}

function bioAgeOf(b) {
  const v = (x) => (x?.value != null && Number.isFinite(Number(x.value)) ? Math.round(Number(x.value)) : null);
  return {
    total: v(b?.totalDetails?.totalBioAge),
    muscle: v(b?.muscleDetails?.muscleBioAge),
    upper: v(b?.muscleDetails?.upperBodyAge),
    core: v(b?.muscleDetails?.coreAge),
    lower: v(b?.muscleDetails?.lowerBodyAge),
  };
}

function goalOf(env) {
  const n = Math.floor(Number(env?.EGYM_WEEKLY_GOAL_MIN));
  return Number.isFinite(n) && n > 0 ? { goal: n, source: "config" } : { goal: DEFAULT_WEEKLY_GOAL_MIN, source: "default" };
}

// Reine Funktion: Rohdaten der drei Endpunkte in die Widget-Daten (je Teil kann null sein, wenn der Abruf scheiterte)
export function buildWidgetKraft({ workouts, strength, bioAge, today, env = {}, generatedAt = new Date().toISOString(), failed = [] }) {
  const { goal, source } = goalOf(env);
  const days = strengthDays(workouts?.workouts ?? workouts ?? []);
  const monday = mondayOnOrBefore(today);
  const sum = (arr, f) => arr.reduce((a, x) => a + (f(x) ?? 0), 0);
  // Herkunft der Minuten einer Woche: "estimate", sobald ein Tag nur geschaetzt ist; null, wenn keine Dauer vorliegt
  const sourceOf = (arr) => (arr.some((d) => d.durationSource === "estimate") ? "estimate" : arr.some((d) => d.durationSource === "egym") ? "egym" : null);
  const weeks = Array.from({ length: WEEKS }, (_, i) => {
    const start = addDays(monday, -7 * (WEEKS - 1 - i));
    const end = addDays(start, 6);
    const inWeek = days.filter((d) => d.date >= start && d.date <= end);
    const src = sourceOf(inWeek);
    return { start, sessions: inWeek.length, sets: sum(inWeek, (d) => d.sets), volumeKg: Math.round(sum(inWeek, (d) => d.volumeKg)), minutes: src ? sum(inWeek, (d) => d.minutes) : inWeek.length ? null : 0, minutesSource: src };
  });
  const cur = weeks[WEEKS - 1];
  const dayAt = (i) => days.find((d) => d.date === addDays(monday, i));
  const dayFlags = Array.from({ length: 7 }, (_, i) => Boolean(dayAt(i)));
  const dayMinutes = Array.from({ length: 7 }, (_, i) => dayAt(i)?.minutes ?? null);
  const hit = (w) => w.minutes != null && w.minutes >= goal;
  // Serie: aufeinanderfolgende abgeschlossene Wochen mit erreichtem Ziel; die laufende Woche zaehlt mit, sobald sie es erreicht
  let streak = 0;
  for (let i = WEEKS - 2; i >= 0 && hit(weeks[i]); i--) streak++;
  if (hit(cur)) streak++;
  const done = weeks.slice(0, WEEKS - 1).slice(-4);
  const withMin = done.filter((w) => w.minutes != null);
  const last = days.length ? days[days.length - 1].date : null;
  const dayDiff = (iso) => Math.round((Date.parse(today + "T00:00:00Z") - Date.parse(iso + "T00:00:00Z")) / 86400000);
  const prog = strength ? strengthProgress(strength.strengthMeasurements ?? strength) : null;
  // Volumen: letzte abgeschlossene Woche gegen die davor (die laufende ist noch unvollstaendig)
  const [vPrev, vLast] = [weeks[WEEKS - 3].volumeKg, weeks[WEEKS - 2].volumeKg];
  return {
    generatedAt,
    today,
    week: { start: monday, minutes: cur.minutes, minutesSource: cur.minutesSource, goalMin: goal, goalSource: source, sessions: cur.sessions, sets: cur.sets, volumeKg: cur.volumeKg, days: dayFlags, dayMinutes },
    weeks,
    lastDay: last,
    daysSince: last ? dayDiff(last) : null,
    streakWeeks: streak,
    weeksHit: { hit: done.filter(hit).length, of: done.length },
    avg4Minutes: withMin.length ? Math.round(sum(withMin, (w) => w.minutes) / withMin.length) : null,
    volumeTrend: vPrev > 0 && vLast > 0 ? { prevKg: vPrev, lastKg: vLast, pct: round1(((vLast - vPrev) / vPrev) * 100) } : null,
    progress: prog,
    bioAge: bioAge ? bioAgeOf(bioAge) : null,
    sourcesFailed: failed,
  };
}

// EGYM-Abrufe sind langsam (Login, drei Endpunkte): das Ergebnis liegt kurz in KV
const CACHE_KEY = "widget:egym-kraft";
export const KRAFT_CACHE_MS = 15 * 60 * 1000;

export async function loadWidgetKraft(env, fresh = false) {
  const today = isoDateBerlin();
  if (!hasEgymCredentials(env)) return { configured: false, today, generatedAt: new Date().toISOString() };
  if (!fresh) {
    const c = await readKvJson(env, CACHE_KEY).catch(() => null);
    if (c?.data && c.data.today === today && Date.now() - c.at < KRAFT_CACHE_MS) return c.data;
  }
  const from = addDays(today, -(7 * WEEKS + 6));
  const settle = async (fn) => { try { return { v: await fn() }; } catch (e) { console.warn("egym fetch failed", String(e?.message ?? e)); return { v: null, e: true }; } };
  const [w, s, b] = await Promise.all([
    settle(() => fetchEgymWorkouts(env, from, today)),
    settle(() => fetchEgymStrength(env, addDays(today, -365), today)),
    settle(() => fetchEgymBioAge(env)),
  ]);
  const failed = [["egymWorkouts", w], ["egymStrength", s], ["egymBioAge", b]].filter(([, r]) => r.e).map(([k]) => k);
  const data = { configured: true, ...buildWidgetKraft({ workouts: w.v, strength: s.v, bioAge: b.v, today, env, failed }) };
  // Nur vollstaendige Staende cachen, ein Ausfall soll nicht eine Viertelstunde haengen bleiben
  if (!failed.length) await writeKvJson(env, CACHE_KEY, { at: Date.now(), data }).catch(() => {});
  return data;
}
