import { isIsoDate, isoDate, isoDateBerlin, isoWeekNumber, mondayOnOrBefore } from "./date-utils.js";
import { activityDay } from "./activity-utils.js";

// Wochenziele, wie Gerbert sie festgelegt hat. Hier aendern, alles andere (Dashboard, get_coach) liest diese Werte.
// enduranceHours: Ausdauerstunden je ISO-Woche (Laufen, Rad, Schwimmen; Kraft zaehlt nicht)
// strengthSets2w: Kraftsaetze in 2 Wochen, rollierend (laufende Woche plus Vorwoche)
// streakHours: Schwelle fuer "X Wochen in Folge ab 4,5 h"
export const WEEK_GOALS = { enduranceHours: 5, strengthSets2w: 90, streakHours: 4.5 };
// So viele Wochen (inkl. der laufenden) zeigt der Verlauf; die Serie zaehlt nur innerhalb dieses Fensters
export const WEEK_TOTALS_WEEKS = 8;

export const DISCIPLINES = ["lauf", "rad", "schwimmen"];
// Sportart-Schluessel des Dashboards (Farben --s-run usw.) je Disziplin
export const DISC_SPORT = { lauf: "run", rad: "bike", schwimmen: "swim" };

// Ausdauer-Disziplin einer Aktivitaet: Lauf-, Rad- und Schwimm-Typen aus Intervals (Run, VirtualRun, TrailRun, Ride,
// VirtualRide, GravelRide, MountainBikeRide, Swim, OpenWaterSwim ...). WeightTraining, Yoga, Workout, Walk und alles
// Unbekannte: null.
export function enduranceDiscipline(a) {
  const t = String(a?.type ?? "").toLowerCase();
  if (t.includes("swim")) return "schwimmen";
  if (/ride|bike|cycl|velomobile|handcycle/.test(t)) return "rad";
  if (/run|treadmill/.test(t)) return "lauf";
  return null;
}

export const isStrengthActivity = (a) => /weight|strength|kraft/i.test(String(a?.type ?? ""));

// Kraftsaetze einer Aktivitaet: die Zahl vor "Saetze" in der Beschreibung (EGYM-Sync: "30 Saetze · 25,4 t Volumen"),
// sonst je Wiederholung × Gewicht in den Uebungszeilen ein Satz ("20 × 45 kg"). Ohne beides 0.
export function strengthSetsOf(a) {
  const text = String(a?.description ?? "");
  const head = text.match(/(\d+)\s*S(?:ä|ae)tze\b/i);
  if (head) return Number(head[1]);
  return (text.match(/\d+\s*[×x]\s*\d+(?:[.,]\d+)?\s*kg\b/gi) ?? []).length;
}

const addDays = (iso, n) => isoDate(new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000));
const hours = (sec) => Math.round((sec / 3600) * 100) / 100;

// Wochensummen je ISO-Woche (Mo–So nach dem lokalen Startdatum der Aktivitaet, bei Gerbert Europe/Berlin), aelteste zuerst,
// die laufende zuletzt (complete: false). Ausdauer aus moving_time, Kraft in Saetzen; icu_ignore_time zaehlt nicht.
// streak = volle Wochen in Folge ab streakHours bis zur letzten vollen Woche, die laufende zaehlt mit, sobald sie die
// Schwelle erreicht. Innerhalb des Fensters (streakCapped: true, wenn die Serie bis an dessen Anfang reicht).
export function computeWeekTotals(activities, todayIso = isoDateBerlin(), weeks = WEEK_TOTALS_WEEKS, goals = WEEK_GOALS) {
  const thisMonday = mondayOnOrBefore(todayIso);
  const mondays = Array.from({ length: weeks + 1 }, (_, i) => addDays(thisMonday, -7 * (weeks - i))); // eine Woche mehr fuer die 2 Wochen Kraft
  const acc = new Map(mondays.map((m) => [m, { sec: 0, by: { lauf: 0, rad: 0, schwimmen: 0 }, sets: 0 }]));
  for (const a of activities ?? []) {
    if (a?.icu_ignore_time) continue;
    const day = activityDay(a);
    if (!isIsoDate(day) || day > todayIso) continue;
    const w = acc.get(mondayOnOrBefore(day));
    if (!w) continue;
    const disc = enduranceDiscipline(a);
    if (disc) {
      const sec = Number(a?.moving_time) || 0;
      w.sec += sec;
      w.by[disc] += sec;
    } else if (isStrengthActivity(a)) w.sets += strengthSetsOf(a);
  }
  const list = mondays.slice(1).map((week, i) => {
    const w = acc.get(week), prev = acc.get(mondays[i]);
    return {
      week,
      kw: isoWeekNumber(week),
      complete: week !== thisMonday,
      enduranceMin: Math.round(w.sec / 60),
      enduranceHours: hours(w.sec),
      byDiscipline: Object.fromEntries(DISCIPLINES.map((k) => [k, hours(w.by[k])])),
      byDisciplineMin: Object.fromEntries(DISCIPLINES.map((k) => [k, Math.round(w.by[k] / 60)])),
      strengthSets: w.sets,
      strengthSets2w: w.sets + prev.sets,
      _sec: w.sec,
    };
  });
  const min = goals.streakHours * 3600;
  let streak = 0, i = list.length - 2;
  while (i >= 0 && list[i]._sec >= min) { streak++; i--; }
  const cur = list[list.length - 1];
  if (cur._sec >= min) streak++;
  return { goals: { ...goals }, weeks: list.map(({ _sec, ...w }) => w), streakWeeksAbove4_5: streak, streakCapped: streak > 0 && i < 0 };
}

// Die Form fuer get_coach (week_totals): die laufende Woche
export function currentWeekTotals(t) {
  const c = t?.weeks?.[t.weeks.length - 1];
  if (!c) return null;
  return { week: c.week, enduranceHours: c.enduranceHours, byDiscipline: c.byDiscipline, strengthSets: c.strengthSets, strengthSets2w: c.strengthSets2w, streakWeeksAbove4_5: t.streakWeeksAbove4_5, goals: t.goals };
}

// Aktivitaeten fuer das ganze Fenster (inkl. Vorwoche fuer die 2 Wochen Kraft)
export const weekTotalsOldest = (todayIso, weeks = WEEK_TOTALS_WEEKS) => addDays(mondayOnOrBefore(todayIso), -7 * weeks);
