import { isIsoDate, isoDate, isoDateBerlin, isoWeekNumber, mondayOnOrBefore } from "./date-utils.js";
import { activityDay } from "./activity-utils.js";

import { DEFAULT_TARGET, targetHistory, targetOn } from "./targets-snapshot.js";
import { swimTimeOf } from "./swim-time.js";

// Die Wochenziele setzt der Coach (save_targets, targets-snapshot.js); hier wird nur Ist gegen das Ziel der jeweiligen Woche
// gerechnet. So viele Wochen (inkl. der laufenden) zeigt der Verlauf; die Serie zaehlt nur innerhalb dieses Fensters
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

// Referenzwoche (wie im Coach-Skill, Abschnitt "Steigerung und Referenzwoche"): Mittel der Ausdauerzeit der letzten 3 vollen Wochen
// ohne leichte Wochen (unter 70 % ihrer eigenen Referenz). Sind alle drei leicht, das Mittel aller drei. list: [{ enduranceMin }],
// aufeinanderfolgende Wochen, aelteste zuerst; vor dem Anfang der Liste ist nichts bekannt (erste Woche: Referenz null).
export const REFERENCE_WEEKS = 3;
export const LIGHT_WEEK_SHARE = 0.7;
export function withReference(list) {
  const out = [];
  for (const w of list) {
    const prev = out.slice(-REFERENCE_WEEKS);
    const normal = prev.filter((x) => !x.light);
    const base = normal.length ? normal : prev;
    const referenceMin = base.length ? Math.round(base.reduce((a, x) => a + x.enduranceMin, 0) / base.length) : null;
    out.push({ ...w, referenceMin, light: referenceMin > 0 && w.enduranceMin < referenceMin * LIGHT_WEEK_SHARE });
  }
  return out;
}

// Ausdauersekunden einer Aktivitaet (Laufen und Rad: moving_time; Schwimmen: Bewegungszeit ohne Pausen, swimTimeOf;
// icu_ignore_time zaehlt nicht), sonst 0
const enduranceSecOf = (a) => {
  const disc = a?.icu_ignore_time ? null : enduranceDiscipline(a);
  if (!disc) return 0;
  return disc === "schwimmen" ? swimTimeOf(a).sec : Number(a?.moving_time) || 0;
};

// Ausdauerminuten je ISO-Woche ab der ersten vollen Woche ab fromIso bis zur laufenden, mit Referenzwoche (withReference).
// Fuer die Intensitaets-Kachel im Trainingsblock (season-block.js); fromIso = ab wann Aktivitaeten geladen sind.
export function enduranceWeeks(activities, fromIso, todayIso = isoDateBerlin()) {
  const first = mondayOnOrBefore(fromIso) === fromIso ? fromIso : addDays(mondayOnOrBefore(fromIso), 7);
  const acc = new Map();
  for (let m = first; m <= todayIso; m = addDays(m, 7)) acc.set(m, 0);
  for (const a of activities ?? []) {
    const day = activityDay(a);
    if (!isIsoDate(day) || day > todayIso) continue;
    const k = mondayOnOrBefore(day);
    if (acc.has(k)) acc.set(k, acc.get(k) + enduranceSecOf(a));
  }
  return withReference([...acc].map(([week, sec]) => ({ week, enduranceMin: Math.round(sec / 60) })));
}

// Wochensummen je ISO-Woche (Mo–So nach dem lokalen Startdatum der Aktivitaet, bei Gerbert Europe/Berlin), aelteste zuerst,
// die laufende zuletzt (complete: false). Ausdauer aus moving_time, beim Schwimmen nur die Bewegungszeit (swimTimeOf), Kraft in
// Saetzen; icu_ignore_time zaehlt nicht. Schwimmen zusaetzlich: swimMovingHours (zaehlt), swimElapsedHours (nur Info),
// swimRestShare (Pausenanteil in %, null ohne Schwimmen) und swimSessions je Einheit (swimTimeEstimated: nur elapsed_time da).
// Jede Woche traegt das Ziel, das in ihr galt (Eintrag am Sonntag bzw. heute, siehe targetOn): goalHours, goalSets2w und
// streakMin (= streakThresholdPct % des Wochenziels in Minuten). Steigt das Ziel, bleiben vergangene Wochen beim alten.
// streak = volle Wochen in Folge mit Ausdauer >= streakMin bis zur letzten vollen Woche, die laufende zaehlt mit, sobald
// sie ihre Schwelle erreicht. Innerhalb des Fensters (streakCapped: true, wenn die Serie bis an dessen Anfang reicht).
// stored = gespeicherte Ziele (KV, readTargets); ohne gilt der Startwert.
export function computeWeekTotals(activities, todayIso = isoDateBerlin(), stored = null, weeks = WEEK_TOTALS_WEEKS) {
  const history = targetHistory(stored);
  const thisMonday = mondayOnOrBefore(todayIso);
  const mondays = Array.from({ length: weeks + 1 }, (_, i) => addDays(thisMonday, -7 * (weeks - i))); // eine Woche mehr fuer die 2 Wochen Kraft
  const acc = new Map(mondays.map((m) => [m, { sec: 0, by: { lauf: 0, rad: 0, schwimmen: 0 }, sets: 0, swimElapsed: 0, swims: [] }]));
  for (const a of activities ?? []) {
    if (a?.icu_ignore_time) continue;
    const day = activityDay(a);
    if (!isIsoDate(day) || day > todayIso) continue;
    const w = acc.get(mondayOnOrBefore(day));
    if (!w) continue;
    const disc = enduranceDiscipline(a);
    if (disc) {
      const sec = enduranceSecOf(a);
      w.sec += sec;
      w.by[disc] += sec;
      if (disc === "schwimmen") {
        const st = swimTimeOf(a);
        w.swimElapsed += st.elapsedSec;
        w.swims.push({ id: a?.id ?? null, date: day, movingMin: Math.round(st.sec / 60), elapsedMin: Math.round(st.elapsedSec / 60), source: st.source, swimTimeEstimated: st.estimated });
      }
    } else if (isStrengthActivity(a)) w.sets += strengthSetsOf(a);
  }
  const refs = withReference(mondays.map((m) => ({ enduranceMin: Math.round(acc.get(m).sec / 60) })));
  const list = mondays.slice(1).map((week, i) => {
    const w = acc.get(week), prev = acc.get(mondays[i]);
    const end = addDays(week, 6), tg = targetOn(history, end < todayIso ? end : todayIso);
    const pct = tg.streakThresholdPct ?? DEFAULT_TARGET.streakThresholdPct, enduranceMin = Math.round(w.sec / 60);
    const streakMin = Math.round(tg.enduranceHoursPerWeek * 60 * pct) / 100;
    return {
      week,
      kw: isoWeekNumber(week),
      complete: week !== thisMonday,
      enduranceMin,
      enduranceHours: hours(w.sec),
      // Referenzwoche (withReference) aus den Wochen davor im Fenster; null ohne Vorwoche
      referenceMin: refs[i + 1].referenceMin,
      byDiscipline: Object.fromEntries(DISCIPLINES.map((k) => [k, hours(w.by[k])])),
      byDisciplineMin: Object.fromEntries(DISCIPLINES.map((k) => [k, Math.round(w.by[k] / 60)])),
      swimMovingHours: hours(w.by.schwimmen),
      swimElapsedHours: hours(w.swimElapsed),
      swimRestShare: w.swimElapsed > 0 ? Math.max(0, Math.round((1 - w.by.schwimmen / w.swimElapsed) * 100)) : null,
      swimSessions: w.swims,
      strengthSets: w.sets,
      strengthSets2w: w.sets + prev.sets,
      goalHours: tg.enduranceHoursPerWeek,
      goalSets2w: tg.strengthSets2w,
      streakThresholdPct: pct,
      streakMin,
      streakMet: w.sec >= streakMin * 60,
    };
  });
  let streak = 0, i = list.length - 2;
  while (i >= 0 && list[i].streakMet) { streak++; i--; }
  const cur = list[list.length - 1];
  if (cur.streakMet) streak++;
  // goals: das heute geltende Ziel (Form wie bisher, die Werte kommen jetzt vom Coach)
  return { goals: { enduranceHours: cur.goalHours, strengthSets2w: cur.goalSets2w, streakThresholdPct: cur.streakThresholdPct }, weeks: list, streakWeeksAbove4_5: streak, streakCapped: streak > 0 && i < 0 };
}

// Die Form fuer get_coach (week_totals): die laufende Woche. streakWeeksAbove4_5 heisst aus Kompatibilitaet so, zaehlt aber
// gegen streakThresholdPct % des Ziels der jeweiligen Woche (beim Startwert 90 % von 5 h = 4,5 h); streakWeeks ist dasselbe.
export function currentWeekTotals(t) {
  const c = t?.weeks?.[t.weeks.length - 1];
  if (!c) return null;
  return { week: c.week, enduranceHours: c.enduranceHours, referenceHours: c.referenceMin != null ? hours(c.referenceMin * 60) : null, byDiscipline: c.byDiscipline, swimMovingHours: c.swimMovingHours, swimElapsedHours: c.swimElapsedHours, swimRestShare: c.swimRestShare,
    ...(c.swimSessions.some((x) => x.swimTimeEstimated) ? { swimTimeEstimated: true } : {}), strengthSets: c.strengthSets, strengthSets2w: c.strengthSets2w, streakWeeksAbove4_5: t.streakWeeksAbove4_5, streakWeeks: t.streakWeeksAbove4_5, goals: t.goals };
}

// Aktivitaeten fuer das ganze Fenster (inkl. Vorwoche fuer die 2 Wochen Kraft)
export const weekTotalsOldest = (todayIso, weeks = WEEK_TOTALS_WEEKS) => addDays(mondayOnOrBefore(todayIso), -7 * weeks);
