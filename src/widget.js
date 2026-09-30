import { json } from "./http-helpers.js";
import { buildDashboard, isAuthorized } from "./dashboard.js";
import { hasYazioCredentials, fetchYazioDailyGoals } from "./yazio-client.js";

// Kompakte Sicht auf das Dashboard für das iOS-Widget (Scriptable, public/dashboard/scriptable-widget.js).
// Gleiche Einschätzungen wie die Seite (siehe dashboard-summary.js), aber nur das Wichtigste und
// ohne Freitexte: Es gehen keine Kommentare, Einheitsnamen oder Heißhunger-Einträge raus.

const firstLine = (s, max = 110) => {
  const line = String(s ?? "").split(/\r?\n/).map((x) => x.trim()).find(Boolean) ?? null;
  return line && line.length > max ? line.slice(0, max - 1) + "…" : line;
};

// Schlüsseleinheiten kennzeichnet der Athlet im Intervals-Kalender selbst mit dem Stichwort (Tag) #key am Workout.
// Zusätzlich gilt ein #key im Namen oder in der Beschreibung.
const KEY_RE = /#key\b/i;
const isKeySession = (p) => (p?.tags ?? []).some((t) => /^#?key$/i.test(String(t).trim())) || KEY_RE.test(`${p?.name ?? ""}\n${p?.description ?? ""}`);
const stripKey = (s) => (s == null ? null : String(s).replace(/[ \t]*#key\b/gi, "").trim() || null);

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

// Wochenziel der TSS: Summe der geplanten Workouts der Woche aus dem Intervals-Kalender. Fehlt sie dort,
// zaehlt der optionale Wert WEEKLY_TSS_GOAL (Worker-Variable); sonst gibt es bewusst kein Ziel.
export function weeklyGoal(week, env) {
  if (week.plannedLoad > 0) return { goal: week.plannedLoad, source: "plan" };
  const cfg = Number(env?.WEEKLY_TSS_GOAL);
  return Number.isFinite(cfg) && cfg > 0 ? { goal: Math.round(cfg), source: "config" } : { goal: null, source: null };
}

export function buildWidget(d, env = {}) {
  const week = d.weeks[d.weeks.length - 1];
  // Tageslast der laufenden Woche (Mo bis So); Tage nach heute sind null, nicht 0.
  const loadByDate = Object.fromEntries(d.daily.map((x) => [x.date, x.load]));
  const sportsByDate = Object.fromEntries(d.daily.map((x) => [x.date, x.sports]));
  const days = Array.from({ length: 7 }, (_, i) => {
    const date = addDays(week.weekStart, i);
    return { date, load: date > d.today ? null : loadByDate[date] ?? 0, sports: date > d.today ? {} : sportsByDate[date] ?? {} };
  });
  const lastWeek = d.weeks.length > 1 ? d.weeks[d.weeks.length - 2] : null;
  const total = (w) => (w ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null);
  const todayPlan = d.planned.filter((p) => p.date === d.today).map((p) => ({ name: stripKey(p.name), sport: p.sport, key: isKeySession(p), durationMin: p.durationMin, distanceKm: p.distanceKm, load: p.load, purpose: firstLine(stripKey(p.description)) }));
  const next = d.planned.find((p) => p.date > d.today);
  const key = d.planned.find((p) => p.date > d.today && isKeySession(p));
  const recentHip = d.hipFlags.filter((f) => f.date >= new Date(Date.parse(d.today + "T00:00:00Z") - 14 * 86400000).toISOString().slice(0, 10));
  const r = d.summary.readiness;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: { name: d.goal.name, date: d.goal.date, daysToGo: d.goal.daysToGo, targetTimeSecs: d.goal.targetTimeSecs },
    readiness: { verdict: r.verdict, sleepHours: r.sleepHours, hrv: r.hrv, restingHR: r.restingHR, items: r.items.map((i) => ({ label: i.label, v: i.v, cls: i.cls })) },
    load: d.summary.load,
    thresholds: d.summary.thresholds,
    plan: {
      today: todayPlan,
      next: next ? { date: next.date, name: next.name } : null,
      key: key ? { date: key.date, name: stripKey(key.name), sport: key.sport, durationMin: key.durationMin, distanceKm: key.distanceKm } : null,
    },
    week: {
      weekStart: week.weekStart,
      days,
      bySport: Object.fromEntries(Object.entries(week.bySport).map(([k, v]) => [k, { count: v.count, minutes: v.minutes, load: v.load, km: v.km, plannedKm: v.plannedKm }])),
      total: total(week),
      lastTotal: total(lastWeek),
      goal: weeklyGoal(week, env).goal,
      goalSource: weeklyGoal(week, env).source,
      strengthMinutes: week.bySport.strength.minutes,
    },
    hip: { recent: recentHip.length, latestDate: d.hipFlags[0]?.date ?? null },
    hm: d.runalyze ? { goalSec: d.goal.targetTimeSecs, estimates: d.runalyze.hmEstimates, vdot: d.runalyze.vdot } : null,
    sourcesFailed: Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k),
  };
}

// Zweite Widget-Ansicht ("detail", mittleres Widget): Form, Halbmarathon-Zeiten, VDOT/Paces, Schwellen sowie
// Ernaehrung und Heisshunger der letzten Tage. Wieder ohne Freitexte.
export function buildWidgetDetail(d) {
  const from28 = addDays(d.today, -27);
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const form = Array.from({ length: 28 }, (_, i) => {
    const date = addDays(from28, i);
    const w = byDate[date];
    return { date, ctl: w?.ctl ?? null, atl: w?.atl ?? null };
  });
  const last7 = Array.from({ length: 7 }, (_, i) => addDays(d.today, -6 + i));
  const nutrition = last7.map((date) => ({ date, calories: byDate[date]?.calories ?? null, goal: byDate[date]?.calorieGoal ?? null }));
  const recentCravings = d.cravings.filter((c) => c.date >= last7[0]);
  const strongest = recentCravings.filter((c) => c.strength != null).sort((a, b) => b.strength - a.strength)[0];
  const r = d.runalyze;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: { name: d.goal.name, date: d.goal.date, daysToGo: d.goal.daysToGo, targetTimeSecs: d.goal.targetTimeSecs },
    load: d.summary.load,
    tsbZones: d.summary.thresholds.tsb,
    form,
    hm: r ? { goalSec: d.goal.targetTimeSecs, estimates: r.hmEstimates } : null,
    vdot: r ? { value: r.vdot, paces: r.paces, fetchedAt: r.fetchedAt } : null,
    thresholds: d.thresholds,
    nutrition: { days: nutrition, hasData: nutrition.some((x) => x.calories != null) },
    cravings: { count: recentCravings.length, strongest: strongest ? { strength: strongest.strength, time: strongest.time } : null },
    sourcesFailed: Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k),
  };
}

// Dritte Ansicht ("training", mittleres Widget): je Disziplin (Schwimmen, Rad, Lauf) Wochenvolumen gegen Plan,
// Wochenzeit und Tage seit der letzten Einheit, dazu die Verteilung der letzten 4 Wochen nach Trainingszeit.
// CTL, TSB und TSS bleiben sportartuebergreifend (Hauptansicht), hier gibt es bewusst keine Last je Sportart.
// Ein Soll fuer die Verteilung gibt es nur, wenn TRI_SPLIT_TARGET gesetzt ist (Schwimmen,Rad,Lauf in Prozent,
// z. B. "20,45,35"); sonst bleibt es leer, nichts wird erfunden.
const TRI = ["swim", "bike", "run"];

function splitTarget(env) {
  const v = String(env?.TRI_SPLIT_TARGET ?? "").split(",").map((x) => Number(x.trim()));
  if (v.length !== 3 || !v.every((x) => Number.isFinite(x) && x > 0)) return null;
  const sum = v.reduce((a, x) => a + x, 0);
  return Object.fromEntries(TRI.map((k, i) => [k, Math.round((v[i] / sum) * 100)]));
}

export function buildWidgetTraining(d, env = {}) {
  const done = d.weeks.filter((w) => w.complete);
  const cur = d.weeks[d.weeks.length - 1];
  const recent = done.slice(-4);
  const dayDiff = (iso) => Math.round((Date.parse(d.today + "T00:00:00Z") - Date.parse(iso + "T00:00:00Z")) / 86400000);
  const lastDay = (k) => [...d.daily].reverse().find((x) => x.sports?.[k])?.date ?? null;
  const sports = Object.fromEntries(TRI.map((k) => {
    const last = lastDay(k);
    return [k, {
      weekKm: cur.bySport[k].km,
      plannedKm: cur.bySport[k].plannedKm,
      weekMinutes: cur.bySport[k].minutes,
      daysSince: last ? dayDiff(last) : null,
    }];
  }));
  const tot = Object.fromEntries(TRI.map((k) => [k, recent.reduce((a, w) => a + w.bySport[k].minutes, 0)]));
  const sum = TRI.reduce((a, k) => a + tot[k], 0);
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    sports,
    split: { share: sum > 0 ? Object.fromEntries(TRI.map((k) => [k, Math.round((tot[k] / sum) * 100)])) : null, target: splitTarget(env), weeks: recent.length },
    sourcesFailed: Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k),
  };
}

// Kleine Widgets ("small"): Schlaf und Erholung sowie Ernaehrung, je die letzten 7 Tage.
// Fehlende Werte bleiben null (Luecke), 0 kcal zaehlt als keine Daten (siehe buildWellness).
const medianOf = (a) => {
  const v = a.filter((x) => x != null).sort((x, y) => x - y);
  if (!v.length) return null;
  const m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
};

export function buildWidgetSmall(d, goals = null) {
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const days = Array.from({ length: 7 }, (_, i) => addDays(d.today, -6 + i));
  const past14 = d.wellness.filter((w) => w.date >= addDays(d.today, -13));
  const sleepDays = days.map((date) => ({ date, hours: byDate[date]?.sleepHours ?? null, hrv: byDate[date]?.hrv ?? null, restingHR: byDate[date]?.restingHR ?? null }));
  const foodDays = days.map((date) => {
    const w = byDate[date];
    return { date, calories: w?.calories ?? null, goal: w?.calorieGoal ?? null, carbs: w?.carbs ?? null, protein: w?.protein ?? null, fat: w?.fat ?? null };
  });
  const latest = [...foodDays].reverse().find((x) => x.calories != null) ?? null;
  const recentCravings = d.cravings.filter((c) => c.date >= days[0]);
  const strongest = recentCravings.filter((c) => c.strength != null).sort((a, b) => b.strength - a.strength)[0];
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    sleep: {
      days: sleepDays,
      latest: [...sleepDays].reverse().find((x) => x.hours != null) ?? null,
      medianHours: medianOf(past14.map((w) => w.sleepHours)),
      medianHrv: medianOf(past14.map((w) => w.hrv)),
      medianRestingHR: medianOf(past14.map((w) => w.restingHR)),
    },
    food: { days: foodDays, latest, hasData: latest != null, goals },
    cravings: { count: recentCravings.length, strongest: strongest ? { strength: strongest.strength, time: strongest.time } : null },
    sourcesFailed: Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k),
  };
}

export async function handleWidgetRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  const dashboard = await buildDashboard(env);
  const view = new URL(req.url).searchParams.get("view");
  // Tagesziele der Ernaehrung kommen aus Yazio (best effort: fehlt der Zugang oder scheitert die Abfrage, bleibt es null)
  const goals = view === "small" && hasYazioCredentials(env) ? await fetchYazioDailyGoals(env, dashboard.today).catch(() => null) : null;
  const body = view === "detail" ? buildWidgetDetail(dashboard) : view === "small" ? buildWidgetSmall(dashboard, goals) : view === "training" ? buildWidgetTraining(dashboard, env) : buildWidget(dashboard, env);
  return json(body, 200, headers);
}
