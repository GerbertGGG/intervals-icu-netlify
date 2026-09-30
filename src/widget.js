import { json } from "./http-helpers.js";
import { buildDashboard, isAuthorized } from "./dashboard.js";

// Kompakte Sicht auf das Dashboard für das iOS-Widget (Scriptable, public/dashboard/scriptable-widget.js).
// Gleiche Einschätzungen wie die Seite (siehe dashboard-summary.js), aber nur das Wichtigste und
// ohne Freitexte: Es gehen keine Kommentare, Einheitsnamen oder Heißhunger-Einträge raus.

const firstLine = (s, max = 110) => {
  const line = String(s ?? "").split(/\r?\n/).map((x) => x.trim()).find(Boolean) ?? null;
  return line && line.length > max ? line.slice(0, max - 1) + "…" : line;
};

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
  const days = Array.from({ length: 7 }, (_, i) => {
    const date = addDays(week.weekStart, i);
    return { date, load: date > d.today ? null : loadByDate[date] ?? 0 };
  });
  const lastWeek = d.weeks.length > 1 ? d.weeks[d.weeks.length - 2] : null;
  const total = (w) => (w ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null);
  const todayPlan = d.planned.filter((p) => p.date === d.today).map((p) => ({ name: p.name, durationMin: p.durationMin, distanceKm: p.distanceKm, load: p.load, purpose: firstLine(p.description) }));
  const next = d.planned.find((p) => p.date > d.today);
  const recentHip = d.hipFlags.filter((f) => f.date >= new Date(Date.parse(d.today + "T00:00:00Z") - 14 * 86400000).toISOString().slice(0, 10));
  const r = d.summary.readiness;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: { name: d.goal.name, date: d.goal.date, daysToGo: d.goal.daysToGo, targetTimeSecs: d.goal.targetTimeSecs },
    readiness: { verdict: r.verdict, sleepHours: r.sleepHours, hrv: r.hrv, restingHR: r.restingHR, items: r.items.map((i) => ({ label: i.label, v: i.v, cls: i.cls })) },
    load: d.summary.load,
    thresholds: d.summary.thresholds,
    plan: { today: todayPlan, next: next ? { date: next.date, name: next.name } : null },
    week: {
      weekStart: week.weekStart,
      days,
      bySport: Object.fromEntries(Object.entries(week.bySport).map(([k, v]) => [k, { count: v.count, minutes: v.minutes, load: v.load }])),
      total: total(week),
      lastTotal: total(lastWeek),
      goal: weeklyGoal(week, env).goal,
      goalSource: weeklyGoal(week, env).source,
      strengthCount: week.bySport.strength.count,
    },
    hip: { recent: recentHip.length, latestDate: d.hipFlags[0]?.date ?? null },
    hm: d.runalyze ? { goalSec: d.goal.targetTimeSecs, estimates: d.runalyze.hmEstimates, vdot: d.runalyze.vdot } : null,
    sourcesFailed: Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k),
  };
}

// Zweite Widget-Ansicht ("detail"): Form, Zielkorridor, VDOT/Paces, Schwellen, Wellness-Verlauf sowie
// Ernaehrung und Heisshunger der letzten Tage. Wieder ohne Freitexte.
export function buildWidgetDetail(d) {
  const from28 = addDays(d.today, -27);
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const form = Array.from({ length: 28 }, (_, i) => {
    const date = addDays(from28, i);
    const w = byDate[date];
    return { date, ctl: w?.ctl ?? null, atl: w?.atl ?? null };
  });
  const days14 = Array.from({ length: 14 }, (_, i) => addDays(d.today, -13 + i));
  const metrics = [["mood", "Stimmung"], ["motivation", "Motivation"], ["fatigue", "Ermüdung"], ["soreness", "Muskelkater"], ["sleepQuality", "Schlaf"]];
  const wellnessRows = metrics.map(([key, label]) => {
    const values = days14.map((day) => byDate[day]?.[key] ?? null);
    return { label, values, max: Math.max(2, ...d.wellness.map((w) => w[key] ?? 0)) };
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
    wellness: { days: days14, rows: wellnessRows },
    nutrition: { days: nutrition, hasData: nutrition.some((x) => x.calories != null) },
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
  return json(view === "detail" ? buildWidgetDetail(dashboard) : buildWidget(dashboard, env), 200, headers);
}
