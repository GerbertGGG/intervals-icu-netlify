import { json } from "./http-helpers.js";
import { buildDashboard, isAuthorized } from "./dashboard.js";

// Kompakte Sicht auf das Dashboard für das iOS-Widget (Scriptable, public/dashboard/scriptable-widget.js).
// Gleiche Einschätzungen wie die Seite (siehe dashboard-summary.js), aber nur das Wichtigste und
// ohne Freitexte: Es gehen keine Kommentare, Einheitsnamen oder Heißhunger-Einträge raus.

const firstLine = (s, max = 110) => {
  const line = String(s ?? "").split(/\r?\n/).map((x) => x.trim()).find(Boolean) ?? null;
  return line && line.length > max ? line.slice(0, max - 1) + "…" : line;
};

export function buildWidget(d) {
  const week = d.weeks[d.weeks.length - 1];
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
      bySport: Object.fromEntries(Object.entries(week.bySport).map(([k, v]) => [k, { count: v.count, minutes: v.minutes, load: v.load }])),
      total: total(week),
      lastTotal: total(lastWeek),
      strengthCount: week.bySport.strength.count,
    },
    hip: { recent: recentHip.length, latestDate: d.hipFlags[0]?.date ?? null },
    hm: d.runalyze ? { goalSec: d.goal.targetTimeSecs, estimates: d.runalyze.hmEstimates, vdot: d.runalyze.vdot } : null,
    sourcesFailed: Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k),
  };
}

export async function handleWidgetRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  return json(buildWidget(await buildDashboard(env)), 200, headers);
}
