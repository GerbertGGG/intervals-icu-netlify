import { json } from "./http-helpers.js";
import { buildDashboard, isAuthorized } from "./dashboard.js";
import { racePhase, raceDistance } from "./race-phase.js";
import { scaleValueCls } from "./dashboard-summary.js";

// Kompakte Sicht auf das Dashboard für das iOS-Widget (Scriptable, public/dashboard/scriptable-widget.js).
// Gleiche Einschätzungen wie die Seite (siehe dashboard-summary.js), aber nur das Wichtigste und
// ohne Freitexte: Es gehen keine Kommentare oder Heißhunger-Einträge raus; nur der Plantext der heutigen Einheit.

// Beschreibung vollständig (nur Leerraum glätten, großzügiges Limit), "Wochenende" als Bezug zum Renntag.
const fullText = (s, max = 240) => {
  const t = String(s ?? "").split(/\r?\n/).map((x) => x.trim()).filter(Boolean).join(" ") || null;
  return t && t.length > max ? t.slice(0, max - 1) + "…" : t;
};
const raceRelative = (text, ph) => {
  if (!text || ph.daysToGo < 0) return text;
  const rel = ph.daysToGo === 0 ? "heute" : `in ${ph.daysToGo} Tag${ph.daysToGo === 1 ? "" : "en"}`;
  return text.replace(/\b(am|zum|fürs|für das) Wochenende\b/gi, (_, p) => (p === "am" ? `am Renntag (${rel})` : `${p} Rennen (${rel})`));
};

const goalBlock = (d) => {
  const dist = raceDistance({ distanceKm: d.goal.distanceKm });
  return { name: d.goal.name, label: dist?.label ?? d.goal.name, date: d.goal.date, daysToGo: d.goal.daysToGo, targetTimeSecs: d.goal.targetTimeSecs, distanceKm: dist?.km ?? d.goal.distanceKm ?? null };
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
    return { date, load: date > d.today ? null : loadByDate[date] ?? 0, isRace: date === d.goal.date };
  });
  const lastWeek = d.weeks.length > 1 ? d.weeks[d.weeks.length - 2] : null;
  const total = (w) => (w ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null);
  const ph = racePhase(d.today, d.goal.date);
  const gb = goalBlock(d);
  // "Marathon" als Titel ist Platzhalter aus dem Plan: Titel folgt der Renndistanz
  const planTitle = (n) => (/^\s*(halb)?marathon\s*$/i.test(n ?? "") ? `Vorbereitung ${gb.label}` : n);
  const todayPlan = d.planned.filter((p) => p.date === d.today).map((p) => ({ name: planTitle(p.name), durationMin: p.durationMin, distanceKm: p.distanceKm, load: p.load, purpose: raceRelative(fullText(p.description), ph) }));
  const next = d.planned.find((p) => p.date > d.today);
  const recentHip = d.hipFlags.filter((f) => f.date >= new Date(Date.parse(d.today + "T00:00:00Z") - 14 * 86400000).toISOString().slice(0, 10));
  const r = d.summary.readiness;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: gb,
    phase: { name: ph.phase, reducedLoad: ph.reducedLoad },
    readiness: { verdict: r.verdict, sleepHours: r.sleepHours, hrv: r.hrv, restingHR: r.restingHR, items: r.items.map((i) => ({ label: i.label, v: i.v, cls: scaleValueCls(i.v, i.max) })) },
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
    goal: goalBlock(d),
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

// Kleine Widgets ("small"): Schlaf und Erholung sowie Ernaehrung, je die letzten 7 Tage.
// Fehlende Werte bleiben null (Luecke), 0 kcal zaehlt als keine Daten (siehe buildWellness).
const medianOf = (a) => {
  const v = a.filter((x) => x != null).sort((x, y) => x - y);
  if (!v.length) return null;
  const m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
};

export function buildWidgetSmall(d) {
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const days = Array.from({ length: 7 }, (_, i) => addDays(d.today, -6 + i));
  const past14 = d.wellness.filter((w) => w.date >= addDays(d.today, -13));
  const sleepDays = days.map((date) => ({ date, hours: byDate[date]?.sleepHours ?? null, hrv: byDate[date]?.hrv ?? null, restingHR: byDate[date]?.restingHR ?? null }));
  const foodDays = days.map((date) => {
    const w = byDate[date];
    return { date, calories: w?.calories ?? null, goal: w?.calorieGoal ?? null, carbs: w?.carbs ?? null, protein: w?.protein ?? null, fat: w?.fat ?? null };
  });
  const latest = [...foodDays].reverse().find((x) => x.calories != null) ?? null;
  // Ernährung richtet sich nach der Phase und dem heutigen Tag; ohne heutige Yazio-Werte gilt nur das Ziel.
  const ph = racePhase(d.today, d.goal.date);
  const todayFood = foodDays[foodDays.length - 1];
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
    food: { days: foodDays, latest, hasData: latest != null, phase: ph.phase, targets: ph.targets, today: todayFood, todayHasData: [todayFood.calories, todayFood.protein, todayFood.carbs].some((x) => x != null) },
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
  return json(view === "detail" ? buildWidgetDetail(dashboard) : view === "small" ? buildWidgetSmall(dashboard) : buildWidget(dashboard, env), 200, headers);
}
