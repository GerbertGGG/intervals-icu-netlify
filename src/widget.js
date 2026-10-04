import { json } from "./http-helpers.js";
import { buildDashboard, isAuthorized } from "./dashboard.js";
import { hasYazioCredentials, fetchYazioDailyGoals } from "./yazio-client.js";
import { readKvJson, writeKvJson } from "./kv.js";
import { isoDateBerlin } from "./date-utils.js";
import { loadWidgetKraft } from "./egym-widget.js";

// Kompakte Sicht auf das Dashboard für das iOS-Widget (Scriptable, public/dashboard/scriptable-widget.js).
// Gleiche Einschätzungen wie die Seite (siehe dashboard-summary.js), aber nur das Wichtigste und
// ohne Freitexte: Es gehen keine Kommentare, Beschreibungen oder Heißhunger-Einträge raus. Jede Ansicht
// bringt alles mit, was sie zeichnet (ein Request je Widget), und liefert fertige Einschätzungen
// (Rennphase, Körper-Ampeln), damit das Skript nur noch darstellt.

const firstLine = (s, max = 110) => {
  const line = String(s ?? "").split(/\r?\n/).map((x) => x.trim()).find(Boolean) ?? null;
  return line && line.length > max ? line.slice(0, max - 1) + "…" : line;
};

// Schlüsseleinheiten kennzeichnet der Athlet im Intervals-Kalender selbst mit dem Stichwort (Tag) #key am Workout.
// Zusätzlich gilt ein #key im Namen oder in der Beschreibung.
const KEY_RE = /#key\b/i;
const isKeySession = (p) => (p?.tags ?? []).some((t) => /^#?key$/i.test(String(t).trim())) || KEY_RE.test(`${p?.name ?? ""}\n${p?.description ?? ""}`);
const stripKey = (s) => (s == null ? null : String(s).replace(/[ \t]*#key\b/gi, "").trim() || null);

// RPE steht im Plan als Freitext ("RPE 2-3"); nur dann gibt es einen Wert, sonst null.
const rpeOf = (s) => { const m = String(s ?? "").match(/\bRPE\s*:?\s*(\d{1,2}(?:\s*[-–]\s*\d{1,2})?)/i); return m ? m[1].replace(/\s*[-–]\s*/, "–") : null; };

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

// Rennphase aus den Tagen bis zum Rennen (Renntag = 0, danach negativ): normal, taper, recovery.
// Dauer je Distanz: Halbmarathon ca. 7 Tage Taper und 7 Tage Erholung (Standard), Mitteldistanz-Triathlon 14 Tage Taper.
export const PHASE_DAYS = { taperFrom: 7, recoveryDays: 7 };
const PHASE_DAYS_BY_DISTANCE = { "5k": { taperFrom: 3, recoveryDays: 3 }, "10k": { taperFrom: 5, recoveryDays: 4 }, hm: PHASE_DAYS, m: { taperFrom: 14, recoveryDays: 14 } };
const PHASE_DAYS_BY_TRIATHLON = { sprint: { taperFrom: 5, recoveryDays: 4 }, olympic: { taperFrom: 7, recoveryDays: 5 }, middle: { taperFrom: 14, recoveryDays: 7 }, long: { taperFrom: 14, recoveryDays: 14 } };
export function phaseDaysFor(race) {
  const tri = race?.triathlon?.format ?? race?.triathlonFormat;
  return (tri && PHASE_DAYS_BY_TRIATHLON[tri]) || PHASE_DAYS_BY_DISTANCE[race?.distance] || PHASE_DAYS;
}
export function racePhase(daysToGo, days = PHASE_DAYS) {
  if (daysToGo == null || daysToGo > days.taperFrom) return "normal";
  if (daysToGo >= 0) return "taper";
  return daysToGo >= -days.recoveryDays ? "recovery" : "normal";
}
// Erholung nach dem letzten Rennen hat Vorrang, auch wenn das naechste Zielrennen schon feststeht.
export function racePhaseFor(goal, recentRace, todayIso) {
  if (recentRace?.date && todayIso && recentRace.date < todayIso) {
    const since = Math.round((Date.parse(todayIso) - Date.parse(recentRace.date)) / 86400000);
    if (since <= phaseDaysFor(recentRace).recoveryDays) return "recovery";
  }
  return racePhase(goal?.daysToGo, phaseDaysFor(goal));
}
const goalOf = (d) => ({ name: d.goal.name, date: d.goal.date, daysToGo: d.goal.daysToGo, targetTimeSecs: d.goal.targetTimeSecs, runKm: d.goal.runKm ?? null, phase: racePhaseFor(d.goal, d.recentRace, d.today) });
const failedOf = (d) => Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k);

// Wochenziel der TSS: Summe der geplanten Workouts der Woche aus dem Intervals-Kalender. Fehlt sie dort,
// zaehlt der optionale Wert WEEKLY_TSS_GOAL (Worker-Variable); sonst gibt es bewusst kein Ziel.
export function weeklyGoal(week, env) {
  if (week.plannedLoad > 0) return { goal: week.plannedLoad, source: "plan" };
  const cfg = Number(env?.WEEKLY_TSS_GOAL);
  return Number.isFinite(cfg) && cfg > 0 ? { goal: Math.round(cfg), source: "config" } : { goal: null, source: null };
}

export function buildWidget(d, env = {}) {
  const week = d.weeks[d.weeks.length - 1];
  const lastWeek = d.weeks.length > 1 ? d.weeks[d.weeks.length - 2] : null;
  const total = (w) => (w ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null);
  const todayPlan = d.planned.filter((p) => p.date === d.today).map((p) => ({ name: stripKey(p.name), sport: p.sport, key: isKeySession(p), durationMin: p.durationMin, distanceKm: p.distanceKm, load: p.load, purpose: firstLine(stripKey(p.description), 160), rpe: rpeOf(p.description) }));
  const next = d.planned.find((p) => p.date > d.today);
  // Die naechsten drei Tage fuer die Kacheln; ohne Kalender (Quelle ausgefallen) null statt "frei"
  const horizon = addDays(d.today, 3);
  const upcoming = d.sources.intervalsEvents?.ok === false ? null : d.planned.filter((p) => p.date > d.today && p.date <= horizon).map((p) => ({ date: p.date, name: stripKey(p.name), sport: p.sport, durationMin: p.durationMin, key: isKeySession(p) }));
  const r = d.summary.readiness;
  const wk = weeklyGoal(week, env);
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: goalOf(d),
    readiness: { verdict: r.verdict, sleepHours: r.sleepHours, hrv: r.hrv, restingHR: r.restingHR, body: { sleep: r.body.sleep, hrv: r.body.hrv, resting: r.body.resting }, items: r.items.map((i) => ({ label: i.label, v: i.v, max: i.max, cls: i.cls })) },
    plan: { today: todayPlan, next: next ? { date: next.date, name: next.name } : null, upcoming },
    week: { total: total(week), lastTotal: total(lastWeek), goal: wk.goal, goalSource: wk.source },
    sourcesFailed: failedOf(d),
  };
}

// Zweite Widget-Ansicht ("detail", mittleres Widget): Form, Halbmarathon-Zeiten, VDOT/Paces und Schwellen.
// Ernaehrung hat ein eigenes Widget (small), Heisshunger bleibt auf der Dashboard-Seite. Wieder ohne Freitexte.
export function buildWidgetDetail(d, env = {}) {
  const from28 = addDays(d.today, -27);
  // Wochen-TSS (alle Sportarten) der letzten 6 Wochen, aelteste zuerst; die laufende Woche ist unvollstaendig (complete: false).
  // Ziel nur fuer die laufende Woche (Plan oder WEEKLY_TSS_GOAL); fehlende Wochen bleiben null, nie 0.
  const tssWeeks = d.weeks.slice(-6).map((w, i, arr) => {
    const hasData = Object.values(w.bySport).some((s) => s.count > 0);
    return { weekStart: w.weekStart, tss: hasData ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null, complete: w.complete, goal: i === arr.length - 1 ? weeklyGoal(w, env).goal : null };
  });
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const form = Array.from({ length: 28 }, (_, i) => {
    const date = addDays(from28, i);
    const w = byDate[date];
    return { date, ctl: w?.ctl ?? null, atl: w?.atl ?? null };
  });
  const r = d.runalyze;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: goalOf(d),
    load: d.summary.load,
    tsbZones: d.summary.thresholds.tsb,
    form,
    tssWeeks,
    hm: r ? { goalSec: d.goal.targetTimeSecs, estimates: r.hmEstimates } : null,
    vdot: r ? { value: r.vdot, paces: r.paces, fetchedAt: r.fetchedAt } : null,
    thresholds: d.thresholds,
    sourcesFailed: failedOf(d),
  };
}

// Vierte Ansicht ("vdot", mittleres Widget): VDOT mit Verlauf und die Trainingsbereiche (Paces) daraus.
// Runalyze liefert nur das VDOT; die Bereiche (Pace von-bis) rechnet der Worker wie die Runalyze-Lauftabellen (siehe vdot.js).
// Verlauf = VDOT je Snapshot-Tag der letzten 12 Wochen (aus dem Runalyze-Verlauf), Veraenderung gegen den aeltesten Punkt.
export function buildWidgetVdot(d) {
  const r = d.runalyze;
  if (!r || r.vdot == null) return { generatedAt: d.generatedAt, today: d.today, vdot: null, sourcesFailed: failedOf(d) };
  const from = addDays(d.today, -83);
  const hist = (r.hmHistory ?? []).filter((e) => e.vdot != null && e.date >= from).map((e) => ({ date: e.date, vdot: Math.round(e.vdot * 10) / 10 }));
  const first = hist[0];
  const spanOk = first != null && first.date <= addDays(d.today, -7);
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    vdot: {
      value: r.vdot,
      fetchedAt: r.fetchedAt,
      history: hist,
      // Veraenderung erst ab 7 Tagen Abstand zum aeltesten Punkt, sonst ist es kein Trend; historySince zeigt, ab wann es Verlauf gibt
      delta: spanOk ? Math.round((r.vdot - first.vdot) * 10) / 10 : null,
      deltaSince: spanOk ? first.date : null,
      historySince: first?.date ?? null,
      paces: (r.paces ?? []).map((p) => ({ key: p.key, label: p.label, pct: p.pct, pace: p.pace, secPerKm: p.secPerKm, fastPace: p.fastPace, slowPace: p.slowPace })),
    },
    sourcesFailed: failedOf(d),
  };
}

// Dritte Ansicht ("training", mittleres Widget): je Disziplin (Schwimmen, Rad, Lauf) Wochen-TSS gegen Plan,
// Wochenzeit, Kilometer und Tage seit der letzten Einheit, dazu die Verteilung der letzten 4 Wochen nach Trainingszeit.
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

// Prozentanteile, die sich exakt auf 100 summieren (groesster Rest).
function sharesTo100(tot, keys) {
  const sum = keys.reduce((a, k) => a + tot[k], 0);
  const raw = keys.map((k) => (tot[k] / sum) * 100);
  const out = raw.map(Math.floor);
  let rest = 100 - out.reduce((a, v) => a + v, 0);
  raw.map((v, i) => [v - out[i], i]).sort((a, b) => b[0] - a[0]).forEach(([, i]) => { if (rest-- > 0) out[i] += 1; });
  return Object.fromEntries(keys.map((k, i) => [k, out[i]]));
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
      weekLoad: cur.bySport[k].load,
      plannedLoad: cur.bySport[k].plannedLoad,
      weekKm: cur.bySport[k].km,
      weekMinutes: cur.bySport[k].minutes,
      daysSince: last ? dayDiff(last) : null,
    }];
  }));
  const tot = Object.fromEntries(TRI.map((k) => [k, recent.reduce((a, w) => a + w.bySport[k].minutes, 0)]));
  const sum = TRI.reduce((a, k) => a + tot[k], 0);
  // Intensitaet nach Zonen (locker, mittel, hart): laufende Woche und Schnitt der letzten 4 Wochen inkl. laufender
  const INT = ["easy", "mid", "hard"];
  const intShare = (ws) => {
    const tot = Object.fromEntries(INT.map((k) => [k, ws.reduce((a, w) => a + (w.intensity?.[k] ?? 0), 0)]));
    return INT.reduce((a, k) => a + tot[k], 0) > 0 ? sharesTo100(tot, INT) : null;
  };
  const intensity = { week: intShare([cur]), avg: intShare(d.weeks.slice(-4)) };
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    sports,
    intensity,
    split: { share: sum > 0 ? Object.fromEntries(TRI.map((k) => [k, Math.round((tot[k] / sum) * 100)])) : null, target: splitTarget(env), weeks: recent.length },
    sourcesFailed: failedOf(d),
  };
}

// Kleine Widgets ("small"): Schlaf und Erholung sowie Ernaehrung, je die letzten 7 Tage.
// Fehlende Werte bleiben null (Luecke), 0 kcal zaehlt als keine Daten (siehe buildWellness).
// Fitness (CTL) der letzten 6 Wochen: ein Punkt je Woche (heute, -7, ... -42 Tage), fehlende Wochen null.
// Veraenderung = heute gegen den aeltesten vorhandenen Wochenpunkt (deltaWeeks Wochen zurueck); ohne beide keine.
function buildFitness(d, byDate) {
  const weekly = Array.from({ length: 7 }, (_, i) => {
    const w = byDate[addDays(d.today, -42 + i * 7)];
    return w?.ctl != null ? Math.round(w.ctl * 10) / 10 : null;
  });
  const now = [...d.wellness].reverse().find((w) => w.date <= d.today && w.ctl != null)?.ctl ?? null;
  weekly[6] = now != null ? Math.round(now * 10) / 10 : weekly[6];
  const from = weekly.slice(0, 6).findIndex((v) => v != null);
  const ok = from >= 0 && weekly[6] != null;
  // TSB (Form) = CTL - ATL an denselben Wochenpunkten; heute aus dem juengsten Tag mit beiden Werten
  const tsbOf = (w) => (w?.ctl != null && w?.atl != null ? Math.round((w.ctl - w.atl) * 10) / 10 : null);
  const tsbWeekly = Array.from({ length: 7 }, (_, i) => tsbOf(byDate[addDays(d.today, -42 + i * 7)]));
  const latest = [...d.wellness].reverse().find((w) => w.date <= d.today && tsbOf(w) != null);
  if (latest) tsbWeekly[6] = tsbOf(latest);
  return { ctl: weekly[6], delta: ok ? Math.round(weekly[6] - weekly[from]) : null, deltaWeeks: ok ? 6 - from : null, weekly, tsb: tsbWeekly[6], tsbWeekly };
}

export function buildWidgetSmall(d, goals = null) {
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const days = Array.from({ length: 7 }, (_, i) => addDays(d.today, -6 + i));
  const body = d.summary.readiness.body;
  const sleepDays = days.map((date) => ({ date, hours: byDate[date]?.sleepHours ?? null, hrv: byDate[date]?.hrv ?? null, restingHR: byDate[date]?.restingHR ?? null }));
  const foodDays = days.map((date) => {
    const w = byDate[date];
    return { date, calories: w?.calories ?? null, goal: w?.calorieGoal ?? null, carbs: w?.carbs ?? null, protein: w?.protein ?? null, fat: w?.fat ?? null };
  });
  const latest = [...foodDays].reverse().find((x) => x.calories != null) ?? null;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: goalOf(d),
    sleep: {
      days: sleepDays,
      latest: [...sleepDays].reverse().find((x) => x.hours != null) ?? null,
      // Mediane der 14 Tage vor heute, dieselben wie in der Bereitschaft (heute zaehlt nicht mit)
      medianHours: body.sleepMedian,
      medianHrv: body.hrvMedian,
      medianRestingHR: body.restingMedian,
    },
    food: { days: foodDays, latest, hasData: latest != null, goals },
    fitness: buildFitness(d, byDate),
    sourcesFailed: failedOf(d),
  };
}

// Das Dashboard wird fuer alle Widgets kurz in KV gehalten: Jede Ansicht ruft den Worker einzeln ab, ohne Cache
// wuerde jedes Widget alle Quellen neu laden. Mit ?fresh=1 wird der Cache uebergangen.
const CACHE_KEY = "widget:dashboard-cache";
export const CACHE_MS = 5 * 60 * 1000;
async function dashboardCached(env, fresh) {
  const today = isoDateBerlin();
  if (!fresh) {
    const c = await readKvJson(env, CACHE_KEY).catch(() => null);
    if (c?.data && c.data.today === today && Date.now() - c.at < CACHE_MS) return c.data;
  }
  const data = await buildDashboard(env, today);
  // Nur vollstaendige Staende cachen: Ein Ausfall soll nicht fuenf Minuten lang haengen bleiben
  if (failedOf(data).length === 0) await writeKvJson(env, CACHE_KEY, { at: Date.now(), data }).catch(() => {});
  return data;
}

export async function handleWidgetRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  const params = new URL(req.url).searchParams;
  const view = params.get("view");
  // Kraft kommt allein aus EGYM, ohne das Dashboard (Intervals, Yazio, Runalyze) zu laden
  if (view === "kraft") return json(await loadWidgetKraft(env, params.get("fresh") === "1"), 200, headers);
  const dashboard = await dashboardCached(env, params.get("fresh") === "1");
  // Tagesziele der Ernaehrung kommen aus Yazio (best effort: fehlt der Zugang oder scheitert die Abfrage, bleibt es null)
  const goals = view === "small" && hasYazioCredentials(env) ? await fetchYazioDailyGoals(env, dashboard.today).catch(() => null) : null;
  const body = view === "detail" ? buildWidgetDetail(dashboard, env) : view === "small" ? buildWidgetSmall(dashboard, goals) : view === "training" ? buildWidgetTraining(dashboard, env) : view === "vdot" ? buildWidgetVdot(dashboard) : buildWidget(dashboard, env);
  return json(body, 200, headers);
}
