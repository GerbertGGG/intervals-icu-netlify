import { json } from "./http-helpers.js";
import { buildDashboard, isAuthorized, timingSafeEqual } from "./dashboard.js";
import { hasYazioCredentials, fetchYazioDailyGoals, fetchYazioDailyNutrition } from "./yazio-client.js";
import { readKvJson, writeKvJson } from "./kv.js";
import { diffDays, isoDateBerlin } from "./date-utils.js";
import { loadWidgetKraft } from "./egym-widget.js";
import { isFreshCoach } from "./coach-snapshot.js";
import { DISC_SPORT } from "./week-totals.js";

const ENDURANCE_SPORTS = ["run", "bike", "swim"];

// Kompakte Sicht auf das Dashboard für das iOS-Widget (Scriptable, public/dashboard/scriptable-widget.js).
// Gleiche Werte wie die Seite (siehe dashboard-summary.js), aber nur das Wichtigste und ohne Freitexte: Es gehen keine
// Kommentare, Beschreibungen oder Heißhunger-Einträge raus. Jede Ansicht bringt alles mit, was sie zeichnet (ein Request je
// Widget). Ein Urteil über den Tag gibt es nur vom Coach (Morgen-Auswertung, save_coach), nicht aus eigenen Regeln.

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
const PHASE_DAYS = { taperFrom: 7, recoveryDays: 7 };
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
// Urteil des Coaches (go) als Farbe: hart = gruen, locker = gelb, frei = rot; ohne Urteil keine Farbe
const GO_CLS = { hart: "ok", locker: "warn", frei: "bad" };
const freshCoach = (d) => (isFreshCoach(d.coach, Date.parse(d.generatedAt)) ? d.coach : null);
const goalOf = (d) => ({ name: d.goal.name, date: d.goal.date, daysToGo: d.goal.daysToGo, targetTimeSecs: d.goal.targetTimeSecs, runKm: d.goal.runKm ?? null, phase: racePhaseFor(d.goal, d.recentRace, d.today) });
const failedOf = (d) => Object.entries(d.sources).filter(([, s]) => !s.ok).map(([k]) => k);

// Wochenziel der TSS: Summe der geplanten Workouts der Woche aus dem Intervals-Kalender. Fehlt sie dort,
// zaehlt der optionale Wert WEEKLY_TSS_GOAL (Worker-Variable); sonst gibt es bewusst kein Ziel.
// Hat die Woche Wochenziele des Coaches (Zeit + TSS-Obergrenze), ist TSS kein Ziel mehr: Die geplanten Workouts
// (z. B. nur Kraft) waeren dann ein falsches Soll, es zaehlen die Wochenziele (weekTargets).
function weeklyGoal(week, env) {
  if (week.plannedLoad > 0 && !hasTargets(week)) return { goal: week.plannedLoad, source: "plan" };
  const cfg = Number(env?.WEEKLY_TSS_GOAL);
  return Number.isFinite(cfg) && cfg > 0 ? { goal: Math.round(cfg), source: "config" } : { goal: null, source: null };
}

// Wochenziele des Coaches je Sportart (Intervals-Kalender, Kategorie TARGET, jeden Montag neu): Zeit ist das Ziel,
// TSS ein Korridor. Schwimmen meist nur mit Zeit. s Sportart, min erledigt, goal Ziel-Minuten, tss erledigt,
// max TSS-Obergrenze (null = keine), lo TSS-Untergrenze (null = keine), over = ueber der Obergrenze, under = unter der
// Untergrenze. Ohne Ziele leer. (min ist hier die erledigte Zeit, deshalb heisst die Untergrenze lo.)
const TARGET_SPORTS = ["run", "bike", "swim", "strength", "other"];
const hasTargets = (week) => Boolean(week && (week.targetMin > 0 || week.targetLoad > 0));
export function weekTargets(week) {
  if (!hasTargets(week)) return [];
  return TARGET_SPORTS.map((k) => {
    const s = week.bySport[k];
    if (!s || !(s.targetMin > 0 || s.targetLoad > 0)) return null;
    const max = s.targetLoad > 0 ? s.targetLoad : null, lo = max != null && s.targetLoadMin > 0 ? s.targetLoadMin : null;
    return { s: k, min: Math.round(s.minutes), goal: s.targetMin > 0 ? s.targetMin : null, tss: Math.round(s.load), max, lo, over: max != null && s.load > max, under: lo != null && s.load < lo };
  }).filter(Boolean);
}

// Woche fuer das grosse Widget wie auf der Uhr: Ringe ZEIT (Minuten gegen das Zeitziel) und TSS (gegen die Obergrenze),
// je [Sportart, erledigt, Vorgabe] fuer Rad, Lauf, Schwimmen, beim TSS-Ring als viertes Element die Untergrenze; open = was noch fehlt (Minuten je Sportart, Kraft in Saetzen
// aus dem Block-Ziel), v = 0 heisst erreicht; daysLeft = Tage bis Sonntag inkl. heute. Ohne Zeitziele leer.
function weekOpen(d, week) {
  const targets = weekTargets(week), zeit = watchRings(targets, "min", "goal");
  if (!zeit.length) return {};
  const open = ["run", "bike", "swim"].map((k) => zeit.find((x) => x[0] === k)).filter(Boolean).map(([s, done, goal]) => ({ s, v: Math.max(0, goal - done), unit: "min" }));
  const st = strengthWeek(d.seasonBlock?.goals?.find((g) => g.key === "strength"));
  if (st) open.push({ s: "strength", v: Math.max(0, st.of - st.v), unit: "Sätze" });
  const tss = watchRings(targets, "tss", "max").map((x) => [...x, targets.find((t) => t.s === x[0]).lo]);
  return { rings: { zeit, tss }, open, daysLeft: 7 - dayIndex(d.today) };
}

function weekGoals(d) {
  const T = d.weekTotals, c = T?.weeks?.[T.weeks.length - 1];
  return c ? { min: c.enduranceMin, goalMin: Math.round(T.goals.enduranceHours * 60), sets2w: c.strengthSets2w, goalSets2w: T.goals.strengthSets2w } : null;
}

export function buildWidget(d, env = {}) {
  const week = d.weeks[d.weeks.length - 1];
  const lastWeek = d.weeks.length > 1 ? d.weeks[d.weeks.length - 2] : null;
  const total = (w) => (w ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null);
  const todayPlan = d.planned.filter((p) => p.date === d.today).map((p) => ({ name: stripKey(p.name), sport: p.sport, key: isKeySession(p), durationMin: p.durationMin, distanceKm: p.distanceKm, load: p.load, purpose: firstLine(stripKey(p.description), 160), rpe: rpeOf(p.description), steps: (p.steps ?? []).slice(0, 40).map((x) => ({ secs: x.secs, pct: x.pct ?? null })) }));
  const next = d.planned.find((p) => p.date > d.today);
  // Die naechsten drei Tage fuer die Kacheln; ohne Kalender (Quelle ausgefallen) null statt "frei"
  const horizon = addDays(d.today, 3);
  const upcoming = d.sources.intervalsEvents?.ok === false ? null : d.planned.filter((p) => p.date > d.today && p.date <= horizon).map((p) => ({ date: p.date, name: stripKey(p.name), sport: p.sport, durationMin: p.durationMin, key: isKeySession(p) }));
  const r = d.summary.recovery;
  const c = freshCoach(d);
  const coach = c ? { note: c.note, headline: c.headline, go: c.go ?? null, blockers: c.blockers ?? [], question: c.question ?? null, answer: c.answer ? true : false, today: isoDateBerlin(new Date(c.fetchedAt)) === d.today } : null;
  const wk = weeklyGoal(week, env);
  // Woche Mo bis So fuer die Kapseln im grossen Widget: erledigter Load je Sportart und die geplanten Einheiten des Tages
  const monday = addDays(d.today, -((new Date(d.today + "T00:00:00Z").getUTCDay() + 6) % 7));
  const dailyBy = Object.fromEntries((d.daily ?? []).map((x) => [x.date, x]));
  const weekPlan = d.plannedWeek ?? d.planned;
  const weekDays = Array.from({ length: 7 }, (_, i) => {
    const date = addDays(monday, i);
    return { date, sports: dailyBy[date]?.sports ?? {}, plan: weekPlan.filter((p) => p.date === date).map((p) => ({ sport: p.sport, load: p.load ?? null })) };
  });
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    goal: goalOf(d),
    // Erholungswerte ohne eigene Ampel. verdict traegt die Kopfzeile des Coaches, die Farbe kommt aus seinem Urteil (go),
    // ohne Urteil "coach", ohne Aussage von heute "none"; line bleibt null.
    // Die Form (verdict, line, body.*) bleibt fuer aeltere Fassungen des Skripts erhalten.
    readiness: {
      verdict: coach ? { cls: GO_CLS[coach.go] ?? "coach", text: coach.headline ?? coach.note } : { cls: "none", text: "Noch keine Coach-Auswertung" }, line: null,
      sleepHours: r.sleepHours, hrv: r.hrv, restingHR: r.restingHR,
      body: { sleep: { score: r.sleepScore, median: r.body.sleepMedian }, hrv: { median: r.body.hrvMedian }, resting: { median: r.body.restingMedian } },
      items: r.items.map((i) => ({ label: i.label, v: i.v, max: i.max, med: i.med ?? null, cls: i.cls })),
    },
    // next.items: alle Einheiten des naechsten Tages mit Plan (z. B. Kraft und Rad am selben Tag)
    plan: { today: todayPlan, next: next ? { date: next.date, name: next.name, items: d.planned.filter((p) => p.date === next.date).map((p) => ({ name: stripKey(p.name), sport: p.sport, key: isKeySession(p) })) } : null, upcoming },
    // targets: Wochenziele des Coaches (Zeit + TSS-Korridor je Sportart); leer, wenn es keine gibt.
    // rings/open/daysLeft: Woche wie auf der Uhr (Ringe ZEIT und TSS, was noch offen ist), nur mit Zeitzielen
    // goals: Wochenziele des Coaches wie die Karte "Woche" (week-totals.js, save_targets): Ausdauerminuten ohne Kraft gegen das
    // Wochenziel, Kraftsaetze laufende Woche plus Vorwoche gegen das 2-Wochen-Ziel; ohne Aktivitaeten null
    week: { total: total(week), lastTotal: total(lastWeek), goal: wk.goal, goalSource: wk.source, tsb: d.summary.load.tsb, days: weekDays, targets: weekTargets(week), ...weekOpen(d, week), goals: weekGoals(d) },
    // Hinweis des Coaches, nur solange er frisch ist; sonst steht im Widget der Zweck aus dem Plan.
    // coach.today: Aussage stammt von heute (sonst laedt das Widget morgens frueher nach)
    coach,
    sourcesFailed: failedOf(d),
  };
}

// Zweite Widget-Ansicht ("detail", mittleres Widget): Form, Halbmarathon-Zeiten, VDOT/Paces und Schwellen.
// Ernaehrung hat ein eigenes Widget (small), Heisshunger bleibt auf der Dashboard-Seite. Wieder ohne Freitexte.
export function buildWidgetDetail(d, env = {}) {
  const from28 = addDays(d.today, -27);
  // Wochen-TSS (alle Sportarten) und Ausdauerzeit (ohne Kraft) der letzten 6 Wochen, aelteste zuerst; die laufende Woche ist unvollstaendig
  // (complete: false). Ziele wie im Dashboard: TSS-Obergrenze bzw. Zeitziel des Coaches (cap: true, goalMin = Untergrenze
  // des TSS-Korridors), sonst der Plan aus dem Kalender; fuer die laufende Woche ohne beides WEEKLY_TSS_GOAL. Ohne Ziel null, nie 0.
  const tssWeeks = d.weeks.slice(-6).map((w, i, arr) => {
    const hasData = Object.values(w.bySport).some((s) => s.count > 0);
    // TSS je Sportart fuer die Fuellung in Sportfarben (nur Sportarten mit Last)
    const sports = Object.fromEntries(Object.entries(w.bySport).filter(([, v]) => v.load > 0).map(([k, v]) => [k, Math.round(v.load)]));
    // Ausdauerminuten je Sportart fuer das Zeit-Widget (gleiche Fuellung in Sportfarben): Laufen, Rad, Schwimmen aus der
    // Wochenrechnung (week-totals.js), Kraft zaehlt nicht; Ziel = Zeitziele bzw. Plan nur dieser drei Sportarten
    const et = (d.weekTotals?.weeks ?? []).find((x) => x.week === w.weekStart);
    const minSports = et ? Object.fromEntries(Object.entries(et.byDisciplineMin).filter(([, v]) => v > 0).map(([k, v]) => [DISC_SPORT[k], v])) : {};
    const endSum = (f) => ENDURANCE_SPORTS.reduce((a, k) => (w.bySport[k][f] > 0 ? (a ?? 0) + w.bySport[k][f] : a), null);
    const cap = w.targetLoad > 0, plannedMin = endSum("plannedMin"), targetMin = endSum("targetMin");
    const goal = cap ? w.targetLoad : i === arr.length - 1 ? weeklyGoal(w, env).goal : w.plannedLoad > 0 && !hasTargets(w) ? w.plannedLoad : null;
    return {
      weekStart: w.weekStart, tss: hasData ? Object.values(w.bySport).reduce((a, s) => a + s.load, 0) : null, sports, minSports, complete: w.complete, goal, cap,
      goalMin: cap && w.targetLoadMin > 0 ? w.targetLoadMin : null,
      min: et && hasData ? et.enduranceMin : null, minGoal: targetMin > 0 ? targetMin : plannedMin > 0 ? plannedMin : null,
      // Ausdauerstunden aus dem Rahmen des Coaches (save_targets), der in dieser Woche galt; als Linie im Zeit-Widget
      minFrame: et?.goalHours > 0 ? Math.round(et.goalHours * 60) : null,
    };
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
// Wochenzeit, Stunden der letzten 4 Wochen und Tage seit der letzten Einheit, dazu die Verteilung der letzten 4 Wochen nach Trainingszeit.
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
    // Umfang der letzten 4 vollen Wochen gegen die 4 davor (Entwicklung); ohne Vergleichswert null
    const minOf = (ws) => ws.reduce((a, w) => a + w.bySport[k].minutes, 0);
    const prior = done.slice(-8, -4), min4 = minOf(recent), minPrior = minOf(prior);
    return [k, {
      min4: Math.round(min4),
      trendPct: prior.length === 4 && minPrior > 0 ? Math.round((min4 / minPrior - 1) * 100) : null,
      weekLoad: cur.bySport[k].load,
      plannedLoad: cur.bySport[k].plannedLoad,
      weekMinutes: cur.bySport[k].minutes,
      // Wochenziel des Coaches: Minuten als Ziel, TSS als Korridor von targetLoadMin bis targetLoad (je null ohne Eintrag)
      targetMin: cur.bySport[k].targetMin ?? null,
      targetLoad: cur.bySport[k].targetLoad ?? null,
      targetLoadMin: cur.bySport[k].targetLoadMin ?? null,
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

export function buildWidgetSmall(d, goals = null, live = null) {
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const days = Array.from({ length: 7 }, (_, i) => addDays(d.today, -6 + i));
  const body = d.summary.recovery.body;
  const sleepDays = days.map((date) => ({ date, hours: byDate[date]?.sleepHours ?? null, hrv: byDate[date]?.hrv ?? null, restingHR: byDate[date]?.restingHR ?? null }));
  const foodDays = days.map((date) => {
    // Heute direkt aus Yazio (live), sonst waere der Stand bis zu 15 Min (Sync) plus 5 Min (Cache) alt
    const w = date === d.today && live?.energyKcal > 0 ? { ...byDate[date], calories: Math.round(live.energyKcal), carbs: Math.round(live.carbG * 10) / 10, protein: Math.round(live.proteinG * 10) / 10, fat: Math.round(live.fatG * 10) / 10 } : byDate[date];
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
      // Mediane der 28 Tage vor heute, dieselben wie in den Erholungswerten (heute zaehlt nicht mit)
      medianHours: body.sleepMedian,
      medianHrv: body.hrvMedian,
      medianRestingHR: body.restingMedian,
    },
    food: { days: foodDays, latest, hasData: latest != null, goals },
    fitness: buildFitness(d, byDate),
    sourcesFailed: failedOf(d),
  };
}

// Trainingsblock aus dem Kalender mit dem Fortschritt der Ziele (Abschlussziele Lauf/Rad/Schwimmen, Leitplanken; aeltere Bloecke: Decoupling, Kraft, ACWR). Das Widget stellt nur dar:
// Status (ok/warn/bad/base/none), Werte und Ziele kommen fertig aus dem Dashboard (season-block.js).
export function buildWidgetBlock(d) {
  const b = d.seasonBlock;
  if (!b) return { generatedAt: d.generatedAt, today: d.today, block: null, sourcesFailed: failedOf(d) };
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    block: {
      name: b.name, status: b.status, start: b.start, end: b.end, weeks: b.weeks, weekNo: b.weekNo, daysToStart: b.daysToStart, daysLeft: b.daysLeft,
      // Ramp Rate und ACWR nicht als eigene Ziele (wie im Dashboard): nur als Hinweis im Rahmen-Widget, wenn sie hoch sind
      goals: (b.goals ?? []).filter((g) => g.key !== "ramp" && g.key !== "acwr").map((g) => ({
        key: g.key, group: g.group ?? null, sport: g.sport ?? null, label: g.label, status: g.status, value: g.value ?? null, of: g.of ?? null, target: g.target, note: g.note ?? null, basis: g.basis ?? null, goalSets: g.goalSets ?? null,
        max: g.max ?? null, must: g.must ?? null, count: g.count ?? null, lastN: g.lastN ?? null,
        // Rad: letzter Ausgangswert (baseline), zaehlt nicht fuers Ziel, nur Orientierung
        orientation: g.orientation ?? null,
        // Verlauf der Abschlussziele (season-block.js goalCourse): Richtung besser/schlechter/gleich, keine Entscheidung
        course: g.course && !g.course.open ? { dir: g.course.dir ?? null } : null,
        weeks: g.key === "strength" ? (g.weeks ?? []).slice(-6).map((w) => ({ done: w.done, hit: w.hit, count: w.count, load: w.load !== false })) : undefined,
      })),
    },
    sourcesFailed: failedOf(d),
  };
}

// Rahmen-Widget (mittel, Parameter "rahmen"): der aktuelle Rahmen des Coaches (save_targets) – Intensitaet je Sportart mit dem
// Stand der laufenden Blockwoche (aus der Intensitaets-Leitplanke, season-block.js), Leitplanken, Nicht erlaubt, naechster
// Blocktest und der stille Hinweis zu Ramp Rate/ACWR (loadHint, nur wenn hoch). Nur Anzeige, keine eigene Bewertung.
const INT_KIND_SPORT = { bikeHiit: "rad", hillSprints: "lauf" };
// kurzer Grund, warum die Woche nicht zaehlt: "Woche 1, zählt erst ab Woche 2" -> "ab Woche 2", Entlastung -> "Entlastung"
const skipShort = (t) => { const m = /ab\s*woche\s*(\d+)/i.exec(t ?? ""); return m ? `ab Woche ${m[1]}` : /entlast/i.test(t ?? "") ? "Entlastung" : "zählt nicht"; };
export function buildWidgetRahmen(d) {
  const c = d.targets?.current ?? null;
  const g = d.seasonBlock?.goals?.find((x) => x.key === "intensity") ?? null;
  const open = g?.weeks?.find((x) => !x.done) ?? null;
  // je Sportart: diese Woche schon gemacht (count), oder warum die Woche nicht zaehlt (skip); null ohne Leitplanke
  const week = g && open ? Object.fromEntries(g.kinds.map((k) => [INT_KIND_SPORT[k], { count: open.has?.[k] ?? 0, skip: open.load === false ? skipShort(open.skip) : null }])) : null;
  // naechster geplanter Rahmen (save_targets mit validFrom in der Zukunft): ab wann und warum, ohne den Vorspann "Gerberts Entscheidung … am …:"
  const nx = d.targets?.upcoming?.[0] ?? null;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    frame: c ? {
      enduranceHoursPerWeek: c.enduranceHoursPerWeek, strengthSets2w: c.strengthSets2w, maxIncreasePct: c.maxIncreasePct,
      intensity: (c.intensity ?? []).map((x) => ({ ...x, week: week?.[x.sport] ?? null })),
      guardrails: c.guardrails ?? [], notAllowed: c.notAllowed ?? [], reason: c.reason, savedAt: c.savedAt ?? null, validFrom: c.validFrom,
    } : null,
    next: nx ? { validFrom: nx.validFrom, reason: String(nx.reason ?? "").replace(/^[^:]{0,80}\bam\s*\d{1,2}\.\d{1,2}\.(?:\d{2,4})?\s*:\s*/, "") } : null,
    nextTest: d.nextTest ?? null,
    loadHint: d.loadHint ?? null,
    sourcesFailed: failedOf(d),
  };
}

// Block-Widget neu (mittel, view=kompass): Block, Rahmen und Experiment in einer Antwort, damit ein Widget alles zeigt,
// was im laufenden Block gilt. Nur zusammengesetzt, keine eigene Auswertung; die Einzelansichten bleiben fuer aeltere Skripte.
export function buildWidgetKompass(d) {
  return { ...buildWidgetExperiment(d), ...buildWidgetRahmen(d), ...buildWidgetBlock(d) };
}

// Experiment-Widget (mittel, Parameter "experiment"): das vom Coach gespeicherte Experiment (save_experiment), die Ausdauerstunden
// je Woche gegen das Wochenziel und die live gerechneten Trainingstage. Nur Werte, keine Bewertung; weekNo = Woche seit startDate (1 = erste Woche).
export function buildWidgetExperiment(d) {
  const x = d.experiment;
  const weekNo = x?.startDate && x.status !== "vorgeschlagen" ? Math.floor(diffDays(x.startDate, d.today) / 7) + 1 : null;
  return {
    generatedAt: d.generatedAt,
    today: d.today,
    experiment: x
      ? { title: x.title, status: x.status, startDate: x.startDate, weekNo, minWeeks: x.minWeeks, targetDaysPerWeek: x.targetDaysPerWeek, verdict: x.verdict ?? null,
          metrics: (x.metrics ?? []).map((m) => ({ key: m.key, label: m.label, unit: m.unit, baseline: m.baseline, current: m.current, better: m.better, ...(m.provisional ? { provisional: true } : {}) })) }
      : null,
    trainingDays: d.trainingDays ?? null,
    // Ausdauer je Woche wie die Karte "Woche" (week-totals.js): Minuten ohne Kraft je Sportart, Ziel der jeweiligen Woche
    // (save_targets), Serie ab streakThresholdPct %; dazu Kraftsaetze in 2 Wochen. Ohne Aktivitaeten null
    endurance: enduranceWeeks(d.weekTotals),
    sourcesFailed: failedOf(d),
  };
}

function enduranceWeeks(T) {
  if (!T?.weeks?.length) return null;
  const c = T.weeks[T.weeks.length - 1];
  return {
    weeks: T.weeks.map((w) => ({ week: w.week, complete: w.complete, min: w.enduranceMin, goalMin: Math.round(w.goalHours * 60),
      by: Object.fromEntries(Object.entries(w.byDisciplineMin).map(([k, v]) => [DISC_SPORT[k], v])) })),
    streak: T.streakWeeksAbove4_5, streakPct: T.goals.streakThresholdPct, sets2w: c.strengthSets2w, goalSets2w: T.goals.strengthSets2w,
  };
}

// Garmin-Watchface (garmin/): eine einzige, kleine Ansicht ("watch"). Connect IQ erlaubt im Hintergrunddienst nur wenige KB
// Antwort, deshalb kurze Schluessel und nur Zahlen, keine Freitexte. Wetter und Akku kommen von der Uhr selbst.
// next = naechste Einheit (d Tage ab heute, s Sportart, m Minuten, km); Tage: Mo bis So der laufenden Woche; s = Sportart (run, bike, swim, strength, other), p = 1 geplant, 0 erledigt, ohne Eintrag frei.
// Ein Tag fuer die Wochenpunkte: bis zu zwei Sportarten, erledigte (nach TSS) vor geplanten (nach geplanter Last).
// Erste in s/p, zweite in s2/p2 (aeltere Uhr-Builds lesen nur s/p). Geplant zaehlt nur ab heute und nur, wenn die
// Sportart an dem Tag noch nicht erledigt ist; ohne beides null.
function watchDay(d, date) {
  const sports = date <= d.today ? d.daily.find((x) => x.date === date)?.sports ?? {} : {};
  const done = Object.entries(sports).filter(([, v]) => v > 0).sort((a, b) => b[1] - a[1]).map(([k]) => k);
  const planned = date >= d.today
    ? d.plannedWeek.filter((x) => x.date === date && !done.includes(x.sport)).sort((a, b) => (b.load ?? 0) - (a.load ?? 0)).map((x) => x.sport)
    : [];
  const list = [...done.map((k) => ({ s: k, p: 0 })), ...planned.map((k) => ({ s: k, p: 1 }))]
    .filter((x, i, arr) => arr.findIndex((y) => y.s === x.s) === i).slice(0, 2);
  if (!list.length) return null;
  return list.length > 1 ? { ...list[0], s2: list[1].s, p2: list[1].p } : list[0];
}

// co = 1, wenn der Coach heute schon seine Morgen-Auswertung geschrieben hat; rd = sein Urteil (go) als ok/warn/bad, sonst
// "none" (aeltere Zifferblaetter zeigen das als Bereit / Mit Vorsicht / Ruhig angehen); bl = Zahl seiner Blocker.
// race.d = Tage bis zum Rennen; block = Trainingsblock (st Status, d Tage bis Start, w Woche von of) fuer die Zeile am unteren Rand.
const dayIndex = (iso) => (new Date(iso + "T00:00:00Z").getUTCDay() + 6) % 7;


// Kraft fuer den Ring: Saetze der laufenden Kalenderwoche (Mo–So, wie die Kraft-Karte im Dashboard) gegen das Wochenziel
// (Standard 60, EGYM_WEEKLY_GOAL_SETS), nicht die Zahl der Wochen im Ziel (value/of des Block-Ziels) und nicht die
// Blockwoche, die am Starttag des Blocks beginnt. Ohne Satzzahlen (kein EGYM) Kraft-Tage gegen 2. Ohne Block-Ziel null.
function strengthWeek(goal) {
  const cur = goal?.calWeek ?? goal?.weeks?.find((w) => !w.done);
  if (!cur || !(goal.weekGoal > 0)) return null;
  return { v: cur.count, of: goal.weekGoal, ok: "ok" };
}

// TSS-Ring der Uhr in Sportfarben: je Sportart [s, erledigt, noch offen laut Plan]; offen = Plan der Woche minus erledigt.
// Reihenfolge wie im Dashboard, nur Sportarten mit einem Wert; ohne beides leer.
const WATCH_SPORTS = ["swim", "bike", "run", "strength", "other"];
function watchTssBySport(week) {
  return WATCH_SPORTS.map((k) => {
    const s = week.bySport[k];
    if (!s) return null;
    const done = Math.round(s.load);
    const open = Math.max(0, Math.round((s.plannedLoad ?? 0) - s.load));
    return done > 0 || open > 0 ? [k, done, open] : null;
  }).filter(Boolean);
}

// Ringe der Uhr im Apple-Stil, wenn der Coach Wochenziele gesetzt hat: aussen Rad, Mitte Lauf, innen Schwimmen.
// Je Sportart [s, erledigt, Vorgabe]; rz = Minuten gegen das Zeitziel, rt = TSS gegen die Obergrenze (Schwimmen meist ohne).
// Nur Sportarten mit Vorgabe; die Uhr zeichnet den Ring an der festen Position der Sportart.
const RING_SPORTS = ["bike", "run", "swim"];
function watchRings(targets, done, goal) {
  return RING_SPORTS.map((k) => targets.find((t) => t.s === k)).filter((t) => t && t[goal] > 0).map((t) => [t.s, t[done], t[goal]]);
}

// Zifferblatt nach Tageszeit (garmin/README.md): morgens Schlaf und HRV, tagsueber die Woche Tag fuer Tag, nach einer Einheit
// deren Werte. Die Uhr waehlt selbst, welche Karte sie zeigt; der Worker liefert alle drei.
const WEEK_SPORTS = ["run", "bike", "swim"];
const hm = (secs) => `${Math.floor(secs / 3600)}:${String(Math.floor((secs % 3600) / 60)).padStart(2, "0")}`;
const ms = (secs) => { const t = Math.round(secs); return `${Math.floor(t / 60)}:${String(t % 60).padStart(2, "0")}`; };

// Woche Mo bis So fuer die Wochenkarte: je Tag [Sportart, Minuten, 1 = geplant / 0 = erledigt] fuer Lauf, Rad, Schwimmen.
// Erledigt bis heute aus den Tagesminuten, geplant ab heute, wenn die Sportart an dem Tag noch nicht erledigt ist.
function watchWeekDays(d) {
  const monday = addDays(d.today, -dayIndex(d.today));
  return Array.from({ length: 7 }, (_, i) => {
    const date = addDays(monday, i);
    const mins = date <= d.today ? d.daily.find((x) => x.date === date)?.minutes ?? {} : {};
    const done = WEEK_SPORTS.filter((k) => mins[k] > 0).map((k) => [k, mins[k], 0]);
    const plan = date >= d.today
      ? WEEK_SPORTS.filter((k) => !(mins[k] > 0)).map((k) => [k, (d.plannedWeek ?? []).filter((p) => p.date === date && p.sport === k).reduce((a, p) => a + (p.durationMin ?? 0), 0), 1]).filter((x) => x[1] > 0)
      : [];
    return [...done, ...plan];
  });
}

// Wochenvergleich fuer das Zifferblatt: Ausdauerminuten je Tag, [diese Woche Mo bis heute, Vorwoche Mo bis So]. Die Uhr
// summiert sie auf und zeichnet beide Kurven mit dem Ziel des Coaches (wm). Ohne Ausdauerwert des Tages (aeltere Daten) die
// Minuten von Lauf, Rad und Schwimmen.
function watchWeekCompare(d) {
  const monday = addDays(d.today, -dayIndex(d.today));
  const minOf = (date) => {
    const x = d.daily.find((y) => y.date === date);
    return x?.endurance ?? WEEK_SPORTS.reduce((a, k) => a + (x?.minutes?.[k] ?? 0), 0);
  };
  return [
    Array.from({ length: dayIndex(d.today) + 1 }, (_, i) => minOf(addDays(monday, i))),
    Array.from({ length: 7 }, (_, i) => minOf(addDays(monday, i - 7))),
  ];
}

// Letzte Einheit, nur wenn sie von heute ist: s Sportart, t Startzeit, e Ende (Sekunden seit 1970), c bis zu vier Werte
// [Text, Beschriftung], lw = Wochen seit der letzten mindestens so langen Einheit derselben Sportart (erst ab 2 Wochen).
// Puls nur, wenn das Dashboard ihn fuer die Einheit zeigt (Pulsregel, avgHr).
function watchLast(d) {
  const L = d.lastActivity;
  if (!L || L.date !== d.today) return null;
  const c = [];
  if (L.distanceKm > 0) c.push(L.sport === "swim" ? [String(Math.round(L.distanceKm * 1000)), "M"] : [String(L.distanceKm).replace(".", ","), "KM"]);
  if (L.movingSecs > 0) c.push([hm(L.movingSecs), "ZEIT"]);
  if (L.paceSecPerKm > 0) c.push([ms(L.paceSecPerKm), "/KM"]);
  if (L.paceSecPer100m > 0) c.push([ms(L.paceSecPer100m), "/100M"]);
  if (L.watts > 0) c.push([String(Math.round(L.watts)), "WATT"]);
  if (L.avgHr > 0) c.push([String(Math.round(L.avgHr)), "HF"]);
  return { s: L.sport, t: L.start ?? null, e: L.endAt ?? null, c: c.slice(0, 4), lw: L.longerAgoDays >= 14 ? Math.floor(L.longerAgoDays / 7) : null };
}

// Erholungswerte fuer die Morgenkarte: Schlaf (h), HRV, Ruhepuls von heute je [Wert, Basis, Vergleich]. Der Vergleich ist nur
// der mit dem eigenen ueblichen Bereich (body.band, dieselben Basistage wie die Basis): 1 = im Bereich oder besser, 2 = schlechter
// als der Bereich (25/75-%-Quantil), 3 = schlechter als 9 von 10 Basistagen (10/90-%-Quantil), 0 = ohne Wert oder Bereich.
// Ein Gesamturteil gibt es nicht, das setzt der Coach (go).
function bandCls(v, b, higherBetter) {
  if (v == null || !b) return 0;
  const [p10, p25, p75, p90] = b;
  return higherBetter ? (v >= p25 ? 1 : v >= p10 ? 2 : 3) : (v <= p75 ? 1 : v <= p90 ? 2 : 3);
}
function watchRecovery(d) {
  const r = d.summary?.recovery, b = r?.body;
  if (!r) return null;
  const one = (v) => (v == null ? null : Math.round(v * 10) / 10);
  return [
    [one(r.sleepHours), one(b?.sleepMedian), bandCls(r.sleepHours, b?.band?.sleep, true)],
    [r.hrv != null ? Math.round(r.hrv) : null, b?.hrvMedian != null ? Math.round(b.hrvMedian) : null, bandCls(r.hrv, b?.band?.hrv, true)],
    [r.restingHR != null ? Math.round(r.restingHR) : null, b?.restingMedian != null ? Math.round(b.restingMedian) : null, bandCls(r.restingHR, b?.band?.resting, false)],
  ];
}

export function buildWidgetWatch(d, env = {}, goals = null, live = null) {
  const week = d.weeks[d.weeks.length - 1];
  const total = (w) => Object.values(w.bySport).reduce((a, s) => a + s.load, 0);
  const monday = addDays(d.today, -dayIndex(d.today));
  const days = Array.from({ length: 7 }, (_, i) => watchDay(d, addDays(monday, i)));
  // Naechste Einheit: die erste geplante von heute, deren Sportart heute noch nicht erledigt ist (Rad erledigt,
  // Kraft offen -> Kraft), sonst die naechste geplante an einem spaeteren Tag
  const doneToday = d.daily.find((x) => x.date === d.today)?.sports ?? {};
  const nextP = d.planned.find((p) => p.date > d.today || (p.date === d.today && !(doneToday[p.sport] > 0)));
  const next = nextP ? { d: Math.round((Date.parse(nextP.date + "T00:00:00Z") - Date.parse(d.today + "T00:00:00Z")) / 86400000), s: nextP.sport, m: nextP.durationMin > 0 ? nextP.durationMin : null, km: nextP.distanceKm > 0 ? nextP.distanceKm : null } : null; // 0 (z. B. Kraft ohne Strecke) nicht anzeigen
  const b = d.seasonBlock;
  const strength = b?.goals?.find((g) => g.key === "strength");
  const byDate = Object.fromEntries(d.wellness.map((w) => [w.date, w]));
  const today = byDate[d.today];
  const kcal = live?.energyKcal > 0 ? Math.round(live.energyKcal) : today?.calories ?? null;
  // Eiweiß heute fuer den inneren Ring von Essen, wie kcal live aus Yazio, sonst aus dem Sync
  const protein = live?.energyKcal > 0 && Number.isFinite(live.proteinG) ? Math.round(live.proteinG) : today?.protein != null ? Math.round(today.protein) : null;
  const cfg = Number(env?.WEEKLY_TSS_GOAL);
  const targets = weekTargets(week), rz = watchRings(targets, "min", "goal"), rt = watchRings(targets, "tss", "max");
  return {
    t: d.today,
    ti: dayIndex(d.today),
    days,
    next,
    race: d.goal ? { d: d.goal.daysToGo ?? null } : null,
    block: b ? { st: b.status, d: b.daysToStart ?? null, w: b.weekNo ?? null, of: b.weeks ?? null } : null,
    // Kein eigenes Urteil: nur das des Coaches
    rd: GO_CLS[freshCoach(d)?.go] ?? "none",
    co: freshCoach(d) ? 1 : 0,
    bl: freshCoach(d)?.blockers?.length ?? 0,
    tss: total(week),
    ts: watchTssBySport(week),
    plan: week.plannedLoad > 0 && !targets.length ? week.plannedLoad : null,
    // Wochenziele des Coaches als Ringe (siehe watchRings); ohne Zeitziele null, dann zeigt die Uhr TSS-Ring und Kraft wie bisher
    rz: rz.length ? rz : null,
    rt: rz.length && rt.length ? rt : null,
    goal: Number.isFinite(cfg) && cfg > 0 ? Math.round(cfg) : null,
    str: strengthWeek(strength),
    kcal: { v: kcal, g: goals?.kcal ?? today?.calorieGoal ?? null },
    pr: { v: protein, g: goals?.proteinG ?? null },
    failed: failedOf(d).length,
    // Zifferblatt nach Tageszeit: sl Schlaf (h), hv HRV der letzten 7 Tage (heute zuletzt, ohne Wert null), hm deren Basis
    // (Median wie im Dashboard), cl Kopfzeile des Coaches von heute (bis 90 Zeichen, die Uhr bricht sie um), rv Erholungswerte
    // (watchRecovery), wd Woche (watchWeekDays), wm Ausdauer der Woche
    // [Minuten, Ziel-Minuten], wv Wochenvergleich (watchWeekCompare), la letzte Einheit von heute (watchLast)
    sl: d.summary?.recovery?.sleepHours ?? null,
    hv: Array.from({ length: 7 }, (_, i) => (byDate[addDays(d.today, i - 6)]?.hrv != null ? Math.round(byDate[addDays(d.today, i - 6)].hrv) : null)),
    hm: d.summary?.recovery?.body?.hrvMedian != null ? Math.round(d.summary.recovery.body.hrvMedian) : null,
    cl: firstLine(freshCoach(d)?.headline ?? freshCoach(d)?.note, 90),
    rv: watchRecovery(d),
    wd: watchWeekDays(d),
    wm: weekGoals(d) ? [Math.round(weekGoals(d).min), weekGoals(d).goalMin] : null,
    wv: watchWeekCompare(d),
    la: watchLast(d),
  };
}

// Das Dashboard wird fuer alle Widgets kurz in KV gehalten: Jede Ansicht ruft den Worker einzeln ab, ohne Cache
// wuerde jedes Widget alle Quellen neu laden. Mit ?fresh=1 wird der Cache uebergangen.
const CACHE_KEY = "widget:dashboard-cache";
const CACHE_MS = 5 * 60 * 1000;
// Letzter Stand auch mit ausgefallener Quelle, nur fuer die Uhr: Faellt eine Quelle laenger aus, gibt es sonst keinen
// Stand von heute im Cache, und jeder Abruf der Uhr wartet auf einen vollen Neuaufbau (-300).
const LAST_KEY = "widget:dashboard-last";
// stale: Die Uhr (view=watch) bricht nach wenigen Sekunden ab (Fehler -300), ein voller Neuaufbau aus allen Quellen dauert
// laenger. Mit stale bekommt sie sofort den letzten Stand von heute, auch wenn er aelter ist, und der Neuaufbau laeuft per
// ctx.waitUntil im Hintergrund fuer den naechsten Abruf.
async function dashboardCached(env, fresh, ctx = null, stale = false) {
  const today = isoDateBerlin();
  if (!fresh) {
    const c = await readKvJson(env, CACHE_KEY).catch(() => null);
    if (c?.data && c.data.today === today && Date.now() - c.at < CACHE_MS) return c.data;
    if (stale && ctx) {
      const last = c?.data?.today === today ? c : await readKvJson(env, LAST_KEY).catch(() => null);
      if (last?.data?.today === today) {
        ctx.waitUntil(rebuildDashboardCache(env, today).catch(() => {}));
        return last.data;
      }
      // Gar kein Stand von heute: nicht bis zum Abbruch der Uhr warten, sondern nach WATCH_WAIT_MS mit 503 antworten
      // (die Uhr zeigt dann "! 503" statt "! -300"), der Neuaufbau laeuft im Hintergrund weiter
      const build = rebuildDashboardCache(env, today);
      ctx.waitUntil(build.catch(() => {}));
      return Promise.race([build, new Promise((_, reject) => setTimeout(() => reject(new WatchBusy()), WATCH_WAIT_MS))]);
    }
  }
  return rebuildDashboardCache(env, today);
}

const WATCH_WAIT_MS = 6000;
class WatchBusy extends Error {}

// Protokoll der Uhr-Abrufe (Zeit, Status, Dauer), damit man ohne Token sieht, ob Anfragen der Uhr ankommen und wie schnell
// sie beantwortet werden. Erfolgreiche Abrufe nur alle 15 Minuten schreiben, sonst kostet jeder 5-Minuten-Abruf einen KV-Schreibvorgang.
const WATCH_LOG_KEY = "widget:watch-log";
async function logWatchRequest(env, entry) {
  const prev = await readKvJson(env, WATCH_LOG_KEY).catch(() => null);
  if (entry.status === 200 && prev?.status === 200 && entry.at - prev.at < 15 * 60 * 1000) return;
  await writeKvJson(env, WATCH_LOG_KEY, entry);
}

// Oeffentlich, ohne Token: nur Zeitstempel, Statuscodes und Namen ausgefallener Quellen, keine Trainingsdaten
export async function handleWatchStatusRequest(env) {
  const [watch, cache, last] = await Promise.all([WATCH_LOG_KEY, CACHE_KEY, LAST_KEY].map((k) => readKvJson(env, k).catch(() => null)));
  const meta = (c) => (c ? { at: new Date(c.at).toISOString(), today: c.data?.today ?? null, failed: c.data ? failedOf(c.data) : null } : null);
  return json({ now: new Date().toISOString(), watch: watch ? { ...watch, at: new Date(watch.at).toISOString() } : null, cache: meta(cache), last: meta(last) }, 200, { "cache-control": "no-store" });
}

async function rebuildDashboardCache(env, today = isoDateBerlin()) {
  const data = await buildDashboard(env, today);
  // Nur vollstaendige Staende cachen: Ein Ausfall soll nicht fuenf Minuten lang haengen bleiben
  if (failedOf(data).length === 0) await writeKvJson(env, CACHE_KEY, { at: Date.now(), data }).catch(() => {});
  else await writeKvJson(env, LAST_KEY, { at: Date.now(), data }).catch(() => {});
  return data;
}

// Vom Cron alle 15 Minuten: haelt den Cache warm, damit die Uhr nie auf einen Neuaufbau warten muss
export async function warmDashboardCache(env) {
  await rebuildDashboardCache(env);
}

// Die Uhr bekommt ein eigenes Token (WATCH_TOKEN), das nur die Ansicht "watch" oeffnet: Es steht im Klartext in den
// Watchface-Einstellungen von Garmin Connect und soll nicht das ganze Dashboard freigeben.
function isWatchAuthorized(req, env) {
  const given = (req.headers.get("authorization") || "").replace(/^Bearer\s+/i, "").trim();
  return Boolean(env?.WATCH_TOKEN) && given.length > 0 && timingSafeEqual(given, env.WATCH_TOKEN);
}

export async function handleWidgetRequest(req, env, ctx = null) {
  if (new URL(req.url).searchParams.get("view") !== "watch") return widgetResponse(req, env, ctx);
  const start = Date.now();
  let res;
  try {
    res = await widgetResponse(req, env, ctx);
  } catch (e) {
    if (!(e instanceof WatchBusy)) throw e;
    res = json({ ok: false, error: "Neuaufbau laeuft" }, 503, { "cache-control": "no-store" });
  }
  const log = logWatchRequest(env, { at: start, status: res.status, ms: Date.now() - start }).catch(() => {});
  if (ctx) ctx.waitUntil(log);
  else await log;
  return res;
}

async function widgetResponse(req, env, ctx) {
  const headers = { "cache-control": "no-store" };
  const view0 = new URL(req.url).searchParams.get("view");
  if (!env?.DASHBOARD_TOKEN && !env?.WATCH_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env) && !(view0 === "watch" && isWatchAuthorized(req, env))) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  const params = new URL(req.url).searchParams;
  const view = params.get("view");
  // Kraft kommt allein aus EGYM, ohne das Dashboard (Intervals, Yazio, Runalyze) zu laden
  if (view === "kraft") return json(await loadWidgetKraft(env, params.get("fresh") === "1"), 200, headers);
  const dashboard = await dashboardCached(env, params.get("fresh") === "1", ctx, view === "watch");
  // Tagesziele der Ernaehrung kommen aus Yazio (best effort: fehlt der Zugang oder scheitert die Abfrage, bleibt es null).
  // Nicht fuer die Uhr: Sie zeigt kein Essen, und die zwei Yazio-Abrufe kosten Zeit bis zum Abbruch (-300).
  const goals = view === "small" && hasYazioCredentials(env) ? await fetchYazioDailyGoals(env, dashboard.today).catch(() => null) : null;
  const live = view === "small" && hasYazioCredentials(env) ? await fetchYazioDailyNutrition(env, dashboard.today).catch(() => null) : null;
  const body = view === "detail" ? buildWidgetDetail(dashboard, env) : view === "small" ? buildWidgetSmall(dashboard, goals, live) : view === "training" ? buildWidgetTraining(dashboard, env) : view === "vdot" ? buildWidgetVdot(dashboard) : view === "block" ? buildWidgetBlock(dashboard) : view === "rahmen" ? buildWidgetRahmen(dashboard) : view === "experiment" ? buildWidgetExperiment(dashboard) : view === "kompass" ? buildWidgetKompass(dashboard) : view === "watch" ? buildWidgetWatch(dashboard, env, goals, live) : buildWidget(dashboard, env);
  return json(body, 200, headers);
}
