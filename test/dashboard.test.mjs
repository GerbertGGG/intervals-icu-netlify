// Testet src/dashboard.js gegen SYNTHETISCHE Intervals.icu-Antworten (nur für Tests,
// nichts davon geht in den Livebetrieb). Ausführen: node test/dashboard.test.mjs
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { buildWidget, buildWidgetDetail, buildWidgetVdot, buildWidgetSmall, buildWidgetTraining, handleWidgetRequest } from "../src/widget.js";
import { computeReadiness, computeLoad } from "../src/dashboard-summary.js";
import { parseCravings, findHipFlags, parseWorkoutSteps } from "../src/dashboard-parse.js";
import { validateStudie, handleStudieRequest } from "../src/studie-snapshot.js";
import { validateSnapshot, bestForDistance } from "../src/runalyze-snapshot.js";
import { sportOf, buildDashboard, buildRunRecord, handleDashboardRequest, classifyRun } from "../src/dashboard.js";

const today = "2026-09-30";
const day = (n) => new Date(Date.parse(today + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const wellness = [];
for (let i = 55; i >= 0; i--) {
  if (i % 5 === 2) continue; // Lücken
  wellness.push({ id: day(-i), ctl: 30 + (55 - i) * 0.1, atl: 28 + Math.sin(i) * 6, mood: 1 + (i % 3), motivation: i % 7 === 0 ? 0 : 2, fatigue: 1 + (i % 4), soreness: null, sleepQuality: 2, sleepSecs: 27000, hrv: 48, restingHR: 52 + (i % 4), comments: i % 6 === 0 ? `HH ${15 + (i % 7)}:30, Stärke ${1 + (i % 5)}, Schokolade, davor 12:00 Salat, Auslöser Müdigkeit` : i === 4 ? "Linke Hüfte zwickt, kein Knieschmerz" : null, ...(i < 14 && i % 3 !== 0 ? { Calories: 1800 + i * 20, Carbs: 200 + i, Protein: 110, Fat: 60, CalorieGoal: 2100 } : {}), ...(i === 1 ? { Calories: 0 } : {}) });
}
const act = (n, o) => ({ id: n, start_date_local: day(-n) + "T10:00:00", type: "Run", icu_training_load: 50, average_heartrate: 140, decoupling: 3.1, icu_rpe: 5, feel: 2, ...o });
const activities = [
  act(1, { name: "Bobingen - Tempowechsellauf", distance: 7130, moving_time: 2897 }),
  act(3, { name: "Bobingen - Kurzer Grundlagenlauf", distance: 6030, moving_time: 2383 }),
  act(5, { name: "Bobingen - Long Slow Run", distance: 16200, moving_time: 6771, description: "locker" }),
  act(7, { name: "Lauf ohne Angabe", distance: 5000, moving_time: 1800 }),
  act(9, { name: "Radfahren", type: "Ride", distance: 30000, moving_time: 3600, icu_training_load: 80, icu_ftp: 210, icu_weighted_avg_watts: 190, icu_average_watts: 170 }),
  act(2, { name: "Schwimmen", type: "Swim", distance: 1500, moving_time: 2700, icu_training_load: 40 }),
  act(11, { name: "Rad locker", type: "VirtualRide", distance: 40000, moving_time: 5400, icu_training_load: 60, icu_ftp: 215 }),
  act(12, { name: "Intervalle 5x1000", distance: 9000, moving_time: 3200, tags: ["intervals"] }),
];
const events = [
  { category: "WORKOUT", start_date_local: day(0) + "T00:00:00", name: "Locker 30'", description: "Zweck: Beine lockern", moving_time: 1800 },
  { category: "WORKOUT", start_date_local: day(1) + "T00:00:00", name: "CSS-Intervalle", description: "10x100 auf CSS", tags: ["#key"], type: "Swim", moving_time: 3000, distance_target: 2000 },
];
globalThis.fetch = async (url) => {
  const u = String(url);
  const body = u.includes("/sport-settings") ? [{ types: ["Run"], threshold_pace: 2.94, lthr: 165, max_hr: 185 }, { types: ["Ride", "VirtualRide"], ftp: 220 }, { types: ["Swim"], threshold_pace: 0.8 }] : u.includes("/wellness") ? wellness : u.includes("/activities") ? activities : u.includes("/events") ? events : null;
  return new Response(JSON.stringify(body), { status: body ? 200 : 404 });
};
const kv = new Map();
const env = { ATHLETE_ID: "i1", INTERVALS_API_KEY: "k", DASHBOARD_TOKEN: "geheim", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const d = await buildDashboard(env, today);
writeFileSync(new URL("./fixture-dashboard.json", import.meta.url), JSON.stringify(d));

// Pulsregel
const records = activities.filter((a) => sportOf(a) === "run").map(buildRunRecord);
assert.equal(records.length, 5); // Rad und Schwimmen nicht dabei
for (const r of records) {
  if (r.kind === "intensity" || r.kind === "unknown") assert.equal(r.avgHr, null, r.name), assert.equal(r.decoupling, null);
  else assert.equal(r.avgHr, 140);
}
// Einzelne Einheiten (Namen, Kommentare) verlassen den Worker nicht mehr
assert.equal("runs" in d, false);
assert.equal("triathlon" in d, false);
assert.equal(JSON.stringify(d).includes("Zweck: Beine lockern"), true); // nur geplante Einheit heute
assert.equal(JSON.stringify(d).includes("Bobingen"), false);
assert.equal(classifyRun(activities[0]), "intensity");
assert.equal(classifyRun(activities[1]), "base");
assert.equal(classifyRun(activities[2]), "unknown"); // lang nur, wenn in Runalyze so eingetragen
assert.equal(classifyRun(activities[2], { rzRuns: [{ date: day(-5), distanceKm: 16.2, type: "Langer Lauf" }] }), "long");
assert.equal(classifyRun(activities.find((a) => a.name === "Intervalle 5x1000")), "intensity");
assert.equal(sportOf({ type: "Swim" }), "swim");
assert.equal(sportOf({ type: "OpenWaterSwim" }), "swim");
assert.equal(sportOf({ type: "VirtualRide" }), "bike");
assert.equal(sportOf({ type: "WeightTraining" }), "strength");
assert.equal(sportOf({ type: "TrailRun" }), "run");
assert.equal(d.thresholds.bike.ftp, 220);
assert.equal(d.thresholds.swim.thresholdPaceSecPer100m, 125);
assert.equal(d.thresholds.run.thresholdPaceSecPerKm, 340 + 0); // 1000 m / 2.94 m/s
assert.equal(d.thresholds.run.lthr, 165);
assert.equal(d.thresholds.run.maxHr, 185);
assert.equal(d.thresholds.bike.lthr, null); // nicht hinterlegt bleibt null
const today0 = d.wellness.find((w) => w.date === today);
assert.equal(today0.sleepHours, 7.5);
assert.equal(today0.hrv, 48);
const sumLoad = d.weeks.reduce((a, w) => a + w.load, 0);
const sumSport = d.weeks.reduce((a, w) => a + Object.values(w.bySport).reduce((b, v) => b + v.load, 0), 0);
assert.equal(sumLoad, sumSport);
assert.ok(d.weeks.some((w) => w.bySport.swim.load === 40));
// Heißhunger, Hüfte, Yazio
assert.ok(d.cravings.length >= 5 && d.cravings.every((c) => /^\d\d:30$/.test(c.time) && c.strength >= 1 && c.strength <= 5 && c.trigger === "Müdigkeit"));
assert.equal(d.hipFlags.length, 1);
assert.equal(d.hipFlags[0].term, "Hüfte");
assert.equal(d.wellness.find((w) => w.date === day(-1)).calories, null); // 0 zählt als keine Daten, nie als 0 kcal
assert.ok(d.wellness.some((w) => w.calories > 1000 && w.calorieGoal === 2100));
assert.ok(d.wellness.every((w) => !("comments" in w)));
assert.equal(d.daily.length, 56);
assert.equal(d.daily.at(-1).date, today);
assert.equal(d.daily.reduce((a, x) => a + x.load, 0), d.weeks.reduce((a, w) => a + w.load, 0));
assert.equal(d.studie, null);
// Parser
assert.equal(parseCravings("d", "Gut geschlafen. hh 21.15 stärke 3 Chips auslöser: Stress; HH 10h00, Stärke 2, Kekse").length, 2);
assert.equal(parseCravings("d", "HH abends sehr stark")[0].time, null);
assert.equal(findHipFlags("Kniebeugen ok, ohne Hüftschmerz").length, 0);
assert.equal(findHipFlags("Linke Hüfte zwickt, Leiste leicht").length, 2);
// Studien-Check
assert.ok(validateStudie({ fetchedAt: "2026-09-27T10:00:00Z", text: "x" }).error); // ohne Quelle nicht
assert.ok(validateStudie({ fetchedAt: "2026-09-27T10:00:00Z", text: "x", source: "Autor 2024", sourceUrl: "http://x.de" }).error);
assert.ok(validateStudie({ fetchedAt: "2026-09-27T10:00:00Z", text: "x", source: "Autor 2024", sourceUrl: "https://x.de/a" }).value);
const { isAuthorized: isAuth } = await import("../src/dashboard.js");
const putS = (body, tok = "geheim") => handleStudieRequest(new Request("https://x/api/studie", { method: "PUT", headers: { authorization: "Bearer " + tok }, body: JSON.stringify(body) }), env, isAuth);
assert.equal((await putS({}, "falsch")).status, 401);
assert.equal((await putS({ fetchedAt: "2026-09-27T10:00:00Z", text: "Kurzer Abschnitt", source: "Autor 2024" })).status, 200);
// Zusammenfassung und Widget
assert.equal(d.summary.load.tsbCls, "ok");
assert.equal(d.summary.readiness.verdict.cls, "ok");
assert.equal(d.summary.readiness.items.length, 5);
const noToday = computeReadiness(d.wellness.filter((w) => w.date !== today), today, computeLoad(d.wellness));
assert.equal(noToday.verdict.cls, "none"); // ohne heutigen Eintrag keine Einschätzung
const wdg = buildWidget(d);
const wjson = JSON.stringify(wdg);
assert.ok(wjson.length < 3000, "Widget-Payload klein");
assert.equal(wdg.readiness.verdict.text, d.summary.readiness.verdict.text);
assert.equal(wdg.goal.daysToGo, 3);
assert.equal(wdg.goal.phase, "taper"); // Rennphase kommt fertig vom Server
assert.equal(wdg.goal.runKm, d.goal.runKm);
assert.equal(wdg.plan.today[0].purpose, "Zweck: Beine lockern");
for (const gone of ["hip", "trend", "load", "thresholds", "hm"]) assert.equal(gone in wdg, false, gone); // nichts, was das Widget nicht zeichnet
assert.deepEqual(Object.keys(wdg.readiness.body), ["sleep", "hrv", "resting"]);
assert.equal(wdg.readiness.body.sleep.cls, "ok");
assert.equal(wdg.readiness.body.hrv.cls, "ok");
assert.equal(wdg.week.goal, null); // ohne Plan-Load und ohne Konfiguration kein Ziel, nichts erfunden
assert.equal(buildWidget(d, { WEEKLY_TSS_GOAL: "250" }).week.goal, 250);
assert.equal(buildWidget(d, { WEEKLY_TSS_GOAL: "250" }).week.goalSource, "config");
assert.equal(buildWidget(d, { WEEKLY_TSS_GOAL: "kaputt" }).week.goal, null);
assert.equal(wdg.week.total, Object.values(d.weeks.at(-1).bySport).reduce((a, s) => a + s.load, 0));
assert.equal(/Schokolade|Bobingen|Knieschmerz/.test(wjson), false); // keine Freitexte
// Naechste Tage fuer die Kacheln: nur Name/Sport/Dauer, keine Beschreibung
assert.deepEqual(wdg.plan.upcoming.map((x) => [x.date, x.name, x.sport, x.key]), [[day(1), "CSS-Intervalle", "swim", true]]);
assert.equal(/10x100|description/.test(wjson), false);
assert.equal(buildWidget({ ...d, sources: { ...d.sources, intervalsEvents: { ok: false } } }).plan.upcoming, null); // ohne Kalender nicht "frei"
assert.equal(/#key/i.test(wjson), false); // das Kennzeichen selbst geht nicht raus
assert.equal(buildWidget({ ...d, planned: d.planned.map((p) => ({ ...p, tags: p.tags.map((t) => t.replace("#", "")) })) }).plan.upcoming[0].key, true); // Tag auch ohne #
assert.equal(wdg.plan.today[0].key, false);
// Phase
{ const { racePhase } = await import("../src/widget.js"); assert.deepEqual([null, 20, 8, 7, 0, -1, -7, -8].map((x) => racePhase(x)), ["normal", "normal", "normal", "taper", "taper", "recovery", "recovery", "normal"]); }
// Wochenvolumen je Disziplin (Basis der Training-Ansicht)
const cw = d.weeks.at(-1).bySport;
assert.equal(cw.swim.km, 1.5);
assert.equal(cw.swim.plannedKm, 2);
assert.equal(cw.run.plannedKm, null); // nichts geplant = null, nie 0
assert.equal(d.daily.find((x) => x.date === day(-2)).sports.swim, 40);
// Urteil beachtet die Koerperwerte: zwei auffaellige Werte (HRV, Schlaf) kippen ein sonst gruenes Urteil
{
  const bad = d.wellness.map((w) => (w.date === today ? { ...w, hrv: 30, sleepHours: 5 } : w));
  const rd = computeReadiness(bad, today, computeLoad(bad));
  assert.equal(rd.body.hrv.cls, "bad");
  assert.equal(rd.body.sleep.cls, "bad");
  assert.equal(rd.verdict.cls, "bad");
  const one = computeReadiness(d.wellness.map((w) => (w.date === today ? { ...w, hrv: 30 } : w)), today, computeLoad(d.wellness));
  assert.equal(one.verdict.cls, "warn"); // ein Koerperwert allein = Vorsicht
  assert.equal(one.body.hrvMedian, 48); // Median ohne heute
}
// Cache: zweiter Abruf kommt aus KV, ?fresh=1 umgeht ihn
{
  kv.delete("widget:dashboard-cache");
  const get = (q = "") => handleWidgetRequest(new Request("https://x/api/widget" + q, { headers: { authorization: "Bearer geheim" } }), env);
  await get();
  const first = JSON.parse(kv.get("widget:dashboard-cache")).at;
  assert.ok(first > 0);
  const f = globalThis.fetch; let n = 0; globalThis.fetch = async (...a) => (n++, f(...a));
  await get(); assert.equal(n, 0); // aus dem Cache
  await get("?fresh=1"); assert.ok(n > 0); // frisch geladen
  globalThis.fetch = f;
}
// Training-Ansicht (mittleres Widget)
const trn = buildWidgetTraining(d, env);
assert.deepEqual(Object.keys(trn.sports), ["swim", "bike", "run"]);
assert.equal(trn.sports.swim.daysSince, 2);
assert.equal(trn.sports.run.daysSince, 1);
assert.equal("avgLoad" in trn.sports.run, false); // TSS/CTL nur sportartuebergreifend
assert.equal(trn.sports.swim.weekMinutes, 45);
assert.ok(trn.split.share && Math.abs(Object.values(trn.split.share).reduce((a, x) => a + x, 0) - 100) <= 2);
assert.equal(trn.split.target, null); // ohne TRI_SPLIT_TARGET kein Soll, nichts erfunden
assert.deepEqual(buildWidgetTraining(d, { TRI_SPLIT_TARGET: "20,45,35" }).split.target, { swim: 20, bike: 45, run: 35 });
assert.equal(buildWidgetTraining(d, { TRI_SPLIT_TARGET: "kaputt" }).split.target, null);
assert.ok(JSON.stringify(trn).length < 2500);
assert.equal(/Bobingen|Schokolade|CSS/.test(JSON.stringify(trn)), false);
const trnRes = await handleWidgetRequest(new Request("https://x/api/widget?view=training", { headers: { authorization: "Bearer geheim" } }), env);
assert.equal(trnRes.status, 200);
assert.ok("split" in (await trnRes.json()));
assert.equal((await handleWidgetRequest(new Request("https://x/api/widget"), env)).status, 401);
assert.equal((await handleWidgetRequest(new Request("https://x/api/widget", { headers: { authorization: "Bearer geheim" } }), env)).status, 200);
// Detail-Ansicht des Widgets
const det = buildWidgetDetail(d);
assert.equal(det.form.length, 28);
assert.equal(det.form.at(-1).date, today);
assert.equal("cravings" in det || "nutrition" in det, false); // Heisshunger und Ernaehrung verlassen den Worker hier nicht
assert.equal(det.goal.runKm, d.goal.runKm);
assert.equal(/Schokolade|Bobingen|Knieschmerz/.test(JSON.stringify(det)), false); // keine Freitexte
assert.ok(JSON.stringify(det).length < 5000);
assert.equal(det.hm, null); // ohne Runalyze-Snapshot keine Zeiten, nichts erfunden
assert.equal(det.vdot, null);
const detRes = await handleWidgetRequest(new Request("https://x/api/widget?view=detail", { headers: { authorization: "Bearer geheim" } }), env);
assert.equal(detRes.status, 200);
assert.ok("form" in (await detRes.json()));
// Kleine Widgets: Schlaf und Ernaehrung
const sm = buildWidgetSmall(d);
assert.equal(sm.sleep.days.length, 7);
assert.equal(sm.sleep.days.at(-1).date, today);
assert.equal(sm.sleep.days.at(-1).hours, 7.5);
assert.equal(sm.sleep.medianHrv, 48);
assert.equal(sm.sleep.medianHrv, d.summary.readiness.body.hrvMedian); // dieselben Mediane wie die Bereitschaft
assert.equal(sm.goal.daysToGo, 3); // Rennziel kommt mit, kein Zweitabruf noetig
assert.equal(sm.food.days.length, 7);
assert.equal(sm.food.days.find((x) => x.date === day(-1)).calories, null); // 0 kcal = keine Daten
assert.ok(sm.food.hasData && sm.food.latest.calories > 1000 && sm.food.latest.goal === 2100);
const smEmpty = buildWidgetSmall({ ...d, wellness: d.wellness.map((w) => ({ ...w, calories: null, carbs: null, calorieGoal: null })) });
assert.equal(smEmpty.food.hasData, false); // ohne Yazio-Daten kein Wert, nichts erfunden
assert.equal(smEmpty.food.latest, null);
assert.equal(sm.food.goals, null); // ohne Yazio-Zugang keine Ziele, nichts erfunden
assert.deepEqual(buildWidgetSmall(d, { kcal: 2100, proteinG: 120, carbsG: 250, fatG: 70 }).food.goals.proteinG, 120);
assert.equal(sm.fitness.weekly.length, 7); // Fitness (CTL): ein Punkt je Woche
assert.equal(sm.fitness.ctl, sm.fitness.weekly[6]);
assert.equal("cravings" in sm, false);
assert.ok(sm.fitness.delta == null || sm.fitness.deltaWeeks >= 1);
assert.ok(JSON.stringify(sm).length < 3000);
assert.equal(/Schokolade|Bobingen|Knieschmerz/.test(JSON.stringify(sm)), false);
const smRes = await handleWidgetRequest(new Request("https://x/api/widget?view=small", { headers: { authorization: "Bearer geheim" } }), env);
assert.equal(smRes.status, 200);
assert.ok("sleep" in (await smRes.json()));
// Lücken bleiben null, 0 wird nicht zu "gut"
assert.equal(d.wellness.find((w) => w.motivation === 0), undefined);
assert.ok(d.wellness.every((w) => w.soreness === null));
assert.equal(d.goal.daysToGo, 3);
assert.equal(d.goal.source, "config");
assert.ok(d.weeks.at(-1).complete === false);
// Ohne Runalyze-Snapshot gibt es keine langen Laeufe
assert.equal(d.fitness.longRuns.length, 0);
assert.equal(d.fitness.longRunTracker.recent.length, 0);
assert.equal(d.runalyze, null); // ohne Snapshot: fehlt, keine Platzhalter
// Runalyze-Snapshot (synthetische Werte, nur Test)
const snap = { fetchedAt: "2026-09-30T05:00:00Z", vdot: 34.67, prognosis: [{ distanceKm: 5, seconds: 1600 }, { distanceKm: 21.1, seconds: 8000 }],
  races: [{ date: "2026-03-22", officialDistanceKm: 5.03, officialTimeSec: 1570 }, { date: "2022-03-20", officialDistanceKm: 5.02, officialTimeSec: 1537 }, { date: "2025-10-03", officialDistanceKm: 10, officialTimeSec: 3431 }] };
assert.equal(bestForDistance(validateSnapshot(snap).value.races, 5).officialTimeSec, 1537);
assert.ok(validateSnapshot({ fetchedAt: "kaputt" }).error);
assert.ok(validateSnapshot({ fetchedAt: snap.fetchedAt }).error);
const { handleRunalyzeSnapshotRequest } = await import("../src/runalyze-snapshot.js");
const { isAuthorized } = await import("../src/dashboard.js");
const put = (body, tok = "geheim", method = "PUT") => handleRunalyzeSnapshotRequest(new Request("https://x/api/runalyze", { method, headers: { authorization: "Bearer " + tok }, body: method === "GET" ? undefined : JSON.stringify(body) }), env, isAuthorized);
assert.equal((await put(snap, "falsch")).status, 401);
assert.equal((await put(snap, "geheim", "GET")).status, 405);
assert.equal((await put({ fetchedAt: "x" })).status, 400);
assert.equal((await put(snap)).status, 200);
const d2 = await buildDashboard(env, today);
const det2 = buildWidgetDetail(d2);
assert.ok(det2.hm.estimates.length >= 3);
assert.equal(det2.vdot.paces.length, 5);
const vd = buildWidgetVdot(d2);
assert.equal(vd.vdot.value, 34.67);
assert.equal(vd.vdot.paces.length, 5);
const thr = vd.vdot.paces.find((p) => p.key === "threshold");
assert.deepEqual([thr.pct, thr.fastPace, thr.slowPace], [[88, 92], "5:36/km", "5:51/km"]); // Bereich wie in den Runalyze-Lauftabellen
assert.equal(buildWidgetVdot(d).vdot, null); // ohne Snapshot nichts erfunden
const vdRes = await handleWidgetRequest(new Request("https://x/api/widget?view=vdot", { headers: { authorization: "Bearer geheim" } }), env);
assert.equal(vdRes.status, 200);
assert.ok("vdot" in (await vdRes.json()));
const hm = d2.runalyze.rows.find((r) => r.label === "Halbmarathon");
assert.equal(hm.bestSeconds, null); assert.equal(hm.prognosisSeconds, 8000);
assert.equal(d2.runalyze.rows[0].bestSeconds, 1537);
assert.equal(d2.runalyze.vdot, 34.67);
assert.equal(d2.runalyze.paces.find((p) => p.key === "threshold").pace, "5:44/km");
assert.ok(validateSnapshot({ fetchedAt: snap.fetchedAt, vdot: 500 }).error);
writeFileSync(new URL("./fixture-dashboard.json", import.meta.url), JSON.stringify(d2));
// Auth
const noTok = await handleDashboardRequest(new Request("https://x/api/dashboard"), env);
assert.equal(noTok.status, 401);
const bad = await handleDashboardRequest(new Request("https://x/api/dashboard", { headers: { authorization: "Bearer falsch" } }), env);
assert.equal(bad.status, 401);
const ok = await handleDashboardRequest(new Request("https://x/api/dashboard", { headers: { authorization: "Bearer geheim" } }), env);
assert.equal(ok.status, 200);
assert.equal((await handleDashboardRequest(new Request("https://x/api/dashboard"), { ATHLETE_ID: "i" })).status, 503);
// Quelle nicht erreichbar -> markiert, kein Absturz
globalThis.fetch = async () => new Response("nope", { status: 500 });
const failed = await buildDashboard(env, today);
assert.equal(failed.sources.intervalsWellness.ok, false);
assert.deepEqual(failed.wellness, []);
{
  const txt = "Diese Einheit soll vorbereiten.\nNach dem Einlaufen -kurz ABC.\n\n-15m 75% Pace\n\n3x\n-4m 95% Pace\n-2m 60% Pace\n\n-10m 55%";
  const st = parseWorkoutSteps(txt, null);
  assert.deepEqual(st.map((x) => [x.secs, x.pct]), [[900, 75], [240, 95], [120, 60], [240, 95], [120, 60], [240, 95], [120, 60], [600, 55]]);
  assert.deepEqual(parseWorkoutSteps("nur Text", null), []);
  assert.deepEqual(parseWorkoutSteps("", { steps: [{ duration: 600, pace: { value: 80, units: "%pace" } }, { reps: 2, steps: [{ duration: 60 }] }] }).map((x) => [x.secs, x.pct]), [[600, 80], [60, null], [60, null]]);
}
console.log("dashboard tests ok");

// Runalyze-Läufe: Art "Langer Lauf" und Decoupling kommen aus dem Snapshot, Intervalle zählen nicht als lang
{
  const snap = validateSnapshot({ fetchedAt: "2026-09-30T05:00:00Z", vdot: 40, runs: [
    { date: day(-11), distanceKm: 15.57, durationSec: 6783, type: "Langer Lauf", decouplingPct: 8.4 },
    { date: day(-18), distanceKm: 16.2, durationSec: 6771, type: "Langer Lauf", decouplingPct: 11.2 },
    { date: day(-7), distanceKm: 11.25, durationSec: 4167, type: "Tempodauerlauf", decouplingPct: 2 },
    { date: day(-3), distanceKm: 6, durationSec: 2383, type: "Easy run" },
  ] });
  assert.equal(snap.value.runs.length, 4);
  kv.set("dashboard:runalyze", JSON.stringify(snap.value));
  const d2 = await buildDashboard(env, today);
  const T = d2.fitness.longRunTracker;
  assert.equal(T.source, "runalyze");
  assert.equal(T.recent.length, 2);
  assert.equal(T.longest.distanceKm, 16.2);
  assert.equal(T.count16, 1);
  assert.deepEqual(d2.fitness.longRuns.map((r) => r.decoupling), [11.2, 8.4]);
  assert.equal(T.recent[0].decoupling, 8.4); // neuester zuerst
  console.log("runalyze runs ok");
}

// CTL/ATL heute: Basis gestern + absolvierter Load (nicht der Plan), Zukunftstage zählen nie
{
  const { withActualToday, todayActualLoad } = await import("../src/live-load.js");
  const t = "2026-10-03";
  const w = [
    { date: "2026-10-02", ctl: 21.93, atl: 16.66 },
    { date: t, ctl: 25.75, atl: 38.93 }, // Intervals: enthält Plan 184
    { date: "2026-10-04", ctl: 30, atl: 50 },
    { date: "2026-10-05", ctl: 31, atl: 49 },
  ];
  const out = withActualToday(w, [], t);
  const l = computeLoad(out, t);
  assert.equal(out.at(-1).date, t);
  assert.ok(Math.abs(l.ctl - 21.42) < 0.01, String(l.ctl));
  assert.ok(Math.abs(l.atl - 14.44) < 0.01, String(l.atl));
  assert.ok(Math.abs(l.tsb - 7) < 0.1, String(l.tsb));
  // Mit absolviertem Load 184 kommt Intervals' Wert heraus
  const l2 = computeLoad(withActualToday(w, [{ start_date_local: t + "T08:00:00", icu_training_load: 184 }], t), t);
  assert.ok(Math.abs(l2.ctl - 25.75) < 0.01 && Math.abs(l2.atl - 38.93) < 0.01, `${l2.ctl} ${l2.atl}`);
  assert.equal(todayActualLoad([{ start_date_local: t + "T08:00:00", icu_training_load: 40 }, { start_date_local: t + "T18:00:00", icu_training_load: 10 }], t), 50);
  // computeLoad nimmt auch ohne Vorberechnung nie Zukunftstage
  assert.equal(computeLoad(w, t).date, t);
}
console.log("live-load ok");

// Einheitenart: "mit" als normales Wort, Steigerungen, Rennen, einheitliche Langlauf-Grenze
{
  const mk = (name, extra = {}) => ({ name, type: "Run", distance: 8000, moving_time: 2800, start_date_local: "2026-09-20T08:00:00", ...extra });
  assert.equal(classifyRun(mk("Dauerlauf mit Anna", { description: "locker" })), "unknown");
  assert.equal(classifyRun(mk("Easy Lauf mit Steigerungen")), "base");
  assert.equal(classifyRun(mk("MIT 4x2km")), "intensity");
  assert.equal(classifyRun(mk("Halbmarathon Berlin", { distance: 21100 })), "race");
  assert.equal(classifyRun(mk("Morgenlauf", { distance: 21100, sub_type: "RACE" })), "race");
  assert.equal(classifyRun(mk("Long Slow Run", { distance: 21000 })), "unknown"); // Name und Distanz allein genuegen nicht
  assert.equal(classifyRun(mk("Lauf", { distance: 16000 }), { rzRuns: [{ date: "2026-09-20", distanceKm: 16.1, type: "Langer Lauf" }] }), "long");
  // Runalyze-Art "Wettkampf" am selben Tag mit passender Distanz
  const ctx = { rzRuns: [{ date: "2026-09-20", distanceKm: 21.1, type: "Wettkampf" }] };
  assert.equal(classifyRun(mk("Lauf", { distance: 21000 }), ctx), "race");
  assert.equal(classifyRun(mk("Lauf", { distance: 5000 }), ctx), "unknown");
  // Kalender-Rennen
  assert.equal(classifyRun(mk("Lauf", { distance: 10000 }), { raceDays: new Map([["2026-09-20", [10]]]) }), "race");
  assert.equal(buildRunRecord(mk("Halbmarathon", { distance: 21100, average_heartrate: 160, decoupling: 3 })).avgHr, null);
}

// Rennphase je Distanz, Erholung nach dem Rennen auch ohne aktuelles Ziel
{
  const { racePhase, racePhaseFor, phaseDaysFor } = await import("../src/widget.js");
  const hm = { distance: "hm", daysToGo: 0 };
  assert.equal(racePhaseFor(hm, null, "2026-10-03"), "taper");
  assert.equal(racePhaseFor({ distance: "hm", daysToGo: 90 }, { date: "2026-10-03", distance: "hm" }, "2026-10-04"), "recovery");
  assert.equal(racePhaseFor({ distance: "hm", daysToGo: 90 }, { date: "2026-10-03", distance: "hm" }, "2026-10-10"), "recovery");
  assert.equal(racePhaseFor({ distance: "hm", daysToGo: 90 }, { date: "2026-10-03", distance: "hm" }, "2026-10-11"), "normal");
  assert.equal(phaseDaysFor({ triathlon: { format: "middle" } }).taperFrom, 14);
  assert.equal(racePhase(10, phaseDaysFor({ distance: "m" })), "taper");
}

// Marathon-Pace aus der Rennvorhersage, Workout-Schritte mit Zonen-Pace
{
  const { paceTargetsFromVdot } = await import("../src/vdot.js");
  const z = Object.fromEntries(paceTargetsFromVdot(50).map((x) => [x.key, x]));
  assert.deepEqual([z.marathon.fastPace, z.marathon.slowPace], ["4:34/km", "5:07/km"]);
  const paces = { easy: z.easy.secPerKm, marathon: z.marathon.secPerKm, threshold: z.threshold.secPerKm, interval: z.interval.secPerKm, repetition: z.repetition.secPerKm };
  const easy = parseWorkoutSteps("- 10km 70% Pace", null, { paces, sport: "run" });
  const hard = parseWorkoutSteps("- 1km 105% Pace", null, { paces, sport: "run" });
  assert.equal(easy[0].secs, 10 * paces.easy);
  assert.equal(hard[0].secs, paces.interval);
  assert.equal(parseWorkoutSteps("- 1km 70% Pace", null)[0].secs, 360);
  assert.equal(parseWorkoutSteps("- 10m Z2", null, { sport: "run" })[0].pct, 83);
  assert.equal(parseWorkoutSteps("- 10m Z2", null, { sport: "bike" })[0].pct, 70);
}
console.log("race/phase/zones ok");

// Laufzonen aus Intervals: obere Grenzen -> Mitte der Zone; Distanz-Schritte ueber die Schwellenpace
{
  const { zonePctFromBounds } = await import("../src/dashboard.js");
  const z = zonePctFromBounds([77.5, 87.7, 94.3, 100.5, 115, 999]);
  assert.equal(z[2], 82.6);
  assert.ok(z[6] > 115 && z[6] < 140);
  assert.equal(zonePctFromBounds(null), null);
  assert.equal(parseWorkoutSteps("- 10m Z2", null, { sport: "run", zonePct: z })[0].pct, 82.6);
  assert.equal(parseWorkoutSteps("- 5km 100% Pace", null, { sport: "run", thresholdSecPerKm: 270 })[0].secs, 1350);
  assert.equal(parseWorkoutSteps("- 5km 90% Pace", null, { sport: "run", thresholdSecPerKm: 270 })[0].secs, 1500);
}
console.log("zones from intervals ok");
