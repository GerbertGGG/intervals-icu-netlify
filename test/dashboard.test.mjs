// Testet src/dashboard.js gegen SYNTHETISCHE Intervals.icu-Antworten (nur für Tests,
// nichts davon geht in den Livebetrieb). Ausführen: node test/dashboard.test.mjs
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { buildWidget, handleWidgetRequest } from "../src/widget.js";
import { computeReadiness, computeLoad } from "../src/dashboard-summary.js";
import { parseCravings, findHipFlags } from "../src/dashboard-parse.js";
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
const events = [{ category: "WORKOUT", start_date_local: day(0) + "T00:00:00", name: "Locker 30'", description: "Zweck: Beine lockern", moving_time: 1800 }];
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
assert.equal(classifyRun(activities[2]), "long");
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
assert.ok(wjson.length < 5000, "Widget-Payload klein");
assert.equal(wdg.readiness.verdict.text, d.summary.readiness.verdict.text);
assert.equal(wdg.goal.daysToGo, 3);
assert.equal(wdg.plan.today[0].purpose, "Zweck: Beine lockern");
assert.equal(wdg.hip.recent, 1);
assert.equal(wdg.week.goal, null); // ohne Plan-Load und ohne Konfiguration kein Ziel, nichts erfunden
assert.equal(buildWidget(d, { WEEKLY_TSS_GOAL: "250" }).week.goal, 250);
assert.equal(buildWidget(d, { WEEKLY_TSS_GOAL: "250" }).week.goalSource, "config");
assert.equal(buildWidget(d, { WEEKLY_TSS_GOAL: "kaputt" }).week.goal, null);
assert.equal(wdg.week.days.length, 7);
assert.equal(wdg.week.days[0].date, wdg.week.weekStart);
assert.equal(wdg.week.days.filter((x) => x.load == null).length, 6 - Math.round((Date.parse(today + "T00:00:00Z") - Date.parse(wdg.week.weekStart + "T00:00:00Z")) / 86400000)); // Zukunft = null
assert.equal(wdg.week.days.reduce((a, x) => a + (x.load ?? 0), 0), wdg.week.total);
assert.equal(/Schokolade|Bobingen|Knieschmerz/.test(wjson), false); // keine Freitexte
assert.equal((await handleWidgetRequest(new Request("https://x/api/widget"), env)).status, 401);
assert.equal((await handleWidgetRequest(new Request("https://x/api/widget", { headers: { authorization: "Bearer geheim" } }), env)).status, 200);
// Lücken bleiben null, 0 wird nicht zu "gut"
assert.equal(d.wellness.find((w) => w.motivation === 0), undefined);
assert.ok(d.wellness.every((w) => w.soreness === null));
assert.equal(d.goal.daysToGo, 3);
assert.equal(d.goal.source, "config");
assert.ok(d.weeks.at(-1).complete === false);
// Fitness: Decoupling nur aus langen Läufen
assert.equal(d.fitness.longRuns.length, 1);
assert.equal(d.fitness.longRuns[0].decoupling, 3.1);
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
const hm = d2.runalyze.rows.find((r) => r.label === "Halbmarathon");
assert.equal(hm.bestSeconds, null); assert.equal(hm.prognosisSeconds, 8000);
assert.equal(d2.runalyze.rows[0].bestSeconds, 1537);
assert.equal(d2.runalyze.vdot, 34.67);
assert.equal(d2.runalyze.paces.find((p) => p.key === "threshold").pace, "5:43/km");
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
console.log("dashboard tests ok");
