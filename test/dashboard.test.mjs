// Testet src/dashboard.js gegen SYNTHETISCHE Intervals.icu-Antworten (nur für Tests,
// nichts davon geht in den Livebetrieb). Ausführen: node test/dashboard.test.mjs
import assert from "node:assert/strict";
import { writeFileSync } from "node:fs";
import { validateSnapshot, bestForDistance } from "../src/runalyze-snapshot.js";
import { buildDashboard, handleDashboardRequest, classifyRun } from "../src/dashboard.js";

const today = "2026-09-30";
const day = (n) => new Date(Date.parse(today + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const wellness = [];
for (let i = 55; i >= 0; i--) {
  if (i % 5 === 0) continue; // Lücken
  wellness.push({ id: day(-i), ctl: 30 + (55 - i) * 0.1, atl: 28 + Math.sin(i) * 6, mood: 1 + (i % 3), motivation: i % 7 === 0 ? 0 : 2, fatigue: 1 + (i % 4), soreness: null, sleepQuality: 2, restingHR: 52 + (i % 4), comments: "HH 16:30" });
}
const act = (n, o) => ({ id: n, start_date_local: day(-n) + "T10:00:00", type: "Run", icu_training_load: 50, average_heartrate: 140, decoupling: 3.1, icu_rpe: 5, feel: 2, ...o });
const activities = [
  act(1, { name: "Bobingen - Tempowechsellauf", distance: 7130, moving_time: 2897 }),
  act(3, { name: "Bobingen - Kurzer Grundlagenlauf", distance: 6030, moving_time: 2383 }),
  act(5, { name: "Bobingen - Long Slow Run", distance: 16200, moving_time: 6771, description: "locker" }),
  act(7, { name: "Lauf ohne Angabe", distance: 5000, moving_time: 1800 }),
  act(9, { name: "Radfahren", type: "Ride", distance: 30000, moving_time: 3600 }),
  act(12, { name: "Intervalle 5x1000", distance: 9000, moving_time: 3200, tags: ["intervals"] }),
];
const events = [{ category: "WORKOUT", start_date_local: day(0) + "T00:00:00", name: "Locker 30'", description: "Zweck: Beine lockern", moving_time: 1800 }];
globalThis.fetch = async (url) => {
  const u = String(url);
  const body = u.includes("/wellness") ? wellness : u.includes("/activities") ? activities : u.includes("/events") ? events : null;
  return new Response(JSON.stringify(body), { status: body ? 200 : 404 });
};
const kv = new Map();
const env = { ATHLETE_ID: "i1", INTERVALS_API_KEY: "k", DASHBOARD_TOKEN: "geheim", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const d = await buildDashboard(env, today);
writeFileSync(new URL("./fixture-dashboard.json", import.meta.url), JSON.stringify(d));

// Pulsregel
for (const r of d.runs) {
  if (r.kind === "intensity" || r.kind === "unknown") assert.equal(r.avgHr, null, r.name), assert.equal(r.decoupling, null);
  else assert.equal(r.avgHr, 140);
}
assert.equal(classifyRun(activities[0]), "intensity");
assert.equal(classifyRun(activities[1]), "base");
assert.equal(classifyRun(activities[2]), "long");
assert.equal(classifyRun(activities[5]), "intensity");
assert.equal(d.runs.length, 5); // Rad nicht dabei
// Lücken bleiben null, 0 wird nicht zu "gut"
assert.equal(d.wellness.find((w) => w.motivation === 0), undefined);
assert.ok(d.wellness.every((w) => w.soreness === null));
assert.equal(d.goal.daysToGo, 3);
assert.equal(d.goal.source, "config");
assert.ok(d.weeks.at(-1).complete === false);
// Fitness: Puls nur aus Grundlagen/Long
assert.ok(d.fitness.baseRuns.every((r) => r.kind === "base" || r.kind === "long"));
assert.equal(d.fitness.longRuns.length, 1);
assert.equal(d.runalyze, null); // ohne Snapshot: fehlt, keine Platzhalter
// Runalyze-Snapshot (synthetische Werte, nur Test)
const snap = { fetchedAt: "2026-09-30T05:00:00Z", prognosis: [{ distanceKm: 5, seconds: 1600 }, { distanceKm: 21.1, seconds: 8000 }],
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
