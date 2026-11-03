import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { analyzeRideStreams, rideSkipOf, rideRow, computeRideEntry, brickMap, swimRow, computeSwimEntry, RIDE_RULES } from "../src/export-tri.js";
import { decouplingFromStreams } from "../src/block-inputs.js";
import { runRow, raceContext, buildDayRows, METHOD_VERSION } from "../src/export-calc.js";
import { RIDE_COLUMNS, SWIM_COLUMNS, RUN_COLUMNS } from "../src/export-schema.js";
import { buildActivityExport } from "../src/export.js";
import { syncExport } from "../scripts/export-sync.mjs";

// Kuenstliche Radfahrt: 1 Probe je Sekunde, watts(t), hr(t), Tempo 8 m/s
function ride(sec, { watts = () => 165, hr = () => 130, speed = () => 8 } = {}) {
  const time = [], w = [], h = [], distance = [];
  let d = 0;
  for (let t = 0; t <= sec; t++) { time.push(t); w.push(watts(t)); h.push(hr(t)); d += t ? speed(t) : 0; distance.push(d); }
  return [{ type: "time", data: time }, { type: "watts", data: w }, { type: "heartrate", data: h }, { type: "distance", data: distance }];
}

// ---------- konstante 165 W bei konstantem Puls ----------
{
  const s = ride(4200);
  const m = analyzeRideStreams(s, 600);
  assert.equal(m.hrAt160_170, 130);
  assert.ok(m.secAt160_170 > 3400 && m.secAt160_170 <= 3600, `sec ${m.secAt160_170}`);
  assert.equal(m.pwN > 600, true);
  assert.equal(m.powerAt130, null); // ohne Streuung im Puls keine Regression
  assert.equal(m.ef, 1.27); // 165 / 130
  assert.equal(Math.abs(decouplingFromStreams(s, "watts", null, { warmupSec: 600 })), 0);
}

// ---------- Puls steigt bei gleicher Leistung: positives Decoupling ----------
{
  const s = ride(4200, { hr: (t) => 120 + (20 * t) / 4200 });
  assert.ok(decouplingFromStreams(s, "watts", null, { warmupSec: 600 }) > 5);
  const m = analyzeRideStreams(s, 600);
  assert.ok(m.hrAt160_170 > 128 && m.hrAt160_170 < 133, `hr ${m.hrAt160_170}`); // Median ueber die Fahrt
}

// ---------- Regression Leistung gegen Puls: 2 W je Schlag ----------
{
  const p = (t) => 140 + 30 * Math.sin(t / 600);
  const m = analyzeRideStreams(ride(5400, { watts: p, hr: (t) => 110 + (p(t) - 140) / 2 }), 600);
  assert.ok(Math.abs(m.pwSlope - 2) < 0.05, `slope ${m.pwSlope}`);
  assert.ok(Math.abs(m.powerAt130 - 180) < 1, `W bei 130: ${m.powerAt130}`);
  assert.ok(Math.abs(m.powerAt120 - 160) < 1, `W bei 120: ${m.powerAt120}`);
  assert.ok(m.pwR2 > 0.99);
}

// ---------- ungleichmaessig (Wechsel 100/250 W alle 30 s): keine gleichmaessigen Fenster ----------
{
  const m = analyzeRideStreams(ride(4200, { watts: (t) => (Math.floor(t / 30) % 2 ? 250 : 100) }), 600);
  assert.deepEqual([m.pwN, m.secAt160_170, m.hrAt160_170], [0, 0, null]);
}

// ---------- Stillstand zaehlt nicht (Ampel 2 min, Tempo 0) ----------
{
  const m = analyzeRideStreams(ride(4200, { speed: (t) => (t > 2000 && t <= 2120 ? 0 : 8), watts: (t) => (t > 2000 && t <= 2120 ? 0 : 165) }), 600);
  assert.ok(m.movingSec < 4100, `moving ${m.movingSec}`);
  assert.equal(m.hrAt160_170, 130);
}

// ---------- Uebersprungene Fahrten ----------
{
  const base = { id: "b1", type: "Ride", start_date_local: "2026-10-01T09:00:00", moving_time: 3600, elapsed_time: 3700, icu_average_watts: 160, has_heartrate: true, average_heartrate: 125 };
  assert.equal(rideSkipOf(base), null);
  assert.equal(rideSkipOf({ ...base, name: "4x4 min VO2" }), "interval");
  assert.equal(rideSkipOf({ ...base, name: "Rolle 30/30" }), "interval");
  assert.equal(rideSkipOf({ ...base, icu_zone_times: [{ id: "Z5", secs: 300 }] }), "interval"); // ≥ 4 min Z5 wie intensityKindOf
  assert.equal(rideSkipOf({ ...base, icu_average_watts: null }), "noPower");
  assert.equal(rideSkipOf({ ...base, moving_time: 1000 }), "short");
  assert.equal(rideSkipOf({ ...base, has_heartrate: false, average_heartrate: null }), "noHr");
  // Intensitaet auch aus dem gekoppelten Workout
  const ev = new Map([["77", { id: 77, name: "HIIT 6x1" }]]);
  assert.equal(rideSkipOf({ ...base, paired_event_id: 77 }, ev), "interval");
  const row = rideRow({ ...base, name: "4x4 min VO2", icu_weighted_avg_watts: 190, icu_intensity: 75.4, icu_variability_index: 1.19, icu_ftp: 252, average_cadence: 84.6, trainer: true }, null);
  assert.deepEqual(Object.keys(row), RIDE_COLUMNS);
  assert.deepEqual([row.intensity, row.streamStatus, row.streamSkip, row.if, row.vi, row.ftp, row.trainer, row.endLocal], ["bikeHiit", "pending", "interval", 0.754, 1.19, 252, true, "2026-10-01T10:01:40"]);
}

// ---------- Koppellaeufe ----------
{
  const r = { id: "R", type: "VirtualRide", start_date_local: "2026-10-10T08:00:00", elapsed_time: 3600, moving_time: 3500, power_load: 60, icu_training_load: 40 };
  const run10 = { id: "L10", type: "Run", start_date_local: "2026-10-10T09:10:00", moving_time: 1800 };
  const run45 = { id: "L45", type: "Run", start_date_local: "2026-10-10T09:45:00", moving_time: 1800 };
  const bm = brickMap([r, run10, run45]);
  assert.deepEqual(bm.get("L10"), { brickRideId: "R", brickGapMin: 10, brickRideMin: 58.3, brickRideLoad: 60 });
  assert.equal(bm.has("L45"), false);
  const ctx = { ...raceContext([]), bricks: bm };
  const row10 = runRow(run10, null, ctx), row45 = runRow(run45, null, ctx);
  assert.deepEqual([row10.brick, row10.brickGapMin, row45.brick, row45.brickRideId], [true, 10, false, null]);
  assert.deepEqual(Object.keys(row10), RUN_COLUMNS);
  // Lauf vor der Fahrt ist kein Koppellauf; 3 min Ueberlappung der Uhren zaehlt als Abstand 0
  assert.equal(brickMap([r, { id: "V", type: "Run", start_date_local: "2026-10-10T07:00:00" }]).has("V"), false);
  assert.equal(brickMap([r, { id: "U", type: "Run", start_date_local: "2026-10-10T08:57:00" }]).get("U").brickGapMin, 0);
}

// ---------- Schwimmen ----------
{
  const a = { id: "s1", type: "Swim", start_date_local: "2026-10-31T14:38:24", name: "Schwimmbad", description: "2x500: 9:40 / 9:52, Pause 45 s", distance: 1125, moving_time: 1455, elapsed_time: 1915, pool_length: 25, average_heartrate: 124, icu_training_load: 24, icu_rpe: 5, feel: 3 };
  const row = swimRow(a, { lapsStatus: "ok", movingSec: 1400, test: { t1Sec: 580, t2Sec: 592, pauseSec: 45 } });
  assert.deepEqual(Object.keys(row), SWIM_COLUMNS);
  assert.deepEqual([row.pool, row.distanceM, row.movingMin, row.pace100, row.test500a, row.test500b, row.testPauseS, row.testPassed], ["pool", 1125, 23.3, 124, 580, 592, 45, true]);
  assert.equal(swimRow(a, { lapsStatus: "ok", movingSec: 1400, test: { t1Sec: 580, t2Sec: 620, pauseSec: 45 } }).testPassed, false); // 8 s/100 m langsamer
  assert.equal(swimRow(a, { lapsStatus: "ok", movingSec: 1400, test: { t1Sec: 580, t2Sec: 590, pauseSec: 90 } }).testPassed, false); // Pause zu lang
  assert.equal(swimRow({ ...a, type: "OpenWaterSwim", pool_length: null }, null).pool, "open");
  assert.equal(swimRow(a, null).lapsStatus, "pending");
}

// ---------- Kalender: Hueft-OP (echter Eintrag: INJURED 14.10.–11.11.2024, Ende exklusiv) ----------
{
  const rows = buildDayRows(["2024-10-13", "2024-10-14", "2024-11-10", "2024-11-11"], { events: [{ category: "INJURED", start_date_local: "2024-10-14T00:00:00", end_date_local: "2024-11-11T00:00:00", name: "Hüft-OP" }] });
  assert.deepEqual(rows.map((r) => r.sick), [false, true, true, false]);
  assert.equal(rows[1].calendar, "Hüft-OP (INJURED)");
}

// ---------- Fahrt mit Zeitreihen: Vergleichseinheit und Ausgangswert wie im Dashboard (fetch gestubbt) ----------
const realFetch = globalThis.fetch;
const ok = (body) => new Response(JSON.stringify(body), { status: 200 });
try {
  const streamsFor = new Map();
  globalThis.fetch = async (url) => {
    const u = String(url);
    const m = u.match(/\/activity\/([^/]+)\/streams/);
    if (m) return ok(streamsFor.get(m[1]));
    if (u.includes("/map")) return ok({ latlngs: [[48.3, 10.8]], weather: null });
    if (u.includes("open-meteo")) return ok({ hourly: { time: ["2026-10-19T11:00"], temperature_2m: [12], relative_humidity_2m: [70], wind_speed_10m: [3] } });
    throw new Error("unerwartet " + u);
  };
  const env = { INTERVALS_API_KEY: "k", ATHLETE_ID: "i1" };
  // Wie 19.10.2026 (echte Metadaten): VirtualRide 60 min, kein Warmup-Block, 165 W, Intervals-Decoupling 9,8 -> Ausgangswert
  const base19 = { id: "c19", type: "VirtualRide", name: "Vergleichstest", start_date_local: "2026-10-19T11:31:41", moving_time: 3600, elapsed_time: 3601, trainer: true, icu_warmup_time: 0, icu_cooldown_time: 0, icu_average_watts: 165, icu_weighted_avg_watts: 165, icu_variability_index: 1, decoupling: 9.803871, has_heartrate: true, average_heartrate: 101 };
  streamsFor.set("c19", ride(3600, { hr: (t) => 95 + (12 * t) / 3600 }));
  const e19 = await computeRideEntry(env, base19);
  assert.equal(e19.testKind, "baseline");
  assert.equal(e19.metrics.decoup60, 9.8); // Intervals-Wert, deckt das Messfenster genau ab (wie classifyRide/Dashboard)
  assert.ok(e19.metrics.mainHr >= 100 && e19.metrics.mainHr <= 102, `mainHr ${e19.metrics.mainHr}`);
  assert.equal(e19.weather, null); // Rolle: kein Wetter
  // Echte Vergleichseinheit: 70 min, die ersten 10 min Einrollen automatisch, danach 60 min bei 165 W
  const cmp = { ...base19, id: "c2", start_date_local: "2026-11-01T10:00:00", moving_time: 4200, elapsed_time: 4200, decoupling: 4.2 };
  streamsFor.set("c2", ride(4200, { watts: (t) => (t < 600 ? 120 : 165), hr: (t) => (t < 600 ? 100 : 118 + (4 * (t - 600)) / 3600) }));
  const e2 = await computeRideEntry(env, cmp);
  assert.equal(e2.testKind, "compare");
  assert.equal(e2.metrics.mainHr, 120); // Ø-Puls im Messfenster 10–70 min
  assert.equal(e2.metrics.decoup60, 1.7); // selbst gerechnet (121/119 − 1), nicht der Intervals-Wert 4,2: der deckt das Fenster nicht
  assert.ok(Math.abs(e2.metrics.hrAt160_170 - 120) <= 0.3, `hr ${e2.metrics.hrAt160_170}`); // Median; die 2 min nach dem Einrollen fallen raus (Leistung davor ungleichmaessig)
  const row2 = rideRow(cmp, e2);
  assert.deepEqual([row2.streamStatus, row2.testKind, row2.mainHr], ["ok", "compare", 120]);
  // Intervallfahrt: keine Kennzahlen, aber Zeitreihen fuer die Testerkennung
  streamsFor.set("i1", ride(3600, { watts: (t) => (Math.floor(t / 240) % 2 ? 260 : 120) }));
  const eI = await computeRideEntry(env, { ...base19, id: "i1", name: "4x4 min VO2", type: "Ride", trainer: false });
  assert.deepEqual([eI.streamStatus, eI.streamSkip, eI.testKind], ["skip", "interval", null]);
  assert.equal(eI.weather.src, "openmeteo"); // draussen: Wetter
  // Fahrt ohne Leistung: kein Abruf der Zeitreihen
  const eN = await computeRideEntry(env, { ...base19, id: "n1", icu_average_watts: null, trainer: true });
  assert.deepEqual([eN.streamStatus, eN.streamSkip], ["skip", "noPower"]);

  // Schwimmen: Laps (Pausen am Rand fallen raus) und Test aus den Intervallen
  globalThis.fetch = async (url) => {
    const u = String(url);
    if (u.includes("/activity/sw1/intervals")) return ok({ icu_intervals: [
      { type: "WORK", distance: 500, moving_time: 580, start_time: 0, end_time: 580 },
      { type: "RECOVERY", distance: 0, moving_time: 0, start_time: 580, end_time: 630 },
      { type: "WORK", distance: 500, moving_time: 595, start_time: 630, end_time: 1225 },
    ] });
    if (u.includes("/activity/sw2/intervals")) return new Response("nope", { status: 404 });
    throw new Error("unerwartet " + u);
  };
  const sw = { id: "sw1", type: "Swim", name: "Test", start_date_local: "2026-10-05T07:00:00", distance: 1000, moving_time: 1300, elapsed_time: 1400, pool_length: 25 };
  const es = await computeSwimEntry(env, sw);
  assert.deepEqual([es.lapsStatus, es.movingSec, es.test.t1Sec, es.test.t2Sec, es.test.pauseSec], ["ok", 1175, 580, 595, 50]);
  assert.equal(swimRow(sw, es).testPassed, true); // 3 s/100 m, Pause 50 s
  const es2 = await computeSwimEntry(env, { ...sw, id: "sw2" });
  assert.deepEqual([es2.lapsStatus, es2.movingSec, es2.test], ["none", 1300, null]);
} finally {
  globalThis.fetch = realFetch;
}

// ---------- Action -> D1 -> Worker: Fahrten, Koppellauf, Schwimmen ----------
{
  const db = new DatabaseSync(":memory:");
  const d1 = async (sql, params = []) => { const st = db.prepare(sql); return /^\s*(SELECT|PRAGMA)/i.test(sql) ? st.all(...params) : (st.run(...params), []); };
  const EXPORT_DB = { prepare: (sql) => ({ bind: (...p) => ({ all: async () => ({ results: db.prepare(sql).all(...p) }) }) }) };
  const day = new Date(Date.now() - 20 * 86400000).toISOString().slice(0, 10); // aelter als 14 Tage: fehlendes Wetter wird nicht erneut gefragt
  const acts = [
    { id: "R1", type: "VirtualRide", name: "Rolle locker", start_date_local: `${day}T08:00:00`, moving_time: 3600, elapsed_time: 3600, trainer: true, icu_average_watts: 165, icu_weighted_avg_watts: 166, has_heartrate: true, average_heartrate: 128, power_load: 50 },
    { id: "L1", type: "Run", name: "Koppel", start_date_local: `${day}T09:08:00`, moving_time: 1800, distance: 5000, has_heartrate: true, average_heartrate: 140 },
    { id: "S1", type: "Swim", name: "Schwimmen", start_date_local: `${day}T18:00:00`, distance: 1500, moving_time: 1800, elapsed_time: 2100, pool_length: 25 },
  ];
  const calls = [];
  globalThis.fetch = async (url) => {
    const u = String(url); calls.push(u);
    const q = () => { const s = new URL(u).searchParams; return [s.get("oldest"), s.get("newest")]; };
    if (u.includes("/activities?")) { const [lo, hi] = q(); return ok(acts.filter((a) => a.start_date_local.slice(0, 10) >= lo && a.start_date_local.slice(0, 10) <= hi)); }
    if (u.includes("/wellness?") || u.includes("/events?")) return ok([]);
    if (u.includes("/activity/R1/streams")) return ok(ride(3600));
    if (u.includes("/activity/L1/streams")) return new Response("x", { status: 404 });
    if (u.includes("/activity/S1/intervals")) return ok({ icu_intervals: [{ type: "WORK", distance: 1500, moving_time: 1700 }] });
    if (u.includes("/map")) return ok({ latlngs: [], weather: null });
    throw new Error("unerwartet " + u);
  };
  try {
    const env = { INTERVALS_API_KEY: "k", ATHLETE_ID: "i1" };
    const s1 = await syncExport({ env, d1, from: day, to: day, today: new Date().toISOString().slice(0, 10), log: () => {} });
    assert.deepEqual([s1.runs, s1.rides, s1.swims, s1.ridesWritten, s1.swimsWritten], [1, 1, 1, 1, 1]);
    const rides = await buildActivityExport({ EXPORT_DB }, "rides", { from: day, to: day });
    assert.deepEqual([rides.total, rides.rows[0].id, rides.rows[0].streamStatus, rides.rows[0].hrAt160_170, rides.rows[0].trainer, rides.rows[0].methodVersion], [1, "R1", "ok", 130, true, METHOD_VERSION]);
    const runs = await buildActivityExport({ EXPORT_DB }, "runs", { from: day, to: day });
    assert.deepEqual([runs.rows[0].brick, runs.rows[0].brickRideId, runs.rows[0].brickGapMin], [true, "R1", 8]);
    const swims = await buildActivityExport({ EXPORT_DB }, "swims", { from: day, to: day });
    assert.deepEqual([swims.rows[0].movingMin, swims.rows[0].pace100, swims.rows[0].lapsStatus], [28.3, 113, "ok"]);
    // zweiter Lauf: nichts neu
    const before = calls.length;
    const s2 = await syncExport({ env, d1, from: day, to: day, today: new Date().toISOString().slice(0, 10), log: () => {} });
    assert.deepEqual([s2.ridesComputed, s2.swimsComputed, s2.ridesWritten, s2.swimsWritten, s2.runsWritten], [0, 0, 0, 0, 0]);
    assert.ok(calls.slice(before).every((u) => !/\/streams\.json|\/intervals(\?|$)/.test(u)), "keine Zeitreihen/Laps beim zweiten Lauf");
  } finally {
    globalThis.fetch = realFetch;
  }
}

console.log("export-tri ok");
