import assert from "node:assert/strict";
import { DatabaseSync } from "node:sqlite";
import { analyzeExportStreams, streamSkipOf, runRow, buildDayRows, sickDaysFromEvents, METHOD_VERSION } from "../src/export-calc.js";
import { RUN_COLUMNS, DAY_COLUMNS, encodeRow, decodeRow } from "../src/export-schema.js";
import { buildRunExport, buildDayExport, toCsv, toTable } from "../src/export.js";
import { syncExport, sqlLiteral } from "../scripts/export-sync.mjs";
import { pickOpenMeteoValues, averageNativeWeather, extractStartLatLng } from "../src/weather.js";

// Kuenstliche Zeitreihe: 1 Probe je Sekunde, speed(t) in m/s, hr(t), alt(t) optional
function synth(sec, { speed = () => 2.5, hr = () => 130, alt = null, gaps = [], jumps = [] } = {}) {
  const time = [0], distance = [0], heartrate = [hr(0)], altitude = alt ? [alt(0)] : null;
  let t = 0, d = 0;
  for (let i = 1; i <= sec; i++) {
    const g = gaps.find((x) => x.at === i);
    t += g ? g.sec : 1; // Auto-Pause: Zeit springt, Distanz nicht
    d += g ? 0 : speed(i);
    const j = jumps.find((x) => x.at === i);
    if (j) d += j.m; // GPS-Sprung
    time.push(t); distance.push(d); heartrate.push(hr(i));
    if (altitude) altitude.push(alt(i));
  }
  const s = [{ type: "time", data: time }, { type: "distance", data: distance }, { type: "heartrate", data: heartrate }];
  if (altitude) s.push({ type: "altitude", data: altitude });
  return s;
}

// ---------- konstante Pace und konstanter Puls ----------
{
  const m = analyzeExportStreams(synth(3600, { alt: () => 500 }));
  assert.equal(m.paceBand125_135, 400); // 2,5 m/s = 6:40/km
  assert.ok(m.secBand125_135 > 3250 && m.secBand125_135 <= 3300, `secBand ${m.secBand125_135}`);
  assert.equal(m.paceBand120_128, null);
  assert.equal(m.secBand120_128, 0);
  assert.equal(m.paceBand125_135Flat, 400);
  assert.equal(m.gradeShareFlat, 1);
  assert.equal(Math.abs(m.decoup), 0);
  assert.equal(m.hrDriftBpm, 0);
  assert.equal(m.effAt130, null); // ohne Streuung im Puls keine Regression
  assert.ok(m.effN > 600);
  assert.equal(m.gpsCorrectedM, 0);
}

// ---------- ohne Hoehe: Flat-Werte und Gelaende null ----------
{
  const m = analyzeExportStreams(synth(1800));
  assert.equal(m.paceBand125_135, 400);
  assert.equal(m.paceBand125_135Flat, null);
  assert.equal(m.gradeShareFlat, null);
}

// ---------- Puls steigt bei gleicher Pace: positive Drift ----------
{
  const m = analyzeExportStreams(synth(3600, { hr: (t) => 120 + (20 * t) / 3600 }));
  assert.ok(m.decoup > 5, `decoup ${m.decoup}`);
  assert.ok(m.hrDriftBpm > 10 && m.hrDriftBpm < 15, `drift ${m.hrDriftBpm}`); // ~ 2/3 von (20 · 55/60)
  assert.equal(m.paceBand120_128, 400);
  // Regression: konstante Geschwindigkeit -> Steigung 0, Vorhersage 150 m/min
  assert.equal(m.effSlope, 0);
  assert.equal(m.effAt130, 150);
}

// ---------- Effizienz-Regression: Geschwindigkeit steigt mit dem Puls ----------
{
  // HF pendelt 115–145 (10-min-Periode), Tempo = 120 m/min + 2 m/min je Schlag ueber 115
  const hr = (t) => 130 + 15 * Math.sin((2 * Math.PI * t) / 600);
  const m = analyzeExportStreams(synth(3600, { hr, speed: (t) => (120 + 2 * (hr(t) - 115)) / 60 }));
  assert.ok(Math.abs(m.effSlope - 2) < 0.1, `slope ${m.effSlope}`);
  assert.ok(Math.abs(m.effAt130 - 150) < 1, `effAt130 ${m.effAt130}`);
  assert.ok(m.effR2 > 0.95);
  assert.equal(m.hrDriftBpm !== undefined, true);
}

// ---------- Auto-Pause und GPS-Sprung ----------
{
  const m = analyzeExportStreams(synth(3600, { gaps: [{ at: 1800, sec: 120 }], jumps: [{ at: 2400, m: 300 }] }));
  assert.equal(m.paceBand125_135, 400); // Pause und Sprung verfaelschen die Pace nicht
  assert.ok(m.gpsCorrectedM > 290 && m.gpsCorrectedM < 310, `korrigiert ${m.gpsCorrectedM}`);
  assert.ok(Math.abs(m.decoup) < 0.5, `decoup ${m.decoup}`);
  assert.ok(m.movingSec <= 3601);
  // Stehen: 60 s ohne Fortschritt, die Zeit laeuft weiter
  const stand = analyzeExportStreams(synth(3600, { speed: (t) => (t > 1500 && t <= 1560 ? 0 : 2.5) }));
  assert.equal(stand.paceBand125_135, 400);
  assert.ok(stand.movingSec < 3560);
}

// ---------- zu wenig Zeit im Band ----------
{
  const m = analyzeExportStreams(synth(1800, { hr: (t) => (t < 540 ? 130 : 145) }));
  assert.ok(m.secBand125_135 < 300, `sec ${m.secBand125_135}`);
  assert.equal(m.paceBand125_135, null);
}

// ---------- kein Puls: null ----------
assert.equal(analyzeExportStreams([{ type: "time", data: [0, 1] }, { type: "distance", data: [0, 2] }]), null);

// ---------- Uebersprungene Laeufe mit Grund ----------
{
  const base = { id: "i1", type: "Run", start_date_local: "2026-10-01T08:00:00", moving_time: 3600, distance: 9000, has_heartrate: true, average_heartrate: 130 };
  assert.equal(streamSkipOf(base), null);
  assert.equal(streamSkipOf({ ...base, name: "6x800 Bahn" }), "interval");
  assert.equal(streamSkipOf({ ...base, tags: ["#intervalle"] }), "interval");
  assert.equal(streamSkipOf({ ...base, name: "Wettkampf Stadtlauf" }), "race");
  assert.equal(streamSkipOf({ ...base, type: "VirtualRun" }), "treadmill");
  assert.equal(streamSkipOf({ ...base, moving_time: 1100 }), "short");
  assert.equal(streamSkipOf({ ...base, has_heartrate: false, average_heartrate: null }), "noHr");
  const row = runRow({ ...base, name: "6x800 Bahn" }, { streamStatus: "skip", streamSkip: "interval", metrics: null, weather: { tempC: 12.3, humidity: 80, windMs: 3.1, src: "openmeteo" } });
  assert.equal(row.interval, true);
  assert.equal(row.runClass, "intensity");
  assert.equal(row.streamSkip, "interval");
  assert.equal(row.paceBand125_135, null);
  assert.equal(row.tempC, 12.3);
  assert.deepEqual(Object.keys(row), RUN_COLUMNS);
  const tm = runRow({ ...base, type: "VirtualRun" }, null);
  assert.equal(tm.streamStatus, "skip");
  assert.equal(tm.weatherSrc, null);
  assert.equal(runRow(base, null).streamStatus, "pending");
}

// ---------- Wetter ----------
{
  const day = { times: ["2026-10-30T10:00", "2026-10-30T11:00"], temps: [10, 12], humidities: [80, 70], winds: [2, 4] };
  assert.deepEqual(pickOpenMeteoValues(day, "2026-10-30T11:00"), { tempC: 12, humidity: 70, windMs: 4 });
  assert.deepEqual(pickOpenMeteoValues(day, "2026-10-30T15:00"), { tempC: 11, humidity: 75, windMs: 3 });
  assert.equal(pickOpenMeteoValues({ times: [], temps: [null], humidities: [null], winds: [] }, null), null);
  assert.deepEqual(averageNativeWeather({ weather: { points: [{ times: [{ temp: 20, humidity: 50 }, { temp: 22, humidity: 60 }] }] } }), { tempC: 21, humidity: 55 });
  assert.equal(averageNativeWeather({ weather: { points: [{ times: [{ temp: 20 }] }] } }), null);
  assert.deepEqual(extractStartLatLng({ latlngs: [null, [52.12345, 13.45678]] }), { lat: 52.123, lng: 13.457 });
}

// ---------- Tageszeilen ----------
{
  const rows = buildDayRows(["2026-10-30", "2026-10-31"], {
    wellness: [{ id: "2026-10-31", ctl: 21.608, atl: 27.6, rampRate: 1.78, restingHR: 45, hrv: 54, sleepSecs: 33360, sleepScore: 91, sleepQuality: 1, soreness: 3, fatigue: 2, mood: 3, motivation: 2, weight: null, Calories: 3531, CalorieGoal: 2890, Protein: 92.9, Carbs: 340.4 }],
    activities: [
      { type: "Run", start_date_local: "2026-10-30T11:37:55", moving_time: 2152, icu_training_load: 25 },
      { type: "Ride", start_date_local: "2026-10-30T18:00:00", moving_time: 3600, icu_training_load: 30, power_load: 50 },
      { type: "WeightTraining", start_date_local: "2026-10-30T19:00:00", moving_time: 1800, icu_training_load: 10, description: "30 Sätze · 25,4 t Volumen" },
      { type: "Swim", start_date_local: "2026-10-31T14:38:24", moving_time: 1455, elapsed_time: 1915, icu_training_load: 24 },
    ],
    events: [{ category: "SICK", start_date_local: "2026-10-31T00:00:00", end_date_local: "2026-11-01T00:00:00" }],
    weights: [{ date: "2026-10-30", kg: 110.4 }],
  });
  assert.deepEqual(Object.keys(rows[0]), DAY_COLUMNS);
  assert.deepEqual([rows[0].loadRun, rows[0].loadBike, rows[0].loadStrength, rows[0].minRun, rows[0].minBike, rows[0].strengthSets], [25, 50, 10, 36, 60, 30]);
  assert.equal(rows[0].weightKg, 110.4);
  assert.equal(rows[0].ctl, null);
  assert.equal(rows[0].sick, false);
  assert.equal(rows[1].sick, true);
  assert.deepEqual([rows[1].ctl, rows[1].sleepH, rows[1].sleepScore, rows[1].kcal, rows[1].minSwim, rows[1].loadSwim], [21.6, 9.3, 91, 3531, 24, 24]);
  assert.equal(rows[1].weightKg, null);
  assert.ok(sickDaysFromEvents([{ category: "NOTE", name: "krank", start_date_local: "2026-10-01T00:00:00" }]).has("2026-10-01"));
}

// ---------- CSV ----------
assert.equal(toCsv([{ a: 'x,"y"', b: null, c: ["t1", "t2"] }], ["a", "b", "c"]), 'a,b,c\n"x,""y""",,t1|t2\n');

// ---------- SQL-Literale und Zeilen-Kodierung ----------
assert.equal(sqlLiteral("O'Brien\0"), "'O''Brien'");
assert.equal(sqlLiteral(null), "NULL");
assert.equal(sqlLiteral(NaN), "NULL");
assert.equal(sqlLiteral(true), "1");
{
  const row = { ...Object.fromEntries(RUN_COLUMNS.map((c) => [c, null])), id: "i1", tags: ["#a", "b"], treadmill: true, race: false, distKm: 9.12 };
  assert.deepEqual(decodeRow(Object.fromEntries(RUN_COLUMNS.map((c, i) => [c, encodeRow(row, RUN_COLUMNS)[i]])), RUN_COLUMNS), { ...row, tags: ["#a", "b"] });
}

// ---------- GitHub Action -> D1 -> Worker (Intervals gestubbt, D1 = node:sqlite) ----------
{
  const db = new DatabaseSync(":memory:");
  const d1 = async (sql, params = []) => { const st = db.prepare(sql); return /^\s*(SELECT|PRAGMA)/i.test(sql) ? st.all(...params) : (st.run(...params), []); };
  // D1-Binding des Workers (prepare().bind().all()) ueber dieselbe Datenbank
  const EXPORT_DB = { prepare: (sql) => ({ bind: (...p) => ({ all: async () => ({ results: db.prepare(sql).all(...p) }) }) }) };
  const ago = (n) => new Date(Date.now() - n * 86400000).toISOString().slice(0, 10);
  const today = ago(0);
  const run = (id, day, extra = {}) => ({ id, type: "Run", start_date_local: `${day}T08:00:00`, moving_time: 3600, distance: 9000, has_heartrate: true, average_heartrate: 130, icu_training_load: 40, ...extra });
  const D = [ago(40), ago(39), ago(38), ago(37), ago(36)];
  let acts = [run("a1", D[0]), run("a2", D[1]), run("a3", D[2], { type: "VirtualRun" }), run("a4", D[3]), run("a5", D[4], { name: "5x1000" }), { id: "r1", type: "Ride", start_date_local: `${D[0]}T18:00:00`, moving_time: 3600, icu_training_load: 50 }];
  const calls = [];
  let streamsFail = false;
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    const u = String(url);
    calls.push(u);
    const ok = (body) => new Response(JSON.stringify(body), { status: 200 });
    const range = () => { const q = new URL(u).searchParams; return [q.get("oldest"), q.get("newest")]; };
    if (u.includes("/activities?")) { const [lo, hi] = range(); return ok(acts.filter((a) => a.start_date_local.slice(0, 10) >= lo && a.start_date_local.slice(0, 10) <= hi)); }
    if (u.includes("/wellness?")) return ok([{ id: D[0], ctl: 21.6, atl: 27.6, restingHR: 45, sleepSecs: 30000 }]);
    if (u.includes("/events?")) return ok([{ category: "SICK", start_date_local: `${D[2]}T00:00:00`, end_date_local: `${D[3]}T00:00:00` }]);
    if (u.includes("/map")) return ok({ latlngs: [[52.5, 13.4]], weather: null });
    if (u.includes("open-meteo")) return ok({ hourly: { time: [`${D[0]}T08:00`], temperature_2m: [9.5], relative_humidity_2m: [88], wind_speed_10m: [2.2] } });
    if (u.includes("/activity/a4/streams")) return new Response("not found", { status: 404 });
    if (u.includes("/streams")) return streamsFail ? new Response("boom", { status: 400 }) : ok(synth(3600));
    throw new Error("unerwartet " + u);
  };
  const env = { INTERVALS_API_KEY: "k", ATHLETE_ID: "i1" };
  const quiet = () => {};
  try {
    // Erster Lauf der Action: alles neu
    const s1 = await syncExport({ env, d1, from: D[0], to: D[4], today, log: quiet });
    assert.deepEqual([s1.runs, s1.computed, s1.reused, s1.pending, s1.runsWritten, s1.days, s1.daysWritten], [5, 5, 0, 0, 5, 5, 5]);
    assert.equal(calls.filter((u) => u.includes("/streams")).length, 3); // a1, a2, a4 (404)
    // Worker liest nur aus D1
    const before = calls.length;
    const r1 = await buildRunExport({ EXPORT_DB }, { from: D[0], to: D[4], limit: 2 });
    assert.equal(calls.length, before); // kein Abruf von Intervals im Worker
    assert.deepEqual([r1.total, r1.count, r1.pending, r1.methodVersion], [5, 2, 0, METHOD_VERSION]);
    assert.equal(r1.rows[0].paceBand125_135, 400);
    assert.deepEqual([r1.rows[0].tempC, r1.rows[0].humidity, r1.rows[0].windMs, r1.rows[0].weatherSrc, r1.rows[0].methodVersion], [9.5, 88, 2.2, "openmeteo", METHOD_VERSION]);
    assert.equal(r1.rows[0].treadmill, false);
    assert.deepEqual(r1.rows[0].tags, []);
    assert.equal(r1.syncedFrom, D[0]);
    assert.ok(r1.syncedAt);
    assert.equal(r1.nextCursor, `${D[1]}T08:00:00|a2`);
    const r2 = await buildRunExport({ EXPORT_DB }, { from: D[0], to: D[4], cursor: r1.nextCursor });
    assert.deepEqual(r2.rows.map((r) => [r.id, r.streamStatus, r.streamSkip]), [["a3", "skip", "treadmill"], ["a4", "none", null], ["a5", "skip", "interval"]]);
    assert.equal(r2.nextCursor, null);
    assert.deepEqual(Object.keys(r2.rows[0]), RUN_COLUMNS);
    const t = toTable(r2, RUN_COLUMNS);
    assert.equal(t.rows[0].length, RUN_COLUMNS.length);
    // Standardbereich: erster Tag in D1 bis gestern
    const r3 = await buildRunExport({ EXPORT_DB }, {});
    assert.equal(r3.from, D[0]);
    assert.equal(r3.total, 5);

    const days = await buildDayExport({ EXPORT_DB }, { from: D[0], to: D[4], limit: 3 });
    assert.deepEqual(Object.keys(days.rows[0]), DAY_COLUMNS);
    assert.deepEqual([days.rows[0].ctl, days.rows[0].loadRun, days.rows[0].loadBike, days.rows[0].sleepH], [21.6, 40, 50, 8.3]);
    assert.deepEqual(days.rows.map((r) => r.sick), [false, false, true]);
    assert.equal(days.nextCursor, D[3]);
    assert.equal((await buildDayExport({ EXPORT_DB }, { from: D[0], to: D[4], cursor: days.nextCursor })).count, 2);

    // Zweiter Lauf ohne Aenderung: nichts neu gerechnet, nichts geschrieben
    const streams1 = calls.filter((u) => u.includes("/streams")).length;
    const s2 = await syncExport({ env, d1, from: D[0], to: D[4], today, log: quiet });
    assert.deepEqual([s2.computed, s2.reused, s2.runsWritten, s2.daysWritten], [0, 5, 0, 0]);
    assert.equal(calls.filter((u) => u.includes("/streams")).length, streams1);

    // Umbenennung zum Rennen: Zeitreihen-Kennzahlen fallen weg (neu gerechnet), geloeschte Aktivitaet verschwindet aus D1
    acts = acts.map((a) => (a.id === "a2" ? { ...a, name: "Wettkampf Stadtlauf" } : a)).filter((a) => a.id !== "a4");
    const s3 = await syncExport({ env, d1, from: D[0], to: D[4], today, log: quiet });
    assert.deepEqual([s3.computed, s3.runsWritten, s3.runsDeleted], [1, 1, 1]);
    const a2 = (await buildRunExport({ EXPORT_DB }, { from: D[1], to: D[1] })).rows[0];
    assert.deepEqual([a2.race, a2.streamSkip, a2.paceBand125_135], [true, "race", null]);
    assert.equal((await buildRunExport({ EXPORT_DB }, { from: D[0], to: D[4] })).total, 4);

    // Neuer Lauf, Zeitreihen nicht abrufbar -> pending, beim naechsten Lauf der Action nachgerechnet
    acts.push(run("a6", D[4], { start_date_local: `${D[4]}T18:00:00` }));
    streamsFail = true;
    const s4 = await syncExport({ env, d1, from: D[0], to: D[4], today, log: quiet });
    assert.equal(s4.pending, 1);
    assert.equal((await buildRunExport({ EXPORT_DB }, { from: D[0], to: D[4] })).pending, 1);
    streamsFail = false;
    const s5 = await syncExport({ env, d1, from: D[0], to: D[4], today, log: quiet });
    assert.deepEqual([s5.computed, s5.pending, s5.runsWritten], [1, 0, 1]);

    // methodVersion: aeltere Zeilen werden neu gerechnet
    db.prepare(`UPDATE runs SET "methodVersion" = 0, "hash" = 'x' WHERE "id" = 'a1'`).run();
    const s6 = await syncExport({ env, d1, from: D[0], to: D[4], today, log: quiet });
    assert.deepEqual([s6.computed, s6.runsWritten], [1, 1]);

    // ohne D1-Binding: klare Fehlermeldung
    await assert.rejects(buildRunExport({}, {}), /EXPORT_DB/);
  } finally {
    globalThis.fetch = realFetch;
  }
}

console.log("export ok");
