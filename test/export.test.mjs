import assert from "node:assert/strict";
import { analyzeExportStreams, streamSkipOf, runRow, buildRunExport, buildDayRows, sickDaysFromEvents, toCsv, RUN_COLUMNS, DAY_COLUMNS, METHOD_VERSION } from "../src/export.js";
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

// ---------- Lauf-Export mit Budget, Cache und 404 (Intervals und KV gestubbt) ----------
{
  const kv = new Map();
  const env = { INTERVALS_API_KEY: "k", ATHLETE_ID: "i1", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => { kv.set(k, v); } } };
  const run = (id, day, extra = {}) => ({ id, type: "Run", start_date_local: `${day}T08:00:00`, moving_time: 3600, distance: 9000, has_heartrate: true, average_heartrate: 130, ...extra });
  const acts = [run("a1", "2026-10-01"), run("a2", "2026-10-02"), run("a3", "2026-10-03", { type: "VirtualRun" }), run("a4", "2026-10-04"), run("a5", "2026-10-05", { name: "5x1000" })];
  const calls = [];
  const realFetch = globalThis.fetch;
  globalThis.fetch = async (url) => {
    const u = String(url);
    calls.push(u);
    const ok = (body) => new Response(JSON.stringify(body), { status: 200 });
    if (u.includes("/activities?")) return ok(acts);
    if (u.includes("/map")) return ok({ latlngs: [[52.5, 13.4]], weather: null });
    if (u.includes("open-meteo")) return ok({ hourly: { time: ["2026-10-01T08:00"], temperature_2m: [9.5], relative_humidity_2m: [88], wind_speed_10m: [2.2] } });
    if (u.includes("/activity/a4/streams")) return new Response("not found", { status: 404 });
    if (u.includes("/streams")) return ok(synth(3600));
    throw new Error("unerwartet " + u);
  };
  try {
    const r1 = await buildRunExport(env, { from: "2026-10-01", to: "2026-10-05", limit: 2 }, { budget: { left: 4 } });
    assert.equal(r1.methodVersion, METHOD_VERSION);
    assert.equal(r1.total, 5);
    assert.equal(r1.count, 2);
    assert.equal(r1.rows[0].streamStatus, "ok");
    assert.equal(r1.rows[0].paceBand125_135, 400);
    assert.deepEqual([r1.rows[0].tempC, r1.rows[0].humidity, r1.rows[0].windMs, r1.rows[0].weatherSrc], [9.5, 88, 2.2, "openmeteo"]);
    assert.equal(r1.rows[1].streamStatus, "pending"); // Budget 4 reicht fuer einen Lauf (3 Abrufe)
    assert.equal(r1.pending, 3); // a2, a4, a5 (a3 ist Laufband)
    assert.equal(r1.nextCursor, "2026-10-02T08:00:00|a2");

    const r2 = await buildRunExport(env, { from: "2026-10-01", to: "2026-10-05", cursor: r1.nextCursor, limit: 10 });
    assert.deepEqual(r2.rows.map((r) => [r.id, r.streamStatus, r.streamSkip]), [["a3", "skip", "treadmill"], ["a4", "none", null], ["a5", "skip", "interval"]]);
    assert.equal(r2.nextCursor, null);
    assert.equal(r2.pending, 1); // a2 nur auf der ersten Seite

    const before = calls.length;
    const r3 = await buildRunExport(env, { from: "2026-10-01", to: "2026-10-05", limit: 1 });
    assert.equal(r3.rows[0].paceBand125_135, 400); // aus dem Cache
    assert.equal(calls.length - before, 1); // nur die Aktivitaetsliste
    assert.ok([...kv.keys()].includes(`icu:export:v${METHOD_VERSION}:a1`));
  } finally {
    globalThis.fetch = realFetch;
  }
}

console.log("export ok");
