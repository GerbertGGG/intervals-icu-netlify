import assert from "node:assert/strict";
import { buildDashboard } from "../src/dashboard.js";
import { buildWidgetBlock } from "../src/widget.js";
import { easyRun } from "../src/block-inputs.js";

// Ende-zu-Ende: Block A aus dem Kalender, Decoupling aus den Zeitreihen (Intervals liefert keins), Schwimmtest aus dem Text.
const today = "2026-11-20";
const day = (n) => new Date(Date.parse(today + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const desc = "Block A – Abschlussziele\n\n1. Lauf: aerobe Stabilität\nDecoupling ≤5 % im Mittel der letzten 3 vergleichbaren Läufe (≥60 min, Z2, flach, kein Hitzetag). Die Pace ist dabei nicht langsamer als bei den ersten Vergleichsläufen des Blocks.\n\n2. Rad (Rolle): aerobe Stabilität\nDecoupling (Leistung zu Puls) ≤5 % im Mittel der letzten 3 vergleichbaren Einheiten: 75 min Z2 bei fester Leistung, mit Ventilator und gleicher Raumtemperatur. Die Z2-Leistung leitest du aus einem FTP-Test in Woche 1 ab.\n\n3. Schwimmen: Dauer mit sauberer Technik\n2× 500 m mit maximal 1 min Pause. Die zweiten 500 m sind höchstens 5 s pro 100 m langsamer als die ersten.\n\nLeitplanken\n\nKraft: ≥60 Sätze pro Woche in 8 von 9 Belastungswochen\nRamp Rate in Belastungswochen ≤5, keine ungeplante Pause über 7 Tage\n\nWoche 12: Sind alle Ziele und Leitplanken erfüllt, geht es mit Block B weiter. Fehlt ein Ziel, verlängerst du die betroffene Sportart um 3–4 Wochen.\n\nWenn du lieber bei 8 % bleiben willst, ist das als Muss-Kriterium in Ordnung. Dann nimm 5 % als Zielwert, den du anpeilst.";
const events = [
  { category: "SEASON_START", start_date_local: "2026-10-19T00:00:00", name: "Block Grundlage (12 Wochen)", description: desc },
  { category: "SEASON_START", start_date_local: "2027-01-11T00:00:00", name: "Block B", description: "" },
];
const run = (id, n, o) => ({ id, start_date_local: day(-n) + "T10:00:00", type: "Run", name: "Long Slow Run", distance: 11000, moving_time: 4500, total_elevation_gain: 40, average_heartrate: 130, has_heartrate: true, icu_training_load: 60, decoupling: 50, ...o }); // Intervals-Wert beim Laufen wird ignoriert
const activities = [
  run("r1", 26), run("r2", 19), run("r3", 12), run("r4", 5), run("r5", 4, { name: "Grundlagenlauf", total_elevation_gain: 300 }),
  { id: "b1", start_date_local: day(-30) + "T12:00:00", type: "VirtualRide", name: "FTP / Vo2Max Test", moving_time: 1500, trainer: true, icu_average_watts: 200 },
  { id: "b2", start_date_local: day(-8) + "T12:00:00", type: "VirtualRide", name: "LIT", moving_time: 4560, icu_warmup_time: 600, trainer: true, icu_average_watts: 160, icu_variability_index: 1.01, icu_intensity: 63, average_heartrate: 125, has_heartrate: true, average_temp: 23 },
  { id: "b3", start_date_local: day(-1) + "T12:00:00", type: "VirtualRide", name: "LIT", moving_time: 4560, icu_warmup_time: 600, trainer: true, icu_average_watts: 160, icu_variability_index: 1.01, icu_intensity: 62, average_temp: 24 },
  { id: "s1", start_date_local: day(-3) + "T07:00:00", type: "Swim", name: "Schwimmen", distance: 1500, moving_time: 2400, description: "2x500: 9:40 / 9:55, Pause 50 s" },
];
let streamCalls = 0;
// Rolle (ohne Vergleichsleistung im Text gilt der Fallback 165 W): 10 min Warmup-Block, dann 60 min 165 W, Puls ab Minute 40 höher
const rideStream = (hr) => { const time = Array.from({ length: 4560 }, (_, i) => i); return [{ type: "time", data: time }, { type: "watts", data: time.map((t) => (t < 600 ? 120 : 165)) }, ...(hr ? [{ type: "heartrate", data: time.map((t) => (t < 2400 ? 130 : 136.5)) }] : [])]; };
const stream = (hr2, field, v) => { const n = 1800; return [{ type: "heartrate", data: Array.from({ length: 2 * n }, (_, i) => (i < n ? 130 : hr2)) }, { type: field, data: Array(2 * n).fill(v) }]; };
globalThis.fetch = async (url) => {
  const u = String(url);
  let body = null;
  if (u.includes("/streams")) { streamCalls++; body = u.includes("/activity/b2/") ? rideStream(true) : u.includes("/activity/b3/") ? rideStream(false) : stream(134, "velocity_smooth", 2.45); }
  else if (u.includes("/sport-settings")) body = [{ types: ["Ride", "VirtualRide"], ftp: 240 }];
  else if (u.includes("/wellness")) body = Array.from({ length: 70 }, (_, i) => ({ id: day(-69 + i), ctl: 20 + i * 0.3, atl: 22 }));
  else if (u.includes("/activities")) body = activities;
  else if (u.includes("/events")) body = events;
  return new Response(JSON.stringify(body), { status: body ? 200 : 404 });
};
const kv = new Map();
const env = { ATHLETE_ID: "i1", INTERVALS_API_KEY: "k", DASHBOARD_TOKEN: "x", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const d = await buildDashboard(env, today);
const sb = d.seasonBlock;
assert.equal(sb.weekNo, 5); assert.equal(sb.next.name, "Block B");
const g = Object.fromEntries(sb.goals.map((x) => [x.key, x]));
assert.equal(g.runDecoupling.count, 4); assert.equal(g.runDecoupling.value, 3); assert.equal(g.runDecoupling.status, "ok");
assert.match(g.runDecoupling.hints.join(), /nicht flach/);
assert.equal(g.bikeDecoupling.count, 1); assert.equal(g.bikeDecoupling.value, 5); // (165/130) / (165/136,5) − 1, selbst gerechnet
assert.match(g.bikeDecoupling.series[0].reason, /Hauptteil 66 min, 165 W, VI 1.*Einrollen 10 min.*selbst gerechnet/);
assert.ok(g.bikeDecoupling.hints.some((h) => /ohne Puls/.test(h))); assert.ok(g.bikeDecoupling.hints.some((h) => /FTP-Test am 21\.10\..*240 W/.test(h)));
assert.equal(g.swimTest.status, "ok"); assert.equal(g.swimTest.value, 3);
assert.equal("decision" in sb, false); // keine Woche-12-Entscheidung: die trifft der Coach
assert.equal(streamCalls, 7); // 4 flache Läufe + 2 Rad-Messfenster (auch ohne Puls: Leistung und VI) + Profil der letzten Einheit
// zweiter Aufruf: alles aus dem KV-Cache
streamCalls = 0;
await buildDashboard(env, today);
assert.equal(streamCalls, 0);
const wb = buildWidgetBlock(d);
assert.equal("decision" in wb.block, false); assert.equal(wb.block.goals.filter((x) => x.group === "ziel").length, 3);
assert.ok(wb.block.goals.filter((x) => x.group === "ziel").every((x) => x.course === null || !("state" in x.course))); // Verlauf ohne Stufe
console.log("block A dashboard ok");

// Lauf ohne Stichwort im Namen: locker, wenn der Puls >= 80 % in Zone 1–2 lag ("Bobingen Laufen" am 20.10.)
assert.equal(easyRun({ kind: "unknown" }, { name: "Bobingen Laufen", icu_hr_zone_times: [1575, 474, 0, 0, 0] }), true);
assert.equal(easyRun({ kind: "unknown" }, { name: "Bobingen Laufen", icu_hr_zone_times: [600, 600, 600, 300, 0] }), false);
assert.equal(easyRun({ kind: "unknown" }, { name: "Bobingen Laufen" }), false);
assert.equal(easyRun({ kind: "intensity" }, { name: "Intervalle", icu_hr_zone_times: [2000, 0, 0, 0, 0] }), false);
console.log("easy run by HR ok");
