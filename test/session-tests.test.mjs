import assert from "node:assert/strict";
import { parseBlockTargets } from "../src/season-block.js";
import { TEST_RULES, testConfigOf, nameOverride, classifyRuns, classifyRide, rideWindowOf, rideWindowStats } from "../src/session-tests.js";

// Block-Eintrag wie im Kalender (Stand 23.10.2026), gekürzt auf die Teile, die die Test-Erkennung liest
const block = "Block A – Grundlage (12 Wochen, ab 19.10.2026)\n\nABLAUF\nBlocktests (langer Lauf): Basistest Fr 23.10., dann So 08.11., So 06.12., So 03.01. Termine fest, unabhängig von Entlastungswochen.\nTypische Woche: 2× Rad, 2× Lauf, 1× Schwimmen (meist Mittwoch), Kraft.\n\nABSCHLUSSZIELE\nLauf: aerobe Stabilität und Haltbarkeit\na) Basis: Decoupling der ersten 60 min im Blocktest, Mittel der letzten 3 Tests: Muss ≤8 %, Ziel ≤5 %.\nJeder Grundlagenlauf ab 60 min unter HF 128 zählt als Trendindikator, nicht direkt für das Abschlussziel.\n\nRad (Rolle): aerobe Stabilität\nDecoupling (Leistung zu Puls) im 60-min-Messfenster ≤5 % im Mittel der letzten 3 Vergleichseinheiten.\nReferenz: FTP 252 W (Rampentest 12.10.). Erster Wert 19.10. (ohne Einrollen, nach Krankheit): 9,8 %, nur Orientierung.\n\nHF-DECKEL\nLauf-Grundlage unter HF 128.\n\nRAD-VERGLEICHSEINHEIT (Protokoll)\n10 min Einrollen (ca. 5 min 110–130 W, 5 min 140–160 W) als „Warmup“, danach mindestens 60 min bei 165 W als „Main Set“ = Messfenster. Danach frei.";
const tg = parseBlockTargets(block);
assert.equal(tg.bike.watts, 165); assert.equal(tg.bike.warmupMin, 10); assert.equal(tg.hrCap, 128);
const cfg = testConfigOf(block, "2026-10-19", tg);
assert.deepEqual(cfg.testDates, ["2026-10-23", "2026-11-08", "2026-12-06", "2027-01-03"]);
assert.deepEqual([cfg.testSource, cfg.hrCap, cfg.bikeWatts, cfg.warnings], ["text", 128, 165, []]);
// Ohne Termine, Deckel und Leistung: Fallback mit Warnung
{
  const warn = console.warn; const seen = []; console.warn = (...a) => seen.push(a.join(" "));
  const fb = testConfigOf("Block B", "2026-10-19", parseBlockTargets("Block B"));
  console.warn = warn;
  assert.deepEqual([fb.testSource, fb.testDates.length, fb.hrCap, fb.bikeWatts, fb.warnings.length, seen.length], ["fallback", 4, 128, 165, 3, 3]);
}
console.log("test config ok");

assert.equal(nameOverride("Vergleichstest"), "bikeCompare"); assert.equal(nameOverride("Basistest"), "runBlocktest");
assert.equal(nameOverride("Blocktest – kein Test"), "exclude"); assert.equal(nameOverride("Ausgangswert"), "bikeCompare"); assert.equal(nameOverride("LIT"), null);

// ---------- Laufen ----------
const run = (id, date, min, hr, o = {}) => ({ id, type: "Run", start_date_local: `${date}T09:00:00`, moving_time: min * 60, average_heartrate: hr, name: "Bobingen Laufen", ...o });
const runs = [
  run("a", "2026-10-20", 34, 125), // kurz: nichts
  run("b", "2026-10-24", 95, 124), run("c", "2026-10-25", 70, 122), // Termin 23.10. verschoben: der längere zählt, der andere ist Trend
  run("d", "2026-10-29", 72, 130), // über dem Deckel: nichts
  run("e", "2026-11-01", 65, 125.6),
  run("f", "2026-11-08", 50, 120), // am Termin, aber unter 60 min
  run("g", "2026-12-04", 90, 121, { name: "Lauf – kein Test" }), run("h", "2026-12-06", 80, 123), // ausgeschlossen: der nächste zählt
  run("i", "2026-12-10", 45, 140, { name: "Basistest" }), // Override
  run("j", "2026-12-12", 75, 120, { type: "VirtualRun" }), // keine Sportart Run
  { id: "k", type: "Ride", start_date_local: "2026-11-08T09:00:00", moving_time: 7200, average_heartrate: 110, name: "Blocktest" }, // Rad nie Blocktest
];
const rc = classifyRuns(runs, cfg);
const cat = (id) => rc.get(id)?.category ?? null;
assert.deepEqual(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"].map(cat), [null, "runBlocktest", "runTrend", null, "runTrend", null, null, "runBlocktest", "runBlocktest", null, null]);
assert.equal(rc.get("b").testDate, "2026-10-23");
assert.equal(rc.get("b").testReason, "Run am Blocktest-Termin 23.10. (±2 Tage, 24.10. statt 23.10.), 95 min, längster von 2");
assert.equal(rc.get("e").testReason, "Run ≥60 min (65 min), Ø-HF 126 < 128");
assert.match(rc.get("g").testReason, /kein Test/);
assert.equal(rc.get("i").testReason, "Run, Name „Basistest“ (Override), 45 min");
console.log("test runs ok");

// ---------- Rad ----------
const secs = (n) => Array.from({ length: n }, (_, i) => i);
const streams = (watts, hr) => { const time = secs(watts.length); return [{ type: "time", data: time }, { type: "watts", data: watts }, { type: "heartrate", data: hr }]; };
const ride = (o) => ({ id: "r", type: "VirtualRide", name: "LIT", moving_time: 4200, icu_warmup_time: 600, icu_cooldown_time: 0, decoupling: 3.1, ...o });
const statsOf = (a, st) => { const w = rideWindowOf(a); return rideWindowStats(st, w.warmSec, TEST_RULES.bikeWindowMin * 60); };
// Saubere Vergleichseinheit: 10 min Einrollen, 60 min 166 W, Gesamtdauer = Einrollen + 60: Intervals-Decoupling
{
  const t = secs(4200), st = streams(t.map((x) => (x < 600 ? 125 : 166)), t.map((x) => (x < 2400 ? 120 : 124)));
  const a = ride({});
  const c = classifyRide(a, statsOf(a, st), cfg);
  assert.deepEqual([c.category, c.baseline, c.decSource, c.decoupling, c.mainWatts, c.vi, c.mainMin], ["bikeCompare", false, "intervals", 3.1, 166, 1, 60]);
  // "Ausgangswert" im Namen: Rad-Vergleich als baseline, auch mit Einrollen und anderer Leistung
  const cb = classifyRide({ ...a, name: "Ausgangswert Rolle" }, statsOf(a, streams(t.map(() => 150), t.map(() => 120))), cfg);
  assert.deepEqual([cb.category, cb.baseline], ["bikeCompare", true]); assert.match(cb.testReason, /Ausgangswert \(baseline, zählt nicht fürs Ziel\)$/);
  assert.equal(classifyRide({ ...a, name: "Ausgangswert kein Test" }, null, cfg).category, null);
  assert.equal(c.testReason, "Ride: Hauptteil 60 min, 166 W, VI 1, Einrollen 10 min, Decoupling von Intervals");
  // 90 min Fahrt (danach frei): Intervals-Wert umfasst mehr als das Messfenster, selbst gerechnet: 124/120 − 1 = 3,3 %
  const t2 = secs(5400), st2 = streams(t2.map((x) => (x < 600 ? 125 : x < 4200 ? 166 : 120)), t2.map((x) => (x < 2400 ? 120 : 124)));
  const a2 = ride({ moving_time: 5400 });
  const c2 = classifyRide(a2, statsOf(a2, st2), cfg);
  assert.deepEqual([c2.category, c2.decSource, c2.decoupling, c2.mainMin], ["bikeCompare", "streams", 3.3, 80]);
  assert.match(c2.testReason, /Decoupling selbst gerechnet/);
}
// icu_warmup_time 0: die ersten 10 min gelten als Einrollen (vermerkt), Decoupling selbst gerechnet
{
  const t = secs(4320), st = streams(t.map((x) => (x < 600 ? 130 : 165)), t.map(() => 118));
  const a = ride({ moving_time: 4320, icu_warmup_time: 0, decoupling: 7 });
  const c = classifyRide(a, statsOf(a, st), cfg);
  assert.deepEqual([c.category, c.warmupAssumed, c.decSource, c.decoupling], ["bikeCompare", true, "streams", 0]);
  assert.equal(c.testReason, "Ride: Hauptteil 62 min, 165 W, VI 1, Einrollen 10 min angenommen (icu_warmup_time 0), Decoupling selbst gerechnet");
}
// HIIT fällt heraus: 30/30 (300/100 W) und 4× 4 min über 165 W
{
  const t = secs(4800);
  const a = ride({ moving_time: 4800, name: "30/30" });
  const c3030 = classifyRide(a, statsOf(a, streams(t.map((x) => (x < 600 ? 120 : Math.floor(x / 30) % 2 ? 100 : 230)), t.map(() => 130))), cfg);
  assert.equal(c3030.category, null); assert.match(c3030.testReason, /VI 1,07 > 1,05/); // 30/30 mit genau 165 W im Mittel: nur über den VI
  const c4x4 = classifyRide(a, statsOf(a, streams(t.map((x) => (x < 600 ? 120 : (x - 600) % 480 < 240 && x < 600 + 4 * 480 ? 280 : 140)), t.map(() => 130))), cfg);
  assert.equal(c4x4.category, null); assert.match(c4x4.testReason, /nicht 165 ±5 W|VI/);
  // Gleichmäßig, aber 150 W: keine Vergleichseinheit
  const c150 = classifyRide(a, statsOf(a, streams(t.map(() => 150), t.map(() => 120))), cfg);
  assert.equal(c150.category, null); assert.match(c150.testReason, /150 W nicht 165 ±5 W/);
}
// 22.10.2026: 45 min ohne Einrollen – Hauptteil zu kurz, keine Zeitreihen nötig
{
  const c = classifyRide({ id: "i200482357", type: "VirtualRide", name: "LIT", moving_time: 2700, icu_warmup_time: 0, decoupling: 8.33 }, null, cfg);
  assert.equal(c.category, null); assert.equal(c.testReason, "Ride: Hauptteil 35 min < 60 min (Einrollen 10 min angenommen (icu_warmup_time 0))");
}
// 19.10.2026 "Vergleichstest" (gekoppeltes Workout "Blocktest"): Override, 60 min ohne Einrollen, Intervals-Wert passt aufs Fenster
{
  const a = { id: "i198960592", type: "VirtualRide", name: "Vergleichstest", moving_time: 3600, icu_warmup_time: 0, icu_cooldown_time: 0, decoupling: 9.803871 };
  const t = secs(3600), st = streams(t.map(() => 165), t.map((x) => (x < 1800 ? 96 : 106)));
  const c = classifyRide(a, statsOf(a, st), cfg);
  assert.deepEqual([c.category, c.baseline, c.noWarmup, c.decSource, c.decoupling, c.mainMin], ["bikeCompare", true, true, "intervals", 9.8, 60]);
  assert.equal(c.testReason, "Ride, Name „Vergleichstest“ (Override): Hauptteil 60 min, 165 W, VI 1, ohne Einrollen, Decoupling von Intervals, Ausgangswert (baseline, zählt nicht fürs Ziel)");
  // "kein Test" schließt aus, auch die Sportart Ride ohne Leistung im Namen
  assert.equal(classifyRide({ ...a, name: "Vergleich kein Test" }, null, cfg).category, null);
  assert.equal(classifyRide({ type: "Run" }, null, cfg), null);
}
console.log("test rides ok");
