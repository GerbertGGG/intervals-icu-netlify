import assert from "node:assert/strict";
import { parseBlockTargets } from "../src/season-block.js";
import { TEST_RULES, testConfigOf, nameOverride, classifyRuns, classifyRide, rideWindowOf, rideWindowStats, classifyRideTrend, rideTrendWindowOf, rideTrendStats, rideTrendView, rideTrendPoints } from "../src/session-tests.js";

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
  run("b", "2026-10-24", 95, 124), run("c", "2026-10-25", 70, 122), // Termin 23.10. verschoben: der längere zählt, der andere ab 65 min automatisch
  run("d", "2026-10-29", 72, 130), // ab 65 min: zählt automatisch als Test, auch über dem Deckel
  run("e", "2026-11-01", 62, 125.6), // 60–64 min unter dem Deckel: Trend
  run("f", "2026-11-08", 50, 120), // am Termin, aber unter 60 min
  run("g", "2026-12-04", 90, 121, { name: "Lauf – kein Test" }), run("h", "2026-12-06", 80, 123), // ausgeschlossen: der nächste zählt
  run("i", "2026-12-10", 45, 140, { name: "Basistest" }), // Override
  run("j", "2026-12-12", 75, 120, { type: "VirtualRun" }), // keine Sportart Run
  { id: "k", type: "Ride", start_date_local: "2026-11-08T09:00:00", moving_time: 7200, average_heartrate: 110, name: "Blocktest" }, // Rad zählt nicht als Lauf-Blocktest (classifyRuns), siehe Rad-Test unten
];
const rc = classifyRuns(runs, cfg);
const cat = (id) => rc.get(id)?.category ?? null;
assert.deepEqual(["a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k"].map(cat), [null, "runBlocktest", "runBlocktest", "runBlocktest", "runTrend", null, null, "runBlocktest", "runBlocktest", null, null]);
assert.equal(rc.get("b").testDate, "2026-10-23");
assert.equal(rc.get("b").testReason, "Run am Blocktest-Termin 23.10. (±2 Tage, 24.10. statt 23.10.), 95 min, längster von 2");
assert.equal(rc.get("e").testReason, "Run ≥60 min (62 min), Ø-HF 126 < 128");
assert.equal(rc.get("d").testReason, "Run ab 65 min (72 min): zählt automatisch als Test"); assert.equal(rc.get("d").testDate, "2026-10-29");
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
  assert.equal(c.testReason, "Ride: Hauptteil 60 min, 166 W, VI 1, Einrollen 10 min (automatisch, erste 10 min), Decoupling von Intervals");
  // 90 min Fahrt (danach frei): Intervals-Wert umfasst mehr als das Messfenster, selbst gerechnet: 124/120 − 1 = 3,3 %
  const t2 = secs(5400), st2 = streams(t2.map((x) => (x < 600 ? 125 : x < 4200 ? 166 : 120)), t2.map((x) => (x < 2400 ? 120 : 124)));
  const a2 = ride({ moving_time: 5400 });
  const c2 = classifyRide(a2, statsOf(a2, st2), cfg);
  assert.deepEqual([c2.category, c2.decSource, c2.decoupling, c2.mainMin], ["bikeCompare", "streams", 3.3, 80]);
  assert.match(c2.testReason, /Decoupling selbst gerechnet/);
}
// Ohne Warmup-Block in Intervals: die ersten 10 min zählen trotzdem automatisch als Einrollen, sobald die Fahrt lang genug ist
{
  const t = secs(4320);
  const a = ride({ moving_time: 4320, icu_warmup_time: 0, decoupling: 7 });
  // 10 min locker, dann 165 W: Messfenster 10–70 min mit 165 W = Vergleichseinheit, zählt fürs Ziel
  const c = classifyRide(a, statsOf(a, streams(t.map((x) => (x < 600 ? 130 : 165)), t.map(() => 118))), cfg);
  assert.deepEqual([c.category, c.baseline, c.noWarmup, c.warmupSec, c.mainWatts, c.decSource], ["bikeCompare", false, false, 600, 165, "streams"]);
  assert.match(c.testReason, /Einrollen 10 min \(automatisch, erste 10 min\)/);
  // durchgehend 165 W, auch mit zu kurzem Warmup-Block in Intervals (240 s): ebenfalls Test mit 10 min Einrollen
  const c2 = classifyRide({ ...a, icu_warmup_time: 240 }, statsOf(a, streams(t.map(() => 165), t.map(() => 118))), cfg);
  assert.deepEqual([c2.category, c2.baseline, c2.noWarmup, c2.decSource, c2.decoupling], ["bikeCompare", false, false, "streams", 0]);
  // Warmup-Block länger als 10 min: trotzdem nur die ersten 10 min als Einrollen
  assert.equal(rideWindowOf(ride({ moving_time: 4800, icu_warmup_time: 900 })).warmSec, 600);
  // Zu kurz fürs Fenster (65 min): wie bisher ohne Einrollen, höchstens Ausgangswert
  const t3 = secs(3900), a3 = ride({ moving_time: 3900, icu_warmup_time: 0 });
  const c3 = classifyRide(a3, statsOf(a3, streams(t3.map(() => 165), t3.map(() => 118))), cfg);
  assert.deepEqual([c3.category, c3.baseline, c3.noWarmup], ["bikeCompare", true, true]);
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
// 22.10.2026: 45 min ohne Einrollen – zu kurz, keine Zeitreihen nötig
{
  const c = classifyRide({ id: "i200482357", type: "VirtualRide", name: "LIT", moving_time: 2700, icu_warmup_time: 0, decoupling: 8.33 }, null, cfg);
  assert.equal(c.category, null); assert.equal(c.testReason, "Ride: Hauptteil 45 min < 60 min (ohne Einrollen (icu_warmup_time 0 s))");
}
// 19.10.2026 "Vergleichstest" (gekoppeltes Workout "Blocktest"): Override, 60 min ohne Einrollen, Intervals-Wert passt aufs Fenster
{
  const a = { id: "i198960592", type: "VirtualRide", name: "Vergleichstest", moving_time: 3600, icu_warmup_time: 0, icu_cooldown_time: 0, decoupling: 9.803871 };
  const t = secs(3600), st = streams(t.map(() => 165), t.map((x) => (x < 1800 ? 96 : 106)));
  const c = classifyRide(a, statsOf(a, st), cfg);
  assert.deepEqual([c.category, c.baseline, c.noWarmup, c.decSource, c.decoupling, c.mainMin], ["bikeCompare", true, true, "intervals", 9.8, 60]);
  assert.equal(c.testReason, "Ride, Name „Vergleichstest“ (Override): Hauptteil 60 min, 165 W, VI 1, ohne Einrollen (icu_warmup_time 0 s), Decoupling von Intervals, Ausgangswert (baseline, zählt nicht fürs Ziel)");
  // "kein Test" schließt aus, auch die Sportart Ride ohne Leistung im Namen
  assert.equal(classifyRide({ ...a, name: "Vergleich kein Test" }, null, cfg).category, null);
  assert.equal(classifyRide({ type: "Run" }, null, cfg), null);
}
console.log("test rides ok");

// 26.10.2026 "Blocktest" auf der Rolle: kein icu_warmup_time, Einrollen steht als erstes Intervall in interval_summary
{
  const a = { id: "i201785523", type: "VirtualRide", name: "Blocktest", moving_time: 4205, icu_warmup_time: 0, icu_cooldown_time: 0, interval_summary: ["1x 10m1s 132w", "1x 1h 164w", "1x 5s 97w"] };
  const w = rideWindowOf(a);
  assert.equal(w.forced, true); assert.equal(w.noWarmup, false); assert.equal(w.warmSec, 601);
  const t = Array.from({ length: 4205 }, (_, i) => i);
  const st = rideWindowStats([{ type: "time", data: t }, { type: "watts", data: t.map((x) => (x < 601 ? 132 : 164)) }, { type: "heartrate", data: t.map(() => 115) }], w.warmSec, TEST_RULES.bikeWindowMin * 60);
  const c = classifyRide(a, st, cfg);
  assert.equal(c.category, "bikeCompare"); assert.equal(c.baseline, false);
}
console.log("ok blocktest rolle");

// ---------- Rad-Trendpunkte (Kategorie trend) ----------
// Echte Fahrten Okt./Nov. 2026 (Kopfdaten aus Intervals), Zeitreihen nachgebaut mit denselben Mittelwerten
{
  const real = [
    { id: "i195027137", start_date_local: "2026-10-08T12:22:13", type: "VirtualRide", name: "Indoor-Radfahren", trainer: true, moving_time: 1802, icu_warmup_time: 0, has_heartrate: false, w: 144, hr: null },
    { id: "i196432644", start_date_local: "2026-10-12T11:53:31", type: "VirtualRide", name: "FTP Test", trainer: true, moving_time: 1347, icu_warmup_time: 0, has_heartrate: true, w: 201, hr: 103, ftp: true },
    { id: "i198960592", start_date_local: "2026-10-19T11:31:41", type: "VirtualRide", name: "Vergleichstest", trainer: true, moving_time: 3600, icu_warmup_time: 0, has_heartrate: true, decoupling: 9.8, w: 165, hr: 101 },
    { id: "i200482357", start_date_local: "2026-10-22T19:09:52", type: "VirtualRide", name: "LIT", trainer: true, moving_time: 2700, icu_warmup_time: 0, has_heartrate: true, w: 165, hr: 106 },
    { id: "i201785523", start_date_local: "2026-10-26T07:56:19", type: "VirtualRide", name: "Blocktest", trainer: true, moving_time: 4205, icu_warmup_time: 0, has_heartrate: true, w: 160, hr: 111 },
    { id: "i203062884", start_date_local: "2026-10-29T09:17:18", type: "VirtualRide", name: "Indoor-Radfahren", trainer: true, moving_time: 3316, icu_warmup_time: 0, has_heartrate: true, w: 165, hr: 104 },
    { id: "i203276642", start_date_local: "2026-10-29T17:36:11", type: "VirtualRide", name: "LIT", trainer: true, moving_time: 3001, icu_warmup_time: 0, has_heartrate: true, w: 150, hr: 99 },
    { id: "i204897557", start_date_local: "2026-11-02T18:42:54", type: "VirtualRide", name: "Indoor-Radfahren", trainer: true, moving_time: 3601, icu_warmup_time: 0, has_heartrate: true, w: 165, hr: 101 },
  ];
  const rides = real.map((a) => {
    const t = secs(a.moving_time);
    const watts = a.ftp ? t.map((x) => (Math.floor(x / 60) % 2 ? 260 : 140)) : t.map(() => a.w);
    const st = a.hr == null ? [{ type: "time", data: t }, { type: "watts", data: watts }] : streams(watts, t.map(() => a.hr));
    const pre = rideTrendWindowOf(a);
    const tc = classifyRideTrend(a, pre.ok ? rideTrendStats(st, pre.windowStartS) : null, cfg);
    return { date: a.start_date_local.slice(0, 10), name: a.name, ...(tc.trend ? { trend: { date: a.start_date_local.slice(0, 10), ...tc } } : { why: tc.testReason }) };
  });
  const yes = rides.filter((r) => r.trend).map((r) => r.date);
  assert.deepEqual(yes, ["2026-10-19", "2026-10-22", "2026-10-29", "2026-11-02"]); // 29.10. nur die 165-W-Fahrt
  assert.ok(rides.every((r) => !r.trend || r.trend.ablauf === "ohne_einrollen" && r.trend.windowStartS === 600));
  const why = Object.fromEntries(rides.filter((r) => !r.trend).map((r) => [`${r.date} ${r.name}`, r.why]));
  assert.match(why["2026-10-08 Indoor-Radfahren"], /30 min < 45 min/);
  assert.match(why["2026-10-12 FTP Test"], /min < 45 min|Test im Namen/);
  assert.match(why["2026-10-26 Blocktest"], /Test im Namen/);
  assert.match(why["2026-10-29 LIT"], /150 W nicht 165 ±10 W/);
  // 19.10. ist zugleich Ausgangswert der Rad-Vergleichseinheit (zählt nicht fürs Ziel) und Trendpunkt
  const a19 = real[2];
  assert.equal(classifyRide(a19, statsOf(a19, streams(secs(3600).map(() => 165), secs(3600).map(() => 101))), cfg).baseline, true);
  const view = rideTrendView(rides);
  assert.deepEqual(view.groups.map((g) => [g.ablauf, g.points.length]), [["ohne_einrollen", 4]]);
  assert.equal(view.note, "Trend, kein Test. Bewertet wird nur am Blocktest.");
  assert.deepEqual(Object.keys(view.groups[0].points[0]), ["date", "ablauf", "durationMin", "windowStartS", "windowMin", "avgWatts", "avgHr", "vi", "ef", "decoupling", "testReason"]);
  assert.equal(view.groups[0].points[0].ef, 1.63); // 165 / 101
}
// Handrechnung: 29.10. nachgebaut, 3316 s, ohne Einrollen -> Fenster 600–3316 s (2716 Proben, Mitte bei 1958 s),
// 165 W, HF 100 in der ersten und 105 in der zweiten Hälfte: EF = 165 / 102,5 = 1,61; Decoupling = (165/100) / (165/105) − 1 = 5,0 %
{
  const a = { id: "x", type: "VirtualRide", name: "Indoor-Radfahren", trainer: true, moving_time: 3316, icu_warmup_time: 0, has_heartrate: true };
  const t = secs(3316), st = streams(t.map(() => 165), t.map((x) => (x < 1958 ? 100 : 105)));
  const s = rideTrendStats(st, 600);
  assert.deepEqual([s.windowMin, s.avgWatts, s.hr, s.ef, s.decoupling, s.vi], [45, 165, 103, 1.61, 5, 1]);
  const c = classifyRideTrend(a, s, cfg);
  assert.equal(c.trend, true); assert.equal(c.ef, 1.61); assert.equal(c.decoupling, 5); assert.equal(c.avgHr, 103);
  assert.match(c.testReason, /Trendpunkt: Rolle, 55 min, Fenster 45 min, 165 W, VI 1.*ohne Einrollen \(Fenster ab Minute 10\)/);
  // Proben ohne HF fallen aus Puls und Decoupling heraus
  const hrGap = t.map((x) => (x < 1958 ? (x < 900 ? 0 : 100) : 105));
  assert.equal(rideTrendStats(streams(t.map(() => 165), hrGap), 600).decoupling, 5);
}
// Mit Einrollen: Fenster ab Ende des Warmup-Blocks bis Ende; Ausschlüsse
{
  const base = { id: "w", type: "Ride", name: "Rolle locker", trainer: true, moving_time: 4200, icu_warmup_time: 420, has_heartrate: true };
  const pre = rideTrendWindowOf(base);
  assert.deepEqual([pre.ok, pre.ablauf, pre.windowStartS], [true, "mit_einrollen", 420]);
  assert.equal(rideTrendWindowOf({ ...base, icu_warmup_time: 299 }).ablauf, "ohne_einrollen");
  const t = secs(4200), even = streams(t.map((x) => (x < 420 ? 120 : 172)), t.map(() => 120));
  const c = classifyRideTrend(base, rideTrendStats(even, 420), cfg);
  assert.equal(c.trend, true); assert.equal(c.windowMin, 63); assert.equal(c.avgWatts, 172); assert.match(c.testReason, /mit Einrollen \(Fenster ab Minute 7\)/);
  assert.equal(classifyRideTrend({ ...base, name: "Rolle kein Test" }, null, cfg).testReason, "Trend: Name „kein Test“: ausgeschlossen");
  assert.match(classifyRideTrend({ ...base, trainer: false }, null, cfg).testReason, /nicht auf der Rolle/);
  assert.match(classifyRideTrend({ ...base, name: "Rampentest" }, null, cfg).testReason, /Test im Namen/);
  assert.match(classifyRideTrend({ ...base, has_heartrate: false }, null, cfg).testReason, /keine Pulsdaten/);
  assert.match(classifyRideTrend(base, null, cfg).testReason, /Zeitreihen noch nicht geladen/);
  // HIIT bei passendem Mittel: VI zu hoch
  const hiit = streams(t.map((x) => (x < 420 ? 120 : Math.floor(x / 30) % 2 ? 90 : 250)), t.map(() => 130));
  assert.match(classifyRideTrend(base, rideTrendStats(hiit, 420), cfg).testReason, /VI .* > 1,05/);
  assert.equal(classifyRideTrend({ type: "Run" }, null, cfg), null);
  // Gruppen getrennt, je Gruppe älteste zuerst
  const mk = (date, ablauf) => ({ trend: { date, ablauf, ef: 1.6, decoupling: 3 } });
  const v = rideTrendView([mk("2026-11-05", "mit_einrollen"), mk("2026-11-01", "ohne_einrollen"), mk("2026-11-03", "mit_einrollen"), {}]);
  assert.deepEqual(v.groups.map((g) => [g.label, g.points.map((p) => p.date)]), [["ohne Einrollen", ["2026-11-01"]], ["mit Einrollen", ["2026-11-03", "2026-11-05"]]]);
  assert.equal(rideTrendView([{}]), null);
  assert.equal(rideTrendPoints(Array.from({ length: 12 }, (_, i) => mk(`2026-11-${String(i + 10)}`, "ohne_einrollen"))).length, 8);
}
console.log("ok rad trendpunkte");
