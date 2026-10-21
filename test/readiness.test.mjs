// Erholungswerte (nur Fakten, kein Urteil) und Rad-Last gegen echte Werte aus Oktober 2026 (Ruhepuls, HRV, Schlaf-Score,
// Gefühlswerte; Krankheit am 14.10. im Kalender). Ausführen: node test/readiness.test.mjs
import assert from "node:assert/strict";
import { computeRecovery, computeDrift } from "../src/dashboard-summary.js";
import { buildBaseTrend } from "../src/base-trend.js";
import { sessionLoad, activityLoad } from "../src/activity-utils.js";

// [Datum, Ruhepuls, HRV, Schlaf-Score, Schlafqualität, Muskelkater, Ermüdung, Stimmung, Motivation]
const ROWS = [
  ["2026-09-23", 53], ["2026-09-26", 58], ["2026-09-29", 49],
  ["2026-09-30", 45, 46, 96, 1, 2, 2, 3, 3], ["2026-10-01", 46, 46, 86, 2, 2, 3, 2, 2], ["2026-10-02", 47, 46, 80, 2, 2, 2, 2, 2],
  ["2026-10-03", 46, 43, 79, 3, 2, 2, 2, 2], ["2026-10-04", 47, 44, 83, 2, 3, 3, 3, 3], ["2026-10-05", 46, 47, 81, 2, 3, 3, 2, 2],
  ["2026-10-06", 45, 47, 81, 2, 3, 2, 2, 3], ["2026-10-07", 44, 51, 87, 2, 2, 2, 2, 2], ["2026-10-08", 44, 53, 89, 2, 1, 2, 2, 1],
  ["2026-10-09", 44, 52, 92, 1, 2, 2, 2, 2], ["2026-10-10", 47, 44, 81, 2, 2, 3, 2, 2], ["2026-10-11", 52, 22, 36, 4, 1, 3, 2, 3],
  ["2026-10-12", 46, 44, 86, 2, 1, 2, 2, 2], ["2026-10-13", 50, 38, 64, 3, 2, 2, 2, 2], ["2026-10-14", 51, 33, 54, 4, 1, 2, 3, 3],
  ["2026-10-15", 46, 42, 83, 2, 1, 2, 2, 2], ["2026-10-16", 50, 37, 81, 2, 1, 2, 2, 2], ["2026-10-17", 52, 38, 70, 3, 1, 2, 2, 2],
  ["2026-10-18", 50, 38, 69, 3, 1, 2, 2, 2], ["2026-10-19", 48, 44, 81, 2, 2, 2, 2, 2], ["2026-10-20", 47, 46, 86, 2, 1, 2, 2, 2],
];
const W = ROWS.map(([date, restingHR, hrv, sleepScore, sleepQuality, soreness, fatigue, mood, motivation]) =>
  ({ date, restingHR, hrv: hrv ?? null, sleepScore: sleepScore ?? null, sleepQuality: sleepQuality ?? null, soreness: soreness ?? null, fatigue: fatigue ?? null, mood: mood ?? null, motivation: motivation ?? null }));
const SICK = ["2026-10-14"];
const upTo = (d) => W.filter((w) => w.date <= d);
const at = (d, list = upTo(d)) => computeRecovery(list, d, { sickDays: SICK });

// 20.10.: Werte von heute, Basis = Median der 28 Tage davor ohne Krankheitstage (14.–17.10.) und ohne Ausreißer
// (Krankheitsnacht 11.10., HRV 22); 7-Tage-Schnitt gegen die Wochen davor. Kein Urteil, keine Klasse.
{
  const r = at("2026-10-20");
  assert.equal(r.hrv, 46); assert.equal(r.restingHR, 47); assert.equal(r.sleepScore, 86);
  for (const k of ["verdict", "cls"]) assert.equal(r[k], undefined, k);
  for (const b of [r.body.hrv, r.body.resting]) assert.equal(b.cls, undefined);
  const vals = W.filter((w) => w.date < "2026-10-20" && w.date >= "2026-09-22" && w.hrv != null && !["2026-10-14", "2026-10-15", "2026-10-16", "2026-10-17"].includes(w.date) && w.hrv !== 22).map((w) => w.hrv).sort((a, b) => a - b);
  const m = vals.length >> 1;
  assert.equal(r.body.hrvMedian, vals.length % 2 ? vals[m] : (vals[m - 1] + vals[m]) / 2);
  assert.ok(r.body.hrv.mean7 < r.body.hrv.base7);
  assert.equal(r.sick, false);
  assert.equal(r.entryDate, "2026-10-20");
  const fat = r.items.find((i) => i.key === "fatigue");
  assert.equal(fat.v, 2); assert.equal(fat.cls, "ok"); assert.ok(fat.band && fat.med != null);
  // Farbe nur gegen den eigenen üblichen Bereich: Muskelkater 4 bei sonst 1–2 ist deutlich schlechter
  const sore = computeRecovery(upTo("2026-10-20").map((w) => (w.date === "2026-10-20" ? { ...w, soreness: 4 } : w)), "2026-10-20", { sickDays: SICK }).items.find((i) => i.key === "soreness");
  assert.equal(sore.cls, "bad");
}
// Krankheit im Kalender heute oder gestern steht als Fakt drin; ohne heutigen Gefühlseintrag gilt der von gestern
{
  assert.equal(at("2026-10-15").sick, true);
  const list = [...W, { date: "2026-10-21", restingHR: 49, hrv: 43 }];
  const r = at("2026-10-21", list);
  assert.equal(r.entryDate, "2026-10-20"); assert.equal(r.items.find((i) => i.key === "soreness").v, 1);
}
// Langzeitbasis: 8 Wochen HRV 50 / Ruhepuls 45, dann 4 Wochen 45 / 47 -> 7-Tage-Schnitt gegen die Tage 29–90 als Werte
{
  const days = Array.from({ length: 90 }, (_, i) => new Date(Date.parse("2026-07-23T00:00:00Z") + i * 86400000).toISOString().slice(0, 10));
  const long = days.map((date, i) => ({ date, hrv: i < 62 ? 50 : 45, restingHR: i < 62 ? 45 : 47 }));
  const d = computeDrift(long, days[89]);
  assert.deepEqual([d.hrv7, d.hrvAnchor, d.rhr7, d.rhrAnchor], [45, 50, 47, 45]);
  assert.equal(d.cls, undefined);
}
// Greift die Grundlage? EF der lockeren flachen Läufe (Median erste 3 gegen letzte 3), Puls bei 165 W
{
  const r = (date, ef, extra = {}) => ({ date, easy: true, treadmill: false, elevPerKm: 4, durationMin: 40, analysis: { ef, efToMin: 35 }, ...extra });
  const runs = [r("2026-10-20", 1.05), r("2026-10-23", 1.06), r("2026-10-25", 1.04), r("2026-10-28", 1.08), r("2026-10-30", 1.09), r("2026-11-01", 1.1), r("2026-11-02", 1.3, { elevPerKm: 30 })];
  const ride = (date, mainHr, noWarmup = false) => ({ date, mainHr, mainWatts: 165, noWarmup, durationMin: 72, indoor: true, easy: true, vi: 1.0, intensity: 65 });
  const rides = [ride("2026-10-19", 101, true), ride("2026-10-21", 110), ride("2026-10-26", 109), ride("2026-11-02", 108), ride("2026-11-09", 106), ride("2026-11-16", 105), ride("2026-11-23", 104)];
  const t = buildBaseTrend({ runs, rides }, { watts: 165, minMin: 60 });
  assert.equal(t.runs.length, 6); // der hügelige Lauf zählt nicht
  assert.deepEqual(t.runTrend, { first: 1.05, last: 1.09, delta: 3.8, dir: "better" });
  assert.equal(t.rides.length, 7); assert.deepEqual(t.rideTrend, { first: 109, last: 105, delta: -4, dir: "better" });
  assert.equal(buildBaseTrend({ runs: runs.slice(0, 5), rides: [] }, null).runTrend, null); // erst ab 6 Läufen
}

// Rad-Last aus Leistung (19.10.: HRSS 26, Leistung 43), Lauf und Fahrten ohne Leistung wie Intervals
assert.equal(sessionLoad({ type: "VirtualRide", icu_training_load: 26, power_load: 43 }), 43);
assert.equal(activityLoad({ type: "VirtualRide", icu_training_load: 26, power_load: 43 }), 26);
assert.equal(sessionLoad({ type: "Ride", icu_training_load: 30, power_load: null }), 30);
assert.equal(sessionLoad({ type: "Run", icu_training_load: 30, power_load: 50, pace_load: 34 }), 30);

console.log("readiness ok");
