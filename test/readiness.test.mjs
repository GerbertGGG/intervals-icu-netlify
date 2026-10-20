// Erholungsampel und Rad-Last gegen echte Werte aus Oktober 2026 (Ruhepuls, HRV, Schlaf-Score, Gefühlswerte; Krankheit
// am 14.10. im Kalender). Ausführen: node test/readiness.test.mjs
import assert from "node:assert/strict";
import { computeReadiness, computeDrift, computeEasyRunSignal } from "../src/dashboard-summary.js";
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
const at = (d, list = upTo(d)) => computeReadiness(list, d, null, { sickDays: SICK });

// 13.10., Vortag der Krankheit: HRV −17 % und Ruhepuls +4 bestätigen sich gegenseitig → zwei Signale → rot (Frühwarnung bleibt)
{
  const r = at("2026-10-13");
  assert.equal(r.body.hrv.cls, "warn");
  assert.equal(r.body.resting.cls, "warn");
  assert.equal(r.verdict.cls, "bad");
}
// 20.10.: HRV und Ruhepuls wieder auf Basis, obwohl die 7-Tage-Schnitte nach der Krankheit noch daneben liegen → grün
{
  const r = at("2026-10-20");
  assert.ok(r.body.hrv.mean7 < r.body.hrv.base7 * 0.93);
  assert.equal(r.body.hrv.cls, "ok");
  assert.equal(r.verdict.cls, "ok");
}
// 19.10.: Ruhepuls noch 2 über den Vorwochen und 7-Tage-Schnitt erhöht (nach Krankheit) → Trend = gelb
{
  const r = at("2026-10-19");
  assert.equal(r.body.resting.high7, true);
  assert.equal(r.verdict.cls, "warn");
  assert.match(r.verdict.reasons.join(), /7-Tage-Schnitt/);
}
// Einzelner HRV-Ausschlag von −13 % bei sonst normalen Werten: angezeigt, aber kein Signal
{
  const d = "2026-10-09";
  const list = upTo(d).map((w) => (w.date === d ? { ...w, hrv: 40 } : w));
  const r = at(d, list);
  assert.equal(r.body.hrv.cls, "ok");
  assert.equal(r.body.hrv.single, true);
  assert.equal(r.verdict.cls, "ok");
  assert.match(r.verdict.watch, /HRV 40/);
  // −20 % zählt allein
  assert.equal(at(d, upTo(d).map((w) => (w.date === d ? { ...w, hrv: 36 } : w))).body.hrv.cls, "warn");
}
// Langsamer Abfall: jeder Tag nur knapp unter Basis, kein Einzelwert auffällig, der 7-Tage-Schnitt zeigt ihn → gelb
{
  const base = W.filter((w) => w.date <= "2026-10-10");
  const slide = [44, 43, 43, 42, 42, 41, 41].map((hrv, i) => ({ date: `2026-10-${String(11 + i).padStart(2, "0")}`, restingHR: 46, hrv, sleepScore: 85, sleepQuality: 2, soreness: 2, fatigue: 2, mood: 2, motivation: 2 }));
  const r = computeReadiness([...base, ...slide], "2026-10-17");
  assert.ok(41 / r.body.hrvMedian > 0.88); // Einzeltag wäre nach der alten Regel unauffällig
  assert.equal(r.body.hrv.cls, "warn");
  assert.equal(r.verdict.cls, "warn");
}
// Eine schlechte Nacht (Schlafqualität 4) bei normalen Körperwerten: höchstens gelb, nicht rot
{
  const d = "2026-10-09";
  const r = at(d, upTo(d).map((w) => (w.date === d ? { ...w, sleepQuality: 4 } : w)));
  assert.equal(r.items.find((i) => i.key === "sleepQuality").cls, "bad");
  assert.equal(r.verdict.cls, "warn");
}
// Schlaf zählt einmal: Schlaf-Score meldet schon (2 Tage < 80), Schlafqualität kommt nicht noch als eigenes Signal dazu
{
  const d = "2026-10-09";
  const list = upTo(d).map((w) => (w.date === d ? { ...w, sleepScore: 70, sleepQuality: 3 } : w.date === "2026-10-08" ? { ...w, sleepScore: 75 } : w));
  const r = at(d, list);
  assert.equal(r.body.sleep.cls, "warn");
  assert.equal(r.verdict.cls, "warn");
}

// ---------- Ampel als Steuerung: Umfang, Laufen je eigener Bremse, schwächere Stimmung/Motivation ----------
{
  const d = "2026-10-20", g = at(d);
  assert.equal(g.verdict.cls, "ok"); assert.equal(g.verdict.dose.factor, 1);
  assert.equal(g.verdict.sports.run.cls, "ok"); assert.equal(g.verdict.sports.bike.cls, "ok");
  // Stimmung deutlich schlechter allein: nur eine Warnung, kein Gelb (Job und Alltag, nicht Erholung)
  assert.equal(at(d, upTo(d).map((w) => (w.date === d ? { ...w, mood: 5 } : w))).verdict.cls, "ok");
  // Stimmung und Motivation etwas schlechter: nur Hinweis
  const soft = at(d, upTo(d).map((w) => (w.date === d ? { ...w, mood: 3, motivation: 3 } : w)));
  assert.equal(soft.verdict.cls, "ok"); assert.match(soft.verdict.watch, /Stimmung 3/);
  // Muskelkater deutlich schlechter: gelb (nicht rot), Laufen aus, Rad locker und kürzer
  const sore = at(d, upTo(d).map((w) => (w.date === d ? { ...w, soreness: 4 } : w)));
  assert.equal(sore.verdict.cls, "warn"); assert.equal(sore.verdict.dose.factor, 0.7);
  assert.equal(sore.verdict.sports.run.cls, "bad"); assert.equal(sore.verdict.sports.bike.cls, "warn");
  // Ermüdung deutlich schlechter bleibt rot: frei
  const tired = at(d, upTo(d).map((w) => (w.date === d ? { ...w, fatigue: 4 } : w)));
  assert.equal(tired.verdict.cls, "bad"); assert.equal(tired.verdict.dose.factor, 0);
  // Hüfte erwähnt: Ampel grün, Laufen aus
  const hip = computeReadiness(upTo(d), d, null, { sickDays: SICK, hipFlags: [{ date: "2026-10-19" }] });
  assert.equal(hip.verdict.cls, "ok"); assert.match(hip.verdict.sports.run.text, /kein Lauf.*Hüfte/);
  // Heute noch nichts eingetragen: kein Grün
  const miss = at(d, upTo(d).map((w) => (w.date === d ? { ...w, sleepQuality: null, soreness: null, fatigue: null, mood: null, motivation: null } : w)));
  assert.equal(miss.verdict.cls, "warn"); assert.equal(miss.verdict.missing, true); assert.match(miss.verdict.note, /gestern/);
}
// Langzeitbasis: 8 Wochen HRV 50 / Ruhepuls 45, dann 4 Wochen langsam auf 45 / 47 -> die 28-Tage-Basis ist mitgewandert,
// gegen die Tage 29–90 liegt der 7-Tage-Schnitt aber deutlich daneben -> ein gelbes Signal
{
  const day = (n) => new Date(Date.parse("2026-10-20T00:00:00Z") - n * 86400000).toISOString().slice(0, 10);
  const long = Array.from({ length: 90 }, (_, i) => { const n = 89 - i; return { date: day(n), hrv: n > 28 ? 50 : 45, restingHR: n > 28 ? 45 : 47 }; });
  const dr = computeDrift(long, "2026-10-20");
  assert.equal(dr.cls, "warn"); assert.equal(dr.hrvAnchor, 50); assert.equal(dr.hrv7, 45);
  assert.equal(computeDrift(long, "2026-10-20", long.slice(-7).map((x) => x.date)), null); // Krankheitstage zählen nicht
  const well = long.slice(-40).map((x) => ({ ...x, sleepScore: 85, sleepQuality: 2, soreness: 2, fatigue: 2, mood: 2, motivation: 2 }));
  const r = computeReadiness(well, "2026-10-20", null, { longWellness: long });
  assert.equal(r.body.hrv.cls, "ok"); assert.equal(r.body.drift.counted, true);
  assert.equal(r.verdict.cls, "warn"); assert.match(r.verdict.reasons.join(), /Langzeitbasis/);
}
// Leistungssignal: Puls bei lockerem Tempo; heutige Läufe zählen nicht (Ampel = Zustand vor dem Training)
{
  const run = (date, pace, hr, kind = "base") => ({ date, kind, movingTimeMin: 50, paceSecPerKm: pace, avgHr: hr });
  const base = ["2026-09-20", "2026-09-27", "2026-10-04", "2026-10-11"].map((x) => run(x, 420, 125));
  assert.equal(computeEasyRunSignal([...base, run("2026-10-18", 420, 134)], "2026-10-20").cls, "warn"); // 7 % weniger Tempo je Schlag
  assert.equal(computeEasyRunSignal([...base, run("2026-10-18", 420, 127)], "2026-10-20").cls, "ok");
  assert.equal(computeEasyRunSignal([...base, run("2026-10-20", 420, 140)], "2026-10-20"), null);
  assert.equal(computeEasyRunSignal([...base, run("2026-10-18", 380, 160, "intensity")], "2026-10-20"), null);
  const perf = { cls: "warn", text: "Puls bei lockerem Tempo erhöht" };
  assert.equal(at("2026-10-20").verdict.cls, computeReadiness(upTo("2026-10-20"), "2026-10-20", null, { sickDays: SICK, perf }).verdict.cls); // allein kein Gelb
  assert.equal(computeReadiness(upTo("2026-10-20").map((w) => (w.date === "2026-10-20" ? { ...w, soreness: 3 } : w)), "2026-10-20", null, { sickDays: SICK, perf }).verdict.cls, "warn"); // mit zweiter Warnung gelb
}

// Rad-Last aus Leistung (19.10.: HRSS 26, Leistung 43), Lauf und Fahrten ohne Leistung wie Intervals
assert.equal(sessionLoad({ type: "VirtualRide", icu_training_load: 26, power_load: 43 }), 43);
assert.equal(activityLoad({ type: "VirtualRide", icu_training_load: 26, power_load: 43 }), 26);
assert.equal(sessionLoad({ type: "Ride", icu_training_load: 30, power_load: null }), 30);
assert.equal(sessionLoad({ type: "Run", icu_training_load: 30, power_load: 50, pace_load: 34 }), 30);

console.log("readiness ok");
