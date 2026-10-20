// Montagsbilanz (week-review.js): wie die letzte volle Woche verkraftet wurde und wie stark der Rahmen steigen darf.
// Ausführen: node test/week-review.test.mjs
import assert from "node:assert/strict";
import { buildWeekReview } from "../src/week-review.js";

const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);
const sport = (minutes, load = minutes, targetMin = null, targetLoad = null) => ({ count: minutes ? 3 : 0, minutes, load, targetMin, targetLoad });
// 5 volle Wochen (Mo 14.09. – So 18.10.) plus die laufende ab 19.10.
const week = (start, runMin, extra = {}) => ({
  weekStart: start, complete: start < "2026-10-19", runMin, load: 300,
  bySport: { run: sport(runMin, runMin, extra.runTarget ?? null), bike: sport(150, 150, extra.bikeTarget ?? null, extra.bikeCap ?? null), swim: sport(60), strength: sport(40), other: sport(0) },
});
const starts = ["2026-09-14", "2026-09-21", "2026-09-28", "2026-10-05", "2026-10-12", "2026-10-19"];
const weeks = starts.map((s, i) => week(s, i === 4 ? 150 : 140, i === 4 ? { runTarget: 150, bikeTarget: 150 } : {}));
const days = Array.from({ length: 35 }, (_, i) => addDays("2026-09-14", i));
const wellness = days.map((date) => ({ date, fatigue: 2, soreness: 2 }));
// jeden Tag trainiert, unterschiedlich lang (keine Monotonie), RPE bei allen Einheiten
const sessions = days.map((date, i) => ({ date, sport: i % 2 ? "bike" : "run", minutes: [30, 90, 45, 60, 20, 120, 40][i % 7], rpe: 3 }));
const runs = days.filter((_, i) => i % 2 === 0).map((date, i) => ({ date, movingTimeMin: [30, 45, 20, 40, 90][i % 5] }));
const base = { today: "2026-10-20", weeks, wellness, sessions, runs, daily: [], ampel: () => "ok", sickDays: [], hipFlags: [], capRuns: null, block: { status: "active", weekNo: 2 } };

// Gut verkraftet: steigern, Laufen höchstens +10 %
{
  const r = buildWeekReview(base);
  assert.equal(r.weekStart, "2026-10-12"); assert.equal(r.level, "up"); assert.equal(r.reasons.length, 0);
  assert.equal(r.ampel.ok, 7); assert.equal(r.rpe.basis, "rpe"); assert.ok(r.rpe.monotony < 2);
  const run = r.proposal.find((p) => p.sport === "run"), bike = r.proposal.find((p) => p.sport === "bike");
  assert.deepEqual([run.base, run.from, run.to], [150, 160, 165]); // Rahmen 150 × 1,05–1,10
  assert.deepEqual([bike.from, bike.to], [160, 165]);
  assert.equal(r.run.ratio, 1.07); // 150 gegen Ø 140
}
// Zwei gelbe Tage: halten
{
  const r = buildWeekReview({ ...base, ampel: (d) => (["2026-10-14", "2026-10-17"].includes(d) ? "warn" : "ok") });
  assert.equal(r.level, "hold"); assert.match(r.reasons.join(), /2 gelbe Tage/);
  assert.equal(r.proposal.find((p) => p.sport === "run").to, 150);
}
// Ermüdung im Wochenschnitt 0,5 schlechter als davor: halten
{
  const r = buildWeekReview({ ...base, wellness: wellness.map((w) => (w.date >= "2026-10-12" ? { ...w, fatigue: 3 } : w)) });
  assert.equal(r.level, "hold"); assert.equal(r.feel.fatigue.delta, 1);
}
// Ein roter Tag oder Krankheit: zurücknehmen
{
  assert.equal(buildWeekReview({ ...base, ampel: (d) => (d === "2026-10-15" ? "bad" : "ok") }).level, "down");
  assert.equal(buildWeekReview({ ...base, sickDays: ["2026-10-13"] }).level, "down");
}
// Hüfte erwähnt: nur Laufen zurück, der Rest steigt
{
  const r = buildWeekReview({ ...base, hipFlags: [{ date: "2026-10-16" }] });
  assert.equal(r.level, "up"); assert.equal(r.runLevel, "down"); assert.match(r.runText, /zurücknehmen/);
  assert.equal(r.proposal.find((p) => p.sport === "run").to, 115); // 150 × 0,75 auf 5 min
}
// Laufumfang deutlich über dem Schnitt oder über dem HF-Deckel: Laufen halten
{
  const steep = weeks.map((w) => (w.weekStart === "2026-10-12" ? { ...w, runMin: 170 } : w));
  assert.equal(buildWeekReview({ ...base, weeks: steep }).runLevel, "hold");
  assert.equal(buildWeekReview({ ...base, capRuns: [{ date: "2026-10-18", overCapMin: 14 }] }).runLevel, "hold");
}
// Monotonie: jeden Tag gleich viel -> halten; ohne RPE rechnet sie mit dem Tages-TSS
{
  const same = sessions.map((x) => ({ ...x, minutes: 60 }));
  const daily = days.map((date) => ({ date, load: 50 }));
  // Standardabweichung 0 -> keine Zahl; ein Ruhetag bei sonst gleicher Last ergibt Monotonie > 2
  const withRest = same.filter((x) => x.date !== "2026-10-18");
  const r = buildWeekReview({ ...base, sessions: withRest });
  assert.ok(r.rpe.monotony > 2); assert.equal(r.level, "hold");
  const tss = buildWeekReview({ ...base, sessions: sessions.map((x) => ({ ...x, rpe: null })), daily: daily.map((x) => (x.date === "2026-10-18" ? { ...x, load: 0 } : x)) });
  assert.equal(tss.rpe.basis, "tss"); assert.ok(tss.rpe.monotony > 2);
}
// Rahmen nur zu 60 % erfüllt: halten (nicht auf eine nicht geschaffte Woche draufpacken)
{
  const low = weeks.map((w) => (w.weekStart === "2026-10-12" ? { ...w, bySport: { ...w.bySport, run: sport(90, 90, 150), bike: sport(90, 90, 150) } } : w));
  const r = buildWeekReview({ ...base, weeks: low });
  assert.equal(r.frameShare, 0.6); assert.equal(r.level, "hold");
}
// Entlastungswoche (Blockwoche 4): Entlastung geht vor
{
  const r = buildWeekReview({ ...base, block: { status: "active", weekNo: 4 } });
  assert.equal(r.level, "deload"); assert.equal(r.proposal.find((p) => p.sport === "run").to, 100); // 150 × 0,65
}
// Ohne volle Woche keine Bilanz
assert.equal(buildWeekReview({ ...base, weeks: [weeks[5]] }), null);

console.log("week review ok");
