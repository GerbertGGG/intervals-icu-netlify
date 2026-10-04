// Triathlon-Eintrag aus Intervals.icu: Format steht in der Beschreibung. Ausführen: node test/triathlon.test.mjs
import assert from "node:assert/strict";
import { parseTriathlonEvent, getEventDistanceFromEvent } from "../src/block-phase.js";
import { deriveAutoGoalFromRaces } from "../src/goal-race.js";

const ev = (description, extra = {}) => ({ name: "Triathlon", category: "RACE_A", start_date_local: "2027-10-03T00:00:00", description, ...extra });

const mid = parseTriathlonEvent(ev("Mitteldistanz"));
assert.equal(mid.format, "middle");
assert.equal(mid.runDistance, "hm");
assert.deepEqual([mid.swimKm, mid.bikeKm], [1.9, 90]);
assert.equal(parseTriathlonEvent(ev("Sprint")).runDistance, "5k");
assert.equal(parseTriathlonEvent(ev("Olympische Distanz")).runDistance, "10k");
assert.equal(parseTriathlonEvent(ev("Langdistanz")).runDistance, "m");
assert.equal(parseTriathlonEvent(ev("70.3")).format, "middle");
assert.equal(parseTriathlonEvent(ev("Mitteldistanz\nLauf 1:55:00")).runTargetSecs, 6900);
assert.equal(parseTriathlonEvent(ev("Mitteldistanz")).runTargetSecs, null);
assert.equal(parseTriathlonEvent({ name: "Halbmarathon", description: "Mitteldistanz" }), null);
assert.equal(getEventDistanceFromEvent(ev("Mitteldistanz")), "hm");

const goal = deriveAutoGoalFromRaces([ev("Mitteldistanz", { time_target: 19800 })], "2026-10-05");
assert.equal(goal.distance, "hm");
assert.equal(goal.targetTimeSecs, 19800);
assert.equal(goal.triathlon.label, "Mitteldistanz");
console.log("triathlon ok");

import { buildTriathlonTargets } from "../src/triathlon-targets.js";
const th = { bike: { ftp: 250 }, swim: { thresholdPaceSecPer100m: 110 } };
const t1 = buildTriathlonTargets(parseTriathlonEvent(ev("Mitteldistanz")), th, 19800);
assert.equal(t1.bike.watts, 180);
assert.equal(t1.bike.source, "vorschlag");
assert.equal(Math.round(t1.swim.pacePer100m), 117);
assert.equal(t1.run, null);
const t2 = buildTriathlonTargets(parseTriathlonEvent(ev("Mitteldistanz\nSchwimmen 40:00\nRad 3:00:00\nLauf 1:55:00")), th);
assert.equal(t2.swim.timeSecs, 2400);
assert.equal(Math.round(t2.bike.speedKmh), 30);
assert.equal(t2.run.timeSecs, 6900);
assert.equal(parseTriathlonEvent(ev("Mitteldistanz\nRad 215 W")).bikeTargetWatts, 215);
assert.equal(buildTriathlonTargets(parseTriathlonEvent(ev("Mitteldistanz")), { bike: {}, swim: {} }).bike, null);
const t3 = buildTriathlonTargets(parseTriathlonEvent(ev("Mitteldistanz")), th, null, { vdot: 40, weightKg: 75 });
assert.ok(t3.bike.timeSecs > 9000 && t3.bike.timeSecs < 11000);
assert.equal(t3.run.source, "vorschlag");
assert.ok(t3.run.timeSecs > 6000 && t3.run.timeSecs < 9000);
console.log("targets ok");

import { historyEntryFromSnapshot, upsertHistory } from "../src/runalyze-history.js";
const snap = { fetchedAt: "2026-10-01T08:00:00Z", vdot: 34.7, prognosis: [{ distanceKm: 5, seconds: 1629 }, { distanceKm: 10, seconds: 3383 }, { distanceKm: 21.0975, seconds: 8061 }], races: [] };
const he = historyEntryFromSnapshot(snap);
assert.equal(he.date, "2026-10-01");
assert.equal(he.hmProgSecs, 8061);
assert.equal(he.p5Secs, 1629);
assert.equal(he.p10Secs, 3383);
assert.ok(he.hmVdotSecs > 6900 && he.hmVdotSecs < 8000);
let hist = upsertHistory([], he);
hist = upsertHistory(hist, { ...he, hmProgSecs: 8000 });
hist = upsertHistory(hist, { ...he, date: "2026-09-24" });
assert.deepEqual(hist.map((e) => e.date), ["2026-09-24", "2026-10-01"]);
assert.equal(hist[1].hmProgSecs, 8000);
assert.equal(historyEntryFromSnapshot({ fetchedAt: "2026-10-01T00:00:00Z", races: [] }), null);
console.log("history ok");
