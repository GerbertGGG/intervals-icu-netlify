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
