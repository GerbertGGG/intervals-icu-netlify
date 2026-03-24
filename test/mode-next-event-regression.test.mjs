import assert from "node:assert/strict";
import { __test } from "../src/index.js";

const env = { INTERVALS_API_KEY: "test-key" };

const hardyEvent = {
  name: "Hardys Stadtlauf",
  start_date_local: "2026-10-03T00:00:00",
  category: "RACE_A",
  type: "Run",
  distance: 21100,
};

const pastRace = {
  name: "Fruehjahrslauf",
  start_date_local: "2026-03-20T09:00:00",
  category: "RACE_A",
  type: "Run",
  distance: 10000,
};

const dayIso = "2026-03-24";

assert.equal(__test.isARaceCategory(hardyEvent.category), true);
assert.equal(__test.isARaceEvent(hardyEvent), true);

const modeInfo = await __test.determineMode(env, dayIso, false, [pastRace, hardyEvent]);
assert.equal(modeInfo.mode, "OPEN");
assert.equal(modeInfo.postEventOpenActive, true);
assert.equal(modeInfo.nextEvent?.name, "Hardys Stadtlauf");

const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);
assert.equal(eventDate, "2026-10-03");

const blockState = __test.determineBlockState({
  today: dayIso,
  eventDate,
  eventDistance: null,
  historyMetrics: {},
  previousState: null,
  efTrend: null,
});

assert.notEqual(blockState.eventDateISO, null);
assert.notEqual(blockState.eventDate, null);
assert.notEqual(blockState.weeksToEvent, null);
assert.ok(!(blockState.reasons || []).includes("Kein Event-Datum gefunden → BASE"));
