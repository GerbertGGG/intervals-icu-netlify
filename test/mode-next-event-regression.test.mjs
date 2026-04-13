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
assert.equal(__test.isARaceCategory("RACE_B"), false);
assert.equal(__test.isARaceEvent({
  name: "Vorbereitungsrennen",
  start_date_local: "2026-05-01T09:00:00",
  category: "RACE_B",
  type: "Race",
}), false);
assert.equal(__test.isARaceEvent({
  name: "Race mit Priorität B",
  start_date_local: "2026-06-10T09:00:00",
  category: "RACE_A",
  racePriority: "B",
  type: "Race",
}), false);

const modeInfo = await __test.determineMode(env, dayIso, false, [pastRace, hardyEvent]);
assert.equal(modeInfo.mode, "OPEN");
assert.equal(modeInfo.postEventOpenActive, true);
assert.equal(modeInfo.nextEvent?.name, "Hardys Stadtlauf");

const modeWithBRaceOnly = await __test.determineMode(env, dayIso, false, [{
  name: "B-Race",
  start_date_local: "2026-04-20T09:00:00",
  category: "RACE_B",
  type: "Race",
  distance: 10000,
}]);
assert.equal(modeWithBRaceOnly.mode, "OPEN");
assert.equal(modeWithBRaceOnly.nextEvent, null);

const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);
assert.equal(eventDate, "2026-10-03");

const blockState = __test.determineBlockState({
  today: dayIso,
  eventDate,
  eventDistance: null,
  historyMetrics: {},
  previousState: null,
  efTrend: null,
  postEventOpenActive: true,
});

assert.equal(blockState.eventDateISO, "2026-10-03");
assert.equal(blockState.eventDate, undefined);
assert.equal(blockState.weeksToEvent, null);
assert.ok((blockState.reasons || []).includes("Post-Race-Fenster aktiv → Re-Entry BASE"));
