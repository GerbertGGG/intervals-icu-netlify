import { isoDate, isMondayIso, daysBetween, listIsoDaysInclusive, clampStartDate } from "./date-utils.js";
import { isARaceEvent } from "./event-utils.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, putWellnessDay } from "./intervals-client.js";
import {
  determineBlockState,
  resolveBlockEvent,
  readLatestBlockStateKv,
  writeLatestBlockStateKv,
  applyManualBlockStartOverride,
  applyManualBlockOverride,
} from "./block-phase.js";
import { computeAndPersistRealVdot } from "./vdot.js";

const FIELD_VDOT = "VDOT";
const FIELD_VDOT_AVG = "VDOTAvg";
const FIELD_BLOCK = "Block";
const EVENT_LOOKAHEAD_DAYS = 365;
const EVENT_LOOKBACK_DAYS = 40;
const ACTIVITIES_LOOKBACK_DAYS = 180;

async function fetchRaces(env, oldest, newest) {
  const start = isoDate(new Date(new Date(oldest + "T00:00:00Z").getTime() - EVENT_LOOKBACK_DAYS * 86400000));
  const end = isoDate(new Date(new Date(newest + "T00:00:00Z").getTime() + EVENT_LOOKAHEAD_DAYS * 86400000));
  const events = await fetchIntervalsEvents(env, start, end);
  const list = Array.isArray(events) ? events : Array.isArray(events?.events) ? events.events : [];
  return list.filter((e) => isARaceEvent(e));
}

export async function syncRange(env, oldest, newest, write, debug, syncOptions = {}) {
  const { raceStartOverrideIso = null, blockStartOverrideIso = null, blockOverride = null } = syncOptions;
  const days = listIsoDaysInclusive(oldest, newest);
  const activitiesOldest = isoDate(new Date(new Date(oldest + "T00:00:00Z").getTime() - ACTIVITIES_LOOKBACK_DAYS * 86400000));
  const activities = await fetchIntervalsActivities(env, activitiesOldest, newest);
  const races = await fetchRaces(env, oldest, newest);

  const results = [];
  let previousBlockState = null;

  for (let i = 0; i < days.length; i++) {
    const day = days[i];
    const isLastDay = i === days.length - 1;
    if (!previousBlockState) {
      const prevDay = isoDate(new Date(new Date(day + "T00:00:00Z").getTime() - 86400000));
      previousBlockState = (await readLatestBlockStateKv(env, prevDay)) || (await readLatestBlockStateKv(env, day));
    }

    const { eventDate, eventDistance } = resolveBlockEvent(races, day);

    let blockState = determineBlockState({ today: day, eventDate, eventDistance, previousState: previousBlockState });

    if (raceStartOverrideIso && blockState.block === "RACE") {
      const overrideStart = clampStartDate(raceStartOverrideIso, day, 3650);
      if (overrideStart) {
        blockState = { ...blockState, startDate: overrideStart, timeInBlockDays: Math.max(0, daysBetween(overrideStart, day)) };
      }
    }
    if (blockStartOverrideIso) {
      blockState = applyManualBlockStartOverride(blockState, blockStartOverrideIso, day);
    }
    if (blockOverride) {
      blockState = applyManualBlockOverride(blockState, blockOverride, day);
    }

    // Only the last day's state needs to land in KV: it's a "latest known state" cache
    // that seeds the next, separate sync invocation. Writing it on every day of a
    // multi-day range hits Cloudflare KV's per-key write rate limit (429) during backfills.
    const vdotResult = await computeAndPersistRealVdot(env, activities, {
      write,
      todayIso: day,
      isMondaySync: isMondayIso(day),
      persistLatest: write && isLastDay,
    });

    const patch = { [FIELD_BLOCK]: blockState.block };
    if (vdotResult?.vdot != null) {
      if (vdotResult.todayRunVdot != null) {
        patch[FIELD_VDOT] = Math.round(vdotResult.todayRunVdot * 10) / 10;
      }
      patch[FIELD_VDOT_AVG] = Math.round(vdotResult.vdot * 10) / 10;
    }

    if (write) {
      await putWellnessDay(env, day, patch);
    }
    if (write && isLastDay) {
      await writeLatestBlockStateKv(env, day, { block: blockState.block, wave: blockState.wave, startDate: blockState.startDate || day });
    }

    previousBlockState = { block: blockState.block, wave: blockState.wave, startDate: blockState.startDate || day };

    results.push({
      day,
      block: blockState.block,
      wave: blockState.wave,
      weeksToEvent: blockState.weeksToEvent,
      nextSuggestedBlock: blockState.nextSuggestedBlock,
      reasons: debug ? blockState.reasons : undefined,
      vdot: vdotResult?.vdot ?? null,
      vdotSource: vdotResult?.source ?? null,
      patch,
    });
  }

  return { ok: true, oldest, newest, write, days: results };
}
