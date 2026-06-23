import { isIsoDate } from "./date-utils.js";
import { parseISODateSafe, weeksBetween, daysBetween, clampStartDate } from "./date-utils.js";
import { mustEnv, readKvJson, writeKvJson } from "./kv.js";

const BLOCK_STATE_KV_PREFIX = "blockstate:latest:";

export const BLOCK_CONFIG = {
  durations: {
    BASE: { minDays: 28, maxDays: 84 },
    BUILD: { minDays: 21, maxDays: 56 },
    RACE: { minDays: 14, maxDays: 28 },
    RESET: { minDays: 7, maxDays: 14 },
  },
  cutoffs: {
    wave1Weeks: 20,
    wave2StartWeeks: 12,
    raceStartWeeks: 6,
    postEventResetWeeks: 2,
  },
};

export const BLOCK_LENGTHS_WEEKS_BY_DISTANCE = {
  "5k": { base: 10, build: 8, race: 6, taper: 1, reset: 2 },
  "10k": { base: 10, build: 8, race: 6, taper: 1, reset: 2 },
  hm: { base: 12, build: 8, race: 8, taper: 2, reset: 3 },
  m: { base: 16, build: 10, race: 8, taper: 2, reset: 4 },
};

export function normalizeEventDistance(value) {
  const s = String(value || "").toLowerCase().trim();
  if (!s) return null;
  if (s.includes("5k") || s.includes("5 km") || s.includes("5km")) return "5k";
  if (s.includes("10k") || s.includes("10 km") || s.includes("10km")) return "10k";
  if (s.includes("half") || s.includes("hm") || s.includes("halb")) return "hm";
  if (s.includes("marathon") || s === "m" || s.includes("42")) return "m";
  const numeric = Number(s.replace(/[^0-9.]/g, ""));
  if (Number.isFinite(numeric) && numeric > 0) {
    const meters = numeric < 1000 ? numeric * 1000 : numeric;
    if (meters >= 4900 && meters <= 5100) return "5k";
    if (meters >= 9500 && meters <= 10500) return "10k";
    if (meters >= 20500 && meters <= 21500) return "hm";
    if (meters >= 41000 && meters <= 43000) return "m";
  }
  return null;
}

export function getEventDistanceFromEvent(event) {
  if (!event) return null;
  const raw = event?.distance ?? event?.distance_target ?? null;
  const fromField = normalizeEventDistance(raw);
  if (fromField) return fromField;
  const name = String(event?.name ?? "");
  const type = String(event?.type ?? "");
  return normalizeEventDistance(`${name} ${type}`);
}

export function getBlockLengthsWeeks(eventDistance) {
  const dist = normalizeEventDistance(eventDistance) || "10k";
  return BLOCK_LENGTHS_WEEKS_BY_DISTANCE[dist] || BLOCK_LENGTHS_WEEKS_BY_DISTANCE["10k"];
}

export function getPlanStartWeeks(eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  return (lengths.base || 0) + (lengths.build || 0) + (lengths.race || 0) + (lengths.taper || 0);
}

export function getRaceStartWeeks(eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  return (lengths.race || 0) + (lengths.taper || 0);
}

export function getBlockDurationForDistance(block, eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  const weekByBlock = {
    BASE: lengths.base,
    BUILD: lengths.build,
    RACE: (lengths.race || 0) + (lengths.taper || 0),
    RESET: lengths.reset,
  };
  const weeks = weekByBlock[block];
  if (!Number.isFinite(weeks) || weeks <= 0) {
    const fallback = BLOCK_CONFIG.durations[block] || { minDays: 7, maxDays: 56 };
    const days = Math.max(7, Math.round(fallback.minDays || 7));
    return { minDays: days, maxDays: days };
  }
  const days = Math.max(7, Math.round(weeks * 7));
  return { minDays: days, maxDays: days };
}

export function getNextBlock(block, wave, weeksToEvent) {
  if (block === "BASE") return "BUILD";
  if (block === "BUILD") {
    if (weeksToEvent > BLOCK_CONFIG.cutoffs.wave2StartWeeks) return "RESET";
    return "RACE";
  }
  if (block === "RESET") {
    if (weeksToEvent > BLOCK_CONFIG.cutoffs.wave2StartWeeks) return "BASE";
    return "BUILD";
  }
  return weeksToEvent < 0 ? "RESET" : "RACE";
}

export function computeWeeksToEvent(todayISO, eventDateISO, reasons) {
  const weeksToEventRaw = weeksBetween(todayISO, eventDateISO);
  let weeksToEvent = weeksToEventRaw;
  const needsGuard = !Number.isFinite(weeksToEvent) || weeksToEvent < -2 || weeksToEvent > 104;
  if (needsGuard) {
    if (Array.isArray(reasons)) {
      const rawText = Number.isFinite(weeksToEventRaw) ? weeksToEventRaw.toFixed(2) : "n/a";
      reasons.push(`weeksToEvent unplausibel (${rawText}) → neu berechnet`);
    }
    weeksToEvent = weeksBetween(todayISO, eventDateISO);
  }
  if (!Number.isFinite(weeksToEvent)) {
    if (Array.isArray(reasons)) reasons.push("weeksToEvent konnte nicht berechnet werden");
    return { weeksToEventRaw, weeksToEvent: null };
  }
  return { weeksToEventRaw, weeksToEvent };
}

// Resolves the relevant event date/distance for block logic: the next upcoming
// A-race, or — if none lies ahead — the most recent past one, so that the
// "Event vorbei" (RESET/BASE-Reentry) branches in determineBlockState stay reachable.
export function resolveBlockEvent(races, todayIso) {
  const normDay = (e) => String(e?.start_date_local || e?.start_date || "").slice(0, 10);
  const sorted = (races || [])
    .map((e) => ({ e, day: normDay(e) }))
    .filter((x) => isIsoDate(x.day))
    .sort((a, b) => a.day.localeCompare(b.day));

  const next = sorted.find((x) => x.day >= todayIso) || null;
  if (next) {
    return { eventDate: next.day, eventDistance: getEventDistanceFromEvent(next.e) };
  }
  const lastPast = [...sorted].reverse().find((x) => x.day < todayIso) || null;
  if (lastPast) {
    return { eventDate: lastPast.day, eventDistance: getEventDistanceFromEvent(lastPast.e) };
  }
  return { eventDate: null, eventDistance: null };
}

// Simplified block/phase state machine: BASE -> BUILD -> RACE -> RESET.
// Block durations are fixed per distance (minDays === maxDays in
// getBlockDurationForDistance), so once a block's minimum duration is
// reached the switch to nextSuggestedBlock always fires immediately —
// there is no readiness gate to evaluate in between.
export function determineBlockState({ today, eventDate, eventDistance, previousState }) {
  const reasons = [];
  const eventDistanceNorm = normalizeEventDistance(eventDistance) || "10k";
  const planStartWeeks = getPlanStartWeeks(eventDistanceNorm);
  const raceStartWeeks = getRaceStartWeeks(eventDistanceNorm);

  const todayISO = today;
  const eventDateISO = eventDate || null;

  const persistedStart = previousState?.startDate || null;
  const clampedStart = clampStartDate(persistedStart, todayISO);
  let startDate = clampedStart || todayISO;
  if (!clampedStart && persistedStart) {
    reasons.push("Block-Startdatum unplausibel → Start neu gesetzt");
  }

  if (!eventDateISO || !parseISODateSafe(eventDateISO)) {
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent: null,
      todayISO,
      eventDateISO,
      startDate,
      timeInBlockDays: daysBetween(startDate, todayISO),
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock: "BUILD",
      reasons: ["Kein Event-Datum gefunden → BASE"],
    };
  }

  const { weeksToEventRaw, weeksToEvent } = computeWeeksToEvent(todayISO, eventDateISO, reasons);
  if (weeksToEvent == null) {
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent: null,
      todayISO,
      eventDateISO,
      startDate,
      timeInBlockDays: daysBetween(startDate, todayISO),
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock: "BUILD",
      reasons,
    };
  }

  if (String(previousState?.block || "").toUpperCase() === "RACE" && weeksToEvent > raceStartWeeks) {
    const fallbackBlock = weeksToEvent <= BLOCK_CONFIG.cutoffs.wave2StartWeeks ? "BUILD" : "BASE";
    reasons.push(
      `Persistierter RACE-Status invalidiert (${weeksToEvent.toFixed(1)} Wochen bis Event > RACE-Fenster ${raceStartWeeks} Wochen) → ${fallbackBlock}`,
    );
    startDate = todayISO;
  }

  if (weeksToEvent <= 4 && weeksToEvent >= 0) {
    const keepRaceStart = previousState?.block === "RACE";
    const raceStartDate = keepRaceStart ? startDate : todayISO;
    return {
      block: "RACE",
      wave: 0,
      weeksToEvent,
      todayISO,
      eventDateISO,
      startDate: raceStartDate,
      timeInBlockDays: keepRaceStart ? Math.max(0, daysBetween(raceStartDate, todayISO)) : 0,
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock: "RESET",
      reasons: [...reasons, "Event sehr nah (≤4 Wochen) → RACE"],
    };
  }

  if (weeksToEvent < 0) {
    if (Math.abs(weeksToEvent) <= BLOCK_CONFIG.cutoffs.postEventResetWeeks) {
      return {
        block: "RESET",
        wave: 0,
        weeksToEvent,
        todayISO,
        eventDateISO,
        startDate: todayISO,
        timeInBlockDays: 0,
        eventDistance: eventDistanceNorm,
        nextSuggestedBlock: "BASE",
        reasons: ["Event vorbei → RESET"],
      };
    }
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent,
      todayISO,
      eventDateISO,
      startDate: todayISO,
      timeInBlockDays: 0,
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock: "BUILD",
      reasons: ["Event vorbei → Re-Entry BASE"],
    };
  }

  if (weeksToEvent > planStartWeeks) {
    const stayedInFreeBase = previousState?.block === "BASE";
    const freeBaseStart = stayedInFreeBase ? startDate : todayISO;
    reasons.push(`Freie Vorphase aktiv (> ${planStartWeeks} Wochen bis Event) → BASE`);
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent,
      todayISO,
      eventDateISO,
      startDate: freeBaseStart,
      timeInBlockDays: stayedInFreeBase ? Math.max(0, daysBetween(freeBaseStart, todayISO)) : 0,
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock: "BASE",
      reasons,
    };
  }

  let wave = weeksToEvent > BLOCK_CONFIG.cutoffs.wave1Weeks ? 1 : 0;
  if (previousState?.wave === 2) wave = 2;
  if (weeksToEvent <= 8 && wave === 1) {
    wave = 0;
    reasons.push("Event ≤8 Wochen → Wave 1 deaktiviert");
  }

  let block = previousState?.block || (weeksToEvent <= raceStartWeeks ? "BUILD" : "BASE");
  if (String(previousState?.block || "").toUpperCase() === "RACE" && weeksToEvent > raceStartWeeks) {
    block = weeksToEvent <= BLOCK_CONFIG.cutoffs.wave2StartWeeks ? "BUILD" : "BASE";
  }

  const forcedBuildCutoff = raceStartWeeks + 6;
  if (weeksToEvent <= forcedBuildCutoff && block === "BASE") {
    block = "BUILD";
    startDate = todayISO;
    reasons.push(`Event ≤${forcedBuildCutoff} Wochen → BASE zu spät, Wechsel zu BUILD`);
  }

  let timeInBlockDays = daysBetween(startDate, todayISO);
  if (!Number.isFinite(timeInBlockDays) || timeInBlockDays < 0) timeInBlockDays = 0;
  const blockLimits = getBlockDurationForDistance(block, eventDistanceNorm);
  const nextSuggestedBlock = getNextBlock(block, wave, weeksToEvent);

  if (weeksToEvent <= raceStartWeeks && weeksToEvent >= 0 && block !== "RACE") {
    reasons.push(`Event ≤${raceStartWeeks} Wochen → sofort RACE (Taper-Puffer)`);
    block = "RACE";
    startDate = todayISO;
    timeInBlockDays = 0;
    return {
      block,
      wave,
      weeksToEvent,
      todayISO,
      eventDateISO,
      startDate,
      timeInBlockDays,
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock: getNextBlock(block, wave, weeksToEvent),
      reasons,
    };
  }

  if (timeInBlockDays < blockLimits.minDays) {
    reasons.push(`Mindestdauer ${blockLimits.minDays} Tage noch nicht erreicht`);
    return {
      block,
      wave,
      weeksToEvent,
      todayISO,
      eventDateISO,
      startDate,
      timeInBlockDays,
      eventDistance: eventDistanceNorm,
      nextSuggestedBlock,
      reasons,
    };
  }

  reasons.push(`Maxdauer ${blockLimits.maxDays} Tage überschritten → Wechsel erzwungen`);
  block = nextSuggestedBlock;
  startDate = todayISO;
  timeInBlockDays = 0;
  return {
    block,
    wave: block === "BASE" && wave === 1 ? 2 : wave,
    weeksToEvent,
    todayISO,
    eventDateISO,
    startDate,
    timeInBlockDays,
    eventDistance: eventDistanceNorm,
    nextSuggestedBlock: getNextBlock(block, wave, weeksToEvent),
    reasons,
  };
}

export function applyManualBlockStartOverride(blockState, overrideIso, dayIso) {
  if (!blockState || !overrideIso || !isIsoDate(dayIso)) return blockState;
  const overrideStart = clampStartDate(overrideIso, dayIso, 3650);
  if (!overrideStart) return blockState;
  return {
    ...blockState,
    startDate: overrideStart,
    timeInBlockDays: Math.max(0, daysBetween(overrideStart, dayIso)),
    reasons: [...(blockState.reasons || []), `Manueller Block-Start aktiv (${overrideStart})`],
  };
}

const VALID_BLOCKS = new Set(["BASE", "BUILD", "RACE", "RESET"]);

export function applyManualBlockOverride(blockState, overrideBlock, dayIso) {
  if (!blockState || !overrideBlock || !isIsoDate(dayIso)) return blockState;
  const block = String(overrideBlock).toUpperCase();
  if (!VALID_BLOCKS.has(block)) return blockState;
  const startDate = blockState.startDate || dayIso;
  return {
    ...blockState,
    block,
    timeInBlockDays: Math.max(0, daysBetween(startDate, dayIso)),
    nextSuggestedBlock: getNextBlock(block, blockState.wave, blockState.weeksToEvent),
    reasons: [...(blockState.reasons || []), `Manueller Block-Override aktiv (${block})`],
  };
}

function getBlockStateKvKey(env) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  return `${BLOCK_STATE_KV_PREFIX}${athleteId}`;
}

export async function readLatestBlockStateKv(env, dayIso) {
  const key = getBlockStateKvKey(env);
  const raw = await readKvJson(env, key);
  if (!raw || typeof raw !== "object") return null;
  if (raw.day && isIsoDate(raw.day) && raw.day > dayIso) return null;
  const state = raw.state;
  if (!state || typeof state !== "object" || !state.block || !state.startDate) return null;
  return {
    block: state.block,
    wave: Number.isFinite(Number(state.wave)) ? Number(state.wave) : 0,
    startDate: isIsoDate(state.startDate) ? state.startDate : null,
  };
}

export async function writeLatestBlockStateKv(env, dayIso, state) {
  if (!state?.block || !state?.startDate) return;
  const key = getBlockStateKvKey(env);
  await writeKvJson(env, key, { day: dayIso, state });
}
