import { isIsoDate, isoDate, weeksBetween, mondayOnOrBefore } from "./date-utils.js";
import { hasKv, readKvJson, writeKvJson, mustEnv } from "./kv.js";
import { isRun, activityDay } from "./activity-utils.js";
import { fetchIntervalsActivities } from "./intervals-client.js";

const LONG_RUN_PLAN_KV_PREFIX = "longrunplan:";

// How far back "the current longest run" (used both as the plan baseline and as the
// "already ahead of the generic peak" check in getPeakLongRunKm) looks - Teil 2.
const LONG_RUN_BASELINE_LOOKBACK_DAYS = 28;

// Peak-long-run sizing per race distance (Teil 1). Kept as named constants so the
// factors/caps can be tuned without touching the classification logic below.
const PEAK_LONG_RUN_MARATHON_MIN_KM = 35;
const PEAK_LONG_RUN_MARATHON_FACTOR = 0.75;
const PEAK_LONG_RUN_MARATHON_CAP_KM = 32;

const PEAK_LONG_RUN_HM_MIN_KM = 18;
const PEAK_LONG_RUN_HM_MAX_KM = 25;
const PEAK_LONG_RUN_HM_FACTOR = 0.9;
const PEAK_LONG_RUN_HM_CAP_KM = 20;

const PEAK_LONG_RUN_10K_MIN_KM = 8;
const PEAK_LONG_RUN_10K_MAX_KM = 12;
const PEAK_LONG_RUN_10K_FACTOR = 1.5;
const PEAK_LONG_RUN_10K_CAP_KM = 16;

const PEAK_LONG_RUN_5K_MAX_KM = 8;
const PEAK_LONG_RUN_5K_FACTOR = 2.2;
const PEAK_LONG_RUN_5K_CAP_KM = 12;

// Advanced-runner override (Teil 1 Sonderfall): if the current longest run already
// beats the generic target, don't shrink it back down - build on top of it instead.
const LONG_RUN_ADVANCED_RUNNER_BUFFER_KM = 2;

// Race-distance enum (as stored by goal-race.js) -> representative km, so the km-based
// rules above work off the existing raceDistance/raceDate goal fields.
export const RACE_DISTANCE_KM_BY_ENUM = { "5k": 5, "10k": 10, hm: 21.0975, m: 42.195 };

export const TAPER_WEEKS_BY_DISTANCE = { m: 3, hm: 2, "10k": 1, "5k": 1 };

// Weekly increase caps a peak-week value into the last taperWeeks weeks before the
// race, as a fraction of peakLongRunKm (Teil 3).
const TAPER_FRACTIONS_BY_DISTANCE = {
  m: [0.65, 0.45, 0.25],
  hm: [0.6, 0.35],
  "10k": [0.5],
  "5k": [0.5],
};

const LONG_RUN_CUTBACK_EVERY_WEEKS = 4;
const LONG_RUN_CUTBACK_FACTOR = 0.75;
const LONG_RUN_MAX_INCREMENT_KM_PER_WEEK = 2.5;

function roundToHalfKm(km) {
  return Math.round(km * 2) / 2;
}

function addDays(dayIso, n) {
  return isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() + n * 86400000));
}

function addWeeks(dayIso, n) {
  return addDays(dayIso, n * 7);
}

export function raceDistanceEnumToKm(distanceEnum) {
  return RACE_DISTANCE_KM_BY_ENUM[distanceEnum] ?? null;
}

// Teil 1: generic peak-long-run target for a race distance, overridden upward when
// the athlete is already running longer than that generic target (see
// LONG_RUN_ADVANCED_RUNNER_BUFFER_KM above).
export function getPeakLongRunKm(raceDistanceKm, currentLongRunKm) {
  if (!(raceDistanceKm > 0)) return null;

  let generic;
  if (raceDistanceKm > PEAK_LONG_RUN_MARATHON_MIN_KM) {
    generic = Math.min(PEAK_LONG_RUN_MARATHON_CAP_KM, raceDistanceKm * PEAK_LONG_RUN_MARATHON_FACTOR);
  } else if (raceDistanceKm >= PEAK_LONG_RUN_HM_MIN_KM && raceDistanceKm <= PEAK_LONG_RUN_HM_MAX_KM) {
    generic = Math.min(PEAK_LONG_RUN_HM_CAP_KM, raceDistanceKm * PEAK_LONG_RUN_HM_FACTOR);
  } else if (raceDistanceKm >= PEAK_LONG_RUN_10K_MIN_KM && raceDistanceKm <= PEAK_LONG_RUN_10K_MAX_KM) {
    generic = Math.min(PEAK_LONG_RUN_10K_CAP_KM, raceDistanceKm * PEAK_LONG_RUN_10K_FACTOR);
  } else if (raceDistanceKm < PEAK_LONG_RUN_5K_MAX_KM) {
    generic = Math.min(PEAK_LONG_RUN_5K_CAP_KM, raceDistanceKm * PEAK_LONG_RUN_5K_FACTOR);
  } else {
    // Distances that fall between two defined bands (e.g. 12-18km, 25-35km) aren't
    // covered by the spec - fall back to whichever neighboring band is closer.
    generic =
      raceDistanceKm < PEAK_LONG_RUN_HM_MIN_KM
        ? Math.min(PEAK_LONG_RUN_10K_CAP_KM, raceDistanceKm * PEAK_LONG_RUN_10K_FACTOR)
        : Math.min(PEAK_LONG_RUN_HM_CAP_KM, raceDistanceKm * PEAK_LONG_RUN_HM_FACTOR);
  }

  if (Number.isFinite(currentLongRunKm) && currentLongRunKm > generic) {
    return roundToHalfKm(currentLongRunKm + LONG_RUN_ADVANCED_RUNNER_BUFFER_KM);
  }
  return roundToHalfKm(generic);
}

function longRunPlanKvKey(env) {
  return `${LONG_RUN_PLAN_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}

export async function readLongRunPlan(env) {
  if (!hasKv(env)) return null;
  return readKvJson(env, longRunPlanKvKey(env)).catch(() => null);
}

export async function writeLongRunPlan(env, plan) {
  await writeKvJson(env, longRunPlanKvKey(env), plan);
}

// Longest run (km) within [startIso, endIso] from raw intervals.icu activities (as
// returned by fetchIntervalsActivities) - used for the weekly-progress report, which
// already has the full activities list loaded.
export function longestRunKmInActivities(activities, startIso, endIso) {
  let maxM = 0;
  let found = false;
  for (const a of activities || []) {
    if (!isRun(a)) continue;
    const day = activityDay(a);
    if (!day || day < startIso || day > endIso) continue;
    const distanceM = Number(a?.distance ?? a?.icu_distance ?? 0) || 0;
    if (distanceM > maxM) {
      maxM = distanceM;
      found = true;
    }
  }
  return found ? Math.round((maxM / 1000) * 100) / 100 : null;
}

// Same, but from already-built run records ({ date, distanceKm }, e.g. the `runs`
// array in form-analysis.js) rather than raw activities.
export function longestRunKmInRunRecords(runs, startIso, endIso) {
  let max = null;
  for (const r of runs || []) {
    if (!r || r.date < startIso || r.date > endIso) continue;
    if (typeof r.distanceKm !== "number") continue;
    if (max == null || r.distanceKm > max) max = r.distanceKm;
  }
  return max;
}

async function computeCurrentLongRunKm(env, todayIso) {
  const oldest = addDays(todayIso, -(LONG_RUN_BASELINE_LOOKBACK_DAYS - 1));
  const activities = await fetchIntervalsActivities(env, oldest, todayIso).catch(() => []);
  return longestRunKmInActivities(activities, oldest, todayIso) ?? 0;
}

// Teil 3: one-shot build of the week-by-week long-run target table, from the
// baseline week (todayIso) through to the race. Returns the final peakLongRunKm
// (possibly overridden to planBaselineLongRunKm if there's no build time left) plus
// the timelineAmbitious flag alongside the table, since both are derived together.
export function buildLongRunProgressionWeeks({ todayIso, raceDate, distanceEnum, peakLongRunKm, planBaselineLongRunKm, taperWeeks }) {
  const baselineMonday = mondayOnOrBefore(todayIso);
  const weeksUntilRace = Math.max(0, Math.floor(weeksBetween(todayIso, raceDate)));
  const buildWeeks = weeksUntilRace - taperWeeks;

  let finalPeakLongRunKm = peakLongRunKm;
  let timelineAmbitious = false;
  const weeks = [];
  let buildWeeksCount = 0;

  if (buildWeeks <= 0) {
    // No time left to build up - hold the current baseline instead of forcing a peak
    // that isn't reachable in the remaining weeks.
    finalPeakLongRunKm = planBaselineLongRunKm;
  } else {
    buildWeeksCount = buildWeeks;
    const totalIncreaseKm = Math.max(0, finalPeakLongRunKm - planBaselineLongRunKm);
    const progressingWeeks = buildWeeksCount - Math.floor(buildWeeksCount / LONG_RUN_CUTBACK_EVERY_WEEKS);
    let incrementPerWeek = totalIncreaseKm / Math.max(1, progressingWeeks);
    if (incrementPerWeek > LONG_RUN_MAX_INCREMENT_KM_PER_WEEK) {
      incrementPerWeek = LONG_RUN_MAX_INCREMENT_KM_PER_WEEK;
      timelineAmbitious = true;
    }

    let prevTarget = planBaselineLongRunKm;
    for (let w = 1; w <= buildWeeksCount; w++) {
      const target =
        w % LONG_RUN_CUTBACK_EVERY_WEEKS === 0
          ? roundToHalfKm(prevTarget * LONG_RUN_CUTBACK_FACTOR)
          : roundToHalfKm(Math.min(finalPeakLongRunKm, prevTarget + incrementPerWeek));
      weeks.push({ weekStart: addWeeks(baselineMonday, w), targetKm: target });
      prevTarget = target;
    }
  }

  const fractions = TAPER_FRACTIONS_BY_DISTANCE[distanceEnum] || TAPER_FRACTIONS_BY_DISTANCE["10k"];
  for (let k = 1; k <= taperWeeks; k++) {
    const fraction = fractions[k - 1] ?? fractions[fractions.length - 1];
    weeks.push({ weekStart: addWeeks(baselineMonday, buildWeeksCount + k), targetKm: roundToHalfKm(finalPeakLongRunKm * fraction) });
  }

  return { peakLongRunKm: finalPeakLongRunKm, timelineAmbitious, weeks };
}

// Teil 2/3 orchestration: rebuilds and persists the long-run plan iff raceDate/
// raceDistance were newly set or actually changed vs. the previously stored goal -
// never on every call, so the target table stays stable relative to actual training.
export async function maybeRebuildLongRunPlanOnGoalChange(env, previousGoal, newGoal, todayIso) {
  const distanceEnum = newGoal?.distance;
  const raceDate = newGoal?.date;
  if (!distanceEnum || !isIsoDate(raceDate)) return null;

  const changed = !previousGoal || previousGoal.date !== raceDate || previousGoal.distance !== distanceEnum;
  if (!changed) return null;

  const raceDistanceKm = raceDistanceEnumToKm(distanceEnum);
  if (raceDistanceKm == null) return null;

  const planBaselineLongRunKm = await computeCurrentLongRunKm(env, todayIso);
  const genericPeakLongRunKm = getPeakLongRunKm(raceDistanceKm, planBaselineLongRunKm);
  const taperWeeks = TAPER_WEEKS_BY_DISTANCE[distanceEnum] ?? 2;

  const { peakLongRunKm, timelineAmbitious, weeks } = buildLongRunProgressionWeeks({
    todayIso,
    raceDate,
    distanceEnum,
    peakLongRunKm: genericPeakLongRunKm,
    planBaselineLongRunKm,
    taperWeeks,
  });

  const plan = {
    planBaselineLongRunKm,
    planBaselineDate: todayIso,
    raceDate,
    raceDistanceKm,
    raceDistanceEnum: distanceEnum,
    peakLongRunKm,
    timelineAmbitious,
    taperWeeks,
    weeks,
  };
  await writeLongRunPlan(env, plan);
  return plan;
}

// Teil 4: current week's target from the persisted table (no recomputation).
export function getTargetLongRunKmForWeek(plan, mondayIso) {
  if (!plan || !Array.isArray(plan.weeks) || !mondayIso) return null;
  const entry = plan.weeks.find((w) => w.weekStart === mondayIso);
  return entry ? entry.targetKm : null;
}
