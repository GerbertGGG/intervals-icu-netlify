import { isoDate } from "./date-utils.js";
import { isRun, isRaceActivity, isTreadmill, isIntervalActivity, isVdotExcluded, activityDay, activityLoad } from "./activity-utils.js";
import {
  fetchIntervalsActivities,
  fetchIntervalsWellnessRange,
  fetchIntervalsActivityDetail,
  fetchIntervalsActivityIntervals,
  fetchIntervalsActivityMap,
} from "./intervals-client.js";
import { resolveMaxHr, estimateTrainingVdotForWindow, computeVdotFromRaceTime } from "./vdot.js";
import {
  resolveRunWeather,
  readCachedActivityWeather,
  writeCachedActivityWeather,
  readCachedActivityGps,
  writeCachedActivityGps,
} from "./weather.js";

// HR% range (of max HR) treated as "easy/aerobic" for the purpose of a like-for-like
// weekly pace comparison, roughly Daniels Easy zone. Runs outside this band (harder
// workouts, races, warm-ups/cool-downs) intentionally have a different pace and would
// otherwise distort a week-over-week comparison.
const EASY_HR_PCT_MIN = 0.6;
const EASY_HR_PCT_MAX = 0.78;
const MIN_WELLNESS_POINTS_FOR_TREND = 4;
const MIN_TRAINING_DAYS_FOR_MONOTONY = 3;

// zoneTimes/intervalSplits (see below) each need one extra fetch() per activity, on
// top of the single /activities + /wellness calls the rest of this endpoint uses. Both
// are opt-in (query params `zoneTimes`/`intervalSplits`, default off) so the default
// weekly check (and the daily/monthly internal callers, recovery-note.js and email.js,
// which never pass these options) keeps its current cost. These caps are a second,
// independent safety net against a wide `days=` range blowing up the Worker's
// per-invocation subrequest budget even when a caller does opt in.
const MAX_ZONE_TIME_ACTIVITY_CALLS = 40;
const MAX_INTERVAL_SPLIT_ACTIVITY_CALLS = 20;
const MAX_WEATHER_ACTIVITY_CALLS = 40;

function addDays(dayIso, n) {
  return isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() + n * 86400000));
}

function paceSecPerKm(distanceM, timeSecs) {
  if (!(distanceM > 0) || !(timeSecs > 0)) return null;
  return (timeSecs / distanceM) * 1000;
}

function formatPace(secPerKm) {
  if (!Number.isFinite(secPerKm) || secPerKm <= 0) return null;
  const s = Math.round(secPerKm);
  const min = Math.floor(s / 60);
  const sec = s % 60;
  return `${min}:${String(sec).padStart(2, "0")}/km`;
}

// Field names for HR drift/decoupling and perceived exertion are best-effort: the
// intervals.icu API only populates them when the account/activity actually has that
// data, and naming isn't fully documented, hence the fallback chains (mirrors the
// `??` pattern already used for distance/time fields elsewhere in this codebase).
function buildRunRecord(a) {
  const distanceM = Number(a?.distance ?? a?.icu_distance ?? 0) || 0;
  const timeSecs = Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
  const avgHr = Number(a?.average_heartrate ?? a?.avg_hr ?? 0) || null;
  const maxHrActivity = Number(a?.max_heartrate ?? a?.max_hr ?? 0) || null;
  const secPerKm = paceSecPerKm(distanceM, timeSecs);
  const rpe = Number(a?.perceived_exertion ?? a?.icu_rpe ?? NaN);
  const feel = Number(a?.feel ?? NaN);
  return {
    date: activityDay(a),
    name: a?.name || a?.title || null,
    distanceKm: Math.round((distanceM / 1000) * 100) / 100,
    movingTimeMin: Math.round(timeSecs / 60),
    avgHr,
    maxHr: maxHrActivity,
    pace: formatPace(secPerKm),
    paceSecPerKm: secPerKm != null ? Math.round(secPerKm) : null,
    load: activityLoad(a) || null,
    hrDrift: a?.decoupling ?? a?.pace_decoupling ?? a?.hr_decoupling ?? null,
    perceivedExertion: Number.isFinite(rpe) ? rpe : null,
    feel: Number.isFinite(feel) ? feel : null,
    isRace: isRaceActivity(a),
    isInterval: isIntervalActivity(a),
    isTreadmill: isTreadmill(a),
    excludedFromVdot: isVdotExcluded(a),
    tags: Array.isArray(a?.tags) ? a.tags : [],
  };
}

// Fitness anchor points: races found in the analysis window, as a separate compact
// list rather than making callers filter runs[] for isRace themselves.
function buildRaceRecord(a) {
  const distanceM = Number(a?.distance ?? a?.icu_distance ?? 0) || 0;
  const timeSecs = Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
  const secPerKm = paceSecPerKm(distanceM, timeSecs);
  return {
    date: activityDay(a),
    name: a?.name || a?.title || null,
    distanceKm: Math.round((distanceM / 1000) * 100) / 100,
    timeSec: timeSecs || null,
    pace: formatPace(secPerKm),
    paceSecPerKm: secPerKm != null ? Math.round(secPerKm) : null,
    vdot: computeVdotFromRaceTime(distanceM, timeSecs),
  };
}

// ─── zoneTimes (Teil 2, opt-in) ────────────────────────────────────────────────
// The bulk /activities list response doesn't include a per-zone time breakdown, so
// this needs a GET /activity/{id} per run. The exact response field for zone times
// isn't confirmed against the live intervals.icu API docs (unreachable from this
// environment at implementation time), hence the fallback chain, mirroring the same
// best-effort pattern already used above for hrDrift/perceivedExertion. Any zone
// beyond Z5 (some accounts configure up to 7) is folded into Z5, since this endpoint
// only needs the Z1-Z2 (easy) vs. Z4-Z5 (hard) split for a polarization check.
function extractRawZoneTimes(detail) {
  return (
    detail?.icu_zone_times ??
    detail?.icu_hr_zone_times ??
    detail?.zone_times ??
    detail?.icu_hr_zones?.secs ??
    null
  );
}

function zoneIndexFromLabel(label) {
  const m = String(label ?? "").match(/(\d+)/);
  return m ? Number(m[1]) - 1 : null;
}

function normalizeZoneTimesToMinutes(raw) {
  if (!raw) return null;
  const secsByIndex = [];
  if (Array.isArray(raw)) {
    if (raw.length && typeof raw[0] === "object" && raw[0] !== null) {
      raw.forEach((entry, i) => {
        const idx = zoneIndexFromLabel(entry?.id ?? entry?.zone ?? entry?.name) ?? i;
        const secs = Number(entry?.secs ?? entry?.seconds ?? entry?.time ?? 0) || 0;
        secsByIndex[idx] = (secsByIndex[idx] || 0) + secs;
      });
    } else {
      raw.forEach((v, i) => {
        secsByIndex[i] = Number(v) || 0;
      });
    }
  } else if (typeof raw === "object") {
    for (const [key, value] of Object.entries(raw)) {
      const idx = zoneIndexFromLabel(key);
      if (idx == null) continue;
      secsByIndex[idx] = (secsByIndex[idx] || 0) + (Number(value) || 0);
    }
  }
  if (!secsByIndex.length) return null;

  const zoneSecs = [0, 0, 0, 0, 0];
  secsByIndex.forEach((secs, idx) => {
    zoneSecs[Math.min(idx, 4)] += secs || 0;
  });
  const toMin = (s) => Math.round((s / 60) * 10) / 10;
  return { z1: toMin(zoneSecs[0]), z2: toMin(zoneSecs[1]), z3: toMin(zoneSecs[2]), z4: toMin(zoneSecs[3]), z5: toMin(zoneSecs[4]) };
}

function sumZoneTimeMin(zoneTimeMinList) {
  const present = zoneTimeMinList.filter(Boolean);
  if (!present.length) return null;
  const sum = { z1: 0, z2: 0, z3: 0, z4: 0, z5: 0 };
  for (const z of present) {
    sum.z1 += z.z1 || 0;
    sum.z2 += z.z2 || 0;
    sum.z3 += z.z3 || 0;
    sum.z4 += z.z4 || 0;
    sum.z5 += z.z5 || 0;
  }
  const round1 = (v) => Math.round(v * 10) / 10;
  return { z1: round1(sum.z1), z2: round1(sum.z2), z3: round1(sum.z3), z4: round1(sum.z4), z5: round1(sum.z5) };
}

// Fetches zone times for the most recent `cap` runs in range (opt-in, see
// MAX_ZONE_TIME_ACTIVITY_CALLS above) and attaches `zoneTimeMin` to each run record
// in place. All runs get the key (null if not fetched/unavailable) so the shape is
// consistent whenever zoneTimes=true was requested.
async function enrichRunsWithZoneTimes(env, runRecords, cap) {
  const targets = runRecords.slice(-cap);
  await Promise.all(
    targets.map(async ({ activity, record }) => {
      const id = activity?.id ?? activity?.icu_activity_id;
      if (id == null) return;
      const detail = await fetchIntervalsActivityDetail(env, id).catch(() => null);
      record.zoneTimeMin = normalizeZoneTimesToMinutes(extractRawZoneTimes(detail));
    }),
  );
  for (const { record } of runRecords) {
    if (!("zoneTimeMin" in record)) record.zoneTimeMin = null;
  }
}

// ─── intervalSplits (Teil 3, opt-in) ───────────────────────────────────────────
// Same doc-access caveat as zoneTimes above: field names for the auto-detected
// interval/repeat structure are a best-effort guess, not confirmed against the live
// API. Work segments become reps; a recovery segment immediately following a work
// segment becomes that rep's restSec.
function isWorkSegment(seg) {
  const t = String(seg?.type ?? seg?.icu_interval_type ?? "").toLowerCase();
  return !t || t.includes("work") || t.includes("interval") || t.includes("active");
}

function isRestSegment(seg) {
  const t = String(seg?.type ?? seg?.icu_interval_type ?? "").toLowerCase();
  return t.includes("rest") || t.includes("recover");
}

function normalizeIntervalSplits(raw) {
  const list = Array.isArray(raw) ? raw : Array.isArray(raw?.icu_intervals) ? raw.icu_intervals : Array.isArray(raw?.intervals) ? raw.intervals : null;
  if (!list || !list.length) return null;

  const splits = [];
  let repNumber = 0;
  for (let i = 0; i < list.length; i++) {
    const seg = list[i];
    if (!isWorkSegment(seg)) continue;
    repNumber += 1;
    const distanceM = Number(seg?.distance ?? seg?.icu_distance ?? 0) || null;
    const timeSecs = Number(seg?.moving_time ?? seg?.elapsed_time ?? 0) || 0;
    const secPerKm = paceSecPerKm(distanceM, timeSecs);
    const avgHr = Number(seg?.average_heartrate ?? seg?.avg_hr ?? 0) || null;
    const next = list[i + 1];
    const restSec = next && isRestSegment(next) ? Math.round(Number(next?.moving_time ?? next?.elapsed_time ?? 0) || 0) || null : null;
    splits.push({
      repNumber,
      distanceM: distanceM != null ? Math.round(distanceM) : null,
      paceSecPerKm: secPerKm != null ? Math.round(secPerKm) : null,
      avgHr,
      restSec,
    });
  }
  return splits.length ? splits : null;
}

// Fetches interval splits for isInterval runs only (opt-in, capped at
// MAX_INTERVAL_SPLIT_ACTIVITY_CALLS most recent). `intervalSplits` is only attached to
// interval runs (per spec), not to every run, to avoid a wall of null keys.
async function enrichRunsWithIntervalSplits(env, runRecords, cap) {
  const targets = runRecords.filter(({ record }) => record.isInterval).slice(-cap);
  await Promise.all(
    targets.map(async ({ activity, record }) => {
      const id = activity?.id ?? activity?.icu_activity_id;
      if (id == null) return;
      const raw = await fetchIntervalsActivityIntervals(env, id).catch(() => null);
      record.intervalSplits = normalizeIntervalSplits(raw);
    }),
  );
  for (const { record } of runRecords) {
    if (record.isInterval && !("intervalSplits" in record)) record.intervalSplits = null;
  }
}

// ─── weather (opt-in) ──────────────────────────────────────────────────────────
// Per-run weather, so a downstream reader can tell "unusual HR-zone spread from
// heat" apart from "unusual HR-zone spread from bad pacing". See weather.js for the
// intervals.icu-native vs. Open-Meteo-fallback logic; treadmill runs are skipped
// without a request since they never have GPS. Same opt-in + cap contract as
// zoneTimes/intervalSplits above (one extra fetch per run, plus a possible
// Open-Meteo call), so the default daily/weekly callers keep their current cost.
//
// Each run's resolved weather is cached in KV by activity id (see
// readCachedActivityWeather/writeCachedActivityWeather in weather.js) since it never
// changes once found. This matters because a wide days= request combined with
// zoneTimes/intervalSplits can add up to more fetch() calls than a single Worker
// invocation's subrequest budget allows - rather than trying to fit everything in
// one call, already-resolved runs become free on every later call, so the history
// backfills itself over a few requests instead of needing to fit in one.
//
// The run's GPS start point is cached separately (readCachedActivityGps/
// writeCachedActivityGps), since a run whose GPS was found but whose Open-Meteo call
// was starved of budget last time would otherwise repeat the already-successful map
// fetch on every retry - skipping straight to the (cheaper, single) Open-Meteo call
// once GPS is known roughly doubles how many runs a wide history can backfill per call.
async function enrichRunsWithWeather(env, runRecords, cap) {
  const targets = runRecords.slice(-cap);
  await Promise.all(
    targets.map(async ({ activity, record }) => {
      if (record.isTreadmill) {
        record.weather = null;
        return;
      }
      const id = activity?.id ?? activity?.icu_activity_id;
      if (id == null) {
        record.weather = null;
        return;
      }
      const cachedWeather = await readCachedActivityWeather(env, id).catch(() => null);
      if (cachedWeather) {
        record.weather = cachedWeather;
        return;
      }

      const cachedGps = await readCachedActivityGps(env, id).catch(() => null);
      if (cachedGps?.none) {
        record.weather = null;
        return;
      }

      let mapData = null;
      if (!cachedGps) mapData = await fetchIntervalsActivityMap(env, id).catch(() => null);

      const { weather, latLng } = await resolveRunWeather(env, activity, {
        mapData,
        cachedLatLng: cachedGps ?? null,
      }).catch(() => ({ weather: null, latLng: cachedGps ?? null }));
      record.weather = weather;
      // Only cache the GPS finding off a fresh map fetch (mapData !== null) - a
      // cache miss combined with a still-null mapData means the fetch itself failed
      // (e.g. hit the subrequest ceiling), not a confirmed absence of GPS.
      if (!cachedGps && mapData !== null) await writeCachedActivityGps(env, id, latLng).catch(() => {});
      if (weather) await writeCachedActivityWeather(env, id, weather).catch(() => {});
    }),
  );
  for (const { record } of runRecords) {
    if (!("weather" in record)) record.weather = null;
  }
}

// ─── vdotHistory / easyPaceHistory (Teil 4) ────────────────────────────────────
// Weekly VDOT anchor: the better (lower, i.e. more conservative) of a race that week
// and the training-estimated VDOT for that week's 7-day window, same "min of the two"
// rule computeAndPersistRealVdot uses for the persisted current VDOT (src/vdot.js) -
// just re-applied per week instead of on a single rolling window.
function buildWeekVdot(bucket, activities, maxHr) {
  let raceVdot = null;
  for (const a of activities) {
    if (!isRun(a) || !isRaceActivity(a) || isVdotExcluded(a)) continue;
    const day = activityDay(a);
    if (day < bucket.start || day > bucket.end) continue;
    const dist = Number(a?.distance ?? a?.icu_distance ?? 0);
    const time = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
    if (dist < 800 || time < 60) continue;
    const v = computeVdotFromRaceTime(dist, time);
    if (v != null && (raceVdot == null || v > raceVdot)) raceVdot = v;
  }
  const trainVdot = maxHr ? estimateTrainingVdotForWindow(activities, bucket.start, bucket.end, maxHr) : null;
  if (raceVdot != null && trainVdot != null) return Math.min(trainVdot, raceVdot);
  return raceVdot ?? trainVdot ?? null;
}

// Field names for wellness metrics are best-effort per the intervals.icu API; entries
// simply come back null if the user doesn't maintain that field.
function buildWellnessRecord(w) {
  const restingHr = Number(w?.restingHR ?? w?.restingHr ?? w?.resting_hr ?? NaN);
  const hrv = Number(w?.hrv ?? w?.hrvSDNN ?? NaN);
  const sleepSecs = Number(w?.sleepSecs ?? w?.sleep_secs ?? NaN);
  const sleepScore = Number(w?.sleepScore ?? w?.sleep_quality ?? NaN);
  return {
    date: String(w?.id ?? w?.date ?? "").slice(0, 10),
    restingHr: Number.isFinite(restingHr) ? restingHr : null,
    hrv: Number.isFinite(hrv) ? hrv : null,
    sleepHours: Number.isFinite(sleepSecs) ? Math.round((sleepSecs / 3600) * 10) / 10 : null,
    sleepScore: Number.isFinite(sleepScore) ? sleepScore : null,
    ctl: Number.isFinite(Number(w?.ctl)) ? Number(w.ctl) : null,
    atl: Number.isFinite(Number(w?.atl)) ? Number(w.atl) : null,
  };
}

// Chronological 7-day buckets counted backward from `newest`, so the most recent
// bucket is always the last full/partial week ending today. Any remainder (when
// `days` isn't a multiple of 7) becomes an older, shorter bucket.
function buildWeekBuckets(newestIso, days) {
  const buckets = [];
  let end = newestIso;
  let remaining = days;
  while (remaining > 0) {
    const span = Math.min(7, remaining);
    const start = addDays(end, -(span - 1));
    buckets.unshift({ start, end });
    end = addDays(start, -1);
    remaining -= span;
  }
  return buckets;
}

function computeMonotonyStrain(dailyLoads) {
  const trainingDays = dailyLoads.filter((v) => v > 0).length;
  if (trainingDays < MIN_TRAINING_DAYS_FOR_MONOTONY) return { monotony: null, strain: null };
  const mean = dailyLoads.reduce((a, b) => a + b, 0) / dailyLoads.length;
  const variance = dailyLoads.reduce((a, b) => a + (b - mean) ** 2, 0) / dailyLoads.length;
  const stdev = Math.sqrt(variance);
  if (stdev <= 0) return { monotony: null, strain: null };
  const monotony = mean / stdev;
  const weeklyLoad = mean * dailyLoads.length;
  return { monotony: Math.round(monotony * 100) / 100, strain: Math.round(weeklyLoad * monotony) };
}

function buildWeekSummary(bucket, activities, maxHr) {
  const dayList = [];
  for (let d = bucket.start; d <= bucket.end; d = addDays(d, 1)) dayList.push(d);
  const dailyLoads = dayList.map(
    (day) => activities.filter((a) => activityDay(a) === day).reduce((sum, a) => sum + activityLoad(a), 0),
  );

  const runsInWeek = activities.filter((a) => isRun(a) && activityDay(a) >= bucket.start && activityDay(a) <= bucket.end);
  const distanceKm = runsInWeek.reduce((sum, a) => sum + (Number(a?.distance ?? a?.icu_distance ?? 0) || 0), 0) / 1000;
  const movingTimeMin =
    runsInWeek.reduce((sum, a) => sum + (Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0), 0) / 60;

  const easyRuns = runsInWeek.filter(
    (a) => !isRaceActivity(a) && !isTreadmill(a) && !isIntervalActivity(a) && !isVdotExcluded(a),
  );
  let easyDistanceM = 0;
  let easyTimeSecs = 0;
  if (maxHr > 0) {
    for (const a of easyRuns) {
      const avgHr = Number(a?.average_heartrate ?? a?.avg_hr ?? 0);
      if (!(avgHr > 0)) continue;
      const hrPct = avgHr / maxHr;
      if (hrPct < EASY_HR_PCT_MIN || hrPct > EASY_HR_PCT_MAX) continue;
      easyDistanceM += Number(a?.distance ?? a?.icu_distance ?? 0) || 0;
      easyTimeSecs += Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
    }
  }
  const easyZonePaceSecPerKm = paceSecPerKm(easyDistanceM, easyTimeSecs);

  const { monotony, strain } = computeMonotonyStrain(dailyLoads);

  return {
    weekStart: bucket.start,
    weekEnd: bucket.end,
    distanceKm: Math.round(distanceKm * 10) / 10,
    movingTimeMin: Math.round(movingTimeMin),
    runSessionCount: runsInWeek.length,
    easyZonePace: formatPace(easyZonePaceSecPerKm),
    easyZonePaceSecPerKm: easyZonePaceSecPerKm != null ? Math.round(easyZonePaceSecPerKm) : null,
    easyZoneSampleKm: Math.round((easyDistanceM / 1000) * 10) / 10,
    loadSum: Math.round(dailyLoads.reduce((a, b) => a + b, 0)),
    monotony,
    strain,
  };
}

// Simple least-squares slope over (dayOffset, value) points, plus a first-half vs.
// second-half average to give a more human-readable trend direction.
function computeTrend(points) {
  const valid = points.filter((p) => Number.isFinite(p.value));
  if (valid.length < MIN_WELLNESS_POINTS_FOR_TREND) return null;
  const n = valid.length;
  const xMean = valid.reduce((a, p) => a + p.dayOffset, 0) / n;
  const yMean = valid.reduce((a, p) => a + p.value, 0) / n;
  let num = 0;
  let den = 0;
  for (const p of valid) {
    num += (p.dayOffset - xMean) * (p.value - yMean);
    den += (p.dayOffset - xMean) ** 2;
  }
  const slopePerDay = den > 0 ? num / den : 0;
  const half = Math.max(1, Math.floor(n / 2));
  const firstHalfAvg = valid.slice(0, half).reduce((a, p) => a + p.value, 0) / half;
  const secondHalfAvg = valid.slice(n - half).reduce((a, p) => a + p.value, 0) / half;
  return {
    dataPoints: n,
    firstHalfAvg: Math.round(firstHalfAvg * 10) / 10,
    secondHalfAvg: Math.round(secondHalfAvg * 10) / 10,
    deltaAbs: Math.round((secondHalfAvg - firstHalfAvg) * 10) / 10,
    slopePerDay: Math.round(slopePerDay * 1000) / 1000,
  };
}

// --- Red-flag thresholds for assessRecoveryStatus (adjust freely) ---
const LOAD_SPIKE_INCREASE_THRESHOLD = 0.8; // last week vs. previous week distance increase (80%)
const RESTING_HR_SLOPE_THRESHOLD = 0.15; // bpm/day rise over the analysis window
const SLEEP_DEBT_MIN_NIGHTS = 3; // minimum nights of data in the last 7 to judge sleep debt
const SLEEP_DEBT_AVG_HOURS_THRESHOLD = 6.5;
const SLEEP_DEBT_SHORT_NIGHT_HOURS = 5;
const SLEEP_DEBT_SHORT_NIGHT_COUNT = 2;
const HRV_DROP_DELTA_ABS_THRESHOLD = -3; // trends.hrv.deltaAbs
const HRV_DROP_SINGLE_DAY_PCT = 0.3; // single day >30% below the 28-day average
const ACUTE_OVERLOAD_RATIO_THRESHOLD = 1.3; // ATL/CTL

// Ordered so the array key order also fixes the canonical order flags are reported
// in (summary/response), independent of the order the detectors below run in.
const FLAG_INFO = {
  sleep_debt: {
    label: "Schlafdefizit",
    category: "recovery",
    recommendation: "Schlafdauer priorisieren (mind. 7h), bis sich Ruhepuls/HRV wieder normalisieren.",
  },
  resting_hr_rising: {
    label: "steigender Ruhepuls",
    category: "recovery",
    recommendation: "Ruhepuls weiter beobachten und bei anhaltendem Trend 1-2 lockere Tage einplanen.",
  },
  hrv_drop: {
    label: "gesunkene HRV",
    category: "recovery",
    recommendation: "Intensive Einheiten pausieren, bis sich die HRV wieder erholt.",
  },
  acute_overload: {
    label: "akute Überlastung (ATL/CTL-Ratio erhöht)",
    category: "load",
    recommendation: "Trainingsbelastung kurzfristig reduzieren, damit sich akute und chronische Last wieder annähern.",
  },
  load_spike: {
    label: "sprunghaft steigender Trainingsumfang",
    category: "load",
    recommendation: "Wochenumfang wieder schrittweise (max. +10-15%/Woche) statt sprunghaft steigern.",
  },
};

function lastNDaysIso(newest, n) {
  const list = [];
  for (let i = n - 1; i >= 0; i--) list.push(addDays(newest, -i));
  return list;
}

// "Jo-Jo": last week jumps back up right after the previous week itself was a decline.
function detectLoadSpike(weeks) {
  if (weeks.length < 3) return false;
  const last = weeks[weeks.length - 1];
  const prev = weeks[weeks.length - 2];
  const beforePrev = weeks[weeks.length - 3];
  if (!(prev.distanceKm > 0) || !(beforePrev.distanceKm > 0)) return false;
  const increase = (last.distanceKm - prev.distanceKm) / prev.distanceKm;
  const prevWasDecline = prev.distanceKm < beforePrev.distanceKm;
  return increase > LOAD_SPIKE_INCREASE_THRESHOLD && prevWasDecline;
}

function detectRestingHrRising(trends) {
  const slope = trends.restingHr?.slopePerDay;
  return Number.isFinite(slope) && slope > RESTING_HR_SLOPE_THRESHOLD;
}

function detectSleepDebt(wellnessByDay, newest) {
  const nights = lastNDaysIso(newest, 7)
    .map((d) => wellnessByDay.get(d)?.sleepHours)
    .filter((v) => Number.isFinite(v));
  if (nights.length < SLEEP_DEBT_MIN_NIGHTS) return false;
  const avg = nights.reduce((a, b) => a + b, 0) / nights.length;
  const shortNights = nights.filter((h) => h < SLEEP_DEBT_SHORT_NIGHT_HOURS).length;
  return avg < SLEEP_DEBT_AVG_HOURS_THRESHOLD || shortNights >= SLEEP_DEBT_SHORT_NIGHT_COUNT;
}

function detectHrvDrop(wellnessByDay, newest, days, trends) {
  const deltaAbs = trends.hrv?.deltaAbs;
  if (Number.isFinite(deltaAbs) && deltaAbs < HRV_DROP_DELTA_ABS_THRESHOLD) return true;

  const windowHrv = lastNDaysIso(newest, days)
    .map((d) => wellnessByDay.get(d)?.hrv)
    .filter((v) => Number.isFinite(v));
  if (windowHrv.length < MIN_WELLNESS_POINTS_FOR_TREND) return false;
  const avg = windowHrv.reduce((a, b) => a + b, 0) / windowHrv.length;

  const last7Hrv = lastNDaysIso(newest, 7)
    .map((d) => wellnessByDay.get(d)?.hrv)
    .filter((v) => Number.isFinite(v));
  return last7Hrv.some((v) => v < avg * (1 - HRV_DROP_SINGLE_DAY_PCT));
}

function detectAcuteOverload(wellnessByDay, newest, days) {
  const allDays = lastNDaysIso(newest, days);
  for (let i = allDays.length - 1; i >= 0; i--) {
    const w = wellnessByDay.get(allDays[i]);
    if (Number.isFinite(w?.atl) && Number.isFinite(w?.ctl) && w.ctl > 0) {
      return w.atl / w.ctl > ACUTE_OVERLOAD_RATIO_THRESHOLD;
    }
  }
  return false;
}

function buildAssessmentText(flags) {
  if (flags.length === 0) {
    return {
      summary: "Form und Erholung sehen unauffällig aus, keine Red Flags in den letzten Wochen.",
      recommendation: "Aktuellen Trainingsplan wie geplant fortsetzen.",
    };
  }

  const recoveryFlags = flags.filter((f) => FLAG_INFO[f].category === "recovery");
  const loadFlags = flags.filter((f) => FLAG_INFO[f].category === "load");
  const verb = flags.length === 1 ? "deutet" : "deuten";
  let cause;
  if (recoveryFlags.length > 0 && loadFlags.length === 0) {
    cause = `${verb} auf unzureichende Erholung hin, nicht auf zu hohes Trainingsvolumen`;
  } else if (loadFlags.length > 0 && recoveryFlags.length === 0) {
    cause = `${verb} auf ein zu schnell gesteigertes bzw. zu hohes Trainingsvolumen hin`;
  } else {
    cause = `${verb} auf ein Zusammenspiel aus hoher Trainingslast und unzureichender Erholung hin`;
  }

  const summary = `${flags.map((f) => FLAG_INFO[f].label).join(" + ")} ${cause}.`;
  const recommendation = [...new Set(flags.map((f) => FLAG_INFO[f].recommendation))].join(" ");
  return { summary, recommendation };
}

// Turns the raw weeks/wellness/trends data into the red-flag checklist from the
// manual analysis this endpoint (and the daily recovery note, see recovery-note.js)
// are meant to replace. Two or more flags together are already treated as "rot" below,
// so a flagged sleep_debt + resting_hr_rising combination hits that bar automatically
// without needing a separate rule for it.
export function assessRecoveryStatus(weeks, wellnessByDay, trends, newest, days) {
  const detected = [];
  if (detectLoadSpike(weeks)) detected.push("load_spike");
  if (detectRestingHrRising(trends)) detected.push("resting_hr_rising");
  if (detectSleepDebt(wellnessByDay, newest)) detected.push("sleep_debt");
  if (detectHrvDrop(wellnessByDay, newest, days, trends)) detected.push("hrv_drop");
  if (detectAcuteOverload(wellnessByDay, newest, days)) detected.push("acute_overload");

  const flags = Object.keys(FLAG_INFO).filter((f) => detected.includes(f));
  const status = flags.length === 0 ? "grün" : flags.length === 1 ? "gelb" : "rot";
  const { summary, recommendation } = buildAssessmentText(flags);

  return { status, flags, summary, recommendation };
}

function wellnessTrends(wellnessByDay, oldest, days) {
  const restingHrPoints = [];
  const hrvPoints = [];
  const sleepPoints = [];
  for (let i = 0; i < days; i++) {
    const day = addDays(oldest, i);
    const w = wellnessByDay.get(day);
    restingHrPoints.push({ dayOffset: i, value: w?.restingHr ?? null });
    hrvPoints.push({ dayOffset: i, value: w?.hrv ?? null });
    sleepPoints.push({ dayOffset: i, value: w?.sleepHours ?? null });
  }
  return {
    restingHr: computeTrend(restingHrPoints),
    hrv: computeTrend(hrvPoints),
    sleepHours: computeTrend(sleepPoints),
  };
}

// Builds a structured JSON snapshot of the last `days` days (running activities +
// wellness) for a downstream causal analysis (overtraining vs. infection vs. pacing
// issue) done by a human/LLM reading the response — this endpoint only aggregates and
// exposes the raw numbers, it doesn't diagnose.
export async function buildRecentFormAnalysis(env, todayIso, options = {}) {
  const { days = 28, includeZoneTimes = false, includeIntervalSplits = false, includeWeather = false } = options;
  const newest = todayIso;
  const oldest = addDays(newest, -(days - 1));

  const activities = await fetchIntervalsActivities(env, oldest, newest);
  const wellnessRaw = await fetchIntervalsWellnessRange(env, oldest, newest);
  // resolveMaxHr does its own live-fetch + 24h KV cache internally, so this stays
  // accurate (the athlete's actual configured max HR, not just a heuristic) without
  // this read-only endpoint needing to opt into anything.
  const maxHr = await resolveMaxHr(env, activities);

  const runRecords = activities
    .filter(isRun)
    .map((activity) => ({ activity, record: buildRunRecord(activity) }))
    .sort((a, b) => String(a.record.date).localeCompare(String(b.record.date)));

  if (includeZoneTimes) await enrichRunsWithZoneTimes(env, runRecords, MAX_ZONE_TIME_ACTIVITY_CALLS);
  if (includeIntervalSplits) await enrichRunsWithIntervalSplits(env, runRecords, MAX_INTERVAL_SPLIT_ACTIVITY_CALLS);
  if (includeWeather) await enrichRunsWithWeather(env, runRecords, MAX_WEATHER_ACTIVITY_CALLS);
  const runs = runRecords.map((r) => r.record);

  const races = activities
    .filter((a) => isRun(a) && isRaceActivity(a))
    .map(buildRaceRecord)
    .sort((a, b) => String(a.date).localeCompare(String(b.date)));

  const wellnessDaily = wellnessRaw.map(buildWellnessRecord).filter((w) => w.date);
  const wellnessByDay = new Map(wellnessDaily.map((w) => [w.date, w]));

  const buckets = buildWeekBuckets(newest, days);
  const weeks = buckets.map((bucket) => buildWeekSummary(bucket, activities, maxHr));

  if (includeZoneTimes) {
    for (let i = 0; i < weeks.length; i++) {
      const bucket = buckets[i];
      const zoneTimesInWeek = runs.filter((r) => r.date >= bucket.start && r.date <= bucket.end).map((r) => r.zoneTimeMin);
      weeks[i].zoneTimeMinWeek = sumZoneTimeMin(zoneTimesInWeek);
    }
  }

  const vdotHistory = buckets.map((bucket) => ({ date: bucket.end, vdot: buildWeekVdot(bucket, activities, maxHr) }));
  const easyPaceHistory = weeks.map((w) => ({ weekStart: w.weekStart, paceSecPerKm: w.easyZonePaceSecPerKm }));

  const trends = wellnessTrends(wellnessByDay, oldest, days);
  const assessment = assessRecoveryStatus(weeks, wellnessByDay, trends, newest, days);

  const notes = [];
  if (!(maxHr > 0)) notes.push("Keine MaxHF ermittelbar – Ø-Pace pro HF-Zone konnte nicht berechnet werden.");
  if (wellnessDaily.length < days / 2) {
    notes.push(`Nur an ${wellnessDaily.length}/${days} Tagen Wellness-Daten gepflegt – Trends sind entsprechend unsicher.`);
  }
  if (weeks.every((w) => w.monotony == null)) {
    notes.push("Zu wenige Trainingstage pro Woche für eine Trainingsmonotonie-Berechnung.");
  }

  return {
    ok: true,
    athleteId: env?.ATHLETE_ID ?? null,
    range: { oldest, newest, days },
    maxHr: maxHr || null,
    weeks,
    trends,
    assessment,
    runs,
    races,
    vdotHistory,
    easyPaceHistory,
    wellnessDaily,
    dataQuality: {
      runCount: runs.length,
      wellnessDayCount: wellnessDaily.length,
      hasMaxHr: maxHr > 0,
      notes,
    },
  };
}
