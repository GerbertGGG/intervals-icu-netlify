import { isoDate, daysBetween } from "./date-utils.js";
import { isRun, isRaceActivity, isIntervalActivity, isVdotExcluded } from "./activity-utils.js";
import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";
import { loadCachedMaxHr, fetchAndCacheMaxHr, fetchRunPaceBenchmarks } from "./intervals-client.js";

const REAL_VDOT_KV_PREFIX = "vdot:real:";
const PACE_BENCH_KV_PREFIX = "vdot:pacebench:";
const CORRECTION_KV_PREFIX = "vdot:correction:";
const PACE_BENCH_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const CORRECTION_MIN_FACTOR = 0.9;
const CORRECTION_MAX_FACTOR = 1.1;
// A race-derived correction factor speaks to the athlete's HR/pace relationship "as of
// that race day". With only ~2 races/year, an unbounded factor would otherwise keep
// steering training-based estimates for months after it stopped being representative
// (fitness, season/heat, HR drift all move on). Past CORRECTION_DECAY_START_DAYS it's
// pulled linearly back toward 1 (no correction), fully neutral by CORRECTION_DECAY_END_DAYS.
const CORRECTION_DECAY_START_DAYS = 90;
const CORRECTION_DECAY_END_DAYS = 180;
// Flags a race whose rawFactor (already clamped to CORRECTION_MIN/MAX_FACTOR) sits this
// far from 1 - i.e. close to the clamp - as possibly unrepresentative (bad weather,
// pacing, illness), for visibility only; the race is still processed normally.
const CORRECTION_WARNING_THRESHOLD = 0.05;
// Pace benchmarks come from ordinary training runs (no race-day pacing, possibly a lucky
// segment inside an interval session), so they're a weaker signal than an actual race.
// Each update only nudges the persisted driftFactor by this much (vs. the race path's
// 50/50 blend), which is what makes it safe to run continuously instead of just at the
// ~2 races/year the race-based factor sees.
const PACE_BENCH_DRIFT_BLEND_WEIGHT = 0.05;

// Pulls a persisted correction factor linearly toward 1 the longer it's been since the
// race it was derived from. Returns the factor unchanged before the decay window starts.
function applyCorrectionDecay(factor, daysSinceLastRace) {
  if (!Number.isFinite(factor)) return 1;
  if (!Number.isFinite(daysSinceLastRace) || daysSinceLastRace <= CORRECTION_DECAY_START_DAYS) return factor;
  if (daysSinceLastRace >= CORRECTION_DECAY_END_DAYS) return 1;
  const progress = (daysSinceLastRace - CORRECTION_DECAY_START_DAYS) / (CORRECTION_DECAY_END_DAYS - CORRECTION_DECAY_START_DAYS);
  return factor + (1 - factor) * progress;
}

function realVdotKvKey(env) {
  return `${REAL_VDOT_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}
function paceBenchKvKey(env) {
  return `${PACE_BENCH_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}
function correctionKvKey(env) {
  return `${CORRECTION_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}
function clamp(v, min, max) {
  return Math.min(max, Math.max(min, v));
}

function isTreadmill(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t === "virtualrun" || t.includes("treadmill");
}

// ─── Jack Daniels VDOT formula ────────────────────────────────────────────────
// v = m/min, t = minutes
// VO2 = -4.60 + 0.182258·v + 0.000104·v²
// %VO2max = 0.8 + 0.1894393·e^(-0.012778·t) + 0.2989558·e^(-0.1932605·t)
// VDOT = VO2 / %VO2max
export function computeVdotFromRaceTime(distanceMeters, timeSecs) {
  const dist = Number(distanceMeters);
  const secs = Number(timeSecs);
  if (!Number.isFinite(dist) || dist < 400) return null;
  if (!Number.isFinite(secs) || secs < 60) return null;

  const v = (dist / secs) * 60;
  const t = secs / 60;

  const vo2 = -4.6 + 0.182258 * v + 0.000104 * v * v;
  const pctVo2max = 0.8 + 0.1894393 * Math.exp(-0.012778 * t) + 0.2989558 * Math.exp(-0.1932605 * t);

  if (pctVo2max <= 0) return null;
  const vdot = vo2 / pctVo2max;
  if (!Number.isFinite(vdot) || vdot < 20 || vdot > 90) return null;
  return Math.round(vdot * 10) / 10;
}

function computeRaceVdot(activities, todayIso = null) {
  if (!Array.isArray(activities)) return null;
  const anchor = todayIso || isoDate(new Date());
  const cutoff = isoDate(new Date(new Date(anchor).getTime() - 180 * 86400000));
  let best = null;
  for (const a of activities) {
    if (!isRun(a)) continue;
    if (!isRaceActivity(a)) continue;
    if (isVdotExcluded(a)) continue;
    const day = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (day < cutoff || day > anchor) continue;
    const dist = Number(a?.distance ?? a?.icu_distance ?? 0);
    const time = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
    if (dist < 800 || time < 60) continue;
    const v = computeVdotFromRaceTime(dist, time);
    if (v != null && (best == null || v > best.vdot)) {
      best = { vdot: v, raceDate: day };
    }
  }
  return best;
}

async function loadCorrectionState(env) {
  if (!hasKv(env)) return null;
  try {
    return await readKvJson(env, correctionKvKey(env));
  } catch {
    return null;
  }
}

async function saveCorrectionState(env, state) {
  if (!hasKv(env)) return;
  try {
    await writeKvJson(env, correctionKvKey(env), state);
  } catch {}
}

// Combines the race-derived factor (state.factor, decayed toward 1 via applyCorrectionDecay
// the longer it's been since state.lastRaceDate) with the pace-benchmark drift factor
// (state.driftFactor) into a single correction factor. Both inputs are already clamped to
// [CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR] individually when persisted; the product is
// re-clamped to the same range so the two can't compound into a larger overall deviation
// than either path is allowed on its own.
function combineCorrectionFactors(state, anchorIso) {
  const rawRaceFactor = Number.isFinite(state?.factor) ? state.factor : 1;
  const daysSinceLastRace = state?.lastRaceDate ? daysBetween(state.lastRaceDate, anchorIso) : null;
  const raceFactor = state?.lastRaceDate ? applyCorrectionDecay(rawRaceFactor, daysSinceLastRace) : rawRaceFactor;
  const driftFactor = Number.isFinite(state?.driftFactor) ? state.driftFactor : 1;
  const combined = clamp(raceFactor * driftFactor, CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR);
  return { raceFactor, driftFactor, combined, daysSinceLastRace };
}

// Reads the persisted correction factor (1 = no correction) as the product of the race-
// derived factor and the pace-benchmark drift factor (see combineCorrectionFactors). The
// persisted state itself is never modified here - only the value returned to callers.
// asOfIso anchors the race decay to the day being processed (so backfills of past days decay
// relative to that day, not wall-clock "now"); defaults to today for live reads.
export async function getRaceCorrectionFactor(env, asOfIso = null) {
  const state = await loadCorrectionState(env).catch(() => null);
  const anchor = asOfIso || isoDate(new Date());
  return combineCorrectionFactors(state, anchor).combined;
}

// When a new race appears, compares its VDOT against the training VDOT predicted from
// the 14 days right before the race (i.e. "what we expected") and blends the resulting
// ratio (clamped to ±10%) into the persisted correction factor via a 50/50 EMA, so a
// single fluke race can't whiplash future estimates. Each race is only processed once
// (tracked via lastRaceDate) so repeated syncs don't reinforce the same data point.
async function updateRaceCorrectionFactor(env, activities, raceResult, maxHr) {
  if (!raceResult?.vdot || !raceResult?.raceDate) return null;
  const state = await loadCorrectionState(env).catch(() => null);
  if (state?.lastRaceDate === raceResult.raceDate) return state;

  const fromIso = isoDate(new Date(new Date(raceResult.raceDate).getTime() - 14 * 86400000));
  const toIso = isoDate(new Date(new Date(raceResult.raceDate).getTime() - 86400000));
  const predictedVdot = maxHr ? estimateTrainingVdotForWindow(activities, fromIso, toIso, maxHr) : null;

  const prevFactor = Number.isFinite(state?.factor) ? state.factor : 1;
  if (!predictedVdot) {
    // No usable pre-race training data to compare against; mark the race as seen so we
    // don't keep retrying it, but leave the factor untouched.
    const newState = {
      ...state,
      factor: prevFactor,
      lastRaceDate: raceResult.raceDate,
      raceCount: state?.raceCount ?? 0,
      raceCorrectionWarning: null,
    };
    await saveCorrectionState(env, newState).catch(() => {});
    return newState;
  }

  const rawFactor = clamp(raceResult.vdot / predictedVdot, CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR);
  const blended = clamp(prevFactor * 0.5 + rawFactor * 0.5, CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR);
  // Visibility only: a rawFactor already this close to the ±10% clamp suggests the race
  // may not have been representative (weather, pacing, illness). Whether to treat it as an
  // outlier stays a manual call - this never rejects or reweights the race automatically.
  const raceCorrectionWarning =
    Math.abs(rawFactor - 1) > CORRECTION_WARNING_THRESHOLD
      ? { largeDeviation: true, predictedVdot, raceVdot: raceResult.vdot, rawFactor: Math.round(rawFactor * 1000) / 1000 }
      : null;
  const newState = {
    ...state,
    factor: Math.round(blended * 1000) / 1000,
    lastRawFactor: Math.round(rawFactor * 1000) / 1000,
    lastRaceDate: raceResult.raceDate,
    raceCount: (state?.raceCount ?? 0) + 1,
    raceCorrectionWarning,
  };
  await saveCorrectionState(env, newState).catch(() => {});
  return newState;
}

// Analog to updateRaceCorrectionFactor, but a separate, weakly-weighted path (see
// PACE_BENCH_DRIFT_BLEND_WEIGHT) that runs continuously instead of only ~2x/year. Only
// blends when both paceBenchVdot and an HR-based hrTrainVdot are available, and only once
// per distinct pace-bench snapshot (tracked via state.driftBenchTs, the fetch timestamp
// from loadCachedPaceBench) - otherwise the same cached benchmark, still within its 7-day
// cache window, would get reprocessed on every sync tick instead of roughly once a week.
async function updatePaceBenchmarkDrift(env, bench, benchTs, hrTrainVdot) {
  const paceBenchVdot = bench ? computeVdotFromPaceBenchmarks(bench) : null;
  if (paceBenchVdot == null || !Number.isFinite(hrTrainVdot) || !benchTs) {
    return { paceBenchVdot, state: null };
  }

  const state = await loadCorrectionState(env).catch(() => null);
  if (state?.driftBenchTs === benchTs) return { paceBenchVdot, state };

  const prevDriftFactor = Number.isFinite(state?.driftFactor) ? state.driftFactor : 1;
  const rawDriftFactor = clamp(paceBenchVdot / hrTrainVdot, CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR);
  const blended = clamp(
    prevDriftFactor * (1 - PACE_BENCH_DRIFT_BLEND_WEIGHT) + rawDriftFactor * PACE_BENCH_DRIFT_BLEND_WEIGHT,
    CORRECTION_MIN_FACTOR,
    CORRECTION_MAX_FACTOR,
  );
  const newState = {
    ...state,
    driftFactor: Math.round(blended * 1000) / 1000,
    lastDriftRawFactor: Math.round(rawDriftFactor * 1000) / 1000,
    driftUpdateCount: (state?.driftUpdateCount ?? 0) + 1,
    driftBenchTs: benchTs,
  };
  await saveCorrectionState(env, newState).catch(() => {});
  return { paceBenchVdot, state: newState };
}

function computeVdotFromPaceBenchmarks(runPace) {
  if (!runPace?.current) return null;
  const dists = [5000, 10000, 21097, 1000];
  let best = null;
  for (const dist of dists) {
    const secs = runPace.current[dist];
    if (!Number.isFinite(secs) || secs <= 0) continue;
    const v = computeVdotFromRaceTime(dist, secs);
    if (v != null && (best == null || v > best)) best = v;
  }
  return best;
}

// ─── Training VDOT from HR + Pace (Daniels zone calibration) ─────────────────
// %VO2max ≈ 1.154 × %HRmax − 0.15  (derived from Daniels E/M/T zone anchors)
function _estimateMaxHrFromActivities(activities) {
  let highest = 0;
  for (const a of activities || []) {
    const hr = Number(a?.max_heartrate || a?.max_hr || 0);
    if (hr > highest) highest = hr;
  }
  return highest > 100 ? Math.round(highest * 1.05) : null;
}

// Same calculation/thresholds as _vdotFromTrainingActivity, but returns the
// intermediate values (and, if excluded, why) instead of just the final number.
// Used both by _vdotFromTrainingActivity itself and by the vdotDebug.todayActivityRaw
// collection in computeAndPersistRealVdot, so the two never drift apart.
function _vdotDetailsFromTrainingActivity(activity, maxHr) {
  const dist = Number(activity?.distance ?? activity?.icu_distance ?? 0);
  const time = Number(activity?.moving_time ?? activity?.elapsed_time ?? 0);
  const avgHr = Number(activity?.average_heartrate ?? activity?.avg_hr ?? 0);
  const details = {
    avgHr,
    distance: dist,
    movingTime: time,
    hrPct: null,
    pctVo2max: null,
    vdot: null,
    excluded: false,
    exclusionReason: null,
  };
  if (dist < 2000 || time < 600 || avgHr <= 0 || maxHr <= 100) {
    details.excluded = true;
    details.exclusionReason =
      dist < 2000 ? "distance<2000m" : time < 600 ? "movingTime<600s" : avgHr <= 0 ? "avgHr<=0" : "maxHr<=100";
    return details;
  }
  const hrPct = avgHr / maxHr;
  details.hrPct = hrPct;
  if (hrPct < 0.55 || hrPct > 0.87) {
    details.excluded = true;
    details.exclusionReason = hrPct < 0.55 ? "hrPct<0.55" : "hrPct>0.87";
    return details;
  }
  const pctVo2max = 1.154 * hrPct - 0.15;
  details.pctVo2max = pctVo2max;
  if (pctVo2max <= 0.3 || pctVo2max >= 1.0) {
    details.excluded = true;
    details.exclusionReason = pctVo2max <= 0.3 ? "pctVo2max<=0.3" : "pctVo2max>=1.0";
    return details;
  }
  const v = (dist / time) * 60;
  const vo2 = -4.6 + 0.182258 * v + 0.000104 * v * v;
  if (vo2 <= 0) {
    details.excluded = true;
    details.exclusionReason = "vo2<=0";
    return details;
  }
  const vdot = vo2 / pctVo2max;
  if (!Number.isFinite(vdot) || vdot < 20 || vdot > 90) {
    details.excluded = true;
    details.exclusionReason = "vdot out of range [20,90]";
    return details;
  }
  details.vdot = Math.round(vdot * 10) / 10;
  return details;
}

function _vdotFromTrainingActivity(activity, maxHr) {
  const details = _vdotDetailsFromTrainingActivity(activity, maxHr);
  return details.excluded ? null : details.vdot;
}

function medianOf(values) {
  if (!values.length) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 === 0 ? (sorted[mid - 1] + sorted[mid]) / 2 : sorted[mid];
}

// Median training VDOT estimate from non-race, non-interval runs within [fromIso, toIso]
// (inclusive). Interval sessions (tagged "#intervalle"/"interval:*") are excluded because
// their built-in recovery jogs/walks dilute the whole-activity average pace and HR that
// _vdotFromTrainingActivity relies on, producing an artificially low VDOT. Activities
// manually tagged "#novdot" are excluded too (see isVdotExcluded).
// Requires at least `minCount` qualifying runs: a median of a single run is just that
// run, so one noisy data point (short recovery jog, GPS drift, atypical HR/pace ratio)
// could otherwise swing the whole-window estimate on its own, e.g. in a short 7-day
// window where most runs got excluded. Callers already treat a null result as "not
// enough data" (weekly report falls back to "–", computeAndPersistRealVdot falls back
// to pace benchmarks or the last persisted value), so returning null here is safe.
export function estimateTrainingVdotForWindow(activities, fromIso, toIso, maxHr, minCount = 2) {
  if (!Array.isArray(activities) || !(maxHr > 100)) return null;
  const estimates = [];
  for (const a of activities) {
    if (!isRun(a) || isRaceActivity(a) || isTreadmill(a) || isIntervalActivity(a) || isVdotExcluded(a)) continue;
    const day = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (day < fromIso || day > toIso) continue;
    const v = _vdotFromTrainingActivity(a, maxHr);
    if (v != null) estimates.push(v);
  }
  if (estimates.length < minCount) return null;
  const m = medianOf(estimates);
  return m != null ? Math.round(m * 10) / 10 : null;
}

function computeTrainingVdotFromActivities(activities, todayIso, maxHr) {
  const anchor = todayIso || isoDate(new Date());
  const cutoff = isoDate(new Date(new Date(anchor).getTime() - 28 * 86400000));
  return estimateTrainingVdotForWindow(activities, cutoff, anchor, maxHr);
}

async function loadRealVdotState(env) {
  if (!hasKv(env)) return null;
  try {
    return await readKvJson(env, realVdotKvKey(env));
  } catch {
    return null;
  }
}

// Reads the persisted "real" VDOT (latest known overall fitness, not a per-window
// estimate) so callers like the weekly report can show it without recomputing.
export async function getCurrentRealVdot(env) {
  const state = await loadRealVdotState(env).catch(() => null);
  const vdot = Number(state?.vdot);
  return Number.isFinite(vdot) && vdot > 0 ? vdot : null;
}

// Inverts the VDOT VO2 formula to get velocity (m/min) for a given VO2 (ml/kg/min).
function velocityFromVo2(vo2) {
  const a = 0.000104;
  const b = 0.182258;
  const c = -(4.6 + vo2);
  const disc = b * b - 4 * a * c;
  if (disc < 0) return null;
  const v = (-b + Math.sqrt(disc)) / (2 * a);
  return v > 0 ? v : null;
}

function formatPacePerKm(velocityMPerMin) {
  if (!Number.isFinite(velocityMPerMin) || velocityMPerMin <= 0) return null;
  const secPerKm = Math.round(60000 / velocityMPerMin);
  const min = Math.floor(secPerKm / 60);
  const sec = secPerKm % 60;
  return `${min}:${String(sec).padStart(2, "0")}/km`;
}

// Jack Daniels training pace zones, derived as the velocity at a fixed %VDOT
// (VDOT approximates VO2max, so vo2_target = pct * vdot).
const PACE_ZONES = [
  { key: "easy", label: "Easy (E)", pct: 0.7 },
  { key: "marathon", label: "Marathon (M)", pct: 0.84 },
  { key: "threshold", label: "Threshold (T)", pct: 0.88 },
  { key: "interval", label: "Interval (I)", pct: 0.975 },
  { key: "repetition", label: "Repetition (R)", pct: 1.05 },
];

// Returns [{ key, label, pace }] pace targets per km for the given VDOT, or null.
export function paceTargetsFromVdot(vdot) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  return PACE_ZONES.map((zone) => ({
    key: zone.key,
    label: zone.label,
    pace: formatPacePerKm(velocityFromVo2(zone.pct * v)),
  }));
}

const RACE_DISTANCES = [
  { key: "5k", meters: 5000, label: "5 km" },
  { key: "10k", meters: 10000, label: "10 km" },
  { key: "hm", meters: 21097, label: "Halbmarathon" },
  { key: "m", meters: 42195, label: "Marathon" },
];

function secondsToRaceTimeString(totalSeconds) {
  if (!Number.isFinite(totalSeconds)) return null;
  const s = Math.round(totalSeconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}` : `${m}:${String(sec).padStart(2, "0")}`;
}

// %VO2max depends on race duration, so predicting a race time from VDOT needs a
// fixed-point iteration: guess t -> get %VO2max(t) -> get required velocity -> get a
// better t = distance/velocity, repeat until it converges (a handful of iterations).
function predictRaceTimeSeconds(vdot, distanceMeters) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  let t = (distanceMeters / 1000) * 4; // initial guess: ~4 min/km
  for (let i = 0; i < 12; i++) {
    const pctVo2max = 0.8 + 0.1894393 * Math.exp(-0.012778 * t) + 0.2989558 * Math.exp(-0.1932605 * t);
    const vo2 = v * pctVo2max;
    const velocity = velocityFromVo2(vo2);
    if (!velocity) return null;
    t = distanceMeters / velocity;
  }
  return Number.isFinite(t) ? Math.round(t * 60) : null;
}

// Returns [{ key, label, meters, seconds, time }] predicted race times for the
// standard distances (5k/10k/HM/M) at the given VDOT, or null.
export function predictRaceTimesFromVdot(vdot) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  return RACE_DISTANCES.map((d) => {
    const secs = predictRaceTimeSeconds(v, d.meters);
    return { key: d.key, label: d.label, meters: d.meters, seconds: secs, time: secondsToRaceTimeString(secs) };
  });
}

async function saveRealVdotState(env, state) {
  if (!hasKv(env)) return;
  try {
    await writeKvJson(env, realVdotKvKey(env), state);
  } catch {}
}

// Returns { data, ts } (ts = when this snapshot was fetched, used by updatePaceBenchmarkDrift
// to dedupe against reprocessing the same snapshot), or null if there's no fresh cache entry.
async function loadCachedPaceBench(env) {
  if (!hasKv(env)) return null;
  try {
    const cached = await readKvJson(env, paceBenchKvKey(env));
    if (!cached?.ts) return null;
    if (Date.now() - cached.ts > PACE_BENCH_MAX_AGE_MS) return null;
    return { data: cached.data || null, ts: cached.ts };
  } catch {
    return null;
  }
}

async function saveCachedPaceBench(env, data) {
  if (!hasKv(env)) return;
  try {
    await writeKvJson(env, paceBenchKvKey(env), { ts: Date.now(), data });
  } catch {}
}

// Resolves max HR via (in order): env override, KV cache, live API fetch (a single
// GET, cached for 24h - see loadCachedMaxHr/saveCachedMaxHr in intervals-client.js -
// so this only hits the API once a day even for read-only callers), highest observed
// max_heartrate, or a heuristic from the highest average HR. Used to only attempt
// the live fetch in write mode, to avoid a per-request intervals.icu call, but the
// 24h cache already bounds that cost, and skipping it meant read-only callers (e.g.
// /api/analysis/recent-form) silently fell back to the cruder activity-based estimate
// every time instead of the athlete's actual configured max HR.
export async function resolveMaxHr(env, activities) {
  const { value } = await resolveMaxHrDetailed(env, activities);
  return value;
}

// Same fallback chain as resolveMaxHr, but also reports which stage the value came
// from. Kept separate (rather than changing resolveMaxHr's return type) so existing
// callers of resolveMaxHr - weekly-progress.js, form-analysis.js - keep getting a
// plain number and don't need to change.
async function resolveMaxHrDetailed(env, activities) {
  let maxHr = Number(env?.MAX_HR || env?.ATHLETE_MAX_HR) || null;
  if (maxHr) return { value: maxHr, source: "env" };

  maxHr = await loadCachedMaxHr(env).catch(() => null);
  if (maxHr) return { value: maxHr, source: "kv_cache" };

  maxHr = await fetchAndCacheMaxHr(env).catch(() => null);
  if (maxHr) return { value: maxHr, source: "live_fetch" };

  maxHr = _estimateMaxHrFromActivities(activities) || null;
  if (maxHr) return { value: maxHr, source: "activity_estimate" };

  let highestAvg = 0;
  for (const a of activities || []) {
    const hr = Number(a?.average_heartrate ?? a?.avg_hr ?? 0);
    if (hr > highestAvg) highestAvg = hr;
  }
  const heuristic = highestAvg > 80 ? Math.round(highestAvg * 1.2) : null;
  return { value: heuristic, source: heuristic != null ? "avg_hr_heuristic" : null };
}

// ─── Main: compute & persist real VDOT ───────────────────────────────────────
// Returns { vdot, source, todayRunVdot } or { vdot: null } if nothing available.
export async function computeAndPersistRealVdot(env, activities, options = {}) {
  const { write = false, todayIso = null, isMondaySync = false, persistLatest = write } = options;

  // 1) Race-based VDOT from activities (free – data already loaded)
  const raceResult = computeRaceVdot(activities, todayIso);

  // 1b) Resolve max HR and the HR-based training VDOT (pre-correction, pre-pace-bench-
  // fallback) - used both as the primary trainVdot below and as the comparison basis for
  // the pace-benchmark drift factor (updatePaceBenchmarkDrift).
  const maxHrDetail = await resolveMaxHrDetailed(env, activities);
  const maxHr = maxHrDetail.value;
  const hrTrainVdot = maxHr ? computeTrainingVdotFromActivities(activities, todayIso, maxHr) : null;

  // 1c) Pace benchmarks (intervals.icu's own best-segment-time bests). Fetched regularly -
  // not just as a trainVdot fallback - so the drift path below has a continuous signal, but
  // still bounded by the 7-day cache and only hits the API on a Monday sync or a write.
  let bench = null;
  let benchTs = null;
  try {
    const cached = await loadCachedPaceBench(env);
    bench = cached?.data ?? null;
    benchTs = cached?.ts ?? null;
    if (!bench && (isMondaySync || write)) {
      const fetched = await fetchRunPaceBenchmarks(env).catch(() => null);
      if (fetched) {
        bench = fetched;
        benchTs = Date.now();
        if (write) saveCachedPaceBench(env, fetched).catch(() => {});
      }
    }
  } catch {}

  // 1d) Race-derived correction factor (see updateRaceCorrectionFactor). Only advanced on
  // writes so read-only calls don't process the same race twice from concurrent requests.
  const correctionAnchor = todayIso || isoDate(new Date());
  let correctionState = null;
  if (write && raceResult) {
    correctionState = await updateRaceCorrectionFactor(env, activities, raceResult, maxHr);
  } else {
    correctionState = await loadCorrectionState(env).catch(() => null);
  }

  // 1e) Pace-benchmark drift factor (see updatePaceBenchmarkDrift) - a separate, weakly-
  // weighted correction path layered on top of (not merged into) the race-blend logic above.
  let paceBenchVdot = bench ? computeVdotFromPaceBenchmarks(bench) : null;
  if (write) {
    const driftResult = await updatePaceBenchmarkDrift(env, bench, benchTs, hrTrainVdot);
    if (driftResult.state) correctionState = driftResult.state;
    if (driftResult.paceBenchVdot != null) paceBenchVdot = driftResult.paceBenchVdot;
  }

  const { raceFactor, driftFactor, combined: correctionFactor, daysSinceLastRace } = combineCorrectionFactors(
    correctionState,
    correctionAnchor,
  );

  // 2) Training-based VDOT: HR-adjusted from recent runs (primary) + pace benchmarks
  // (fallback only, when no HR-based estimate is available)
  let trainVdot = hrTrainVdot;
  if (trainVdot == null && paceBenchVdot != null) {
    trainVdot = paceBenchVdot;
  }
  if (trainVdot != null && correctionFactor !== 1) {
    trainVdot = Math.round(trainVdot * correctionFactor * 10) / 10;
  }

  // 2b) VDOT from today's specific run (for wellness field). Interval sessions and
  // activities manually tagged "#novdot" are excluded (see estimateTrainingVdotForWindow /
  // isVdotExcluded) so a tagged day falls back to the rolling currentVdot below instead of
  // writing a distorted or unwanted number.
  let todayRunVdot = null;
  // Whether a #novdot-tagged run happened today with no other qualifying run to fall back
  // on. Callers use this to actively clear the day's VDOT wellness field instead of leaving
  // a stale value from a previous sync in place, which would otherwise look like a fresh
  // (and possibly misleading) result for today's excluded run.
  let todayVdotExcluded = false;
  if (todayIso) {
    todayVdotExcluded = (activities || []).some(
      (a) => isRun(a) && isVdotExcluded(a) && String(a?.start_date_local || a?.start_date || "").slice(0, 10) === todayIso,
    );
  }
  // Debug: raw per-activity values for whichever run(s) today were actually fed into
  // _vdotFromTrainingActivity below, so a mismatch against Runalyze/Intervals.icu can be
  // traced back to the specific avgHr/distance/movingTime/hrPct that produced it (or the
  // reason it got filtered out). Purely additive - doesn't affect todayRunVdot itself.
  let todayActivityRaw = [];
  if (maxHr && todayIso) {
    const todayRuns = (activities || []).filter(
      (a) =>
        isRun(a) &&
        !isRaceActivity(a) &&
        !isIntervalActivity(a) &&
        !isVdotExcluded(a) &&
        String(a?.start_date_local || a?.start_date || "").slice(0, 10) === todayIso,
    );
    todayActivityRaw = todayRuns.map((a) => {
      const d = _vdotDetailsFromTrainingActivity(a, maxHr);
      return {
        id: a?.id ?? a?.icu_id ?? null,
        name: a?.name ?? null,
        avgHr: d.avgHr,
        distance: d.distance,
        movingTime: d.movingTime,
        hrPct: d.hrPct,
        pctVo2max: d.pctVo2max,
        vdot: d.vdot,
        excluded: d.excluded,
        exclusionReason: d.exclusionReason,
      };
    });
    const todayEstimates = todayActivityRaw.map((d) => d.vdot).filter((v) => v != null);
    const m = medianOf(todayEstimates);
    todayRunVdot = m != null ? Math.round(m * correctionFactor * 10) / 10 : null;
  }
  if (todayRunVdot != null) todayVdotExcluded = false;

  // 3) Load previous state for decay protection
  const prevState = await loadRealVdotState(env).catch(() => null);
  const prevVdot = Number(prevState?.vdot ?? 0);

  // 4) Determine current VDOT: current fitness = min(race, training)
  let currentVdot = null;
  let source = null;
  if (raceResult?.vdot != null && trainVdot != null) {
    if (trainVdot < raceResult.vdot) {
      currentVdot = trainVdot;
      source = "training";
    } else {
      currentVdot = raceResult.vdot;
      source = "race";
    }
  } else if (raceResult?.vdot != null) {
    currentVdot = raceResult.vdot;
    source = "race";
  } else if (trainVdot != null) {
    currentVdot = trainVdot;
    source = "training";
  }

  // 5) Sanity guard: don't drop more than 8 points in one sync (catches stale/corrupt data).
  if (prevVdot > 0 && currentVdot != null && prevVdot - currentVdot > 8) {
    currentVdot = prevVdot - 8;
  }

  // 6) If no new data, return persisted value
  if (currentVdot == null) {
    if (prevVdot > 0) {
      return { vdot: prevVdot, source: prevState?.source || "cached", todayRunVdot: null, todayVdotExcluded };
    }
    return { vdot: null, source: null, todayRunVdot: null, todayVdotExcluded };
  }

  currentVdot = Math.round(currentVdot * 10) / 10;

  const vdotDebug = {
    rawCorrectionFactor: Number.isFinite(correctionState?.factor) ? correctionState.factor : 1,
    appliedCorrectionFactor: correctionFactor,
    raceFactor,
    driftFactor,
    paceBenchVdot,
    daysSinceLastRace,
    raceCorrectionWarning: correctionState?.raceCorrectionWarning ?? null,
    resolvedMaxHr: maxHr,
    resolvedMaxHrSource: maxHrDetail.source,
    todayActivityRaw,
  };

  const result = { vdot: currentVdot, source, todayRunVdot, todayVdotExcluded, correctionFactor, vdotDebug };

  if (persistLatest) {
    await saveRealVdotState(env, { ...result, updatedAt: new Date().toISOString() }).catch(() => {});
  }

  return result;
}
