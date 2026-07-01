import { isoDate } from "./date-utils.js";
import { isRun, isRaceActivity, isIntervalActivity, isVdotExcluded } from "./activity-utils.js";
import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";
import { loadCachedMaxHr, fetchAndCacheMaxHr, fetchRunPaceBenchmarks } from "./intervals-client.js";

const REAL_VDOT_KV_PREFIX = "vdot:real:";
const PACE_BENCH_KV_PREFIX = "vdot:pacebench:";
const CORRECTION_KV_PREFIX = "vdot:correction:";
const PACE_BENCH_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;
const CORRECTION_MIN_FACTOR = 0.9;
const CORRECTION_MAX_FACTOR = 1.1;

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
function computeVdotFromRaceTime(distanceMeters, timeSecs) {
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

// Reads the persisted race-derived correction factor (1 = no correction). Applied as a
// multiplier on training-based VDOT estimates to account for individual deviations
// (pacing, race-day form) that the generic HR/pace formula can't see.
export async function getRaceCorrectionFactor(env) {
  const state = await loadCorrectionState(env).catch(() => null);
  return Number.isFinite(state?.factor) ? state.factor : 1;
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
    const newState = { factor: prevFactor, lastRaceDate: raceResult.raceDate, raceCount: state?.raceCount ?? 0 };
    await saveCorrectionState(env, newState).catch(() => {});
    return newState;
  }

  const rawFactor = clamp(raceResult.vdot / predictedVdot, CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR);
  const blended = clamp(prevFactor * 0.5 + rawFactor * 0.5, CORRECTION_MIN_FACTOR, CORRECTION_MAX_FACTOR);
  const newState = {
    factor: Math.round(blended * 1000) / 1000,
    lastRawFactor: Math.round(rawFactor * 1000) / 1000,
    lastRaceDate: raceResult.raceDate,
    raceCount: (state?.raceCount ?? 0) + 1,
  };
  await saveCorrectionState(env, newState).catch(() => {});
  return newState;
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

function _vdotFromTrainingActivity(activity, maxHr) {
  const dist = Number(activity?.distance ?? activity?.icu_distance ?? 0);
  const time = Number(activity?.moving_time ?? activity?.elapsed_time ?? 0);
  const avgHr = Number(activity?.average_heartrate ?? activity?.avg_hr ?? 0);
  if (dist < 2000 || time < 600 || avgHr <= 0 || maxHr <= 100) return null;
  const hrPct = avgHr / maxHr;
  if (hrPct < 0.55 || hrPct > 0.87) return null;
  const pctVo2max = 1.154 * hrPct - 0.15;
  if (pctVo2max <= 0.3 || pctVo2max >= 1.0) return null;
  const v = (dist / time) * 60;
  const vo2 = -4.6 + 0.182258 * v + 0.000104 * v * v;
  if (vo2 <= 0) return null;
  const vdot = vo2 / pctVo2max;
  if (!Number.isFinite(vdot) || vdot < 20 || vdot > 90) return null;
  return Math.round(vdot * 10) / 10;
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

async function loadCachedPaceBench(env) {
  if (!hasKv(env)) return null;
  try {
    const cached = await readKvJson(env, paceBenchKvKey(env));
    if (!cached?.ts) return null;
    if (Date.now() - cached.ts > PACE_BENCH_MAX_AGE_MS) return null;
    return cached.data || null;
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

// Resolves max HR via (in order): env override, KV cache, live API fetch (write-mode
// only), highest observed max_heartrate, or a heuristic from the highest average HR.
export async function resolveMaxHr(env, activities, { write = false } = {}) {
  let maxHr = Number(env?.MAX_HR || env?.ATHLETE_MAX_HR) || null;
  if (maxHr) return maxHr;

  maxHr = await loadCachedMaxHr(env).catch(() => null);
  if (maxHr) return maxHr;

  if (write) maxHr = await fetchAndCacheMaxHr(env).catch(() => null);
  if (maxHr) return maxHr;

  maxHr = _estimateMaxHrFromActivities(activities) || null;
  if (maxHr) return maxHr;

  let highestAvg = 0;
  for (const a of activities || []) {
    const hr = Number(a?.average_heartrate ?? a?.avg_hr ?? 0);
    if (hr > highestAvg) highestAvg = hr;
  }
  return highestAvg > 80 ? Math.round(highestAvg * 1.2) : null;
}

// ─── Main: compute & persist real VDOT ───────────────────────────────────────
// Returns { vdot, source, todayRunVdot } or { vdot: null } if nothing available.
export async function computeAndPersistRealVdot(env, activities, options = {}) {
  const { write = false, todayIso = null, isMondaySync = false, persistLatest = write } = options;

  // 1) Race-based VDOT from activities (free – data already loaded)
  const raceResult = computeRaceVdot(activities, todayIso);

  // 1b) Race-derived correction factor for training-based estimates (see
  // updateRaceCorrectionFactor for rationale). Only advanced on writes so read-only
  // calls don't process the same race twice from concurrent requests.
  const maxHr = await resolveMaxHr(env, activities, { write });
  let correctionFactor = 1;
  if (write && raceResult) {
    const correctionState = await updateRaceCorrectionFactor(env, activities, raceResult, maxHr);
    correctionFactor = correctionState?.factor ?? 1;
  } else {
    correctionFactor = await getRaceCorrectionFactor(env);
  }

  // 2) Training-based VDOT: HR-adjusted from recent runs (primary) + pace benchmarks (fallback)
  let trainVdot = null;
  if (maxHr) {
    trainVdot = computeTrainingVdotFromActivities(activities, todayIso, maxHr);
  }
  if (trainVdot == null) {
    try {
      let bench = await loadCachedPaceBench(env);
      if (!bench && (isMondaySync || write)) {
        bench = await fetchRunPaceBenchmarks(env).catch(() => null);
        if (bench && write) saveCachedPaceBench(env, bench).catch(() => {});
      }
      if (bench) trainVdot = computeVdotFromPaceBenchmarks(bench);
    } catch {}
  }
  if (trainVdot != null && correctionFactor !== 1) {
    trainVdot = Math.round(trainVdot * correctionFactor * 10) / 10;
  }

  // 2b) VDOT from today's specific run (for wellness field). Interval sessions and
  // activities manually tagged "#novdot" are excluded (see estimateTrainingVdotForWindow /
  // isVdotExcluded) so a tagged day falls back to the rolling currentVdot below instead of
  // writing a distorted or unwanted number.
  let todayRunVdot = null;
  if (maxHr && todayIso) {
    const todayEstimates = (activities || [])
      .filter(
        (a) =>
          isRun(a) &&
          !isRaceActivity(a) &&
          !isIntervalActivity(a) &&
          !isVdotExcluded(a) &&
          String(a?.start_date_local || a?.start_date || "").slice(0, 10) === todayIso,
      )
      .map((a) => _vdotFromTrainingActivity(a, maxHr))
      .filter((v) => v != null);
    const m = medianOf(todayEstimates);
    todayRunVdot = m != null ? Math.round(m * correctionFactor * 10) / 10 : null;
  }

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
      return { vdot: prevVdot, source: prevState?.source || "cached", todayRunVdot: null };
    }
    return { vdot: null, source: null, todayRunVdot: null };
  }

  currentVdot = Math.round(currentVdot * 10) / 10;

  const result = { vdot: currentVdot, source, todayRunVdot, correctionFactor };

  if (persistLatest) {
    await saveRealVdotState(env, { ...result, updatedAt: new Date().toISOString() }).catch(() => {});
  }

  return result;
}
