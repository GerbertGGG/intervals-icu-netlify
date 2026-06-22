import { isoDate } from "./date-utils.js";
import { isRun, isRaceActivity } from "./activity-utils.js";
import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";
import { loadCachedMaxHr, fetchAndCacheMaxHr, fetchRunPaceBenchmarks } from "./intervals-client.js";

const REAL_VDOT_KV_PREFIX = "vdot:real:";
const PACE_BENCH_KV_PREFIX = "vdot:pacebench:";
const PACE_BENCH_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000;

function realVdotKvKey(env) {
  return `${REAL_VDOT_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}
function paceBenchKvKey(env) {
  return `${PACE_BENCH_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
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

function computeTrainingVdotFromActivities(activities, todayIso, maxHr) {
  if (!Array.isArray(activities) || !(maxHr > 100)) return null;
  const anchor = todayIso || isoDate(new Date());
  const cutoff = isoDate(new Date(new Date(anchor).getTime() - 28 * 86400000));
  const estimates = [];
  for (const a of activities) {
    if (!isRun(a) || isRaceActivity(a) || isTreadmill(a)) continue;
    const day = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (day < cutoff || day > anchor) continue;
    const v = _vdotFromTrainingActivity(a, maxHr);
    if (v != null) estimates.push(v);
  }
  const m = medianOf(estimates);
  return m != null ? Math.round(m * 10) / 10 : null;
}

async function loadRealVdotState(env) {
  if (!hasKv(env)) return null;
  try {
    return await readKvJson(env, realVdotKvKey(env));
  } catch {
    return null;
  }
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

// ─── Main: compute & persist real VDOT ───────────────────────────────────────
// Returns { vdot, source, todayRunVdot } or { vdot: null } if nothing available.
export async function computeAndPersistRealVdot(env, activities, options = {}) {
  const { write = false, todayIso = null, isMondaySync = false, persistLatest = write } = options;

  // 1) Race-based VDOT from activities (free – data already loaded)
  const raceResult = computeRaceVdot(activities, todayIso);

  // 2) Training-based VDOT: HR-adjusted from recent runs (primary) + pace benchmarks (fallback)
  let trainVdot = null;
  let maxHr = Number(env?.MAX_HR || env?.ATHLETE_MAX_HR) || null;
  if (!maxHr) {
    maxHr = await loadCachedMaxHr(env).catch(() => null);
    if (!maxHr) {
      if (write) maxHr = await fetchAndCacheMaxHr(env).catch(() => null);
      if (!maxHr) maxHr = _estimateMaxHrFromActivities(activities) || null;
      if (!maxHr) {
        let highestAvg = 0;
        for (const a of activities || []) {
          const hr = Number(a?.average_heartrate ?? a?.avg_hr ?? 0);
          if (hr > highestAvg) highestAvg = hr;
        }
        if (highestAvg > 80) maxHr = Math.round(highestAvg * 1.2);
      }
    }
  }
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

  // 2b) VDOT from today's specific run (for wellness field)
  let todayRunVdot = null;
  if (maxHr && todayIso) {
    const todayEstimates = (activities || [])
      .filter((a) => isRun(a) && !isRaceActivity(a) && String(a?.start_date_local || a?.start_date || "").slice(0, 10) === todayIso)
      .map((a) => _vdotFromTrainingActivity(a, maxHr))
      .filter((v) => v != null);
    const m = medianOf(todayEstimates);
    todayRunVdot = m != null ? Math.round(m * 10) / 10 : null;
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

  const result = { vdot: currentVdot, source, todayRunVdot };

  if (persistLatest) {
    await saveRealVdotState(env, { ...result, updatedAt: new Date().toISOString() }).catch(() => {});
  }

  return result;
}
