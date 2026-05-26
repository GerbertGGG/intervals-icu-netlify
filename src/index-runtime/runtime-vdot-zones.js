export default `

// ─── Real VDOT (Jack Daniels) + Training Zones ───────────────────────────────
// Computes VDOT from the last A-race result and/or weekly pace benchmarks.
// Derives the 5 standard Daniels training zones (E, M, T, I, R) from VDOT.
// Persists to KV so the value is available every day without extra API calls.

const REAL_VDOT_KV_PREFIX = "vdot:real:";
const PACE_BENCH_KV_PREFIX = "vdot:pacebench:";
const MAX_HR_KV_PREFIX = "vdot:maxhr:";
const PACE_BENCH_MAX_AGE_MS = 7 * 24 * 60 * 60 * 1000; // 7 days
const MAX_HR_MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 hours

function realVdotKvKey(env) {
  return \`\${REAL_VDOT_KV_PREFIX}\${mustEnv(env, "ATHLETE_ID")}\`;
}
function paceBenchKvKey(env) {
  return \`\${PACE_BENCH_KV_PREFIX}\${mustEnv(env, "ATHLETE_ID")}\`;
}
function maxHrKvKey(env) {
  return \`\${MAX_HR_KV_PREFIX}\${mustEnv(env, "ATHLETE_ID")}\`;
}

async function loadCachedMaxHr(env) {
  if (!hasKv(env)) return null;
  try {
    const cached = await readKvJson(env, maxHrKvKey(env));
    if (!cached?.ts || !cached?.maxHr) return null;
    if (Date.now() - cached.ts > MAX_HR_MAX_AGE_MS) return null;
    return Number(cached.maxHr) || null;
  } catch { return null; }
}

async function saveCachedMaxHr(env, maxHr) {
  if (!hasKv(env)) return;
  try { await writeKvJson(env, maxHrKvKey(env), { ts: Date.now(), maxHr }); }
  catch {}
}

async function fetchAndCacheMaxHr(env) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const resp = await fetch(BASE_URL + "/athlete/" + uid, {
      headers: { Authorization: authHeader(env) },
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const maxHr = Number(data?.max_hr || data?.maxHr || data?.hrMax || 0);
    if (maxHr > 100) {
      saveCachedMaxHr(env, maxHr).catch(() => {});
      return maxHr;
    }
    return null;
  } catch { return null; }
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

  const v = (dist / secs) * 60; // m/min
  const t = secs / 60;          // minutes

  const vo2 = -4.60 + 0.182258 * v + 0.000104 * v * v;
  const pctVo2max =
    0.8 +
    0.1894393 * Math.exp(-0.012778 * t) +
    0.2989558 * Math.exp(-0.1932605 * t);

  if (pctVo2max <= 0) return null;
  const vdot = vo2 / pctVo2max;
  if (!Number.isFinite(vdot) || vdot < 20 || vdot > 90) return null;
  return Math.round(vdot * 10) / 10;
}

// ─── Velocity → sec/km at given %VO2max ───────────────────────────────────────
// Solves: 0.000104·v² + 0.182258·v − (VDOT·pct + 4.60) = 0
function _secPerKmAtPct(vdot, pct) {
  const c = -(vdot * pct + 4.60);
  const disc = 0.182258 * 0.182258 - 4 * 0.000104 * c;
  if (disc < 0) return null;
  const v = (-0.182258 + Math.sqrt(disc)) / (2 * 0.000104); // m/min
  if (v <= 0) return null;
  return Math.round((60 * 1000) / v); // sec/km
}

// ─── Training zones from VDOT (Jack Daniels %VO2max ranges) ──────────────────
function computeVdotZones(vdot) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v < 20 || v > 90) return null;

  // pctLo = slower/easier end, pctHi = faster/harder end
  const defs = [
    { key: "E", label: "E  (Locker/GA)",     pctLo: 0.59, pctHi: 0.74, hrPct: "60–75 %" },
    { key: "M", label: "M  (Marathon)",       pctLo: 0.75, pctHi: 0.84, hrPct: "75–84 %" },
    { key: "T", label: "T  (Schwelle)",       pctLo: 0.83, pctHi: 0.88, hrPct: "82–88 %" },
    { key: "I", label: "I  (Intervall/VO2)",  pctLo: 0.95, pctHi: 1.00, hrPct: ">92 %"   },
    { key: "R", label: "R  (Wiederholung)",   pctLo: 1.05, pctHi: 1.05, hrPct: "max"     },
  ];

  const zones = {};
  for (const d of defs) {
    const fastSec = _secPerKmAtPct(v, d.pctHi); // high %VO2max = fast = low sec/km
    const slowSec = _secPerKmAtPct(v, d.pctLo); // low  %VO2max = slow = high sec/km
    zones[d.key] = {
      label: d.label,
      hrPct: d.hrPct,
      fastSecPerKm: fastSec,
      slowSecPerKm: slowSec,
    };
  }
  return zones;
}

// ─── Helpers ──────────────────────────────────────────────────────────────────
function _fmtPace(secPerKm) {
  if (!Number.isFinite(secPerKm) || secPerKm <= 0) return "n/a";
  const m = Math.floor(secPerKm / 60);
  const s = Math.round(secPerKm % 60);
  return \`\${m}:\${String(s).padStart(2, "0")}\`;
}

function _fmtTime(secs) {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = Math.round(secs % 60);
  if (h > 0) return \`\${h}:\${String(m).padStart(2,"0")}:\${String(s).padStart(2,"0")}\`;
  return \`\${m}:\${String(s).padStart(2,"0")}\`;
}

// ─── Find last A-race in activities ───────────────────────────────────────────
function findLastARaceForVdot(activities) {
  if (!Array.isArray(activities)) return null;
  const races = activities
    .filter((a) => {
      if (!isRun(a)) return false;
      if (!isRaceActivity(a)) return false;
      const dist = Number(a?.distance ?? a?.icu_distance ?? 0);
      const time = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
      return dist >= 800 && time >= 60;
    })
    .sort((a, b) => {
      const da = String(a?.start_date_local || a?.start_date || "");
      const db = String(b?.start_date_local || b?.start_date || "");
      return db.localeCompare(da); // newest first
    });
  return races[0] || null;
}

// ─── Compute VDOT from all races in activities (best of last 180 days) ────────
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
      best = {
        vdot: v,
        raceDate: day,
        raceName: String(a?.name || "Rennen").slice(0, 60),
        distKm: Math.round(dist / 100) / 10,
        timeFmt: _fmtTime(time),
        distanceM: dist,
        timeSecs: time,
      };
    }
  }
  return best;
}

// ─── Compute VDOT from pace benchmarks (API or cached) ────────────────────────
// runPace = { current: { 1000: totalSecs, 5000: ..., 10000: ..., 21097: ... } }
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
// Requires average_heartrate and distance/moving_time per activity.
function _estimateMaxHrFromActivities(activities) {
  let highest = 0;
  for (const a of (activities || [])) {
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
  // Only use E-zone runs (60–80% HRmax) for reliable VO2max estimation.
  // Threshold/harder efforts have cardiovascular drift that distorts the HR→VO2 relationship.
  if (hrPct < 0.55 || hrPct > 0.80) return null;
  const pctVo2max = 1.154 * hrPct - 0.15;
  if (pctVo2max <= 0.30 || pctVo2max >= 1.00) return null;
  const v = (dist / time) * 60; // m/min
  const vo2 = -4.60 + 0.182258 * v + 0.000104 * v * v;
  if (vo2 <= 0) return null;
  const vdot = vo2 / pctVo2max;
  if (!Number.isFinite(vdot) || vdot < 20 || vdot > 90) return null;
  return Math.round(vdot * 10) / 10;
}

function computeTrainingVdotFromActivities(activities, todayIso, maxHr) {
  if (!Array.isArray(activities) || !(maxHr > 100)) return null;
  const anchor = todayIso || isoDate(new Date());
  const cutoff = isoDate(new Date(new Date(anchor).getTime() - 28 * 86400000));
  const estimates = [];
  for (const a of activities) {
    if (!isRun(a) || isRaceActivity(a)) continue;
    const day = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (day < cutoff || day > anchor) continue;
    const v = _vdotFromTrainingActivity(a, maxHr);
    if (v != null) estimates.push(v);
  }
  if (!estimates.length) return null;
  estimates.sort((a, b) => a - b);
  const mid = Math.floor(estimates.length / 2);
  const median = estimates.length % 2 === 0
    ? (estimates[mid - 1] + estimates[mid]) / 2
    : estimates[mid];
  return Math.round(median * 10) / 10;
}

// ─── KV persistence ───────────────────────────────────────────────────────────
async function loadRealVdotState(env) {
  if (!hasKv(env)) return null;
  try { return await readKvJson(env, realVdotKvKey(env)); }
  catch { return null; }
}

async function saveRealVdotState(env, state) {
  if (!hasKv(env)) return;
  try { await writeKvJson(env, realVdotKvKey(env), state); }
  catch {}
}

async function loadCachedPaceBench(env) {
  if (!hasKv(env)) return null;
  try {
    const cached = await readKvJson(env, paceBenchKvKey(env));
    if (!cached?.ts) return null;
    if (Date.now() - cached.ts > PACE_BENCH_MAX_AGE_MS) return null;
    return cached.data || null;
  } catch { return null; }
}

async function saveCachedPaceBench(env, data) {
  if (!hasKv(env)) return;
  try { await writeKvJson(env, paceBenchKvKey(env), { ts: Date.now(), data }); }
  catch {}
}

// ─── Main: compute & persist real VDOT ───────────────────────────────────────
// Returns { vdot, source, zones, raceDate, raceName, distKm, timeFmt, trainVdot, zonesText }
// or null if nothing available.
async function computeAndPersistRealVdot(env, activities, options = {}) {
  const { write = false, todayIso = null, isMondaySync = false } = options;

  // 1) Race-based VDOT from activities (free – data already loaded)
  const raceResult = computeRaceVdot(activities, todayIso);

  // 2) Training-based VDOT: HR-adjusted from recent runs (primary) + pace benchmarks (fallback)
  let trainVdot = null;
  // Resolve max HR: env var → API (cached 24h) → estimate from activities
  let maxHr = Number(env?.MAX_HR || env?.ATHLETE_MAX_HR) || null;
  if (!maxHr) {
    maxHr = await loadCachedMaxHr(env).catch(() => null);
    if (!maxHr && write) maxHr = await fetchAndCacheMaxHr(env).catch(() => null);
    if (!maxHr) maxHr = _estimateMaxHrFromActivities(activities) || null;
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

  // 3) Load previous state for decay protection
  const prevState = await loadRealVdotState(env).catch(() => null);
  const prevVdot = Number(prevState?.vdot ?? 0);
  const prevUpdated = prevState?.updatedAt ? new Date(prevState.updatedAt).getTime() : 0;
  const daysSincePrev = (Date.now() - prevUpdated) / 86400000;

  // 4) Determine current VDOT
  // Zones always reflect *current* fitness = min(race, training).
  // Peak race VDOT is stored separately for reference display.
  let currentVdot = null;
  let source = null;
  const peakVdot = raceResult?.vdot ?? null;

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

  // 5) Decay protection: training-source VDOT decays ≤ 0.3/day without race confirmation
  if (prevVdot > 0 && currentVdot != null && source === "training") {
    const maxDrop = Math.min(0.3 * daysSincePrev, 2.0);
    if (prevVdot - currentVdot > maxDrop) {
      currentVdot = prevVdot - maxDrop;
    }
  }

  // 6) If no new data, return persisted value
  if (currentVdot == null) {
    if (prevVdot > 0) {
      const zones = computeVdotZones(prevVdot);
      return {
        vdot: prevVdot,
        source: prevState?.source || "cached",
        zones,
        peakVdot: prevState?.peakVdot || null,
        raceDate: prevState?.raceDate || null,
        raceName: prevState?.raceName || null,
        distKm: prevState?.distKm || null,
        timeFmt: prevState?.timeFmt || null,
        trainVdot: trainVdot || prevState?.trainVdot || null,
        fromCache: true,
      };
    }
    return null;
  }

  currentVdot = Math.round(currentVdot * 10) / 10;
  const zones = computeVdotZones(currentVdot);

  const result = {
    vdot: currentVdot,
    source,
    zones,
    peakVdot: peakVdot != null ? Math.round(peakVdot * 10) / 10 : (prevState?.peakVdot || null),
    raceDate: raceResult?.raceDate || prevState?.raceDate || null,
    raceName: raceResult?.raceName || prevState?.raceName || null,
    distKm: raceResult?.distKm || prevState?.distKm || null,
    timeFmt: raceResult?.timeFmt || prevState?.timeFmt || null,
    trainVdot,
    fromCache: false,
  };

  if (write) {
    await saveRealVdotState(env, {
      ...result,
      updatedAt: new Date().toISOString(),
    }).catch(() => {});
  }

  return result;
}

// ─── Report text block ────────────────────────────────────────────────────────
function buildRealVdotBlock(vdotResult) {
  if (!vdotResult?.vdot || !vdotResult?.zones) return "";
  const { vdot, source, zones, peakVdot, raceDate, raceName, distKm, timeFmt, trainVdot } = vdotResult;

  const lines = [];
  const srcLabel = source === "race" ? "Rennergebnis" : source === "training" ? "Training (aktuell)" : "Gespeichert";
  lines.push(\`VDOT \${vdot.toFixed(1)}  (\${srcLabel})\`);

  if (source === "race" && raceDate && (raceName || distKm)) {
    const nameStr = raceName ? raceName : "";
    const distStr = distKm ? \`\${distKm} km\` : "";
    const timeStr = timeFmt ? timeFmt : "";
    const detail = [nameStr, distStr && timeStr ? \`\${distStr} in \${timeStr}\` : distStr || timeStr]
      .filter(Boolean).join(" – ");
    if (detail) lines.push(\`Basis: \${detail} (\${raceDate})\`);
  }
  if (source === "training" && peakVdot != null && peakVdot > vdot) {
    const raceRef = raceDate ? \` (\${raceDate})\` : "";
    lines.push(\`Peak-VDOT: \${peakVdot.toFixed(1)} (Rennen\${raceRef}) — Zonen spiegeln aktuelles Training\`);
  }
  if (trainVdot != null && source === "race") {
    lines.push(\`Training-VDOT: \${trainVdot.toFixed(1)}\`);
  }

  lines.push("");
  lines.push("Trainingszonen (min/km):");
  for (const key of ["E", "M", "T", "I", "R"]) {
    const z = zones[key];
    if (!z) continue;
    const fast = _fmtPace(z.fastSecPerKm);
    const slow = _fmtPace(z.slowSecPerKm);
    const paceStr = fast === slow ? fast : \`\${fast} – \${slow}\`;
    lines.push(\`  \${z.label}: \${paceStr} /km\`);
  }

  return lines.join("\\n");
}
`;
