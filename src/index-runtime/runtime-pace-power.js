export default `
// Formats total seconds for a distance into min:sec/km pace string.
function fmtPacePerKm(totalSecs, distM) {
  const spk = totalSecs / (distM / 1000);
  const min = Math.floor(spk / 60);
  const sec = Math.round(spk % 60);
  return min + ":" + String(sec).padStart(2, "0") + "/km";
}

// Derives easy/GA run pace (secs/km) from best race paces using standard multipliers.
// Priority: 5km ×1.30, 10km ×1.20, HM ×1.12. Returns null if no data available.
function computeEasyRunPaceSecsPerKm(runPace) {
  if (!runPace || !runPace.current) return null;
  const configs = [
    { dist: 5000, factor: 1.30 },
    { dist: 10000, factor: 1.20 },
    { dist: 21097, factor: 1.12 },
  ];
  for (const { dist, factor } of configs) {
    const totalSecs = runPace.current[dist];
    if (totalSecs != null) {
      return (totalSecs / (dist / 1000)) * factor;
    }
  }
  return null;
}

// Fetches best run pace at 1k/5k/10k/HM for two 8-week windows and compares them.
// Returns { current, prev, deltas } keyed by distance (meters), or null on failure.
async function fetchRunPaceBenchmarks(env) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const now = new Date();
    const nowIso = isoDate(now);
    const w8  = isoDate(new Date(now.getTime() -  56 * 86400000));
    const w16 = isoDate(new Date(now.getTime() - 112 * 86400000));
    const dists = "1000,5000,10000,21097";
    const base  = BASE_URL + "/athlete/" + uid + "/activity-pace-curves?type=Run&distances=" + dists;
    const [curr, prev] = await Promise.all([
      fetch(base + "&oldest=" + w8  + "&newest=" + nowIso, { headers: { Authorization: authHeader(env) } })
        .then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; }),
      fetch(base + "&oldest=" + w16 + "&newest=" + w8,    { headers: { Authorization: authHeader(env) } })
        .then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; }),
    ]);
    if (!curr) return null;
    var parsePace = function(data, dist) {
      if (!data) return null;
      if (Array.isArray(data)) {
        var entry = data.find(function(d) { return Math.abs(Number(d.distance || d.dist || 0) - dist) < 100; });
        if (entry) { var s = Number(entry.secs || entry.time || entry.value); return Number.isFinite(s) && s > 0 ? s : null; }
      }
      if (data && typeof data === "object") { var v = data[String(dist)]; return v != null ? Number(v) || null : null; }
      return null;
    };
    var keyDists = [1000, 5000, 10000, 21097];
    var result = { current: {}, prev: {}, deltas: {} };
    for (var i = 0; i < keyDists.length; i++) {
      var d = keyDists[i];
      var cP = parsePace(curr, d), pP = parsePace(prev, d);
      if (cP != null) result.current[d] = cP;
      if (pP != null) result.prev[d]    = pP;
      if (cP != null && pP != null && pP > 0) result.deltas[d] = round((cP - pP) / pP * 100, 1);
    }
    return Object.keys(result.current).length > 0 ? result : null;
  } catch (_e) { return null; }
}

// Fetches best bike power at 5-min/20-min/60-min for two 8-week windows and compares.
// Returns { current, prev, deltas } keyed by duration (seconds), or null on failure.
async function fetchBikePowerBenchmarks(env) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const now = new Date();
    const nowIso = isoDate(now);
    const w8  = isoDate(new Date(now.getTime() -  56 * 86400000));
    const w16 = isoDate(new Date(now.getTime() - 112 * 86400000));
    const secsParam = "300,1200,3600";
    const base = BASE_URL + "/athlete/" + uid + "/activity-power-curves?type=Ride&secs=" + secsParam;
    const [curr, prev] = await Promise.all([
      fetch(base + "&oldest=" + w8  + "&newest=" + nowIso, { headers: { Authorization: authHeader(env) } })
        .then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; }),
      fetch(base + "&oldest=" + w16 + "&newest=" + w8,    { headers: { Authorization: authHeader(env) } })
        .then(function(r) { return r.ok ? r.json() : null; }).catch(function() { return null; }),
    ]);
    if (!curr) return null;
    var parsePow = function(data, sec) {
      if (!data) return null;
      if (Array.isArray(data)) {
        var entry = data.find(function(d) { return Math.abs(Number(d.secs || d.duration || 0) - sec) < 10; });
        if (entry) { var w = Number(entry.watts || entry.power || entry.value); return Number.isFinite(w) && w > 0 ? w : null; }
      }
      if (data && typeof data === "object") { var v = data[String(sec)]; return v != null ? Number(v) || null : null; }
      return null;
    };
    var keyDurs = [300, 1200, 3600];
    var result = { current: {}, prev: {}, deltas: {} };
    for (var i = 0; i < keyDurs.length; i++) {
      var s = keyDurs[i];
      var cW = parsePow(curr, s), pW = parsePow(prev, s);
      if (cW != null) result.current[s] = Math.round(cW);
      if (pW != null) result.prev[s]    = Math.round(pW);
      if (cW != null && pW != null && pW > 0) result.deltas[s] = round((cW - pW) / pW * 100, 1);
    }
    return Object.keys(result.current).length > 0 ? result : null;
  } catch (_e) { return null; }
}

// Derives aerobic/anaerobic energy profile from pace and power curve data.
// Returns classification + key metrics, or null if insufficient data.
function computeAerobicProfile(runPace, bikePower) {
  const result = {};

  // ── Bike metrics ─────────────────────────────────────────────────────────────
  if (bikePower && bikePower.current) {
    const p5  = bikePower.current[300];
    const p20 = bikePower.current[1200];
    const p60 = bikePower.current[3600];
    // FTP: 60min power preferred, fallback to 95% of 20min
    const ftp = p60 != null ? p60 : (p20 != null ? Math.round(p20 * 0.95) : null);
    if (ftp != null) result.bikeFtp = ftp;
    if (p5 != null && ftp != null && ftp > 0) {
      result.bikeAnaeroRatio = round(p5 / ftp, 2);
      // W' in kJ: only valid when 5-min power exceeds FTP
      if (p5 > ftp) result.bikeWprimeKj = Math.round((p5 - ftp) * 300 / 1000);
    }
    if (p20 != null && p60 != null && p20 > 0) {
      result.bikeAerobicEfficiency = round(p60 / p20, 2);
    }
  }

  // ── Run metrics ──────────────────────────────────────────────────────────────
  if (runPace && runPace.current) {
    const s1k  = runPace.current[1000];
    const s5k  = runPace.current[5000];
    const s10k = runPace.current[10000];
    const sHm  = runPace.current[21097];
    // Pace per km for each distance (sec/km)
    const p1k  = s1k  != null ? s1k          : null;
    const p5k  = s5k  != null ? s5k  / 5     : null;
    const p10k = s10k != null ? s10k / 10    : null;
    const pHm  = sHm  != null ? sHm  / 21.097: null;
    // Speed reserve: % pace drop from 1km to 5km — higher = more anaerobic capacity
    if (p1k != null && p5k != null && p1k > 0) {
      result.runSpeedReservePct = round((p5k - p1k) / p1k * 100, 1);
    }
    // Aerobic index: HM_pace / 1km_pace — closer to 1.0 = more aerobic
    if (p1k != null && pHm != null && p1k > 0) {
      result.runAerobicIndex = round(pHm / p1k, 3);
    }
    // Pace decay 5k→10k
    if (p5k != null && p10k != null && p5k > 0) {
      result.runPaceDecay5to10Pct = round((p10k - p5k) / p5k * 100, 1);
    }
  }

  // ── Overall classification ───────────────────────────────────────────────────
  let score = 0, n = 0;
  // Bike anaerobic ratio: <1.35 aerobic, >1.65 anaerobic
  if (result.bikeAnaeroRatio != null) {
    if      (result.bikeAnaeroRatio < 1.35) { score += 2; n++; }
    else if (result.bikeAnaeroRatio < 1.5)  { score += 1; n++; }
    else if (result.bikeAnaeroRatio > 1.65) { score -= 1; n++; }
    else n++;
  }
  // Run speed reserve: <15% aerobic, >28% anaerobic
  if (result.runSpeedReservePct != null) {
    if      (result.runSpeedReservePct < 15) { score += 2; n++; }
    else if (result.runSpeedReservePct < 22) { score += 1; n++; }
    else if (result.runSpeedReservePct > 28) { score -= 1; n++; }
    else n++;
  }
  // Bike aerobic efficiency (p60/p20): >0.92 strong aerobic base, <0.85 aerobic weakness
  if (result.bikeAerobicEfficiency != null) {
    if      (result.bikeAerobicEfficiency > 0.92) { score += 2; n++; }
    else if (result.bikeAerobicEfficiency > 0.88) { score += 1; n++; }
    else if (result.bikeAerobicEfficiency < 0.85) { score -= 1; n++; }
    else n++;
  }
  // Run pace decay 5k→10k: <8% aerobic strong, >15% aerobic deficiency
  if (result.runPaceDecay5to10Pct != null) {
    if      (result.runPaceDecay5to10Pct < 8)  { score += 2; n++; }
    else if (result.runPaceDecay5to10Pct < 12) { score += 1; n++; }
    else if (result.runPaceDecay5to10Pct > 15) { score -= 1; n++; }
    else n++;
  }
  if (n > 0) {
    const avg = score / n;
    result.profile = avg >= 1.2 ? "aerob-dominant" : avg <= -0.3 ? "anaerob-stärke" : "ausgeglichen";
  }

  return Object.keys(result).length > 0 ? result : null;
}

// Fetches HRV, sleep and resting-HR data for the last N days from intervals.icu.
// Returns a lightweight summary or null if data is unavailable / insufficient.
async function fetchWellnessTrend(env, days) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const now = new Date();
    const newest = isoDate(now);
    const oldest = isoDate(new Date(now.getTime() - days * 86400000));
    const url = BASE_URL + "/athlete/" + uid + "/wellness?oldest=" + oldest + "&newest=" + newest;
    const resp = await fetch(url, { headers: { Authorization: authHeader(env) } });
    if (!resp.ok) return null;
    const list = await resp.json();
    if (!Array.isArray(list) || list.length === 0) return null;

    const hrvVals = list.map((w) => Number(w.hrv)).filter((v) => v > 0 && Number.isFinite(v));
    const sleepVals = list.map((w) => Number(w.sleepSecs)).filter((v) => v > 0 && Number.isFinite(v));

    const avgHrv = hrvVals.length >= 3 ? round(hrvVals.reduce((a, b) => a + b, 0) / hrvVals.length, 0) : null;
    const avgSleepH = sleepVals.length >= 3 ? round(sleepVals.reduce((a, b) => a + b, 0) / sleepVals.length / 3600, 1) : null;

    let hrvTrend = "stabil";
    if (hrvVals.length >= 6) {
      // API returns oldest→newest, so slice(half) is the more recent half
      const half = Math.floor(hrvVals.length / 2);
      const olderAvg = hrvVals.slice(0, half).reduce((a, b) => a + b, 0) / half;
      const recentAvg = hrvVals.slice(half).reduce((a, b) => a + b, 0) / (hrvVals.length - half);
      if (recentAvg > olderAvg * 1.05) hrvTrend = "steigend";
      else if (recentAvg < olderAvg * 0.95) hrvTrend = "fallend";
    }

    const lowHrvDays = avgHrv != null ? hrvVals.filter((v) => v < avgHrv * 0.85).length : 0;
    const feelVals = list.map((w) => Number(w.feel)).filter((v) => v >= 1 && v <= 5 && Number.isFinite(v));
    const avgFeel = feelVals.length >= 3 ? round(feelVals.reduce((a, b) => a + b, 0) / feelVals.length, 1) : null;
    const sleepScoreVals = list.map((w) => Number(w.sleepScore ?? w.sleepQuality ?? 0)).filter((v) => v > 0 && Number.isFinite(v));
    const avgSleepScore = sleepScoreVals.length >= 3 ? Math.round(sleepScoreVals.reduce((a, b) => a + b, 0) / sleepScoreVals.length) : null;
    const vo2maxVals = list.map((w) => Number(w.vo2max)).filter((v) => v > 0 && Number.isFinite(v));\n    const avgVo2max = vo2maxVals.length >= 3 ? Math.round(vo2maxVals.reduce((a, b) => a + b, 0) / vo2maxVals.length) : null;\n    let vo2maxTrend = null;\n    if (vo2maxVals.length >= 6) {\n      const half = Math.floor(vo2maxVals.length / 2);\n      const olderAvg = vo2maxVals.slice(0, half).reduce((a, b) => a + b, 0) / half;\n      const recentAvg = vo2maxVals.slice(half).reduce((a, b) => a + b, 0) / (vo2maxVals.length - half);\n      if (recentAvg > olderAvg + 0.5) vo2maxTrend = "steigend";\n      else if (recentAvg < olderAvg - 0.5) vo2maxTrend = "fallend";\n      else vo2maxTrend = "stabil";\n    }\n    return { avgHrv, avgSleepH, avgSleepScore, avgFeel, hrvTrend, lowHrvDays, avgVo2max, vo2maxTrend };
  } catch (_e) {
    return null;
  }
}

`;
