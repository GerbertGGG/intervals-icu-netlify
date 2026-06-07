export default `

// ─── Training Effectiveness Analysis ─────────────────────────────────────────
// Analyses up to 52 weeks of stored WeekDocs to answer:
//  1. Which key types lead to EF improvement ~3 weeks later? (lagged effect)
//  2. At what load level does this athlete improve? (load sweet spot)
//  3. What was training like before the best/worst performance periods? (peaks)
//  4. Summary narrative via Cloudflare AI (if env.AI is available)

const EFFECTIVENESS_MIN_WEEKS = 4;
const EFFECTIVENESS_LAG_WEEKS = 3;
const EFFECTIVENESS_MIN_GROUP_N = 2;
const EFFECTIVENESS_ALL_KEY_TYPES = ["schwelle", "vo2_touch", "racepace", "strides", "steady", "longrun"];

// Loads all stored WeekDocs (up to WEEKDOC_INDEX_LIMIT, typically 52 weeks).
// The pattern loader uses PATTERN_WINDOW_WEEKS=16; this one goes further back.
async function loadAllWeekDocsForEffectiveness(env) {
  if (!hasKv(env)) return [];
  const uid = mustEnv(env, "ATHLETE_ID");
  const idxKey = buildWeekIndexKey(uid);
  const idx = (await readKvJson(env, idxKey)) || [];
  const weekIds = idx.filter((w) => typeof w === "string");
  const docs = [];
  for (const weekId of weekIds) {
    const doc = await readKvJson(env, buildWeekDocKey(uid, weekId));
    if (doc) docs.push(doc);
  }
  return docs.filter((d) => hasAnyPatternOutputData(d));
}

// Lagged key-type effect: for each key type, compare EF trend in the
// EFFECTIVENESS_LAG_WEEKS weeks AFTER a week where that type was used vs. not.
// Uses weeks sorted chronologically (oldest first).
function computeLaggedKeyTypeEffect(weeks) {
  if (!weeks || weeks.length < EFFECTIVENESS_MIN_WEEKS + EFFECTIVENESS_LAG_WEEKS) return {};

  // Sort oldest first so we can look forward
  const chron = [...weeks]
    .filter((w) => w?.weekId && hasAnyPatternOutputData(w))
    .sort((a, b) => String(a.weekId).localeCompare(String(b.weekId)));

  const results = {};

  for (const keyType of EFFECTIVENESS_ALL_KEY_TYPES) {
    const efAfterWith = [];
    const efAfterWithout = [];

    for (let i = 0; i < chron.length; i++) {
      const week = chron[i];
      // Collect lagged EF deltas (weeks i+2 to i+LAG, skip immediate next week)
      let laggedEfSum = 0;
      let lagCount = 0;
      const lagStart = i + 2;
      for (let j = lagStart; j <= i + EFFECTIVENESS_LAG_WEEKS && j < chron.length; j++) {
        const lagWeek = chron[j];
        if (Number.isFinite(lagWeek?.output?.ef_delta_pct)) {
          laggedEfSum += lagWeek.output.ef_delta_pct;
          lagCount++;
        }
      }
      if (lagCount === 0) continue;

      const avgLaggedEf = laggedEfSum / lagCount;
      const hasType =
        Array.isArray(week?.input?.key_types) && week.input.key_types.includes(keyType);

      if (hasType) efAfterWith.push(avgLaggedEf);
      else efAfterWithout.push(avgLaggedEf);
    }

    if (
      efAfterWith.length >= EFFECTIVENESS_MIN_GROUP_N &&
      efAfterWithout.length >= EFFECTIVENESS_MIN_GROUP_N
    ) {
      const medWith = median(efAfterWith);
      const medWithout = median(efAfterWithout);
      results[keyType] = {
        nWith: efAfterWith.length,
        nWithout: efAfterWithout.length,
        laggedEfWith: round(medWith, 2),
        laggedEfWithout: round(medWithout, 2),
        laggedEfDiff: round(medWith - medWithout, 2),
      };
    }
  }

  return results;
}

// Finds the top and bottom EF-level periods and describes
// what training looked like in the 4 weeks preceding each.
function findPerformancePeaksAndTroughs(weeks) {
  const usable = (weeks || []).filter((w) => Number.isFinite(w?.output?.ef_level) && w?.weekId);
  if (usable.length < 5) return null;

  const chron = [...usable].sort((a, b) => String(a.weekId).localeCompare(String(b.weekId)));
  const byEf = [...usable].sort((a, b) => b.output.ef_level - a.output.ef_level);

  const n = Math.max(2, Math.min(3, Math.floor(usable.length * 0.2)));
  const peaks = byEf.slice(0, n);
  const troughs = byEf.slice(-n);

  function getPrecedingPattern(weekId, lookback = 4) {
    const idx = chron.findIndex((w) => w.weekId === weekId);
    if (idx < 1) return null;
    const preceding = chron.slice(Math.max(0, idx - lookback), idx);
    if (!preceding.length) return null;

    const finiteVals = (arr) => arr.filter((v) => Number.isFinite(v));
    const avg = (arr) => {
      const v = finiteVals(arr);
      return v.length ? round(v.reduce((a, b) => a + b, 0) / v.length, 1) : null;
    };

    const loads = preceding.map((w) => w?.input?.load_total);
    const easyShares = preceding.map((w) => w?.input?.easy_share);
    const keyCounts = preceding.map((w) => Number(w?.input?.keys_count));
    const longruns = preceding.map((w) => Number(w?.input?.longruns_count));
    const keyTypes = [...new Set(preceding.flatMap((w) => w?.input?.key_types || []))];

    return {
      avgLoad: avg(loads),
      avgEasyShare: avg(easyShares),
      avgKeysPerWeek: avg(keyCounts),
      avgLongrunsPerWeek: avg(longruns),
      keyTypes,
      weeks: preceding.length,
    };
  }

  const efValues = usable.map((w) => w.output.ef_level).filter(Number.isFinite);
  return {
    peaks: peaks.map((w) => ({
      weekId: w.weekId,
      efLevel: round(w.output.ef_level, 5),
      preceding: getPrecedingPattern(w.weekId),
    })),
    troughs: troughs.map((w) => ({
      weekId: w.weekId,
      efLevel: round(w.output.ef_level, 5),
      preceding: getPrecedingPattern(w.weekId),
    })),
    efMedian: round(median(efValues), 5),
  };
}

// Computes at which weekly load level (low/mid/high thirds) the athlete
// shows the best EF improvement in subsequent weeks.
function computeLoadSweetSpot(weeks) {
  const usable = (weeks || []).filter(
    (w) => Number.isFinite(w?.input?.load_total) && Number.isFinite(w?.output?.ef_delta_pct)
  );
  if (usable.length < 6) return null;

  const loads = usable.map((w) => w.input.load_total).sort((a, b) => a - b);
  const p33 = loads[Math.floor(loads.length * 0.33)] ?? loads[0];
  const p66 = loads[Math.floor(loads.length * 0.66)] ?? loads[loads.length - 1];

  const low = usable.filter((w) => w.input.load_total <= p33);
  const mid = usable.filter((w) => w.input.load_total > p33 && w.input.load_total <= p66);
  const high = usable.filter((w) => w.input.load_total > p66);

  const medEfDelta = (arr) => {
    const vals = arr.map((w) => w.output.ef_delta_pct).filter(Number.isFinite);
    return vals.length ? round(median(vals), 2) : null;
  };

  return {
    low: { maxLoad: round(p33, 0), n: low.length, medEfDelta: medEfDelta(low) },
    mid: { minLoad: round(p33, 0), maxLoad: round(p66, 0), n: mid.length, medEfDelta: medEfDelta(mid) },
    high: { minLoad: round(p66, 0), n: high.length, medEfDelta: medEfDelta(high) },
  };
}

// Foster's Training Monotony Index: mean(weekly_load) / sd(weekly_load) over last 8 weeks.
// > 2.0 signals dangerously uniform load — adaptation suffers and injury risk rises.
function computeTrainingMonotony(weeks) {
  const usable = (weeks || [])
    .filter((w) => Number.isFinite(w?.input?.load_total) && w.input.load_total > 0)
    .sort((a, b) => String(b.weekId).localeCompare(String(a.weekId)))
    .slice(0, 8);
  if (usable.length < 4) return null;
  const loads = usable.map((w) => w.input.load_total);
  const mean = loads.reduce((a, b) => a + b, 0) / loads.length;
  if (mean < 1) return null;
  const variance = loads.reduce((sum, l) => sum + (l - mean) ** 2, 0) / loads.length;
  const stdDev = Math.sqrt(variance);
  if (stdDev < 1) return { monotony: 9.9, mean: round(mean, 0), stdDev: 0, n: usable.length };
  return { monotony: round(mean / stdDev, 2), mean: round(mean, 0), stdDev: round(stdDev, 0), n: usable.length };
}

// Correlates weekly execution quality (avg_execution_score in WeekDoc input)
// with EF improvement ~3 weeks later. Also derives an 8-week quality trend.
// Returns { highQualityEfDelta, lowQualityEfDelta, diff, trend8w, avgScore8w, n } or null.
function computeExecutionQualityInsights(weeks) {
  const HIGH_THRESH = 0.67;
  const chron = [...(weeks || [])]
    .filter((w) => w?.weekId && hasAnyPatternOutputData(w) && Number.isFinite(w?.input?.avg_execution_score))
    .sort((a, b) => String(a.weekId).localeCompare(String(b.weekId)));

  if (chron.length < 4) return null;

  const highQuality = [];
  const lowQuality = [];

  for (let i = 0; i < chron.length; i++) {
    const score = chron[i].input.avg_execution_score;
    let laggedEfSum = 0, lagCount = 0;
    for (let j = i + 1; j <= i + EFFECTIVENESS_LAG_WEEKS && j < chron.length; j++) {
      if (Number.isFinite(chron[j]?.output?.ef_delta_pct)) {
        laggedEfSum += chron[j].output.ef_delta_pct;
        lagCount++;
      }
    }
    if (lagCount === 0) continue;
    const avgLag = laggedEfSum / lagCount;
    if (score >= HIGH_THRESH) highQuality.push(avgLag);
    else lowQuality.push(avgLag);
  }

  const recent8 = chron.slice(-8);
  const scores = recent8.map((w) => w.input.avg_execution_score).filter(Number.isFinite);
  let trend8w = "stabil";
  if (scores.length >= 4) {
    const half = Math.floor(scores.length / 2);
    const olderAvg = scores.slice(0, half).reduce((a, b) => a + b, 0) / half;
    const recentAvg = scores.slice(half).reduce((a, b) => a + b, 0) / (scores.length - half);
    if (recentAvg > olderAvg + 0.05) trend8w = "steigend";
    else if (recentAvg < olderAvg - 0.05) trend8w = "fallend";
  }
  const avgScore8w = scores.length
    ? round(scores.reduce((a, b) => a + b, 0) / scores.length, 2)
    : null;

  const result = { n: chron.length, trend8w, avgScore8w };
  if (highQuality.length >= 2) result.highQualityEfDelta = round(median(highQuality), 2);
  if (lowQuality.length >= 2) result.lowQualityEfDelta = round(median(lowQuality), 2);
  if (result.highQualityEfDelta != null && result.lowQualityEfDelta != null) {
    result.diff = round(result.highQualityEfDelta - result.lowQualityEfDelta, 2);
  }
  return result;
}

`;
