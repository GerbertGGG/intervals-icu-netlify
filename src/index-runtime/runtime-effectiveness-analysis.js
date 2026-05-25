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

// Formats the effectiveness section text for the Monday report.
function buildEffectivenessText(laggedEffects, peaks, sweetSpot, weekCount, execQuality, monotony) {
  const lines = [];
  lines.push("📈 TRAININGS-EFFEKTIVITÄT (" + weekCount + "W)");

  // 1. Lagged key-type effects
  const effectEntries = Object.entries(laggedEffects || {});
  if (effectEntries.length > 0) {
    const positive = effectEntries
      .filter(([, d]) => d.laggedEfDiff >= 0.4)
      .sort(([, a], [, b]) => b.laggedEfDiff - a.laggedEfDiff);
    const negative = effectEntries
      .filter(([, d]) => d.laggedEfDiff <= -0.4)
      .sort(([, a], [, b]) => a.laggedEfDiff - b.laggedEfDiff);
    const neutral = effectEntries.filter(([, d]) => Math.abs(d.laggedEfDiff) < 0.4);

    if (positive.length > 0) {
      lines.push("");
      lines.push("Was bei dir wirkt (~" + EFFECTIVENESS_LAG_WEEKS + "W Verzögerung):");
      for (const [keyType, d] of positive) {
        const diff = d.laggedEfDiff > 0 ? "+" + d.laggedEfDiff.toFixed(1) : d.laggedEfDiff.toFixed(1);
        lines.push(
          "- ✓ " + keyType + ": EF " + diff + "% besser nach solchen Wochen" +
          " (n=" + d.nWith + " mit / " + d.nWithout + " ohne)"
        );
      }
    }

    if (negative.length > 0) {
      lines.push("");
      lines.push("Vorsicht – eher gegenteilig:");
      for (const [keyType, d] of negative) {
        lines.push(
          "- ✗ " + keyType + ": EF " + d.laggedEfDiff.toFixed(1) + "% schlechter nach solchen Wochen" +
          " (n=" + d.nWith + " / " + d.nWithout + ")"
        );
      }
    }

    if (positive.length === 0 && negative.length === 0) {
      lines.push("");
      lines.push(
        "Kein klares Verzögerungs-Signal erkennbar (n=" +
          weekCount +
          "). Alle Key-Typen reagieren ähnlich – weiter sammeln."
      );
    }
  }

  // 2. Load sweet spot
  if (sweetSpot) {
    const zones = [
      { label: "≤" + sweetSpot.low.maxLoad, ...sweetSpot.low },
      { label: sweetSpot.mid.minLoad + "–" + sweetSpot.mid.maxLoad, ...sweetSpot.mid },
      { label: "≥" + sweetSpot.high.minLoad, ...sweetSpot.high },
    ].filter((z) => z.medEfDelta != null);

    if (zones.length >= 2) {
      const best = [...zones].sort((a, b) => b.medEfDelta - a.medEfDelta)[0];
      lines.push("");
      const bestDelta = best.medEfDelta > 0 ? "+" + best.medEfDelta.toFixed(1) : best.medEfDelta.toFixed(1);
      lines.push("Load-Sweet-Spot: ~" + best.label + " /Woche → beste EF-Reaktion (" + bestDelta + "%)");
      lines.push(
        "- " +
          zones
            .map(
              (z) =>
                z.label +
                ": " +
                (z.medEfDelta > 0 ? "+" : "") +
                z.medEfDelta.toFixed(1) +
                "% (n=" +
                z.n +
                ")"
            )
            .join(" | ")
      );
    }
  }

  // 3. Performance peaks context
  if (peaks && peaks.peaks.length > 0) {
    lines.push("");
    lines.push("Beste Phasen (EF-Spitzen):");
    for (const peak of peaks.peaks.slice(0, 2)) {
      if (!peak.preceding) continue;
      const p = peak.preceding;
      const keyStr =
        p.keyTypes.length > 0 ? p.keyTypes.slice(0, 3).join(" + ") : "kein Key";
      const easyPct =
        p.avgEasyShare != null ? Math.round(p.avgEasyShare * 100) + "% easy" : "";
      lines.push(
        "- " + peak.weekId +
          ": vorher " + p.weeks + "W – Load ~" + (p.avgLoad ?? "?") +
          "/W, " + easyPct +
          ", Keys " + (p.avgKeysPerWeek != null ? p.avgKeysPerWeek.toFixed(1) : "?") + "/W" +
          " (" + keyStr + ")"
      );
    }

    if (peaks.troughs.length > 0) {
      lines.push("");
      lines.push("Schwache Phasen (EF-Tiefs):");
      for (const trough of peaks.troughs.slice(0, 1)) {
        if (!trough.preceding) continue;
        const p = trough.preceding;
        const keyStr = p.keyTypes.length > 0 ? p.keyTypes.slice(0, 3).join(" + ") : "kein Key";
        const easyPct =
          p.avgEasyShare != null ? Math.round(p.avgEasyShare * 100) + "% easy" : "";
        lines.push(
          "  " + trough.weekId +
            ": vorher " + p.weeks + "W – Load ~" + (p.avgLoad ?? "?") +
            "/W, " + easyPct +
            ", Keys " + (p.avgKeysPerWeek != null ? p.avgKeysPerWeek.toFixed(1) : "?") + "/W" +
            " (" + keyStr + ")"
        );
      }
    }
  }

  // 4. Execution quality insights
  if (execQuality && execQuality.avgScore8w != null) {
    lines.push("");
    const scorePct = Math.round(execQuality.avgScore8w * 100);
    const trendEmoji = execQuality.trend8w === "steigend" ? "↑" : execQuality.trend8w === "fallend" ? "↓" : "→";
    lines.push("Ausführungsqualität (letzte 8W): " + scorePct + "% " + trendEmoji + " " + execQuality.trend8w + " (n=" + execQuality.n + ")");
    if (execQuality.diff != null && Math.abs(execQuality.diff) >= 0.3) {
      const sign = execQuality.diff > 0 ? "+" : "";
      lines.push(
        "- Hohe Qualität (≥67%): EF " + (execQuality.highQualityEfDelta > 0 ? "+" : "") + execQuality.highQualityEfDelta.toFixed(1) + "% " +
        "vs. niedrige Qualität: EF " + (execQuality.lowQualityEfDelta > 0 ? "+" : "") + execQuality.lowQualityEfDelta.toFixed(1) + "% " +
        "→ Differenz " + sign + execQuality.diff.toFixed(1) + "%"
      );
    } else if (execQuality.diff != null) {
      lines.push("- Kein klarer Ausführungsqualitäts-Effekt auf EF (Diff " + execQuality.diff.toFixed(1) + "%).");
    }
  }

  // 5. Training Monotony
  if (monotony && monotony.monotony != null) {
    const level = monotony.monotony > 2.0 ? "⚠️ HOCH" : monotony.monotony > 1.5 ? "erhöht" : "gut";
    lines.push("");
    lines.push(
      "Trainingsmonotonie (8W): " + monotony.monotony.toFixed(1) + " → " + level +
      " (∅ " + monotony.mean + " Load/W, SD=" + monotony.stdDev + ", n=" + monotony.n + ")"
    );
    if (monotony.monotony > 2.0) {
      lines.push("- Zu gleichförmige Belastung: mehr Variation im Wochen-Load einbauen (Ruhewochen, Spitzenwochen).");
    }
  }

  return lines.join("\\n");
}

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

// Calls Cloudflare AI to generate a 3-4 sentence coaching narrative in German.
// Falls back gracefully if AI binding unavailable or call fails.
async function generateEffectivenessNarrativeAI(env, data) {
  if (!env?.AI) return null;

  try {
    const { laggedEffects, sweetSpot, peaks, weekCount, athleteProfile, fourWeekInsights, runPace, bikePower, aerobicProfile, execQuality, monotony, recentEasyShares } = data;
    const contextParts = [];

    // ── 1. Kausal-Signale (Lagged Key-Type Effects) ───────────────────────────
    const effectEntries = Object.entries(laggedEffects || {});
    const positive = effectEntries
      .filter(([, d]) => d.laggedEfDiff >= 0.4)
      .sort(([, a], [, b]) => b.laggedEfDiff - a.laggedEfDiff)
      .map(([t, d]) => t + ": +" + d.laggedEfDiff.toFixed(1) + "%");
    const negative = effectEntries
      .filter(([, d]) => d.laggedEfDiff <= -0.4)
      .map(([t, d]) => t + ": " + d.laggedEfDiff.toFixed(1) + "%");

    if (positive.length) contextParts.push("Positiv wirkende Key-Typen (Verzögerungseffekt ~3W): " + positive.join(", "));
    if (negative.length) contextParts.push("Weniger wirksame Key-Typen: " + negative.join(", "));

    // ── 2. Load-Sweet-Spot ────────────────────────────────────────────────────
    if (sweetSpot) {
      const zones = [
        sweetSpot.low.medEfDelta != null
          ? "Low (≤" + sweetSpot.low.maxLoad + "): " + (sweetSpot.low.medEfDelta > 0 ? "+" : "") + sweetSpot.low.medEfDelta.toFixed(1) + "%"
          : null,
        sweetSpot.mid.medEfDelta != null
          ? "Mid (" + sweetSpot.mid.minLoad + "–" + sweetSpot.mid.maxLoad + "): " + (sweetSpot.mid.medEfDelta > 0 ? "+" : "") + sweetSpot.mid.medEfDelta.toFixed(1) + "%"
          : null,
        sweetSpot.high.medEfDelta != null
          ? "High (≥" + sweetSpot.high.minLoad + "): " + (sweetSpot.high.medEfDelta > 0 ? "+" : "") + sweetSpot.high.medEfDelta.toFixed(1) + "%"
          : null,
      ].filter(Boolean);
      if (zones.length) contextParts.push("Load-Response: " + zones.join(", "));
    }

    // ── 3. Performance-Peaks ──────────────────────────────────────────────────
    if (peaks && peaks.peaks.length > 0) {
      const peakInfo = peaks.peaks
        .filter((p) => p.preceding)
        .slice(0, 2)
        .map(
          (p) =>
            p.weekId +
            " (EF " +
            p.efLevel.toFixed(4) +
            "): " +
            (p.preceding.keyTypes.join("+") || "kein Key") +
            ", Load ~" +
            p.preceding.avgLoad +
            "/W, Easy " +
            (p.preceding.avgEasyShare != null ? Math.round(p.preceding.avgEasyShare * 100) + "%" : "?")
        )
        .join("; ");
      if (peakInfo) contextParts.push("Beste Leistungsphasen: " + peakInfo);
    }

    // ── 4. EF-Basistrend (aus Athletenprofil) ────────────────────────────────
    const efHistory = Array.isArray(athleteProfile?.efStats?.baseline42dHistory)
      ? athleteProfile.efStats.baseline42dHistory.slice(0, 5)
      : [];
    if (efHistory.length >= 2) {
      const efFirst = efHistory[efHistory.length - 1].value;
      const efLast = efHistory[0].value;
      const efDir = efLast > efFirst + 0.0005 ? "steigend" : efLast < efFirst - 0.0005 ? "fallend" : "stabil";
      const efStr = efHistory.map((e) => e.date + ": " + e.value.toFixed(4)).join(", ");
      contextParts.push("EF-Basistrend (42d, letzte " + efHistory.length + " Wochen): " + efStr + " → Trend: " + efDir);
    }

    // ── 5. Letzte 4 Wochen im Detail ─────────────────────────────────────────
    const recentWeeks = Array.isArray(athleteProfile?.weekHistory) ? athleteProfile.weekHistory.slice(0, 4) : [];
    if (recentWeeks.length >= 2) {
      const weekLines = recentWeeks.map((w) => {
        const motorStr = Number.isFinite(w.motorAvg) ? ", Motor " + w.motorAvg : "";
        const kmStr = Number.isFinite(w.weekRunKm) ? ", " + w.weekRunKm + "km" : "";
        return w.weekIso + ": " + (w.block || "?") + ", Last " + (w.totalLoad ?? "?") + ", " + (w.runCount ?? "?") + " Läufe" + kmStr + ", Key: " + (w.hasKey ? "ja" : "nein") + motorStr;
      });
      contextParts.push("Letzte Wochen (aktuell zuerst):\\n" + weekLines.join("\\n"));
    }

    if (recentEasyShares && recentEasyShares.length > 0) {
      contextParts.push("Einfach-Anteil (easy_share) letzte Wochen: " + recentEasyShares.join(", "));
    }

    // ── 6. 4-Wochen-Fortschritt ───────────────────────────────────────────────
    if (fourWeekInsights?.progressCategory) {
      const efDelta = Number.isFinite(fourWeekInsights.efDelta)
        ? "EF " + (fourWeekInsights.efDelta >= 0 ? "+" : "") + (fourWeekInsights.efDelta * 1000).toFixed(1) + "‰"
        : null;
      const vdotDelta = Number.isFinite(fourWeekInsights.vdotDelta)
        ? "VDOT " + (fourWeekInsights.vdotDelta >= 0 ? "+" : "") + fourWeekInsights.vdotDelta.toFixed(2)
        : null;
      const parts = [efDelta, vdotDelta].filter(Boolean);
      contextParts.push(
        "4-Wochen-Fortschritt: " + fourWeekInsights.progressCategory +
        (parts.length ? " (" + parts.join(", ") + ")" : "")
      );
    }

    // ── 7. Ausführungsqualität der Key-Sessions ───────────────────────────────
    if (execQuality && execQuality.avgScore8w != null) {
      const qPct = Math.round(execQuality.avgScore8w * 100);
      const qParts = ["∅ " + qPct + "% (" + execQuality.trend8w + ", n=" + execQuality.n + " Wochen)"];
      if (execQuality.diff != null) {
        qParts.push(
          "hohe Qualität→ EF " + (execQuality.highQualityEfDelta >= 0 ? "+" : "") + execQuality.highQualityEfDelta.toFixed(1) + "% vs. " +
          "niedrig→ EF " + (execQuality.lowQualityEfDelta >= 0 ? "+" : "") + execQuality.lowQualityEfDelta.toFixed(1) + "% " +
          "(Diff " + (execQuality.diff >= 0 ? "+" : "") + execQuality.diff.toFixed(1) + "%)"
        );
      }
      contextParts.push("Ausführungsqualität Key-Sessions: " + qParts.join(", "));
    }

    // ── 8. Wellness-Trend (HRV + Schlaf, letzte 2 Wochen) ────────────────────
    const wellness = await fetchWellnessTrend(env, 14);
    if (wellness) {
      const wParts = [];
      if (wellness.avgHrv != null) wParts.push("HRV ∅ " + wellness.avgHrv + " ms (" + wellness.hrvTrend + ")");
      if (wellness.avgSleepScore != null) wParts.push("Schlaf-Score ∅ " + wellness.avgSleepScore);
      else if (wellness.avgSleepH != null) wParts.push("Schlaf ∅ " + wellness.avgSleepH + "h");
      if (wellness.avgFeel != null) wParts.push("Befinden ∅ " + wellness.avgFeel + "/5");
      if (wellness.lowHrvDays > 0) wParts.push(wellness.lowHrvDays + " Tag(e) mit niedriger HRV");\n      if (wellness.avgVo2max != null) {\n        const vo2Str = "VO2max ∅ " + wellness.avgVo2max + (wellness.vo2maxTrend ? " (" + wellness.vo2maxTrend + ")" : "");\n        wParts.push(vo2Str);\n      }
      if (wParts.length) contextParts.push("Wellness letzte 2 Wochen: " + wParts.join(", "));
    }

    if (monotony && monotony.monotony != null) {
      const mLevel = monotony.monotony > 2.0 ? "hoch (zu gleichförmig)" : monotony.monotony > 1.5 ? "erhöht" : "gut";
      contextParts.push("Trainingsmonotonie (8W): " + monotony.monotony.toFixed(1) + " (" + mLevel + ", ∅ Load " + monotony.mean + ", SD " + monotony.stdDev + ")");
    }

    // ── 9. Pace/Power-Entwicklung (letzte 8W vs. vorherige 8W) ───────────────
    const _distNames = { 1000: "1km", 5000: "5km", 10000: "10km", 21097: "HM" };
    const _durNames = { 300: "5min", 1200: "20min", 3600: "60min" };
    if (runPace && Object.keys(runPace.current).length > 0) {
      const runParts = [1000, 5000, 10000, 21097]
        .filter((d) => runPace.current[d] != null)
        .map((d) => {
          const pace = fmtPacePerKm(runPace.current[d], d);
          const delta = runPace.deltas[d];
          const dStr = delta != null ? " (" + (delta <= 0 ? "+" : "") + (-delta).toFixed(1) + "%)" : "";
          return _distNames[d] + " " + pace + dStr;
        });
      if (runParts.length) contextParts.push("Lauf-Bestzeiten (letzte 8W): " + runParts.join(", "));
      const easyPaceSecs = computeEasyRunPaceSecsPerKm(runPace);
      if (easyPaceSecs != null) {
        contextParts.push("Easy-Lauftempo (GA/locker): " + fmtPacePerKm(easyPaceSecs, 1000) + " (abgeleitet aus Bestzeit)");
      }
    }
    if (bikePower && Object.keys(bikePower.current).length > 0) {
      const bikeParts = [300, 1200, 3600]
        .filter((s) => bikePower.current[s] != null)
        .map((s) => {
          const delta = bikePower.deltas[s];
          const dStr = delta != null ? " (" + (delta >= 0 ? "+" : "") + delta.toFixed(1) + "%)" : "";
          return _durNames[s] + " " + bikePower.current[s] + "W" + dStr;
        });
      if (bikeParts.length) contextParts.push("Bike-Power (letzte 8W): " + bikeParts.join(", "));
    }

    // ── 10. Aerob/Anaerob-Energieprofil ──────────────────────────────────────
    if (aerobicProfile && aerobicProfile.profile) {
      const apParts = ["Klassifikation: " + aerobicProfile.profile];
      if (aerobicProfile.bikeFtp != null) apParts.push("FTP ~" + aerobicProfile.bikeFtp + "W");
      if (aerobicProfile.bikeAnaeroRatio != null) apParts.push("Anaerob-Ratio " + aerobicProfile.bikeAnaeroRatio + " (1.0=reiner Ausdauerer, >1.6=stark anaerob)");
      if (aerobicProfile.bikeWprimeKj != null) apParts.push("W' ~" + aerobicProfile.bikeWprimeKj + "kJ");
      if (aerobicProfile.runSpeedReservePct != null) apParts.push("Lauf Speed-Reserve " + aerobicProfile.runSpeedReservePct + "% (<15%=aerob, >28%=anaerob)");
      if (aerobicProfile.runAerobicIndex != null) apParts.push("Aerob-Index " + aerobicProfile.runAerobicIndex);
      if (aerobicProfile.bikeAerobicEfficiency != null) apParts.push("Power-Nachhaltigkeit 60/20min " + aerobicProfile.bikeAerobicEfficiency + " (>0.92=starke Ausdauerbasis, <0.85=aerobe Schwäche → mehr Zone2)");
      if (aerobicProfile.runPaceDecay5to10Pct != null) apParts.push("Pace-Abfall 5→10km " + aerobicProfile.runPaceDecay5to10Pct + "% (<8%=starke aerobe Basis, >15%=aerobe Schwäche → mehr Grundlagenvolumen)");
      contextParts.push("Energieprofil: " + apParts.join(", "));
    }

    if (!contextParts.length) return null;

    // ── System-Prompt: persönliche Athleten-Identität einbauen ───────────────
    const profileWeeks = Array.isArray(athleteProfile?.weekHistory) ? athleteProfile.weekHistory.length : null;
    const avgLoad = athleteProfile?.loadStats?.avgWeeklyLoad;
    const keyPct = athleteProfile?.consistency?.keyComplianceLast8Weeks;
    const identityParts = [];
    if (profileWeeks) identityParts.push("seit " + profileWeeks + " Wochen begleitet");
    if (avgLoad != null) identityParts.push("Avg-Last " + avgLoad);
    if (keyPct != null) identityParts.push("Key-Compliance " + Math.round(keyPct * 100) + "%");
    const identityStr = identityParts.length ? " (" + identityParts.join(", ") + ")" : "";

    const systemPrompt =
      "Du bist ein persoenlicher Trainer fuer Laufen und Radfahren" + identityStr + ". " +
      "Du hast echte Leistungsdaten: FTP, Pace-Bestzeiten, Energieprofil (aerob/anaerob), Kausaleffekte, Wellness und Trainingshistorie. " +
      "Antworte auf Deutsch in genau diesem Format:\\n" +
      "EINSCHAETZUNG: 1-2 Saetze — was bei diesem Athleten wirkt, welches Energieprofil dominiert, wie der Trend ist.\\n" +
      "NAECHSTE WOCHE:\\n" +
      "- [Wochentag]: [Einheit mit konkreten Zahlen aus den Daten — Watt aus FTP, Pace aus Lauf-Bestzeiten, Dauer, Sets/Reps]\\n" +
      "- [Wochentag]: [...]\\n" +
      "- [Wochentag]: [...]\\n" +
      "WARNUNG (nur wenn HRV niedrig, Readiness <55 oder Wellness-Auffaelligkeit): 1 Satz.\\n" +
      "Regeln: Nutze tatsaechliche Watt/Pace-Zahlen aus den Kontext-Daten. Fuer lockere/easy Einheiten nutze das 'Easy-Lauftempo (GA/locker)' aus den Daten. Keine Theorie, keine Erklaerungen, Du-Form. " +
      "Passe Intensitaeten dem Energieprofil an: aerob-dominant = mehr kurze VO2max-Intervalle (Zone 3), weniger Schwelle (Schwelle nur als Erhalt); anaerob-Staerke = mehr Grundlagenvolumen, kaum Intensitaet. " +
      "Bevorzugte Einheitentypen je Profil: aerob-dominant → vo2_touch und strides bevorzugen; anaerob-Staerke → steady und longrun bevorzugen, keine harten Keys. " +
      "easy_share Zielkorridore (Anteil leichter Einheiten/Woche): aerob-dominant ≥75%, ausgeglichen ≥70%, anaerob-Staerke ≥80%. " +
      "Falls easy_share der letzten Wochen deutlich darunter liegt: explizit mehr lockere Einheiten in NAECHSTE WOCHE einplanen und begruenden.";

    const userPrompt =
      "Athletendaten (" + weekCount + " Wochen analysiert):\\n" +
      contextParts.join("\\n") +
      "\\n\\nErstelle jetzt: EINSCHAETZUNG, NAECHSTE WOCHE (3 Einheiten mit Watt/Pace-Zahlen), WARNUNG falls noetig.";

    const result = await env.AI.run("@cf/meta/llama-3.3-70b-instruct", {
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      max_tokens: 500,
    });

    const text = result?.response || result?.text || null;
    return text ? text.trim() : null;
  } catch (_err) {
    return null;
  }
}

// Main integration: loads all WeekDocs, runs all analyses,
// and appends the effectiveness section to the existing Monday report.
async function computeAndAppendEffectivenessInsights(env, rep) {
  if (!rep) return rep;

  try {
    const weeks = await loadAllWeekDocsForEffectiveness(env);
    if (!weeks || weeks.length < EFFECTIVENESS_MIN_WEEKS) return rep;

    const laggedEffects = computeLaggedKeyTypeEffect(weeks);
    const peaks = findPerformancePeaksAndTroughs(weeks);
    const sweetSpot = computeLoadSweetSpot(weeks);
    const execQuality = computeExecutionQualityInsights(weeks);
    const monotony = computeTrainingMonotony(weeks);

    const chronDesc = [...weeks].sort((a, b) => String(b.weekId).localeCompare(String(a.weekId)));
    const recentEasyShares = chronDesc.slice(0, 4)
      .filter((w) => Number.isFinite(w?.input?.easy_share))
      .map((w) => Math.round(w.input.easy_share * 100) + "% (" + w.weekId + ")");

    const sectionText = buildEffectivenessText(laggedEffects, peaks, sweetSpot, weeks.length, execQuality, monotony);

    const [athleteProfile, runPace, bikePower] = await Promise.all([
      readAthleteProfile(env).catch(() => null),
      fetchRunPaceBenchmarks(env),
      fetchBikePowerBenchmarks(env),
    ]);

    // Build pace/power section for the report
    const pacePowerLines = [];
    const distLabels = { 1000: "1km", 5000: "5km", 10000: "10km", 21097: "HM" };
    const durLabels  = { 300: "5min", 1200: "20min", 3600: "60min" };
    if (runPace && Object.keys(runPace.current).length > 0) {
      const runParts = [1000, 5000, 10000, 21097]
        .filter((d) => runPace.current[d] != null)
        .map((d) => {
          const pace = fmtPacePerKm(runPace.current[d], d);
          const delta = runPace.deltas[d];
          const dStr = delta != null ? " (" + (delta <= 0 ? "+" : "") + (-delta).toFixed(1) + "%)" : "";
          return distLabels[d] + " " + pace + dStr;
        });
      if (runParts.length) pacePowerLines.push("🏃 Laufen: " + runParts.join(", "));
      const easyPaceSecs = computeEasyRunPaceSecsPerKm(runPace);
      if (easyPaceSecs != null) {
        pacePowerLines.push("🚶 Easy-Tempo (GA): " + fmtPacePerKm(easyPaceSecs, 1000));
      }
    }
    if (bikePower && Object.keys(bikePower.current).length > 0) {
      const bikeParts = [300, 1200, 3600]
        .filter((s) => bikePower.current[s] != null)
        .map((s) => {
          const delta = bikePower.deltas[s];
          const dStr = delta != null ? " (" + (delta >= 0 ? "+" : "") + delta.toFixed(1) + "%)" : "";
          return durLabels[s] + " " + bikePower.current[s] + "W" + dStr;
        });
      if (bikeParts.length) pacePowerLines.push("🚴 Bike: " + bikeParts.join(", "));
    }

    const aerobicProfile = computeAerobicProfile(runPace, bikePower);
    if (aerobicProfile && aerobicProfile.profile) {
      const profileParts = [];
      if (aerobicProfile.bikeFtp != null) profileParts.push("FTP ~" + aerobicProfile.bikeFtp + "W");
      if (aerobicProfile.bikeAnaeroRatio != null) profileParts.push("Anaerob-Ratio " + aerobicProfile.bikeAnaeroRatio);
      if (aerobicProfile.bikeWprimeKj != null) profileParts.push("W' ~" + aerobicProfile.bikeWprimeKj + "kJ");
      if (aerobicProfile.runSpeedReservePct != null) profileParts.push("Speed-Reserve " + aerobicProfile.runSpeedReservePct + "%");
      if (aerobicProfile.runAerobicIndex != null) profileParts.push("Aerob-Index " + aerobicProfile.runAerobicIndex);
      if (aerobicProfile.bikeAerobicEfficiency != null) profileParts.push("Power-Nachhaltigkeit " + aerobicProfile.bikeAerobicEfficiency);
      if (aerobicProfile.runPaceDecay5to10Pct != null) profileParts.push("Pace-Abfall 5→10k " + aerobicProfile.runPaceDecay5to10Pct + "%");
      pacePowerLines.push("⚡ Energieprofil: " + aerobicProfile.profile + (profileParts.length ? " (" + profileParts.join(", ") + ")" : ""));
    }

    const aiNarrative = await generateEffectivenessNarrativeAI(env, {
      laggedEffects,
      sweetSpot,
      peaks,
      weekCount: weeks.length,
      athleteProfile,
      fourWeekInsights: rep?.fourWeekInsights || null,
      runPace,
      bikePower,
      aerobicProfile,
      execQuality,
      monotony,
      recentEasyShares,
    });

    const lines = rep.text.split("\\n");
    lines.push("");
    lines.push("────────────────────");
    lines.push(sectionText);

    if (pacePowerLines.length > 0) {
      lines.push("");
      lines.push("📊 PACE/POWER (8W-Vergleich):");
      pacePowerLines.forEach((l) => lines.push(l));
    }

    if (aiNarrative) {
      lines.push("");
      lines.push("💬 Trainer-Einschätzung:");
      lines.push(aiNarrative);
    }

    return { ...rep, text: lines.join("\\n"), effectivenessData: { laggedEffects, peaks, sweetSpot, execQuality } };
  } catch (_err) {
    return rep;
  }
}

// ─── Sync-engine helpers ──────────────────────────────────────────────────────

const EFFECTIVENESS_CACHE_KV_KEY_PREFIX = "eff-ctx-v1-";
const EFFECTIVENESS_CACHE_TTL_MS = 23 * 60 * 60 * 1000; // 23h — refresh once/day

// Computes the effectiveness context from stored WeekDocs.
async function computeEffectivenessContext(env) {
  const weeks = await loadAllWeekDocsForEffectiveness(env);
  if (!weeks || weeks.length < EFFECTIVENESS_MIN_WEEKS) return null;
  return {
    laggedEffects: computeLaggedKeyTypeEffect(weeks),
    sweetSpot: computeLoadSweetSpot(weeks),
    weekCount: weeks.length,
  };
}

// Called once per sync cycle. Returns { laggedEffects, sweetSpot, weekCount } or null.
// Result is cached in KV for 23h — normally only recomputed once per day.
// Cache is invalidated after /backfill-weekdocs runs.
async function loadEffectivenessContextForSync(env) {
  try {
    if (!hasKv(env)) return await computeEffectivenessContext(env);

    const uid = mustEnv(env, "ATHLETE_ID");
    const cacheKey = EFFECTIVENESS_CACHE_KV_KEY_PREFIX + uid;

    const cached = await readKvJson(env, cacheKey);
    if (
      cached?.computed_at &&
      Number.isFinite(Number(cached.computed_at)) &&
      Date.now() - Number(cached.computed_at) < EFFECTIVENESS_CACHE_TTL_MS &&
      cached.data
    ) {
      return cached.data;
    }

    const data = await computeEffectivenessContext(env);
    if (data) {
      writeKvJson(env, cacheKey, { computed_at: Date.now(), data }).catch(() => {});
    }
    return data;
  } catch (_err) {
    return null;
  }
}

// Given the currently planned key type and the allowed key rules, returns
// an alternative key type if effectiveness data shows it performs meaningfully
// better for this specific athlete (lagged EF effect ≥ 1.0%, min 3 samples).
// Returns null if no compelling alternative exists.
// Conservative by design: never overrides to banned types, never overrides
// when the current type already has a strong positive signal (≥ 1.5%).
function pickEffectivenessPreferredKeyType(currentType, keyRules, effectivenessCtx) {
  const laggedEffects = effectivenessCtx?.laggedEffects;
  if (!laggedEffects || !currentType) return null;

  const MIN_IMPROVEMENT = 1.0;
  const MIN_SAMPLES = 3;
  const STRONG_CURRENT_THRESHOLD = 1.5;

  const currentEffect = laggedEffects[currentType]?.laggedEfDiff ?? 0;
  if (currentEffect >= STRONG_CURRENT_THRESHOLD) return null; // already good

  const banned = new Set(keyRules?.bannedKeyTypes || []);
  const allowed = keyRules?.allowedKeyTypes || [];

  let bestAlt = null;
  let bestDiff = -Infinity;

  for (const altType of allowed) {
    if (altType === currentType) continue;
    if (banned.has(altType)) continue;
    const altData = laggedEffects[altType];
    if (!altData) continue;
    if (altData.nWith < MIN_SAMPLES) continue;
    const improvement = altData.laggedEfDiff - currentEffect;
    if (improvement >= MIN_IMPROVEMENT && altData.laggedEfDiff > bestDiff) {
      bestDiff = altData.laggedEfDiff;
      bestAlt = altType;
    }
  }

  return bestAlt;
}

`;
