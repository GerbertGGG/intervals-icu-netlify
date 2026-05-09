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
      const lagStart = Math.min(i + 2, i + 1); // at least i+1
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

// Formats the effectiveness section text for the Monday report.
function buildEffectivenessText(laggedEffects, peaks, sweetSpot, weekCount) {
  const lines = [];
  lines.push("Trainings-Effektivität (" + weekCount + "W):");

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
          "  + " + keyType + ": EF " + diff + "% besser nach solchen Wochen" +
          " (n=" + d.nWith + " mit / " + d.nWithout + " ohne)"
        );
      }
    }

    if (negative.length > 0) {
      lines.push("");
      lines.push("Vorsicht – eher gegenteilig:");
      for (const [keyType, d] of negative) {
        lines.push(
          "  - " + keyType + ": EF " + d.laggedEfDiff.toFixed(1) + "% schlechter nach solchen Wochen" +
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
        "  " +
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
        "  " + peak.weekId +
          ": vorher " + p.weeks + "W – Load ~" + (p.avgLoad ?? "?") +
          "/W, " + easyPct +
          ", Keys " + (p.avgKeysPerWeek != null ? p.avgKeysPerWeek.toFixed(1) : "?") + "/W" +
          " (" + keyStr + ")"
      );
    }

    if (peaks.troughs.length > 0) {
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

  return lines.join("\\n");
}

// Calls Cloudflare AI to generate a 3-4 sentence coaching narrative in German.
// Falls back gracefully if AI binding unavailable or call fails.
async function generateEffectivenessNarrativeAI(env, data) {
  if (!env?.AI) return null;

  try {
    const { laggedEffects, sweetSpot, peaks, weekCount } = data;
    const contextParts = [];

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

    if (!contextParts.length) return null;

    const systemPrompt =
      "Du bist ein erfahrener Lauftrainer. Analysiere die Trainingsdaten des Athleten und schreibe 3-4 direkte, prägnante Sätze auf Deutsch: " +
      "(1) Was bei diesem Athleten klar wirkt, (2) worauf er am besten anspricht, (3) eine konkrete Empfehlung für die nächsten Wochen. " +
      'Schreibe direkt ("Du...", keine Einleitung, kein "Als Trainer..."). Keine Fachbegriff-Erklärungen.';

    const userPrompt =
      "Trainingsdaten des Athleten (" + weekCount + " Wochen analysiert):\\n" +
      contextParts.join("\\n") +
      "\\n\\nKurze, direkte Trainer-Analyse:";

    const result = await env.AI.run("@cf/meta/llama-3.3-70b-instruct", {
      messages: [
        { role: "system", content: systemPrompt },
        { role: "user", content: userPrompt },
      ],
      max_tokens: 250,
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

    const sectionText = buildEffectivenessText(laggedEffects, peaks, sweetSpot, weeks.length);

    const aiNarrative = await generateEffectivenessNarrativeAI(env, {
      laggedEffects,
      sweetSpot,
      peaks,
      weekCount: weeks.length,
    });

    const lines = rep.text.split("\\n");
    lines.push("");
    lines.push(sectionText);

    if (aiNarrative) {
      lines.push("");
      lines.push("Trainer-Einschätzung:");
      lines.push(aiNarrative);
    }

    return { ...rep, text: lines.join("\\n"), effectivenessData: { laggedEffects, peaks, sweetSpot } };
  } catch (_err) {
    return rep;
  }
}

`;
