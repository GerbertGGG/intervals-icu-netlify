export default `
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

    const [athleteProfile, runPace, bikePower, vdotState] = await Promise.all([
      readAthleteProfile(env).catch(() => null),
      fetchRunPaceBenchmarks(env),
      fetchBikePowerBenchmarks(env),
      loadRealVdotState(env).catch(() => null),
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

    const vdotBlock = buildRealVdotBlock(vdotState);
    if (vdotBlock) {
      lines.push("");
      lines.push("🎯 DANIELS-ZONEN:");
      vdotBlock.split("\\n").forEach((l) => lines.push(l));
    }

    if (aiNarrative) {
      lines.push("");
      lines.push("💬 Trainer-Einschätzung:");
      lines.push(aiNarrative);
    }

    return { ...rep, text: lines.join("\\n") };
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
    monotony: computeTrainingMonotony(weeks),
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
