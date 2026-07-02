import { isoDate, parseISODateSafe } from "./date-utils.js";
import { isRun, activityDay, activityLoad } from "./activity-utils.js";
import { hasKv, readKvJson, writeKvJson, mustEnv } from "./kv.js";
import { fetchIntervalsActivities, fetchIntervalsWellnessDay, upsertIntervalsNote } from "./intervals-client.js";
import {
  resolveMaxHr,
  estimateTrainingVdotForWindow,
  getCurrentRealVdot,
  paceTargetsFromVdot,
  predictRaceTimesFromVdot,
  getRaceCorrectionFactor,
} from "./vdot.js";
import { readLatestBlockStateKv } from "./block-phase.js";
import { readGoalRace, computeGoalRaceInfo } from "./goal-race.js";

const HISTORY_KV_PREFIX = "weeklyprogress:history:";
const MAX_HISTORY_ENTRIES = 12;
// Extra lookback before the comparison window purely to give resolveMaxHr's
// fallback heuristics (max observed HR) a wider, more reliable sample.
const MAX_HR_LOOKBACK_DAYS = 180;
const WELLNESS_FALLBACK_LOOKBACK_DAYS = 3;

function historyKvKey(env) {
  return `${HISTORY_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}

function addDays(dayIso, n) {
  return isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() + n * 86400000));
}

// The most recently *completed* Mon–Sun week strictly before anchorIso.
export function lastCompletedWeek(anchorIso) {
  const d = parseISODateSafe(anchorIso);
  const dow = d.getUTCDay(); // 0=Sun..6=Sat
  const daysSinceLastSunday = dow === 0 ? 7 : dow;
  const end = addDays(anchorIso, -daysSinceLastSunday);
  const start = addDays(end, -6);
  return { start, end };
}

export function previousWeek(week) {
  const end = addDays(week.start, -1);
  const start = addDays(end, -6);
  return { start, end };
}

function sumActivitiesInWeek(activities, week) {
  let loadSum = 0;
  let movingTimeSecs = 0;
  let distanceM = 0;
  let sessionCount = 0;
  let runSessionCount = 0;
  for (const a of activities || []) {
    const day = activityDay(a);
    if (!day || day < week.start || day > week.end) continue;
    sessionCount++;
    if (isRun(a)) runSessionCount++;
    loadSum += activityLoad(a);
    movingTimeSecs += Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
    distanceM += Number(a?.distance ?? a?.icu_distance ?? 0) || 0;
  }
  return {
    loadSum: Math.round(loadSum),
    movingTimeMin: Math.round(movingTimeSecs / 60),
    distanceKm: Math.round((distanceM / 1000) * 10) / 10,
    sessionCount,
    runSessionCount,
  };
}

// intervals.icu only has a wellness row for days that were actually filled in;
// walk a few days back from the week's end to still get a usable CTL/ATL reading.
async function wellnessNearWeekEnd(env, weekEndIso) {
  let day = weekEndIso;
  for (let i = 0; i <= WELLNESS_FALLBACK_LOOKBACK_DAYS; i++) {
    const w = await fetchIntervalsWellnessDay(env, day).catch(() => null);
    const ctl = Number(w?.ctl);
    if (Number.isFinite(ctl)) {
      const atl = Number(w?.atl);
      const rampRate = Number(w?.rampRate);
      return {
        ctl: Math.round(ctl * 10) / 10,
        atl: Number.isFinite(atl) ? Math.round(atl * 10) / 10 : null,
        rampRate: Number.isFinite(rampRate) ? Math.round(rampRate * 10) / 10 : null,
        date: day,
      };
    }
    day = addDays(day, -1);
  }
  return { ctl: null, atl: null, rampRate: null, date: null };
}

async function buildWeekSnapshot(env, activities, week, maxHr) {
  const activitySums = sumActivitiesInWeek(activities, week);
  const wellness = await wellnessNearWeekEnd(env, week.end);
  const vdot = estimateTrainingVdotForWindow(activities, week.start, week.end, maxHr);
  const tsb =
    Number.isFinite(wellness.ctl) && Number.isFinite(wellness.atl) ? Math.round((wellness.ctl - wellness.atl) * 10) / 10 : null;
  return { week, ...activitySums, vdot, ctl: wellness.ctl, atl: wellness.atl, tsb, rampRate: wellness.rampRate };
}

function delta(curr, prev) {
  if (!Number.isFinite(curr) || !Number.isFinite(prev)) return null;
  return Math.round((curr - prev) * 10) / 10;
}

function pctChange(curr, prev) {
  if (!Number.isFinite(curr) || !Number.isFinite(prev) || prev === 0) return null;
  return Math.round(((curr - prev) / Math.abs(prev)) * 1000) / 10;
}

function compareSnapshots(curr, prev) {
  return {
    dVdot: delta(curr.vdot, prev.vdot),
    dCtl: delta(curr.ctl, prev.ctl),
    dAtl: delta(curr.atl, prev.atl),
    dTsb: delta(curr.tsb, prev.tsb),
    dLoad: delta(curr.loadSum, prev.loadSum),
    pctLoad: pctChange(curr.loadSum, prev.loadSum),
    dMovingTimeMin: delta(curr.movingTimeMin, prev.movingTimeMin),
    pctMovingTime: pctChange(curr.movingTimeMin, prev.movingTimeMin),
    dDistanceKm: delta(curr.distanceKm, prev.distanceKm),
    dSessionCount: delta(curr.sessionCount, prev.sessionCount),
  };
}

const fmt = (v, digits = 1) => (Number.isFinite(v) ? v.toFixed(digits) : "–");
const fmtSigned = (v, digits = 1) => (Number.isFinite(v) ? (v > 0 ? `+${v.toFixed(digits)}` : v.toFixed(digits)) : "–");

function fmtTimeDeltaSeconds(deltaSecs) {
  if (!Number.isFinite(deltaSecs) || deltaSecs === 0) return "±0s";
  const sign = deltaSecs > 0 ? "+" : "-";
  const abs = Math.abs(Math.round(deltaSecs));
  const m = Math.floor(abs / 60);
  const s = abs % 60;
  return m > 0 ? `${sign}${m}:${String(s).padStart(2, "0")}` : `${sign}${s}s`;
}

// Rule-based verdict: VDOT trend is the primary "did performance improve" signal
// (it's literally derived from pace+HR), CTL trend is the secondary "is training
// building fitness" signal. Both are weighted equally since either can lead.
function buildVerdict(cmp, curr, prev) {
  let score = 0;
  let signals = 0;
  if (cmp.dVdot != null) {
    signals++;
    if (cmp.dVdot >= 0.3) score += 1;
    else if (cmp.dVdot <= -0.3) score -= 1;
  }
  if (cmp.dCtl != null) {
    signals++;
    if (cmp.dCtl >= 1) score += 1;
    else if (cmp.dCtl <= -1) score -= 1;
  }

  let verdict;
  if (signals === 0) verdict = "UNKLAR";
  else if (score > 0) verdict = "BESSER";
  else if (score < 0) verdict = "SCHLECHTER";
  else verdict = "STABIL";

  const reasons = [];

  // VDOT – primary performance signal
  if (cmp.dVdot != null) {
    reasons.push(
      cmp.dVdot >= 0.3
        ? `VDOT ${fmtSigned(cmp.dVdot)} – deine Laufeffizienz zieht an. Das Training wirkt.`
        : cmp.dVdot <= -0.3
          ? `VDOT ${fmtSigned(cmp.dVdot)} – deine Laufleistung ist diese Woche gesunken.`
          : `VDOT ${fmtSigned(cmp.dVdot)} – deine Laufleistung hält sich stabil.`,
    );
  } else {
    reasons.push("VDOT: Zu wenige auswertbare Läufe für einen Vergleich – nächste Woche mehr Laufdata sammeln.");
  }

  // CTL – fitness buildup
  if (cmp.dCtl != null) {
    reasons.push(
      cmp.dCtl >= 1
        ? `Fitness (CTL) ${fmtSigned(cmp.dCtl)} – du baust Grundlagenausdauer auf. Gut so.`
        : cmp.dCtl <= -1
          ? `Fitness (CTL) ${fmtSigned(cmp.dCtl)} – deine Trainingsbasis geht leicht zurück.`
          : `Fitness (CTL) ${fmtSigned(cmp.dCtl)} – Fitness hält sich stabil, kein Aufbau, kein Verlust.`,
    );
  }

  // ATL – fatigue trend
  if (cmp.dAtl != null) {
    if (cmp.dAtl > 5) {
      reasons.push(`Ermüdung (ATL) ${fmtSigned(cmp.dAtl)} – du hast diese Woche deutlich mehr Reize gesetzt. Behalte die Erholung im Blick.`);
    } else if (cmp.dAtl < -5) {
      reasons.push(`Ermüdung (ATL) ${fmtSigned(cmp.dAtl)} – deine Müdigkeit sinkt, du wirst frischer. Guter Zeitpunkt für einen neuen Reiz.`);
    }
  }

  // TSB – current form level
  if (Number.isFinite(curr.tsb)) {
    const tsbLabel =
      curr.tsb < -20
        ? "stark negativ – du bist gerade ziemlich müde"
        : curr.tsb < -5
          ? "moderat negativ – produktive Ermüdung, typisch unter Aufbau"
          : curr.tsb <= 10
            ? "ausgeglichen – gutes Gleichgewicht aus Reiz und Erholung"
            : "deutlich positiv – du bist frisch, aber setz auch Reize";
    reasons.push(`Form (TSB): ${fmt(curr.tsb)} – ${tsbLabel}.`);
  }

  // TSB trend – is form improving or declining?
  if (cmp.dTsb != null) {
    if (cmp.dTsb >= 5) {
      reasons.push(`Form-Trend (TSB ${fmtSigned(cmp.dTsb)}) – du wirst von Woche zu Woche frischer.`);
    } else if (cmp.dTsb <= -5) {
      reasons.push(`Form-Trend (TSB ${fmtSigned(cmp.dTsb)}) – deine Frische sinkt, du akkumulierst gerade Müdigkeit.`);
    }
  }

  // Ramp rate – injury risk signal
  if (Number.isFinite(curr.rampRate)) {
    reasons.push(
      curr.rampRate > 8
        ? `Ramp Rate ${fmt(curr.rampRate)}/Woche – du steigerst die Belastung sehr schnell. Achtung: erhöhtes Verletzungsrisiko.`
        : `Ramp Rate ${fmt(curr.rampRate)}/Woche – Belastungssteigerung im grünen Bereich.`,
    );
  }

  // Load change
  if (cmp.pctLoad != null) {
    reasons.push(`Gesamtbelastung ${fmtSigned(cmp.pctLoad, 0)}% gegenüber Vorwoche.`);
  }

  // Session count shift
  if (cmp.dSessionCount != null && cmp.dSessionCount !== 0) {
    reasons.push(
      cmp.dSessionCount > 0
        ? `${fmtSigned(cmp.dSessionCount, 0)} Einheit(en) mehr als letzte Woche – mehr Trainingsimpulse.`
        : `${fmtSigned(cmp.dSessionCount, 0)} Einheit(en) weniger als letzte Woche – weniger Reize gesetzt.`,
    );
  }

  // Load per session – intensity per unit
  if (prev && curr.sessionCount > 0 && prev.sessionCount > 0) {
    const loadPerCurr = curr.loadSum / curr.sessionCount;
    const loadPerPrev = prev.loadSum / prev.sessionCount;
    if (loadPerPrev > 0) {
      const loadPerPct = ((loadPerCurr - loadPerPrev) / loadPerPrev) * 100;
      if (Math.abs(loadPerPct) >= 15) {
        reasons.push(
          loadPerPct > 0
            ? `Belastung pro Einheit ${Math.round(loadPerPct)}% höher als Vorwoche – du trainierst intensiver.`
            : `Belastung pro Einheit ${Math.round(loadPerPct)}% niedriger – lockerere Woche im Schnitt.`,
        );
      }
    }
  }

  return { verdict, score, reasons };
}

function buildRecommendation(verdict, curr) {
  const tsb = curr.tsb;
  const ramp = curr.rampRate;
  const highFatigue = (Number.isFinite(tsb) && tsb < -20) || (Number.isFinite(ramp) && ramp > 8);

  if (verdict === "UNKLAR") {
    return "Zu wenig Daten für eine belastbare Einordnung – z. B. wenige Läufe oder fehlende Wellness-Werte. Sorge nächste Woche für mehr Trainingsdaten und trag deine Wellness-Werte ein.";
  }
  if (verdict === "BESSER") {
    return highFatigue
      ? "Du entwickelst dich gut, aber dein Körper zeigt Ermüdungssignale. Nächste Woche bewusst etwas Gas rausnehmen – Umfang oder Intensität leicht senken. So sicherst du den Fortschritt, statt ihn zu riskieren."
      : "Du bist auf einem guten Weg – weiter so. Du kannst nächste Woche moderat drauflegen: +5–10% Umfang oder eine zusätzliche Qualitätseinheit.";
  }
  if (verdict === "SCHLECHTER") {
    return highFatigue
      ? "Deine Performance sinkt bei gleichzeitig hoher Ermüdung – ein klares Signal, dass du gerade zu viel verlangst. Reduziere Umfang und Intensität diese Woche deutlich (~-20 bis -30%), und priorisiere Schlaf und aktive Erholung."
      : "Deine Leistung sinkt trotz moderater Ermüdung – wahrscheinlich fehlt der nötige Reiz oder die Konsistenz. Hebe Umfang und Intensität wieder leicht an und verteile die Belastung gleichmäßiger über die Woche.";
  }
  // STABIL
  return "Deine Form hält sich im Gleichgewicht. Um den nächsten Schritt zu machen, braucht es einen gezielten Impuls – z. B. ein Tempolauf, eine Schwelleneinheit oder etwas mehr Gesamtumfang nächste Woche.";
}

const VERDICT_LABELS = {
  BESSER: "✅ Besser geworden",
  SCHLECHTER: "🔻 Schlechter geworden",
  STABIL: "➡️ Stabil",
  UNKLAR: "❓ Unklar (zu wenig Daten)",
};

const VERDICT_COLORS = {
  BESSER: "green",
  SCHLECHTER: "red",
  STABIL: "orange",
  UNKLAR: "orange",
};

function buildGoalRaceSection(goalInfo) {
  if (!goalInfo) return null;
  const { distanceLabel, weeksToRace, daysToRace, isPast, schedule, recommendedBlock, prediction } = goalInfo;
  if (isPast) return null;
  const lines = [];
  lines.push("ZIELRENNEN");
  lines.push(`- ${distanceLabel} am ${goalInfo.goal.date} (${weeksToRace} Wochen / ${daysToRace} Tage)`);
  if (prediction) {
    if (prediction.targetTime) {
      const gapStr =
        prediction.gapSecs != null
          ? prediction.faster
            ? `${prediction.gapFormatted} schneller als Ziel ✅`
            : `${prediction.gapFormatted} langsamer als Ziel`
          : "";
      lines.push(`- Zielzeit: ${prediction.targetTime} | Aktuelle Prognose: ${prediction.predictedTime ?? "–"} (${gapStr})`);
    } else {
      lines.push(`- Aktuelle Prognose: ${prediction.predictedTime ?? "–"}`);
    }
  }
  if (recommendedBlock) {
    lines.push(`- Empfohlener Block jetzt: ${recommendedBlock}`);
  }
  lines.push(`- Trainingsplan: BASE ab ${schedule.planStart} → BUILD ab ${schedule.buildStart} → RACE/Taper ab ${schedule.raceStart}`);
  return lines.join("\n");
}

function buildReportText({
  todayIso,
  week,
  prevWeek,
  curr,
  prev,
  cmp,
  verdictResult,
  blockState,
  realVdot,
  paceTargets,
  currRaceTimes,
  prevRaceTimes,
  correctionFactor,
  goalInfo,
}) {
  const lines = [];
  lines.push(`📊 Wochenvergleich – ${VERDICT_LABELS[verdictResult.verdict]}`);
  lines.push(`Woche ${week.start} – ${week.end} vs. Vorwoche ${prevWeek.start} – ${prevWeek.end}`);
  lines.push("");
  lines.push("DEINE WOCHE IM ÜBERBLICK");
  lines.push(`- VDOT (Wochenfenster): ${fmt(prev.vdot)} → ${fmt(curr.vdot)} (${fmtSigned(cmp.dVdot)})`);
  lines.push(`- Fitness (CTL): ${fmt(prev.ctl)} → ${fmt(curr.ctl)} (${fmtSigned(cmp.dCtl)})`);
  lines.push(`- Ermüdung (ATL): ${fmt(prev.atl)} → ${fmt(curr.atl)} (${fmtSigned(cmp.dAtl)})`);
  lines.push(`- Form (TSB): ${fmt(curr.tsb)}`);
  lines.push(`- Ramp Rate: ${fmt(curr.rampRate)}/Woche`);
  lines.push(
    `- Umfang: ${prev.movingTimeMin} → ${curr.movingTimeMin} min (${fmtSigned(cmp.pctMovingTime, 0)}%), ${prev.distanceKm} → ${curr.distanceKm} km`,
  );
  lines.push(`- Load: ${prev.loadSum} → ${curr.loadSum} (${fmtSigned(cmp.pctLoad, 0)}%)`);
  lines.push(`- Einheiten: ${prev.sessionCount} → ${curr.sessionCount} (davon Läufe: ${curr.runSessionCount})`);
  if (Number.isFinite(realVdot)) {
    lines.push(`- Aktueller VDOT (28-Tage-Fenster, geglättet): ${fmt(realVdot)}`);
  }
  if (Number.isFinite(correctionFactor) && correctionFactor !== 1) {
    lines.push(`- Wettkampf-Korrekturfaktor: ${correctionFactor.toFixed(3)} (aus bisherigen Rennergebnissen)`);
  }
  lines.push("");
  if (paceTargets) {
    lines.push("PACEVORGABEN");
    for (const t of paceTargets) {
      lines.push(`- ${t.label}: ${t.pace || "–"}`);
    }
    lines.push("");
  }
  if (currRaceTimes && prevRaceTimes) {
    lines.push("WETTKAMPFPROGNOSE (aus aktuellem VDOT)");
    for (let i = 0; i < currRaceTimes.length; i++) {
      const c = currRaceTimes[i];
      const p = prevRaceTimes[i];
      const dSecs = Number.isFinite(c.seconds) && Number.isFinite(p.seconds) ? c.seconds - p.seconds : null;
      lines.push(`- ${c.label}: ${p.time || "–"} → ${c.time || "–"} (${dSecs != null ? fmtTimeDeltaSeconds(dSecs) : "–"})`);
    }
    lines.push("");
  }
  lines.push("WAS STECKT DAHINTER");
  for (const r of verdictResult.reasons) lines.push(`- ${r}`);
  lines.push("");
  lines.push("DEIN FAHRPLAN FÜR NÄCHSTE WOCHE");
  lines.push(buildRecommendation(verdictResult.verdict, curr));
  if (blockState?.block) {
    lines.push("");
    lines.push(`Block: ${blockState.block}${blockState.startDate ? ` (seit ${blockState.startDate})` : ""}`);
  }
  const goalSection = buildGoalRaceSection(goalInfo);
  if (goalSection) {
    lines.push("");
    lines.push(goalSection);
  }
  return lines.join("\n");
}

async function loadHistory(env) {
  if (!hasKv(env)) return [];
  const raw = await readKvJson(env, historyKvKey(env));
  return Array.isArray(raw) ? raw : [];
}

async function saveHistory(env, history) {
  if (!hasKv(env)) return;
  const trimmed = [...history].sort((a, b) => a.weekStart.localeCompare(b.weekStart)).slice(-MAX_HISTORY_ENTRIES);
  await writeKvJson(env, historyKvKey(env), trimmed);
}

export async function buildWeeklyProgressReport(env, todayIso, options = {}) {
  const { write = false } = options;

  const week = lastCompletedWeek(todayIso);
  const prevWeek = previousWeek(week);

  const activitiesOldest = addDays(prevWeek.start, -MAX_HR_LOOKBACK_DAYS);
  const activities = await fetchIntervalsActivities(env, activitiesOldest, week.end);
  const maxHr = await resolveMaxHr(env, activities);

  const curr = await buildWeekSnapshot(env, activities, week, maxHr);
  const prev = await buildWeekSnapshot(env, activities, prevWeek, maxHr);

  const cmp = compareSnapshots(curr, prev);
  const verdictResult = buildVerdict(cmp, curr, prev);
  const blockState = await readLatestBlockStateKv(env, todayIso).catch(() => null);
  const realVdot = await getCurrentRealVdot(env).catch(() => null);
  const goalRace = await readGoalRace(env).catch(() => null);
  const goalInfo = computeGoalRaceInfo(goalRace, todayIso, realVdot ?? curr.vdot);
  const correctionFactor = await getRaceCorrectionFactor(env).catch(() => 1);
  const paceTargets = paceTargetsFromVdot(realVdot ?? curr.vdot);
  const currRaceTimes = predictRaceTimesFromVdot(realVdot ?? curr.vdot);
  const prevRaceTimes = predictRaceTimesFromVdot(prev.vdot);

  const reportText = buildReportText({
    todayIso,
    week,
    prevWeek,
    curr,
    prev,
    cmp,
    verdictResult,
    blockState,
    realVdot,
    paceTargets,
    currRaceTimes,
    prevRaceTimes,
    correctionFactor,
    goalInfo,
  });

  let note = null;
  if (write) {
    note = await upsertIntervalsNote(env, {
      dayIso: todayIso,
      externalId: `weekly-progress-${week.start}`,
      name: "Wochenvergleich",
      description: reportText,
      color: VERDICT_COLORS[verdictResult.verdict],
    });

    const history = await loadHistory(env);
    const entry = {
      weekStart: week.start,
      weekEnd: week.end,
      vdot: curr.vdot,
      ctl: curr.ctl,
      atl: curr.atl,
      tsb: curr.tsb,
      rampRate: curr.rampRate,
      loadSum: curr.loadSum,
      movingTimeMin: curr.movingTimeMin,
      distanceKm: curr.distanceKm,
      sessionCount: curr.sessionCount,
      verdict: verdictResult.verdict,
      generatedAt: new Date().toISOString(),
    };
    const idx = history.findIndex((h) => h.weekStart === week.start);
    if (idx >= 0) history[idx] = entry;
    else history.push(entry);
    await saveHistory(env, history);
  }

  return {
    ok: true,
    todayIso,
    week,
    prevWeek,
    curr,
    prev,
    comparison: cmp,
    verdict: verdictResult,
    realVdot,
    correctionFactor,
    paceTargets,
    currRaceTimes,
    prevRaceTimes,
    goalInfo,
    reportText,
    note,
  };
}
