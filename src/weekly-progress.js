import { isoDate, parseISODateSafe } from "./date-utils.js";
import { isRun } from "./activity-utils.js";
import { hasKv, readKvJson, writeKvJson, mustEnv } from "./kv.js";
import { fetchIntervalsActivities, fetchIntervalsWellnessDay, upsertIntervalsNote } from "./intervals-client.js";
import { resolveMaxHr, estimateTrainingVdotForWindow } from "./vdot.js";
import { readLatestBlockStateKv } from "./block-phase.js";

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

function activityDay(a) {
  return String(a?.start_date_local || a?.start_date || "").slice(0, 10);
}

function activityLoad(a) {
  const v = Number(a?.icu_training_load ?? a?.training_load ?? a?.load ?? 0);
  return Number.isFinite(v) && v > 0 ? v : 0;
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

// Rule-based verdict: VDOT trend is the primary "did performance improve" signal
// (it's literally derived from pace+HR), CTL trend is the secondary "is training
// building fitness" signal. Both are weighted equally since either can lead.
function buildVerdict(cmp, curr) {
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
  if (cmp.dVdot != null) {
    reasons.push(
      cmp.dVdot >= 0.3
        ? `VDOT ${fmtSigned(cmp.dVdot)} – Lauf-Performance-Signal steigt.`
        : cmp.dVdot <= -0.3
          ? `VDOT ${fmtSigned(cmp.dVdot)} – Lauf-Performance-Signal sinkt.`
          : `VDOT ${fmtSigned(cmp.dVdot)} – praktisch unverändert.`,
    );
  } else {
    reasons.push("VDOT: zu wenige auswertbare Läufe in einer der beiden Wochen für einen Vergleich.");
  }

  if (cmp.dCtl != null) {
    reasons.push(
      cmp.dCtl >= 1
        ? `Fitness/CTL ${fmtSigned(cmp.dCtl)} – Trainingsbelastung baut sinnvoll auf.`
        : cmp.dCtl <= -1
          ? `Fitness/CTL ${fmtSigned(cmp.dCtl)} – Trainingsumfang/-belastung geht zurück.`
          : `Fitness/CTL ${fmtSigned(cmp.dCtl)} – Belastung stabil.`,
    );
  }

  if (Number.isFinite(curr.tsb)) {
    const tsbLabel = curr.tsb < -20 ? "stark negativ – hohe Ermüdung" : curr.tsb < -5 ? "moderat negativ – produktive Ermüdung" : curr.tsb <= 10 ? "ausgeglichen" : "deutlich positiv – frisch/wenig Reiz";
    reasons.push(`Form/TSB: ${fmt(curr.tsb)} (${tsbLabel}).`);
  }

  if (Number.isFinite(curr.rampRate)) {
    reasons.push(
      curr.rampRate > 8
        ? `Ramp Rate ${fmt(curr.rampRate)}/Woche – Belastung steigt sehr schnell (Verletzungsrisiko erhöht).`
        : `Ramp Rate ${fmt(curr.rampRate)}/Woche – im üblichen Rahmen.`,
    );
  }

  if (cmp.pctLoad != null) {
    reasons.push(`Wochenbelastung (Load) ${fmtSigned(cmp.pctLoad, 0)}% gegenüber Vorwoche.`);
  }

  return { verdict, score, reasons };
}

function buildRecommendation(verdict, curr) {
  const tsb = curr.tsb;
  const ramp = curr.rampRate;
  const highFatigue = (Number.isFinite(tsb) && tsb < -20) || (Number.isFinite(ramp) && ramp > 8);

  if (verdict === "UNKLAR") {
    return "Zu wenig Daten für eine belastbare Einordnung (z. B. wenige Läufe oder fehlende Wellness-Werte). Nächste Woche erneut prüfen.";
  }
  if (verdict === "BESSER") {
    return highFatigue
      ? "Performance steigt, aber Ermüdung ist hoch. Nächste Woche bewusst etwas entlasten (Umfang/Intensität leicht senken), um die Form zu sichern, statt sie zu verspielen."
      : "Guter Trend – Belastung kann nächste Woche moderat weiter steigen (z. B. +5–10% Umfang oder ein zusätzlicher Reiz).";
  }
  if (verdict === "SCHLECHTER") {
    return highFatigue
      ? "Performance sinkt bei hoher Ermüdung – klares Überlastungssignal. Diese Woche bewusst entlasten (Umfang ca. -20–30%, keine intensiven Einheiten), Schlaf/Erholung priorisieren."
      : "Performance sinkt trotz moderater Ermüdung – möglicherweise zu wenig Reiz oder Konsistenz. Umfang/Intensität wieder leicht anheben und auf gleichmäßigere Belastung über die Woche achten.";
  }
  return "Form hält sich im Gleichgewicht. Ein gezielter zusätzlicher Reiz (z. B. ein Tempo- oder Schwellenlauf) könnte den nächsten Fortschritt bringen.";
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

function buildReportText({ todayIso, week, prevWeek, curr, prev, cmp, verdictResult, blockState }) {
  const lines = [];
  lines.push(`📊 Wochenvergleich – ${VERDICT_LABELS[verdictResult.verdict]}`);
  lines.push(`Woche ${week.start} – ${week.end} vs. Vorwoche ${prevWeek.start} – ${prevWeek.end}`);
  lines.push("");
  lines.push("KENNZAHLEN");
  lines.push(`- VDOT: ${fmt(prev.vdot)} → ${fmt(curr.vdot)} (${fmtSigned(cmp.dVdot)})`);
  lines.push(`- Fitness (CTL): ${fmt(prev.ctl)} → ${fmt(curr.ctl)} (${fmtSigned(cmp.dCtl)})`);
  lines.push(`- Ermüdung (ATL): ${fmt(prev.atl)} → ${fmt(curr.atl)} (${fmtSigned(cmp.dAtl)})`);
  lines.push(`- Form (TSB): ${fmt(curr.tsb)}`);
  lines.push(`- Ramp Rate: ${fmt(curr.rampRate)}/Woche`);
  lines.push(
    `- Umfang: ${prev.movingTimeMin} → ${curr.movingTimeMin} min (${fmtSigned(cmp.pctMovingTime, 0)}%), ${prev.distanceKm} → ${curr.distanceKm} km`,
  );
  lines.push(`- Load: ${prev.loadSum} → ${curr.loadSum} (${fmtSigned(cmp.pctLoad, 0)}%)`);
  lines.push(`- Einheiten: ${prev.sessionCount} → ${curr.sessionCount} (davon Läufe: ${curr.runSessionCount})`);
  lines.push("");
  lines.push("BEGRÜNDUNG");
  for (const r of verdictResult.reasons) lines.push(`- ${r}`);
  lines.push("");
  lines.push("EMPFEHLUNG");
  lines.push(buildRecommendation(verdictResult.verdict, curr));
  if (blockState?.block) {
    lines.push("");
    lines.push(`Block: ${blockState.block}${blockState.startDate ? ` (seit ${blockState.startDate})` : ""}`);
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
  const maxHr = await resolveMaxHr(env, activities, { write });

  const curr = await buildWeekSnapshot(env, activities, week, maxHr);
  const prev = await buildWeekSnapshot(env, activities, prevWeek, maxHr);

  const cmp = compareSnapshots(curr, prev);
  const verdictResult = buildVerdict(cmp, curr);
  const blockState = await readLatestBlockStateKv(env, todayIso).catch(() => null);

  const reportText = buildReportText({ todayIso, week, prevWeek, curr, prev, cmp, verdictResult, blockState });

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

  return { ok: true, todayIso, week, prevWeek, curr, prev, comparison: cmp, verdict: verdictResult, reportText, note };
}
