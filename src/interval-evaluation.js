/**
 * Interval Evaluation + Progress Tracking
 * --------------------------------------
 * Bewertet Intervall-Sessions auf Basis von intervals.icu Segmentdaten.
 *
 * Wichtige Design-Entscheidungen:
 * - min_speed/max_speed werden NICHT genutzt (zu verrauscht).
 * - Reps werden über average_speed + Dauer + Typ gefiltert.
 * - Mikro-Segmente werden ignoriert.
 */

/** @param {number} x */
function clamp01(x) {
  return Math.max(0, Math.min(1, x));
}

/** @param {number[]} arr */
function mean(arr) {
  if (arr.length === 0) return NaN;
  return arr.reduce((a, b) => a + b, 0) / arr.length;
}

/** @param {number[]} arr */
function std(arr) {
  if (arr.length < 2) return 0;
  const m = mean(arr);
  const v = mean(arr.map((x) => (x - m) * (x - m)));
  return Math.sqrt(v);
}

/** @param {number} speed */
function paceFromSpeed(speed) {
  return 1000 / speed;
}

/** @param {unknown} x */
function safeNum(x) {
  return typeof x === "number" && Number.isFinite(x);
}

/**
 * Lineare Abbildung: x in [a..b] => Score in [1..0] (fallend).
 * @param {number} x
 * @param {number} a
 * @param {number} b
 */
function linearDown(x, a, b) {
  if (x <= a) return 1;
  if (x >= b) return 0;
  return 1 - (x - a) / (b - a);
}

/**
 * @typedef {Object} ICUInterval
 * @property {"WORK"|"RECOVERY"} type
 * @property {number|null} distance
 * @property {number} moving_time
 * @property {number} elapsed_time
 * @property {number|null} average_speed
 * @property {number|null} gap
 * @property {number|null} average_heartrate
 * @property {number|null} average_cadence
 * @property {number|null} average_respiration
 * @property {number|null=} average_temp
 * @property {number|null=} average_gradient
 * @property {number|null=} zone
 * @property {number|null=} intensity
 * @property {string|null=} group_id
 */

/**
 * @typedef {Object} EvalConfig
 * @property {number} hfMax
 * @property {number} repMinSec
 * @property {number} repMaxSec
 * @property {number} repMinSpeed
 * @property {number} microSec
 * @property {[number, number]} racepaceHrFracRange
 * @property {[number, number]} thresholdHrFracRange
 * @property {number} vo2HrFracMin
 * @property {number} cvGood
 * @property {number} cvOk
 * @property {number} cvBad
 * @property {number} fadeOk
 * @property {number} fadeBad
 * @property {number} cadenceDropWarn
 */

/**
 * @typedef {Object} Rep
 * @property {number} idx
 * @property {number} movingSec
 * @property {number} distM
 * @property {number} speed
 * @property {number=} gapSpeed
 * @property {number} paceSecPerKm
 * @property {number=} hr
 * @property {number=} cad
 * @property {number=} resp
 * @property {number=} temp
 */

/** @typedef {"racepace"|"threshold"|"vo2"|"unknown"} Intent */

/**
 * @typedef {Object} SessionScores
 * @property {number} execution
 * @property {number} dose
 * @property {number} strain
 * @property {number} intentMatch
 * @property {number} overall
 * @property {Rep[]} reps
 * @property {number} repCount
 * @property {number} qualityKm
 * @property {number} qualityMin
 * @property {number|null} paceCv
 * @property {number|null} fadePct
 * @property {number|null} hrFracAvg
 * @property {number|null} cadenceDrop
 * @property {"racepace_like"|"threshold_like"|"vo2_like"|"mixed"|"unknown"} intent
 * @property {string[]} notes
 */

/**
 * @param {ICUInterval[]} icu_intervals
 * @param {EvalConfig} cfg
 * @returns {Rep[]}
 */
function extractQualifyingReps(icu_intervals, cfg) {
  const reps = [];

  for (let i = 0; i < icu_intervals.length; i += 1) {
    const seg = icu_intervals[i];
    if (seg.moving_time < cfg.microSec) continue;
    if (seg.type !== "WORK") continue;
    if (!safeNum(seg.average_speed) || seg.average_speed <= 0) continue;
    if (!safeNum(seg.distance) || seg.distance <= 0) continue;
    if (seg.moving_time < cfg.repMinSec) continue;
    if (seg.moving_time > cfg.repMaxSec) continue;
    if (seg.average_speed < cfg.repMinSpeed) continue;

    const speed = seg.average_speed;
    reps.push({
      idx: i,
      movingSec: seg.moving_time,
      distM: seg.distance,
      speed,
      gapSpeed: safeNum(seg.gap) ? seg.gap : undefined,
      paceSecPerKm: paceFromSpeed(speed),
      hr: safeNum(seg.average_heartrate) ? seg.average_heartrate : undefined,
      cad: safeNum(seg.average_cadence) ? seg.average_cadence : undefined,
      resp: safeNum(seg.average_respiration) ? seg.average_respiration : undefined,
      temp: safeNum(seg.average_temp) ? seg.average_temp : undefined,
    });
  }

  return reps;
}

/** @param {Rep[]} reps @param {EvalConfig} cfg */
function scoreExecution(reps, cfg) {
  const notes = [];
  if (reps.length < 2) {
    return { score: 0, paceCv: null, fadePct: null, notes: ["Zu wenige Reps für Execution-Score."] };
  }

  const paces = reps.map((r) => r.paceSecPerKm);
  const m = mean(paces);
  const s = std(paces);
  const cv = m > 0 ? s / m : NaN;
  const fadePct = (reps[reps.length - 1].paceSecPerKm - reps[0].paceSecPerKm) / reps[0].paceSecPerKm;

  let cvScore;
  if (!Number.isFinite(cv)) cvScore = 0;
  else {
    cvScore = linearDown(cv, cfg.cvGood, cfg.cvBad);
    if (cv > cfg.cvOk) notes.push(`Pace-Streuung erhöht (CV ${(cv * 100).toFixed(1)}%).`);
  }

  let fadeScore;
  if (!Number.isFinite(fadePct)) fadeScore = 0;
  else {
    if (fadePct <= 0) fadeScore = 1;
    else fadeScore = linearDown(fadePct, cfg.fadeOk, cfg.fadeBad);
    if (fadePct > cfg.fadeOk) notes.push(`Einbruch am Ende: ${(fadePct * 100).toFixed(1)}% langsamer.`);
  }

  const score = Math.round(100 * clamp01(0.7 * cvScore + 0.3 * fadeScore));
  if (score >= 85) notes.push("Execution: sehr sauber (konstante Pace, kein Zerfall).");
  return { score, paceCv: Number.isFinite(cv) ? cv : null, fadePct: Number.isFinite(fadePct) ? fadePct : null, notes };
}

/** @typedef {{targetKm?: number, targetMin?: number}} DoseTarget */

/** @param {Rep[]} reps @param {DoseTarget} target */
function scoreDose(reps, target) {
  const notes = [];
  const qualityKm = reps.reduce((acc, r) => acc + r.distM / 1000, 0);
  const qualityMin = reps.reduce((acc, r) => acc + r.movingSec / 60, 0);

  let score01;
  if (safeNum(target.targetKm)) {
    const pct = qualityKm / target.targetKm;
    score01 = clamp01(pct);
    notes.push(`Qualität: ${qualityKm.toFixed(2)} km von Ziel ${target.targetKm.toFixed(2)} km (${Math.round(pct * 100)}%).`);
  } else if (safeNum(target.targetMin)) {
    const pct = qualityMin / target.targetMin;
    score01 = clamp01(pct);
    notes.push(`Qualität: ${qualityMin.toFixed(1)} min von Ziel ${target.targetMin.toFixed(1)} min (${Math.round(pct * 100)}%).`);
  } else {
    score01 = clamp01(qualityKm / 3.0);
    notes.push(`Qualität: ${qualityKm.toFixed(2)} km (ohne explizites Ziel → heuristische Bewertung).`);
  }

  return { score: Math.round(100 * score01), qualityKm, qualityMin, notes };
}

/** @param {Rep[]} reps @param {EvalConfig} cfg */
function scoreStrain(reps, cfg) {
  const notes = [];
  if (reps.length < 2) {
    return { score: 50, hrFracAvg: null, cadenceDrop: null, notes: ["Zu wenige Reps für Strain-Analyse."] };
  }

  const cadFirst = reps[0].cad;
  const cadLast = reps[reps.length - 1].cad;
  const cadenceDrop = safeNum(cadFirst) && safeNum(cadLast) ? cadFirst - cadLast : null;

  let cadenceScore = 1;
  if (cadenceDrop !== null) {
    if (cadenceDrop > cfg.cadenceDropWarn) {
      cadenceScore = 0.5;
      notes.push(`Kadenz fällt um ${cadenceDrop.toFixed(1)} spm → möglicher Technik-Zerfall/Overpacing.`);
    } else {
      notes.push("Kadenz stabil → gutes Techniksignal.");
    }
  } else {
    notes.push("Kadenzdaten fehlen/ungenau → Strain weniger sicher.");
  }

  const hrs = reps.map((r) => r.hr).filter(safeNum);
  const hrAvg = hrs.length ? mean(hrs) : NaN;
  const hrFracAvg = Number.isFinite(hrAvg) ? hrAvg / cfg.hfMax : null;

  const resps = reps.map((r) => r.resp).filter(safeNum);
  let respScore = 1;
  if (resps.length >= 2) {
    const respDelta = resps[resps.length - 1] - resps[0];
    if (respDelta > 6) {
      respScore = 0.7;
      notes.push(`Atemfrequenz steigt stark (+${respDelta.toFixed(1)}/min).`);
    }
  }

  const score = Math.round(100 * clamp01(0.6 * cadenceScore + 0.4 * respScore));
  return { score, hrFracAvg, cadenceDrop, notes };
}

/** @param {Rep[]} reps @param {EvalConfig} cfg */
function classifyIntent(reps, cfg) {
  const notes = [];
  const hrs = reps.map((r) => r.hr).filter(safeNum);
  const hrAvg = hrs.length ? mean(hrs) : NaN;
  const hrFracAvg = Number.isFinite(hrAvg) ? hrAvg / cfg.hfMax : null;
  const secAvg = reps.length ? mean(reps.map((r) => r.movingSec)) : NaN;

  if (hrFracAvg === null || !Number.isFinite(secAvg)) {
    return { intent: "unknown", hrFracAvg, notes: ["Zu wenig HR/Rep-Daten für Intent-Klassifikation."] };
  }

  const [rpLo, rpHi] = cfg.racepaceHrFracRange;
  const [thLo, thHi] = cfg.thresholdHrFracRange;
  const isShort = secAvg >= 120 && secAvg <= 360;
  const isLong = secAvg >= 360 && secAvg <= 900;

  let intent = "mixed";
  if (isShort && hrFracAvg >= cfg.vo2HrFracMin) {
    intent = "vo2_like";
    notes.push(`Intent: VO2-ähnlich (Rep Ø ${(secAvg / 60).toFixed(1)} min, HR Ø ${(hrFracAvg * 100).toFixed(1)}% HFmax).`);
  } else if (isLong && hrFracAvg >= thLo && hrFracAvg <= thHi) {
    intent = "threshold_like";
    notes.push(`Intent: Schwelle-ähnlich (lange Reps, HR Ø ${(hrFracAvg * 100).toFixed(1)}% HFmax).`);
  } else if (isShort && hrFracAvg >= rpLo && hrFracAvg <= rpHi) {
    intent = "racepace_like";
    notes.push(`Intent: Racepace-ähnlich (kurze Reps, HR Ø ${(hrFracAvg * 100).toFixed(1)}% HFmax).`);
  } else {
    notes.push(`Intent: gemischt/unklar (Rep Ø ${(secAvg / 60).toFixed(1)} min, HR Ø ${(hrFracAvg * 100).toFixed(1)}% HFmax).`);
  }

  return { intent, hrFracAvg, notes };
}

/** @param {SessionScores["intent"]} classified @param {Intent} planned */
function scoreIntentMatch(classified, planned) {
  if (planned === "unknown") return 50;
  if (planned === "racepace" && classified === "racepace_like") return 100;
  if (planned === "threshold" && classified === "threshold_like") return 100;
  if (planned === "vo2" && classified === "vo2_like") return 100;
  if (classified === "mixed") return 65;
  if (classified === "unknown") return 50;
  return 35;
}

/**
 * @typedef {Object} EvalInput
 * @property {ICUInterval[]} icu_intervals
 * @property {Intent=} plannedIntent
 * @property {DoseTarget=} doseTarget
 */

/** @param {EvalInput} input @param {EvalConfig} cfg @returns {SessionScores} */
function evaluateIntervalsSession(input, cfg) {
  const plannedIntent = input.plannedIntent ?? "unknown";
  const doseTarget = input.doseTarget ?? {};
  const reps = extractQualifyingReps(input.icu_intervals, cfg);

  const ex = scoreExecution(reps, cfg);
  const dose = scoreDose(reps, doseTarget);
  const strain = scoreStrain(reps, cfg);
  const cls = classifyIntent(reps, cfg);
  const intentMatch = scoreIntentMatch(cls.intent, plannedIntent);

  const overall = Math.round(
    100 *
      clamp01(
        0.35 * (ex.score / 100) +
          0.25 * (dose.score / 100) +
          0.2 * (strain.score / 100) +
          0.2 * (intentMatch / 100)
      )
  );

  return {
    execution: ex.score,
    dose: dose.score,
    strain: strain.score,
    intentMatch,
    overall,
    reps,
    repCount: reps.length,
    qualityKm: dose.qualityKm,
    qualityMin: dose.qualityMin,
    paceCv: ex.paceCv,
    fadePct: ex.fadePct,
    hrFracAvg: cls.hrFracAvg,
    cadenceDrop: strain.cadenceDrop,
    intent: cls.intent,
    notes: [...ex.notes, ...dose.notes, ...strain.notes, ...cls.notes],
  };
}

/**
 * @typedef {Object} ProgressPoint
 * @property {string} dateISO
 * @property {string} activityId
 * @property {Intent} intent
 * @property {number} repSecAvg
 * @property {number} qualityKm
 * @property {number|null} effSpeedPerBpm
 * @property {number|null} costBpmAtSpeed
 * @property {number} execution
 * @property {number} overall
 */

/** @param {Intent} intent @param {number} repSecAvg @param {number} qualityKm */
function buildComparableKey(intent, repSecAvg, qualityKm) {
  const repBin = Math.round(repSecAvg / 30) * 30;
  const kmBin = Math.round(qualityKm / 0.5) * 0.5;
  return `${intent}|rep${repBin}|km${kmBin}`;
}

/** @param {string} dateISO @param {string} activityId @param {Intent} plannedIntent @param {SessionScores} scores */
function toProgressPoint(dateISO, activityId, plannedIntent, scores) {
  const repSecAvg = scores.reps.length ? mean(scores.reps.map((r) => r.movingSec)) : 0;
  const speeds = scores.reps.map((r) => (safeNum(r.gapSpeed) ? r.gapSpeed : r.speed));
  const speedAvg = speeds.length ? mean(speeds) : NaN;
  const hrs = scores.reps.map((r) => r.hr).filter(safeNum);
  const hrAvg = hrs.length ? mean(hrs) : NaN;

  const eff = Number.isFinite(speedAvg) && Number.isFinite(hrAvg) && hrAvg > 0 ? speedAvg / hrAvg : null;

  return {
    dateISO,
    activityId,
    intent: plannedIntent,
    repSecAvg,
    qualityKm: scores.qualityKm,
    effSpeedPerBpm: eff,
    costBpmAtSpeed: null,
    execution: scores.execution,
    overall: scores.overall,
  };
}

/** @param {number[]} xs */
function median(xs) {
  if (!xs.length) return null;
  const s = [...xs].sort((a, b) => a - b);
  const mid = Math.floor(s.length / 2);
  return s.length % 2 ? s[mid] : (s[mid - 1] + s[mid]) / 2;
}

/** @param {ProgressPoint[]} points @param {number=} lookbackN */
function summarizeProgress(points, lookbackN = 4) {
  const grouped = {};
  for (const p of points) {
    const key = buildComparableKey(p.intent, p.repSecAvg, p.qualityKm);
    (grouped[key] ??= []).push(p);
  }

  const byKey = {};
  for (const [key, arr] of Object.entries(grouped)) {
    arr.sort((a, b) => a.dateISO.localeCompare(b.dateISO));
    const last = arr[arr.length - 1] ?? null;

    const effArr = arr.map((x) => x.effSpeedPerBpm).filter(safeNum);
    const execArr = arr.map((x) => x.execution).filter(safeNum);

    const effN = effArr.slice(-lookbackN);
    const effPrev = effArr.slice(-2 * lookbackN, -lookbackN);
    const execN = execArr.slice(-lookbackN);
    const execPrev = execArr.slice(-2 * lookbackN, -lookbackN);

    const medEff = median(effN);
    const medEffPrev = median(effPrev);
    const medExec = median(execN);
    const medExecPrev = median(execPrev);

    byKey[key] = {
      trendEff: medEff !== null && medEffPrev !== null ? medEff - medEffPrev : null,
      trendExec: medExec !== null && medExecPrev !== null ? medExec - medExecPrev : null,
      last,
    };
  }

  return { byKey };
}

const DEFAULT_EVAL_CFG = {
  hfMax: 173,
  repMinSec: 90,
  repMaxSec: 900,
  repMinSpeed: 2.8,
  microSec: 15,
  racepaceHrFracRange: [0.84, 0.92],
  thresholdHrFracRange: [0.82, 0.9],
  vo2HrFracMin: 0.9,
  cvGood: 0.02,
  cvOk: 0.04,
  cvBad: 0.07,
  fadeOk: 0.02,
  fadeBad: 0.05,
  cadenceDropWarn: 3,
};

export {
  DEFAULT_EVAL_CFG,
  buildComparableKey,
  classifyIntent,
  evaluateIntervalsSession,
  extractQualifyingReps,
  scoreDose,
  scoreExecution,
  scoreIntentMatch,
  scoreStrain,
  summarizeProgress,
  toProgressPoint,
};
