// src/index.js
// Cloudflare Worker – Run only
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom numeric fields (create these in Intervals):
// VDOT, Drift, Motor
//
// Daily behavior:
// - Writes a wellness comment for EVERY day in range (even if no run).
// - Minimum stimulus block is ALWAYS included in the comment.
// - Monday detective is written as a CALENDAR NOTE (category NOTE), not in comments.
//   It is created/updated even if no run on Monday.
//
// GA (no key:*, >=30min):
// - VDOT_like from EF = avg_speed/avg_hr
// - Drift from streams (warmup skip default 10min)
// - Negative drift => null (dropped)
//
// Motor Index:
// - GA comparable only (no key, >=35min, steady pace)
// - EF trend (28d) + Drift trend (14d), mapped to 0..100
//
// Bench reports (only on Bench days):
// - Use tag "bench:<name>" or "Bench:<name>" (case-insensitive).
// - On Bench day, posts a short report comparing:
//     - today vs last same bench
//     - today vs median of last 3–5 same bench
//
// URL:
//   /sync?date=YYYY-MM-DD&write=true&debug=true
//   /sync?days=14&write=true&debug=true
//   /sync?from=YYYY-MM-DD&to=YYYY-MM-DD&write=true&debug=true
// Optional:
//   &warmup_skip=600

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") return new Response("ok");

    if (url.pathname === "/sync") {
      const write = (url.searchParams.get("write") || "").toLowerCase() === "true";
      const debug = (url.searchParams.get("debug") || "").toLowerCase() === "true";

      const date = url.searchParams.get("date");
      const from = url.searchParams.get("from");
      const to = url.searchParams.get("to");
      const days = clampInt(url.searchParams.get("days") ?? "14", 1, 31);

      const warmupSkipSec = clampInt(url.searchParams.get("warmup_skip") ?? "600", 0, 1800);

      let oldest, newest;
      if (date) {
        oldest = date;
        newest = date;
      } else if (from && to) {
        oldest = from;
        newest = to;
      } else {
        newest = isoDate(new Date());
        oldest = isoDate(new Date(Date.now() - days * 86400000));
      }

      if (!isIsoDate(oldest) || !isIsoDate(newest)) {
        return json({ ok: false, error: "Invalid date format (YYYY-MM-DD)" }, 400);
      }
      if (newest < oldest) {
        return json({ ok: false, error: "`to` must be >= `from`" }, 400);
      }
      if (diffDays(oldest, newest) > 31) {
        return json({ ok: false, error: "Max range is 31 days" }, 400);
      }

      if (debug) {
        try {
          const result = await syncRange(env, oldest, newest, write, true, warmupSkipSec);
          return json(result);
        } catch (e) {
          return json(
            {
              ok: false,
              error: "Worker exception",
              message: String(e?.message ?? e),
              stack: String(e?.stack ?? ""),
              oldest,
              newest,
              write,
              warmupSkipSec,
            },
            500
          );
        }
      }

      ctx?.waitUntil?.(syncRange(env, oldest, newest, write, false, warmupSkipSec).catch(() => {}));
      return json({ ok: true, oldest, newest, write, warmupSkipSec });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // Daily sync so you always get at least the minimum-stimulus comment.
    ctx.waitUntil(
      syncRange(
        env,
        isoDate(new Date(Date.now() - 14 * 86400000)),
        isoDate(new Date()),
        true,
        false,
        600
      ).catch(() => {})
    );
  },
};

// ================= CONFIG =================
const GA_MIN_SECONDS = 30 * 60;
const GA_COMPARABLE_MIN_SECONDS = 35 * 60;
const MOTOR_STALE_DAYS = 5;
const MIN_STIMULUS_7D_RUN_LOAD = 150;

const TREND_WINDOW_DAYS = 28;
const TREND_MIN_N = 3;

const MOTOR_WINDOW_DAYS = 28;
const MOTOR_NEED_N_PER_HALF = 2;
const MOTOR_DRIFT_WINDOW_DAYS = 14;

const HFMAX = 173;

// "Trainingslehre" detective
const LONGRUN_MIN_SECONDS = 60 * 60; // FIX >= 60 minutes (your choice)
const DETECTIVE_WINDOWS = [14, 28, 42, 56, 84]; // adaptive
const DETECTIVE_MIN_RUNS = 3; // minimum runs to say something meaningful
const DETECTIVE_MIN_WEEKS = 2; // for weekly-rate interpretation

const MIN_RUN_SPEED = 1.8;
const MIN_POINTS = 300;
const GA_SPEED_CV_MAX = 0.10;

// Bench
const BENCH_LOOKBACK_DAYS = 180;
const BENCH_MAX_HISTORY = 5;
const BENCH_MIN_FOR_MEDIAN = 3;

// Wellness field codes
const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_MOTOR = "Motor";

// ================= MAIN =================
async function syncRange(env, oldest, newest, write, debug, warmupSkipSec) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);
const notesPreview = debug ? {} : null;
  const byDay = new Map();
  const debugOut = debug ? {} : null;

  let activitiesSeen = 0;
  let activitiesUsed = 0;

  for (const a of acts) {
    activitiesSeen++;
    const day = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!day) {
      if (debug) addDebug(debugOut, day || "unknown-day", a, "skip:no_day", null);
      continue;
    }
    if (!isRun(a)) {
      if (debug) addDebug(debugOut, day, a, `skip:not_run:${a.type ?? "unknown"}`, null);
      continue;
    }
    if (!byDay.has(day)) byDay.set(day, []);
    byDay.get(day).push(a);
    activitiesUsed++;
  }

  const patches = {};
  let daysWritten = 0;

  // Iterate *every day* in range (even if no run)
  const daysList = listIsoDaysInclusive(oldest, newest);

  for (const day of daysList) {
    const runs = byDay.get(day) ?? [];
    const patch = {};
    const perRunInfo = [];

    // Motor Index (works even if no run today)
    let motor = null;
    try {
      motor = await computeMotorIndex(env, day, warmupSkipSec);
      if (motor?.value != null) patch[FIELD_MOTOR] = round(motor.value, 1);
    } catch (e) {
      motor = { ok: false, value: null, text: `🏎️ Motor-Index: n/a – Fehler (${String(e?.message ?? e)})` };
    }

    // Process runs
    for (const a of runs) {
      const isKey = hasKeyTag(a);
      const ga = isGA(a);

      const ef = extractEF(a);
      const load = extractLoad(a);

      let drift = null;
      let drift_raw = null;
      let drift_source = "none";

      if (ga && !isKey) {
        drift_source = "streams";
        try {
          const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
          const ds = computeDriftAndStabilityFromStreams(streams, warmupSkipSec);
          drift_raw = Number.isFinite(ds?.hr_drift_pct) ? ds.hr_drift_pct : null;
          drift = drift_raw;

          // Negative drift => null (dropped)
          if (drift != null && drift < 0) {
            drift = null;
            drift_source = "streams_negative_dropped";
          }

          if (drift == null && drift_source === "streams") drift_source = "streams_insufficient";
        } catch (e) {
          drift = null;
          drift_source = "streams_failed";
          if (debug) addDebug(debugOut, day, a, "warn:streams_failed", { message: String(e?.message ?? e) });
        }
      }

      // Write GA fields
      if (ga && !isKey) {
        if (ef != null) patch[FIELD_VDOT] = round(vdotLikeFromEf(ef), 1);
        if (drift != null) patch[FIELD_DRIFT] = round(drift, 1);
      }

      perRunInfo.push({
        activityId: a.id,
        type: a.type,
        tags: a.tags ?? [],
        ga,
        isKey,
        ef,
        drift,
        drift_raw,
        drift_source,
        load,
        moving_time: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
      });

      if (debug) {
        addDebug(debugOut, day, a, "ok", {
          ga,
          isKey,
          ef,
          drift,
          drift_raw,
          drift_source,
          load,
        });
      }
    }

    // Aerobic trend
    let trend;
    try {
      trend = await computeAerobicTrend(env, day, warmupSkipSec);
    } catch (e) {
      trend = { ok: false, text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – Fehler (${String(e?.message ?? e)})` };
    }

    // Minimum stimulus (always)
    let min;
    try {
      min = await computeMinStimulus(env, day);
    } catch {
      min = { runLoad7: 0, minOk: false };
    }

    // Bench reports only on bench days
    const benchReports = [];
    for (const a of runs) {
      const benchName = getBenchTag(a); // case-insensitive "bench:"
      if (!benchName) continue;

      try {
        const rep = await computeBenchReport(env, a, benchName, warmupSkipSec);
        if (rep) benchReports.push(rep);
      } catch (e) {
        benchReports.push(`🧪 bench:${benchName}\nFehler: ${String(e?.message ?? e)}`);
      }
    }

    // Daily comment ALWAYS (includes min stimulus ALWAYS)
    patch.comments = renderWellnessComment({
      perRunInfo,
      trend,
      motor,
      runLoad7: min.runLoad7,
      minOk: min.minOk,
      benchReports,
    });

    patches[day] = patch;

    // Monday detective NOTE (calendar) – always on Mondays, even if no run
    let detectiveNoteText = null;
    if (isMondayIso(day)) {
      try {
        detectiveNoteText = await computeDetectiveNoteAdaptive(env, day, warmupSkipSec);
        if (write) {
          await upsertMondayDetectiveNote(env, day, detectiveNoteText);
        }
      } catch (e) {
        detectiveNoteText = `🕵️‍♂️ Montags-Detektiv\nFehler: ${String(e?.message ?? e)}`;
        if (write) {
          await upsertMondayDetectiveNote(env, day, detectiveNoteText);
        }
      }
      if (debug) {
        // surface in debug response (not in comments)
        if (debug) notesPreview[day] = detectiveNoteText;

      }
    }

    if (write) {
      await putWellnessDay(env, day, patch);
      daysWritten++;
    }
  }

  return {
    ok: true,
    oldest,
    newest,
    write,
    notesPreview: debug ? notesPreview : undefined,
    activitiesSeen,
    activitiesUsed,
    daysComputed: Object.keys(patches).length,
    daysWritten,
    patches: debug ? patches : undefined,
    debug: debug ? debugOut : undefined,
  };
}

// ================= COMMENT =================
function renderWellnessComment({ perRunInfo, trend, motor, runLoad7, minOk, benchReports }) {
  const hadKey = perRunInfo.some((x) => x.isKey);
  const hadGA = perRunInfo.some((x) => x.ga && !x.isKey);
  const hadAnyRun = perRunInfo.length > 0;

  const lines = [];
  lines.push("ℹ️ Tages-Status");
  lines.push("");

  if (!hadAnyRun) lines.push("Heute: Kein Lauf");
  else if (hadKey && !hadGA) lines.push("Heute: Schlüsseltraining (Key)");
  else if (hadGA && !hadKey) lines.push("Heute: Grundlage (GA)");
  else if (hadKey && hadGA) lines.push("Heute: Gemischt (GA + Key)");
  else lines.push("Heute: Lauf");

  lines.push("");
  lines.push(trend?.text ?? "ℹ️ Aerober Kontext (nur GA)\nTrend: n/a");

  lines.push("");
  lines.push(motor?.text ?? "🏎️ Motor-Index: n/a");

  if (Array.isArray(benchReports) && benchReports.length) {
    lines.push("");
    lines.push(benchReports.join("\n\n"));
  }

  // ALWAYS minimum stimulus block
  lines.push("");
  if (minOk) {
    lines.push("ℹ️ Mindest-Laufreiz erreicht");
    lines.push(`7-Tage Lauf-Load ≥ ${MIN_STIMULUS_7D_RUN_LOAD} (${Math.round(runLoad7)})`);
  } else {
    lines.push("ℹ️ Mindest-Laufreiz unterschritten");
    lines.push(`7-Tage Lauf-Load < ${MIN_STIMULUS_7D_RUN_LOAD} (${Math.round(runLoad7)})`);
    lines.push("➡️ Kurzfristig ok – langfristig kein Aufbau.");
  }

  return lines.join("\n");
}

// ================= TREND (GA-only) =================
async function computeAerobicTrend(env, dayIso, warmupSkipSec) {
  const end = new Date(dayIso + "T00:00:00Z");
  const mid = new Date(end.getTime() - TREND_WINDOW_DAYS * 86400000);
  const start = new Date(end.getTime() - 2 * TREND_WINDOW_DAYS * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const gaActs = [];

  for (const a of acts) {
    if (!isRun(a)) continue;
    if (hasKeyTag(a)) continue;
    if (!isGA(a)) continue;

    const ef = extractEF(a);
    if (ef == null) continue;

    let drift = null;
    try {
      const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
      const ds = computeDriftAndStabilityFromStreams(streams, warmupSkipSec);
      drift = Number.isFinite(ds?.hr_drift_pct) ? ds.hr_drift_pct : null;
      if (drift != null && drift < 0) drift = null;
    } catch {
      drift = null;
    }
    if (drift == null) continue;

    gaActs.push({ a, ef, drift });
  }

  const recent = gaActs.filter((x) => new Date(x.a.start_date_local || x.a.start_date) >= mid);
  const prev = gaActs.filter((x) => new Date(x.a.start_date_local || x.a.start_date) < mid);

  if (recent.length < TREND_MIN_N || prev.length < TREND_MIN_N) {
    return {
      ok: false,
      text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – zu wenig GA-Daten (recent=${recent.length}, prev=${prev.length})`,
    };
  }

  const ef1 = avg(recent.map((x) => x.ef));
  const ef0 = avg(prev.map((x) => x.ef));
  const d1 = median(recent.map((x) => x.drift));
  const d0 = median(prev.map((x) => x.drift));

  if (ef0 == null || ef1 == null || d0 == null || d1 == null) {
    return { ok: false, text: "ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – fehlende Werte" };
  }

  const dv = ((ef1 - ef0) / ef0) * 100;
  const dd = d1 - d0;

  let emoji = "🟡";
  let label = "Stabil / gemischt";
  if (dv > 1.5 && dd <= 0) {
    emoji = "🟢";
    label = "Aerober Fortschritt";
  } else if (dv < -1.5 && dd > 1) {
    emoji = "🔴";
    label = "Warnsignal";
  }

  return {
    ok: true,
    dv,
    dd,
    text:
      `${emoji} ${label}\n` +
      `Aerober Kontext (nur GA): VDOT ~ ${dv.toFixed(1)}% | HR-Drift ${dd > 0 ? "↑" : "↓"} ${Math.abs(dd).toFixed(
        1
      )}%-Pkt`,
  };
}

// ================= MOTOR INDEX (GA comparable only) =================
async function computeMotorIndex(env, dayIso, warmupSkipSec) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - 2 * MOTOR_WINDOW_DAYS * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const samples = [];

  for (const a of acts) {
    if (!isRun(a)) continue;
    if (hasKeyTag(a)) continue;
    if (!isGAComparable(a)) continue;

    const ef = extractEF(a);
    if (ef == null) continue;

    let drift = null;
    let cv = null;
    try {
      const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
      const ds = computeDriftAndStabilityFromStreams(streams, warmupSkipSec);
      drift = Number.isFinite(ds?.hr_drift_pct) ? ds.hr_drift_pct : null;
      cv = Number.isFinite(ds?.speed_cv) ? ds.speed_cv : null;
      if (drift != null && drift < 0) drift = null;
    } catch {
      drift = null;
      cv = null;
    }

    if (drift == null) continue;
    if (cv == null || cv > GA_SPEED_CV_MAX) continue;

    const date = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!date) continue;

    samples.push({ date, ef, drift });
  }
  // --- Option 1: Wenn keine aktuellen comparable GA-Daten, dann Motor = n/a ---
  const lastDate = samples.length ? samples.map((s) => s.date).sort().at(-1) : null;
  if (!lastDate) {
    return {
      ok: false,
      value: null,
      text: "🏎️ Motor-Index: n/a (keine vergleichbaren GA-Läufe im Fenster)",
    };
  }
  const ageDays = diffDays(lastDate, dayIso);
  if (ageDays > MOTOR_STALE_DAYS) {
    return {
      ok: false,
      value: null,
      text: `🏎️ Motor-Index: n/a (letzter vergleichbarer GA-Lauf vor ${ageDays} Tagen: ${lastDate})`,
    };
  }

  const mid = new Date(end.getTime() - MOTOR_WINDOW_DAYS * 86400000);
  const recent = samples.filter((x) => new Date(x.date + "T00:00:00Z") >= mid);
  const prev = samples.filter((x) => new Date(x.date + "T00:00:00Z") < mid);

  if (recent.length < MOTOR_NEED_N_PER_HALF || prev.length < MOTOR_NEED_N_PER_HALF) {
    return {
      ok: false,
      value: null,
      text: `🏎️ Motor-Index: n/a (zu wenig vergleichbare GA-Läufe: recent=${recent.length}, prev=${prev.length})`,
    };
  }

  const ef1 = median(recent.map((x) => x.ef));
  const ef0 = median(prev.map((x) => x.ef));
  if (ef0 == null || ef1 == null) {
    return { ok: false, value: null, text: "🏎️ Motor-Index: n/a (fehlende EF-Werte)" };
  }

  // Drift trend uses last 14d vs previous 14d within the last 28d
  const mid14 = new Date(end.getTime() - MOTOR_DRIFT_WINDOW_DAYS * 86400000);
  const recent14 = samples.filter((x) => new Date(x.date + "T00:00:00Z") >= mid14);
  const prev14 = samples.filter((x) => {
    const t = new Date(x.date + "T00:00:00Z");
    return t < mid14 && t >= new Date(end.getTime() - 2 * MOTOR_DRIFT_WINDOW_DAYS * 86400000);
  });

  const d1 = recent14.length ? median(recent14.map((x) => x.drift)) : null;
  const d0 = prev14.length ? median(prev14.map((x) => x.drift)) : null;

  const dv = ((ef1 - ef0) / ef0) * 100; // + good
  const dd = d0 != null && d1 != null ? d1 - d0 : null; // + bad

  // Map to 0..100
  let val = 50;
  val += clamp(dv, -6, 6) * 4; // EF: +/-6% -> +/-24 pts
  if (dd != null) val += clamp(-dd, -6, 6) * 2; // drift down -> plus

  val = clamp(val, 0, 100);

  const arrow = dv > 0.5 ? "↑" : dv < -0.5 ? "↓" : "→";
  const label = val >= 70 ? "stark" : val >= 55 ? "stabil" : val >= 40 ? "fragil" : "schwach";
  const extra = dd == null ? "" : ` | Drift Δ ${dd > 0 ? "+" : ""}${dd.toFixed(1)}%-Pkt (14d)`;

  return {
    ok: true,
    value: val,
    text: `🏎️ Motor-Index: ${val.toFixed(0)}/100 (${label}) ${arrow} | EF Δ ${dv.toFixed(1)}% (28d)${extra}`,
  };
}

// ================= MINIMUM STIMULUS =================
async function computeMinStimulus(env, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - 7 * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const runLoad7 = acts.filter((a) => isRun(a)).reduce((s, a) => s + extractLoad(a), 0);

  return { runLoad7, minOk: runLoad7 >= MIN_STIMULUS_7D_RUN_LOAD };
}

// ================= MONDAY DETECTIVE NOTE (TRAININGSLEHRE V2) =================
async function computeDetectiveNoteAdaptive(env, mondayIso, warmupSkipSec) {
  for (const w of DETECTIVE_WINDOWS) {
    const rep = await computeDetectiveNote(env, mondayIso, warmupSkipSec, w);
    if (rep.ok) return rep.text;
  }
  // fallback: last attempt (most info)
  const last = await computeDetectiveNote(env, mondayIso, warmupSkipSec, DETECTIVE_WINDOWS[DETECTIVE_WINDOWS.length - 1]);
  return last.text;
}

async function computeDetectiveNote(env, mondayIso, warmupSkipSec, windowDays) {
  const end = new Date(mondayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - windowDays * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const runs = acts.filter((a) => isRun(a)).map((a) => ({
    id: a.id,
    date: String(a.start_date_local || a.start_date || "").slice(0, 10),
    moving_time: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    load: extractLoad(a),
    isKey: hasKeyTag(a),
    keyType: getKeyType(a),
    isGA: !hasKeyTag(a) && Number(a?.moving_time ?? a?.elapsed_time ?? 0) >= GA_MIN_SECONDS,
    isLong: Number(a?.moving_time ?? a?.elapsed_time ?? 0) >= LONGRUN_MIN_SECONDS,
    avgHR: Number(a?.average_heartrate),
    ef: extractEF(a),
  })).filter((x) => x.date);

  const weeks = Math.max(1, windowDays / 7);

  // Distribution stats
  const totalRuns = runs.length;
  const totalMin = sum(runs.map((x) => x.moving_time)) / 60;
  const totalLoad = sum(runs.map((x) => x.load));

  const longRuns = runs.filter((x) => x.isLong);
  const keyRuns = runs.filter((x) => x.isKey);
  const gaRuns = runs.filter((x) => x.isGA && !x.isKey);
  const shortRuns = runs.filter((x) => x.moving_time > 0 && x.moving_time < GA_MIN_SECONDS);

  const longPerWeek = longRuns.length / weeks;
  const keyPerWeek = keyRuns.length / weeks;
  const runsPerWeek = totalRuns / weeks;

  // Monotony/strain (simple)
  const dailyLoads = bucketLoadsByDay(runs); // {day: loadSum}
  const loadArr = Object.values(dailyLoads);
  const meanLoad = avg(loadArr) ?? 0;
  const sdLoad = std(loadArr) ?? 0;
  const monotony = sdLoad > 0 ? meanLoad / sdLoad : (meanLoad > 0 ? 99 : 0);
  const strain = monotony * sum(loadArr);

  // Optional: comparable GA evidence (EF/Drift)
  const comp = await gatherComparableGASamples(env, mondayIso, warmupSkipSec, windowDays);
  // comp: { n, efMed, driftMed, droppedNegCount, cvTooHighCount, insufficientCount }

  // Findings (Trainingslehre)
  const findings = [];
  const actions = [];

  // Absolute: too little training
  if (totalRuns === 0) {
    findings.push("Kein Lauf im Analysefenster → keine belastbare Diagnose möglich.");
    actions.push("Starte mit 2–3 lockeren Läufen/Woche (30–50min), bevor du harte Schlüsse ziehst.");
  } else {
    // Longrun
    if (longRuns.length === 0) {
      findings.push(`Zu wenig Longruns: 0× ≥60min in ${windowDays} Tagen.`);
      actions.push("1×/Woche Longrun ≥60–75min (locker) als Basisbaustein.");
    } else if (longPerWeek < 0.8 && windowDays >= 14) {
      findings.push(`Longrun-Frequenz niedrig: ${longRuns.length}× in ${windowDays} Tagen (~${longPerWeek.toFixed(1)}/Woche).`);
      actions.push("Longrun-Frequenz Richtung 1×/Woche stabilisieren.");
    }

    // Key
    if (keyRuns.length === 0) {
      findings.push(`Zu wenig Qualität: 0× Key (key:*) in ${windowDays} Tagen.`);
      actions.push("Wenn Aufbau/Spezifisch: 1× Key/Woche (Schwelle ODER VO2) einbauen.");
    } else if (keyPerWeek < 0.6 && windowDays >= 14) {
      findings.push(`Key-Frequenz niedrig: ${keyRuns.length}× in ${windowDays} Tagen (~${keyPerWeek.toFixed(1)}/Woche).`);
      actions.push("Key-Frequenz auf 1×/Woche anheben (wenn Regeneration ok).");
    }

    // Volume / frequency
    if (runsPerWeek < 2.0 && windowDays >= 14) {
      findings.push(`Lauffrequenz niedrig: Ø ${runsPerWeek.toFixed(1)}/Woche.`);
      actions.push("Wenn möglich: erst Frequenz hoch (kurze easy Läufe), dann Intensität.");
    }

    // Too many shorts (no base)
    const shortShare = totalRuns ? (shortRuns.length / totalRuns) * 100 : 0;
    if (shortRuns.length >= 3 && shortShare >= 45) {
      findings.push(`Viele kurze Läufe (<30min): ${shortRuns.length}/${totalRuns} (${shortShare.toFixed(0)}%).`);
      actions.push("Mind. 2 Einheiten/Woche auf 35–50min verlängern (ruhig).");
    }
  }

  // Load-based “minimum stimulus” insight
  // (We don't re-use the 7d load from wellness; compute 28d mean weekly load here)
  const weeklyLoad = totalLoad / weeks;
  if (windowDays >= 14) {
    if (weeklyLoad < 120) {
      findings.push(`Wöchentlicher Laufreiz niedrig: ~${Math.round(weeklyLoad)}/Woche (Load).`);
      actions.push("Motor-Aufbau braucht Kontinuität: 2–4 Wochen stabilen Reiz setzen, erst dann bewerten.");
    }
  }

  // Comparable GA evidence
  if (comp.n > 0) {
    findings.push(`Messbasis (GA comparable): n=${comp.n} | EF(med)=${comp.efMed != null ? comp.efMed.toFixed(5) : "n/a"} | Drift(med)=${comp.driftMed != null ? comp.driftMed.toFixed(1) + "%" : "n/a"}`);
    if (comp.droppedNegCount > 0) findings.push(`Hinweis: negative Drift verworfen: ${comp.droppedNegCount}× (Sensor/Stop&Go möglich).`);
  } else {
    findings.push("GA comparable: keine/zu wenig saubere Läufe → EF/Drift-Belege schwach (Trend/Signal fragil).");
    actions.push("Für Diagnose: 1×/Woche steady GA 45–60min (oder bench:GA45) auf möglichst ähnlicher Strecke.");
  }

  // Key type distribution (if tagged)
  const keyTypeCounts = countBy(keyRuns.map((x) => x.keyType).filter(Boolean));
  const keyTypeLine = Object.keys(keyTypeCounts).length
    ? `Key-Typen: ${Object.entries(keyTypeCounts).map(([k, v]) => `${k}=${v}`).join(", ")}`
    : "Key-Typen: n/a (keine key:<type> Untertags genutzt)";

  // Compose note
  const title = `🕵️‍♂️ Montags-Detektiv (${windowDays}T)`;
  const lines = [];
  lines.push(title);
  lines.push("");
  lines.push("Struktur (Trainingslehre):");
  lines.push(`- Läufe: ${totalRuns} (Ø ${runsPerWeek.toFixed(1)}/Woche)`);
  lines.push(`- Minuten: ${Math.round(totalMin)} | Load: ${Math.round(totalLoad)} (~${Math.round(weeklyLoad)}/Woche)`);
  lines.push(`- Longruns (≥60min): ${longRuns.length} (Ø ${longPerWeek.toFixed(1)}/Woche)`);
  lines.push(`- Key (key:*): ${keyRuns.length} (Ø ${keyPerWeek.toFixed(1)}/Woche)`);
  lines.push(`- GA (≥30min, nicht key): ${gaRuns.length}`);
  lines.push(`- Kurz (<30min): ${shortRuns.length}`);
  lines.push(`- ${keyTypeLine}`);
  lines.push("");
  lines.push("Belastungsbild:");
  lines.push(`- Monotony: ${isFiniteNumber(monotony) ? monotony.toFixed(2) : "n/a"} | Strain: ${isFiniteNumber(strain) ? strain.toFixed(0) : "n/a"}`);
  lines.push("");

  lines.push("Fundstücke:");
  if (!findings.length) lines.push("- Keine klaren strukturellen Probleme gefunden.");
  else for (const f of findings.slice(0, 8)) lines.push(`- ${f}`);

  lines.push("");
  lines.push("Nächste Schritte:");
  if (!actions.length) lines.push("- Struktur beibehalten, Bench/GA comparable weiter sammeln.");
  else for (const a of uniq(actions).slice(0, 8)) lines.push(`- ${a}`);

  // ok criteria: enough runs OR strong structural issue
  const ok = totalRuns >= DETECTIVE_MIN_RUNS || longRuns.length === 0 || weeklyLoad < 120;

  return { ok, text: lines.join("\n") };
}

async function gatherComparableGASamples(env, endDayIso, warmupSkipSec, windowDays) {
  const end = new Date(endDayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - windowDays * 86400000);
  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));

  let droppedNegCount = 0;
  let cvTooHighCount = 0;
  let insufficientCount = 0;

  const samples = [];

  for (const a of acts) {
    if (!isRun(a)) continue;
    if (hasKeyTag(a)) continue;
    if (!isGAComparable(a)) continue;

    const ef = extractEF(a);
    if (ef == null) continue;

    try {
      const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
      const ds = computeDriftAndStabilityFromStreams(streams, warmupSkipSec);
      let drift = Number.isFinite(ds?.hr_drift_pct) ? ds.hr_drift_pct : null;
      const cv = Number.isFinite(ds?.speed_cv) ? ds.speed_cv : null;

      if (drift == null || cv == null) {
        insufficientCount++;
        continue;
      }
      if (drift < 0) {
        droppedNegCount++;
        continue;
      }
      if (cv > GA_SPEED_CV_MAX) {
        cvTooHighCount++;
        continue;
      }

      samples.push({ ef, drift });
    } catch {
      insufficientCount++;
    }
  }

  return {
    n: samples.length,
    efMed: samples.length ? median(samples.map((x) => x.ef)) : null,
    driftMed: samples.length ? median(samples.map((x) => x.drift)) : null,
    droppedNegCount,
    cvTooHighCount,
    insufficientCount,
  };
}

// Create/update a NOTE event for the Monday detective
async function upsertMondayDetectiveNote(env, dayIso, noteText) {
  const external_id = `detektiv-${dayIso}`;
  const name = "Montags-Detektiv";
  const description = noteText;

  // Find existing note by external_id on that day
  const events = await fetchIntervalsEvents(env, dayIso, dayIso);
  const existing = (events || []).find((e) => String(e?.external_id || "") === external_id);

  if (existing?.id) {
    await updateIntervalsEvent(env, existing.id, {
      category: "NOTE",
      start_date_local: `${dayIso}T00:00:00`,
      name,
      description,
      color: "orange",
      external_id,
    });
    return;
  }

  await createIntervalsEvent(env, {
    category: "NOTE",
    start_date_local: `${dayIso}T00:00:00`,
    name,
    description,
    color: "orange",
    external_id,
  });
}

// ================= BENCH REPORTS =================

function getBenchType(benchName) {
  const s = benchName.toLowerCase();
  if (s.startsWith("vo2")) return "VO2";
  if (s.startsWith("th") || s.startsWith("schwelle")) return "THRESHOLD";
  if (s.startsWith("int")) return "INTERVAL";
  if (s.startsWith("rsd") || s.startsWith("sprint")) return "RSD";
  return "GA";
}


function getBenchTag(a) {
  const tags = a?.tags || [];
  for (const t of tags) {
    const s = String(t || "").trim();
    if (s.toLowerCase().startsWith("bench:")) return s.slice(6).trim();
  }
  return null;
}


async function computeBenchReport(env, activity, benchName, warmupSkipSec) {
  const dayIso = String(activity.start_date_local || activity.start_date || "").slice(0, 10);
  if (!dayIso) return null;

  const benchType = getBenchType(benchName);
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - BENCH_LOOKBACK_DAYS * 86400000);
  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));

  const same = acts
    .filter((a) => isRun(a) && getBenchTag(a) === benchName && a.id !== activity.id)
    .sort((a, b) => new Date(b.start_date_local || b.start_date) - new Date(a.start_date_local || a.start_date));

  const today = await computeBenchMetrics(env, activity, warmupSkipSec);
  if (!today) return `🧪 bench:${benchName}\nHeute: n/a`;

  let intervalMetrics = null;
  if (benchType !== "GA") {
    intervalMetrics = await computeIntervalBenchMetrics(env, activity, warmupSkipSec);
  }

  const lines = [];
  lines.push(`🧪 bench:${benchName}`);

  if (!same.length) {
    lines.push("Erster Benchmark – noch kein Vergleich.");
  } else {
    const last = await computeBenchMetrics(env, same[0], warmupSkipSec);

    const efVsLast = last?.ef != null ? pct(today.ef, last.ef) : null;
    const dVsLast = today.drift != null && last?.drift != null ? today.drift - last.drift : null;

    lines.push(`EF: ${fmtSigned1(efVsLast)}% vs letzte`);
    lines.push(`Drift: ${fmtSigned1(dVsLast)}%-Pkt vs letzte`);
  }

  if (intervalMetrics) {
    if (intervalMetrics.hrr60 != null) {
      lines.push(`Erholung: HRR60 ${intervalMetrics.hrr60.toFixed(0)} bpm`);
    }
    if (intervalMetrics.vo2min != null) {
      lines.push(`VO₂-Zeit ≥90% HFmax: ${intervalMetrics.vo2min.toFixed(1)} min`);
    }
  }

  let verdict = "Stabil / innerhalb Normalrauschen.";
  if (intervalMetrics?.hrr60 != null && intervalMetrics.hrr60 < 15) {
    verdict = "Hohe Belastung – Erholung limitiert.";
  } else if (intervalMetrics?.vo2min != null && intervalMetrics.vo2min >= 4) {
    verdict = "VO₂-Reiz ausreichend gesetzt.";
  }

  lines.push(`Fazit: ${verdict}`);
  return lines.join("\n");
}



async function computeIntervalBenchMetrics(env, a, warmupSkipSec) {
  const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
  if (!streams) return null;

  const hrr60 = hrr60FromStreams(streams);
  const vo2sec = timeAtHrPct(streams, 0.9);

  return {
    hrr60,
    vo2min: vo2sec ? vo2sec / 60 : null,
  };
}


async function computeBenchMetrics(env, a, warmupSkipSec) {
  const ef = extractEF(a);
  if (ef == null) return null;

  let drift = null;
  try {
    const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
    const ds = computeDriftAndStabilityFromStreams(streams, warmupSkipSec);
    drift = Number.isFinite(ds?.hr_drift_pct) ? ds.hr_drift_pct : null;
    if (drift != null && drift < 0) drift = null;
  } catch {
    drift = null;
  }

  return { ef, drift };
}

function pct(a, b) {
  return a != null && b != null && Number.isFinite(a) && Number.isFinite(b) && b !== 0 ? ((a - b) / b) * 100 : null;
}

function fmtSigned1(x) {
  if (!Number.isFinite(x)) return "n/a";
  return (x > 0 ? "+" : "") + x.toFixed(1);
}

function medianOrNull(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x));
  return v.length ? median(v) : null;
}

function interpretBench(efVsLast, dVsLast, efVsMed, dVsMed) {
  const ef = Number.isFinite(efVsMed) ? efVsMed : efVsLast;
  const dd = Number.isFinite(dVsMed) ? dVsMed : dVsLast;

  if (!Number.isFinite(ef) && !Number.isFinite(dd)) return "Gemischt/unklar (zu wenig Vergleichsdaten).";

  if (Number.isFinite(ef) && Number.isFinite(dd)) {
    if (ef >= +1.0 && dd <= -0.5) return "Motor besser (mehr Output + stabiler).";
    if (ef <= -1.0 && dd >= +0.5) return "Motor schlechter (weniger Output + instabiler).";
    if (ef >= +1.0) return "Output besser, Stabilität gemischt.";
    if (dd <= -0.5) return "Stabilität besser, Output gemischt.";
    if (ef <= -1.0) return "Output schlechter, Stabilität gemischt.";
    if (dd >= +0.5) return "Stabilität schlechter, Output gemischt.";
    return "Stabil / innerhalb Normalrauschen.";
  }

  if (Number.isFinite(ef)) {
    if (ef >= +1.0) return "Output besser (EF ↑).";
    if (ef <= -1.0) return "Output schlechter (EF ↓).";
    return "EF stabil / Normalrauschen.";
  }

  if (Number.isFinite(dd)) {
    if (dd <= -0.5) return "Stabilität besser (Drift ↓).";
    if (dd >= +0.5) return "Stabilität schlechter (Drift ↑).";
    return "Drift stabil / Normalrauschen.";
  }

  return "Gemischt/unklar.";
}

// ================= STREAMS METRICS =================
function timeAtHrPct(streams, pct, hfmax = HFMAX) {
  const hr = streams?.heartrate;
  const t = streams?.time;
  if (!Array.isArray(hr) || !Array.isArray(t)) return 0;

  const thr = pct * hfmax;
  let sec = 0;

  for (let i = 1; i < hr.length; i++) {
    const dt = Number(t[i]) - Number(t[i - 1]);
    if (Number(hr[i]) >= thr && Number.isFinite(dt)) sec += dt;
  }
  return sec;
}

function hrr60FromStreams(streams) {
  const hr = streams?.heartrate;
  const t = streams?.time;
  if (!Array.isArray(hr) || !Array.isArray(t)) return null;

  let peak = -Infinity;
  let idx = -1;

  for (let i = 0; i < hr.length; i++) {
    if (hr[i] > peak) {
      peak = hr[i];
      idx = i;
    }
  }
  if (idx < 0) return null;

  const tPeak = t[idx];
  for (let i = idx; i < t.length; i++) {
    if (t[i] >= tPeak + 60) return peak - hr[i];
  }
  return null;
}

function computeDriftAndStabilityFromStreams(streams, warmupSkipSec = 600) {
  if (!streams) return null;

  const hr = streams.heartrate;
  const speed = streams.velocity_smooth;
  const time = streams.time;

  if (!Array.isArray(hr) || !Array.isArray(speed)) return null;

  const n = Math.min(hr.length, speed.length);
  if (n < MIN_POINTS) return null;

  let startIdx = 0;
  if (Array.isArray(time) && time.length >= n) {
    while (startIdx < n && Number(time[startIdx]) < warmupSkipSec) startIdx++;
  } else {
    startIdx = Math.min(n - 1, warmupSkipSec);
  }

  const idx = [];
  for (let i = startIdx; i < n; i++) {
    const h = Number(hr[i]);
    const v = Number(speed[i]);
    if (!Number.isFinite(h) || h < 40) continue;
    if (!Number.isFinite(v) || v < MIN_RUN_SPEED) continue;
    idx.push(i);
  }

  if (idx.length < MIN_POINTS) return null;

  const half = Math.floor(idx.length / 2);

  const mean = (arr) => (arr.length ? arr.reduce((a, b) => a + b, 0) / arr.length : null);

  const hr1 = mean(idx.slice(0, half).map((i) => Number(hr[i])));
  const hr2 = mean(idx.slice(half).map((i) => Number(hr[i])));
  if (hr1 == null || hr2 == null || hr1 <= 0) return null;

  const hr_drift_pct = ((hr2 - hr1) / hr1) * 100;

  const vs = idx.map((i) => Number(speed[i]));
  const vMean = mean(vs);

  let speed_cv = null;
  if (vMean != null && vMean > 0) {
    const vVar = mean(vs.map((v) => (v - vMean) * (v - vMean)));
    const vSd = vVar != null ? Math.sqrt(vVar) : null;
    speed_cv = vSd != null ? vSd / vMean : null;
  }

  return { hr1, hr2, hr_drift_pct, used_points: idx.length, warmupSkipSec, speed_cv };
}

// ================= EXTRACTORS =================
function extractEF(a) {
  const sp = Number(a?.average_speed);
  const hr = Number(a?.average_heartrate);
  if (Number.isFinite(sp) && sp > 0 && Number.isFinite(hr) && hr > 0) return sp / hr;
  return null;
}

function extractLoad(a) {
  const l = Number(a?.icu_training_load);
  if (Number.isFinite(l) && l >= 0) return l;
  const l2 = Number(a?.hr_load);
  if (Number.isFinite(l2) && l2 >= 0) return l2;
  return 0;
}

// ================= CLASSIFICATION =================
function isRun(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t === "run" || t === "running" || t.includes("run") || t.includes("laufen");
}

function hasKeyTag(a) {
  return (a?.tags || []).some((t) => String(t).toLowerCase().startsWith("key:"));
}

function getKeyType(a) {
  // key:schwelle, key:vo2, key:tempo, ...
  const tags = a?.tags || [];
  for (const t of tags) {
    const s = String(t || "").toLowerCase().trim();
    if (s.startsWith("key:")) return s.slice(4).trim() || "key";
  }
  return "key";
}

function isGA(a) {
  if (hasKeyTag(a)) return false;
  const dur = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
  return Number.isFinite(dur) && dur >= GA_MIN_SECONDS;
}

function isGAComparable(a) {
  if (hasKeyTag(a)) return false;
  const dur = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
  return Number.isFinite(dur) && dur >= GA_COMPARABLE_MIN_SECONDS;
}

function vdotLikeFromEf(ef) {
  return ef * 1200;
}

// ================= DEBUG =================
function addDebug(debugOut, day, a, status, computed) {
  if (!debugOut) return;
  debugOut[day] ??= [];
  debugOut[day].push({
    activityId: a?.id ?? null,
    start: a?.start_date ?? null,
    start_local: a?.start_date_local ?? null,
    type: a?.type ?? null,
    tags: a?.tags ?? [],
    stream_types: a?.stream_types ?? [],
    status,
    computed,
  });
}

// ================= INTERVALS API =================
async function fetchIntervalsActivities(env, oldest, newest) {
  const url = `https://intervals.icu/api/v1/athlete/0/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`activities ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchIntervalsStreams(env, activityId, types) {
  const url = `https://intervals.icu/api/v1/activity/${activityId}/streams?types=${encodeURIComponent(types.join(","))}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`streams ${r.status}: ${await r.text()}`);
  const raw = await r.json();
  return normalizeStreams(raw);
}

function normalizeStreams(raw) {
  if (!raw) return null;

  if (raw.heartrate || raw.velocity_smooth || raw.time) return raw;
  if (raw.streams && (raw.streams.heartrate || raw.streams.velocity_smooth)) return raw.streams;
  if (raw.data && (raw.data.heartrate || raw.data.velocity_smooth)) return raw.data;

  if (Array.isArray(raw)) {
    const out = {};
    for (const item of raw) {
      const type = item?.type ?? item?.name ?? item?.key;
      const data = item?.data ?? item?.values ?? item?.stream;
      if (type && Array.isArray(data)) out[String(type)] = data;
    }
    return out;
  }

  return raw;
}

async function putWellnessDay(env, day, patch) {
  const url = `https://intervals.icu/api/v1/athlete/0/wellness/${day}`;
  const r = await fetch(url, {
    method: "PUT",
    headers: { Authorization: auth(env), "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!r.ok) throw new Error(`wellness PUT ${day} ${r.status}: ${await r.text()}`);
}

// Events (for NOTE)
async function fetchIntervalsEvents(env, oldest, newest) {
  // local dates (yyyy-MM-dd)
  const url = `https://intervals.icu/api/v1/athlete/0/events?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`events ${r.status}: ${await r.text()}`);
  return r.json();
}

async function createIntervalsEvent(env, eventObj) {
  const url = `https://intervals.icu/api/v1/athlete/0/events`;
  const r = await fetch(url, {
    method: "POST",
    headers: { Authorization: auth(env), "Content-Type": "application/json" },
    body: JSON.stringify(eventObj),
  });
  if (!r.ok) throw new Error(`events POST ${r.status}: ${await r.text()}`);
  return r.json();
}

async function updateIntervalsEvent(env, eventId, eventObj) {
  const url = `https://intervals.icu/api/v1/athlete/0/events/${encodeURIComponent(String(eventId))}`;
  const r = await fetch(url, {
    method: "PUT",
    headers: { Authorization: auth(env), "Content-Type": "application/json" },
    body: JSON.stringify(eventObj),
  });
  if (!r.ok) throw new Error(`events PUT ${r.status}: ${await r.text()}`);
  return r.json();
}

function auth(env) {
  return "Basic " + btoa(`API_KEY:${env.INTERVALS_API_KEY}`);
}

// ================= HELPERS =================
function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s));
}

function isMondayIso(dayIso) {
  const d = new Date(dayIso + "T00:00:00Z");
  return d.getUTCDay() === 1;
}

function diffDays(a, b) {
  const da = new Date(a + "T00:00:00Z").getTime();
  const db = new Date(b + "T00:00:00Z").getTime();
  return Math.round((db - da) / 86400000);
}

function listIsoDaysInclusive(oldest, newest) {
  const out = [];
  const start = new Date(oldest + "T00:00:00Z").getTime();
  const end = new Date(newest + "T00:00:00Z").getTime();
  for (let t = start; t <= end; t += 86400000) out.push(isoDate(new Date(t)));
  return out;
}

function json(o, status = 200) {
  return new Response(JSON.stringify(o, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function clampInt(x, min, max) {
  const n = Number(x);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
}

function clamp(x, lo, hi) {
  return Math.max(lo, Math.min(hi, x));
}

function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function avg(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x));
  return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
}

function median(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x)).sort((a, b) => a - b);
  if (!v.length) return null;
  const m = Math.floor(v.length / 2);
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}

function sum(arr) {
  let s = 0;
  for (const x of arr) s += Number(x) || 0;
  return s;
}

function std(arr) {
  const v = arr.filter((x) => x != null && Number.isFinite(x));
  if (v.length < 2) return null;
  const m = v.reduce((a, b) => a + b, 0) / v.length;
  const vv = v.reduce((a, b) => a + (b - m) * (b - m), 0) / v.length;
  return Math.sqrt(vv);
}

function isFiniteNumber(x) {
  return Number.isFinite(x);
}

function uniq(arr) {
  const out = [];
  const seen = new Set();
  for (const x of arr) {
    const k = String(x);
    if (seen.has(k)) continue;
    seen.add(k);
    out.push(x);
  }
  return out;
}

function countBy(arr) {
  const m = {};
  for (const x of arr) {
    const k = String(x);
    m[k] = (m[k] || 0) + 1;
  }
  return m;
}

function bucketLoadsByDay(runs) {
  const m = {};
  for (const r of runs) {
    const d = r.date;
    if (!d) continue;
    m[d] = (m[d] || 0) + (Number(r.load) || 0);
  }
  return m;
}
