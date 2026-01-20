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
// - Always writes a comment for every day in range (even if no run).
// - Minimum stimulus (7d run-load) always included in comment.
// - Monday detective is included every Monday, even if no run.
//
// GA logic (no key:*, >=30min):
// - VDOT_like from EF = avg_speed/avg_hr
// - Drift from streams (warmup skip default 10min)
// - Negative drift => null (dropped; not written; not used in stats)
//
// Motor Index:
// - GA comparable only (no key, >=35–40min, steady pace)
// - EF trend (28d) + Drift trend (14d), 0..100
//
// URL:
//   /sync?date=YYYY-MM-DD&write=true&debug=true
//   /sync?days=14&write=true&debug=true
//   /sync?from=YYYY-MM-DD&to=YYYY-MM-DD&write=true&debug=true

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

      ctx?.waitUntil?.(
        syncRange(env, oldest, newest, write, false, warmupSkipSec).catch(() => {})
      );
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
const GA_COMPARABLE_MIN_SECONDS = 35 * 60; // comparable GA threshold (slightly relaxed)
const MIN_STIMULUS_7D_RUN_LOAD = 150;

const TREND_WINDOW_DAYS = 28;
const TREND_MIN_N = 3;

const MOTOR_WINDOW_DAYS = 28;
const MOTOR_NEED_N_PER_HALF = 2; // more robust (was 3)
const MOTOR_DRIFT_WINDOW_DAYS = 14;

const HFMAX = 173;

const DETECTIVE_LOOKBACK_DAYS = 14;
const DETECTIVE_MIN_RECENT = 2;
const DETECTIVE_MIN_PREV = 2;

const MIN_RUN_SPEED = 1.8;
const MIN_POINTS = 300;
const GA_SPEED_CV_MAX = 0.10; // relaxed from 0.08

// Wellness field codes
const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_MOTOR = "Motor";

// ================= MAIN =================
async function syncRange(env, oldest, newest, write, debug, warmupSkipSec) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);

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

  // NEW: iterate *every day* in the requested range
  const daysList = listIsoDaysInclusive(oldest, newest);

  for (const day of daysList) {
    const runs = byDay.get(day) ?? [];
    const patch = {};
    const perRunInfo = [];

    // Motor Index computed per day (works even if no run that day)
    let motor = null;
    try {
      motor = await computeMotorIndex(env, day, warmupSkipSec);
      if (motor?.value != null) patch[FIELD_MOTOR] = round(motor.value, 1);
    } catch (e) {
      motor = { ok: false, value: null, text: `🏎️ Motor-Index: n/a – Fehler (${String(e?.message ?? e)})` };
    }

    // Process runs (if any)
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

    // Aerobic trend (works regardless of run day)
    let trend;
    try {
      trend = await computeAerobicTrend(env, day, warmupSkipSec);
    } catch (e) {
      trend = { ok: false, text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – Fehler (${String(e?.message ?? e)})` };
    }

    // Minimum stimulus always
    let min;
    try {
      min = await computeMinStimulus(env, day);
    } catch {
      min = { runLoad7: 0, minOk: false };
    }

    // Monday detective ALWAYS on Mondays (even if no run)
    let detectiveText = null;
    try {
      if (isMondayIso(day)) detectiveText = await computeDetectiveReport(env, day, warmupSkipSec);
    } catch (e) {
      detectiveText = `🕵️‍♂️ Montags-Detektiv\nFehler: ${String(e?.message ?? e)}`;
    }

    patch.comments = renderWellnessComment({
      day,
      perRunInfo,
      trend,
      motor,
      runLoad7: min.runLoad7,
      minOk: min.minOk,
      detectiveText,
    });

    patches[day] = patch;

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
    activitiesSeen,
    activitiesUsed,
    daysComputed: Object.keys(patches).length,
    daysWritten,
    patches: debug ? patches : undefined,
    debug: debug ? debugOut : undefined,
  };
}

// ================= COMMENT =================
function renderWellnessComment({ day, perRunInfo, trend, motor, runLoad7, minOk, detectiveText }) {
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

  lines.push("");
  if (minOk) {
    lines.push("ℹ️ Mindest-Laufreiz erreicht");
    lines.push(`7-Tage Lauf-Load ≥ ${MIN_STIMULUS_7D_RUN_LOAD} (${Math.round(runLoad7)})`);
  } else {
    lines.push("ℹ️ Mindest-Laufreiz unterschritten");
    lines.push(`7-Tage Lauf-Load < ${MIN_STIMULUS_7D_RUN_LOAD} (${Math.round(runLoad7)})`);
    lines.push("➡️ Kurzfristig ok – langfristig kein Aufbau.");
  }

  if (detectiveText) {
    lines.push("");
    lines.push("— — —");
    lines.push(detectiveText);
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
  const dd = d0 != null && d1 != null ? (d1 - d0) : null; // + bad

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

// ================= MINIMUM STIMULUS (Run-only) =================
async function computeMinStimulus(env, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - 7 * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const runLoad7 = acts.filter((a) => isRun(a)).reduce((s, a) => s + extractLoad(a), 0);

  return { runLoad7, minOk: runLoad7 >= MIN_STIMULUS_7D_RUN_LOAD };
}

// ================= MONDAY DETECTIVE =================
async function computeDetectiveReport(env, mondayIso, warmupSkipSec) {
  const end = new Date(mondayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - DETECTIVE_LOOKBACK_DAYS * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));

  const ga = [];
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
    } catch {}

    if (drift == null) continue;
    if (cv == null || cv > GA_SPEED_CV_MAX) continue;

    const hr = Number(a?.average_heartrate);
    const date = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!date) continue;

    ga.push({
      date,
      ef,
      drift,
      hrPct: Number.isFinite(hr) ? (hr / HFMAX) * 100 : null,
      load: extractLoad(a),
    });
  }

  const mid = new Date(end.getTime() - 7 * 86400000);
  const recent = ga.filter((x) => new Date(x.date + "T00:00:00Z") >= mid);
  const prev = ga.filter((x) => new Date(x.date + "T00:00:00Z") < mid);

  if (recent.length < DETECTIVE_MIN_RECENT || prev.length < DETECTIVE_MIN_PREV) {
    return `🕵️‍♂️ Montags-Detektiv\nZu wenig vergleichbare GA-Daten (recent=${recent.length}, prev=${prev.length}).`;
  }

  const ef1 = median(recent.map((x) => x.ef));
  const ef0 = median(prev.map((x) => x.ef));
  const d1 = median(recent.map((x) => x.drift));
  const d0 = median(prev.map((x) => x.drift));
  const hrp1 = avg(recent.map((x) => x.hrPct));
  const hrp0 = avg(prev.map((x) => x.hrPct));
  const l1 = recent.reduce((s, x) => s + (x.load || 0), 0);
  const l0 = prev.reduce((s, x) => s + (x.load || 0), 0);

  const dv = ef0 && ef1 ? ((ef1 - ef0) / ef0) * 100 : null;
  const dd = d0 != null && d1 != null ? d1 - d0 : null;
  const dhr = hrp0 != null && hrp1 != null ? hrp1 - hrp0 : null;

  const efDown = dv != null && dv < -1.0;
  const driftUp = dd != null && dd > 1.0;

  const intensityCreep = dhr != null && dhr > 1.0;
  const fatigueLoad = l1 > l0 * 1.15;

  let verdict = "Motor stabil/unklar (kein klares Negativmuster).";
  if (efDown && driftUp) {
    if (fatigueLoad) verdict = "Signal klar: EF ↓ & Drift ↑ – sehr wahrscheinlich Ermüdung / Dichte (Load ↑).";
    else if (intensityCreep) verdict = "Signal klar: EF ↓ & Drift ↑ – sehr wahrscheinlich GA zu hart (HR%max ↑).";
    else verdict = "Signal klar: EF ↓ & Drift ↑ – Ursache gemischt (Ermüdung/Intensität/Bedingungen prüfen).";
  } else if (efDown && !driftUp) {
    verdict = "EF ↓ ohne Drift-Anstieg: eher Bedingungen/Route als Aerobik-Problem.";
  } else if (!efDown && driftUp) {
    verdict = "Drift ↑ bei stabiler EF: Ausführung aktuell schlechter (Müdigkeit/Fueling/Bedingungen).";
  }

  const lines = [];
  lines.push("🕵️‍♂️ Montags-Detektiv (GA comparable, 14 Tage)");
  lines.push(verdict);
  lines.push("");
  lines.push("Belege (letzte 7T vs davor):");
  lines.push(`- EF: ${dv != null ? dv.toFixed(1) + "%" : "n/a"}`);
  lines.push(`- HR-Drift: ${dd != null ? (dd > 0 ? "+" : "") + dd.toFixed(1) + "%-Pkt" : "n/a"}`);
  lines.push(`- HR%max: ${dhr != null ? (dhr > 0 ? "+" : "") + dhr.toFixed(1) + "%-Pkt" : "n/a"}`);
  lines.push(`- GA-Load: ${Math.round(l1)} vs ${Math.round(l0)}`);

  lines.push("");
  lines.push("Nächste Schritte:");
  if (verdict.includes("GA zu hart")) {
    lines.push("- 7–10 Tage GA lockerer (HR% runter); Pace egal.");
    lines.push("- 1 Benchmark-Run flach/steady (45–60min) zur Bestätigung.");
  } else if (verdict.includes("Ermüdung")) {
    lines.push("- Dichte senken: 1 Ruhetag mehr ODER 1 Einheit sehr kurz+easy.");
    lines.push("- Key nicht mit hartem GA am Folgetag verschärfen.");
  } else {
    lines.push("- Like-for-like prüfen (gleiche Route/Dauer/steady).");
    lines.push("- Prüfe unbewussten Pace-Creep (HR%max ↑?).");
  }

  return lines.join("\n");
}

// ================= STREAMS METRICS =================
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
