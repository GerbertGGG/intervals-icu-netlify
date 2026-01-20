// src/index.js
// Cloudflare Worker – Run only (TTT fully removed)
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom numeric fields (exact codes):
// VDOT, Drift, EF, Score
//
// Run-only logic:
// - GA (no key:*, >=30min):
//     - VDOT_like (from EF summary)
//     - Drift = HR-Drift (%), computed from streams with warmup skip (default 10min)
//     - Do NOT write EF
// - Key (tag key:*):
//     - EF (summary)
//     - (TTT removed: no plan/compliance)
// - Score = execution quality (GA uses HR-Drift; Key uses fixed quality baseline), plus capped load
// - Aerobic trend (comment): GA-only (requires EF + HR-Drift), guarded by sample sizes
// - Minimum stimulus (comment only): 100 Run-load over last 7 days
//
// URL:
//   /sync?date=YYYY-MM-DD&write=true&debug=true
//   /sync?days=14&write=true&debug=true

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

      // optional override for drift warmup skip
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

      // In debug mode: run synchronously and return errors as JSON instead of 1101
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

      // Non-debug: background execution
      ctx?.waitUntil?.(
        syncRange(env, oldest, newest, write, false, warmupSkipSec).catch(() => {
          // swallow to avoid unhandled promise rejection
        })
      );
      return json({ ok: true, oldest, newest, write, warmupSkipSec });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
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
const MIN_STIMULUS_7D_RUN_LOAD = 100;
const TREND_WINDOW_DAYS = 28;
const TREND_MIN_N = 3;

// Wellness field codes
const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_EF = "EF";
const FIELD_SCORE = "Score";

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

  for (const [day, runs] of byDay.entries()) {
    const patch = {};
    const perRunInfo = [];

    for (const a of runs) {
      const isKey = hasKeyTag(a);
      const ga = isGA(a);

      const ef = extractEF(a);
      const load = extractLoad(a);

      // HR-Drift only for GA non-key
      let drift = null;
      let drift_source = "none";

      if (ga && !isKey) {
        drift_source = "streams";
        try {
          const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
          const hrDriftObj = computeHRDriftFromStreams(streams, warmupSkipSec);
          drift = Number.isFinite(hrDriftObj?.hr_drift_pct) ? hrDriftObj.hr_drift_pct : null;
          if (drift == null) drift_source = "streams_insufficient";
        } catch (e) {
          drift = null;
          drift_source = "streams_failed";
          if (debug) {
            addDebug(debugOut, day, a, "warn:streams_failed", { message: String(e?.message ?? e) });
          }
        }
      }

      const score = computeScore({ ga, isKey, drift, load });

      // GA fields
      if (ga && !isKey) {
        if (ef != null) patch[FIELD_VDOT] = round(vdotLikeFromEf(ef), 1);
        if (drift != null) patch[FIELD_DRIFT] = round(drift, 1);
      }

      // Key fields (TTT removed)
      if (isKey) {
        if (ef != null) patch[FIELD_EF] = round(ef, 5);
      }

      // Always score
      patch[FIELD_SCORE] = score;

      perRunInfo.push({
        activityId: a.id,
        type: a.type,
        tags: a.tags ?? [],
        ga,
        isKey,
        ef,
        drift,
        drift_source,
        load,
        score,
      });

      if (debug) {
        addDebug(debugOut, day, a, "ok", {
          ga,
          isKey,
          ef,
          drift,
          drift_source,
          load,
          score,
        });
      }
    }

    // Trend + Minimum + comment
    let trend;
    try {
      trend = await computeAerobicTrend(env, day, warmupSkipSec);
    } catch (e) {
      trend = {
        ok: false,
        text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – Fehler (${String(e?.message ?? e)})`,
      };
    }

    let min;
    try {
      min = await computeMinStimulus(env, day);
    } catch {
      min = { runLoad7: 0, minOk: false };
    }

    patch.comments = renderWellnessComment({
      perRunInfo,
      trend,
      runLoad7: min.runLoad7,
      minOk: min.minOk,
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
function renderWellnessComment({ perRunInfo, trend, runLoad7, minOk }) {
  const scores = perRunInfo.map((x) => x.score).filter((x) => Number.isFinite(x));
  const scoreAvg = scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length) : null;

  const hadKey = perRunInfo.some((x) => x.isKey);
  const hadGA = perRunInfo.some((x) => x.ga && !x.isKey);

  const lines = [];
  lines.push(`${scoreEmoji(scoreAvg)} Trainingausführung`);
  if (scoreAvg != null) lines.push(`Score Ø: ${scoreAvg}/100`);

  lines.push("");
  if (hadKey && !hadGA) lines.push("Heute: Schlüsseltraining (Key)");
  else if (hadGA && !hadKey) lines.push("Heute: Grundlage (GA)");
  else if (hadKey && hadGA) lines.push("Heute: Gemischt (GA + Key)");
  else lines.push("Heute: Lauf");

  lines.push("");
  lines.push(trend.text);

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

function scoreEmoji(score) {
  if (score == null) return "ℹ️";
  if (score >= 85) return "🟢";
  if (score >= 70) return "🟡";
  if (score >= 55) return "🟠";
  return "🔴";
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
      const hrDriftObj = computeHRDriftFromStreams(streams, warmupSkipSec);
      drift = Number.isFinite(hrDriftObj?.hr_drift_pct) ? hrDriftObj.hr_drift_pct : null;
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

// ================= MINIMUM STIMULUS (Run-only) =================
async function computeMinStimulus(env, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - 7 * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const runLoad7 = acts.filter((a) => isRun(a)).reduce((s, a) => s + extractLoad(a), 0);

  return { runLoad7, minOk: runLoad7 >= MIN_STIMULUS_7D_RUN_LOAD };
}

// ================= SCORE =================
function computeScore({ ga, isKey, drift, load }) {
  const C = clamp(Number(load) || 0, 0, 70);

  let Q = 65;

  if (isKey) {
    // No plan/compliance -> no TTT. Use a fixed baseline quality for Key.
    Q = 75;
  } else if (ga) {
    const d = Number.isFinite(drift) ? Math.max(0, drift) : null;

    if (Number.isFinite(d)) {
      if (d <= 3) Q = 98;
      else if (d <= 6) Q = 88;
      else if (d <= 10) Q = 70;
      else if (d <= 15) Q = 50;
      else Q = 30;
    } else {
      Q = 65;
    }
  }

  return round(clamp(0.75 * Q + 0.25 * C, 0, 100), 1);
}

function vdotLikeFromEf(ef) {
  return ef * 1200;
}

// ================= HR-DRIFT FROM STREAMS =================
function computeHRDriftFromStreams(streams, warmupSkipSec = 600) {
  if (!streams) return null;

  const hr = streams.heartrate;
  const speed = streams.velocity_smooth;
  const time = streams.time;

  if (!Array.isArray(hr) || !Array.isArray(speed)) return null;

  const n = Math.min(hr.length, speed.length);
  if (n < 300) return null;

  let startIdx = 0;
  if (Array.isArray(time) && time.length >= n) {
    startIdx = 0;
    while (startIdx < n && Number(time[startIdx]) < warmupSkipSec) startIdx++;
  } else {
    startIdx = Math.min(n - 1, warmupSkipSec);
  }

  const idx = [];
  for (let i = startIdx; i < n; i++) {
    const h = Number(hr[i]);
    const v = Number(speed[i]);
    if (!Number.isFinite(h) || h < 40) continue;
    const MIN_RUN_SPEED = 1.8; // m/s ~ 9:15 min/km
    if (!Number.isFinite(v) || v < MIN_RUN_SPEED) continue;
    idx.push(i);
  }

  if (idx.length < 300) return null;

  const half = Math.floor(idx.length / 2);

  const meanHR = (a, b) => {
    let s = 0;
    let c = 0;
    for (let k = a; k < b; k++) {
      const i = idx[k];
      const h = Number(hr[i]);
      if (!Number.isFinite(h) || h < 40) continue;
      s += h;
      c++;
    }
    return c ? s / c : null;
  };

  const hr1 = meanHR(0, half);
  const hr2 = meanHR(half, idx.length);

  if (hr1 == null || hr2 == null || hr1 <= 0) return null;

  const hr_drift_pct = ((hr2 - hr1) / hr1) * 100;
  return { hr1, hr2, hr_drift_pct, used_points: idx.length, warmupSkipSec };
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

function diffDays(a, b) {
  const da = new Date(a + "T00:00:00Z").getTime();
  const db = new Date(b + "T00:00:00Z").getTime();
  return Math.round((db - da) / 86400000);
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