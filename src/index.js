// src/index.js
// Cloudflare Worker – Run only
// Writes metrics + explanatory comments into Intervals Wellness
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom numeric fields (exact codes):
// VDOT, Drift, EF, TTT, Score
//
// Logic summary (Run-only):
// - Only Run activities (VirtualRide etc. ignored)
// - GA (no key:*, >=30min):
//     - Write VDOT_like (derived from EF) and Drift (decoupling if available)
//     - Do NOT write TTT
// - Key (tag key:*):
//     - Write EF and TTT (compliance)
//     - Do NOT write VDOT/Drift
// - Score = execution quality (not progress) – uses GA drift or Key TTT, plus capped load
// - Progress context (comment): GA-only trends (28d vs prev 28d) with sample guard
// - Minimum stimulus (comment only): 100 Run-TSS (icu_training_load) over last 7 days
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

      // basic validation
      if (!isIsoDate(oldest) || !isIsoDate(newest)) {
        return json({ ok: false, error: "Invalid date format (YYYY-MM-DD)" }, 400);
      }
      if (newest < oldest) {
        return json({ ok: false, error: "`to` must be >= `from`" }, 400);
      }
      if (diffDays(oldest, newest) > 31) {
        return json({ ok: false, error: "Max range is 31 days" }, 400);
      }

      // Run synchronously in debug mode so errors surface immediately
      if (debug) {
        const result = await syncRange(env, oldest, newest, write, true);
        return json(result);
      }

      // Otherwise fire-and-forget
      ctx?.waitUntil?.(syncRange(env, oldest, newest, write, false));
      return json({ ok: true, oldest, newest, write });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // Daily cron: last 14 days, write=true
    ctx.waitUntil(syncRange(env, isoDate(new Date(Date.now() - 14 * 86400000)), isoDate(new Date()), true, false));
  },
};

// ================= CONFIG =================
const GA_MIN_SECONDS = 30 * 60;
const MIN_STIMULUS_7D_RUN_LOAD = 100;
const TREND_WINDOW_DAYS = 28;
const TREND_MIN_N = 3;

// Wellness field codes (must match your Intervals custom fields exactly)
const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_EF = "EF";
const FIELD_TTT = "TTT";
const FIELD_SCORE = "Score";

// ================= MAIN =================
async function syncRange(env, oldest, newest, write, debug) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);

  // group RUN activities by local day
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
    // Compute one patch per day (if multiple runs, we keep the "best/most relevant" fields)
    const patch = {};
    const perRunInfo = [];

    for (const a of runs) {
      const isKey = hasKeyTag(a);
      const ga = isGA(a);

      // Summary values
      const ef = extractEF(a); // speed/hr
      const drift = extractDrift(a); // decoupling (may be null)
      const ttt = extractTTT(a); // compliance (%)
      const load = extractLoad(a); // icu_training_load

      // Compute score using the most relevant signal:
      // - GA: drift (if present) else neutral Q
      // - Key: TTT (if present) else neutral Q
      const score = computeScore({ ga, isKey, drift, ttt, load });

      // GA fields
      if (ga && !isKey) {
        if (ef != null) patch[FIELD_VDOT] = round(vdotLikeFromEf(ef), 1);
        if (drift != null) patch[FIELD_DRIFT] = round(drift, 1);
        // IMPORTANT: no TTT on GA
      }

      // Key fields
      if (isKey) {
        if (ef != null) patch[FIELD_EF] = round(ef, 5);
        if (ttt != null) patch[FIELD_TTT] = round(ttt, 1);
        // IMPORTANT: no VDOT/Drift on Key
      }

      // Always write Score (it is day-level)
      patch[FIELD_SCORE] = score;

      perRunInfo.push({
        activityId: a.id,
        type: a.type,
        tags: a.tags ?? [],
        ga,
        isKey,
        ef,
        drift,
        ttt,
        load,
        score,
      });

      if (debug) addDebug(debugOut, day, a, "ok", { ga, isKey, ef, drift, ttt, load, score });
    }

    // Add comment blocks:
    // 1) Execution (Score)
    // 2) Aerobic context (GA-only trend, guarded)
    // 3) Minimum stimulus (Run-only 7d load)
    const trend = await computeAerobicTrend(env, day);
    const { runLoad7, minOk } = await computeMinStimulus(env, day);

    patch.comments = renderWellnessComment({
      day,
      perRunInfo,
      trend,
      runLoad7,
      minOk,
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
  // Score section: average score for the day
  const scores = perRunInfo.map((x) => x.score).filter((x) => Number.isFinite(x));
  const scoreAvg = scores.length ? Math.round(scores.reduce((a, b) => a + b, 0) / scores.length) : null;

  const lines = [];

  // 1) Execution
  lines.push(`${scoreEmoji(scoreAvg)} Trainingausführung`);
  if (scoreAvg != null) lines.push(`Score Ø: ${scoreAvg}/100`);

  // Optional: add 1-line context what was trained today
  const hadKey = perRunInfo.some((x) => x.isKey);
  const hadGA = perRunInfo.some((x) => x.ga && !x.isKey);
  lines.push("");
  if (hadKey && !hadGA) lines.push("Heute: Schlüsseltraining (Key)");
  else if (hadGA && !hadKey) lines.push("Heute: Grundlage (GA)");
  else if (hadKey && hadGA) lines.push("Heute: Gemischt (GA + Key)");
  else lines.push("Heute: Lauf");

  // 2) Aerobic trend context (GA-only, guarded)
  lines.push("");
  lines.push(trend.text);

  // 3) Minimum stimulus (comment only)
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
async function computeAerobicTrend(env, dayIso) {
  // Compare last 28d vs previous 28d, GA-only (no key:*), needs EF + drift
  const end = new Date(dayIso + "T00:00:00Z");
  const mid = new Date(end.getTime() - TREND_WINDOW_DAYS * 86400000);
  const start = new Date(end.getTime() - 2 * TREND_WINDOW_DAYS * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));

  const ga = acts.filter((a) => {
    if (!isRun(a)) return false;
    if (hasKeyTag(a)) return false;
    if (!isGA(a)) return false;
    const ef = extractEF(a);
    const d = extractDrift(a);
    return ef != null && d != null;
  });

  const recent = ga.filter((a) => new Date(a.start_date_local || a.start_date) >= mid);
  const prev = ga.filter((a) => new Date(a.start_date_local || a.start_date) < mid);

  if (recent.length < TREND_MIN_N || prev.length < TREND_MIN_N) {
    return {
      ok: false,
      text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – zu wenig GA-Daten (recent=${recent.length}, prev=${prev.length})`,
    };
  }

  const ef1 = avg(recent.map((a) => extractEF(a)));
  const ef0 = avg(prev.map((a) => extractEF(a)));
  const d1 = median(recent.map((a) => extractDrift(a)));
  const d0 = median(prev.map((a) => extractDrift(a)));

  if (ef0 == null || ef1 == null || d0 == null || d1 == null) {
    return {
      ok: false,
      text: "ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – fehlende Werte",
    };
  }

  const dv = ((ef1 - ef0) / ef0) * 100; // EF proxy for VDOT_like trend
  const dd = d1 - d0; // drift delta in %-points

  // Ampel
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
      `Aerober Kontext (nur GA): VDOT ~ ${dv.toFixed(1)}% | Drift ${dd > 0 ? "↑" : "↓"} ${Math.abs(dd).toFixed(1)}%-Pkt`,
  };
}

// ================= MINIMUM STIMULUS (Run-only) =================
async function computeMinStimulus(env, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - 7 * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const runLoad7 = acts
    .filter((a) => isRun(a))
    .reduce((s, a) => s + extractLoad(a), 0);

  return { runLoad7, minOk: runLoad7 >= MIN_STIMULUS_7D_RUN_LOAD };
}

// ================= SCORING =================
function computeScore({ ga, isKey, drift, ttt, load }) {
  const C = clamp(Number(load) || 0, 0, 70); // capped load contribution

  let Q = 65; // neutral quality baseline

  if (isKey) {
    if (Number.isFinite(ttt)) {
      if (ttt >= 95) Q = 98;
      else if (ttt >= 90) Q = 88;
      else if (ttt >= 80) Q = 68;
      else Q = 45;
    } else {
      Q = 60;
    }
  } else if (ga) {
    if (Number.isFinite(drift)) {
      if (drift <= 3) Q = 98;
      else if (drift <= 6) Q = 88;
      else if (drift <= 10) Q = 70;
      else if (drift <= 15) Q = 50;
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

// ================= EXTRACTORS =================
function extractEF(a) {
  const sp = Number(a?.average_speed);
  const hr = Number(a?.average_heartrate);
  if (Number.isFinite(sp) && sp > 0 && Number.isFinite(hr) && hr > 0) return sp / hr;
  return null;
}

function extractDrift(a) {
  // Intervals activity JSON may store decoupling as `decoupling` or `pahr_decoupling` etc.
  const v1 = Number(a?.pahr_decoupling);
  if (Number.isFinite(v1) && v1 > 0) return v1;
  const v2 = Number(a?.pwhr_decoupling);
  if (Number.isFinite(v2) && v2 > 0) return v2;
  const v3 = Number(a?.decoupling);
  if (Number.isFinite(v3) && v3 > 0) return v3;
  return null;
}

function extractTTT(a) {
  const c = Number(a?.compliance);
  if (Number.isFinite(c) && c >= 0) return c; // percent 0..100
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
