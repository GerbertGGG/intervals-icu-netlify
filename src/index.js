// src/index.js
// Cloudflare Worker: Intervals streams -> compute VDOT_like + Drift (GA), EF (non-GA), TTT (interval workouts)
// Write results ONLY into Intervals custom Wellness fields (per-day PUT).
//
// Required secrets (wrangler secret put):
// - INTERVALS_API_KEY
// - SYNC_TOKEN   (for /sync endpoint)
//
// In Intervals (UI): create Custom Wellness Fields (numeric) with these codes (or adjust below):
// - VDOT, Drift, EF, TTT

export default {
  async scheduled(event, env, ctx) {
    // Cron schreibt immer (wie gehabt)
    ctx.waitUntil(sync(env, 14, true));
  },

  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") return new Response("ok");

    if (url.pathname === "/sync") {
      // --- Params ---
      const write = (url.searchParams.get("write") || "").toLowerCase() === "true";

      const date = url.searchParams.get("date"); // YYYY-MM-DD
      const from = url.searchParams.get("from"); // YYYY-MM-DD
      const to = url.searchParams.get("to");     // YYYY-MM-DD
      const days = clampInt(url.searchParams.get("days") ?? "14", 1, 31); // hard cap

      // --- Build range ---
      let oldest, newest;

      if (date) {
        // single-day sync
        oldest = date;
        newest = date;
      } else if (from && to) {
        oldest = from;
        newest = to;
      } else {
        newest = isoDate(new Date());
        oldest = isoDate(new Date(Date.now() - days * 86400000));
      }

      // --- Safety rails (recommended) ---
      if (!isIsoDate(oldest) || !isIsoDate(newest)) {
        return json({ ok: false, error: "Invalid date format. Use YYYY-MM-DD." }, 400);
      }

      if (newest < oldest) {
        return json({ ok: false, error: "`to/newest` must be >= `from/oldest`." }, 400);
      }

      // limit range length to 31 days
      const rangeDays = diffDays(oldest, newest);
      if (rangeDays > 31) {
        return json({ ok: false, error: "Range too large. Max 31 days." }, 400);
      }

      // optional: don't allow very old dates
      const oldestAllowed = isoDate(new Date(Date.now() - 365 * 86400000));
      if (oldest < oldestAllowed) {
        return json({ ok: false, error: "Date too old. Max 365 days back." }, 400);
      }

      // --- Run ---
      ctx.waitUntil(syncRange(env, oldest, newest, write));
      return json({ ok: true, oldest, newest, write });
    }

    return new Response("Not found", { status: 404 });
  },
};
  

// ====== CONFIG ======
const GA_TAGS = ["GA", "Z2", "Easy"]; // optional, if you use tags in Intervals
const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_EF = "EF";
const FIELD_TTT = "TTT";

// ====== SYNC ======
async function sync(env, days) {
  const newest = isoDate(new Date());
  const oldest = isoDate(new Date(Date.now() - days * 86400000));
  return syncRange(env, oldest, newest);
}

async function syncRange(env, oldest, newest) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);

  // Sort stable by start date so "later activity overwrites earlier" in same day patch
  acts.sort((a, b) => String(a.start_date || "").localeCompare(String(b.start_date || "")));

  // day -> patch object containing only our fields
  const dayPatch = new Map();

  for (const a of acts) {
    if (!isRun(a)) continue;

    const day = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!day) continue;

    // Streams we need
    const streams = await fetchIntervalsStreams(env, a.id, ["heartrate", "velocity_smooth"]);
    if (!streams) continue;

    const patch = dayPatch.get(day) || {};

    // EF + Drift (from streams)
    const q = calcEfAndDrift(streams);
    if (q) {
      const ga = isGrundlage(a, q);

      if (ga) {
        patch[FIELD_VDOT] = round(vdotLikeFromEf(q.ef_overall), 1);
        patch[FIELD_DRIFT] = round(q.drift_pct, 1);
      } else {
        patch[FIELD_EF] = round(q.ef_overall, 5);
      }
    }

    // TTT (interval workouts) from speed only
    const ttt = calcTTTFromSpeed(streams.velocity_smooth);
    if (ttt?.isIntervalWorkout) {
      patch[FIELD_TTT] = round(ttt.ttt_pct, 1);
    }

    if (Object.keys(patch).length) dayPatch.set(day, patch);
  }

  // Write patches day-by-day (PUT /wellness/YYYY-MM-DD)
  for (const [day, patch] of dayPatch.entries()) {
    await putWellnessDay(env, day, patch);
  }

  return { ok: true, oldest, newest, daysWritten: dayPatch.size };
}

// ====== CLASSIFICATION ======
function isRun(a) {
  const t = String(a.type || a.activity_type || "").toLowerCase();
  return t.includes("run") || t.includes("laufen");
}

function isGrundlage(a, q) {
  // Tag-based first (most robust)
  const tags = (a.tags || []).map(String);
  if (tags.some((t) => GA_TAGS.includes(t))) return true;

  // Fallback heuristic: >=30 min and not huge drift
  const dur = Number(a.moving_time || a.elapsed_time || 0);
  if (dur < 30 * 60) return false;

  if (q.drift_pct > 10) return false;

  return true;
}

// ====== METRICS: EF + DRIFT ======
function calcEfAndDrift(streams) {
  const hr = streams.heartrate;
  const v = streams.velocity_smooth; // m/s
  if (!hr || !v) return null;

  const n = Math.min(hr.length, v.length);
  if (n < 300) return null; // <~5min insufficient

  const half = Math.floor(n / 2);

  const efRange = (from, to) => {
    let sum = 0;
    let cnt = 0;
    for (let i = from; i < to; i++) {
      const h = hr[i];
      const s = v[i];
      if (!h || h < 40) continue;
      if (!s || s <= 0) continue;
      sum += s / h; // (m/s)/bpm
      cnt++;
    }
    return cnt ? sum / cnt : null;
  };

  const ef1 = efRange(0, half);
  const ef2 = efRange(half, n);
  if (ef1 == null || ef2 == null) return null;

  const driftPct = ((ef2 - ef1) / ef1) * 100;

  return {
    ef_overall: (ef1 + ef2) / 2,
    drift_pct: driftPct,
  };
}

// “VDOT_like” as trend proxy from EF (scale factor only for readability)
function vdotLikeFromEf(ef) {
  const K = 1200;
  return ef * K;
}

// ====== METRICS: TTT (Time-to-Target) ======
function calcTTTFromSpeed(speed) {
  if (!speed || speed.length < 600) return null; // <10min
  const v = speed.filter((x) => typeof x === "number" && x > 0);
  if (v.length < 600) return null;

  const p50 = percentile(v, 50);
  const p90 = percentile(v, 90);

  // "Spikiness" check: if not much faster than median, likely no intervals
  if (!p50 || !p90 || p90 <= p50 * 1.08) return { isIntervalWorkout: false };

  // Adaptive work threshold
  const workThr = (p50 + p90) / 2;

  // Detect "work" segments above threshold
  const segs = detectSegments(speed, workThr, { minLen: 60, maxDrop: 25 });
  if (segs.length < 3) return { isIntervalWorkout: false };

  // Target center from first two work segments
  const center = meanSegmentSpeed(speed, segs.slice(0, 2));
  if (!center) return { isIntervalWorkout: false };

  // Target band ±3%
  const low = center * 0.97;
  const high = center * 1.03;

  let planned = 0;
  let hit = 0;

  for (const [a, b] of segs) {
    planned += b - a;
    for (let i = a; i < b; i++) {
      const s = speed[i];
      if (s >= low && s <= high) hit++;
    }
  }
  if (!planned) return { isIntervalWorkout: false };

  return {
    isIntervalWorkout: true,
    ttt_pct: (hit / planned) * 100,
  };
}

function detectSegments(speed, thr, { minLen, maxDrop }) {
  const segs = [];
  let i = 0;

  while (i < speed.length) {
    while (i < speed.length && speed[i] < thr) i++;
    if (i >= speed.length) break;

    const start = i;
    let below = 0;

    while (i < speed.length) {
      if (speed[i] >= thr) below = 0;
      else below++;
      if (below > maxDrop) break; // ended by sustained drop
      i++;
    }

    const end = i - below;
    if (end - start >= minLen) segs.push([start, end]);
  }

  return segs;
}

function meanSegmentSpeed(speed, segs) {
  let sum = 0;
  let cnt = 0;
  for (const [a, b] of segs) {
    for (let i = a; i < b; i++) {
      const s = speed[i];
      if (typeof s === "number" && s > 0) {
        sum += s;
        cnt++;
      }
    }
  }
  return cnt ? sum / cnt : null;
}

function percentile(arr, p) {
  if (!arr.length) return null;
  const a = [...arr].sort((x, y) => x - y);
  const idx = (p / 100) * (a.length - 1);
  const lo = Math.floor(idx);
  const hi = Math.ceil(idx);
  if (lo === hi) return a[lo];
  return a[lo] + (a[hi] - a[lo]) * (idx - lo);
}

// ====== INTERVALS API ======
async function fetchIntervalsActivities(env, oldest, newest) {
  const url = `https://intervals.icu/api/v1/athlete/0/activities?oldest=${oldest}&newest=${newest}`;
  const res = await fetch(url, { headers: { Authorization: intervalsAuth(env) } });
  if (!res.ok) throw new Error(`Intervals activities ${res.status}: ${await res.text()}`);
  return res.json();
}

async function fetchIntervalsStreams(env, activityId, typesArr) {
  const types = encodeURIComponent(typesArr.join(","));
  const url = `https://intervals.icu/api/v1/activity/${activityId}/streams?types=${types}`;
  const res = await fetch(url, { headers: { Authorization: intervalsAuth(env) } });
  if (!res.ok) throw new Error(`Intervals streams ${res.status}: ${await res.text()}`);
  return res.json();
}

// Write day wellness: PUT /api/v1/athlete/0/wellness/YYYY-MM-DD
async function putWellnessDay(env, day, patch) {
  const url = `https://intervals.icu/api/v1/athlete/0/wellness/${day}`;
  const res = await fetch(url, {
    method: "PUT",
    headers: {
      Authorization: intervalsAuth(env),
      "Content-Type": "application/json",
    },
    body: JSON.stringify(patch),
  });
  if (!res.ok) throw new Error(`wellness PUT ${day} ${res.status}: ${await res.text()}`);
}

function intervalsAuth(env) {
  // Intervals API uses Basic auth: username "API_KEY", password = your key
  return "Basic " + btoa(`API_KEY:${env.INTERVALS_API_KEY}`);
}

// ====== HELPERS ======
function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function json(obj, status = 200) {
  return new Response(JSON.stringify(obj), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

function clampInt(x, min, max) {
  const n = Number(x);
  if (!Number.isFinite(n)) return min;
  return Math.max(min, Math.min(max, Math.floor(n)));
}