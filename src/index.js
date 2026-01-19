// src/index.js
// Cloudflare Worker
// - Berechnet VDOT_like + Drift (GA), EF (sonstige), TTT (Intervall)
// - Schreibt AUSSCHLIESSLICH in Intervals Wellnessfelder
// - Schreiben NUR wenn ?write=true
// - Steuerung über URL: ?date=YYYY-MM-DD | ?from=...&to=... | ?days=N

export default {
  async scheduled(event, env, ctx) {
    // Cron schreibt IMMER (letzte 14 Tage)
    ctx.waitUntil(sync(env, 14, true));
  },

  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") {
      return new Response("ok");
    }

    if (url.pathname === "/sync") {
      const write = (url.searchParams.get("write") || "").toLowerCase() === "true";

      const date = url.searchParams.get("date"); // YYYY-MM-DD
      const from = url.searchParams.get("from"); // YYYY-MM-DD
      const to = url.searchParams.get("to");     // YYYY-MM-DD
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

      // ---- Safety Rails ----
      if (!isIsoDate(oldest) || !isIsoDate(newest)) {
        return json({ ok: false, error: "Invalid date format (YYYY-MM-DD)" }, 400);
      }

      if (newest < oldest) {
        return json({ ok: false, error: "`to` must be >= `from`" }, 400);
      }

      const rangeDays = diffDays(oldest, newest);
      if (rangeDays > 31) {
        return json({ ok: false, error: "Max range is 31 days" }, 400);
      }

      const oldestAllowed = isoDate(new Date(Date.now() - 365 * 86400000));
      if (oldest < oldestAllowed) {
        return json({ ok: false, error: "Date too old (max 365 days back)" }, 400);
      }

      ctx.waitUntil(syncRange(env, oldest, newest, write));
      return json({ ok: true, oldest, newest, write });
    }

    return new Response("Not found", { status: 404 });
  },
};

// ================= CONFIG =================
const GA_TAGS = ["GA", "Z2", "Easy"];

const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_EF = "EF";
const FIELD_TTT = "TTT";

// ================= SYNC =================
async function sync(env, days, write) {
  const newest = isoDate(new Date());
  const oldest = isoDate(new Date(Date.now() - days * 86400000));
  return syncRange(env, oldest, newest, write);
}

async function syncRange(env, oldest, newest, write) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);
  acts.sort((a, b) => String(a.start_date || "").localeCompare(String(b.start_date || "")));

  const dayPatch = new Map();

  for (const a of acts) {
    if (!isRun(a)) continue;

    const day = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!day) continue;

    const streams = await fetchIntervalsStreams(env, a.id, ["heartrate", "velocity_smooth"]);
    if (!streams) continue;

    const patch = dayPatch.get(day) || {};

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

    const ttt = calcTTTFromSpeed(streams.velocity_smooth);
    if (ttt?.isIntervalWorkout) {
      patch[FIELD_TTT] = round(ttt.ttt_pct, 1);
    }

    if (Object.keys(patch).length) {
      dayPatch.set(day, patch);
    }
  }

  if (write) {
    for (const [day, patch] of dayPatch.entries()) {
      await putWellnessDay(env, day, patch);
    }
  }

  return { ok: true, oldest, newest, write, daysComputed: dayPatch.size };
}

// ================= CLASSIFICATION =================
function isRun(a) {
  const t = String(a.type || a.activity_type || "").toLowerCase();
  return t.includes("run") || t.includes("laufen");
}

function isGrundlage(a, q) {
  const tags = (a.tags || []).map(String);
  if (tags.some(t => GA_TAGS.includes(t))) return true;

  const dur = Number(a.moving_time || a.elapsed_time || 0);
  if (dur < 30 * 60) return false;

  return q.drift_pct <= 10;
}

// ================= METRICS =================
function calcEfAndDrift(streams) {
  const hr = streams.heartrate;
  const v = streams.velocity_smooth;
  if (!hr || !v) return null;

  const n = Math.min(hr.length, v.length);
  if (n < 300) return null;

  const half = Math.floor(n / 2);

  const ef = (a, b) => {
    let s = 0, c = 0;
    for (let i = a; i < b; i++) {
      if (hr[i] > 40 && v[i] > 0) {
        s += v[i] / hr[i];
        c++;
      }
    }
    return c ? s / c : null;
  };

  const ef1 = ef(0, half);
  const ef2 = ef(half, n);
  if (ef1 == null || ef2 == null) return null;

  return {
    ef_overall: (ef1 + ef2) / 2,
    drift_pct: ((ef2 - ef1) / ef1) * 100,
  };
}

function vdotLikeFromEf(ef) {
  return ef * 1200;
}

// ================= TTT =================
function calcTTTFromSpeed(speed) {
  if (!speed || speed.length < 600) return null;
  const v = speed.filter(x => x > 0);
  if (v.length < 600) return null;

  const p50 = percentile(v, 50);
  const p90 = percentile(v, 90);
  if (p90 <= p50 * 1.08) return { isIntervalWorkout: false };

  const workThr = (p50 + p90) / 2;
  const segs = detectSegments(speed, workThr, 60, 25);
  if (segs.length < 3) return { isIntervalWorkout: false };

  const center = meanSegmentSpeed(speed, segs.slice(0, 2));
  if (!center) return { isIntervalWorkout: false };

  const low = center * 0.97;
  const high = center * 1.03;

  let planned = 0, hit = 0;
  for (const [a, b] of segs) {
    planned += b - a;
    for (let i = a; i < b; i++) {
      if (speed[i] >= low && speed[i] <= high) hit++;
    }
  }

  return {
    isIntervalWorkout: true,
    ttt_pct: (hit / planned) * 100,
  };
}

function detectSegments(speed, thr, minLen, maxDrop) {
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
      if (below > maxDrop) break;
      i++;
    }

    const end = i - below;
    if (end - start >= minLen) segs.push([start, end]);
  }
  return segs;
}

function meanSegmentSpeed(speed, segs) {
  let s = 0, c = 0;
  for (const [a, b] of segs) {
    for (let i = a; i < b; i++) {
      if (speed[i] > 0) {
        s += speed[i];
        c++;
      }
    }
  }
  return c ? s / c : null;
}

function percentile(arr, p) {
  const a = [...arr].sort((x, y) => x - y);
  const i = (p / 100) * (a.length - 1);
  const lo = Math.floor(i), hi = Math.ceil(i);
  return lo === hi ? a[lo] : a[lo] + (a[hi] - a[lo]) * (i - lo);
}

// ================= INTERVALS API =================
async function fetchIntervalsActivities(env, oldest, newest) {
  const url = `https://intervals.icu/api/v1/athlete/0/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function fetchIntervalsStreams(env, id, types) {
  const url = `https://intervals.icu/api/v1/activity/${id}/streams?types=${types.join(",")}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

async function putWellnessDay(env, day, patch) {
  const url = `https://intervals.icu/api/v1/athlete/0/wellness/${day}`;
  await fetch(url, {
    method: "PUT",
    headers: { Authorization: auth(env), "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
}

function auth(env) {
  return "Basic " + btoa(`API_KEY:${env.INTERVALS_API_KEY}`);
}

// ================= HELPERS =================
function isoDate(d) {
  return d.toISOString().slice(0, 10);
}

function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function clampInt(x, min, max) {
  const n = Number(x);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, n)) : min;
}

function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s));
}

function diffDays(a, b) {
  return Math.round((new Date(b) - new Date(a)) / 86400000);
}

function json(o, status = 200) {
  return new Response(JSON.stringify(o), {
    status,
    headers: { "content-type": "application/json" },
  });
}