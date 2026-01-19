// src/index.js
// Cloudflare Worker
// - Berechnet VDOT_like + Drift (GA), EF (sonstige), TTT (Intervall)
// - Schreibt AUSSCHLIESSLICH in Intervals Wellnessfelder
// - Schreiben NUR wenn ?write=true
// - Debug-Ausgabe NUR wenn ?debug=true (gibt berechnete Werte als JSON zurück)
// - Steuerung über URL: ?date=YYYY-MM-DD | ?from=...&to=... | ?days=N
//
// Verbesserungen:
// - Streams-Response wird normalisiert (Objekt/Wrapper/Array-Formate)
// - Fallbacks aus Activity-Summary, falls Streams fehlen:
//   - EF = average_speed / average_heartrate
//   - TTT = compliance (%)
//   - Drift optional aus decoupling/pahr_decoupling/pwhr_decoupling falls vorhanden
//
// Required secret:
// - INTERVALS_API_KEY

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
      const debug = (url.searchParams.get("debug") || "").toLowerCase() === "true";

      const date = url.searchParams.get("date"); // YYYY-MM-DD
      const from = url.searchParams.get("from"); // YYYY-MM-DD
      const to = url.searchParams.get("to"); // YYYY-MM-DD
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

      // If debug=true, run synchronously and return results.
      // Otherwise run async via waitUntil and return quickly.
      if (debug) {
        const result = await syncRange(env, oldest, newest, write, true);
        return json(result);
      } else {
        ctx.waitUntil(syncRange(env, oldest, newest, write, false));
        return json({ ok: true, oldest, newest, write });
      }
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
  return syncRange(env, oldest, newest, write, false);
}

async function syncRange(env, oldest, newest, write, debug = false) {
  const acts = await fetchIntervalsActivities(env, oldest, newest);
  acts.sort((a, b) => String(a.start_date || "").localeCompare(String(b.start_date || "")));

  const dayPatch = new Map();
  const debugOut = debug ? {} : null;

  let activitiesSeen = 0;
  let activitiesUsed = 0;

  for (const a of acts) {
    activitiesSeen++;

    if (!isRun(a)) {
      if (debug) addDebug(debugOut, a, null, null, null, "skip:not_run");
      continue;
    }

    const day = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!day) {
      if (debug) addDebug(debugOut, a, null, null, null, "skip:no_day");
      continue;
    }

    const patch = dayPatch.get(day) || {};

    // ---- Prefer summary fallbacks where possible ----
    // EF from summary (always available if HR+speed exist)
    const efSummary = extractActivityEF(a);

    // TTT from summary (Intervals compliance is already a %)
    const tttSummary = extractActivityTTT(a);

    // Drift from summary if present (not in your sample, but supported)
    const driftSummary = extractActivityDecoupling(a);

    // ---- Streams path (for drift and/or better TTT) ----
    let streams = null;
    let qStreams = null;
    let tttStreams = null;

    // Only call streams if we might need them:
    // - For GA: drift is important; if driftSummary missing, try streams
    // - For interval workouts: if compliance missing, try streams TTT
    // - If EF summary missing but streams might have it (rare)
    const mightNeedDrift =
      isProbablyGA(a) && (driftSummary == null); // GA heuristic without q yet
    const mightNeedTTT = (tttSummary == null);
    const mightNeedEF = (efSummary == null);

    if (mightNeedDrift || mightNeedTTT || mightNeedEF) {
      try {
        streams = await fetchIntervalsStreams(env, a.id, [
          "heartrate",
          "velocity_smooth",
          "velocity",
          "pace",
          "time",
          "distance",
        ]);
      } catch (e) {
        // Streams failed - we'll rely on summary fallbacks
        streams = null;
      }

      if (streams) {
        qStreams = calcEfAndDriftFromStreams(streams);
        tttStreams = calcTTTFromStreamsFlexible(streams);
      }
    }

    // ---- Determine EF ----
    const ef = qStreams?.ef_overall ?? efSummary;

    // ---- Determine Drift ----
    // For GA: prefer summary drift if present, else stream-based drift
    const drift = driftSummary ?? qStreams?.drift_pct ?? null;

    // ---- Determine TTT ----
    // Prefer compliance (Intervals own target compliance) if present, else stream heuristic
    const ttt = tttSummary ?? (tttStreams?.isIntervalWorkout ? tttStreams.ttt_pct : null);

    // ---- Decide GA vs non-GA (now that we might have drift) ----
    // If we have streams drift, use it. Otherwise fall back to tags/duration only.
    const ga = isGrundlageWithOptionalDrift(a, drift);

    // ---- Write fields into patch ----
    // GA -> VDOT_like + Drift
    // Non-GA -> EF
    // Interval workout -> TTT
    let wroteSomething = false;

    if (ga) {
      if (ef != null) {
        patch[FIELD_VDOT] = round(vdotLikeFromEf(ef), 1);
        wroteSomething = true;
      }
      if (drift != null) {
        patch[FIELD_DRIFT] = round(drift, 1);
        wroteSomething = true;
      }
    } else {
      if (ef != null) {
        patch[FIELD_EF] = round(ef, 5);
        wroteSomething = true;
      }
    }

    // Always allow TTT to be written if present (it is "intervallleistung" signal)
    if (ttt != null) {
      patch[FIELD_TTT] = round(ttt, 1);
      wroteSomething = true;
    }

    if (!wroteSomething) {
      if (debug) addDebug(debugOut, a, ef, drift, ttt, streams ? "skip:no_metrics" : "skip:no_metrics_no_streams");
      continue;
    }

    dayPatch.set(day, patch);
    activitiesUsed++;

    if (debug) addDebug(debugOut, a, ef, drift, ttt, streams ? "ok" : "ok:summary_only");
  }

  let daysWritten = 0;
  if (write) {
    for (const [day, patch] of dayPatch.entries()) {
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
    daysComputed: dayPatch.size,
    daysWritten: write ? daysWritten : 0,
    patches: debug ? Object.fromEntries(dayPatch.entries()) : undefined,
    debug: debug ? debugOut : undefined,
  };
}

function addDebug(debugOut, a, ef, drift, ttt, status) {
  if (!debugOut) return;
  const day = String(a.start_date_local || a.start_date || "").slice(0, 10) || "unknown-day";
  debugOut[day] ??= [];
  debugOut[day].push({
    activityId: a.id ?? null,
    start: a.start_date ?? null,
    start_local: a.start_date_local ?? null,
    type: a.type ?? a.activity_type ?? null,
    tags: a.tags ?? [],
    stream_types: a.stream_types ?? [],
    has_heartrate: a.has_heartrate ?? null,
    average_speed: a.average_speed ?? null,
    average_heartrate: a.average_heartrate ?? null,
    compliance: a.compliance ?? null,
    status,
    ga: isGrundlageWithOptionalDrift(a, drift),
    ef,
    drift,
    vdot_like: ef != null ? vdotLikeFromEf(ef) : null,
    ttt,
  });
}

// ================= CLASSIFICATION =================
function isRun(a) {
  // strict-ish, but robust for Intervals
  const t = String(a?.type ?? "").toLowerCase();
  return t === "run" || t === "running" || t.includes("run") || t.includes("laufen");
}

function isProbablyGA(a) {
  // used only to decide whether to try streams for drift
  const tags = (a.tags || []).map(String);
  if (tags.some((t) => GA_TAGS.includes(t))) return true;

  const dur = Number(a.moving_time || a.elapsed_time || 0);
  if (dur < 30 * 60) return false;

  // If it's a keyed workout (like your "key:schwelle"), likely not GA
  if ((a.tags || []).some((t) => String(t).startsWith("key:"))) return false;

  return true;
}

function isGrundlageWithOptionalDrift(a, driftMaybe) {
  const tags = (a.tags || []).map(String);
  if (tags.some((t) => GA_TAGS.includes(t))) return true;

  // if it's a key workout -> not GA
  if (tags.some((t) => String(t).startsWith("key:"))) return false;

  const dur = Number(a.moving_time || a.elapsed_time || 0);
  if (dur < 30 * 60) return false;

  // if we have drift, use it to filter GA
  if (driftMaybe != null) return driftMaybe <= 10;

  // otherwise just accept as GA by duration (fallback)
  return true;
}

// ================= SUMMARY FALLBACK EXTRACTORS =================
function extractActivityEF(a) {
  const sp = Number(a?.average_speed);
  const hr = Number(a?.average_heartrate);
  if (Number.isFinite(sp) && sp > 0 && Number.isFinite(hr) && hr > 0) {
    return sp / hr;
  }
  return null;
}

function extractActivityTTT(a) {
  // Intervals "compliance" is already a percent (0..100) in your sample
  const c = Number(a?.compliance);
  if (Number.isFinite(c) && c > 0) return c;
  return null;
}

function extractActivityDecoupling(a) {
  const v1 = Number(a?.pahr_decoupling);
  if (Number.isFinite(v1) && v1 > 0) return v1;

  const v2 = Number(a?.pwhr_decoupling);
  if (Number.isFinite(v2) && v2 > 0) return v2;

  const v3 = Number(a?.decoupling);
  if (Number.isFinite(v3) && v3 > 0) return v3;

  return null;
}

// ================= METRICS FROM STREAMS =================
function calcEfAndDriftFromStreams(streams) {
  const hr = streams.heartrate;
  const speed = pickSpeedFromStreams(streams); // m/s
  if (!hr || !speed) return null;

  const n = Math.min(hr.length, speed.length);
  if (n < 300) return null;

  const half = Math.floor(n / 2);

  const ef = (a, b) => {
    let s = 0,
      c = 0;
    for (let i = a; i < b; i++) {
      const h = hr[i];
      const sp = speed[i];
      if (!h || h < 40) continue;
      if (!sp || sp <= 0) continue;
      s += sp / h;
      c++;
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

// TTT with flexible streams (speed from velocity/velocity_smooth or pace)
function calcTTTFromStreamsFlexible(streams) {
  const speed = pickSpeedFromStreams(streams); // m/s
  if (!speed) return null;
  return calcTTTFromSpeed(speed);
}

function pickSpeedFromStreams(streams) {
  if (!streams) return null;

  // Preferred: velocity_smooth (m/s)
  if (Array.isArray(streams.velocity_smooth) && streams.velocity_smooth.length) {
    return streams.velocity_smooth;
  }

  // Fallback: velocity (m/s)
  if (Array.isArray(streams.velocity) && streams.velocity.length) {
    return streams.velocity;
  }

  // Fallback: pace (likely sec/km in Intervals; convert to m/s)
  if (Array.isArray(streams.pace) && streams.pace.length) {
    // If pace is already m/s, conversion would break.
    // We assume pace is sec/km (common). We add heuristic:
    // - if typical values are > 20, it's likely sec/km
    // - if typical values are < 15, it might already be m/s (rare)
    const p = streams.pace;
    const p50 = percentile(p.filter((x) => typeof x === "number" && x > 0), 50);
    if (p50 && p50 > 20) {
      return p.map((secPerKm) => (secPerKm > 0 ? 1000 / secPerKm : 0));
    }
    // Otherwise, assume it's already speed-like
    return p;
  }

  return null;
}

// “VDOT_like” as trend proxy from EF (scale factor only for readability)
function vdotLikeFromEf(ef) {
  return ef * 1200;
}

// ================= TTT (from speed array) =================
function calcTTTFromSpeed(speed) {
  if (!speed || speed.length < 600) return null;
  const v = speed.filter((x) => typeof x === "number" && x > 0);
  if (v.length < 600) return null;

  const p50 = percentile(v, 50);
  const p90 = percentile(v, 90);
  if (!p50 || !p90 || p90 <= p50 * 1.08) return { isIntervalWorkout: false };

  const workThr = (p50 + p90) / 2;
  const segs = detectSegments(speed, workThr, 60, 25);
  if (segs.length < 3) return { isIntervalWorkout: false };

  const center = meanSegmentSpeed(speed, segs.slice(0, 2));
  if (!center) return { isIntervalWorkout: false };

  const low = center * 0.97;
  const high = center * 1.03;

  let planned = 0,
    hit = 0;
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
  let s = 0,
    c = 0;
  for (const [a, b] of segs) {
    for (let i = a; i < b; i++) {
      const sp = speed[i];
      if (typeof sp === "number" && sp > 0) {
        s += sp;
        c++;
      }
    }
  }
  return c ? s / c : null;
}

function percentile(arr, p) {
  const clean = arr.filter((x) => typeof x === "number" && Number.isFinite(x));
  if (!clean.length) return null;
  const a = [...clean].sort((x, y) => x - y);
  const i = (p / 100) * (a.length - 1);
  const lo = Math.floor(i),
    hi = Math.ceil(i);
  return lo === hi ? a[lo] : a[lo] + (a[hi] - a[lo]) * (i - lo);
}

// ================= INTERVALS API =================
async function fetchIntervalsActivities(env, oldest, newest) {
  const url = `https://intervals.icu/api/v1/athlete/0/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`activities ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchIntervalsStreams(env, id, types) {
  const url = `https://intervals.icu/api/v1/activity/${id}/streams?types=${encodeURIComponent(
    types.join(",")
  )}`;
  const r = await fetch(url, { headers: { Authorization: auth(env) } });
  if (!r.ok) throw new Error(`streams ${r.status}: ${await r.text()}`);

  const raw = await r.json();
  return normalizeStreams(raw);
}

function normalizeStreams(raw) {
  if (!raw) return null;

  // Case 1: already direct object with arrays
  if (
    raw.heartrate ||
    raw.velocity_smooth ||
    raw.velocity ||
    raw.pace ||
    raw.time ||
    raw.distance
  ) {
    return raw;
  }

  // Case 2: wrapper objects
  if (raw.streams && (raw.streams.heartrate || raw.streams.velocity_smooth || raw.streams.velocity || raw.streams.pace)) {
    return raw.streams;
  }
  if (raw.data && (raw.data.heartrate || raw.data.velocity_smooth || raw.data.velocity || raw.data.pace)) {
    return raw.data;
  }

  // Case 3: array format
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

function round(x, n) {
  const p = 10 ** n;
  return Math.round(x * p) / p;
}

function clampInt(x, min, max) {
  const n = Number(x);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
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
  return new Response(JSON.stringify(o), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}