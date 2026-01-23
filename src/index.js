// ====== src/index.js (PART 1/4) ======
// Cloudflare Worker – Run only
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom numeric fields (create these in Intervals):
// VDOT, Drift, Motor
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

      // async fire-and-forget (but don't swallow silently)
      ctx?.waitUntil?.(
        syncRange(env, oldest, newest, write, false, warmupSkipSec).catch((e) => {
          console.error("syncRange failed", e);
        })
      );

      return json({ ok: true, oldest, newest, write, warmupSkipSec });
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // Daily sync: only yesterday+today (and Monday detective if today is Monday).
    // This keeps cost low and still ensures minimum-stimulus comment exists.
    const today = isoDate(new Date());
    const yday = isoDate(new Date(Date.now() - 86400000));

    ctx.waitUntil(
      syncRange(env, yday, today, true, false, 600).catch((e) => {
        console.error("scheduled syncRange failed", e);
      })
    );
  },
};

// ================= CONFIG =================
// ================= GUARDRAILS (NEW) =================
const MAX_KEYS_7D = 2;
const BASE_URL = "https://intervals.icu/api/v1";
const DETECTIVE_KV_PREFIX = "detective:week:";
const DETECTIVE_KV_HISTORY_KEY = "detective:history";
const DETECTIVE_HISTORY_LIMIT = 12;
// REMOVE or stop using this for Aerobic:
// const BIKE_EQ_FACTOR = 0.65;







function mustEnv(env, key) {
  const v = env?.[key];
  if (!v) throw new Error(`Missing env: ${key}`);
  return String(v);
}

// Local YYYY-MM-DD (Europe/Berlin kompatibel genug für Intervals events query)
function toLocalYMD(d) {
  const x = new Date(d);
  const y = x.getFullYear();
  const m = String(x.getMonth() + 1).padStart(2, "0");
  const day = String(x.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

// Fatigue override thresholds (tune later)
const RAMP_PCT_7D_LIMIT = 0.25;    // +25% vs previous 7d
const MONOTONY_7D_LIMIT = 2.0;     // mean/sd daily load
const STRAIN_7D_LIMIT = 1200;      // monotony * weekly load (scale depends on your load units)


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
// ================= MODE / EVENTS (NEW) =================
const EVENT_LOOKAHEAD_DAYS = 365; // how far we look for next event

// AerobicFloor = k * Intensity7  (Bike & Run zählen aerob gleichwertig)
const AEROBIC_K_DEFAULT = 2.8;
const DELOAD_FACTOR = 0.65;
const BLOCK_GROWTH = 1.10;
const BLOCK_HIT_WEEKS = 3;
const INTENSITY_HR_PCT = 0.85;


// Minimum stimulus thresholds per mode (tune later)
const MIN_STIMULUS_7D_RUN_EVENT = 150;   // your current value (5k/run blocks)
const MIN_STIMULUS_7D_BIKE_EVENT = 220;  // bike primary
const MIN_STIMULUS_7D_AEROBIC_OPEN = 220;// open mode: run + bike*factor

// Maintenance anchors (soft hints, not hard fails)
const RUN_MAINTENANCE_14D_MIN = 1;
const BIKE_MAINTENANCE_14D_MIN = 1;

// Streams/types
const STREAM_TYPES_BIKE = ["time", "heartrate", "watts"]; // watts optional

// "Trainingslehre" detective
const LONGRUN_MIN_SECONDS = 60 * 60; // >= 60 minutes
const DETECTIVE_WINDOWS = [14, 28, 42, 56, 84];
const DETECTIVE_MIN_RUNS = 3;
const DETECTIVE_MIN_WEEKS = 2;

const MIN_RUN_SPEED = 1.8;
const MIN_POINTS = 300;
const GA_SPEED_CV_MAX = 0.10;

// Bench
const BENCH_LOOKBACK_DAYS = 180;

// Wellness field codes
const FIELD_VDOT = "VDOT";
const FIELD_DRIFT = "Drift";
const FIELD_MOTOR = "Motor";

// Streams/types we need often
const STREAM_TYPES_GA = ["time", "velocity_smooth", "heartrate"];

// ================= CONTEXT / CACHES =================
function createLimiter(max = 6) {
  let active = 0;
  const queue = [];

  const runNext = () => {
    if (active >= max) return;
    const item = queue.shift();
    if (!item) return;
    active++;
    Promise.resolve()
      .then(item.fn)
      .then(item.resolve, item.reject)
      .finally(() => {
        active--;
        runNext();
      });
  };

  return (fn) =>
    new Promise((resolve, reject) => {
      queue.push({ fn, resolve, reject });
      runNext();
    });
}

function createCtx(env, warmupSkipSec, debug) {
  return {
    env,
    warmupSkipSec,
    debug,
    // activity caches
    activitiesAll: [],
    byDayRuns: new Map(), // YYYY-MM-DD -> run activities
    // streams memo
    byDayBikes: new Map(), // NEW
    streamsCache: new Map(), // activityId -> Promise(streams)
    // derived GA samples cache (for windows)
    gaSampleCache: new Map(), // key: `${endIso}|${windowDays}|${mode}` -> result
    // concurrency limiter
    limit: createLimiter(6),
    // debug accumulator
    debugOut: debug ? {} : null,
  };
}

// ================= HELPERS =================
// ================= KEY CAP + FATIGUE (NEW) =================
function isFiniteNumber(x) {
  return Number.isFinite(Number(x));
}
async function getStreams(ctx, activityId, types) {
  const key = `${activityId}|${(types || []).join(",")}`;

  if (ctx.streamsCache.has(key)) return ctx.streamsCache.get(key);

  const p = ctx.limit(async () => {
    // nutzt env aus ctx, damit authHeader funktioniert
    return fetchIntervalsStreams(ctx.env, activityId, types);
  });

  ctx.streamsCache.set(key, p);
  return p;
}

function inferSportFromEvent(ev) {
  const t = String(ev?.type || "").toLowerCase();
  if (t.includes("run")) return "run";
  if (t.includes("ride") || t.includes("bike") || t.includes("cycling")) return "bike";
  return "unknown";
}

async function computeKeyCount7d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 7 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let keyCount7 = 0;

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (hasKeyTag(a)) keyCount7++;
  }
  return keyCount7;
}

function bucketAllLoadsByDay(acts) {
  const m = {};
  for (const a of acts) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d) continue;
    m[d] = (m[d] || 0) + extractLoad(a);
  }
  return m;
}

async function computeFatigue7d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");

  const start7Iso = isoDate(new Date(end.getTime() - 7 * 86400000));
  const start14Iso = isoDate(new Date(end.getTime() - 14 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const acts14 = ctx.activitiesAll.filter((a) => {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    return d && d >= start14Iso && d < endIso;
  });

  const dailyLoads = bucketAllLoadsByDay(acts14); // day -> load
  const days = Object.keys(dailyLoads).sort();

  // split prev7 and last7 deterministically
  let prev7 = 0;
  let last7 = 0;

  for (const d of days) {
    const v = Number(dailyLoads[d]) || 0;
    if (d >= start7Iso) last7 += v;
    else prev7 += v;
  }

  // monotony/strain for last7 only (need daily values in last7)
  const last7Vals = [];
  for (let i = 0; i < 7; i++) {
    const di = isoDate(new Date(new Date(start7Iso + "T00:00:00Z").getTime() + i * 86400000));
    last7Vals.push(Number(dailyLoads[di]) || 0);
  }
  const mean = avg(last7Vals) ?? 0;
  const sd = std(last7Vals) ?? 0;
  const monotony = sd > 0 ? mean / sd : mean > 0 ? 99 : 0;
  const strain = monotony * (sum(last7Vals) || 0);

  const rampPct = prev7 > 0 ? (last7 - prev7) / prev7 : last7 > 0 ? 999 : 0;

  const keyCount7 = await computeKeyCount7d(ctx, dayIso);

  const reasons = [];
  if (keyCount7 > MAX_KEYS_7D) reasons.push(`Key-Cap: ${keyCount7}/${MAX_KEYS_7D} Key in 7 Tagen`);
  if (rampPct > RAMP_PCT_7D_LIMIT) reasons.push(`Ramp: ${(rampPct * 100).toFixed(0)}% vs vorherige 7 Tage`);
  if (monotony > MONOTONY_7D_LIMIT) reasons.push(`Monotony: ${monotony.toFixed(2)} (> ${MONOTONY_7D_LIMIT})`);
  if (strain > STRAIN_7D_LIMIT) reasons.push(`Strain: ${strain.toFixed(0)} (> ${STRAIN_7D_LIMIT})`);

  const override = reasons.length > 0;

  return {
    override,
    reasons,
    keyCount7,
    rampPct,
    monotony,
    strain,
    last7Load: last7,
    prev7Load: prev7,
  };
}

function applyRecoveryOverride(policy, fatigue) {
  if (!fatigue?.override) return policy;

  return {
    ...policy,
    label: "RECOVERY",
    specificThreshold: 0,
    useAerobicFloor: false,
    recovery: true,
  };
}

// ================= LOAD SUPPORT =================
async function computeLoads7d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 7 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let runTotal7 = 0;
  let bikeTotal7 = 0;

  let aerobicRun7 = 0;
  let aerobicBike7 = 0;

  let intensity7 = 0;
  let intensityKey7 = 0;
  let intensityHr7 = 0;
  let intensityOther7 = 0;

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;

    const load = extractLoad(a);
    const totalLoad = Number.isFinite(load) ? load : 0;

    const run = isRun(a);
    const bike = isBike(a);

    if (run) runTotal7 += totalLoad;
    if (bike) bikeTotal7 += totalLoad;

    const intensityKey = isIntensity(a);
    const intensityHr = isIntensityByHr(a);
    const nonGa = !isAerobic(a);

    if (intensityKey) {
      intensityKey7 += totalLoad;
      intensity7 += totalLoad;
      continue;
    }
    if (intensityHr) {
      intensityHr7 += totalLoad;
      intensity7 += totalLoad;
      continue;
    }
    if (nonGa) {
      intensityOther7 += totalLoad;
      intensity7 += totalLoad;
      continue;
    }

    if (isAerobic(a)) {
      if (run) aerobicRun7 += load;
      else if (bike) aerobicBike7 += load;
    }
  }

  const aerobicEq7 = aerobicRun7 + aerobicBike7; // Bike = 1.0 !
  const totalLoad7 = runTotal7 + bikeTotal7;
  const intensitySignal = intensity7 > 0 ? "ok" : totalLoad7 > 0 ? "low" : "none";
  return {
    runTotal7,
    bikeTotal7,
    aerobicRun7,
    aerobicBike7,
    aerobicEq7,
    intensity7,
    totalLoad7,
    intensitySignal,
    intensitySources: {
      key: intensityKey7,
      hr: intensityHr7,
      nonGa: intensityOther7,
    },
  };
}




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

function hasKv(env) {
  return Boolean(env?.KV && typeof env.KV.get === "function" && typeof env.KV.put === "function");
}

async function readKvJson(env, key) {
  if (!hasKv(env)) return null;
  const raw = await env.KV.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

async function writeKvJson(env, key, value) {
  if (!hasKv(env)) return;
  await env.KV.put(key, JSON.stringify(value));
}

function authHeader(env) {
  return "Basic " + btoa(`API_KEY:${mustEnv(env, "INTERVALS_API_KEY")}`);
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
function isMondayIso(dayIso) {
  const d = new Date(dayIso + "T00:00:00Z");
  return d.getUTCDay() === 1;
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
// ====== src/index.js (PART 2/4) ======

// ================= MAIN =================
async function syncRange(env, oldest, newest, write, debug, warmupSkipSec) {
  const ctx = createCtx(env, warmupSkipSec, debug);

  // We need lookback up to 2*MOTOR_WINDOW_DAYS (and detective up to 84d and bench 180d).
  // For this sync we only need enough to compute what we will write inside [oldest..newest].
  const neededLookbackDays = Math.max(
    2 * MOTOR_WINDOW_DAYS,
    2 * TREND_WINDOW_DAYS,
    7,
    ...DETECTIVE_WINDOWS,
    BENCH_LOOKBACK_DAYS
  );

  const globalOldest = isoDate(new Date(new Date(oldest + "T00:00:00Z").getTime() - neededLookbackDays * 86400000));
  const globalNewest = newest;

  // 1) Fetch ALL activities once
  ctx.activitiesAll = await fetchIntervalsActivities(env, globalOldest, globalNewest);

  // 2) Build byDayRuns / byDayBikes for quick access
  let activitiesSeen = 0;
  let activitiesUsed = 0;

  for (const a of ctx.activitiesAll) {
    activitiesSeen++;
    const day = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!day) {
      if (debug) addDebug(ctx.debugOut, "unknown-day", a, "skip:no_day", null);
      continue;
    }

    if (isRun(a)) {
      if (!ctx.byDayRuns.has(day)) ctx.byDayRuns.set(day, []);
      ctx.byDayRuns.get(day).push(a);
      activitiesUsed++;
      continue;
    }

    if (isBike(a)) {
      if (!ctx.byDayBikes.has(day)) ctx.byDayBikes.set(day, []);
      ctx.byDayBikes.get(day).push(a);
      continue;
    }

    if (debug) addDebug(ctx.debugOut, day, a, `skip:unsupported:${a.type ?? "unknown"}`, null);
  }


  const patches = {};
  const notesPreview = debug ? {} : null;

  let daysWritten = 0;
  const daysList = listIsoDaysInclusive(oldest, newest);

  for (const day of daysList) {
    // NEW: mode + policy for this day (based on next event)
    let modeInfo;
    let policy;
    try {
      modeInfo = await determineMode(env, day, ctx.debug);
      policy = getModePolicy(modeInfo);
    } catch (e) {
      modeInfo = { mode: "OPEN", primary: "open", nextEvent: null };
      policy = getModePolicy(modeInfo);
    }
    // NEW: fatigue / key-cap override
    let fatigue = null;
    try {
      fatigue = await computeFatigue7d(ctx, day);
      policy = applyRecoveryOverride(policy, fatigue);
    } catch {
      fatigue = null;
    }

    const runs = ctx.byDayRuns.get(day) ?? [];
    const patch = {};
    const perRunInfo = [];

    // Motor Index (works even if no run today)
    let motor = null;
    try {
      motor = await computeMotorIndex(ctx, day);
      if (motor?.value != null) patch[FIELD_MOTOR] = round(motor.value, 1);
    } catch (e) {
      motor = { ok: false, value: null, text: `🏎️ Motor-Index: n/a – Fehler (${String(e?.message ?? e)})` };
    }

    // Process runs (collect detailed info, but write VDOT/Drift from a single representative GA run)
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
          const streams = await getStreams(ctx, a.id, STREAM_TYPES_GA);
          const ds = computeDriftAndStabilityFromStreams(streams, ctx.warmupSkipSec);
          drift_raw = Number.isFinite(ds?.pa_hr_decouple_pct) ? ds.pa_hr_decouple_pct : null;

          drift = drift_raw;

          // Negative drift => do not write numeric, but keep raw and source
          if (drift != null && drift < 0) {
            drift = null;
            drift_source = "streams_negative_dropped";
          }
          if (drift == null && drift_source === "streams") drift_source = "streams_insufficient";
        } catch (e) {
          drift = null;
          drift_source = "streams_failed";
          if (debug) {
            addDebug(ctx.debugOut, day, a, "warn:streams_failed", {
              message: String(e?.message ?? e),
              stack: String(e?.stack ?? ""),
              activityId: a.id,
              streamTypes: a?.stream_types ?? null,
            });
          }
        }

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
        addDebug(ctx.debugOut, day, a, "ok", {
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

    // Choose ONE representative GA run for numeric fields (prevents overwrite randomness)
    const rep = pickRepresentativeGARun(perRunInfo);
    if (rep) {
      if (rep.ef != null) patch[FIELD_VDOT] = round(vdotLikeFromEf(rep.ef), 1);
      if (rep.drift != null) patch[FIELD_DRIFT] = round(rep.drift, 1);
    }

    // Aerobic trend (GA-only)
    let trend;
    try {
      trend = await computeAerobicTrend(ctx, day);
    } catch (e) {
      trend = { ok: false, text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – Fehler (${String(e?.message ?? e)})` };
    }

    // NEW: loads + min stimulus depends on mode
    let loads7 = { runLoad7: 0, bikeLoad7: 0, aerobicEq7: 0 };
    try {
      loads7 = await computeLoads7d(ctx, day);
    } catch {}

    let specificValue = 0;
    if (policy.specificKind === "run") specificValue = loads7.runTotal7;
    else if (policy.specificKind === "bike") specificValue = loads7.bikeTotal7;
    else specificValue = 0;

    const specificOk = policy.specificThreshold > 0 ? specificValue >= policy.specificThreshold : true;
    const aerobicEq = loads7.aerobicEq7 ?? 0;
    const intensity = loads7.intensity7 ?? 0;
    const intensitySignal = loads7.intensitySignal ?? "none";
    const aerobicFloorActive = policy.useAerobicFloor && intensitySignal === "ok";

    const aerobicFloor = aerobicFloorActive ? policy.aerobicK * intensity : 0;
    const aerobicOk = aerobicFloorActive ? aerobicEq >= aerobicFloor : true;


async function computeMaintenance14d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 14 * 86400000));
  const endIso = dayIso;

  let runCount14 = 0;
  let bikeCount14 = 0;

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (isRun(a)) runCount14++;
    else if (isBike(a)) bikeCount14++;
  }
  return { runCount14, bikeCount14 };
}

    // Bench reports only on bench days
    const benchReports = [];
    for (const a of runs) {
      const benchName = getBenchTag(a);
      if (!benchName) continue;
      try {
        const rep = await computeBenchReport(env, a, benchName, ctx.warmupSkipSec);
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
      benchReports,
      modeInfo,
      policy,
      loads7,
      fatigue,
      specificOk,
      specificValue,
      aerobicOk,
      aerobicFloor,
      aerobicFloorActive,
    });





    patches[day] = patch;

    // Monday detective NOTE (calendar) – always on Mondays, even if no run
    if (isMondayIso(day)) {
      let detectiveNoteText = null;
      try {
        const detectiveNote = await computeDetectiveNoteAdaptive(env, day, ctx.warmupSkipSec);
        detectiveNoteText = detectiveNote?.text ?? "";
        if (write) {
          await persistDetectiveSummary(env, day, detectiveNote?.summary);
        }
      } catch (e) {
        detectiveNoteText = `🕵️‍♂️ Montags-Detektiv\nFehler: ${String(e?.message ?? e)}`;
      }
      if (write) {
        await upsertMondayDetectiveNote(env, day, detectiveNoteText);
      }
      if (debug) notesPreview[day] = detectiveNoteText;
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
    debug: debug ? ctx.debugOut : undefined,
  };
}

// Representative GA run: longest GA (not key), tie-breaker: has drift, then higher moving_time
function pickRepresentativeGARun(perRunInfo) {
  const ga = perRunInfo.filter((x) => x.ga && !x.isKey);
  if (!ga.length) return null;
  ga.sort((a, b) => {
    const ta = Number(a.moving_time) || 0;
    const tb = Number(b.moving_time) || 0;
    if (tb !== ta) return tb - ta;
    const ad = a.drift != null ? 1 : 0;
    const bd = b.drift != null ? 1 : 0;
    if (bd !== ad) return bd - ad;
    return 0;
  });
  return ga[0] || null;
}

function buildIntensityLine(loads7) {
  const total = Math.round(loads7?.intensity7 ?? 0);
  const sources = loads7?.intensitySources || {};
  const parts = [];
  if ((sources.key ?? 0) > 0) parts.push(`key ${Math.round(sources.key)}`);
  if ((sources.hr ?? 0) > 0) parts.push(`HR≥${Math.round(INTENSITY_HR_PCT * 100)}% ${Math.round(sources.hr)}`);
  if ((sources.nonGa ?? 0) > 0) parts.push(`non-GA ${Math.round(sources.nonGa)}`);

  const signal = loads7?.intensitySignal ?? "none";
  if (signal === "low" && total === 0) return "Intensity: low (numeric=0)";
  if (signal === "low") return `Intensity: ${total} (Signal niedrig: nur GA/fehlendes HR)`;
  if (signal === "none") return `Intensity: ${total} (keine Daten)`;
  if (!parts.length) return `Intensity: ${total}`;
  return `Intensity: ${total} (${parts.join(" + ")})`;
}

function buildBottomLine({
  hadAnyRun,
  hadKey,
  hadGA,
  fatigue,
  policy,
  specificOk,
  hasSpecific,
  aerobicOk,
  intensitySignal,
}) {
  let today = "Rest oder locker (nach Gefühl)";
  if (fatigue?.override) {
    const reason = fatigue.reasons?.[0] ? ` – ${fatigue.reasons[0]}` : "";
    today = `Rest/Recovery${reason}`;
  } else if (hadKey) {
    today = "Key erledigt ✅ (qualitativ)";
  } else if (hadGA) {
    today = "Easy/GA ✅ (locker)";
  } else if (hadAnyRun) {
    today = "Easy ✅";
  } else {
    today = "Rest (kein Lauf)";
  }

  let next = "45–60min GA locker";
  if (fatigue?.override) {
    next = "25–40min locker nach Ruhetag";
  } else if (hasSpecific && !specificOk) {
    next = "35–50min locker/steady (Run) – Volumen auffüllen";
  } else if (policy?.useAerobicFloor && intensitySignal === "ok" && !aerobicOk) {
    next = "30–45min locker (kein Key) – Intensität deckeln";
  } else if ((fatigue?.keyCount7 ?? 0) === 0) {
    next = "Schwelle (20–30min) ODER 45–60min GA – je nach Frische";
  }

  let trigger = "keine Abweichung";
  if (fatigue?.override) {
    trigger = "Fatigue Override (RECOVERY)";
  } else if (hasSpecific && !specificOk) {
    trigger = `${policy?.specificLabel ?? "RunFloor"} unterschritten`;
  } else if (policy?.useAerobicFloor && intensitySignal === "ok" && !aerobicOk) {
    trigger = "AerobicFloor unterschritten";
  }

  return { today, next, trigger };
}

// ================= COMMENT =================
function renderWellnessComment({
  perRunInfo,
  trend,
  motor,
  benchReports,
  modeInfo,
  policy,
  loads7,
  fatigue,
  specificOk,
  specificValue,
  aerobicOk,
  aerobicFloor,
  aerobicFloorActive
}) {
  const hadKey = perRunInfo.some((x) => x.isKey);
  const hadGA = perRunInfo.some((x) => x.ga && !x.isKey);
  const hadAnyRun = perRunInfo.length > 0;
  const hasSpecific = (policy?.specificThreshold ?? 0) > 0;
  const intensitySignal = loads7?.intensitySignal ?? "none";

  const lines = [];
  lines.push("ℹ️ Tages-Status");
  lines.push("");

  if (!hadAnyRun) lines.push("Heute: Kein Lauf");
  else if (hadKey && !hadGA) lines.push("Heute: Schlüsseltraining (Key)");
  else if (hadGA && !hadKey) lines.push("Heute: Grundlage (GA)");
  else if (hadKey && hadGA) lines.push("Heute: Gemischt (GA + Key)");
  else lines.push("Heute: Lauf");

  lines.push("");
  lines.push(`🧭 Mode: ${policy?.label ?? "OPEN"}`);

  if (modeInfo?.nextEvent) {
    const d = String(modeInfo.nextEvent.start_date_local || modeInfo.nextEvent.start_date || "").slice(0, 10);
    const n = String(modeInfo.nextEvent.name || "RACE");
    lines.push(`Nächstes Event: ${d} – ${n}`);
  } else {
    lines.push("Nächstes Event: keines → OPEN MODE");
  }

  lines.push("");
  lines.push(trend?.text ?? "ℹ️ Aerober Kontext (nur GA)\nTrend: n/a");

  lines.push("");
  lines.push(motor?.text ?? "🏎️ Motor-Index: n/a");

  if (Array.isArray(benchReports) && benchReports.length) {
    lines.push("");
    lines.push(benchReports.join("\n\n"));
  }

  if (fatigue?.override) {
    lines.push("");
    lines.push("🛡️ Fatigue Override: RECOVERY");
    for (const r of fatigue.reasons.slice(0, 5)) lines.push(`- ${r}`);
    lines.push("➡️ Empfehlung: heute keine harte Einheit. Fokus: easy / locker / Technik / frei.");
  } else if (fatigue?.keyCount7 != null) {
    lines.push("");
    lines.push(`🧨 Keys (7 Tage): ${fatigue.keyCount7}/${MAX_KEYS_7D}`);
  }

  // Load block
  lines.push("");
  lines.push("📦 Load (7 Tage)");
  lines.push(`Run: ${Math.round(loads7?.runTotal7 ?? 0)} | Bike: ${Math.round(loads7?.bikeTotal7 ?? 0)}`);
  lines.push(
    `AerobicEq: ${Math.round(loads7?.aerobicEq7 ?? 0)} (AerobicRun ${Math.round(loads7?.aerobicRun7 ?? 0)} + AerobicBike ${Math.round(loads7?.aerobicBike7 ?? 0)})`
  );
  const intensityLine = buildIntensityLine(loads7);
  lines.push(intensityLine);

  // Floors (nur EINMAL – du hattest es doppelt)
  lines.push("");
  lines.push("🎯 Floors (7 Tage)");

  // Specific (z.B. RunFloor / BikeFloor)
  if (hasSpecific) {
    const label = policy?.specificLabel ?? "SpecificFloor";
    lines.push(
      `${label}: ${Math.round(policy.specificThreshold)} ${specificOk ? "✅" : "⚠️"} (${Math.round(specificValue)})`
    );
  }

  // AerobicFloor (Intensity Guard)
  if (policy?.useAerobicFloor) {
    if (!aerobicFloorActive) {
      lines.push("AerobicFloor: n/a (Intensity-Signal zu schwach)");
    } else {
      lines.push(
        `AerobicFloor: ${Math.round(aerobicFloor)} ${aerobicOk ? "✅" : "⚠️"} (k=${policy.aerobicK} × Intensity ${Math.round(
          loads7?.intensity7 ?? 0
        )})`
      );
    }
  }
  lines.push("Hinweis: RunFloor basiert nur auf Run-Load; Bike zählt nur für AerobicEq.");

  // ================= OPTION B: Interpretation (ohne "SpecificFloor verfehlt") =================
  if (!policy?.recovery && hasSpecific && !specificOk && aerobicOk) {
    lines.push("");
    lines.push("💬 Interpretation");
    lines.push(
      "Zu wenig sportartspezifisches Volumen im betrachteten Zeitraum. " +
        "Die Intensitätsverteilung ist aktuell unkritisch – fehlendes Volumen sollte " +
        "durch mehr lockere/steady Dauer in der Ziel-Sportart ergänzt werden, " +
        "nicht durch höhere Intensität."
    );
  }

  // Empfehlungen (ohne redundantes SpecificFloor-Prosa)
  lines.push("");
  if (policy?.recovery) {
    lines.push("➡️ RECOVERY aktiv: keine Floors erzwungen. Fokus: locker / Technik / frei.");
  } else if (policy?.useAerobicFloor && intensitySignal === "ok" && !aerobicOk) {
    lines.push("➡️ AerobicFloor verfehlt: Intensität diese Woche deckeln (max 1× Key), mehr locker/aerob auffüllen (Run oder Bike).");
  } else if (!specificOk && hasSpecific) {
    // bewusst kurz halten, weil Interpretation den Inhalt liefert
    lines.push("➡️ Fokus: mehr lockere/steady Dauer in der Ziel-Sportart.");
  } else {
    lines.push("➡️ Grün: Floors ok. Qualität möglich (phaseabhängig), Rest locker.");
  }

  lines.push("");
  lines.push("✅ Bottom line");
  const bottomLine = buildBottomLine({
    hadAnyRun,
    hadKey,
    hadGA,
    fatigue,
    policy,
    specificOk,
    hasSpecific,
    aerobicOk,
    intensitySignal,
  });
  lines.push(`Trigger: ${bottomLine.trigger}`);
  lines.push(`Heute: ${bottomLine.today}`);
  lines.push(`Nächster Lauf: ${bottomLine.next}`);

  return lines.join("\n");
}


// ================= TREND (GA-only) =================
function trendConfidence(nRecent, nPrev) {
  const n = Math.min(nRecent ?? 0, nPrev ?? 0);
  if (n >= 6) return "hoch";
  if (n >= 3) return "mittel";
  return "niedrig";
}

async function computeAerobicTrend(ctx, dayIso) {

  const endIso = dayIso;

  // We compare last 28d vs previous 28d (within last 56d)
  const recentStart = isoDate(new Date(new Date(endIso + "T00:00:00Z").getTime() - TREND_WINDOW_DAYS * 86400000));
  const prevStart = isoDate(new Date(new Date(endIso + "T00:00:00Z").getTime() - 2 * TREND_WINDOW_DAYS * 86400000));

  const gaActs = await gatherGASamples(ctx, endIso, 2 * TREND_WINDOW_DAYS, { comparable: false });

  // split by date string (deterministic)
  const recent = gaActs.filter((x) => x.date >= recentStart);
  const prev = gaActs.filter((x) => x.date < recentStart && x.date >= prevStart);

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
    emoji = "🟠";
    label = "Warnsignal";
  }

  const confidence = trendConfidence(recent.length, prev.length);
  return {
    ok: true,
    dv,
    dd,
    text:
      `${emoji} ${label}${label === "Warnsignal" && confidence === "mittel" ? " (Confidence: mittel)" : ""}\n` +
      `Aerober Kontext (nur GA): VDOT ~ ${dv.toFixed(1)}% | HR-Drift ${dd > 0 ? "↑" : "↓"} ${Math.abs(dd).toFixed(
        1
      )}%-Pkt\n` +
      `Confidence: ${confidence} (recent=${recent.length}, prev=${prev.length})`,
  };
}

// ================= MOTOR INDEX (GA comparable only) =================
async function buildMotorFallback(ctx, dayIso) {
  const samples = await gatherGASamples(ctx, dayIso, MOTOR_WINDOW_DAYS, { comparable: false });
  if (!samples.length) return null;
  const last = samples.slice().sort((a, b) => a.date.localeCompare(b.date)).at(-1);
  if (!last) return null;
  return `↪️ Fallback: letzter GA-Lauf ${last.date} | EF ${last.ef.toFixed(5)} | Drift ${last.drift.toFixed(1)}%`;
}

async function computeMotorIndex(ctx, dayIso) {
  const endIso = dayIso;

  // Need 56d window for 28+28 split
  const samples = await gatherGASamples(ctx, endIso, 2 * MOTOR_WINDOW_DAYS, { comparable: true, needCv: true });

  // stale check: most recent sample date
  const lastDate = samples.length ? samples.map((s) => s.date).sort().at(-1) : null;
  if (!lastDate) {
    const fallback = await buildMotorFallback(ctx, dayIso);
    return {
      ok: false,
      value: null,
      text: `🏎️ Motor-Index: n/a (keine vergleichbaren GA-Läufe im Fenster)${fallback ? `\n${fallback}` : ""}`,
    };
  }
  const ageDays = diffDays(lastDate, dayIso);
  if (ageDays > MOTOR_STALE_DAYS) {
    const fallback = await buildMotorFallback(ctx, dayIso);
    return {
      ok: false,
      value: null,
      text: `🏎️ Motor-Index: n/a (letzter vergleichbarer GA-Lauf vor ${ageDays} Tagen: ${lastDate})${
        fallback ? `\n${fallback}` : ""
      }`,
    };
  }

  const midIso = isoDate(new Date(new Date(endIso + "T00:00:00Z").getTime() - MOTOR_WINDOW_DAYS * 86400000));
  const prevStartIso = isoDate(new Date(new Date(endIso + "T00:00:00Z").getTime() - 2 * MOTOR_WINDOW_DAYS * 86400000));

  const recent = samples.filter((x) => x.date >= midIso);
  const prev = samples.filter((x) => x.date < midIso && x.date >= prevStartIso);

  if (recent.length < MOTOR_NEED_N_PER_HALF || prev.length < MOTOR_NEED_N_PER_HALF) {
    const fallback = await buildMotorFallback(ctx, dayIso);
    return {
      ok: false,
      value: null,
      text: `🏎️ Motor-Index: n/a (zu wenig vergleichbare GA-Läufe: recent=${recent.length}, prev=${prev.length})${
        fallback ? `\n${fallback}` : ""
      }`,
    };
  }

  const ef1 = median(recent.map((x) => x.ef));
  const ef0 = median(prev.map((x) => x.ef));
  if (ef0 == null || ef1 == null) {
    const fallback = await buildMotorFallback(ctx, dayIso);
    return {
      ok: false,
      value: null,
      text: `🏎️ Motor-Index: n/a (fehlende EF-Werte)${fallback ? `\n${fallback}` : ""}`,
    };
  }

  // Drift trend: last 14d vs previous 14d within last 28d
  const mid14Iso = isoDate(new Date(new Date(endIso + "T00:00:00Z").getTime() - MOTOR_DRIFT_WINDOW_DAYS * 86400000));
  const prev14StartIso = isoDate(
    new Date(new Date(endIso + "T00:00:00Z").getTime() - 2 * MOTOR_DRIFT_WINDOW_DAYS * 86400000)
  );

  const recent14 = samples.filter((x) => x.date >= mid14Iso);
  const prev14 = samples.filter((x) => x.date < mid14Iso && x.date >= prev14StartIso);

  const d1 = recent14.length ? median(recent14.map((x) => x.drift)) : null;
  const d0 = prev14.length ? median(prev14.map((x) => x.drift)) : null;

  const dv = ((ef1 - ef0) / ef0) * 100; // + good
  const dd = d0 != null && d1 != null ? d1 - d0 : null; // + bad

  let val = 50;
  val += clamp(dv, -6, 6) * 4;
  if (dd != null) val += clamp(-dd, -6, 6) * 2;
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


// ================= GA SAMPLE GATHERER (shared + cached) =================
async function gatherGASamples(ctx, endIso, windowDays, opts) {
  const mode = `${opts?.comparable ? "comp" : "ga"}|${opts?.needCv ? "cv" : "nocv"}`;
  const key = `${endIso}|${windowDays}|${mode}`;
  if (ctx.gaSampleCache.has(key)) return ctx.gaSampleCache.get(key);

  const end = new Date(endIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));

  const p = (async () => {
    const samples = [];

    for (const a of ctx.activitiesAll) {
      const date = String(a.start_date_local || a.start_date || "").slice(0, 10);
      if (!date) continue;
      if (date < startIso || date >= endIso) continue;

      if (!isRun(a)) continue;
      if (hasKeyTag(a)) continue;

      if (opts?.comparable) {
        if (!isGAComparable(a)) continue;
      } else {
        if (!isGA(a)) continue;
      }

      const ef = extractEF(a);
      if (ef == null) continue;

      try {
        const streams = await getStreams(ctx, a.id, STREAM_TYPES_GA);
        const ds = computeDriftAndStabilityFromStreams(streams, ctx.warmupSkipSec);
        let drift = Number.isFinite(ds?.pa_hr_decouple_pct) ? ds.pa_hr_decouple_pct : null;

        const cv = Number.isFinite(ds?.speed_cv) ? ds.speed_cv : null;

        if (drift == null) continue;
        if (drift < 0) continue; // keep your “negative dropped” rule for signal stability

        if (opts?.needCv) {
          if (cv == null || cv > GA_SPEED_CV_MAX) continue;
        }

        samples.push({ date, ef, drift });
      } catch {
        // ignore sample
      }
    }

    return samples;
  })();

  ctx.gaSampleCache.set(key, p);
  return p;
}
// ================= MONDAY DETECTIVE NOTE (TRAININGSLEHRE V2) =================
async function persistDetectiveSummary(env, mondayIso, summary) {
  if (!summary || !hasKv(env)) return;
  const key = `${DETECTIVE_KV_PREFIX}${mondayIso}`;
  await writeKvJson(env, key, summary);

  const history = (await readKvJson(env, DETECTIVE_KV_HISTORY_KEY)) || [];
  const next = [key, ...history.filter((k) => k !== key)].slice(0, DETECTIVE_HISTORY_LIMIT);
  await writeKvJson(env, DETECTIVE_KV_HISTORY_KEY, next);
}

async function loadDetectiveHistory(env, mondayIso) {
  if (!hasKv(env)) return [];
  const key = `${DETECTIVE_KV_PREFIX}${mondayIso}`;
  const history = (await readKvJson(env, DETECTIVE_KV_HISTORY_KEY)) || [];
  const keys = history.filter((k) => k !== key).slice(0, DETECTIVE_HISTORY_LIMIT);
  const summaries = [];
  for (const k of keys) {
    const s = await readKvJson(env, k);
    if (s) summaries.push(s);
  }
  return summaries;
}

function buildDetectiveWhyInsights(current, previous) {
  if (!current || !previous) return null;

  const improvements = [];
  const regressions = [];
  const context = [];
  const actions = [];
  const helped = [];

  const pct = (a, b) => (a != null && b != null && b !== 0 ? ((a - b) / b) * 100 : null);

  const efPct = pct(current.efMed, previous.efMed);
  const driftDelta = current.driftMed != null && previous.driftMed != null ? current.driftMed - previous.driftMed : null;

  if (efPct != null && efPct >= 1 && driftDelta != null && driftDelta <= -1) {
    improvements.push(`Ökonomie besser: EF +${efPct.toFixed(1)}% & Drift ${driftDelta.toFixed(1)}%-Pkt.`);
    helped.push("Stabilere, ökonomischere GA-Läufe (EF ↑, Drift ↓).");
  } else if (efPct != null && efPct <= -1 && driftDelta != null && driftDelta >= 1) {
    regressions.push(`Ökonomie schlechter: EF ${efPct.toFixed(1)}% & Drift +${driftDelta.toFixed(1)}%-Pkt.`);
    actions.push("Mehr ruhige GA-Läufe für Ökonomie & Stabilität (konstant, nicht hart).");
  } else {
    if (efPct != null && Math.abs(efPct) >= 1) {
      (efPct > 0 ? improvements : regressions).push(`EF ${efPct > 0 ? "+" : ""}${efPct.toFixed(1)}% (Ökonomie).`);
      if (efPct > 0) {
        helped.push("Bessere Laufökonomie (EF ↑) – das hat geholfen.");
      } else {
        actions.push("Mehr Grundlagentempo (GA) für bessere Ökonomie.");
      }
    }
    if (driftDelta != null && Math.abs(driftDelta) >= 1) {
      (driftDelta < 0 ? improvements : regressions).push(`Drift ${driftDelta.toFixed(1)}%-Pkt (Stabilität).`);
      if (driftDelta < 0) {
        helped.push("Stabilere GA-Läufe mit weniger Drift.");
      } else {
        actions.push("Mehr stabile, gleichmäßige GA-Läufe (weniger Drift).");
      }
    }
  }

  const loadPct = pct(current.weeklyLoad, previous.weeklyLoad);
  const runFreqDelta = current.runsPerWeek != null && previous.runsPerWeek != null ? current.runsPerWeek - previous.runsPerWeek : null;
  const longDelta = current.longPerWeek != null && previous.longPerWeek != null ? current.longPerWeek - previous.longPerWeek : null;

  if (loadPct != null && loadPct >= 10 && (longDelta == null || longDelta >= 0)) {
    improvements.push(`Reizaufbau: Wochenload +${loadPct.toFixed(0)}% (Longruns stabil/↑).`);
    helped.push("Mehr Wochenreiz mit stabilen/mehr Longruns.");
  }
  if (loadPct != null && loadPct <= -10 && runFreqDelta != null && runFreqDelta <= -0.5) {
    regressions.push(`Reizverlust: Wochenload ${loadPct.toFixed(0)}% & Frequenz ↓ (${runFreqDelta.toFixed(1)}/Woche).`);
    actions.push("Frequenz & Wochenload wieder stabil erhöhen (zuerst kurz & locker).");
  }

  const monotonyDelta =
    current.monotony != null && previous.monotony != null ? current.monotony - previous.monotony : null;
  const strainDelta =
    current.strain != null && previous.strain != null ? current.strain - previous.strain : null;

  if (monotonyDelta != null && strainDelta != null) {
    if (monotonyDelta >= 0.3 && strainDelta >= 150) {
      regressions.push("Belastungsdichte hoch: Monotonie ↑ & Strain ↑ → Erholungsrisiko.");
      actions.push("Mehr Variabilität/Erholung einbauen (Monotonie senken).");
    } else if (monotonyDelta <= -0.3 && strainDelta <= -150) {
      improvements.push("Belastungsdichte entspannt: Monotonie ↓ & Strain ↓.");
      helped.push("Entspanntere Belastungsdichte (Monotonie/Strain ↓).");
    }
  }

  if (current.compN != null && current.compN < 2) {
    context.push("Messbasis dünn: wenige GA comparable → Trends unsicher.");
  }

  if (!improvements.length && !regressions.length && !context.length) return null;

  return {
    title: `Warum (Vergleich zu ${previous.week})`,
    improvements,
    regressions,
    context,
    actions,
    helped,
  };
}

function appendWhySection(lines, insights) {
  if (!insights) return;
  lines.push("");
  lines.push(insights.title);
  lines.push(`- Kurz gesagt: ${buildWhySummary(insights)}.`);
  if (!insights.improvements.length && !insights.regressions.length) {
    lines.push("- Keine klaren Veränderungen.");
  } else {
    if (insights.improvements.length) {
      lines.push("- Verbesserungen:");
      for (const item of insights.improvements) lines.push(`  - ${item}`);
    }
    if (insights.regressions.length) {
      lines.push("- Verschlechterungen:");
      for (const item of insights.regressions) lines.push(`  - ${item}`);
    }
  }
  if (insights.context.length) {
    lines.push("- Kontext:");
    for (const item of insights.context) lines.push(`  - ${item}`);
  }
  if (insights.actions.length) {
    lines.push("- So wirst du besser:");
    for (const item of insights.actions) lines.push(`  - ${item}`);
  }
  if (insights.helped.length) {
    lines.push("- Das hat zuletzt geholfen:");
    for (const item of insights.helped) lines.push(`  - ${item}`);
  }
}

function buildWhySummary(insights) {
  const hasImprovements = insights.improvements.length > 0;
  const hasRegressions = insights.regressions.length > 0;

  if (hasImprovements && hasRegressions) {
    return "gemischtes Bild – einige Fortschritte, aber auch spürbare Rückschritte";
  }
  if (hasImprovements) {
    return "überwiegend Fortschritte gegenüber der Vorwoche";
  }
  if (hasRegressions) {
    return "überwiegend Rückschritte gegenüber der Vorwoche";
  }
  return "keine klaren Veränderungen zur Vorwoche";
}

function applyDetectiveWhy(rep, insights) {
  if (!insights) return rep;
  const lines = rep.text.split("\n");
  appendWhySection(lines, insights);
  return { ...rep, text: lines.join("\n"), insights };
}

async function computeDetectiveNoteAdaptive(env, mondayIso, warmupSkipSec) {
  for (const w of DETECTIVE_WINDOWS) {
    const rep = await computeDetectiveNote(env, mondayIso, warmupSkipSec, w);
    if (rep.ok) {
      const history = await loadDetectiveHistory(env, mondayIso);
      const insights = buildDetectiveWhyInsights(rep.summary, history[0]);
      return applyDetectiveWhy(rep, insights);
    }
  }
  // fallback: last attempt (most info)
  const last = await computeDetectiveNote(
    env,
    mondayIso,
    warmupSkipSec,
    DETECTIVE_WINDOWS[DETECTIVE_WINDOWS.length - 1]
  );
  const history = await loadDetectiveHistory(env, mondayIso);
  const insights = buildDetectiveWhyInsights(last.summary, history[0]);
  return applyDetectiveWhy(last, insights);
}

function buildMiniPlanTargets({ runsPerWeek, weeklyLoad }) {
  let runTarget = "3–4";
  if (runsPerWeek < 2) runTarget = "2–3";
  else if (runsPerWeek < 3) runTarget = "3";

  let loadTarget = "150–210";
  if (weeklyLoad < 120) loadTarget = "110–160";
  else if (weeklyLoad < 180) loadTarget = "140–200";
  else if (weeklyLoad >= 180) {
    const low = Math.max(120, Math.round(weeklyLoad * 0.9));
    const high = Math.round(weeklyLoad * 1.1);
    loadTarget = `${low}–${high}`;
  }

  const exampleWeek =
    runTarget === "2–3"
      ? ["Mi 30–35′ easy", "So 60–75′ longrun"]
      : ["Mi 30–35′ easy", "Fr 40–50′ GA", "So 60–75′ longrun"];

  return { runTarget, loadTarget, exampleWeek };
}

async function computeDetectiveNote(env, mondayIso, warmupSkipSec, windowDays) {
  const end = new Date(mondayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - windowDays * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));
  const runs = acts
    .filter((a) => isRun(a))
    .map((a) => ({
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
    }))
    .filter((x) => x.date);

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

  // Monotony/strain (simple) – include zero days for the full window
  const dailyLoads = bucketLoadsByDay(runs); // {day: loadSum} (runs only)
  const startIso = isoDate(start);
  const endIso = isoDate(new Date(end.getTime() - 86400000));
  const daysAll = listIsoDaysInclusive(startIso, endIso);
  const loadArr = daysAll.map((d) => Number(dailyLoads[d]) || 0);
  const meanLoad = avg(loadArr) ?? 0;
  const sdLoad = std(loadArr) ?? 0;
  const monotony = sdLoad > 0 ? meanLoad / sdLoad : meanLoad > 0 ? 99 : 0;
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
      findings.push(
        `Longrun-Frequenz niedrig: ${longRuns.length}× in ${windowDays} Tagen (~${longPerWeek.toFixed(1)}/Woche).`
      );
      actions.push("Longrun-Frequenz Richtung 1×/Woche stabilisieren.");
    }

    // Key
    if (keyRuns.length === 0) {
      findings.push(`Zu wenig Qualität: 0× Key (key:*) in ${windowDays} Tagen.`);
      actions.push("Wenn Aufbau/Spezifisch: 1× Key/Woche (Schwelle ODER VO2) einbauen.");
    } else if (keyPerWeek < 0.6 && windowDays >= 14) {
      findings.push(
        `Key-Frequenz niedrig: ${keyRuns.length}× in ${windowDays} Tagen (~${keyPerWeek.toFixed(1)}/Woche).`
      );
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
    findings.push(
      `Messbasis (GA comparable): n=${comp.n} | EF(med)=${
        comp.efMed != null ? comp.efMed.toFixed(5) : "n/a"
      } | Drift(med)=${comp.driftMed != null ? comp.driftMed.toFixed(1) + "%" : "n/a"}`
    );
    if (comp.droppedNegCount > 0) findings.push(`Hinweis: negative Drift verworfen: ${comp.droppedNegCount}× (Sensor/Stop&Go möglich).`);
  } else {
    findings.push("GA comparable: keine/zu wenig saubere Läufe → EF/Drift-Belege schwach (Trend/Signal fragil).");
    actions.push("Für Diagnose: 1×/Woche steady GA 45–60min (oder bench:GA45) auf möglichst ähnlicher Strecke.");
  }

  // Key type distribution (if tagged)
  const keyTypeCounts = countBy(keyRuns.map((x) => x.keyType).filter(Boolean));
  const keyTypeLine = Object.keys(keyTypeCounts).length
    ? `Key-Typen: ${Object.entries(keyTypeCounts)
        .map(([k, v]) => `${k}=${v}`)
        .join(", ")}`
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
  lines.push(`- Basis: tägliche Run-Loads inkl. 0-Tage (Fenster: ${windowDays} Tage, nur Run).`);
  lines.push("");

  lines.push("Fundstücke:");
  if (!findings.length) lines.push("- Keine klaren strukturellen Probleme gefunden.");
  else for (const f of findings.slice(0, 8)) lines.push(`- ${f}`);

  lines.push("");
  lines.push("Nächste Schritte:");
  if (!actions.length) lines.push("- Struktur beibehalten, Bench/GA comparable weiter sammeln.");
  else for (const a of uniq(actions).slice(0, 8)) lines.push(`- ${a}`);

  const miniPlan = buildMiniPlanTargets({ runsPerWeek, weeklyLoad });
  lines.push("");
  lines.push("Konkrete nächste Woche (Mini-Plan):");
  lines.push(
    `- Zielwerte: ${miniPlan.runTarget} Läufe/Woche | ${miniPlan.loadTarget} Run-Load/Woche | 1× Longrun 60–75′`
  );
  lines.push(`- Beispielwoche: ${miniPlan.exampleWeek.join(" · ")}`);

  const summary = {
    week: mondayIso,
    windowDays,
    totalRuns,
    totalLoad,
    weeklyLoad,
    runsPerWeek,
    longPerWeek,
    keyPerWeek,
    gaPerWeek: gaRuns.length / weeks,
    monotony,
    strain,
    efMed: comp.efMed ?? null,
    driftMed: comp.driftMed ?? null,
    compN: comp.n ?? 0,
  };

  // ok criteria: enough runs OR strong structural issue
  const ok = totalRuns >= DETECTIVE_MIN_RUNS || longRuns.length === 0 || weeklyLoad < 120;

  return { ok, text: lines.join("\n"), summary };
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
      let drift = Number.isFinite(ds?.pa_hr_decouple_pct) ? ds.pa_hr_decouple_pct : null;

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
  const isKey = hasKeyTag(activity);
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - BENCH_LOOKBACK_DAYS * 86400000);
  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));

  const same = acts
    .filter((a) => isRun(a) && getBenchTag(a) === benchName && a.id !== activity.id)
    .sort((a, b) => new Date(b.start_date_local || b.start_date) - new Date(a.start_date_local || a.start_date));

  const today = await computeBenchMetrics(env, activity, warmupSkipSec, { allowDrift: !isKey });
  if (!today) return `🧪 bench:${benchName}\nHeute: n/a`;

  let intervalMetrics = null;
  if (benchType !== "GA" || isKey) {
    intervalMetrics = await computeIntervalBenchMetrics(env, activity, warmupSkipSec);
  }

  const lines = [];
  lines.push(`🧪 bench:${benchName}`);

  const last = same.length
    ? await computeBenchMetrics(env, same[0], warmupSkipSec, { allowDrift: benchType === "GA" && !isKey })
    : null;

  if (!same.length) {
    lines.push("Erster Benchmark – noch kein Vergleich.");
  }

  if (today.ef != null) {
    if (same.length && last?.ef != null) {
      const efVsLast = pct(today.ef, last.ef);
      lines.push(`EF: ${fmtSigned1(efVsLast)}% vs letzte`);
    } else {
      lines.push(`EF: ${today.ef.toFixed(3)}`);
    }
  } else {
    lines.push("EF: n/a");
  }

  let secondaryLine = null;
  if (benchType === "GA" && !isKey) {
    if (same.length && today.drift != null && last?.drift != null) {
      const dVsLast = today.drift - last.drift;
      secondaryLine = `Drift: ${fmtSigned1(dVsLast)}%-Pkt vs letzte`;
    } else if (today.drift != null) {
      secondaryLine = `Drift: ${fmtSigned1(today.drift)}%-Pkt`;
    }
  } else if (intervalMetrics?.hrr60 != null) {
    secondaryLine = `Erholung: HRR60 ${intervalMetrics.hrr60.toFixed(0)} bpm`;
  } else if (isKey) {
    if (same.length && last?.avgSpeed != null) {
      const speedVsLast = pct(today.avgSpeed, last.avgSpeed);
      secondaryLine = `Tempo: ${fmtSigned1(speedVsLast)}% vs letzte`;
    } else if (today.avgSpeed != null) {
      secondaryLine = `Tempo: ${today.avgSpeed.toFixed(2)} m/s`;
    }
  } else if (intervalMetrics?.vo2min != null) {
    secondaryLine = `VO₂-Zeit ≥90% HFmax: ${intervalMetrics.vo2min.toFixed(1)} min`;
  }

  if (secondaryLine) {
    lines.push(secondaryLine);
  }

  let verdict = "Stabil / innerhalb Normalrauschen.";
  if (intervalMetrics?.hrr60 != null && intervalMetrics.hrr60 < 15) {
    verdict = "Hohe Belastung – Erholung limitiert.";
  } else if (intervalMetrics?.vo2min != null && intervalMetrics.vo2min >= 4) {
    verdict = "Intervall-Reiz ausreichend gesetzt.";
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

async function computeBenchMetrics(env, a, warmupSkipSec, { allowDrift = true } = {}) {
  const ef = extractEF(a);
  if (ef == null) return null;

  const avgSpeed = Number(a?.average_speed);
  const avgHr = Number(a?.average_heartrate);

  let drift = null;
  if (allowDrift) {
    try {
      const streams = await fetchIntervalsStreams(env, a.id, ["time", "velocity_smooth", "heartrate"]);
      const ds = computeDriftAndStabilityFromStreams(streams, warmupSkipSec);
      drift = Number.isFinite(ds?.pa_hr_decouple_pct) ? ds.pa_hr_decouple_pct : null;

      if (drift != null && drift < 0) drift = null;
    } catch {
      drift = null;
    }
  }

  return {
    ef,
    drift,
    avgSpeed: Number.isFinite(avgSpeed) ? avgSpeed : null,
    avgHr: Number.isFinite(avgHr) ? avgHr : null,
  };
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
  if (hr1 == null || hr2 == null || hr1 <= 0 || hr2 <= 0) return null;

  const v1 = mean(idx.slice(0, half).map((i) => Number(speed[i])));
  const v2 = mean(idx.slice(half).map((i) => Number(speed[i])));
  if (v1 == null || v2 == null || v1 <= 0 || v2 <= 0) return null;

  const ef1 = v1 / hr1;
  const ef2 = v2 / hr2;

  // Pa:HR Decoupling (positiv = schlechter, weil EF droppt)
  const pa_hr_decouple_pct = ef1 > 0 ? ((ef1 - ef2) / ef1) * 100 : null;

  // speed stability (CV) wie gehabt
  const vs = idx.map((i) => Number(speed[i]));
  const vMean = mean(vs);

  let speed_cv = null;
  if (vMean != null && vMean > 0) {
    const vVar = mean(vs.map((v) => (v - vMean) * (v - vMean)));
    const vSd = vVar != null ? Math.sqrt(vVar) : null;
    speed_cv = vSd != null ? vSd / vMean : null;
  }

  return {
    hr1,
    hr2,
    v1,
    v2,
    ef1,
    ef2,
    pa_hr_decouple_pct,
    used_points: idx.length,
    warmupSkipSec,
    speed_cv,
  };
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
function isIntensity(a) {
  // MVP: key:* bedeutet intensiv
  return hasKeyTag(a);
}

function isIntensityByHr(a) {
  const hr = Number(a?.average_heartrate);
  if (!Number.isFinite(hr) || hr <= 0) return false;
  return hr >= HFMAX * INTENSITY_HR_PCT;
}

function isAerobic(a) {
  // MVP: nicht key und ausreichend lang
  if (hasKeyTag(a)) return false;
  const dur = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
  return Number.isFinite(dur) && dur >= GA_MIN_SECONDS;
}

function isRun(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t === "run" || t === "running" || t.includes("run") || t.includes("laufen");
}
function isBike(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return (
    t === "ride" ||
    t === "cycling" ||
    t.includes("ride") ||
    t.includes("bike") ||
    t.includes("cycling") ||
    t.includes("rad") ||
    t.includes("velo")
  );
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
  if (String(status).startsWith("skip:unsupported")) {
    const type = String(a?.type ?? "unknown");
    debugOut.__summary ??= {};
    debugOut.__summary.skippedUnsupported ??= {};
    debugOut.__summary.skippedUnsupported[type] = (debugOut.__summary.skippedUnsupported[type] || 0) + 1;
    debugOut.__summary.skippedUnsupportedTotal =
      (debugOut.__summary.skippedUnsupportedTotal || 0) + 1;
    return;
  }
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
// ================= EVENTS -> MODE (NEW) =================

async function determineMode(env, dayIso, debug = false) {
  const auth = authHeader(env);
  const races = await fetchUpcomingRaces(env, auth, debug, 8000, dayIso);

  // sort by start date (local)
  const normDay = (e) => String(e?.start_date_local || e?.start_date || "").slice(0, 10);
  const future = (races || [])
    .map((e) => ({ e, day: normDay(e) }))
    .filter((x) => isIsoDate(x.day))
    .sort((a, b) => a.day.localeCompare(b.day));

  const next = future.find((x) => x.day >= dayIso)?.e || null;

  if (!next) return { mode: "OPEN", primary: "open", nextEvent: null, eventError: null };

  const primary = inferSportFromEvent(next);
  if (primary === "bike") return { mode: "EVENT", primary: "bike", nextEvent: next, eventError: null };
  // Default RACE_A bei dir ist sehr wahrscheinlich Lauf – aber wir bleiben bei heuristics:
  if (primary === "run" || primary === "unknown") return { mode: "EVENT", primary: "run", nextEvent: next, eventError: null };

  return { mode: "OPEN", primary: "open", nextEvent: next, eventError: null };
}


function getModePolicy(modeInfo) {
  if (modeInfo.mode === "EVENT" && modeInfo.primary === "run") {
    return {
      label: "EVENT:RUN",
      specificLabel: "RunFloor (Mindest-Laufreiz)",
      specificKind: "run",
      specificThreshold: MIN_STIMULUS_7D_RUN_EVENT,
      aerobicK: AEROBIC_K_DEFAULT,
      useAerobicFloor: true,
      recovery: false,
    };
  }

  if (modeInfo.mode === "EVENT" && modeInfo.primary === "bike") {
    return {
      label: "EVENT:BIKE",
      specificLabel: "BikeFloor (Mindest-Radreiz)",
      specificKind: "bike",
      specificThreshold: MIN_STIMULUS_7D_BIKE_EVENT,
      aerobicK: AEROBIC_K_DEFAULT,
      useAerobicFloor: true,
      recovery: false,
    };
  }

  return {
    label: "OPEN",
    specificLabel: "SpecificFloor (OPEN)",
    specificKind: "open",
    specificThreshold: 0, // OPEN: kein harter spezifischer Floor
    aerobicK: AEROBIC_K_DEFAULT,
    useAerobicFloor: true,
    recovery: false,
  };
}



// ================= INTERVALS API =================
async function fetchIntervalsActivities(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: authHeader(env) } });
  if (!r.ok) throw new Error(`activities ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchIntervalsStreams(env, activityId, types) {
  const url = `https://intervals.icu/api/v1/activity/${activityId}/streams?types=${encodeURIComponent(types.join(","))}`;
  const r = await fetch(url, { headers: { Authorization: authHeader(env) } });
  if (!r.ok) {
  const txt = await r.text().catch(() => "");
  throw new Error(`streams ${r.status}: ${txt.slice(0, 400)}`);
}

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
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/wellness/${day}`;
  const r = await fetch(url, {
    method: "PUT",
    headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  });
  if (!r.ok) throw new Error(`wellness PUT ${day} ${r.status}: ${await r.text()}`);
}

// Events (for NOTE)
async function fetchIntervalsEvents(env, oldest, newest) {
  // local dates (yyyy-MM-dd)
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events?oldest=${oldest}&newest=${newest}`;
  const r = await fetch(url, { headers: { Authorization: authHeader(env) } });
  if (!r.ok) throw new Error(`events ${r.status}: ${await r.text()}`);
  return r.json();
}

async function createIntervalsEvent(env, eventObj) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events`;
  const r = await fetch(url, {
    method: "POST",
    headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
    body: JSON.stringify(eventObj),
  });
  if (!r.ok) throw new Error(`events POST ${r.status}: ${await r.text()}`);
  return r.json();
}

async function updateIntervalsEvent(env, eventId, eventObj) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events/${encodeURIComponent(String(eventId))}`;
  const r = await fetch(url, {
    method: "PUT",
    headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
    body: JSON.stringify(eventObj),
  });
  if (!r.ok) throw new Error(`events PUT ${r.status}: ${await r.text()}`);
  return r.json();
}


async function fetchUpcomingRaces(env, auth, debug, timeoutMs, dayIso) {

  const athleteId = mustEnv(env, "ATHLETE_ID");

  // window relative to the day we are computing
  const start = new Date(dayIso + "T00:00:00Z");
  start.setDate(start.getDate() - 21);

  const end = new Date(dayIso + "T00:00:00Z");
  end.setDate(end.getDate() + EVENT_LOOKAHEAD_DAYS); // use your config (365)

  const oldest = toLocalYMD(start);
  const newest = toLocalYMD(end);

  const url = `${BASE_URL}/athlete/${athleteId}/events?oldest=${oldest}&newest=${newest}`;
  const res = await fetch(url, { headers: { Authorization: auth } });

  if (!res.ok) {
    if (debug) console.log("⚠️ Event-API fehlgeschlagen:", res.status, "url:", url);
    return [];
  }

  const payload = await res.json();
  const events = Array.isArray(payload) ? payload : Array.isArray(payload?.events) ? payload.events : [];

  // IMPORTANT: sort + deterministic pick later
  const races = events.filter((e) => String(e.category ?? "").toUpperCase() === "RACE_A");

  if (debug) {
    console.log(
      "🏁 races preview:",
      races.slice(0, 5).map((e) => ({
        day: String(e.start_date_local || e.start_date || "").slice(0, 10),
        cat: e.category,
        type: e.type,
        name: e.name,
      }))
    );
  }

  return races;
}
