// ====== src/index.js (PART 1/4) ======
// Cloudflare Worker – Run only
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom fields (create these in Intervals):
// VDOT, Drift, Motor, EF, Block
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
  if (url.pathname === "/watchface" || url.pathname === "/watchface/") {
  // CORS preflight (sicher ist sicher)
  if (req.method === "OPTIONS") {
    return new Response("", {
      status: 204,
      headers: {
        "access-control-allow-origin": "*",
        "access-control-allow-methods": "GET, OPTIONS",
        "access-control-allow-headers": "content-type",
        "access-control-max-age": "86400",
      },
    });
  }

  // optional: ?date=YYYY-MM-DD zum Testen, sonst "heute"
  const date = url.searchParams.get("date");
  const endIso = (date && isIsoDate(date)) ? date : isoDate(new Date());

  // Watchface should always reflect the latest data, so do not cache.

  try {
    const payload = await buildWatchfacePayload(env, endIso);

    const cacheControl = "no-store";
    const res = new Response(JSON.stringify(payload), {
      headers: {
        "content-type": "application/json; charset=utf-8",
        "cache-control": cacheControl,
        "access-control-allow-origin": "*",
      },
    });
    return res;
  } catch (e) {
    return new Response(
      JSON.stringify({
        ok: false,
        error: "watchface_failed",
        message: String(e?.message ?? e),
        endIso,
      }),
      {
        status: 500,
        headers: {
          "content-type": "application/json; charset=utf-8",
          "access-control-allow-origin": "*",
        },
      }
    );
  }
}


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
const STRENGTH_MIN_7D = 60;
const BASE_URL = "https://intervals.icu/api/v1";
const DETECTIVE_KV_PREFIX = "detective:week:";
const DETECTIVE_KV_HISTORY_KEY = "detective:history";
const DETECTIVE_HISTORY_LIMIT = 12;
// REMOVE or stop using this for Aerobic:
// const BIKE_EQ_FACTOR = 0.65;

// ================= BLOCK CONFIG (NEW) =================
const BLOCK_CONFIG = {
  durations: {
    BASE: { minDays: 28, maxDays: 84 },
    BUILD: { minDays: 21, maxDays: 56 },
    RACE: { minDays: 14, maxDays: 28 },
    RESET: { minDays: 7, maxDays: 14 },
  },
  cutoffs: {
    wave1Weeks: 20,
    wave2StartWeeks: 12,
    forceRaceWeeks: 2,
    raceStartWeeks: 6,
    postEventResetWeeks: 2,
  },
  thresholds: {
    runFloorPct: 0.9,
    hrDriftMax: 1.0,
    plateauEfDeltaPct: 1.0,
    plateauMotorDelta: 3,
    keyGrace: 0.25,
  },
};






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
const ACWR_HIGH_LIMIT = 1.5;       // acute:chronic workload ratio
const ACWR_LOW_LIMIT = 0.8;        // underload threshold


const GA_MIN_SECONDS = 30 * 60;
const GA_COMPARABLE_MIN_SECONDS = 35 * 60;
const MOTOR_STALE_DAYS = 5;
const MIN_STIMULUS_7D_RUN_LOAD = 150;
const HRV_NEGATIVE_THRESHOLD_PCT = -5;
const PERSONAL_OVERLOAD_PATTERNS = [
  {
    id: "PAT_001",
    signals: ["drift_high", "hrv_down", "key_felt_hard"],
    window_days: 7,
    match_rule: { required: 2, out_of: 3 },
    severity: "high",
    action: "keine Intensität für 5-7 Tage",
  },
  {
    id: "PAT_002",
    signals: ["hrv_2d_negative", "sleep_low", "fatigue_override"],
    window_days: 3,
    match_rule: { required: 2, out_of: 3 },
    severity: "high",
    action: "nur easy, Volumen reduzieren (24-48h)",
  },
  {
    id: "PAT_003",
    signals: ["frequency_high", "runfloor_gap", "drift_high"],
    window_days: 14,
    match_rule: { required: 2, out_of: 3 },
    severity: "medium",
    action: "Dichte runter, keine Zusatzreize",
  },
];

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
const TRANSITION_BIKE_EQ = {
  startWeeks: 24,
  endWeeks: 12,
  startFactor: 1.0,
  endFactor: 0.0,
};


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
const FIELD_EF = "EF";
const FIELD_BLOCK = "Block";

// Streams/types we need often
const STREAM_TYPES_GA = ["time", "velocity_smooth", "heartrate"];
const STREAM_TYPES_INTERVAL = ["time", "heartrate", "velocity_smooth", "watts"];

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
    wellnessCache: new Map(), // dayIso -> wellness payload
    blockStateCache: new Map(), // dayIso -> block state
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
  const startIso = isoDate(new Date(end.getTime() - 6 * 86400000));
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

async function computeFatigue7d(ctx, dayIso, options = {}) {
  const end = new Date(dayIso + "T00:00:00Z");

  const start7Iso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const start14Iso = isoDate(new Date(end.getTime() - 13 * 86400000));
  const start28Iso = isoDate(new Date(end.getTime() - 27 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const acts28 = ctx.activitiesAll.filter((a) => {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    return d && d >= start28Iso && d < endIso;
  });

  const dailyLoads = bucketAllLoadsByDay(acts28); // day -> load
  const days = Object.keys(dailyLoads).sort();

  // split prev7 and last7 deterministically
  let prev7 = 0;
  let last7 = 0;

  for (const d of days) {
    const v = Number(dailyLoads[d]) || 0;
    if (d >= start7Iso) last7 += v;
    else if (d >= start14Iso) prev7 += v;
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

  // chronic (28d) load and ACWR
  let last28 = 0;
  for (const d of days) {
    const v = Number(dailyLoads[d]) || 0;
    if (d >= start28Iso) last28 += v;
  }
  const chronicWeekly = last28 > 0 ? last28 / 4 : 0;
  const acwr = chronicWeekly > 0 ? last7 / chronicWeekly : null;

  const keyCount7 = await computeKeyCount7d(ctx, dayIso);
  const keyCap = Number.isFinite(options.maxKeys7d) ? options.maxKeys7d : MAX_KEYS_7D;

  const reasons = [];
  if (keyCount7 > keyCap) reasons.push(`Key-Cap: ${keyCount7}/${keyCap} Key in 7 Tagen`);
  if (rampPct > RAMP_PCT_7D_LIMIT) reasons.push(`Ramp: ${(rampPct * 100).toFixed(0)}% vs vorherige 7 Tage`);
  if (acwr != null && acwr > ACWR_HIGH_LIMIT) reasons.push(`ACWR: ${acwr.toFixed(2)} (> ${ACWR_HIGH_LIMIT})`);
  if (acwr != null && acwr < ACWR_LOW_LIMIT && last7 > 0)
    reasons.push(`ACWR: ${acwr.toFixed(2)} (< ${ACWR_LOW_LIMIT})`);
  if (monotony > MONOTONY_7D_LIMIT) reasons.push(`Monotony: ${monotony.toFixed(2)} (> ${MONOTONY_7D_LIMIT})`);
  if (strain > STRAIN_7D_LIMIT) reasons.push(`Strain: ${strain.toFixed(0)} (> ${STRAIN_7D_LIMIT})`);

  const override = reasons.length > 0;

  return {
    override,
    reasons,
    keyCount7,
    keyCap,
    keyCapExceeded: keyCount7 > keyCap,
    rampPct,
    monotony,
    strain,
    acwr,
    chronicWeekly,
    last7Load: last7,
    prev7Load: prev7,
  };
}

function computeRobustness(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start7Iso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const start14Iso = isoDate(new Date(end.getTime() - 13 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let strength7 = 0;
  let strength14 = 0;

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d >= endIso) continue;
    if (isStrength(a)) {
      const sec = Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
      if (d >= start14Iso) strength14 += sec / 60;
      if (d >= start7Iso) strength7 += sec / 60;
    }
  }

  const strengthOk = strength7 >= STRENGTH_MIN_7D;
  const reasons = [];
  if (!strengthOk) reasons.push("Kraft/Stabi fehlt");

  return {
    strengthMinutes7d: Math.round(strength7),
    strengthMinutes14d: Math.round(strength14),
    strengthOk,
    reasons,
  };
}

function computeKeySpacing(ctx, dayIso, windowDays = 14) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  const keyDates = [];

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (hasKeyTag(a)) keyDates.push(d);
  }
  keyDates.sort();

  let ok = true;
  let violation = null;
  for (let i = 1; i < keyDates.length; i++) {
    const gap = diffDays(keyDates[i - 1], keyDates[i]);
    if (gap < 2) {
      ok = false;
      violation = { prev: keyDates[i - 1], next: keyDates[i], gapDays: gap };
      break;
    }
  }

  const lastKeyIso = keyDates.length ? keyDates[keyDates.length - 1] : null;
  const nextAllowedIso = lastKeyIso ? isoDate(new Date(new Date(lastKeyIso + "T00:00:00Z").getTime() + 2 * 86400000)) : null;

  return {
    ok,
    violation,
    lastKeyIso,
    nextAllowedIso,
  };
}

const RUN_FLOOR_DELOAD_SUM21_MIN = 450;
const RUN_FLOOR_DELOAD_ACTIVE_DAYS_MIN = 14;
const RUN_FLOOR_DELOAD_STABILITY_WINDOW_DAYS = 14;
const RUN_FLOOR_DELOAD_LOAD_GAP_PCT = 0.25;
const RUN_FLOOR_DELOAD_LOAD_GAP_MAX = 3;
const RUN_FLOOR_DELOAD_WINDOW_DAYS = 21;
const RUN_FLOOR_DELOAD_DAYS = 7;
const RUN_FLOOR_TAPER_START_DAYS = 14;
const RUN_FLOOR_TAPER_END_DAYS = 2;
const RUN_FLOOR_RECOVER_DAYS = 9;
const RUN_FLOOR_DELOAD_RANGE = { min: 0.6, max: 0.7 };
const RUN_FLOOR_DELOAD_FACTOR = {
  BASE: 0.7,
  BUILD: 0.65,
  DEFAULT: 0.65,
};
const RUN_FLOOR_RECOVER_FACTOR = 0.65;
const RUN_FLOOR_FLOOR_STEP = {
  BASE: 6,
  BUILD: 10,
};
const RUN_FLOOR_MAX_INCREASE_PCT = 0.1;

function mapBlockToPhase(block) {
  if (block === "BASE") return "BASE";
  if (block === "BUILD") return "BUILD";
  if (block === "RACE") return "PEAK";
  if (block === "RESET") return "RECOVER";
  return "BASE";
}

function computeTaperFactor(eventInDays) {
  if (!Number.isFinite(eventInDays)) return 1;
  if (eventInDays <= RUN_FLOOR_TAPER_END_DAYS) return 0.6;
  if (eventInDays >= RUN_FLOOR_TAPER_START_DAYS) return 0.9;
  const span = RUN_FLOOR_TAPER_START_DAYS - RUN_FLOOR_TAPER_END_DAYS;
  const ratio = (eventInDays - RUN_FLOOR_TAPER_END_DAYS) / span;
  return 0.6 + ratio * (0.9 - 0.6);
}

function computeBikeSubstitutionFactor(weeksToEvent) {
  if (!Number.isFinite(weeksToEvent)) return 0;
  if (weeksToEvent >= TRANSITION_BIKE_EQ.startWeeks) return TRANSITION_BIKE_EQ.startFactor;
  if (weeksToEvent <= TRANSITION_BIKE_EQ.endWeeks) return TRANSITION_BIKE_EQ.endFactor;
  const span = TRANSITION_BIKE_EQ.startWeeks - TRANSITION_BIKE_EQ.endWeeks;
  if (span <= 0) return TRANSITION_BIKE_EQ.endFactor;
  const ratio = (weeksToEvent - TRANSITION_BIKE_EQ.endWeeks) / span;
  const raw = TRANSITION_BIKE_EQ.endFactor + ratio * (TRANSITION_BIKE_EQ.startFactor - TRANSITION_BIKE_EQ.endFactor);
  return clamp(raw, 0, 1);
}

function computeAvg(windowDays, dailyLoads) {
  if (!Array.isArray(dailyLoads) || windowDays <= 0) return 0;
  const slice = dailyLoads.slice(-windowDays);
  const total = slice.reduce((acc, v) => acc + (Number(v) || 0), 0);
  return total / windowDays;
}

function computeSum(windowDays, dailyLoads) {
  if (!Array.isArray(dailyLoads) || windowDays <= 0) return 0;
  const slice = dailyLoads.slice(-windowDays);
  return slice.reduce((acc, v) => acc + (Number(v) || 0), 0);
}

function countActiveDays(windowDays, dailyLoads) {
  if (!Array.isArray(dailyLoads) || windowDays <= 0) return 0;
  const slice = dailyLoads.slice(-windowDays);
  return slice.reduce((acc, v) => acc + ((Number(v) || 0) > 0 ? 1 : 0), 0);
}

function computeStability(last14Days, floorDaily) {
  if (!Array.isArray(last14Days) || last14Days.length === 0 || !(floorDaily > 0)) {
    return { loadGap: 0, stabilityOK: true };
  }
  const gapThreshold = floorDaily * RUN_FLOOR_DELOAD_LOAD_GAP_PCT;
  const loadGap = last14Days.reduce((acc, v) => acc + ((Number(v) || 0) < gapThreshold ? 1 : 0), 0);
  return { loadGap, stabilityOK: loadGap <= RUN_FLOOR_DELOAD_LOAD_GAP_MAX };
}

function shouldTriggerDeload(sum21, activeDays21, deloadActive) {
  if (deloadActive) return false;
  return (
    sum21 >= RUN_FLOOR_DELOAD_SUM21_MIN && activeDays21 >= RUN_FLOOR_DELOAD_ACTIVE_DAYS_MIN
  );
}

function applyDeloadRules(currentTargets) {
  const floorTarget = Number(currentTargets?.floorTarget) || 0;
  const phase = currentTargets?.phase ?? "BASE";
  const factor = RUN_FLOOR_DELOAD_FACTOR[phase] ?? RUN_FLOOR_DELOAD_FACTOR.DEFAULT;
  return {
    effectiveFloorTarget: floorTarget * factor,
    deloadTargetLow: floorTarget * RUN_FLOOR_DELOAD_RANGE.min,
    deloadTargetHigh: floorTarget * RUN_FLOOR_DELOAD_RANGE.max,
  };
}

function buildRunDailyLoads(ctx, todayISO, windowDays) {
  const end = new Date(todayISO + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - (windowDays - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const dailyLoads = {};
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;
    dailyLoads[d] = (dailyLoads[d] || 0) + extractLoad(a);
  }

  const days = listIsoDaysInclusive(startIso, todayISO);
  return days.map((d) => Number(dailyLoads[d]) || 0);
}

function evaluateRunFloorState({
  todayISO,
  floorTarget,
  phase,
  eventInDays,
  eventDateISO,
  previousState,
  dailyRunLoads,
}) {
  const reasons = [];
  const safeEventInDays = Number.isFinite(eventInDays) ? Math.round(eventInDays) : 9999;
  const prevFloorTarget = Number.isFinite(previousState?.floorTarget) ? previousState.floorTarget : null;
  const baseFloorTarget = Number.isFinite(floorTarget) ? floorTarget : prevFloorTarget ?? 0;

  let updatedFloorTarget = Number.isFinite(prevFloorTarget) ? prevFloorTarget : baseFloorTarget;
  let deloadStartDate = isIsoDate(previousState?.deloadStartDate) ? previousState.deloadStartDate : null;
  let lastDeloadCompletedISO = isIsoDate(previousState?.lastDeloadCompletedISO)
    ? previousState.lastDeloadCompletedISO
    : null;
  let lastFloorIncreaseDate = isIsoDate(previousState?.lastFloorIncreaseDate)
    ? previousState.lastFloorIncreaseDate
    : null;
  let lastEventDate = isIsoDate(previousState?.lastEventDate) ? previousState.lastEventDate : null;

  if (eventDateISO && safeEventInDays <= 0) {
    lastEventDate = eventDateISO;
  }

  let daysSinceEvent = null;
  if (lastEventDate) {
    const delta = daysBetween(lastEventDate, todayISO);
    if (Number.isFinite(delta) && delta >= 0) daysSinceEvent = Math.round(delta);
  }

  if (deloadStartDate && diffDays(deloadStartDate, todayISO) >= RUN_FLOOR_DELOAD_DAYS) {
    deloadStartDate = null;
    lastDeloadCompletedISO = todayISO;
    reasons.push("Deload beendet → neue Aufbauphase");
  }

  let deloadEndDate = null;
  let deloadActive = false;
  if (deloadStartDate) {
    deloadEndDate = isoDate(new Date(new Date(deloadStartDate + "T00:00:00Z").getTime() + 6 * 86400000));
    deloadActive = diffDays(deloadStartDate, todayISO) < RUN_FLOOR_DELOAD_DAYS;
  }

  const safeDailyLoads = Array.isArray(dailyRunLoads)
    ? dailyRunLoads.slice(-RUN_FLOOR_DELOAD_WINDOW_DAYS)
    : [];
  const floorDaily = baseFloorTarget > 0 ? baseFloorTarget / 7 : 0;
  const avg21 = computeAvg(RUN_FLOOR_DELOAD_WINDOW_DAYS, safeDailyLoads);
  const avg7 = computeAvg(7, safeDailyLoads);
  const sum21 = computeSum(RUN_FLOOR_DELOAD_WINDOW_DAYS, safeDailyLoads);
  const activeDays21 = countActiveDays(RUN_FLOOR_DELOAD_WINDOW_DAYS, safeDailyLoads);
  const last14Loads = safeDailyLoads.slice(-RUN_FLOOR_DELOAD_STABILITY_WINDOW_DAYS);
  const { loadGap, stabilityOK } = computeStability(last14Loads, floorDaily);
  const deloadReady = shouldTriggerDeload(sum21, activeDays21, deloadActive);
  const stabilityWarn = !stabilityOK && avg21 >= floorDaily * 1.0 && floorDaily > 0;

  let overlayMode = "NORMAL";
  if (safeEventInDays >= 0 && safeEventInDays <= RUN_FLOOR_TAPER_START_DAYS) {
    overlayMode = "TAPER";
    reasons.push("Taper aktiv (Event in ≤14 Tagen)");
  } else if (daysSinceEvent != null && daysSinceEvent <= RUN_FLOOR_RECOVER_DAYS) {
    overlayMode = "RECOVER_OVERLAY";
    reasons.push("Recover-Overlay aktiv (Event gerade passiert)");
  } else if (deloadActive) {
    overlayMode = "DELOAD";
    reasons.push("Deload läuft");
  } else if (deloadReady && (phase === "BASE" || phase === "BUILD") && safeEventInDays > RUN_FLOOR_DELOAD_DAYS) {
    overlayMode = "DELOAD";
    deloadStartDate = todayISO;
    deloadEndDate = isoDate(new Date(new Date(todayISO + "T00:00:00Z").getTime() + 6 * 86400000));
    deloadActive = true;
    reasons.push("Deload ausgelöst (21T Summe + 14 aktive Tage)");
  } else if (stabilityWarn) {
    reasons.push("Aufgebaut aber instabil → erst stabilisieren");
  }

  let effectiveFloorTarget = updatedFloorTarget;
  if (overlayMode === "DELOAD") {
    effectiveFloorTarget = applyDeloadRules({ floorTarget: updatedFloorTarget, phase }).effectiveFloorTarget;
  } else if (overlayMode === "TAPER") {
    effectiveFloorTarget = updatedFloorTarget * computeTaperFactor(safeEventInDays);
  } else if (overlayMode === "RECOVER_OVERLAY") {
    effectiveFloorTarget = updatedFloorTarget * RUN_FLOOR_RECOVER_FACTOR;
  }

  const deloadCompletedSinceIncrease =
    lastDeloadCompletedISO && (!lastFloorIncreaseDate || lastDeloadCompletedISO > lastFloorIncreaseDate);

  if (
    (phase === "BASE" || phase === "BUILD") &&
    overlayMode === "NORMAL" &&
    safeEventInDays > 28 &&
    deloadCompletedSinceIncrease
  ) {
    const step = RUN_FLOOR_FLOOR_STEP[phase] ?? 6;
    const maxIncrease = Math.max(1, Math.round(updatedFloorTarget * RUN_FLOOR_MAX_INCREASE_PCT));
    const increase = Math.min(step, maxIncrease);
    if (increase > 0) {
      updatedFloorTarget += increase;
      lastFloorIncreaseDate = todayISO;
      reasons.push(`RunFloor erhöht (+${increase}) nach Deload`);
    }
  }

  return {
    overlayMode,
    effectiveFloorTarget,
    floorTarget: updatedFloorTarget,
    useAerobicFloor: true,
    deloadStartDate,
    deloadEndDate,
    deloadActive,
    avg21,
    avg7,
    sum21,
    activeDays21,
    floorDaily,
    loadGap,
    stabilityOK,
    decisionText:
      overlayMode === "RECOVER_OVERLAY"
        ? "Recover"
        : overlayMode === "DELOAD"
          ? "Deload"
          : stabilityWarn
            ? "Warn: Instabil"
            : "Build",
    lastDeloadCompletedISO,
    lastFloorIncreaseDate,
    lastEventDate,
    daysSinceEvent,
    reasons,
  };
}

// ================= LOAD SUPPORT =================
async function computeLoads7d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let runTotal7 = 0;
  let bikeTotal7 = 0;
  let runMinutes7 = 0;
  let bikeMinutes7 = 0;

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
    const seconds = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
    const minutes = Number.isFinite(seconds) ? seconds / 60 : 0;

    const run = isRun(a);
    const bike = isBike(a);

    if (run) {
      runMinutes7 += minutes;
      runTotal7 += totalLoad;
    }
    if (bike) {
      bikeMinutes7 += minutes;
      bikeTotal7 += totalLoad;
    }

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
    runMinutes7,
    bikeMinutes7,
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

function computeLongRunSummary7d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let longest = null;
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;
    const seconds = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
    if (!longest || seconds > longest.seconds) {
      longest = {
        seconds,
        date: d,
        isKey: hasKeyTag(a),
        ga: isGA(a),
        intensity: isIntensity(a) || isIntensityByHr(a) || !isAerobic(a),
      };
    }
  }

  if (!longest) return { minutes: 0, date: null, quality: "n/a", isKey: false, intensity: false };
  const minutes = Math.round(longest.seconds / 60);
  let quality = "locker/steady";
  if (longest.isKey) quality = "Key/Intensität";
  else if (longest.intensity) quality = "mit Intensität";
  else if (!longest.ga) quality = "gemischt";
  return {
    minutes,
    date: longest.date,
    quality,
    isKey: longest.isKey,
    intensity: longest.intensity,
  };
}

// ================= BLOCK / KEY LOGIC (NEW) =================
function normalizeEventDistance(value) {
  const s = String(value || "").toLowerCase().trim();
  if (!s) return null;
  if (s.includes("5k") || s.includes("5 km") || s.includes("5km")) return "5k";
  if (s.includes("10k") || s.includes("10 km") || s.includes("10km")) return "10k";
  if (s.includes("half") || s.includes("hm") || s.includes("halb")) return "hm";
  if (s.includes("marathon") || s === "m" || s.includes("42")) return "m";
  const numeric = Number(s.replace(/[^0-9.]/g, ""));
  if (Number.isFinite(numeric) && numeric > 0) {
    const meters = numeric < 1000 ? numeric * 1000 : numeric;
    if (meters >= 4900 && meters <= 5100) return "5k";
    if (meters >= 9500 && meters <= 10500) return "10k";
    if (meters >= 20500 && meters <= 21500) return "hm";
    if (meters >= 41000 && meters <= 43000) return "m";
  }
  return null;
}

function getEventDistanceFromEvent(event) {
  if (!event) return null;

  // ✅ Primär: echtes Feld aus Intervals
  const raw = event?.distance ?? event?.distance_target ?? null;

  const fromField = normalizeEventDistance(raw);
  if (fromField) return fromField;

  // Fallback: Name/Typ (nur wenn distance fehlt/unbrauchbar)
  const name = String(event?.name ?? "");
  const type = String(event?.type ?? "");
  return normalizeEventDistance(`${name} ${type}`);
}




function normalizeKeyType(rawType, workoutMeta = {}) {
  const s = String(rawType || "").toLowerCase().replace(/[_-]+/g, " ").trim();
  if (!s) return "steady";

  const racepaceRegex = /\b(race|rp|5k pace|10k pace|hm pace|mp)\b/;
  if (racepaceRegex.test(s) || s.includes("race pace") || s.includes("wettkampf")) return "racepace";
  if (s.includes("threshold") || s.includes("schwelle") || s.includes("tempo")) return "schwelle";
  if (s.includes("vo2") || s.includes("v02")) return "vo2_touch";
  if (s.includes("strides") || s.includes("hill sprint")) return "strides";
  return "steady";
}

function getKeyRules(block, eventDistance, weeksToEvent) {
  const dist = eventDistance || "10k";
  if (block === "RESET") {
    return {
      expectedKeysPerWeek: 0,
      maxKeysPerWeek: 0,
      allowedKeyTypes: ["steady", "strides"],
      preferredKeyTypes: ["steady"],
      bannedKeyTypes: ["schwelle", "racepace", "vo2_touch"],
    };
  }

  if (block === "BASE") {
    if (dist === "5k" || dist === "10k") {
      return {
        expectedKeysPerWeek: 0.5,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["steady", "strides", "vo2_touch"],
        preferredKeyTypes: ["vo2_touch"],
        bannedKeyTypes: ["schwelle", "racepace"],
      };
    }
    if (dist === "m" || dist === "hm") {
      return {
        expectedKeysPerWeek: 0.5,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["steady", "strides"],
        preferredKeyTypes: ["steady"],
        bannedKeyTypes: ["schwelle", "racepace", "vo2_touch"],
      };
    }
    return {
      expectedKeysPerWeek: 0.5,
      maxKeysPerWeek: 1,
      allowedKeyTypes: ["steady", "strides"],
      preferredKeyTypes: ["steady"],
      bannedKeyTypes: ["schwelle", "racepace", "vo2_touch"],
    };
  }

  if (block === "BUILD") {
    if (dist === "5k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["schwelle", "vo2_touch", "strides", "steady"],
        preferredKeyTypes: ["vo2_touch", "schwelle"],
        bannedKeyTypes: ["racepace"],
      };
    }
    if (dist === "10k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["schwelle", "vo2_touch", "strides", "steady"],
        preferredKeyTypes: ["schwelle", "vo2_touch"],
        bannedKeyTypes: ["racepace"],
      };
    }
    if (dist === "hm") {
      const allowRacePace = weeksToEvent != null && weeksToEvent <= 8;
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: allowRacePace ? ["schwelle", "racepace", "steady"] : ["schwelle", "steady"],
        preferredKeyTypes: allowRacePace ? ["racepace", "schwelle"] : ["schwelle"],
        bannedKeyTypes: allowRacePace ? ["vo2_touch", "strides"] : ["racepace", "vo2_touch", "strides"],
      };
    }
    if (dist === "m") {
      const allowRacePace = weeksToEvent != null && weeksToEvent <= 10;
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: allowRacePace ? ["schwelle", "racepace", "steady"] : ["schwelle", "steady"],
        preferredKeyTypes: allowRacePace ? ["racepace", "schwelle"] : ["schwelle"],
        bannedKeyTypes: allowRacePace ? ["vo2_touch", "strides"] : ["racepace", "vo2_touch", "strides"],
      };
    }
  }

  if (block === "RACE") {
    if (dist === "5k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "vo2_touch", "strides", "steady"],
        preferredKeyTypes: ["racepace", "vo2_touch"],
        bannedKeyTypes: ["schwelle"],
      };
    }
    if (dist === "10k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "schwelle", "strides", "steady"],
        preferredKeyTypes: ["racepace"],
        bannedKeyTypes: ["vo2_touch"],
      };
    }
    if (dist === "hm") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "schwelle", "steady"],
        preferredKeyTypes: ["racepace", "schwelle"],
        bannedKeyTypes: ["vo2_touch", "strides"],
      };
    }
    if (dist === "m") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "schwelle", "steady"],
        preferredKeyTypes: ["racepace"],
        bannedKeyTypes: ["vo2_touch", "strides"],
      };
    }
  }

  return {
    expectedKeysPerWeek: 0.5,
    maxKeysPerWeek: 1,
    allowedKeyTypes: ["steady", "strides"],
    preferredKeyTypes: ["steady"],
    bannedKeyTypes: ["schwelle", "racepace", "vo2_touch"],
  };
}

function collectKeyStats(ctx, dayIso, windowDays) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let count = 0;
  const types = {};
  const list = [];

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!hasKeyTag(a)) continue;
    count++;
    const rawType = getKeyType(a);
    const type = normalizeKeyType(rawType, {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    types[type] = (types[type] || 0) + 1;
    list.push(type);
  }
  return { count, types, list };
}

function evaluateKeyCompliance(keyRules, keyStats7, keyStats14, context = {}) {
  const expected = keyRules.expectedKeysPerWeek;
  const maxKeys = keyRules.maxKeysPerWeek;
  const actual7 = keyStats7.count;
  const actual14 = keyStats14.count;
  const perWeek14 = actual14 / 2;
  const maxKeysCap = Number.isFinite(context.maxKeys7d) ? context.maxKeys7d : maxKeys;
  const capExceeded = actual7 > maxKeysCap;

  const actualTypes7 = keyStats7.list || [];
  const actualTypes14 = keyStats14.list || [];
  const typesForOutput = actualTypes7.length ? actualTypes7 : actualTypes14;
  const uniqueTypes = [...new Set(typesForOutput)];
  const uniqueTypes7 = [...new Set(actualTypes7)];
  const bannedHits = uniqueTypes7.filter((t) => keyRules.bannedKeyTypes.includes(t));
  const allowedHits = uniqueTypes7.filter((t) => keyRules.allowedKeyTypes.includes(t));
  const preferredHits = uniqueTypes7.filter((t) => keyRules.preferredKeyTypes.includes(t));
  const disallowedHits = uniqueTypes7.filter((t) => !keyRules.allowedKeyTypes.includes(t));

  const freqOk = actual7 >= expected;
  const typeOk = bannedHits.length === 0 && disallowedHits.length === 0;
  const preferredMissing = keyRules.preferredKeyTypes.length > 0 && preferredHits.length === 0;

  let suggestion = "";
  const preferred = keyRules.preferredKeyTypes[0] || keyRules.allowedKeyTypes[0] || "steady";
  const blockLabel = context.block ? `Block=${context.block}` : "Block=n/a";
  const distLabel = context.eventDistance ? `Distanz=${context.eventDistance}` : "Distanz=n/a";

  if (capExceeded) {
    suggestion = "Kein weiterer Key diese Woche.";
  } else if (bannedHits.length) {
    suggestion = `Verbotener Key-Typ (${bannedHits[0]}) – Alternative: ${preferred}`;
  } else if (!freqOk) {
    suggestion = `Nächster Key: ${preferred} (${blockLabel}, ${distLabel})`;
  } else if (actual7 >= 1 && typeOk) {
    suggestion = "Key diese Woche erledigt ✅ – restliche Einheiten locker/steady.";
  } else if (preferredMissing) {
    suggestion = `Nächster Key: ${preferred} (${blockLabel}, ${distLabel})`;
  } else {
    suggestion = "Kein Key geplant – locker/steady.";
  }

  const keySpacingOk = context.keySpacing?.ok ?? true;
  const nextKeyEarliest = context.keySpacing?.nextAllowedIso ?? null;
  if (!capExceeded && !keySpacingOk && nextKeyEarliest) {
    suggestion = `Nächster Key frühestens ${nextKeyEarliest} (≥48h Abstand).`;
  }

  const status = capExceeded ? "red" : freqOk && typeOk ? "ok" : "warn";

  return {
    expected,
    maxKeys,
    maxKeysCap,
    actual7,
    actual14,
    perWeek14,
    freqOk,
    typeOk,
    preferredMissing,
    bannedHits,
    allowedHits,
    preferredHits,
    actualTypes: uniqueTypes,
    disallowedHits,
    status,
    suggestion,
    basedOn: "7T",
    capExceeded,
    keySpacingOk,
    nextKeyEarliest,
  };
}

function getNextBlock(block, wave, weeksToEvent) {
  if (block === "BASE") return "BUILD";
  if (block === "BUILD") {
    if (wave === 1 && weeksToEvent > BLOCK_CONFIG.cutoffs.wave2StartWeeks) return "RESET";
    return "RACE";
  }
  if (block === "RESET") {
    if (weeksToEvent > BLOCK_CONFIG.cutoffs.wave2StartWeeks) return "BASE";
    return "BUILD";
  }
  return weeksToEvent < 0 ? "RESET" : "RACE";
}

function computeWeeksToEvent(todayISO, eventDateISO, reasons) {
  const weeksToEventRaw = weeksBetween(todayISO, eventDateISO);
  let weeksToEvent = weeksToEventRaw;
  const needsGuard =
    !Number.isFinite(weeksToEvent) || weeksToEvent < -2 || weeksToEvent > 104;
  if (needsGuard) {
    if (Array.isArray(reasons)) {
      const rawText = Number.isFinite(weeksToEventRaw) ? weeksToEventRaw.toFixed(2) : "n/a";
      reasons.push(`weeksToEvent unplausibel (${rawText}) → neu berechnet`);
    }
    weeksToEvent = weeksBetween(todayISO, eventDateISO);
  }
  if (!Number.isFinite(weeksToEvent)) {
    if (Array.isArray(reasons)) reasons.push("weeksToEvent konnte nicht berechnet werden");
    return { weeksToEventRaw, weeksToEvent: null };
  }
  return { weeksToEventRaw, weeksToEvent };
}

function determineBlockState({
  today,
  eventDate,
  eventDistance,
  historyMetrics,
  previousState,
}) {
  const reasons = [];
  const eventDistanceNorm = eventDistance || "10k";
  

  const todayISO = today;
  const eventDateISO = eventDate || null;

  const persistedStart = previousState?.startDate || null;
  const clampedStart = clampStartDate(persistedStart, todayISO);
  const startWasReset = clampedStart == null;
  let startDate = clampedStart || todayISO;
  if (startWasReset && persistedStart) {
    reasons.push("Block-Startdatum unplausibel → Start neu gesetzt");
  }

  if (!eventDateISO || !parseISODateSafe(eventDateISO)) {
    const timeInBlockDays = daysBetween(startDate, todayISO);
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent: null,
      weeksToEventRaw: null,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: startDate,
      startWasReset,
      reasons: ["Kein Event-Datum gefunden → BASE"],
      readinessScore: 50,
      forcedSwitch: false,
      nextSuggestedBlock: "BUILD",
      timeInBlockDays: Number.isFinite(timeInBlockDays) ? timeInBlockDays : null,
      startDate,
      eventDistance: eventDistanceNorm,
    };
  }

  const { weeksToEventRaw, weeksToEvent } = computeWeeksToEvent(todayISO, eventDateISO, reasons);
  if (weeksToEvent == null) {
    const timeInBlockDays = daysBetween(startDate, todayISO);
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent: null,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: startDate,
      startWasReset,
      reasons,
      readinessScore: 50,
      forcedSwitch: false,
      nextSuggestedBlock: "BUILD",
      timeInBlockDays: Number.isFinite(timeInBlockDays) ? timeInBlockDays : null,
      startDate,
      eventDistance: eventDistanceNorm,
    };
  }

  if (weeksToEvent <= 4 && weeksToEvent >= 0) {
    return {
      block: "RACE",
      wave: 0,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: todayISO,
      startWasReset,
      reasons: [...reasons, "Event sehr nah (≤4 Wochen) → RACE"],
      readinessScore: 90,
      forcedSwitch: false,
      nextSuggestedBlock: "RESET",
      timeInBlockDays: 0,
      startDate: todayISO,
      eventDistance: eventDistanceNorm,
    };
  }

  if (weeksToEvent <= BLOCK_CONFIG.cutoffs.forceRaceWeeks && weeksToEvent >= 0) {
    return {
      block: "RACE",
      wave: weeksToEvent > BLOCK_CONFIG.cutoffs.wave1Weeks ? 1 : 0,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: todayISO,
      startWasReset,
      reasons: ["Event sehr nah → sofort RACE"],
      readinessScore: 90,
      forcedSwitch: false,
      nextSuggestedBlock: "RESET",
      timeInBlockDays: 0,
      startDate: todayISO,
      eventDistance: eventDistanceNorm,
    };
  }

  if (weeksToEvent < 0) {
    if (Math.abs(weeksToEvent) <= BLOCK_CONFIG.cutoffs.postEventResetWeeks) {
      return {
        block: "RESET",
        wave: 0,
        weeksToEvent,
        weeksToEventRaw,
        todayISO,
        eventDateISO,
        blockStartPersisted: persistedStart,
        blockStartEffective: todayISO,
        startWasReset,
        reasons: ["Event vorbei → RESET"],
        readinessScore: 60,
        forcedSwitch: false,
        nextSuggestedBlock: "BASE",
        timeInBlockDays: 0,
        startDate: todayISO,
        eventDistance: eventDistanceNorm,
      };
    }
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: todayISO,
      startWasReset,
      reasons: ["Event vorbei → Re-Entry BASE"],
      readinessScore: 50,
      forcedSwitch: false,
      nextSuggestedBlock: "BUILD",
      timeInBlockDays: 0,
      startDate: todayISO,
      eventDistance: eventDistanceNorm,
    };
  }

  let wave = weeksToEvent > BLOCK_CONFIG.cutoffs.wave1Weeks ? 1 : 0;
  if (previousState?.wave === 2) wave = 2;
  if (weeksToEvent <= 8 && wave === 1) {
    wave = 0;
    reasons.push("Event ≤8 Wochen → Wave 1 deaktiviert");
  }

  let block = previousState?.block || (weeksToEvent <= BLOCK_CONFIG.cutoffs.raceStartWeeks ? "BUILD" : "BASE");

  const runFloorTarget = historyMetrics?.runFloorTarget ?? 0;
  const runFloorIsLow =
    runFloorTarget > 0 && (historyMetrics?.runFloor7 ?? 0) < runFloorTarget * 0.5;
  if (weeksToEvent <= 8 && block === "BASE") {
    if (runFloorIsLow) {
      reasons.push("BASE bleibt trotz Event-Nähe: RunFloor extrem niedrig");
    } else {
      block = "BUILD";
      startDate = todayISO;
      reasons.push("Event ≤8 Wochen → BASE zu spät, Wechsel zu BUILD");
    }
  }

  let timeInBlockDays = daysBetween(startDate, todayISO);
  if (!Number.isFinite(timeInBlockDays) || timeInBlockDays < 0) {
    timeInBlockDays = 0;
  }
  const blockLimits = BLOCK_CONFIG.durations[block] || { minDays: 7, maxDays: 56 };
  

  const runFloorReady =
    runFloorTarget > 0
      ? historyMetrics.runFloor7 >= runFloorTarget * BLOCK_CONFIG.thresholds.runFloorPct &&
        historyMetrics.runFloorPrev7 >= runFloorTarget * BLOCK_CONFIG.thresholds.runFloorPct
      : true;

  const aerobicReady = historyMetrics?.aerobicOk && historyMetrics?.aerobicOkPrev;
  const driftReady =
    historyMetrics?.hrDriftDelta == null || historyMetrics.hrDriftDelta <= BLOCK_CONFIG.thresholds.hrDriftMax;
  const fatigueOk = !historyMetrics?.fatigue?.override;

  let readinessScore = 40;
  if (runFloorReady) readinessScore += 20;
  if (aerobicReady) readinessScore += 15;
  if (driftReady) readinessScore += 10;
  if (fatigueOk) readinessScore += 10;
  readinessScore = clamp(readinessScore, 0, 100);

  let forcedSwitch = false;
  let nextSuggestedBlock = getNextBlock(block, wave, weeksToEvent);

  if (weeksToEvent <= BLOCK_CONFIG.cutoffs.raceStartWeeks && weeksToEvent >= 0 && block !== "RACE") {
    forcedSwitch = true;
    reasons.push("Event ≤6 Wochen → sofort RACE (Taper-Puffer)");
    block = "RACE";
    startDate = todayISO;
    timeInBlockDays = 0;
    return {
      block,
      wave,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: startDate,
      startWasReset,
      reasons,
      readinessScore,
      forcedSwitch,
      nextSuggestedBlock: getNextBlock(block, wave, weeksToEvent),
      timeInBlockDays,
      startDate,
      eventDistance: eventDistanceNorm,
    };
  }

  if (timeInBlockDays < blockLimits.minDays) {
    reasons.push(`Mindestdauer ${blockLimits.minDays} Tage noch nicht erreicht`);
    return {
      block,
      wave,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: startDate,
      startWasReset,
      reasons,
      readinessScore,
      forcedSwitch,
      nextSuggestedBlock,
      timeInBlockDays,
      startDate,
      eventDistance: eventDistanceNorm,
    };
  }

  if (timeInBlockDays >= blockLimits.maxDays) {
    forcedSwitch = true;
    reasons.push(`Maxdauer ${blockLimits.maxDays} Tage überschritten → Wechsel erzwungen`);
    block = nextSuggestedBlock;
    startDate = todayISO;
    timeInBlockDays = 0;
    return {
      block,
      wave: block === "BASE" && wave === 1 ? 2 : wave,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: startDate,
      startWasReset,
      reasons,
      readinessScore,
      forcedSwitch,
      nextSuggestedBlock: getNextBlock(block, wave, weeksToEvent),
      timeInBlockDays,
      startDate,
      eventDistance: eventDistanceNorm,
    };
  }

  if (block === "BASE") {
    if (runFloorReady && aerobicReady && driftReady && fatigueOk) {
      reasons.push("BASE Exit: Floors stabil + Drift ok + keine Overload-Signale");
      block = "BUILD";
      startDate = todayISO;
      timeInBlockDays = 0;
    } else {
      if (!runFloorReady) reasons.push("BASE bleibt: RunFloor noch instabil");
      if (!aerobicReady) reasons.push("BASE bleibt: AerobicEq/Floor noch instabil");
      if (!driftReady) reasons.push("BASE bleibt: HR-Drift steigt");
      if (!fatigueOk) reasons.push("BASE bleibt: Overload/Monotony");
    }
  } else if (block === "BUILD") {
    const keyCompliance = historyMetrics?.keyCompliance;
    const plateauEf = Math.abs(historyMetrics?.efDeltaPct ?? 0) <= BLOCK_CONFIG.thresholds.plateauEfDeltaPct;
    const plateauMotor =
      historyMetrics?.motorDelta == null || Math.abs(historyMetrics.motorDelta) <= BLOCK_CONFIG.thresholds.plateauMotorDelta;

    const buildReady = keyCompliance?.freqOk && keyCompliance?.typeOk && (plateauEf || plateauMotor);
    const eventForcesRace = weeksToEvent <= BLOCK_CONFIG.cutoffs.raceStartWeeks;

    if (wave === 1 && weeksToEvent > BLOCK_CONFIG.cutoffs.wave2StartWeeks) {
      const keysOk = (historyMetrics?.keyStats14?.count ?? 0) >= 3;
      if (keysOk) {
        reasons.push("BUILD I abgeschlossen → RESET (Wave 1)");
        block = "RESET";
        startDate = todayISO;
        timeInBlockDays = 0;
      } else {
        reasons.push("BUILD bleibt: zu wenige Keys für Wave-Reset");
      }
    } else if (buildReady || eventForcesRace) {
      reasons.push(eventForcesRace ? "Event rückt näher → RACE" : "BUILD Exit: Keys ok + Plateau erreicht");
      block = "RACE";
      startDate = todayISO;
      timeInBlockDays = 0;
    } else {
      if (!keyCompliance?.freqOk) reasons.push("BUILD bleibt: Key-Frequenz zu niedrig/hoch");
      if (!keyCompliance?.typeOk) reasons.push("BUILD bleibt: Key-Typen passen nicht");
      if (!(plateauEf || plateauMotor)) reasons.push("BUILD bleibt: Leistungsmarker steigen noch");
    }
  } else if (block === "RESET") {
   if (fatigueOk || timeInBlockDays >= BLOCK_CONFIG.durations.RESET.maxDays) {
      reasons.push("RESET erfüllt → BASE II");
      block = "BASE";
      wave = 2;
      startDate = todayISO;
      timeInBlockDays = 0;
    } else {
      reasons.push("RESET bleibt: Ermüdungssignale noch aktiv");
    }
  } else if (block === "RACE") {
    if (weeksToEvent <= 0) {
      reasons.push("Event erreicht → RESET");
      block = "RESET";
      startDate = todayISO;
      timeInBlockDays = 0;
    } else {
      reasons.push("RACE bleibt: Taper/Peak läuft");
    }
  }

  return {
    block,
    wave,
    weeksToEvent,
    weeksToEventRaw,
    todayISO,
    eventDateISO,
    blockStartPersisted: persistedStart,
    blockStartEffective: startDate,
    startWasReset,
    reasons,
    readinessScore,
    forcedSwitch,
    nextSuggestedBlock: getNextBlock(block, wave, weeksToEvent),
    timeInBlockDays,
    startDate,
    eventDistance: eventDistanceNorm,
  };
}

function buildBlockStateLine(state) {
  if (!state) return "";
  const payload = {
    block: state.block,
    wave: state.wave,
    start: state.startDate,
    eventDate: state.eventDate,
    eventDistance: state.eventDistance,
    floorTarget: Number.isFinite(state.floorTarget) ? state.floorTarget : null,
    deloadStartDate: isIsoDate(state.deloadStartDate) ? state.deloadStartDate : null,
    lastDeloadCompletedISO: isIsoDate(state.lastDeloadCompletedISO) ? state.lastDeloadCompletedISO : null,
    lastFloorIncreaseDate: isIsoDate(state.lastFloorIncreaseDate) ? state.lastFloorIncreaseDate : null,
    lastEventDate: isIsoDate(state.lastEventDate) ? state.lastEventDate : null,
  };
  return `BlockState: ${JSON.stringify(payload)}`;
}

function parseBlockStateFromComment(comment) {
  if (!comment) return null;
  const line = String(comment)
    .split("\n")
    .find((l) => l.trim().startsWith("BlockState:"));
  if (!line) return null;
  const raw = line.replace("BlockState:", "").trim();
  try {
    const parsed = JSON.parse(raw);
    if (!parsed?.block || !parsed?.start) return null;
    return {
      block: parsed.block,
      wave: parsed.wave ?? 0,
      startDate: parsed.start,
      eventDate: parsed.eventDate ?? null,
      eventDistance: parsed.eventDistance ?? null,
      floorTarget: Number.isFinite(parsed.floorTarget) ? parsed.floorTarget : null,
      loadDays: Number.isFinite(parsed.loadDays) ? parsed.loadDays : 0,
      deloadStartDate: isIsoDate(parsed.deloadStartDate) ? parsed.deloadStartDate : null,
      lastDeloadCompletedISO: isIsoDate(parsed.lastDeloadCompletedISO) ? parsed.lastDeloadCompletedISO : null,
      lastFloorIncreaseDate: isIsoDate(parsed.lastFloorIncreaseDate) ? parsed.lastFloorIncreaseDate : null,
      lastEventDate: isIsoDate(parsed.lastEventDate) ? parsed.lastEventDate : null,
    };
  } catch {
    return null;
  }
}

async function fetchWellnessDay(ctx, env, dayIso) {
  if (ctx.wellnessCache.has(dayIso)) return ctx.wellnessCache.get(dayIso);
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/wellness/${dayIso}`;
  const p = fetch(url, { headers: { Authorization: authHeader(env) } })
    .then(async (r) => {
      if (!r.ok) return null;
      return r.json();
    })
    .catch(() => null);
  ctx.wellnessCache.set(dayIso, p);
  return p;
}

async function getPersistedBlockState(ctx, env, dayIso) {
  if (ctx.blockStateCache.has(dayIso)) return ctx.blockStateCache.get(dayIso);
  const wellness = await fetchWellnessDay(ctx, env, dayIso);
  const comment = wellness?.comments || wellness?.comment || null;
  const parsed = parseBlockStateFromComment(comment);
  ctx.blockStateCache.set(dayIso, parsed);
  return parsed;
}

function addBlockDebug(debugOut, day, blockState, keyRules, keyCompliance, historyMetrics) {
  if (!debugOut) return;
  debugOut.__blocks ??= {};
  debugOut.__blocks[day] = {
    blockState,
    keyRules,
    keyCompliance,
    historyMetrics,
  };
}

function addRunFloorDebug(debugOut, day, payload) {
  if (!debugOut) return;
  debugOut.__runFloor ??= {};
  debugOut.__runFloor[day] = payload;
}


function isoDate(d) {
  return d.toISOString().slice(0, 10);
}
function parseISODateSafe(iso) {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(String(iso))) return null;
  const [y, m, d] = String(iso).split("-").map((v) => Number(v));
  if (!Number.isFinite(y) || !Number.isFinite(m) || !Number.isFinite(d)) return null;
  const date = new Date(Date.UTC(y, m - 1, d));
  if (Number.isNaN(date.getTime())) return null;
  if (date.getUTCFullYear() !== y || date.getUTCMonth() + 1 !== m || date.getUTCDate() !== d) return null;
  return date;
}
function isIsoDate(s) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(s));
}
function weeksBetween(dateAISO, dateBISO) {
  const a = parseISODateSafe(dateAISO);
  const b = parseISODateSafe(dateBISO);
  if (!a || !b) return NaN;
  return (b.getTime() - a.getTime()) / (7 * 86400000);
}
function daysBetween(dateAISO, dateBISO) {
  const a = parseISODateSafe(dateAISO);
  const b = parseISODateSafe(dateBISO);
  if (!a || !b) return NaN;
  return (b.getTime() - a.getTime()) / 86400000;
}
function clampStartDate(startISO, todayISO, maxAgeDays = 180) {
  const start = parseISODateSafe(startISO);
  const today = parseISODateSafe(todayISO);
  if (!start || !today) return null;
  if (start.getTime() > today.getTime()) return null;
  const ageDays = (today.getTime() - start.getTime()) / 86400000;
  if (ageDays > maxAgeDays) return null;
  return isoDate(start);
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
  let previousBlockState = null;

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
    // NEW: fatigue / key-cap metrics (keine RECOVERY-Logik mehr)
    let fatigueBase = null;
    try {
      fatigueBase = await computeFatigue7d(ctx, day);
    } catch {
      fatigueBase = null;
    }

    let robustness = null;
    try {
      robustness = computeRobustness(ctx, day);
    } catch {
      robustness = null;
    }

    const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);
    const eventDistance = getEventDistanceFromEvent(modeInfo?.nextEvent);
    if (ctx.debug) {
  console.log("[debug:eventDistance]", {
    day,
    eventName: modeInfo?.nextEvent?.name,
    eventType: modeInfo?.nextEvent?.type,
    DistanceRaw: modeInfo?.nextEvent?.Distance,
    DistanceType: typeof modeInfo?.nextEvent?.Distance,
    computedEventDistance: eventDistance,
  });
}

    if (!previousBlockState) {
      const prevDay = isoDate(new Date(new Date(day + "T00:00:00Z").getTime() - 86400000));
      previousBlockState = await getPersistedBlockState(ctx, env, prevDay);
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
      const keyType = isKey ? getKeyType(a) : null;

      let drift = null;
      let drift_raw = null;
      let drift_source = "none";
      let speed_cv = null;
      let intervalMetrics = null;

      if (ga && !isKey) {
        drift_source = "streams";
        try {
          const streams = await getStreams(ctx, a.id, STREAM_TYPES_GA);
          const ds = computeDriftAndStabilityFromStreams(streams, ctx.warmupSkipSec);
          drift_raw = Number.isFinite(ds?.pa_hr_decouple_pct) ? ds.pa_hr_decouple_pct : null;
          speed_cv = Number.isFinite(ds?.speed_cv) ? ds.speed_cv : null;

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
      if (isKey) {
        try {
          const streams = await getStreams(ctx, a.id, STREAM_TYPES_INTERVAL);
          intervalMetrics = computeIntervalMetricsFromStreams(streams, {
            intervalType: getIntervalTypeFromActivity(a),
          });
        } catch {
          intervalMetrics = null;
        }
      }

      perRunInfo.push({
        activityId: a.id,
        type: a.type,
        tags: a.tags ?? [],
        ga,
        isKey,
        keyType,
        ef,
        drift,
        drift_raw,
        drift_source,
        speed_cv,
        load,
        intervalMetrics,
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
      if (rep.ef != null) patch[FIELD_EF] = round(rep.ef, 3);
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
    let longRunSummary = { minutes: 0, date: null, quality: "n/a", isKey: false, intensity: false };
    try {
      longRunSummary = computeLongRunSummary7d(ctx, day);
    } catch {}
    let recoverySignals = null;
    try {
      recoverySignals = await computeRecoverySignals(ctx, env, day);
    } catch {
      recoverySignals = null;
    }

    const weeksInfo = eventDate ? computeWeeksToEvent(day, eventDate, null) : { weeksToEvent: null };
    const weeksToEvent = weeksInfo.weeksToEvent ?? null;
    const bikeSubFactor = computeBikeSubstitutionFactor(weeksToEvent);
    const runEquivalent7 = (loads7.runTotal7 ?? 0) + (loads7.bikeTotal7 ?? 0) * bikeSubFactor;

    let specificValue = 0;
    if (policy.specificKind === "run") specificValue = runEquivalent7;
    else if (policy.specificKind === "bike") specificValue = loads7.bikeTotal7;
    else specificValue = 0;

    let specificOk = policy.specificThreshold > 0 ? specificValue >= policy.specificThreshold : true;
    const aerobicEq = loads7.aerobicEq7 ?? 0;
    const intensity = loads7.intensity7 ?? 0;
    const intensitySignal = loads7.intensitySignal ?? "none";
    const aerobicFloorActive = policy.useAerobicFloor && intensitySignal === "ok";

    const aerobicFloor = aerobicFloorActive ? policy.aerobicK * intensity : 0;
    const aerobicOk = aerobicFloorActive ? aerobicEq >= aerobicFloor : true;

    const prevWindowDay = isoDate(new Date(new Date(day + "T00:00:00Z").getTime() - 7 * 86400000));
    let loads7Prev = { runTotal7: 0, bikeTotal7: 0, aerobicEq7: 0, intensity7: 0, intensitySignal: "none" };
    try {
      loads7Prev = await computeLoads7d(ctx, prevWindowDay);
    } catch {}
    const weeksPrev = eventDate ? computeWeeksToEvent(prevWindowDay, eventDate, null) : { weeksToEvent: null };
    const bikeSubFactorPrev = computeBikeSubstitutionFactor(weeksPrev.weeksToEvent ?? null);
    const runEquivalentPrev7 = (loads7Prev.runTotal7 ?? 0) + (loads7Prev.bikeTotal7 ?? 0) * bikeSubFactorPrev;

    const prevIntensitySignal = loads7Prev.intensitySignal ?? "none";
    const prevAerobicFloorActive = policy.useAerobicFloor && prevIntensitySignal === "ok";
    const prevAerobicFloor = prevAerobicFloorActive ? policy.aerobicK * (loads7Prev.intensity7 ?? 0) : 0;
    const aerobicOkPrev = prevAerobicFloorActive ? (loads7Prev.aerobicEq7 ?? 0) >= prevAerobicFloor : true;

    const keyStats7 = collectKeyStats(ctx, day, 7);
    const keyStats14 = collectKeyStats(ctx, day, 14);
    const keySpacing = computeKeySpacing(ctx, day);
    const baseBlock =
      previousBlockState?.block ||
      (weeksToEvent != null && weeksToEvent <= BLOCK_CONFIG.cutoffs.raceStartWeeks ? "BUILD" : "BASE");
    const keyRulesPre = getKeyRules(baseBlock, eventDistance, weeksToEvent);
    const keyCompliancePre = evaluateKeyCompliance(keyRulesPre, keyStats7, keyStats14, {
      block: baseBlock,
      eventDistance,
    });

    const baseRunFloorTarget =
      Number.isFinite(previousBlockState?.floorTarget) && previousBlockState.floorTarget > 0
        ? previousBlockState.floorTarget
        : MIN_STIMULUS_7D_RUN_EVENT;

    const historyMetrics = {
      runFloor7: runEquivalent7 ?? 0,
      runFloorPrev7: runEquivalentPrev7 ?? 0,
      runFloorTarget: baseRunFloorTarget,
      aerobicOk,
      aerobicOkPrev,
      aerobicEq7: loads7.aerobicEq7 ?? 0,
      intensity7: loads7.intensity7 ?? 0,
      hrDriftDelta: trend?.dd ?? null,
      efDeltaPct: trend?.dv ?? null,
      motorValue: motor?.value ?? null,
      motorDelta: null,
      fatigue: fatigueBase,
      keyStats14,
      keyCompliance: keyCompliancePre,
    };

    const blockState = determineBlockState({
      today: day,
      eventDate: eventDate || null,
      eventDistance,
      historyMetrics,
      previousState: previousBlockState,
    });
    blockState.eventDate = eventDate || null;
    blockState.eventDistance = eventDistance || blockState.eventDistance;

    const phase = mapBlockToPhase(blockState.block);
    const eventInDays = eventDate ? daysBetween(day, eventDate) : null;
    const dailyRunLoads = buildRunDailyLoads(ctx, day, RUN_FLOOR_DELOAD_WINDOW_DAYS);
    const runFloorState = evaluateRunFloorState({
      todayISO: day,
      floorTarget: baseRunFloorTarget,
      phase,
      eventInDays,
      eventDateISO: eventDate || null,
      previousState: previousBlockState,
      dailyRunLoads,
    });

    if (policy.specificKind === "run" || policy.specificKind === "open") {
      policy = {
        ...policy,
        specificThreshold: runFloorState.effectiveFloorTarget,
      };
    }
    specificOk = policy.specificThreshold > 0 ? specificValue >= policy.specificThreshold : true;
    blockState.floorTarget = runFloorState.floorTarget;
    blockState.deloadStartDate = runFloorState.deloadStartDate;
    blockState.lastDeloadCompletedISO = runFloorState.lastDeloadCompletedISO;
    blockState.lastFloorIncreaseDate = runFloorState.lastFloorIncreaseDate;
    blockState.lastEventDate = runFloorState.lastEventDate;

    const dynamicKeyCap = {
      maxKeys7d: MAX_KEYS_7D,
      reasons: [],
    };

    if (runFloorState.overlayMode === "RECOVER_OVERLAY") {
      dynamicKeyCap.maxKeys7d = 0;
      dynamicKeyCap.reasons.push("Recover-Overlay aktiv");
    } else if (runFloorState.overlayMode === "TAPER") {
      dynamicKeyCap.maxKeys7d = 0;
      dynamicKeyCap.reasons.push("Taper aktiv");
    } else if (runFloorState.overlayMode === "DELOAD") {
      dynamicKeyCap.maxKeys7d = 1;
      dynamicKeyCap.reasons.push("Deload aktiv");
    } else if (fatigueBase?.override) {
      dynamicKeyCap.maxKeys7d = 1;
      dynamicKeyCap.reasons.push("Fatigue/Overload");
    } else if (robustness && !robustness.strengthOk) {
      dynamicKeyCap.maxKeys7d = 1;
      dynamicKeyCap.reasons.push("Robustheit fehlt");
    } else if ((motor?.value ?? 0) >= 70) {
      dynamicKeyCap.maxKeys7d = 2;
      dynamicKeyCap.reasons.push("Motor stark");
    } else {
      dynamicKeyCap.maxKeys7d = 1;
      dynamicKeyCap.reasons.push("Motor <70");
    }

    let fatigue = fatigueBase;
    try {
      fatigue = await computeFatigue7d(ctx, day, { maxKeys7d: dynamicKeyCap.maxKeys7d });
    } catch {
      fatigue = fatigueBase;
    }
    historyMetrics.fatigueCap = fatigue;

    const keyRulesBase = getKeyRules(blockState.block, eventDistance, blockState.weeksToEvent);
    const keyRules = {
      ...keyRulesBase,
      maxKeysPerWeek: Math.min(keyRulesBase.maxKeysPerWeek, dynamicKeyCap.maxKeys7d),
    };
    const keyCompliance = evaluateKeyCompliance(keyRules, keyStats7, keyStats14, {
      block: blockState.block,
      eventDistance,
      maxKeys7d: dynamicKeyCap.maxKeys7d,
      keySpacing,
    });

    patch[FIELD_BLOCK] = blockState.block;
    previousBlockState = {
      block: blockState.block,
      wave: blockState.wave,
      startDate: blockState.startDate || day,
      eventDate,
      eventDistance,
      floorTarget: blockState.floorTarget,
      deloadStartDate: blockState.deloadStartDate,
      lastDeloadCompletedISO: blockState.lastDeloadCompletedISO,
      lastFloorIncreaseDate: blockState.lastFloorIncreaseDate,
      lastEventDate: blockState.lastEventDate,
    };

    addBlockDebug(ctx.debugOut, day, blockState, keyRules, keyCompliance, historyMetrics);
    addRunFloorDebug(ctx.debugOut, day, {
      overlayMode: runFloorState.overlayMode,
      effectiveFloorTarget: runFloorState.effectiveFloorTarget,
      floorTarget: runFloorState.floorTarget,
      deloadStartDate: runFloorState.deloadStartDate,
      deloadEndDate: runFloorState.deloadEndDate,
      deloadActive: runFloorState.deloadActive,
      avg21: runFloorState.avg21,
      avg7: runFloorState.avg7,
      floorDaily: runFloorState.floorDaily,
      loadGap: runFloorState.loadGap,
      stabilityOK: runFloorState.stabilityOK,
      decisionText: runFloorState.decisionText,
      lastDeloadCompletedISO: runFloorState.lastDeloadCompletedISO,
      lastFloorIncreaseDate: runFloorState.lastFloorIncreaseDate,
      lastEventDate: runFloorState.lastEventDate,
      daysSinceEvent: runFloorState.daysSinceEvent,
      reasons: runFloorState.reasons,
    });

    const maintenance14d = computeMaintenance14d(ctx, day);

    // Daily report text ALWAYS (includes min stimulus ALWAYS)
    const dailyReportText = buildComments({
      perRunInfo,
      trend,
      motor,
      robustness,
      modeInfo,
      blockState,
      keyRules,
      keyCompliance,
      dynamicKeyCap,
      keySpacing,
      policy,
      loads7,
      runEquivalent7,
      runFloorState,
      specificOk,
      specificValue,
      aerobicOk,
      aerobicFloor,
      aerobicFloorActive,
      fatigue,
      longRunSummary,
      recoverySignals,
      weeksToEvent,
      maintenance14d,
    }, { debug });

    // Do not write into wellness comment field anymore.
    patch.comments = "";





    patches[day] = patch;

    if (write) {
      await upsertDailyReportNote(env, day, dailyReportText);
    }
    if (debug) notesPreview[`${day}:daily`] = dailyReportText;

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
        detectiveNoteText = `🕵️‍♂️ Montags-Report\nFehler: ${String(e?.message ?? e)}`;
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

// Create/update a NOTE event for the daily report
async function upsertDailyReportNote(env, dayIso, noteText) {
  const external_id = `daily-report-${dayIso}`;
  const name = "Daily-Report";
  const description = noteText;

  const events = await fetchIntervalsEvents(env, dayIso, dayIso);
  const existing = (events || []).find((e) => String(e?.external_id || "") === external_id);

  if (existing?.id) {
    await updateIntervalsEvent(env, existing.id, {
      category: "NOTE",
      start_date_local: `${dayIso}T00:00:00`,
      name,
      description,
      color: "blue",
      external_id,
    });
    return;
  }

  await createIntervalsEvent(env, {
    category: "NOTE",
    start_date_local: `${dayIso}T00:00:00`,
    name,
    description,
    color: "blue",
    external_id,
  });
}

// Representative GA run: prefer non-key GA; fallback to key GA for EF/VDOT.
// Longest GA wins, tie-breaker: has drift, then higher moving_time
function pickRepresentativeGARun(perRunInfo) {
  const nonKeyGA = perRunInfo.filter((x) => x.ga && !x.isKey);
  const ga = nonKeyGA.length ? nonKeyGA : perRunInfo.filter((x) => x.ga);
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

function formatEventDistance(dist) {
  if (!dist) return "n/a";
  if (dist === "5k") return "5 km";
  if (dist === "10k") return "10 km";
  if (dist === "hm") return "HM";
  if (dist === "m") return "Marathon";
  return String(dist);
}

function formatKeyType(type) {
  if (type === "schwelle") return "Schwelle";
  if (type === "racepace") return "Racepace";
  if (type === "vo2_touch") return "VO2";
  if (type === "strides") return "Strides";
  if (type === "steady") return "steady";
  return type || "n/a";
}

function formatKeyTypeList(types = []) {
  if (!types.length) return "n/a";
  return types.map(formatKeyType).join("/");
}

function buildKeyRuleLine({ keyRules, block, eventDistance }) {
  if (!keyRules) return null;
  const blockLabel = block || "n/a";
  const distLabel = eventDistance || "n/a";
  const allowed = formatKeyTypeList(keyRules.allowedKeyTypes);
  const preferred = formatKeyTypeList(keyRules.preferredKeyTypes);
  const banned = keyRules.bannedKeyTypes?.length ? formatKeyTypeList(keyRules.bannedKeyTypes) : null;
  return `Key-Regel (${blockLabel}, ${distLabel}): erlaubt ${allowed}, bevorzugt ${preferred}${banned ? `, tabu ${banned}` : ""}.`;
}

function buildAerobicTrendLine(trend) {
  const dv = Number.isFinite(trend?.dv) ? trend.dv : null;
  const dd = Number.isFinite(trend?.dd) ? trend.dd : null;
  if (dv == null || dd == null) return "GA-Form n/a";

  const vdotArrow = dv > 0.5 ? "↑" : dv < -0.5 ? "↓" : "↔";
  const driftArrow = dd > 0.5 ? "↑" : dd < -0.5 ? "↓" : "↔";
  const dvText = `${dv > 0 ? "+" : ""}${dv.toFixed(1)}%`;
  const ddText = `${dd > 0 ? "+" : ""}${dd.toFixed(1)}%`;

  if (dv <= -1.5 && dd >= 1) return `GA-Form rückläufig (VDOT ↓ ${dvText}, HR-Drift ↑ ${ddText})`;
  if (dv >= 1.5 && dd <= 0) return `GA-Form verbessert (VDOT ↑ ${dvText}, HR-Drift ↓ ${ddText})`;
  return `GA-Form stabil/gemischt (VDOT ${vdotArrow} ${dvText}, HR-Drift ${driftArrow} ${ddText})`;
}

function extractSleepHoursFromWellness(wellness) {
  if (!wellness) return null;
  const candidates = [
    wellness.sleep,
    wellness.sleep_hours,
    wellness.sleep_duration,
    wellness.sleep_time,
    wellness.sleep_hr,
  ];
  for (const value of candidates) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) continue;
    if (num > 24) return round(num / 60, 2);
    return num;
  }
  return null;
}

function extractHrvFromWellness(wellness) {
  if (!wellness) return null;
  const candidates = [
    wellness.hrv,
    wellness.hrv_rmssd,
    wellness.hrv_sdnn,
  ];
  for (const value of candidates) {
    const num = Number(value);
    if (!Number.isFinite(num) || num <= 0) continue;
    return num;
  }
  return null;
}

function extractSubjectiveTag(wellness, keys) {
  if (!wellness || !Array.isArray(keys)) return null;
  for (const key of keys) {
    const raw = wellness?.[key];
    if (raw == null) continue;
    const value = String(raw).trim().toLowerCase();
    if (!value) continue;
    return value;
  }
  return null;
}

function buildRecoverySignalLines(recoverySignals) {
  if (!recoverySignals) return [];
  const lines = [];
  const { sleepHours, sleepBaseline, sleepDeltaPct, hrv, hrvBaseline, hrvDeltaPct, sleepLow, hrvLow } = recoverySignals;
  const hasSleep = sleepHours != null;
  const hasHrv = hrv != null;
  if (!hasSleep && !hasHrv) return [];
  const parts = [];
  if (hasSleep) {
    const sleepDeltaText = sleepBaseline != null ? ` (${sleepDeltaPct > 0 ? "+" : ""}${sleepDeltaPct.toFixed(0)}% vs 7T)` : "";
    parts.push(`Schlaf ${sleepHours.toFixed(1)}h${sleepDeltaText}`);
  }
  if (hasHrv) {
    const hrvDeltaText = hrvBaseline != null ? ` (${hrvDeltaPct > 0 ? "+" : ""}${hrvDeltaPct.toFixed(0)}% vs 7T)` : "";
    parts.push(`HRV ${Math.round(hrv)}${hrvDeltaText}`);
  }
  lines.push(`Recovery-Check: ${parts.join(" | ")}.`);
  if (sleepLow || hrvLow) {
    const issues = [];
    if (sleepLow) issues.push("Schlaf");
    if (hrvLow) issues.push("HRV");
    lines.push(`Hinweis: ${issues.join(" & ")} niedriger als üblich → Ermüdung wahrscheinlicher.`);
  }
  return lines;
}

async function computeRecoverySignals(ctx, env, dayIso) {
  const today = await fetchWellnessDay(ctx, env, dayIso);
  const sleepToday = extractSleepHoursFromWellness(today);
  const hrvToday = extractHrvFromWellness(today);
  if (sleepToday == null && hrvToday == null) {
    return null;
  }
  const priorDays = [];
  for (let i = 1; i <= 7; i += 1) {
    priorDays.push(isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - i * 86400000)));
  }
  const sleepVals = [];
  const hrvVals = [];
  for (const iso of priorDays) {
    const wellness = await fetchWellnessDay(ctx, env, iso);
    const sleep = extractSleepHoursFromWellness(wellness);
    const hrv = extractHrvFromWellness(wellness);
    if (sleep != null) sleepVals.push(sleep);
    if (hrv != null) hrvVals.push(hrv);
  }
  const sleepBaseline = sleepVals.length ? avg(sleepVals) : null;
  const hrvBaseline = hrvVals.length ? avg(hrvVals) : null;
  const sleepDeltaPct = sleepBaseline ? ((sleepToday - sleepBaseline) / sleepBaseline) * 100 : 0;
  const hrvDeltaPct = hrvBaseline ? ((hrvToday - hrvBaseline) / hrvBaseline) * 100 : 0;
  const ydayIso = isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - 86400000));
  const yday = await fetchWellnessDay(ctx, env, ydayIso);
  const ydayHrv = extractHrvFromWellness(yday);
  const ydayHrvDeltaPct = hrvBaseline && ydayHrv ? ((ydayHrv - hrvBaseline) / hrvBaseline) * 100 : null;
  const sleepLow = sleepBaseline != null && sleepToday < sleepBaseline * 0.9;
  const hrvLow = hrvBaseline != null && hrvToday < hrvBaseline * 0.9;
  const legsTag = extractSubjectiveTag(today, ["legs", "legs_feel", "leg_feel", "muscle_feel", "fatigue_feel"]);
  const moodTag = extractSubjectiveTag(today, ["mood", "mood_state", "readiness_mood"]);
  const painTag = extractSubjectiveTag(today, ["pain", "injury", "injury_flag", "pain_flag"]);
  const legsNegative = !!legsTag && /heavy|schwer|dead|tired|müde|low/.test(legsTag);
  const moodNegative = !!moodTag && /low|down|bad|schlecht|negativ/.test(moodTag);
  const painInjury = !!painTag && !/none|no|0|false|ok/.test(painTag);
  return {
    sleepHours: sleepToday,
    sleepBaseline,
    sleepDeltaPct,
    hrv: hrvToday,
    hrvBaseline,
    hrvDeltaPct,
    ydayHrvDeltaPct,
    sleepLow,
    hrvLow,
    legsNegative,
    moodNegative,
    painInjury,
  };
}

function computeMaintenance14d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 14 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

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

function buildNextRunRecommendation({
  runFloorState,
  policy,
  specificOk,
  hasSpecific,
  aerobicOk,
  intensitySignal,
  keyCapExceeded,
  keySpacingOk,
}) {
  let next = "45–60 min locker/steady";
  const overlay = runFloorState?.overlayMode ?? "NORMAL";
  if (overlay === "RECOVER_OVERLAY") {
    next = "25–40 min locker / Technik / frei";
  } else if (overlay === "TAPER") {
    next = "20–35 min locker (Taper)";
  } else if (overlay === "DELOAD") {
    next = "30–45 min locker / Technik (Deload)";
  } else if (hasSpecific && !specificOk) {
    next = "35–50 min locker/steady (Volumenaufbau)";
  } else if (policy?.useAerobicFloor && intensitySignal === "ok" && !aerobicOk) {
    next = "30–45 min locker (kein Key) – Intensität deckeln";
  }
  if (keyCapExceeded) {
    next = "Kein weiterer Key diese Woche – locker/steady.";
  } else if (!keySpacingOk) {
    next = "Nächster Key frühestens in 48h – bis dahin locker/steady.";
  }

  return next;
}

function buildBottomLineCoachMessage({
  hadAnyRun,
  hadGA,
  runFloorState,
  hasSpecific,
  specificOk,
  policy,
  intensitySignal,
  aerobicOk,
  keyCapExceeded,
  keySpacingOk,
  todayText,
  nextText,
}) {
  const overlay = runFloorState?.overlayMode ?? "NORMAL";
  if (overlay === "RECOVER_OVERLAY") {
    return `Heute ist Recovery angesagt. ${todayText}. Wenn du läufst: ${nextText}.`;
  }
  if (overlay === "TAPER") {
    return `Taper-Phase: Frische schützen. ${todayText}. ${nextText}.`;
  }
  if (overlay === "DELOAD") {
    return `Deload aktiv: locker & Technik. ${todayText}. ${nextText}.`;
  }
  if (keyCapExceeded) {
    return `Key ist für diese Woche abgehakt. Halte den Rest locker/steady. ${nextText}.`;
  }
  if (!keySpacingOk) {
    return `Gib dem Körper 48h zwischen Keys. Heute ruhig bleiben. ${nextText}.`;
  }
  if (hasSpecific && !specificOk) {
    return `Volumen fehlt noch ein Stück. Fülle locker/steady auf. ${nextText}.`;
  }
  if (policy?.useAerobicFloor && intensitySignal === "ok" && !aerobicOk) {
    if (hadAnyRun && hadGA) {
      return `GA heute erledigt (${todayText}). ${nextText}.`;
    }
    if (hadAnyRun) {
      return `Heute schon gelaufen (${todayText}) – Fokus GA & Intensität deckeln. ${nextText}.`;
    }
    return `GA ist heute der Fokus – Intensität deckeln. ${nextText}.`;
  }
  return `Alles im grünen Bereich. ${todayText}. ${nextText}.`;
}

function buildTransitionLine({ bikeSubFactor, weeksToEvent }) {
  if (!(bikeSubFactor > 0)) return null;
  const pct = Math.round(bikeSubFactor * 100);
  const weeksText = Number.isFinite(weeksToEvent) ? `${Math.round(weeksToEvent)} Wochen` : "n/a";
  return `Übergang aktiv: Rad zählt ${pct}% zum RunFloor (aktuell ${weeksText} bis Event, 0% ab ≤${TRANSITION_BIKE_EQ.endWeeks} Wochen).`;
}

function formatWeeksOut(weeksToEvent) {
  if (!Number.isFinite(weeksToEvent)) return "n/a";
  return `${Math.max(0, Math.round(weeksToEvent))}`;
}

function inferWeekIntent({ blockState, runFloorState, eventDistance }) {
  const phase = blockState?.activeBlock || "BASE";
  const stabilityOK = !!runFloorState?.stabilityOK;
  if (phase === "RACE") return `Primäres Ziel dieser Woche: Spezifität schärfen + Frische schützen (${eventDistance || "Event"}-Fokus).`;
  if (phase === "BUILD") return "Primäres Ziel dieser Woche: aerobe Stabilisierung + Frequenzaufbau mit kontrollierter Spezifität.";
  if (phase === "RESET") return "Primäres Ziel dieser Woche: Erholung sichern + technische Sauberkeit wiederherstellen.";
  return stabilityOK
    ? "Primäres Ziel dieser Woche: Belastbarkeit stabil halten und den aeroben Unterbau festigen."
    : "Primäres Ziel dieser Woche: aerobe Stabilisierung + Frequenzaufbau (nicht über Tempo erzwingen).";
}

function inferNotImportantNow({ blockState, weeksToEvent }) {
  const phase = blockState?.activeBlock || "BASE";
  if (phase === "BASE" || phase === "RESET") return "Aktuell NICHT wichtig: harte Pace-Ziele oder zusätzliche VO2-Spitzen.";
  if (phase === "BUILD" && Number.isFinite(weeksToEvent) && weeksToEvent > 4) {
    return "Aktuell NICHT wichtig: Race-Pace ausreizen – zuerst Tragfähigkeit aufbauen.";
  }
  return "Aktuell NICHT wichtig: unnötiger Zusatzreiz außerhalb des Wochenziels.";
}

function classifyGaDriftPct(driftPct) {
  if (!Number.isFinite(driftPct) || driftPct < 0) return null;
  if (driftPct <= 3) {
    return {
      zone: "🟢",
      label: "sehr gut",
      summary: "saubere aerobe Basis",
      action: "Perfekter GA-Lauf.",
    };
  }
  if (driftPct <= 5) {
    return {
      zone: "🟡",
      label: "ok / Grenzbereich",
      summary: "aerob solide, aber nur begrenzt steigerbar",
      action: "Noch akzeptabel – ggf. Dauer oder Tempo leicht anpassen.",
    };
  }
  if (driftPct <= 8) {
    return {
      zone: "🟠",
      label: "Warnsignal",
      summary: "über aktueller aerober Kapazität",
      action: "Kein klassischer GA-Lauf mehr – eher Tempo senken oder kürzen.",
    };
  }
  return {
    zone: "🔴",
    label: "klar anaerob geprägt",
    summary: "deutliche Entkopplung",
    action: "Für GA-Ziel zu hoch, Erholungskosten steigen.",
  };
}

function buildGaDriftInterpretationLines({ perRunInfo, recoverySignals, longRunSummary }) {
  const rep = pickRepresentativeGARun(perRunInfo);
  if (!rep || !Number.isFinite(rep.drift) || rep.drift < 0) return [];
  const drift = rep.drift;
  const bucket = classifyGaDriftPct(drift);
  if (!bucket) return [];
  const lines = [];
  lines.push(`Drift heute: ${bucket.zone} ${drift.toFixed(1)}% (${bucket.label}) – ${bucket.summary}.`);
  const context = [];
  if (recoverySignals?.hrvLow) context.push("HRV niedriger als 7T");
  if (recoverySignals?.sleepLow) context.push("Schlaf niedriger als 7T");
  if ((longRunSummary?.minutes ?? 0) >= 75) context.push("längerer Lauf");
  if (context.length) {
    lines.push(`Kontext: erhöhte Drift kann auch durch ${context.join(" / ")} erklärt sein (nicht automatisch "schlechte Form").`);
  }
  if (drift > 7) {
    lines.push("Coach-Logik: Drift >7% signalisiert limitierte aerobe Effizienz – metabolische Kosten steigen schneller als der Nutzen.");
  }
  lines.push(`Einordnung: ${bucket.action}`);
  return lines;
}

function confidenceBucket(score) {
  if (score >= 70) return "high";
  if (score >= 40) return "medium";
  return "low";
}

function computeSectionConfidence({ hasDrift, hasHrv, hasLoad, consistent, subjectiveAligned, contradictions, hasHistory }) {
  let score = 0;
  if (hasDrift) score += 15;
  if (hasHrv) score += 15;
  if (hasLoad) score += 10;
  if (consistent) score += 20;
  if (subjectiveAligned) score += 10;
  if (!contradictions) score += 10;
  if (hasHistory) score += 10;
  return { score, bucket: confidenceBucket(score) };
}

function matchOverloadPatterns(signalMap) {
  const matches = [];
  for (const pattern of PERSONAL_OVERLOAD_PATTERNS) {
    const outOf = pattern?.match_rule?.out_of || pattern?.signals?.length || 0;
    const required = pattern?.match_rule?.required || outOf;
    const hitSignals = (pattern.signals || []).filter((s) => !!signalMap[s]);
    if (hitSignals.length >= required && outOf > 0) {
      matches.push({
        id: pattern.id,
        severity: pattern.severity,
        action: pattern.action,
        hitSignals,
      });
    }
  }
  return matches;
}

// ================= COMMENT =================
function buildComments(
  {
    perRunInfo,
    trend,
    motor,
    robustness,
    modeInfo,
    blockState,
    keyRules,
    keyCompliance,
    dynamicKeyCap,
    keySpacing,
    policy,
    loads7,
    runEquivalent7,
    runFloorState,
    fatigue,
    longRunSummary,
    recoverySignals,
    weeksToEvent,
    maintenance14d,
  },
  { debug = false } = {}
) {
  const hadKey = perRunInfo.some((x) => x.isKey);
  const hadGA = perRunInfo.some((x) => x.ga && !x.isKey);
  const hadAnyRun = perRunInfo.length > 0;
  const totalMinutesToday = Math.round(sum(perRunInfo.map((x) => x.moving_time || 0)) / 60);
  const repRun = pickRepresentativeGARun(perRunInfo);
  const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);
  const eventDistance = formatEventDistance(blockState?.eventDistance || getEventDistanceFromEvent(modeInfo?.nextEvent));
  const daysToEvent = eventDate ? daysBetween(isoDate(new Date()), eventDate) : null;

  const drift = Number.isFinite(repRun?.drift) ? repRun.drift : null;
  const personalDriftWarn = 6;
  const personalDriftCritical = 8;
  const driftSignal = drift == null ? "unknown" : drift >= personalDriftCritical ? "red" : drift >= personalDriftWarn ? "orange" : "green";

  const hrvDeltaPct = Number.isFinite(recoverySignals?.hrvDeltaPct) ? recoverySignals.hrvDeltaPct : null;
  const ydayHrvDeltaPct = Number.isFinite(recoverySignals?.ydayHrvDeltaPct) ? recoverySignals.ydayHrvDeltaPct : null;
  const hrv1dNegative = hrvDeltaPct != null && hrvDeltaPct <= HRV_NEGATIVE_THRESHOLD_PCT;
  const hrv2dNegative = hrv1dNegative && ydayHrvDeltaPct != null && ydayHrvDeltaPct <= HRV_NEGATIVE_THRESHOLD_PCT;
  const counterIndicator = !hadKey && driftSignal === "green";

  const freqCount14 = maintenance14d?.runCount14 ?? null;
  const sweetspotLow = 7;
  const sweetspotHigh = 11;
  const upperLimit = 12;
  const freqSignal = freqCount14 == null ? "unknown" : freqCount14 > upperLimit ? "red" : (freqCount14 < sweetspotLow || freqCount14 > sweetspotHigh ? "orange" : "green");

  const runLoad7 = Math.round(loads7?.runTotal7 ?? 0);
  const runTarget = Math.round(runFloorState?.effectiveFloorTarget ?? 0);
  const runBaseTarget = Math.round(runFloorState?.floorTarget ?? 0);
  const runFloorGap = runTarget > 0 && runLoad7 < runTarget;
  const overlayMode = runFloorState?.overlayMode ?? "NORMAL";
  const floorModeText =
    overlayMode === "DELOAD"
      ? "Deload aktiv (Soll fällt automatisch)"
      : overlayMode === "TAPER"
        ? "Taper aktiv (Soll reduziert)"
        : overlayMode === "RECOVER_OVERLAY"
          ? "Recovery-Overlay (Soll abgesenkt)"
          : "Build-Modus (Soll kann stufenweise steigen)";

  const signalMap = {
    drift_high: driftSignal !== "green" && driftSignal !== "unknown",
    hrv_down: hrv1dNegative,
    hrv_2d_negative: hrv2dNegative,
    key_felt_hard: hadKey && driftSignal !== "green",
    sleep_low: !!recoverySignals?.sleepLow,
    fatigue_override: !!fatigue?.override,
    frequency_high: freqSignal === "red",
    runfloor_gap: runFloorGap,
  };
  const patternMatches = matchOverloadPatterns(signalMap);
  const highPattern = patternMatches.find((p) => p.severity === "high");

  const warningSignals = [
    driftSignal === "orange" || driftSignal === "red",
    hrv1dNegative,
    freqSignal === "orange" || freqSignal === "red",
    !!recoverySignals?.sleepLow,
    !!fatigue?.override,
  ];
  const warningCount = warningSignals.filter(Boolean).length;
  const subjectiveNegative = !!recoverySignals?.legsNegative || !!recoverySignals?.moodNegative;

  const hardRedFlags = {
    hrv2dNegative: hrv2dNegative && !counterIndicator,
    confirmedOverloadHigh: !!highPattern,
    multiWarningPlusSubjectiveNegative: warningCount >= 2 && subjectiveNegative,
    painInjury: !!recoverySignals?.painInjury,
  };
  const hasHardRedFlag = Object.values(hardRedFlags).some(Boolean);

  const softRedFlags = {
    frequencyBelowSweetspot: freqCount14 != null && freqCount14 < sweetspotLow,
    driftNearWarn: drift != null && drift >= personalDriftWarn - 1 && drift < personalDriftCritical,
    runFloorBelowTarget: runFloorGap,
    sleepStressSuboptimal: !!recoverySignals?.sleepLow || hrv1dNegative,
    isolatedWarningSignal: warningCount === 1,
  };
  const hasSoftRedFlag = Object.values(softRedFlags).some(Boolean);

  let readinessAmpel = "🟢";
  if (hasHardRedFlag) readinessAmpel = "🔴";
  else if (hasSoftRedFlag) readinessAmpel = "🟠";

  const readinessDecision =
    readinessAmpel === "🔴"
      ? "Heute gibt es keine Intensität und keinen zusätzlichen Belastungspush."
      : "Heute gibt es keine Eskalation über den geplanten Reiz hinaus.";

  const readinessConf = computeSectionConfidence({
    hasDrift: drift != null,
    hasHrv: hrvDeltaPct != null,
    hasLoad: Number.isFinite(runLoad7),
    consistent: !(driftSignal === "green" && (hrv1dNegative || !!fatigue?.override)),
    subjectiveAligned: hadKey || !counterIndicator,
    contradictions: driftSignal === "green" && hrv2dNegative,
    hasHistory: Number.isFinite(trend?.recentN) || Number.isFinite(trend?.prevN),
  });
  const aerobicConf = computeSectionConfidence({
    hasDrift: drift != null,
    hasHrv: hrvDeltaPct != null,
    hasLoad: true,
    consistent: driftSignal !== "unknown",
    subjectiveAligned: true,
    contradictions: false,
    hasHistory: Number.isFinite(trend?.dv) || Number.isFinite(trend?.dd),
  });
  const loadConf = computeSectionConfidence({
    hasDrift: drift != null,
    hasHrv: hrvDeltaPct != null,
    hasLoad: true,
    consistent: freqSignal !== "unknown",
    subjectiveAligned: true,
    contradictions: false,
    hasHistory: true,
  });

  const confirmedRules = [];
  if (keySpacing && keySpacing.ok) confirmedRules.push("Nach Key immer 24-48h easy");
  if (freqSignal !== "red") confirmedRules.push("Frequenz halten, Intensität dosieren");

  const proposedRules = [];
  if (hrv1dNegative) proposedRules.push(`Wenn HRV <= ${HRV_NEGATIVE_THRESHOLD_PCT}% vs 7T an 2 Tagen, dann Intensität stoppen (Test über nächste 4 Wochen).`);
  if (driftSignal !== "green") proposedRules.push("Wenn Easy-Drift > Warnschwelle, dann Pace senken oder Lauf kürzen (3 Beobachtungen sammeln).");

  const lines = [];
  lines.push('1) 🧭 Tagesstatus');
  lines.push(`- Heute: ${buildTodayStatus({ hadAnyRun, hadKey, hadGA, totalMinutesToday })} -> ${readinessDecision}.`);
  lines.push(`- Kontext: ${eventDistance} am ${eventDate || "n/a"}${Number.isFinite(daysToEvent) ? ` (${daysToEvent} Tage)` : ""}.`);

  lines.push('');
  lines.push('2) 🚦 Readiness (safety-first)');
  const readinessMissing = [];
  if (drift == null) readinessMissing.push('Drift heute nicht messbar');
  if (hrvDeltaPct == null) readinessMissing.push('HRV heute fehlt');
  let readinessBucket = readinessConf.bucket;
  if (readinessMissing.length && readinessBucket === 'high') readinessBucket = 'medium';
  const readinessSummary = hasHardRedFlag ? 'harte Red Flag aktiv' : 'keine harte Red Flag aktiv';
  const whyNotRed =
    readinessAmpel !== '🔴' && warningCount > 0
      ? ' Warnsignale betreffen aktuell die Trainingsstruktur, nicht die akute Belastbarkeit.'
      : '';
  lines.push(`- Ampel: ${readinessAmpel}`);
  lines.push(`- Red-Flag-Check: HRV ≥2 Tage negativ ${hardRedFlags.hrv2dNegative ? '🔴' : '🟢'} | Bestätigtes Overload-Pattern ${hardRedFlags.confirmedOverloadHigh ? '🔴' : '🟢'} | Mehrere Warnsignale + subjektiv negativ ${hardRedFlags.multiWarningPlusSubjectiveNegative ? '🔴' : '🟢'} | Schmerz/Verletzung ${hardRedFlags.painInjury ? '🔴' : '🟢'}.`);
  lines.push(`- Zusammenfassung: ${readinessSummary}.${whyNotRed}`);
  lines.push(`- Confidence: ${readinessBucket}${readinessMissing.length ? ` (${readinessMissing.join('; ')})` : ''}`);
  lines.push(`- Entscheidung: ${readinessDecision}`);

  lines.push('');
  lines.push('3) 🫁 Aerober Status (personalisiert)');
  lines.push(`- Drift: ${drift != null ? drift.toFixed(1) + '%' : 'unknown'} vs persönlich ${personalDriftWarn}/${personalDriftCritical}% -> ${driftSignal === 'green' ? '🟢' : driftSignal === 'orange' ? '🟠' : driftSignal === 'red' ? '🔴' : '🟠'}.`);
  lines.push(`- Einordnung: ${driftSignal === 'red' ? 'aerober Preis zu hoch, heute entlasten' : driftSignal === 'orange' ? 'Grenzbereich, nur kontrolliert belasten' : 'stabil genug für planmäßiges easy'}.`);
  lines.push(`- Confidence: ${aerobicConf.bucket}`);
  lines.push(`- If-Then: Wenn Drift > ${personalDriftWarn}% bei easy, dann Pace runter oder Einheit um 10-15' kürzen.`);

  lines.push('');
  lines.push('4) 📈 Belastung & Frequenz');
  lines.push(`- Frequenz: ${freqCount14 ?? 'unknown'} Läufe/14d vs Sweetspot ${sweetspotLow}-${sweetspotHigh}, Limit ${upperLimit} -> ${freqSignal === 'green' ? '🟢' : freqSignal === 'red' ? '🔴' : '🟠'}.`);
  lines.push(`- AerobicFloor 7T: Ist ${runLoad7} / Soll ${runTarget || 'n/a'} (Basisziel ${runBaseTarget || 'n/a'}) -> ${runFloorGap ? 'unter Soll, über Häufigkeit schließen' : 'im Zielkorridor'}.`);
  lines.push(`- Floor-Logik: ${floorModeText}.`);
  lines.push(`- RunFloor/Volumen: ${runLoad7}/${runTarget || 'n/a'} -> ${runFloorGap ? 'heute nicht über Intensität kompensieren, eher Umfang stabilisieren' : 'Volumen im Korridor halten'}.`);
  lines.push(`- Confidence: ${loadConf.bucket}`);
  lines.push(`- If-Then: Wenn 2+ Warnsignale gleichzeitig, dann nur easy + optional kürzen.`);

  lines.push('');
  lines.push('5) ✅ Top-3 Coaching-Entscheidungen (heute/48h)');
  lines.push(`- 1) ${readinessAmpel === '🔴' ? 'Intensität pausieren, nur easy/recovery.' : 'Geplanten Reiz halten, aber nicht eskalieren.'}`);
  lines.push(`- 2) ${runFloorGap ? 'AerobicFloor über Häufigkeit auffüllen statt Tempo erzwingen.' : 'AerobicFloor stabil halten, nächster Schritt kommt über Konsistenz.'}`);
  lines.push(`- 3) ${robustness?.strengthOk ? 'Kraft/Stabi normal fortführen.' : "20-30' Kraft/Stabi einplanen."}`);
  lines.push(`- Warum (1 Satz): Safety-first priorisiert ${highPattern ? highPattern.id : hrv2dNegative ? 'Recovery-Status' : 'Belastungsstabilität'} vor Tempo.`);

  lines.push('');
  lines.push('6) 🧬 Ich-Regeln & Lernen (MVP)');
  lines.push(`- Confirmed (anwenden): ${(confirmedRules.slice(0,2).join(' | ') || 'noch keine bestätigte Regel mit hoher Evidenz')}.`);
  lines.push(`- Proposed/Updated (testen): ${(proposedRules.slice(0,2).join(' | ') || 'keine neue Hypothese heute')}.`);
  lines.push(`- Learning today: Du reagierst auf kumulierten Stress robuster mit Frequenzsteuerung als mit zusätzlicher Intensität.`);

  if (debug) {
    const trace = {
      highest_priority_trigger: highPattern?.id || (hrv2dNegative ? 'HRV_2D_NEGATIVE' : 'none'),
      overruled_signals: highPattern && driftSignal === 'green' ? ['drift_ok'] : [],
      guardrail_applied: readinessAmpel !== '🟢',
    };
    lines.push('');
    lines.push(`DecisionTrace: ${JSON.stringify(trace)}`);
  }

  return lines.join("\n");
}

function buildTodayStatus({ hadAnyRun, hadKey, hadGA, totalMinutesToday }) {
  if (!hadAnyRun) return "Kein Lauf";
  const minutesText = totalMinutesToday > 0 ? `${totalMinutesToday}′ ` : "";
  if (hadKey && !hadGA) return `Lauf: ${minutesText}Key`;
  if (hadGA && !hadKey) return `Lauf: ${minutesText}locker`;
  if (hadKey && hadGA) return `Lauf: ${minutesText}GA + Key`;
  return `Lauf: ${minutesText}Lauf`;
}

function buildTodayClassification({ hadAnyRun, hadKey, hadGA, totalMinutesToday }) {
  if (!hadAnyRun) return "Ruhetag (kein Lauf)";
  if (hadKey && hadGA) return "GA + Key (gemischt)";
  if (hadKey) return "Key (intensiv)";
  if (hadGA) return totalMinutesToday > 0 ? `Easy/GA ${totalMinutesToday}′` : "Easy/GA";
  return totalMinutesToday > 0 ? `Lauf ${totalMinutesToday}′` : "Lauf";
}

function buildBottomLineToday({ hadAnyRun, hadKey, hadGA, runFloorState, totalMinutesToday }) {
  const overlay = runFloorState?.overlayMode ?? "NORMAL";
  if (overlay === "RECOVER_OVERLAY") return "Rest/Recovery";
  if (overlay === "TAPER") return "Taper/Frische";
  if (overlay === "DELOAD") return "Deload";
  if (hadKey) return "Training absolviert";
  if (hadGA) return totalMinutesToday > 0 ? `GA ${totalMinutesToday}′` : "GA";
  if (hadAnyRun) return totalMinutesToday > 0 ? `Lauf ${totalMinutesToday}′` : "Lauf";
  return "Rest (kein Lauf)";
}

function buildKeyConsequence({ keyCompliance, keySpacing, keyCap }) {
  if (keyCompliance?.capExceeded) return "Weitere Einheiten nur locker/steady.";
  if (keySpacing?.ok === false) return "Weitere Einheiten nur locker/steady (Key-Abstand <48h).";
  if ((keyCompliance?.actual7 ?? 0) < keyCap) return "1 Key noch möglich.";
  return "Weitere Einheiten nur locker/steady.";
}

function buildDeloadExplanation(runFloorState) {
  if (!runFloorState || runFloorState.overlayMode !== "DELOAD") return null;
  const reason =
    runFloorState.reasons?.find((r) => r.startsWith("Deload ausgelöst")) ||
    runFloorState.reasons?.find((r) => r.startsWith("Deload läuft")) ||
    "Deload aktiv";
  const endText = runFloorState.deloadEndDate ? ` bis ${runFloorState.deloadEndDate}` : "";
  return `${reason}${endText}`;
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
  const comparableActs = await gatherGASamples(ctx, endIso, 2 * TREND_WINDOW_DAYS, { comparable: true });
  const lastComparableDate = comparableActs.length
    ? comparableActs.map((x) => x.date).sort((a, b) => a.localeCompare(b)).at(-1)
    : null;

  // split by date string (deterministic)
  const recent = gaActs.filter((x) => x.date >= recentStart);
  const prev = gaActs.filter((x) => x.date < recentStart && x.date >= prevStart);

  if (recent.length < TREND_MIN_N || prev.length < TREND_MIN_N) {
    return {
      ok: false,
      confidence: "niedrig",
      recentCount: recent.length,
      prevCount: prev.length,
      recentStart,
      prevStart,
      windowEnd: endIso,
      lastComparableDate,
      text: `ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – zu wenig GA-Daten (recent=${recent.length}, prev=${prev.length})`,
    };
  }

  const ef1 = avg(recent.map((x) => x.ef));
  const ef0 = avg(prev.map((x) => x.ef));
  const d1 = median(recent.map((x) => x.drift));
  const d0 = median(prev.map((x) => x.drift));

  if (ef0 == null || ef1 == null || d0 == null || d1 == null) {
    return {
      ok: false,
      confidence: "niedrig",
      recentCount: recent.length,
      prevCount: prev.length,
      recentStart,
      prevStart,
      windowEnd: endIso,
      lastComparableDate,
      text: "ℹ️ Aerober Kontext (nur GA)\nTrend: n/a – fehlende Werte",
    };
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
    confidence,
    recentCount: recent.length,
    prevCount: prev.length,
    recentStart,
    prevStart,
    windowEnd: endIso,
    lastComparableDate,
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

function buildMiniPlanTargets({ runsPerWeek, weeklyLoad, keyPerWeek }) {
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

  const includeKey = keyPerWeek >= 0.6 || (runsPerWeek >= 3 && weeklyLoad >= 140);
  const exampleWeek =
    runTarget === "2–3"
      ? ["Mi 30–35′ easy", "So 60–75′ longrun"]
      : includeKey
      ? ["Di 35–45′ key (Schwelle/VO2)", "Fr 40–50′ GA", "So 60–75′ longrun"]
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
  const title = `🕵️‍♂️ Montags-Report (${windowDays}T)`;
  const lines = [];
  lines.push(title);
  lines.push("");
  lines.push("🏗️ Struktur:");
  lines.push(`• 🏃 Läufe: ${totalRuns} (Ø ${runsPerWeek.toFixed(1)}/Woche)`);
  lines.push(`• ⏱️ Minuten: ${Math.round(totalMin)} | Load: ${Math.round(totalLoad)} (~${Math.round(weeklyLoad)}/Woche)`);
  lines.push(`• 🧱 Longruns: ${longRuns.length} (Ø ${longPerWeek.toFixed(1)}/Woche) | 🎯 Key: ${keyRuns.length} (Ø ${keyPerWeek.toFixed(1)}/Woche)`);
  lines.push(`• 🌿 GA (≥30′, nicht key): ${gaRuns.length} | ⚡ Kurz (<30′): ${shortRuns.length}`);
  lines.push(`• 🧭 ${keyTypeLine}`);
  lines.push("");
  lines.push("📈 Belastung:");
  lines.push(`• 📊 Monotony: ${isFiniteNumber(monotony) ? monotony.toFixed(2) : "n/a"} | Strain: ${isFiniteNumber(strain) ? strain.toFixed(0) : "n/a"}`);
  lines.push("");

  lines.push("🔍 Highlights:");
  if (!findings.length) lines.push("• ✅ Keine klaren strukturellen Probleme.");
  else for (const f of findings.slice(0, 4)) lines.push(`• 🧩 ${f}`);

  lines.push("");
  lines.push("✅ Nächste Schritte:");
  if (!actions.length) lines.push("• 📌 Struktur halten, Bench/GA comparable sammeln.");
  else for (const a of uniq(actions).slice(0, 4)) lines.push(`• 🛠️ ${a}`);

  const miniPlan = buildMiniPlanTargets({ runsPerWeek, weeklyLoad, keyPerWeek });
  lines.push("");
  lines.push("🗓️ Mini-Plan nächste Woche:");
  lines.push(
    `• 🎯 Ziele: ${miniPlan.runTarget} Läufe/Woche | ${miniPlan.loadTarget} Run-Load/Woche | 1× Longrun 60–75′`
  );
  lines.push(`• 📅 Beispiel: ${miniPlan.exampleWeek.join(" · ")}`);

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
  const name = "Montags-Report";
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
  const contextParts = [];
  const durationText = fmtDurationMin(Number(activity?.moving_time ?? activity?.elapsed_time ?? 0));
  if (durationText) contextParts.push(`Dauer ${durationText}`);
  const distanceMeters = Number(activity?.distance ?? activity?.distance_metres ?? activity?.distanceMeters);
  const distanceText = fmtDistanceKm(distanceMeters);
  if (distanceText) contextParts.push(`Dist ${distanceText}`);
  if (Number.isFinite(activity?.average_heartrate)) contextParts.push(`ØHF ${Math.round(activity.average_heartrate)} bpm`);
  if (Number.isFinite(activity?.average_temp)) contextParts.push(`Temp ${activity.average_temp.toFixed(1)}°C`);
  const load = extractLoad(activity);
  if (Number.isFinite(load) && load > 0) contextParts.push(`Load ${Math.round(load)}`);
  if (contextParts.length) lines.push(`Kontext: ${contextParts.join(" | ")}`);

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

  if (benchType === "GA" && !isKey) {
    if (same.length && today.drift != null && last?.drift != null) {
      const dVsLast = today.drift - last.drift;
      lines.push(`Drift: ${fmtSigned1(dVsLast)}%-Pkt vs letzte`);
    } else if (today.drift != null) {
      lines.push(`Drift: ${fmtSigned1(today.drift)}%-Pkt`);
    }
  } else {
    if (intervalMetrics?.HR_Drift_bpm != null) {
      const driftPct = intervalMetrics.HR_Drift_pct;
      const driftFlagLabel = formatDriftFlag(intervalMetrics.drift_flag);
      const driftFlag = driftFlagLabel ? ` (${driftFlagLabel})` : "";
      const driftPctText = Number.isFinite(driftPct) ? `, ${fmtSigned1(driftPct)}%` : "";
      lines.push(`HF-Drift (Intervall): ${fmtSigned1(intervalMetrics.HR_Drift_bpm)} bpm${driftPctText}${driftFlag}`);
    }
    if (intervalMetrics?.HRR60_median != null) {
      lines.push(`Erholung: HRR60 ${intervalMetrics.HRR60_median.toFixed(0)} bpm (HF-Abfall in 60s)`);
    }
    if (!intervalMetrics?.HR_Drift_bpm && isKey) {
      if (same.length && last?.avgSpeed != null) {
        const speedVsLast = pct(today.avgSpeed, last.avgSpeed);
        lines.push(`Tempo: ${fmtSigned1(speedVsLast)}% vs letzte`);
      } else if (today.avgSpeed != null) {
        lines.push(`Tempo: ${today.avgSpeed.toFixed(2)} m/s`);
      }
    }
  }

  let verdict = "Stabil – Basis bestätigt (Trend intakt).";
  let lastIntervalMetrics = null;
  if (same.length && (benchType !== "GA" || isKey)) {
    lastIntervalMetrics = await computeIntervalBenchMetrics(env, same[0], warmupSkipSec);
  }

  if (same.length && intervalMetrics && lastIntervalMetrics) {
    if (intervalMetrics.HRR60_median != null && lastIntervalMetrics.HRR60_median != null) {
      const hrr60Delta = intervalMetrics.HRR60_median - lastIntervalMetrics.HRR60_median;
      if (hrr60Delta >= 3) {
        verdict = `Einheit besser – schnellere Erholung (HRR60 ${fmtSigned1(hrr60Delta)} bpm vs letzte).`;
      } else if (hrr60Delta <= -3) {
        verdict = `Einheit schlechter – langsamere Erholung (HRR60 ${fmtSigned1(hrr60Delta)} bpm vs letzte).`;
      } else {
        verdict = `Einheit ähnlich – Erholung nahezu gleich (HRR60 ${fmtSigned1(hrr60Delta)} bpm vs letzte).`;
      }
    } else if (intervalMetrics.HR_Drift_bpm != null && lastIntervalMetrics.HR_Drift_bpm != null) {
      const driftDelta = intervalMetrics.HR_Drift_bpm - lastIntervalMetrics.HR_Drift_bpm;
      if (driftDelta >= 3) {
        verdict = `Einheit härter – HF-Drift höher (${fmtSigned1(driftDelta)} bpm vs letzte).`;
      } else if (driftDelta <= -3) {
        verdict = `Einheit leichter – HF-Drift niedriger (${fmtSigned1(driftDelta)} bpm vs letzte).`;
      } else {
        verdict = `Einheit ähnlich – HF-Drift vergleichbar (${fmtSigned1(driftDelta)} bpm vs letzte).`;
      }
    }
  }

  if (verdict === "Stabil – Basis bestätigt (Trend intakt).") {
    if (intervalMetrics?.HRR60_median != null && intervalMetrics.HRR60_median < 15) {
      verdict = "Hohe Belastung – Erholung limitiert.";
    } else if (intervalMetrics?.drift_flag === "too_hard") {
      verdict = "Hohe Belastung – HF-Drift zu hoch.";
    } else if (intervalMetrics?.drift_flag === "overreaching") {
      verdict = "Überzogen – HF-Drift spricht für Overreaching.";
    }
  }

  lines.push(`Fazit: ${verdict}`);
  return lines.join("\n");
}

async function computeIntervalBenchMetrics(env, a, warmupSkipSec) {
  const streams = await fetchIntervalsStreams(env, a.id, STREAM_TYPES_INTERVAL);
  if (!streams) return null;

  return computeIntervalMetricsFromStreams(streams, {
    intervalType: getIntervalTypeFromActivity(a),
  });
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

function fmtDurationMin(seconds) {
  if (!Number.isFinite(seconds) || seconds <= 0) return null;
  return `${Math.round(seconds / 60)}′`;
}

function fmtDistanceKm(distanceMeters) {
  if (!Number.isFinite(distanceMeters) || distanceMeters <= 0) return null;
  if (distanceMeters >= 1000) return `${(distanceMeters / 1000).toFixed(1)} km`;
  return `${distanceMeters.toFixed(0)} m`;
}

// ================= STREAMS METRICS =================
function quantile(arr, q) {
  const v = arr.filter((x) => x != null && Number.isFinite(x)).sort((a, b) => a - b);
  if (!v.length) return null;
  const pos = (v.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (v[base + 1] != null) return v[base] + rest * (v[base + 1] - v[base]);
  return v[base];
}

function pickIntervalIntensity(streams) {
  const watts = streams?.watts;
  const speed = streams?.velocity_smooth;
  if (Array.isArray(watts) && watts.some((x) => Number.isFinite(x))) return { data: watts, kind: "watts" };
  if (Array.isArray(speed) && speed.some((x) => Number.isFinite(x))) return { data: speed, kind: "speed" };
  return null;
}

function buildWorkIntervals(time, intensity, { threshold, minIntervalSec = 60, maxGapSec = 5 } = {}) {
  const n = Math.min(time.length, intensity.length);
  if (n < 2) return [];

  const intervals = [];
  let startIdx = null;
  let lastAboveIdx = null;
  let gapStart = null;

  const timeAt = (i) => {
    const t = Number(time[i]);
    return Number.isFinite(t) ? t : i;
  };

  for (let i = 0; i < n; i++) {
    const v = Number(intensity[i]);
    if (Number.isFinite(v) && v >= threshold) {
      if (startIdx == null) startIdx = i;
      lastAboveIdx = i;
      gapStart = null;
      continue;
    }

    if (startIdx != null) {
      if (gapStart == null) gapStart = timeAt(i);
      if (timeAt(i) - gapStart > maxGapSec) {
        const startTime = timeAt(startIdx);
        const endTime = timeAt(lastAboveIdx);
        const duration = endTime - startTime;
        if (duration >= minIntervalSec) {
          intervals.push({ startIdx, endIdx: lastAboveIdx, startTime, endTime, duration });
        }
        startIdx = null;
        lastAboveIdx = null;
        gapStart = null;
      }
    }
  }

  if (startIdx != null && lastAboveIdx != null) {
    const startTime = timeAt(startIdx);
    const endTime = timeAt(lastAboveIdx);
    const duration = endTime - startTime;
    if (duration >= minIntervalSec) {
      intervals.push({ startIdx, endIdx: lastAboveIdx, startTime, endTime, duration });
    }
  }

  return intervals;
}

function classifyIntervalDrift(intervalType, driftBpm) {
  if (!Number.isFinite(driftBpm)) return null;
  if (intervalType === "threshold") {
    if (driftBpm <= 5) return "controlled";
    if (driftBpm <= 8) return "acceptable";
    return "too_hard";
  }
  if (intervalType === "vo2") {
    return driftBpm > 10 ? "overreaching" : "acceptable";
  }
  return null;
}

function formatDriftFlag(flag) {
  if (!flag) return null;
  if (flag === "controlled") return "kontrolliert";
  if (flag === "acceptable") return "akzeptabel";
  if (flag === "too_hard") return "zu hart";
  if (flag === "overreaching") return "Überreizung";
  return flag;
}

function computeIntervalMetricsFromStreams(streams, { intervalType } = {}) {
  const hr = streams?.heartrate;
  const time = streams?.time;
  if (!Array.isArray(hr) || !Array.isArray(time)) return null;

  const intensityInfo = pickIntervalIntensity(streams);
  if (!intensityInfo) return null;

  const n = Math.min(hr.length, time.length, intensityInfo.data.length);
  if (n < 2) return null;

  const timeSlice = time.slice(0, n);
  const intensity = intensityInfo.data.slice(0, n);
  const hrSlice = hr.slice(0, n);

  const intensityVals = intensity.filter((x) => Number.isFinite(x));
  const threshold = quantile(intensityVals, 0.75);
  if (!Number.isFinite(threshold)) return null;

  const intervals = buildWorkIntervals(timeSlice, intensity, { threshold });
  if (intervals.length < 2) return null;

  const durations = intervals.map((i) => i.duration);
  const minDur = Math.min(...durations);
  const maxDur = Math.max(...durations);
  if (minDur <= 0 || maxDur / minDur > 1.1) return null;

  const intensityMeans = intervals.map((interval) => {
    let sum = 0;
    let count = 0;
    for (let i = interval.startIdx; i <= interval.endIdx; i++) {
      const v = Number(intensity[i]);
      if (Number.isFinite(v)) {
        sum += v;
        count++;
      }
    }
    return count ? sum / count : null;
  });

  const validIntensity = intensityMeans.filter((x) => Number.isFinite(x));
  if (validIntensity.length !== intervals.length) return null;
  const minIntensity = Math.min(...validIntensity);
  const maxIntensity = Math.max(...validIntensity);
  if (minIntensity <= 0 || maxIntensity / minIntensity > 1.1) return null;

  const timeAt = (i) => {
    const t = Number(timeSlice[i]);
    return Number.isFinite(t) ? t : i;
  };

  const intervalHr = intervals.map((interval) => {
    const startTime = interval.startTime;
    const endTime = interval.endTime;
    const duration = interval.duration;
    const lateStart = startTime + duration * 0.6;

    let lateSum = 0;
    let lateCount = 0;
    let peak = -Infinity;
    for (let i = interval.startIdx; i <= interval.endIdx; i++) {
      const t = timeAt(i);
      const h = Number(hrSlice[i]);
      if (!Number.isFinite(h)) continue;
      if (h > peak) peak = h;
      if (t >= lateStart && t <= endTime) {
        lateSum += h;
        lateCount++;
      }
    }
    const lateAvg = lateCount ? lateSum / lateCount : null;

    const target = endTime + 60;
    let hr60 = null;
    for (let i = interval.endIdx; i < n; i++) {
      const t = timeAt(i);
      if (t >= target) {
        const h = Number(hrSlice[i]);
        if (Number.isFinite(h)) hr60 = h;
        break;
      }
    }

    return {
      lateAvg,
      peak: Number.isFinite(peak) ? peak : null,
      hr60,
    };
  });

  const first = intervalHr[0]?.lateAvg;
  const last = intervalHr[intervalHr.length - 1]?.lateAvg;
  if (!Number.isFinite(first) || !Number.isFinite(last) || first <= 0) return null;

  const hrDriftBpm = last - first;
  const hrDriftPct = ((last - first) / first) * 100;

  const hrr60Drops = intervalHr
    .map((x) => (Number.isFinite(x.peak) && Number.isFinite(x.hr60) ? x.peak - x.hr60 : null))
    .filter((x) => Number.isFinite(x));
  const hrr60Median = hrr60Drops.length ? median(hrr60Drops) : null;

  return {
    HR_Drift_bpm: hrDriftBpm,
    HR_Drift_pct: hrDriftPct,
    HRR60_median: hrr60Median,
    drift_flag: classifyIntervalDrift(intervalType, hrDriftBpm),
    interval_type: intervalType ?? null,
  };
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

function normalizeTags(tags) {
  return (tags || []).map((t) => String(t || "").toLowerCase().trim()).filter(Boolean);
}

function isStrength(a) {
  const type = String(a?.type ?? "").toLowerCase();
  const typeHit =
    type.includes("strength") || type.includes("gym") || type.includes("workout") || type.includes("training");
  if (typeHit) return true;
  const tags = normalizeTags(a?.tags);
  const strengthTags = new Set(["strength", "stabi", "kraft", "gym", "core", "mobility"]);
  return tags.some((t) => strengthTags.has(t));
}
async function buildWatchfacePayload(env, endIso) {
  const end = parseISODateSafe(endIso) ? endIso : isoDate(new Date());
  const startIso = isoDate(new Date(new Date(end + "T00:00:00Z").getTime() - 6 * 86400000));

  // Fetch activities only for these 7 days (klein halten)
  const acts = await fetchIntervalsActivities(env, startIso, end);

  const days = listIsoDaysInclusive(startIso, end); // genau 7
  const runLoadByDay = {};
  const strengthMinByDay = {};

  for (const d of days) { runLoadByDay[d] = 0; strengthMinByDay[d] = 0; }

  for (const a of acts) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || !(d in runLoadByDay)) continue;

    if (isRun(a)) {
      runLoadByDay[d] += Number(extractLoad(a)) || 0; // dein “TSS/Load”-Proxy (icu_training_load/hr_load)
      continue;
    }

    if (isStrength(a)) {
      const sec = Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
      strengthMinByDay[d] += sec / 60;
      continue;
    }
  }

  const runLoad = days.map((d) => Math.round(runLoadByDay[d] || 0));
  const strengthMin = days.map((d) => Math.round(strengthMinByDay[d] || 0));

  const runSum7 = runLoad.reduce((a, b) => a + b, 0);
  const strengthSum7 = strengthMin.reduce((a, b) => a + b, 0);

  return {
    ok: true,
    endIso: end,
    days,
    runLoad,
    runSum7,
    runGoal: 150,
    strengthMin,
    strengthSum7,
    strengthGoal: 60,
    updatedAt: new Date().toISOString(),
  };
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

function getIntervalTypeFromActivity(a) {
  const keyType = getKeyType(a);
  if (!keyType) return null;
  const s = String(keyType).toLowerCase();
  if (s.includes("vo2") || s.includes("v02")) return "vo2";
  if (s.includes("schwelle") || s.includes("threshold")) return "threshold";
  return null;
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

      // ✅ NEU: Distanzfelder dumpen
      distance: e.distance,
      Distance: e.Distance,
      raceDistance: e.raceDistance,
      plannedDistance: e.plannedDistance,
      eventDistance: e.eventDistance,
      targetDistance: e.targetDistance,
      distanceMeters: e.distanceMeters,
      distance_metres: e.distance_metres,
      distance_km: e.distance_km,
      details_distance: e.details?.distance,
      details_distanceMeters: e.details?.distanceMeters,
      race_distance: e.race?.distance,
      race_distanceMeters: e.race?.distanceMeters,
    }))
  );
}

  return races;
}
