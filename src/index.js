// ====== src/index.js (PART 1/4) ======
// Cloudflare Worker – Run only
//
// Required Secret:
// INTERVALS_API_KEY
//
// Wellness custom fields (create these in Intervals):
// VDOT, Drift, Motor, EF, Block, BlockEffective
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
    const cron = String(event?.cron || "");
    const isMorningRun = cron === "0 6 * * *";
    const isEveningWatcher = !isMorningRun;
    let latestActivityIso = null;

    try {
      latestActivityIso = await fetchLatestActivityIso(env, yday, today);
    } catch (e) {
      console.error("scheduled latest activity fetch failed", e);
    }

    if (isEveningWatcher) {
      if (!latestActivityIso) {
        console.log("scheduled evening sync skipped: no recent activity");
        return;
      }

      const lastSeen = await readKvJson(env, LAST_ACTIVITY_SYNC_KEY);
      if (lastSeen && new Date(latestActivityIso) <= new Date(lastSeen)) {
        console.log("scheduled evening sync skipped: no new activity");
        return;
      }
    }

    ctx.waitUntil(
      (async () => {
        await syncRange(env, yday, today, true, false, 600);
        if (latestActivityIso) {
          await writeKvJson(env, LAST_ACTIVITY_SYNC_KEY, latestActivityIso);
        }
      })().catch((e) => {
        console.error("scheduled syncRange failed", e);
      })
    );
  },
};

// ================= CONFIG =================
// ================= GUARDRAILS (NEW) =================
const MAX_KEYS_7D = 2;
const STRENGTH_MIN_7D = 60;
const DRIFT_WARN_PCT = 6; // Adjust steady_t drift warning threshold here
const DRIFT_CRITICAL_PCT = 8; // Adjust critical drift threshold here
const DRIFT_STEADY_T_MAX_PCT = 6.5; // Adjust expected steady_t drift ceiling here
const DRIFT_TREND_WORSENING_PCT = 1.0; // Δ drift (recent-prev) that triggers delay
const STEADY_T_MAX_PER_7D = 1;
const KEY_HARD_MAX_PER_7D = 2;
const STEADY_T_DELAY_DAYS_RANGE = { min: 5, max: 7 };
const DECISION_CONF_MIN = 40; // NEW: STEADY_T decision confidence threshold
const STEADY_T_QUALITY_MIN_MINUTES = 20;
const STEADY_T_QUALITY_MAX_MINUTES = 30;
const STEADY_T_BUDGET_MODE = "exclusive"; // NEW: replacement vs exclusive budget mode
const STEADY_T_BUDGET_KEY_TYPE = "keyHard";
const STEADY_T_TAGS = new Set(["steady_t", "steady-t", "steady:t", "steady t"]);
const INTENSITY_CLASS = {
  EASY: "EASY",
  STEADY_T: "STEADY_T",
  KEY_HARD: "KEY_HARD",
};
const INTENSITY_RECOMMENDATION_CLASS = {
  EASY_BASE: "EASY_BASE",
  STEADY: "STEADY",
  STRIDES: "STRIDES",
  RACEPACE: "RACEPACE",
  VO2_TOUCH: "VO2_TOUCH",
};
const BASE_URL = "https://intervals.icu/api/v1";
const DETECTIVE_KV_PREFIX = "detective:week:";
const DETECTIVE_KV_HISTORY_KEY = "detective:history";
const DETECTIVE_HISTORY_LIMIT = 12;
const LAST_ACTIVITY_SYNC_KEY = "scheduled:last-activity-iso";
const REENTRY_STATE_KEY = "blockEffective:reentryState";
const REENTRY_DAYS_DEFAULT = 7; // env: REENTRY_DAYS
const OVERRIDE_CATEGORIES = ["INJURED", "SICK", "HOLIDAY"];
const OVERRIDE_PRIORITY = { INJURED: 3, SICK: 2, HOLIDAY: 1 };
const OVERRIDE_TO_BLOCK = {
  INJURED: "HOLD_INJURY",
  SICK: "HOLD_ILLNESS",
  HOLIDAY: "HOLD_LIFE",
};
// REMOVE or stop using this for Aerobic:
// const BIKE_EQ_FACTOR = 0.65;

// ================= BLOCK CONFIG (NEW) =================
const HRR60_INTERVAL_CONFIG = {
  minIntervalSec: 90,
  detectionMinIntervalSec: 10,
  detectedMinIntervalSec: 75,
  maxGapSec: 5,
  mergeGapSec: 12,
  minValidHrWindowSec: 75,
  maxHrDropoutSec: 5,
  hrZoneZ4Ratio: 0.88,
  hrZoneZ4Quantile: 0.85,
  racepaceToleranceSecPerKm: 10,
  racepaceSpeedQuantile: 0.85,
  vo2PowerQuantile: 0.9,
  tagSpeedQuantile: 0.7,
  tagPowerQuantile: 0.8,
  hrr60ExactToleranceSec: 3,
  hrr60FallbackWindowSec: { start: 45, end: 75 },
};

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

function getReentryDays(env) {
  const raw = Number(env?.REENTRY_DAYS);
  if (Number.isFinite(raw) && raw > 0) return Math.floor(raw);
  return REENTRY_DAYS_DEFAULT;
}

// Fatigue override thresholds (tune later)
const RAMP_HARD_PCT = 0.5;         // NEW: guardrail severity (hard)
const RAMP_SOFT_PCT = 0.3;         // NEW: guardrail severity (soft)
const MONOTONY_7D_LIMIT = 2.0;     // mean/sd daily load
const STRAIN_7D_LIMIT = 1200;      // monotony * weekly load (scale depends on your load units)
const ACWR_HARD = 1.6;             // NEW: guardrail severity (hard)
const ACWR_SOFT = 1.3;             // NEW: guardrail severity (soft)
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

const POLICY_REGISTRY = {
  PAT_PAT_001__NO_INTENSITY_7D: {
    short: "Keine Intensität für 5–7 Tage",
    why: "Kombination aus Drift/HRV/Key-Signal deutet auf Überlastungsrisiko hin.",
    effect: "Intensität pausieren, Fokus auf Erholung.",
    severity: "high",
    tags: ["guardrail"],
  },
  PAT_PAT_002__EASY_ONLY_TODAY: {
    short: "Nur easy (24–48h)",
    why: "Akute Stressmarker sind erhöht.",
    effect: "Nur lockere Einheiten in den nächsten 24–48h.",
    severity: "high",
    tags: ["guardrail"],
  },
  PAT_PAT_002__REDUCE_VOL_15: {
    short: "Volumen reduzieren (10–15%)",
    why: "Akute Belastungssignale sprechen für eine Volumenreduktion.",
    effect: "Gesamtvolumen für 24–48h um ~10–15% senken.",
    severity: "medium",
    tags: ["load"],
  },
  PAT_PAT_003__ACTION_GENERIC_MEDIUM: {
    short: "Belastungsdichte reduzieren",
    why: "Häufung von Warnsignalen bei hohem Dichteprofil.",
    effect: "Zusatzreize vermeiden, Dichte runterfahren.",
    severity: "medium",
    tags: ["guardrail"],
  },
  SIG__hrv_2d_negative__HIGH: {
    short: "HRV 2 Tage negativ",
    why: "HRV ist über 2 Tage deutlich gesunken.",
    effect: "Belastung temporär reduzieren.",
    severity: "high",
    tags: ["signal"],
  },
  SIG__fatigue_override__MEDIUM: {
    short: "Fatigue-Schwelle überschritten",
    why: "Kumulierte Last überschreitet deine Schwelle.",
    effect: "Zusatzreize vermeiden.",
    severity: "medium",
    tags: ["signal"],
  },
};

const TREND_WINDOW_DAYS = 28;
const TREND_MIN_N = 3;

const MOTOR_WINDOW_DAYS = 28;
const MOTOR_NEED_N_PER_HALF = 2;
const MOTOR_DRIFT_WINDOW_DAYS = 14;
const LEARNING_LOOKBACK_DAYS = 120;
const LEARNING_DECAY_DAYS = 45;
const LEARNING_GOOD_OUTCOME_THRESHOLD = 2;
const LEARNING_MIN_NEFF = 3;
const LEARNING_CONFIDENCE_K = 6;
const LEARNING_MIN_CONF = 0.4;
const LEARNING_UTILITY_EPS = 0.05;
const LEARNING_UTILITY_LAMBDA = 1.5;
const LEARNING_MIN_ARM_NEFF = 0.5;
const LEARNING_TEXT_MIN_ARM_NEFF = 3.0;
const LEARNING_TEXT_MIN_COMPARE_NEFF = 3.0;
const LEARNING_TEXT_MIN_COMPARE_ARMS = 2;
const LEARNING_TEXT_MIN_CONF_FOR_STRONG = 0.6;
const LEARNING_TEXT_UTILITY_EPS = 0.05;
const LEARNING_SAFE_FALLBACK_ARMS = ["HOLD_ABSORB", "FREQ_UP", "NEUTRAL"];
const LEARNING_EXPLORE_UNTRIED = false;
const LEARNING_GLOBAL_CONTEXT_KEY = "ALL";

const STRATEGY_ARMS = [
  "FREQ_UP",
  "INTENSITY_SHIFT",
  "VOLUME_ADJUST",
  "HOLD_ABSORB",
  "PROTECT_DELOAD",
  "NEUTRAL",
];

const STRATEGY_LABELS = {
  FREQ_UP: "Häufiger, kürzer, locker",
  INTENSITY_SHIFT: "Qualitätsreiz statt mehr Umfang",
  VOLUME_ADJUST: "Umfang gezielt anpassen",
  HOLD_ABSORB: "Stabilisieren & absorbieren",
  PROTECT_DELOAD: "Schützen & deloaden",
  NEUTRAL: "Neutral (keine klare Strategie)",
};

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
const FIELD_BLOCK_EFFECTIVE = "BlockEffective";

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
  const end = getHistoryWindowEnd(dayIso);
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

async function computeKeyTypeCounts7d(ctx, dayIso) {
  const end = getHistoryWindowEnd(dayIso);
  const startIso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const keyTypes = [];
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!hasKeyTag(a)) continue;
    const keyType = getKeyType(a);
    if (keyType) keyTypes.push(keyType);
  }
  return countBy(keyTypes);
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
  const end = getHistoryWindowEnd(dayIso);

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
  const keyTypeCounts7d = await computeKeyTypeCounts7d(ctx, dayIso);
  const keyCap = Number.isFinite(options.maxKeys7d) ? options.maxKeys7d : MAX_KEYS_7D;

  const reasons = [];
  const guardrailReasons = [];
  let guardrailSeverity = "none";
  let fatigueSeverity = "low";
  const severityRank = { none: 0, soft: 1, hard: 2 };
  const upgradeSeverity = (next) => {
    if (severityRank[next] > severityRank[guardrailSeverity]) guardrailSeverity = next;
    if (next === "hard") fatigueSeverity = "high";
    else if (next === "soft" && fatigueSeverity === "low") fatigueSeverity = "medium";
  };
  const guardrailReasonText = (id, value) => `${id}:${value}`;

  if (keyCount7 > keyCap) {
    reasons.push(`Key-Cap: ${keyCount7}/${keyCap} Key in 7 Tagen`);
    upgradeSeverity("soft");
  }
  if (rampPct > RAMP_SOFT_PCT) {
    const isHard = rampPct >= RAMP_HARD_PCT;
    reasons.push(`Ramp: ${(rampPct * 100).toFixed(0)}% vs vorherige 7 Tage`);
    guardrailReasons.push(guardrailReasonText("ramp_pct", rampPct.toFixed(2)));
    upgradeSeverity(isHard ? "hard" : "soft"); // NEW: guardrail severity
  }
  if (acwr != null && acwr > ACWR_SOFT) {
    const isHard = acwr >= ACWR_HARD;
    reasons.push(`ACWR: ${acwr.toFixed(2)} (> ${isHard ? ACWR_HARD : ACWR_SOFT})`);
    guardrailReasons.push(guardrailReasonText("acwr", acwr.toFixed(2)));
    upgradeSeverity(isHard ? "hard" : "soft"); // NEW: guardrail severity
  }
  if (acwr != null && acwr < ACWR_LOW_LIMIT && last7 > 0) {
    reasons.push(`ACWR: ${acwr.toFixed(2)} (< ${ACWR_LOW_LIMIT})`);
    upgradeSeverity("soft");
  }
  if (monotony > MONOTONY_7D_LIMIT) {
    reasons.push(`Monotony: ${monotony.toFixed(2)} (> ${MONOTONY_7D_LIMIT})`);
    upgradeSeverity("soft");
  }
  if (strain > STRAIN_7D_LIMIT) {
    reasons.push(`Strain: ${strain.toFixed(0)} (> ${STRAIN_7D_LIMIT})`);
    upgradeSeverity("soft");
  }

  const override = reasons.length > 0;

  return {
    override,
    reasons,
    guardrailSeverity,
    guardrailReasons,
    severity: fatigueSeverity,
    keyCount7,
    keyTypeCounts7d,
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
  const end = getHistoryWindowEnd(dayIso);
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
  const end = getHistoryWindowEnd(dayIso);
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

function keyTypeToFamily(keyType) {
  if (keyType === "racepace") return "racepace";
  if (keyType === "vo2_touch") return "vo2_touch";
  if (keyType === "strides") return "strides";
  if (keyType === "steady") return "steady";
  return null;
}

function computeLastKeyInfo(ctx, dayIso, windowDays = 14) {
  const end = getHistoryWindowEnd(dayIso);
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  const keyHistory = [];

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!hasKeyTag(a)) continue;
    const rawType = getKeyType(a);
    const keyType = normalizeKeyType(rawType, {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    const family = keyTypeToFamily(keyType);
    keyHistory.push({ date: d, keyType, family });
  }

  keyHistory.sort((a, b) => a.date.localeCompare(b.date));
  const lastEntry = keyHistory.length ? keyHistory[keyHistory.length - 1] : null;
  return {
    windowDays,
    keyHistory,
    lastKeyIso: lastEntry?.date ?? null,
    lastKeyType: lastEntry?.keyType ?? null,
    lastKeyFamily: lastEntry?.family ?? null,
  };
}

async function computeLastKeyIntervalInsights(ctx, dayIso, windowDays = 21) {
  const end = getHistoryWindowEnd(dayIso);
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  const keyHistory = [];

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!hasKeyTag(a)) continue;
    const rawType = getKeyType(a);
    const keyType = normalizeKeyType(rawType, {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    keyHistory.push({ date: d, keyType, activity: a });
  }

  if (!keyHistory.length) return null;
  keyHistory.sort((a, b) => a.date.localeCompare(b.date));

  const racepaceCandidates = keyHistory.filter((entry) => entry.keyType === "racepace");
  const lastEntry = racepaceCandidates.length ? racepaceCandidates[racepaceCandidates.length - 1] : keyHistory[keyHistory.length - 1];
  if (!lastEntry?.activity) return null;

  try {
    const streams = await getStreams(ctx, lastEntry.activity.id, STREAM_TYPES_INTERVAL);
    const intervalMetrics = computeIntervalMetricsFromStreams(streams, {
      intervalType: getIntervalTypeFromActivity(lastEntry.activity),
      activity: lastEntry.activity,
    });
    if (!intervalMetrics) return null;
    const paceText = formatPaceSeconds(intervalMetrics.interval_pace_sec_per_km);
    return {
      activityId: lastEntry.activity.id,
      date: lastEntry.date,
      keyType: lastEntry.keyType,
      intervalMetrics,
      paceText,
    };
  } catch {
    return null;
  }
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
const RUN_FLOOR_POST_ILLNESS_EF_DROP_PCT = 5;
const RUN_FLOOR_POST_ILLNESS_DRIFT_WORSEN_PCT = 1.5;
const RUN_FLOOR_POST_ILLNESS_MOTOR_LOW = 45;
const RUN_FLOOR_INJURY_PAUSE_DAYS = 14;

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

function evaluatePostIllnessPerformance({ motor, trend }) {
  const motorValueLow = Number.isFinite(motor?.value) && motor.value <= RUN_FLOOR_POST_ILLNESS_MOTOR_LOW;
  const motorEfDrop = Number.isFinite(motor?.dv) && motor.dv <= -RUN_FLOOR_POST_ILLNESS_EF_DROP_PCT;
  const trendEfDrop = Number.isFinite(trend?.dv) && trend.dv <= -RUN_FLOOR_POST_ILLNESS_EF_DROP_PCT;
  const trendDriftWorse =
    Number.isFinite(trend?.dd) && trend.dd >= RUN_FLOOR_POST_ILLNESS_DRIFT_WORSEN_PCT;
  const motorDriftWorse =
    Number.isFinite(motor?.dd) && motor.dd >= RUN_FLOOR_POST_ILLNESS_DRIFT_WORSEN_PCT;

  const reasons = [];
  if (motorValueLow) reasons.push("Motor-Index deutlich schlechter");
  if (motorEfDrop || trendEfDrop) {
    reasons.push(`GA-Leistung ≥${RUN_FLOOR_POST_ILLNESS_EF_DROP_PCT}% schlechter`);
  }
  if (trendDriftWorse || motorDriftWorse) reasons.push("Drift klar schlechter");

  return {
    shouldDown: reasons.length > 0,
    reasons,
  };
}

function computeRunfloorMode({
  dayIso,
  blockEffective,
  overrideInfo,
  reentryState,
  runfloorAdjustedInBlock,
  motor,
  trend,
}) {
  const isBaseBuildRace = ["BASE", "BUILD", "RACE"].includes(blockEffective);
  const lastOverrideCategory = reentryState?.lastOverrideCategory ?? null;
  const reentryEndDay = isIsoDate(reentryState?.reentryEndDay) ? reentryState.reentryEndDay : null;
  const reentryCheckDay = reentryEndDay ? addDaysIso(reentryEndDay, 1) : null;

  const hold = (dailyText, reason) => ({
    mode: "HOLD",
    adjustmentPct: 0,
    applyAdjustment: false,
    dailyText: dailyText || null,
    reason: reason || null,
  });

  if (!isBaseBuildRace) {
    if (blockEffective === "HOLD_LIFE") {
      return hold("Runfloor: HOLD (Urlaub)", "Life override: fehlender Reiz ≠ verlorene Anpassung.");
    }
    if (blockEffective === "HOLD_ILLNESS") {
      return hold("Runfloor: HOLD (Krankheit – Bewertung nach ReEntry)", "Illness: Bewertung nach ReEntry.");
    }
    if (blockEffective === "HOLD_INJURY") {
      const pauseDays = Number.isFinite(overrideInfo?.dayIndex) ? overrideInfo.dayIndex : null;
      if (pauseDays != null && pauseDays >= RUN_FLOOR_INJURY_PAUSE_DAYS && !runfloorAdjustedInBlock) {
        // Injury ≥14 Tage: struktureller Ausfall → -10 % Runfloor.
        return {
          mode: "DOWN_10",
          adjustmentPct: 0.1,
          applyAdjustment: true,
          dailyText: "Runfloor: −10 % (längere Verletzungspause)",
          reason: `Injury pause ≥${RUN_FLOOR_INJURY_PAUSE_DAYS} Tage → -10%.`,
        };
      }
      const note = pauseDays != null ? `Verletzung – Tag ${pauseDays}` : "Verletzung";
      return hold(`Runfloor: HOLD (${note})`, "Injury override: Runfloor bleibt stabil.");
    }
    if (blockEffective === "REENTRY") {
      const detail =
        lastOverrideCategory === "SICK"
          ? "Krankheit – Bewertung nach ReEntry"
          : lastOverrideCategory === "INJURED"
            ? "Verletzung"
            : lastOverrideCategory === "HOLIDAY"
              ? "Urlaub"
              : "ReEntry";
      return hold(`Runfloor: HOLD (${detail})`, "ReEntry: keine Progression.");
    }
    return hold(null, "BlockEffective != BASE/BUILD/RACE → HOLD.");
  }

  if (lastOverrideCategory === "SICK" && reentryCheckDay && dayIso === reentryCheckDay) {
    if (runfloorAdjustedInBlock) {
      return hold("Runfloor: HOLD (bereits gesenkt im Block)", "Kein doppeltes Senken im Block.");
    }
    const evaluation = evaluatePostIllnessPerformance({ motor, trend });
    if (evaluation.shouldDown) {
      // Nach Krankheit nur bei objektivem Leistungsabfall → -5 % Runfloor.
      return {
        mode: "DOWN_5",
        adjustmentPct: 0.05,
        applyAdjustment: true,
        dailyText: "Runfloor: −5 % (Leistungsabfall nach Krankheit)",
        reason: `Post-Illness-Evaluation: ${evaluation.reasons.join(" | ")}`,
      };
    }
    return hold("Runfloor: HOLD (Krankheit – Bewertung nach ReEntry)", "Post-Illness: keine klare Verschlechterung.");
  }

  return {
    mode: null,
    adjustmentPct: 0,
    applyAdjustment: false,
    dailyText: null,
    reason: null,
  };
}

function buildRunDailyLoads(ctx, todayISO, windowDays) {
  const end = getHistoryWindowEnd(todayISO);
  const startIso = isoDate(new Date(end.getTime() - (windowDays - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const dailyLoads = {};
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;
    dailyLoads[d] = (dailyLoads[d] || 0) + extractLoad(a);
  }

  const days = listIsoDaysInclusive(startIso, isoDate(end));
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
  const end = getHistoryWindowEnd(dayIso);
  const startIso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let runTotal7 = 0;
  let bikeTotal7 = 0;
  let runMinutes7 = 0;
  let bikeMinutes7 = 0;
  let runCount7 = 0;
  let gaRuns7 = 0;
  let longRuns7 = 0;

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
      runCount7 += 1;
      runMinutes7 += minutes;
      runTotal7 += totalLoad;
      if (!hasKeyTag(a) && seconds >= GA_MIN_SECONDS) gaRuns7 += 1;
      if (seconds >= LONGRUN_MIN_SECONDS) longRuns7 += 1;
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
    runCount7,
    gaRuns7,
    longRuns7,
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

function computeIntensityBudget(ctx, dayIso, windowDays = 7) {
  const end = getHistoryWindowEnd(dayIso);
  const startIso = isoDate(new Date(end.getTime() - (windowDays - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let steadyCount = 0;
  let keyAnyCount = 0;
  let keyHardCount = 0;
  let lastSteadyIso = null;
  let lastKeyAnyIso = null;
  let lastKeyHardIso = null;

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;

    const intensityClass = getIntensityClassForActivity(a);
    if (intensityClass === INTENSITY_CLASS.KEY_HARD) {
      keyAnyCount += 1;
      if (!lastKeyAnyIso || d > lastKeyAnyIso) lastKeyAnyIso = d;
      if (isKeyHardActivity(a)) {
        keyHardCount += 1;
        if (!lastKeyHardIso || d > lastKeyHardIso) lastKeyHardIso = d;
      }
    } else if (intensityClass === INTENSITY_CLASS.STEADY_T) {
      steadyCount += 1;
      if (!lastSteadyIso || d > lastSteadyIso) lastSteadyIso = d;
    }
  }

  return {
    windowDays,
    startIso,
    endIso,
    steadyCount,
    keyAnyCount,
    keyHardCount,
    lastSteadyIso,
    lastKeyAnyIso,
    lastKeyHardIso,
    limits: {
      steadyMax: STEADY_T_MAX_PER_7D,
      keyHardMax: KEY_HARD_MAX_PER_7D,
    },
  };
}

function computeLongRunSummary7d(ctx, dayIso) {
  const end = getHistoryWindowEnd(dayIso);
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

function extractTargetTimeFromEvent(event) {
  if (!event) return null;
  const candidates = [
    event?.target_time,
    event?.targetTime,
    event?.goal_time,
    event?.goalTime,
    event?.time_target,
    event?.details?.target_time,
    event?.details?.targetTime,
    event?.details?.goal_time,
    event?.details?.goalTime,
    event?.race?.target_time,
    event?.race?.goal_time,
    event?.race?.targetTime,
    event?.race?.goalTime,
  ];
  for (const value of candidates) {
    const parsed = parseTimeToSeconds(value);
    if (parsed) return parsed;
  }
  return null;
}

function getEventDistanceKm(distanceKey) {
  if (!distanceKey) return null;
  if (distanceKey === "5k") return 5;
  if (distanceKey === "10k") return 10;
  if (distanceKey === "hm") return 21.0975;
  if (distanceKey === "m") return 42.195;
  return null;
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
      allowedKeyTypes: ["strides"],
      preferredKeyTypes: ["strides"],
      bannedKeyTypes: ["schwelle", "racepace", "vo2_touch", "steady"],
    };
  }

  const fromMatrix = PHASE_DISTANCE_RULES?.[dist]?.[block]?.keyRules;
  if (fromMatrix) return fromMatrix;

  return {
    expectedKeysPerWeek: 0.5,
    maxKeysPerWeek: 1,
    allowedKeyTypes: ["strides"],
    preferredKeyTypes: ["strides"],
    bannedKeyTypes: ["schwelle", "racepace", "vo2_touch", "steady"],
  };
}

function collectKeyStats(ctx, dayIso, windowDays) {
  const end = getHistoryWindowEnd(dayIso);
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

// FIX: key policy source-of-truth
function normalizeKeyPolicy(keyRules, source = "blockState.keyRules") {
  const allowedKeyTypes = Array.from(new Set(keyRules?.allowedKeyTypes || []));
  const preferredKeyTypes = Array.from(new Set(keyRules?.preferredKeyTypes || []));
  const bannedKeyTypes = Array.from(new Set(keyRules?.bannedKeyTypes || []))
    .filter((t) => !allowedKeyTypes.includes(t));
  return {
    source,
    allowedKeyTypes,
    preferredKeyTypes,
    bannedKeyTypes,
  };
}

function evaluateKeyCompliance(keyRules, keyStats7, keyStats14, context = {}) {
  const policySource = context.keyPolicySource || "blockState.keyRules";
  const keyPolicy = normalizeKeyPolicy(keyRules, policySource);
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
  const bannedHits = uniqueTypes7.filter((t) => keyPolicy.bannedKeyTypes.includes(t));
  const allowedHits = uniqueTypes7.filter((t) => keyPolicy.allowedKeyTypes.includes(t));
  const preferredHits = uniqueTypes7.filter((t) => keyPolicy.preferredKeyTypes.includes(t));
  const disallowedHits = uniqueTypes7.filter((t) => !keyPolicy.allowedKeyTypes.includes(t));

  const freqOk = actual7 >= expected;
  const typeOk = bannedHits.length === 0 && disallowedHits.length === 0;
  const preferredMissing = keyPolicy.preferredKeyTypes.length > 0 && preferredHits.length === 0;

  let suggestion = "";
  const preferred = keyPolicy.preferredKeyTypes[0] || keyPolicy.allowedKeyTypes[0] || "steady";
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
    keyPolicySource: keyPolicy.source,
    allowedKeyTypes: keyPolicy.allowedKeyTypes,
    bannedKeyTypes: keyPolicy.bannedKeyTypes,
    computedDisallowed: disallowedHits,
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
  const fatigueOk = historyMetrics?.fatigue?.severity !== "high";

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
    runfloorAdjustedInBlock: state.runfloorAdjustedInBlock ?? false,
    runfloorAdjustmentMode: state.runfloorAdjustmentMode ?? null,
    runfloorAdjustmentDate: isIsoDate(state.runfloorAdjustmentDate) ? state.runfloorAdjustmentDate : null,
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
      runfloorAdjustedInBlock: parsed.runfloorAdjustedInBlock ?? false,
      runfloorAdjustmentMode: parsed.runfloorAdjustmentMode ?? null,
      runfloorAdjustmentDate: isIsoDate(parsed.runfloorAdjustmentDate) ? parsed.runfloorAdjustmentDate : null,
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

async function fetchDailyReportNote(env, dayIso) {
  const externalId = `daily-report-${dayIso}`;
  const events = await fetchIntervalsEvents(env, dayIso, dayIso);
  const existing = (events || []).find((e) => String(e?.external_id || "") === externalId);
  return existing?.description ?? null;
}

function extractRecommendationLines(noteText) {
  if (!noteText) return null;
  const lines = String(noteText).split("\n");
  const startIndex = lines.findIndex((line) => line.trim() === "🎯 HEUTIGE EMPFEHLUNG");
  if (startIndex === -1) return null;
  const section = lines.slice(startIndex, startIndex + 4);
  if (section.length < 4) return null;
  if (!section[1]?.trim().startsWith("- Empfehlung:")) return null;
  if (!section[2]?.trim().startsWith("- Begründung:")) return null;
  if (!section[3]?.trim().startsWith("- Konkret:")) return null;
  return section;
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
    keyPolicy: {
      keyPolicySource: keyCompliance?.keyPolicySource ?? "blockState.keyRules",
      allowedKeyTypes: keyCompliance?.allowedKeyTypes ?? keyRules?.allowedKeyTypes ?? [],
      bannedKeyTypes: keyCompliance?.bannedKeyTypes ?? keyRules?.bannedKeyTypes ?? [],
      computedDisallowed: keyCompliance?.computedDisallowed ?? [],
    },
    keyCompliance,
    historyMetrics,
  };
}

function addRunFloorDebug(debugOut, day, payload) {
  if (!debugOut) return;
  debugOut.__runFloor ??= {};
  debugOut.__runFloor[day] = payload;
}

function addDecisionDebug(debugOut, day, payload) {
  if (!debugOut) return;
  debugOut.__decision ??= {};
  debugOut.__decision[day] = payload;
}

function addHrr60Debug(debugOut, day, payload) {
  if (!debugOut || !payload) return;
  debugOut.__hrr60 ??= {};
  debugOut.__hrr60[day] = payload;
}

function addWorkoutDebug(debugOut, day, payload) {
  if (!debugOut) return;
  debugOut.__workout ??= {};
  debugOut.__workout[day] = payload;
}


function isoDate(d) {
  return d.toISOString().slice(0, 10);
}
function getHistoryWindowEnd(dayIso) {
  return new Date(dayIso + "T00:00:00Z");
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
function addDaysIso(dayIso, deltaDays) {
  const base = parseISODateSafe(dayIso);
  if (!base || !Number.isFinite(deltaDays)) return null;
  return isoDate(new Date(base.getTime() + deltaDays * 86400000));
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

async function readReentryState(env) {
  const state = await readKvJson(env, REENTRY_STATE_KEY);
  if (!state || typeof state !== "object") return null;
  return {
    lastOverrideDay: isIsoDate(state.lastOverrideDay) ? state.lastOverrideDay : null,
    lastOverrideCategory: state.lastOverrideCategory || null,
    reentryStartDay: isIsoDate(state.reentryStartDay) ? state.reentryStartDay : null,
    reentryEndDay: isIsoDate(state.reentryEndDay) ? state.reentryEndDay : null,
    reentryDays: Number.isFinite(state.reentryDays) ? state.reentryDays : null,
  };
}

async function writeReentryState(env, state) {
  if (!state) return;
  await writeKvJson(env, REENTRY_STATE_KEY, state);
}

function coversDay(event, dayIso) {
  const start = event?.start_date_local || event?.start_date || null;
  const end = event?.end_date_local || event?.end_date || null;
  if (!start || !end) return false;
  const dayStart = `${dayIso}T00:00:00`;
  return start <= dayStart && dayStart < end;
}

function getOverrideWindowInfo(event, dayIso) {
  const startLocal = event?.start_date_local || event?.start_date || null;
  const endLocal = event?.end_date_local || event?.end_date || null;
  const startDate = startLocal ? String(startLocal).slice(0, 10) : null;
  const endDate = endLocal ? String(endLocal).slice(0, 10) : null;
  if (!isIsoDate(startDate) || !isIsoDate(endDate)) return { dayIndex: null, totalDays: null };
  const totalDays = daysBetween(startDate, endDate);
  const dayIndex = daysBetween(startDate, dayIso) + 1;
  if (!Number.isFinite(totalDays) || totalDays <= 0) return { dayIndex: null, totalDays: null };
  if (!Number.isFinite(dayIndex) || dayIndex <= 0) return { dayIndex: null, totalDays };
  return { dayIndex, totalDays };
}

function pickOverrideEvent(events, dayIso) {
  if (!Array.isArray(events) || !isIsoDate(dayIso)) return null;
  const candidates = events.filter((event) => {
    const category = String(event?.category || "").toUpperCase();
    return OVERRIDE_CATEGORIES.includes(category) && coversDay(event, dayIso);
  });
  if (!candidates.length) return null;
  candidates.sort((a, b) => {
    const ca = String(a?.category || "").toUpperCase();
    const cb = String(b?.category || "").toUpperCase();
    return (OVERRIDE_PRIORITY[cb] || 0) - (OVERRIDE_PRIORITY[ca] || 0);
  });
  const event = candidates[0];
  const category = String(event?.category || "").toUpperCase();
  const blockEffective = OVERRIDE_TO_BLOCK[category] || null;
  const { dayIndex, totalDays } = getOverrideWindowInfo(event, dayIso);
  return {
    category,
    blockEffective,
    event,
    dayIndex,
    totalDays,
  };
}

function computeBlockEffectiveForDay({ dayIso, planBlock, overrideInfo, reentryState, reentryDays }) {
  const nextState = {
    lastOverrideDay: reentryState?.lastOverrideDay ?? null,
    lastOverrideCategory: reentryState?.lastOverrideCategory ?? null,
    reentryStartDay: reentryState?.reentryStartDay ?? null,
    reentryEndDay: reentryState?.reentryEndDay ?? null,
    reentryDays: reentryDays,
  };

  if (overrideInfo?.blockEffective) {
    nextState.lastOverrideDay = dayIso;
    nextState.lastOverrideCategory = overrideInfo.category;
    nextState.reentryStartDay = null;
    nextState.reentryEndDay = null;
    return {
      blockEffective: overrideInfo.blockEffective,
      overrideInfo,
      reentryInfo: null,
      nextState,
    };
  }

  if (!nextState.reentryStartDay && nextState.lastOverrideDay && daysBetween(nextState.lastOverrideDay, dayIso) >= 1) {
    const reentryStartDay = addDaysIso(nextState.lastOverrideDay, 1);
    const reentryEndDay = addDaysIso(reentryStartDay, (reentryDays || REENTRY_DAYS_DEFAULT) - 1);
    nextState.reentryStartDay = reentryStartDay;
    nextState.reentryEndDay = reentryEndDay;
    nextState.reentryDays = reentryDays;
  }

  const reentryActive =
    nextState.reentryStartDay &&
    nextState.reentryEndDay &&
    dayIso >= nextState.reentryStartDay &&
    dayIso <= nextState.reentryEndDay;
  if (reentryActive) {
    const dayIndex = daysBetween(nextState.reentryStartDay, dayIso) + 1;
    const totalDays = nextState.reentryDays || null;
    return {
      blockEffective: "REENTRY",
      overrideInfo: null,
      reentryInfo: { dayIndex, totalDays },
      nextState,
    };
  }

  nextState.reentryStartDay = null;
  nextState.reentryEndDay = null;
  return {
    blockEffective: planBlock,
    overrideInfo: null,
    reentryInfo: null,
    nextState,
  };
}

async function appendLearningEvent(env, event) {
  if (!hasKv(env)) return;
  if (!event || !isIsoDate(event.day)) return;
  const key = `learning:event:${event.day}`;
  await writeKvJson(env, key, {
    schema: 2,
    ...event,
  });
}

// NEW: STEADY_T learning exposure/outcome tracking
async function registerStrategyExposure(env, strategy, day) {
  if (!hasKv(env)) return;
  if (!strategy || !isIsoDate(day)) return;
  const key = `learning:exposure:${strategy}:${day}`;
  await writeKvJson(env, key, {
    schema: 1,
    strategy,
    day,
    type: "exposure",
  });
}

async function registerStrategyOutcome(env, strategy, day, payload = {}) {
  if (!hasKv(env)) return;
  if (!strategy || !isIsoDate(day)) return;
  const key = `learning:outcome:${strategy}:${day}`;
  await writeKvJson(env, key, {
    schema: 1,
    strategy,
    day,
    type: "outcome",
    ...payload,
  });
}

async function loadLearningEvents(env, endDay, lookbackDays = LEARNING_LOOKBACK_DAYS) {
  if (!hasKv(env) || !isIsoDate(endDay) || !Number.isFinite(lookbackDays) || lookbackDays <= 0) return [];
  const end = parseISODateSafe(endDay);
  if (!end) return [];
  const out = [];
  for (let i = 0; i <= lookbackDays; i++) {
    const day = isoDate(new Date(end.getTime() - i * 86400000));
    const item = await readKvJson(env, `learning:event:${day}`);
    if (item && isIsoDate(item.day)) out.push(item);
  }
  return out.sort((a, b) => String(a.day).localeCompare(String(b.day)));
}

function decayWeight(dayIso, endDayIso, decayDays = LEARNING_DECAY_DAYS) {
  const d = daysBetween(dayIso, endDayIso);
  if (!Number.isFinite(d) || d < 0) return 0;
  const tau = Number.isFinite(decayDays) && decayDays > 0 ? decayDays : LEARNING_DECAY_DAYS;
  return Math.exp(-d / tau);
}

function betaPosteriorMean(alpha, beta) {
  const a = Number(alpha) || 0;
  const b = Number(beta) || 0;
  const denom = a + b;
  if (denom <= 0) return 0.5;
  return a / denom;
}

function formatContextSummary(contextKey) {
  if (!contextKey || contextKey === "LEGACY") return "Legacy-Kontext";
  if (contextKey === LEARNING_GLOBAL_CONTEXT_KEY) return "globaler Kontext";
  const parts = String(contextKey).split("|");
  return parts.slice(0, 3).map((part) => {
    const [key, value] = part.split("=");
    if (!value) return part;
    if (key === "RFgap") return `RunFloorGap ${value === "T" ? "ja" : "nein"}`;
    if (key === "stress") return `Stress ${value}`;
    if (key === "hrv") return `HRV ${value}`;
    if (key === "drift") return `Drift ${value}`;
    if (key === "sleep") return `Schlaf ${value}`;
    if (key === "mono") return `Monotony ${value}`;
    return `${key} ${value}`;
  }).join(", ");
}

function normalizeOutcomeClass(outcomeClass, outcomeScore, outcomeGood) {
  if (outcomeClass === "GOOD" || outcomeClass === "NEUTRAL" || outcomeClass === "BAD") return outcomeClass;
  if (typeof outcomeGood === "boolean") return outcomeGood ? "GOOD" : "BAD";
  if (Number.isFinite(outcomeScore)) {
    if (outcomeScore >= 2) return "GOOD";
    if (outcomeScore === 1) return "NEUTRAL";
    return "BAD";
  }
  return "NEUTRAL";
}

function normalizeStrategyArm(strategyArm, decisionArm) {
  if (STRATEGY_ARMS.includes(strategyArm)) return strategyArm;
  if (decisionArm === "frequency") return "FREQ_UP";
  if (decisionArm === "intensity") return "INTENSITY_SHIFT";
  if (decisionArm === "neutral") return "NEUTRAL";
  return "NEUTRAL";
}

function computeLearningUtility(goodPosterior, badPosterior) {
  const pGood = Number.isFinite(goodPosterior) ? goodPosterior : 0.5;
  const pBad = Number.isFinite(badPosterior) ? badPosterior : 0.5;
  return pGood - LEARNING_UTILITY_LAMBDA * pBad;
}

function computeLearningStats(events, endDayIso) {
  const valid = Array.isArray(events) ? events.filter((e) => e && isIsoDate(e.day)) : [];
  const withWeights = valid.map((e) => ({
    ...e,
    w: decayWeight(e.day, endDayIso, LEARNING_DECAY_DAYS),
  })).filter((e) => e.w > 0);

  const byArm = {};
  for (const arm of STRATEGY_ARMS) {
    byArm[arm] = withWeights.filter((e) => normalizeStrategyArm(e.strategyArm, e.decisionArm) === arm);
  }

  const armStats = {};
  for (const arm of STRATEGY_ARMS) {
    const sample = byArm[arm];
    let goodSuccess = 0;
    let goodFail = 0;
    let badSuccess = 0;
    let badFail = 0;
    let nEff = 0;
    for (const item of sample) {
      const outcome = normalizeOutcomeClass(item.outcomeClass, item.outcomeScore, item.outcomeGood);
      const w = item.w || 0;
      nEff += w;
      if (outcome === "GOOD") {
        goodSuccess += w;
        badFail += w;
      } else if (outcome === "NEUTRAL") {
        goodFail += w;
        badFail += w;
      } else {
        goodFail += w;
        badSuccess += w;
      }
    }

    const goodPosterior = betaPosteriorMean(1 + goodSuccess, 1 + goodFail);
    const badPosterior = betaPosteriorMean(1 + badSuccess, 1 + badFail);
    const utilityMean = computeLearningUtility(goodPosterior, badPosterior);
    const confidenceArm = clamp(nEff / (nEff + LEARNING_CONFIDENCE_K), 0, 1);

    armStats[arm] = {
      nEff,
      goodPosterior,
      badPosterior,
      utilityMean,
      confidenceArm,
    };
  }

  const totalEff = sum(Object.values(armStats).map((x) => x.nEff || 0));
  const contextConfidence = clamp(totalEff / (totalEff + LEARNING_CONFIDENCE_K), 0, 1);
  const armsWithData = STRATEGY_ARMS.filter((arm) => (armStats[arm]?.nEff || 0) >= LEARNING_MIN_ARM_NEFF);
  const rankingPool = armsWithData.length > 0 ? armsWithData : LEARNING_SAFE_FALLBACK_ARMS.slice();
  const sortedByUtility = rankingPool.slice().sort((a, b) => armStats[b].utilityMean - armStats[a].utilityMean);
  const bestArm = sortedByUtility[0] || "NEUTRAL";
  const secondArm = sortedByUtility[1] || bestArm;
  const utilityDiff = Math.abs(armStats[bestArm].utilityMean - armStats[secondArm].utilityMean);
  const nArmsWithData = armsWithData.length;
  const explorationNeed = contextConfidence < LEARNING_MIN_CONF
    || nArmsWithData < 2
    || utilityDiff < LEARNING_UTILITY_EPS;

  return {
    totalEff,
    contextConfidence,
    armStats,
    rankingPool,
    armsWithData,
    nArmsWithData,
    bestArm,
    secondArm,
    utilityDiff,
    explorationNeed,
  };
}

function chooseLearningRecommendation(stats, contextKey, contextSummary) {
  const explorationNeed = stats.explorationNeed ?? true;
  const recommended = stats.bestArm || "NEUTRAL";
  const nEffTotal = stats.totalEff ?? 0;
  const nEffArm = stats.armStats?.[recommended]?.nEff ?? 0;
  const confidenceContext = stats.contextConfidence ?? 0;
  const confidenceArm = stats.armStats?.[recommended]?.confidenceArm ?? 0;
  const isGlobal = contextKey === LEARNING_GLOBAL_CONTEXT_KEY;
  const exploreUntried = LEARNING_EXPLORE_UNTRIED;
  const secondArm = stats.secondArm || recommended;
  const utilityDiff = stats.utilityDiff ?? null;
  let recommendationMode = explorationNeed ? "CONSERVATIVE" : "EXPLOIT";
  let chosenArm = recommended;
  let chosenEff = nEffArm;
  let chosenConfidence = confidenceArm;

  if (explorationNeed && exploreUntried) {
    const untried = stats.rankingPool?.filter((arm) => (stats.armStats?.[arm]?.nEff || 0) <= 0) || [];
    const safeUntried = untried.filter((arm) => LEARNING_SAFE_FALLBACK_ARMS.includes(arm));
    const exploreArm = safeUntried[0];
    if (exploreArm) {
      chosenArm = exploreArm;
      chosenEff = stats.armStats?.[exploreArm]?.nEff ?? 0;
      chosenConfidence = stats.armStats?.[exploreArm]?.confidenceArm ?? 0;
      recommendationMode = "EXPLORATION";
    }
  }

  return {
    strategyArm: chosenArm,
    confidenceContext,
    confidenceArm: chosenConfidence,
    explorationNeed,
    recommendationMode,
    exploreUntried,
    nEffTotal,
    nEffArm: chosenEff,
    secondArm,
    utilityDiff,
    contextKey,
    contextSummary,
    globalFallback: isGlobal,
  };
}

function zScoreForConfidence(level) {
  if (level >= 0.99) return 2.576;
  if (level >= 0.95) return 1.96;
  if (level >= 0.90) return 1.645;
  return 1.96;
}

function wilsonInterval(success, total, confidence = 0.95) {
  const n = Number(total) || 0;
  const k = Number(success) || 0;
  if (n <= 0) return { low: 0, high: 1 };
  const z = zScoreForConfidence(confidence);
  const phat = k / n;
  const denom = 1 + (z * z) / n;
  const centre = phat + (z * z) / (2 * n);
  const margin = z * Math.sqrt((phat * (1 - phat) + (z * z) / (4 * n)) / n);
  return {
    low: Math.max(0, (centre - margin) / denom),
    high: Math.min(1, (centre + margin) / denom),
  };
}

function weightedAverage(values, weights) {
  if (!Array.isArray(values) || !Array.isArray(weights) || values.length !== weights.length) return null;
  let num = 0;
  let den = 0;
  for (let i = 0; i < values.length; i++) {
    const v = Number(values[i]);
    const w = Number(weights[i]);
    if (!Number.isFinite(v) || !Number.isFinite(w) || w <= 0) continue;
    num += v * w;
    den += w;
  }
  return den > 0 ? num / den : null;
}

function computeLearningEvidence(events, endDayIso, contextKey) {
  const valid = Array.isArray(events) ? events.filter((e) => e && isIsoDate(e.day)) : [];
  const redFlagCount = valid.filter((e) => e.learningEligible === false).length;
  const eligible = valid.filter((e) => e.learningEligible !== false);
  const contextEvents = eligible.filter((e) => (e.contextKey || "LEGACY") === contextKey);
  const stats = computeLearningStats(contextEvents, endDayIso);
  const useGlobal = stats.totalEff < LEARNING_MIN_NEFF && contextKey !== LEARNING_GLOBAL_CONTEXT_KEY;
  const fallbackEvents = useGlobal ? eligible : contextEvents;
  const fallbackStats = useGlobal ? computeLearningStats(fallbackEvents, endDayIso) : stats;
  const finalContextKey = useGlobal ? LEARNING_GLOBAL_CONTEXT_KEY : contextKey;
  const contextSummary = formatContextSummary(finalContextKey);
  const recommendation = chooseLearningRecommendation(fallbackStats, finalContextKey, contextSummary);

  return {
    lookbackDays: LEARNING_LOOKBACK_DAYS,
    decayDays: LEARNING_DECAY_DAYS,
    sampleCount: eligible.length,
    effectiveSamples: fallbackStats.totalEff,
    contextKey: finalContextKey,
    contextSummary,
    globalFallback: useGlobal,
    redFlagCount,
    arms: fallbackStats.armStats,
    recommendation,
  };
}

function buildLearningNarrativeState(evidence) {
  const recommendation = evidence?.recommendation;
  const nEffRec = Number.isFinite(recommendation?.nEffArm) ? recommendation.nEffArm : 0;
  const nEffTotal = Number.isFinite(recommendation?.nEffTotal) ? recommendation.nEffTotal : 0;
  const confidenceRec = Number.isFinite(recommendation?.confidenceArm) ? recommendation.confidenceArm : 0;
  const confidenceContext = Number.isFinite(recommendation?.confidenceContext) ? recommendation.confidenceContext : 0;
  const baseExplorationNeed = recommendation?.explorationNeed ?? true;
  const isGlobalFallback = recommendation?.globalFallback ?? false;
  const recommendedArm = recommendation?.strategyArm || "NEUTRAL";
  const contextSummary = recommendation?.contextSummary || evidence?.contextSummary || "aktueller Kontext";
  const contextKey = recommendation?.contextKey || evidence?.contextKey || "LEGACY";
  const arms = evidence?.arms || {};
  const nEffByArm = {};
  for (const arm of STRATEGY_ARMS) {
    nEffByArm[arm] = Number.isFinite(arms[arm]?.nEff) ? arms[arm].nEff : 0;
  }
  const rankedArms = STRATEGY_ARMS
    .filter((arm) => nEffByArm[arm] >= LEARNING_TEXT_MIN_ARM_NEFF)
    .slice()
    .sort((a, b) => (arms[b]?.utilityMean ?? -Infinity) - (arms[a]?.utilityMean ?? -Infinity));
  const secondArm = rankedArms.find((arm) => arm !== recommendedArm) || recommendedArm;
  const nEffSecond = Number.isFinite(nEffByArm[secondArm]) ? nEffByArm[secondArm] : 0;
  const nArmsWithData = rankedArms.length;

  const explorationNeed = baseExplorationNeed && (nEffRec < LEARNING_TEXT_MIN_ARM_NEFF || confidenceRec < 0.4);

  return {
    contextKey,
    contextSummary,
    isGlobalFallback,
    recommendedArm,
    secondArm,
    nEffTotal,
    nEffByArm,
    nArmsWithData,
    nEffRec,
    nEffSecond,
    confidenceRec,
    confidenceContext,
    explorationNeed,
  };
}

function learningEvidenceLabel(isGlobal) {
  return isGlobal ? "Evidenz (global)" : "Evidenz (Kontext)";
}

function formatLearningEvidenceLines(evidence, narrativeState) {
  const arms = evidence?.arms || {};
  const armLines = STRATEGY_ARMS.map((arm) => {
    const label = STRATEGY_LABELS[arm] || arm;
    const nEff = Number.isFinite(arms[arm]?.nEff) ? arms[arm].nEff : 0;
    return `- ${label}: ${formatNeff(nEff)} Beobachtungen`;
  });

  const confArmPct = formatPct(narrativeState.confidenceRec);
  const confContextPct = formatPct(narrativeState.confidenceContext);
  const confidenceLine = `- Evidence-Confidence: Kontext ${confContextPct} | Arm ${confArmPct}`;

  return [learningEvidenceLabel(narrativeState.isGlobalFallback), ...armLines, confidenceLine];
}

function formatPct(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0%";
  return `${Math.round(n * 100)}%`;
}

function formatNeff(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "0.0";
  return n.toFixed(1);
}

function sanitizeLearningText(text) {
  let out = String(text || "");
  out = out.replace(/NaN|undefined|null/g, "");
  out = out.replace(/[ \t]{2,}/g, " ");
  out = out.replace(/\s+\n/g, "\n").replace(/\n\s+/g, "\n");
  out = out.replace(/\s+([.,;:])/g, "$1");
  const suffix = "(basierend auf globalen Daten)";
  const firstIndex = out.indexOf(suffix);
  if (firstIndex >= 0) {
    out = out.slice(0, firstIndex + suffix.length) + out.slice(firstIndex + suffix.length).replaceAll(suffix, "");
  }
  return out.trim();
}

function getClaimsLevel(state) {
  if (state.explorationNeed || state.nEffRec < LEARNING_TEXT_MIN_ARM_NEFF || state.confidenceRec < 0.4) return 0;
  const hasCompareArms = state.nArmsWithData >= LEARNING_TEXT_MIN_COMPARE_ARMS
    && state.nEffRec >= LEARNING_TEXT_MIN_COMPARE_NEFF
    && state.nEffSecond >= LEARNING_TEXT_MIN_COMPARE_NEFF;
  if (!hasCompareArms) return 1;
  const level = state.confidenceContext >= LEARNING_TEXT_MIN_CONF_FOR_STRONG ? 3 : 2;
  return state.recommendedArm === "NEUTRAL" ? Math.min(level, 1) : level;
}

function applyTextGate(text, state) {
  const claimsLevel = getClaimsLevel(state);
  const isNeutral = state.recommendedArm === "NEUTRAL";
  const blocklist = [
    "robuster als",
    "besser als",
    "als Intensität",
    "als die Alternativen",
    "reagierst robuster",
    "robuster reagierst",
  ];
  const neutralBlocklist = ["robust", "bewährt", "stabiler"];
  let out = String(text || "");

  if (claimsLevel <= 1) {
    const hasBlocked = blocklist.some((phrase) => out.toLowerCase().includes(phrase));
    if (hasBlocked) {
      if (state.policyReason === "RUN_FLOOR_GAP_HIGH_STRESS") {
        out =
          "Wir entscheiden uns heute für Häufigkeit statt Tempo, weil dein Runload unter dem Ziel liegt und der Gesamtstress erhöht ist – so schließen wir die Lücke kontrolliert, ohne zusätzliche Intensität zu erzwingen.";
      } else {
        out =
          "Wir priorisieren heute Häufigkeit und Kontrolle, weil das in dieser Situation die sicherste und plan-stabile Option ist.";
      }
    }
  }

  if (claimsLevel <= 1) {
    for (const phrase of blocklist) {
      const rx = new RegExp(phrase, "gi");
      out = out.replace(rx, "");
    }
  }

  if (isNeutral) {
    for (const phrase of neutralBlocklist) {
      const rx = new RegExp(phrase, "gi");
      out = out.replace(rx, "");
    }
  }

  return sanitizeLearningText(out);
}

function getLearningText(state) {
  const armLabel = STRATEGY_LABELS[state.recommendedArm] || state.recommendedArm || "keine Anpassung";
  const secondArmLabel = STRATEGY_LABELS[state.secondArm] || state.secondArm || "Alternative";
  const contextText = state.contextSummary || "aktueller Kontext";
  const suffix = state.isGlobalFallback || state.contextKey === LEARNING_GLOBAL_CONTEXT_KEY
    ? " (basierend auf globalen Daten)"
    : "";
  const isNeutral = state.recommendedArm === "NEUTRAL";
  let level = getClaimsLevel(state);
  if (isNeutral) level = Math.min(level, 1);
  let text = "";

  if (isNeutral) {
    text = `Learning heute:\nWir halten die Strategie im (${contextText}) konservativ.\nEs gibt heute keinen klaren Grund umzusteuern, daher stabilisieren wir und beobachten weiter.`;
  } else if (level === 0) {
    text = `Learning heute:\nWir haben im (${contextText}) noch zu wenig Vergleich, um sicher umzuschalten.\nWir bleiben vorerst bei ${armLabel} und sammeln weitere Beobachtungen.`;
  } else if (level === 1) {
    text = `Learning heute:\n${armLabel} wirkt im (${contextText}) bislang stabil.\nFür einen fairen Vergleich fehlen noch Tests der Alternativen.`;
  } else if (level === 2) {
    text = `Learning heute:\nIm (${contextText}) spricht aktuell mehr für ${armLabel} als für ${secondArmLabel}.\nDer Vergleich ist noch vorläufig, deshalb bleiben wir vorsichtig.`;
  } else {
    text = `Learning heute:\nIm (${contextText}) war ${armLabel} robuster als ${secondArmLabel}.\nDas ist unser Grund, die Empfehlung beizubehalten.`;
  }

  return applyTextGate(`${text}${suffix}`, state);
}

function buildLearningNarrative(payload) {
  const normalized = payload?.evidence ? payload : { evidence: payload };
  const { evidence, confirmedRules = [], proposedRules = [] } = normalized || {};
  const narrativeState = buildLearningNarrativeState(evidence);
  const confirmedText = confirmedRules.length ? confirmedRules.slice(0, 2).join(" | ") : "Noch keine bestätigte Regel.";
  const proposedText = proposedRules.length ? proposedRules.slice(0, 2).join(" | ") : "Aktuell keine neue Hypothese.";
  const evidenceLines = formatLearningEvidenceLines(evidence, narrativeState);
  const learningToday = getLearningText(narrativeState);

  return [
    "1) 🧭 Ich-Regeln (Confirmed)",
    confirmedText,
    "",
    "2) 🔬 Beobachtung / Test (Proposed)",
    proposedText,
    "",
    "3) 📊 Evidenz (deskriptiv)",
    ...evidenceLines,
    "",
    "4) 🧠 Learning heute (menschlich & ehrlich)",
    learningToday,
  ].join("\n");
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
  const learningEvents = await loadLearningEvents(env, newest, LEARNING_LOOKBACK_DAYS);
  const reentryDays = getReentryDays(env);
  let reentryState = (await readReentryState(env)) || {
    lastOverrideDay: null,
    lastOverrideCategory: null,
    reentryStartDay: null,
    reentryEndDay: null,
    reentryDays,
  };
  let overrideEvents = [];
  try {
    const overrideOldest = addDaysIso(oldest, -30) || oldest;
    const overrideNewest = addDaysIso(newest, 1) || newest;
    overrideEvents = await fetchOverrideEvents(env, overrideOldest, overrideNewest);
  } catch (e) {
    overrideEvents = [];
    if (debug) {
      ctx.debugOut = ctx.debugOut || {};
      addDebug(ctx.debugOut, "override-events", null, "warn:fetch_failed", {
        message: String(e?.message ?? e),
      });
    }
  }

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
      // NEW: STEADY_T intensity model
      const intensityProfile = classifyIntensity(a);
      const isKey = intensityProfile.isKey;
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
            activity: a,
          });
        } catch {
          intervalMetrics = null;
        }
      }

      const intensityClass = intensityProfile.intensityClass;
      perRunInfo.push({
        activityId: a.id,
        type: a.type,
        tags: a.tags ?? [],
        ga,
        isKey,
        intensityClass,
        // NEW: STEADY_T excluded from diagnostic trends
        excludeFromTrends: !shouldIncludeInTrends(intensityClass),
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
          intensityClass,
          ef,
          drift,
          drift_raw,
          drift_source,
          load,
          intervalMetrics,
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
    let latestGaSample = null;
    try {
      latestGaSample = await getLatestGaSample(ctx, day, TREND_WINDOW_DAYS * 2);
    } catch {
      latestGaSample = null;
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
    const lastKeyInfo = computeLastKeyInfo(ctx, day, 14);
    let lastKeyIntervalInsights = null;
    try {
      lastKeyIntervalInsights = await computeLastKeyIntervalInsights(ctx, day, 21);
    } catch {
      lastKeyIntervalInsights = null;
    }
    let ga21Context = null;
    try {
      ga21Context = await computeGa21DayContext(ctx, day);
    } catch {
      ga21Context = null;
    }
    let intervalContext = null;
    try {
      intervalContext = await computeIntervalContext(ctx, day, perRunInfo);
    } catch {
      intervalContext = null;
    }
    const baseBlock =
      previousBlockState?.block ||
      (weeksToEvent != null && weeksToEvent <= BLOCK_CONFIG.cutoffs.raceStartWeeks ? "BUILD" : "BASE");
    const keyRulesPre = getKeyRules(baseBlock, eventDistance, weeksToEvent);
    const keyCompliancePre = evaluateKeyCompliance(keyRulesPre, keyStats7, keyStats14, {
      block: baseBlock,
      eventDistance,
      keyPolicySource: "baseBlock.keyRules",
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

    const overrideInfo = pickOverrideEvent(overrideEvents, day);
    const blockEffectiveResult = computeBlockEffectiveForDay({
      dayIso: day,
      planBlock: blockState.block,
      overrideInfo,
      reentryState,
      reentryDays,
    });
    patch[FIELD_BLOCK_EFFECTIVE] = blockEffectiveResult.blockEffective;

    const isNewBlock = blockState.startDate && blockState.startDate !== previousBlockState?.startDate;
    const runfloorAdjustedInBlock = isNewBlock
      ? false
      : previousBlockState?.runfloorAdjustedInBlock ?? false;
    const runfloorModeInfo = computeRunfloorMode({
      dayIso: day,
      blockEffective: blockEffectiveResult.blockEffective,
      overrideInfo: blockEffectiveResult.overrideInfo,
      reentryState,
      runfloorAdjustedInBlock,
      motor,
      trend,
    });

    const phase = mapBlockToPhase(blockState.block);
    const eventInDays = eventDate ? daysBetween(day, eventDate) : null;
    const dailyRunLoads = buildRunDailyLoads(ctx, day, RUN_FLOOR_DELOAD_WINDOW_DAYS);
    const runfloorAdjustedTarget = runfloorModeInfo.applyAdjustment
      ? Math.max(1, Math.round(baseRunFloorTarget * (1 - runfloorModeInfo.adjustmentPct)))
      : baseRunFloorTarget;
    const didAdjustRunfloor = runfloorModeInfo.applyAdjustment && runfloorAdjustedTarget !== baseRunFloorTarget;
    const runFloorState = evaluateRunFloorState({
      todayISO: day,
      floorTarget: runfloorAdjustedTarget,
      phase,
      eventInDays,
      eventDateISO: eventDate || null,
      previousState: previousBlockState,
      dailyRunLoads,
    });
    runFloorState.runfloorMode = runfloorModeInfo.mode;
    runFloorState.runfloorModeText = runfloorModeInfo.dailyText;
    runFloorState.runfloorModeReason = runfloorModeInfo.reason;

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
      reasons: ["Statischer Key-Cap"],
    };

    let fatigue = fatigueBase;
    try {
      fatigue = await computeFatigue7d(ctx, day, { maxKeys7d: MAX_KEYS_7D });
    } catch {
      fatigue = fatigueBase;
    }
    historyMetrics.fatigueCap = fatigue;

    const keyRulesBase = getKeyRules(blockState.block, eventDistance, blockState.weeksToEvent);
    const keyRules = {
      ...keyRulesBase,
      maxKeysPerWeek: Math.min(keyRulesBase.maxKeysPerWeek, MAX_KEYS_7D),
    };
    const keyCompliance = evaluateKeyCompliance(keyRules, keyStats7, keyStats14, {
      block: blockState.block,
      eventDistance,
      maxKeys7d: MAX_KEYS_7D,
      keySpacing,
      keyPolicySource: "blockState.keyRules",
    });
    historyMetrics.keyCompliance = keyCompliance;

    patch[FIELD_BLOCK] = blockState.block;
    reentryState = blockEffectiveResult.nextState;
    if (write) {
      await writeReentryState(env, reentryState);
    }
    blockState.runfloorAdjustedInBlock = runfloorAdjustedInBlock || didAdjustRunfloor;
    blockState.runfloorAdjustmentMode = didAdjustRunfloor
      ? runfloorModeInfo.mode
      : previousBlockState?.runfloorAdjustmentMode ?? null;
    blockState.runfloorAdjustmentDate = didAdjustRunfloor
      ? day
      : previousBlockState?.runfloorAdjustmentDate ?? null;
    previousBlockState = {
      block: blockState.block,
      wave: blockState.wave,
      startDate: blockState.startDate || day,
      eventDate,
      eventDistance,
      floorTarget: blockState.floorTarget,
      runfloorAdjustedInBlock: blockState.runfloorAdjustedInBlock,
      runfloorAdjustmentMode: blockState.runfloorAdjustmentMode,
      runfloorAdjustmentDate: blockState.runfloorAdjustmentDate,
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
      runfloorMode: runFloorState.runfloorMode,
      runfloorModeText: runFloorState.runfloorModeText,
      runfloorModeReason: runFloorState.runfloorModeReason,
    });

    const maintenance14d = computeMaintenance14d(ctx, day);
    const pastLearningEvents = learningEvents.filter((e) => String(e.day) < day);
    const hrvDeltaPct = Number.isFinite(recoverySignals?.hrvDeltaPct) ? recoverySignals.hrvDeltaPct : null;
    const ydayHrvDeltaPct = Number.isFinite(recoverySignals?.ydayHrvDeltaPct) ? recoverySignals.ydayHrvDeltaPct : null;
    const hrv1dNegative = hrvDeltaPct != null && hrvDeltaPct <= HRV_NEGATIVE_THRESHOLD_PCT;
    const hrv2dNegative = hrv1dNegative && ydayHrvDeltaPct != null && ydayHrvDeltaPct <= HRV_NEGATIVE_THRESHOLD_PCT;
    const subjectiveAvgNegative = recoverySignals?.subjectiveAvgNegative ?? null;
    const subjectiveNegative = recoverySignals?.subjectiveNegative ?? false;
    const hrv1dConcern = hrv1dNegative && (subjectiveAvgNegative == null || subjectiveAvgNegative >= 0.5);
    const hrv2dConcern = hrv2dNegative && (subjectiveAvgNegative == null || subjectiveAvgNegative >= 0.5);

    const repGARun = pickRepresentativeGARun(perRunInfo);
    const repDrift = Number.isFinite(repGARun?.drift) ? repGARun.drift : null;
    const driftSignalForLearning =
      repDrift == null
        ? "unknown"
        : repDrift >= DRIFT_CRITICAL_PCT
          ? "red"
          : repDrift >= DRIFT_WARN_PCT
            ? "orange"
            : "green";
    const runLoad7 = Math.round(loads7?.runTotal7 ?? 0);
    const runTarget = Math.round(runFloorState?.effectiveFloorTarget ?? 0);
    const runFloorGap = runTarget > 0 && runLoad7 < runTarget;
    const freqCount14 = maintenance14d?.runCount14 ?? null;
    const freqSignal =
      freqCount14 == null ? "unknown" : freqCount14 > 12 ? "red" : (freqCount14 < 7 || freqCount14 > 11 ? "orange" : "green");
    const warningSignals = [
      driftSignalForLearning === "orange" || driftSignalForLearning === "red",
      hrv1dConcern,
      freqSignal === "orange" || freqSignal === "red",
      !!recoverySignals?.sleepLow,
      !!fatigue?.override,
      subjectiveNegative,
    ];
    const warningCount = warningSignals.filter(Boolean).length;
    const hasHardRedFlag = hrv2dConcern || (warningCount >= 2 && subjectiveNegative) || !!recoverySignals?.painInjury;
    const intensityBudget = computeIntensityBudget(ctx, day, 7);
    const driftRecentMedian = Number.isFinite(trend?.driftRecentMed) ? trend.driftRecentMed : null;
    const driftTrendWorsening = Number.isFinite(trend?.dd) ? trend.dd > DRIFT_TREND_WORSENING_PCT : false;
    const motorTrendDownStrong = Number.isFinite(motor?.dv) ? motor.dv < -1.5 : false;
    const fatigueHigh = fatigue?.severity === "high" || !!fatigue?.override;
    const loadState = fatigueHigh ? "overreached" : "ok";
    const guardrailState = buildGuardrailState({
      hasHardRedFlag,
      hrv2dNegative: hrv2dConcern,
      warningCount,
      loadState,
      painInjury: !!recoverySignals?.painInjury,
      keySpacingOk: keySpacing?.ok,
      fatigueGuardrailSeverity: fatigue?.guardrailSeverity ?? "none",
      fatigueGuardrailReasons: fatigue?.guardrailReasons ?? [],
    });
    const hrvNegativeDays = hrvDeltaPct == null ? null : hrv2dConcern ? 2 : hrv1dConcern ? 1 : 0;
    const hadKey = perRunInfo.some((x) => !!x.isKey);
    const decisionConfidence = computeReadinessConfidence({
      driftSignal: driftSignalForLearning,
      hrvDeltaPct,
      runLoad7,
      fatigueOverride: !!fatigue?.override,
      hadKey,
      counterIndicator: !hadKey && driftSignalForLearning === "green",
      hrv1dNegative: hrv1dConcern,
      hrv2dNegative: hrv2dConcern,
      trend,
    });
    const steadyDecision = computeBuildSteadyDecision({
      phase: blockState.block,
      guardrailHardActive: guardrailState.hardActive,
      guardrailSoftActive: guardrailState.mediumActive,
      hrvNegativeDays,
      hrv2dNegative: hrv2dConcern,
      driftRecentMedian,
      driftTrendWorsening,
      loadState,
      fatigueHigh,
      motorTrendDownStrong,
      intensityBudget,
      decisionConfidenceScore: decisionConfidence.score,
    });
    const keyHardDecision = computeKeyHardDecision({
      guardrailHardActive: guardrailState.hardActive,
      guardrailMediumActive: guardrailState.mediumActive,
      hrv2dNegative: hrv2dConcern,
      loadState,
      keySpacingOk: keySpacing?.ok,
      keyCompliance,
      intensityBudget,
    });
    const overruledSignals = [];
    if (steadyDecision?.delaySteady && steadyDecision?.allowConditionsMet) {
      overruledSignals.push("BUILD_STEADY_ALLOWED");
    }
    if (guardrailState.hardActive) overruledSignals.push("HARD_GUARDRAIL");
    const intensitySelection = selectIntensityRecommendation({
      blockState,
      weeksToEvent,
      guardrailState,
      keyRules,
      keyCompliance,
      intensityBudget,
      keySpacing,
      steadyDecision,
      runFloorGap,
    });
    const intensityClassActual = deriveDailyIntensityClass(perRunInfo);
    const intensityClassToday = intensityClassActual ?? intensitySelection?.intensityClass ?? null;
    const excludeFromTrends = {
      motorTrend: intensityClassActual === INTENSITY_CLASS.STEADY_T,
      vdotTrend: intensityClassActual === INTENSITY_CLASS.STEADY_T,
      efDriftTrend: intensityClassActual === INTENSITY_CLASS.STEADY_T,
    };
    const decisionTrace = buildDecisionTrace({
      steadyDecision,
      guardrailState,
      overruledSignals,
      intensityClassToday,
      intensityBudget,
      excludeFromTrends,
      intensitySelection,
    });
    addDecisionDebug(ctx.debugOut, day, buildIntensityDebugPayload({
      intensityClassToday,
      intensityBudget,
      steadyDecision,
      keyHardDecision,
      decisionTrace,
      guardrailState,
      excludeFromTrends,
      intensitySelection,
    }));
    const lifeStress = deriveStressBucket({ fatigueOverride: !!fatigue?.override, warningCount });
    const hrvState = deriveHrvBucket(hrvDeltaPct);
    const driftState = deriveDriftBucket(driftSignalForLearning);
    const highMonotony = deriveMonotonyBucket(fatigue?.monotony) === "HIGH";
    const freqNotRed = freqSignal !== "red";
    const strategyDecision = deriveStrategyArm({
      runFloorGap,
      lifeStress,
      hrvState,
      driftState,
      hadKey,
      freqNotRed,
      highMonotony,
      fatigueHigh: !!fatigue?.override,
      hasHardRedFlag,
    });
    const outcomeScore = computeLearningOutcomeScore({
      driftSignal: driftSignalForLearning,
      hrv1dNegative,
      hrv2dNegative,
      fatigueOverride: !!fatigue?.override,
      warningCount,
    });
    const outcomeClass =
      outcomeScore >= 2 ? "GOOD" : outcomeScore === 1 ? "NEUTRAL" : "BAD";
    const contextKey = deriveContextKey({
      runFloorGap,
      fatigueOverride: !!fatigue?.override,
      warningCount,
      hrvDeltaPct,
      driftSignal: driftSignalForLearning,
      recoverySignals,
      monotony: fatigue?.monotony,
    });
    const learningEvidence = computeLearningEvidence(pastLearningEvents, day, contextKey);
    const learningNarrativeState = buildLearningNarrativeState(learningEvidence);
    const learningEvent = {
      day,
      decisionArm: strategyDecision.strategyArm === "FREQ_UP" ? "frequency" : strategyDecision.strategyArm === "INTENSITY_SHIFT" ? "intensity" : "neutral",
      strategyArm: strategyDecision.strategyArm,
      policyReason: strategyDecision.policyReason,
      contextKey,
      runFloorGap,
      outcomeScore,
      outcomeGood: outcomeScore >= LEARNING_GOOD_OUTCOME_THRESHOLD,
      outcomeClass,
      learningEligible: strategyDecision.learningEligible,
      warningCount,
      signalsSnapshot: {
        runFloorGap,
        lifeStress,
        hrvState,
        driftState,
        sleepState: deriveSleepBucket(recoverySignals),
        monotonyState: deriveMonotonyBucket(fatigue?.monotony),
        freqSignal,
        warningCount,
        hasHardRedFlag,
      },
      outcomeVector: {
        hrvDeltaBucket: hrvState,
        driftDeltaBucket: driftState,
        fatigueBucket: fatigue?.override ? "HIGH" : "OK",
        adherenceBucket: freqSignal === "red" ? "LOW" : freqSignal === "orange" ? "MED" : "OK",
      },
      context: {
        hadKey,
        hadGA: perRunInfo.some((x) => !!x.ga && !x.isKey),
        fatigueOverride: !!fatigue?.override,
      },
    };
    learningEvents.push(learningEvent);

    let gaComparableStats = null;
    try {
      const samples = await gatherGASamples(ctx, day, MOTOR_WINDOW_DAYS, { comparable: true, needCv: true });
      if (samples?.length) {
        gaComparableStats = {
          n: samples.length,
          efMed: median(samples.map((x) => x.ef)),
          driftMed: median(samples.map((x) => x.drift)),
        };
      }
    } catch {
      gaComparableStats = null;
    }

    // Daily report text ALWAYS (includes min stimulus ALWAYS)
    const commentBundle = buildComments({
      perRunInfo,
      trend,
      motor,
      robustness,
      modeInfo,
      blockState,
      blockEffective: blockEffectiveResult.blockEffective,
      overrideInfo: blockEffectiveResult.overrideInfo,
      reentryInfo: blockEffectiveResult.reentryInfo,
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
      learningEvidence,
      learningNarrativeState,
      strategyDecision,
      intensityBudget,
      steadyDecision,
      keyHardDecision,
      decisionTrace,
      guardrailState,
      intensityClassToday,
      intensitySelection,
      readinessConfidence: decisionConfidence,
      latestGaSample,
      ga21Context,
      lastKeyInfo,
      lastKeyIntervalInsights,
      intervalContext,
      gaComparableStats,
    }, { debug });

    const dailyReportText = commentBundle.dailyReportText;

    patches[day] = patch;

    if (debug && commentBundle.workoutDebug) {
      addWorkoutDebug(ctx.debugOut, day, commentBundle.workoutDebug);
    }
    if (debug && commentBundle.hrr60Readiness) {
      addHrr60Debug(ctx.debugOut, day, commentBundle.hrr60Readiness);
    }

    if (write) {
      // NEW: STEADY_T learning exposure/outcome (no circular "0 observations")
      if (steadyDecision?.allowSteady) {
        await registerStrategyExposure(env, "STEADY_T", day);
      }
      if (intensityClassToday === INTENSITY_CLASS.STEADY_T) {
        await registerStrategyOutcome(env, "STEADY_T", day, { performed: true });
      }
      await appendLearningEvent(env, learningEvent);
      await upsertDailyReportNote(env, day, dailyReportText);
    }
    if (debug) notesPreview[`${day}:daily`] = dailyReportText;

    // Monday detective NOTE (calendar) – always on Mondays, even if no run
    if (isMondayIso(day)) {
      let detectiveNoteText = null;
      try {
        const detectiveNote = await computeDetectiveNoteAdaptive(env, day, ctx.warmupSkipSec);
        const sections = [];
        const detectiveSections = detectiveNote?.sections ?? null;
        const weeklySections = commentBundle.weeklyReportSections ?? null;
        if (detectiveSections?.title) sections.push(detectiveSections.title);
        if (weeklySections?.blockStatus?.length) sections.push(...weeklySections.blockStatus);
        if (weeklySections?.weeklyVerdict?.length) sections.push(...weeklySections.weeklyVerdict);
        if (detectiveSections?.loadBasis?.length) sections.push(...detectiveSections.loadBasis);
        if (weeklySections?.learnings?.length) sections.push(...weeklySections.learnings);
        if (weeklySections?.decision?.length) sections.push(...weeklySections.decision);
        if (weeklySections?.risk?.length) sections.push(...weeklySections.risk);
        detectiveNoteText = sections.filter(Boolean).join("\n\n");
        if (!detectiveNoteText && commentBundle.weeklyReportLines?.length) {
          detectiveNoteText = commentBundle.weeklyReportLines.filter(Boolean).join("\n\n");
        }
        if (!detectiveNoteText) detectiveNoteText = detectiveNote?.text ?? "";
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

function pickRepresentativeIntervalRun(perRunInfo) {
  return perRunInfo.find((x) => x.isKey && x.intervalMetrics) || null;
}

async function getLatestGaSample(ctx, endIso, windowDays) {
  const samples = await gatherGASamples(ctx, endIso, windowDays, { comparable: false });
  if (!samples.length) return null;
  const latest = samples.slice().sort((a, b) => a.date.localeCompare(b.date)).at(-1);
  return latest || null;
}

function formatEventDistance(dist) {
  if (!dist) return "n/a";
  if (dist === "5k") return "5 km";
  if (dist === "10k") return "10 km";
  if (dist === "hm") return "HM";
  if (dist === "m") return "Marathon";
  return String(dist);
}

const BLOCK_DESCRIPTION_LIBRARY = {
  "5k": {
    distanceLabel: "5 KM",
    blocks: {
      BASE: {
        title: "BASE – 5 KM",
        principle: "Belastbar werden, nicht müde.",
        goal: ["Aerobe Basis", "Robustheit", "Keine Ermüdung anhäufen"],
        content: ["GA1 45′/60′/75′", "Langer Lauf 75–100′ locker", "Strides 6–8×15–20″", "VO2-Impulse nur limitiert: 8×20″/40″", "Hügel kurz: 6–8×8–10″", "Kraft/Stabi 2×/Woche"],
        week: ["60′ GA1 locker", "6×20″ Strides (volle Erholung)", "45′ locker + Lauf-ABC", "8×20″/40″ VO2-Impuls (optional)", "80–90′ langer Lauf locker"],
      },
      BUILD: {
        title: "BUILD – 5 KM",
        principle: "Spezifisch, aber kontrolliert.",
        goal: ["Schwelle + VO2 entwickeln", "Tempohärte formen"],
        content: ["Schwelle kurz: 4×6′ / 5×5′", "VO2max: 6×600 m / 5×1000 m", "Tempohärte: 10×400 m", "Langer Lauf ~90′ locker", "Ergänzend: Strides + Kraft"],
        week: ["4×6′ @ Schwelle", "45′ locker", "6×600 m @ 3–5-km-Pace", "10×400 m kontrolliert flott", "90′ locker"],
      },
      RACE: {
        title: "RACE – 5 KM",
        principle: "Frische schlägt Fitness.",
        goal: ["Frische gewinnen", "Racepace automatisieren", "nichts Neues"],
        content: ["Racepace: 6×400 m / 3×1 km", "Schärfe: 8×200 m flott", "GA1: 30–45′", "Strides: 6×20″"],
        week: ["6×400 m @ 5-km-Pace", "35′ GA1 locker", "8×200 m flott (voll erholt)", "30′ locker + 6×20″", "Wettkampf"],
      },
    },
  },
  "10k": {
    distanceLabel: "10 KM",
    blocks: {
      BASE: {
        title: "BASE – 10 KM",
        principle: "Belastbar werden, nicht müde.",
        goal: ["Aerobe Kapazität", "Umfangstoleranz", "saubere Basis"],
        content: ["GA1 60′/75′", "Langer Lauf 90–110′", "Strides 6×20″", "VO2 nur selten: 6×20″", "Hügel locker/wellig", "Kraft/Stabi regelmäßig"],
        week: ["60′ GA1 locker", "6×20″ Strides", "50′ locker wellig", "6×20″ VO2-Impulse (selten)", "95–105′ langer Lauf"],
      },
      BUILD: {
        title: "BUILD – 10 KM",
        principle: "Spezifisch, aber kontrolliert.",
        goal: ["Schwelle anheben", "10-km-Tempo stabilisieren"],
        content: ["Schwelle: 4×8′ / 3×10′", "Intervalle: 5×1000 m / 4×2000 m", "Tempodauerlauf: 30–40′", "Langer Lauf: 100–120′"],
        week: ["4×8′ @ Schwelle", "45′ locker", "5×1000 m @ 10-km-Pace", "35′ Tempodauerlauf", "105′ locker"],
      },
      RACE: {
        title: "RACE – 10 KM",
        principle: "Frische schlägt Fitness.",
        goal: ["Frische + Tempogefühl"],
        content: ["Racepace: 3×2 km / 2×3 km", "Kontrolle: 5×1 km", "GA1: 40–50′", "Strides regelmäßig"],
        week: ["3×2 km @ 10-km-Pace", "45′ locker", "5×1 km kontrolliert", "35′ locker + Strides", "Wettkampf"],
      },
    },
  },
  hm: {
    distanceLabel: "HALBMARATHON",
    blocks: {
      BASE: {
        title: "BASE – HM",
        principle: "Belastbar werden, nicht müde.",
        goal: ["Große aerobe Basis", "Belastungsverträglichkeit", "lange ruhige Qualität"],
        content: ["GA1 60′/75′/90′", "Langer Lauf 100–130′ (Kern)", "Strides optional 4–6×20″", "VO2 sehr selten 4–6×15″", "Hügel locker", "Kraft/Stabi als Schwerpunkt"],
        week: ["75′ GA1 locker", "60′ locker wellig", "4–6×20″ Strides optional", "105–125′ langer Lauf locker", "Kraft/Stabi"],
      },
      BUILD: {
        title: "BUILD – HM",
        principle: "Spezifisch, aber kontrolliert.",
        goal: ["HM-Pace ökonomisch halten", "lange Schwelle stabilisieren"],
        content: ["Lange Schwelle: 3×12′ / 2×20′", "HM-Pace: 3×3 km / 2×5 km", "Tempodauerlauf: 40–60′", "Langer Lauf: 120–150′", "Ergänzend: kurze Strides"],
        week: ["3×12′ @ Schwelle", "50′ locker", "3×3 km @ HM-Pace", "45′ Tempodauerlauf", "130′ locker"],
      },
      RACE: {
        title: "RACE – HM",
        principle: "Frische schlägt Fitness.",
        goal: ["Ermüdung raus", "Pace sichern"],
        content: ["HM-Pace: 2×5 km / 3×3 km", "Rhythmuslauf: 10 km @ HM", "GA1: 40–60′", "Strides leicht"],
        week: ["2×5 km @ HM-Pace", "50′ locker", "10 km @ HM-Rhythmus", "40′ locker + Strides", "Wettkampf"],
      },
    },
  },
  m: {
    distanceLabel: "MARATHON",
    blocks: {
      BASE: {
        title: "BASE – MARATHON",
        principle: "Belastbar werden, nicht müde.",
        goal: ["Aerobe Tiefe", "Robustheit", "Umfangsverträglichkeit"],
        content: ["GA1 75′/90′", "Langer Lauf 120–150′ (Schlüssel)", "Strides selten 4×15″", "Hügel locker/technisch", "Kraft/Stabi sehr wichtig"],
        week: ["90′ GA1 locker", "60′ locker technisch-wellig", "4×15″ Strides (selten)", "130–145′ langer Lauf", "Kraft/Stabi"],
      },
      BUILD: {
        title: "BUILD – MARATHON",
        principle: "Spezifisch, aber kontrolliert.",
        goal: ["Marathonpace stabilisieren", "Ermüdungsresistenz"],
        content: ["Marathonpace: 3×5 km / 2×8 km", "Schwelle moderat: 3×10′", "Strukturierter Longrun: 28 km mit 3×5 km @ M", "Langer Lauf: 150–180′", "Ergänzend: Kraft + Ökonomie"],
        week: ["3×5 km @ Marathonpace", "60′ locker", "3×10′ @ Schwelle", "28 km mit 3×5 km @ M", "Kraft/Ökonomie"],
      },
      RACE: {
        title: "RACE – MARATHON",
        principle: "Frische schlägt Fitness.",
        goal: ["Frische & Fokus", "Pace automatisieren"],
        content: ["Marathonpace: 2×6–8 km", "Letzter langer Lauf: 12–16 km @ M (10–14T vorher)", "GA1 kurz & locker", "Aktivierung: 3×1′ flott"],
        week: ["2×6 km @ Marathonpace", "45′ GA1 locker", "3×1′ flott", "30′ locker", "Wettkampf"],
      },
    },
  },
};

const PHASE_DISTANCE_RULES = {
  "5k": {
    BASE: {
      allowed: ["GA1 locker", "Langer Lauf", "Strides", "Hügel kurz", "Kraft/Stabi"],
      limited: ["VO₂-Impulse"],
      forbidden: ["Schwelle", "Intervalle lang"],
      keyRules: {
        expectedKeysPerWeek: 0.5,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["strides", "vo2_touch"],
        preferredKeyTypes: ["strides"],
        bannedKeyTypes: ["schwelle", "racepace", "steady"],
      },
    },
    BUILD: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["schwelle", "vo2_touch"],
        preferredKeyTypes: ["vo2_touch", "schwelle"],
        bannedKeyTypes: ["racepace", "steady", "strides"],
      },
    },
    RACE: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "vo2_touch"],
        preferredKeyTypes: ["racepace", "vo2_touch"],
        bannedKeyTypes: ["schwelle", "steady", "strides"],
      },
    },
  },
  "10k": {
    BASE: {
      allowed: ["GA1 locker", "Langer Lauf", "Strides", "Hügel locker", "Kraft/Stabi"],
      limited: ["VO₂-Impulse"],
      forbidden: ["Tempoläufe", "Intervalle"],
      keyRules: {
        expectedKeysPerWeek: 0.5,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["strides", "vo2_touch"],
        preferredKeyTypes: ["strides"],
        bannedKeyTypes: ["schwelle", "racepace", "steady"],
      },
    },
    BUILD: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["schwelle", "vo2_touch"],
        preferredKeyTypes: ["schwelle", "vo2_touch"],
        bannedKeyTypes: ["racepace", "steady", "strides"],
      },
    },
    RACE: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "vo2_touch"],
        preferredKeyTypes: ["racepace", "vo2_touch"],
        bannedKeyTypes: ["schwelle", "steady", "strides"],
      },
    },
  },
  hm: {
    BASE: {
      allowed: ["GA1 locker", "Langer Lauf", "Hügel locker", "Kraft/Stabi"],
      limited: ["Strides", "VO₂-Impulse"],
      forbidden: ["Schwelle", "Racepace"],
      keyRules: {
        expectedKeysPerWeek: 0.25,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["strides", "vo2_touch"],
        preferredKeyTypes: ["strides"],
        bannedKeyTypes: ["schwelle", "racepace", "steady"],
      },
    },
    BUILD: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["schwelle", "racepace"],
        preferredKeyTypes: ["racepace", "schwelle"],
        bannedKeyTypes: ["vo2_touch", "steady", "strides"],
      },
    },
    RACE: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace"],
        preferredKeyTypes: ["racepace"],
        bannedKeyTypes: ["schwelle", "vo2_touch", "steady", "strides"],
      },
    },
  },
  m: {
    BASE: {
      allowed: ["GA1 locker", "Langer Lauf", "Hügel locker", "Kraft/Stabi"],
      limited: ["Strides"],
      forbidden: ["VO₂-Impulse", "Tempoläufe", "Marathonpace"],
      keyRules: {
        expectedKeysPerWeek: 0.25,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["strides"],
        preferredKeyTypes: ["strides"],
        bannedKeyTypes: ["schwelle", "racepace", "vo2_touch", "steady"],
      },
    },
    BUILD: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace", "schwelle"],
        preferredKeyTypes: ["racepace", "schwelle"],
        bannedKeyTypes: ["vo2_touch", "steady", "strides"],
      },
    },
    RACE: {
      keyRules: {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 1,
        allowedKeyTypes: ["racepace"],
        preferredKeyTypes: ["racepace"],
        bannedKeyTypes: ["schwelle", "vo2_touch", "steady", "strides"],
      },
    },
  },
};

function buildBlockDescriptionLines({ block, eventDistance }) {
  if (!block || !eventDistance) return null;
  if (!["BASE", "BUILD", "RACE"].includes(block)) return null;
  const libraryEntry = BLOCK_DESCRIPTION_LIBRARY[eventDistance];
  if (!libraryEntry) return null;
  const blockEntry = libraryEntry.blocks?.[block];
  if (!blockEntry) return null;

  const lines = [];
  lines.push(blockEntry.title);
  if (blockEntry.principle) lines.push(`Leitprinzip: ${blockEntry.principle}`);
  lines.push("Ziel:");
  blockEntry.goal.forEach((item) => lines.push(`- ${item}`));
  if (blockEntry.content?.length) {
    lines.push("");
    lines.push("Inhalt:");
    blockEntry.content.forEach((item) => lines.push(`- ${item}`));
  }
  if (blockEntry.week?.length) {
    lines.push("");
    lines.push("Beispielwoche:");
    blockEntry.week.forEach((item) => lines.push(`- ${item}`));
  }

  const phaseRules = PHASE_DISTANCE_RULES?.[eventDistance]?.[block];
  if (phaseRules) {
    lines.push("");
    lines.push("Regel-Kompass:");
    if (phaseRules.allowed?.length) {
      lines.push(`- ✅ Erlaubt: ${phaseRules.allowed.join(", ")}`);
    }
    if (phaseRules.limited?.length) {
      lines.push(`- ⚠️ Limitiert: ${phaseRules.limited.join(", ")}`);
    }
    if (phaseRules.forbidden?.length) {
      lines.push(`- ❌ Nicht vorgesehen: ${phaseRules.forbidden.join(", ")}`);
    }
  }
  return lines;
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

function formatSignedPct(value) {
  if (!Number.isFinite(value)) return "n/a";
  return `${value > 0 ? "+" : ""}${value.toFixed(1)}%`;
}

function buildAerobicStatusLines(trend) {
  const efTrend = Number.isFinite(trend?.efDeltaPct) ? trend.efDeltaPct : Number.isFinite(trend?.dv) ? trend.dv : null;
  const vdotTrend = efTrend;
  const confidence = trend?.confidence ?? null;
  const confidenceText = confidence ? ` (Confidence ${confidence})` : "";
  return [
    "🫁 Aerober Status (personalisiert)",
    `• EF-Trend (28d vs 28d): ${formatSignedPct(efTrend)}${confidenceText}.`,
    `• VDOT-Trend (28d vs 28d): ${formatSignedPct(vdotTrend)}.`,
  ];
}

function parseWellnessNumber(value) {
  if (value == null) return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  if (typeof value === "string") {
    const trimmed = value.trim();
    if (!trimmed) return null;
    const normalized = trimmed.replace(",", ".");
    const num = Number(normalized);
    return Number.isFinite(num) ? num : null;
  }
  return null;
}

function parseSleepHours(value) {
  if (value == null) return null;
  if (typeof value === "string") {
    const trimmed = value.trim().toLowerCase();
    if (!trimmed) return null;
    const timeMatch = trimmed.match(/^(\d+)\s*[:h]\s*(\d{1,2})$/);
    if (timeMatch) {
      const hours = Number(timeMatch[1]);
      const minutes = Number(timeMatch[2]);
      if (Number.isFinite(hours) && Number.isFinite(minutes)) {
        return hours + minutes / 60;
      }
    }
    const hourMinuteMatch = trimmed.match(/(\d+)\s*h(?:\s*(\d{1,2}))?/);
    if (hourMinuteMatch) {
      const hours = Number(hourMinuteMatch[1]);
      const minutes = hourMinuteMatch[2] ? Number(hourMinuteMatch[2]) : 0;
      if (Number.isFinite(hours) && Number.isFinite(minutes)) {
        return hours + minutes / 60;
      }
    }
  }
  const num = parseWellnessNumber(value);
  if (num == null || num <= 0) return null;
  if (num > 24) return round(num / 60, 2);
  return num;
}

function extractSleepHoursFromWellness(wellness) {
  if (!wellness) return null;
  const candidates = [
    wellness.sleep,
    wellness.sleep_hours,
    wellness.sleep_duration,
    wellness.sleep_time,
    wellness.sleep_hr,
    wellness.sleep_min,
    wellness.sleep_minutes,
    wellness.sleep_mins,
  ];
  for (const value of candidates) {
    const parsed = parseSleepHours(value);
    if (parsed != null) return parsed;
  }
  return null;
}

function extractSleepQualityFromWellness(wellness) {
  if (!wellness) return null;
  const candidates = [
    wellness.sleepQuality,
    wellness.sleep_quality,
    wellness.sleep_quality_score,
  ];
  for (const value of candidates) {
    const num = parseWellnessNumber(value);
    if (!Number.isFinite(num)) continue;
    if (num >= 1 && num <= 4) return Math.round(num);
  }
  return null;
}

function extractSleepScoreFromWellness(wellness) {
  if (!wellness) return null;
  const candidates = [
    wellness.sleepScore,
    wellness.sleep_score,
  ];
  for (const value of candidates) {
    const num = parseWellnessNumber(value);
    if (!Number.isFinite(num)) continue;
    if (num >= 0 && num <= 100) return Math.round(num);
  }
  return null;
}

function deriveSleepQualityLabel({ sleepQuality, sleepScore } = {}) {
  if (sleepQuality != null) {
    if (sleepQuality === 1) return "super";
    if (sleepQuality === 2) return "gut";
    if (sleepQuality === 3) return "ok";
    if (sleepQuality === 4) return "schlecht";
  }
  if (sleepScore != null) {
    if (sleepScore >= 85) return "super";
    if (sleepScore >= 70) return "gut";
    if (sleepScore >= 55) return "ok";
    return "schlecht";
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
    const num = parseWellnessNumber(value);
    if (num == null || num <= 0) continue;
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

function subjectiveTagIsNegative(tag, type) {
  if (!tag) return null;
  if (type === "pain") return !/none|no|0|false|ok/.test(tag);
  if (type === "legs") return /heavy|schwer|dead|tired|müde|low/.test(tag);
  if (type === "mood") return /low|down|bad|schlecht|negativ/.test(tag);
  if (type === "motivation") return /low|down|bad|schlecht|negativ|unmotiv/.test(tag);
  return null;
}

function subjectiveRollingFlag(entries, minShare = 0.5) {
  if (!entries.length) return null;
  const negatives = entries.filter(Boolean).length;
  return negatives / entries.length >= minShare;
}

function subjectiveShare(entries) {
  if (!entries.length) return null;
  const negatives = entries.filter(Boolean).length;
  return negatives / entries.length;
}

function averageSubjectiveShare(shares) {
  const usable = shares.filter((share) => typeof share === "number");
  if (!usable.length) return null;
  return avg(usable);
}

function buildRecoverySignalLines(recoverySignals) {
  if (!recoverySignals) return [];
  const lines = [];
  const {
    sleepHours,
    sleepBaseline,
    sleepDeltaPct,
    sleepQuality,
    sleepScore,
    sleepQualityLabel,
    hrv,
    hrvBaseline,
    hrvDeltaPct,
    sleepLow,
    hrvLow,
  } = recoverySignals;
  const hasSleep = sleepHours != null;
  const hasHrv = hrv != null;
  const hasSleepQuality = sleepQuality != null || sleepScore != null;
  if (!hasSleep && !hasHrv && !hasSleepQuality) return [];
  const parts = [];
  if (hasSleep) {
    const sleepDeltaText = sleepBaseline != null ? ` (${sleepDeltaPct > 0 ? "+" : ""}${sleepDeltaPct.toFixed(0)}% vs 7T)` : "";
    parts.push(`Schlaf ${sleepHours.toFixed(1)}h${sleepDeltaText}`);
  }
  if (hasSleepQuality) {
    const qualityLabel = sleepQualityLabel ? sleepQualityLabel : "unbekannt";
    const scoreText = sleepScore != null ? ` (Score ${sleepScore})` : "";
    const qualityText = sleepQuality != null ? `Qualität ${qualityLabel}${scoreText}` : `Schlafscore ${qualityLabel}${scoreText}`;
    parts.push(qualityText);
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

function buildSubjectiveAverageLine(recoverySignals, label = "4T") {
  if (!recoverySignals?.subjectiveShares) return null;
  const formatShare = (share) => (typeof share === "number" ? `${Math.round(share * 100)}%` : null);
  const parts = [];
  const { pain, legs, mood, motivation } = recoverySignals.subjectiveShares;
  const shareValues = [pain, legs, mood, motivation].filter((share) => typeof share === "number");
  const painText = formatShare(pain);
  if (painText) parts.push(`Schmerz ${painText}`);
  const legsText = formatShare(legs);
  if (legsText) parts.push(`Ermüdung ${legsText}`);
  const moodText = formatShare(mood);
  if (moodText) parts.push(`Stimmung ${moodText}`);
  const motivationText = formatShare(motivation);
  if (motivationText) parts.push(`Motivation ${motivationText}`);
  if (!parts.length) return null;
  const hasZero = shareValues.some((share) => share === 0);
  const zeroHint = hasZero ? " (0% = keine negativen Einträge im Zeitraum)" : "";
  return `Subjektiv Ø ${label} negativ: ${parts.join(" | ")}.${zeroHint}`;
}

async function computeRecoverySignals(ctx, env, dayIso) {
  const today = await fetchWellnessDay(ctx, env, dayIso);
  const sleepToday = extractSleepHoursFromWellness(today);
  const sleepQuality = extractSleepQualityFromWellness(today);
  const sleepScore = extractSleepScoreFromWellness(today);
  const hrvToday = extractHrvFromWellness(today);

  const priorDays = [];
  for (let i = 1; i <= 7; i += 1) {
    priorDays.push(isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - i * 86400000)));
  }
  const subjectiveWindow = [dayIso, ...priorDays.slice(0, 3)];
  const sleepVals = [];
  const hrvVals = [];
  const legsEntries = [];
  const moodEntries = [];
  const painEntries = [];
  const motivationEntries = [];
  for (const iso of priorDays) {
    const wellness = await fetchWellnessDay(ctx, env, iso);
    const sleep = extractSleepHoursFromWellness(wellness);
    const hrv = extractHrvFromWellness(wellness);
    if (sleep != null) sleepVals.push(sleep);
    if (hrv != null) hrvVals.push(hrv);
  }
  for (const iso of subjectiveWindow) {
    const wellness = iso === dayIso ? today : await fetchWellnessDay(ctx, env, iso);
    if (!wellness) continue;
    const legsTag = extractSubjectiveTag(wellness, ["legs", "legs_feel", "leg_feel", "muscle_feel", "fatigue_feel"]);
    const moodTag = extractSubjectiveTag(wellness, ["mood", "mood_state", "readiness_mood"]);
    const painTag = extractSubjectiveTag(wellness, ["pain", "injury", "injury_flag", "pain_flag"]);
    const motivationTag = extractSubjectiveTag(wellness, ["motivation", "motivation_level", "motivation_state", "motivation_score"]);
    const legsNeg = subjectiveTagIsNegative(legsTag, "legs");
    const moodNeg = subjectiveTagIsNegative(moodTag, "mood");
    const painNeg = subjectiveTagIsNegative(painTag, "pain");
    const motivationNeg = subjectiveTagIsNegative(motivationTag, "motivation");
    if (legsNeg != null) legsEntries.push(legsNeg);
    if (moodNeg != null) moodEntries.push(moodNeg);
    if (painNeg != null) painEntries.push(painNeg);
    if (motivationNeg != null) motivationEntries.push(motivationNeg);
  }

  if (
    sleepToday == null &&
    sleepQuality == null &&
    sleepScore == null &&
    hrvToday == null &&
    !legsEntries.length &&
    !moodEntries.length &&
    !painEntries.length &&
    !motivationEntries.length
  ) {
    return null;
  }

  const sleepBaseline = sleepVals.length ? avg(sleepVals) : null;
  const hrvBaseline = hrvVals.length ? avg(hrvVals) : null;
  const sleepDeltaPct = sleepBaseline ? ((sleepToday - sleepBaseline) / sleepBaseline) * 100 : 0;
  const hrvDeltaPct = hrvBaseline ? ((hrvToday - hrvBaseline) / hrvBaseline) * 100 : 0;
  const ydayIso = isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - 86400000));
  const yday = await fetchWellnessDay(ctx, env, ydayIso);
  const ydayHrv = extractHrvFromWellness(yday);
  const ydayHrvDeltaPct = hrvBaseline && ydayHrv ? ((ydayHrv - hrvBaseline) / hrvBaseline) * 100 : null;
  const sleepQualityLabel = deriveSleepQualityLabel({ sleepQuality, sleepScore });
  const sleepLowByHours = sleepBaseline != null && sleepToday < sleepBaseline * 0.9;
  const sleepLowByQuality = sleepQuality != null && sleepQuality >= 4;
  const sleepLowByScore = sleepScore != null && sleepScore < 55;
  const sleepLow = sleepLowByHours || sleepLowByQuality || sleepLowByScore;
  const hrvLow = hrvBaseline != null && hrvToday < hrvBaseline * 0.9;
  const legsNegative = subjectiveRollingFlag(legsEntries);
  const moodNegative = subjectiveRollingFlag(moodEntries);
  const painInjury = painEntries.length ? painEntries.some(Boolean) : null;
  const motivationNegative = subjectiveRollingFlag(motivationEntries);
  const legsShare = subjectiveShare(legsEntries);
  const moodShare = subjectiveShare(moodEntries);
  const painShare = subjectiveShare(painEntries);
  const motivationShare = subjectiveShare(motivationEntries);
  const subjectiveAvgNegative = averageSubjectiveShare([legsShare, moodShare, painShare, motivationShare]);
  const subjectiveNegative =
    subjectiveAvgNegative != null
      ? subjectiveAvgNegative >= 0.5
      : !!legsNegative || !!moodNegative || !!motivationNegative || !!painInjury;
  return {
    sleepHours: sleepToday,
    sleepBaseline,
    sleepDeltaPct,
    sleepQuality,
    sleepScore,
    sleepQualityLabel,
    hrv: hrvToday,
    hrvBaseline,
    hrvDeltaPct,
    ydayHrvDeltaPct,
    sleepLow,
    hrvLow,
    legsNegative,
    moodNegative,
    painInjury,
    motivationNegative,
    subjectiveAvgNegative,
    subjectiveNegative,
    subjectiveShares: {
      legs: legsShare,
      mood: moodShare,
      pain: painShare,
      motivation: motivationShare,
    },
  };
}

function computeMaintenance14d(ctx, dayIso) {
  const end = getHistoryWindowEnd(dayIso);
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

// NEW: STEADY_T confidence helper (shared by decision + comment)
function computeReadinessConfidence({
  driftSignal,
  hrvDeltaPct,
  runLoad7,
  fatigueOverride,
  hadKey,
  counterIndicator,
  hrv1dNegative,
  hrv2dNegative,
  trend,
}) {
  return computeSectionConfidence({
    hasDrift: driftSignal != null && driftSignal !== "unknown",
    hasHrv: hrvDeltaPct != null,
    hasLoad: Number.isFinite(runLoad7),
    consistent: !(driftSignal === "green" && (hrv1dNegative || !!fatigueOverride)),
    subjectiveAligned: hadKey || !counterIndicator,
    contradictions: driftSignal === "green" && hrv2dNegative,
    hasHistory: Number.isFinite(trend?.recentN) || Number.isFinite(trend?.prevN),
  });
}

function downgradeConfidenceOneStep(confidence) {
  if (!confidence) return confidence;
  const currentBucket = confidence.bucket || confidenceBucket(confidence.score ?? 0);
  if (currentBucket === "low") return { ...confidence, bucket: "low", score: Math.min(confidence.score ?? 0, 39) };
  if (currentBucket === "medium") return { ...confidence, bucket: "low", score: Math.min(confidence.score ?? 0, 39) };
  return { ...confidence, bucket: "medium", score: Math.min(confidence.score ?? 0, 69) };
}

function formatKeyCapValue(dynamicKeyCap, fallbackCap) {
  if (Number.isFinite(dynamicKeyCap?.maxKeys7d)) return dynamicKeyCap.maxKeys7d;
  if (Number.isFinite(fallbackCap)) return fallbackCap;
  return null;
}

function formatKeyCapReason(dynamicKeyCap) {
  if (!dynamicKeyCap?.reasons?.length) return null;
  return dynamicKeyCap.reasons.join(", ");
}

function normalizeActionKey(action, severity) {
  const text = String(action || "").toLowerCase();
  if (text.includes("keine intens") || text.includes("no intensity")) {
    if (text.includes("5-7") || text.includes("5–7") || text.includes("7")) return "NO_INTENSITY_7D";
    return "NO_INTENSITY";
  }
  if (text.includes("nur easy") || text.includes("easy only") || text.includes("easy")) {
    return "EASY_ONLY_TODAY";
  }
  if (text.includes("volumen reduzieren") || text.includes("reduce volume")) {
    return "REDUCE_VOL_15";
  }
  if (text.includes("dichte runter") || text.includes("keine zusatzreize")) {
    return `ACTION_GENERIC_${String(severity || "medium").toUpperCase()}`;
  }
  return `ACTION_GENERIC_${String(severity || "medium").toUpperCase()}`;
}

function extractActionKeys(action, severity) {
  if (Array.isArray(action)) {
    return action.map((a) => normalizeActionKey(a, severity));
  }
  const text = String(action || "").toLowerCase();
  const keys = new Set();
  if (text.includes("keine intens") || text.includes("no intensity")) keys.add(normalizeActionKey("no intensity", severity));
  if (text.includes("nur easy") || text.includes("easy only") || text.includes("easy")) keys.add("EASY_ONLY_TODAY");
  if (text.includes("volumen reduzieren") || text.includes("reduce volume")) keys.add("REDUCE_VOL_15");
  if (!keys.size) keys.add(normalizeActionKey(action, severity));
  return Array.from(keys);
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

function patternToPolicies(matchedPatterns) {
  const policies = [];
  for (const pattern of matchedPatterns) {
    const actionKeys = extractActionKeys(pattern.action, pattern.severity);
    actionKeys.forEach((actionKey, idx) => {
      const id = `PAT_${pattern.id}__${actionKey}`;
      policies.push({
        id,
        severity: pattern.severity,
        primary: idx === 0,
        guardrail: actionKey.includes("NO_INTENSITY"),
        actionKey,
      });
    });
  }
  return policies;
}

function signalToPolicies(signalMap) {
  const candidates = [];
  if (signalMap?.hrv_2d_negative) {
    candidates.push({ id: "SIG__hrv_2d_negative__HIGH", severity: "high", guardrail: true });
  }
  if (signalMap?.fatigue_override) {
    candidates.push({ id: "SIG__fatigue_override__MEDIUM", severity: "medium", guardrail: false });
  }
  return candidates;
}

function mergeAndRankPolicies(policies) {
  const severityRank = { high: 3, medium: 2, low: 1 };
  const dedup = new Map();
  for (const p of policies) {
    const existing = dedup.get(p.id);
    if (!existing || severityRank[p.severity] > severityRank[existing.severity]) {
      dedup.set(p.id, p);
    }
  }
  return Array.from(dedup.values())
    .sort((a, b) => {
      if (severityRank[b.severity] !== severityRank[a.severity]) {
        return severityRank[b.severity] - severityRank[a.severity];
      }
      if (b.guardrail !== a.guardrail) return b.guardrail ? 1 : -1;
      return a.id.localeCompare(b.id);
    })
    .map((p) => p.id);
}

function buildPolicyDecision({ matchedPatterns, signalMap, confidenceScore }) {
  const patternPolicies = patternToPolicies(matchedPatterns);
  const signalPolicies = patternPolicies.length ? [] : signalToPolicies(signalMap);
  const rankedIds = mergeAndRankPolicies([...patternPolicies, ...signalPolicies]);
  if (!rankedIds.length) return null;

  const primaryId = rankedIds[0];
  const primary = POLICY_REGISTRY[primaryId];
  const confidence = confidenceScore ?? 0;
  const bucket = confidenceBucket(confidence);
  const primaryPattern = matchedPatterns.find((p) => primaryId.startsWith(`PAT_${p.id}__`));
  const intensityLock = Boolean(primaryPattern?.severity === "high" && String(primaryPattern?.action || "").toLowerCase().includes("keine intens"));
  const reasonSuffix =
    confidence < 40
      ? "Wenn die Signale zutreffen, ist dies eine vorsichtige Empfehlung."
      : "Zusatzsignal: mindestens ein Warnsignal im Kontext aktiv.";

  return {
    title: primary?.short || primaryId,
    reason: `${primary?.why || "Signalbasierte Entscheidung."} ${reasonSuffix}`,
    effect: primary?.effect || "Belastung anpassen.",
    confidence: { score: confidence, bucket },
    intensity_lock: intensityLock,
    policies_applied: rankedIds,
    decision_trace: { primaryId, matchedPatterns: matchedPatterns.length },
  };
}

function deterministicRoll(seed) {
  const text = String(seed ?? "seed");
  let hash = 2166136261;
  for (let i = 0; i < text.length; i++) {
    hash ^= text.charCodeAt(i);
    hash = Math.imul(hash, 16777619);
  }
  return ((hash >>> 0) % 10000) / 10000;
}

function deriveStressBucket({ fatigueOverride, warningCount }) {
  if (fatigueOverride || warningCount >= 3) return "HIGH";
  if (warningCount >= 1) return "MED";
  return "LOW";
}

function deriveHrvBucket(hrvDeltaPct) {
  if (!Number.isFinite(hrvDeltaPct)) return "UNK";
  if (hrvDeltaPct <= HRV_NEGATIVE_THRESHOLD_PCT) return "LOW";
  if (hrvDeltaPct >= 5) return "HIGH";
  return "NORMAL";
}

function deriveDriftBucket(driftSignal) {
  if (!driftSignal || driftSignal === "unknown") return "UNK";
  if (driftSignal === "red") return "BAD";
  if (driftSignal === "orange") return "WARN";
  return "OK";
}

function deriveSleepBucket(recoverySignals) {
  if (!recoverySignals) return "UNK";
  if (recoverySignals.sleepLow) return "LOW";
  if (Number.isFinite(recoverySignals.sleepDeltaPct)) {
    if (recoverySignals.sleepDeltaPct >= 10) return "HIGH";
    if (recoverySignals.sleepDeltaPct <= -10) return "LOW";
  }
  return "OK";
}

function deriveMonotonyBucket(monotony) {
  if (!Number.isFinite(monotony)) return "UNK";
  return monotony > MONOTONY_7D_LIMIT ? "HIGH" : "LOW";
}

function deriveContextKey({
  runFloorGap,
  fatigueOverride,
  warningCount,
  hrvDeltaPct,
  driftSignal,
  recoverySignals,
  monotony,
}) {
  const lifeStress = deriveStressBucket({ fatigueOverride, warningCount });
  const hrvState = deriveHrvBucket(hrvDeltaPct);
  const driftState = deriveDriftBucket(driftSignal);
  const sleepState = deriveSleepBucket(recoverySignals);
  const monotonyState = deriveMonotonyBucket(monotony);
  return `RFgap=${runFloorGap ? "T" : "F"}|stress=${lifeStress}|hrv=${hrvState}|drift=${driftState}|sleep=${sleepState}|mono=${monotonyState}`;
}

function deriveStrategyArm({
  runFloorGap,
  lifeStress,
  hrvState,
  driftState,
  hadKey,
  freqNotRed,
  highMonotony,
  fatigueHigh,
  hasHardRedFlag,
}) {
  if (hasHardRedFlag) {
    return {
      strategyArm: "NEUTRAL",
      policyReason: "RED_FLAG",
      learningEligible: false,
    };
  }

  if (runFloorGap && (lifeStress === "MED" || lifeStress === "HIGH")) {
    return {
      strategyArm: "FREQ_UP",
      policyReason: "RUN_FLOOR_GAP_HIGH_STRESS",
      learningEligible: true,
    };
  }

  if (runFloorGap && lifeStress === "LOW") {
    return {
      strategyArm: "VOLUME_ADJUST",
      policyReason: "RUN_FLOOR_GAP_LOW_STRESS",
      learningEligible: true,
    };
  }

  if (hadKey && freqNotRed) {
    return {
      strategyArm: "INTENSITY_SHIFT",
      policyReason: "QUALITY_OK",
      learningEligible: true,
    };
  }

  if (highMonotony || driftState === "BAD") {
    return {
      strategyArm: fatigueHigh ? "PROTECT_DELOAD" : "HOLD_ABSORB",
      policyReason: fatigueHigh ? "HIGH_MONOTONY_FATIGUE" : "HIGH_MONOTONY_OR_DRIFT",
      learningEligible: true,
    };
  }

  if (fatigueHigh || (hrvState === "LOW" && lifeStress === "HIGH")) {
    return {
      strategyArm: "PROTECT_DELOAD",
      policyReason: "HIGH_STRESS",
      learningEligible: true,
    };
  }

  return {
    strategyArm: "HOLD_ABSORB",
    policyReason: "DEFAULT_HOLD",
    learningEligible: true,
  };
}

function computeLearningOutcomeScore({ driftSignal, hrv1dNegative, hrv2dNegative, fatigueOverride, warningCount }) {
  let score = 0;
  if (driftSignal === "green") score += 2;
  else if (driftSignal === "orange") score += 0;
  else if (driftSignal === "red") score -= 2;

  if (!hrv1dNegative) score += 1;
  if (!hrv2dNegative) score += 1;
  if (!fatigueOverride) score += 1;

  if (warningCount >= 3) score -= 2;
  else if (warningCount === 2) score -= 1;

  return score;
}

// NEW: STEADY_T guardrail state (split KEY_HARD vs STEADY_T)
function buildGuardrailState({
  hasHardRedFlag,
  hrv2dNegative,
  warningCount,
  loadState,
  painInjury,
  keySpacingOk,
  fatigueGuardrailSeverity,
  fatigueGuardrailReasons,
}) {
  const hardReasons = [];
  const softReasons = [];
  const guardrailReasons = [];
  if (painInjury) hardReasons.push("pain_injury");
  if (hrv2dNegative) hardReasons.push("hrv_2d_negative");
  if (loadState === "overreached") hardReasons.push("overreached");
  if (hasHardRedFlag && !hrv2dNegative) hardReasons.push("hard_red_flag");
  if (fatigueGuardrailSeverity === "hard") hardReasons.push("fatigue_hard");
  if (fatigueGuardrailSeverity === "soft") softReasons.push("fatigue_soft");
  if (warningCount >= 2) softReasons.push("cumulative_warnings");

  if (Array.isArray(fatigueGuardrailReasons)) {
    guardrailReasons.push(...fatigueGuardrailReasons);
  }
  if (hrv2dNegative) guardrailReasons.push("hrv_2d_negative");
  if (painInjury) guardrailReasons.push("pain_injury");
  if (warningCount >= 2) guardrailReasons.push(`warning_count:${warningCount}`);

  const guardrailSeverity = hardReasons.length > 0 ? "hard" : softReasons.length > 0 ? "soft" : "none"; // NEW: guardrail severity
  const hardActive = guardrailSeverity === "hard";
  const mediumActive = guardrailSeverity === "soft";

  const blocksKeyHard = new Set();
  const blocksSteady = new Set();

  if (hardActive) {
    blocksKeyHard.add("HARD_GUARDRAIL");
    blocksSteady.add("HARD_GUARDRAIL");
  } else if (mediumActive) {
    blocksKeyHard.add("SOFT_GUARDRAIL");
  }
  if (hrv2dNegative) blocksKeyHard.add("HRV_2D_NEGATIVE");
  if (warningCount >= 2) blocksKeyHard.add("CUMULATIVE_WARNINGS");
  if (keySpacingOk === false) blocksKeyHard.add("KEY_SPACING");

  return {
    guardrailSeverity,
    hardActive,
    mediumActive,
    hardReasons,
    softReasons,
    guardrailReasons,
    blocks: {
      keyHard: Array.from(blocksKeyHard),
      steady: Array.from(blocksSteady),
    },
  };
}

function computeBuildSteadyDecision({
  phase,
  guardrailHardActive,
  guardrailSoftActive,
  hrvNegativeDays,
  hrv2dNegative,
  driftRecentMedian,
  driftTrendWorsening,
  loadState,
  fatigueHigh,
  motorTrendDownStrong,
  intensityBudget,
  decisionConfidenceScore,
}) {
  const driftOk = Number.isFinite(driftRecentMedian) ? driftRecentMedian <= DRIFT_WARN_PCT : false;
  const hrvOk = Number.isFinite(hrvNegativeDays) ? hrvNegativeDays <= 1 : false;
  const loadOk = loadState !== "overreached";
  const steadyBudgetOk = intensityBudget.steadyCount < STEADY_T_MAX_PER_7D;
  const keyBudgetCount =
    STEADY_T_BUDGET_KEY_TYPE === "keyAny" ? intensityBudget.keyAnyCount ?? 0 : intensityBudget.keyHardCount ?? 0;
  const keyConflict = keyBudgetCount >= KEY_HARD_MAX_PER_7D;
  const budgetMode = STEADY_T_BUDGET_MODE;
  const budgetOk = steadyBudgetOk && (budgetMode === "exclusive" ? !keyConflict : true);
  const replacementEligible = budgetMode === "replacement" && keyConflict && !guardrailHardActive && !fatigueHigh;

  const delaySignals = {
    drift_trend_worsening: !!driftTrendWorsening,
    hrv_negative_2days: !!hrv2dNegative,
    motor_trend_down_strong: !!motorTrendDownStrong,
  };
  const delayActive = Object.values(delaySignals).some(Boolean);

  const confidenceOk = Number.isFinite(decisionConfidenceScore) ? decisionConfidenceScore >= DECISION_CONF_MIN : false; // NEW: STEADY_T confidence gate
  const replacementOk = budgetMode !== "replacement" || !keyConflict || replacementEligible;
  const allowConditionsMet = !guardrailHardActive && loadOk && budgetOk && driftOk && hrvOk && replacementOk;
  const eligibility = {
    guardrailHardActive,
    guardrailSoftActive,
    loadOk,
    budgetOk,
    driftOk,
    hrvOk,
    steadyBudgetOk,
    keyConflict,
    confidenceOk,
    budgetModeExclusive: budgetMode === "exclusive",
    budgetModeReplacement: budgetMode === "replacement",
    replacementEligible,
    fatigueHigh: !!fatigueHigh,
  };

  let status = "blocked";
  let reasonId = "UNKNOWN";
  let reasonText = "Steady-T heute nicht freigegeben.";

  if (guardrailHardActive) {
    reasonId = "HARD_GUARDRAIL";
    reasonText = hrv2dNegative
      ? "Harter Guardrail aktiv (HRV 2 Tage negativ)."
      : "Harter Guardrail aktiv.";
  } else if (!loadOk) {
    reasonId = "FATIGUE_HIGH";
    reasonText = "Belastungsstatus: overreached.";
  } else if (!budgetOk) {
    if (!steadyBudgetOk) {
      reasonId = "BUDGET_STEADY_MAX";
      reasonText = `STEADY_T-Limit (${STEADY_T_MAX_PER_7D}/7T) erreicht.`;
    } else {
      reasonId = "BUDGET_KEY_PRESENT";
      const lastKeyIso =
        STEADY_T_BUDGET_KEY_TYPE === "keyAny" ? intensityBudget.lastKeyAnyIso : intensityBudget.lastKeyHardIso;
      reasonText = `Wochen-Budget erreicht (Key in letzten 7T am ${lastKeyIso || "n/a"}).`;
    }
  } else if (delayActive) {
    status = "delayed";
    reasonId = "BUILD_STEADY_DELAY";
    reasonText = "Build aktiv, aber System absorbiert noch – Schwellenreiz wird verschoben, nicht gestrichen.";
  } else if (!hrvOk) {
    reasonId = "HRV_2D_NEG";
    reasonText = "HRV an 2 Tagen negativ.";
  } else if (!driftOk) {
    reasonId = "DRIFT_TOO_HIGH";
    reasonText = `Drift-Median > ${DRIFT_WARN_PCT}%.`;
  } else {
    status = "allowed";
    reasonId = "BUILD_STEADY_ALLOWED";
    reasonText = guardrailSoftActive
      ? "Soft Guardrail aktiv: kontrollierter Schwellenreiz erlaubt (Signal setzen, kein Ermüdungsaufbau)."
      : "Kontrollierter Schwellenreiz erlaubt (Signal setzen, kein Ermüdungsaufbau).";
    if (replacementEligible) {
      reasonText = `${reasonText} Key diese Woche erledigt → nur wenn du statt eines weiteren Keys einen kontrollierten Steady-Block willst.`;
    }
  }

  const delayReasons = Object.entries(delaySignals)
    .filter(([, active]) => active)
    .map(([key]) => key);

  return {
    status,
    allowSteady: status === "allowed",
    delaySteady: status === "delayed",
    reasonId,
    reasonText,
    delayRange: STEADY_T_DELAY_DAYS_RANGE,
    delayReasons,
    allowConditionsMet,
    confidenceOk,
    eligibility,
  };
}

function computeKeyHardDecision({
  guardrailHardActive,
  guardrailMediumActive,
  hrv2dNegative,
  loadState,
  keySpacingOk,
  keyCompliance,
  intensityBudget,
}) {
  if (guardrailHardActive || hrv2dNegative) {
    return { allowed: false, reason: "Harter Guardrail aktiv." };
  }
  if (guardrailMediumActive) {
    return { allowed: false, reason: "Soft Guardrail aktiv – Key-Hard heute gesperrt." };
  }
  if (loadState === "overreached") {
    return { allowed: false, reason: "Belastungsstatus: overreached." };
  }
  if (keySpacingOk === false) {
    return { allowed: false, reason: "Key-Abstand <48h." };
  }
  if (keyCompliance?.capExceeded) {
    return { allowed: false, reason: "Key-Cap diese Woche erreicht." };
  }
  if (intensityBudget?.steadyCount > 0) {
    return { allowed: false, reason: "STEADY_T in den letzten 7 Tagen." };
  }
  if (intensityBudget?.keyHardCount >= KEY_HARD_MAX_PER_7D) {
    return { allowed: false, reason: `KEY_HARD-Limit (${KEY_HARD_MAX_PER_7D}/7T) erreicht.` };
  }
  return { allowed: true, reason: "Key-Hard möglich (Budget frei)." };
}

function mapKeyTypeToIntensityClass(keyType) {
  if (keyType === "racepace") return INTENSITY_RECOMMENDATION_CLASS.RACEPACE;
  if (keyType === "vo2_touch") return INTENSITY_RECOMMENDATION_CLASS.VO2_TOUCH;
  if (keyType === "strides") return INTENSITY_RECOMMENDATION_CLASS.STRIDES;
  return INTENSITY_RECOMMENDATION_CLASS.STEADY;
}

function computeFloorGapPolicy({ runFloorGap, guardrailSeverity, keyBudgetAvailable, spacingOk }) {
  const blockVolumeEscalationDueToFloorGap = !!runFloorGap;
  const allowIntensityDespiteFloorGap = runFloorGap
    ? guardrailSeverity !== "hard" && keyBudgetAvailable && spacingOk
    : true;
  return {
    blockVolumeEscalationDueToFloorGap,
    allowIntensityDespiteFloorGap,
    reasonId: runFloorGap ? "FLOOR_GAP_BLOCKS_VOLUME_ONLY" : null,
    reasonText: runFloorGap
      ? "Runfloor-Lücke: Volumen nicht erhöhen, Intensität möglich wenn Guardrails/Budget ok."
      : null,
  };
}

function selectIntensityRecommendation({
  blockState,
  weeksToEvent,
  guardrailState,
  keyRules,
  keyCompliance,
  intensityBudget,
  keySpacing,
  steadyDecision,
  runFloorGap,
}) {
  const guardrailSeverity = guardrailState?.guardrailSeverity ?? "none";
  const hardGuardrail = guardrailSeverity === "hard";
  const spacingOk = keySpacing?.ok !== false;
  const keyBudgetAvailable =
    (intensityBudget?.keyHardCount ?? 0) < KEY_HARD_MAX_PER_7D && !keyCompliance?.capExceeded;
  const floorGapPolicy = computeFloorGapPolicy({
    runFloorGap,
    guardrailSeverity,
    keyBudgetAvailable,
    spacingOk,
  });
  const keyEligible = guardrailSeverity !== "hard" && spacingOk && keyBudgetAvailable;
  const allowedKeyTypes = keyCompliance?.allowedKeyTypes ?? keyRules?.allowedKeyTypes ?? [];
  const preferredKeyTypes = keyRules?.preferredKeyTypes ?? [];
  const bannedKeyTypes = keyCompliance?.bannedKeyTypes ?? keyRules?.bannedKeyTypes ?? [];
  const disallowedKeyTypes = keyCompliance?.computedDisallowed ?? [];
  const bannedHits = keyCompliance?.bannedHits ?? [];
  const bannedLast = bannedHits.length ? bannedHits[0] : null;

  const isTypeAllowed = (type) =>
    allowedKeyTypes.includes(type) && !bannedKeyTypes.includes(type) && !disallowedKeyTypes.includes(type);
  const softDowngradeBlock = guardrailSeverity === "soft" ? new Set(["vo2_touch"]) : new Set();
  const filterForGuardrails = (types) => types.filter((type) => isTypeAllowed(type) && !softDowngradeBlock.has(type));

  const availablePreferred = filterForGuardrails(preferredKeyTypes);
  const availableAllowed = filterForGuardrails(allowedKeyTypes);

  const block = blockState?.block || "BASE";
  const preferRaceKey = block === "RACE" && weeksToEvent != null && weeksToEvent <= 6;

  if (hardGuardrail) {
    return {
      intensityClass: INTENSITY_RECOMMENDATION_CLASS.EASY_BASE,
      reasonId: "POSITIVE_SELECT_EASY_BASE",
      reasonText: "Positive Auswahl: EASY_BASE (Hard-Guardrail aktiv).",
      floorGapPolicy,
    };
  }

  if (bannedLast && keyEligible) {
    const altKey = availablePreferred[0] ?? availableAllowed[0] ?? null;
    if (altKey) {
      return {
        intensityClass: mapKeyTypeToIntensityClass(altKey),
        keyType: altKey,
        reasonId: `POSITIVE_SELECT_${mapKeyTypeToIntensityClass(altKey)}`,
        reasonText: `Alternative statt verbotener Key-Typ: ${altKey} (statt ${bannedLast}).`,
        floorGapPolicy,
      };
    }
  }

  if (block === "RACE" && keyEligible) {
    const chosenKey = availablePreferred[0] ?? availableAllowed[0] ?? null;
    if (chosenKey) {
      return {
        intensityClass: mapKeyTypeToIntensityClass(chosenKey),
        keyType: chosenKey,
        reasonId: preferRaceKey ? "RACE_PREF_KEY_CHOSEN" : `POSITIVE_SELECT_${mapKeyTypeToIntensityClass(chosenKey)}`,
        reasonText: preferRaceKey
          ? `RACE-Block: bevorzuge ${chosenKey} (wettkampfnah).`
          : `Positive Auswahl: ${mapKeyTypeToIntensityClass(chosenKey)} (Guardrails ok, Budget ok, Spacing ok).`,
        floorGapPolicy,
      };
    }
  }

  if (steadyDecision?.allowSteady && steadyDecision?.eligibility?.steadyBudgetOk) {
    return {
      intensityClass: INTENSITY_RECOMMENDATION_CLASS.STEADY,
      reasonId: "POSITIVE_SELECT_STEADY",
      reasonText: "Positive Auswahl: STEADY (Guardrails ok, Budget ok, Spacing ok).",
      floorGapPolicy,
    };
  }

  if (spacingOk && availableAllowed.includes("strides")) {
    return {
      intensityClass: INTENSITY_RECOMMENDATION_CLASS.STRIDES,
      reasonId: "POSITIVE_SELECT_STRIDES",
      reasonText: "Positive Auswahl: STRIDES (Guardrails ok, Budget ok, Spacing ok).",
      floorGapPolicy,
    };
  }

  return {
    intensityClass: INTENSITY_RECOMMENDATION_CLASS.EASY_BASE,
    reasonId: "POSITIVE_SELECT_EASY_BASE",
    reasonText: "Positive Auswahl: EASY_BASE (Guardrails ok, Budget ok, Spacing ok).",
    floorGapPolicy,
  };
}

function buildDecisionTrace({
  steadyDecision,
  guardrailState,
  overruledSignals,
  intensityClassToday,
  intensityBudget,
  excludeFromTrends,
  intensitySelection,
}) {
  // NEW: STEADY_T decision trace payload
  const guardrailApplied = { blocks: [], allows: [] };
  if (guardrailState) {
    const keyBlocked = guardrailState.blocks?.keyHard?.length > 0;
    const steadyBlocked = guardrailState.blocks?.steady?.length > 0;
    if (keyBlocked) guardrailApplied.blocks.push("KEY_HARD");
    else guardrailApplied.allows.push("KEY_HARD");
    if (steadyBlocked) guardrailApplied.blocks.push("STEADY_T");
    else guardrailApplied.allows.push("STEADY_T");
    guardrailApplied.guardrailSeverity = guardrailState.guardrailSeverity || "none";
    guardrailApplied.severity = guardrailApplied.guardrailSeverity;
    guardrailApplied.guardrailReasons = guardrailState.guardrailReasons || [];
    guardrailApplied.steadyEligibility = steadyDecision?.eligibility || null;
  }
  if (!guardrailApplied.guardrailSeverity) guardrailApplied.guardrailSeverity = "none";
  if (!guardrailApplied.severity) guardrailApplied.severity = guardrailApplied.guardrailSeverity;
  if (!guardrailApplied.guardrailReasons) guardrailApplied.guardrailReasons = [];
  if (!("steadyEligibility" in guardrailApplied)) guardrailApplied.steadyEligibility = steadyDecision?.eligibility || null;

  const excluded = Object.entries(excludeFromTrends || {})
    .filter(([, value]) => value)
    .map(([key]) => key);

  return {
    highest_priority_trigger: steadyDecision?.reasonId ?? null,
    guardrail_applied: guardrailApplied,
    intensity_class_today: intensityClassToday ?? null,
    intensity_budget: intensityBudget ?? null,
    intensity_recommendation: intensitySelection ?? null,
    delayed: {
      steady: !!steadyDecision?.delaySteady,
      days: steadyDecision?.delaySteady ? steadyDecision?.delayRange?.max ?? null : null,
    },
    excluded_from_trends: excluded,
    overruled_signals: overruledSignals ?? [],
  };
}

function buildIntensityDebugPayload({
  intensityClassToday,
  intensityBudget,
  steadyDecision,
  keyHardDecision,
  decisionTrace,
  guardrailState,
  excludeFromTrends,
  intensitySelection,
}) {
  return {
    intensity_class: intensityClassToday,
    intensity_budget: intensityBudget,
    steady_decision: steadyDecision,
    steadyEligibility: steadyDecision?.eligibility ?? null,
    key_hard_decision: keyHardDecision,
    intensity_selection: intensitySelection ?? null,
    decision_trace: decisionTrace,
    computedGuardrail: guardrailState
      ? { severity: guardrailState.guardrailSeverity, reasons: guardrailState.guardrailReasons }
      : null,
    exclude_from: excludeFromTrends,
  };
}

// ================= WORKOUT BUILDER (Key-Reiz Konkret) =================

function normalizeConfidenceLevel(confidenceLevel) {
  const text = String(confidenceLevel || "").toLowerCase();
  if (text === "hoch" || text === "high") return "high";
  if (text === "mittel" || text === "medium") return "medium";
  if (text === "niedrig" || text === "low") return "low";
  return "unknown";
}

function toMinutes(seconds) {
  return seconds / 60;
}

function formatIntervalSeconds(seconds) {
  if (seconds % 60 === 0) return `${seconds / 60}′`;
  return `${seconds}s`;
}

function formatMinuteBlock(minutes) {
  return `${Math.round(minutes)}′`;
}

function applyIntervalInsightsToWorkoutPlan(plan, intervalInsights) {
  if (!plan || !intervalInsights) return { plan, adjustmentNotes: [] };
  const metrics = intervalInsights.intervalMetrics;
  if (!metrics) return { plan, adjustmentNotes: [] };
  if (plan.keyType === "strides") return { plan, adjustmentNotes: [] };

  let adjusted = { ...plan };
  const adjustmentNotes = [];
  let recBonus = 0;

  if (metrics.drift_flag === "too_hard" || metrics.drift_flag === "overreaching") {
    if (adjusted.reps > 4) {
      adjusted.reps -= 1;
      adjustmentNotes.push("1 Wdh weniger (HF-Drift hoch)");
    } else {
      recBonus += 30;
      adjustmentNotes.push("Pausen +30s (HF-Drift hoch)");
    }
  }

  if (recBonus > 0) {
    adjusted.recSec = Math.min(adjusted.recSec + recBonus, 240);
  }

  if (adjustmentNotes.length) {
    adjusted.intensityMinutes = toMinutes(adjusted.reps * adjusted.workSec);
    adjusted.totalMinutes = computeTotalMinutes({
      warmupMin: adjusted.warmupMin,
      cooldownMin: adjusted.cooldownMin,
      reps: adjusted.reps,
      workSec: adjusted.workSec,
      recSec: adjusted.recSec,
    });
  }

  return { plan: adjusted, adjustmentNotes };
}

function computeReadinessTier({ readinessAmpel, readinessScore, fatigueSeverity, guardrailSeverity }) {
  if (guardrailSeverity === "hard" || readinessAmpel === "🔴") return "BAD";
  if (fatigueSeverity === "high" || (readinessAmpel === "🟠" && readinessScore < 55)) return "LOW";
  if (readinessAmpel === "🟢" || readinessScore >= 70) return "GOOD";
  if (readinessAmpel === "🟠" && readinessScore >= 55 && readinessScore <= 69) return "OK";
  return "OK";
}

function computeWorkoutProgressIndex({
  family,
  lastKeyInfo,
  readinessTier,
  guardrailSeverity,
  runFloorGap,
  hrvDeltaPct,
  driftCapActive,
}) {
  const familyHistory = (lastKeyInfo?.keyHistory || []).filter((entry) => entry.family === family);
  let index = familyHistory.length >= 3 ? 2 : familyHistory.length >= 2 ? 1 : 0;
  if (lastKeyInfo?.lastKeyFamily === family && readinessTier !== "LOW" && guardrailSeverity !== "hard" && !runFloorGap) {
    index += 1;
  }
  if (readinessTier === "LOW" || (hrvDeltaPct != null && hrvDeltaPct <= -12) || driftCapActive) {
    index -= 1;
  }
  return clamp(index, -1, 2);
}

function selectTemplateForFamily(family, progressIndex, lastWorkoutSignature) {
  const templates = WORKOUT_TEMPLATE_LIBRARY[family] || [];
  if (!templates.length) return null;
  const index = Math.min(Math.max(progressIndex, 0), templates.length - 1);
  let template = templates[index];
  if (lastWorkoutSignature && template?.id && lastWorkoutSignature === template.id && templates.length > 1) {
    template = templates[(index + 1) % templates.length];
  }
  return template;
}

function pickWarmupCooldown(template, { runFloorGap, scalingLevel }) {
  const [wuMin, wuMax] = template.warmupRangeMin;
  const [cdMin, cdMax] = template.cooldownRangeMin;
  if (runFloorGap || scalingLevel < 0) {
    return { warmupMin: wuMin, cooldownMin: cdMin };
  }
  const warmupMin = Math.round((wuMin + wuMax) / 2);
  const cooldownMin = Math.round((cdMin + cdMax) / 2);
  return { warmupMin, cooldownMin };
}

function computeIntensityCaps(keyType, scalingLevel) {
  if (scalingLevel >= 2) return 22;
  if (keyType === "vo2_touch") return 14;
  if (keyType === "racepace") return 18;
  return 18;
}

function applyScalingToTemplate({
  template,
  scalingLevel,
  keyType,
  weeksToEvent,
  readinessTier,
  runFloorGap,
}) {
  const baseReps = template.baseReps;
  const baseWorkSec = template.baseWorkSec;
  const baseRecSec = template.baseRecSec;

  let reps = baseReps;
  let workSec = baseWorkSec;
  let recSec = baseRecSec;

  if (scalingLevel === -2) {
    reps = Math.max(4, Math.round(baseReps * 0.6));
  } else if (scalingLevel === -1) {
    reps = Math.max(5, Math.round(baseReps * 0.8));
  } else if (scalingLevel >= 1) {
    const intensityCap = computeIntensityCaps(keyType, scalingLevel);
    const baseIntensityMin = toMinutes(baseReps * baseWorkSec);
    const canAddRep = toMinutes((baseReps + 1) * baseWorkSec) <= intensityCap;
    const canAddTwoRep = toMinutes((baseReps + 2) * baseWorkSec) <= intensityCap;
    const canAddWork = toMinutes(baseReps * (baseWorkSec + 30)) <= intensityCap;

    if (scalingLevel === 2 && weeksToEvent != null && weeksToEvent > 4 && readinessTier === "GOOD" && !runFloorGap) {
      if (canAddTwoRep) reps = baseReps + 2;
      else if (canAddRep) reps = baseReps + 1;
      else if (canAddWork) workSec = baseWorkSec + 30;
    } else if (scalingLevel >= 1) {
      if (canAddRep) reps = baseReps + 1;
      else if (canAddWork) workSec = baseWorkSec + 30;
    }
    if (baseIntensityMin > intensityCap) {
      reps = baseReps;
      workSec = baseWorkSec;
    }
  }

  const intensityMinutes = toMinutes(reps * workSec);

  return {
    reps,
    workSec,
    recSec,
    intensityMinutes,
  };
}

function computeTotalMinutes({ warmupMin, cooldownMin, reps, workSec, recSec }) {
  return warmupMin + cooldownMin + toMinutes(reps * (workSec + recSec));
}

function buildWorkoutPlan({
  decisionKeyType,
  weeksToEvent,
  readinessAmpel,
  readinessScore,
  guardrailSeverity,
  hrvDeltaPct,
  driftPct,
  confidenceLevel,
  fatigueSeverity,
  keyBudget,
  runFloorGap,
  lastKeyInfo,
  lastWorkoutSignature,
}) {
  const reasonIds = [];
  const guardrailBlock = guardrailSeverity === "hard";
  if (guardrailBlock || readinessAmpel === "🔴") {
    return { plan: null, reasonIds };
  }

  const readinessTier = computeReadinessTier({
    readinessAmpel,
    readinessScore,
    fatigueSeverity,
    guardrailSeverity,
  });

  if (readinessTier === "BAD") {
    return { plan: null, reasonIds };
  }

  const spacingOk = keyBudget?.spacingOk !== false;
  const keyBudgetAvailable =
    (keyBudget?.keyHardCount7 ?? 0) < (keyBudget?.keyHardMax ?? KEY_HARD_MAX_PER_7D) && !keyBudget?.capExceeded;
  if (!spacingOk || !keyBudgetAvailable) {
    return { plan: null, reasonIds };
  }

  let keyType = decisionKeyType;
  if (keyType === "vo2_touch" && guardrailSeverity === "soft") {
    keyType = "racepace";
    reasonIds.push("SOFT_GUARDRAIL_SWITCH_VO2_TO_RP");
  }

  const driftCapActive = driftPct != null && driftPct >= 5.5 && normalizeConfidenceLevel(confidenceLevel) !== "high";
  let scalingLevel = readinessTier === "GOOD" ? 1 : readinessTier === "LOW" ? -1 : 0;

  if (runFloorGap && scalingLevel > 0) {
    scalingLevel = 0;
    reasonIds.push("FLOOR_GAP_CAPS_PROGRESSION");
  }
  if (hrvDeltaPct != null && hrvDeltaPct <= -12 && scalingLevel > -1) {
    scalingLevel = -1;
    reasonIds.push("HRV_CAPS_WORKOUT");
  }
  if (driftCapActive && scalingLevel > -1) {
    scalingLevel = -1;
    reasonIds.push("DRIFT_CAPS_WORKOUT");
  }

  if (scalingLevel <= -2 && keyType === "vo2_touch") {
    keyType = "racepace";
  }

  const family = keyType === "vo2_touch" ? "vo2_touch" : "racepace";
  const progressIndex = computeWorkoutProgressIndex({
    family,
    lastKeyInfo,
    readinessTier,
    guardrailSeverity,
    runFloorGap,
    hrvDeltaPct,
    driftCapActive,
  });
  const template = selectTemplateForFamily(family, progressIndex, lastWorkoutSignature);
  if (!template) {
    return { plan: null, reasonIds: ["FALLBACK_STRIDES_NO_KEY_SLOT"] };
  }

  const { warmupMin, cooldownMin } = pickWarmupCooldown(template, { runFloorGap, scalingLevel });
  let scaled = applyScalingToTemplate({
    template,
    scalingLevel,
    keyType,
    weeksToEvent,
    readinessTier,
    runFloorGap,
  });

  if (scalingLevel === -2 && scaled.intensityMinutes < 6) {
    const strides = WORKOUT_TEMPLATE_LIBRARY.strides;
    scaled = applyScalingToTemplate({
      template: strides,
      scalingLevel: 0,
      keyType: "strides",
      weeksToEvent,
      readinessTier,
      runFloorGap,
    });
    const warmupCooldown = pickWarmupCooldown(strides, { runFloorGap, scalingLevel: 0 });
    const totalMinutes = computeTotalMinutes({
      warmupMin: warmupCooldown.warmupMin,
      cooldownMin: warmupCooldown.cooldownMin,
      reps: scaled.reps,
      workSec: scaled.workSec,
      recSec: scaled.recSec,
    });
    return {
      plan: {
        templateId: strides.id,
        keyType: "strides",
        family: "strides",
        reps: scaled.reps,
        workSec: scaled.workSec,
        recSec: scaled.recSec,
        warmupMin: warmupCooldown.warmupMin,
        cooldownMin: warmupCooldown.cooldownMin,
        intensityMinutes: scaled.intensityMinutes,
        totalMinutes,
        scalingLevel: 0,
      },
      reasonIds: ["FALLBACK_STRIDES_NO_KEY_SLOT"],
    };
  }

  let totalMinutes = computeTotalMinutes({
    warmupMin,
    cooldownMin,
    reps: scaled.reps,
    workSec: scaled.workSec,
    recSec: scaled.recSec,
  });

  if (runFloorGap && totalMinutes > 55) {
    let reps = scaled.reps;
    const minReps = Math.max(4, Math.min(reps, template.baseReps));
    while (reps > minReps) {
      reps -= 1;
      totalMinutes = computeTotalMinutes({
        warmupMin,
        cooldownMin,
        reps,
        workSec: scaled.workSec,
        recSec: scaled.recSec,
      });
      if (totalMinutes <= 55) break;
    }
    scaled.reps = reps;
    totalMinutes = computeTotalMinutes({
      warmupMin,
      cooldownMin,
      reps: scaled.reps,
      workSec: scaled.workSec,
      recSec: scaled.recSec,
    });
  }

  reasonIds.push(`WORKOUT_TEMPLATE_CHOSEN_${template.id}`);
  reasonIds.push(`WORKOUT_SCALED_${scalingLevel}`);

  return {
    plan: {
      templateId: template.id,
      keyType,
      family,
      reps: scaled.reps,
      workSec: scaled.workSec,
      recSec: scaled.recSec,
      warmupMin,
      cooldownMin,
      intensityMinutes: scaled.intensityMinutes,
      totalMinutes,
      scalingLevel,
    },
    reasonIds,
  };
}

function formatWorkoutKonkret(plan, { paceText = null, adjustmentNotes = [] } = {}) {
  if (!plan) return null;
  const warmup = `${formatMinuteBlock(plan.warmupMin)} EL`;
  const cooldown = `${formatMinuteBlock(plan.cooldownMin)} AL`;
  if (plan.keyType === "strides") {
    const work = formatIntervalSeconds(plan.workSec);
    const rec = formatIntervalSeconds(plan.recSec);
    return `${warmup}, ${plan.reps}×${work} zügig/locker (${rec} trab), ${cooldown}.`;
  }
  const work = formatIntervalSeconds(plan.workSec);
  const rec = formatIntervalSeconds(plan.recSec);
  const paceSuffix = plan.keyType === "racepace" && paceText ? ` Zielpace ca. ${paceText}.` : "";
  const adjustmentSuffix = adjustmentNotes.length ? ` Anpassung: ${adjustmentNotes.join(" | ")}.` : "";
  return `${warmup}, ${plan.reps}×${work} @ ${plan.keyType} (${rec} trab), ${cooldown}.${paceSuffix}${adjustmentSuffix}`;
}

function applyConfidenceTone(sentence, bucket) {
  if (!sentence) return sentence;
  if (bucket === "low") return `Beobachtung (Test): ${sentence}`;
  if (bucket === "medium") return `Beobachtung: ${sentence}`;
  return sentence;
}

function normalizeReasonText(reasonText) {
  if (!reasonText) return "";
  return String(reasonText).replace(/\.*\s*$/, "");
}

// FIX: single source of truth for build status text
function formatSteadyDecisionStatus(steadyDecision, { includeReason = false, includeDelayRange = false } = {}) {
  if (!steadyDecision) return "✖ STEADY_T gesperrt";
  const baseReason = normalizeReasonText(steadyDecision.reasonText);
  const reasonSuffix = includeReason && baseReason ? `: ${baseReason}` : "";
  const delayRange =
    includeDelayRange && steadyDecision.delayRange
      ? ` (${steadyDecision.delayRange.min}–${steadyDecision.delayRange.max}T)`
      : "";
  if (steadyDecision.status === "allowed") return `✔ STEADY_T erlaubt${reasonSuffix}`;
  if (steadyDecision.status === "delayed") return `⏳ STEADY_T verschoben${delayRange}${reasonSuffix}`;
  return `✖ STEADY_T gesperrt${reasonSuffix}`;
}

const INTERVAL_TEMPLATE_LIBRARY = {
  "5k": {
    BASE: [
      "GA5-1: 45–75′ GA1 locker",
      "STR5-1: 6–8×15–20″ Strides (volle Erholung)",
      "VO2T5-1: 8×20″/40″ (limitiert)",
      "HILL5-1: 6–8×8–10″ Hügel kurz",
    ],
    BUILD: [
      "ST5-1: 4×6′ @ Schwelle",
      "ST5-2: 5×5′ @ Schwelle",
      "VO25-1: 6×600 m @ VO₂max",
      "VO25-2: 5×1000 m @ VO₂max",
      "HRT5-1: 10×400 m Tempohärte",
    ],
    RACE: [
      "RP5-1: 6×400 m @ 5-km-Pace",
      "RP5-2: 3×1 km @ 5-km-Pace",
      "SHARP5-1: 8×200 m flott",
    ],
    RESET: ["GA5-1: 45–60′ GA locker", "STR5-1: 6×20″ Steigerungen"],
  },
  "10k": {
    BASE: [
      "GA10-1: 60–75′ GA1 locker",
      "STR10-1: 6×20″ Strides",
      "VO2T10-1: 6×20″ VO₂-Impulse (selten)",
      "HILL10-1: 60′ locker wellig",
    ],
    BUILD: [
      "ST10-1: 4×8′ @ Schwelle",
      "ST10-2: 3×10′ @ Schwelle",
      "I10-1: 5×1000 m @ 10-km-Pace",
      "I10-2: 4×2000 m @ 10-km-Pace",
      "TL10-1: 30–40′ Tempodauerlauf",
    ],
    RACE: ["RP10-1: 3×2 km @ 10-km-Pace", "RP10-2: 2×3 km @ 10-km-Pace", "CTRL10-1: 5×1 km kontrolliert"],
    RESET: ["GA10-1: 60–75′ locker", "STR10-1: 6×20″ Steigerungen"],
  },
  hm: {
    BASE: [
      "GAHM-1: 60–90′ GA1 locker",
      "LLHM-1: 100–130′ locker",
      "STRHM-1: 4–6×20″ Strides (optional)",
      "VO2THM-1: 4–6×15″ VO₂-Impulse (sehr selten)",
    ],
    BUILD: [
      "STHM-1: 3×12′ @ Schwelle",
      "STHM-2: 2×20′ @ Schwelle",
      "RPHM-1: 3×3 km @ HM-Pace",
      "RPHM-2: 2×5 km @ HM-Pace",
      "TDLHM-1: 40–60′ Tempodauerlauf",
    ],
    RACE: [
      "RPHM-1: 2×5 km @ HM-Pace",
      "RPHM-2: 3×3 km @ HM-Pace",
      "RHYHM-1: 10 km @ HM-Rhythmus",
    ],
    RESET: ["GAHM-1: 75–90′ locker", "STRHM-1: 4×20″ Strides"],
  },
  m: {
    BASE: [
      "GAM-1: 75–90′ GA1 locker",
      "LLM-1: 120–150′ locker",
      "STRM-1: 4×15″ Strides (selten)",
      "HILLM-1: 60′ locker, technisch wellig",
    ],
    BUILD: [
      "MPM-1: 3×5 km @ Marathonpace",
      "MPM-2: 2×8 km @ Marathonpace",
      "STM-1: 3×10′ Schwelle moderat",
      "LLM-2: 28 km mit 3×5 km @ M",
    ],
    RACE: [
      "MPM-1: 2×6–8 km @ Marathonpace",
      "MPM-2: 12–16 km @ M (10–14T vor Wettkampf)",
      "ACTM-1: 3×1′ flott (Aktivierung)",
    ],
    RESET: ["GAM-1: 80–100′ locker", "STRM-1: 6×20″ Steigerungen"],
  },
  default: {
    BASE: [
      "GA: 45–60′ locker",
      "STR: 6×20″ Steigerungen",
    ],
    BUILD: [
      "RP: 5×3′ @ Racepace, 2′ Trab",
      "ST: 3×10′ @ Schwelle, 2′ Trab",
      "VO2: 6×2′ zügig, 2′ Trab",
    ],
    RACE: ["RP: 3×6′ @ Racepace", "SHARP: 4×200 m flott"],
    RESET: ["GA: 45–60′ locker", "STR: 6×20″ Steigerungen"],
  },
};

function parseStructuredWorkoutTemplate(rawTemplate, { id, family, baseRecSec }) {
  const text = String(rawTemplate || "");
  const repsMatch = text.match(/(\d+)\s*×/u);
  const reps = Number.parseInt(repsMatch?.[1] || "", 10);
  if (!Number.isFinite(reps) || reps <= 0) return null;

  const minutesMatch = text.match(/×\s*(\d+)\s*′/u);
  const secondsMatch = text.match(/×\s*(\d+)\s*["″]|×\s*(\d+)\s*s\b/u);

  let workSec = null;
  if (minutesMatch) {
    workSec = Number.parseInt(minutesMatch[1], 10) * 60;
  } else if (secondsMatch) {
    workSec = Number.parseInt(secondsMatch[1] || secondsMatch[2], 10);
  }
  if (!Number.isFinite(workSec) || workSec <= 0) return null;

  return {
    id,
    family,
    label: text,
    baseReps: reps,
    baseWorkSec: workSec,
    baseRecSec,
    warmupRangeMin: [10, 15],
    cooldownRangeMin: [8, 12],
  };
}

function buildWorkoutTemplateLibraryFromIntervalLibrary() {
  const racepaceTemplates = [];
  const vo2Templates = [];

  const candidateLibrary = INTERVAL_TEMPLATE_LIBRARY?.default || {};
  const raceSources = [...(candidateLibrary.BUILD || []), ...(candidateLibrary.RACE || [])].filter((item) =>
    String(item || "").toUpperCase().includes("RP:"),
  );
  const vo2Sources = (candidateLibrary.BUILD || []).filter((item) => String(item || "").toUpperCase().includes("VO2:"));
  const stridesSource = (candidateLibrary.BASE || []).find((item) => String(item || "").toUpperCase().includes("STR:"));

  raceSources.forEach((source, idx) => {
    const parsed = parseStructuredWorkoutTemplate(source, {
      id: `RP${idx + 1}`,
      family: "racepace",
      baseRecSec: 120,
    });
    if (parsed) racepaceTemplates.push(parsed);
  });

  vo2Sources.forEach((source, idx) => {
    const parsed = parseStructuredWorkoutTemplate(source, {
      id: `VO2_${idx + 1}`,
      family: "vo2_touch",
      baseRecSec: 120,
    });
    if (parsed) vo2Templates.push(parsed);
  });

  const stridesTemplate =
    parseStructuredWorkoutTemplate(stridesSource, {
      id: "STRIDES",
      family: "strides",
      baseRecSec: 75,
    }) || {
      id: "STRIDES",
      family: "strides",
      label: "6×20″ Strides",
      baseReps: 6,
      baseWorkSec: 20,
      baseRecSec: 75,
      warmupRangeMin: [10, 15],
      cooldownRangeMin: [8, 12],
    };

  return {
    racepace: racepaceTemplates,
    vo2_touch: vo2Templates,
    strides: stridesTemplate,
  };
}

const WORKOUT_TEMPLATE_LIBRARY = buildWorkoutTemplateLibraryFromIntervalLibrary();

const LONGRUN_TARGET_LIBRARY = {
  "5k": { BASE: "60–75′", BUILD: "65–80′", RACE: "50–60′", RESET: "60–70′" },
  "10k": { BASE: "70–85′", BUILD: "75–90′", RACE: "60–75′", RESET: "70–80′" },
  hm: { BASE: "90–110′", BUILD: "95–120′", RACE: "75–95′", RESET: "85–100′" },
  m: { BASE: "120–150′", BUILD: "140–180′", RACE: "90–120′", RESET: "110–140′" },
  default: { BASE: "70–90′", BUILD: "75–95′", RACE: "60–75′", RESET: "70–85′" },
};

function normalizeEventDistanceKey(distance) {
  const raw = String(distance || "").toLowerCase();
  if (!raw) return "default";
  if (raw.includes("5k")) return "5k";
  if (raw.includes("10k")) return "10k";
  if (raw.includes("hm") || raw.includes("halb")) return "hm";
  if (raw.includes("marathon") || raw === "m") return "m";
  return "default";
}

function normalizeBlockKey(block) {
  const key = String(block || "").toUpperCase();
  if (key === "BASE" || key === "BUILD" || key === "RACE" || key === "RESET") return key;
  return "BASE";
}

function getTemplateByBlock(library, distanceKey, blockKey) {
  const selection = (library[distanceKey] || library.default || {})[blockKey] || library.default?.BASE || "";
  if (Array.isArray(selection)) return selection.join(" | ");
  return selection;
}

function buildDailyTrainingSuggestionLines({
  todayAction,
  readinessAmpel,
  steadyDecision,
  keyHardDecision,
  blockState,
  eventDistanceRaw,
  keyRules,
}) {
  const blockKey = normalizeBlockKey(blockState?.block);
  const distanceKey = normalizeEventDistanceKey(eventDistanceRaw);
  const intervalTemplate = getTemplateByBlock(INTERVAL_TEMPLATE_LIBRARY, distanceKey, blockKey);
  const longrunTarget = getTemplateByBlock(LONGRUN_TARGET_LIBRARY, distanceKey, blockKey);
  const allowedTypes = new Set(keyRules?.allowedKeyTypes || []);

  let todaySuggestion = "35–55′ locker (GA1) + 4–6 lockere Steigerungen optional.";
  if (todayAction === "kein Lauf") {
    todaySuggestion = "Ruhetag oder 20–40′ Spaziergang/Mobility.";
  } else if (todayAction === "locker mit kontrolliertem Reiz") {
    todaySuggestion = "45–60′ GA1 locker + 4–6×20″ Strides (optional), jederzeit abbrechbar.";
  }

  const canPush = readinessAmpel === "🟢";
  const allowKeyHard = canPush && keyHardDecision?.allowed;
  const allowSteady = canPush && steadyDecision?.allowSteady;
  const intervalSuggestion = allowKeyHard
    ? intervalTemplate
    : allowedTypes.has("strides")
        ? "Neuromuskulär: 4–8×15–20″ Strides, dazwischen volle Erholung."
        : "Heute keine Intervalle (locker/Regeneration).";

  return [
    "🎯 TRAININGSVORSCHLAG",
    `- Heute: ${todaySuggestion}`,
    `- Intervalle (wenn Key erlaubt): ${intervalSuggestion}`,
    `- Longrun (wenn dran): ${longrunTarget} locker`,
    "- Legende: GA1 = locker aerob, Strides = kurze Steigerungen mit voller Erholung.",
  ];
}

function buildHrr60ReadinessOutput({
  intervalMetrics,
  prevIntervalMetrics,
  isKeySession,
  hrv2dNegative,
  rampPct,
  acwr,
  intensityBudget,
}) {
  if (!intervalMetrics || !isKeySession) {
    return {
      machine: {
        hrr60: {
          status: "n/a",
          badge: "⚪",
          value: "n/a",
          zone: "n/a",
          trend: "n/a",
          confidence: "low",
          ruleHits: ["not_key_or_missing"],
          coachOneLiner: "HRR60 n/a – nur bei Key-Einheiten.",
          action: "n/a.",
        },
      },
      human: "HRR60 ⚪ n/a (Trend n/a) | Confidence: low | Action: n/a.",
      report: ["- HRR60 n/a (nur bei Key-Einheiten)."],
    };
  }

  const value = Number.isFinite(intervalMetrics?.HRR60_median) ? intervalMetrics.HRR60_median : null;
  const count = Number.isFinite(intervalMetrics?.HRR60_count) ? intervalMetrics.HRR60_count : 0;
  const min = Number.isFinite(intervalMetrics?.HRR60_min) ? intervalMetrics.HRR60_min : null;
  const max = Number.isFinite(intervalMetrics?.HRR60_max) ? intervalMetrics.HRR60_max : null;
  const prevValue = Number.isFinite(prevIntervalMetrics?.HRR60_median) ? prevIntervalMetrics.HRR60_median : null;
  const trendDelta = value != null && prevValue != null ? value - prevValue : null;
  const hasEnough = count >= 2;
  const ruleHits = [];

  const contextActive =
    !!hrv2dNegative ||
    (Number.isFinite(rampPct) && rampPct > 0.5) ||
    (Number.isFinite(acwr) && acwr > 1.2);
  if (hrv2dNegative) ruleHits.push("context:hrv_2d_negative");
  if (Number.isFinite(rampPct) && rampPct > 0.5) ruleHits.push("context:ramp_gt_50pct");
  if (Number.isFinite(acwr) && acwr > 1.2) ruleHits.push("context:acwr_gt_1_2");

  let zone = "n/a";
  let badge = "⚪";
  let status = "n/a";

  if (value != null && hasEnough) {
    if (value <= 5) {
      zone = "Extreme Red";
      badge = "🟥";
      status = "extreme_red";
      ruleHits.push("zone:extreme_red");
    } else if (value < 10) {
      zone = "Red";
      badge = "🔴";
      status = "red";
      ruleHits.push("zone:red");
    } else if (value < 20) {
      zone = "Caution";
      badge = "🟠";
      status = "caution";
      ruleHits.push("zone:caution");
    } else {
      zone = "Good";
      badge = "🟢";
      status = "ok";
      ruleHits.push("zone:good");
    }
  } else if (value != null && !hasEnough) {
    ruleHits.push("count_lt_2");
  }

  if (contextActive && (status === "red" || status === "extreme_red")) {
    status = "block";
    ruleHits.push("context:escalated");
  }

  let confidenceIndex = 0;
  if (value != null && hasEnough) {
    confidenceIndex = count >= 4 ? 2 : 1;
    if (min != null && max != null) {
      const spread = max - min;
      if (Number.isFinite(spread) && spread >= 12) {
        confidenceIndex = Math.max(confidenceIndex - 1, 0);
        ruleHits.push("stability:wide_range");
      }
      if (Number.isFinite(spread) && spread >= 18) {
        confidenceIndex = 0;
        ruleHits.push("stability:very_wide_range");
      }
    }
  }
  const confidence = confidenceIndex === 2 ? "high" : confidenceIndex === 1 ? "medium" : "low";

  const budgetOk =
    intensityBudget?.limits?.keyHardMax != null && intensityBudget?.keyHardCount != null
      ? intensityBudget.keyHardCount < intensityBudget.limits.keyHardMax
      : null;

  let coachOneLiner = "HRR60 n/a – kein verlässliches Signal.";
  let action = "n/a.";

  if (value != null && !hasEnough) {
    coachOneLiner = "HRR60 unsicher – zu wenig Intervalle.";
    action = "Kein harter Entscheid nur aus HRR60; andere Signale nutzen.";
  } else if (value != null) {
    if (status === "block") {
      coachOneLiner = "Erholung deutlich limitiert, Kontext verschärft.";
      action = "Keine Intensität für 48–72h, nur EASY/REST.";
    } else if (status === "extreme_red" || status === "red") {
      coachOneLiner = "Erholung klar limitiert.";
      action = "Heute nur easy, keine harte Intensität (24–48h).";
    } else if (status === "caution") {
      coachOneLiner = "Erholung verhalten – Vorsicht.";
      action = contextActive
        ? "Intensität verschieben, heute easy/locker."
        : "Intensität nur wenn Budget ok; nur „touch“, kein „hard“.";
    } else if (status === "ok") {
      coachOneLiner = "Erholung gut – aber kein Freifahrtschein.";
      action =
        budgetOk === false
          ? "Budget/Leitplanken begrenzen Intensität heute."
          : "Intensität ok, wenn Budget/Guardrails ok.";
    }
  }

  const valueText = value != null ? `${Math.round(value)}` : "n/a";
  const trendText =
    trendDelta == null
      ? "Trend n/a"
      : `${trendDelta > 0 ? "↑" : trendDelta < 0 ? "↓" : "→"} ${trendDelta > 0 ? "+" : ""}${Math.round(trendDelta)} vs last`;

  const humanLine = `HRR60 ${badge} ${valueText} bpm (${trendText}) | Confidence: ${confidence} | Action: ${action}`;

  const reportLines = [
    `- HRR60 ${badge} ${valueText} bpm (${trendText}).`,
    `- Confidence: ${confidence}${!hasEnough ? " (zu wenig Intervalle)" : ""}.`,
    `- Konsequenz: ${action}`,
  ];

  return {
    machine: {
      hrr60: {
        status,
        badge,
        value: value != null ? Math.round(value) : "n/a",
        zone,
        trend: trendDelta != null ? Math.round(trendDelta) : "n/a",
        confidence,
        ruleHits,
        coachOneLiner,
        action,
      },
    },
    human: humanLine,
    report: reportLines,
  };
}

// ================= COMMENT =================
function buildComments(
  {
    perRunInfo,
    latestGaSample,
    trend,
    motor,
    robustness,
    modeInfo,
    blockState,
    blockEffective,
    overrideInfo,
    reentryInfo,
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
    learningEvidence,
    learningNarrativeState,
    strategyDecision,
    intensityBudget,
    steadyDecision,
    keyHardDecision,
    decisionTrace,
    guardrailState,
    intensityClassToday,
    intensitySelection,
    readinessConfidence,
    lastKeyInfo,
    lastKeyIntervalInsights,
    ga21Context,
    intervalContext,
    gaComparableStats,
  },
  { debug = false } = {}
) {
  const hadKey = perRunInfo.some((x) => x.isKey);
  const hadGA = perRunInfo.some((x) => x.ga && !x.isKey);
  const hadAnyRun = perRunInfo.length > 0;
  const totalMinutesToday = Math.round(sum(perRunInfo.map((x) => x.moving_time || 0)) / 60);
  const repRun = pickRepresentativeGARun(perRunInfo);
  const repDisplayRun = repRun ?? latestGaSample;
  const repDisplayDate = repRun ? null : latestGaSample?.date ?? null;
  const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);
  const eventDistanceRaw = blockState?.eventDistance || getEventDistanceFromEvent(modeInfo?.nextEvent);
  const eventDistance = formatEventDistance(eventDistanceRaw);
  const daysToEvent = eventDate ? daysBetween(isoDate(new Date()), eventDate) : null;
  const blockPlanLabel = blockState?.block ?? "n/a";
  const effectiveBlockLabel = blockEffective || blockPlanLabel;

  const drift = Number.isFinite(repRun?.drift) ? repRun.drift : null;
  const displayDrift = Number.isFinite(repDisplayRun?.drift) ? repDisplayRun.drift : null;
  const displayEf = Number.isFinite(repDisplayRun?.ef) ? repDisplayRun.ef : null;
  const repEf = Number.isFinite(repRun?.ef) ? repRun.ef : null;
  const repVdot = repEf != null ? vdotLikeFromEf(repEf) : null;
  const displayVdot = displayEf != null ? vdotLikeFromEf(displayEf) : null;
  const personalDriftWarn = DRIFT_WARN_PCT;
  const personalDriftCritical = DRIFT_CRITICAL_PCT;
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
  const deloadSum21 = Number.isFinite(runFloorState?.sum21) ? Math.round(runFloorState.sum21) : null;
  const deloadTargetSum = RUN_FLOOR_DELOAD_SUM21_MIN;
  const deloadDelta = deloadSum21 != null ? deloadSum21 - deloadTargetSum : null;
  const deloadActiveDays = Number.isFinite(runFloorState?.activeDays21) ? runFloorState.activeDays21 : null;
  const overlayMode = runFloorState?.overlayMode ?? "NORMAL";
  const floorModeText =
    overlayMode === "DELOAD"
      ? "Deload aktiv (Soll fällt automatisch)"
      : overlayMode === "TAPER"
        ? "Taper aktiv (Soll reduziert)"
        : overlayMode === "RECOVER_OVERLAY"
          ? "Recovery-Overlay (Soll abgesenkt)"
          : "Build-Modus (Soll kann stufenweise steigen)";

  const subjectiveAvgNegative = recoverySignals?.subjectiveAvgNegative ?? null;
  const subjectiveNegative = recoverySignals?.subjectiveNegative ?? false;
  const hrv1dConcern = hrv1dNegative && (subjectiveAvgNegative == null || subjectiveAvgNegative >= 0.5);
  const hrv2dConcern = hrv2dNegative && (subjectiveAvgNegative == null || subjectiveAvgNegative >= 0.5);
  const hrr60Readiness = buildHrr60ReadinessOutput({
    intervalMetrics: intervalContext?.today ?? null,
    prevIntervalMetrics: intervalContext?.prev?.intervalMetrics ?? null,
    isKeySession: hadKey,
    hrv2dNegative: hrv2dConcern,
    rampPct: fatigue?.rampPct ?? null,
    acwr: fatigue?.acwr ?? null,
    intensityBudget,
  });

  const signalMap = {
    drift_high: driftSignal !== "green" && driftSignal !== "unknown",
    hrv_down: hrv1dConcern,
    hrv_2d_negative: hrv2dConcern,
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
    hrv1dConcern,
    !!recoverySignals?.sleepLow,
    !!fatigue?.override,
    subjectiveNegative,
  ];
  const warningCount = warningSignals.filter(Boolean).length;
  const warningSignalStates = [
    { label: 'Drift-Trend auffällig (🟠/🔴)', active: driftSignal === "orange" || driftSignal === "red" },
    { label: 'HRV 1T negativ', active: hrv1dConcern },
    { label: recoverySignals?.sleepLow ? 'Schlaf/Erholung suboptimal' : 'Schlaf/Erholung im Zielbereich', active: !!recoverySignals?.sleepLow },
    { label: fatigue?.override ? 'Belastung strukturell erhöht' : 'Belastung strukturell im Rahmen', active: !!fatigue?.override },
  ];

  const hardRedFlags = {
    hrv2dNegative: hrv2dConcern && !counterIndicator,
    confirmedOverloadHigh: !!highPattern,
    multiWarningPlusSubjectiveNegative: warningCount >= 2 && subjectiveNegative,
    painInjury: !!recoverySignals?.painInjury,
  };
  const hasHardRedFlag = Object.values(hardRedFlags).some(Boolean);

  const softRedFlags = {
    frequencyBelowSweetspot: freqCount14 != null && freqCount14 < sweetspotLow,
    driftNearWarn: drift != null && drift >= personalDriftWarn - 1 && drift < personalDriftCritical,
    // Runfloor-Gap bleibt Warnsignal, soll aber Intervalle nicht mehr direkt sperren.
    runFloorBelowTarget: false,
    sleepStressSuboptimal: !!recoverySignals?.sleepLow || hrv1dConcern,
    isolatedWarningSignal: warningCount === 1,
  };
  const hasSoftRedFlag = Object.values(softRedFlags).some(Boolean);

  let readinessAmpel = "🟢";
  if (hasHardRedFlag) readinessAmpel = "🔴";
  else if (hasSoftRedFlag) readinessAmpel = "🟠";

  const readinessDecision =
    readinessAmpel === "🔴"
      ? "Heute keine Intensität – Fokus auf Erholung."
      : steadyDecision?.allowSteady
        ? "Heute ist ein kontrollierter Schwellenreiz möglich, ohne zusätzlichen Ermüdungsaufbau."
        : "Heute bleiben wir beim geplanten Reiz und setzen keine zusätzliche Intensität.";

  const sleepMissing =
    recoverySignals?.sleepHours == null &&
    recoverySignals?.sleepQuality == null &&
    recoverySignals?.sleepScore == null;
  let readinessConf = readinessConfidence || computeReadinessConfidence({
    driftSignal,
    hrvDeltaPct,
    runLoad7,
    fatigueOverride: !!fatigue?.override,
    hadKey,
    counterIndicator,
    hrv1dNegative: hrv1dConcern,
    hrv2dNegative: hrv2dConcern,
    trend,
  });
  if (sleepMissing) readinessConf = downgradeConfidenceOneStep(readinessConf);
  const policyDecision = buildPolicyDecision({
    matchedPatterns: patternMatches,
    signalMap,
    confidenceScore: readinessConf.score,
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
  if (hrv1dConcern) proposedRules.push(`Wenn HRV <= ${HRV_NEGATIVE_THRESHOLD_PCT}% vs 7T an 2 Tagen, dann Intensität stoppen (Test über nächste 4 Wochen).`);
  if (driftSignal !== "green") proposedRules.push("Wenn Easy-Drift > Warnschwelle, dann Pace senken oder Lauf kürzen (3 Beobachtungen sammeln).");
  const keyTypeCounts7d = fatigue?.keyTypeCounts7d ? Object.keys(fatigue.keyTypeCounts7d).length : 0;
  if (keyTypeCounts7d > 1) {
    proposedRules.push("Pro Woche nur 1 Schwerpunkt: nur einen Key-Typ setzen, alles andere unterstützt.");
  }
  if (Number.isFinite(fatigue?.rampPct) && fatigue.rampPct > 0.1 && (intensityBudget?.keyHardCount ?? 0) > 0) {
    proposedRules.push("Nur eine Variable steigern: wenn Umfang steigt, Intensität stabil halten.");
  }

  const baseBlockLabel = blockState?.block === "BASE" ? "Base" : blockState?.block === "RACE" ? "Race" : blockState?.block === "RESET" ? "Reset" : "Build";
  const blockStatus =
    overlayMode === "DELOAD" ? "Deload" : overlayMode === "RECOVER_OVERLAY" || overlayMode === "TAPER" ? "Stabilisieren/Absorb" : baseBlockLabel;
  const blockGoal =
    blockStatus === "Deload"
      ? "Erholung priorisieren und Systeme beruhigen."
      : blockStatus === "Stabilisieren/Absorb"
        ? "Belastung aufnehmen und den Körper ruhig festigen."
        : blockStatus === "Base"
          ? "Fundament stabilisieren und Belastung ruhig aufbauen."
          : blockStatus === "Race"
            ? "Form zuspitzen, Frische schützen, keine Eskalation."
            : blockStatus === "Reset"
              ? "Reset und Systeme beruhigen, bevor neu aufgebaut wird."
              : "Kapazität behutsam ausbauen, ohne unnötigen Druck.";
  const blockRisk =
    hasHardRedFlag || recoverySignals?.painInjury
      ? "Zu viel Druck trotz Warnzeichen."
      : warningCount > 0
        ? "Zu viel Tempo statt sauberer Erholung."
        : "Zu viel Ehrgeiz ohne saubere Basis.";

  const blockDescriptionLines = buildBlockDescriptionLines({
    block: blockState?.block,
    eventDistance: eventDistanceRaw,
  });

  const readinessReasons = [];
  if (hrv2dConcern) readinessReasons.push("HRV 2T unter 7T-Niveau");
  else if (hrv1dConcern) readinessReasons.push("HRV 1T unter 7T-Niveau");
  if (driftSignal === "orange" || driftSignal === "red") readinessReasons.push("Drift erhöht");
  if (recoverySignals?.sleepLow) readinessReasons.push("Schlaf/Erholung angespannt");
  if (fatigue?.override) readinessReasons.push("Belastung strukturell erhöht");
  if (runFloorGap) readinessReasons.push("Runfloor-Lücke");
  if (recoverySignals?.painInjury) readinessReasons.push("Schmerz/Verletzung");
  if (subjectiveNegative) readinessReasons.push("subjektiv schwer");

  const readinessReason =
    readinessAmpel === "🔴"
      ? readinessReasons.length
        ? `Heute entlasten: ${readinessReasons.join(", ")}.`
        : "Heute entlasten, um Sicherheit zu halten."
      : readinessAmpel === "🟠"
        ? readinessReasons.length
          ? `Heute vorsichtig: ${readinessReasons.join(", ")}. 🟠 = mindestens ein weiches Warnsignal (z.B. Runfloor-Lücke).`
          : "Einige Signale sind angespannt, heute vorsichtig. 🟠 = mindestens ein weiches Warnsignal."
        : "Keine klaren Warnsignale, stabil für ruhige Belastung.";

  let fatigueSignalLine = null;
  if (recoverySignals?.painInjury) fatigueSignalLine = "Schmerz";
  else if (driftSignal === "red" || driftSignal === "orange") fatigueSignalLine = "Drift";
  else if (subjectiveNegative) fatigueSignalLine = "Gefühl";

  const loadLevel = hasHardRedFlag || recoverySignals?.painInjury || freqSignal === "red" || fatigue?.override ? "hoch" : warningCount > 0 ? "moderat" : "niedrig";
  const loadReasons = [];
  if (hasHardRedFlag) loadReasons.push("kritische Warnsignale");
  if (recoverySignals?.painInjury) loadReasons.push("Schmerz/Verletzung");
  if (freqSignal === "red") loadReasons.push("Frequenz > Obergrenze");
  if (fatigue?.override) loadReasons.push("Fatigue-Override");
  if (driftSignal === "orange" || driftSignal === "red") loadReasons.push("Drift erhöht");
  if (hrv2dConcern) loadReasons.push("HRV 2T niedrig");
  if (runFloorGap) loadReasons.push("Runfloor-Lücke");
  const loadReasonText = loadReasons.length ? loadReasons.join(", ") : "keine zusätzlichen Warnsignale";
  const loadConsequence =
    loadLevel === "hoch"
      ? "Heute nicht kompensieren, Erholung hat Priorität."
      : loadLevel === "moderat"
        ? "Heute ruhig bleiben und nichts erzwingen."
        : "Heute stabil bleiben und nicht eskalieren.";

  let todayAction = "locker + abbrechbar";
  if (recoverySignals?.painInjury || readinessAmpel === "🔴") {
    todayAction = "kein Lauf";
  } else if (intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.STEADY) {
    todayAction = "locker mit kontrolliertem Reiz";
  } else if (intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.STRIDES) {
    todayAction = "locker + Steigerungen";
  } else if (
    intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.RACEPACE ||
    intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.VO2_TOUCH
  ) {
    todayAction = "locker + Key-Reiz";
  }

  const decisionKeyType =
    intensitySelection?.keyType ??
    (intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.RACEPACE
      ? "racepace"
      : intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.VO2_TOUCH
        ? "vo2_touch"
        : null);
  const guardrailSeverity = guardrailState?.guardrailSeverity ?? "none";
  let workoutPlan = null;
  let workoutDebug = null;
  let workoutAdjustmentNotes = [];
  let workoutDisplayPlan = null;
  const lastRacePaceText = lastKeyIntervalInsights?.keyType === "racepace" ? lastKeyIntervalInsights?.paceText : null;
  if (todayAction === "locker + Key-Reiz" && decisionKeyType) {
    const planResult = buildWorkoutPlan({
      decisionKeyType,
      weeksToEvent,
      readinessAmpel,
      readinessScore: readinessConf?.score ?? 0,
      guardrailSeverity,
      hrvDeltaPct,
      driftPct: displayDrift,
      confidenceLevel: trend?.confidence ?? null,
      fatigueSeverity: fatigue?.severity ?? "low",
      keyBudget: {
        keyHardMax: KEY_HARD_MAX_PER_7D,
        keyHardCount7: intensityBudget?.keyHardCount ?? 0,
        spacingOk: keySpacing?.ok !== false,
        nextKeyEarliest: keySpacing?.nextAllowedIso ?? null,
        capExceeded: keyCompliance?.capExceeded ?? false,
      },
      runFloorGap,
      lastKeyInfo,
      lastWorkoutSignature: null,
    });
    workoutPlan = planResult.plan;
    const adjusted = applyIntervalInsightsToWorkoutPlan(workoutPlan, lastKeyIntervalInsights);
    workoutDisplayPlan = adjusted.plan;
    workoutAdjustmentNotes = adjusted.adjustmentNotes;
    workoutDebug = workoutPlan
      ? {
        chosenTemplateId: workoutPlan.templateId,
        scalingLevel: workoutPlan.scalingLevel,
        computedTotalMinutes: workoutPlan.totalMinutes,
        intensityMinutes: workoutPlan.intensityMinutes,
        reasonIds: planResult.reasonIds,
        adjustmentNotes: workoutAdjustmentNotes,
        adjustedRecSec: workoutDisplayPlan?.recSec ?? null,
        adjustedReps: workoutDisplayPlan?.reps ?? null,
      }
      : { chosenTemplateId: null, scalingLevel: null, computedTotalMinutes: null, intensityMinutes: null, reasonIds: planResult.reasonIds };
  }

  const todayStatusLine = buildTodayClassification({ hadAnyRun, hadKey, hadGA, totalMinutesToday });
  const modeLabel = modeInfo?.mode === "EVENT" ? "Event" : "Open";
  const nextEventLine = eventDate
    ? `${eventDistance || "Event"} am ${eventDate}${daysToEvent != null ? ` (in ${daysToEvent}T)` : ""}`
    : "kein Event geplant";
  const driftText = displayDrift == null ? "n/a" : `${displayDrift.toFixed(1)}%`;
  const efText = displayEf == null ? "n/a" : displayEf.toFixed(2);
  const vdotText = displayVdot == null ? "n/a" : displayVdot.toFixed(1);
  const gaSourceDate = repRun?.date ?? repDisplayDate ?? null;
  const intervalToday = intervalContext?.today ?? null;
  const intervalPrev = intervalContext?.prev?.intervalMetrics ?? null;
  const intervalDriftText = intervalToday?.HR_Drift_bpm != null ? `${fmtSigned1(intervalToday.HR_Drift_bpm)} bpm` : "n/a";
  const intervalDriftDelta =
    intervalToday?.HR_Drift_bpm != null && intervalPrev?.HR_Drift_bpm != null
      ? intervalToday.HR_Drift_bpm - intervalPrev.HR_Drift_bpm
      : null;
  const intervalHrr60Count = intervalToday?.HRR60_count ?? 0;
  const intervalDetectedCount = intervalToday?.intervals_detected_count;
  const intervalEligibleCount = intervalToday?.intervals_eligible_count ?? intervalHrr60Count;
  const intervalExcludedSummary = intervalToday?.excluded_summary_text ?? null;
  const intervalHrr60Text = (() => {
    if (intervalToday?.HRR60_median == null) return "n/a";
    const countText =
      Number.isFinite(intervalDetectedCount) &&
      intervalDetectedCount > 0 &&
      Number.isFinite(intervalEligibleCount) &&
      intervalEligibleCount !== intervalDetectedCount
        ? `${intervalEligibleCount}/${intervalDetectedCount} Intervalle`
        : `${intervalEligibleCount} Intervalle`;
    const summaryText = intervalExcludedSummary ? `; ${intervalExcludedSummary}` : "";
    return `${intervalToday.HRR60_median.toFixed(0)} bpm (${countText}${summaryText})`;
  })();
  const intervalHrr60Delta =
    intervalToday?.HRR60_median != null && intervalPrev?.HRR60_median != null
      ? intervalToday.HRR60_median - intervalPrev.HRR60_median
      : null;
  const gaDetailLine =
    repDisplayRun || repRun
      ? [
          `Drift ${displayDrift != null ? `${displayDrift.toFixed(1)}%` : "n/a"}`,
          `EF ${displayEf != null ? displayEf.toFixed(2) : "n/a"}`,
        ].join(" | ")
      : "n/a (kein GA-Lauf für Vergleich)";
  const intervalKeyMetricsLine = intervalToday
    ? `HF-Drift (Intervalle) ${intervalToday.HR_Drift_bpm != null ? `${fmtSigned1(intervalToday.HR_Drift_bpm)} bpm` : "n/a"} | HRR60 ${intervalHrr60Text}`
    : null;
  const keyMetricsLine = intervalKeyMetricsLine || `HF-Drift (Intervalle) ${intervalDriftText} | HRR60 ${intervalHrr60Text}`;
  const intervalContextParts = [];
  if (intervalHrr60Delta != null) {
    intervalContextParts.push(`Δ HRR60 ${fmtSigned1(intervalHrr60Delta)} bpm vs letzte`);
  }
  if (intervalDriftDelta != null) {
    intervalContextParts.push(`Δ HF-Drift ${fmtSigned1(intervalDriftDelta)} bpm vs letzte`);
  }
  if (intervalExcludedSummary && intervalToday?.HRR60_median == null) {
    intervalContextParts.push(`Ausschluss: ${intervalExcludedSummary}`);
  }
  const intervalContextLine = intervalContextParts.length ? `Intervall-Kontext: ${intervalContextParts.join(" | ")}` : null;
  const motorWeekly = buildMotorWeeklyExplanation(motor);
  const runEvaluationText = buildRunEvaluationText({ hadAnyRun, repRun, trend });
  const keyCount7 = keyCompliance?.actual7 ?? fatigue?.keyCount7 ?? intensityBudget?.keyAnyCount ?? null;
  const keyCapValue = formatKeyCapValue(dynamicKeyCap, fatigue?.keyCap ?? null);
  const keyCapReason = formatKeyCapReason(dynamicKeyCap);
  const keyBudgetFull =
    Number.isFinite(keyCount7) && Number.isFinite(keyCapValue) ? keyCount7 >= keyCapValue : false;

  const activeWarnings = warningSignalStates.filter((s) => s.active).map((s) => s.label);
  if (runFloorGap) activeWarnings.push("Runfloor-Lücke");
  if (recoverySignals?.painInjury) activeWarnings.push("Schmerz/Verletzung");

  const subjectiveAvgLine = buildSubjectiveAverageLine(recoverySignals);
  const subjectiveLine = subjectiveAvgLine ? subjectiveAvgLine : "Subjektiv: n/a.";

  const aerobicContextAvailable =
    Number.isFinite(trend?.efDeltaPct) || Number.isFinite(trend?.dv) || Number.isFinite(trend?.dd);

  const needsKey = keyCompliance?.freqOk === false || keyCompliance?.preferredMissing;
  const needsLongRun = (longRunSummary?.minutes ?? 0) < 60;

  const runsLast7 = Number.isFinite(loads7?.runCount7) ? loads7.runCount7 : null;
  const gaRuns7 = Number.isFinite(loads7?.gaRuns7) ? loads7.gaRuns7 : null;
  const longRuns7 = Number.isFinite(loads7?.longRuns7) ? loads7.longRuns7 : null;
  const monotony = isFiniteNumber(fatigue?.monotony) ? fatigue.monotony : null;
  const strain = isFiniteNumber(fatigue?.strain) ? fatigue.strain : null;
  const monotonyText = monotony != null ? monotony.toFixed(2) : "n/a";
  const strainText = strain != null ? Math.round(strain).toFixed(0) : "n/a";
  const blockGoalShort = blockGoal.replace(/,?\s*keine Eskalation\.?/i, "").replace(/\.$/, "");
  const blockLabel = (blockState?.block ?? blockStatus ?? "n/a").toUpperCase();
  const priorityParts = ["Qualität > Umfang", freqSignal === "red" ? "Frequenz drosseln" : "Frequenz halten"];
  const priorityLine = priorityParts.join(" | ");
  const keyTypeSummary = (() => {
    const counts = fatigue?.keyTypeCounts7d;
    if (!counts || !Object.keys(counts).length) return null;
    const labelMap = {
      schwelle: "Schwelle",
      vo2_touch: "VO2",
      racepace: "Racepace",
      steady: "Steady",
      strides: "Strides",
    };
    return Object.entries(counts)
      .sort((a, b) => b[1] - a[1])
      .map(([key]) => labelMap[key] || key)
      .join("/");
  })();
  const keySummaryLine =
    keyCount7 == null
      ? "Key: n/a"
      : `Key: ${keyCount7}${keyCount7 > 0 && keyTypeSummary ? ` (${keyTypeSummary})` : ""}`;
  const runMinutes7 = Number.isFinite(loads7?.runMinutes7) ? Math.round(loads7.runMinutes7) : null;
  const miniPlan = buildMiniPlanTargets({
    runsPerWeek: Number.isFinite(runsLast7) ? runsLast7 : 0,
    weeklyLoad: runLoad7,
    keyPerWeek: Number.isFinite(keyCount7) ? keyCount7 : 0,
  });
  const keyMax = Number.isFinite(keyCapValue) ? keyCapValue : KEY_HARD_MAX_PER_7D;
  const parseTargetRange = (text) => {
    const nums = String(text || "").match(/\d+/g)?.map((n) => Number(n)) ?? [];
    if (!nums.length) return { min: null, max: null };
    return { min: nums[0], max: nums[nums.length - 1] };
  };
  const runTargetRange = parseTargetRange(miniPlan.runTarget);
  const longRunMet = (longRuns7 ?? 0) >= 1;
  const runFloorMet = runTarget > 0 ? runLoad7 >= runTarget : false;
  const keyDoseNeeded = keyCompliance?.freqOk === false || keyCompliance?.preferredMissing;
  const keyDoseMet =
    keyCompliance?.capExceeded === true
      ? false
      : keyDoseNeeded
        ? (keyCount7 ?? 0) >= 1 && (keyCount7 ?? 0) <= keyMax
        : (keyCount7 ?? 0) <= keyMax;
  const frequencyMet = runTargetRange.min != null ? (runsLast7 ?? 0) >= runTargetRange.min : false;
  const coreGoals = [longRunMet, runFloorMet, keyDoseMet, frequencyMet];
  const coreGoalsMet = coreGoals.filter(Boolean).length;
  const coreGoalsTotal = coreGoals.length;
  const planDeviationCritical = !longRunMet || (runTarget > 0 && runFloorGap);
  const weeklyAmpel =
    coreGoalsMet <= 1 || (warningCount >= 2 && subjectiveNegative) || fatigue?.override
      ? "🔴"
      : coreGoalsMet >= 3 && !planDeviationCritical
        ? "🟢"
        : "🟠";
  const weeklyFazit =
    weeklyAmpel === "🟢"
      ? "🟢 Stabil – Struktur trägt."
      : weeklyAmpel === "🔴"
        ? "🔴 Instabil – Struktur bricht."
        : "🟠 Auf Kurs – Basis lückenhaft.";
  const weeklyWhy = (() => {
    if (!longRunMet) return "Warum: Longrun fehlt; Basis wirkt fragil und bremst die Blockwirkung.";
    if (runFloorGap) return "Warum: Runfloor unter Soll; Basiswoche nicht stabil genug für Progression.";
    if (!keyDoseMet) return "Warum: Key-Dosis passt nicht zur Woche; Qualität/Regeneration aus dem Gleichgewicht.";
    if (!frequencyMet) return "Warum: Frequenz unter Soll; Kontinuität fehlt für stabile Basis.";
    return "Warum: Kernziele erfüllt; Blockpriorität wird aktuell sauber bedient.";
  })();
  const learningHelps = [];
  if (freqSignal !== "red") learningHelps.push("Häufiger & kürzer hält dich stabil");
  if (keySpacing?.ok) learningHelps.push("Easy-Tage nach Key wirken");
  if (!learningHelps.length) learningHelps.push("Struktur wirkt stabil, weiter beobachten");
  const learningBrakes = [];
  if (needsLongRun) learningBrakes.push("Fehlende Longruns");
  if (driftSignal !== "green" && driftSignal !== "unknown") learningBrakes.push("Easy-Tempo oft zu hoch → Drift steigt");
  if (runFloorGap) learningBrakes.push("Runfloor-Lücke bremst Basis");
  if (!learningBrakes.length) learningBrakes.push("Keine klaren Bremser aktuell");
  const gaComparableLine =
    gaComparableStats?.n > 0 && gaComparableStats.efMed != null && gaComparableStats.driftMed != null
      ? `Messbasis GA comparable: n=${gaComparableStats.n} | EF(med)=${gaComparableStats.efMed.toFixed(
          5
        )} | Drift(med)=${gaComparableStats.driftMed.toFixed(1)}%`
      : null;
  const eventCountdownLine =
    Number.isFinite(daysToEvent) && daysToEvent >= 0
      ? `Zeit bis Event: ${
          daysToEvent >= 14 ? `${Math.round(daysToEvent / 7)} Wochen` : `${daysToEvent} Tage`
        }`
      : null;
  const planSoll = `Soll: ${miniPlan.runTarget} Läufe | Run-Load ${miniPlan.loadTarget} | 1× Longrun ≥60′ | max ${keyMax} Key`;
  const planIst = `Ist: ${runsLast7 ?? 0} Läufe | Run-Load ${runLoad7} | Longrun ${longRuns7 ?? 0}× | Key ${
    keyCount7 ?? 0
  }×`;
  const planConsequence = (() => {
    if (!longRunMet) return "Longrun zuerst stabilisieren, keine Eskalation.";
    if (runFloorGap) return "Frequenz/GA stabilisieren, erst dann Progression.";
    if (!keyDoseMet) return "Key-Dosis glätten, Qualität gezielt setzen.";
    if (!frequencyMet) return "Frequenz erhöhen, Woche wieder tragfähig machen.";
    return "Plan halten, minimal progressieren.";
  })();
  const planRating = `Bewertung: ${coreGoalsMet}/${coreGoalsTotal} Kernziele erreicht → ${
    planDeviationCritical ? `kritisch: ${planConsequence}` : planConsequence
  }`;
  const blockFitNotes = [];
  if (weeklyAmpel === "🟢") blockFitNotes.push("✔ Blockpriorität getroffen, Reize passen zur Phase.");
  if (!longRunMet) blockFitNotes.push("⚠ Longrun fehlt – Basis trägt die Blockziele noch nicht.");
  if (runFloorGap) blockFitNotes.push("⚠ Runfloor wacklig – Blockwirkung wird ausgebremst.");
  if (!keyDoseMet) blockFitNotes.push("⚠ Key-Dosis nicht sauber gesetzt – Qualität/Erholung ausbalancieren.");
  if (warningCount > 0 || subjectiveNegative) blockFitNotes.push("⚠ Erholungssignale beachten, Reize dosieren.");
  if (blockFitNotes.length < 2) blockFitNotes.push("✔ Keine strukturellen Konflikte zum Block erkennbar.");
  const confidenceEvidence = Number.isFinite(trend?.recentCount)
    ? trend.recentCount
    : Number.isFinite(ga21Context?.count)
      ? ga21Context.count
      : 0;
  const confidencePct =
    trend?.confidence === "hoch" ? 80 : trend?.confidence === "mittel" ? 60 : trend?.confidence === "niedrig" ? 40 : 40;
  const trainerDecision = (() => {
    if (!longRunMet) return "Diese Woche keine Eskalation – Longrun zuerst setzen, dann Qualität aufbauen.";
    if (runFloorGap) return "Frequenz/GA stabilisieren; Qualität nur, wenn die Basis steht.";
    if (!keyDoseMet) return "Plan halten, 1 Key sauber, restliche Läufe bewusst easy.";
    return "Plan halten, 1 Key sauber, Progression nur minimal.";
  })();
  const riskNotes = [];
  if (!longRunMet) riskNotes.push("Wenn Longrun fehlt → Ausdauerbasis stagniert, Blockwirkung bleibt flach.");
  if (runFloorGap) riskNotes.push("Wenn Runfloor niedrig bleibt → Belastbarkeit sinkt, Qualität wird brüchig.");
  if (driftSignal === "orange" || driftSignal === "red")
    riskNotes.push("Wenn Easy-Drift hoch bleibt → Ermüdung kumuliert, Fortschritt verzögert sich.");
  if (!riskNotes.length) riskNotes.push("Wenn Struktur wackelt → Blockziel verzögert sich leicht.");
  const weeklyFocusGoal = !longRunMet
    ? "1× Longrun 60–75′ locker."
    : runFloorGap
      ? `Runfloor stabilisieren: ${miniPlan.runTarget} Läufe locker.`
      : !keyDoseMet
        ? "1× Key (Schwelle/VO2) sauber, sonst easy."
        : "Struktur halten: 1× Key sauber, Longrun locker.";

  const weeklyReport = buildMondayReportLines({
    blockLabel,
    blockGoalShort,
    priorityLine,
    eventCountdownLine,
    weeklyFazit,
    weeklyWhy,
    blockFitNotes,
    learningHelps,
    learningBrakes,
    confidenceEvidence,
    confidencePct,
    trainerDecision,
    riskNotes,
    weeklyFocusGoal,
    keyMax,
    runTarget,
    runTargetFallback: miniPlan.loadTarget,
  });

  const topTriggers = [];
  if (runFloorGap) topTriggers.push("Runfloor-Gap");
  if (keyBudgetFull || keyCompliance?.capExceeded) topTriggers.push("Key-Budget voll");
  if (driftSignal === "orange" || driftSignal === "red") topTriggers.push("Drift erhöht");
  if (recoverySignals?.sleepLow) topTriggers.push("Schlaf low");
  if (fatigue?.override) topTriggers.push("Fatigue-Override");
  if (subjectiveNegative) topTriggers.push("Gefühl schwer");
  const topTriggerText = topTriggers.length ? topTriggers.slice(0, 2).join(" + ") : "keine dominanten Trigger";
  const keyStimulusRecommended =
    intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.RACEPACE ||
    intensitySelection?.intensityClass === INTENSITY_RECOMMENDATION_CLASS.VO2_TOUCH;
  const todayAllowed =
    readinessAmpel === "🔴"
      ? "nur EASY/REST"
      : keyStimulusRecommended
        ? "GA/STRIDES/RP"
        : keyHardDecision?.allowed && readinessAmpel === "🟢"
        ? "Key/Intervalle möglich"
        : steadyDecision?.allowSteady
          ? "GA + STEADY"
          : "GA/STRIDES";
  const nextKeyEarliest =
    keyCompliance?.nextKeyEarliest ||
    keySpacing?.nextAllowedIso ||
    (keyCompliance?.capExceeded ? "nach Ablauf 7T-Fenster" : "heute");

  const lines = [];
  lines.push(`🧱 Block (Plan): ${blockPlanLabel}`);
  if (overrideInfo?.blockEffective) {
    const tagText =
      Number.isFinite(overrideInfo?.dayIndex) && Number.isFinite(overrideInfo?.totalDays)
        ? ` (Tag ${overrideInfo.dayIndex}/${overrideInfo.totalDays})`
        : "";
    lines.push(`⏸ Override: ${overrideInfo.category}${tagText} → ${overrideInfo.blockEffective}`);
  }
  if (reentryInfo?.dayIndex) {
    const totalText = Number.isFinite(reentryInfo?.totalDays) ? reentryInfo.totalDays : "n/a";
    lines.push(`↩️ ReEntry: Tag ${reentryInfo.dayIndex}/${totalText} → REENTRY`);
  }
  lines.push(`→ Effektiv: ${effectiveBlockLabel}`);
  if (blockDescriptionLines?.length) {
    lines.push("");
    lines.push("🧱 BLOCK-KOMPASS");
    blockDescriptionLines.forEach((line) => lines.push(line));
  }
  lines.push("");
  lines.push("⚡ DAILY SUMMARY");
  lines.push(`- Ampel & Trigger: ${readinessAmpel} ${topTriggerText}`);
  lines.push(`- Heute erlaubt: ${todayAllowed}`);
  lines.push(`- Nächster Key frühestens: ${nextKeyEarliest}`);
  lines.push("");

  lines.push("🧭 DAILY REPORT – Entscheidungsebene");
  lines.push(`- Readiness-Ampel: ${readinessAmpel}`);
  lines.push(`- Aktive Warnsignale: ${activeWarnings.length ? activeWarnings.join(", ") : "keine"}`);
  lines.push(`- ${subjectiveLine}`);

  lines.push("");
  lines.push("🧭 TAGESSTATUS");
  lines.push(`- Heute: ${todayStatusLine}`);
  const trainerLine = "Trainer: Qualität vor Umfang.";
  lines.push(`- Kontext: ${nextEventLine} | ${trainerLine}`);
  if (hadKey && !hadGA) {
    lines.push(`- Key-Metriken: ${keyMetricsLine}`);
  } else {
    lines.push(`- Laufbewertung: ${runEvaluationText}`);
    if (intervalKeyMetricsLine) {
      lines.push(`- Key-Metriken: ${intervalKeyMetricsLine}`);
    }
  }
  if (hadGA) {
    const driftContext = ga21Context ? `${driftText} (Ø21T ${ga21Context.driftAvg.toFixed(1)}%)` : driftText;
    const efContext = ga21Context ? `${efText} (Ø21T ${ga21Context.efAvg.toFixed(2)})` : efText;
    const gaContextNote = gaSourceDate ? ` (letzter GA-Lauf ${gaSourceDate})` : "";
    lines.push(`- GA-Kontext: Drift ${driftContext} | EF ${efContext}${gaContextNote}`);
    if (gaDetailLine) {
      lines.push(`- GA-Werte: ${gaDetailLine}${gaContextNote}`);
    }
  }
  if (intervalContextLine) {
    lines.push(`- ${intervalContextLine}`);
  }

  lines.push("");
  lines.push("📈 BELASTUNG & KONSEQUENZ");
  lines.push(`- Heutige Belastung: ${loadLevel}`);
  if (runTarget > 0) {
    lines.push(`- Runfloor-Status: ${runLoad7} / Soll ${runTarget}${runFloorGap ? " (Lücke)" : ""}`);
  } else {
    lines.push("- Runfloor-Status: n/a");
  }
  if (runFloorState?.runfloorModeText) {
    lines.push(`- ${runFloorState.runfloorModeText}`);
  }
  lines.push(
    `- Deload-Status (21T): ${deloadSum21 ?? "n/a"} / Ziel ${deloadTargetSum}${
      deloadDelta != null ? ` (Δ ${deloadDelta >= 0 ? "+" : ""}${Math.round(deloadDelta)})` : ""
    }${deloadActiveDays != null ? ` | aktive Tage ${deloadActiveDays}/${RUN_FLOOR_DELOAD_ACTIVE_DAYS_MIN}` : ""}`
  );
  lines.push(`- Konsequenz: ${loadConsequence}`);

  lines.push("");
  lines.push("🔎 BEGRÜNDUNG & ZAHLEN");
  lines.push(`- HRV Δ (vs 7T): ${hrvDeltaPct != null ? formatSignedPct(hrvDeltaPct) : "n/a"}${hrv2dConcern ? " (2T negativ)" : ""}`);
  const sleepQualityLabel = recoverySignals?.sleepQualityLabel;
  const sleepScoreText = recoverySignals?.sleepScore != null ? `Score ${recoverySignals.sleepScore}` : null;
  const sleepQualityText =
    recoverySignals?.sleepQuality != null || recoverySignals?.sleepScore != null
      ? `${sleepQualityLabel || "unbekannt"}${sleepScoreText ? ` (${sleepScoreText})` : ""}`
      : null;
  const sleepHoursText = recoverySignals?.sleepHours != null ? `${recoverySignals.sleepHours.toFixed(1)}h` : null;
  const sleepParts = [sleepHoursText, sleepQualityText].filter(Boolean);
  lines.push(`- Schlaf: ${sleepParts.length ? sleepParts.join(" | ") : "n/a"}${recoverySignals?.sleepLow ? " (unter Basis)" : ""}`);
  if (sleepMissing) {
    lines.push("- Hinweis: Schlafdaten fehlen → Confidence -1 Stufe.");
  }
  lines.push(`- GA-Drift${gaSourceDate ? ` (letzter GA-Lauf ${gaSourceDate})` : ""}: ${driftText} | EF ${efText} | VDOT ${vdotText}`);
  lines.push(`- Load 7T: ${runLoad7} (vorher 7T: ${fatigue?.prev7Load != null ? Math.round(fatigue.prev7Load) : "n/a"})`);
  lines.push(`- Ramp/ACWR: ${fatigue?.rampPct != null ? formatSignedPct(fatigue.rampPct * 100) : "n/a"} | ${fatigue?.acwr != null ? fatigue.acwr.toFixed(2) : "n/a"}`);
  lines.push(
    `- Keys 7T: ${keyCount7 ?? "n/a"} / Cap ${keyCapValue ?? "n/a"}${keyCapReason ? ` (Regel: ${keyCapReason})` : ""} | Spacing ${keySpacing?.ok === false ? "zu eng" : "ok"}`
  );
  if (lastRacePaceText) {
    const racePaceDate = lastKeyIntervalInsights?.date ? ` (${lastKeyIntervalInsights.date})` : "";
    lines.push(`- Racepace (letzte Intervalle${racePaceDate}): ${lastRacePaceText}`);
  }

  if (hadKey) {
    lines.push("");
    lines.push("❤️ HRR60 READINESS");
    hrr60Readiness.report.forEach((line) => lines.push(line));
  }

  if (aerobicContextAvailable) {
    lines.push("");
    lines.push("🫁 AEROBER KONTEXT");
    lines.push(`- GA-Form: ${buildAerobicTrendLine(trend)}`);
    if (trend?.confidence) lines.push(`- Confidence-Level: ${trend.confidence}`);
  }

  lines.push("");
  return {
    dailyReportText: lines.join("\n"),
    weeklyReportLines: weeklyReport.lines,
    weeklyReportSections: weeklyReport.sections,
    wellnessComment: null,
    workoutDebug,
    hrr60Readiness,
  };
}

function buildTodayStatus({ hadAnyRun, hadKey, hadGA, totalMinutesToday }) {
  if (!hadAnyRun) return "Kein Lauf";
  const minutesText = totalMinutesToday > 0 ? `${totalMinutesToday}′ ` : "";
  if (hadKey && !hadGA) return `Lauf: ${minutesText}Key`;
  if (hadGA && !hadKey) return `Lauf: ${minutesText}locker`;
  if (hadKey && hadGA) return `Lauf: ${minutesText}GA + Key`;
  return `Lauf: ${minutesText}Lauf`;
}

function buildRunEvaluationText({ hadAnyRun, repRun, trend }) {
  if (!hadAnyRun) return "kein Lauf";
  if (!repRun) return "n/a (kein GA-Lauf für Vergleich)";
  const efAvg = trend?.efRecentAvg;
  const driftAvg = trend?.driftRecentMed;
  const efDelta = pct(repRun.ef, efAvg);
  const driftDelta = repRun.drift != null && driftAvg != null ? repRun.drift - driftAvg : null;
  const parts = [];
  if (efDelta != null) parts.push(`EF ${fmtSigned1(efDelta)}% vs Ø 28d`);
  if (driftDelta != null) parts.push(`Drift ${fmtSigned1(driftDelta)}%-Pkt vs Ø 28d`);
  if (!parts.length) return "n/a (zu wenig Vergleichsdaten)";
  let verdict = "gemischt";
  if (efDelta != null && driftDelta != null) {
    if (efDelta >= 1 && driftDelta <= -0.5) verdict = "besser";
    else if (efDelta <= -1 && driftDelta >= 0.5) verdict = "schwächer";
  } else if (efDelta != null) {
    if (efDelta >= 1) verdict = "besser";
    else if (efDelta <= -1) verdict = "schwächer";
  }
  return `${verdict} (${parts.join(" | ")})`;
}

function buildMotorCoachingComment(motor) {
  if (!Number.isFinite(motor?.value)) {
    return "kein belastbarer Wert – erst mehr vergleichbare GA-Läufe sammeln.";
  }
  const value = motor.value;
  const trendDown = Number.isFinite(motor?.dv) && motor.dv <= -1.5;
  const driftWorse = Number.isFinite(motor?.dd) && motor.dd >= 1;
  const trendNote = trendDown || driftWorse ? " Trend zeigt nach unten." : "";
  if (value >= 70) return `stark – Qualität halten, Progression dosiert möglich.${trendNote}`;
  if (value >= 55) return `stabil – Kontinuität sichern, keine Eskalation nötig.${trendNote}`;
  if (value >= 40) return `fragil – Basis stabilisieren, Reize klein halten.${trendNote}`;
  return `schwach – Fokus auf ruhige GA-Kontinuität und Erholung.${trendNote}`;
}

function buildMotorWeeklyExplanation(motor) {
  const motorLine = motor?.text || "🏎️ Motor-Index: n/a (keine vergleichbaren GA-Läufe im Fenster)";
  const coach = buildMotorCoachingComment(motor);
  const explanation =
    "Erklärung: Trend-Score aus vergleichbaren GA-Läufen; EF-Median 28T vs 28T davor und Drift-Median 14T vs 14T davor. Einzelwerte können gegen den Trend laufen.";
  return { motorLine, coach, explanation };
}

function buildTodayClassification({ hadAnyRun, hadKey, hadGA, totalMinutesToday }) {
  if (!hadAnyRun) return "Ruhetag (kein Lauf)";
  if (hadKey && hadGA) return "GA + Key (gemischt)";
  if (hadKey) return "Key (intensiv)";
  if (hadGA) return totalMinutesToday > 0 ? `Easy/GA ${totalMinutesToday}′` : "Easy/GA";
  return totalMinutesToday > 0 ? `Lauf ${totalMinutesToday}′` : "Lauf";
}

function deriveDailyIntensityClass(perRunInfo) {
  if (!perRunInfo?.length) return null;
  if (perRunInfo.some((run) => run.intensityClass === INTENSITY_CLASS.KEY_HARD)) {
    return INTENSITY_CLASS.KEY_HARD;
  }
  if (perRunInfo.some((run) => run.intensityClass === INTENSITY_CLASS.STEADY_T)) {
    return INTENSITY_CLASS.STEADY_T;
  }
  return INTENSITY_CLASS.EASY;
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
      efRecentAvg: null,
      efPrevAvg: null,
      driftRecentMed: null,
      driftPrevMed: null,
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

  const efDeltaPct = ((ef1 - ef0) / ef0) * 100;
  const dv = efDeltaPct;
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
    efDeltaPct,
    dv,
    dd,
    efRecentAvg: ef1,
    efPrevAvg: ef0,
    driftRecentMed: d1,
    driftPrevMed: d0,
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

async function computeGa21DayContext(ctx, dayIso) {
  const end = getHistoryWindowEnd(dayIso);
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  const samples = await gatherGASamples(ctx, endIso, 21, { comparable: false });
  if (!samples.length) return null;

  const efAvg = avg(samples.map((x) => x.ef));
  const driftAvg = avg(samples.map((x) => x.drift));
  if (efAvg == null || driftAvg == null) return null;

  return {
    count: samples.length,
    efAvg,
    driftAvg,
    vdotAvg: vdotLikeFromEf(efAvg),
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
    dv,
    dd,
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

async function computePreviousKeyIntervalInsights(ctx, dayIso, windowDays, excludeActivityIds) {
  const end = getHistoryWindowEnd(dayIso);
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  const keyHistory = [];

  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (d >= dayIso) continue;
    if (!hasKeyTag(a)) continue;
    if (excludeActivityIds?.has?.(a.id)) continue;
    const rawType = getKeyType(a);
    const keyType = normalizeKeyType(rawType, {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    keyHistory.push({ date: d, keyType, activity: a });
  }

  if (!keyHistory.length) return null;
  keyHistory.sort((a, b) => a.date.localeCompare(b.date));
  const lastEntry = keyHistory[keyHistory.length - 1];
  if (!lastEntry?.activity) return null;

  try {
    const streams = await getStreams(ctx, lastEntry.activity.id, STREAM_TYPES_INTERVAL);
    const intervalMetrics = computeIntervalMetricsFromStreams(streams, {
      intervalType: getIntervalTypeFromActivity(lastEntry.activity),
      activity: lastEntry.activity,
    });
    if (!intervalMetrics) return null;
    return {
      activityId: lastEntry.activity.id,
      date: lastEntry.date,
      keyType: lastEntry.keyType,
      intervalMetrics,
    };
  } catch {
    return null;
  }
}

async function computeIntervalContext(ctx, dayIso, perRunInfo) {
  const todayRun = pickRepresentativeIntervalRun(perRunInfo);
  if (!todayRun?.intervalMetrics) return null;

  const excludeIds = new Set([todayRun.activityId]);
  const prev = await computePreviousKeyIntervalInsights(ctx, dayIso, 21, excludeIds);
  return {
    today: todayRun.intervalMetrics,
    todayKeyType: todayRun.keyType,
    prev,
  };
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
  return { ...rep, insights };
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

function buildMondayReportLines({
  blockLabel,
  blockGoalShort,
  priorityLine,
  eventCountdownLine,
  weeklyFazit,
  weeklyWhy,
  blockFitNotes,
  learningHelps,
  learningBrakes,
  confidenceEvidence,
  confidencePct,
  trainerDecision,
  riskNotes,
  weeklyFocusGoal,
  keyMax,
  runTarget,
  runTargetFallback,
}) {
  const blockFitEmoji = String(weeklyFazit || "")
    .trim()
    .split(/\s+/)[0];
  const blockFitDetail = blockFitNotes?.length
    ? blockFitNotes[0].replace(/^[⚠✔]+\s*/u, "")
    : "n/a";
  const runTargetText = runTarget > 0 ? runTarget : runTargetFallback;

  const blockStatus = [
    "🏗️ BLOCK-STATUS",
    `Block: ${blockLabel}`,
    `Ziel: ${blockGoalShort}`,
    `Priorität: ${priorityLine}`,
    ...(eventCountdownLine ? [eventCountdownLine] : []),
    `Block-Fit: ${blockFitEmoji || "n/a"}`,
    `→ ${blockFitDetail}`,
  ];

  const weeklyVerdict = ["📊 WOCHENURTEIL (Trainer)", weeklyFazit, weeklyWhy];

  const learnings = [
    "🧠 LEARNINGS (nur das Relevante)",
    "Was funktioniert",
    ...learningHelps.slice(0, 3).map((item) => `• ${item}`),
    "Was dich bremst",
    ...learningBrakes.slice(0, 3).map((item) => `• ${item}`),
    `Confidence: ${confidencePct}% (${confidenceEvidence} Beobachtungen)`,
  ];

  const decision = [
    "🎯 ENTSCHEIDUNG & WOCHENZIEL",
    `Entscheidung: ${trainerDecision}`,
    "Wochenfokus (1 Punkt):",
    `👉 ${weeklyFocusGoal}`,
    "Leitplanken",
    `• max ${keyMax} Key/7T`,
    `• Runfloor ≥${runTargetText}`,
    "• Easy-Läufe: Drift unter Warnschwelle",
  ];

  const risk = ["⚠️ RISIKO-BLICK (2–3 Wochen)", ...riskNotes.slice(0, 2).map((item) => `• ${item}`)];

  const sections = { blockStatus, weeklyVerdict, learnings, decision, risk };
  const lines = [
    ...blockStatus,
    "",
    ...weeklyVerdict,
    "",
    ...learnings,
    "",
    ...decision,
    "",
    ...risk,
  ];

  return { sections, lines };
}

function buildMondayReportPreview() {
  const report = buildMondayReportLines({
    blockLabel: "BUILD",
    blockGoalShort: "Basis stärken und eine Key-Qualität pro Woche sauber setzen",
    priorityLine: "Qualität > Umfang | Frequenz halten",
    eventCountdownLine: "Zeit bis Event: 5 Wochen",
    weeklyFazit: "🟠 Auf Kurs – Basis lückenhaft.",
    weeklyWhy: "Warum: Longrun fehlt; Basis wirkt fragil und bremst die Blockwirkung.",
    blockFitNotes: [
      "⚠ Longrun fehlt – Basis trägt die Blockziele noch nicht.",
      "⚠ Runfloor wacklig – Blockwirkung wird ausgebremst.",
      "✔ Blockpriorität getroffen, Reize passen zur Phase.",
    ],
    learningHelps: ["Easy-Tage nach Key wirken", "Häufiger & kürzer hält dich stabil"],
    learningBrakes: ["Fehlende Longruns", "Runfloor-Lücke bremst Basis"],
    confidenceEvidence: 5,
    confidencePct: 60,
    trainerDecision: "Diese Woche keine Eskalation – Longrun zuerst setzen, dann Qualität aufbauen.",
    riskNotes: [
      "Wenn Longrun fehlt → Ausdauerbasis stagniert, Blockwirkung bleibt flach.",
      "Wenn Runfloor niedrig bleibt → Belastbarkeit sinkt, Qualität wird brüchig.",
    ],
    weeklyFocusGoal: "1× Longrun 60–75′ locker.",
    keyMax: 1,
    runTarget: 160,
    runTargetFallback: "150–210",
  });

  return report.lines.filter(Boolean).join("\n\n");
}

async function computeDetectiveNote(env, mondayIso, warmupSkipSec, windowDays) {
  const end = new Date(mondayIso + "T00:00:00Z");
  const newest = new Date(end.getTime() - 86400000);
  const start = new Date(end.getTime() - windowDays * 86400000);

  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(newest));
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

  // Compose note
  const title = `🕵️‍♂️ Montags-Report (${windowDays}T)`;
  const loadSummary = (() => {
    if (totalRuns === 0) return "Kernaussage: keine belastbare Basis.";
    if (longRuns.length === 0) return "Kernaussage: Umfang nicht kritisch – Verteilung schon.";
    if (weeklyLoad < 120) return "Kernaussage: Run-Load niedrig – Basis wacklig.";
    if (keyRuns.length === 0) return "Kernaussage: Qualität fehlt – Basis zwar da, aber ohne Reiz.";
    return "Kernaussage: Struktur ok, Basis tragfähig.";
  })();
  const loadBasis = [
    "📈 BELASTUNG & BASIS (kompakt)",
    `Läufe: ${totalRuns}`,
    `Run-Load Ø/Woche: ${Math.round(weeklyLoad)}`,
    `Key: ${keyRuns.length}×`,
    `Longrun ≥60′: ${longRuns.length}×${longRuns.length === 0 ? " ⚠" : ""}`,
    `GA-Messbasis (vergleichbar): n=${comp.n ?? 0}`,
    `Drift (med): ${comp.driftMed != null ? `${comp.driftMed.toFixed(1)} %` : "n/a"}`,
    loadSummary,
  ];

  const lines = [title, "", ...loadBasis];
  const sections = { title, loadBasis };

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

  return { ok, text: lines.filter(Boolean).join("\n\n"), summary, sections };
}

async function gatherComparableGASamples(env, endDayIso, warmupSkipSec, windowDays) {
  const end = new Date(endDayIso + "T00:00:00Z");
  const newest = new Date(end.getTime() - 86400000);
  const start = new Date(end.getTime() - windowDays * 86400000);
  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(newest));

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
      const hrr60Count = intervalMetrics.HRR60_count ?? 0;
      lines.push(`Erholung: HRR60 ${intervalMetrics.HRR60_median.toFixed(0)} bpm (${hrr60Count} Intervalle)`);
    } else if (intervalMetrics) {
      lines.push("Erholung: HRR60 n/a (keine geeigneten Intervalle)");
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
    activity: a,
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

function formatPaceSeconds(secPerKm) {
  if (!Number.isFinite(secPerKm) || secPerKm <= 0) return null;
  const totalSec = Math.round(secPerKm);
  const minutes = Math.floor(totalSec / 60);
  const seconds = totalSec % 60;
  return `${minutes}:${String(seconds).padStart(2, "0")}/km`;
}

function formatTimeSeconds(totalSec) {
  if (!Number.isFinite(totalSec) || totalSec <= 0) return null;
  const rounded = Math.round(totalSec);
  const hours = Math.floor(rounded / 3600);
  const minutes = Math.floor((rounded % 3600) / 60);
  const seconds = rounded % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, "0")}:${String(seconds).padStart(2, "0")}`;
  }
  return `${minutes}:${String(seconds).padStart(2, "0")}`;
}

function parseTimeToSeconds(raw) {
  if (raw == null) return null;
  if (Number.isFinite(raw)) {
    if (raw <= 0) return null;
    return raw >= 300 ? raw : raw * 60;
  }
  const s = String(raw).trim();
  if (!s) return null;
  if (s.includes(":")) {
    const parts = s.split(":").map((p) => Number(p));
    if (parts.some((p) => !Number.isFinite(p))) return null;
    if (parts.length === 3) {
      const [h, m, sec] = parts;
      return h * 3600 + m * 60 + sec;
    }
    if (parts.length === 2) {
      const [m, sec] = parts;
      return m * 60 + sec;
    }
  }
  const numeric = Number(s.replace(",", "."));
  if (!Number.isFinite(numeric) || numeric <= 0) return null;
  return numeric >= 300 ? numeric : numeric * 60;
}

function getDistanceKmFromKey(distanceKey) {
  const key = String(distanceKey || "").toLowerCase();
  if (key === "5k") return 5;
  if (key === "10k") return 10;
  if (key === "hm" || key === "half" || key === "half_marathon") return 21.0975;
  if (key === "m" || key === "marathon") return 42.195;
  return null;
}

function formatPaceDeltaSeconds(deltaSec) {
  if (!Number.isFinite(deltaSec)) return null;
  const rounded = Math.round(deltaSec);
  const sign = rounded > 0 ? "+" : "";
  return `${sign}${rounded}s/km`;
}

function formatPaceFromSpeed(speedMps) {
  if (!Number.isFinite(speedMps) || speedMps <= 0) return null;
  return formatPaceSeconds(1000 / speedMps);
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

function hasIntervalKeyTag(activity) {
  const tags = normalizeTags(activity?.tags);
  return tags.some((t) => t === "key:vo2" || t === "key:racepace" || t === "key:interval");
}

function extractRacepaceSecPerKm(activity) {
  const candidates = [
    activity?.racepace_sec_per_km,
    activity?.race_pace_sec_per_km,
    activity?.racepaceSecPerKm,
    activity?.racePaceSecPerKm,
    activity?.race_pace,
    activity?.racepace,
  ];
  for (const value of candidates) {
    const v = Number(value);
    if (Number.isFinite(v) && v > 0) return v;
  }
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

function buildWorkIntervalsFromSignal(time, signal, { minIntervalSec = 60, maxGapSec = 5 } = {}) {
  const n = Math.min(time.length, signal.length);
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
    const isWork = !!signal[i];
    if (isWork) {
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

function mergeWorkIntervals(intervals, { mergeGapSec = 12, detectedMinIntervalSec = 75 } = {}) {
  if (!Array.isArray(intervals) || intervals.length < 2) return intervals ?? [];
  const merged = [];
  let current = { ...intervals[0] };

  for (let i = 1; i < intervals.length; i++) {
    const next = intervals[i];
    const gapSec = next.startTime - current.endTime;
    if (gapSec <= mergeGapSec) {
      const combinedDuration = next.endTime - current.startTime;
      if (combinedDuration >= detectedMinIntervalSec) {
        current = {
          startIdx: current.startIdx,
          endIdx: next.endIdx,
          startTime: current.startTime,
          endTime: next.endTime,
          duration: combinedDuration,
        };
        continue;
      }
    }
    merged.push(current);
    current = { ...next };
  }

  merged.push(current);
  return merged;
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

function deriveSpeedThreshold(speed, config, activity, tagPresent) {
  if (!Array.isArray(speed)) return null;
  const speedVals = speed.filter((x) => Number.isFinite(x));
  if (!speedVals.length) return null;
  const racepaceSecPerKm = extractRacepaceSecPerKm(activity);
  if (Number.isFinite(racepaceSecPerKm)) {
    const thresholdSec = racepaceSecPerKm + config.racepaceToleranceSecPerKm;
    return thresholdSec > 0 ? 1000 / thresholdSec : null;
  }
  const fallback = quantile(speedVals, tagPresent ? config.tagSpeedQuantile : config.racepaceSpeedQuantile);
  return Number.isFinite(fallback) ? fallback : null;
}

function derivePowerThreshold(watts, config, tagPresent) {
  if (!Array.isArray(watts)) return null;
  const vals = watts.filter((x) => Number.isFinite(x));
  if (!vals.length) return null;
  const threshold = quantile(vals, tagPresent ? config.tagPowerQuantile : config.vo2PowerQuantile);
  return Number.isFinite(threshold) ? threshold : null;
}

function deriveHrThreshold(hr, config) {
  if (!Array.isArray(hr)) return null;
  const vals = hr.filter((x) => Number.isFinite(x) && x > 0);
  if (!vals.length) return null;
  const maxHr = Math.max(...vals);
  const ratioThreshold = Number.isFinite(maxHr) ? maxHr * config.hrZoneZ4Ratio : null;
  if (Number.isFinite(ratioThreshold)) return ratioThreshold;
  const fallback = quantile(vals, config.hrZoneZ4Quantile);
  return Number.isFinite(fallback) ? fallback : null;
}

function buildHrr60IntervalsFromStreams({ time, speed, watts, hr, activity, config }) {
  const tagPresent = hasIntervalKeyTag(activity);
  const speedThreshold = deriveSpeedThreshold(speed, config, activity, tagPresent);
  const powerThreshold = derivePowerThreshold(watts, config, tagPresent);
  const hrThreshold = deriveHrThreshold(hr, config);

  const n = Math.min(time.length, hr.length, speed?.length ?? time.length, watts?.length ?? time.length);
  if (n < 2) return { intervals: [], hrThreshold };

  if (!Number.isFinite(speedThreshold) && !Number.isFinite(powerThreshold) && !Number.isFinite(hrThreshold)) {
    return { intervals: [], hrThreshold };
  }

  const signal = new Array(n).fill(false);
  const hasSpeedThreshold = Number.isFinite(speedThreshold);
  const hasPowerThreshold = Number.isFinite(powerThreshold);
  const useHrSignal = !hasSpeedThreshold && !hasPowerThreshold;
  for (let i = 0; i < n; i++) {
    const vSpeed = Number(speed?.[i]);
    const vWatts = Number(watts?.[i]);
    const vHr = Number(hr[i]);
    const speedOk = hasSpeedThreshold && Number.isFinite(vSpeed) && vSpeed >= speedThreshold;
    const powerOk = hasPowerThreshold && Number.isFinite(vWatts) && vWatts >= powerThreshold;
    const hrOk = Number.isFinite(hrThreshold) && Number.isFinite(vHr) && vHr >= hrThreshold;
    signal[i] = speedOk || powerOk || (useHrSignal && hrOk);
  }

  const intervals = buildWorkIntervalsFromSignal(time, signal, {
    minIntervalSec: config.detectionMinIntervalSec,
    maxGapSec: config.maxGapSec,
  });

  return { intervals, hrThreshold };
}

function computeHrr60Summary({ intervals, hr, time, hrThreshold, config }) {
  const excludedReasonsCount = {
    insufficient_recovery_data: 0,
    hr_dropout: 0,
    too_short: 0,
    no_hr_peak: 0,
  };
  const mergedIntervals = mergeWorkIntervals(intervals, config);
  const detectedIntervals = mergedIntervals.filter(
    (interval) => interval.duration >= config.detectedMinIntervalSec,
  );
  const intervalsDetectedCount = detectedIntervals.length;

  const buildExcludedSummaryText = (reasons) => {
    const labels = {
      insufficient_recovery_data: "zu wenig Recovery-Daten",
      hr_dropout: "HR-Dropout",
      too_short: "Belastung zu kurz",
      no_hr_peak: "kein HF-Peak",
    };
    const entries = Object.entries(reasons)
      .filter(([, count]) => count > 0)
      .sort((a, b) => b[1] - a[1]);
    if (!entries.length) return null;
    const parts = [];
    for (const [key, count] of entries) {
      const label = labels[key];
      if (!label) continue;
      const part = `${count}× ${label}`;
      const nextText = parts.length ? `${parts.join(", ")}, ${part}` : part;
      if (nextText.length > 40) break;
      parts.push(part);
      if (parts.length >= 2) break;
    }
    return parts.length ? parts.join(", ") : null;
  };

  if (!detectedIntervals.length) {
    return {
      HRR60_median: null,
      HRR60_count: 0,
      HRR60_min: null,
      HRR60_max: null,
      HR_peak_median: null,
      HR_60s_median: null,
      HRR60_values: [],
      intervals_detected_count: 0,
      intervals_eligible_count: 0,
      excluded_reasons_count: excludedReasonsCount,
      excluded_summary_text: null,
    };
  }

  const n = Math.min(hr.length, time.length);
  const timeAt = (i) => {
    const t = Number(time[i]);
    return Number.isFinite(t) ? t : i;
  };

  const hrr60Drops = [];
  const peaks = [];
  const hr60s = [];

  for (const interval of detectedIntervals) {
    if (interval.duration < config.minIntervalSec) {
      excludedReasonsCount.too_short += 1;
      continue;
    }

    let peak = -Infinity;
    for (let i = interval.startIdx; i <= interval.endIdx; i++) {
      const h = Number(hr[i]);
      if (!Number.isFinite(h) || h <= 0) continue;
      if (h > peak) peak = h;
    }
    if (!Number.isFinite(peak)) {
      excludedReasonsCount.no_hr_peak += 1;
      continue;
    }
    if (Number.isFinite(hrThreshold) && peak < hrThreshold) {
      excludedReasonsCount.no_hr_peak += 1;
      continue;
    }

    const windowEnd = interval.endTime + config.minValidHrWindowSec;
    let firstValidTime = null;
    let lastValidTime = null;
    let dropoutTooLong = false;

    for (let i = interval.endIdx; i < n; i++) {
      const t = timeAt(i);
      if (t < interval.endTime) continue;
      if (t > windowEnd) break;
      const h = Number(hr[i]);
      if (!Number.isFinite(h) || h <= 0) continue;
      if (firstValidTime == null) {
        if (t - interval.endTime > config.maxHrDropoutSec) dropoutTooLong = true;
        firstValidTime = t;
        lastValidTime = t;
      } else {
        if (t - lastValidTime > config.maxHrDropoutSec) dropoutTooLong = true;
        lastValidTime = t;
      }
    }

    if (dropoutTooLong || firstValidTime == null || lastValidTime == null) {
      excludedReasonsCount.hr_dropout += 1;
      continue;
    }
    if (lastValidTime - interval.endTime < config.minValidHrWindowSec) {
      excludedReasonsCount.insufficient_recovery_data += 1;
      continue;
    }

    const target = interval.endTime + 60;
    let hr60 = null;
    let bestDelta = Infinity;
    const tol = config.hrr60ExactToleranceSec;
    for (let i = interval.endIdx; i < n; i++) {
      const t = timeAt(i);
      if (t < target - tol) continue;
      if (t > target + tol) break;
      const h = Number(hr[i]);
      if (!Number.isFinite(h) || h <= 0) continue;
      const delta = Math.abs(t - target);
      if (delta < bestDelta) {
        bestDelta = delta;
        hr60 = h;
      }
    }

    if (!Number.isFinite(hr60)) {
      const start = interval.endTime + config.hrr60FallbackWindowSec.start;
      const end = interval.endTime + config.hrr60FallbackWindowSec.end;
      let minHr = null;
      for (let i = interval.endIdx; i < n; i++) {
        const t = timeAt(i);
        if (t < start) continue;
        if (t > end) break;
        const h = Number(hr[i]);
        if (!Number.isFinite(h) || h <= 0) continue;
        if (minHr == null || h < minHr) minHr = h;
      }
      hr60 = minHr;
    }

    if (!Number.isFinite(hr60)) {
      excludedReasonsCount.insufficient_recovery_data += 1;
      continue;
    }

    hrr60Drops.push(peak - hr60);
    peaks.push(peak);
    hr60s.push(hr60);
  }

  const hrr60Median = hrr60Drops.length ? median(hrr60Drops) : null;
  const hrPeakMedian = median(peaks.filter((x) => Number.isFinite(x)));
  const hr60Median = median(hr60s.filter((x) => Number.isFinite(x)));

  return {
    HRR60_median: Number.isFinite(hrr60Median) ? hrr60Median : null,
    HRR60_count: hrr60Drops.length,
    HRR60_min: hrr60Drops.length ? Math.min(...hrr60Drops) : null,
    HRR60_max: hrr60Drops.length ? Math.max(...hrr60Drops) : null,
    HR_peak_median: Number.isFinite(hrPeakMedian) ? hrPeakMedian : null,
    HR_60s_median: Number.isFinite(hr60Median) ? hr60Median : null,
    HRR60_values: hrr60Drops,
    intervals_detected_count: intervalsDetectedCount,
    intervals_eligible_count: hrr60Drops.length,
    excluded_reasons_count: excludedReasonsCount,
    excluded_summary_text: buildExcludedSummaryText(excludedReasonsCount),
  };
}

function computeIntervalMetricsFromStreams(streams, { intervalType, activity } = {}) {
  const hr = streams?.heartrate;
  const time = streams?.time;
  if (!Array.isArray(hr) || !Array.isArray(time)) return null;

  const intensityInfo = pickIntervalIntensity(streams);
  const speed = streams?.velocity_smooth;
  const watts = streams?.watts;

  const n = Math.min(hr.length, time.length, speed?.length ?? time.length, watts?.length ?? time.length);
  if (n < 2) return null;

  const timeSlice = time.slice(0, n);
  const hrSlice = hr.slice(0, n);
  const speedSlice = Array.isArray(speed) ? speed.slice(0, n) : null;
  const wattsSlice = Array.isArray(watts) ? watts.slice(0, n) : null;

  const hrr60Intervals = buildHrr60IntervalsFromStreams({
    time: timeSlice,
    speed: speedSlice,
    watts: wattsSlice,
    hr: hrSlice,
    activity,
    config: HRR60_INTERVAL_CONFIG,
  });
  const hrr60Summary = computeHrr60Summary({
    intervals: hrr60Intervals.intervals,
    hr: hrSlice,
    time: timeSlice,
    hrThreshold: hrr60Intervals.hrThreshold,
    config: HRR60_INTERVAL_CONFIG,
  });

  let driftMetrics = null;
  if (intensityInfo) {
    const intensity = intensityInfo.data.slice(0, n);
    const intensityVals = intensity.filter((x) => Number.isFinite(x));
    const threshold = quantile(intensityVals, 0.75);
    if (Number.isFinite(threshold)) {
      const intervals = buildWorkIntervals(timeSlice, intensity, { threshold });
      if (intervals.length >= 2) {
        const durations = intervals.map((i) => i.duration);
        const minDur = Math.min(...durations);
        const maxDur = Math.max(...durations);
        if (minDur > 0 && maxDur / minDur <= 1.1) {
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
          if (validIntensity.length === intervals.length) {
            const minIntensity = Math.min(...validIntensity);
            const maxIntensity = Math.max(...validIntensity);
            if (minIntensity > 0 && maxIntensity / minIntensity <= 1.1) {
              const avgIntensity = avg(validIntensity);
              const intervalAvgSpeedMps =
                intensityInfo.kind === "speed" && Number.isFinite(avgIntensity) ? avgIntensity : null;
              const intervalPaceSecPerKm =
                intervalAvgSpeedMps != null && intervalAvgSpeedMps > 0 ? 1000 / intervalAvgSpeedMps : null;

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
                for (let i = interval.startIdx; i <= interval.endIdx; i++) {
                  const t = timeAt(i);
                  const h = Number(hrSlice[i]);
                  if (!Number.isFinite(h)) continue;
                  if (t >= lateStart && t <= endTime) {
                    lateSum += h;
                    lateCount++;
                  }
                }
                const lateAvg = lateCount ? lateSum / lateCount : null;

                return {
                  lateAvg,
                };
              });

              const first = intervalHr[0]?.lateAvg;
              const last = intervalHr[intervalHr.length - 1]?.lateAvg;
              if (Number.isFinite(first) && Number.isFinite(last) && first > 0) {
                const hrDriftBpm = last - first;
                const hrDriftPct = ((last - first) / first) * 100;
                driftMetrics = {
                  HR_Drift_bpm: hrDriftBpm,
                  HR_Drift_pct: hrDriftPct,
                  drift_flag: classifyIntervalDrift(intervalType, hrDriftBpm),
                  interval_avg_speed_mps: intervalAvgSpeedMps,
                  interval_pace_sec_per_km: intervalPaceSecPerKm,
                };
              }
            }
          }
        }
      }
    }
  }

  const hasIntervals = hrr60Intervals.intervals.length > 0;
  if (!driftMetrics && !hasIntervals) return null;

  return {
    ...(driftMetrics || {
      HR_Drift_bpm: null,
      HR_Drift_pct: null,
      drift_flag: null,
      interval_avg_speed_mps: null,
      interval_pace_sec_per_km: null,
    }),
    ...hrr60Summary,
    interval_type: intervalType ?? null,
    racepace_assessment: computeRacePaceAssessmentFromStreams(streams, activity),
  };
}

function computeRacePaceAssessmentFromStreams(streams, activity) {
  const time = streams?.time;
  const speed = streams?.velocity_smooth;
  const hr = streams?.heartrate;
  if (!Array.isArray(time) || !Array.isArray(speed)) return null;
  const tagPresent = hasIntervalKeyTag(activity);

  const n = Math.min(time.length, speed.length, Array.isArray(hr) ? hr.length : time.length);
  if (n < 2) return null;

  const timeSlice = time.slice(0, n);
  const speedSlice = speed.slice(0, n);
  const hrSlice = Array.isArray(hr) ? hr.slice(0, n) : null;

  let racepaceSecPerKm = extractRacepaceSecPerKm(activity);
  if (!Number.isFinite(racepaceSecPerKm) || racepaceSecPerKm <= 0) {
    if (tagPresent) {
      const speedVals = speedSlice.filter((x) => Number.isFinite(x));
      const fallbackSpeed = quantile(speedVals, HRR60_INTERVAL_CONFIG.tagSpeedQuantile);
      if (Number.isFinite(fallbackSpeed) && fallbackSpeed > 0) {
        racepaceSecPerKm = 1000 / fallbackSpeed;
      }
    }
  }
  if (!Number.isFinite(racepaceSecPerKm) || racepaceSecPerKm <= 0) return null;

  const paceTolerancePct = 2.5;
  const paceLower = racepaceSecPerKm * (1 - paceTolerancePct / 100);
  const paceUpper = racepaceSecPerKm * (1 + paceTolerancePct / 100);
  const speedLower = paceUpper > 0 ? 1000 / paceUpper : null;
  const speedUpper = paceLower > 0 ? 1000 / paceLower : null;
  if (!Number.isFinite(speedLower) || !Number.isFinite(speedUpper)) return null;

  const signal = new Array(n).fill(false);
  for (let i = 0; i < n; i++) {
    const v = Number(speedSlice[i]);
    if (!Number.isFinite(v)) continue;
    signal[i] = v >= speedLower && v <= speedUpper;
  }

  const intervals = buildWorkIntervalsFromSignal(timeSlice, signal, { minIntervalSec: 60, maxGapSec: 6 });
  if (!intervals.length) return null;

  const timeAt = (i) => {
    const t = Number(timeSlice[i]);
    return Number.isFinite(t) ? t : i;
  };

  const reps = intervals.map((interval) => {
    let speedSum = 0;
    let speedCount = 0;
    let hrSum = 0;
    let hrCount = 0;
    let hrMax = null;
    for (let i = interval.startIdx; i <= interval.endIdx; i++) {
      const v = Number(speedSlice[i]);
      if (Number.isFinite(v)) {
        speedSum += v;
        speedCount += 1;
      }
      if (hrSlice) {
        const h = Number(hrSlice[i]);
        if (Number.isFinite(h)) {
          hrSum += h;
          hrCount += 1;
          if (hrMax == null || h > hrMax) hrMax = h;
        }
      }
    }

    const avgSpeed = speedCount ? speedSum / speedCount : null;
    const paceSecPerKm = avgSpeed != null && avgSpeed > 0 ? 1000 / avgSpeed : null;
    const avgHr = hrCount ? hrSum / hrCount : null;

    return {
      duration: interval.duration,
      paceSecPerKm,
      avgHr,
      hrMax,
      startTime: interval.startTime,
      endTime: interval.endTime,
    };
  });

  const durations = reps.map((r) => r.duration).filter((x) => Number.isFinite(x) && x > 0);
  const medianDuration = durations.length ? median(durations) : null;
  const sessionKind =
    medianDuration == null
      ? null
      : medianDuration <= 150
        ? "short"
        : medianDuration <= 330
          ? "vo2ish"
          : medianDuration <= 750
            ? "threshold"
            : "steady";

  const paceVals = reps.map((r) => r.paceSecPerKm).filter((x) => Number.isFinite(x) && x > 0);
  const meanPace = paceVals.length ? avg(paceVals) : null;
  const maxDevPct =
    meanPace != null && paceVals.length
      ? Math.max(...paceVals.map((p) => (Math.abs(p - meanPace) / meanPace) * 100))
      : null;

  let paceStabilityScore = 0.6;
  const reasonFlags = [];
  if (maxDevPct != null) {
    if (maxDevPct <= 2.5) paceStabilityScore = 1.0;
    else if (maxDevPct <= 4.0) paceStabilityScore = 0.6;
    else paceStabilityScore = 0.2;
    if (maxDevPct > 4.0) reasonFlags.push("PACE_VAR_HIGH");
  }

  const hrVals = reps.map((r) => r.avgHr).filter((x) => Number.isFinite(x) && x > 0);
  let hrDriftScore = 0.6;
  if (hrVals.length >= 3) {
    const drift = hrVals[hrVals.length - 1] - hrVals[0];
    if (drift <= 5) hrDriftScore = 1.0;
    else if (drift <= 10) hrDriftScore = 0.6;
    else hrDriftScore = 0.2;
    if (drift > 10) reasonFlags.push("HR_DRIFT_HIGH");
  } else {
    reasonFlags.push("HR_INSUFFICIENT");
  }

  let recoveryScore = 0.6;
  if (hrSlice) {
    const drops = [];
    for (let i = 0; i < intervals.length - 1; i++) {
      const current = intervals[i];
      const next = intervals[i + 1];
      const rep = reps[i];
      if (!rep || rep.hrMax == null) continue;
      const pauseEnd = next.startTime;
      let lastHr = null;
      for (let j = current.endIdx; j < next.startIdx; j++) {
        const t = timeAt(j);
        if (t > pauseEnd) break;
        if (pauseEnd - t <= 12) {
          const h = Number(hrSlice[j]);
          if (Number.isFinite(h)) lastHr = h;
        }
      }
      if (lastHr == null) continue;
      drops.push(rep.hrMax - lastHr);
    }
    if (drops.length) {
      const dropMedian = median(drops);
      if (dropMedian >= 25) recoveryScore = 1.0;
      else if (dropMedian >= 15) recoveryScore = 0.6;
      else recoveryScore = 0.2;
      if (dropMedian < 15) reasonFlags.push("RECOVERY_POOR");
    }
  }

  const tAtTarget = reps.reduce((sum, rep) => {
    if (!Number.isFinite(rep.paceSecPerKm)) return sum;
    const within = rep.paceSecPerKm >= paceLower && rep.paceSecPerKm <= paceUpper;
    return within ? sum + (rep.duration ?? 0) : sum;
  }, 0);

  const timeScore = Math.min(1.0, tAtTarget / (12 * 60));

  let score =
    paceStabilityScore * 0.4 +
    hrDriftScore * 0.35 +
    recoveryScore * 0.15 +
    timeScore * 0.1;

  const tempC = getActivityTemperatureC(activity);
  if (Number.isFinite(tempC) && tempC > 20) {
    score -= 0.08;
    reasonFlags.push("HEAT_PENALTY");
  }
  const elevGain = getActivityElevationGain(activity);
  if (Number.isFinite(elevGain) && elevGain > 150) {
    score -= 0.06;
    reasonFlags.push("HILLS_PENALTY");
  }

  score = clamp(score, 0, 1);

  const holdBase = sessionKind === "short" ? 2.0 : sessionKind === "vo2ish" ? 2.6 : sessionKind === "threshold" ? 3.2 : 3.6;
  const holdFactor = 1.6 + (holdBase - 1.6) * score;
  const tHold = tAtTarget * holdFactor;

  const distanceKey = extractTargetDistanceKey(activity);
  const distanceBounds = getRaceDistanceBounds(distanceKey);
  const lowerBound = distanceBounds?.min ?? null;
  const realistic = lowerBound != null ? tHold >= lowerBound * 0.85 : null;
  if (realistic === false) reasonFlags.push("DISTANCE_MISMATCH");

  let deltaSecPerKm = 0;
  if (realistic === false && lowerBound != null) {
    const gap = Math.max(0, lowerBound - tHold);
    const basePenalty = getRacePacePenaltyPer5Min(distanceKey);
    if (basePenalty != null) {
      deltaSecPerKm = (gap / (5 * 60)) * basePenalty;
      deltaSecPerKm *= 1.0 + (0.6 - score);
      deltaSecPerKm = clamp(deltaSecPerKm, 2, 25);
      if (deltaSecPerKm >= 10) reasonFlags.push("PACE_TOO_FAST");
    }
  }

  if (realistic === true && score > 0.85 && lowerBound != null && tHold > lowerBound * 1.05) {
    deltaSecPerKm = -1 * clamp((score - 0.85) * 20, 1, 6);
    reasonFlags.push("YOU_CAN_PUSH");
  }

  const suggestedRacePace = racepaceSecPerKm + deltaSecPerKm;

  let confidence = Math.round(score * 100);
  if (hrVals.length < 3) confidence = Math.round(confidence * 0.9);
  if (!hrSlice || recoveryScore === 0.6) confidence = Math.round(confidence * 0.95);
  confidence = clamp(confidence, 0, 100);

  return {
    realistic,
    score,
    confidence,
    delta_s_per_km: deltaSecPerKm,
    suggestedRacePace,
    tAtTarget,
    tHold,
    sessionKind,
    reasonFlags,
  };
}

function getActivityTemperatureC(activity) {
  const candidates = [activity?.average_temp, activity?.temperature, activity?.temp_c, activity?.temp];
  for (const value of candidates) {
    const v = Number(value);
    if (Number.isFinite(v)) return v;
  }
  return null;
}

function getActivityElevationGain(activity) {
  const candidates = [
    activity?.total_elevation_gain,
    activity?.elevation_gain,
    activity?.elevationGain,
    activity?.elevation,
  ];
  for (const value of candidates) {
    const v = Number(value);
    if (Number.isFinite(v)) return v;
  }
  return null;
}

function extractTargetDistanceKey(activity) {
  const candidates = [
    activity?.targetDistance,
    activity?.raceDistance,
    activity?.eventDistance,
    activity?.distance_km,
    activity?.distanceKm,
    activity?.distanceMeters,
    activity?.distance_metres,
    activity?.distance,
  ];
  for (const value of candidates) {
    if (typeof value === "string" && value.trim()) {
      const norm = normalizeEventDistanceKey(value.trim());
      if (norm) return norm;
    }
    const num = Number(value);
    if (Number.isFinite(num)) {
      if (num > 1000) {
        const km = num / 1000;
        const key = normalizeEventDistanceKey(`${km.toFixed(1)}k`);
        if (key) return key;
      } else if (num >= 1) {
        const key = normalizeEventDistanceKey(`${num}k`);
        if (key) return key;
      }
    }
  }
  return null;
}

function getRaceDistanceBounds(distanceKey) {
  const key = String(distanceKey || "").toLowerCase();
  if (key === "5k") return { min: 16 * 60, max: 35 * 60 };
  if (key === "10k") return { min: 33 * 60, max: 65 * 60 };
  if (key === "hm" || key === "half" || key === "half_marathon") return { min: 70 * 60, max: 140 * 60 };
  if (key === "m" || key === "marathon") return { min: 150 * 60, max: 300 * 60 };
  return null;
}

function getRacePacePenaltyPer5Min(distanceKey) {
  const key = String(distanceKey || "").toLowerCase();
  if (key === "5k") return 6;
  if (key === "10k") return 8;
  if (key === "hm" || key === "half" || key === "half_marathon") return 10;
  if (key === "m" || key === "marathon") return 12;
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
// NEW: STEADY_T intensity classifier + trend filter
function classifyIntensity(a) {
  const intensityClass = hasKeyTag(a)
    ? INTENSITY_CLASS.KEY_HARD
    : hasSteadyTTag(a)
      ? INTENSITY_CLASS.STEADY_T
      : INTENSITY_CLASS.EASY;
  return {
    intensityClass,
    isKey: intensityClass === INTENSITY_CLASS.KEY_HARD,
    isSteadyT: intensityClass === INTENSITY_CLASS.STEADY_T,
    isEasy: intensityClass === INTENSITY_CLASS.EASY,
  };
}

function shouldIncludeInTrends(intensityClass) {
  return intensityClass === INTENSITY_CLASS.EASY;
}

function isDiagnosticRun(a) {
  return shouldIncludeInTrends(classifyIntensity(a).intensityClass);
}

function isIntensity(a) {
  // MVP: key:* bedeutet intensiv
  return hasKeyTag(a);
}

function isIntensityByHr(a) {
  const hr = Number(a?.average_heartrate);
  if (!Number.isFinite(hr) || hr <= 0) return false;
  return hr >= HFMAX * INTENSITY_HR_PCT;
}

function hasSteadyTTag(a) {
  const tags = normalizeTags(a?.tags);
  return tags.some((t) => STEADY_T_TAGS.has(t));
}

function getIntensityClassForActivity(a) {
  // NEW: STEADY_T classifier pipeline
  return classifyIntensity(a).intensityClass;
}

function isSteadyTActivity(a) {
  return getIntensityClassForActivity(a) === INTENSITY_CLASS.STEADY_T;
}

function isAerobic(a) {
  // MVP: nicht key und ausreichend lang
  // NEW: STEADY_T excluded from diagnostic/aerobic buckets
  if (!isDiagnosticRun(a)) return false;
  const dur = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
  return Number.isFinite(dur) && dur >= GA_MIN_SECONDS;
}

function normalizeTags(tags) {
  return (tags || [])
    .map((t) => String(t || "").toLowerCase().trim().replace(/^#+/, ""))
    .filter(Boolean);
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
  if (isStrength(a)) return false;
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
  const tags = normalizeTags(a?.tags);
  return tags.some((t) => t.startsWith("key:"));
}

function getKeyType(a) {
  // key:schwelle, key:vo2, key:tempo, ...
  const tags = normalizeTags(a?.tags);
  for (const s of tags) {
    if (s.startsWith("key:")) return s.slice(4).trim() || "key";
  }
  return "key";
}

function isKeyHardType(keyType) {
  const s = String(keyType || "").toLowerCase();
  if (!s) return false;
  if (s.includes("vo2") || s.includes("v02")) return true;
  if (s.includes("racepace") || s.includes("race pace") || s.includes("race")) return true;
  if (s.includes("anaerob") || s.includes("anaerobic")) return true;
  if (s.includes("allout") || s.includes("max")) return true;
  if (s.includes("schwelle") && (s.includes("hart") || s.includes("hard"))) return true;
  if (s.includes("threshold") && s.includes("hard")) return true;
  return false;
}

function isKeyHardActivity(a) {
  if (!hasKeyTag(a)) return false;
  return isKeyHardType(getKeyType(a));
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
  // NEW: STEADY_T excluded from GA trend windows
  if (!isDiagnosticRun(a)) return false;
  const dur = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
  return Number.isFinite(dur) && dur >= GA_MIN_SECONDS;
}

function isGAComparable(a) {
  // NEW: STEADY_T excluded from GA comparable windows
  if (!isDiagnosticRun(a)) return false;
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

async function fetchLatestActivityIso(env, oldest, newest) {
  const activities = await fetchIntervalsActivities(env, oldest, newest);
  if (!Array.isArray(activities) || activities.length === 0) return null;

  let latestIso = null;
  for (const activity of activities) {
    const raw = activity?.start_date_local || activity?.start_date;
    if (!raw) continue;
    const parsed = new Date(raw);
    if (Number.isNaN(parsed.getTime())) continue;
    const iso = parsed.toISOString();
    if (!latestIso || parsed > new Date(latestIso)) latestIso = iso;
  }

  return latestIso;
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

async function fetchOverrideEvents(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const categories = OVERRIDE_CATEGORIES.join(",");
  const url = `${BASE_URL}/athlete/${athleteId}/events?oldest=${oldest}&newest=${newest}&category=${categories}`;
  const r = await fetch(url, { headers: { Authorization: authHeader(env) } });
  if (!r.ok) throw new Error(`override events ${r.status}: ${await r.text()}`);
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

export { computeIntervalMetricsFromStreams, buildMondayReportPreview };
