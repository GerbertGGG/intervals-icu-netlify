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
const KRAFT_MIN_RUNFLOOR = 30;
const KRAFT_TARGET = 60;
const KRAFT_MAX = 75;
const STRENGTH_MIN_7D = KRAFT_TARGET;
const STRENGTH_PHASE_PLANS = {
  BASE: {
    phase: "BASE",
    focus: "Struktur & Stabilität",
    objective: "Gewebe robust machen",
    sessionsPerWeek: 2,
    durationMin: [15, 20],
    sessions: [
      {
        name: "Einheit A – Unterkörper stabil",
        exercises: ["3×12 Split Squats", "3×12 Hip Thrust mit Band", "3×30s Plank", "2×12 Clamshell mit Band"],
      },
      {
        name: "Einheit B – Balance & Core",
        exercises: ["3×10 Single Leg RDL", "3×30s Seitstütz", "2×12 Monster Walk", "2×30s Stabikissen Einbeinstand"],
      },
    ],
  },
  BUILD: {
    phase: "BUILD",
    focus: "Kraft → Power",
    objective: "Laufökonomie",
    sessionsPerWeek: 2,
    durationMin: [20, 25],
    sessions: [
      {
        name: "Einheit A – Kraft + Explosiv",
        exercises: ["3×8 Bulgarian Split Squat", "3×8 Hip Thrust einbeinig", "3×8 Jump Squats (kontrolliert)", "2×30s Plank"],
      },
      {
        name: "Einheit B – Lauf-spezifisch",
        exercises: ["3×8 Step-Ups explosiv", "3×8 Single Leg Deadlift", "3×20s Skippings auf Stelle", "2×30s Seitstütz"],
      },
    ],
  },
  RACE: {
    phase: "RACE",
    focus: "Erhalt",
    objective: "Frische",
    sessionsPerWeek: 2,
    durationMin: [12, 15],
    sessions: [
      {
        name: "Einheit – Erhalt",
        exercises: ["2×8 Split Squats", "2×8 Hip Thrust", "2×20s Plank", "1×30s Stabikissen Einbein"],
      },
    ],
  },
};
const INTENSITY_DISTRIBUTION_TARGET = {
  BASE: {
    easyMin: 0.75,
    easyMax: 0.88,
    midMin: 0.08,
    midMax: 0.2,
    hardMax: 0.08,
  },
  BUILD: {
    easyMin: 0.65,
    easyMax: 0.8,
    midMin: 0.15,
    midMax: 0.3,
    hardMax: 0.12,
  },
  RACE: {
    easyMin: 0.7,
    easyMax: 0.85,
    midMin: 0.1,
    midMax: 0.2,
    hardMax: 0.1,
  },
  RESET: {
    easyMin: 0.9,
    hardMax: 0.03,
  },
};
const INTENSITY_LOOKBACK_DAYS = 14;
const INTENSITY_FALLBACK_LOOKBACK_DAYS = 7;
const INTENSITY_MIN_TOTAL_MIN_14D = 90;
const INTENSITY_CLEAR_OVERSHOOT = 0.01;
const BASE_URL = "https://intervals.icu/api/v1";
const DETECTIVE_KV_PREFIX = "detective:week:";
const DETECTIVE_KV_HISTORY_KEY = "detective:history";
const DETECTIVE_HISTORY_LIMIT = 12;
// REMOVE or stop using this for Aerobic:
// const BIKE_EQ_FACTOR = 0.65;

/*
 * TRAININGSPHASEN / BLOCK-LOGIK / PROGRESSION (Konzept, bisher in separater Doku)
 *
 * Zielbild:
 * - Tagesempfehlungen folgen BASE -> BUILD -> RACE -> RESET.
 * - Progression erfolgt primär über Zeit/Umfang, nicht über Pace-Erhöhung.
 * - Eventnähe, Lastsignale und Robustheit steuern progressiv vs. deloaded vs. konservativ.
 *
 * Aktueller Stand im Code:
 * 1) determineBlockState(...)
 *    - berücksichtigt Event-Datum/Distanz, Last, Fatigue (Ramp/Monotony/Strain/ACWR),
 *      Key-Compliance inkl. Spacing und Robustheit (Kraft/Stabi-Minuten).
 *
 * 2) Moduslogik (EVENT/OPEN)
 *    - EVENT:RUN, EVENT:BIKE oder OPEN bestimmen Floors/Policies als Basis der Tagesbewertung.
 *
 * 3) computeRunFloorState(...)
 *    - operative Overlays: NORMAL, DELOAD, TAPER, RECOVER_OVERLAY.
 *    - beeinflusst Floor-Ziele, Key-Caps und Tagesempfehlung.
 *
 * 4) getKeyRules(...)
 *    - regelt erlaubte/bevorzugte Key-Typen, erwartete/maximale Keys/Woche und Verbote je Phase,
 *      differenziert nach Distanz (5k/10k/hm/m) und Block.
 *
 * 5) Progressions-Engine
 *    - PHASE_MAX_MINUTES + computeProgressionTarget(...) + PROGRESSION_DELOAD_EVERY_WEEKS
 *      + RACEPACE_BUDGET_DAYS.
 *    - Ausgabe im Daily-Report: Key-Format, Wochenziel, Block-Maximum, Coaching-Notiz.
 *
 * 6) Coach-Hinweise
 *    - buildKeySuggestion(...) + buildProgressionSuggestion(...)
 *      liefern den nächsten Reiz und den Belastungs-/Sicherheitskontext.
 *
 * Leitplanken:
 * - Fatigue/Overload begrenzen Intensitätsfreigaben (dynamischer Key-Cap).
 * - Taper/Recover begrenzen oder deaktivieren Key-Einheiten.
 * - Deload-Wochen kappen den Progressionsumfang.
 * - Distanz- und phasenspezifische Reiztypen werden bevorzugt.
 *
 * Offene Weiterentwicklungen:
 * - PHASE_MAX_MINUTES in konfigurierbare Quelle (KV/JSON) auslagern.
 * - Reiztyp -> Workout-Template-Mapping (z.B. 3x10, 5x3, 2x20).
 * - Athlete-Level als Multiplikator.
 * - Bike-Progressionslogik vertiefen.
 * - Block-State-Persistenz transparenter dokumentieren.
 */

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

const BLOCK_LENGTHS_WEEKS_BY_DISTANCE = {
  "5k": { base: 10, build: 8, race: 6, taper: 1 },
  "10k": { base: 10, build: 8, race: 6, taper: 1 },
  hm: { base: 12, build: 8, race: 8, taper: 2 },
  m: { base: 16, build: 10, race: 8, taper: 2 },
};

function getBlockLengthsWeeks(eventDistance) {
  const dist = normalizeEventDistance(eventDistance) || "10k";
  return BLOCK_LENGTHS_WEEKS_BY_DISTANCE[dist] || BLOCK_LENGTHS_WEEKS_BY_DISTANCE["10k"];
}

function getPlanStartWeeks(eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  return (lengths.base || 0) + (lengths.build || 0) + (lengths.race || 0) + (lengths.taper || 0);
}

function getRaceStartWeeks(eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  return (lengths.race || 0) + (lengths.taper || 0);
}

function getForceRaceWeeks(eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  return lengths.taper || BLOCK_CONFIG.cutoffs.forceRaceWeeks;
}

function getBlockDurationForDistance(block, eventDistance) {
  const lengths = getBlockLengthsWeeks(eventDistance);
  const weekByBlock = {
    BASE: lengths.base,
    BUILD: lengths.build,
    RACE: (lengths.race || 0) + (lengths.taper || 0),
  };
  const weeks = weekByBlock[block];
  if (!Number.isFinite(weeks) || weeks <= 0) return BLOCK_CONFIG.durations[block] || { minDays: 7, maxDays: 56 };
  const days = Math.max(7, Math.round(weeks * 7));
  return { minDays: days, maxDays: days };
}






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

const TREND_WINDOW_DAYS = 28;
const TREND_MIN_N = 3;

const MOTOR_WINDOW_DAYS = 28;
const MOTOR_NEED_N_PER_HALF = 2;
const MOTOR_DRIFT_WINDOW_DAYS = 14;

const HFMAX = 173;
// ================= MODE / EVENTS (NEW) =================
const EVENT_LOOKAHEAD_DAYS = 365; // how far we look for next event
const POST_EVENT_OPEN_DAYS = 14;  // 2-week open block after each event

// AerobicFloor = k * Intensity7  (Bike & Run zählen aerob gleichwertig)
const AEROBIC_K_DEFAULT = 2.8;
const THRESHOLD_HR_PCT = 0.88;
const VO2_HR_PCT = 0.94;
const PLAN_START_WEEKS = 24;
const PREPLAN_WINDOW_WEEKS = 48;

const TRANSITION_BIKE_EQ = {
  prePlanWeeks: PREPLAN_WINDOW_WEEKS,
  startWeeks: PLAN_START_WEEKS,
  endWeeks: 12,
  prePlanFactor: 0.5,
  startFactor: 0.2,
  endFactor: 0.0,
};

const PREPLAN_RUN_SHARE = {
  min: 0.5,
  targetAtPlanStart: 0.8,
};

const LONGRUN_PREPLAN = {
  stepDays: 14,
  maxStepPct: 0.10,
  startMin: 45,
  targetMinByDistance: {
    "5k": 60,
    "10k": 60,
    hm: 90,
    m: 120,
  },
};


// Minimum stimulus thresholds per mode (tune later)
const MIN_STIMULUS_7D_RUN_EVENT = 150;   // your current value (5k/run blocks)
const MIN_STIMULUS_7D_BIKE_EVENT = 220;  // bike primary

// Maintenance anchors (soft hints, not hard fails)

// Streams/types

// "Trainingslehre" detective
const LONGRUN_MIN_SECONDS = 60 * 60; // >= 60 minutes
const DETECTIVE_WINDOWS = [14, 28, 42, 56, 84];
const DETECTIVE_MIN_RUNS = 3;

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

  const strengthPolicy = evaluateStrengthPolicy(strength7);
  const strengthOk = strength7 >= STRENGTH_MIN_7D;
  const reasons = [];
  if (!strengthOk) reasons.push("Kraft/Stabi fehlt");

  return {
    strengthMinutes7d: Math.round(strength7),
    strengthMinutes14d: Math.round(strength14),
    strengthOk,
    strengthPolicy,
    reasons,
  };
}

function computeStrengthScore(strengthMin7d) {
  const mins = Number(strengthMin7d) || 0;
  if (mins < 30) return 0;
  if (mins < 45) return 1;
  if (mins < 60) return 2;
  return 3;
}

function evaluateStrengthPolicy(strengthMin7d) {
  const mins = Math.round(Number(strengthMin7d) || 0);
  const score = computeStrengthScore(mins);
  const belowRunfloor = mins < KRAFT_MIN_RUNFLOOR;
  let confidenceDelta = 0;

  if (belowRunfloor) {
    const deficit = KRAFT_MIN_RUNFLOOR - mins;
    const bucketSize = Math.max(1, KRAFT_MIN_RUNFLOOR / 5);
    const penalty = Math.ceil(deficit / bucketSize);
    confidenceDelta = -Math.min(5, Math.max(1, penalty));
  } else if (mins >= KRAFT_TARGET) {
    confidenceDelta = 5;
  } else {
    const span = Math.max(1, KRAFT_TARGET - KRAFT_MIN_RUNFLOOR);
    const progress = Math.max(0, mins - KRAFT_MIN_RUNFLOOR);
    confidenceDelta = Math.min(4, Math.floor((progress / span) * 5));
  }

  return {
    minRunfloor: KRAFT_MIN_RUNFLOOR,
    target: KRAFT_TARGET,
    max: KRAFT_MAX,
    minutes7d: mins,
    score,
    confidenceDelta,
    belowRunfloor,
    keyCapOverride: null,
  };
}

function getStrengthPhasePlan(block) {
  const phase = ["BASE", "BUILD", "RACE"].includes(block) ? block : "BASE";
  return STRENGTH_PHASE_PLANS[phase] || STRENGTH_PHASE_PLANS.BASE;
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
const RUN_FLOOR_TAPER_START_DAYS_DEFAULT = 14;
const RUN_FLOOR_TAPER_START_DAYS_BY_DISTANCE = {
  "5k": 7,
  "10k": 7,
  hm: 14,
  m: 14,
};
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
const LIFE_EVENT_CATEGORY_PRIORITY = ["SICK", "INJURED", "HOLIDAY"];

function mapBlockToPhase(block) {
  if (block === "BASE") return "BASE";
  if (block === "BUILD") return "BUILD";
  if (block === "RACE") return "PEAK";
  if (block === "RESET") return "RECOVER";
  return "BASE";
}

function normalizeEventCategory(category) {
  return String(category ?? "").toUpperCase().trim();
}

function isLifeEventCategory(category) {
  const cat = normalizeEventCategory(category);
  return cat === "SICK" || cat === "INJURED" || cat === "HOLIDAY";
}

function isLifeEventActiveOnDay(event, dayIso) {
  const startIso = String(event?.start_date_local || event?.start_date || "").slice(0, 10);
  if (!isIsoDate(startIso) || !isIsoDate(dayIso)) return false;

  const endIsoRaw = String(event?.end_date_local || event?.end_date || "").slice(0, 10);
  if (!isIsoDate(endIsoRaw)) return dayIso === startIso;
  return dayIso >= startIso && dayIso < endIsoRaw;
}

function getLifeEventEffect(activeLifeEvent) {
  const category = normalizeEventCategory(activeLifeEvent?.category);

  if (category === "SICK" || category === "INJURED") {
    return {
      active: true,
      category,
      runFloorFactor: 0,
      allowKeys: false,
      freezeProgression: true,
      freezeFloorIncrease: true,
      ignoreRunFloorGap: true,
      overlayMode: "LIFE_EVENT_STOP",
      reason: `${category}: kompletter Freeze`,
      event: activeLifeEvent,
    };
  }

  if (category === "HOLIDAY") {
    return {
      active: true,
      category,
      runFloorFactor: 0.6,
      allowKeys: false,
      freezeProgression: true,
      freezeFloorIncrease: true,
      ignoreRunFloorGap: true,
      overlayMode: "LIFE_EVENT_HOLIDAY",
      reason: "HOLIDAY: RunFloor reduziert + Keys/Progression pausiert",
      event: activeLifeEvent,
    };
  }

  return {
    active: false,
    category: null,
    runFloorFactor: 1,
    allowKeys: null,
    freezeProgression: false,
    freezeFloorIncrease: false,
    ignoreRunFloorGap: false,
    overlayMode: null,
    reason: null,
    event: null,
  };
}

function getLifeEventCategoryLabel(category) {
  const cat = normalizeEventCategory(category);
  if (cat === "SICK") return "krank";
  if (cat === "INJURED") return "verletzt";
  if (cat === "HOLIDAY") return "Urlaub";
  return cat || "unbekannt";
}

function parseLifeEventBoundary(event, field) {
  const value = String(event?.[field] || "").slice(0, 10);
  return isIsoDate(value) ? value : null;
}

function computeHolidayWindowFactor({ todayISO, lifeEventEffect, previousState, recentHolidayEvent }) {
  const windowStartIso = isoDate(new Date(new Date(todayISO + "T00:00:00Z").getTime() - 6 * 86400000));
  const windowEndIso = isoDate(new Date(new Date(todayISO + "T00:00:00Z").getTime() + 86400000));

  let holidayStartIso = null;
  let holidayEndIso = null;

  if (lifeEventEffect?.active && lifeEventEffect?.category === "HOLIDAY") {
    holidayStartIso =
      parseLifeEventBoundary(lifeEventEffect?.event, "start_date_local") ||
      parseLifeEventBoundary(lifeEventEffect?.event, "start_date");
    holidayEndIso =
      parseLifeEventBoundary(lifeEventEffect?.event, "end_date_local") ||
      parseLifeEventBoundary(lifeEventEffect?.event, "end_date");
  } else if (normalizeEventCategory(recentHolidayEvent?.category) === "HOLIDAY") {
    holidayStartIso =
      parseLifeEventBoundary(recentHolidayEvent, "start_date_local") ||
      parseLifeEventBoundary(recentHolidayEvent, "start_date");
    holidayEndIso =
      parseLifeEventBoundary(recentHolidayEvent, "end_date_local") ||
      parseLifeEventBoundary(recentHolidayEvent, "end_date");
  } else if (normalizeEventCategory(previousState?.lastLifeEventCategory) === "HOLIDAY") {
    holidayStartIso = isIsoDate(previousState?.lastLifeEventStartISO) ? previousState.lastLifeEventStartISO : null;
    holidayEndIso = isIsoDate(previousState?.lastLifeEventEndISO) ? previousState.lastLifeEventEndISO : null;
  }

  if (!holidayStartIso) return 1;
  const normalizedHolidayEndIso = holidayEndIso || isoDate(new Date(new Date(holidayStartIso + "T00:00:00Z").getTime() + 86400000));

  const overlapStart = holidayStartIso > windowStartIso ? holidayStartIso : windowStartIso;
  const overlapEnd = normalizedHolidayEndIso < windowEndIso ? normalizedHolidayEndIso : windowEndIso;
  const overlapDays = overlapEnd > overlapStart ? diffDays(overlapStart, overlapEnd) : 0;

  const blockedDays = clampInt(String(overlapDays), 0, 7);
  const trainableDays = 7 - blockedDays;
  return clamp(trainableDays / 7, 0, 1);
}

function getTaperStartDays(eventDistance) {
  const dist = normalizeEventDistance(eventDistance);
  return RUN_FLOOR_TAPER_START_DAYS_BY_DISTANCE[dist] ?? RUN_FLOOR_TAPER_START_DAYS_DEFAULT;
}

function computeTaperFactor(eventInDays, taperStartDays) {
  if (!Number.isFinite(eventInDays)) return 1;
  if (eventInDays <= RUN_FLOOR_TAPER_END_DAYS) return 0.6;
  if (eventInDays >= taperStartDays) return 0.9;
  const span = taperStartDays - RUN_FLOOR_TAPER_END_DAYS;
  if (span <= 0) return 0.9;
  const ratio = (eventInDays - RUN_FLOOR_TAPER_END_DAYS) / span;
  return 0.6 + ratio * (0.9 - 0.6);
}

function computeBikeSubstitutionFactor(weeksToEvent) {
  if (!Number.isFinite(weeksToEvent)) return 0;

  if (weeksToEvent >= TRANSITION_BIKE_EQ.prePlanWeeks) {
    return clamp(TRANSITION_BIKE_EQ.prePlanFactor, 0, 1);
  }

  if (weeksToEvent > TRANSITION_BIKE_EQ.startWeeks) {
    const span = TRANSITION_BIKE_EQ.prePlanWeeks - TRANSITION_BIKE_EQ.startWeeks;
    if (span <= 0) return clamp(TRANSITION_BIKE_EQ.startFactor, 0, 1);
    const ratio = (weeksToEvent - TRANSITION_BIKE_EQ.startWeeks) / span;
    const raw = TRANSITION_BIKE_EQ.startFactor + ratio * (TRANSITION_BIKE_EQ.prePlanFactor - TRANSITION_BIKE_EQ.startFactor);
    return clamp(raw, 0, 1);
  }

  if (weeksToEvent <= TRANSITION_BIKE_EQ.endWeeks) return clamp(TRANSITION_BIKE_EQ.endFactor, 0, 1);
  const span = TRANSITION_BIKE_EQ.startWeeks - TRANSITION_BIKE_EQ.endWeeks;
  if (span <= 0) return clamp(TRANSITION_BIKE_EQ.endFactor, 0, 1);
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
  eventDistance,
  eventDateISO,
  previousState,
  dailyRunLoads,
  lifeEventEffect,
  recentHolidayEvent,
}) {
  const reasons = [];
  let syntheticLifeEvent = null;
  const safeEventInDays = Number.isFinite(eventInDays) ? Math.round(eventInDays) : 9999;
  const taperStartDays = getTaperStartDays(eventDistance);
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
  let lastLifeEventCategory = normalizeEventCategory(previousState?.lastLifeEventCategory);
  let lastLifeEventStartISO = isIsoDate(previousState?.lastLifeEventStartISO) ? previousState.lastLifeEventStartISO : null;
  let lastLifeEventEndISO = isIsoDate(previousState?.lastLifeEventEndISO) ? previousState.lastLifeEventEndISO : null;

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
  const hasLifeEvent = lifeEventEffect?.active === true;
  if (hasLifeEvent) {
    overlayMode = lifeEventEffect.overlayMode || "LIFE_EVENT";
    reasons.push(lifeEventEffect.reason || "LifeEvent aktiv");
  } else if (safeEventInDays >= 0 && safeEventInDays <= taperStartDays) {
    overlayMode = "TAPER";
    reasons.push(`Taper aktiv (Event in ≤${taperStartDays} Tagen)`);
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
  if (hasLifeEvent) {
    const factor = Number.isFinite(lifeEventEffect?.runFloorFactor) ? lifeEventEffect.runFloorFactor : 1;
    const holidayRampFactor = computeHolidayWindowFactor({
      todayISO,
      lifeEventEffect,
      previousState,
      recentHolidayEvent,
    });
    effectiveFloorTarget = updatedFloorTarget * (lifeEventEffect?.category === "HOLIDAY" ? holidayRampFactor : factor);
    lastLifeEventCategory = normalizeEventCategory(lifeEventEffect?.category);
    const startIso =
      parseLifeEventBoundary(lifeEventEffect?.event, "start_date_local") ||
      parseLifeEventBoundary(lifeEventEffect?.event, "start_date");
    const endIso =
      parseLifeEventBoundary(lifeEventEffect?.event, "end_date_local") ||
      parseLifeEventBoundary(lifeEventEffect?.event, "end_date");
    if (startIso) lastLifeEventStartISO = startIso;
    if (endIso) lastLifeEventEndISO = endIso;
  } else if (overlayMode === "DELOAD") {
    effectiveFloorTarget = applyDeloadRules({ floorTarget: updatedFloorTarget, phase }).effectiveFloorTarget;
  } else if (overlayMode === "TAPER") {
    effectiveFloorTarget = updatedFloorTarget * computeTaperFactor(safeEventInDays, taperStartDays);
  } else if (overlayMode === "RECOVER_OVERLAY") {
    effectiveFloorTarget = updatedFloorTarget * RUN_FLOOR_RECOVER_FACTOR;
  } else {
    const holidayRampFactor = computeHolidayWindowFactor({
      todayISO,
      lifeEventEffect,
      previousState,
      recentHolidayEvent,
    });
    if (holidayRampFactor < 1) {
      effectiveFloorTarget = updatedFloorTarget * holidayRampFactor;
      reasons.push("Post-Holiday Ramp aktiv");
      syntheticLifeEvent = {
        category: "HOLIDAY",
        runFloorFactor: holidayRampFactor,
        allowKeys: null,
        freezeProgression: false,
        freezeFloorIncrease: false,
        ignoreRunFloorGap: true,
        name: "post_holiday_ramp",
      };
    } else {
      lastLifeEventCategory = "";
      lastLifeEventStartISO = null;
      lastLifeEventEndISO = null;
    }
  }

  const deloadCompletedSinceIncrease =
    lastDeloadCompletedISO && (!lastFloorIncreaseDate || lastDeloadCompletedISO > lastFloorIncreaseDate);

  if (
    (phase === "BASE" || phase === "BUILD") &&
    overlayMode === "NORMAL" &&
    safeEventInDays > 28 &&
    !lifeEventEffect?.freezeFloorIncrease &&
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
      overlayMode === "LIFE_EVENT_STOP"
        ? "LifeEvent: Stop"
        : overlayMode === "LIFE_EVENT_HOLIDAY"
          ? "LifeEvent: Holiday"
          : overlayMode === "RECOVER_OVERLAY"
        ? "Recover"
        : overlayMode === "DELOAD"
          ? "Deload"
          : stabilityWarn
            ? "Warn: Instabil"
            : "Build",
    lastDeloadCompletedISO,
    lastFloorIncreaseDate,
    lastEventDate,
    lastLifeEventCategory,
    lastLifeEventStartISO,
    lastLifeEventEndISO,
    daysSinceEvent,
    reasons,
    lifeEvent: lifeEventEffect?.active
      ? {
          category: lifeEventEffect.category,
          runFloorFactor: lifeEventEffect.runFloorFactor,
          allowKeys: lifeEventEffect.allowKeys,
          freezeProgression: lifeEventEffect.freezeProgression,
          freezeFloorIncrease: lifeEventEffect.freezeFloorIncrease,
          ignoreRunFloorGap: lifeEventEffect.ignoreRunFloorGap,
          name: lifeEventEffect?.event?.name || null,
        }
      : syntheticLifeEvent,
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
  let quality = "locker/GA";
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

function computeLongRunSummary14d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - (LONGRUN_PREPLAN.stepDays - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let longest = null;
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;
    const seconds = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
    if (!longest || seconds > longest.seconds) longest = { seconds, date: d };
  }

  if (!longest) return { minutes: 0, date: null };
  return { minutes: Math.round(longest.seconds / 60), date: longest.date };
}

function computeLongRunTargetMinutes(weeksToEvent, eventDistance) {
  const dist = normalizeEventDistance(eventDistance) || "10k";
  const planStartWeeks = getPlanStartWeeks(dist);
  const target = LONGRUN_PREPLAN.targetMinByDistance?.[dist] ?? LONGRUN_PREPLAN.targetMinByDistance["10k"];

  if (!Number.isFinite(weeksToEvent)) {
    return {
      dist,
      targetMin: target,
      plannedMin: LONGRUN_PREPLAN.startMin,
      progressPct: 0,
      startMin: LONGRUN_PREPLAN.startMin,
      maxStepPct: LONGRUN_PREPLAN.maxStepPct,
      stepDays: LONGRUN_PREPLAN.stepDays,
    };
  }

  const clampedWeeks = clamp(weeksToEvent, planStartWeeks, PREPLAN_WINDOW_WEEKS);
  const span = PREPLAN_WINDOW_WEEKS - planStartWeeks;
  const ratio = span > 0 ? (PREPLAN_WINDOW_WEEKS - clampedWeeks) / span : 1;
  const progressPct = clamp(ratio, 0, 1);
  const plannedMin = Math.round(LONGRUN_PREPLAN.startMin + (target - LONGRUN_PREPLAN.startMin) * progressPct);

  return {
    dist,
    targetMin: target,
    plannedMin: Math.max(LONGRUN_PREPLAN.startMin, Math.min(target, plannedMin)),
    progressPct,
    startMin: LONGRUN_PREPLAN.startMin,
    maxStepPct: LONGRUN_PREPLAN.maxStepPct,
    stepDays: LONGRUN_PREPLAN.stepDays,
  };
}

function computeRunShareTarget(weeksToEvent, eventDistance) {
  const planStartWeeks = getPlanStartWeeks(eventDistance);
  if (!Number.isFinite(weeksToEvent)) return PREPLAN_RUN_SHARE.min;
  if (weeksToEvent >= PREPLAN_WINDOW_WEEKS) return PREPLAN_RUN_SHARE.min;
  if (weeksToEvent <= planStartWeeks) return PREPLAN_RUN_SHARE.targetAtPlanStart;
  const span = PREPLAN_WINDOW_WEEKS - planStartWeeks;
  if (span <= 0) return PREPLAN_RUN_SHARE.targetAtPlanStart;
  const ratio = (PREPLAN_WINDOW_WEEKS - weeksToEvent) / span;
  const raw = PREPLAN_RUN_SHARE.min + ratio * (PREPLAN_RUN_SHARE.targetAtPlanStart - PREPLAN_RUN_SHARE.min);
  return clamp(raw, PREPLAN_RUN_SHARE.min, PREPLAN_RUN_SHARE.targetAtPlanStart);
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

const PHASE_MAX_MINUTES = {
  BASE: {
    "5k": { ga: 75, schwelle: 25, longrun: 105, vo2_touch: 3, strides: 3 },
    "10k": { ga: 80, schwelle: 30, longrun: 120, vo2_touch: 2, strides: 2 },
    hm: { ga: 90, schwelle: 35, longrun: 150, vo2_touch: 2, strides: 2 },
    m: { ga: 95, schwelle: 20, longrun: 180, strides: 1 },
  },
  BUILD: {
    "5k": { schwelle: 35, vo2_touch: 18, racepace: 8, longrun: 100 },
    "10k": { schwelle: 35, vo2_touch: 35, racepace: 25, longrun: 135 },
    hm: { schwelle: 45, racepace: 40, longrun: 165 },
    m: { schwelle: 35, racepace: 70, longrun: 195 },
  },
  RACE: {
    "5k": { racepace: 18, vo2_touch: 5, schwelle: 15, ga: 50, longrun: 90 },
    "10k": { racepace: 28, vo2_touch: 8, schwelle: 20, ga: 60, longrun: 110 },
    hm: { racepace: 45, vo2_touch: 6, schwelle: 25, ga: 70, longrun: 135 },
    m: { racepace: 75, schwelle: 20, ga: 55, longrun: 150 },
  },
};

const RACEPACE_DISTANCE_TARGET_KM = {
  "5k": 3.0,
  "10k": 6.0,
  hm: 12.0,
  m: 20.0,
};

const PROGRESSION_TEMPLATES = {
  BUILD: {
    "10k": {
      schwelle: [
        { reps: 4, work_min: 6 },
        { reps: 3, work_min: 8 },
        { reps: 3, work_min: 10 },
        { reps: 2, work_min: 8, deload_step: true },
      ],
    },
    hm: {
      schwelle: [
        { reps: 3, work_min: 10 },
        { reps: 3, work_min: 12 },
        { reps: 2, work_min: 15 },
        { reps: 2, work_min: 10, deload_step: true },
      ],
      racepace: [
        { reps: 3, work_km: 2.0 },
        { reps: 2, work_km: 3.0 },
        { reps: 2, work_km: 4.0 },
        { reps: 2, work_km: 2.0, deload_step: true },
      ],
    },
    m: {
      racepace: [
        { reps: 3, work_km: 4.0 },
        { reps: 2, work_km: 6.0 },
        { reps: 2, work_km: 8.0 },
        { reps: 2, work_km: 4.0, deload_step: true },
      ],
    },
  },
  RACE: {
    "5k": {
      racepace: [
        { reps: 3, work_km: 0.8 },
        { reps: 3, work_km: 1.0 },
        { reps: 2, work_km: 1.2, deload_step: true },
      ],
    },
    "10k": {
      racepace: [
        { reps: 3, work_km: 1.5 },
        { reps: 3, work_km: 2.0 },
        { reps: 2, work_km: 2.0, deload_step: true },
      ],
    },
    hm: {
      racepace: [
        { reps: 2, work_km: 4.0 },
        { reps: 2, work_km: 5.0 },
        { reps: 2, work_km: 3.0, deload_step: true },
      ],
    },
    m: {
      racepace: [
        { reps: 2, work_km: 6.0 },
        { reps: 2, work_km: 8.0 },
        { reps: 2, work_km: 5.0, deload_step: true },
      ],
    },
  },
};


const KEY_SESSION_RECOMMENDATIONS = {
  BASE: {
    "5k": {
      ga: ["45–75′ GA1 locker", "langer Lauf 75–100′"],
      schwelle: ["3×8′ @ Schwelle", "4×6′ @ Schwelle", "20′ steady"],
      vo2: ["8–10×10″ Hill Sprints (volle 2–3′ Pause)"]
    },
    "10k": {
      ga: ["60–75′ GA1 locker", "langer Lauf 90–110′"],
      schwelle: ["3×8′ @ Schwelle", "4×6′ @ Schwelle", "20′ steady"],
      vo2: ["6–8×10″ Hill Sprints (volle 2–3′ Pause)"]
    },
    "hm": {
      ga: ["60–90′ GA1 locker", "langer Lauf 100–130′"],
      schwelle: ["3×10′ @ Schwelle", "2×15′ @ Schwelle"],
      vo2: ["6×8–10″ Hill Sprints (volle 2–3′ Pause)"]
    },
    "m": {
      ga: ["75–90′ GA1 locker", "langer Lauf 120–150′"],
      vo2: ["4–6×8–10″ Hill Sprints (volle 2–3′ Pause)"]
    }
  },

  BUILD: {
    "5k": {
      vo2: ["5×3′ @ vVO₂max", "6×800 m @ 3–5k-Pace"],
      schwelle: ["4×6′ @ Schwelle", "3×8′ @ Schwelle"],
      racepace: ["4×1 km @ 5k-Pace (kontrolliert)", "6×600 m @ 5k-Pace"],
      longrun: ["langer Lauf 90′"]
    },
    "10k": {
      schwelle: ["3×10′ @ Schwelle", "2×15′ @ Schwelle"],
      vo2: ["5×1000 m @ 5–10k-Pace", "6×3′ @ vVO₂max"],
      racepace: ["3×2 km @ 10k-Pace (moderat)", "2×3 km @ 10k-Pace (kontrolliert)"],
      longrun: ["langer Lauf 100–120′"]
    },
    "hm": {
      schwelle: ["2×20′ @ Schwelle", "3×15′ @ Schwelle"],
      longrun: ["langer Lauf 120–150′"]
    },
    "m": {
      racepace: ["3×5 km @ M-Pace", "14–18 km @ M im Longrun"],
      longrun: ["150′ Struktur-Longrun mit 3×15′ @ M", "langer Lauf 150–180′"]
    }
  },

  RACE: {
    "5k": {
      racepace: ["4–5×1000 m @ 5k-Pace", "8–10×400 m leicht schneller als 5k-Pace"],
      ga: ["30–45′ GA1 locker"]
    },
    "10k": {
      racepace: ["3×2 km @ 10k-Pace", "2×3 km @ 10k-Pace"],
      vo2: ["5×2′ @ VO2 (lange Pause)", "6×400 m @ 5k-Pace"],
      schwelle: ["2×8′ @ Schwelle (Erhalt)"],
      ga: ["40–50′ GA1 locker"]
    },
    "hm": {
      racepace: ["2×4–5 km @ HM-Pace"],
      vo2: ["4×2′ @ VO2 (kurz, frisch)"],
      schwelle: ["2×10′ @ Schwelle (Erhalt)"],
      ga: ["40–60′ GA1 locker"]
    },
    "m": {
      racepace: ["2×6–8 km @ M-Pace (3 Wochen vor WK)"],
      longrun: ["75–90′ letzter Longrun @ M (10–14 Tage vor Rennen)"],
      ga: ["30–45′ GA1 locker"]
    }
  }
};

const PROGRESSION_DELOAD_EVERY_WEEKS = 4;
const RACEPACE_BUDGET_DAYS = 4;

function resolvePrimaryKeyType(keyRules, block) {
  const preferred = keyRules?.preferredKeyTypes?.find((k) => k !== "steady");
  if (preferred) return preferred;
  if (block === "BASE") return "ga";
  if (block === "RACE") return "racepace";
  return "steady";
}

function pickProgressionStep({ block, dist, keyType, weekInBlock, overlayMode, weeksToEvent }) {
  const steps = PROGRESSION_TEMPLATES?.[block]?.[dist]?.[keyType];
  if (!Array.isArray(steps) || !steps.length) return { step: null, stepIndex: null, steps: null };

  const cycleLength = steps.length;
  let idx = (Math.max(1, Number(weekInBlock) || 1) - 1) % cycleLength;

  if (overlayMode === "DELOAD") {
    const deloadIdx = steps.findIndex((s) => s?.deload_step);
    idx = deloadIdx >= 0 ? deloadIdx : Math.max(0, idx - 1);
  } else if (overlayMode === "RECOVER_OVERLAY") {
    idx = 0;
  } else if (overlayMode === "TAPER") {
    const deloadIdx = steps.findIndex((s) => s?.deload_step);
    idx = deloadIdx >= 0 ? deloadIdx : Math.max(0, idx - 1);
    if (Number.isFinite(weeksToEvent) && weeksToEvent <= 1.5) idx = 0;
  }

  return { step: steps[idx], stepIndex: idx, steps };
}

function computeProgressionTarget(context = {}, keyRules = {}, overlayMode = "NORMAL") {
  const block = context.block || "BASE";
  const dist = context.eventDistance || "10k";
  const weeksToEvent = Number.isFinite(context.weeksToEvent) ? context.weeksToEvent : null;
  const phaseConfig = PHASE_MAX_MINUTES?.[block]?.[dist] || null;
  const primaryType = resolvePrimaryKeyType(keyRules, block);
  const rawMaxMinutes = phaseConfig?.[primaryType] ?? null;
  if (!Number.isFinite(rawMaxMinutes) || rawMaxMinutes <= 0) {
    return {
      available: false,
      primaryType,
      targetMinutes: null,
      targetKm: null,
      maxMinutes: null,
      note: "Für diese Distanz/Phase fehlt noch eine Progressionsvorlage.",
    };
  }

  const timeInBlockDays = Math.max(0, Number(context.timeInBlockDays ?? 0));
  const weekInBlock = Math.max(1, Math.floor(timeInBlockDays / 7) + 1);
  const budgetDays = primaryType === "racepace" ? RACEPACE_BUDGET_DAYS : 7;
  const maxMinutes = Math.max(1, Math.round((rawMaxMinutes * budgetDays) / 7));
  const { step, stepIndex } = pickProgressionStep({
    block,
    dist,
    keyType: primaryType,
    weekInBlock,
    overlayMode,
    weeksToEvent,
  });

  let targetMinutes = null;
  let targetKm = null;
  if (primaryType === "racepace") {
    if (step && Number.isFinite(step.work_km)) {
      const reps = Number(step.reps) || 1;
      targetKm = Math.max(0.5, reps * Number(step.work_km));
    } else {
      const goal = Number(RACEPACE_DISTANCE_TARGET_KM?.[dist]) || null;
      targetKm = goal ? Math.max(0.5, Math.round(goal * 0.8 * 10) / 10) : null;
    }
  } else if (primaryType === "schwelle") {
    if (step && Number.isFinite(step.work_min)) {
      const reps = Number(step.reps) || 1;
      targetMinutes = Math.max(1, reps * Number(step.work_min));
    }
  }

  if (targetMinutes != null) targetMinutes = Math.min(maxMinutes, Math.round(targetMinutes));
  const templateText = getProgressionTemplate(block, dist, primaryType, weekInBlock, overlayMode === "DELOAD");

  return {
    available: true,
    primaryType,
    weekInBlock,
    maxMinutes,
    targetMinutes,
    targetKm,
    stepIndex,
    templateText,
    note:
      overlayMode === "DELOAD"
        ? "Deload aktiv: Volumen runter, Intensität stabil."
        : overlayMode === "TAPER"
          ? "Taper aktiv: weniger Volumen, frisch bleiben."
          : "Progression über Umfang, Pace nicht parallel anheben.",
  };
}

function mapKeyTypeToIntensity(type, eventDistance) {
  const normalized = normalizeKeyType(type);
  if (normalized === "ga" || normalized === "steady") return "easy";
  if (normalized === "schwelle") return "mid";
  if (normalized === "racepace") return eventDistance === "5k" ? "hard" : "mid";
  if (normalized === "vo2_touch" || normalized === "strides") return "hard";
  return "easy";
}

function classifyIntensityCategory(a, eventDistance) {
  if (hasKeyTag(a)) {
    return mapKeyTypeToIntensity(getKeyType(a), eventDistance);
  }

  const hr = Number(a?.average_heartrate);
  if (Number.isFinite(hr) && hr > 0) {
    if (hr >= HFMAX * VO2_HR_PCT) return "hard";
    if (hr >= HFMAX * THRESHOLD_HR_PCT) return "mid";
  }

  return "easy";
}

function computeIntensityDistributionForWindow(ctx, dayIso, lookbackDays, eventDistance) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - lookbackDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let easyMinutes = 0;
  let midMinutes = 0;
  let hardMinutes = 0;
  let totalMinutes = 0;

  for (const a of ctx.activitiesAll || []) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso || !isRun(a)) continue;
    const minutes = (Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0) / 60;
    if (!(minutes > 0)) continue;

    totalMinutes += minutes;
    const category = classifyIntensityCategory(a, eventDistance);
    if (category === "hard") hardMinutes += minutes;
    else if (category === "mid") midMinutes += minutes;
    else easyMinutes += minutes;
  }

  const hasData = totalMinutes > 0;
  const easyShare = hasData ? easyMinutes / totalMinutes : null;
  const midShare = hasData ? midMinutes / totalMinutes : null;
  const hardShare = hasData ? hardMinutes / totalMinutes : null;

  return {
    totalMinutes: Math.round(totalMinutes),
    easyMinutes: Math.round(easyMinutes),
    midMinutes: Math.round(midMinutes),
    hardMinutes: Math.round(hardMinutes),
    easyShare,
    midShare,
    hardShare,
  };
}

function computeIntensityDistribution(ctx, dayIso, block, eventDistance) {
  const targets = INTENSITY_DISTRIBUTION_TARGET[block] ?? INTENSITY_DISTRIBUTION_TARGET.BASE;
  const metrics14 = computeIntensityDistributionForWindow(ctx, dayIso, INTENSITY_LOOKBACK_DAYS, eventDistance);
  const useFallback = metrics14.totalMinutes < INTENSITY_MIN_TOTAL_MIN_14D;
  const metrics = useFallback
    ? computeIntensityDistributionForWindow(ctx, dayIso, INTENSITY_FALLBACK_LOOKBACK_DAYS, eventDistance)
    : metrics14;

  const hasData = (metrics?.totalMinutes ?? 0) > 0;
  const easyShare = metrics?.easyShare;
  const midShare = metrics?.midShare;
  const hardShare = metrics?.hardShare;

  const hardOver =
    hasData && Number.isFinite(hardShare) && Number.isFinite(targets?.hardMax)
      ? hardShare > targets.hardMax + INTENSITY_CLEAR_OVERSHOOT
      : false;
  const midOver =
    hasData && Number.isFinite(midShare) && Number.isFinite(targets?.midMax)
      ? midShare > targets.midMax + INTENSITY_CLEAR_OVERSHOOT
      : false;
  const easyUnder =
    hasData && Number.isFinite(easyShare) && Number.isFinite(targets?.easyMin)
      ? easyShare < targets.easyMin
      : false;

  return {
    hasData,
    lookbackDays: useFallback ? INTENSITY_FALLBACK_LOOKBACK_DAYS : INTENSITY_LOOKBACK_DAYS,
    targets,
    easyShare,
    midShare,
    hardShare,
    easyMinutes: metrics?.easyMinutes ?? 0,
    midMinutes: metrics?.midMinutes ?? 0,
    hardMinutes: metrics?.hardMinutes ?? 0,
    totalMinutes: metrics?.totalMinutes ?? 0,
    hardOver,
    midOver,
    easyUnder,
  };
}

function buildProgressionSuggestion(progression) {
  if (!progression?.available) return progression?.note || "Progression aktuell nicht verfügbar.";

  if (progression?.primaryType === "racepace") {
    const kmNow = Number(progression?.targetKm);
    const note = progression?.note ? ` ${progression.note}` : "";
    const text = Number.isFinite(kmNow)
      ? `Diese Woche ca. ${formatDecimalKm(kmNow)} km RP als Hauptblock.`
      : "";
    return `Racepace: ${text}${note}${progression?.templateText ? ` ${progression.templateText}` : ""}`;
  }

  if (progression.primaryType === "schwelle") {
    const minutes = Number(progression?.targetMinutes);
    const note = progression?.note ? ` ${progression.note}` : "";
    const text = Number.isFinite(minutes) ? `Diese Woche ~${Math.round(minutes)}′ Schwelle.` : "";
    return `Schwelle: ${text}${note}${progression?.templateText ? ` ${progression.templateText}` : ""}`;
  }

  const keyType = formatKeyType(progression.primaryType);
  return `${keyType}: ${progression?.templateText || progression?.note || ""}`.trim();
}

function buildExplicitKeySessionRecommendation(context = {}, keyRules = {}, progression = null) {
  const block = context.block || "BASE";
  const distance = context.eventDistance || "10k";
  const preferredType = resolvePrimaryKeyType(keyRules, block);
  const catalog = KEY_SESSION_RECOMMENDATIONS?.[block]?.[distance] || null;
  if (!catalog) return null;

  const preferredList = Array.isArray(catalog?.[preferredType]) ? catalog[preferredType] : null;
  const fallbackType = Object.keys(catalog).find((type) => Array.isArray(catalog[type]) && catalog[type].length > 0) || null;
  const chosenType = preferredList?.length ? preferredType : fallbackType;
  const entries = chosenType ? catalog[chosenType] : null;
  if (!Array.isArray(entries) || !entries.length) return null;

  const progressionStepSession = getCurrentProgressionStepSession(block, distance, chosenType, progression?.weekInBlock);
  const sessionText = progressionStepSession || entries[0];
  const racepaceTarget = chosenType === "racepace"
    ? getRacepaceTargetText(distance)
    : "";
  return `${formatKeyType(chosenType)} konkret: ${sessionText}.${racepaceTarget}`;
}

function getCurrentProgressionStepSession(block, distance, keyType, weekInBlock) {
  const steps = PROGRESSION_TEMPLATES?.[block]?.[distance]?.[keyType];
  if (!Array.isArray(steps) || !steps.length) return null;

  const cycleLength = steps.length;
  const weekIndex = Math.max(1, Number(weekInBlock) || 1);
  const currentStep = steps[((weekIndex - 1) % cycleLength)];
  if (!currentStep) return null;

  const reps = Number(currentStep.reps) || 0;
  if (!reps) return null;

  if (Number.isFinite(currentStep.work_km)) {
    const workKm = formatDecimalKm(Number(currentStep.work_km));
    const racepaceLabel = keyType === "racepace" ? ` @ ${distance.toUpperCase()}-RP` : "";
    return `${reps}×${workKm} km${racepaceLabel}`;
  }

  if (Number.isFinite(currentStep.work_min)) {
    return `${reps}×${Math.round(Number(currentStep.work_min))}′`;
  }

  return null;
}

function formatDecimalKm(km) {
  const value = Number(km);
  if (!Number.isFinite(value)) return "0";
  return value.toLocaleString("de-DE", {
    minimumFractionDigits: Number.isInteger(value) ? 0 : 1,
    maximumFractionDigits: 1,
  });
}

function getRacepaceTargetText(distance) {
  const km = Number(RACEPACE_DISTANCE_TARGET_KM?.[distance]);
  if (!Number.isFinite(km) || km <= 0) return "";
  return ` RP-Ziel bis Blockende: ${formatDecimalKm(km)} km am Stück in RP-Qualität.`;
}

function getProgressionTemplate(block, distance, keyType, weekIndexInBlock, isDeload) {
  const steps = PROGRESSION_TEMPLATES?.[block]?.[distance]?.[keyType];
  if (!Array.isArray(steps) || !steps.length) return null;

  const formatted = steps.map((step, idx) => {
    const reps = Number(step.reps) || 0;
    const hasKm = Number.isFinite(step.work_km);
    if (hasKm) {
      const workKm = Number(step.work_km);
      const totalKm = Number.isFinite(step.total_work_km)
        ? Number(step.total_work_km)
        : reps * workKm;
      const main = `${reps}×${formatDecimalKm(workKm)} km`;
      const deload = step.deload_step ? " Deload" : "";
      const total = totalKm > 0 ? ` ≈${formatDecimalKm(totalKm)} km` : "";
      return `W${idx + 1}${deload} ${main}${total}`;
    }

    const totalWork = Number.isFinite(step.total_work_min)
      ? step.total_work_min
      : reps * (Number(step.work_min) || 0);
    const main = `${reps}×${step.work_min}`;
    const rest = Number.isFinite(step.rest_min) ? ` (${step.rest_min}′ Trabpause)` : "";
    const deload = step.deload_step ? " Deload" : "";
    const total = totalWork > 0 ? ` ≈${Math.round(totalWork)}′` : "";
    return `W${idx + 1}${deload} ${main}${rest}${total}`;
  });

  const cycleLength = steps.length;
  const weekIndex = Math.max(1, Number(weekIndexInBlock) || 1);
  const currentStep = ((weekIndex - 1) % cycleLength) + 1;
  const deloadHint = isDeload ? ` Aktuelle Woche: Deload (W${currentStep}).` : ` Aktuelle Woche: W${currentStep}.`;
  return `${formatKeyType(keyType)} (${distance}) Progression: ${formatted.join(", ")}.${deloadHint}`;
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
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["steady", "schwelle", "strides", "vo2_touch"],
        preferredKeyTypes: ["schwelle", "steady"],
        bannedKeyTypes: ["racepace"],
      };
    }
    if (dist === "m" || dist === "hm") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["steady", "schwelle", "strides"],
        preferredKeyTypes: ["schwelle", "steady"],
        bannedKeyTypes: ["racepace", "vo2_touch"],
      };
    }
    return {
      expectedKeysPerWeek: 0.5,
      maxKeysPerWeek: 2,
      allowedKeyTypes: ["steady", "strides"],
      preferredKeyTypes: ["steady"],
      bannedKeyTypes: ["schwelle", "racepace", "vo2_touch"],
    };
  }

  if (block === "BUILD") {
    if (dist === "5k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["schwelle", "vo2_touch", "racepace", "strides", "steady"],
        preferredKeyTypes: ["vo2_touch", "schwelle", "racepace"],
        bannedKeyTypes: [],
      };
    }
    if (dist === "10k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["schwelle", "vo2_touch", "racepace", "strides", "steady"],
        preferredKeyTypes: ["schwelle", "vo2_touch", "racepace"],
        bannedKeyTypes: [],
      };
    }
    if (dist === "hm") {
      const allowRacePace = weeksToEvent != null && weeksToEvent <= 8;
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: allowRacePace ? ["schwelle", "racepace", "steady"] : ["schwelle", "steady"],
        preferredKeyTypes: allowRacePace ? ["racepace", "schwelle"] : ["schwelle"],
        bannedKeyTypes: allowRacePace ? ["vo2_touch", "strides"] : ["racepace", "vo2_touch", "strides"],
      };
    }
    if (dist === "m") {
      const allowRacePace = weeksToEvent != null && weeksToEvent <= 10;
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
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
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["racepace", "vo2_touch", "schwelle", "strides", "steady"],
        preferredKeyTypes: ["racepace", "vo2_touch", "schwelle"],
        bannedKeyTypes: [],
      };
    }
    if (dist === "10k") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["racepace", "schwelle", "vo2_touch", "strides", "steady"],
        preferredKeyTypes: ["racepace", "schwelle", "vo2_touch"],
        bannedKeyTypes: [],
      };
    }
    if (dist === "hm") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["racepace", "schwelle", "vo2_touch", "steady"],
        preferredKeyTypes: ["racepace", "schwelle"],
        bannedKeyTypes: ["strides"],
      };
    }
    if (dist === "m") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["racepace", "schwelle", "steady"],
        preferredKeyTypes: ["racepace"],
        bannedKeyTypes: ["vo2_touch", "strides"],
      };
    }
  }

  return {
    expectedKeysPerWeek: 0.5,
    maxKeysPerWeek: 2,
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
  const maxKeysCap7 = Number.isFinite(context.maxKeys7d) ? context.maxKeys7d : MAX_KEYS_7D;
  const capExceeded = actual7 >= maxKeysCap7;

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

  const preferred = keyRules.preferredKeyTypes[0] || keyRules.allowedKeyTypes[0] || "steady";
  const blockLabel = context.block ? `Block=${context.block}` : "Block=n/a";
  const distLabel = context.eventDistance ? `Distanz=${context.eventDistance}` : "Distanz=n/a";
  const progression = computeProgressionTarget(context, keyRules, context.overlayMode || "NORMAL");

  const dayIso = context.dayIso || null;
  const lastKeyIso = context.keySpacing?.lastKeyIso ?? null;
  const lastKeyGapDays =
    dayIso && lastKeyIso && isIsoDate(dayIso) && isIsoDate(lastKeyIso) ? diffDays(lastKeyIso, dayIso) : null;
  const keySpacingNowOk = !Number.isFinite(lastKeyGapDays) || lastKeyGapDays >= 2;
  const nextKeyEarliest = context.keySpacing?.nextAllowedIso ?? null;

  const intensityDistribution = context.intensityDistribution || null;
  const hardShareBlocked = intensityDistribution?.hardOver === true;
  const midShareBlocked = intensityDistribution?.midOver === true;
  const easyShareBlocked = intensityDistribution?.easyUnder === true;
  const preferredIntensity = mapKeyTypeToIntensity(preferred, context.eventDistance);

  let suggestion = "";
  let keyAllowedNow = false;

  if (capExceeded) {
    suggestion = `Key-Budget erschöpft (${actual7}/${maxKeysCap7} in 7 Tagen) – restliche Einheiten locker/GA.`;
  } else if (bannedHits.length) {
    suggestion = `Verbotener Key-Typ (${bannedHits[0]}) – Alternative: ${preferred}`;
  } else if (!keySpacingNowOk && nextKeyEarliest) {
    suggestion = `Nächster Key frühestens ${nextKeyEarliest} (≥48h Abstand). Bis dahin locker/GA.`;
  } else if (hardShareBlocked && preferredIntensity === "hard") {
    const hardPct = Math.round((intensityDistribution?.hardShare ?? 0) * 100);
    const maxPct = Math.round((intensityDistribution?.targets?.hardMax ?? 0) * 100);
    suggestion = `Hard-Anteil hoch (${hardPct}% > ${maxPct}%) – heute kein weiterer harter Key. Nur Mid/Easy.`;
    keyAllowedNow = true;
  } else if (easyShareBlocked) {
    const easyPct = Math.round((intensityDistribution?.easyShare ?? 0) * 100);
    const minPct = Math.round((intensityDistribution?.targets?.easyMin ?? 0) * 100);
    suggestion = `Easy-Anteil zu niedrig (${easyPct}% < ${minPct}%) – nächste Einheit zwingend locker.`;
  } else if (midShareBlocked && preferred === "schwelle") {
    const midPct = Math.round((intensityDistribution?.midShare ?? 0) * 100);
    const maxPct = Math.round((intensityDistribution?.targets?.midMax ?? 0) * 100);
    const hardShare = intensityDistribution?.hardShare ?? 0;
    const hardMax = intensityDistribution?.targets?.hardMax ?? 0;
    if (hardShare + INTENSITY_CLEAR_OVERSHOOT < hardMax && keyRules.allowedKeyTypes.includes("vo2_touch")) {
      suggestion = `Mid-Anteil hoch (${midPct}% > ${maxPct}%) – heute keine zusätzliche Schwelle, VO2 kurz optional.`;
      keyAllowedNow = true;
    } else {
      suggestion = `Mid-Anteil hoch (${midPct}% > ${maxPct}%) – heute keine zusätzliche Schwelle, besser locker.`;
    }
  } else if (actual7 === 1 && typeOk) {
    suggestion = `2. Key diese Woche optional/erlaubt: ${preferred} (${blockLabel}, ${distLabel}).`;
    keyAllowedNow = true;
  } else if (!freqOk || preferredMissing) {
    suggestion = `Nächster Key: ${preferred} (${blockLabel}, ${distLabel})`;
    keyAllowedNow = true;
  } else {
    suggestion = "Kein Key geplant – locker/GA.";
  }

  if (suggestion && keyAllowedNow) {
    const progressionHint = buildProgressionSuggestion(progression);
    if (progressionHint) suggestion = `${suggestion} ${progressionHint}`;
  }

  const explicitSession = buildExplicitKeySessionRecommendation(context, keyRules, progression);
  if (explicitSession && keyAllowedNow) {
    suggestion = `${suggestion} Konkrete Session-Idee: ${explicitSession}`;
  }

  const status = capExceeded ? "red" : freqOk && typeOk ? "ok" : "warn";

  return {
    expected,
    maxKeys,
    maxKeysCap7,
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
    progression,
    basedOn: "7T",
    capExceeded,
    keySpacingOk: keySpacingNowOk,
    nextKeyEarliest,
    lastKeyGapDays,
    intensityDistribution,
    keyAllowedNow,
    explicitSession,
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
  const eventDistanceNorm = normalizeEventDistance(eventDistance) || "10k";
  const planStartWeeks = getPlanStartWeeks(eventDistanceNorm);
  const raceStartWeeks = getRaceStartWeeks(eventDistanceNorm);
  const forceRaceWeeks = getForceRaceWeeks(eventDistanceNorm);


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

  if (weeksToEvent <= forceRaceWeeks && weeksToEvent >= 0) {
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

  if (weeksToEvent > planStartWeeks) {
    reasons.push(`Freie Vorphase aktiv (> ${planStartWeeks} Wochen bis Event) → BASE`);
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
      reasons,
      readinessScore: 55,
      forcedSwitch: false,
      nextSuggestedBlock: "BASE",
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

  let block = previousState?.block || (weeksToEvent <= raceStartWeeks ? "BUILD" : "BASE");

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
  const blockLimits = getBlockDurationForDistance(block, eventDistanceNorm);
  

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

  if (weeksToEvent <= raceStartWeeks && weeksToEvent >= 0 && block !== "RACE") {
    forcedSwitch = true;
    reasons.push(`Event ≤${raceStartWeeks} Wochen → sofort RACE (Taper-Puffer)`);
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
    const eventForcesRace = weeksToEvent <= raceStartWeeks;

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
      effectiveFloorTarget: Number.isFinite(parsed.effectiveFloorTarget) ? parsed.effectiveFloorTarget : null,
      loadDays: Number.isFinite(parsed.loadDays) ? parsed.loadDays : 0,
      deloadStartDate: isIsoDate(parsed.deloadStartDate) ? parsed.deloadStartDate : null,
      lastDeloadCompletedISO: isIsoDate(parsed.lastDeloadCompletedISO) ? parsed.lastDeloadCompletedISO : null,
      lastFloorIncreaseDate: isIsoDate(parsed.lastFloorIncreaseDate) ? parsed.lastFloorIncreaseDate : null,
      lastEventDate: isIsoDate(parsed.lastEventDate) ? parsed.lastEventDate : null,
      lastLifeEventCategory: parsed.lastLifeEventCategory ? normalizeEventCategory(parsed.lastLifeEventCategory) : "",
      lastLifeEventStartISO: isIsoDate(parsed.lastLifeEventStartISO) ? parsed.lastLifeEventStartISO : null,
      lastLifeEventEndISO: isIsoDate(parsed.lastLifeEventEndISO) ? parsed.lastLifeEventEndISO : null,
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
      modeInfo = { mode: "OPEN", primary: "open", nextEvent: null, activeLifeEvent: null, lifeEventEffect: getLifeEventEffect(null) };
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
      let intervalMetrics = null;

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
    let longRunSummary = { minutes: 0, date: null, quality: "n/a", isKey: false, intensity: false, longRun14d: { minutes: 0, date: null }, plan: null };
    try {
      longRunSummary = computeLongRunSummary7d(ctx, day);
    } catch {}

    const weeksInfo = eventDate ? computeWeeksToEvent(day, eventDate, null) : { weeksToEvent: null };
    const weeksToEvent = weeksInfo.weeksToEvent ?? null;
    const bikeSubFactor = computeBikeSubstitutionFactor(weeksToEvent);
    const longRun14d = computeLongRunSummary14d(ctx, day);
    const longRunPlan = computeLongRunTargetMinutes(weeksToEvent, eventDistance);
    longRunSummary = {
      ...longRunSummary,
      longRun14d,
      plan: longRunPlan,
    };
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
      (weeksToEvent != null && weeksToEvent <= getRaceStartWeeks(eventDistance) ? "BUILD" : "BASE");
    const keyRulesPre = getKeyRules(baseBlock, eventDistance, weeksToEvent);
    const keyCompliancePre = evaluateKeyCompliance(keyRulesPre, keyStats7, keyStats14, {
      block: baseBlock,
      eventDistance,
      timeInBlockDays: previousBlockState?.timeInBlockDays ?? 0,
      weeksToEvent,
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

    if (modeInfo?.lifeEventEffect?.active && previousBlockState?.block) {
      blockState.block = previousBlockState.block;
      blockState.wave = previousBlockState.wave || blockState.wave;
      blockState.startDate = previousBlockState.startDate || blockState.startDate;
      blockState.timeInBlockDays = previousBlockState.timeInBlockDays ?? blockState.timeInBlockDays;
      blockState.reasons = [...(blockState.reasons || []), `LifeEvent ${modeInfo.lifeEventEffect.category}: Blockwechsel eingefroren`];
    }

    const phase = mapBlockToPhase(blockState.block);
    const eventInDays = eventDate ? daysBetween(day, eventDate) : null;
    const dailyRunLoads = buildRunDailyLoads(ctx, day, RUN_FLOOR_DELOAD_WINDOW_DAYS);
    const runFloorState = evaluateRunFloorState({
      todayISO: day,
      floorTarget: baseRunFloorTarget,
      phase,
      eventInDays,
      eventDistance,
      eventDateISO: eventDate || null,
      previousState: previousBlockState,
      dailyRunLoads,
      lifeEventEffect: modeInfo?.lifeEventEffect || getLifeEventEffect(null),
      recentHolidayEvent: modeInfo?.recentHolidayEvent || null,
    });

    if (policy.specificKind === "run" || policy.specificKind === "open") {
      policy = {
        ...policy,
        specificThreshold: runFloorState.effectiveFloorTarget,
      };
    }
    specificOk = policy.specificThreshold > 0 ? specificValue >= policy.specificThreshold : true;
    blockState.floorTarget = runFloorState.floorTarget;
    blockState.effectiveFloorTarget = runFloorState.effectiveFloorTarget;
    blockState.deloadStartDate = runFloorState.deloadStartDate;
    blockState.lastDeloadCompletedISO = runFloorState.lastDeloadCompletedISO;
    blockState.lastFloorIncreaseDate = runFloorState.lastFloorIncreaseDate;
    blockState.lastEventDate = runFloorState.lastEventDate;

    const dynamicKeyCap = {
      maxKeys7d: MAX_KEYS_7D,
      reasons: [],
    };

    if (modeInfo?.lifeEventEffect?.active && modeInfo.lifeEventEffect.allowKeys === false) {
      dynamicKeyCap.maxKeys7d = 0;
      dynamicKeyCap.reasons.push(`LifeEvent ${modeInfo.lifeEventEffect.category}: Keys pausiert`);
    } else if (runFloorState.overlayMode === "RECOVER_OVERLAY") {
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
   
    } else if ((motor?.value ?? 0) >= 70) {
      dynamicKeyCap.maxKeys7d = 2;
      dynamicKeyCap.reasons.push("Motor stark");
    } else {
      dynamicKeyCap.maxKeys7d = 2;
      dynamicKeyCap.reasons.push("Standard-Cap 2 Keys/7 Tage");
    }
    const strengthPolicy = robustness?.strengthPolicy || evaluateStrengthPolicy(robustness?.strengthMinutes7d || 0);
    if (robustness && !robustness.strengthOk) {
      dynamicKeyCap.reasons.push("Kraft unter Zielbereich (Hinweis)");
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
    const intensityDistribution = computeIntensityDistribution(ctx, day, blockState.block, eventDistance);
    const keyCompliance = evaluateKeyCompliance(keyRules, keyStats7, keyStats14, {
      dayIso: day,
      block: blockState.block,
      eventDistance,
      maxKeys7d: dynamicKeyCap.maxKeys7d,
      keySpacing,
      intensityDistribution,
      timeInBlockDays: blockState.timeInBlockDays,
      weeksToEvent: blockState.weeksToEvent,
    });
if (modeInfo?.lifeEventEffect?.active && modeInfo.lifeEventEffect.allowKeys === false) {
      keyCompliance.keyAllowedNow = false;
      keyCompliance.suggestion = `LifeEvent ${modeInfo.lifeEventEffect.category}: keine Keys (Freeze aktiv).`;
    }
historyMetrics.keyCompliance = keyCompliance;
    patch[FIELD_BLOCK] = blockState.block;
    previousBlockState = {
      block: blockState.block,
      wave: blockState.wave,
      startDate: blockState.startDate || day,
      eventDate,
      eventDistance,
      floorTarget: blockState.floorTarget,
      effectiveFloorTarget: blockState.effectiveFloorTarget,
      timeInBlockDays: blockState.timeInBlockDays,
      deloadStartDate: blockState.deloadStartDate,
      lastDeloadCompletedISO: blockState.lastDeloadCompletedISO,
      lastFloorIncreaseDate: blockState.lastFloorIncreaseDate,
      lastEventDate: blockState.lastEventDate,
      lastLifeEventCategory: runFloorState.lastLifeEventCategory,
      lastLifeEventStartISO: runFloorState.lastLifeEventStartISO,
      lastLifeEventEndISO: runFloorState.lastLifeEventEndISO,
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
      lastLifeEventCategory: runFloorState.lastLifeEventCategory,
      lastLifeEventStartISO: runFloorState.lastLifeEventStartISO,
      lastLifeEventEndISO: runFloorState.lastLifeEventEndISO,
      daysSinceEvent: runFloorState.daysSinceEvent,
      reasons: runFloorState.reasons,
      lifeEvent: runFloorState.lifeEvent,
    });

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

    // Daily report text (used for calendar NOTE instead of wellness comments)
    const dailyReportText = buildComments({
      perRunInfo,
      trend,
      motor,
      benchReports,
      robustness,
      modeInfo,
      blockState,
      keyRules,
      keyCompliance,
      dynamicKeyCap,
      keySpacing,
      todayIso: day,
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
      bikeSubFactor,
      weeksToEvent,
      eventDistance,
    }, { debug });

    // Explicitly clear wellness comments; report is written only as NOTE.
    patch.comments = "";





    patches[day] = patch;

    if (debug) {
      notesPreview[day] = dailyReportText || "";
    }

    // Daily NOTE (calendar): stores the daily report text in blue
    if (write) {
      await upsertDailyReportNote(env, day, dailyReportText || "");
    }

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
      if (debug) {
        const detectiveBlock = String(detectiveNoteText || "").startsWith("🕵️‍♂️")
          ? detectiveNoteText || ""
          : ["🕵️‍♂️ Montags-Report", detectiveNoteText || ""].join("\n");
        notesPreview[day] = [
          notesPreview[day],
          "",
          detectiveBlock,
        ]
          .filter((line) => line != null)
          .join("\n");
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
    debug: debug ? ctx.debugOut : undefined,
  };
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
  if (type === "steady") return "GA";
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

function buildNextRunRecommendation({
  runFloorState,
  policy,
  specificOk,
  hasSpecific,
  aerobicOk,
  intensitySignal,
  keyCapExceeded,
  keySpacingOk,
  keyAllowedNow,
  keySuggestion,
}) {
  let next = "45–60 min locker/GA";
  const overlay = runFloorState?.overlayMode ?? "NORMAL";
  if (overlay === "LIFE_EVENT_STOP") {
    next = "Pause / nur Regeneration (LifeEvent)";
  } else if (overlay === "LIFE_EVENT_HOLIDAY") {
    next = "20–45 min locker (Holiday-Modus)";
  } else if (overlay === "RECOVER_OVERLAY") {
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
    next = "Kein weiterer Key diese Woche – locker/GA.";
  } else if (!keySpacingOk) {
    next = "Nächster Key frühestens in 48h – bis dahin locker/GA.";
  } else if (keyAllowedNow) {
    const optionalKeyHint = keySuggestion ? ` Optional: ${keySuggestion}` : " Optional: kurzer Key möglich, wenn du dich frisch fühlst.";
    next = `${next}.${optionalKeyHint}`;
  }

  return next;
}

function limitText(text, maxLen = 140) {
  const clean = String(text || "").replace(/\s+/g, " ").trim();
  if (clean.length <= maxLen) return clean;
  return `${clean.slice(0, Math.max(0, maxLen - 1)).trimEnd()}…`;
}

function shortExplicitSession(explicitSession) {
  if (!explicitSession) return null;
  const firstSentence = String(explicitSession)
    .split(".")[0]
    .trim();
  const cleaned = firstSentence
    .replace(/^Racepace konkret:\s*/i, "")
    .replace(/\s+/g, " ")
    .trim();
  return limitText(cleaned, 90);
}

function capLines(lines, maxLines) {
  return (lines || []).filter(Boolean).slice(0, maxLines);
}

function capText(s, maxChars) {
  const x = String(s || "").trim();
  if (x.length <= maxChars) return x;
  return `${x.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
}

function buildRecommendationsAndBottomLine(state) {
  const rec = [];
  const bottom = [];

  const runFloorTarget = state?.runFloorTarget;
  const runFloor7 = state?.runFloor7;
  const explicitSessionShort = state?.explicitSessionShort;
  const longRunDoneMin = Number(state?.longRunDoneMin ?? 0);
  const longRunTargetMin = Number(state?.longRunTargetMin ?? 0);
  const longRunGapMin = Number(state?.longRunGapMin ?? 0);
  const longRunStepCapMin = Number(state?.longRunStepCapMin ?? 0);
  const blockLongRunNextWeekTargetMin = Number(state?.blockLongRunNextWeekTargetMin ?? 0);

  bottom.push(`Heute: ${String(state?.todayAction || "35–50′ locker/steady").replace(/\.$/, "")}.`);
  if (state?.keyAllowedNow && explicitSessionShort) {
    bottom.push(`Key (wenn frisch): ${explicitSessionShort}.`);
  }

  if (Number.isFinite(runFloor7) && Number.isFinite(runFloorTarget) && runFloor7 < runFloorTarget) {
    rec.push(`RunFloor ${runFloor7}/${runFloorTarget} → Volumen priorisieren.`);
  }
  if (Number.isFinite(longRunDoneMin) && Number.isFinite(longRunTargetMin) && longRunTargetMin > 0) {
    if (longRunGapMin < 0) {
      rec.push(`Longrun ${longRunDoneMin}′/${longRunTargetMin}′ → diese Woche locker auf ${longRunTargetMin}′ annähern.`);
    } else if (longRunDoneMin > 0 && Number.isFinite(longRunStepCapMin) && Number.isFinite(blockLongRunNextWeekTargetMin)) {
      rec.push(`Longrun-Progression: nächster Schritt bis ${longRunStepCapMin}′ (Blockziel ${blockLongRunNextWeekTargetMin}′).`);
    }
  }
  if (state?.intensityDistribution?.easyUnder === true) {
    const easyPct = Math.round((state.intensityDistribution.easyShare || 0) * 100);
    const easyMinPct = Math.round((state.intensityDistribution?.targets?.easyMin || 0) * 100);
    rec.push(`Easy-Anteil ${easyPct}% (<${easyMinPct}%) → nächste Einheit locker.`);
  }
  if (state?.intensityDistribution?.hardOver === true) {
    const hardPct = Math.round((state.intensityDistribution.hardShare || 0) * 100);
    const hardMaxPct = Math.round((state.intensityDistribution?.targets?.hardMax || 0) * 100);
    rec.push(`Hard-Anteil ${hardPct}% (>${hardMaxPct}%) → kein weiterer harter Key.`);
  }
  if (state?.budgetBlocked) {
    rec.push(`Key-Budget ${state.actualKeys7}/${state.keyCap7} (7T) erreicht → kein weiterer Key.`);
  }
  if (state?.spacingBlocked) {
    rec.push(`Key-Abstand <48h${state.nextAllowed ? ` (ab ${state.nextAllowed})` : ""} → heute kein Key.`);
  }
  if (state?.overlayMode && state.overlayMode !== "NORMAL") {
    rec.push(`Overlay: ${state.overlayMode} → konservativ bleiben.`);
  }

  return {
    recommendations: capLines(rec, 3).map((x) => capText(x, 110)),
    bottomLine: capLines(bottom, 2).map((x) => capText(x, 110)),
  };
}

function buildTransitionLine({ bikeSubFactor, weeksToEvent, eventDistance }) {
  if (!(bikeSubFactor > 0)) return null;
  const pct = Math.round(bikeSubFactor * 100);
  const runSharePct = Math.round(computeRunShareTarget(weeksToEvent, eventDistance) * 100);
  const bikeSharePct = Math.max(0, 100 - runSharePct);
  const weeksText = Number.isFinite(weeksToEvent) ? `${Math.round(weeksToEvent)} Wochen` : "n/a";
  return `Übergang aktiv: Zielmix Lauf/Rad ~${runSharePct}/${bikeSharePct} (aktuell ${weeksText} bis Event). Rad zählt ${pct}% zum RunFloor.`;
}

// ================= COMMENT =================
function buildComments(
  {
    perRunInfo,
    trend,
    motor,
    benchReports,
    robustness,
    modeInfo,
    blockState,
    keyRules,
    keyCompliance,
    dynamicKeyCap,
    keySpacing,
    todayIso,
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
    bikeSubFactor,
    weeksToEvent,
    eventDistance,
  },
  { debug = false } = {}
) {
  const lines = [];
  const formatPct1 = (value) => (Number.isFinite(value) ? `${value.toFixed(1).replace('.', ',')} %` : "n/a");
  const formatSignedPct1 = (value) =>
    Number.isFinite(value)
      ? `${value >= 0 ? "+" : ""}${value.toFixed(1).replace('.', ',')} %`
      : "n/a";
  const addDecisionBlock = (title, metrics = []) => {
    const titleEmojis = {
      "HEUTIGER LAUF": "🏃",
      "BELASTUNG & PROGRESSION": "📈",
      "KEY-CHECK": "🔑",
      "EMPFEHLUNGEN": "🧭",
      "HEUTE-ENTSCHEIDUNG": "🎯",
      "BOTTOM LINE": "🧾",
    };
    lines.push(`${titleEmojis[title] || "✅"} ${title}`);
    for (const metric of metrics) {
      if (metric) lines.push(metric);
    }
    lines.push("⸻");
    lines.push("");
  };

  const keyCap7 = keyCompliance?.maxKeysCap7 ?? MAX_KEYS_7D;
  const actualKeys14 = keyCompliance?.actual14 ?? 0;
  const actualKeys7 = keyCompliance?.actual7 ?? 0;
  const runLoad7 = Math.round(loads7?.runTotal7 ?? 0);
  const runTarget = Math.round(runFloorState?.effectiveFloorTarget ?? 0);
  const runFloorGap = runTarget > 0 ? runLoad7 - runTarget : 0;
  const lifeEvent = runFloorState?.lifeEvent || null;
  const ignoreRunFloorGap = lifeEvent?.ignoreRunFloorGap === true;
  const intensityDistribution = keyCompliance?.intensityDistribution;
  const easySharePct = intensityDistribution?.hasData ? Math.round((intensityDistribution.easyShare ?? 0) * 100) : null;
  const midSharePct = intensityDistribution?.hasData ? Math.round((intensityDistribution.midShare ?? 0) * 100) : null;
  const hardSharePct = intensityDistribution?.hasData ? Math.round((intensityDistribution.hardShare ?? 0) * 100) : null;
  const easyMinPct = Math.round((intensityDistribution?.targets?.easyMin ?? 0) * 100);
  const midMaxPct = Math.round((intensityDistribution?.targets?.midMax ?? 0) * 100);
  const hardMaxPct = Math.round((intensityDistribution?.targets?.hardMax ?? 0) * 100);
  const spacingOk = keyCompliance?.keySpacingOk ?? keySpacing?.ok ?? true;
  const nextAllowed = keyCompliance?.nextKeyEarliest ?? keySpacing?.nextAllowedIso ?? null;
  const overlayMode = runFloorState?.overlayMode ?? "NORMAL";
  const strengthPolicy = robustness?.strengthPolicy || evaluateStrengthPolicy(robustness?.strengthMinutes7d || 0);
  const strengthPlan = getStrengthPhasePlan(blockState?.block);

  const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);
  const daysToEvent = eventDate && todayIso ? diffDays(todayIso, eventDate) : null;

  const keyBlocked = keyCompliance?.keyAllowedNow === false || overlayMode === "DELOAD" || overlayMode === "TAPER" || overlayMode === "RECOVER_OVERLAY" || overlayMode === "LIFE_EVENT_STOP";
  const budgetBlocked = keyCompliance?.capExceeded === true;
  const spacingBlocked = !spacingOk;
  const easyShareBlocked = intensityDistribution?.hasData && intensityDistribution?.easyUnder === true;
  const hardShareBlocked = intensityDistribution?.hasData && intensityDistribution?.hardOver === true;
  const deloadBlocked = overlayMode === "DELOAD" || overlayMode === "TAPER" || overlayMode === "RECOVER_OVERLAY" || overlayMode === "LIFE_EVENT_STOP";
  const runFloorBlocked = !ignoreRunFloorGap && runTarget > 0 && runFloorGap < 0;

  let mainBlockReason = null;
  if (keyBlocked) {
    if (budgetBlocked) mainBlockReason = `Budget ${actualKeys7}/${keyCap7} (7T)`;
    else if (spacingBlocked) mainBlockReason = `Spacing bis ${nextAllowed || "n/a"}`;
    else if (hardShareBlocked) mainBlockReason = `HardShare >${hardMaxPct}%`;
    else if (easyShareBlocked) mainBlockReason = `EasyShare <${easyMinPct}%`;
    else if (deloadBlocked) mainBlockReason = `Overlay ${overlayMode}`;
    else if (runFloorBlocked) mainBlockReason = `RunFloor-Gap ${runFloorGap}`;
  }

  const modeLabel =
    overlayMode === "DELOAD"
      ? "Deload"
      : overlayMode === "TAPER"
        ? "Taper"
        : overlayMode === "RECOVER_OVERLAY"
          ? "Recovery"
          : overlayMode === "LIFE_EVENT_STOP"
            ? "LifeEvent Freeze"
            : overlayMode === "LIFE_EVENT_HOLIDAY"
              ? "Holiday"
          : keyBlocked
            ? "Easy only"
            : "Key möglich";
  const ampel = keyBlocked ? "🟠" : "🟢";
  const keyStatus = keyBlocked && mainBlockReason ? `Key blockiert (${mainBlockReason})` : keyBlocked ? "Key blockiert" : "Key frei";
  const progressionStatus = lifeEvent?.freezeProgression
    ? "LifeEvent-Freeze"
    : runFloorState?.deloadActive
      ? "Deload aktiv"
      : "im Plan";
  const keyRuleLine = buildKeyRuleLine({
    keyRules,
    block: blockState?.block,
    eventDistance: formatEventDistance(modeInfo?.nextEvent?.distance_type),
  });
  const nextRunText = buildNextRunRecommendation({
    runFloorState,
    policy,
    specificOk,
    hasSpecific: Number.isFinite(specificValue),
    aerobicOk,
    intensitySignal: fatigue?.intensitySignal,
    keyCapExceeded: budgetBlocked,
    keySpacingOk: spacingOk,
    keyAllowedNow: keyCompliance?.keyAllowedNow,
    keySuggestion: keyCompliance?.suggestion,
  });
  const transitionLine = buildTransitionLine({ bikeSubFactor, weeksToEvent, eventDistance });

  const longRun14d = longRunSummary?.longRun14d || { minutes: 0, date: null };
  const longRunPlan = longRunSummary?.plan || computeLongRunTargetMinutes(weeksToEvent, eventDistance || modeInfo?.nextEvent?.distance_type);
  const longRun7d = longRunSummary || { minutes: 0, date: null, quality: "n/a" };
  const longRunDoneMin = Math.round(longRun14d?.minutes ?? 0);
  const prePlanLongRunTargetMin = Math.round(longRunPlan?.plannedMin ?? LONGRUN_PREPLAN.startMin);
  const phaseLongRunMaxMin = Number(PHASE_MAX_MINUTES?.[blockState?.block || "BASE"]?.[eventDistance || "10k"]?.longrun ?? 0);
  const longRunStepCapRawMin = Math.round(longRunDoneMin * (1 + LONGRUN_PREPLAN.maxStepPct));
  const longRunStepCapMin = phaseLongRunMaxMin > 0
    ? Math.min(longRunStepCapRawMin, phaseLongRunMaxMin)
    : longRunStepCapRawMin;
  const planStartWeeks = getPlanStartWeeks(eventDistance);
  const inPlanPhase = Number.isFinite(weeksToEvent) && weeksToEvent <= planStartWeeks;
  const longRunTargetMin = inPlanPhase && phaseLongRunMaxMin > 0
    ? Math.max(prePlanLongRunTargetMin, longRunStepCapMin || prePlanLongRunTargetMin)
    : prePlanLongRunTargetMin;
  const longRunGapMin = longRunDoneMin - longRunTargetMin;
  const blockLongRunNextWeekTargetMin = longRunDoneMin > 0
    ? longRunStepCapMin
    : LONGRUN_PREPLAN.startMin;

  const runMetrics = [];
  if (!perRunInfo?.length) {
    runMetrics.push("Status: Heute kein Lauf.");
  } else {
    const gaToday = perRunInfo.find((x) => x.ga && !x.isKey);
    const intervalToday = perRunInfo.find((x) => x.isKey && x.intervalMetrics);

    if (gaToday) {
      const drift = gaToday.drift;
      const driftText = formatPct1(drift);
      const driftTooHigh = Number.isFinite(drift) && drift > 5;
      const driftEval =
        drift == null
          ? "keine belastbare Einordnung."
          : drift <= 5
            ? "innerhalb der 5 %-Leitplanke."
            : "über der 5 %-Leitplanke. Mögliche Ermüdung, zu hohe Pace oder Umweltfaktoren.";
      const efTrend = trend?.dv;
      const efText = Number.isFinite(efTrend)
        ? `${formatSignedPct1(efTrend)} vs. Referenz ähnlicher GA-Läufe.`
        : "n/a (zu wenig vergleichbare Läufe).";
      const vdotText = Number.isFinite(efTrend)
        ? `${formatSignedPct1(efTrend)} Trend.`
        : "n/a.";

      runMetrics.push(`Drift: ${driftText} → ${driftEval}`);
      if (drift != null && drift <= 5) runMetrics.push("Stabilität: ✔ Aerobe Stabilität gegeben.");
      if (driftTooHigh) {
        runMetrics.push("Bewertung: Drift > 5 %. EF/VDOT weiter anzeigen, aber mit Vorsicht interpretieren.");
        const likelyCauses = [];
        const lifeEventCategory = normalizeEventCategory(lifeEvent?.category);
        if (lifeEventCategory === "HOLIDAY") {
          likelyCauses.push("Urlaubs-/Rückkehr-Effekt erkannt: 3–5 Tage progressive Belastungssteigerung einplanen.");
        } else if (lifeEventCategory === "SICK" || lifeEventCategory === "INJURED") {
          likelyCauses.push(`LifeEvent ${getLifeEventCategoryLabel(lifeEventCategory)} aktiv: erhöhte Drift kann regenerationsbedingt sein.`);
        }
        if (overlayMode === "RECOVER_OVERLAY") {
          likelyCauses.push("Recover-Overlay aktiv: erhöhte Drift nach Event/Belastung ist aktuell plausibel.");
        }
        if (!likelyCauses.length) {
          likelyCauses.push("Mögliche Treiber: zu hohe Pace, Hitze/Dehydrierung oder kumulative Ermüdung.");
        }
        runMetrics.push(`Ursachen-Check: ${likelyCauses.join(" ")}`);
      }
      runMetrics.push(`EF: ${efText}`);
      runMetrics.push("EF-Hinweis: Nur als Trendsignal interpretieren, keine absolute Bewertung.");
      runMetrics.push(`VDOT: ${vdotText}`);
      runMetrics.push("VDOT-Hinweis: Nur bei vergleichbarer Intensität interpretieren.");
      runMetrics.push("Gesamt-Hinweis: Stabilität und Ermüdung immer im Verlauf bewerten, nicht aus einem Einzelwert.");
    } else if (intervalToday) {
      const m = intervalToday.intervalMetrics;
      const hrr = m?.HRR60_median;
      let hrrEval = "n/a";
      if (Number.isFinite(hrr)) {
        if (hrr >= 25) hrrEval = "gute akute Erholung (Heuristik)";
        else if (hrr >= 15) hrrEval = "normaler Bereich (Heuristik)";
        else hrrEval = "mögliche kumulative Ermüdung (Heuristik)";
      }
      const efSeries = Number.isFinite(m?.HR_Drift_pct)
        ? `${m.HR_Drift_pct >= 0 ? "+" : ""}${m.HR_Drift_pct.toFixed(1)}% HR-Drift über die Intervalle`
        : "n/a";
      const paceConsistency = m ? "weitgehend konstant (Serie als gleichförmig erkannt)" : "n/a";

      runMetrics.push(`HRR60: Ø ${Number.isFinite(hrr) ? hrr.toFixed(0) : "n/a"} bpm → ${hrrEval}.`);
      runMetrics.push(`EF/Serienverlauf: ${efSeries} (nur interpretierbar bei stabiler Pace).`);
      runMetrics.push(`Pace-Konsistenz: ${paceConsistency}.`);
      runMetrics.push("Hinweis: HRR60 ist protokollabhängig (Stop vs. aktiver Cooldown) und kein medizinisches Urteil.");
    } else {
      runMetrics.push("Status: Lauf vorhanden, aber kein GA- oder Intervallsignal mit ausreichender Datenqualität.");
    }
  }
  addDecisionBlock("HEUTIGER LAUF", runMetrics);

  addDecisionBlock("BELASTUNG & PROGRESSION", [
    `Longrun: ${Math.round(longRun7d?.minutes ?? 0)}′ → Ziel: ${longRunTargetMin}′`,
    `Qualität: ${longRun7d?.quality || "n/a"}${longRun7d?.date ? ` (${longRun7d.date})` : ""}`,
    `RunFloor (7 Tage): ${runLoad7} / ${runTarget > 0 ? runTarget : "n/a"}`,
    `21-Tage Progression: ${Math.round(runFloorState?.sum21 ?? 0)} / ${Math.round(runFloorState?.baseSum21Target ?? 0) || 450}`,
    `Aktive Tage (21T): ${Math.round(runFloorState?.activeDays21 ?? 0)} / ${Math.round(runFloorState?.baseActiveDays21Target ?? 0) || 14}`,
    `Stabilität: ${runFloorState?.deloadActive ? "kritisch" : "wackelig"}`,
    `Status: ${progressionStatus === "im Plan" ? "Im Plan." : progressionStatus}`,
  ]);

  const keyCheckMetrics = [
    `Keys (7 Tage): ${actualKeys7}/${keyCap7}${budgetBlocked ? " ⚠️" : ""}`,
    `Next Allowed: ${formatNextAllowed(todayIso, nextAllowed)}`,
    `Intensität 14T: Easy ${easySharePct != null ? easySharePct + " %" : "n/a"} (≥${easyMinPct}%), Mid ${midSharePct != null ? midSharePct + " %" : "n/a"} (≤${midMaxPct || "n/a"}%), Hard ${hardSharePct != null ? hardSharePct + " %" : "n/a"} (≤${hardMaxPct}%)`,
    `Kraft 7T: ${strengthPolicy.minutes7d}′ (Runfloor ≥${strengthPolicy.minRunfloor}′ | Ziel ${strengthPolicy.target}′ | Max ${strengthPolicy.max}′)`,
    `Kraft-Score: ${strengthPolicy.score}/3 | Confidence Δ ${strengthPolicy.confidenceDelta >= 0 ? "+" : ""}${strengthPolicy.confidenceDelta}`,
  ];
  const hasEventDistance = formatEventDistance(modeInfo?.nextEvent?.distance_type) !== "n/a";
  if (keyRuleLine && hasEventDistance) keyCheckMetrics.push(keyRuleLine);
  if (transitionLine) keyCheckMetrics.push(transitionLine);
  addDecisionBlock("KEY-CHECK", keyCheckMetrics);

  const explicitSessionShort = shortExplicitSession(keyCompliance?.explicitSession);
  const keyAllowedNow = keyCompliance?.keyAllowedNow === true && !keyBlocked;
  const decisionCompact = buildRecommendationsAndBottomLine({
    runFloor7: runLoad7,
    runFloorTarget: runTarget > 0 ? runTarget : null,
    intensityDistribution: keyCompliance?.intensityDistribution,
    budgetBlocked,
    spacingBlocked,
    nextAllowed,
    overlayMode: runFloorState?.overlayMode,
    keyAllowedNow,
    explicitSessionShort,
    todayAction: nextRunText.replace(/ Optional:.*$/i, "").trim(),
    actualKeys7,
    keyCap7,
    strengthPolicy,
    longRunDoneMin,
    longRunTargetMin,
    longRunGapMin,
    longRunStepCapMin,
    blockLongRunNextWeekTargetMin,
  });
  addDecisionBlock("EMPFEHLUNGEN", [
    ...decisionCompact.recommendations,
    `Kraft-Integration: 2×/Woche, nach GA1≤60′ oder Strides; kein Kraftblock vor Longrun / <24h vor Key.`,
  ]);

  addDecisionBlock("HEUTE-ENTSCHEIDUNG", [
    `Modus: ${modeLabel}${keyBlocked ? " (kein weiterer Key)" : ""}`,
    `Fokus: ${ampel} ${!ignoreRunFloorGap && runFloorGap < 0 ? "Volumen (RunFloor-Gap schließen)" : "Stabilität"}`,
    `Key: ${actualKeys7} / ${keyCap7} (7T)${budgetBlocked ? " ⚠️" : ""}`,
    `Kraft-Phase ${strengthPlan.phase}: ${strengthPlan.sessionsPerWeek}×/Woche à ${strengthPlan.durationMin[0]}–${strengthPlan.durationMin[1]}′ (${strengthPlan.focus}) | Score ${strengthPolicy.score}/3`,
    Number.isFinite(weeksToEvent) && weeksToEvent > getPlanStartWeeks(eventDistance)
      ? `Freie Vorphase (> ${getPlanStartWeeks(eventDistance)} Wochen): Zielmix Lauf/Rad ~${Math.round(computeRunShareTarget(weeksToEvent, eventDistance) * 100)}/${Math.max(0, 100 - Math.round(computeRunShareTarget(weeksToEvent, eventDistance) * 100))}`
      : `Planphase aktiv (<= ${getPlanStartWeeks(eventDistance)} Wochen): Blocksteuerung BASE/BUILD/RACE`,
  ]);

  addDecisionBlock("KRAFTPLAN", [
    `Phase: ${strengthPlan.phase} · Fokus: ${strengthPlan.focus}`,
    `Ziel: ${strengthPlan.objective}`,
    `Umfang: ${strengthPlan.sessionsPerWeek}×/Woche à ${strengthPlan.durationMin[0]}–${strengthPlan.durationMin[1]}′`,
    ...strengthPlan.sessions.map((session) => `${session.name}: ${session.exercises.join(" · ")}`),
    `Notfallmodus: 2×12 Squats · 2×30s Plank · 2×12 Monster Walk`,
  ]);

  addDecisionBlock("BOTTOM LINE", decisionCompact.bottomLine);

  return lines.join("\n");
}

function formatNextAllowed(dayIso, nextAllowedIso) {
  if (!nextAllowedIso) return "n/a";
  if (!dayIso || !isIsoDate(dayIso) || !isIsoDate(nextAllowedIso)) return nextAllowedIso;
  const delta = diffDays(dayIso, nextAllowedIso);
  if (delta <= 0) return `${nextAllowedIso} (ab heute)`;
  if (delta === 1) return `${nextAllowedIso} (in 1 Tag)`;
  return `${nextAllowedIso} (in ${delta} Tagen)`;
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
  const lifeEventDaysCurrent = Number(current.lifeEventDays || 0);
  const lifeEventDaysPrevious = Number(previous.lifeEventDays || 0);
  const stopLifeEventDaysCurrent = Number(current.stopLifeEventDays || 0);

  if (lifeEventDaysCurrent > 0) {
    context.push(
      `LifeEvent im aktuellen Fenster: ${lifeEventDaysCurrent} Tag(e) reduziert/pausiert (${stopLifeEventDaysCurrent} Tag(e) krank/verletzt).`
    );
  }

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
    if (lifeEventDaysCurrent > 0 || lifeEventDaysPrevious > 0) {
      const delta = lifeEventDaysCurrent - lifeEventDaysPrevious;
      const deltaText = delta === 0 ? "gleich viel" : delta > 0 ? `+${delta}` : `${delta}`;
      context.push(
        `Reizverlust teilweise durch LifeEvent-Tage erklärbar (aktuell ${lifeEventDaysCurrent}, vorher ${lifeEventDaysPrevious}, Δ ${deltaText}).`
      );
    }
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
  const startIso = isoDate(start);
  const endIsoExclusive = isoDate(end);
  const endIsoInclusive = isoDate(new Date(end.getTime() - 86400000));

  const acts = await fetchIntervalsActivities(env, startIso, endIsoExclusive);
  const events = await fetchIntervalsEvents(env, startIso, endIsoInclusive).catch(() => []);
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

  const lifeEvents = (events || []).filter((e) => isLifeEventCategory(e?.category));
  const eventDaysWithinWindow = (event) => {
    const eventStart = String(event?.start_date_local || event?.start_date || "").slice(0, 10);
    if (!isIsoDate(eventStart)) return 0;
    const eventEndRaw = String(event?.end_date_local || event?.end_date || "").slice(0, 10);
    const eventEndExclusive = isIsoDate(eventEndRaw)
      ? eventEndRaw
      : isoDate(new Date(new Date(eventStart + "T00:00:00Z").getTime() + 86400000));
    const overlapStart = eventStart > startIso ? eventStart : startIso;
    const overlapEndExclusive = eventEndExclusive < endIsoExclusive ? eventEndExclusive : endIsoExclusive;
    const days = daysBetween(overlapStart, overlapEndExclusive);
    return Number.isFinite(days) ? Math.max(0, days) : 0;
  };

  const eventDays = lifeEvents.map((e) => ({
    category: normalizeEventCategory(e?.category),
    days: eventDaysWithinWindow(e),
  }));
  const lifeEventDays = sum(eventDays.map((x) => x.days));
  const stopLifeEventDays = sum(eventDays.filter((x) => x.category === "SICK" || x.category === "INJURED").map((x) => x.days));
  const holidayLifeEventDays = sum(eventDays.filter((x) => x.category === "HOLIDAY").map((x) => x.days));
  const lifeEventSummary = eventDays
    .filter((x) => x.days > 0)
    .reduce((acc, x) => {
      acc[x.category] = (acc[x.category] || 0) + x.days;
      return acc;
    }, {});
  const hasLifeEvent = lifeEventDays > 0;

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
  const daysAll = listIsoDaysInclusive(startIso, endIsoInclusive);
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

  if (hasLifeEvent) {
    const lifeEventLine = Object.entries(lifeEventSummary)
      .map(([category, days]) => `${getLifeEventCategoryLabel(category)}=${days}d`)
      .join(", ");
    findings.push(`LifeEvent erkannt: ${lifeEventLine}. Bewertung von Reiz/Frequenz entsprechend relativieren.`);
    if (stopLifeEventDays > 0) {
      actions.push("Bei krank/verletzt: Fokus zuerst auf vollständige Regeneration, dann mit kurzen lockeren Läufen wieder einsteigen.");
    } else if (holidayLifeEventDays > 0) {
      actions.push("Nach Urlaub: Belastung 3–5 Tage progressiv hochfahren (nicht direkt volle Intensität).");
    }
  }

  // Absolute: too little training
  if (totalRuns === 0) {
    findings.push("Kein Lauf im Analysefenster → keine belastbare Diagnose möglich.");
    actions.push("Starte mit 2–3 lockeren Läufen/Woche (30–50min), bevor du harte Schlüsse ziehst.");
  } else {
    // Longrun
    if (longRuns.length === 0 && !hasLifeEvent) {
      findings.push(`Zu wenig Longruns: 0× ≥60min in ${windowDays} Tagen.`);
      actions.push("1×/Woche Longrun ≥60–75min (locker) als Basisbaustein.");
    } else if (longPerWeek < 0.8 && windowDays >= 14 && !hasLifeEvent) {
      findings.push(
        `Longrun-Frequenz niedrig: ${longRuns.length}× in ${windowDays} Tagen (~${longPerWeek.toFixed(1)}/Woche).`
      );
      actions.push("Longrun-Frequenz Richtung 1×/Woche stabilisieren.");
    }

    // Key
    if (keyRuns.length === 0 && !hasLifeEvent) {
      findings.push(`Zu wenig Qualität: 0× Key (key:*) in ${windowDays} Tagen.`);
      actions.push("Wenn Aufbau/Spezifisch: 1× Key/Woche (Schwelle ODER VO2) einbauen.");
    } else if (keyPerWeek < 0.6 && windowDays >= 14 && !hasLifeEvent) {
      findings.push(
        `Key-Frequenz niedrig: ${keyRuns.length}× in ${windowDays} Tagen (~${keyPerWeek.toFixed(1)}/Woche).`
      );
      actions.push("Key-Frequenz auf 1×/Woche anheben (wenn Regeneration ok).");
    }

    // Volume / frequency
    if (runsPerWeek < 2.0 && windowDays >= 14 && !hasLifeEvent) {
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
      if (hasLifeEvent) {
        findings.push(`Wöchentlicher Laufreiz niedrig (~${Math.round(weeklyLoad)}/Woche), plausibel mit LifeEvent-Tagen im Fenster.`);
      } else {
        findings.push(`Wöchentlicher Laufreiz niedrig: ~${Math.round(weeklyLoad)}/Woche (Load).`);
        actions.push("Motor-Aufbau braucht Kontinuität: 2–4 Wochen stabilen Reiz setzen, erst dann bewerten.");
      }
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
  lines.push("Struktur (Trainingslehre):");
  if (hasLifeEvent) {
    lines.push(
      `- Verfügbarkeit: eingeschränkt (${Object.entries(lifeEventSummary)
        .map(([category, days]) => `${getLifeEventCategoryLabel(category)} ${days}d`)
        .join(" · ")})`
    );
  } else {
    lines.push("- Verfügbarkeit: normal (kein Urlaub/krank/verletzt im Fenster)");
  }
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

  const miniPlan = buildMiniPlanTargets({ runsPerWeek, weeklyLoad, keyPerWeek });
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
    lifeEventDays,
    stopLifeEventDays,
    holidayLifeEventDays,
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
  const description = toHardLineBreakText(noteText);

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

// Create/update a blue NOTE event for the daily wellness report
async function upsertDailyReportNote(env, dayIso, noteText) {
  const external_id = `daily-report-${dayIso}`;
  const name = "Daily-Report";
  const description = toHardLineBreakText(noteText);

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

function toHardLineBreakText(text) {
  const normalized = String(text ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
  return normalized.split("\n").join("<br />\n");
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
  return hr >= HFMAX * THRESHOLD_HR_PCT;
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
  const runGoal = await resolveWatchfaceRunGoal(env, end);
  const strengthPolicy = evaluateStrengthPolicy(strengthSum7);
  return {
    ok: true,
    endIso: end,
    days,
    runLoad,
    runSum7,
    runGoal,
    strengthMin,
    strengthSum7,
    strengthGoal: KRAFT_TARGET,
    strengthMinRunfloor: KRAFT_MIN_RUNFLOOR,
    strengthMax: KRAFT_MAX,
    strengthScore: strengthPolicy.score,
    strengthConfidenceDelta: strengthPolicy.confidenceDelta,
    strengthKeyCap: strengthPolicy.keyCapOverride,
    updatedAt: new Date().toISOString(),
  };
}

async function resolveWatchfaceRunGoal(env, dayIso) {
  const ctx = {
    wellnessCache: new Map(),
    blockStateCache: new Map(),
  };
  const lookbackDays = 14;

  for (let i = 0; i <= lookbackDays; i += 1) {
    const probeDay = isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - i * 86400000));

    // Prefer the goal that was already written into the Daily-Report NOTE
    // so watchface consumes exactly the same output and does not re-derive it.
    const events = await fetchIntervalsEvents(env, probeDay, probeDay);
    const dailyReport = (events || []).find((e) => String(e?.external_id || "") === `daily-report-${probeDay}`);
    const goalFromDailyReport = parseRunGoalFromDailyReportNote(dailyReport?.description);
    if (Number.isFinite(goalFromDailyReport) && goalFromDailyReport > 0) {
      return Math.round(goalFromDailyReport);
    }

    const persisted = await getPersistedBlockState(ctx, env, probeDay);
    if (Number.isFinite(persisted?.effectiveFloorTarget) && persisted.effectiveFloorTarget > 0) {
      return Math.round(persisted.effectiveFloorTarget);
    }
    if (Number.isFinite(persisted?.floorTarget) && persisted.floorTarget > 0) {
      return Math.round(persisted.floorTarget);
    }
  }

  return MIN_STIMULUS_7D_RUN_EVENT;
}

function parseRunGoalFromDailyReportNote(description) {
  if (!description) return null;
  const plain = String(description)
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/&nbsp;/gi, " ");
  const match = plain.match(/RunFloor\s*\(7\s*Tage\)\s*:\s*\d+\s*\/\s*(\d+)/i);
  if (!match) return null;
  const goal = Number(match[1]);
  return Number.isFinite(goal) ? goal : null;
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
  const events = await fetchUpcomingEvents(env, auth, debug, 8000, dayIso);
  const races = (events || []).filter((e) => normalizeEventCategory(e.category) === "RACE_A");
  const recentHolidayEvent = findRecentHolidayEvent(events || [], dayIso);

  const activeLifeEvents = (events || []).filter(
    (e) => isLifeEventCategory(e?.category) && isLifeEventActiveOnDay(e, dayIso)
  );
  activeLifeEvents.sort((a, b) => {
    const pa = LIFE_EVENT_CATEGORY_PRIORITY.indexOf(normalizeEventCategory(a?.category));
    const pb = LIFE_EVENT_CATEGORY_PRIORITY.indexOf(normalizeEventCategory(b?.category));
    return (pa === -1 ? 999 : pa) - (pb === -1 ? 999 : pb);
  });
  const activeLifeEvent = activeLifeEvents[0] || null;
  const lifeEventEffect = getLifeEventEffect(activeLifeEvent);

  // sort by start date (local)
  const normDay = (e) => String(e?.start_date_local || e?.start_date || "").slice(0, 10);
  const sorted = (races || [])
    .map((e) => ({ e, day: normDay(e) }))
    .filter((x) => isIsoDate(x.day))
    .sort((a, b) => a.day.localeCompare(b.day));

  const next = sorted.find((x) => x.day >= dayIso) || null;
  const lastPast = [...sorted].reverse().find((x) => x.day < dayIso) || null;

  if (lastPast) {
    const daysSinceLastEvent = diffDays(lastPast.day, dayIso);
    if (Number.isFinite(daysSinceLastEvent) && daysSinceLastEvent >= 0 && daysSinceLastEvent <= POST_EVENT_OPEN_DAYS) {
      return {
        mode: "OPEN",
        primary: "open",
        nextEvent: null,
        eventError: null,
        postEventOpenActive: true,
        postEventOpenDaysLeft: POST_EVENT_OPEN_DAYS - daysSinceLastEvent,
        lastEventDate: lastPast.day,
        activeLifeEvent,
        lifeEventEffect,
        recentHolidayEvent,
      };
    }
  }

  if (!next?.e) {
    return {
      mode: "OPEN",
      primary: "open",
      nextEvent: null,
      eventError: null,
      postEventOpenActive: false,
      activeLifeEvent,
      lifeEventEffect,
      recentHolidayEvent,
    };
  }

  const primary = inferSportFromEvent(next.e);
  if (primary === "bike") {
    return {
      mode: "EVENT",
      primary: "bike",
      nextEvent: next.e,
      eventError: null,
      postEventOpenActive: false,
      activeLifeEvent,
      lifeEventEffect,
      recentHolidayEvent,
    };
  }
  // Default RACE_A bei dir ist sehr wahrscheinlich Lauf – aber wir bleiben bei heuristics:
  if (primary === "run" || primary === "unknown") {
    return {
      mode: "EVENT",
      primary: "run",
      nextEvent: next.e,
      eventError: null,
      postEventOpenActive: false,
      activeLifeEvent,
      lifeEventEffect,
      recentHolidayEvent,
    };
  }

  return {
    mode: "OPEN",
    primary: "open",
    nextEvent: next.e,
    eventError: null,
    postEventOpenActive: false,
    activeLifeEvent,
    lifeEventEffect,
    recentHolidayEvent,
  };
}

function findRecentHolidayEvent(events, dayIso) {
  if (!Array.isArray(events) || !isIsoDate(dayIso)) return null;
  const windowStartIso = isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - 6 * 86400000));
  const windowEndIso = isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() + 86400000));

  const holidays = events
    .filter((e) => normalizeEventCategory(e?.category) === "HOLIDAY")
    .map((event) => {
      const startIso =
        parseLifeEventBoundary(event, "start_date_local") ||
        parseLifeEventBoundary(event, "start_date");
      if (!startIso) return null;

      const endIso =
        parseLifeEventBoundary(event, "end_date_local") ||
        parseLifeEventBoundary(event, "end_date") ||
        isoDate(new Date(new Date(startIso + "T00:00:00Z").getTime() + 86400000));

      const overlapStart = startIso > windowStartIso ? startIso : windowStartIso;
      const overlapEnd = endIso < windowEndIso ? endIso : windowEndIso;
      const overlapDays = overlapEnd > overlapStart ? diffDays(overlapStart, overlapEnd) : 0;
      if (overlapDays <= 0) return null;

      return { event, endIso, startIso };
    })
    .filter(Boolean)
    .sort((a, b) => {
      if (a.endIso === b.endIso) return b.startIso.localeCompare(a.startIso);
      return b.endIso.localeCompare(a.endIso);
    });

  return holidays[0]?.event || null;
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

  if (modeInfo?.postEventOpenActive) {
    return {
      label: "OPEN:POST_EVENT",
      specificLabel: "Freier Block (2 Wochen nach Event)",
      specificKind: "open",
      specificThreshold: 0,
      aerobicK: AEROBIC_K_DEFAULT,
      useAerobicFloor: false,
      recovery: true,
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


async function fetchUpcomingEvents(env, auth, debug, timeoutMs, dayIso) {

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

  const races = events.filter((e) => normalizeEventCategory(e.category) === "RACE_A");

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

  return events;
}
