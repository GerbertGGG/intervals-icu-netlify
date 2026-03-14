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
          headers: WATCHFACE_PREFLIGHT_HEADERS,
        });
      }

      // optional: ?date=YYYY-MM-DD zum Testen, sonst "heute"
      const date = url.searchParams.get("date");
      const endIso = date && isIsoDate(date) ? date : isoDateBerlin(new Date());

      try {
        const payload = await buildWatchfacePayload(env, endIso);

        return new Response(JSON.stringify(payload), {
          headers: WATCHFACE_JSON_HEADERS,
        });
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
            headers: WATCHFACE_ERROR_HEADERS,
          }
        );
      }
    }


    if (url.pathname === "/sync") {
      const write = parseBooleanParam(url.searchParams, "write");
      const debug = parseBooleanParam(url.searchParams, "debug");
      const reportVerbosity = parseReportVerbosity(url.searchParams, { debug });

      const date = url.searchParams.get("date");
      const from = url.searchParams.get("from");
      const to = url.searchParams.get("to");
      const days = clampInt(url.searchParams.get("days") ?? "14", 1, 31);

      const warmupSkipSec = clampInt(url.searchParams.get("warmup_skip") ?? "600", 0, 1800);
      const raceStartParamRaw = getSearchParamAny(url.searchParams, [
        "race_start",
        "race_start_override",
        "race_start_iso",
        "racestart",
        "raceStart",
      ]);
      const raceStartOverrideIso = raceStartParamRaw && isIsoDate(raceStartParamRaw) ? raceStartParamRaw : null;
      const blockStartParamRaw = getSearchParamAny(url.searchParams, [
        "block_start",
        "block_start_override",
        "block_start_iso",
        "blockstart",
        "blockStart",
      ]);
      let blockStartOverrideIso = blockStartParamRaw && isIsoDate(blockStartParamRaw) ? blockStartParamRaw : null;

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

      if (raceStartParamRaw && !raceStartOverrideIso) {
        return json({ ok: false, error: "Invalid race_start format (YYYY-MM-DD)" }, 400);
      }
      if (blockStartParamRaw && !blockStartOverrideIso) {
        return json({ ok: false, error: "Invalid block_start format (YYYY-MM-DD)" }, 400);
      }

      if (!write && debug && !blockStartOverrideIso) {
        // Dry-run tests with explicit date/range should not silently inherit a persisted block start.
        // This keeps /sync?date=...&debug=true&write=false predictable without requiring block_start.
        blockStartOverrideIso = oldest;
      }

      if (debug) {
        try {
          const result = await syncRange(env, oldest, newest, write, true, warmupSkipSec, {
            raceStartOverrideIso,
            blockStartOverrideIso,
            reportVerbosity,
          });
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
              raceStartOverrideIso,
              blockStartOverrideIso,
            },
            500
          );
        }
      }

      // async fire-and-forget (but don't swallow silently)
      ctx?.waitUntil?.(
        (async () => {
          await syncRange(env, oldest, newest, write, false, warmupSkipSec, {
            raceStartOverrideIso,
            blockStartOverrideIso,
            reportVerbosity,
          });
        })().catch((e) => {
          console.error("sync/model job failed", e);
        })
      );

      return json({ ok: true, oldest, newest, write, warmupSkipSec, raceStartOverrideIso, blockStartOverrideIso, reportVerbosity });
    }

    if (url.pathname === "/sync-step") {
      try {
        return await handleStepSync(req, env, ctx);
      } catch (e) {
        return json(
          {
            ok: false,
            error: "Worker exception",
            message: String(e?.message ?? e),
            stack: String(e?.stack ?? ""),
          },
          500
        );
      }
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // Run every 30 minutes, but only process/write between 07:00 and 21:00 Berlin time.
    // This avoids unnecessary requests overnight.
    if (!isScheduledWindowBerlin(event)) {
      return;
    }

    // Sync only when today's run set changed since the previous crawl.
    // In runMetricsOnly mode we refresh the canonical decision blocks from fresh render output
    // (HEUTE/WARUM/STATUS/FOKUS/TRAININGSSTAND/EMPFEHLUNGEN/DIAGNOSE/BOTTOM LINE) and avoid
    // piecemeal legacy text carry-over from older baseline structures.
    const today = isoDate(new Date());
    const runMetricsOnly = true;
    const berlinHour = getBerlinHourFromScheduledEvent(event);
    const isSevenAmBaselineRun = berlinHour === 7;

    ctx.waitUntil(
      (async () => {
        if (isSevenAmBaselineRun) {
          await syncRange(env, today, today, true, false, 600);
          await writeScheduledRunState(env, {
            day: today,
            runIdsSignature: "",
            runCount: 0,
            checkedAt: new Date().toISOString(),
          });
          return;
        }

        const activities = await fetchIntervalsActivities(env, today, today);
        const todaysRuns = Array.isArray(activities)
          ? activities.filter((a) => {
              const day = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
              return day === today && isRun(a);
            })
          : [];

        if (!todaysRuns.length) {
          await writeScheduledRunState(env, {
            day: today,
            runIdsSignature: "",
            runCount: 0,
            checkedAt: new Date().toISOString(),
          });
          return;
        }

        const currentSignature = buildRunIdsSignature(todaysRuns);
        const previousState = await readScheduledRunState(env);
        if (
          previousState?.day === today
          && String(previousState?.runIdsSignature || "") === currentSignature
        ) {
          return;
        }

        await syncRange(env, today, today, true, false, 600, { runMetricsOnly, runMetricsOnlyIfExisting: true });

        await writeScheduledRunState(env, {
          day: today,
          runIdsSignature: currentSignature,
          runCount: todaysRuns.length,
          checkedAt: new Date().toISOString(),
        });
      })().catch((e) => {
        console.error("scheduled sync/model job failed", e);
      })
    );
  },
};

const WATCHFACE_PREFLIGHT_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, OPTIONS",
  "access-control-allow-headers": "content-type",
  "access-control-max-age": "86400",
};

const WATCHFACE_JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
  "access-control-allow-origin": "*",
};

const WATCHFACE_ERROR_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "access-control-allow-origin": "*",
};

const STEP_SYNC_KV_PREFIX = "syncstep:state:";
const SCHEDULED_RUN_STATE_KV_PREFIX = "scheduled:runs:state:";
const STEP_SYNC_ADVANCE_DAYS = 7;
const REPORT_VERBOSITY_VALUES = new Set(["coach", "diagnose", "debug"]);

function parseBooleanParam(searchParams, key) {
  return (searchParams.get(key) || "").toLowerCase() === "true";
}

function parseReportVerbosity(searchParams, { debug = false } = {}) {
  const raw = String(searchParams.get("verbosity") || "").trim().toLowerCase();
  if (REPORT_VERBOSITY_VALUES.has(raw)) return raw;
  return "coach";
}

function getSearchParamAny(searchParams, keys) {
  for (const key of keys) {
    const direct = searchParams.get(key);
    if (direct) return direct;
  }

  const lowerMap = new Map();
  for (const [key, value] of searchParams.entries()) {
    if (!value) continue;
    const normalizedKey = String(key || "").toLowerCase();
    if (!lowerMap.has(normalizedKey)) lowerMap.set(normalizedKey, value);
  }

  for (const key of keys) {
    const value = lowerMap.get(String(key).toLowerCase());
    if (value) return value;
  }
  return "";
}

function addDaysIso(dayIso, days) {
  return isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() + Number(days) * 86400000));
}

function makeStepSyncStateKey(env) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  return `${STEP_SYNC_KV_PREFIX}${athleteId}`;
}

async function readStepSyncState(env) {
  if (!hasKv(env)) return null;
  return readKvJson(env, makeStepSyncStateKey(env));
}

async function writeStepSyncState(env, state) {
  if (!hasKv(env)) return;
  await writeKvJson(env, makeStepSyncStateKey(env), state);
}

function makeScheduledRunStateKey(env) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  return `${SCHEDULED_RUN_STATE_KV_PREFIX}${athleteId}`;
}

async function readScheduledRunState(env) {
  if (!hasKv(env)) return null;
  return readKvJson(env, makeScheduledRunStateKey(env));
}

async function writeScheduledRunState(env, state) {
  if (!hasKv(env)) return;
  await writeKvJson(env, makeScheduledRunStateKey(env), state);
}

function buildRunIdsSignature(runs) {
  const ids = (runs || [])
    .map((a) => String(a?.id || "").trim())
    .filter(Boolean)
    .sort();
  return ids.join(",");
}

function normalizeStepSyncState(raw) {
  if (!raw || typeof raw !== "object") return null;
  const from = isIsoDate(raw.from) ? raw.from : null;
  const to = isIsoDate(raw.to) ? raw.to : null;
  const next = isIsoDate(raw.next) ? raw.next : from;
  if (!from || !to || !next) return null;
  if (to < from) return null;
  return {
    from,
    to,
    next,
    done: next > to,
    updatedAt: raw.updatedAt || null,
  };
}

async function handleStepSync(req, env, ctx) {
  const url = new URL(req.url);
  const write = parseBooleanParam(url.searchParams, "write");
  const debug = parseBooleanParam(url.searchParams, "debug");
  const reportVerbosity = parseReportVerbosity(url.searchParams, { debug });
  const reset = parseBooleanParam(url.searchParams, "reset");
  const warmupSkipSec = clampInt(url.searchParams.get("warmup_skip") ?? "600", 0, 1800);

  if (!hasKv(env)) {
    return json({ ok: false, error: "KV binding required for /sync-step" }, 400);
  }

  const fromParam = url.searchParams.get("from");
  const toParam = url.searchParams.get("to");
  const raceStartParamRaw = getSearchParamAny(url.searchParams, [
    "race_start",
    "race_start_override",
    "race_start_iso",
    "racestart",
    "raceStart",
  ]);
  const raceStartOverrideIso = raceStartParamRaw && isIsoDate(raceStartParamRaw) ? raceStartParamRaw : null;
  const blockStartParamRaw = getSearchParamAny(url.searchParams, [
    "block_start",
    "block_start_override",
    "block_start_iso",
    "blockstart",
    "blockStart",
  ]);
  const blockStartOverrideIso = blockStartParamRaw && isIsoDate(blockStartParamRaw) ? blockStartParamRaw : null;

  if (raceStartParamRaw && !raceStartOverrideIso) {
    return json({ ok: false, error: "Invalid race_start format (YYYY-MM-DD)" }, 400);
  }
  if (blockStartParamRaw && !blockStartOverrideIso) {
    return json({ ok: false, error: "Invalid block_start format (YYYY-MM-DD)" }, 400);
  }

  let state = normalizeStepSyncState(await readStepSyncState(env));

  if (reset || !state) {
    if (!isIsoDate(fromParam || "") || !isIsoDate(toParam || "")) {
      return json({ ok: false, error: "Provide valid from/to (YYYY-MM-DD) when starting or resetting step sync" }, 400);
    }
    if (toParam < fromParam) {
      return json({ ok: false, error: "`to` must be >= `from`" }, 400);
    }
    if (diffDays(fromParam, toParam) > 62) {
      return json({ ok: false, error: "Max step-sync range is 62 days" }, 400);
    }
    state = {
      from: fromParam,
      to: toParam,
      next: fromParam,
      done: false,
      updatedAt: new Date().toISOString(),
    };
    await writeStepSyncState(env, state);
  }

  if (state.done || state.next > state.to) {
    return json({
      ok: true,
      mode: "step-sync",
      done: true,
      from: state.from,
      to: state.to,
      next: null,
      message: "Range already completed. Use reset=true with from/to to start a new range.",
    });
  }

  const day = state.next;
  const result = await syncRange(env, day, day, write, debug, warmupSkipSec, {
    raceStartOverrideIso,
    blockStartOverrideIso,
    reportVerbosity,
  });

  const next = addDaysIso(day, STEP_SYNC_ADVANCE_DAYS);
  const done = next > state.to;
  const nextState = {
    ...state,
    next,
    done,
    updatedAt: new Date().toISOString(),
  };
  await writeStepSyncState(env, nextState);

  return json({
    ok: true,
    mode: "step-sync",
    processedDay: day,
    done,
    from: state.from,
    to: state.to,
    next: done ? null : next,
    stepDays: STEP_SYNC_ADVANCE_DAYS,
    write,
    warmupSkipSec,
    ...(debug ? { result } : {}),
  });
}


function getBerlinHourFromScheduledEvent(event) {
  const t = Number(event?.scheduledTime);
  if (!Number.isFinite(t)) return null;
  const hour = Number(
    new Intl.DateTimeFormat("en-GB", {
      hour: "2-digit",
      hour12: false,
      timeZone: "Europe/Berlin",
    }).format(new Date(t))
  );
  return Number.isFinite(hour) ? hour : null;
}

function isScheduledWindowBerlin(event) {
  const hour = getBerlinHourFromScheduledEvent(event);
  return Number.isFinite(hour) && hour >= 7 && hour <= 21;
}

function isEveningBerlinRun(event) {
  const hour = getBerlinHourFromScheduledEvent(event);
  return Number.isFinite(hour) && hour >= 20;
}

// ================= CONFIG =================
// ================= GUARDRAILS (NEW) =================
const MAX_KEYS_7D = 2;
const KEY_MIN_GAP_DAYS_DEFAULT = 3;
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
        exercises: [
          "3×10 Single Leg RDL",
          "3×30s Seitstütz",
          "2×12 Monster Walk",
          "2×30s Stabikissen Einbeinstand",
          "2×8–10 Tibialis Raises (optional)",
        ],
      },
    ],
  },
  BUILD: {
    phase: "BUILD",
    focus: "Schwelle-Block kompatibel",
    objective: "Kraft + Laufökonomie ohne Ermüdungs-Overkill",
    sessionsPerWeek: 2,
    durationMin: [20, 25],
    sessions: [
      {
        name: "Einheit A – Kraft + 1× Plyo",
        exercises: [
          "3×8 Bulgarian Split Squat",
          "3×8 Hip Thrust einbeinig",
          "3×6 Jump Squats (kontrolliert, volle Pause)",
          "2×30s Plank",
          "2×30–45s Waden isometrisch (Soleus) pro Seite",
        ],
      },
      {
        name: "Einheit B – Kraft (ohne Plyo)",
        exercises: ["3×8 Step-Ups (kontrolliert, sauber)", "3×8 Single Leg Deadlift", "2×12 Monster Walk", "2×30s Seitstütz"],
      },
    ],
  },
  RACE: {
    phase: "RACE",
    focus: "Erhalt",
    objective: "Frische",
    sessionsPerWeek: 1,
    durationMin: [12, 15],
    sessions: [
      {
        name: "Einheit – Erhalt",
        exercises: [
          "2×8 Split Squats",
          "2×8 Hip Thrust",
          "2×20s Plank",
          "1×30s Stabikissen Einbein",
          "1×30–45s Waden isometrisch (Soleus) pro Seite",
        ],
      },
    ],
  },
};
const INTENSITY_DISTRIBUTION_TARGET = {
  BASE: {
    easyMin: 0.7,
    easyMax: 0.9,
    midMin: 0.05,
    midMax: 0.15,
    hardMax: 0.1,
    byDistance: {
      "5k": { easyMin: 0.7, easyMax: 0.75, midMin: 0.12, midMax: 0.15, hardMax: 0.1 },
      "10k": { easyMin: 0.75, easyMax: 0.8, midMin: 0.1, midMax: 0.12, hardMax: 0.08 },
      hm: { easyMin: 0.8, easyMax: 0.85, midMin: 0.08, midMax: 0.1, hardMax: 0.05 },
      m: { easyMin: 0.85, easyMax: 0.9, midMin: 0.05, midMax: 0.08, hardMax: 0.03 },
    },
  },
  BUILD: {
    easyMin: 0.7,
    easyMax: 0.75,
    midMin: 0.1,
    midMax: 0.3,
    hardMax: 0.2,
    byDistance: {
      "5k": { easyMin: 0.7, easyMax: 0.75, midMin: 0.16, midMax: 0.24, hardMax: 0.14 },
      "10k": { easyMin: 0.72, easyMax: 0.78, midMin: 0.17, midMax: 0.23, hardMax: 0.1 },
      hm: { easyMin: 0.75, easyMax: 0.82, midMin: 0.14, midMax: 0.2, hardMax: 0.08 },
      m: { easyMin: 0.78, easyMax: 0.85, midMin: 0.1, midMax: 0.16, hardMax: 0.06 },
    },
  },
  RACE: {
    easyMin: 0.6,
    easyMax: 0.75,
    midMin: 0,
    midMax: 0.2,
    hardMax: 0.35,
    byDistance: {
      "5k": { easyMin: 0.65, easyMax: 0.75, midMin: 0, midMax: 0.05, hardMax: 0.35 },
      "10k": { easyMin: 0.6, easyMax: 0.65, midMin: 0.15, midMax: 0.2, hardMax: 0.25 },
      hm: { easyMin: 0.6, easyMax: 0.65, midMin: 0.15, midMax: 0.2, hardMax: 0.25 },
      m: { easyMin: 0.65, easyMax: 0.75, midMin: 0.05, midMax: 0.1, hardMax: 0.3 },
    },
  },
  RESET: {
    easyMin: 0.9,
    hardMax: 0.03,
  },
};

function getIntensityDistributionTargets(block, eventDistance) {
  const blockTargets = INTENSITY_DISTRIBUTION_TARGET[block] ?? INTENSITY_DISTRIBUTION_TARGET.BASE;
  const dist = normalizeEventDistance(eventDistance);
  const byDistance = blockTargets?.byDistance?.[dist];
  return byDistance ? { ...blockTargets, ...byDistance } : blockTargets;
}

const INTENSITY_LOOKBACK_DAYS = 14;
const INTENSITY_CLEAR_OVERSHOOT = 0.01;
const INTENSITY_COMPARISON_TOLERANCE = INTENSITY_CLEAR_OVERSHOOT;
const BASE_URL = "https://intervals.icu/api/v1";
const DETECTIVE_KV_PREFIX = "detective:week:";
const DETECTIVE_KV_HISTORY_KEY = "detective:history";
const WEEKDOC_KV_PREFIX = "u:";
const WEEKDOC_INDEX_SUFFIX = ":idx:weeks";
const WEEKDOC_KEY_PREFIX = ":week:";
const WEEKDOC_INDEX_LIMIT = 20;
const PATTERN_WINDOW_WEEKS = 8;
const PATTERN_MIN_WEEKS = 4;
const PATTERN_MIN_GROUP_N = 3;
const PATTERN_MIN_CORR_N = 6;
const PATTERN_MIN_CORR_ABS = 0.25;
const BLOCK_STATE_KV_PREFIX = "blockstate:latest:";
const LEVER_REVIEW_KV_PREFIX = "lever:review:";
const DETECTIVE_HISTORY_LIMIT = 12;
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
 *    - operative Overlays: NORMAL, DELOAD, TAPER, POST_RACE_RAMP.
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
const ENABLE_FATIGUE_OVERRIDE_DEFAULT = true;
const RAMP_PCT_7D_LIMIT = 0.25;    // +25% vs previous 7d
const MONOTONY_7D_LIMIT = 2.0;     // mean/sd daily load
const STRAIN_7D_LIMIT = 1200;      // monotony * weekly load (scale depends on your load units)
const ACWR_HIGH_LIMIT = 1.5;       // acute:chronic workload ratio
const RUN_DISTANCE_14D_LIMIT = 1.3; // +30% vs previous 14d

const RUNTIME_CONFIG_DEFAULTS = {
  enableFatigueOverride: ENABLE_FATIGUE_OVERRIDE_DEFAULT,
  maxKeys7d: MAX_KEYS_7D,
  keyMinGapDays: KEY_MIN_GAP_DAYS_DEFAULT,
  fatigueThresholds: {
    rampPct: RAMP_PCT_7D_LIMIT,
    monotony: MONOTONY_7D_LIMIT,
    strain: STRAIN_7D_LIMIT,
    acwrHigh: ACWR_HIGH_LIMIT,
    runDistance14dLimit: RUN_DISTANCE_14D_LIMIT,
  },
};

function parseBooleanEnv(value, fallback) {
  if (typeof value === "boolean") return value;
  const raw = String(value ?? "").trim().toLowerCase();
  if (!raw) return fallback;
  if (["1", "true", "yes", "on"].includes(raw)) return true;
  if (["0", "false", "no", "off"].includes(raw)) return false;
  return fallback;
}

function parseNumberEnv(value, fallback, min = null, max = null) {
  const n = Number(value);
  if (!Number.isFinite(n)) return fallback;
  let next = n;
  if (Number.isFinite(min)) next = Math.max(min, next);
  if (Number.isFinite(max)) next = Math.min(max, next);
  return next;
}

function loadRuntimeConfig(env) {
  const defaults = RUNTIME_CONFIG_DEFAULTS;
  return {
    enableFatigueOverride: parseBooleanEnv(env?.ENABLE_FATIGUE_OVERRIDE, defaults.enableFatigueOverride),
    maxKeys7d: parseNumberEnv(env?.MAX_KEYS_7D, defaults.maxKeys7d, 0, 3),
    keyMinGapDays: parseNumberEnv(env?.KEY_MIN_GAP_DAYS, defaults.keyMinGapDays, 1, 7),
    fatigueThresholds: {
      rampPct: parseNumberEnv(env?.FATIGUE_RAMP_PCT_LIMIT, defaults.fatigueThresholds.rampPct, 0, 2),
      monotony: parseNumberEnv(env?.FATIGUE_MONOTONY_LIMIT, defaults.fatigueThresholds.monotony, 0, 10),
      strain: parseNumberEnv(env?.FATIGUE_STRAIN_LIMIT, defaults.fatigueThresholds.strain, 0, 100000),
      acwrHigh: parseNumberEnv(env?.FATIGUE_ACWR_HIGH_LIMIT, defaults.fatigueThresholds.acwrHigh, 0, 10),
      runDistance14dLimit: parseNumberEnv(env?.RUN_DISTANCE_14D_LIMIT, defaults.fatigueThresholds.runDistance14dLimit, 1, 5),
    },
  };
}


const GA_MIN_SECONDS = 25 * 60;
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
  spikeGuardLookbackDays: 14,
  startMin: 45,
  targetMinByDistance: {
    "5k": 60,
    "10k": 60,
    hm: 90,
    m: 120,
  },
};

const DISTANCE_REQUIREMENTS = {
  "5k": {
    weights: { base: 0.35, robustness: 0.25, specificity: 0.20, execution: 0.12, longrun: 0.08 },
    longrunTargetMin: 60,
    keyFocus: ["vo2", "racepace", "threshold"],
    intensityProfile: "5k",
  },
  "10k": {
    weights: { base: 0.33, robustness: 0.22, specificity: 0.20, longrun: 0.15, execution: 0.10 },
    longrunTargetMin: 75,
    keyFocus: ["threshold", "vo2", "racepace"],
    intensityProfile: "10k",
  },
  hm: {
    weights: { base: 0.30, longrun: 0.22, robustness: 0.20, specificity: 0.18, execution: 0.10 },
    longrunTargetMin: 90,
    keyFocus: ["threshold", "hm_specific_longrun", "racepace"],
    intensityProfile: "hm",
  },
  m: {
    weights: { base: 0.28, longrun: 0.28, robustness: 0.18, specificity: 0.16, execution: 0.10 },
    longrunTargetMin: 120,
    keyFocus: ["marathonpace", "longrun", "threshold"],
    intensityProfile: "m",
  },
};

const DISTANCE_INTENSITY_TARGETS = {
  "5k": { easyMin: 0.70, hardMax: 0.18 },
  "10k": { easyMin: 0.72, hardMax: 0.16 },
  hm: { easyMin: 0.75, hardMax: 0.14 },
  m: { easyMin: 0.78, hardMax: 0.12 },
};


// Minimum stimulus thresholds per mode (tune later)
const MIN_STIMULUS_7D_RUN_EVENT = 135;
const MIN_STIMULUS_7D_BIKE_EVENT = 200;  // bike primary
const RUN_FLOOR_EWMA_LOOKBACK_DAYS = 14;
const WATCHFACE_LOAD_WINDOW_DAYS = 7;
const WATCHFACE_STRENGTH_WINDOW_DAYS = 7;

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
const FIELD_RACE_START_OVERRIDE = "RaceStartOverride";

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
    lifeEventsAll: [],
    byDayRuns: new Map(), // YYYY-MM-DD -> run activities
    // streams memo
    byDayBikes: new Map(), // NEW
    streamsCache: new Map(), // activityId -> Promise(streams)
    activityDetailsCache: new Map(), // activityId -> Promise(activity with intervals)
    // derived GA samples cache (for windows)
    gaSampleCache: new Map(), // key: `${endIso}|${windowDays}|${mode}` -> result
    wellnessCache: new Map(), // dayIso -> wellness payload
    blockStateCache: new Map(), // dayIso -> block state
    // concurrency limiter
    limit: createLimiter(6),
    // debug accumulator
    debugOut: debug ? {} : null,
    runtimeConfig: RUNTIME_CONFIG_DEFAULTS,
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

async function getActivityWithIntervals(ctx, activity) {
  if (!activity?.id) return activity;

  const hasIntervals = getActivityIntervals(activity).length > 0 || getActivityGroups(activity).length > 0;
  if (hasIntervals) return activity;

  const key = String(activity.id);
  if (!ctx.activityDetailsCache.has(key)) {
    const req = ctx.limit(async () => {
      const fromIntervalsEndpoint = await fetchActivityIntervals(ctx.env, key).catch(() => null);
      const intervalPayload = normalizeIntervalsPayload(fromIntervalsEndpoint);
      if (extractIntervals(intervalPayload).length > 0) {
        return intervalPayload;
      }
      return fetchActivityWithIntervals(ctx.env, key).catch(() => null);
    });
    ctx.activityDetailsCache.set(key, req);
  }

  const detailed = await ctx.activityDetailsCache.get(key);
  if (!detailed || typeof detailed !== 'object') return activity;

  return {
    ...activity,
    ...detailed,
    icu_intervals: Array.isArray(detailed?.icu_intervals)
      ? detailed.icu_intervals
      : (Array.isArray(activity?.icu_intervals) ? activity.icu_intervals : []),
    intervals: Array.isArray(detailed?.intervals)
      ? detailed.intervals
      : (Array.isArray(activity?.intervals) ? activity.intervals : []),
    icu_groups: Array.isArray(detailed?.icu_groups)
      ? detailed.icu_groups
      : (Array.isArray(activity?.icu_groups) ? activity.icu_groups : []),
    groups: Array.isArray(detailed?.groups)
      ? detailed.groups
      : (Array.isArray(activity?.groups) ? activity.groups : []),
  };
}

function inferSportFromEvent(ev) {
  const t = String(ev?.type || "").toLowerCase();
  if (t.includes("run")) return "run";
  if (t.includes("ride") || t.includes("bike") || t.includes("cycling")) return "bike";
  return "unknown";
}

function countHolidayDaysInWindow(events, startIsoInclusive, endIsoExclusive) {
  if (!Array.isArray(events) || !isIsoDate(startIsoInclusive) || !isIsoDate(endIsoExclusive) || endIsoExclusive <= startIsoInclusive) {
    return 0;
  }

  const holidayDays = new Set();
  for (const event of events) {
    if (normalizeEventCategory(event?.category) !== "HOLIDAY") continue;

    const eventStartIso = String(event?.start_date_local || event?.start_date || "").slice(0, 10);
    if (!isIsoDate(eventStartIso)) continue;

    const eventEndRaw = String(event?.end_date_local || event?.end_date || "").slice(0, 10);
    const eventEndExclusive = isIsoDate(eventEndRaw)
      ? eventEndRaw
      : isoDate(new Date(new Date(eventStartIso + "T00:00:00Z").getTime() + 86400000));

    const overlapStart = eventStartIso > startIsoInclusive ? eventStartIso : startIsoInclusive;
    const overlapEnd = eventEndExclusive < endIsoExclusive ? eventEndExclusive : endIsoExclusive;
    if (overlapEnd <= overlapStart) continue;

    for (let d = overlapStart; d < overlapEnd; d = isoDate(new Date(new Date(d + "T00:00:00Z").getTime() + 86400000))) {
      holidayDays.add(d);
    }
  }

  return holidayDays.size;
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
  const runtimeConfig = ctx?.runtimeConfig || RUNTIME_CONFIG_DEFAULTS;
  const thresholds = runtimeConfig.fatigueThresholds || RUNTIME_CONFIG_DEFAULTS.fatigueThresholds;

  const start7Iso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const start14Iso = isoDate(new Date(end.getTime() - 13 * 86400000));
  const start28Iso = isoDate(new Date(end.getTime() - 27 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const acts28 = ctx.activitiesAll.filter((a) => {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    return d && d >= start28Iso && d < endIso;
  });

  const dailyLoads = bucketAllLoadsByDay(acts28); // day -> load
  const dailyRunDistKm = {};
  for (const a of acts28) {
    if (!isRun(a)) continue;
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d) continue;
    dailyRunDistKm[d] = (dailyRunDistKm[d] || 0) + extractRunDistanceKm(a);
  }
  const days = Object.keys(dailyLoads).sort();

  // split prev7 and last7 deterministically
  let prev7 = 0;
  let last7 = 0;
  let prev14RunDistKm = 0;
  let last14RunDistKm = 0;
  const start28To14Iso = isoDate(new Date(end.getTime() - 27 * 86400000));
  const endIsoExclusive = isoDate(new Date(end.getTime() + 86400000));

  for (const d of days) {
    const v = Number(dailyLoads[d]) || 0;
    if (d >= start7Iso) last7 += v;
    else if (d >= start14Iso) prev7 += v;

    const runDist = Number(dailyRunDistKm[d]) || 0;
    if (d >= start14Iso) {
      last14RunDistKm += runDist;
    } else if (d >= start28To14Iso) {
      prev14RunDistKm += runDist;
    }
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
  const acuteLoad = last7 / 7;
  const chronicLoad = last28 / 28;
  const acwr = chronicLoad > 0 ? acuteLoad / chronicLoad : null;

  const keyCount7 = await computeKeyCount7d(ctx, dayIso);
  const keyCap = Number.isFinite(options.maxKeys7d) ? options.maxKeys7d : runtimeConfig.maxKeys7d;

  const reasons = [];
  if (keyCount7 > keyCap) reasons.push(`Key-Cap: ${keyCount7}/${keyCap} Key in 7 Tagen`);
  if (rampPct > thresholds.rampPct) reasons.push(`Ramp: ${(rampPct * 100).toFixed(0)}% vs vorherige 7 Tage`);
  if (acwr != null && acwr > thresholds.acwrHigh) reasons.push(`ACWR: ${acwr.toFixed(2)} (> ${thresholds.acwrHigh})`);
  const last14HolidayDays = countHolidayDaysInWindow(ctx?.lifeEventsAll, start14Iso, endIsoExclusive);
  const prev14HolidayDays = countHolidayDaysInWindow(ctx?.lifeEventsAll, start28To14Iso, start14Iso);
  const last14TrainableDays = Math.max(0, 14 - last14HolidayDays);
  const prev14TrainableDays = Math.max(0, 14 - prev14HolidayDays);
  const last14RunDistAdjKm = last14TrainableDays > 0 ? (last14RunDistKm / last14TrainableDays) * 14 : null;
  const prev14RunDistAdjKm = prev14TrainableDays > 0 ? (prev14RunDistKm / prev14TrainableDays) * 14 : null;
  const runDist14dRatio = prev14RunDistAdjKm > 0 ? last14RunDistAdjKm / prev14RunDistAdjKm : null;
  if (runDist14dRatio != null && runDist14dRatio > thresholds.runDistance14dLimit) {
    reasons.push(
      `Run-Distanz 14d (Urlaub bereinigt): ${(runDist14dRatio * 100).toFixed(0)}% der Vorperiode (> ${(thresholds.runDistance14dLimit * 100).toFixed(0)}%)`
    );
  }
  if (monotony > thresholds.monotony) reasons.push(`Monotony: ${monotony.toFixed(2)} (> ${thresholds.monotony})`);
  if (strain > thresholds.strain) reasons.push(`Strain: ${strain.toFixed(0)} (> ${thresholds.strain})`);

  const overrideEnabled = options.enableOverride ?? runtimeConfig.enableFatigueOverride;
  const override = overrideEnabled && reasons.length > 0;

  return {
    override,
    reasons: override ? reasons : [],
    keyCount7,
    keyCap,
    keyCapExceeded: keyCount7 > keyCap,
    rampPct,
    monotony,
    strain,
    acwr,
    runDist14dRatio,
    runDistLast14Km: last14RunDistKm,
    runDistPrev14Km: prev14RunDistKm,
    runDistLast14AdjKm: last14RunDistAdjKm,
    runDistPrev14AdjKm: prev14RunDistAdjKm,
    runDistLast14HolidayDays: last14HolidayDays,
    runDistPrev14HolidayDays: prev14HolidayDays,
    chronicWeekly: chronicLoad * 7,
    acuteLoad,
    chronicLoad,
    last7Load: last7,
    prev7Load: prev7,
  };
}

function extractRunDistanceKm(activity) {
  const candidates = [
    { value: activity?.distanceMeters, unit: "m" },
    { value: activity?.distance_meters, unit: "m" },
    { value: activity?.distance_metres, unit: "m" },
    { value: activity?.details?.distanceMeters, unit: "m" },
    { value: activity?.details?.distance_meters, unit: "m" },
    { value: activity?.details?.distance_metres, unit: "m" },
    { value: activity?.distance, unit: "auto" },
    { value: activity?.details?.distance, unit: "auto" },
  ];
  for (const candidate of candidates) {
    const n = Number(candidate?.value);
    if (!Number.isFinite(n) || n <= 0) continue;
    if (candidate.unit === "m") return n / 1000;
    // Ambiguous "distance" fields are usually meters for API payloads and km for some imports.
    // Single-run values >60 are treated as meters to avoid inflating short runs (<1000m) by 1000x.
    return n > 60 ? n / 1000 : n;
  }
  return 0;
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
  const minGapDays = clampInt(String(ctx?.runtimeConfig?.keyMinGapDays ?? KEY_MIN_GAP_DAYS_DEFAULT), 1, 7);
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
    if (gap < minGapDays) {
      ok = false;
      violation = { prev: keyDates[i - 1], next: keyDates[i], gapDays: gap };
      break;
    }
  }

  const lastKeyIso = keyDates.length ? keyDates[keyDates.length - 1] : null;
  const nextAllowedIso = lastKeyIso ? isoDate(new Date(new Date(lastKeyIso + "T00:00:00Z").getTime() + minGapDays * 86400000)) : null;

  return {
    ok,
    violation,
    lastKeyIso,
    nextAllowedIso,
    minGapDays,
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
const POST_RACE_RAMP_DAYS_BY_DISTANCE = {
  "5k": [3, 9, 16],
  "10k": [3, 10, 18],
  hm: [4, 10, 21],
  m: [5, 14, 28],
};
const POST_RACE_RAMP_FACTORS = [0.5, 0.65, 0.8, 1.0];
const RUN_FLOOR_DELOAD_RANGE = { min: 0.6, max: 0.7 };
const RUN_FLOOR_DELOAD_FACTOR = {
  BASE: 0.7,
  BUILD: 0.65,
  DEFAULT: 0.65,
};
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

function isARaceCategory(category) {
  const cat = normalizeEventCategory(category);
  if (!cat) return false;

  // Intervals kann je nach Quelle unterschiedliche Schreibweisen liefern.
  // Wir berücksichtigen bewusst NUR A-Rennen.
  return cat === "RACE_A" || cat === "A_RACE" || cat === "A-RACE" || cat === "RACE A";
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

function computeRunFloorEwma(
  ctx,
  dayIso,
  {
    eventDate = null,
    eventDistance = null,
    lookbackDays = RUN_FLOOR_EWMA_LOOKBACK_DAYS,
    debugTrace = false,
  } = {}
) {
  const safeLookbackDays = Math.max(10, Math.round(Number(lookbackDays) || RUN_FLOOR_EWMA_LOOKBACK_DAYS));
  const alpha = 2 / (safeLookbackDays + 1);
  const decay = 1 - alpha;
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - (safeLookbackDays - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  const dailyLoads = {};
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    const load = Number(extractLoad(a)) || 0;
    if (isRun(a)) {
      dailyLoads[d] = dailyLoads[d] || { run: 0, bike: 0 };
      dailyLoads[d].run += load;
    } else if (isBike(a)) {
      dailyLoads[d] = dailyLoads[d] || { run: 0, bike: 0 };
      dailyLoads[d].bike += load;
    }
  }

  let smooth = null;
  const debugRows = [];
  const days = listIsoDaysInclusive(startIso, dayIso);
  for (const d of days) {
    const loads = dailyLoads[d] || { run: 0, bike: 0 };
    const weeksInfo = eventDate ? computeWeeksToEvent(d, eventDate, eventDistance) : { weeksToEvent: null };
    const bikeSubFactor = computeBikeSubstitutionFactor(weeksInfo?.weeksToEvent ?? null);
    const tss = loads.run + loads.bike * bikeSubFactor;
    smooth = smooth == null ? tss : tss + decay * smooth;
    if (debugTrace) debugRows.push({ day: d, run: loads.run, bike: loads.bike, bikeSubFactor, tss, smooth });
  }

  if (debugTrace) {
    console.log("RUNFLOOR_TRACE", { dayIso, startIso, endIso, alpha, decay, safeLookbackDays, rows: debugRows });
  }

  return Number.isFinite(smooth) ? smooth : 0;
}

function computePostRaceRampFactor(daysSinceEvent, eventDistance) {
  if (!Number.isFinite(daysSinceEvent) || daysSinceEvent < 0) {
    return { factor: 1, phase: "NORMAL", keyAllowed: true, rampUntilDays: null };
  }

  const dist = normalizeEventDistance(eventDistance) || "10k";
  const rampDays = POST_RACE_RAMP_DAYS_BY_DISTANCE[dist] || POST_RACE_RAMP_DAYS_BY_DISTANCE["10k"];
  const [phase1End, phase2End, phase3End] = rampDays;

  if (daysSinceEvent <= phase1End) {
    return { factor: POST_RACE_RAMP_FACTORS[0], phase: "POST_RACE_RAMP_1", keyAllowed: false, rampUntilDays: phase3End };
  }
  if (daysSinceEvent <= phase2End) {
    return { factor: POST_RACE_RAMP_FACTORS[1], phase: "POST_RACE_RAMP_2", keyAllowed: false, rampUntilDays: phase3End };
  }
  if (daysSinceEvent <= phase3End) {
    return { factor: POST_RACE_RAMP_FACTORS[2], phase: "POST_RACE_RAMP_3", keyAllowed: true, rampUntilDays: phase3End };
  }

  return { factor: POST_RACE_RAMP_FACTORS[3], phase: "NORMAL", keyAllowed: true, rampUntilDays: phase3End };
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
  let postRaceRampUntilISO = isIsoDate(previousState?.postRaceRampUntilISO) ? previousState.postRaceRampUntilISO : null;
  let lastLifeEventCategory = normalizeEventCategory(previousState?.lastLifeEventCategory);
  let lastLifeEventStartISO = isIsoDate(previousState?.lastLifeEventStartISO) ? previousState.lastLifeEventStartISO : null;
  let lastLifeEventEndISO = isIsoDate(previousState?.lastLifeEventEndISO) ? previousState.lastLifeEventEndISO : null;

  if (eventDateISO && safeEventInDays <= 0) {
    lastEventDate = eventDateISO;
    const rampMeta = computePostRaceRampFactor(0, eventDistance);
    if (Number.isFinite(rampMeta?.rampUntilDays)) {
      postRaceRampUntilISO = isoDate(new Date(new Date(eventDateISO + "T00:00:00Z").getTime() + rampMeta.rampUntilDays * 86400000));
    }
  }

  let daysSinceEvent = null;
  if (lastEventDate) {
    const delta = daysBetween(lastEventDate, todayISO);
    if (Number.isFinite(delta) && delta >= 0) daysSinceEvent = Math.round(delta);
  }
  const postRaceRamp = computePostRaceRampFactor(daysSinceEvent, eventDistance);

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

  if (deloadStartDate && diffDays(deloadStartDate, todayISO) >= RUN_FLOOR_DELOAD_DAYS) {
    const deloadExitStable = stabilityOK && !stabilityWarn && avg7 <= avg21 * 1.05;
    if (deloadExitStable) {
      deloadStartDate = null;
      deloadEndDate = null;
      deloadActive = false;
      lastDeloadCompletedISO = todayISO;
      reasons.push("Deload beendet → stabile Rückkehr erreicht");
    } else {
      reasons.push("Deload verlängert: Stabilitäts-Gate noch nicht erfüllt");
    }
  }

  let overlayMode = "NORMAL";
  const hasLifeEvent = lifeEventEffect?.active === true;
  if (hasLifeEvent) {
    overlayMode = lifeEventEffect.overlayMode || "LIFE_EVENT";
    reasons.push(lifeEventEffect.reason || "LifeEvent aktiv");
  } else if (safeEventInDays > 0 && safeEventInDays <= taperStartDays) {
    overlayMode = "TAPER";
    reasons.push(`Taper aktiv (Event in ≤${taperStartDays} Tagen)`);
  } else if (postRaceRamp.factor < 1) {
    overlayMode = "POST_RACE_RAMP";
    reasons.push(`Post-Race-Ramp aktiv (${postRaceRamp.phase})`);
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
  } else if (overlayMode === "POST_RACE_RAMP") {
    effectiveFloorTarget = updatedFloorTarget * postRaceRamp.factor;
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
  const postRaceRampCompletedRecently =
    Number.isFinite(postRaceRamp?.rampUntilDays) && Number.isFinite(daysSinceEvent)
      ? daysSinceEvent <= postRaceRamp.rampUntilDays
      : false;

  if (
    (phase === "BASE" || phase === "BUILD") &&
    overlayMode === "NORMAL" &&
    safeEventInDays > 28 &&
    !lifeEventEffect?.freezeFloorIncrease &&
    deloadCompletedSinceIncrease &&
    !postRaceRampCompletedRecently &&
    stabilityOK &&
    !stabilityWarn &&
    avg7 <= avg21 * 1.05
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
          : overlayMode === "POST_RACE_RAMP"
        ? "Post-Race Ramp"
        : overlayMode === "DELOAD"
          ? "Deload"
          : stabilityWarn
            ? "Warn: Instabil"
            : "Build",
    lastDeloadCompletedISO,
    lastFloorIncreaseDate,
    lastEventDate,
    postRaceRampUntilISO,
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
        activity: a,
        seconds,
        date: d,
        isKey: hasKeyTag(a),
        keyType: getKeyType(a),
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
    keyType: longest.keyType || null,
    activityId: longest.activity?.id ?? null,
  };
}

function isLongrunSpecificMode(eventDistance, block) {
  const dist = normalizeEventDistance(eventDistance);
  return (dist === "hm" || dist === "m") && (block === "BUILD" || block === "RACE");
}

function estimateRunsPerWeek(ctx, dayIso, windowDays = 28) {
  const safeWindow = Math.max(7, Number(windowDays) || 28);
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - (safeWindow - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  let runCount = 0;

  for (const a of ctx.activitiesAll || []) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;
    runCount++;
  }

  return runCount / (safeWindow / 7);
}

function evaluateLongrunSpecificity(ctx, dayIso, longRunSummary, { eventDistance, block } = {}) {
  const dist = normalizeEventDistance(eventDistance) || "10k";
  const summary = longRunSummary || { minutes: 0, isKey: false, intensity: false };
  const isSpecificMode = isLongrunSpecificMode(dist, block);
  const minutes = Number(summary?.minutes ?? 0);
  const rawType = summary?.keyType || null;
  const normalizedType = normalizeKeyType(rawType, {
    movingTime: Number.isFinite(minutes) ? minutes * 60 : 0,
  });
  const runsPerWeek = estimateRunsPerWeek(ctx, dayIso, 28);
  const lowFrequencyWeek = Number.isFinite(runsPerWeek) && runsPerWeek <= 4.2;

  if (!isSpecificMode) {
    return {
      active: false,
      specific: false,
      dominantQuality: false,
      qualityBudgetUsed: 0,
      runsPerWeek,
      confidence: "none",
      notes: "Longrun bleibt primär aerob (Distanz/Block ohne Spezifitäts-Overlay).",
    };
  }

  const specificType = normalizedType === "racepace" || normalizedType === "schwelle" || normalizedType === "longrun";
  const blockDurationThresholds = {
    hm: { BUILD: 95, RACE: 90 },
    m: { BUILD: 120, RACE: 105 },
  };
  const minSpecificMinutes = Number(blockDurationThresholds?.[dist]?.[block] ?? 100);

  const durationReady = minutes >= minSpecificMinutes;
  const explicitSpecificLongrun = summary?.isKey === true && specificType;
  const moderateSpecificLongrun = durationReady && specificType;

  const specific = explicitSpecificLongrun || moderateSpecificLongrun;

  let confidence = "none";
  if (explicitSpecificLongrun && durationReady) confidence = "high";
  else if (moderateSpecificLongrun) confidence = "medium";

  const dominantQuality = specific && lowFrequencyWeek && confidence === "high";
  const qualityBudgetUsed = dominantQuality ? 1 : specific ? confidence === "high" ? 0.75 : 0.5 : 0;

  const typeLabel = specificType ? formatKeyType(normalizedType) : "aerob";
  const notes = specific
    ? dominantQuality
      ? `Spezifischer HM/M-Longrun (${minutes}′, ${typeLabel}) zählt als dominanter Qualitätsreiz.`
      : `Spezifischer HM/M-Longrun erkannt (${minutes}′, ${typeLabel}); weitere Qualität nur dosiert ergänzen.`
    : `Longrun diese Woche aerob belassen (Spezifität erst ab ~${minSpecificMinutes}′ mit klarem HM/M-Reiz).`;

  return {
    active: true,
    specific,
    dominantQuality,
    qualityBudgetUsed,
    runsPerWeek,
    confidence,
    minutes,
    minSpecificMinutes,
    keyType: normalizedType || null,
    notes,
  };
}

function computeLongestRunSummaryWindow(ctx, dayIso, windowDays = LONGRUN_PREPLAN.spikeGuardLookbackDays) {
  const safeWindowDays = Math.max(1, Number(windowDays) || LONGRUN_PREPLAN.spikeGuardLookbackDays || 30);
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - (safeWindowDays - 1) * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let longest = null;
  for (const a of ctx.activitiesAll) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    if (!isRun(a)) continue;
    const seconds = Number(a?.moving_time ?? a?.elapsed_time ?? 0);
    if (!longest || seconds > longest.seconds) longest = { seconds, date: d };
  }

  if (!longest) return { minutes: 0, date: null, windowDays: safeWindowDays };
  return {
    minutes: Math.round(longest.seconds / 60),
    date: longest.date,
    windowDays: safeWindowDays,
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

  const racepaceRegex = /\b(race\s?pace|racepace|rp|wk\s?pace|5k\s?pace|10k\s?pace|hm\s?pace|mp)\b/;
  if (racepaceRegex.test(s) || s.includes("wettkampf") || s.includes("wettkampftempo")) return "racepace";
  if (s.includes("threshold") || s.includes("schwelle") || s.includes("tempo")) return "schwelle";
  if (s.includes("vo2") || s.includes("v02")) return "vo2_touch";
  if (s.includes("strides") || s.includes("hill sprint")) return "strides";
  return "steady";
}

function hasRacepaceHint(a) {
  const text = [a?.name, a?.description, a?.workout_name, a?.workout_doc]
    .filter(Boolean)
    .map((v) => String(v).toLowerCase())
    .join(" ")
    .replace(/[_-]+/g, " ");
  if (!text) return false;
  const racepaceRegex = /\b(race\s?pace|racepace|rp|wk\s?pace|5k\s?pace|10k\s?pace|hm\s?pace|mp)\b/;
  return racepaceRegex.test(text) || text.includes("wettkampftempo") || text.includes("wettkampf");
}

function hasExplicitIntervalStructure(a) {
  const text = [a?.name, a?.description, a?.workout_name, a?.workout_doc]
    .filter(Boolean)
    .map((v) => String(v).toLowerCase())
    .join(" ")
    .replace(/[_-]+/g, " ");
  if (!text) return false;

  // examples: 3x800m, 4×1 km, 5x3', 6×90s
  const repeatDistance = /\b\d{1,2}\s*[x×]\s*\d+(?:[.,]\d+)?\s*(?:km|m)\b/i;
  const repeatTime = /\b\d{1,2}\s*[x×]\s*\d+(?:[.,]\d+)?\s*(?:min|s|sec|"|''|′|″|')\b/i;
  return repeatDistance.test(text) || repeatTime.test(text);
}

function getActivityIntervals(activity) {
  if (Array.isArray(activity?.icu_intervals)) return activity.icu_intervals;
  if (Array.isArray(activity?.intervals)) return activity.intervals;
  return [];
}

function getActivityGroups(activity) {
  if (Array.isArray(activity?.icu_groups)) return activity.icu_groups;
  if (Array.isArray(activity?.groups)) return activity.groups;
  return [];
}

function hasIcuIntervalSignal(activity) {
  const groups = getActivityGroups(activity);
  const intervals = getActivityIntervals(activity);

  const repeatedHard = groups.some((g) => {
    const count = Number(g?.count);
    const moving = Number(g?.moving_time);
    const zone = Number(g?.zone);
    return Number.isFinite(count)
      && count >= 2
      && Number.isFinite(moving)
      && moving >= 90
      && moving <= 480
      && Number.isFinite(zone)
      && zone >= 3;
  });
  if (repeatedHard) return true;

  // Fallback ohne Zonen: erkenne wiederholte Work-Intervalle auch bei Geh-/Stehpausen in Recovery.
  const workReps = intervals.filter((seg) => {
    const type = String(seg?.type ?? "").toUpperCase();
    const sec = Number(seg?.moving_time ?? seg?.elapsed_time);
    const dist = Number(seg?.distance);
    if (type && !(type === "WORK" || type === "INTERVAL" || type === "ON")) return false;
    return Number.isFinite(sec) && sec >= 90 && sec <= 480
      && Number.isFinite(dist) && dist >= 300;
  });
  return workReps.length >= 2;
}

function inferPaceConsistencyFromIcu(activity) {
  const intervals = getActivityIntervals(activity);
  const groups = getActivityGroups(activity);
  if (!groups.length || !intervals.length) return null;

  const repeated = groups
    .filter((g) => Number(g?.count) >= 2)
    .map((g) => {
      const speed = Number(g?.average_speed);
      if (!Number.isFinite(speed) || speed <= 0) return null;
      const moving = Number(g?.moving_time);
      if (!Number.isFinite(moving) || moving < 90 || moving > 480) return null;
      return {
        id: String(g?.id ?? ""),
        speed,
        count: Number(g?.count) || 0,
        zone: Number(g?.zone),
      };
    })
    .filter(Boolean);

  if (!repeated.length) return null;

  const prioritized = repeated
    .filter((g) => Number.isFinite(g.zone) && g.zone >= 3)
    .sort((a, b) => b.speed - a.speed);
  const pool = prioritized.length ? prioritized : [...repeated].sort((a, b) => b.speed - a.speed);
  const candidate = pool[0];
  if (!candidate) return null;

  const paces = intervals
    .filter((x) => String(x?.group_id ?? "") === candidate.id)
    .map((x) => {
      const speed = Number(x?.average_speed);
      return Number.isFinite(speed) && speed > 0 ? 1000 / speed : null;
    })
    .filter((x) => Number.isFinite(x));
  if (paces.length < 2) return null;

  const paceMean = avg(paces);
  const paceStd = std(paces);
  if (!Number.isFinite(paceMean) || paceMean <= 0 || !Number.isFinite(paceStd)) return null;
  const paceCvPct = (paceStd / paceMean) * 100;

  if (paceCvPct <= 3) {
    return {
      stable: true,
      label: "stabil (Wiederholungen mit enger Pace-Streuung)",
    };
  }
  if (paceCvPct <= 6) {
    return {
      stable: true,
      label: "weitgehend konstant (leichte Streuung in den Wiederholungen)",
    };
  }
  return {
    stable: false,
    label: "uneinheitlich (deutlich streuende Wiederholungs-Pace)",
  };
}

function parseTargetPaceSecPerKmFromActivity(activity) {
  const text = [activity?.name, activity?.description, activity?.workout_name, activity?.workout_doc]
    .filter(Boolean)
    .map((v) => String(v))
    .join(" ");
  if (!text) return null;

  const match = text.match(/(\d{1,2})\s*[:.,]\s*(\d{2})\s*(?:\/\s?km|\/km|min\/?km|pace|rp)/i);
  if (!match) return null;
  const min = Number(match[1]);
  const sec = Number(match[2]);
  if (!Number.isFinite(min) || !Number.isFinite(sec) || sec >= 60) return null;
  return min * 60 + sec;
}

function deriveThresholdLeverMeta(metrics = {}) {
  const {
    executionPoints,
    consistencyPoints,
    specificityPoints,
    overpacedStart,
    clearDrop,
    qualityMin,
    medianRepSec,
    recoveryWorkRatio,
    cvPct,
    fadePct,
    rangePct,
    lowRepConfidence,
  } = metrics;

  if (executionPoints <= 1.5) {
    if (overpacedStart && clearDrop) {
      return { domain: "execution", reason: "overpace_start", severity: "high", action: "control_start", cue: "1. Rep bewusst kontrolliert / nicht zu schnell anlaufen" };
    }
    if (overpacedStart) {
      return { domain: "execution", reason: "overpace_start", severity: "medium", action: "control_start", cue: "1. Rep bewusst kontrolliert / nicht zu schnell anlaufen" };
    }
    if (clearDrop) {
      return { domain: "execution", reason: "finish_loss", severity: "medium", action: "improve_durability", cue: "Schlussstabilität priorisieren" };
    }
    return { domain: "execution", reason: "pacing_general", severity: "low", action: "smooth_pacing", cue: "am Median orientieren, nicht aggressiv eröffnen" };
  }

  if (specificityPoints <= 1.5) {
    if (qualityMin < 12) {
      return { domain: "specificity", reason: "low_quality_time", severity: "high", action: "add_quality_time", cue: "mehr zusammenhängende Qualitätszeit" };
    }
    if (Number.isFinite(medianRepSec) && medianRepSec < 180) {
      return { domain: "specificity", reason: "short_reps", severity: "medium", action: "lengthen_reps", cue: "längere, threshold-typische Reps wählen" };
    }
    if (Number.isFinite(recoveryWorkRatio) && recoveryWorkRatio > 1.2) {
      return { domain: "specificity", reason: "long_recovery", severity: "medium", action: "tighten_recovery", cue: "Pausen kompakter und aktiver gestalten" };
    }
    return { domain: "specificity", reason: "low_quality_time", severity: "low", action: "add_quality_time", cue: "Struktur auf zusammenhängenden Schwellenreiz ausrichten" };
  }

  if (consistencyPoints <= 1.5) {
    if (Number.isFinite(cvPct) && cvPct > 6) {
      return { domain: "consistency", reason: "high_cv", severity: lowRepConfidence ? "medium" : "high", action: "stabilize_rhythm", cue: "alle Reps am Session-Median ausrichten" };
    }
    if (Number.isFinite(rangePct) && rangePct > 0.1) {
      return { domain: "consistency", reason: "high_range", severity: "medium", action: "reduce_spread", cue: "gleichförmige Reps statt variabler Blöcke" };
    }
    if (Number.isFinite(fadePct) && fadePct > 0.06) {
      return { domain: "consistency", reason: "high_fade", severity: "medium", action: "improve_evenness", cue: "keinen schnellen ersten Rep jagen" };
    }
    return { domain: "consistency", reason: "high_cv", severity: "low", action: "stabilize_rhythm", cue: "Rhythmus über alle Reps reproduzierbar halten" };
  }

  if (executionPoints <= specificityPoints && executionPoints <= consistencyPoints) {
    return { domain: "execution", reason: "pacing_general", severity: "low", action: "smooth_pacing", cue: "ruhig anlaufen und Schluss stabil halten" };
  }
  if (specificityPoints <= executionPoints && specificityPoints <= consistencyPoints) {
    return { domain: "specificity", reason: "low_quality_time", severity: "low", action: "add_quality_time", cue: "etwas mehr schwellen-typische Qualitätszeit setzen" };
  }
  return { domain: "consistency", reason: "high_cv", severity: "low", action: "stabilize_rhythm", cue: "Reps reproduzierbar um den Session-Rhythmus laufen" };
}

function deriveRacepaceLeverMeta(metrics = {}) {
  const {
    executionPoints,
    consistencyPoints,
    specificityPoints,
    overpacedStart,
    clearDrop,
    recoveryWorkRatio,
    qualityMin,
    cvPct,
    rangePct,
    fadePct,
    medianRepSec,
    lowRepConfidence,
  } = metrics;

  if (executionPoints <= 1.6) {
    if (overpacedStart && clearDrop) {
      return { domain: "execution", reason: "overpace_start", severity: "high", action: "control_start", cue: "1. Rep kontrollierter anlaufen und den Schluss stabil halten" };
    }
    if (overpacedStart) {
      return { domain: "execution", reason: "overpace_start", severity: "medium", action: "control_start", cue: "1. Rep bewusst defensiver starten" };
    }
    if (clearDrop) {
      return { domain: "execution", reason: "finish_loss", severity: "medium", action: "improve_durability", cue: "Start leicht defensiver wählen, Schlussverlust vermeiden" };
    }
  }

  if (consistencyPoints <= 1.8) {
    if (Number.isFinite(cvPct) && cvPct > 6) {
      return { domain: "consistency", reason: "high_cv", severity: lowRepConfidence ? "medium" : "high", action: "stabilize_rhythm", cue: "Reps enger bündeln und Pace-Ausschläge reduzieren" };
    }
    if (Number.isFinite(rangePct) && rangePct > 0.1) {
      return { domain: "consistency", reason: "high_range", severity: "medium", action: "reduce_spread", cue: "Reps enger bündeln und Pace-Ausschläge reduzieren" };
    }
    if (Number.isFinite(fadePct) && fadePct > 0.06) {
      return { domain: "consistency", reason: "high_fade", severity: "medium", action: "improve_evenness", cue: "Pace-Verlauf glätten und keine aggressive Eröffnung setzen" };
    }
  }

  if (Number.isFinite(recoveryWorkRatio) && recoveryWorkRatio > 1.05) {
    return { domain: "density", reason: "long_recovery", severity: recoveryWorkRatio > 1.25 ? "high" : "medium", action: "tighten_recovery", cue: "Pausen kürzer/aktiver halten, damit die Dichte steigt" };
  }

  if (specificityPoints <= 1.7) {
    if (qualityMin < 10) {
      return { domain: "specificity", reason: "low_quality_time", severity: "medium", action: "add_quality_time", cue: "mehr zusammenhängende RP-Qualitätszeit sammeln" };
    }
    if (Number.isFinite(medianRepSec) && medianRepSec < 150) {
      return { domain: "specificity", reason: "short_reps", severity: "medium", action: "lengthen_reps", cue: "RP-Reps etwas länger und wettkampfspezifischer wählen" };
    }
  }

  return { domain: "execution", reason: "pacing_general", severity: "low", action: "smooth_pacing", cue: "Racepace kontrolliert eröffnen und stabil schließen" };
}

function deriveVo2LeverMeta(metrics = {}) {
  const base = deriveThresholdLeverMeta(metrics);
  if (!base?.domain) return null;
  return {
    ...base,
    cue: base.cue || "VO2-Abschnitte gleichmäßig steuern und Pausen aktiv halten",
  };
}

function deriveKeyLeverMeta(session = {}, metrics = {}) {
  const keyType = normalizeKeyType(session?.keyType, {
    activity: session?.activity,
    movingTime: Number(session?.activity?.moving_time ?? session?.activity?.elapsed_time ?? 0),
  });
  switch (keyType) {
    case "schwelle":
      return deriveThresholdLeverMeta(metrics);
    case "racepace":
      return deriveRacepaceLeverMeta(metrics);
    case "vo2_touch":
      return deriveVo2LeverMeta(metrics);
    default:
      return null;
  }
}

function extractNextLeverText(review = null) {
  if (!review) return "";
  if (typeof review?.nextLever === "string" && review.nextLever.trim()) return review.nextLever.trim();
  const lines = Array.isArray(review?.lines) ? review.lines : [];
  for (const line of lines) {
    const match = String(line || "").match(/Nächster\s+Hebel\s*:\s*(.+?)(?:\.|$)/i);
    if (match?.[1]) return match[1].trim();
  }
  return "";
}

function inferLeverMetaFromText(nextLeverText = "", keyType = null) {
  const text = String(nextLeverText || "").toLowerCase().trim();
  if (!text) return null;

  const normalizedKeyType = normalizeKeyType(keyType);
  const leverRelevantType = normalizedKeyType === "schwelle" || normalizedKeyType === "racepace" || normalizedKeyType === "vo2_touch";
  if (!leverRelevantType) return null;

  // bewusst konservative Rettungsnetz-Heuristik: nur klar erkennbare, bekannte Formulierungen mappen
  if (/\bkonstanz\s+erh\w+/.test(text)) {
    const reason = /pace\s*-?aussch|streu|korridor|enger/.test(text) ? "high_range" : "high_cv";
    return {
      domain: "consistency",
      reason,
      severity: "medium",
      action: reason === "high_range" ? "reduce_spread" : "stabilize_rhythm",
      cue: "Reps enger bündeln und Pace-Ausschläge reduzieren",
    };
  }

  if (/\bdosierung\s+(priorisieren|verbessern)|kontrollierter\s+anlaufen/.test(text)) {
    return {
      domain: "execution",
      reason: "pacing_general",
      severity: "medium",
      action: "smooth_pacing",
      cue: normalizedKeyType === "racepace"
        ? "Racepace kontrolliert eröffnen und stabil schließen"
        : "ruhig anlaufen und Schluss stabil halten",
    };
  }

  if (/\bspezifit\w*\s+erh\w+|mehr\s+qualit\w*zeit/.test(text)) {
    return {
      domain: "specificity",
      reason: "low_quality_time",
      severity: "medium",
      action: "add_quality_time",
      cue: normalizedKeyType === "racepace"
        ? "mehr zusammenhängende RP-Qualitätszeit sammeln"
        : "mehr zusammenhängende Qualitätszeit",
    };
  }

  return null;
}
function ensureStructuredSessionReview(activity, keyType = null) {
  if (!activity) return null;
  const reviewFromAnalysis = summarizeIntervalSessionQuality(activity, { keyType }) || null;
  let review = reviewFromAnalysis || activity?.sessionReview || activity?.review || null;
  if (!review) return null;

  let nextLeverMeta = review?.nextLeverMeta?.domain ? review.nextLeverMeta : null;
  if (!nextLeverMeta) {
    const leverText = extractNextLeverText(review);
    nextLeverMeta = inferLeverMetaFromText(leverText, keyType);
  }
  if (!nextLeverMeta?.domain) return review;

  const normalized = {
    ...review,
    nextLever: review?.nextLever || leverMetaToText(nextLeverMeta),
    nextLeverMeta,
  };
  activity.sessionReview = normalized;
  return normalized;
}

function deriveNextLeverMeta(metrics = {}) {
  return deriveThresholdLeverMeta(metrics);
}

function leverMetaToText(leverMeta = null) {
  if (!leverMeta?.domain) return "Pausen aktiv und rhythmisch traben";
  const textMap = {
    execution: {
      overpace_start: "Dosierung priorisieren: kontrollierter anlaufen statt früh zu überpacen",
      finish_loss: "Haltbarkeit priorisieren: Einstieg minimal defensiver, Schluss stabiler halten",
      pacing_general: "Dosierung priorisieren: Einstieg konservativer und gleichmäßig aufbauen",
    },
    consistency: {
      high_cv: "Konstanz erhöhen: Reps enger bündeln und Pace-Ausschläge reduzieren",
      high_range: "Konstanz erhöhen: gleichförmige Reps mit engerem Korridor laufen",
      high_fade: "Konstanz erhöhen: Tempoverlauf glätten, damit der Fade sinkt",
    },
    density: {
      long_recovery: "Dichte erhöhen: Pausen kürzer/aktiver halten und RP-Anteile enger takten",
    },
    specificity: {
      low_quality_time: "Spezifität erhöhen: mehr Qualitätszeit im Schwellenbereich sammeln",
      short_reps: "Spezifität erhöhen: etwas längere, threshold-typische Reps wählen",
      long_recovery: "Spezifität erhöhen: Pausen kompakter/aktiver gestalten",
    },
  };
  return textMap?.[leverMeta.domain]?.[leverMeta.reason] || leverMeta.cue || "Pausen aktiv und rhythmisch traben";
}

function summarizeIntervalSessionQuality(activity, options = {}) {
  const intervals = getActivityIntervals(activity);
  if (!intervals.length) return null;

  const reps = intervals
    .filter((seg) => {
      const type = String(seg?.type ?? "").toUpperCase();
      const sec = Number(seg?.moving_time ?? seg?.elapsed_time);
      const dist = Number(seg?.distance);
      const speed = Number(seg?.average_speed);
      if (type === "RECOVERY") return false;
      if (type && !(type === "WORK" || type === "INTERVAL" || type === "ON")) return false;
      return Number.isFinite(sec) && sec >= 90 && sec <= 480
        && Number.isFinite(dist) && dist >= 300
        && Number.isFinite(speed) && speed > 0;
    })
    .map((seg) => {
      const speed = Number(seg.average_speed);
      const sec = Number(seg?.moving_time ?? seg?.elapsed_time);
      return {
        distM: Number(seg.distance),
        sec,
        paceSecPerKm: 1000 / speed,
      };
    });

  if (reps.length < 2) return null;

  const recoveries = intervals
    .filter((seg) => String(seg?.type ?? "").toUpperCase() === "RECOVERY")
    .map((seg) => ({
      speed: Number(seg?.average_speed),
      sec: Number(seg?.moving_time ?? seg?.elapsed_time),
    }))
    .filter((x) => Number.isFinite(x.speed) && x.speed > 0);

  const repPaces = reps.map((r) => r.paceSecPerKm);
  const avgPace = avg(repPaces);
  const paceStd = std(repPaces);
  const cvPct = Number.isFinite(avgPace) && avgPace > 0 ? (paceStd / avgPace) * 100 : null;
  const fadePct = (repPaces[repPaces.length - 1] - repPaces[0]) / repPaces[0];
  const qualityKm = reps.reduce((sum, r) => sum + r.distM, 0) / 1000;

  const targetPace = parseTargetPaceSecPerKmFromActivity(activity);
  const hasTargetPace = Number.isFinite(targetPace) && targetPace > 0;
  const repMedianPace = median(repPaces);
  const firstRepPace = repPaces[0];
  const lastRepPace = repPaces[repPaces.length - 1];
  const fastestRepPace = Math.min(...repPaces);
  const slowestRepPace = Math.max(...repPaces);
  const fastestRepIdx = repPaces.indexOf(fastestRepPace);
  const firstHalfCutoff = Math.floor((repPaces.length - 1) / 2);

  const startDeltaAbsPct = Number.isFinite(repMedianPace) && repMedianPace > 0
    ? Math.abs(firstRepPace - repMedianPace) / repMedianPace
    : null;
  const startFastPct = Number.isFinite(repMedianPace) && repMedianPace > 0
    ? Math.max(0, (repMedianPace - firstRepPace) / repMedianPace)
    : null;
  const finishLossPct = Number.isFinite(repMedianPace) && repMedianPace > 0
    ? Math.max(0, (lastRepPace - repMedianPace) / repMedianPace)
    : null;
  const rangePct = Number.isFinite(repMedianPace) && repMedianPace > 0
    ? (slowestRepPace - fastestRepPace) / repMedianPace
    : null;
  const closeShareTight = Number.isFinite(repMedianPace) && repMedianPace > 0
    ? repPaces.filter((pace) => Math.abs(pace - repMedianPace) / repMedianPace <= 0.025).length / repPaces.length
    : null;
  const closeShareLoose = Number.isFinite(repMedianPace) && repMedianPace > 0
    ? repPaces.filter((pace) => Math.abs(pace - repMedianPace) / repMedianPace <= 0.035).length / repPaces.length
    : null;

  const earlyFastestRep = fastestRepIdx <= firstHalfCutoff;
  const overpacedStart = Number.isFinite(startFastPct) && startFastPct > 0.03;
  const clearDrop = Number.isFinite(finishLossPct) && finishLossPct > 0.04;

  let executionPenalty = 0;
  if (Number.isFinite(startFastPct) && startFastPct > 0.03) executionPenalty += 0.5;
  else if (Number.isFinite(startFastPct) && startFastPct > 0.015) executionPenalty += 0.25;

  if (Number.isFinite(finishLossPct) && finishLossPct > 0.04) executionPenalty += 0.5;
  else if (Number.isFinite(finishLossPct) && finishLossPct > 0.025) executionPenalty += 0.25;

  if (earlyFastestRep && overpacedStart && clearDrop) executionPenalty += 0.5;
  executionPenalty = Math.min(2, executionPenalty);
  let executionPoints = Math.max(1, Math.min(3, 3 - executionPenalty));
  executionPoints = Math.round(executionPoints * 2) / 2;

  let consistencyPoints = 1.5;
  if (Number.isFinite(cvPct) && Number.isFinite(fadePct) && Number.isFinite(rangePct)) {
    if (cvPct <= 2.5 && fadePct <= 0.02 && rangePct <= 0.05 && (closeShareTight ?? 0) >= 0.8) consistencyPoints = 3;
    else if (cvPct <= 4 && fadePct <= 0.04 && rangePct <= 0.07 && (closeShareLoose ?? 0) >= 0.6) consistencyPoints = 2.5;
    else if (cvPct <= 6 && fadePct <= 0.06 && rangePct <= 0.1) consistencyPoints = 2;
    else if (cvPct <= 8 && fadePct <= 0.08) consistencyPoints = 1.5;
    else consistencyPoints = 1;
  }
  const lowRepConfidence = reps.length < 4;

  const qualitySec = reps.reduce((sum, r) => sum + (Number.isFinite(r.sec) ? r.sec : 0), 0);
  const qualityMin = qualitySec / 60;
  const repDurations = reps.map((r) => r.sec).filter((x) => Number.isFinite(x) && x > 0);
  const medianRepSec = repDurations.length ? median(repDurations) : null;
  const recoverySec = recoveries.reduce((sum, r) => sum + (Number.isFinite(r.sec) ? r.sec : 0), 0);
  const recoveryWorkRatio = qualitySec > 0 ? recoverySec / qualitySec : null;
  const jogRecoveryShare = recoveries.length
    ? recoveries.filter((v) => v.speed >= 1.8).length / recoveries.length
    : 0;

  let specificityQualityTime = 0.7;
  if (qualityMin >= 30) specificityQualityTime = 1.6;
  else if (qualityMin >= 20) specificityQualityTime = 1.4;
  else if (qualityMin >= 12) specificityQualityTime = 1.1;

  let specificityRepDuration = 0.3;
  if (Number.isFinite(medianRepSec) && medianRepSec >= 180 && medianRepSec <= 900) specificityRepDuration = 0.9;
  else if (Number.isFinite(medianRepSec) && medianRepSec >= 120 && medianRepSec <= 1200) specificityRepDuration = 0.6;

  let specificityRecovery = 0.3;
  if (Number.isFinite(recoveryWorkRatio) && recoveryWorkRatio >= 0.25 && recoveryWorkRatio <= 0.9 && jogRecoveryShare >= 0.6) specificityRecovery = 0.8;
  else if (Number.isFinite(recoveryWorkRatio) && recoveryWorkRatio <= 1.2) specificityRecovery = 0.5;

  const specificityPoints = Math.max(1, Math.min(3, specificityQualityTime + specificityRepDuration + specificityRecovery));

  const totalPoints = executionPoints + consistencyPoints + specificityPoints;
  let verdict = "gute Einheit";
  if (totalPoints >= 8.2) verdict = "sehr gute Einheit";
  else if (totalPoints < 6) verdict = "solide Einheit";
  const needsSpecificity = specificityPoints < 2;

  const formatPace = (paceSec) => {
    const min = Math.floor(paceSec / 60);
    const sec = Math.round(paceSec - min * 60);
    return `${min}:${String(sec).padStart(2, "0")}/km`;
  };

  const executionLine = executionPoints >= 2.8
    ? "Ausführung: sehr gut dosiert (kontrollierter Einstieg, Schluss haltbar)."
    : executionPoints >= 2.4
      ? "Ausführung: überwiegend sauber (leichter Start-/Finish-Drift, aber gut kontrolliert)."
      : executionPoints >= 1.9
        ? "Ausführung: solide, aber suboptimal eingeteilt (spürbarer Drift)."
        : executionPoints >= 1.4
          ? `Ausführung: eher unrund (${overpacedStart ? "zu schnell angelaufen" : "unruhige Dosierung"}${clearDrop ? ", mit deutlichem Abfall" : ""}).`
          : `Ausführung: klar überzogen (${overpacedStart ? "aggressiver Start" : "schwache Einteilung"}${clearDrop ? ", starker Einbruch" : ""}).`;

  const consistencyCoreLine = consistencyPoints >= 2.8
    ? "Stabilität: sehr hoch (geringe Streuung, kaum Fade, Reps eng beieinander)."
    : consistencyPoints >= 2.4
      ? "Stabilität: gut (kleine Unruhe, insgesamt reproduzierbar)."
      : consistencyPoints >= 1.9
        ? "Stabilität: mittel (merkliche Streuung oder moderater Fade)."
        : consistencyPoints >= 1.4
          ? "Stabilität: eingeschränkt (deutlicher Fade oder unruhige Rep-Verteilung)."
          : "Stabilität: schwach (Reps stark auseinandergefallen).";
  const consistencyLine = lowRepConfidence
    ? `${consistencyCoreLine} Hinweis: geringe Rep-Anzahl, Stabilitätsurteil mit reduzierter Sicherheit.`
    : consistencyCoreLine;

  const specificityLine = specificityPoints >= 2.8
    ? "Spezifität: hoch (gute Qualitätszeit, passende Rep-Länge, sinnvolle Pausenstruktur)."
    : specificityPoints >= 2.4
      ? "Spezifität: solide (grundsätzlich threshold-taugliche Struktur)."
      : specificityPoints >= 1.9
        ? "Spezifität: mittel (Reiz vorhanden, aber noch ausbaufähig)."
        : specificityPoints >= 1.4
          ? "Spezifität: eher begrenzt (Struktur nur teilweise schwellen-spezifisch)."
          : "Spezifität: niedrig (kein klarer Schwellencharakter in der Struktur).";

  const targetContextLine = hasTargetPace
    ? `RP-Kontext: Zielpace hinterlegt; Median ${formatPace(repMedianPace)}.`
    : `RP-Kontext: keine Zielpace hinterlegt; Bewertung über Session-Struktur (Median ${formatPace(repMedianPace)}).`;

  const nextLeverMeta = deriveKeyLeverMeta({
    keyType: options?.keyType || getKeyType(activity),
    activity,
  }, {
    executionPoints,
    consistencyPoints,
    specificityPoints,
    overpacedStart,
    clearDrop,
    qualityMin,
    medianRepSec,
    recoveryWorkRatio,
    cvPct,
    fadePct,
    rangePct,
    lowRepConfidence,
  });
  const nextLever = leverMetaToText(nextLeverMeta);

  return {
    qualityKm,
    specificityPoints,
    nextLever,
    nextLeverMeta,
    lines: [
      executionLine,
      consistencyLine,
      specificityLine,
      targetContextLine,
      `Session-Score: ${totalPoints.toFixed(1)}/9 → ${verdict}${needsSpecificity ? ", aber noch nicht spezifisch genug" : ""}. Nächster Hebel: ${nextLever}.`,
    ],
  };
}

function getIntervalDataQualityReason(activity, intervalMetrics = null) {
  const intervals = getActivityIntervals(activity);
  if (!intervals.length) {
    if (intervalMetrics?.intensity_source) {
      return `keine Intervall-Segmente erkannt (Fallback via Streams/${intervalMetrics.intensity_source} möglich)`;
    }
    return "keine Intervall-Segmente erkannt";
  }

  const validRepCount = intervals.filter((seg) => {
    const type = String(seg?.type ?? "").toUpperCase();
    const sec = Number(seg?.moving_time ?? seg?.elapsed_time);
    const dist = Number(seg?.distance);
    const speed = Number(seg?.average_speed);
    if (type === "RECOVERY") return false;
    if (type && !(type === "WORK" || type === "INTERVAL" || type === "ON")) return false;
    return Number.isFinite(sec) && sec >= 90 && sec <= 480
      && Number.isFinite(dist) && dist >= 300
      && Number.isFinite(speed) && speed > 0;
  }).length;

  if (validRepCount < 2) {
    return "zu wenige valide Wiederholungen (mind. 2 Reps à 90–480s / ≥300m)";
  }
  return null;
}

const PHASE_MAX_MINUTES = {
  BASE: {
    // Beginner-orientierte Obergrenzen für Base-Longruns
    "5k": { ga: 75, longrun: 60, vo2_touch: 3, strides: 3 },
    "10k": { ga: 80, longrun: 70, vo2_touch: 2, strides: 2 },
    hm: { ga: 90, longrun: 90, vo2_touch: 2, strides: 2 },
    m: { ga: 95, longrun: 110, strides: 1 },
  },
  BUILD: {
    // Beginner-orientierte Peak-Longruns (race-specific)
    "5k": { schwelle: 35, vo2_touch: 18, racepace: 12, longrun: 80 },
    "10k": { schwelle: 35, vo2_touch: 28, racepace: 20, longrun: 95 },
    hm: { schwelle: 55, racepace: 25, longrun: 140 },
    m: { schwelle: 35, racepace: 70, longrun: 180 },
  },
  RACE: {
    // Beginner-orientierte Taper-Longruns
    "5k": { racepace: 18, vo2_touch: 5, schwelle: 6, ga: 50, longrun: 55 },
    "10k": { racepace: 28, vo2_touch: 8, schwelle: 20, ga: 60, longrun: 65 },
    hm: { racepace: 50, vo2_touch: 4, schwelle: 20, ga: 70, longrun: 90 },
    m: { racepace: 75, schwelle: 10, ga: 55, longrun: 105 },
  },
};

const RACEPACE_DISTANCE_TARGET_KM = {
  "5k": {
    min: 3.2,
    peak: 4.0,
    max: 4.5,
  },
  "10k": {
    min: 6.0,
    peak: 7.0,
    max: 8.0,
  },
  hm: {
    min: 12.0,
    peak: 15.0,
    max: 16.0,
  },
  m: {
    min: 16.0,
    peak: 22.0,
    max: 26.0,
  },
};

function getRacepaceDistanceTarget(distance) {
  const configured = RACEPACE_DISTANCE_TARGET_KM?.[distance];
  if (Number.isFinite(configured)) {
    return {
      min: Number(configured),
      peak: Number(configured),
      max: Number(configured),
    };
  }
  if (!configured || typeof configured !== "object") return null;
  const min = Number(configured.min);
  const peak = Number(configured.peak);
  const max = Number(configured.max);
  return {
    min: Number.isFinite(min) && min > 0 ? min : null,
    peak: Number.isFinite(peak) && peak > 0 ? peak : null,
    max: Number.isFinite(max) && max > 0 ? max : null,
  };
}

const PROGRESSION_TEMPLATES = {
  BUILD: {
    "5k": {
      vo2_touch: [
        { reps: 5, work_min: 3 },
        { reps: 6, work_min: 3 },
        { reps: 5, work_min: 4 },
        { reps: 4, work_min: 3, deload_step: true },
        { reps: 6, work_min: 3 },
        { reps: 5, work_min: 4 },
        { reps: 5, work_min: 4 },
        { reps: 4, work_min: 3, deload_step: true },
      ],
      schwelle: [
        { reps: 4, work_min: 6 },
        { reps: 3, work_min: 8 },
        { reps: 3, work_min: 10 },
        { reps: 3, work_min: 6, deload_step: true },
        { reps: 3, work_min: 9 },
        { reps: 2, work_min: 12 },
        { reps: 3, work_min: 10 },
        { reps: 2, work_min: 8, deload_step: true },
      ],
      racepace: [
        { reps: 5, work_km: 0.6 },
        { reps: 4, work_km: 0.8 },
        { reps: 4, work_km: 1.0 },
        { reps: 6, work_km: 0.4, deload_step: true },
        { reps: 5, work_km: 0.8 },
        { reps: 4, work_km: 1.0 },
        { reps: 3, work_km: 1.2 },
        { reps: 5, work_km: 0.6, deload_step: true },
      ],
    },

    "10k": {
      schwelle: [
        { reps: 4, work_min: 6 },
        { reps: 3, work_min: 8 },
        { reps: 3, work_min: 10 },
        { reps: 2, work_min: 8, deload_step: true },
        { reps: 3, work_min: 10 },
        { reps: 2, work_min: 12 },
        { reps: 2, work_min: 15 },
        { reps: 2, work_min: 8, deload_step: true },
      ],
      vo2_touch: [
        { reps: 6, work_min: 3 },
        { reps: 5, work_min: 4 },
        { reps: 4, work_min: 5 },
        { reps: 5, work_min: 3, deload_step: true },
        { reps: 5, work_min: 4 },
        { reps: 4, work_min: 5 },
        { reps: 4, work_min: 6 },
        { reps: 4, work_min: 3, deload_step: true },
      ],
      racepace: [
        { reps: 3, work_km: 1.5 },
        { reps: 3, work_km: 2.0 },
        { reps: 2, work_km: 3.0 },
        { reps: 5, work_km: 1.0, deload_step: true },
        { reps: 3, work_km: 2.0 },
        { reps: 2, work_km: 3.0 },
        { reps: 2, work_km: 3.5 },
        { reps: 4, work_km: 1.5, deload_step: true },
      ],
    },

    hm: {
      schwelle: [
        { reps: 3, work_min: 10 },
        { reps: 3, work_min: 12 },
        { reps: 2, work_min: 15 },
        { reps: 2, work_min: 10, deload_step: true },
        { reps: 3, work_min: 12 },
        { reps: 2, work_min: 16 },
        { reps: 2, work_min: 20 },
        { reps: 2, work_min: 12, deload_step: true },
      ],
      vo2_touch: [
        { reps: 10, work_sec: 20, rest_sec: 70 },
        { reps: 12, work_sec: 20, rest_sec: 70 },
        { reps: 10, work_sec: 30, rest_sec: 90 },
        { reps: 8, work_sec: 20, rest_sec: 90, deload_step: true },
        { reps: 10, work_sec: 30, rest_sec: 90 },
        { reps: 8, work_sec: 40, rest_sec: 90 },
        { reps: 6, work_sec: 40, rest_sec: 90 },
        { reps: 8, work_sec: 20, rest_sec: 90, deload_step: true },
      ],
      racepace: [
        { reps: 3, work_km: 2.0 },
        { reps: 2, work_km: 3.0 },
        { reps: 2, work_km: 4.0 },
        { reps: 2, work_km: 2.0, deload_step: true },
        { reps: 2, work_km: 4.0 },
        { reps: 2, work_km: 5.0 },
        { reps: 3, work_km: 3.0 },
        { reps: 2, work_km: 3.0, deload_step: true },
      ],
    },

    m: {
      racepace: [
        { reps: 3, work_km: 4.0 },
        { reps: 2, work_km: 6.0 },
        { reps: 2, work_km: 8.0 },
        { reps: 2, work_km: 4.0, deload_step: true },
        { reps: 3, work_km: 5.0 },
        { reps: 2, work_km: 9.0 },
        { reps: 2, work_km: 10.0 },
        { reps: 2, work_km: 6.0, deload_step: true },
        { reps: 2, work_km: 8.0 },
        { reps: 2, work_km: 6.0 },
      ],
    },
  },

  RACE: {
    "5k": {
      racepace: [
        { reps: 4, work_km: 0.8 },
        { reps: 4, work_km: 1.0 },
        { reps: 4, work_km: 1.5 },
        { reps: 5, work_km: 1.0, deload_step: true },
        { reps: 3, work_km: 1.2 },
        { reps: 3, work_km: 1.6 },
        { reps: 4, work_km: 0.8, deload_step: true },
      ],
    },

    "10k": {
      racepace: [
        { reps: 4, work_km: 2.0 },
        { reps: 3, work_km: 3.0 },
        { reps: 5, work_km: 2.0 },
        { reps: 4, work_km: 3.0 },
        { reps: 2, work_km: 2.0, deload_step: true },
        { reps: 3, work_km: 2.5 },
        { reps: 2, work_km: 2.0, deload_step: true },
      ],
      schwelle: [
        { reps: 2, work_min: 8 },
        { reps: 2, work_min: 10 },
        { reps: 2, work_min: 6, deload_step: true },
      ],
    },

    hm: {
      racepace: [
        { reps: 2, work_km: 4.0 },
        { reps: 2, work_km: 5.0 },
        { reps: 3, work_km: 3.0 },
        { reps: 2, work_km: 6.0 },
        { reps: 2, work_km: 4.0, deload_step: true },
        { reps: 1, work_km: 7.0 },
        { reps: 2, work_km: 4.5 },
        { reps: 2, work_km: 5.0 },
        { reps: 3, work_km: 3.0, deload_step: true },
        { reps: 2, work_km: 4.0, deload_step: true },
      ],
      vo2_touch: [
        { reps: 8, work_sec: 20, rest_sec: 90 },
        { reps: 6, work_sec: 20, rest_sec: 90, deload_step: true },
      ],
      schwelle: [
        { reps: 2, work_min: 10 },
        { reps: 2, work_min: 12 },
        { reps: 2, work_min: 8, deload_step: true },
      ],
    },

    m: {
      racepace: [
        { reps: 2, work_km: 6.0 },
        { reps: 2, work_km: 8.0 },
        { reps: 3, work_km: 5.0 },
        { reps: 2, work_km: 10.0 },
        { reps: 2, work_km: 6.0, deload_step: true },
        { reps: 2, work_km: 9.0 },
        { reps: 2, work_km: 10.0 },
        { reps: 1, work_km: 12.0 },
        { reps: 2, work_km: 6.0, deload_step: true },
        { reps: 2, work_km: 5.0, deload_step: true },
      ],
    },
  },
};


const KEY_SESSION_RECOMMENDATIONS = {
  BASE: {
    "5k": {
      ga: ["45–75′ GA1 locker", "langer Lauf 75–100′"],
      vo2_touch: ["8–10×10″ Hill Sprints (volle 2–3′ Pause)"]
    },
    "10k": {
      ga: ["60–75′ GA1 locker", "langer Lauf 90–110′"],
      vo2_touch: ["6–8×10″ Hill Sprints (volle 2–3′ Pause)"]
    },
    "hm": {
      ga: ["60–90′ GA1 locker", "langer Lauf 100–130′"],
      vo2_touch: ["6×8–10″ Hill Sprints (volle 2–3′ Pause)"],
      strides: ["4–6×8–10″ Hill Sprints (volle 2–3′ Pause)"]
    },
    "m": {
      ga: ["75–90′ GA1 locker", "langer Lauf 120–150′"],
      strides: ["4–6×8–10″ Hill Sprints (volle 2–3′ Pause)"]
    }
  },

  BUILD: {
    "5k": {
      vo2_touch: ["5×3′ @ vVO₂max", "6×800 m @ 3–5k-Pace"],
      schwelle: ["4×6′ @ Schwelle", "3×8′ @ Schwelle"],
      racepace: ["4×1 km @ 5k-Pace (kontrolliert)", "6×600 m @ 5k-Pace"],
      longrun: ["langer Lauf 90′"]
    },
    "10k": {
      schwelle: ["3×10′ @ Schwelle", "2×15′ @ Schwelle"],
      vo2_touch: ["5×1000 m @ 5–10k-Pace", "6×3′ @ vVO₂max"],
      racepace: ["3×2 km @ 10k-Pace (moderat)", "2×3 km @ 10k-Pace (kontrolliert)"],
      longrun: ["langer Lauf 100–120′"]
    },
    "hm": {
      schwelle: ["2×20′ @ Schwelle", "3×15′ @ Schwelle"],
      racepace: [
        "Tempowechsel: 5×(3′ @ HM-Pace / 2′ locker)",
        "Tempowechsel: 4×(5′ leicht über HM-Pace / 3′ locker)",
        "Progressiver Tempowechsellauf 40–50′",
      ],
      longrun: [
        "120–150′ locker",
        "Longrun mit EB: 90′ locker + 20′ steady + 10′ bis HM-nah (nicht drüber)",
        "Longrun strukturiert: 80′ locker + 2×(10′ HM-Pace / 5′ locker) + auslaufen",
      ]
    },
    "m": {
      racepace: ["3×5 km @ M-Pace", "14–18 km @ M im Longrun"],
      longrun: ["150′ Struktur-Longrun mit 3×15′ @ M", "langer Lauf 150–180′"]
    }
  },

  RACE: {
    "5k": {
      racepace: [
        "4×0,8 km @ 5k-RP",
        "4×1,0 km @ 5k-RP",
        "4×1,5 km @ 5k-RP",
      ],
    },
    "10k": {
      racepace: [
        "4×2 km @ 10k-RP",
        "3×3 km @ 10k-RP",
        "5×2 km @ 10k-RP",
        "4×3 km @ 10k-RP",
        "2×2 km @ 10k-RP (Deload)",
      ],
      vo2_touch: ["5×2′ @ VO2 (lange Pause)", "6×400 m @ 5k-Pace"],
      schwelle: ["2×8′ @ Schwelle (Erhalt)"],
      ga: ["40–50′ GA1 locker"]
    },
    "hm": {
      racepace: [
        "2×4 km @ HM-RP",
        "2×5 km @ HM-RP",
        "2×3 km @ HM-RP (Deload)",
      ],
      vo2_touch: ["4×2′ @ VO2 (kurz, frisch)"],
      schwelle: ["2×10′ @ Schwelle (Erhalt)"],
      ga: ["40–60′ GA1 locker"],
      longrun: [
        "100–120′ locker (Taper: 75–90′)",
        "Ökonomie-Longrun: 70′ locker + 2×15′ @ HM-Pace (5′ locker) + auslaufen",
      ],
    },
    "m": {
      racepace: ["2×6 km @ M-RP", "2×8 km @ M-RP", "2×5 km @ M-RP (Deload)"],
      longrun: ["75–90′ letzter Longrun @ M (10–14 Tage vor Rennen)"],
      ga: ["30–45′ GA1 locker"]
    }
  }
};

const PROGRESSION_DELOAD_EVERY_WEEKS = 4;
const RACEPACE_BUDGET_DAYS = 4;
const KEY_PATTERN_1PERWEEK = {
  BASE: {
    "5k": ["steady", "steady", "vo2_touch"],
    "10k": ["steady", "steady", "vo2_touch"],
    hm: ["steady", "strides", "steady", "strides"],
    m: ["steady", "steady", "steady", "strides"],
  },
  BUILD: {
    "5k": ["vo2_touch", "schwelle"],
    "10k": ["schwelle", "vo2_touch", "schwelle", "schwelle"],
    hm: ["schwelle", "schwelle", "schwelle", "vo2_touch"],
    m: ["racepace", "racepace", "racepace", "racepace"],
  },
  RACE: {
    "5k": ["racepace", "racepace", "racepace"],
    "10k": ["racepace", "racepace", "schwelle"],
    hm: ["racepace", "racepace", "schwelle", "racepace"],
    m: ["racepace", "racepace", "racepace", "racepace"],
  },
};

function pickPatternBlock(context = {}) {
  const block = context.block || "BASE";
  const dist = normalizeEventDistance(context.eventDistance) || "10k";
  const weeksToEvent = Number.isFinite(context.weeksToEvent) ? context.weeksToEvent : null;
  if (block === "BUILD" && dist === "5k" && weeksToEvent != null && weeksToEvent <= getRaceStartWeeks(dist)) {
    return "RACE";
  }
  return block;
}

function pickPatternKeyType(context = {}) {
  const patternBlock = pickPatternBlock(context);
  const dist = normalizeEventDistance(context.eventDistance) || "10k";
  const pattern = KEY_PATTERN_1PERWEEK?.[patternBlock]?.[dist];
  if (!Array.isArray(pattern) || !pattern.length) return null;
  const weekInBlock = Math.max(1, Number(context.weekInBlock) || 1);
  const idx = (weekInBlock - 1) % pattern.length;
  return pattern[idx] || null;
}

function decideKeyType1PerWeek(context = {}, keyRules = {}) {
  const block = context.block || "BASE";
  const dist = normalizeEventDistance(context.eventDistance) || "10k";
  const overlayMode = context.overlayMode || "NORMAL";
  const lastKeyType = normalizeKeyType(context.lastKeyType || null);
  const intensityDistribution = context.intensityDistribution || {};
  const hardShare = Number(intensityDistribution?.hardShare);
  const midShare = Number(intensityDistribution?.midShare);
  const hardMax = Number(intensityDistribution?.targets?.hardMax);
  const midMax = Number(intensityDistribution?.targets?.midMax);
  const fatigueHigh = context?.fatigue?.override === true;

  if (block === "RESET") return "steady";
  if (overlayMode === "LIFE_EVENT_STOP" || overlayMode === "POST_RACE_RAMP") return "steady";

  let planned = pickPatternKeyType({ ...context, block, eventDistance: dist });
  if (!planned) {
    planned = keyRules?.preferredKeyTypes?.find((k) => k !== "steady") || "steady";
  }

  if (block === "RACE" && dist === "5k") {
    planned = "racepace";
  }

  if (overlayMode === "DELOAD" || overlayMode === "TAPER" || overlayMode === "LIFE_EVENT_HOLIDAY") {
    if (planned === "vo2_touch" || planned === "strides") {
      planned = block === "RACE" ? "racepace" : block === "BASE" ? "steady" : "schwelle";
    }
    if (block === "RACE" && dist === "5k") {
      planned = "schwelle";
    }
  }

  const patternBlock = pickPatternBlock({ ...context, block, eventDistance: dist });
  const pattern = KEY_PATTERN_1PERWEEK?.[patternBlock]?.[dist] || [];
  if (lastKeyType && planned === lastKeyType && pattern.length > 1) {
    const alternatives = pattern.filter((type) => type !== planned);
    if (alternatives.length) planned = alternatives[0];
  }

  if ((planned === "vo2_touch" || planned === "strides") && (fatigueHigh || (Number.isFinite(hardShare) && Number.isFinite(hardMax) && hardShare > hardMax))) {
    planned = block === "RACE" ? "racepace" : block === "BASE" ? "steady" : "schwelle";
  }

  if (block === "RACE" && dist === "5k" && fatigueHigh) {
    planned = "schwelle";
  }

  if (planned === "racepace" && dist === "5k" && (fatigueHigh || (Number.isFinite(hardShare) && Number.isFinite(hardMax) && hardShare > hardMax))) {
    planned = "schwelle";
  }

  if ((planned === "vo2_touch" || planned === "strides" || (planned === "racepace" && dist === "5k")) && Number.isFinite(hardShare) && Number.isFinite(hardMax) && hardShare > hardMax) {
    planned = "schwelle";
  }

  if (planned === "schwelle" && Number.isFinite(midShare) && Number.isFinite(midMax) && midShare > midMax) {
    planned = "steady";
  }

  if (block === "BASE" && (dist === "hm" || dist === "m") && planned === "strides") {
    const stridesSeconds = Number(context?.stridesSeconds ?? context?.stridesDurationSec ?? context?.keyWorkSec ?? 0);
    if (!Number.isFinite(stridesSeconds) || stridesSeconds > 60) planned = "steady";
  }

  const allowed = Array.isArray(keyRules?.allowedKeyTypes) ? keyRules.allowedKeyTypes : [];
  if (allowed.length && !allowed.includes(planned)) {
    const preferredAllowed = (keyRules?.preferredKeyTypes || []).find((k) => allowed.includes(k));
    planned = preferredAllowed || allowed[0] || "steady";
  }

  return planned;
}

function resolvePrimaryKeyType(keyRules, block) {
  if (keyRules?.plannedPrimaryType) return keyRules.plannedPrimaryType;
  const preferred = keyRules?.preferredKeyTypes?.find((k) => k !== "steady");
  if (preferred) return preferred;
  if (block === "BASE") return "ga";
  if (block === "RACE") return "racepace";
  return "steady";
}

function getSessionsDoneInBlock(ctx, { blockStartIso, dayIso, keyType, eventDistance, explicitOnly = false } = {}) {
  if (!isIsoDate(blockStartIso) || !isIsoDate(dayIso)) return 0;
  const normalizedKeyType = normalizeKeyType(keyType);
  if (!normalizedKeyType) return 0;

  let count = 0;
  for (const activity of ctx?.activitiesAll || []) {
    const activityIso = String(activity.start_date_local || activity.start_date || "").slice(0, 10);
    if (!activityIso || activityIso < blockStartIso || activityIso > dayIso) continue;

    const explicitKeyTag = hasKeyTag(activity);
    if (explicitOnly && !explicitKeyTag) continue;

    const rawType = explicitKeyTag ? getKeyType(activity) : null;
    let normalizedType = normalizeKeyType(rawType, {
      activity,
      movingTime: Number(activity?.moving_time ?? activity?.elapsed_time ?? 0),
      eventDistance,
    });
    if (!explicitKeyTag && normalizedKeyType === "racepace" && hasRacepaceHint(activity)) {
      normalizedType = "racepace";
    }

    if (normalizedType === normalizedKeyType) count++;
  }

  return count;
}

function getVolumeFactorForOverlay(overlayMode, fatigue = null, weeksToEvent = null) {
  if (overlayMode === "POST_RACE_RAMP") return 0.55;
  if (overlayMode === "DELOAD") return fatigue?.override ? 0.6 : 0.7;
  if (overlayMode === "TAPER") {
    if (Number.isFinite(weeksToEvent) && weeksToEvent <= 1.5) return 0.5;
    return 0.6;
  }
  if (overlayMode === "LIFE_EVENT_HOLIDAY") return 0.65;
  if (overlayMode === "LIFE_EVENT_STOP") return 0.5;
  return 1;
}

function roundStepValue(value, increment, min = 0) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return value;
  const step = Number.isFinite(increment) && increment > 0 ? increment : 1;
  const rounded = Math.round(numeric / step) * step;
  return Math.max(min, Number(rounded.toFixed(3)));
}

function applyVolumeFactorToStep(step, factor = 1) {
  if (!step || !Number.isFinite(factor) || factor >= 0.999) return step;
  const scaled = { ...step };

  if (Number.isFinite(step.work_km)) {
    scaled.work_km = roundStepValue(Number(step.work_km) * factor, 0.1, 0.1);
  }
  if (Number.isFinite(step.total_work_km)) {
    scaled.total_work_km = roundStepValue(Number(step.total_work_km) * factor, 0.1, 0.1);
  }
  if (Number.isFinite(step.work_min)) {
    scaled.work_min = roundStepValue(Number(step.work_min) * factor, 1, 1);
  }
  if (Number.isFinite(step.total_work_min)) {
    scaled.total_work_min = roundStepValue(Number(step.total_work_min) * factor, 1, 1);
  }
  if (Number.isFinite(step.work_sec)) {
    scaled.work_sec = roundStepValue(Number(step.work_sec) * factor, 15, 15);
  }

  return scaled;
}

function pickProgressionStep({ block, dist, keyType, overlayMode, weeksToEvent, ctx, dayIso, blockStartIso, fatigue, lifeEventEffect }) {
  const steps = PROGRESSION_TEMPLATES?.[block]?.[dist]?.[keyType];
  if (!Array.isArray(steps) || !steps.length) return { step: null, stepIndex: null, steps: null };

  const sessionsDone = getSessionsDoneInBlock(ctx, {
    blockStartIso,
    dayIso,
    keyType,
    eventDistance: dist,
    explicitOnly: true,
  });
  let idx = Math.min(Math.max(0, sessionsDone), steps.length - 1);

  if (overlayMode === "DELOAD") {
    idx = Math.max(0, idx - 1);
  } else if (overlayMode === "POST_RACE_RAMP") {
    idx = 0;
  } else if (overlayMode === "TAPER") {
    idx = Math.max(0, idx - 1);
    if (Number.isFinite(weeksToEvent) && weeksToEvent <= 1.5) idx = 0;
  }

  if (lifeEventEffect?.freezeProgression) {
    idx = Math.max(0, Math.min(idx, Math.max(0, sessionsDone - 1)));
  }

  if (lifeEventEffect?.name === "post_holiday_ramp") {
    idx = Math.max(0, idx - 1);
  }

  const volumeFactor = getVolumeFactorForOverlay(overlayMode, fatigue, weeksToEvent);
  const rawStep = steps[idx] || null;
  const step = applyVolumeFactorToStep(rawStep, volumeFactor);

  return { step, rawStep, stepIndex: idx, steps, sessionsDone, volumeFactor };
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
  const { step, stepIndex, sessionsDone, volumeFactor } = pickProgressionStep({
    block,
    dist,
    keyType: primaryType,
    overlayMode,
    weeksToEvent,
    ctx: context.ctx,
    dayIso: context.dayIso,
    blockStartIso: context.blockStartIso,
    fatigue: context.fatigue,
    lifeEventEffect: context.lifeEvent,
  });

  let targetMinutes = null;
  let targetKm = null;
  if (primaryType === "racepace") {
    if (step && Number.isFinite(step.work_km)) {
      const reps = Number(step.reps) || 1;
      targetKm = Math.max(0.5, reps * Number(step.work_km));
    } else {
      const goal = getRacepaceDistanceTarget(dist)?.peak || null;
      targetKm = goal ? Math.max(0.5, Math.round(goal * 0.8 * 10) / 10) : null;
    }
  } else if (primaryType === "schwelle" || primaryType === "vo2_touch") {
    if (step && Number.isFinite(step.work_min)) {
      const reps = Number(step.reps) || 1;
      targetMinutes = Math.max(1, reps * Number(step.work_min));
    } else if (step && Number.isFinite(step.work_sec)) {
      const reps = Number(step.reps) || 1;
      targetMinutes = Math.max(1, (reps * Number(step.work_sec)) / 60);
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
    sessionsDoneInBlock: sessionsDone,
    volumeFactor,
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
  if (normalized === "ga" || normalized === "steady" || normalized === "strides") return "easy";
  if (normalized === "schwelle") return "mid";
  if (normalized === "racepace") return eventDistance === "5k" ? "hard" : "mid";
  if (normalized === "vo2_touch") return "hard";
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

function computeIntensityDistribution(ctx, dayIso, block, eventDistance, blockStartIso = null) {
  const targets = getIntensityDistributionTargets(block, eventDistance);
  let lookbackDays = INTENSITY_LOOKBACK_DAYS;
  if (blockStartIso) {
    const end = new Date(dayIso + "T00:00:00Z");
    const start = new Date(blockStartIso + "T00:00:00Z");
    const blockDays = Math.floor((end.getTime() - start.getTime()) / 86400000);
    if (Number.isFinite(blockDays)) lookbackDays = Math.max(1, blockDays + 1);
  }

  const metrics = computeIntensityDistributionForWindow(ctx, dayIso, lookbackDays, eventDistance);

  const hasData = (metrics?.totalMinutes ?? 0) > 0;
  const easyShare = metrics?.easyShare;
  const midShare = metrics?.midShare;
  const hardShare = metrics?.hardShare;

  const hardOver =
    hasData && Number.isFinite(hardShare) && Number.isFinite(targets?.hardMax)
      ? hardShare > targets.hardMax + INTENSITY_COMPARISON_TOLERANCE
      : false;
  const hardOverStrict =
    hasData && Number.isFinite(hardShare) && Number.isFinite(targets?.hardMax)
      ? hardShare > targets.hardMax
      : false;
  const midOver =
    hasData && Number.isFinite(midShare) && Number.isFinite(targets?.midMax)
      ? midShare > targets.midMax + INTENSITY_COMPARISON_TOLERANCE
      : false;
  const midOverStrict =
    hasData && Number.isFinite(midShare) && Number.isFinite(targets?.midMax)
      ? midShare > targets.midMax
      : false;
  const easyUnder =
    hasData && Number.isFinite(easyShare) && Number.isFinite(targets?.easyMin)
      ? easyShare < targets.easyMin
      : false;

  return {
    hasData,
    lookbackDays,
    targets,
    easyShare,
    midShare,
    hardShare,
    easyMinutes: metrics?.easyMinutes ?? 0,
    midMinutes: metrics?.midMinutes ?? 0,
    hardMinutes: metrics?.hardMinutes ?? 0,
    totalMinutes: metrics?.totalMinutes ?? 0,
    hardOver,
    hardOverStrict,
    midOver,
    midOverStrict,
    comparisonTolerance: INTENSITY_COMPARISON_TOLERANCE,
    midStatus: midOver ? "over" : midOverStrict ? "within_tolerance" : "ok",
    easyUnder,
  };
}


const THRESHOLD_FORMAT_TEMPLATES = {
  intervals: {
    "5k": ["3×8′ @ Schwelle", "4×6′ @ Schwelle", "2×10′ @ Schwelle"],
    "10k": ["3×8–10′ @ Schwelle", "4×6–8′ @ Schwelle", "2×15′ @ Schwelle"],
    hm: ["3×10′ @ Schwelle", "2×15′ @ Schwelle", "4×8′ @ Schwelle"],
    m: ["3×8′ @ Schwelle (kontrolliert)", "2×12′ @ Schwelle", "4×6′ @ Schwelle"],
  },
  continuous: {
    "5k": ["20–25′ steady @ LT2-nah (kontrolliert)"],
    "10k": ["20–25′ steady @ LT2-nah", "25′ progressiv bis LT2-nah"],
    hm: ["25–35′ steady/progressiv", "2×15′ steady mit kurzer Pause"],
    m: ["40–60′ steady (nicht all-out)", "30–40′ progressiv bis MP+/LT2-nah"],
  },
  maintenance: {
    "5k": ["2×8′ @ Schwelle (Erhalt)"],
    "10k": ["2×10′ @ Schwelle (Erhalt)", "3×6′ @ Schwelle (Erhalt)"],
    hm: ["2×10′ @ Schwelle (Erhalt)"],
    m: ["2×8–10′ @ Schwelle (Erhalt)"],
  },
};

function maybeBiasThresholdFormatWithLever(baseDecision = {}, lever = null) {
  if (!lever?.domain || !baseDecision?.format) return baseDecision;
  if (lever?.severity === "low") return baseDecision;

  const format = baseDecision.format;
  const biasToIntervals = lever.domain === "execution" && (lever.reason === "overpace_start" || lever.reason === "finish_loss")
    || lever.domain === "consistency" && (lever.reason === "high_cv" || lever.reason === "high_range" || lever.reason === "high_fade")
    || lever.domain === "specificity" && lever.reason === "long_recovery";
  const biasToContinuous = lever.domain === "specificity" && lever.reason === "low_quality_time";

  if (biasToIntervals && format === "continuous") {
    return {
      ...baseDecision,
      format: "intervals",
      reason: `${baseDecision.reason} Lever-Bias: besser steuerbares Intervallformat für den nächsten Hebel.`,
      leverBiased: true,
    };
  }

  if (biasToContinuous && format === "intervals") {
    return {
      ...baseDecision,
      format: "continuous",
      reason: `${baseDecision.reason} Lever-Bias: leicht mehr zusammenhängende Qualitätszeit.`,
      leverBiased: true,
    };
  }

  return baseDecision;
}

function chooseThresholdFormat(context = {}, keyRules = {}, lever = null) {
  const block = String(context?.block || "BASE").toUpperCase();
  const distance = normalizeEventDistance(context?.eventDistance) || "10k";
  const weeklyQualitySlots = clampInt(String(keyRules?.maxKeysPerWeek ?? keyRules?.expectedKeysPerWeek ?? 2), 1, 3);
  const driftHigh = Number(context?.historyMetrics?.hrDriftDelta) > BLOCK_CONFIG.thresholds.hrDriftMax;
  const rpeCreep = context?.intensityDistribution?.midOver === true;
  const nextDayFatigueHigh = context?.fatigue?.override === true || context?.overlayMode === "DELOAD";

  let decision;
  if (block === "BASE") {
    decision = { format: "intervals", reason: "BASE: keine Schwelle einplanen, stattdessen locker/ökonomisch halten." };
  } else if (block === "BUILD" && (driftHigh || rpeCreep || nextDayFatigueHigh || weeklyQualitySlots <= 2)) {
    decision = { format: "intervals", reason: "BUILD + Kontrollsignal: Intervalle vorziehen (geringere Drift/Cost)." };
  } else if (block === "RACE" && (distance === "hm" || distance === "m")) {
    decision = { format: "continuous", reason: "RACE HM/M: mehr Spezifität am Stück, Schwelle nur dosiert." };
  } else if (block === "RACE" && (distance === "5k" || distance === "10k")) {
    decision = { format: "maintenance", reason: "RACE 5k/10k: Schwelle nur kurz als Erhalt." };
  } else {
    decision = { format: "intervals", reason: "Standard: Schwellenintervalle für bessere Steuerbarkeit." };
  }

  return maybeBiasThresholdFormatWithLever(decision, lever);
}

function selectThresholdSessionTemplate(format, distance, fallback = null) {
  const dist = normalizeEventDistance(distance) || "10k";
  const candidates = THRESHOLD_FORMAT_TEMPLATES?.[format]?.[dist] || [];
  if (fallback) return fallback;
  return candidates[0] || null;
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

function humanizeProgressionSessionText(keyType, progressionStepSession, fallbackRecommendation = "") {
  if (!progressionStepSession) return fallbackRecommendation || null;

  const intro = keyType === "racepace"
    ? "kontrolliert im Zieltempo"
    : keyType === "schwelle"
      ? "ruhig stabil an der Schwelle"
      : keyType === "vo2_touch"
        ? "sauber, mit voller Qualität"
        : "kontrolliert";

  return `${progressionStepSession} — ${intro}`.trim();
}

function adaptSessionToLever({ chosenType, sessionText, progressionStepSession, context = {}, keyRules = {}, lever = null }) {
  if (!sessionText || !lever?.domain) {
    return { sessionText, applied: false, cue: null, adjustmentLine: null };
  }

  const severity = lever.severity || "low";
  const thresholdKey = chosenType === "schwelle";
  const allowStructureAdjust = thresholdKey && severity !== "low";
  let adapted = String(sessionText);
  let adjustmentLine = null;

  const replaceFirst = (patterns, replacement) => {
    for (const pattern of patterns) {
      if (pattern.test(adapted)) {
        adapted = adapted.replace(pattern, replacement);
        return true;
      }
    }
    return false;
  };

  if (allowStructureAdjust) {
    if (lever.domain === "execution") {
      if (lever.reason === "overpace_start") {
        const changed = replaceFirst([/2×15′/i, /3×10′/i], "4×6′");
        if (changed) adjustmentLine = "Anpassung: steuerbarere Schwellenstruktur mit gleichförmigen Reps.";
      } else if (lever.reason === "finish_loss" && severity === "high") {
        const changed = replaceFirst([/4×8′/i, /3×10′/i, /2×15′/i], "3×8′");
        if (changed) adjustmentLine = "Anpassung: Umfang leicht kompakter für stabileren Schluss.";
      }
    } else if (lever.domain === "consistency") {
      if (lever.reason === "high_cv" || lever.reason === "high_range") {
        const changed = replaceFirst([/2×15′/i, /3×10′/i], "4×6′");
        if (changed) adjustmentLine = "Anpassung: gleichförmigere Reps für bessere Reproduzierbarkeit.";
      } else if (lever.reason === "high_fade" && severity === "high") {
        const changed = replaceFirst([/5×/i], "4×");
        if (changed) adjustmentLine = "Anpassung: leicht reduzierte Wiederholungszahl für gleichmäßigeres Profil.";
      }
    } else if (lever.domain === "specificity") {
      if (lever.reason === "low_quality_time") {
        const changed = replaceFirst([/3×8′/i, /4×6′/i, /20–25′/i], severity === "high" ? "4×8′" : "3×10′");
        if (changed) adjustmentLine = "Anpassung: moderat mehr zusammenhängende Qualitätszeit.";
      } else if (lever.reason === "short_reps" && severity === "high") {
        const changed = replaceFirst([/6×3′/i, /4×4′/i], "4×6′");
        if (changed) adjustmentLine = "Anpassung: längere Reps statt sehr kurzer Schwellenstücke.";
      } else if (lever.reason === "long_recovery") {
        const changed = replaceFirst([/3′ Trabpause/i, /kurzer Pause/i], "2′ Trabpause");
        if (changed) adjustmentLine = "Anpassung: kompaktere aktive Pausenstruktur.";
      }
    }
  }

  const cue = lever.cue || null;
  if (!thresholdKey && !adjustmentLine && cue) {
    adjustmentLine = "Anpassung: Key-Typ bleibt unverändert, Hebel nur als Coaching-Cue gesetzt.";
  }

  return {
    sessionText: adapted,
    applied: adapted !== sessionText || Boolean(adjustmentLine) || Boolean(cue),
    cue,
    adjustmentLine,
  };
}

function formatLeverAwareSessionText(baseText, lever = null, adaptation = null) {
  if (!baseText || !lever?.domain) return baseText;
  const domainLabel = lever.domain === "execution"
    ? "Dosierung"
    : lever.domain === "consistency"
      ? "Konstanz"
      : lever.domain === "density"
        ? "Dichte"
        : "Spezifität";
  const reasonLabel = String(lever.reason || "").replace(/_/g, "-");
  const leverLine = `Hebel aus letzter Key-Session: ${domainLabel} / ${reasonLabel}.`;
  const adjustmentLine = adaptation?.adjustmentLine || "Anpassung: keine strukturelle Änderung, Fokus auf Ausführungscue.";
  const cueLine = adaptation?.cue ? `Cue: ${adaptation.cue}.` : "";
  return `${leverLine} ${adjustmentLine} ${baseText}${cueLine ? ` ${cueLine}` : ""}`.trim();
}

function formatPendingLeverPlan({ pendingLever, nextKeyEarliest, plannedKeyType, explicitSession } = {}) {
  if (!pendingLever?.domain) return { pendingLeverLine: null, pendingLeverPlanLine: null };

  const domain = String(pendingLever.domain || "").toLowerCase();
  const reason = String(pendingLever.reason || "").toLowerCase();
  const normalizedPlannedType = normalizeKeyType(plannedKeyType);
  const thresholdPlanned = normalizedPlannedType === "schwelle";
  const nextKeyLabel = nextKeyEarliest ? `ab ${nextKeyEarliest}` : "beim nächsten erlaubten Key";

  const focusLabelMap = {
    consistency: "Konstanz",
    execution: "Dosierung",
    specificity: "Spezifität",
    density: "Dichte",
  };
  const focusLabel = focusLabelMap[domain] || "gezielte Qualitätssteuerung";
  const pendingLeverLine = `Hebel vorgemerkt: Der nächste erlaubte Key ${nextKeyLabel} wird auf ${focusLabel} ausgerichtet.`;

  const fromReasonMap = {
    consistency: {
      high_cv: "Geplante Anpassung: gleichförmigere Rep-Struktur mit engerem Pacing-Fenster statt variabler oder progressiver Belastung.",
      high_range: "Geplante Anpassung: identische Wiederholungen mit stabiler Trabpause, um Pace-Ausschläge zu reduzieren.",
      high_fade: "Geplante Anpassung: identische Wiederholungen mit stabiler Trabpause, um Pace-Ausschläge zu reduzieren.",
    },
    execution: {
      overpace_start: "Geplante Anpassung: steuerbareres Format mit kontrollierter erster Wiederholung und Fokus auf Schlussstabilität.",
      finish_loss: "Geplante Anpassung: steuerbareres Format mit kontrollierter erster Wiederholung und Fokus auf Schlussstabilität.",
      pacing_general: "Geplante Anpassung: steuerbareres Format mit kontrollierter erster Wiederholung und Fokus auf Schlussstabilität.",
    },
    specificity: {
      low_quality_time: "Geplante Anpassung: mehr zusammenhängende Qualitätszeit bzw. passendere Rep-/Pausenstruktur.",
      short_reps: "Geplante Anpassung: mehr zusammenhängende Qualitätszeit bzw. passendere Rep-/Pausenstruktur.",
      long_recovery: "Geplante Anpassung: mehr zusammenhängende Qualitätszeit bzw. passendere Rep-/Pausenstruktur.",
    },
    density: {
      long_recovery: "Geplante Anpassung: aktive, kürzere Pausen zur Erhöhung der Reizdichte.",
    },
  };

  let pendingLeverPlanLine = fromReasonMap?.[domain]?.[reason] || null;
  if (!pendingLeverPlanLine) {
    if (domain === "consistency") {
      pendingLeverPlanLine = "Geplante Anpassung: der nächste erlaubte Key wird gleichförmiger aufgebaut, um Pace-Schwankungen zu reduzieren.";
    } else if (domain === "execution") {
      pendingLeverPlanLine = "Geplante Anpassung: steuerbareres Format mit kontrollierter erster Wiederholung und Fokus auf Schlussstabilität.";
    } else if (domain === "specificity") {
      pendingLeverPlanLine = "Geplante Anpassung: mehr zusammenhängende Qualitätszeit bzw. passendere Rep-/Pausenstruktur.";
    } else if (domain === "density") {
      pendingLeverPlanLine = "Geplante Anpassung: aktive, kürzere Pausen zur Erhöhung der Reizdichte.";
    } else {
      pendingLeverPlanLine = "Geplante Anpassung: der nächste erlaubte Key wird über den vorgemerkten Hebel gezielt, aber dosiert moduliert.";
    }
  }

  if (!thresholdPlanned && pendingLeverPlanLine && domain !== "consistency") {
    pendingLeverPlanLine = pendingLeverPlanLine.replace(/^Geplante Anpassung:/, "Geplante Anpassung (modulierend):");
  }

  if (!pendingLeverPlanLine && explicitSession) {
    pendingLeverPlanLine = `Geplante Anpassung: beim nächsten erlaubten Key wird die Session-Idee gezielt über den Hebel angepasst (${explicitSession}).`;
  }

  return { pendingLeverLine, pendingLeverPlanLine };
}

function buildExplicitKeySessionRecommendation(context = {}, keyRules = {}, progression = null, plannedKeyType = null, lever = null) {
  const block = context.block || "BASE";
  const distance = context.eventDistance || "10k";
  const preferredType = normalizeKeyType(plannedKeyType) || resolvePrimaryKeyType(keyRules, block);
  const catalog = KEY_SESSION_RECOMMENDATIONS?.[block]?.[distance] || null;
  if (!catalog) return null;

  const allowed = Array.isArray(keyRules?.allowedKeyTypes) ? keyRules.allowedKeyTypes : [];
  const preferredAllowed = !allowed.length || allowed.includes(preferredType);
  const safePreferred = preferredAllowed
    ? preferredType
    : (keyRules?.preferredKeyTypes || []).find((type) => allowed.includes(type)) || allowed[0] || null;

  const preferredList = safePreferred && Array.isArray(catalog?.[safePreferred]) ? catalog[safePreferred] : null;
  const fallbackType = Object.keys(catalog).find((type) => Array.isArray(catalog[type]) && catalog[type].length > 0) || null;
  const chosenType = preferredList?.length ? safePreferred : fallbackType;
  const entries = chosenType ? catalog[chosenType] : null;
  if (!Array.isArray(entries) || !entries.length) return null;

  const progressionStepSession = getCurrentProgressionStepSession(block, distance, chosenType, progression?.stepIndex);
  let sessionText = progressionStepSession
    ? humanizeProgressionSessionText(chosenType, progressionStepSession, entries[0])
    : entries[0];
  let formatNote = "";
  if (chosenType === "schwelle") {
    const thresholdDecision = chooseThresholdFormat(context, keyRules, lever);
    const thresholdTemplate = selectThresholdSessionTemplate(thresholdDecision?.format, distance, progressionStepSession || null);
    if (thresholdTemplate) {
      sessionText = progressionStepSession
        ? humanizeProgressionSessionText(chosenType, thresholdTemplate, entries[0])
        : thresholdTemplate;
    }
    if (thresholdDecision?.reason) formatNote = ` Format-Entscheid: ${thresholdDecision.reason}`;
  }

  const adaptation = adaptSessionToLever({
    chosenType,
    sessionText,
    progressionStepSession,
    context,
    keyRules,
    lever,
  });
  const finalSessionText = adaptation?.sessionText || sessionText;
  const progressionMissingNote = progressionStepSession ? "" : " Progression template missing.";
  const racepaceTarget = chosenType === "racepace"
    ? getRacepaceTargetText(distance)
    : "";
  const baseText = `${formatKeyType(chosenType)} konkret: ${finalSessionText}.${formatNote}${progressionMissingNote}${racepaceTarget}`;
  return formatLeverAwareSessionText(baseText, lever, adaptation);
}

function getCurrentProgressionStepSession(block, distance, keyType, stepIndex) {
  const steps = PROGRESSION_TEMPLATES?.[block]?.[distance]?.[keyType];
  if (!Array.isArray(steps) || !steps.length) return null;

  const idx = Math.max(0, Math.min(steps.length - 1, Number(stepIndex) || 0));
  const currentStep = steps[idx];
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

  if (Number.isFinite(currentStep.work_sec)) {
    const workSec = Math.round(Number(currentStep.work_sec));
    const restSec = Number.isFinite(currentStep.rest_sec) ? ` / ${Math.round(Number(currentStep.rest_sec))}″ easy` : "";
    return `${reps}×${workSec}″${restSec}`;
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
  const target = getRacepaceDistanceTarget(distance);
  if (!target) return "";
  const peak = Number(target.peak);
  if (!Number.isFinite(peak) || peak <= 0) return "";
  const minText = Number.isFinite(target.min) && target.min > 0 ? `${formatDecimalKm(target.min)}–` : "";
  const maxText = Number.isFinite(target.max) && target.max > peak
    ? ` (max ${formatDecimalKm(target.max)} km)`
    : "";
  return ` RP-Ziel bis Blockende: ${minText}${formatDecimalKm(peak)} km am Stück in RP-Qualität${maxText}.`;
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

    const totalWorkMin = Number.isFinite(step.work_sec)
      ? (reps * Number(step.work_sec)) / 60
      : Number.isFinite(step.total_work_min)
        ? step.total_work_min
        : reps * (Number(step.work_min) || 0);
    const main = Number.isFinite(step.work_sec)
      ? `${reps}×${Math.round(Number(step.work_sec))}″`
      : `${reps}×${step.work_min}`;
    const rest = Number.isFinite(step.rest_sec)
      ? ` (${Math.round(Number(step.rest_sec))}″ easy)`
      : Number.isFinite(step.rest_min)
        ? ` (${step.rest_min}′ Trabpause)`
        : "";
    const deload = step.deload_step ? " Deload" : "";
    const total = totalWorkMin > 0 ? ` ≈${Math.round(totalWorkMin)}′` : "";
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
        allowedKeyTypes: ["steady", "strides", "vo2_touch"],
        preferredKeyTypes: ["steady", "vo2_touch"],
        bannedKeyTypes: ["schwelle", "racepace"],
      };
    }
    if (dist === "m" || dist === "hm") {
      return {
        expectedKeysPerWeek: 1,
        maxKeysPerWeek: 2,
        allowedKeyTypes: ["steady", "strides"],
        preferredKeyTypes: ["steady", "strides"],
        bannedKeyTypes: ["schwelle", "racepace", "vo2_touch"],
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
        expectedKeysPerWeek: 1.5,
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

function getLastKeyTypeBeforeDay(ctx, dayIso, windowDays = 21) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - windowDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let lastActivity = null;
  let lastDate = "";
  for (const a of ctx.activitiesAll || []) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso || !hasKeyTag(a)) continue;
    if (!lastActivity || d > lastDate) {
      lastActivity = a;
      lastDate = d;
    }
  }

  if (!lastActivity) return null;
  const rawType = getKeyType(lastActivity);
  return normalizeKeyType(rawType, {
    activity: lastActivity,
    movingTime: Number(lastActivity?.moving_time ?? lastActivity?.elapsed_time ?? 0),
  });
}

function getLastRelevantKeyLeverBeforeDay(ctx, dayIso, lookbackDays = 35, options = {}) {
  const requireNoKeyAfter = options?.requireNoKeyAfter !== false;
  if (!dayIso || !isIsoDate(dayIso)) return null;
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - lookbackDays * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));

  let lastKeyActivity = null;
  let lastKeyType = null;
  let lastDate = "";
  for (const a of ctx.activitiesAll || []) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    const explicitKeyTag = hasKeyTag(a);
    if (!explicitKeyTag) continue;
    const rawType = explicitKeyTag ? getKeyType(a) : null;
    const normalized = normalizeKeyType(rawType, {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    const leverRelevantType = normalized === "schwelle" || normalized === "racepace" || normalized === "vo2_touch";
    if (!leverRelevantType) continue;
    if (!lastKeyActivity || d > lastDate) {
      lastKeyActivity = a;
      lastKeyType = normalized;
      lastDate = d;
    }
  }

  if (!lastKeyActivity || !lastDate || !lastKeyType) return null;

  // Hebel nur bis zum nächsten Key mitschleppen: sobald nach der letzten lever-relevanten Key-Session
  // (schwelle/racepace/vo2_touch) ein weiterer Key stattfand, wird für die aktuelle Entscheidung
  // neu bewertet statt den alten Hebel weiterzutragen.
  const hasAnyKeyAfterLastRelevant = (ctx.activitiesAll || []).some((a) => {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    return d && d > lastDate && d < endIso && hasKeyTag(a);
  });
  if (requireNoKeyAfter && hasAnyKeyAfterLastRelevant) return null;

  const review = ensureStructuredSessionReview(lastKeyActivity, lastKeyType);
  if (!review?.nextLeverMeta?.domain) return null;
  return {
    date: lastDate,
    keyType: lastKeyType,
    nextLever: review.nextLever || null,
    nextLeverMeta: review.nextLeverMeta,
  };
}

function getLastSessionLeverBeforeDay(ctx, dayIso, lookbackDays = 35) {
  if (!dayIso || !isIsoDate(dayIso)) return null;
  return getLastRelevantKeyLeverBeforeDay(ctx, dayIso, lookbackDays, { requireNoKeyAfter: false });
}

function findLeverRelevantKeyOnDay(ctx, dayIso) {
  if (!dayIso || !isIsoDate(dayIso)) return null;
  let selected = null;
  for (const a of ctx.activitiesAll || []) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (d !== dayIso || !hasKeyTag(a)) continue;
    const normalized = normalizeKeyType(getKeyType(a), {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    const leverRelevantType = normalized === "schwelle" || normalized === "racepace" || normalized === "vo2_touch";
    if (!leverRelevantType) continue;
    if (!selected) {
      selected = { activity: a, keyType: normalized };
      continue;
    }
    const prevTs = Number(new Date(selected.activity?.start_date_local || selected.activity?.start_date || 0).getTime() || 0);
    const nextTs = Number(new Date(a?.start_date_local || a?.start_date || 0).getTime() || 0);
    if (nextTs >= prevTs) selected = { activity: a, keyType: normalized };
  }
  return selected;
}

async function buildLeverPersistenceDebug(ctx, dayIso) {
  if (!dayIso || !isIsoDate(dayIso)) return null;
  const prevDayIso = isoDate(new Date(new Date(dayIso + "T00:00:00Z").getTime() - 86400000));
  const persistedKvRead = await readLeverReviewKvDetailed(ctx?.env, prevDayIso);
  const prevDayLever = findLeverRelevantKeyOnDay(ctx, prevDayIso);
  const persistedReview = prevDayLever?.activity?.sessionReview || prevDayLever?.activity?.review || null;
  const persistedSessionReviewFound = !!(persistedReview && typeof persistedReview === "object");
  const persistedNextLeverMetaFound = !!persistedReview?.nextLeverMeta?.domain;

  const persistedCarry = getLastSessionLeverBeforeDay(ctx, dayIso, 35);
  const persistedCarryFound = !!persistedCarry?.nextLeverMeta?.domain;
  const carriedFromPrevDay = persistedCarryFound && persistedCarry?.date === prevDayIso;
  let adoptedOrDiscarded = "discarded";
  let reason = "no_historical_lever_found";
  if (persistedCarryFound) {
    if (carriedFromPrevDay) {
      adoptedOrDiscarded = "adopted";
      reason = "historical_lever_found_on_prev_day";
    } else {
      reason = "historical_lever_found_but_not_from_prev_day";
    }
  } else if (!persistedSessionReviewFound) {
    reason = "no_persisted_sessionReview_on_prev_day";
  } else if (!persistedNextLeverMetaFound) {
    reason = "persisted_sessionReview_without_nextLeverMeta_on_prev_day";
  }

  return {
    prevDayIso,
    prevDayLeverRelevantKeyFound: !!prevDayLever,
    persistedSessionReviewFound,
    persistedNextLeverMetaFound,
    persistedCarryFound,
    carriedFromDate: persistedCarry?.date || null,
    carriedFromPrevDay,
    adoptedOrDiscarded,
    reason,
    leverReviewReadDebug: {
      key: persistedKvRead?.key || getLeverReviewKvKey(prevDayIso),
      kvFound: persistedKvRead?.kvFound === true,
      parseOk: persistedKvRead?.parseOk === true,
      payloadHasSessionReview: persistedKvRead?.payloadHasSessionReview === true,
      payloadHasNextLeverMeta: persistedKvRead?.payloadHasNextLeverMeta === true,
      readError: persistedKvRead?.readError || null,
      parseError: persistedKvRead?.parseError || null,
    },
  };
}

function computeRacepaceBlockProgress(ctx, context = {}) {
  const block = context.block || "BASE";
  const eventDistance = context.eventDistance || "10k";
  const dayIso = context.dayIso;
  const blockStartIso = context.blockStartIso;
  if (!dayIso || !blockStartIso || !isIsoDate(dayIso) || !isIsoDate(blockStartIso)) return null;

  const steps = PROGRESSION_TEMPLATES?.[block]?.[eventDistance]?.racepace;
  if (!Array.isArray(steps) || !steps.length) return null;

  const racepaceActs = [];
  for (const a of ctx.activitiesAll || []) {
    const d = String(a.start_date_local || a.start_date || "").slice(0, 10);
    if (!d || d < blockStartIso || d > dayIso) continue;
    const explicitKeyTag = hasKeyTag(a);
    const rawType = explicitKeyTag ? getKeyType(a) : null;
    const type = normalizeKeyType(rawType, {
      activity: a,
      movingTime: Number(a?.moving_time ?? a?.elapsed_time ?? 0),
    });
    const racepaceByHint = !explicitKeyTag && hasRacepaceHint(a);
    if (type === "racepace" || racepaceByHint) racepaceActs.push(d);
  }

  racepaceActs.sort();
  const cycleLength = steps.length;
  let doneKm = 0;
  for (let i = 0; i < racepaceActs.length; i++) {
    const step = steps[i % cycleLength];
    if (!step) continue;
    const reps = Number(step.reps) || 0;
    const workKm = Number.isFinite(step.total_work_km)
      ? Number(step.total_work_km)
      : reps * (Number(step.work_km) || 0);
    if (Number.isFinite(workKm) && workKm > 0) doneKm += workKm;
  }

  let targetKm = 0;
  for (let i = 0; i < steps.length; i++) {
    const step = steps[i];
    if (!step) continue;
    const reps = Number(step.reps) || 0;
    const workKm = Number.isFinite(step.total_work_km)
      ? Number(step.total_work_km)
      : reps * (Number(step.work_km) || 0);
    if (Number.isFinite(workKm) && workKm > 0) targetKm += workKm;
  }

  const fallbackSingleSessionPeakKm = Number(getRacepaceDistanceTarget(eventDistance)?.peak);
  if (!(targetKm > 0) && fallbackSingleSessionPeakKm > 0) targetKm = fallbackSingleSessionPeakKm;

  const doneRounded = Math.round(doneKm * 10) / 10;
  const targetRounded = Math.round(targetKm * 10) / 10;
  const pct = Number.isFinite(targetKm) && targetKm > 0
    ? Math.min(999, Math.round((doneRounded / targetKm) * 100))
    : null;

  return {
    available: true,
    sessionsDone: racepaceActs.length,
    doneKm: doneRounded,
    targetKm: Number.isFinite(targetRounded) && targetRounded > 0 ? targetRounded : null,
    pct,
  };
}

function evaluateKeyCompliance(keyRules, keyStats7, keyStats14, context = {}) {
  const expected = keyRules.expectedKeysPerWeek;
  const maxKeys = keyRules.maxKeysPerWeek;
  const actual7Raw = keyStats7.count;
  const longrunSpecificity = context.longrunSpecificity || null;
  const qualityBudget = Number(longrunSpecificity?.qualityBudgetUsed ?? 0);
  const actual7 = actual7Raw + qualityBudget;
  const actual14 = keyStats14.count;
  const perWeek14 = actual14 / 2;
  const maxKeysCap7 = Number.isFinite(context.maxKeys7d) ? context.maxKeys7d : (context.ctx?.runtimeConfig?.maxKeys7d ?? MAX_KEYS_7D);
  const capExceeded = actual7 >= maxKeysCap7;
  const longrunDominant = longrunSpecificity?.dominantQuality === true;

  const actualTypes7 = keyStats7.list || [];
  const actualTypes14 = keyStats14.list || [];
  const typesForOutput = actualTypes7.length ? actualTypes7 : actualTypes14;
  const uniqueTypes = [...new Set(typesForOutput)];
  const uniqueTypes7 = [...new Set(actualTypes7)];
  const bannedHits = uniqueTypes7.filter((t) => keyRules.bannedKeyTypes.includes(t));
  const allowedHits = uniqueTypes7.filter((t) => keyRules.allowedKeyTypes.includes(t));
  const preferredHits = uniqueTypes7.filter((t) => keyRules.preferredKeyTypes.includes(t));
  const disallowedHits = uniqueTypes7.filter((t) => !keyRules.allowedKeyTypes.includes(t));
  const focusTarget = keyRules.preferredKeyTypes.length || keyRules.allowedKeyTypes.length || 0;
  const focusHits = focusTarget > 0
    ? keyRules.preferredKeyTypes.filter((t) => uniqueTypes.includes(t)).length
    : 0;
  const coverageSummary = `${focusHits}/${focusTarget}`;

  const freqOk = actual7 >= expected;
  const typeOk = bannedHits.length === 0 && disallowedHits.length === 0;
  const preferredMissing = keyRules.preferredKeyTypes.length > 0 && preferredHits.length === 0;

  const plannedKeyType = decideKeyType1PerWeek(context, keyRules) || keyRules.preferredKeyTypes[0] || keyRules.allowedKeyTypes[0] || "steady";
  keyRules.plannedPrimaryType = plannedKeyType;
  const preferred = plannedKeyType;
  const blockLabel = context.block ? `Block=${context.block}` : "Block=n/a";
  const distLabel = context.eventDistance ? `Distanz=${context.eventDistance}` : "Distanz=n/a";
  const progression = computeProgressionTarget(context, keyRules, context.overlayMode || "NORMAL");
  const racepaceBlockProgress = computeRacepaceBlockProgress(context.ctx, {
    block: context.block,
    eventDistance: context.eventDistance,
    dayIso: context.dayIso,
    blockStartIso: context.blockStartIso,
  });

  const dayIso = context.dayIso || null;
  const lastKeyIso = context.keySpacing?.lastKeyIso ?? null;
  const lastKeyGapDays =
    dayIso && lastKeyIso && isIsoDate(dayIso) && isIsoDate(lastKeyIso) ? diffDays(lastKeyIso, dayIso) : null;
  const minGapDays = clampInt(String(context.keySpacing?.minGapDays ?? context.ctx?.runtimeConfig?.keyMinGapDays ?? KEY_MIN_GAP_DAYS_DEFAULT), 1, 7);
  const keySpacingNowOk = !Number.isFinite(lastKeyGapDays) || lastKeyGapDays >= minGapDays;
  const nextKeyEarliest = context.keySpacing?.nextAllowedIso ?? null;

  const intensityDistribution = context.intensityDistribution || null;
  const hardShareBlocked = intensityDistribution?.hardOver === true;
  const midShareBlocked = intensityDistribution?.midOver === true;
  const easyShareBlocked = intensityDistribution?.easyUnder === true;
  const preferredIntensity = mapKeyTypeToIntensity(preferred, context.eventDistance);
  const activeLever = context?.lastRelevantKeyLever?.nextLeverMeta?.domain ? context.lastRelevantKeyLever.nextLeverMeta : null;
  const activeLeverText = context?.lastRelevantKeyLever?.nextLever || (activeLever ? leverMetaToText(activeLever) : "");
  const pendingLeverSource = context?.lastSessionLever?.nextLeverMeta?.domain
    ? context.lastSessionLever
    : context?.lastRelevantKeyLever?.nextLeverMeta?.domain
      ? context.lastRelevantKeyLever
      : null;
  const pendingLeverMeta = pendingLeverSource?.nextLeverMeta?.domain ? pendingLeverSource.nextLeverMeta : null;

  let suggestion = "";
  let keyAllowedNow = false;

  if (capExceeded) {
    suggestion = `Key-Budget erschöpft (${actual7.toFixed(1)}/${maxKeysCap7} in 7 Tagen) – restliche Einheiten locker/GA.`;
  } else if (longrunDominant) {
    suggestion = `Spezifischer Longrun war Hauptreiz der Woche (${actual7.toFixed(1)}/${maxKeysCap7} Budget) – kein zusätzlicher harter Key.`;
  } else if (bannedHits.length) {
    suggestion = `Verbotener Key-Typ (${bannedHits[0]}) – Alternative: ${preferred}`;
  } else if (!keySpacingNowOk && nextKeyEarliest) {
    suggestion = `Nächster Key frühestens ${nextKeyEarliest} (≥${minGapDays * 24}h Abstand). Bis dahin locker/GA.${activeLeverText ? ` Hebel vorgemerkt: ${activeLeverText}.` : ""}`;
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
    if (activeLeverText) suggestion = `${suggestion} Hebel aus letzter Key-Session: ${activeLeverText}.`;
  }

  const explicitSession = buildExplicitKeySessionRecommendation(context, keyRules, progression, plannedKeyType, activeLever);
  const pendingLever = !keyAllowedNow && pendingLeverMeta ? pendingLeverMeta : null;
  const pendingLeverPlan = !keyAllowedNow
    ? formatPendingLeverPlan({
        pendingLever,
        nextKeyEarliest,
        plannedKeyType,
        explicitSession,
      })
    : { pendingLeverLine: null, pendingLeverPlanLine: null };
  if (explicitSession && keyAllowedNow) {
    suggestion = `${suggestion} Konkrete Session-Idee: ${explicitSession}`;
  }

  const status = capExceeded ? "red" : freqOk && typeOk ? "ok" : "warn";

  return {
    expected,
    maxKeys,
    maxKeysCap7,
    actual7,
    actual7Raw,
    actual14,
    perWeek14,
    freqOk,
    typeOk,
    preferredMissing,
    bannedHits,
    allowedHits,
    preferredHits,
    focusHits,
    focusTarget,
    coverageSummary,
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
    keyMinGapDays: minGapDays,
    intensityDistribution,
    keyAllowedNow,
    plannedKeyType,
    explicitSession,
    activeLever,
    pendingLever,
    pendingLeverLine: pendingLeverPlan.pendingLeverLine,
    pendingLeverPlanLine: pendingLeverPlan.pendingLeverPlanLine,
    racepaceBlockProgress,
    longrunSpecificity,
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
    const keepRaceStart = previousState?.block === "RACE";
    const raceStartDate = keepRaceStart ? startDate : todayISO;
    const raceTimeInBlockDays = keepRaceStart ? Math.max(0, daysBetween(raceStartDate, todayISO)) : 0;
    return {
      block: "RACE",
      wave: 0,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: raceStartDate,
      startWasReset,
      reasons: [...reasons, "Event sehr nah (≤4 Wochen) → RACE"],
      readinessScore: 90,
      forcedSwitch: false,
      nextSuggestedBlock: "RESET",
      timeInBlockDays: raceTimeInBlockDays,
      startDate: raceStartDate,
      eventDistance: eventDistanceNorm,
    };
  }

  if (weeksToEvent <= forceRaceWeeks && weeksToEvent >= 0) {
    const keepRaceStart = previousState?.block === "RACE";
    const raceStartDate = keepRaceStart ? startDate : todayISO;
    const raceTimeInBlockDays = keepRaceStart ? Math.max(0, daysBetween(raceStartDate, todayISO)) : 0;
    return {
      block: "RACE",
      wave: weeksToEvent > BLOCK_CONFIG.cutoffs.wave1Weeks ? 1 : 0,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: raceStartDate,
      startWasReset,
      reasons: ["Event sehr nah → sofort RACE"],
      readinessScore: 90,
      forcedSwitch: false,
      nextSuggestedBlock: "RESET",
      timeInBlockDays: raceTimeInBlockDays,
      startDate: raceStartDate,
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
    const stayedInFreeBase = previousState?.block === "BASE";
    const freeBaseStart = stayedInFreeBase ? startDate : todayISO;
    const freeBaseDays = stayedInFreeBase ? Math.max(0, daysBetween(freeBaseStart, todayISO)) : 0;
    reasons.push(`Freie Vorphase aktiv (> ${planStartWeeks} Wochen bis Event) → BASE`);
    return {
      block: "BASE",
      wave: 0,
      weeksToEvent,
      weeksToEventRaw,
      todayISO,
      eventDateISO,
      blockStartPersisted: persistedStart,
      blockStartEffective: freeBaseStart,
      startWasReset,
      reasons,
      readinessScore: 55,
      forcedSwitch: false,
      nextSuggestedBlock: "BASE",
      timeInBlockDays: freeBaseDays,
      startDate: freeBaseStart,
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
  const runFloorNow = historyMetrics?.runFloorEwma10 ?? historyMetrics?.runFloor7 ?? 0;
  const runFloorPrev = historyMetrics?.runFloorEwma10Prev ?? historyMetrics?.runFloorPrev7 ?? 0;
  const runFloorIsLow = runFloorTarget > 0 && runFloorNow < runFloorTarget * 0.5;
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
      ? runFloorNow >= runFloorTarget * BLOCK_CONFIG.thresholds.runFloorPct &&
        runFloorPrev >= runFloorTarget * BLOCK_CONFIG.thresholds.runFloorPct
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


function parseManualRaceStartIso(value, todayIso) {
  const raw = String(value || "").trim();
  if (!raw || !isIsoDate(raw)) return null;
  const clamped = clampStartDate(raw, todayIso, 3650);
  return clamped || null;
}

function extractPersistedBlockStateFromWellness(wellness) {
  if (!wellness) return null;
  const blockRaw = wellness?.[FIELD_BLOCK] ?? wellness?.block ?? null;
  const block = String(blockRaw || "").trim().toUpperCase();
  if (!block) return null;
  const waveRaw = wellness?.BlockWave ?? wellness?.blockWave ?? 0;
  const wave = Number.isFinite(Number(waveRaw)) ? Number(waveRaw) : 0;
  return {
    block,
    wave,
    startDate: null,
    eventDate: null,
    eventDistance: null,
    floorTarget: null,
    effectiveFloorTarget: null,
    loadDays: 0,
    deloadStartDate: null,
    lastDeloadCompletedISO: null,
    lastFloorIncreaseDate: null,
    lastEventDate: null,
    postRaceRampUntilISO: null,
    lastLifeEventCategory: "",
    lastLifeEventStartISO: null,
    lastLifeEventEndISO: null,
  };
}

function getManualRaceStartOverride(env, wellness, dayIso) {
  const envValue = env?.RACE_START_OVERRIDE_ISO || env?.MANUAL_RACE_START_ISO || null;
  const fromEnv = parseManualRaceStartIso(envValue, dayIso);
  if (fromEnv) return fromEnv;
  const fromWellness = parseManualRaceStartIso(wellness?.[FIELD_RACE_START_OVERRIDE], dayIso);
  if (fromWellness) return fromWellness;
  return null;
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
      postRaceRampUntilISO: isIsoDate(parsed.postRaceRampUntilISO) ? parsed.postRaceRampUntilISO : null,
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
  const parsedFromComment = parseBlockStateFromComment(comment);
  const parsedFromFields = extractPersistedBlockStateFromWellness(wellness);
  const parsedFromKv = await readLatestBlockStateKv(env, dayIso);
  // Prefer sources that include a persisted start date (comment/KV).
  // Wellness custom fields currently only carry block/wave and would otherwise
  // wipe the remembered block start on the next run.
  const parsed = parsedFromComment || parsedFromKv || parsedFromFields;
  ctx.blockStateCache.set(dayIso, parsed);
  return parsed;
}

function getBlockStateKvKey(env) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  return `${BLOCK_STATE_KV_PREFIX}${athleteId}`;
}

async function readLatestBlockStateKv(env, dayIso) {
  const key = getBlockStateKvKey(env);
  const raw = await readKvJson(env, key);
  if (!raw || typeof raw !== "object") return null;
  if (raw.day && isIsoDate(raw.day) && raw.day > dayIso) return null;
  const state = raw.state;
  if (!state || typeof state !== "object" || !state.block || !state.startDate) return null;
  return {
    block: state.block,
    wave: Number.isFinite(Number(state.wave)) ? Number(state.wave) : 0,
    startDate: isIsoDate(state.startDate) ? state.startDate : null,
    eventDate: isIsoDate(state.eventDate) ? state.eventDate : null,
    eventDistance: state.eventDistance ?? null,
    floorTarget: Number.isFinite(state.floorTarget) ? state.floorTarget : null,
    effectiveFloorTarget: Number.isFinite(state.effectiveFloorTarget) ? state.effectiveFloorTarget : null,
    timeInBlockDays: Number.isFinite(state.timeInBlockDays) ? state.timeInBlockDays : 0,
    deloadStartDate: isIsoDate(state.deloadStartDate) ? state.deloadStartDate : null,
    lastDeloadCompletedISO: isIsoDate(state.lastDeloadCompletedISO) ? state.lastDeloadCompletedISO : null,
    lastFloorIncreaseDate: isIsoDate(state.lastFloorIncreaseDate) ? state.lastFloorIncreaseDate : null,
    lastEventDate: isIsoDate(state.lastEventDate) ? state.lastEventDate : null,
    postRaceRampUntilISO: isIsoDate(state.postRaceRampUntilISO) ? state.postRaceRampUntilISO : null,
    lastLifeEventCategory: state.lastLifeEventCategory ? normalizeEventCategory(state.lastLifeEventCategory) : "",
    lastLifeEventStartISO: isIsoDate(state.lastLifeEventStartISO) ? state.lastLifeEventStartISO : null,
    lastLifeEventEndISO: isIsoDate(state.lastLifeEventEndISO) ? state.lastLifeEventEndISO : null,
  };
}

async function writeLatestBlockStateKv(env, dayIso, state) {
  if (!state?.block || !state?.startDate) return;
  const key = getBlockStateKvKey(env);
  await writeKvJson(env, key, {
    day: dayIso,
    state,
  });
}

// ==========================
// RunFloor Snapshot KV
// ==========================
function getRunSnapshotKvKey(env) {
  return String(env?.RUN_SNAPSHOT_KV_KEY || "RUN_SNAPSHOT_LATEST");
}

async function writeLatestRunSnapshotKv(env, dayIso, snapshot) {
  if (!snapshot) return;

  const runValue = Number(snapshot.runValue);
  const runGoal = Number(snapshot.runGoal);

  if (!Number.isFinite(runValue) || !Number.isFinite(runGoal)) return;

  const key = getRunSnapshotKvKey(env);

  await writeKvJson(env, key, {
    day: dayIso,
    snapshot: {
      runValue: Math.round(runValue),
      runGoal: Math.round(runGoal),
    },
    updatedAt: new Date().toISOString(),
  });
}

async function readLatestRunSnapshotKv(env) {
  const key = getRunSnapshotKvKey(env);

  let data = null;
  try {
    data = await readKvJson(env, key);
  } catch {
    return null;
  }

  const runValue = Number(data?.snapshot?.runValue);
  const runGoal = Number(data?.snapshot?.runGoal);

  if (!Number.isFinite(runValue) || !Number.isFinite(runGoal)) return null;

  return {
    day: data?.day ?? null,
    runValue,
    runGoal,
    updatedAt: data?.updatedAt ?? null,
  };
}


function getLeverReviewKvKey(dayIso) {
  return `${LEVER_REVIEW_KV_PREFIX}${dayIso}`;
}

async function readLeverReviewKv(env, dayIso) {
  const detailed = await readLeverReviewKvDetailed(env, dayIso);
  return detailed?.normalized || null;
}

async function readLeverReviewKvDetailed(env, dayIso) {
  const key = getLeverReviewKvKey(dayIso);
  if (!hasKv(env) || !dayIso || !isIsoDate(dayIso)) {
    return {
      key,
      kvFound: false,
      parseOk: false,
      payloadHasSessionReview: false,
      payloadHasNextLeverMeta: false,
      normalized: null,
    };
  }

  let rawText = null;
  try {
    rawText = await env.KV.get(key);
  } catch (err) {
    return {
      key,
      kvFound: false,
      parseOk: false,
      payloadHasSessionReview: false,
      payloadHasNextLeverMeta: false,
      readError: String(err?.message ?? err),
      normalized: null,
    };
  }

  if (!rawText) {
    return {
      key,
      kvFound: false,
      parseOk: true,
      payloadHasSessionReview: false,
      payloadHasNextLeverMeta: false,
      normalized: null,
    };
  }

  let payload = null;
  try {
    payload = JSON.parse(rawText);
  } catch (err) {
    return {
      key,
      kvFound: true,
      parseOk: false,
      payloadHasSessionReview: false,
      payloadHasNextLeverMeta: false,
      parseError: String(err?.message ?? err),
      normalized: null,
    };
  }

  const payloadHasSessionReview = !!(payload?.sessionReview && typeof payload.sessionReview === "object");
  const nextLeverMeta = payload?.nextLeverMeta?.domain
    ? payload.nextLeverMeta
    : (payload?.sessionReview?.nextLeverMeta?.domain ? payload.sessionReview.nextLeverMeta : null);
  const nextLever = payload?.nextLever || payload?.sessionReview?.nextLever || null;
  const payloadHasNextLeverMeta = !!nextLeverMeta?.domain;

  return {
    key,
    kvFound: true,
    parseOk: true,
    payloadHasSessionReview,
    payloadHasNextLeverMeta,
    normalized: payloadHasNextLeverMeta
      ? {
          ...payload,
          nextLever,
          nextLeverMeta,
        }
      : null,
  };
}

async function writeLeverReviewKv(env, dayIso, payload) {
  const key = getLeverReviewKvKey(dayIso);
  const payloadHasSessionReview = !!(payload?.sessionReview && typeof payload.sessionReview === "object");
  const payloadHasNextLeverMeta = !!(
    payload?.nextLeverMeta?.domain || payload?.sessionReview?.nextLeverMeta?.domain
  );
  const normalizedPayload = payloadHasNextLeverMeta
    ? {
        ...payload,
        sessionReview: payload?.sessionReview && typeof payload.sessionReview === "object"
          ? payload.sessionReview
          : {
              nextLever: payload?.nextLever || null,
              nextLeverMeta: payload?.nextLeverMeta || null,
            },
      }
    : null;
  if (!hasKv(env) || !dayIso || !isIsoDate(dayIso) || !normalizedPayload?.sessionReview?.nextLeverMeta?.domain) {
    return {
      leverReviewKvWriteAttempted: false,
      leverReviewKvKey: key,
      leverReviewKvPayloadExists: !!payload,
      leverReviewKvWriteSuccess: false,
      leverReviewWriteDebug: {
        key,
        payloadHasSessionReview,
        payloadHasNextLeverMeta,
        nextLeverMetaDomain: normalizedPayload?.sessionReview?.nextLeverMeta?.domain || null,
      },
    };
  }
  await writeKvJson(env, key, {
    ...normalizedPayload,
    nextLever: normalizedPayload?.nextLever || normalizedPayload?.sessionReview?.nextLever || null,
    nextLeverMeta: normalizedPayload?.nextLeverMeta || normalizedPayload?.sessionReview?.nextLeverMeta || null,
    dayIso,
    updatedAt: new Date().toISOString(),
  });
  return {
    leverReviewKvWriteAttempted: true,
    leverReviewKvKey: key,
    leverReviewKvPayloadExists: !!payload,
    leverReviewKvWriteSuccess: true,
    leverReviewWriteDebug: {
      key,
      payloadHasSessionReview,
      payloadHasNextLeverMeta,
      nextLeverMetaDomain: normalizedPayload?.sessionReview?.nextLeverMeta?.domain || null,
    },
  };
}

async function hydrateActivitiesWithPersistedLeverReviews(env, activities, oldestIso, newestIso) {
  if (!Array.isArray(activities) || !activities.length) return;
  const byDay = new Map();
  for (const a of activities) {
    const d = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (!d) continue;
    if (!byDay.has(d)) byDay.set(d, []);
    byDay.get(d).push(a);
  }

  const days = listIsoDaysInclusive(oldestIso, newestIso);
  for (const dayIso of days) {
    const persisted = await readLeverReviewKv(env, dayIso);
    if (!persisted?.nextLeverMeta?.domain) continue;
    const onDay = byDay.get(dayIso) || [];
    let target = null;
    if (persisted?.activityId != null) {
      target = onDay.find((a) => String(a?.id) === String(persisted.activityId)) || null;
    }
    if (!target) {
      target = onDay.find((a) => hasKeyTag(a)) || onDay[0] || null;
    }
    if (!target) continue;
    target.sessionReview = {
      ...(target.sessionReview || {}),
      nextLever: persisted.nextLever || target?.sessionReview?.nextLever || null,
      nextLeverMeta: persisted.nextLeverMeta,
    };
  }
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

function addLeverKvDebug(debugOut, day, payload) {
  if (!debugOut) return;
  debugOut.__leverKv ??= {};
  debugOut.__leverKv[day] = payload;
}


function isoDate(d) {
  return d.toISOString().slice(0, 10);
}
function isoDateBerlin(d = new Date()) {
  const formatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Berlin",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return formatter.format(d);
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
function pearsonCorrelation(pairs) {
  const clean = (pairs || []).filter((p) => Array.isArray(p) && Number.isFinite(p[0]) && Number.isFinite(p[1]));
  if (clean.length < 2) return null;
  const xs = clean.map((p) => p[0]);
  const ys = clean.map((p) => p[1]);
  const mx = avg(xs);
  const my = avg(ys);
  if (!Number.isFinite(mx) || !Number.isFinite(my)) return null;
  let num = 0;
  let denX = 0;
  let denY = 0;
  for (let i = 0; i < clean.length; i++) {
    const dx = xs[i] - mx;
    const dy = ys[i] - my;
    num += dx * dy;
    denX += dx * dx;
    denY += dy * dy;
  }
  if (!(denX > 0) || !(denY > 0)) return null;
  return num / Math.sqrt(denX * denY);
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
function getIsoWeekInfo(dayIso) {
  const date = parseISODateSafe(dayIso);
  if (!date) return null;
  const d = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));
  d.setUTCDate(d.getUTCDate() + 4 - (d.getUTCDay() || 7));
  const weekYear = d.getUTCFullYear();
  const yearStart = new Date(Date.UTC(weekYear, 0, 1));
  const week = Math.ceil(((d - yearStart) / 86400000 + 1) / 7);
  return { year: weekYear, week, weekId: `${weekYear}-${String(week).padStart(2, "0")}` };
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

function pickRunMetricsPatch(patch) {
  const next = {};
  const runMetricFields = [FIELD_VDOT, FIELD_EF, FIELD_DRIFT, FIELD_MOTOR];
  for (const key of runMetricFields) {
    if (Object.prototype.hasOwnProperty.call(patch, key)) next[key] = patch[key];
  }
  return next;
}

// ================= MAIN =================
async function syncRange(env, oldest, newest, write, debug, warmupSkipSec, runtimeOverrides = {}) {
  const ctx = createCtx(env, warmupSkipSec, debug);
  ctx.runtimeConfig = loadRuntimeConfig(env);
  const runMetricsOnly = runtimeOverrides?.runMetricsOnly === true;
  const runMetricsOnlyIfExisting = runtimeOverrides?.runMetricsOnlyIfExisting === true;
  const reportVerbosity = REPORT_VERBOSITY_VALUES.has(runtimeOverrides?.reportVerbosity)
    ? runtimeOverrides.reportVerbosity
    : (debug ? "debug" : "coach");

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
  const modeOldest = isoDate(new Date(new Date(oldest + "T00:00:00Z").getTime() - 21 * 86400000));
  const modeNewest = isoDate(new Date(new Date(newest + "T00:00:00Z").getTime() + EVENT_LOOKAHEAD_DAYS * 86400000));

  // 1) Fetch ALL activities once
  ctx.activitiesAll = await fetchIntervalsActivities(env, globalOldest, globalNewest);
  await hydrateActivitiesWithPersistedLeverReviews(env, ctx.activitiesAll, globalOldest, globalNewest);
  ctx.lifeEventsAll = await fetchIntervalsEvents(env, globalOldest, globalNewest).catch(() => []);
  ctx.modeEventsAll = await fetchIntervalsEvents(env, modeOldest, modeNewest).catch(() => []);
  const lifeEventsByExternalId = new Map(
    (ctx.lifeEventsAll || [])
      .filter((event) => event?.external_id)
      .map((event) => [String(event.external_id), event])
  );

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
      modeInfo = await determineMode(env, day, ctx.debug, ctx.modeEventsAll);
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
      if (!previousBlockState) {
        // Fallback for daily cron runs: if D-1 has no persisted block start,
        // seed from the current day so we do not reset startDate accidentally.
        previousBlockState = await getPersistedBlockState(ctx, env, day);
      }
    }

    const runs = ctx.byDayRuns.get(day) ?? [];
    const patch = {};
    const perRunInfo = [];
    const existingDailyReportEvent =
      write && runMetricsOnlyIfExisting ? await fetchDailyReportNoteEvent(env, day, lifeEventsByExternalId) : null;
    const runSectionOnly =
      runMetricsOnly && (!runMetricsOnlyIfExisting || Boolean(existingDailyReportEvent?.id));
    const wellnessToday = await fetchWellnessDay(ctx, env, day);
    const manualRaceStartIso = parseManualRaceStartIso(
      runtimeOverrides?.raceStartOverrideIso,
      day
    ) || getManualRaceStartOverride(env, wellnessToday, day);

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
      const activityWithIntervals = await getActivityWithIntervals(ctx, a);
      const isKey = hasKeyTag(a);
      const ga = isGA(a);
      const intervalSignal = isKey
        || hasExplicitIntervalStructure(activityWithIntervals)
        || hasIcuIntervalSignal(activityWithIntervals);

      const ef = extractEF(a);
      const load = extractLoad(a);
      const keyType = isKey ? getKeyType(a) : null;
      const intervalStructureHint = intervalSignal ? hasExplicitIntervalStructure(activityWithIntervals) : false;
      const paceConsistencyHint = intervalSignal ? inferPaceConsistencyFromIcu(activityWithIntervals) : null;

      let drift = null;
      let drift_raw = null;
      let drift_source = "none";
      let intervalMetrics = null;

      const isTempoDauerlauf = isTempoDauerlaufKey(a);

      if ((ga && !isKey) || isTempoDauerlauf) {
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
      if (intervalSignal) {
        try {
          intervalMetrics = await computeIntervalMetrics(env, activityWithIntervals, {
            intervalType: getIntervalTypeFromActivity(activityWithIntervals),
          });
        } catch {
          intervalMetrics = null;
        }
      }

      perRunInfo.push({
        activityId: a.id,
        activity: activityWithIntervals,
        type: a.type,
        tags: a.tags ?? [],
        ga,
        isKey,
        intervalSignal,
        keyType,
        ef,
        drift,
        drift_raw,
        drift_source,
        load,
        intervalMetrics,
        intervalStructureHint,
        paceConsistencyHint,
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
    let longRunSummary = { minutes: 0, date: null, quality: "n/a", isKey: false, intensity: false, longRun14d: { minutes: 0, date: null }, longestRun30d: { minutes: 0, date: null, windowDays: LONGRUN_PREPLAN.spikeGuardLookbackDays }, plan: null };
    try {
      longRunSummary = computeLongRunSummary7d(ctx, day);
    } catch {}

    const weeksInfo = eventDate ? computeWeeksToEvent(day, eventDate, null) : { weeksToEvent: null };
    const weeksToEvent = weeksInfo.weeksToEvent ?? null;
    const bikeSubFactor = computeBikeSubstitutionFactor(weeksToEvent);
    const longRun14d = computeLongRunSummary14d(ctx, day);
    const longestRun30d = computeLongestRunSummaryWindow(ctx, day, LONGRUN_PREPLAN.spikeGuardLookbackDays);
    const longRunPlan = computeLongRunTargetMinutes(weeksToEvent, eventDistance);
    longRunSummary = {
      ...longRunSummary,
      longRun14d,
      longestRun30d,
      plan: longRunPlan,
    };
    const runFloorDebugFlag =
      ctx?.env?.RUN_FLOOR_DEBUG_TRACE ??
      (typeof globalThis !== "undefined" && globalThis?.process?.env
        ? globalThis.process.env.RUN_FLOOR_DEBUG_TRACE
        : "");
    const runFloorEwma10 = computeRunFloorEwma(ctx, day, {
      eventDate,
      eventDistance,
      debugTrace: /^1|true|yes$/i.test(String(runFloorDebugFlag || "")),
    });

    let specificValue = 0;
    if (policy.specificKind === "run") specificValue = runFloorEwma10;
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
    const runFloorEwma10Prev = computeRunFloorEwma(ctx, prevWindowDay, { eventDate, eventDistance });

    const prevIntensitySignal = loads7Prev.intensitySignal ?? "none";
    const prevAerobicFloorActive = policy.useAerobicFloor && prevIntensitySignal === "ok";
    const prevAerobicFloor = prevAerobicFloorActive ? policy.aerobicK * (loads7Prev.intensity7 ?? 0) : 0;
    const aerobicOkPrev = prevAerobicFloorActive ? (loads7Prev.aerobicEq7 ?? 0) >= prevAerobicFloor : true;

    const keyStats7 = collectKeyStats(ctx, day, 7);
    const keyStats14 = collectKeyStats(ctx, day, 14);
    const lastKeyType = getLastKeyTypeBeforeDay(ctx, day, 21);
    const lastRelevantKeyLeverReview = getLastRelevantKeyLeverBeforeDay(ctx, day, 35);
    const lastSessionLeverReview = getLastSessionLeverBeforeDay(ctx, day, 35);
    const leverPersistenceDebug = await buildLeverPersistenceDebug(ctx, day);
    const keySpacing = computeKeySpacing(ctx, day);
    const baseBlock =
      previousBlockState?.block ||
      (weeksToEvent != null && weeksToEvent <= getRaceStartWeeks(eventDistance) ? "BUILD" : "BASE");
    const keyRulesPre = getKeyRules(baseBlock, eventDistance, weeksToEvent);
    const longrunSpecificityPre = evaluateLongrunSpecificity(ctx, day, longRunSummary, {
      eventDistance,
      block: baseBlock,
    });
    const keyCompliancePre = evaluateKeyCompliance(keyRulesPre, keyStats7, keyStats14, {
      block: baseBlock,
      eventDistance,
      timeInBlockDays: previousBlockState?.timeInBlockDays ?? 0,
      weeksToEvent,
      lastKeyType,
      longrunSpecificity: longrunSpecificityPre,
    });

    const baseRunFloorTarget =
      Number.isFinite(previousBlockState?.floorTarget) && previousBlockState.floorTarget > 0
        ? previousBlockState.floorTarget
        : MIN_STIMULUS_7D_RUN_EVENT;

    const historyMetrics = {
      runFloorEwma10: runFloorEwma10 ?? 0,
      runFloorEwma10Prev: runFloorEwma10Prev ?? 0,
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
      leverPersistenceDebug,
    };

    const blockState = determineBlockState({
      today: day,
      eventDate: eventDate || null,
      eventDistance,
      historyMetrics,
      previousState: previousBlockState,
    });

    if (manualRaceStartIso && blockState.block === "RACE") {
      const overrideStart = clampStartDate(manualRaceStartIso, day, 3650);
      if (overrideStart) {
        blockState.startDate = overrideStart;
        blockState.blockStartEffective = overrideStart;
        blockState.timeInBlockDays = Math.max(0, daysBetween(overrideStart, day));
        blockState.reasons = [...(blockState.reasons || []), `Manueller RACE-Start aktiv (${overrideStart})`];
      }
    }
    if (runtimeOverrides?.blockStartOverrideIso) {
      const overrideStart = clampStartDate(runtimeOverrides.blockStartOverrideIso, day, 3650);
      if (overrideStart) {
        blockState.startDate = overrideStart;
        blockState.blockStartEffective = overrideStart;
        blockState.timeInBlockDays = Math.max(0, daysBetween(overrideStart, day));
        blockState.reasons = [...(blockState.reasons || []), `Manueller Block-Start aktiv (${overrideStart})`];
      }
    }
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

    const runSnapshot = {
      runValue: runFloorEwma10,
      runGoal: runFloorState?.effectiveFloorTarget ?? runFloorState?.floorTarget ?? null,
    };

    // Keep watchface run-floor snapshot in sync even for scheduled run-only updates.
    if (write) {
      await writeLatestRunSnapshotKv(env, day, runSnapshot);
    }

    blockState.floorTarget = runFloorState.floorTarget;
    blockState.effectiveFloorTarget = runFloorState.effectiveFloorTarget;
    blockState.deloadStartDate = runFloorState.deloadStartDate;
    blockState.lastDeloadCompletedISO = runFloorState.lastDeloadCompletedISO;
    blockState.lastFloorIncreaseDate = runFloorState.lastFloorIncreaseDate;
    blockState.lastEventDate = runFloorState.lastEventDate;
    blockState.postRaceRampUntilISO = runFloorState.postRaceRampUntilISO;

    const dynamicKeyCap = {
      maxKeys7d: ctx.runtimeConfig.maxKeys7d,
      reasons: [],
    };

    if (modeInfo?.lifeEventEffect?.active && modeInfo.lifeEventEffect.allowKeys === false) {
      dynamicKeyCap.maxKeys7d = 0;
      dynamicKeyCap.reasons.push(`LifeEvent ${modeInfo.lifeEventEffect.category}: Keys pausiert`);
    } else if (runFloorState.overlayMode === "POST_RACE_RAMP") {
      dynamicKeyCap.maxKeys7d = 0;
      dynamicKeyCap.reasons.push("Post-Race-Ramp aktiv");
    } else if (runFloorState.overlayMode === "TAPER") {
      dynamicKeyCap.maxKeys7d = 0;
      dynamicKeyCap.reasons.push("Taper aktiv");
    } else if (runFloorState.overlayMode === "DELOAD") {
      dynamicKeyCap.maxKeys7d = 1;
      dynamicKeyCap.reasons.push("Deload aktiv");
    } else if (fatigueBase?.override) {
      dynamicKeyCap.maxKeys7d = 2;
      dynamicKeyCap.reasons.push("Fatigue/Overload (Cap bleibt 2, nur konservative Empfehlung)");
   
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
    const longrunSpecificity = evaluateLongrunSpecificity(ctx, day, longRunSummary, {
      eventDistance,
      block: blockState.block,
    });
    const intensityDistribution = computeIntensityDistribution(
      ctx,
      day,
      blockState.block,
      eventDistance,
      blockState.startDate || blockState.blockStartEffective || null
    );
    const weekInBlock = Math.max(1, Math.floor((blockState.timeInBlockDays ?? 0) / 7) + 1);
    const plannedPrimaryType = decideKeyType1PerWeek(
      {
        block: blockState.block,
        eventDistance,
        weeksToEvent: blockState.weeksToEvent,
        overlayMode: runFloorState.overlayMode,
        intensityDistribution,
        fatigue,
        weekInBlock,
        lastKeyType,
      },
      keyRules
    );
    keyRules.plannedPrimaryType = plannedPrimaryType;
    const keyCompliance = evaluateKeyCompliance(keyRules, keyStats7, keyStats14, {
      ctx,
      dayIso: day,
      block: blockState.block,
      eventDistance,
      blockStartIso: blockState.startDate || blockState.blockStartEffective || day,
      maxKeys7d: dynamicKeyCap.maxKeys7d,
      keySpacing,
      overlayMode: runFloorState.overlayMode,
      intensityDistribution,
      fatigue,
      timeInBlockDays: blockState.timeInBlockDays,
      weeksToEvent: blockState.weeksToEvent,
      weekInBlock,
      lifeEvent: runFloorState.lifeEvent,
      lastKeyType,
      lastRelevantKeyLever: lastRelevantKeyLeverReview || null,
      lastSessionLever: lastSessionLeverReview || null,
      historyMetrics,
      longrunSpecificity,
    });
if (modeInfo?.lifeEventEffect?.active && modeInfo.lifeEventEffect.allowKeys === false) {
      keyCompliance.keyAllowedNow = false;
      keyCompliance.suggestion = `LifeEvent ${modeInfo.lifeEventEffect.category}: keine Keys (Freeze aktiv).`;
    }

    const executionScore = computeExecutionQualityScore({
      keyCompliance,
      trend,
      longRunSummary,
      fatigue,
    });
    const weeklySnapshot = buildWeeklySnapshot(ctx, day, {
      eventDistance,
      block: blockState.block,
      runFloor: runFloorEwma10,
      runLoad7: loads7.runTotal7,
      intensityDistribution,
      keyStats7,
      longRunSummary,
      longrunSpecificity,
      robustness,
      fatigue,
      keyCompliance,
      trend,
      executionScore,
    });
    const longrunFrequency21d = computeLongrunFrequency21d(ctx, day);
    const longrunFrequency35d = computeLongrunFrequency35d(ctx, day);
    const longrunSpikeIndex = computeLongrunSpikeIndex(longRunSummary);
    const distanceDiagnostics = computeDistanceDiagnostics(weeklySnapshot, {
      dayIso: day,
      activitiesAll: ctx.activitiesAll,
      runFloorTarget: runFloorState.effectiveFloorTarget,
      keyCompliance,
      fatigue,
      longRunSummary,
      strengthPolicy: robustness?.strengthPolicy,
      weeksToEvent: blockState?.weeksToEvent,
      longrunFrequency21d,
      longrunFrequency35d,
      longrunSpikeIndex,
    });
    const gapRecommendations = buildGapRecommendations(distanceDiagnostics);

    historyMetrics.keyCompliance = keyCompliance;
    historyMetrics.distanceDiagnostics = distanceDiagnostics;
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
      postRaceRampUntilISO: blockState.postRaceRampUntilISO,
      lastLifeEventCategory: runFloorState.lastLifeEventCategory,
      lastLifeEventStartISO: runFloorState.lastLifeEventStartISO,
      lastLifeEventEndISO: runFloorState.lastLifeEventEndISO,
    };

    if (write && !runSectionOnly) {
      await writeLatestBlockStateKv(env, day, previousBlockState);
    }

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
      postRaceRampUntilISO: runFloorState.postRaceRampUntilISO,
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
    const dailyReportTextRaw = buildComments({
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
      runFloorEwma10,
      runFloorState,
      specificOk,
      specificValue,
      aerobicOk,
      aerobicFloor,
      aerobicFloorActive,
      fatigue,
      longRunSummary,
      distanceDiagnostics,
      gapRecommendations,
      bikeSubFactor,
      weeksToEvent,
      eventDistance,
    }, { debug, verbosity: reportVerbosity });
    const dailyReportText = normalizeDailyReportText(day, dailyReportTextRaw);

    if (!runSectionOnly) {
      // Explicitly clear wellness comments; report is written only as NOTE.
      patch.comments = "";
    }



    if (debug) {
      notesPreview[day] = dailyReportText || "";
    }

    // Daily NOTE (calendar): stores the daily report text in blue
    if (write && runSectionOnly && existingDailyReportEvent?.id) {
      await upsertDailyReportTodayRunSection(env, day, dailyReportText || "", existingDailyReportEvent);
    } else if (write && !runSectionOnly) {
      await upsertDailyReportNote(env, day, dailyReportText || "", lifeEventsByExternalId);
    }

    // Monday detective NOTE (calendar) – always on Mondays, even if no run
    if (!runSectionOnly && isMondayIso(day)) {
      let detectiveNoteText = null;
      let patternAnalysis = null;
      try {
        const detectiveNote = await computeDetectiveNoteAdaptive(env, day, ctx.warmupSkipSec);
        detectiveNoteText = detectiveNote?.text ?? "";
        if (write) {
          await persistDetectiveSummary(env, day, detectiveNote?.summary);
          await upsertWeekDocAndIndex(env, day, ctx.warmupSkipSec, { activitiesAll: ctx.activitiesAll, eventDistance });
        }
        const weekDocs = await loadWeekDocsForPattern(env);
        patternAnalysis = buildPatternAnalysis(weekDocs);
        const patternText = renderPatternAnalysisBlock(patternAnalysis);
        if (patternText) detectiveNoteText = [detectiveNoteText, "", patternText].filter(Boolean).join("\n");
      } catch (e) {
        detectiveNoteText = `🕵️‍♂️ Montags-Report
Fehler: ${String(e?.message ?? e)}`;
      }
      if (write) {
        await upsertMondayDetectiveNote(env, day, detectiveNoteText, lifeEventsByExternalId);
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
        if (patternAnalysis) {
          ctx.debugOut.__musterAnalyse ??= {};
          ctx.debugOut.__musterAnalyse[day] = patternAnalysis;
        }
      }
    }

    const patchToWrite = runSectionOnly ? pickRunMetricsPatch(patch) : patch;

    let leverKvWriteDebug = {
      leverRelevantKeySessionFound: false,
      leverRelevantKeyType: null,
      leverRelevantActivityHydrated: false,
      hydrationError: null,
      leverRelevantSessionHasReview: false,
      leverRelevantSessionHasNextLeverMeta: false,
      leverReviewKvWriteAttempted: false,
      leverReviewKvKey: getLeverReviewKvKey(day),
      leverReviewKvPayloadExists: false,
      leverReviewKvWriteSuccess: false,
      leverReviewWriteDebug: {
        key: getLeverReviewKvKey(day),
        payloadHasSessionReview: false,
        payloadHasNextLeverMeta: false,
        nextLeverMetaDomain: null,
      },
    };

    if (write) {
      const leverOnDay = findLeverRelevantKeyOnDay(ctx, day);
      leverKvWriteDebug.leverRelevantKeySessionFound = !!leverOnDay?.activity;
      leverKvWriteDebug.leverRelevantKeyType = leverOnDay?.keyType || null;
      if (leverOnDay?.activity) {
        let leverActivityForReview = leverOnDay.activity;
        try {
          const detailedActivity = await getActivityWithIntervals(ctx, leverOnDay.activity);
          if (detailedActivity) {
            leverActivityForReview = detailedActivity;
            leverKvWriteDebug.leverRelevantActivityHydrated = true;
          }
        } catch (err) {
          leverKvWriteDebug.hydrationError = String(err?.message ?? err);
          leverKvWriteDebug.leverRelevantActivityHydrated = false;
        }

        const persistedReview = ensureStructuredSessionReview(leverActivityForReview, leverOnDay.keyType);
        leverKvWriteDebug.leverRelevantSessionHasReview = !!(persistedReview && typeof persistedReview === "object");
        leverKvWriteDebug.leverRelevantSessionHasNextLeverMeta = !!persistedReview?.nextLeverMeta?.domain;
        if (persistedReview?.nextLeverMeta?.domain) {
          leverKvWriteDebug = await writeLeverReviewKv(env, day, {
            ...leverKvWriteDebug,
            activityId: leverOnDay.activity?.id ?? null,
            keyType: leverOnDay.keyType,
            nextLever: persistedReview?.nextLever || null,
            nextLeverMeta: persistedReview.nextLeverMeta,
            sessionReview: persistedReview,
          });
        }
      }
      await putWellnessDay(env, day, patchToWrite);
      daysWritten++;
    }

    if (debug) {
      addLeverKvDebug(ctx.debugOut, day, {
        ...leverKvWriteDebug,
        leverReviewReadDebug: leverPersistenceDebug?.leverReviewReadDebug || null,
      });
    }

    patches[day] = patchToWrite;
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

function normalizeDiagnosticKeyType(type) {
  const t = String(type || "").trim().toLowerCase();
  if (!t) return "other";
  if (t === "schwelle" || t === "threshold") return "threshold";
  if (t === "vo2_touch" || t === "vo2") return "vo2";
  if (t === "racepace") return "racepace";
  if (t === "longrun") return "longrun";
  return t;
}

function buildWeeklySnapshot(ctx, dayIso, context = {}) {
  const end = new Date(dayIso + "T00:00:00Z");
  const start7Iso = isoDate(new Date(end.getTime() - 6 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  const runs7 = (ctx?.activitiesAll || []).filter((a) => {
    if (!isRun(a)) return false;
    const d = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    return d && d >= start7Iso && d < endIso;
  });

  const keyTypes = uniq((context?.keyStats7?.list || []).map(normalizeDiagnosticKeyType).filter(Boolean));
  const longrunSpecificity = context?.longrunSpecificity || null;
  const qualityBudgetUsed = Number(longrunSpecificity?.qualityBudgetUsed ?? 0);

  return {
    eventDistance: normalizeEventDistance(context?.eventDistance) || "10k",
    block: context?.block || "BASE",
    runFloor: Number(context?.runFloor ?? 0),
    runLoad7: Number(context?.runLoad7 ?? 0),
    runsCount: runs7.length,
    easyShare: Number(context?.intensityDistribution?.easyShare ?? 0),
    midShare: Number(context?.intensityDistribution?.midShare ?? 0),
    hardShare: Number(context?.intensityDistribution?.hardShare ?? 0),
    keyCount: Number(context?.keyStats7?.count ?? 0),
    keyTypes,
    longrunMin: Number(context?.longRunSummary?.minutes ?? 0),
    longrunSpecificMin: Number.isFinite(qualityBudgetUsed) ? qualityBudgetUsed : 0,
    strengthMin: Number(context?.robustness?.strengthMinutes7d ?? 0),
    fatigueOverride: context?.fatigue?.override === true,
    keySpacingOk: context?.keyCompliance?.keySpacingOk !== false,
    efTrend: Number(context?.trend?.dv ?? 0),
    driftTrend: Number(context?.trend?.dd ?? 0),
    executionScoreRaw: Number(context?.executionScore ?? 0),
    executionScore: Number(context?.executionScore ?? 0),
  };
}

function scoreByTargetRatio(value, target, lowerCap = 0, upperCap = 1.2) {
  if (!(target > 0)) return 50;
  const ratio = clamp((Number(value) || 0) / target, lowerCap, upperCap);
  return clamp(Math.round((ratio / upperCap) * 100), 0, 100);
}

function computeExecutionQualityScore({ keyCompliance, trend, longRunSummary, fatigue }) {
  let score = 72;
  if (keyCompliance?.freqOk === false) score -= 14;
  if (keyCompliance?.typeOk === false) score -= 14;
  if (keyCompliance?.preferredMissing === true) score -= 8;
  if (keyCompliance?.keySpacingOk === false) score -= 8;
  if (fatigue?.override === true) score -= 10;
  if (Number.isFinite(trend?.dd) && trend.dd > 1.5) score -= 8;
  if (Number.isFinite(trend?.dv) && trend.dv < 0) score -= 6;
  if (longRunSummary?.quality === "weak") score -= 8;
  const racepaceProgress = Number(keyCompliance?.racepaceBlockProgress?.pct ?? null);
  if (Number.isFinite(racepaceProgress) && racepaceProgress >= 90) score += 6;
  return clamp(Math.round(score), 0, 100);
}

function computeDistanceDiagnostics(snapshot, context = {}) {
  const dist = normalizeEventDistance(snapshot?.eventDistance) || "10k";
  const req = DISTANCE_REQUIREMENTS[dist] || DISTANCE_REQUIREMENTS["10k"];
  const intensityTargets = DISTANCE_INTENSITY_TARGETS[req.intensityProfile] || DISTANCE_INTENSITY_TARGETS["10k"];

  const parseDay = (a) => String(a?.start_date_local || a?.start_date || "").slice(0, 10);
  const dayIso = String(context?.dayIso || "");
  const end = dayIso ? new Date(dayIso + "T00:00:00Z") : new Date();

  const collectRunStats = (windowDays = 28) => {
    const safeDays = Math.max(7, Number(windowDays) || 28);
    const startIso = isoDate(new Date(end.getTime() - (safeDays - 1) * 86400000));
    const endIso = isoDate(new Date(end.getTime() + 86400000));
    let runCount = 0;
    let runMinutes = 0;
    let easyMinutes = 0;
    const days = new Set();

    for (const a of context?.activitiesAll || []) {
      if (!isRun(a)) continue;
      const d = parseDay(a);
      if (!d || d < startIso || d >= endIso) continue;
      const sec = Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0;
      const min = sec / 60;
      if (!(min > 0)) continue;
      runCount += 1;
      runMinutes += min;
      days.add(d);
      const isLikelyEasy = a?.is_key !== true && a?.key_session !== true;
      if (isLikelyEasy) easyMinutes += min;
    }

    const weeks = safeDays / 7;
    return {
      runCount,
      runMinutes,
      easyMinutes,
      runDays: days.size,
      runsPerWeek: weeks > 0 ? runCount / weeks : 0,
      runDaysPerWeek: weeks > 0 ? days.size / weeks : 0,
    };
  };

  const scoreStrengthTier = (minutes7d) => {
    const mins = Number(minutes7d || 0);
    if (mins <= 15) return 25;
    if (mins <= 35) return 55;
    if (mins <= 60) return 78;
    return 92;
  };

  const collectKeyStats = (windowDays = 28) => {
    const safeDays = Math.max(14, Number(windowDays) || 28);
    const startIso = isoDate(new Date(end.getTime() - (safeDays - 1) * 86400000));
    const endIso = isoDate(new Date(end.getTime() + 86400000));
    const keyTypesWindow = [];

    for (const a of context?.activitiesAll || []) {
      if (!isRun(a) || !hasKeyTag(a)) continue;
      const d = parseDay(a);
      if (!d || d < startIso || d >= endIso) continue;
      keyTypesWindow.push(normalizeDiagnosticKeyType(getKeyType(a)));
    }

    return {
      count: keyTypesWindow.length,
      types: uniq(keyTypesWindow.filter(Boolean)),
    };
  };

  const computeIntensityBalanceScore = (easyShare, hardShare) => {
    const easy = Number(easyShare || 0);
    const hard = Number(hardShare || 0);
    const easyPenalty = Math.max(0, intensityTargets.easyMin - easy) * 110;
    const hardPenalty = Math.max(0, hard - intensityTargets.hardMax) * 90;
    const corridorBonus = easy >= intensityTargets.easyMin && hard <= intensityTargets.hardMax ? 8 : 0;
    return clamp(Math.round(70 - easyPenalty - hardPenalty + corridorBonus), 0, 100);
  };

  const floorTarget = Number(context?.runFloorTarget ?? 0);
  const stats28 = collectRunStats(28);
  const stats42 = collectRunStats(42);
  const keyStats14 = collectKeyStats(14);
  const keyStats28 = collectKeyStats(28);
  const keyStats42 = collectKeyStats(42);
  const runsTarget = dist === "5k" ? 3 : dist === "10k" ? 3 : 4;

  const runFloorScore = floorTarget > 0 ? scoreByTargetRatio(snapshot.runFloor, floorTarget, 0, 1.05) : 58;
  const consistencyScore = clamp(Math.round((stats42.runDaysPerWeek / Math.max(1, runsTarget)) * 100), 0, 100);
  const freqScore = clamp(Math.round((stats42.runsPerWeek / runsTarget) * 100), 0, 100);
  const easyVolumeTarget = Math.max(90, runsTarget * 40);
  const easyVolumeScore = clamp(Math.round((stats42.easyMinutes / easyVolumeTarget) * 100), 0, 100);
  const easyShareScore = clamp(Math.round((stats28.easyMinutes / Math.max(1, stats28.runMinutes)) * 100), 0, 100);
  const intensityBalanceScore = computeIntensityBalanceScore(snapshot.easyShare, snapshot.hardShare);
  let base = clamp(
    Math.round(
      runFloorScore * 0.30 +
      consistencyScore * 0.24 +
      freqScore * 0.20 +
      easyVolumeScore * 0.16 +
      easyShareScore * 0.08 +
      intensityBalanceScore * 0.02
    ),
    0,
    100
  );
  const runFloorRatio = floorTarget > 0 ? clamp((Number(snapshot.runFloor) || 0) / floorTarget, 0, 1.2) : 1;
  if (floorTarget > 0 && runFloorRatio < 0.9) {
    const floorPenalty = Math.round((0.9 - runFloorRatio) * 80);
    base = clamp(base - floorPenalty, 0, 100);
  }
  const easyShareDeficit = Math.max(0, intensityTargets.easyMin - (snapshot.easyShare || 0));
  if (easyShareDeficit > 0.06) base = clamp(base - Math.round(easyShareDeficit * 120), 0, 100);
  if (floorTarget > 0 && runFloorRatio < 0.85 && stats42.runsPerWeek < runsTarget + 0.2) base = Math.min(base, 80);
  if (stats42.runDaysPerWeek < runsTarget - 0.1) base = Math.min(base, 89);
  const baseEliteUnlocked =
    stats42.runDaysPerWeek >= runsTarget + 0.4 &&
    (snapshot.easyShare || 0) >= intensityTargets.easyMin - 0.02 &&
    consistencyScore >= 88 &&
    runFloorScore >= 88;
  if (!baseEliteUnlocked) base = Math.min(base, 92);

  const focus = (req.keyFocus || []).map(normalizeDiagnosticKeyType);
  const keyTypes = keyStats28.types.length ? keyStats28.types : (snapshot.keyTypes || []).map(normalizeDiagnosticKeyType);
  const keyTypes42 = keyStats42.types.length ? keyStats42.types : keyTypes;
  const matchedFocusTypes = focus.filter((type) => keyTypes42.includes(type));
  const matchedRecentFocusTypes = focus.filter((type) => keyTypes.includes(type));
  const focusHits = Number(context?.keyCompliance?.focusHits ?? matchedFocusTypes.length);
  const focusCoverage = focus.length ? focusHits / focus.length : 0;
  const recentFocusCoverage = focus.length ? matchedRecentFocusTypes.length / focus.length : 0;
  const keyDensity28 = clamp(keyStats28.count / Math.max(1, 4), 0, 1);
  const keyDensity42 = clamp(keyStats42.count / Math.max(1, 6), 0, 1);
  const intensityPenalty = Math.max(0, Number(snapshot.hardShare || 0) - Number(intensityTargets.hardMax || 0));
  const weeksToEvent = Number(context?.weeksToEvent);
  const raceProximity = Number.isFinite(weeksToEvent)
    ? clamp((16 - clamp(weeksToEvent, 0, 16)) / 16, 0, 1)
    : snapshot.block === "RACE"
      ? 1
      : snapshot.block === "BUILD"
        ? 0.6
        : 0.25;
  const specificityInfluence = 0.55 + raceProximity * 0.45;
  const blockFit =
    snapshot.block === "BASE"
      ? clamp(0.75 + focusCoverage * 0.25, 0, 1)
      : snapshot.block === "BUILD"
        ? clamp(0.55 + focusCoverage * 0.45, 0, 1)
        : clamp(0.45 + focusCoverage * 0.55, 0, 1);
  let specificity = clamp(
    Math.round(
      30 +
      focusCoverage * 30 +
      recentFocusCoverage * 20 +
      keyDensity28 * 12 +
      keyDensity42 * 8 +
      blockFit * 12 * specificityInfluence
    ),
    0,
    100
  );
  if (context?.keyCompliance?.preferredMissing) specificity = clamp(specificity - 8, 0, 100);
  if (keyStats14.count === 0 && keyStats28.count > 0) specificity = clamp(specificity - 4, 0, 100);

  const longrunTarget = Math.max(Number(req.longrunTargetMin || 0), Number(LONGRUN_PREPLAN.targetMinByDistance?.[dist] || 0));
  const progressionLongrunTarget = Number(context?.longRunSummary?.plan?.targetMin ?? 0);
  const longrunInput14d = Number(context?.longRunSummary?.longRun14d?.minutes ?? 0);
  const longrunInput28d = Number(context?.longRunSummary?.longRun28d?.minutes ?? longrunInput14d);
  const lastLongrunMin = Number(snapshot.longrunMin || 0);
  const longRunFreq = Number(context?.longrunFrequency21d ?? 0);
  const longRunFreq35d = Number(context?.longrunFrequency35d ?? longRunFreq);
  const spikeIndex = Number(context?.longrunSpikeIndex ?? 1);
  const capabilityScore = clamp(Math.round((Math.max(longrunInput14d, longrunInput28d) / Math.max(1, longrunTarget)) * 100), 0, 100);
  const consistencyLongrunScore = clamp(Math.round((longRunFreq35d / 3) * 100), 0, 100);
  const recencyLongrunScore = clamp(Math.round((Math.max(lastLongrunMin, Number(context?.longRunSummary?.longRun7d?.minutes ?? 0)) / Math.max(1, longrunTarget)) * 100), 0, 100);
  const driftSignal = Number(snapshot.driftTrend || 0);
  const driftDurabilityScore = clamp(Math.round(70 - Math.max(0, driftSignal) * 10), 0, 100);
  const spikePenalty = spikeIndex > 1.18 ? Math.round((spikeIndex - 1.18) * 60) : 0;
  const longrun = clamp(
    Math.round(capabilityScore * 0.48 + consistencyLongrunScore * 0.28 + recencyLongrunScore * 0.20 + driftDurabilityScore * 0.04 - spikePenalty),
    0,
    100
  );

  const strengthScore = scoreStrengthTier(snapshot.strengthMin);
  const strengthTargetCoachMin = Number(context?.strengthPolicy?.target ?? 60);
  const monotony = Number(context?.fatigue?.monotony ?? 0);
  const strain = Number(context?.fatigue?.strain ?? 0);
  const acwr = Number(context?.fatigue?.acwr ?? 0);
  const continuityScore = clamp(Math.round((stats42.runDaysPerWeek / Math.max(1, runsTarget)) * 100), 0, 100);
  const runDayScore = clamp(Math.round((stats28.runDaysPerWeek / Math.max(1, runsTarget)) * 100), 0, 100);
  const spacingScore = snapshot.keySpacingOk ? 88 : 38;
  let fatiguePenalty = 0;
  if (snapshot.fatigueOverride) fatiguePenalty += 14;
  if (monotony > 2.1) fatiguePenalty += 8;
  if (strain > 1200) fatiguePenalty += 8;
  if (acwr > 1.3) fatiguePenalty += 8;
  if (!snapshot.keySpacingOk) fatiguePenalty += 12;
  let robustness = clamp(Math.round(continuityScore * 0.30 + runDayScore * 0.18 + strengthScore * 0.36 + spacingScore * 0.10 + (100 - fatiguePenalty) * 0.06), 0, 100);
  if (snapshot.strengthMin <= 15 && !snapshot.keySpacingOk) robustness = Math.min(robustness, 62);
  else if (snapshot.strengthMin <= 15) robustness = Math.min(robustness, 64);
  else if (!snapshot.keySpacingOk) robustness = Math.min(robustness, 74);

  const executionProcess = clamp(Math.round(snapshot.executionScoreRaw || 0), 0, 100);
  const chaosPenalty = snapshot.keySpacingOk ? 0 : 14;
  const compliancePenalty = context?.keyCompliance?.freqOk === false ? 10 : 0;
  const execution = clamp(Math.round(executionProcess * 0.7 + (100 - chaosPenalty - compliancePenalty) * 0.3), 0, 100);

  const readinessRaw =
    base * req.weights.base +
    specificity * req.weights.specificity +
    longrun * req.weights.longrun +
    robustness * req.weights.robustness +
    execution * req.weights.execution;
  const readiness = clamp(Math.round(readinessRaw), 0, 100);

  const entries = [
    ["base", base],
    ["specificity", specificity],
    ["longrun", longrun],
    ["robustness", robustness],
    ["execution", execution],
  ].sort((a, b) => a[1] - b[1]);

  const primaryGap = entries[0]?.[0] || "base";
  const secondaryGap = entries[1]?.[0] || "specificity";
  const strengths = entries.slice(-2).reverse().map(([name]) => name);

  const toConfidenceLabel = (value) => {
    if (value >= 0.75) return "hoch";
    if (value >= 0.45) return "mittel";
    return "niedrig";
  };
  const baseConfidenceRaw = [
    floorTarget > 0 ? 0.95 : 0.65,
    clamp(consistencyScore / 100, 0.4, 1),
    clamp(freqScore / 100, 0.4, 1),
    clamp(easyVolumeScore / 100, 0.35, 1),
    clamp(easyShareScore / 100, 0.35, 1),
  ].reduce((sum, x) => sum + x, 0) / 5;
  const specificityConfidenceRaw = [
    focus.length > 0 ? 0.95 : 0.55,
    clamp((snapshot.keyCount || 0) / 2, 0.35, 1),
    clamp(specificityInfluence, 0.35, 1),
  ].reduce((sum, x) => sum + x, 0) / 3;
  const longrunConfidenceRaw = [
    clamp(capabilityScore / 100, 0.4, 1),
    clamp(consistencyLongrunScore / 100, 0.35, 1),
    clamp(recencyLongrunScore / 100, 0.35, 1),
    clamp(driftDurabilityScore / 100, 0.35, 1),
  ].reduce((sum, x) => sum + x, 0) / 4;
  const robustnessConfidenceRaw = [
    clamp(continuityScore / 100, 0.4, 1),
    clamp(runDayScore / 100, 0.35, 1),
    clamp(strengthScore / 100, 0.35, 1),
    snapshot.fatigueOverride ? 0.45 : 0.95,
  ].reduce((sum, x) => sum + x, 0) / 4;
  const executionConfidenceRaw = [
    clamp((snapshot.executionScoreRaw || 0) / 100, 0.35, 1),
    context?.keyCompliance ? 0.95 : 0.45,
    context?.fatigue ? 0.95 : 0.45,
  ].reduce((sum, x) => sum + x, 0) / 3;

  const longrunCapabilityConfirmed = Math.max(longrunInput14d, longrunInput28d) >= longrunTarget;

  const componentDetails = {
    base: {
      score: base,
      confidence: {
        value: Math.round(baseConfidenceRaw * 100),
        label: toConfidenceLabel(baseConfidenceRaw),
      },
      confirmedAbility: runFloorScore >= 75 ? "Aerobe Basis im Zielkorridor nachgewiesen." : "Aerobe Basis teils nachgewiesen, noch unter Zielkorridor.",
      consistency: `4-6 Wochen: ${stats42.runDaysPerWeek.toFixed(1)} Lauftage/Woche, Easy-Volumen ${Math.round(stats28.easyMinutes)}′/28T.`,
      recency: `Aktuell: ${Math.round(stats28.runsPerWeek)} Läufe/Woche, Easy-Anteil ${Math.round((snapshot.easyShare || 0) * 100)}%.`,
      constraints: [
        floorTarget > 0 && snapshot.runFloor < floorTarget ? `RunFloor unter Ziel (${Math.round(floorTarget - snapshot.runFloor)} Gap)` : null,
        stats28.runsPerWeek < runsTarget ? "Wochenfrequenz unter Distanzprofil" : null,
        snapshot.easyShare < intensityTargets.easyMin ? "Easy-Anteil unter Ziel" : null,
      ].filter(Boolean),
      inputs: [
        `confirmed ability: ${runFloorScore >= 75 ? "ja" : "teilweise"}`,
        `consistency: ${stats42.runDaysPerWeek.toFixed(1)} Lauftage/Woche`,
        `recency: ${Math.round(stats28.runsPerWeek)} Läufe/Woche`,
      ],
      factorsUp: [runFloorScore >= 75 ? "Fähigkeit bestätigt" : null, stats42.runDaysPerWeek >= runsTarget ? "Regelmäßigkeit stabil" : null].filter(Boolean),
      factorsDown: [
        floorTarget > 0 && snapshot.runFloor < floorTarget ? "RunFloor unter Ziel" : null,
        stats28.runsPerWeek < runsTarget ? "Frequenz unter Ziel" : null,
        snapshot.easyShare < intensityTargets.easyMin ? "Easy-Anteil unter Ziel" : null,
      ].filter(Boolean),
      interpretation: base >= 84 ? "Basis stabil und belastbar." : base >= 60 ? "Basis solide, aber noch unter Zielkorridor." : "Basis okay, über Kontinuität weiter festigen.",
    },
    specificity: {
      score: specificity,
      confidence: {
        value: Math.round(specificityConfidenceRaw * 100),
        label: toConfidenceLabel(specificityConfidenceRaw),
      },
      confirmedAbility: focusCoverage >= 0.66 ? "Distanzspezifische Reize sind nachgewiesen." : "Spezifische Reize bisher nur teilweise nachgewiesen.",
      consistency: `Blockpassung: ${snapshot.block || "n/a"}, Fokusabdeckung ${focusHits}/${focus.length || 0}.`,
      recency: `Wettkampfnähe-Faktor ${(specificityInfluence * 100).toFixed(0)}% (höher bei Rennnähe).`,
      constraints: [
        focusCoverage < 0.5 ? "Zu wenig distanzspezifische Reize" : null,
        intensityPenalty > 0 ? "Hard-Anteil über Ziel" : null,
        context?.keyCompliance?.preferredMissing ? "Bevorzugter Reiz fehlt" : null,
      ].filter(Boolean),
      inputs: [
        `confirmed ability: ${focusCoverage >= 0.66 ? "ja" : "teilweise"}`,
        `consistency: Fokusabdeckung ${focusHits}/${focus.length || 0}`,
        `recency: Wettkampfnähe ${(specificityInfluence * 100).toFixed(0)}%`,
      ],
      factorsUp: [focusCoverage >= 0.66 ? "Block-/Distanzfit gegeben" : null].filter(Boolean),
      factorsDown: [focusCoverage < 0.5 ? "Abdeckung niedrig" : null, intensityPenalty > 0 ? "Hard-Anteil hoch" : null].filter(Boolean),
      interpretation: specificity >= 75 ? "Spezifität passt zu Distanz und Zyklusphase." : specificity >= 55 ? "Spezifität vorhanden, gezielt weiter schärfen." : "Spezifität ist aktuell ausbaufähig.",
    },
    longrun: {
      score: longrun,
      confidence: {
        value: Math.round(longrunConfidenceRaw * 100),
        label: toConfidenceLabel(longrunConfidenceRaw),
      },
      confirmedAbility: longrunCapabilityConfirmed ? "Longrun-Ziel nachgewiesen, Fähigkeit vorhanden." : "Longrun-Fähigkeit teilweise nachgewiesen.",
      consistency: `Longruns 35T: ${longRunFreq35d} (21T: ${longRunFreq}); Capability ${Math.round(Math.max(longrunInput14d, longrunInput28d))}/${Math.round(longrunTarget)}′.`,
      recency: `Zuletzt ${Math.round(lastLongrunMin)}′ (7T), letzter Peak 14T ${Math.round(longrunInput14d)}′.`,
      constraints: [
        longRunFreq35d < 3 ? "Longrun-Frequenz über 5 Wochen niedrig" : null,
        recencyLongrunScore < 70 && longrunCapabilityConfirmed ? "Zuletzt kürzer: Fähigkeit eher gehalten als ausgebaut" : null,
        !longrunCapabilityConfirmed ? "Zielumfang noch nicht voll nachgewiesen" : null,
        spikePenalty > 0 ? "Spike-Guard aktiv" : null,
      ].filter(Boolean),
      inputs: [
        `confirmed ability: ${longrunCapabilityConfirmed ? "ja" : "teilweise"}`,
        `consistency: Longruns35T ${longRunFreq35d}`,
        `recency: letzter Longrun ${Math.round(lastLongrunMin)}′`,
      ],
      factorsUp: [longrunCapabilityConfirmed ? "Ziel nachgewiesen" : null, longRunFreq35d >= 3 ? "Frequenz stabil" : null].filter(Boolean),
      factorsDown: [longRunFreq35d < 3 ? "Frequenz niedrig" : null, spikePenalty > 0 ? "Spike-Guard" : null].filter(Boolean),
      interpretation: longrunCapabilityConfirmed
        ? recencyLongrunScore >= 80
          ? "Longrun-Fähigkeit bestätigt und aktuell gut gehalten."
          : "Longrun-Fähigkeit bestätigt; zuletzt kürzer, aktuell eher gehalten als ausgebaut."
        : longrun >= 55
          ? "Longrun-Aufbau läuft, Fähigkeit noch nicht voll bestätigt."
          : "Longrun bleibt ein klarer Entwicklungshebel.",
    },
    robustness: {
      score: robustness,
      confidence: {
        value: Math.round(robustnessConfidenceRaw * 100),
        label: toConfidenceLabel(robustnessConfidenceRaw),
      },
      confirmedAbility: robustness >= 70 ? "Belastbarkeit im Alltagstraining ist nachgewiesen." : "Belastbarkeit ist vorhanden, aber noch fragil.",
      consistency: `Kontinuität 4-6 Wochen: ${stats42.runDaysPerWeek.toFixed(1)} Lauftage/Woche; Kraft ${Math.round(snapshot.strengthMin)}′/7T.`,
      recency: `Fatigue-Status: Override ${snapshot.fatigueOverride ? "aktiv" : "aus"}, Key-Spacing ${snapshot.keySpacingOk ? "ok" : "verletzt"}.`,
      constraints: [
        snapshot.strengthMin <= 15 ? "Kraftumfang 0-15′/Woche (schwach)" : null,
        snapshot.strengthMin > 15 && snapshot.strengthMin <= 35 ? "Kraftumfang 20-35′/Woche (okay)" : null,
        snapshot.fatigueOverride ? "Fatigue-Override aktiv" : null,
        !snapshot.keySpacingOk ? "Key-Abstand verletzt" : null,
        monotony > 2.1 ? "Monotony-Schwelle überschritten" : null,
        strain > 1200 ? "Strain-Schwelle überschritten" : null,
        acwr > 1.3 ? "ACWR-Schwelle überschritten" : null,
      ].filter(Boolean),
      inputs: [
        `confirmed ability: ${robustness >= 70 ? "ja" : "teilweise"}`,
        `consistency: ${stats42.runDaysPerWeek.toFixed(1)} Lauftage/Woche`,
        `recency: Kraft ${Math.round(snapshot.strengthMin)}′/7T`,
      ],
      factorsUp: [robustness >= 75 ? "Belastbarkeit stabil" : null].filter(Boolean),
      factorsDown: [snapshot.fatigueOverride ? "Fatigue-Flag" : null, !snapshot.keySpacingOk ? "Spacing-Verletzung" : null].filter(Boolean),
      interpretation: robustness >= 75 ? "Robustheit gut ausgeprägt." : robustness >= 55 ? "Robustheit okay, mit Reserven in Kontinuität/Kraft." : "Robustheit limitiert derzeit die Belastbarkeit.",
    },
    execution: {
      score: execution,
      confidence: {
        value: Math.round(executionConfidenceRaw * 100),
        label: toConfidenceLabel(executionConfidenceRaw),
      },
      confirmedAbility: execution >= 70 ? "Planumsetzung als Prozess ist stabil." : "Planumsetzung ist nur teilweise stabil.",
      consistency: `Prozess: Key-Frequenz ${context?.keyCompliance?.freqOk === false ? "außerhalb Ziel" : "im Ziel"}, Spacing ${snapshot.keySpacingOk ? "stabil" : "chaotisch"}.`,
      recency: `Aktuelle Compliance: Execution ${execution}/100, Fatigue-Bremse ${context?.fatigue?.override ? "ja" : "nein"}.`,
      constraints: [
        context?.keyCompliance?.freqOk === false ? "Key-Frequenz außerhalb Ziel" : null,
        context?.keyCompliance?.typeOk === false ? "Key-Typen nicht passend" : null,
        !snapshot.keySpacingOk ? "Wochenstruktur instabil" : null,
        context?.fatigue?.override ? "Fatigue bremst Ausführung" : null,
      ].filter(Boolean),
      inputs: [
        `confirmed ability: ${execution >= 70 ? "ja" : "teilweise"}`,
        `consistency: Wochenstruktur ${snapshot.keySpacingOk ? "stabil" : "instabil"}`,
        `recency: Compliance ${execution}/100`,
      ],
      factorsUp: [execution >= 75 ? "Prozess stabil" : null].filter(Boolean),
      factorsDown: [context?.keyCompliance?.freqOk === false ? "Frequenzabweichung" : null, !snapshot.keySpacingOk ? "Chaos in Wochenstruktur" : null].filter(Boolean),
      interpretation: execution >= 75 ? "Ausführung prozessstabil." : execution >= 55 ? "Ausführung solide mit leichten Brüchen." : "Ausführung aktuell inkonsistent.",
    },
  };

  const diagnosticSnapshot = {
    ...snapshot,
    executionScoreRaw: executionProcess,
    executionScoreFinal: execution,
  };

  return {
    readiness,
    scores: { base, specificity, longrun, robustness, execution },
    components: componentDetails,
    weights: req.weights,
    primaryGap,
    secondaryGap,
    strengths,
    snapshot: diagnosticSnapshot,
  };
}

function buildGapRecommendations(diagnostics) {
  const primary = diagnostics?.primaryGap;
  const secondary = diagnostics?.secondaryGap;
  const map = {
    longrun: ["Longrun häufiger einplanen", "Longrun schrittweise verlängern", "distanzspezifische Segmente im Longrun einbauen"],
    specificity: ["mehr distanzspezifische Key-Reize setzen", "bevorzugte Reize priorisieren", "Intensitätsverteilung am Zielprofil ausrichten"],
    base: ["mehr easy Minuten sammeln", "Lauffrequenz pro Woche erhöhen", "RunFloor stabil über mehrere Wochen halten"],
    robustness: ["Krafttraining auf 2x/Woche stabilisieren", "Load-Anstieg konservativer gestalten", "Key-Abstände strikt einhalten"],
    execution: ["Key-Sessions sauber zu Ende laufen", "Pace-Stabilität priorisieren", "Qualität vor zusätzlichem Volumen"],
  };
  return {
    primaryFocus: map[primary] || [],
    secondaryFocus: map[secondary] || [],
  };
}

const COACH_GAP_LANGUAGE = {
  base: {
    label: "Basis",
    aliases: ["Volumen", "Kontinuität"],
    focus: "Volumen stabilisieren.",
  },
  specificity: {
    label: "Spezifik",
    aliases: ["rennahe Reize"],
    focus: "Den nächsten passenden Reiz sauber setzen.",
  },
  longrun: {
    label: "Longrun",
    aliases: ["längerer aerober Reiz"],
    focus: "Longrun stabil halten.",
  },
  robustness: {
    label: "Robustheit",
    aliases: ["Kraft", "Belastbarkeit"],
    focus: "Kraft zurückbringen.",
  },
  execution: {
    label: "Struktur / Wochenrhythmus",
    aliases: ["Rhythmus", "Wochenstruktur", "Planruhe"],
    focus: "Rhythmus sauber halten.",
  },
};

function mapGapToCoachLanguage(gap) {
  return COACH_GAP_LANGUAGE[gap] || { label: "Stabilität", aliases: ["Rhythmus"], focus: "Wochenstruktur stabilisieren." };
}

function buildCoachFocusSummary(primaryGap, secondaryGap) {
  const primary = mapGapToCoachLanguage(primaryGap);
  const secondary = secondaryGap ? mapGapToCoachLanguage(secondaryGap) : null;
  return {
    label: secondary ? `${primary.label} + ${secondary.label}` : primary.label,
    action: secondary ? `${primary.focus} ${secondary.focus}` : primary.focus,
    primary,
    secondary,
  };
}

function computeLongrunFrequency21d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 20 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  let count = 0;
  for (const a of ctx?.activitiesAll || []) {
    if (!isRun(a)) continue;
    const d = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    const min = (Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0) / 60;
    if (min >= 60) count += 1;
  }
  return count;
}

function computeLongrunFrequency35d(ctx, dayIso) {
  const end = new Date(dayIso + "T00:00:00Z");
  const startIso = isoDate(new Date(end.getTime() - 34 * 86400000));
  const endIso = isoDate(new Date(end.getTime() + 86400000));
  let count = 0;
  for (const a of ctx?.activitiesAll || []) {
    if (!isRun(a)) continue;
    const d = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    if (!d || d < startIso || d >= endIso) continue;
    const min = (Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0) / 60;
    if (min >= 60) count += 1;
  }
  return count;
}

function computeLongrunSpikeIndex(longRunSummary) {
  const current = Number(longRunSummary?.minutes ?? 0);
  const recentPeak = Number(longRunSummary?.longestRun30d?.minutes ?? 0);
  if (!(current > 0) || !(recentPeak > 0)) return 1;
  return current / recentPeak;
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

function buildKeyPatternDistributionLine({ block, eventDistance, plannedType, weeksToEvent }) {
  const dist = normalizeEventDistance(eventDistance) || "10k";
  const patternBlock = pickPatternBlock({ block, eventDistance: dist, weeksToEvent });
  const pattern = KEY_PATTERN_1PERWEEK?.[patternBlock]?.[dist];
  if (!Array.isArray(pattern) || !pattern.length) return null;

  const counts = pattern.reduce((acc, type) => {
    acc[type] = (acc[type] || 0) + 1;
    return acc;
  }, {});
  const distribution = Object.entries(counts)
    .map(([type, count]) => `${formatKeyType(type)} ${Math.round((count / pattern.length) * 100)}%`)
    .join(" | ");
  const plannedLabel = plannedType ? formatKeyType(plannedType) : "n/a";
  return `Pattern 1 Key/Woche (${patternBlock}, ${formatEventDistance(dist)}): geplant ${plannedLabel}; Verteilung ${distribution}.`;
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
  nextAllowed,
  todayIso,
  keyMinGapDays,
}) {
  let next = "45–60 min locker/GA";
  const overlay = runFloorState?.overlayMode ?? "NORMAL";
  if (overlay === "LIFE_EVENT_STOP") {
    next = "Pause / nur Regeneration (LifeEvent)";
  } else if (overlay === "LIFE_EVENT_HOLIDAY") {
    next = "20–45 min locker (Holiday-Modus)";
  } else if (overlay === "POST_RACE_RAMP") {
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
    if (nextAllowed && todayIso && isIsoDate(todayIso) && isIsoDate(nextAllowed)) {
      const remainingWaitHours = Math.max(0, diffDays(todayIso, nextAllowed)) * 24;
      next = `Nächster Key frühestens ab ${nextAllowed} (in ${remainingWaitHours}h) – bis dahin locker/GA.`;
    } else {
      const minGapHours = Math.max(24, Math.round((Number(keyMinGapDays) || KEY_MIN_GAP_DAYS_DEFAULT) * 24));
      next = `Mindestabstand für Key-Reize: ${minGapHours}h – bis dahin locker/GA.`;
    }
  }

  return next;
}

function isLegacyNextKeyText(text) {
  const lower = String(text || "").toLowerCase();
  if (!lower) return false;
  return lower.includes("nächster key") || lower.includes("next key") || lower.includes("key frühestens");
}

function addUniqueTopicLine(topicBucket, topic, line) {
  if (!line) return;
  if (topicBucket.has(topic)) return;
  topicBucket.add(topic);
  topicBucket.lines.push(line);
}


function resolveNextKeyTiming({ spacingBlocked, nextAllowed, todayIso, keySpacingOk, keyMinGapDays }) {
  const requiredMinGapHours = Math.max(24, Math.round((Number(keyMinGapDays) || KEY_MIN_GAP_DAYS_DEFAULT) * 24));
  const hasEarliestDate = spacingBlocked && nextAllowed && isIsoDate(nextAllowed);
  const remainingWaitHours = hasEarliestDate && todayIso && isIsoDate(todayIso)
    ? Math.max(0, diffDays(todayIso, nextAllowed)) * 24
    : (keySpacingOk === false ? requiredMinGapHours : null);

  return {
    requiredMinGapHours,
    remainingWaitHours,
    nextKeyEarliestDate: hasEarliestDate ? nextAllowed : null,
  };
}

function buildResolvedDecision({
  todayDecision,
  todayIso,
  spacingBlocked,
  nextAllowed,
  keySpacingOk,
  keyMinGapDays,
  readinessScore,
  mainLimiter,
}) {
  const resolvedTiming = resolveNextKeyTiming({
    spacingBlocked,
    nextAllowed,
    todayIso,
    keySpacingOk,
    keyMinGapDays,
  });
  return {
    todayDecision: `${todayDecision}.`,
    requiredMinGapHours: resolvedTiming.requiredMinGapHours,
    remainingWaitHours: resolvedTiming.remainingWaitHours,
    nextKeyEarliestDate: resolvedTiming.nextKeyEarliestDate,
    readinessScore: Number.isFinite(readinessScore) ? readinessScore : null,
    mainLimiter: mainLimiter || "n/a",
  };
}

function buildResolvedNextKeyLine(resolvedDecision) {
  if (resolvedDecision?.remainingWaitHours == null) return null;
  if (resolvedDecision?.nextKeyEarliestDate) {
    return `Nächster Key frühestens ab ${resolvedDecision.nextKeyEarliestDate} (in ${resolvedDecision.remainingWaitHours}h; Mindestabstand ${resolvedDecision.requiredMinGapHours}h).`;
  }
  return `Mindestabstand bis zum nächsten Key: ${resolvedDecision.remainingWaitHours}h.`;
}

function resolveBottomLine({ candidate, todayDecision }) {
  const text = String(candidate || "").trim();
  const fallback = "Heute dosiert arbeiten und den nächsten Qualitätsreiz sauber vorbereiten.";
  if (!text) return fallback;
  const lower = text.toLowerCase();
  const introducesPrimaryTopic = ["nächster key", "readiness", "hauptlimit", "heute:"].some((token) => lower.includes(token));
  if (introducesPrimaryTopic) return fallback;
  if (text === String(todayDecision || "").trim()) return fallback;
  return text;
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

function explicitSessionFromSuggestion(suggestion) {
  const text = String(suggestion || "");
  const match = text.match(/Konkrete Session-Idee:\s*([^\n]+)/i);
  if (!match) return null;
  return String(match[1] || "")
    .split(".")[0]
    .replace(/\s+/g, " ")
    .trim();
}

function capLines(lines, maxLines) {
  return (lines || []).filter(Boolean).slice(0, maxLines);
}

function capText(s, maxChars) {
  const x = String(s || "").trim();
  if (x.length <= maxChars) return x;
  return `${x.slice(0, Math.max(0, maxChars - 1)).trimEnd()}…`;
}

function isEasyTodayDecision(text) {
  const normalized = String(text || "").toLowerCase();
  if (!normalized) return false;
  const easySignals = ["locker", "steady", "ga", "regeneration", "easy"];
  const hardSignals = ["@", "interval", "vo2", "schwelle", "racepace", "key", "tempo"];
  const hasEasy = easySignals.some((signal) => normalized.includes(signal));
  const hasHard = hardSignals.some((signal) => normalized.includes(signal));
  return hasEasy && !hasHard;
}

function buildRecommendationsAndBottomLine(state) {
  const rec = [];
  const bottom = [];
  const insight = [];

  const runFloorTarget = state?.runFloorTarget;
  const runFloorNow = state?.runFloorEwma10 ?? state?.runFloor7;
  const explicitSessionShort = state?.explicitSessionShort;
  const longRunDoneMin = Number(state?.longRunDoneMin ?? 0);
  const longRunTargetMin = Number(state?.longRunTargetMin ?? 0);
  const longRunGapMin = Number(state?.longRunGapMin ?? 0);
  const longRunStepCapMin = Number(state?.longRunStepCapMin ?? 0);
  const longRunSpikeCapMin = Number(state?.longRunSpikeCapMin ?? 0);
  const longRunSpikeWindowDays = Number(state?.longRunSpikeWindowDays ?? LONGRUN_PREPLAN.spikeGuardLookbackDays);
  const blockLongRunNextWeekTargetMin = Number(state?.blockLongRunNextWeekTargetMin ?? 0);
  const longRunDiagnosisTargetMin = Number(state?.longRunDiagnosisTargetMin ?? 0);
  const fatigue = state?.fatigue || null;
  const distanceDiagnostics = state?.distanceDiagnostics || null;
  const gapRecommendations = state?.gapRecommendations || null;

  const todayAction = String(state?.todayAction || "35–50′ locker/steady").replace(/\.$/, "");
  const easyDecision = isEasyTodayDecision(todayAction);
  bottom.push(`Heute: ${todayAction}.`);
  if (!easyDecision && state?.keyAllowedNow && explicitSessionShort) {
    // Nur bei einem expliziten Key-Tag als Tagesentscheidung in die Bottom-Line aufnehmen.
    bottom.push(`Key (wenn frisch): ${explicitSessionShort}.`);
  }

  if (Number.isFinite(runFloorNow) && Number.isFinite(runFloorTarget) && runFloorNow < runFloorTarget) {
    const runGap = Math.round(runFloorTarget - runFloorNow);
    rec.push(`RunFloor ${Math.round(runFloorNow)}/${Math.round(runFloorTarget)} → Volumen priorisieren (Gap ${runGap}).`);
  }
  if (Number.isFinite(longRunDoneMin) && Number.isFinite(longRunTargetMin) && longRunTargetMin > 0) {
    const meetsBlockTarget = longRunDoneMin >= longRunTargetMin;
    const meetsDiagnosisTarget = Number.isFinite(longRunDiagnosisTargetMin) && longRunDiagnosisTargetMin > 0
      ? longRunDoneMin >= longRunDiagnosisTargetMin
      : false;
    if (meetsBlockTarget || meetsDiagnosisTarget) {
      rec.push("Longrun im Zielbereich → aktuell halten.");
    } else {
      const spikeGuardNote = Number.isFinite(longRunSpikeCapMin) && longRunSpikeCapMin > 0
        ? ` (Spike-Guard ${longRunSpikeWindowDays}T: ≤${longRunSpikeCapMin}′)`
        : "";
      const progressionTargetMin = Number.isFinite(longRunStepCapMin) && longRunStepCapMin > 0
        ? longRunStepCapMin
        : blockLongRunNextWeekTargetMin;
      rec.push(`Longrun-Progression: nächster Schritt bis ${Math.round(progressionTargetMin)}′.${spikeGuardNote}`);
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
    const minGapText = Number.isFinite(state?.keyMinGapDays) ? `${state.keyMinGapDays * 24}h` : `${KEY_MIN_GAP_DAYS_DEFAULT * 24}h`;
    rec.push(`Key-Abstand <${minGapText}${state.nextAllowed ? ` (ab ${state.nextAllowed})` : ""} → heute kein Key.`);
  }
  if (state?.overlayMode && state.overlayMode !== "NORMAL") {
    rec.push(`Overlay: ${state.overlayMode} → konservativ bleiben.`);
  }

  if (fatigue?.override && Array.isArray(fatigue?.reasons) && fatigue.reasons.length) {
    insight.push(`Fatigue-Override aktiv: ${fatigue.reasons.slice(0, 2).join(" | ")}.`);
  }
  if (Number.isFinite(fatigue?.runDist14dRatio)) {
    insight.push(`Belastungs-Ratio 14T: ${fatigue.runDist14dRatio.toFixed(2)} (Guard <= ${RUN_DISTANCE_14D_LIMIT.toFixed(2)}).`);
  }
  if (Number.isFinite(fatigue?.acwr)) {
    insight.push(`ACWR: ${fatigue.acwr.toFixed(2)} (${fatigue.acwr > 1.3 ? "erhöht" : "stabil"}).`);
  }

  if (insight.length) {
    rec.push(...insight.map((line) => `Evidenz: ${line}`));
  }

  if (distanceDiagnostics) {
    rec.unshift(`Readiness ${distanceDiagnostics.readiness}/100 · Gap: ${distanceDiagnostics.primaryGap}${distanceDiagnostics.secondaryGap ? ` → ${distanceDiagnostics.secondaryGap}` : ""}.`);
    for (const line of (gapRecommendations?.primaryFocus || []).slice(0, 2)) {
      rec.push(`Diagnose-Fokus: ${line}.`);
    }
  }

  return {
    recommendations: capLines(rec, 6).map((x) => capText(x, 180)),
    bottomLine: capLines(bottom, 1).map((x) => capText(x, 180)),
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

function buildBikeAllowanceLine({ bikeSubFactor }) {
  const factor = Number.isFinite(bikeSubFactor) ? clamp(bikeSubFactor, 0, 1) : 0;
  const allowed = factor > 0;
  const factorPct = Math.round(factor * 100);
  return `Bike-Crosstraining: ${allowed ? "erlaubt" : "nicht erlaubt"} (Faktor ${factor.toFixed(2)} = ${factorPct}% RunFloor-Anrechnung).`;
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
    runFloorEwma10,
    runFloorState,
    specificOk,
    specificValue,
    aerobicOk,
    aerobicFloor,
    aerobicFloorActive,
    fatigue,
    longRunSummary,
    distanceDiagnostics,
    gapRecommendations,
    bikeSubFactor,
    weeksToEvent,
    eventDistance,
  },
  { debug = false, verbosity = "coach" } = {}
) {
  const lines = [];
  const formatPct1 = (value) => (Number.isFinite(value) ? `${value.toFixed(1).replace('.', ',')} %` : "n/a");
  const formatSignedPct1 = (value) =>
    Number.isFinite(value)
      ? `${value >= 0 ? "+" : ""}${value.toFixed(1).replace('.', ',')} %`
      : "n/a";
  const addDecisionBlock = (title, metrics = []) => {
    const titleEmojis = {
      "COACH-ENTSCHEIDUNG": "🎯",
      "KURZBEGRÜNDUNG": "🧩",
      "DIAGNOSE ERKLÄRT": "🧠",
      "DEBUG / NERD": "🛠️",
      "HEUTIGER LAUF": "🏃",
      "BELASTUNG & PROGRESSION": "📈",
      "KEY-CHECK": "🔑",
      "DIAGNOSE": "🧠",
      "EMPFEHLUNGEN": "🧭",
      "TRAININGSSTAND": "📊",
      "HEUTE-ENTSCHEIDUNG": "🎯",
      "BOTTOM LINE": "🧾",
      "HEUTE": "🏃",
      "WARUM": "🧩",
      "STATUS": "🧠",
      "DIAGNOSE": "🧠",
      "FOKUS": "📌",
    };
    lines.push(`${titleEmojis[title] || "✅"} ${title}`);
    for (const metric of metrics) {
      if (metric) lines.push(metric);
    }
    lines.push("⸻");
    lines.push("");
  };

  const keyCap7 = keyCompliance?.maxKeysCap7 ?? dynamicKeyCap?.maxKeys7d ?? MAX_KEYS_7D;
  const actualKeys7 = keyCompliance?.actual7 ?? 0;
  const actualKeys7Raw = keyCompliance?.actual7Raw ?? actualKeys7;
  const longrunSpecificity = keyCompliance?.longrunSpecificity || null;
  const runFloorCurrent = Math.round(Number.isFinite(runFloorEwma10) ? runFloorEwma10 : 0);
  const runTarget = Math.round(runFloorState?.effectiveFloorTarget ?? 0);
  const runCount7 = Number.isFinite(distanceDiagnostics?.snapshot?.runsCount)
    ? Math.round(distanceDiagnostics.snapshot.runsCount)
    : 0;
  const runGoal = eventDistance === "5k" || eventDistance === "10k" ? 3 : 4;
  const runFloorGap = runTarget > 0 ? runFloorCurrent - runTarget : 0;
  const lifeEvent = runFloorState?.lifeEvent || null;
  const ignoreRunFloorGap = lifeEvent?.ignoreRunFloorGap === true;
  const intensityDistribution = keyCompliance?.intensityDistribution;
  const easySharePct = intensityDistribution?.hasData ? Math.round((intensityDistribution.easyShare ?? 0) * 100) : null;
  const midSharePct = intensityDistribution?.hasData ? Math.round((intensityDistribution.midShare ?? 0) * 100) : null;
  const hardSharePct = intensityDistribution?.hasData ? Math.round((intensityDistribution.hardShare ?? 0) * 100) : null;
  const easyMinPct = Math.round((intensityDistribution?.targets?.easyMin ?? 0) * 100);
  const midMaxPct = Math.round((intensityDistribution?.targets?.midMax ?? 0) * 100);
  const hardMaxPct = Math.round((intensityDistribution?.targets?.hardMax ?? 0) * 100);
  const intensityLookbackDays = Math.max(1, Math.round(intensityDistribution?.lookbackDays ?? INTENSITY_LOOKBACK_DAYS));
  const spacingOk = keyCompliance?.keySpacingOk ?? keySpacing?.ok ?? true;
  const nextAllowed = keyCompliance?.nextKeyEarliest ?? keySpacing?.nextAllowedIso ?? null;
  const overlayMode = runFloorState?.overlayMode ?? "NORMAL";
  const strengthPolicy = robustness?.strengthPolicy || evaluateStrengthPolicy(robustness?.strengthMinutes7d || 0);
  const strengthPlan = getStrengthPhasePlan(blockState?.block);

  const eventDate = String(modeInfo?.nextEvent?.start_date_local || modeInfo?.nextEvent?.start_date || "").slice(0, 10);

  const keyBlocked = keyCompliance?.keyAllowedNow === false || overlayMode === "DELOAD" || overlayMode === "TAPER" || overlayMode === "POST_RACE_RAMP" || overlayMode === "LIFE_EVENT_STOP";
  const budgetBlocked = keyCompliance?.capExceeded === true;
  const spacingBlocked = !spacingOk;
  const easyShareBlocked = intensityDistribution?.hasData && intensityDistribution?.easyUnder === true;
  const hardShareBlocked = intensityDistribution?.hasData && intensityDistribution?.hardOver === true;
  const deloadBlocked = overlayMode === "DELOAD" || overlayMode === "TAPER" || overlayMode === "POST_RACE_RAMP" || overlayMode === "LIFE_EVENT_STOP";
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
        : overlayMode === "POST_RACE_RAMP"
          ? "Post-Race Ramp"
          : overlayMode === "LIFE_EVENT_STOP"
            ? "LifeEvent Freeze"
            : overlayMode === "LIFE_EVENT_HOLIDAY"
              ? "Holiday"
          : keyBlocked
            ? "Easy only"
            : "Key möglich";
  const ampel = keyBlocked ? "🟠" : "🟢";
  const missingKeyFrequency = keyCompliance?.freqOk === false;
  const regressionSignal =
    runFloorBlocked ||
    missingKeyFrequency ||
    keyCompliance?.status === "warn" ||
    keyCompliance?.preferredMissing === true;
  const progressionStatus = lifeEvent?.freezeProgression
    ? "LifeEvent-Freeze"
    : runFloorState?.deloadActive
      ? "Deload aktiv"
      : regressionSignal
        ? "Teilweise im Plan – Basis ist da, jetzt Reiz/Frequenz stabilisieren"
        : "Ja – im Plan";
  const progressionExplanation = lifeEvent?.freezeProgression
    ? "LifeEvent erkannt: Progression ist bewusst pausiert und wird nach dem Event wieder aufgebaut"
    : runFloorState?.deloadActive
      ? "Deload ist aktiv: Die reduzierte Belastung dient der Erholung und verbessert den nächsten Aufbaublock"
      : regressionSignal
        ? "Erklärung: Aktuell fehlen noch konstante Key-Reize bzw. regelmäßige Frequenz – mit 1 Key/Woche + lockeren Läufen wird der Trend schnell wieder positiv"
        : "Erklärung: Reiz, Frequenz und Belastungsrahmen passen aktuell gut zusammen";
  const keyRuleLine = buildKeyRuleLine({
    keyRules,
    block: blockState?.block,
    eventDistance: formatEventDistance(modeInfo?.nextEvent?.distance_type),
  });
  const keyPatternLine = buildKeyPatternDistributionLine({
    block: blockState?.block,
    eventDistance: modeInfo?.nextEvent?.distance_type,
    plannedType: keyRules?.plannedPrimaryType,
    weeksToEvent,
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
    nextAllowed,
    todayIso,
    keyMinGapDays: keyCompliance?.keyMinGapDays ?? keySpacing?.minGapDays ?? KEY_MIN_GAP_DAYS_DEFAULT,
  });
  const transitionLine = buildTransitionLine({ bikeSubFactor, weeksToEvent, eventDistance });
  const bikeAllowanceLine = buildBikeAllowanceLine({ bikeSubFactor });

  const longRun14d = longRunSummary?.longRun14d || { minutes: 0, date: null };
  const longRun30d = longRunSummary?.longestRun30d || { minutes: 0, date: null, windowDays: LONGRUN_PREPLAN.spikeGuardLookbackDays };
  const longRunPlan = longRunSummary?.plan || computeLongRunTargetMinutes(weeksToEvent, eventDistance || modeInfo?.nextEvent?.distance_type);
  const longRun7d = longRunSummary || { minutes: 0, date: null, quality: "n/a" };
  const longRunDoneMin = Math.round(longRun14d?.minutes ?? 0);
  const longestRun30dMin = Math.round(longRun30d?.minutes ?? 0);
  const prePlanLongRunTargetMin = Math.round(longRunPlan?.plannedMin ?? LONGRUN_PREPLAN.startMin);
  const phaseLongRunMaxMin = Number(PHASE_MAX_MINUTES?.[blockState?.block || "BASE"]?.[eventDistance || "10k"]?.longrun ?? 0);
  const longRunStepCapRawMin = Math.round(longRunDoneMin * (1 + LONGRUN_PREPLAN.maxStepPct));
  const longRunSpikeCapMin = longestRun30dMin > 0 ? Math.round(longestRun30dMin * (1 + LONGRUN_PREPLAN.maxStepPct)) : 0;
  const longRunStepCapMin = phaseLongRunMaxMin > 0
    ? Math.min(longRunStepCapRawMin, phaseLongRunMaxMin)
    : longRunStepCapRawMin;
  const longRunSafetyCapMin = longRunSpikeCapMin > 0
    ? Math.min(longRunStepCapMin, longRunSpikeCapMin)
    : longRunStepCapMin;
  const planStartWeeks = getPlanStartWeeks(eventDistance);
  const inPlanPhase = Number.isFinite(weeksToEvent) && weeksToEvent <= planStartWeeks;
  const longRunTargetPhaseCapMin = inPlanPhase && phaseLongRunMaxMin > 0
    ? Math.min(prePlanLongRunTargetMin, phaseLongRunMaxMin)
    : prePlanLongRunTargetMin;
  const longRunTargetMin = longRunSafetyCapMin > 0
    ? Math.min(longRunTargetPhaseCapMin, longRunSafetyCapMin)
    : longRunTargetPhaseCapMin;
  const longRunGapMin = longRunDoneMin - longRunTargetMin;
  const blockLongRunNextWeekTargetMin = longRunDoneMin > 0
    ? longRunSafetyCapMin
    : LONGRUN_PREPLAN.startMin;

  const runMetrics = [];
  if (!perRunInfo?.length) {
    runMetrics.push("Status: Heute kein Lauf.");
  } else {
    const intervalToday = perRunInfo.find((x) => x.intervalSignal && (x.intervalMetrics || x.intervalStructureHint || x.paceConsistencyHint));
    const tdlToday = perRunInfo.find((x) => isTempoDauerlaufKey(x.activity));
    const gaToday = perRunInfo.find((x) => x.ga && !x.isKey && !x.intervalSignal && !isTempoDauerlaufKey(x.activity));
    if (intervalToday) {
      const sessionQuality = ensureStructuredSessionReview(intervalToday.activity, getKeyType(intervalToday.activity));
      if (sessionQuality?.lines?.length) {
        runMetrics.push(...sessionQuality.lines);
      } else if (intervalToday?.intervalMetrics) {
        const driftBpm = Number(intervalToday.intervalMetrics.HR_Drift_bpm);
        const driftLabel = Number.isFinite(driftBpm)
          ? driftBpm <= 3
            ? "stabil"
            : driftBpm <= 7
              ? "leicht ansteigend"
              : "deutlich ansteigend"
          : "n/a";
        const hrr60 = Number(intervalToday.intervalMetrics.HRR60_median);
        const hrrLabel = Number.isFinite(hrr60)
          ? hrr60 >= 20
            ? "gut"
            : hrr60 >= 15
              ? "ok"
              : "ausbaufähig"
          : "n/a";

        runMetrics.push(`Intervall-Bewertung: via Stream-Analyse (Fallback), trotz Geh-/Stehpausen auswertbar.`);
        runMetrics.push(`Intervall-Drift: ${Number.isFinite(driftBpm) ? `${fmtSigned1(driftBpm)} bpm` : "n/a"} (${driftLabel}).`);
        runMetrics.push(`Erholung zwischen Reps (HRR60): ${Number.isFinite(hrr60) ? `${hrr60.toFixed(0)} bpm` : "n/a"} (${hrrLabel}).`);
      } else {
        const paceConsistency = intervalToday?.paceConsistencyHint?.label || "n/a";
        const qualityReason = getIntervalDataQualityReason(intervalToday.activity, intervalToday?.intervalMetrics);
        runMetrics.push(`Intervall-Bewertung: Datenqualität begrenzt${qualityReason ? ` (${qualityReason})` : ""}.`);
        runMetrics.push(`Pace-Konsistenz: ${paceConsistency}.`);
        runMetrics.push("Nächster Hebel: Zielpace im Workouttext angeben und Reps mit konsistenter Struktur laufen.");
      }
    } else if (tdlToday) {
      const drift = tdlToday.drift;
      const driftText = formatPct1(drift);
      const driftEval =
        drift == null
          ? "keine belastbare Einordnung (zu wenig Daten/zu kurzer Abschnitt)."
          : drift <= 5
            ? "kontrolliert für TDL (≤ 5 %)."
            : "erhöht für TDL (> 5 %): eher zu hart oder kumulierte Ermüdung.";
      const paceConsistency = tdlToday?.paceConsistencyHint?.label || "n/a";

      runMetrics.push(`TDL-Einschätzung: Drift ${driftText} → ${driftEval}`);
      runMetrics.push(`TDL-Pace-Konsistenz: ${paceConsistency}.`);
      runMetrics.push("TDL-Hinweis: Bewertung primär als durchgehender Lauf (Drift/Ökonomie), Intervallwerte nur ergänzend.");
    } else if (gaToday) {
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
        if (overlayMode === "POST_RACE_RAMP") {
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
    } else {
      runMetrics.push("Status: Lauf vorhanden, aber kein GA- oder Intervallsignal mit ausreichender Datenqualität.");
    }

    const runFloorDeltaLabel = runTarget > 0
      ? `${runFloorGap >= 0 ? "+" : ""}${runFloorGap}`
      : "n/a";
    runMetrics.push(
      `RunFloor (neu): ${runFloorCurrent}${runTarget > 0 ? ` / ${runTarget}` : ""} (Δ ${runFloorDeltaLabel})`
    );
  }
  const todayRunMetricsBlock = runMetrics;

  const progressionMetricsBlock = [
    `Longrun (14T): ${longRunDoneMin}′ → Ziel: ${longRunTargetMin}′`,
    `Longrun-Spike-Index: ${longestRun30dMin > 0 ? (Math.max(0, Math.round((longRunDoneMin / longestRun30dMin) * 100)) / 100).toFixed(2) : "n/a"} (14T vs. max ${longRun30d?.windowDays ?? 30}T; Guard <= 1.10)`,
    `Qualität: ${longRun7d?.quality || "n/a"}${longRun7d?.date ? ` (${longRun7d.date})` : ""}`,
    `RunFloor (14T EWMA): ${runFloorCurrent} / ${runTarget > 0 ? runTarget : "n/a"}`,
    `Run-Distanz 14T (Urlaub bereinigt): ${Number.isFinite(fatigue?.runDistLast14AdjKm) ? fatigue.runDistLast14AdjKm.toFixed(1) : "n/a"} km (raw ${Number.isFinite(fatigue?.runDistLast14Km) ? fatigue.runDistLast14Km.toFixed(1) : "n/a"}, Urlaub ${Number.isFinite(fatigue?.runDistLast14HolidayDays) ? fatigue.runDistLast14HolidayDays : 0}d) | Vorperiode: ${Number.isFinite(fatigue?.runDistPrev14AdjKm) ? fatigue.runDistPrev14AdjKm.toFixed(1) : "n/a"} km (raw ${Number.isFinite(fatigue?.runDistPrev14Km) ? fatigue.runDistPrev14Km.toFixed(1) : "n/a"}, Urlaub ${Number.isFinite(fatigue?.runDistPrev14HolidayDays) ? fatigue.runDistPrev14HolidayDays : 0}d) | Ratio: ${Number.isFinite(fatigue?.runDist14dRatio) ? fatigue.runDist14dRatio.toFixed(2) : "n/a"} (<= ${RUN_DISTANCE_14D_LIMIT.toFixed(2)})`,
    `21-Tage Progression: ${Math.round(runFloorState?.sum21 ?? 0)} / ${Math.round(runFloorState?.baseSum21Target ?? 0) || 450}`,
    `Aktive Tage (21T): ${Math.round(runFloorState?.activeDays21 ?? 0)} / ${Math.round(runFloorState?.baseActiveDays21Target ?? 0) || 14}`,
    `Stabilität: ${runFloorState?.deloadActive ? "kritisch" : "im Aufbau"} (${runFloorState?.deloadActive ? "Erholung priorisieren" : "Kontinuität aufbauen"})`,
    `Status: ${progressionStatus}. ${progressionExplanation}.`,
  ];

  const keyUsageText = Number.isFinite(actualKeys7Raw) && Math.abs(actualKeys7 - actualKeys7Raw) > 0.01
    ? `${actualKeys7.toFixed(1)} (inkl. Longrun ${actualKeys7Raw.toFixed(0)} + ${(actualKeys7 - actualKeys7Raw).toFixed(1)})`
    : `${Math.round(actualKeys7)}`;
  const keyCheckMetrics = [
    `Keys (7 Tage): ${keyUsageText}/${keyCap7}${budgetBlocked ? " ⚠️" : ""}`,
    `Next Allowed: ${formatNextAllowed(todayIso, nextAllowed)}`,
    fatigue?.override
      ? `Fatigue-Override: aktiv ⚠️ (${(fatigue.reasons || []).slice(0, 2).join(" | ")}${(fatigue.reasons || []).length > 2 ? " …" : ""})`
      : "Fatigue-Override: aus",
    `Kraft 7T: ${strengthPolicy.minutes7d}′ (Runfloor ≥${strengthPolicy.minRunfloor}′ | Ziel ${strengthPolicy.target}′ | Max ${strengthPolicy.max}′)`,
    `Kraft-Score: ${strengthPolicy.score}/3 | Confidence Δ ${strengthPolicy.confidenceDelta >= 0 ? "+" : ""}${strengthPolicy.confidenceDelta}`,
  ];
  const hasEventDistance = formatEventDistance(modeInfo?.nextEvent?.distance_type) !== "n/a";
  if (keyRuleLine && hasEventDistance) keyCheckMetrics.push(keyRuleLine);
  if (keyPatternLine && hasEventDistance) keyCheckMetrics.push(keyPatternLine);
  if (longrunSpecificity?.active) keyCheckMetrics.push(`Longrun-Spezifik: ${longrunSpecificity.notes}`);
  if (bikeAllowanceLine) keyCheckMetrics.push(bikeAllowanceLine);
  if (transitionLine) keyCheckMetrics.push(transitionLine);

  const explicitSessionShort = shortExplicitSession(
    keyCompliance?.explicitSession || explicitSessionFromSuggestion(keyCompliance?.suggestion)
  );
  const keyAllowedNow = keyCompliance?.keyAllowedNow === true && !keyBlocked;
  const pendingLever = keyCompliance?.pendingLever || keyCompliance?.activeLever || null;
  const pendingLeverPlan = !keyAllowedNow && pendingLever?.domain
    ? formatPendingLeverPlan({
        pendingLever,
        nextKeyEarliest: nextAllowed,
        plannedKeyType: keyCompliance?.plannedKeyType,
        explicitSession: keyCompliance?.explicitSession,
      })
    : { pendingLeverLine: null, pendingLeverPlanLine: null };
  const pendingLeverLine = pendingLeverPlan.pendingLeverLine || keyCompliance?.pendingLeverLine || null;
  const pendingLeverPlanLine = pendingLeverPlan.pendingLeverPlanLine || keyCompliance?.pendingLeverPlanLine || null;
  const normalizedVerbosity = REPORT_VERBOSITY_VALUES.has(verbosity) ? verbosity : "coach";
  const todayDecision = nextRunText.replace(/ Optional:.*$/i, "").replace(/\.$/, "");
  const resolvedDecision = buildResolvedDecision({
    todayDecision,
    todayIso,
    spacingBlocked,
    nextAllowed,
    keySpacingOk: spacingOk,
    keyMinGapDays: keyCompliance?.keyMinGapDays ?? keySpacing?.minGapDays ?? KEY_MIN_GAP_DAYS_DEFAULT,
    readinessScore: distanceDiagnostics?.readiness,
    mainLimiter: mapGapToCoachLanguage(distanceDiagnostics?.primaryGap).label || "n/a",
  });
  const resolvedNextKeyLine = buildResolvedNextKeyLine(resolvedDecision);
  const decisionCompact = buildRecommendationsAndBottomLine({
    runFloorEwma10,
    runFloorTarget: runTarget > 0 ? runTarget : null,
    intensityDistribution: keyCompliance?.intensityDistribution,
    budgetBlocked,
    spacingBlocked,
    nextAllowed,
    keyMinGapDays: keyCompliance?.keyMinGapDays ?? keySpacing?.minGapDays ?? KEY_MIN_GAP_DAYS_DEFAULT,
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
    longRunStepCapMin: longRunSafetyCapMin,
    longRunDiagnosisTargetMin: prePlanLongRunTargetMin,
    longRunSpikeCapMin,
    longRunSpikeWindowDays: Number(longRun30d?.windowDays ?? LONGRUN_PREPLAN.spikeGuardLookbackDays),
    blockLongRunNextWeekTargetMin,
    fatigue,
    distanceDiagnostics,
    gapRecommendations,
  });
  const recommendationMetricsBlockRaw = [
    ...decisionCompact.recommendations,
    `Kraft-Integration: 2×/Woche, nach GA1≤60′ oder Strides; kein Kraftblock vor Longrun / <24h vor Key.`,
  ];
  const recommendationMetricsBlock = recommendationMetricsBlockRaw.filter((line) => {
    if (!resolvedDecision?.nextKeyEarliestDate) return true;
    return !isLegacyNextKeyText(line);
  });
  if (!keyAllowedNow && pendingLeverLine) {
    recommendationMetricsBlock.push(pendingLeverLine);
  }
  if (!keyAllowedNow && pendingLeverPlanLine) {
    recommendationMetricsBlock.push(pendingLeverPlanLine);
  }

  const coachFocus = buildCoachFocusSummary(distanceDiagnostics?.primaryGap, distanceDiagnostics?.secondaryGap);
  const focusLabel = coachFocus.label;
  const fatigueReasonSnippet = Array.isArray(fatigue?.reasons) && fatigue.reasons.length
    ? ` (${fatigue.reasons.slice(0, 2).join(" | ")}${fatigue.reasons.length > 2 ? " …" : ""})`
    : "";
  const fatigueWhyLine = fatigue?.override ? `Rhythmus aktuell unruhig${fatigueReasonSnippet}.` : null;
  const gapReasonMap = {
    base: [
      !ignoreRunFloorGap && runFloorGap < 0 ? `RunFloor unter Ziel (${runFloorCurrent}/${runTarget})` : null,
      intensityDistribution?.easyUnder ? `Easy-Anteil unter Ziel (${easySharePct}% < ${easyMinPct}%)` : null,
      "Volumen noch nicht stabil.",
    ],
    specificity: [
      keyCompliance?.preferredMissing ? "Distanzspezifische Reize noch nicht breit genug." : null,
      keyCompliance?.freqOk === false ? "Passender Reiz fehlt oder ist noch zu selten." : null,
      "Nächsten Reiz gezielter setzen.",
    ],
    longrun: [
      longRunDoneMin < longRunTargetMin ? `Longrun zuletzt kürzer (${longRunDoneMin}′/${longRunTargetMin}′)` : null,
      Number(distanceDiagnostics?.snapshot?.longrunFrequency35d ?? 0) < 2 ? "Longrun-Frequenz zu niedrig." : null,
      "Längerer aerober Reiz nicht regelmäßig genug.",
    ],
    robustness: [
      Number(strengthPolicy.minutes7d || 0) < Number(strengthPolicy.target || 0)
        ? `Krafttraining unter Soll (${strengthPolicy.minutes7d}′/${strengthPolicy.target}′)`
        : null,
      "Belastbarkeit noch nicht stabil genug.",
    ],
    execution: [
      spacingBlocked ? `Key-Abstand noch nicht erfüllt (ab ${nextAllowed || "n/a"})` : null,
      budgetBlocked ? `Key-Budget erreicht (${actualKeys7}/${keyCap7} in 7 Tagen)` : null,
      (spacingBlocked || budgetBlocked || keyCompliance?.freqOk === false || keyCompliance?.typeOk === false)
        ? "Wochenstruktur aktuell nicht sauber genug."
        : null,
      fatigueWhyLine,
    ],
  };

  const rankedGaps = [distanceDiagnostics?.primaryGap, distanceDiagnostics?.secondaryGap].filter(Boolean);
  const shortReasons = [];
  for (const gap of rankedGaps) {
    for (const reason of gapReasonMap[gap] || []) {
      if (!reason) continue;
      if (!shortReasons.includes(reason)) shortReasons.push(reason);
      if (shortReasons.length >= 3) break;
    }
    if (shortReasons.length >= 3) break;
  }
  if (shortReasons.length < 3 && Number(strengthPolicy.minutes7d || 0) < Number(strengthPolicy.target || 0)) {
    const strengthReason = `Krafttraining unter Soll (${strengthPolicy.minutes7d}′/${strengthPolicy.target}′)`;
    if (!shortReasons.includes(strengthReason)) shortReasons.push(strengthReason);
  }

  const focusLines = [coachFocus.action || "Wochenstruktur stabilisieren."];
  if (Number(strengthPolicy.minutes7d || 0) < Number(strengthPolicy.target || 0) && !focusLines.includes("Kraft zurückbringen.")) {
    focusLines.push("Kraft zurückbringen.");
  }

  const renderedTopics = new Set();
  renderedTopics.lines = [];

  const whyLines = shortReasons.length
    ? shortReasons.map((reason) => `• ${reason}`).slice(0, 4)
    : ["• Keine harten Restriktionen aktiv."];

  const statusLines = [
    `Readiness (overall): ${resolvedDecision.readinessScore ?? "n/a"}/100`,
    `Hauptlimit: ${resolvedDecision.mainLimiter}`,
  ];

  const intensityWindowLabel = `${intensityLookbackDays}T`;
  const intensityLine = intensityDistribution?.hasData
    ? `Intensitätsverteilung (${intensityWindowLabel}, Block): Easy ${easySharePct}% | Mid ${midSharePct}% | Hard ${hardSharePct}% (Ziel ≥${easyMinPct}% / ≤${midMaxPct}% / ≤${hardMaxPct}%)`
    : `Intensitätsverteilung (${intensityWindowLabel}, Block): n/a (noch keine Laufdaten)`;

  const trainingStateLines = [
    runTarget > 0 && runFloorCurrent < runTarget
      ? `RunFloor: ${runFloorCurrent} / ${runTarget}`
      : runTarget > 0
        ? runFloorState?.stabilityOK === false
          ? `RunFloor im Zielkorridor (${runFloorCurrent} / ${runTarget}), Stabilität noch nicht bestätigt`
          : `RunFloor im Zielkorridor (${runFloorCurrent} / ${runTarget})`
        : `RunFloor: ${runFloorCurrent} / n/a`,
    `Longrun 14T: ${longRunDoneMin}′ → Blockziel ${longRunTargetMin}′`,
    `Kraft 7T: ${strengthPolicy.minutes7d}′ / Ziel ${strengthPolicy.target}′`,
    intensityLine,
  ];

  const recommendationLines = [];
  for (const rec of recommendationMetricsBlock) {
    const text = String(rec || "").trim();
    if (!text) continue;
    const lower = text.toLowerCase();
    if (
      lower.includes("readiness")
      || lower.includes("hauptlimit")
      || lower.includes("nächster key frühestens")
      || lower.includes("heute kein")
      || lower.includes("heute:")
      || lower.includes("hebel vorgemerkt")
      || lower.includes("geplante anpassung")
      || lower.includes("fokus:")
      || (resolvedDecision?.nextKeyEarliestDate && isLegacyNextKeyText(lower))
    ) continue;
    recommendationLines.push(text);
    if (recommendationLines.length >= 4) break;
  }

  const diagnoseLines = [];
  for (const name of ["base", "specificity", "longrun", "robustness", "execution"]) {
    const component = distanceDiagnostics?.components?.[name];
    if (!component) continue;
    diagnoseLines.push(`${name[0].toUpperCase()}${name.slice(1)}: ${component.score} → ${component.interpretation}`);
  }

  addUniqueTopicLine(renderedTopics, "today", resolvedDecision.todayDecision);
  if (resolvedDecision.mainLimiter) addUniqueTopicLine(renderedTopics, "main_limiter", resolvedDecision.mainLimiter);
  if (focusLabel) addUniqueTopicLine(renderedTopics, "focus_cue", focusLabel);
  if (!keyAllowedNow && pendingLeverPlanLine) addUniqueTopicLine(renderedTopics, "lever_plan", pendingLeverPlanLine);

  const focusRenderLines = [];
  if (focusLabel) focusRenderLines.push(`Fokus: ${focusLabel}.`);
  if (renderedTopics.has("lever_plan") && pendingLeverPlanLine) focusRenderLines.push(pendingLeverPlanLine);
  if (!focusRenderLines.length) focusRenderLines.push(focusLines[0] || "Wochenstruktur stabilisieren.");

  const recommendationRenderLines = recommendationLines.slice(0, 4);
  if (!recommendationRenderLines.length) {
    recommendationRenderLines.push("Belastung heute kontrolliert halten und nächste Woche wieder progressiv aufbauen.");
  }

  addDecisionBlock("HEUTIGER LAUF", todayRunMetricsBlock);
  addDecisionBlock("HEUTE", [resolvedDecision.todayDecision]);
  addDecisionBlock("WARUM", whyLines);
  addDecisionBlock("STATUS", statusLines);
  addDecisionBlock("FOKUS", focusRenderLines.slice(0, 3));
  addDecisionBlock("TRAININGSSTAND", trainingStateLines);
  addDecisionBlock("EMPFEHLUNGEN", recommendationRenderLines);
  addDecisionBlock("DIAGNOSE", diagnoseLines);
  const bottomLine = resolveBottomLine({
    candidate: capLines(decisionCompact.bottomLine, 1)[0],
    todayDecision: resolvedDecision.todayDecision,
  });
  addDecisionBlock("BOTTOM LINE", [bottomLine]);

  if (normalizedVerbosity !== "debug") {
    return lines.join("\n");
  }

  const diagnoseKernelLines = [
    "DIAGNOSE-KERN",
    `Readiness: ${distanceDiagnostics?.readiness ?? "n/a"}`,
    `Stärken: ${(distanceDiagnostics?.strengths || []).slice(0, 2).join(", ") || "n/a"}`,
    `Limitierend: ${distanceDiagnostics?.primaryGap || "n/a"}${distanceDiagnostics?.secondaryGap ? `, ${distanceDiagnostics.secondaryGap}` : ""}`,
    `Scores: Base ${distanceDiagnostics?.scores?.base ?? "n/a"} | Specificity ${distanceDiagnostics?.scores?.specificity ?? "n/a"} | Longrun ${distanceDiagnostics?.scores?.longrun ?? "n/a"} | Robustness ${distanceDiagnostics?.scores?.robustness ?? "n/a"} | Execution ${distanceDiagnostics?.scores?.execution ?? "n/a"}`,
    "",
  ];
  const scoreExplanationLines = [
    "SCORE-ERKLÄRUNG",
    `Base · RunFloor ${runFloorCurrent}/${runTarget} | Läufe/Woche ${runCount7}/${runGoal} | Easy ${Math.round((intensityDistribution?.easyShare || 0) * 100)}% → ${(distanceDiagnostics?.components?.base?.interpretation || "n/a")}`,
    `Specificity · Keytyp ${formatKeyType(keyRules?.plannedPrimaryType || "steady")} | Fokusabdeckung ${keyCompliance?.focusHits ?? 0}/${keyCompliance?.focusTarget ?? 0} | Block ${blockState?.block || "n/a"} | Wettkampfnähe ${Number.isFinite(keyCompliance?.racepaceBlockProgress?.pct) ? `${keyCompliance.racepaceBlockProgress.pct}%` : "n/a"} → ${(distanceDiagnostics?.components?.specificity?.interpretation || "n/a")}`,
    `Longrun · 14T ${longRunDoneMin}′ | Diagnose-Ziel ${prePlanLongRunTargetMin}′ | Progressionsschritt ${longRunSafetyCapMin}′ | Blockziel ${blockLongRunNextWeekTargetMin}′ → ${(distanceDiagnostics?.components?.longrun?.interpretation || "n/a")}`,
    `Robustness · Kraft 7T ${strengthPolicy.minutes7d}′ | Coach-Ziel ${strengthPolicy.target}′ | Score-Anker 45′ → ${(distanceDiagnostics?.components?.robustness?.interpretation || "n/a")}`,
    `Execution · Key-Frequenz ${actualKeys7Raw}/${keyCap7} | Spacing ${spacingOk ? "ok" : "nicht ok"} | Fatigue-Bremse ${fatigue?.override ? "ja" : "nein"} → ${(distanceDiagnostics?.components?.execution?.interpretation || "n/a")}`,
    "",
  ];
  const loadSignalsLines = [
    "BELASTUNG / SIGNALS",
    `Ramp: ${fmtSigned1(trend?.dv || 0)}%`,
    `ACWR: ${Number(fatigue?.acwr || 0).toFixed(2)}`,
    `Monotony: ${Number(fatigue?.monotony || 0).toFixed(2)}`,
    `RunFloor: ${runFloorCurrent}/${runTarget}`,
    `14T Distanz-Ratio: ${Number(fatigue?.runDist14dRatio || 0).toFixed(2)}`,
    `Longrun 14T: ${longRunDoneMin}′`,
    "",
  ];
  const rulesCapsLines = [
    "REGELN / CAPs",
    `Keys 7T: ${actualKeys7}/${keyCap7}`,
    `Fatigue Override: ${fatigue?.override ? "aktiv" : "aus"}`,
    `Next Allowed: ${formatNextAllowed(todayIso, nextAllowed)}`,
    `Block: ${blockState?.block || "n/a"}`,
    `Bike-Faktor: ${Number(bikeSubFactor || 0).toFixed(2)}`,
    "",
  ];
  const coachConsequencesLines = [
    "COACH-FOLGEN",
    `• ${keyBlocked ? "Kein weiterer Key diese Woche" : "Key möglich, falls frisch"}`,
    "• Volumen priorisieren",
    `• Kraft auf ${Number(strengthPolicy.target || 60) >= 60 ? "1–2 kurze Einheiten" : "mind. 1 kurze Einheit"} stabilisieren`,
    "• Load-Anstieg konservativ halten",
  ];

  addDecisionBlock("DEBUG / NERD", [
    ...diagnoseKernelLines,
    ...scoreExplanationLines,
    ...loadSignalsLines,
    ...rulesCapsLines,
    ...coachConsequencesLines,
  ]);

  return lines.join("\n");
}

function formatNextAllowed(dayIso, nextAllowedIso) {
  if (!nextAllowedIso) return "n/a";
  if (!dayIso || !isIsoDate(dayIso) || !isIsoDate(nextAllowedIso)) return nextAllowedIso;
  const delta = diffDays(dayIso, nextAllowedIso);
  if (delta === 0) return `${nextAllowedIso} (ab heute, sofern Key-Budget frei)`;
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


function buildWeekDocKey(uid, weekId) {
  return `${WEEKDOC_KV_PREFIX}${uid}${WEEKDOC_KEY_PREFIX}${weekId}`;
}

function buildWeekIndexKey(uid) {
  return `${WEEKDOC_KV_PREFIX}${uid}${WEEKDOC_INDEX_SUFFIX}`;
}

function hasAnyPatternOutputData(weekDoc) {
  const output = weekDoc?.output || {};
  return [output.vdot_delta, output.ef_delta_pct, output.drift_delta].some((v) => Number.isFinite(v));
}

function asMedianOutput(weeks, field) {
  return median((weeks || []).map((w) => Number(w?.output?.[field])));
}

function asMedianInput(weeks, field) {
  return median((weeks || []).map((w) => Number(w?.input?.[field])));
}

function patternConfidence(groupA, groupB, diffAbs, threshold) {
  const nA = Number(groupA?.length || 0);
  const nB = Number(groupB?.length || 0);
  if (nA >= 4 && nB >= 4 && Number.isFinite(diffAbs) && diffAbs >= threshold) return "high";
  if (nA >= 3 && nB >= 3) return "medium";
  return "low";
}

async function upsertWeekDocAndIndex(env, mondayIso, warmupSkipSec, context = {}) {
  if (!hasKv(env)) return null;

  const uid = mustEnv(env, "ATHLETE_ID");
  const weekEndDate = new Date(mondayIso + "T00:00:00Z");
  const prevMondayDate = new Date(weekEndDate.getTime() - 7 * 86400000);
  const startIso = isoDate(prevMondayDate);
  const endExclusiveIso = mondayIso;
  const weekInfo = getIsoWeekInfo(startIso);
  if (!weekInfo?.weekId) return null;

  const allActivities = Array.isArray(context?.activitiesAll)
    ? context.activitiesAll
    : await fetchIntervalsActivities(env, startIso, isoDate(new Date(weekEndDate.getTime() - 86400000)));

  const runs = (allActivities || []).filter((a) => {
    const d = String(a?.start_date_local || a?.start_date || "").slice(0, 10);
    return d && d >= startIso && d < endExclusiveIso && isRun(a);
  });

  const keyRuns = runs.filter((a) => hasKeyTag(a));
  const keyTypes = uniq(keyRuns.map((a) => normalizeKeyType(getKeyType(a) || "key")).filter(Boolean));

  const minutesTotal = sum(runs.map((a) => (Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0) / 60));
  const loadTotal = sum(runs.map((a) => extractLoad(a)));
  const longrunsCount = runs.filter((a) => (Number(a?.moving_time ?? a?.elapsed_time ?? 0) || 0) >= 60 * 60).length;

  const eventDistance = normalizeEventDistance(context?.eventDistance) || "10k";
  let easyMinutes = 0;
  let midMinutes = 0;
  let hardMinutes = 0;
  for (const run of runs) {
    const minutes = (Number(run?.moving_time ?? run?.elapsed_time ?? 0) || 0) / 60;
    if (!(minutes > 0)) continue;
    const cat = classifyIntensityCategory(run, eventDistance);
    if (cat === "hard") hardMinutes += minutes;
    else if (cat === "mid") midMinutes += minutes;
    else easyMinutes += minutes;
  }
  const totalMinutes = easyMinutes + midMinutes + hardMinutes;

  const comp = await gatherComparableGASamples(env, endExclusiveIso, warmupSkipSec, 7);

  const weekDoc = {
    weekId: weekInfo.weekId,
    weekStart: startIso,
    weekEndExclusive: endExclusiveIso,
    input: {
      load_total: round(loadTotal, 1),
      runs: runs.length,
      minutes_total: round(minutesTotal, 1),
      keys_count: keyRuns.length,
      key_types: keyTypes,
      longruns_count: longrunsCount,
      easy_share: totalMinutes > 0 ? round(easyMinutes / totalMinutes, 3) : null,
      mid_share: totalMinutes > 0 ? round(midMinutes / totalMinutes, 3) : null,
      hard_share: totalMinutes > 0 ? round(hardMinutes / totalMinutes, 3) : null,
    },
    output: {
      vdot_level: comp?.efMed != null ? round(vdotLikeFromEf(comp.efMed), 1) : null,
      vdot_delta: null,
      ef_level: comp?.efMed != null ? round(comp.efMed, 5) : null,
      ef_delta_pct: null,
      drift_level: comp?.driftMed != null ? round(comp.driftMed, 2) : null,
      drift_delta: null,
    },
    wellness: null,
    quality: {
      ga_comparable_n: Number(comp?.n || 0),
      drift_has_data: Number.isFinite(comp?.driftMed),
      ef_has_data: Number.isFinite(comp?.efMed),
      vdot_has_data: Number.isFinite(comp?.efMed),
    },
  };

  const indexKey = buildWeekIndexKey(uid);
  const existingIdx = (await readKvJson(env, indexKey)) || [];
  const cleanedIdx = existingIdx.filter((w) => typeof w === "string");
  const prevWeekId = cleanedIdx.find((id) => id !== weekInfo.weekId) || null;
  const prevWeekDoc = prevWeekId ? await readKvJson(env, buildWeekDocKey(uid, prevWeekId)) : null;

  if (prevWeekDoc?.output) {
    const prevOutput = prevWeekDoc.output;
    if (Number.isFinite(weekDoc.output.vdot_level) && Number.isFinite(prevOutput.vdot_level) && prevOutput.vdot_level !== 0) {
      weekDoc.output.vdot_delta = round(weekDoc.output.vdot_level - prevOutput.vdot_level, 2);
    }
    if (Number.isFinite(weekDoc.output.ef_level) && Number.isFinite(prevOutput.ef_level) && prevOutput.ef_level !== 0) {
      weekDoc.output.ef_delta_pct = round(((weekDoc.output.ef_level - prevOutput.ef_level) / prevOutput.ef_level) * 100, 2);
    }
    if (Number.isFinite(weekDoc.output.drift_level) && Number.isFinite(prevOutput.drift_level)) {
      weekDoc.output.drift_delta = round(weekDoc.output.drift_level - prevOutput.drift_level, 2);
    }
  }

  const nextIndex = [weekInfo.weekId, ...cleanedIdx.filter((id) => id !== weekInfo.weekId)].slice(0, WEEKDOC_INDEX_LIMIT);
  await writeKvJson(env, buildWeekDocKey(uid, weekInfo.weekId), weekDoc);
  await writeKvJson(env, indexKey, nextIndex);
  return weekDoc;
}

async function loadWeekDocsForPattern(env) {
  if (!hasKv(env)) return [];
  const uid = mustEnv(env, "ATHLETE_ID");
  const idx = (await readKvJson(env, buildWeekIndexKey(uid))) || [];
  const last8 = idx.filter((w) => typeof w === "string").slice(0, PATTERN_WINDOW_WEEKS);
  const docs = [];
  for (const weekId of last8) {
    const doc = await readKvJson(env, buildWeekDocKey(uid, weekId));
    if (doc) docs.push(doc);
  }
  return docs.filter((d) => hasAnyPatternOutputData(d));
}

function buildPatternFinding(title, evidence, action, confidence = "medium") {
  return { title, evidence, action, confidence };
}

function buildPatternAnalysis(weeks) {
  const usableWeeks = (weeks || []).filter((w) => hasAnyPatternOutputData(w));
  const findings = [];
  const correlationFindings = [];

  if (usableWeeks.length < PATTERN_MIN_WEEKS) {
    return {
      header: "🧠 MUSTER-ANALYSE (8W)",
      insufficientData: true,
      findings: [buildPatternFinding("Zu wenig Daten", `Auswertbare Wochen: n=${usableWeeks.length} (mind. ${PATTERN_MIN_WEEKS} nötig).`, "Weiter WeekDocs sammeln und vergleichbare GA-Läufe priorisieren.", "medium")],
      guidance: {
        keysTarget: 1,
        easyShareMin: 0.75,
        loadChangePctCap: 0.1,
        flags: ["insufficient_pattern_data"],
        rationale: ["<4 Wochen mit Output-Daten im 8W-Fenster."],
      },
    };
  }

  const keysLow = usableWeeks.filter((w) => Number(w?.input?.keys_count) <= 1);
  const keysHigh = usableWeeks.filter((w) => Number(w?.input?.keys_count) >= 2);
  if (keysLow.length >= PATTERN_MIN_GROUP_N && keysHigh.length >= PATTERN_MIN_GROUP_N) {
    const efLow = asMedianOutput(keysLow, "ef_delta_pct");
    const efHigh = asMedianOutput(keysHigh, "ef_delta_pct");
    const driftLow = asMedianOutput(keysLow, "drift_delta");
    const driftHigh = asMedianOutput(keysHigh, "drift_delta");
    const vdotLow = asMedianOutput(keysLow, "vdot_delta");
    const vdotHigh = asMedianOutput(keysHigh, "vdot_delta");

    if (Number.isFinite(efHigh) && Number.isFinite(efLow) && Number.isFinite(driftHigh) && Number.isFinite(driftLow) && efHigh > efLow && driftHigh > driftLow) {
      findings.push(
        buildPatternFinding(
          "2 Keys/Woche: EF ↑, Drift ↑",
          `0–1 Key (n=${keysLow.length}): EFΔ=${efLow.toFixed(1)}%, DriftΔ=${driftLow.toFixed(1)} | 2+ Keys (n=${keysHigh.length}): EFΔ=${efHigh.toFixed(1)}%, DriftΔ=${driftHigh.toFixed(1)}`,
          "Nächste Woche: max 1 Key; 2. Reiz nur als strides/steady.",
          patternConfidence(keysLow, keysHigh, Math.abs(driftHigh - driftLow), 0.8)
        )
      );
    }

    if (Number.isFinite(vdotHigh) && Number.isFinite(vdotLow) && Number.isFinite(driftHigh) && Number.isFinite(driftLow) && vdotHigh > vdotLow && driftHigh <= driftLow + 0.2) {
      findings.push(
        buildPatternFinding(
          "2 Keys funktionieren bei stabiler Drift",
          `0–1 Key (n=${keysLow.length}): VDOTΔ=${vdotLow.toFixed(1)}, DriftΔ=${driftLow.toFixed(1)} | 2+ Keys (n=${keysHigh.length}): VDOTΔ=${vdotHigh.toFixed(1)}, DriftΔ=${driftHigh.toFixed(1)}`,
          "2. Key nur freigeben, wenn Drift in den letzten 2–3 Wochen stabil bleibt.",
          patternConfidence(keysLow, keysHigh, Math.abs(vdotHigh - vdotLow), 1.0)
        )
      );
    }
  }

  const easyHigh = usableWeeks.filter((w) => Number(w?.input?.easy_share) >= 0.8);
  const easyLow = usableWeeks.filter((w) => Number(w?.input?.easy_share) < 0.8);
  if (easyHigh.length >= PATTERN_MIN_GROUP_N && easyLow.length >= PATTERN_MIN_GROUP_N) {
    const driftHigh = asMedianOutput(easyHigh, "drift_delta");
    const driftLow = asMedianOutput(easyLow, "drift_delta");
    if (Number.isFinite(driftHigh) && Number.isFinite(driftLow) && driftLow > driftHigh) {
      findings.push(
        buildPatternFinding(
          "Easy ≥80% senkt Drift",
          `Easy ≥80% (n=${easyHigh.length}): DriftΔ=${driftHigh.toFixed(1)} | Easy <80% (n=${easyLow.length}): DriftΔ=${driftLow.toFixed(1)}`,
          "Nächste Woche Easy-Anteil auf mindestens 80% setzen.",
          patternConfidence(easyHigh, easyLow, Math.abs(driftLow - driftHigh), 0.8)
        )
      );
    }
  }

  const wellnessWeeks = usableWeeks.filter((w) => w?.wellness && Number.isFinite(w?.wellness?.sleep_avg));
  if (wellnessWeeks.length >= 6) {
    const wellGood = wellnessWeeks.filter((w) => Number(w.wellness.sleep_avg) >= 3.5 && Number(w.wellness.fatigue_avg) <= 3.2 && Number(w.wellness.pain_max) === 0);
    const wellBad = wellnessWeeks.filter((w) => !wellGood.includes(w));
    if (wellGood.length >= PATTERN_MIN_GROUP_N && wellBad.length >= PATTERN_MIN_GROUP_N) {
      const goodDrift = asMedianOutput(wellGood, "drift_delta");
      const badDrift = asMedianOutput(wellBad, "drift_delta");
      const goodVdot = asMedianOutput(wellGood, "vdot_delta");
      const badVdot = asMedianOutput(wellBad, "vdot_delta");
      if ((Number.isFinite(goodDrift) && Number.isFinite(badDrift) && badDrift > goodDrift) || (Number.isFinite(goodVdot) && Number.isFinite(badVdot) && badVdot < goodVdot)) {
        findings.push(
          buildPatternFinding(
            "Recovery gate: Qualität wirkt nur erholt",
            `Wellness gut (n=${wellGood.length}): VDOTΔ=${Number.isFinite(goodVdot) ? goodVdot.toFixed(1) : "n/a"}, DriftΔ=${Number.isFinite(goodDrift) ? goodDrift.toFixed(1) : "n/a"} | schlecht (n=${wellBad.length}): VDOTΔ=${Number.isFinite(badVdot) ? badVdot.toFixed(1) : "n/a"}, DriftΔ=${Number.isFinite(badDrift) ? badDrift.toFixed(1) : "n/a"}`,
            "Key-Qualität nur bei guter Recovery freigeben; sonst Umfang/Easy priorisieren.",
            "medium"
          )
        );
      }
    }
  }

  const tradeoffWeeks = usableWeeks.filter((w) => Number(w?.output?.ef_delta_pct) > 2 && Number(w?.output?.drift_delta) > 0.8);
  if (tradeoffWeeks.length >= Math.max(2, Math.round(usableWeeks.length * 0.35))) {
    const hardShareTradeoff = asMedianInput(tradeoffWeeks, "hard_share");
    const hardShareAll = asMedianInput(usableWeeks, "hard_share");
    const keysTradeoff = median(tradeoffWeeks.map((w) => Number(w?.input?.keys_count)));
    const keysAll = median(usableWeeks.map((w) => Number(w?.input?.keys_count)));
    if ((Number.isFinite(hardShareTradeoff) && Number.isFinite(hardShareAll) && hardShareTradeoff > hardShareAll) || (Number.isFinite(keysTradeoff) && Number.isFinite(keysAll) && keysTradeoff > keysAll)) {
      findings.push(
        buildPatternFinding(
          "Tradeoff: EF ↑ aber Drift ↑",
          `Tradeoff-Wochen: ${tradeoffWeeks.length}/${usableWeeks.length} | hard_share med ${Number.isFinite(hardShareTradeoff) ? (hardShareTradeoff * 100).toFixed(0) + "%" : "n/a"} vs ${Number.isFinite(hardShareAll) ? (hardShareAll * 100).toFixed(0) + "%" : "n/a"} | keys med ${Number.isFinite(keysTradeoff) ? keysTradeoff.toFixed(1) : "n/a"} vs ${Number.isFinite(keysAll) ? keysAll.toFixed(1) : "n/a"}`,
          "Intensität deckeln, bis Drift wieder stabilisiert ist (Easy hoch, Keys runter).",
          "medium"
        )
      );
    }
  }

  if (usableWeeks.length >= PATTERN_MIN_CORR_N) {
    const corrDefs = [
      { a: "easy_share", b: "drift_delta", label: "corr(EasyShare, DriftΔ)" },
      { a: "keys_count", b: "drift_delta", label: "corr(Keys, DriftΔ)" },
      { a: "keys_count", b: "ef_delta_pct", label: "corr(Keys, EFΔ%)" },
      { a: "load_total", b: "vdot_delta", label: "corr(Load, VDOTΔ)" },
    ];
    for (const def of corrDefs) {
      const r = pearsonCorrelation(usableWeeks.map((w) => [Number(w?.input?.[def.a]), Number(w?.output?.[def.b])]));
      if (Number.isFinite(r) && Math.abs(r) >= PATTERN_MIN_CORR_ABS) {
        correlationFindings.push(`${def.label} = ${r.toFixed(2)}`);
      }
    }
    if (correlationFindings.length) {
      findings.push(buildPatternFinding("Korrelationen (ergänzend)", correlationFindings.join(" | "), "Korrelationen nur als Hinweis, Gruppenfindings priorisieren.", "medium"));
    }
  }

  const driftRecent3 = median(usableWeeks.slice(0, 3).map((w) => Number(w?.output?.drift_delta)));
  const vdotRecent3 = median(usableWeeks.slice(0, 3).map((w) => Number(w?.output?.vdot_delta)));
  const driftStable = Number.isFinite(driftRecent3) && driftRecent3 <= 0.5;

  const guidance = {
    keysTarget: driftStable && Number.isFinite(vdotRecent3) && vdotRecent3 > 0 ? 2 : 1,
    easyShareMin: driftStable ? 0.75 : 0.8,
    loadChangePctCap: 0.1,
    flags: [],
    rationale: [],
  };

  if (!driftStable) {
    guidance.flags.push("drift_rising");
    guidance.rationale.push("Drift-Median der letzten 3 Wochen > +0.5.");
  }
  if (Number.isFinite(vdotRecent3) && vdotRecent3 > 0 && driftStable) {
    guidance.flags.push("vdot_positive_drift_stable");
    guidance.rationale.push("VDOTΔ positiv bei stabiler Drift.");
  }
  if (tradeoffWeeks.length > 0) {
    guidance.flags.push("intensity_tradeoff");
    guidance.rationale.push("Mehrere Wochen mit EF↑ und Drift↑.");
  }

  for (const finding of findings.slice(0, 5)) {
    guidance.rationale.push(finding.title);
  }

  return {
    header: "🧠 MUSTER-ANALYSE (8W)",
    insufficientData: false,
    findings: findings.slice(0, 5),
    guidance,
  };
}

function renderPatternAnalysisBlock(analysis) {
  if (!analysis) return "";
  const lines = [analysis.header];
  for (const finding of analysis.findings || []) {
    lines.push(`- ${finding.title}`);
    lines.push(`  Evidenz: ${finding.evidence}`);
    lines.push(`  Handlung: ${finding.action}`);
  }
  lines.push("NextWeekGuidance:");
  lines.push(...renderNextWeekGuidance(analysis.guidance || {}));
  return lines.join("\n");
}

function renderNextWeekGuidance(guidance) {
  const lines = [];
  const keysTarget = Number(guidance?.keysTarget);
  const easyShareMin = Number(guidance?.easyShareMin);
  const loadChangePctCap = Number(guidance?.loadChangePctCap);
  const flags = Array.isArray(guidance?.flags) ? guidance.flags.filter(Boolean) : [];
  const rationale = Array.isArray(guidance?.rationale) ? guidance.rationale.filter(Boolean) : [];

  if (Number.isFinite(keysTarget)) {
    lines.push(`- Key-Einheiten Ziel: ${keysTarget} pro Woche`);
  }
  if (Number.isFinite(easyShareMin)) {
    lines.push(`- Easy-Anteil Minimum: ${(easyShareMin * 100).toFixed(0)}%`);
  }
  if (Number.isFinite(loadChangePctCap)) {
    lines.push(`- Wochenlast-Änderung max.: ±${(loadChangePctCap * 100).toFixed(0)}%`);
  }

  if (flags.length) {
    lines.push(`- Hinweise: ${flags.join(", ")}`);
  }

  if (rationale.length) {
    lines.push("- Begründung:");
    for (const reason of rationale) {
      lines.push(`  • ${reason}`);
    }
  }

  if (!lines.length) {
    return ["- Keine Guidance verfügbar."];
  }

  return lines;
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
  const vdotPct = pct(current.vdotMed, previous.vdotMed);
  const driftDelta = current.driftMed != null && previous.driftMed != null ? current.driftMed - previous.driftMed : null;

  if (efPct != null && efPct >= 1 && driftDelta != null && driftDelta <= -1) {
    improvements.push(`Ökonomie besser: EF +${efPct.toFixed(1)}% & Drift ${driftDelta.toFixed(1)}%-Pkt.`);
    helped.push("Stabilere, ökonomischere GA-Läufe (EF ↑, Drift ↓).");
  } else if (efPct != null && efPct <= -1 && driftDelta != null && driftDelta >= 1) {
    regressions.push(`Ökonomie schlechter: EF ${efPct.toFixed(1)}% & Drift +${driftDelta.toFixed(1)}%-Pkt.`);
    actions.push("Mehr ruhige GA-Läufe für Ökonomie & Stabilität (konstant, nicht hart).");
  } else {
    if (efPct != null && Math.abs(efPct) >= 1) {
      const vdotSuffix =
        vdotPct != null && Math.abs(vdotPct) >= 0.5 ? ` | VDOT ${vdotPct > 0 ? "+" : ""}${vdotPct.toFixed(1)}%` : "";
      (efPct > 0 ? improvements : regressions).push(
        `EF ${efPct > 0 ? "+" : ""}${efPct.toFixed(1)}% (Ökonomie)${vdotSuffix}.`
      );
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

function appendFourWeekProgressSection(rep, insights) {
  if (!rep) return rep;
  const lines = rep.text.split("\n");
  lines.push("");
  lines.push("4-Wochen-Fazit:");

  if (!insights) {
    lines.push("- Aktuell keine belastbare 4-Wochen-Aussage möglich (zu wenig Vergleichsdaten).");
    return { ...rep, text: lines.join("\n"), fourWeekInsights: null };
  }

  const verdict = buildFourWeekVerdict(insights);

  lines.push(`- Fortschritt letzte 4 Wochen: ${verdict}`);
  lines.push(`- Werte (letzte 4 Wochen vs vorherige 4 Wochen): ${buildFourWeekValuesLine(insights)}`);
  if (isFiniteNumber(insights.progressScore)) {
    lines.push(
      `- Progress-Score (Output-basiert): ${insights.progressScore.toFixed(2)} → ${insights.progressCategory}`
    );
  }
  if (isFiniteNumber(insights.fatigueAdjustedProgress)) {
    lines.push(`- Fatigue-korrigiert (VDOT + Load-Effekt): ${fmtSigned1(insights.fatigueAdjustedProgress)}`);
  }

  if (insights.progressCategory !== "klarer Fortschritt" && (insights.regressions.length || insights.context.length)) {
    lines.push("- Wenn nicht besser: wahrscheinliche Gründe:");
    for (const item of [...insights.regressions, ...insights.context].slice(0, 6)) lines.push(`  - ${item}`);
  }

  if (insights.actions.length) {
    lines.push("- Fokus für die nächsten Wochen:");
    for (const item of insights.actions.slice(0, 4)) lines.push(`  - ${item}`);
  }

  return { ...rep, text: lines.join("\n"), fourWeekInsights: insights };
}

function buildFourWeekVerdict(insights) {
  const category = insights?.progressCategory || "stabil";
  if (category === "klarer Fortschritt") {
    return "Ja – messbarer Fortschritt in den letzten 4 Wochen (Output > Belastungsanstieg).";
  }
  if (category === "Rückgang / Fatigue") {
    return "Nein – aktuell kein messbarer Leistungsgewinn; eher Ermüdung bzw. Rückgang sichtbar.";
  }
  return "Stabil – Leistungsniveau aktuell eher gehalten (Adaption läuft, Fortschritt noch nicht klar messbar).";
}

function buildFourWeekValuesLine(insights) {
  const c = insights?.currentSummary;
  const p = insights?.previousSummary;
  if (!c || !p) return "noch keine Werte verfügbar.";

  const efText =
    isFiniteNumber(c.efMed) && isFiniteNumber(p.efMed)
      ? `${c.efMed.toFixed(3)} vs ${p.efMed.toFixed(3)}`
      : "n/a";
  const vdotText =
    isFiniteNumber(c.vdotMed) && isFiniteNumber(p.vdotMed)
      ? `${c.vdotMed.toFixed(1)} vs ${p.vdotMed.toFixed(1)}`
      : "n/a";
  const driftText =
    isFiniteNumber(c.driftMed) && isFiniteNumber(p.driftMed)
      ? `${c.driftMed.toFixed(1)}%-Pkt vs ${p.driftMed.toFixed(1)}%-Pkt`
      : "n/a";
  const loadText =
    isFiniteNumber(c.weeklyLoad) && isFiniteNumber(p.weeklyLoad)
      ? `${Math.round(c.weeklyLoad)} vs ${Math.round(p.weeklyLoad)}`
      : "n/a";
  const runsText =
    isFiniteNumber(c.runsPerWeek) && isFiniteNumber(p.runsPerWeek)
      ? `${c.runsPerWeek.toFixed(1)} vs ${p.runsPerWeek.toFixed(1)}`
      : "n/a";

  return `EF ${efText} | VDOT ${vdotText} | Drift ${driftText} | Load/Woche ${loadText} | Läufe/Woche ${runsText}`;
}

async function computeFourWeekProgressInsights(env, mondayIso, warmupSkipSec) {
  const current = await computeDetectiveNote(env, mondayIso, warmupSkipSec, 28);
  const mondayDate = new Date(mondayIso + "T00:00:00Z");
  const prevMondayIso = isoDate(new Date(mondayDate.getTime() - 28 * 86400000));
  const previous = await computeDetectiveNote(env, prevMondayIso, warmupSkipSec, 28);

  if (!current?.summary || !previous?.summary) return null;

  const baseInsights = buildDetectiveWhyInsights(
    { ...current.summary, week: "letzte 4 Wochen" },
    { ...previous.summary, week: "vorherige 4 Wochen" }
  );
  if (!baseInsights) return null;

  return {
    ...baseInsights,
    currentSummary: current.summary,
    previousSummary: previous.summary,
    ...buildFourWeekProgressMetrics(current.summary, previous.summary),
  };
}

function buildFourWeekProgressMetrics(current, previous) {
  const efDelta = isFiniteNumber(current?.efMed) && isFiniteNumber(previous?.efMed) ? current.efMed - previous.efMed : null;
  const vdotDelta =
    isFiniteNumber(current?.vdotMed) && isFiniteNumber(previous?.vdotMed) ? current.vdotMed - previous.vdotMed : null;
  const driftDelta =
    isFiniteNumber(current?.driftMed) && isFiniteNumber(previous?.driftMed) ? current.driftMed - previous.driftMed : null;
  const loadDeltaPct =
    isFiniteNumber(current?.weeklyLoad) && isFiniteNumber(previous?.weeklyLoad) && previous.weeklyLoad > 0
      ? ((current.weeklyLoad - previous.weeklyLoad) / previous.weeklyLoad) * 100
      : null;

  const progressScore =
    efDelta == null || vdotDelta == null || driftDelta == null
      ? null
      : efDelta * 1000 * 0.4 + vdotDelta * 0.3 - driftDelta * 1.5 * 0.3;
  const fatigueAdjustedProgress =
    vdotDelta == null
      ? null
      : vdotDelta + (isFiniteNumber(loadDeltaPct) ? loadDeltaPct * 0.1 : 0);

  let progressCategory = "stabil";
  if (progressScore != null) {
    if (progressScore > 0.5) progressCategory = "klarer Fortschritt";
    else if (progressScore < -0.3) progressCategory = "Rückgang / Fatigue";
  }

  return {
    efDelta,
    vdotDelta,
    driftDelta,
    loadDeltaPct,
    progressScore,
    fatigueAdjustedProgress,
    progressCategory,
  };
}

async function computeDetectiveNoteAdaptive(env, mondayIso, warmupSkipSec) {
  for (const w of DETECTIVE_WINDOWS) {
    const rep = await computeDetectiveNote(env, mondayIso, warmupSkipSec, w);
    if (rep.ok) {
      const history = await loadDetectiveHistory(env, mondayIso);
      const insights = buildDetectiveWhyInsights(rep.summary, history[0]);
      const withWhy = applyDetectiveWhy(rep, insights);
      const fourWeekInsights = await computeFourWeekProgressInsights(env, mondayIso, warmupSkipSec);
      return appendFourWeekProgressSection(withWhy, fourWeekInsights);
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
  const withWhy = applyDetectiveWhy(last, insights);
  const fourWeekInsights = await computeFourWeekProgressInsights(env, mondayIso, warmupSkipSec);
  return appendFourWeekProgressSection(withWhy, fourWeekInsights);
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
  const activeLifeEventsAtWindowEnd = lifeEvents.filter((event) => isLifeEventActiveOnDay(event, endIsoInclusive));
  const activeLifeEventSummary = activeLifeEventsAtWindowEnd
    .map((e) => normalizeEventCategory(e?.category))
    .filter(Boolean)
    .reduce((acc, category) => {
      acc[category] = (acc[category] || 0) + 1;
      return acc;
    }, {});
  const hasActiveLifeEventAtWindowEnd = Object.keys(activeLifeEventSummary).length > 0;
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
  if (hasActiveLifeEventAtWindowEnd) {
    lines.push(
      `- Verfügbarkeit: eingeschränkt (aktives LifeEvent: ${Object.entries(activeLifeEventSummary)
        .map(([category, count]) => `${getLifeEventCategoryLabel(category)}${count > 1 ? ` ×${count}` : ""}`)
        .join(" · ")})`
    );
  } else if (hasLifeEvent) {
    lines.push(
      `- Verfügbarkeit: normal (aktuell kein LifeEvent; im Fenster: ${Object.entries(lifeEventSummary)
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
    vdotMed: comp.efMed != null ? vdotLikeFromEf(comp.efMed) : null,
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
async function upsertMondayDetectiveNote(env, dayIso, noteText, eventsByExternalId = null) {
  const external_id = `detektiv-${dayIso}`;
  const name = "Montags-Report";
  const description = toHardLineBreakText(noteText);

  // If we already fetched events for the whole range, don't trigger per-day fetches again.
  const hasPrefetchedEvents = eventsByExternalId instanceof Map;
  const dayEvents = hasPrefetchedEvents ? null : await fetchIntervalsEvents(env, dayIso, dayIso);
  const existing = hasPrefetchedEvents
    ? (eventsByExternalId.get(external_id) || null)
    : (dayEvents || []).find((e) => String(e?.external_id || "") === external_id);

  if (existing?.id) {
    if (String(existing?.description || "") === description) return;
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

  const created = await createIntervalsEvent(env, {
    category: "NOTE",
    start_date_local: `${dayIso}T00:00:00`,
    name,
    description,
    color: "orange",
    external_id,
  });
  if (hasPrefetchedEvents && created?.id) eventsByExternalId.set(external_id, created);
}

async function fetchDailyReportNoteEvent(env, dayIso, eventsByExternalId = null) {
  const external_id = `daily-report-${dayIso}`;
  if (eventsByExternalId instanceof Map) return eventsByExternalId.get(external_id) || null;
  const events = await fetchIntervalsEvents(env, dayIso, dayIso);
  return (events || []).find((e) => String(e?.external_id || "") === external_id) || null;
}

function fromHardLineBreakText(text) {
  return String(text ?? "")
    .replace(/<br\s*\/?>\s*\n?/gi, "\n")
    .replace(/&nbsp;/gi, " ")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
}

function splitDecisionBlocks(text) {
  const normalized = fromHardLineBreakText(text).trim();
  if (!normalized) return [];
  return normalized.split(/\n⸻\n\s*\n/).map((b) => b.trim()).filter(Boolean);
}

function getDecisionBlockTitle(block) {
  const first = String(block || "").split("\n")[0] || "";
  return first.replace(/^[^\p{L}\p{N}]+/u, "").trim();
}

function composeDecisionBlocks(blocks) {
  const clean = (blocks || []).map((b) => String(b || "").trim()).filter(Boolean);
  if (!clean.length) return "";
  return `${clean.join("\n⸻\n\n")}\n⸻\n\n`;
}

function mergeTodayRunSection(existingText, freshText) {
  const canonicalTitles = [
    "HEUTIGER LAUF",
    "HEUTE",
    "WARUM",
    "STATUS",
    "FOKUS",
    "TRAININGSSTAND",
    "EMPFEHLUNGEN",
    "DIAGNOSE",
    "BOTTOM LINE",
  ];
  const freshBlocks = splitDecisionBlocks(freshText);
  if (!freshBlocks.length) return fromHardLineBreakText(existingText);

  const freshMap = new Map();
  for (const block of freshBlocks) {
    const title = getDecisionBlockTitle(block);
    if (canonicalTitles.includes(title)) freshMap.set(title, block);
  }

  const canonicalBlocks = canonicalTitles.map((title) => freshMap.get(title)).filter(Boolean);
  return composeDecisionBlocks(canonicalBlocks.length ? canonicalBlocks : freshBlocks);
}

async function upsertDailyReportTodayRunSection(env, dayIso, freshNoteText, existingEvent = null) {
  const external_id = `daily-report-${dayIso}`;
  const name = "Daily-Report";
  const existing = existingEvent || await fetchDailyReportNoteEvent(env, dayIso);
  if (!existing?.id) return false;

  const normalizedFreshText = normalizeDailyReportText(dayIso, freshNoteText || "");
  const merged = mergeTodayRunSection(existing?.description || "", normalizedFreshText);
  const existingNormalized = fromHardLineBreakText(existing?.description || "").trim();
  if (existingNormalized === String(merged || "").trim()) return true;
  await updateIntervalsEvent(env, existing.id, {
    category: "NOTE",
    start_date_local: `${dayIso}T00:00:00`,
    name,
    description: toHardLineBreakText(merged),
    color: "blue",
    external_id,
  });
  return true;
}

// Create/update a blue NOTE event for the daily wellness report
async function upsertDailyReportNote(env, dayIso, noteText, eventsByExternalId = null) {
  const external_id = `daily-report-${dayIso}`;
  const name = "Daily-Report";
  const description = toHardLineBreakText(normalizeDailyReportText(dayIso, noteText));

  const hasPrefetchedEvents = eventsByExternalId instanceof Map;
  const dayEvents = hasPrefetchedEvents ? null : await fetchIntervalsEvents(env, dayIso, dayIso);
  const existing = hasPrefetchedEvents
    ? (eventsByExternalId.get(external_id) || null)
    : (dayEvents || []).find((e) => String(e?.external_id || "") === external_id);

  if (existing?.id) {
    if (String(existing?.description || "") === description) return;
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

  const created = await createIntervalsEvent(env, {
    category: "NOTE",
    start_date_local: `${dayIso}T00:00:00`,
    name,
    description,
    color: "blue",
    external_id,
  });
  if (hasPrefetchedEvents && created?.id) eventsByExternalId.set(external_id, created);
}

function toHardLineBreakText(text) {
  const normalized = String(text ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n");
  return normalized.split("\n").join("<br />\n");
}

function normalizeDailyReportText(dayIso, text) {
  const raw = String(text ?? "");
  const trimmed = raw.trim();
  if (!trimmed || !trimmed.startsWith("{") || !trimmed.includes('"notesPreview"')) return raw;

  try {
    const parsed = JSON.parse(trimmed);
    const dayNote = parsed?.notesPreview?.[dayIso];
    if (typeof dayNote === "string" && dayNote.trim()) return dayNote;
  } catch (_) {
    // keep original text when payload is not valid JSON
  }
  return raw;
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

function isTempoDauerlaufKey(activity) {
  const keyType = String(getKeyType(activity) || "").toLowerCase();
  return keyType.includes("tdl") || keyType.includes("tempo");
}

async function computeBenchReport(env, activity, benchName, warmupSkipSec) {
  const dayIso = String(activity.start_date_local || activity.start_date || "").slice(0, 10);
  if (!dayIso) return null;

  const benchType = getBenchType(benchName);
  const isKey = hasKeyTag(activity);
  const keyType = getKeyType(activity);
  const isLongrunProgression = benchType === "GA" && String(keyType || "").toLowerCase().includes("prog");
  const isTempoDauerlauf = isTempoDauerlaufKey(activity);
  const end = new Date(dayIso + "T00:00:00Z");
  const start = new Date(end.getTime() - BENCH_LOOKBACK_DAYS * 86400000);
  const acts = await fetchIntervalsActivities(env, isoDate(start), isoDate(end));

  const same = acts
    .filter((a) => isRun(a) && getBenchTag(a) === benchName && a.id !== activity.id)
    .sort((a, b) => new Date(b.start_date_local || b.start_date) - new Date(a.start_date_local || a.start_date));

  const today = await computeBenchMetrics(env, activity, warmupSkipSec, { allowDrift: !isKey || isTempoDauerlauf });
  if (!today) return `🧪 bench:${benchName}\nHeute: n/a`;

  let progressionMetrics = null;
  if (isLongrunProgression) {
    const streams = await fetchIntervalsStreams(env, activity.id, ["time", "velocity_smooth", "heartrate"]);
    progressionMetrics = computeLongrunProgressionMetricsFromStreams(streams, warmupSkipSec);
  }

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
    ? await computeBenchMetrics(env, same[0], warmupSkipSec, { allowDrift: (benchType === "GA" && !isKey) || isTempoDauerlauf })
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

  if (isLongrunProgression) {
    const steadyPct = Number.isFinite(progressionMetrics?.steadyEndPct) ? Math.round(progressionMetrics.steadyEndPct * 100) : 65;
    if (progressionMetrics?.steadyDriftPct != null) {
      lines.push(`Steady-Drift (0–${steadyPct}%): ${fmtSigned1(progressionMetrics.steadyDriftPct)}%`);
    } else {
      lines.push(`Steady-Drift (0–${steadyPct}%): n/a`);
    }
    if (progressionMetrics) {
      lines.push(`Progression: Pace ${progressionMetrics.paceIncreased ? "↑" : "nicht klar steigend"}, HF ${progressionMetrics.hrProportional ? "proportional" : "überproportional"}`);
      if (!progressionMetrics.noHrJump) {
        lines.push("Warnung: HF-Sprung >5 bpm in ~150s erkannt.");
      }
      if (!progressionMetrics.below90PctHfmax) {
        lines.push("Warnung: >90% HFmax im Progressions-Teil.");
      }
    }
  } else if ((benchType === "GA" && !isKey) || isTempoDauerlauf) {
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

  if (isLongrunProgression && progressionMetrics) {
    const failReasons = [];
    if (progressionMetrics.steadyDriftPct == null) {
      failReasons.push("Steady-Drift n/a");
    } else if (progressionMetrics.steadyDriftPct > 5) {
      failReasons.push(`Steady-Drift ${progressionMetrics.steadyDriftPct.toFixed(1)}% > 5%`);
    }
    if (!progressionMetrics.paceIncreased) failReasons.push("Pace-Anstieg fehlt");
    if (!progressionMetrics.hrProportional) failReasons.push("HF steigt überproportional");
    if (!progressionMetrics.noHrJump) failReasons.push("HF-Sprung >5 bpm/150s");
    if (!progressionMetrics.below90PctHfmax) failReasons.push(">90% HFmax erreicht");

    verdict = failReasons.length
      ? `Longrun-Progression teilweise verfehlt: ${failReasons.join(", ")}.`
      : "Longrun-Progression erfüllt: Steady stabil, Progression kontrolliert.";
  } else if (same.length && intervalMetrics && lastIntervalMetrics) {
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
  } else if (same.length && isTempoDauerlauf && today.drift != null && last?.drift != null) {
    const driftDelta = today.drift - last.drift;
    if (driftDelta >= 1.5) {
      verdict = `TDL härter – Drift höher (${fmtSigned1(driftDelta)}%-Pkt vs letzte).`;
    } else if (driftDelta <= -1.5) {
      verdict = `TDL stabiler – Drift niedriger (${fmtSigned1(driftDelta)}%-Pkt vs letzte).`;
    } else {
      verdict = `TDL ähnlich – Drift vergleichbar (${fmtSigned1(driftDelta)}%-Pkt vs letzte).`;
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

function computeLongrunProgressionMetricsFromStreams(streams, warmupSkipSec = 600) {
  if (!streams) return null;
  const time = Array.isArray(streams.time) ? streams.time : null;
  const speed = Array.isArray(streams.velocity_smooth) ? streams.velocity_smooth : null;
  const hr = Array.isArray(streams.heartrate) ? streams.heartrate : null;
  if (!time || !speed || !hr) return null;

  const n = Math.min(time.length, speed.length, hr.length);
  if (n < MIN_POINTS) return null;

  const points = [];
  for (let i = 0; i < n; i++) {
    const t = Number(time[i]);
    const v = Number(speed[i]);
    const h = Number(hr[i]);
    if (!Number.isFinite(t) || !Number.isFinite(v) || !Number.isFinite(h)) continue;
    points.push({ t, v, h });
  }
  if (points.length < MIN_POINTS) return null;

  const t0 = points[0].t;
  for (const p of points) p.t -= t0;
  const durationSec = points[points.length - 1].t;
  if (!Number.isFinite(durationSec) || durationSec <= 0) return null;

  const mean = (arr) => (arr.length ? arr.reduce((sum, x) => sum + x, 0) / arr.length : null);
  const pick = (fromPct) => points.filter((p) => p.t >= durationSec * fromPct);

  // Suche den plausibelsten Progressionsstart zwischen 70–95% (deckt auch sehr späte 95/5-Finishes ab).
  let best = null;
  for (let pct = 0.7; pct <= 0.95; pct += 0.05) {
    const progCand = pick(pct);
    if (progCand.length < Math.max(6, Math.floor(points.length * 0.05))) continue;
    const preCand = points.filter((p) => p.t < durationSec * pct);
    if (preCand.length < Math.max(6, Math.floor(points.length * 0.2))) continue;

    const spPre = mean(preCand.map((p) => p.v));
    const spProg = mean(progCand.map((p) => p.v));
    if (!(spPre > 0 && spProg > 0)) continue;

    const gainPct = ((spProg - spPre) / spPre) * 100;
    if (!best || gainPct > best.gainPct) best = { pct, gainPct };
  }

  const progressionStartPct = best?.gainPct >= 1 ? best.pct : 0.8;
  const steadyEndPct = Math.max(0.6, Math.min(0.7, progressionStartPct - 0.1));

  const steadyEndSec = durationSec * steadyEndPct;
  const progressionStartSec = durationSec * progressionStartPct;

  const steadyPoints = points.filter((p) => p.t <= steadyEndSec);
  const progressionPoints = points.filter((p) => p.t >= progressionStartSec);

  const steadyDrift = computeDriftAndStabilityFromStreams(
    {
      time: steadyPoints.map((p) => p.t),
      velocity_smooth: steadyPoints.map((p) => p.v),
      heartrate: steadyPoints.map((p) => p.h),
    },
    Math.min(warmupSkipSec, Math.max(0, steadyEndSec * 0.4))
  );
  const steadyDriftPct = Number.isFinite(steadyDrift?.pa_hr_decouple_pct) ? steadyDrift.pa_hr_decouple_pct : null;

  const pLen = progressionPoints.length;
  const third = Math.max(1, Math.floor(pLen / 3));
  const progStart = progressionPoints.slice(0, third);
  const progEnd = progressionPoints.slice(Math.max(0, pLen - third));

  const startSpeed = mean(progStart.map((p) => p.v));
  const endSpeed = mean(progEnd.map((p) => p.v));
  const startHr = mean(progStart.map((p) => p.h));
  const endHr = mean(progEnd.map((p) => p.h));

  const speedPct = startSpeed > 0 && endSpeed > 0 ? ((endSpeed - startSpeed) / startSpeed) * 100 : null;
  const hrPct = startHr > 0 && endHr > 0 ? ((endHr - startHr) / startHr) * 100 : null;
  const paceIncreased = Number.isFinite(speedPct) && speedPct >= 1;
  const hrProportional = Number.isFinite(speedPct) && Number.isFinite(hrPct) && hrPct >= 0 && hrPct <= speedPct * 1.5 + 1;

  let noHrJump = true;
  if (pLen > 1) {
    for (let i = 0; i < pLen; i++) {
      const base = progressionPoints[i];
      let j = i + 1;
      while (j < pLen && progressionPoints[j].t - base.t < 150) j++;
      if (j < pLen) {
        const deltaHr = progressionPoints[j].h - base.h;
        if (deltaHr > 5) {
          noHrJump = false;
          break;
        }
      }
    }
  }

  const maxHrProg = progressionPoints.length ? Math.max(...progressionPoints.map((p) => p.h)) : null;
  const below90PctHfmax = Number.isFinite(maxHrProg) ? maxHrProg <= HFMAX * 0.9 : true;

  return {
    steadyDriftPct,
    paceIncreased,
    hrProportional,
    noHrJump,
    below90PctHfmax,
    speedPct,
    hrPct,
    steadyEndPct,
    progressionStartPct,
  };
}

async function computeIntervalBenchMetrics(env, a, warmupSkipSec) {
  return computeIntervalMetrics(env, a, {
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
function pickNumber(...vals) {
  for (const v of vals) {
    const n = Number(v);
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function extractIntervals(activity) {
  const out = [];
  const resolveIntervalBounds = (itv, cursorSec = null) => {
    let start = pickNumber(
      itv?.start,
      itv?.start_sec,
      itv?.startTime,
      itv?.start_time,
      itv?.from,
      itv?.offset,
      itv?.offset_sec
    );
    let end = pickNumber(itv?.end, itv?.end_sec, itv?.endTime, itv?.end_time, itv?.to);
    const duration = pickNumber(
      itv?.moving_time,
      itv?.elapsed_time,
      itv?.duration,
      itv?.duration_sec,
      itv?.time
    );

    if (!Number.isFinite(end) && Number.isFinite(start) && Number.isFinite(duration) && duration > 0) {
      end = start + duration;
    }
    if (!Number.isFinite(start) && Number.isFinite(end) && Number.isFinite(duration) && duration > 0) {
      start = end - duration;
    }
    if (!Number.isFinite(start) && !Number.isFinite(end) && Number.isFinite(cursorSec) && Number.isFinite(duration) && duration > 0) {
      start = cursorSec;
      end = cursorSec + duration;
    }

    return { start, end };
  };

  const pushInterval = (itv, fallbackType = null, cursorSec = null) => {
    const { start, end } = resolveIntervalBounds(itv, cursorSec);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) return;
    out.push({
      start,
      end,
      name: itv?.name ?? itv?.label ?? itv?.lap_name ?? null,
      type: itv?.type ?? itv?.kind ?? fallbackType,
    });
    return { start, end };
  };

  if (Array.isArray(activity?.intervals)) {
    let cursor = 0;
    activity.intervals.forEach((itv) => {
      const inserted = pushInterval(itv, null, cursor);
      if (inserted) cursor = inserted.end;
    });
    if (out.length) return out;
  }
  if (Array.isArray(activity?.icu_intervals)) {
    let cursor = 0;
    activity.icu_intervals.forEach((itv) => {
      const inserted = pushInterval(itv, null, cursor);
      if (inserted) cursor = inserted.end;
    });
    if (out.length) return out;
  }
  if (Array.isArray(activity?.laps)) {
    let cursor = 0;
    activity.laps.forEach((lap) => {
      const inserted = pushInterval(lap, 'lap', cursor);
      if (inserted) cursor = inserted.end;
    });
    if (out.length) return out;
  }

  const walkTree = (node) => {
    if (!node) return;
    if (Array.isArray(node)) {
      node.forEach(walkTree);
      return;
    }
    pushInterval(node, 'interval');
    const kids = node?.children ?? node?.intervals ?? node?.items;
    if (kids) walkTree(kids);
  };

  walkTree(activity?.interval_tree ?? activity?.intervalTree ?? activity?.intervals_tree ?? null);
  return out;
}

function lowerBound(arr, x) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] < x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function upperBound(arr, x) {
  let lo = 0;
  let hi = arr.length;
  while (lo < hi) {
    const mid = (lo + hi) >> 1;
    if (arr[mid] <= x) lo = mid + 1;
    else hi = mid;
  }
  return lo;
}

function sliceStreamsByTime(streams, startTime, endTime) {
  const time = streams?.time;
  if (!Array.isArray(time) || time.length < 2) return null;
  if (!Number.isFinite(startTime) || !Number.isFinite(endTime) || endTime <= startTime) return null;

  const i0 = lowerBound(time, startTime);
  const i1 = upperBound(time, endTime);
  if (i1 - i0 < 2) return null;

  const out = { time: time.slice(i0, i1) };
  for (const [k, v] of Object.entries(streams)) {
    if (k === 'time') continue;
    if (Array.isArray(v)) out[k] = v.slice(i0, i1);
  }
  return out;
}

function filterPaused(samples, { speedKey = 'velocity_smooth', minSpeed = 0.3 } = {}) {
  const time = samples?.time;
  const speed = samples?.[speedKey];
  if (!Array.isArray(time) || !Array.isArray(speed) || speed.length !== time.length) return samples;

  const keepIdx = [];
  for (let i = 0; i < time.length; i++) {
    const v = Number(speed[i]);
    if (Number.isFinite(v) && v >= minSpeed) keepIdx.push(i);
  }
  if (keepIdx.length < 2) return samples;

  const out = { time: keepIdx.map((i) => time[i]) };
  for (const [k, v] of Object.entries(samples)) {
    if (k === 'time') continue;
    if (Array.isArray(v) && v.length === time.length) out[k] = keepIdx.map((i) => v[i]);
  }
  return out;
}

function pickIntensityStreamKey(streams) {
  if (Array.isArray(streams?.watts)) return 'watts';
  if (Array.isArray(streams?.velocity_smooth)) return 'velocity_smooth';
  if (Array.isArray(streams?.pace)) return 'pace';
  return null;
}

function computeHRDriftAndDecoupling(samples, intensityKey) {
  const hr = samples?.heartrate;
  const time = samples?.time;
  if (!Array.isArray(hr) || !Array.isArray(time) || hr.length < 10) return null;

  const n = hr.length;
  const mid = Math.floor(n / 2);
  const hr1 = avg(hr.slice(0, mid));
  const hr2 = avg(hr.slice(mid));
  if (!Number.isFinite(hr1) || !Number.isFinite(hr2)) return null;

  const driftBpm = hr2 - hr1;
  const driftPct = hr1 ? (driftBpm / hr1) * 100 : null;

  let decouplingPct = null;
  if (intensityKey && Array.isArray(samples[intensityKey])) {
    const x = samples[intensityKey];
    const x1 = avg(x.slice(0, mid));
    const x2 = avg(x.slice(mid));
    if (Number.isFinite(x1) && Number.isFinite(x2) && x1 !== 0 && x2 !== 0) {
      const r1 = hr1 / x1;
      const r2 = hr2 / x2;
      if (Number.isFinite(r1) && Number.isFinite(r2) && r1 !== 0) {
        decouplingPct = ((r2 - r1) / r1) * 100;
      }
    }
  }

  return { driftBpm, driftPct, decouplingPct };
}

function computeHRR60(fullStreams, intervalEndSec) {
  const endWindow = sliceStreamsByTime(fullStreams, Math.max(0, intervalEndSec - 10), intervalEndSec);
  const afterWindow = sliceStreamsByTime(fullStreams, intervalEndSec + 50, intervalEndSec + 60);
  const hrEnd = median(endWindow?.heartrate ?? []);
  const hrAfter = median(afterWindow?.heartrate ?? []);
  if (!Number.isFinite(hrEnd) || !Number.isFinite(hrAfter)) return null;
  return hrEnd - hrAfter;
}

function computePerIntervalMetrics(fullStreams, interval, { dropPaused = true } = {}) {
  const rawSlice = sliceStreamsByTime(fullStreams, interval.start, interval.end);
  if (!rawSlice) return null;

  const slice = dropPaused ? filterPaused(rawSlice) : rawSlice;
  if (!Array.isArray(slice?.heartrate) || slice.heartrate.length < 10) return null;

  const intensityKey = pickIntensityStreamKey(slice);
  const drift = computeHRDriftAndDecoupling(slice, intensityKey);
  return {
    start: interval.start,
    end: interval.end,
    duration_sec: interval.end - interval.start,
    avg_hr: avg(slice.heartrate),
    intensity_key: intensityKey,
    avg_intensity: intensityKey ? avg(slice[intensityKey]) : null,
    hr_drift_bpm: drift?.driftBpm ?? null,
    hr_drift_pct: drift?.driftPct ?? null,
    decoupling_pct: drift?.decouplingPct ?? null,
    hrr60: computeHRR60(fullStreams, interval.end),
  };
}

function reduceIntervalMetrics(perInterval) {
  return {
    intervals: perInterval.length,
    hr_drift_bpm_median: median(perInterval.map((x) => x.hr_drift_bpm)),
    hr_drift_pct_median: median(perInterval.map((x) => x.hr_drift_pct)),
    hrr60_median: median(perInterval.map((x) => x.hrr60)),
    decoupling_pct_median: median(perInterval.map((x) => x.decoupling_pct)),
  };
}

function quantile(arr, q) {
  if (!Array.isArray(arr) || !arr.length) return null;
  const vals = arr
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v))
    .sort((a, b) => a - b);
  if (!vals.length) return null;
  const clampedQ = Math.min(1, Math.max(0, Number(q)));
  const pos = (vals.length - 1) * clampedQ;
  const lo = Math.floor(pos);
  const hi = Math.ceil(pos);
  if (lo === hi) return vals[lo];
  const w = pos - lo;
  return vals[lo] * (1 - w) + vals[hi] * w;
}

function deriveIntervalsFromStreams(fullStreams, { minDurationSec = 120, maxIntervals = 50 } = {}) {
  const time = Array.isArray(fullStreams?.time) ? fullStreams.time : null;
  if (!time || time.length < 30) return [];

  const key = Array.isArray(fullStreams?.watts)
    ? 'watts'
    : Array.isArray(fullStreams?.velocity_smooth)
      ? 'velocity_smooth'
      : null;
  if (!key) return [];

  const raw = fullStreams[key];
  const n = Math.min(time.length, raw.length);
  if (n < 30) return [];

  const values = [];
  for (let i = 0; i < n; i++) {
    const v = Number(raw[i]);
    if (Number.isFinite(v) && v > 0) values.push(v);
  }
  if (values.length < 30) return [];

  const p50 = quantile(values, 0.5);
  const p80 = quantile(values, 0.8);
  if (!Number.isFinite(p50) || !Number.isFinite(p80) || p80 <= p50) return [];

  const threshold = p50 + (p80 - p50) * 0.35;
  const maxGapSec = 35;
  const segments = [];

  let active = null;
  let gapStart = null;
  for (let i = 0; i < n; i++) {
    const t = Number(time[i]);
    const v = Number(raw[i]);
    if (!Number.isFinite(t)) continue;

    const isOn = Number.isFinite(v) && v >= threshold;
    if (isOn) {
      if (!active) {
        active = { start: t, end: t };
      } else {
        active.end = t;
      }
      gapStart = null;
      continue;
    }

    if (!active) continue;
    if (gapStart == null) gapStart = t;
    if (t - gapStart > maxGapSec) {
      if ((active.end - active.start) >= minDurationSec) {
        segments.push({
          start: active.start,
          end: active.end,
          type: 'ON',
          name: 'Derived interval',
        });
      }
      active = null;
      gapStart = null;
    }
  }

  if (active && (active.end - active.start) >= minDurationSec) {
    segments.push({
      start: active.start,
      end: active.end,
      type: 'ON',
      name: 'Derived interval',
    });
  }

  return segments.slice(0, maxIntervals);
}

async function computeIntervalMetricsStable(env, activity, options = {}) {
  const {
    minDurationSec = 60,
    maxIntervals = 50,
    dropPaused = true,
  } = options;

  const best = await fetchBestIntervals(env, activity, { minDurationSec, maxIntervals });
  const sourceActivity = best.sourceActivity ?? activity;
  let intervals = best.intervals;
  let intervalSource = best.source;

  intervals = intervals.filter((itv) => isLikelyWorkInterval(itv));

  const available = new Set(Array.isArray(sourceActivity?.stream_types) ? sourceActivity.stream_types : []);
  const wanted = ['time', 'heartrate', 'watts', 'cadence', 'velocity_smooth', 'pace'];
  const types = available.size ? wanted.filter((t) => available.has(t)) : wanted.slice();
  if (!types.includes('time')) types.unshift('time');
  if (!types.includes('heartrate')) types.push('heartrate');

  const fullStreams = await fetchIntervalsStreams(env, activity.id, types);
  if (!Array.isArray(fullStreams?.time) || !Array.isArray(fullStreams?.heartrate)) return null;

  if (!intervals.length) {
    intervals = deriveIntervalsFromStreams(fullStreams, { minDurationSec, maxIntervals });
    intervalSource = intervals.length ? 'streams_heuristic' : null;
  }
  if (!intervals.length) return null;

  const perInterval = intervals
    .map((itv) => computePerIntervalMetrics(fullStreams, itv, { dropPaused }))
    .filter(Boolean);
  if (!perInterval.length) return null;

  return {
    interval_count_input: intervals.length,
    interval_count_used: perInterval.length,
    interval_source: intervalSource,
    summary: reduceIntervalMetrics(perInterval),
    perInterval,
  };
}

function isLikelyWorkInterval(interval) {
  const type = String(interval?.type ?? '').toUpperCase();
  if (!type) return true;
  return !(/REST|RECOV|OFF|COOLDOWN|WARMUP|WARM-UP|EASY/.test(type));
}

function normalizeIntervalsPayload(payload) {
  if (Array.isArray(payload)) return { intervals: payload };
  if (payload && typeof payload === 'object') return payload;
  return {};
}

async function fetchBestIntervals(env, activity, options = {}) {
  const {
    minDurationSec = 60,
    maxIntervals = 50,
  } = options;

  const base = activity && typeof activity === 'object' ? activity : { id: activity };
  const activityId = base?.id;
  if (!activityId) {
    return { intervals: [], source: null, sourceActivity: base };
  }

  try {
    const dto = await fetchActivityIntervals(env, activityId);
    const payload = normalizeIntervalsPayload(dto);
    const intervals = extractIntervals(payload)
      .filter((itv) => (itv.end - itv.start) >= minDurationSec)
      .slice(0, maxIntervals);
    if (intervals.length) {
      return {
        intervals,
        source: 'intervals_endpoint',
        sourceActivity: { ...base, ...payload },
      };
    }
  } catch (_) {
    // fall through to activity details
  }

  try {
    const detailed = await fetchActivityWithIntervals(env, activityId);
    const intervals = extractIntervals(detailed)
      .filter((itv) => (itv.end - itv.start) >= minDurationSec)
      .slice(0, maxIntervals);
    if (intervals.length) {
      return {
        intervals,
        source: 'activity_with_intervals',
        sourceActivity: { ...base, ...detailed },
      };
    }
    return {
      intervals: [],
      source: null,
      sourceActivity: { ...base, ...detailed },
    };
  } catch (_) {
    return { intervals: [], source: null, sourceActivity: base };
  }
}


function classifyIntervalDrift(intervalType, driftBpm) {
  if (!Number.isFinite(driftBpm)) return null;
  const type = String(intervalType || "").toUpperCase();
  const hardType = /VO2|REP|INTERVAL|HILL|ANAEROB/.test(type);
  const thresholdTooHard = hardType ? 8 : 7;
  const thresholdOverreaching = hardType ? 11 : 9;
  if (driftBpm >= thresholdOverreaching) return "overreaching";
  if (driftBpm >= thresholdTooHard) return "too_hard";
  return "ok";
}

function formatDriftFlag(flag) {
  if (!flag) return null;
  if (flag === "ok") return "im Rahmen";
  if (flag === "too_hard") return "zu hart";
  if (flag === "overreaching") return "Overreaching";
  return flag;
}

async function computeIntervalMetrics(env, activity, { intervalType } = {}) {
  const stable = await computeIntervalMetricsStable(env, activity);
  if (!stable?.summary) return null;

  const drift = Number(stable.summary.hr_drift_bpm_median);
  const driftPct = Number(stable.summary.hr_drift_pct_median);
  const hrr60 = Number(stable.summary.hrr60_median);
  const decoupling = Number(stable.summary.decoupling_pct_median);
  return {
    HR_Drift_bpm: Number.isFinite(drift) ? drift : null,
    HR_Drift_pct: Number.isFinite(driftPct) ? driftPct : null,
    HRR60_median: Number.isFinite(hrr60) ? hrr60 : null,
    drift_flag: classifyIntervalDrift(intervalType, drift),
    interval_type: intervalType ?? null,
    intensity_source: stable.interval_source || 'intervals_stable',
    decoupling_pct_median: Number.isFinite(decoupling) ? decoupling : null,
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
  const end = parseISODateSafe(endIso) ? endIso : isoDateBerlin(new Date());
  const startIso = isoDate(new Date(new Date(end + "T00:00:00Z").getTime() - (WATCHFACE_LOAD_WINDOW_DAYS - 1) * 86400000));

  // Fetch activities only for this watchface window (klein halten)
  const acts = await fetchIntervalsActivities(env, startIso, end);

  const days = listIsoDaysInclusive(startIso, end); // genau WATCHFACE_LOAD_WINDOW_DAYS
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

  const strengthWindowDays = days.slice(-WATCHFACE_STRENGTH_WINDOW_DAYS);
  const strengthSum7 = strengthWindowDays.reduce((sum, day) => sum + (Math.round(strengthMinByDay[day] || 0)), 0);
  const runSnapshot = await resolveWatchfaceRunSnapshot(env, end);
  const runSum7 = Number.isFinite(runSnapshot?.runValue) ? Math.round(runSnapshot.runValue) : null;
  const runGoal = Number.isFinite(runSnapshot?.runGoal) ? Math.round(runSnapshot.runGoal) : 0;
  const strengthPolicy = evaluateStrengthPolicy(strengthSum7);
  return {
    ok: true,
    endIso: end,
    days,
    runLoad,
    runLoadWindowDays: WATCHFACE_LOAD_WINDOW_DAYS,
    runFloorNow: runSum7,
    runFloorGoal: runGoal,
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

async function resolveWatchfaceRunSnapshot(env) {
  const kv = await readLatestRunSnapshotKv(env);
  if (kv?.runValue != null && kv?.runGoal != null) {
    return { runValue: kv.runValue, runGoal: kv.runGoal, source: "kv" };
  }

  return { runValue: null, runGoal: null, source: "unavailable" };
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
function normalizeKeyToken(raw) {
  return String(raw || "")
    .toLowerCase()
    .trim()
    .replace(/^#+\s*/, "")
    .replace(/^key\s*:\s*/, "key:");
}

function extractTagCandidates(tag) {
  if (tag == null) return [];
  if (typeof tag === "string" || typeof tag === "number") return [String(tag)];
  if (typeof tag === "object") {
    return [tag.name, tag.tag, tag.value, tag.label, tag.key]
      .filter((v) => v != null)
      .map((v) => String(v));
  }
  return [String(tag)];
}

function extractKeyTypeFromText(a) {
  const text = [a?.name, a?.description, a?.workout_name, a?.workout_doc]
    .filter(Boolean)
    .map((v) => String(v))
    .join(" ");
  if (!text) return null;
  const match = text.match(/(?:^|\s)#\s*key\s*:\s*([a-z0-9_:-]+)/i);
  if (!match) return null;
  const typed = normalizeKeyToken(match[1]);
  return typed || null;
}

function hasKeyTag(a) {
  const tagHit = (a?.tags || []).some((t) => {
    const candidates = extractTagCandidates(t);
    return candidates.some((raw) => {
      const s = normalizeKeyToken(raw);
      return s === "key" || s.startsWith("key:");
    });
  });
  if (tagHit) return true;
  const text = [a?.name, a?.description, a?.workout_name, a?.workout_doc]
    .filter(Boolean)
    .map((v) => String(v))
    .join(" ");
  return /(?:^|\s)#\s*key(?:\s*:[a-z0-9_:-]+)?\b/i.test(text);
}

function getKeyType(a) {
  // key:schwelle, key:vo2, key:tempo, ...
  const tags = a?.tags || [];
  for (const t of tags) {
    const candidates = extractTagCandidates(t);
    for (const raw of candidates) {
      const s = normalizeKeyToken(raw);
      if (s.startsWith("key:")) {
        const typed = s.slice(4).trim();
        if (typed && typed !== "key") return typed;
        return "key";
      }
      if (s === "key") return "key";
    }
  }

  const fromText = extractKeyTypeFromText(a);
  if (fromText) return fromText;
  return hasKeyTag(a) ? "key" : null;
}

function getIntervalTypeFromActivity(a) {

  const keyType = getKeyType(a);
  if (!keyType) return null;
  const s = String(keyType).toLowerCase();
  if (s.includes("vo2") || s.includes("v02")) return "vo2";
  if (s.includes("schwelle") || s.includes("threshold") || s.includes("tempo") || s.includes("tdl")) {
    return "threshold";
  }
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

async function determineMode(env, dayIso, debug = false, prefetchedEvents = null) {
  const auth = authHeader(env);
  const events = Array.isArray(prefetchedEvents)
    ? prefetchedEvents
    : await fetchUpcomingEvents(env, auth, debug, 8000, dayIso);
  const races = (events || []).filter((e) => isARaceCategory(e?.category));
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
const INTERVALS_RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);
const INTERVALS_FETCH_TIMEOUT_MS = 8000;
const INTERVALS_MAX_RETRY_DELAY_MS = 250;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function parseRetryAfterMs(value) {
  if (!value) return null;
  const seconds = Number(value);
  if (Number.isFinite(seconds) && seconds >= 0) return Math.round(seconds * 1000);
  const retryAt = Date.parse(value);
  if (Number.isFinite(retryAt)) {
    const delta = retryAt - Date.now();
    return delta > 0 ? delta : 0;
  }
  return null;
}

async function fetchIntervalsWithRetry(url, options = {}, meta = {}) {
  const label = meta.label || "intervals_api";
  const maxRetries = Number.isFinite(meta.maxRetries) ? meta.maxRetries : 3;
  const baseDelayMs = Number.isFinite(meta.baseDelayMs) ? meta.baseDelayMs : 500;

  let attempt = 0;
  while (true) {
    let response;
    const controller = new AbortController();
    const timeoutMs = Number.isFinite(meta.timeoutMs) ? meta.timeoutMs : INTERVALS_FETCH_TIMEOUT_MS;
    const timeoutId = setTimeout(() => controller.abort(`timeout ${timeoutMs}ms`), timeoutMs);
    try {
      response = await fetch(url, { ...options, signal: controller.signal });
    } catch (err) {
      clearTimeout(timeoutId);
      if (attempt >= maxRetries) throw err;
      const delayMs = Math.min(baseDelayMs * Math.pow(2, attempt), INTERVALS_MAX_RETRY_DELAY_MS);
      console.warn(`${label} network error, retrying in ${delayMs}ms`, err);
      attempt++;
      await sleep(delayMs);
      continue;
    }
    clearTimeout(timeoutId);

    if (!INTERVALS_RETRYABLE_STATUS.has(response.status) || attempt >= maxRetries) {
      return response;
    }

    const retryAfterMs = parseRetryAfterMs(response.headers.get("retry-after"));
    const backoffMs = Math.min(baseDelayMs * Math.pow(2, attempt), INTERVALS_MAX_RETRY_DELAY_MS);
    const delayMs = Math.min(Math.max(backoffMs, retryAfterMs ?? 0), INTERVALS_MAX_RETRY_DELAY_MS);
    console.warn(`${label} ${response.status}, retrying in ${delayMs}ms (attempt ${attempt + 1}/${maxRetries})`);
    attempt++;
    await sleep(delayMs);
  }
}

async function fetchIntervalsActivities(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetchIntervalsWithRetry(url, {
    headers: { Authorization: authHeader(env) },
  }, {
    label: "activities",
  });
  if (!r.ok) throw new Error(`activities ${r.status}: ${await r.text()}`);
  return r.json();
}

async function fetchActivityWithIntervals(env, activityId) {
  const url = `https://intervals.icu/api/v1/activity/${activityId}?intervals=true`;
  const r = await fetchIntervalsWithRetry(url, {
    headers: { Authorization: authHeader(env) },
  }, {
    label: `activity ${activityId} intervals`,
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`activity intervals ${r.status}: ${txt.slice(0, 400)}`);
  }
  return r.json();
}

async function fetchActivityIntervals(env, activityId) {
  const url = `https://intervals.icu/api/v1/activity/${activityId}/intervals`;
  const r = await fetchIntervalsWithRetry(url, {
    headers: { Authorization: authHeader(env) },
  }, {
    label: `activity ${activityId} dedicated intervals`,
  });
  if (!r.ok) {
    const txt = await r.text().catch(() => "");
    throw new Error(`activity dedicated intervals ${r.status}: ${txt.slice(0, 400)}`);
  }
  return r.json();
}

async function fetchIntervalsStreams(env, activityId, types) {
  const query = encodeURIComponent(types.join(","));
  const endpoints = [
    `https://intervals.icu/api/v1/activity/${activityId}/streams.json?types=${query}`,
    `https://intervals.icu/api/v1/activity/${activityId}/streams?types=${query}`,
  ];

  let lastErr = null;
  for (const url of endpoints) {
    const r = await fetchIntervalsWithRetry(url, {
      headers: { Authorization: authHeader(env) },
    }, {
      label: `streams ${activityId}`,
    });
    if (r.ok) {
      const raw = await r.json();
      return normalizeStreams(raw);
    }

    const txt = await r.text().catch(() => "");
    lastErr = new Error(`streams ${r.status}: ${txt.slice(0, 400)}`);

    if (r.status !== 404) break;
  }

  throw lastErr || new Error("streams request failed");
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
  const r = await fetchIntervalsWithRetry(url, {
    method: "PUT",
    headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
    body: JSON.stringify(patch),
  }, {
    label: `wellness PUT ${day}`,
  });
  if (!r.ok) throw new Error(`wellness PUT ${day} ${r.status}: ${await r.text()}`);
}

// Events (for NOTE)
async function fetchIntervalsEvents(env, oldest, newest) {
  // local dates (yyyy-MM-dd)
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events?oldest=${oldest}&newest=${newest}`;
  const r = await fetchIntervalsWithRetry(url, {
    headers: { Authorization: authHeader(env) },
  }, {
    label: "events",
  });
  if (!r.ok) throw new Error(`events ${r.status}: ${await r.text()}`);
  return r.json();
}

async function createIntervalsEvent(env, eventObj) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events`;
  const r = await fetchIntervalsWithRetry(url, {
    method: "POST",
    headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
    body: JSON.stringify(eventObj),
  }, {
    label: "events POST",
  });
  if (!r.ok) throw new Error(`events POST ${r.status}: ${await r.text()}`);
  return r.json();
}

async function updateIntervalsEvent(env, eventId, eventObj) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events/${encodeURIComponent(String(eventId))}`;
  const r = await fetchIntervalsWithRetry(url, {
    method: "PUT",
    headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
    body: JSON.stringify(eventObj),
  }, {
    label: `events PUT ${eventId}`,
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
  const res = await fetchIntervalsWithRetry(url, { headers: { Authorization: auth } }, {
    label: "events preview",
  });

  if (!res.ok) {
    if (debug) console.log("⚠️ Event-API fehlgeschlagen:", res.status, "url:", url);
    return [];
  }

  const payload = await res.json();
  const events = Array.isArray(payload) ? payload : Array.isArray(payload?.events) ? payload.events : [];

  const races = events.filter((e) => isARaceCategory(e?.category));

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
