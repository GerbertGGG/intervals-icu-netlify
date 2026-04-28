import {
  WATCHFACE_ERROR_HEADERS,
  WATCHFACE_JSON_HEADERS,
  WATCHFACE_PREFLIGHT_HEADERS,
  clampInt,
  getSearchParamAny,
  json,
  parseBooleanParam,
  parseReportVerbosity,
} from "./http-helpers.js";
import { diffDays, isIsoDate, isoDate, isoDateBerlin } from "./date-utils.js";

const REPORT_VERBOSITY_VALUES = new Set(["coach", "diagnose", "debug"]);

export function isWatchfacePath(pathname) {
  return pathname === "/watchface" || pathname === "/watchface/";
}

export function isWeeklyMailTestPath(pathname) {
  return pathname === "/test-strength-mail" || pathname === "/test-weekly-email";
}

export async function withWorkerErrorBoundary(fn) {
  try {
    return await fn();
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

export async function handleWatchfaceRequest(req, url, env, deps) {
  const { buildWatchfacePayload } = deps;
  if (req.method === "OPTIONS") {
    return new Response("", {
      status: 204,
      headers: WATCHFACE_PREFLIGHT_HEADERS,
    });
  }

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

export async function handleSyncRequest(url, env, ctx, deps) {
  const { syncRange } = deps;
  const syncRequest = parseSyncRequest(url.searchParams);
  if (!syncRequest.ok) return syncRequest.response;

  const {
    write,
    debug,
    reportVerbosity,
    oldest,
    newest,
    warmupSkipSec,
    raceStartOverrideIso,
    blockStartOverrideIso,
    blockStartOverrideDerivedFromOldest,
    manualFocus,
  } = syncRequest;

  const syncOptions = {
    raceStartOverrideIso,
    blockStartOverrideIso,
    blockStartOverrideDerivedFromOldest,
    reportVerbosity,
    manualFocus,
  };

  if (debug) {
    return runSyncDebugMode(env, {
      oldest,
      newest,
      write,
      warmupSkipSec,
      syncOptions,
      raceStartOverrideIso,
      blockStartOverrideIso,
      blockStartOverrideDerivedFromOldest,
      manualFocus,
      syncRange,
    });
  }

  ctx?.waitUntil?.(
    (async () => {
      await syncRange(env, oldest, newest, write, false, warmupSkipSec, syncOptions);
    })().catch((e) => {
      console.error("sync/model job failed", e);
    })
  );

  return json({
    ok: true,
    oldest,
    newest,
    write,
    warmupSkipSec,
    raceStartOverrideIso,
    blockStartOverrideIso,
    blockStartOverrideDerivedFromOldest,
    reportVerbosity,
    manualFocus,
  });
}

export async function handleWeeklyMailTestRequest(url, env, deps) {
  const { sendWeeklyStrengthMail } = deps;
  const dryRun = parseBooleanParam(url.searchParams, "dry") || parseBooleanParam(url.searchParams, "dry_run");
  const blockRaw = String(url.searchParams.get("block") || "BASE").toUpperCase();
  const allowedBlocks = new Set(["BASE", "BUILD", "RACE", "RESET"]);
  const block = allowedBlocks.has(blockRaw) ? blockRaw : "BASE";
  const strengthCountThisWeek = clampInt(url.searchParams.get("strength_count") ?? "0", 0, 20);
  const toOverride = String(url.searchParams.get("to") || "").trim() || null;

  const result = await sendWeeklyStrengthMail(
    env,
    { block },
    strengthCountThisWeek,
    { dryRun, toOverride }
  );
  return json({
    ok: true,
    endpoint: url.pathname,
    dryRun,
    block,
    strengthCountThisWeek,
    ...(toOverride ? { toOverride } : {}),
    mail: result,
  });
}

function parseSyncRequest(searchParams) {
  const write = parseBooleanParam(searchParams, "write");
  const debug = parseBooleanParam(searchParams, "debug");
  const reportVerbosity = parseReportVerbosity(searchParams, { debug });

  const date = searchParams.get("date");
  const from = searchParams.get("from");
  const to = searchParams.get("to");
  const days = clampInt(searchParams.get("days") ?? "14", 1, 31);
  const manualFocusRaw = searchParams.get("focus") ?? null;
  const manualFocus = ["kraft", "longrun", "frequenz", "erholung", "spezifik"].includes(manualFocusRaw)
    ? manualFocusRaw
    : null;

  const warmupSkipSec = clampInt(searchParams.get("warmup_skip") ?? "600", 0, 1800);
  const raceStartParamRaw = getSearchParamAny(searchParams, [
    "race_start",
    "race_start_override",
    "race_start_iso",
    "racestart",
    "raceStart",
  ]);
  const raceStartOverrideIso = raceStartParamRaw && isIsoDate(raceStartParamRaw) ? raceStartParamRaw : null;
  const blockStartParamRaw = getSearchParamAny(searchParams, [
    "block_start",
    "block_start_override",
    "block_start_iso",
    "blockstart",
    "blockStart",
  ]);
  let blockStartOverrideIso = blockStartParamRaw && isIsoDate(blockStartParamRaw) ? blockStartParamRaw : null;
  let blockStartOverrideDerivedFromOldest = false;

  let oldest;
  let newest;
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
    return { ok: false, response: json({ ok: false, error: "Invalid date format (YYYY-MM-DD)" }, 400) };
  }
  if (newest < oldest) {
    return { ok: false, response: json({ ok: false, error: "`to` must be >= `from`" }, 400) };
  }
  if (diffDays(oldest, newest) > 31) {
    return { ok: false, response: json({ ok: false, error: "Max range is 31 days" }, 400) };
  }

  if (raceStartParamRaw && !raceStartOverrideIso) {
    return { ok: false, response: json({ ok: false, error: "Invalid race_start format (YYYY-MM-DD)" }, 400) };
  }
  if (blockStartParamRaw && !blockStartOverrideIso) {
    return { ok: false, response: json({ ok: false, error: "Invalid block_start format (YYYY-MM-DD)" }, 400) };
  }

  if (!write && debug && !blockStartOverrideIso) {
    blockStartOverrideIso = oldest;
    blockStartOverrideDerivedFromOldest = true;
  }

  return {
    ok: true,
    write,
    debug,
    reportVerbosity: REPORT_VERBOSITY_VALUES.has(reportVerbosity) ? reportVerbosity : "coach",
    oldest,
    newest,
    warmupSkipSec,
    raceStartOverrideIso,
    blockStartOverrideIso,
    blockStartOverrideDerivedFromOldest,
    manualFocus,
  };
}

async function runSyncDebugMode(env, options) {
  const {
    oldest,
    newest,
    write,
    warmupSkipSec,
    syncOptions,
    raceStartOverrideIso,
    blockStartOverrideIso,
    blockStartOverrideDerivedFromOldest,
    manualFocus,
    syncRange,
  } = options;

  try {
    const result = await syncRange(env, oldest, newest, write, true, warmupSkipSec, syncOptions);
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
        blockStartOverrideDerivedFromOldest,
        manualFocus,
      },
      500
    );
  }
}
