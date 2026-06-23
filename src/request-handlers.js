import { clampInt, getSearchParamAny, json, parseBooleanParam } from "./http-helpers.js";
import { diffDays, isIsoDate, isoDate, listIsoDaysInclusive } from "./date-utils.js";
import { buildWeeklyProgressReport } from "./weekly-progress.js";

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

export async function handleSyncRequest(url, env, ctx, deps) {
  const { syncRange } = deps;
  const syncRequest = parseSyncRequest(url.searchParams);
  if (!syncRequest.ok) return syncRequest.response;

  const { write, debug, oldest, newest, raceStartOverrideIso, blockStartOverrideIso, blockOverride } = syncRequest;
  const syncOptions = { raceStartOverrideIso, blockStartOverrideIso, blockOverride };

  if (debug) {
    return runSyncDebugMode(env, { oldest, newest, write, syncOptions, syncRange });
  }

  ctx?.waitUntil?.(
    (async () => {
      await syncRange(env, oldest, newest, write, false, syncOptions);
    })().catch((e) => {
      console.error("sync job failed", e);
    })
  );

  return json({ ok: true, oldest, newest, write, raceStartOverrideIso, blockStartOverrideIso, blockOverride });
}

// Cloudflare caps fetch() calls per Worker invocation ("subrequests"). A wide
// weeks=N backfill needs more wellness PUTs than fit in one invocation, so each
// call here only processes one safe slice, then schedules the next slice via a
// self-fetch (a fresh invocation gets its own fresh subrequest budget) instead
// of looping over the whole range synchronously.
export async function handleBackfillProfileRequest(url, env, ctx, deps) {
  const { syncRange } = deps;
  const chunkDays = 14;

  const continueOldest = url.searchParams.get("continue_oldest");
  const continueNewest = url.searchParams.get("continue_newest");

  let oldest;
  let finalNewest;
  if (continueOldest && continueNewest && isIsoDate(continueOldest) && isIsoDate(continueNewest)) {
    oldest = continueOldest;
    finalNewest = continueNewest;
  } else {
    const weeks = clampInt(url.searchParams.get("weeks") ?? "12", 1, 52);
    finalNewest = isoDate(new Date());
    oldest = isoDate(new Date(Date.now() - weeks * 7 * 86400000));
  }

  const remainingDays = listIsoDaysInclusive(oldest, finalNewest);
  const chunkNewest = remainingDays[Math.min(chunkDays - 1, remainingDays.length - 1)];
  const hasMore = chunkNewest < finalNewest;

  ctx.waitUntil(
    (async () => {
      await syncRange(env, oldest, chunkNewest, true, false, {});
      if (hasMore) {
        const nextOldest = isoDate(new Date(new Date(chunkNewest + "T00:00:00Z").getTime() + 86400000));
        const continueUrl = new URL(`${url.origin}${url.pathname}`);
        continueUrl.searchParams.set("continue_oldest", nextOldest);
        continueUrl.searchParams.set("continue_newest", finalNewest);
        await fetch(continueUrl.toString()).catch((e) => console.error("backfill continuation failed", String(e?.message ?? e)));
      }
    })().catch((e) => console.error("backfill chunk failed", { oldest, chunkNewest, error: String(e?.message ?? e) })),
  );

  return json({ ok: true, scheduled: true, chunkOldest: oldest, chunkNewest, finalNewest, hasMore });
}

export async function handleWeeklyProgressRequest(url, env, ctx, deps) {
  const { buildWeeklyProgressReport: buildReport } = deps;
  const write = parseBooleanParam(url.searchParams, "write");
  const debug = parseBooleanParam(url.searchParams, "debug");
  const dateParam = url.searchParams.get("date");
  const todayIso = dateParam && isIsoDate(dateParam) ? dateParam : isoDate(new Date());

  if (debug) {
    try {
      const result = await buildReport(env, todayIso, { write });
      return json(result);
    } catch (e) {
      return json(
        {
          ok: false,
          error: "Worker exception",
          message: String(e?.message ?? e),
          stack: String(e?.stack ?? ""),
          todayIso,
          write,
        },
        500
      );
    }
  }

  ctx?.waitUntil?.(
    buildReport(env, todayIso, { write }).catch((e) => {
      console.error("weekly progress job failed", e);
    })
  );

  return json({ ok: true, todayIso, write });
}

function parseSyncRequest(searchParams) {
  const write = parseBooleanParam(searchParams, "write");
  const debug = parseBooleanParam(searchParams, "debug");

  const date = searchParams.get("date");
  const from = searchParams.get("from");
  const to = searchParams.get("to");
  const days = clampInt(searchParams.get("days") ?? "14", 1, 31);

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
  const blockStartOverrideIso = blockStartParamRaw && isIsoDate(blockStartParamRaw) ? blockStartParamRaw : null;

  const blockOverrideRaw = getSearchParamAny(searchParams, ["block_override", "block", "force_block"]);
  const VALID_BLOCKS = ["BASE", "BUILD", "RACE", "RESET"];
  const blockOverride = blockOverrideRaw && VALID_BLOCKS.includes(blockOverrideRaw.toUpperCase()) ? blockOverrideRaw.toUpperCase() : null;

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
  if (blockOverrideRaw && !blockOverride) {
    return { ok: false, response: json({ ok: false, error: "Invalid block_override (BASE|BUILD|RACE|RESET)" }, 400) };
  }

  return { ok: true, write, debug, oldest, newest, raceStartOverrideIso, blockStartOverrideIso, blockOverride };
}

async function runSyncDebugMode(env, options) {
  const { oldest, newest, write, syncOptions, syncRange } = options;
  try {
    const result = await syncRange(env, oldest, newest, write, true, syncOptions);
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
      },
      500
    );
  }
}
