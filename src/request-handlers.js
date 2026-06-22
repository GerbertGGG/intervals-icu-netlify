import { clampInt, getSearchParamAny, json, parseBooleanParam } from "./http-helpers.js";
import { diffDays, isIsoDate, isoDate, listIsoDaysInclusive } from "./date-utils.js";

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

  const { write, debug, oldest, newest, raceStartOverrideIso, blockStartOverrideIso } = syncRequest;
  const syncOptions = { raceStartOverrideIso, blockStartOverrideIso };

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

  return json({ ok: true, oldest, newest, write, raceStartOverrideIso, blockStartOverrideIso });
}

export async function handleBackfillProfileRequest(url, env, deps) {
  const { syncRange } = deps;
  const weeks = clampInt(url.searchParams.get("weeks") ?? "12", 1, 52);
  const newest = isoDate(new Date());
  const oldest = isoDate(new Date(Date.now() - weeks * 7 * 86400000));
  const chunkDays = 14;
  const allDays = listIsoDaysInclusive(oldest, newest);

  const chunks = [];
  for (let i = 0; i < allDays.length; i += chunkDays) {
    const chunkOldest = allDays[i];
    const chunkNewest = allDays[Math.min(i + chunkDays - 1, allDays.length - 1)];
    const result = await syncRange(env, chunkOldest, chunkNewest, true, false, {});
    chunks.push({ oldest: chunkOldest, newest: chunkNewest, days: result.days.length });
  }

  return json({ ok: true, weeks, oldest, newest, chunks });
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

  return { ok: true, write, debug, oldest, newest, raceStartOverrideIso, blockStartOverrideIso };
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
