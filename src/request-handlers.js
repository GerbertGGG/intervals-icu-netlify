import { clampInt, json, parseBooleanParam } from "./http-helpers.js";
import { diffDays, isIsoDate, isoDate } from "./date-utils.js";
import { isIntervalsEnabled } from "./kv.js";

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
  if (!isIntervalsEnabled(env)) {
    return json({ ok: true, skipped: true, reason: "INTERVALS_ENABLED=false" });
  }

  const { syncRange } = deps;
  const syncRequest = parseSyncRequest(url.searchParams);
  if (!syncRequest.ok) return syncRequest.response;

  const { write, debug, oldest, newest, includeYazio } = syncRequest;
  const syncOptions = { includeYazio };

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

  return json({ ok: true, oldest, newest, write });
}

function parseSyncRequest(searchParams) {
  const write = parseBooleanParam(searchParams, "write");
  const debug = parseBooleanParam(searchParams, "debug");

  const date = searchParams.get("date");
  const from = searchParams.get("from");
  const to = searchParams.get("to");
  const days = clampInt(searchParams.get("days") ?? "14", 1, 31);

  // Yazio wird sonst nur vom Cron geholt; ein manueller Aufruf kann es hiermit anstoßen.
  const includeYazio = parseBooleanParam(searchParams, "yazio");

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

  return { ok: true, write, debug, oldest, newest, includeYazio };
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
