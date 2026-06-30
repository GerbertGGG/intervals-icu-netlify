import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";

const SYNC_STATUS_KV_PREFIX = "sync:status:";

function syncStatusKvKey(env) {
  return `${SYNC_STATUS_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}

export async function readSyncStatus(env) {
  if (!hasKv(env)) return null;
  return readKvJson(env, syncStatusKvKey(env)).catch(() => null);
}

export async function recordSyncSuccess(env) {
  if (!hasKv(env)) return;
  try {
    const existing = (await readKvJson(env, syncStatusKvKey(env))) || {};
    await writeKvJson(env, syncStatusKvKey(env), {
      ...existing,
      lastSuccessAt: new Date().toISOString(),
      consecutiveErrors: 0,
    });
  } catch {}
  if (env?.HEALTHCHECK_URL) {
    fetch(env.HEALTHCHECK_URL).catch(() => {});
  }
}

export async function recordSyncError(env, errorMsg) {
  if (!hasKv(env)) return;
  try {
    const existing = (await readKvJson(env, syncStatusKvKey(env))) || {};
    await writeKvJson(env, syncStatusKvKey(env), {
      ...existing,
      lastErrorAt: new Date().toISOString(),
      lastErrorMsg: String(errorMsg).slice(0, 500),
      consecutiveErrors: (existing.consecutiveErrors || 0) + 1,
      totalErrors: (existing.totalErrors || 0) + 1,
    });
  } catch {}
}
