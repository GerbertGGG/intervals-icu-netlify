import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";

// Tracks the recent-form report email (sent by the Monday cron job in index.js), so a
// silent send failure - caught and only console.error'd there so it never blocks the
// rest of the scheduled job - is still visible afterwards via /status instead of only
// in ephemeral Workers logs.
const EMAIL_STATUS_KV_PREFIX = "email:status:";

function emailStatusKvKey(env) {
  return `${EMAIL_STATUS_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}

export async function readEmailStatus(env) {
  if (!hasKv(env)) return null;
  return readKvJson(env, emailStatusKvKey(env)).catch(() => null);
}

export async function recordEmailSuccess(env) {
  if (!hasKv(env)) return;
  try {
    const existing = (await readKvJson(env, emailStatusKvKey(env))) || {};
    await writeKvJson(env, emailStatusKvKey(env), {
      ...existing,
      lastSuccessAt: new Date().toISOString(),
      consecutiveErrors: 0,
    });
  } catch {}
}

export async function recordEmailError(env, errorMsg) {
  if (!hasKv(env)) return;
  try {
    const existing = (await readKvJson(env, emailStatusKvKey(env))) || {};
    await writeKvJson(env, emailStatusKvKey(env), {
      ...existing,
      lastErrorAt: new Date().toISOString(),
      lastErrorMsg: String(errorMsg).slice(0, 500),
      consecutiveErrors: (existing.consecutiveErrors || 0) + 1,
      totalErrors: (existing.totalErrors || 0) + 1,
    });
  } catch {}
}
