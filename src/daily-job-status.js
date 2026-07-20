import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";

// Lets the once-per-day scheduled jobs in index.js (daily Formcheck note, Monday
// weekly-progress note, Monday report email) retry on a later 30-min tick within the
// same 07:00-21:00 Berlin window if the very first tick of the day (07:00-07:29)
// never fired or the job errored - previously they only ever got that one shot per
// day, so a missed/failed first tick meant the job silently didn't run at all until
// the same time next day (or next Monday).
const DAILY_JOB_STATUS_KV_PREFIX = "dailyjob:status:";

function dailyJobStatusKvKey(env, jobKey) {
  return `${DAILY_JOB_STATUS_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}:${jobKey}`;
}

export async function hasSucceededToday(env, jobKey, todayIso) {
  if (!hasKv(env)) return false;
  const status = await readKvJson(env, dailyJobStatusKvKey(env, jobKey)).catch(() => null);
  return status?.lastRunDate === todayIso;
}

export async function recordSucceededToday(env, jobKey, todayIso) {
  if (!hasKv(env)) return;
  try {
    await writeKvJson(env, dailyJobStatusKvKey(env, jobKey), { lastRunDate: todayIso, ranAt: new Date().toISOString() });
  } catch {}
}
