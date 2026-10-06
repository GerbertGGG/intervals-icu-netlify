import { mustEnv } from "./kv.js";

const BASE_URL = "https://intervals.icu/api/v1";

const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);
const FETCH_TIMEOUT_MS = 20000;
const MAX_RETRIES = 2;
const BASE_DELAY_MS = 500;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function authHeader(env) {
  return "Basic " + btoa(`API_KEY:${mustEnv(env, "INTERVALS_API_KEY")}`);
}

async function fetchWithRetry(url, options = {}, label = "intervals_api") {
  let attempt = 0;
  while (true) {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(`timeout ${FETCH_TIMEOUT_MS}ms`), FETCH_TIMEOUT_MS);
    let response;
    try {
      response = await fetch(url, { ...options, signal: controller.signal });
    } catch (err) {
      clearTimeout(timeoutId);
      if (attempt >= MAX_RETRIES) throw err;
      const delayMs = BASE_DELAY_MS * 2 ** attempt;
      console.warn(`${label} network error, retrying in ${delayMs}ms`, err);
      attempt++;
      await sleep(delayMs);
      continue;
    }
    clearTimeout(timeoutId);
    if (!RETRYABLE_STATUS.has(response.status) || attempt >= MAX_RETRIES) return response;
    const delayMs = BASE_DELAY_MS * 2 ** attempt;
    console.warn(`${label} ${response.status}, retrying in ${delayMs}ms (attempt ${attempt + 1}/${MAX_RETRIES})`);
    attempt++;
    await sleep(delayMs);
  }
}

export async function fetchIntervalsActivities(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/activities?oldest=${oldest}&newest=${newest}`;
  const r = await fetchWithRetry(url, { headers: { Authorization: authHeader(env) } }, "activities");
  if (!r.ok) throw new Error(`activities ${r.status}: ${await r.text()}`);
  return r.json();
}

// Returns the wellness list for [oldest, newest] (one entry per day that has data),
// as opposed to fetchIntervalsWellnessDay which targets a single day.
export async function fetchIntervalsWellnessRange(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/wellness?oldest=${oldest}&newest=${newest}`;
  const r = await fetchWithRetry(url, { headers: { Authorization: authHeader(env) } }, "wellness range");
  if (!r.ok) throw new Error(`wellness range ${r.status}: ${await r.text()}`);
  const data = await r.json();
  return Array.isArray(data) ? data : Array.isArray(data?.wellness) ? data.wellness : [];
}

export async function fetchIntervalsEvents(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events?oldest=${oldest}&newest=${newest}`;
  const r = await fetchWithRetry(url, { headers: { Authorization: authHeader(env) } }, "events");
  if (!r.ok) throw new Error(`events ${r.status}: ${await r.text()}`);
  return r.json();
}

// Intervall-Splits einer Aktivitaet (IntervalsDTO: icu_intervals mit type WORK/RECOVERY).
export async function fetchIntervalsActivityIntervals(env, activityId) {
  const url = `${BASE_URL}/activity/${encodeURIComponent(activityId)}/intervals`;
  const r = await fetchWithRetry(url, { headers: { Authorization: authHeader(env) } }, "activity intervals");
  if (!r.ok) throw new Error(`activity intervals ${r.status}: ${await r.text()}`);
  return r.json();
}

export async function putWellnessDay(env, day, patch) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/wellness/${day}`;
  const r = await fetchWithRetry(
    url,
    {
      method: "PUT",
      headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
      body: JSON.stringify(patch),
    },
    `wellness PUT ${day}`,
  );
  if (!r.ok) throw new Error(`wellness PUT ${day} ${r.status}: ${await r.text()}`);
}

// Rohe Sportart-Einstellungen (Schwellen, Zonen, FTP). Best effort: null bei jedem Fehler.
export async function fetchIntervalsSportSettings(env) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const resp = await fetch(`${BASE_URL}/athlete/${uid}/sport-settings`, { headers: { Authorization: authHeader(env) } });
    if (!resp.ok) return null;
    const data = await resp.json();
    return Array.isArray(data) ? data : null;
  } catch {
    return null;
  }
}
