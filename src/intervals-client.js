import { isoDate } from "./date-utils.js";
import { mustEnv, hasKv, readKvJson, writeKvJson } from "./kv.js";

export const BASE_URL = "https://intervals.icu/api/v1";

const RETRYABLE_STATUS = new Set([429, 500, 502, 503, 504]);
const FETCH_TIMEOUT_MS = 20000;
const MAX_RETRIES = 2;
const BASE_DELAY_MS = 500;

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function authHeader(env) {
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

export async function fetchIntervalsEvents(env, oldest, newest) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events?oldest=${oldest}&newest=${newest}`;
  const r = await fetchWithRetry(url, { headers: { Authorization: authHeader(env) } }, "events");
  if (!r.ok) throw new Error(`events ${r.status}: ${await r.text()}`);
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

// Returns null if the athlete has no wellness row for that day (e.g. 404) instead
// of throwing, since that's an expected/normal state, not a failure.
export async function fetchIntervalsWellnessDay(env, day) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/wellness/${day}`;
  const r = await fetchWithRetry(url, { headers: { Authorization: authHeader(env) } }, `wellness GET ${day}`);
  if (!r.ok) return null;
  return r.json();
}

export async function createIntervalsEvent(env, eventObj) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events`;
  const r = await fetchWithRetry(
    url,
    {
      method: "POST",
      headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
      body: JSON.stringify(eventObj),
    },
    "events POST",
  );
  if (!r.ok) throw new Error(`events POST ${r.status}: ${await r.text()}`);
  return r.json();
}

export async function updateIntervalsEvent(env, eventId, eventObj) {
  const athleteId = mustEnv(env, "ATHLETE_ID");
  const url = `${BASE_URL}/athlete/${athleteId}/events/${encodeURIComponent(String(eventId))}`;
  const r = await fetchWithRetry(
    url,
    {
      method: "PUT",
      headers: { Authorization: authHeader(env), "Content-Type": "application/json" },
      body: JSON.stringify(eventObj),
    },
    `events PUT ${eventId}`,
  );
  if (!r.ok) throw new Error(`events PUT ${r.status}: ${await r.text()}`);
  return r.json();
}

function toNoteDescription(text) {
  return String(text ?? "")
    .replace(/\r\n/g, "\n")
    .replace(/\r/g, "\n")
    .split("\n")
    .join("<br />\n");
}

// Creates or updates a NOTE calendar event, matched by external_id within that day.
// No-ops if the existing note already has the same description (avoids needless PUTs).
export async function upsertIntervalsNote(env, { dayIso, externalId, name, description, color = "blue" }) {
  const events = await fetchIntervalsEvents(env, dayIso, dayIso);
  const list = Array.isArray(events) ? events : Array.isArray(events?.events) ? events.events : [];
  const existing = list.find((e) => String(e?.external_id || "") === externalId) || null;

  const body = {
    category: "NOTE",
    start_date_local: `${dayIso}T00:00:00`,
    name,
    description: toNoteDescription(description),
    color,
    external_id: externalId,
  };

  if (existing?.id) {
    if (String(existing?.description || "") === body.description) return { id: existing.id, updated: false };
    await updateIntervalsEvent(env, existing.id, body);
    return { id: existing.id, updated: true };
  }

  const created = await createIntervalsEvent(env, body);
  return { id: created?.id ?? null, updated: true, created: true };
}

const MAX_HR_KV_PREFIX = "vdot:maxhr:";
const MAX_HR_MAX_AGE_MS = 24 * 60 * 60 * 1000;

function maxHrKvKey(env) {
  return `${MAX_HR_KV_PREFIX}${mustEnv(env, "ATHLETE_ID")}`;
}

export async function loadCachedMaxHr(env) {
  if (!hasKv(env)) return null;
  try {
    const cached = await readKvJson(env, maxHrKvKey(env));
    if (!cached?.ts || !cached?.maxHr) return null;
    if (Date.now() - cached.ts > MAX_HR_MAX_AGE_MS) return null;
    return Number(cached.maxHr) || null;
  } catch {
    return null;
  }
}

export async function saveCachedMaxHr(env, maxHr) {
  if (!hasKv(env)) return;
  try {
    await writeKvJson(env, maxHrKvKey(env), { ts: Date.now(), maxHr });
  } catch {}
}

export async function fetchAndCacheMaxHr(env) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const resp = await fetch(`${BASE_URL}/athlete/${uid}`, { headers: { Authorization: authHeader(env) } });
    if (!resp.ok) return null;
    const data = await resp.json();
    const maxHr = Number(data?.max_hr || data?.maxHr || data?.hrMax || 0);
    if (maxHr > 100) {
      saveCachedMaxHr(env, maxHr).catch(() => {});
      return maxHr;
    }
    return null;
  } catch {
    return null;
  }
}

export async function fetchRunPaceBenchmarks(env) {
  try {
    if (!env?.INTERVALS_API_KEY || !env?.ATHLETE_ID) return null;
    const uid = mustEnv(env, "ATHLETE_ID");
    const now = new Date();
    const nowIso = isoDate(now);
    const oldest = isoDate(new Date(now.getTime() - 56 * 86400000));
    const url = `${BASE_URL}/athlete/${uid}/activity-pace-curves?type=Run&distances=1000,5000,10000,21097&oldest=${oldest}&newest=${nowIso}`;
    const data = await fetch(url, { headers: { Authorization: authHeader(env) } })
      .then((r) => (r.ok ? r.json() : null))
      .catch(() => null);
    if (!data) return null;

    const parsePace = (dist) => {
      if (Array.isArray(data)) {
        const entry = data.find((d) => Math.abs(Number(d.distance || d.dist || 0) - dist) < 100);
        if (!entry) return null;
        const s = Number(entry.secs || entry.time || entry.value);
        return Number.isFinite(s) && s > 0 ? s : null;
      }
      if (data && typeof data === "object") {
        const v = data[String(dist)];
        return v != null ? Number(v) || null : null;
      }
      return null;
    };

    const current = {};
    for (const dist of [1000, 5000, 10000, 21097]) {
      const secs = parsePace(dist);
      if (secs != null) current[dist] = secs;
    }
    return Object.keys(current).length > 0 ? { current } : null;
  } catch {
    return null;
  }
}
