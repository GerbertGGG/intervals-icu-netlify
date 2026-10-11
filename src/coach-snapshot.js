import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";

// Tagesaussage des Coaching-Tasks. Der Task schickt sie per PUT /api/coach (Bearer DASHBOARD_TOKEN), per MCP-Tool
// save_coach oder als coach.json auf den GitHub-Branch; das Dashboard zeigt sie als Karte, das Widget als Hinweiszeile.
//
// Body: { fetchedAt: ISO-Zeitstempel,
//         note: string (max 160, Hinweis fuer heute, steht im Widget),
//         headline?: string (max 80, Kurzurteil),
//         text?: string (max 8000, ganzer Bericht fuers Dashboard) }

export const COACH_KV_KEY = "dashboard:coach";
export const COACH_MAX_AGE_HOURS = 36;
const MAX_BODY_BYTES = 40_000;
const MAX_NOTE = 160;
const MAX_HEADLINE = 80;
const MAX_TEXT = 8000;

export function validateCoach(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const note = typeof body?.note === "string" ? body.note.trim() : "";
  if (!note) return { error: "note fehlt" };
  if (note.length > MAX_NOTE) return { error: `note länger als ${MAX_NOTE} Zeichen` };
  const headline = typeof body?.headline === "string" ? body.headline.trim() : "";
  if (headline.length > MAX_HEADLINE) return { error: `headline länger als ${MAX_HEADLINE} Zeichen` };
  const text = typeof body?.text === "string" ? body.text.trim() : "";
  if (text.length > MAX_TEXT) return { error: `text länger als ${MAX_TEXT} Zeichen` };
  return { value: { fetchedAt: new Date(fetchedAt).toISOString(), note, headline: headline || null, text: text || null } };
}

export async function readCoach(env) {
  return readKvJson(env, COACH_KV_KEY).catch(() => null);
}

// Das Widget zeigt nur eine frische Aussage; eine von gestern Mittag wuerde heute falsch wirken.
export function isFreshCoach(coach, nowMs) {
  const at = Date.parse(coach?.fetchedAt);
  return Number.isFinite(at) && Number.isFinite(nowMs) && nowMs - at >= 0 && nowMs - at <= COACH_MAX_AGE_HOURS * 3600_000;
}

export async function handleCoachRequest(req, env, isAuthorized) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  if (req.method !== "PUT") return json({ ok: false, error: "Nur PUT erlaubt" }, 405, { ...headers, allow: "PUT" });
  if (!env?.KV) return json({ ok: false, error: "KV nicht verfügbar" }, 503, headers);
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return json({ ok: false, error: "Body zu groß" }, 413, headers);
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    return json({ ok: false, error: "Kein gültiges JSON" }, 400, headers);
  }
  const { value, error } = validateCoach(body);
  if (error) return json({ ok: false, error }, 400, headers);
  await writeKvJson(env, COACH_KV_KEY, value);
  return json({ ok: true, chars: value.note.length }, 200, headers);
}
