import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";

// Studien-Check der Woche (Sonntagsabschnitt des Coaching-Berichts). Der Coaching-Task schickt ihn
// per PUT /api/studie (Bearer DASHBOARD_TOKEN) hierher, das Dashboard liest ihn aus dem KV.
//
// Body: { fetchedAt: ISO-Zeitstempel, week?: "2026-09-27", title?: string, text: string (max 6000),
//         source: string (Quelle, z. B. Autor/Journal/Jahr), sourceUrl?: "https://..." }

export const STUDIE_KV_KEY = "dashboard:studie";
const MAX_BODY_BYTES = 30_000;
const MAX_TEXT = 6000;

export function validateStudie(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const text = typeof body?.text === "string" ? body.text.trim() : "";
  if (!text) return { error: "text fehlt" };
  if (text.length > MAX_TEXT) return { error: `text länger als ${MAX_TEXT} Zeichen` };
  const source = typeof body?.source === "string" ? body.source.trim().slice(0, 300) : "";
  if (!source) return { error: "source (Quelle) fehlt: ohne Quelle zeige ich den Abschnitt nicht" };
  let sourceUrl = null;
  if (body?.sourceUrl) {
    try {
      const u = new URL(String(body.sourceUrl));
      if (u.protocol !== "https:") return { error: "sourceUrl muss https sein" };
      sourceUrl = u.toString();
    } catch {
      return { error: "sourceUrl ungültig" };
    }
  }
  const week = /^\d{4}-\d{2}-\d{2}$/.test(String(body?.week)) ? String(body.week) : null;
  const title = typeof body?.title === "string" && body.title.trim() ? body.title.trim().slice(0, 200) : null;
  return { value: { fetchedAt: new Date(fetchedAt).toISOString(), week, title, text, source, sourceUrl } };
}

export async function readStudie(env) {
  return readKvJson(env, STUDIE_KV_KEY).catch(() => null);
}

export async function handleStudieRequest(req, env, isAuthorized) {
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
  const { value, error } = validateStudie(body);
  if (error) return json({ ok: false, error }, 400, headers);
  await writeKvJson(env, STUDIE_KV_KEY, value);
  return json({ ok: true, chars: value.text.length }, 200, headers);
}
