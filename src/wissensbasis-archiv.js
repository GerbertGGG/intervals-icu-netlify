import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";
import { gate } from "./decisions-snapshot.js";
import { readWissensbasis } from "./wissensbasis-snapshot.js";
import { isoDateBerlin } from "./date-utils.js";

// Archiv der Wissensbasis-Punkte: Gerbert blendet einen Punkt im Dashboard selbst aus (POST /api/wissensbasis/:id/archive, .../restore).
// Eigener KV-Schlüssel, nur beim Archivieren und Wiederherstellen geschrieben (nie beim Lesen). Die gespeicherte Wissensbasis bleibt
// unverändert; save_wissensbasis ersetzt weiter den ganzen Stand, Punkte mit archivierter id werden dann nur nicht angezeigt.
// Punkte ohne id bekommen beim Lesen eine abgeleitete, stabile id (itemId); gespeichert wird sie nur hier im Archiv-Eintrag.
//
// KV: { items: [{ id, title (erste 80 Zeichen des Textes), source, archivedAt (YYYY-MM-DD, Berlin), reason? (max 160) }] }, höchstens 100, ältester zuerst

export const WB_ARCHIV_KV_KEY = "dashboard:wissensbasis-archiv";
export const MAX_ARCHIV = 100;
const MAX_REASON = 160;
const MAX_BODY_BYTES = 4_000;

// FNV-1a (32 Bit): kurz, synchron, ohne Crypto; reicht als Kennung innerhalb von höchstens ~100 Punkten
function shortHash(s) {
  let h = 0x811c9dc5;
  for (let i = 0; i < s.length; i++) { h ^= s.charCodeAt(i); h = Math.imul(h, 0x01000193) >>> 0; }
  return h.toString(16).padStart(8, "0");
}

// Stabile id eines Punktes: die id des Coaches, sonst Kurzhash aus Quelle + Anfang des Textes
export function itemId(it) {
  if (it?.id) return it.id;
  const norm = (v) => String(v ?? "").toLowerCase().replace(/\s+/g, " ").trim();
  return "wb-" + shortHash(`${norm(it?.source)}|${norm(it?.text).slice(0, 40)}`);
}

export const archivList = (stored) => (Array.isArray(stored?.items) ? stored.items : []);
export const archivIds = (stored) => new Set(archivList(stored).map((x) => x.id));

export async function readArchiv(env) {
  return readKvJson(env, WB_ARCHIV_KV_KEY).catch(() => null);
}

// Wissensbasis ohne archivierte Punkte (Abschnitte ohne Punkte entfallen). annotate: jeder Punkt bekommt aid = itemId (nur Anzeige, nie gespeichert).
export function visibleWissensbasis(wb, archiv, annotate = false) {
  if (!wb || !Array.isArray(wb.sections)) return wb;
  const hidden = archivIds(archiv);
  if (!hidden.size && !annotate) return wb;
  const sections = [];
  for (const s of wb.sections) {
    const items = [];
    for (const it of s.items ?? []) {
      const aid = itemId(it);
      if (hidden.has(aid)) continue;
      items.push(annotate ? { ...it, aid } : it);
    }
    if (items.length) sections.push({ ...s, items });
  }
  return { ...wb, sections };
}

// Für get_coach.wissensbasisArchiv und das Dashboard: neueste zuerst
export function archivForCoach(archiv) {
  return [...archivList(archiv)].reverse().map((x) => ({ id: x.id, title: x.title, source: x.source ?? null, archivedAt: x.archivedAt, reason: x.reason ?? null }));
}

// Hinweise beim Speichern (kein Fehler): Punkte mit archivierter id werden gespeichert, aber nicht angezeigt
export function archivWarnings(value, archiv) {
  const hidden = archivIds(archiv);
  if (!hidden.size) return [];
  const out = [];
  for (const s of value.sections) for (const it of s.items) {
    const id = itemId(it);
    if (hidden.has(id)) out.push(`id ${id} ist archiviert, wird nicht angezeigt`);
  }
  return out;
}

const findItem = (wb, id) => (wb?.sections ?? []).flatMap((s) => s.items ?? []).find((it) => itemId(it) === id);

// Archivieren: { value (neuer Bestand), already } oder { error, status }. Reihenfolge der Prüfung: schon archiviert (200) vor unbekannt (404)
export function applyArchive(wb, archiv, id, body, today = isoDateBerlin()) {
  if (archivIds(archiv).has(id)) return { value: archiv, already: true };
  const it = findItem(wb, id);
  if (!it) return { error: `Wissensbasis-Punkt ${id} nicht gefunden`, status: 404 };
  let reason;
  if (body?.reason != null && body.reason !== "") {
    if (typeof body.reason !== "string") return { error: "reason muss Text sein", status: 400 };
    reason = body.reason.trim();
    if (reason.length > MAX_REASON) return { error: `reason länger als ${MAX_REASON} Zeichen`, status: 400 };
  }
  const entry = { id, title: it.text.slice(0, 80), source: it.source ?? null, archivedAt: today, ...(reason ? { reason } : {}) };
  return { value: { items: [...archivList(archiv), entry].slice(-MAX_ARCHIV) }, already: false };
}

// Wiederherstellen: unbekannt = weder im Archiv noch in der Wissensbasis (404); nicht archiviert = 200 ohne Schreiben
export function applyRestore(wb, archiv, id) {
  if (!archivIds(archiv).has(id)) {
    return findItem(wb, id) ? { value: archiv, already: true } : { error: `Wissensbasis-Punkt ${id} nicht gefunden`, status: 404 };
  }
  return { value: { items: archivList(archiv).filter((x) => x.id !== id) }, already: false };
}

export async function handleWissensbasisArchivRequest(req, env, isAuthorized, id, action) {
  const headers = { "cache-control": "no-store" };
  const denied = gate(req, env, isAuthorized, headers);
  if (denied) return denied;
  if (req.method !== "POST") return json({ ok: false, error: "Nur POST erlaubt" }, 405, { ...headers, allow: "POST" });
  if (!env?.KV) return json({ ok: false, error: "KV nicht verfügbar" }, 503, headers);
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return json({ ok: false, error: "Body zu groß" }, 413, headers);
  let body = {};
  if (text.trim()) {
    try { body = JSON.parse(text); } catch { return json({ ok: false, error: "Kein gültiges JSON" }, 400, headers); }
  }
  const [wb, archiv] = await Promise.all([readWissensbasis(env), readArchiv(env)]);
  const r = action === "archive" ? applyArchive(wb, archiv, id, body) : applyRestore(wb, archiv, id);
  if (r.error) return json({ ok: false, error: r.error }, r.status, headers);
  if (!r.already) await writeKvJson(env, WB_ARCHIV_KV_KEY, r.value);
  return json({ ok: true, id, archived: action === "archive", already: r.already }, 200, headers);
}
