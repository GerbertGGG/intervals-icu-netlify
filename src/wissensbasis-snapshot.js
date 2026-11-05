import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";

// Wissensbasis des Coaches (Karte "Wissensbasis" im Dashboard): Stichpunkte aus der Literatur mit Evidenzstufe und
// Übertragbarkeit. Der Coach schreibt den ganzen Stand per save_wissensbasis (MCP) oder PUT /api/wissensbasis
// (Bearer DASHBOARD_TOKEN); get_coach liefert ihn unter wissensbasis zurück.
//
// Body: { fetchedAt: ISO-Zeitstempel, updatedAt: "YYYY-MM-DD", folderCheckedAt?: "YYYY-MM-DD",
//         sections: [{ title (max 60), items: [{ text (max 220), evidence, fit, source? (max 80), isNew?,
//         consensus?, counterCheckedAt?, practice? (max 160), match?, atYou?, link? { type, id }, next? (max 160) }] }],
//         open?: [string (max 160), max. 6] }

export const WISSENSBASIS_KV_KEY = "dashboard:wissensbasis";
const MAX_BODY_BYTES = 60_000;
const MAX_SECTIONS = 12;
const MAX_ITEMS = 8;
const MAX_OPEN = 6;
export const EVIDENCE = ["stark", "mittel", "schwach", "praxis"];
export const FIT = ["hoch", "mittel", "gering", "unklar"];
export const CONSENSUS = ["einig", "erklaert", "umstritten", "eine_seite"];
export const MATCH = ["passt", "luecke", "bewusst_anders", "nicht_pruefbar"];
export const AT_YOU = ["nicht_getestet", "laeuft", "gewirkt", "nicht_gewirkt", "offen"];
export const LINK_TYPES = ["insight", "experiment", "targets"];
const ENUM_FIELDS = { consensus: CONSENSUS, match: MATCH, atYou: AT_YOU };
const DATE_RE =/^\d{4}-\d{2}-\d{2}$/;

const isDate = (s) => typeof s === "string" && DATE_RE.test(s) && Number.isFinite(Date.parse(s));
const str = (v) => (typeof v === "string" ? v.trim() : "");

export function validateWissensbasis(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  if (!isDate(body?.updatedAt)) return { error: "updatedAt (YYYY-MM-DD) fehlt oder ist ungültig" };
  let folderCheckedAt = null;
  if (body.folderCheckedAt != null) {
    if (!isDate(body.folderCheckedAt)) return { error: "folderCheckedAt muss YYYY-MM-DD sein" };
    folderCheckedAt = body.folderCheckedAt;
  }
  if (!Array.isArray(body.sections) || body.sections.length < 1) return { error: "sections fehlt oder ist leer (1–12 Abschnitte)" };
  if (body.sections.length > MAX_SECTIONS) return { error: `sections: mehr als ${MAX_SECTIONS} Abschnitte` };
  const sections = [];
  for (const [i, s] of body.sections.entries()) {
    const at = `sections[${i}]`;
    const title = str(s?.title);
    if (!title) return { error: `${at}.title fehlt` };
    if (title.length > 60) return { error: `${at}.title länger als 60 Zeichen` };
    if (!Array.isArray(s.items) || s.items.length < 1) return { error: `${at}.items fehlt oder ist leer (1–8 Punkte)` };
    if (s.items.length > MAX_ITEMS) return { error: `${at}.items: mehr als ${MAX_ITEMS} Punkte` };
    const items = [];
    for (const [j, it] of s.items.entries()) {
      const p = `${at}.items[${j}]`;
      const text = str(it?.text);
      if (!text) return { error: `${p}.text fehlt` };
      if (text.length > 220) return { error: `${p}.text länger als 220 Zeichen` };
      if (!EVIDENCE.includes(it.evidence)) return { error: `${p}.evidence muss ${EVIDENCE.join(" | ")} sein` };
      if (!FIT.includes(it.fit)) return { error: `${p}.fit muss ${FIT.join(" | ")} sein` };
      const out = { text, evidence: it.evidence, fit: it.fit };
      if (it.source != null && it.source !== "") {
        const source = str(it.source);
        if (typeof it.source !== "string" || !source) return { error: `${p}.source muss Text sein` };
        if (source.length > 80) return { error: `${p}.source länger als 80 Zeichen` };
        out.source = source;
      }
      if (it.isNew != null) {
        if (typeof it.isNew !== "boolean") return { error: `${p}.isNew muss true oder false sein` };
        if (it.isNew) out.isNew = true;
      }
      for (const [key, allowed] of Object.entries(ENUM_FIELDS)) {
        if (it[key] == null) continue;
        if (!allowed.includes(it[key])) return { error: `${p}.${key} muss ${allowed.join(" | ")} sein` };
        out[key] = it[key];
      }
      if (it.counterCheckedAt != null) {
        if (!isDate(it.counterCheckedAt)) return { error: `${p}.counterCheckedAt muss YYYY-MM-DD sein` };
        out.counterCheckedAt = it.counterCheckedAt;
      }
      for (const key of ["practice", "next"]) {
        if (it[key] == null || it[key] === "") continue;
        const v = str(it[key]);
        if (typeof it[key] !== "string" || !v) return { error: `${p}.${key} muss Text sein` };
        if (v.length > 160) return { error: `${p}.${key} länger als 160 Zeichen` };
        out[key] = v;
      }
      if (it.link != null) {
        if (typeof it.link !== "object" || Array.isArray(it.link)) return { error: `${p}.link muss ein Objekt { type, id } sein` };
        if (!LINK_TYPES.includes(it.link.type)) return { error: `${p}.link.type muss ${LINK_TYPES.join(" | ")} sein` };
        const id = str(it.link.id);
        if (!id) return { error: `${p}.link.id fehlt` };
        if (id.length > 60) return { error: `${p}.link.id länger als 60 Zeichen` };
        out.link = { type: it.link.type, id };
      }
      items.push(out);
    }
    sections.push({ title, items });
  }
  let open = [];
  if (body.open != null) {
    if (!Array.isArray(body.open)) return { error: "open muss eine Liste sein" };
    if (body.open.length > MAX_OPEN) return { error: `open: mehr als ${MAX_OPEN} Einträge` };
    for (const [k, o] of body.open.entries()) {
      const t = str(o);
      if (!t) return { error: `open[${k}] fehlt oder ist kein Text` };
      if (t.length > 160) return { error: `open[${k}] länger als 160 Zeichen` };
      open.push(t);
    }
  }
  const value = { fetchedAt: new Date(fetchedAt).toISOString(), updatedAt: body.updatedAt, ...(folderCheckedAt ? { folderCheckedAt } : {}), sections, open };
  return { value };
}

export async function readWissensbasis(env) {
  return readKvJson(env, WISSENSBASIS_KV_KEY).catch(() => null);
}

// Validieren und speichern; ein älterer fetchedAt überschreibt nie einen neueren.
export async function saveWissensbasis(env, body) {
  const { value, error } = validateWissensbasis(body);
  if (error) return { error };
  const prev = await readWissensbasis(env);
  if (prev?.fetchedAt && Date.parse(prev.fetchedAt) > Date.parse(value.fetchedAt)) {
    return { error: `fetchedAt ${value.fetchedAt} ist älter als der gespeicherte Stand (${prev.fetchedAt}); nicht überschrieben` };
  }
  await writeKvJson(env, WISSENSBASIS_KV_KEY, value);
  return { value };
}

export async function handleWissensbasisRequest(req, env, isAuthorized) {
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
  const { value, error } = await saveWissensbasis(env, body);
  if (error) return json({ ok: false, error }, 400, headers);
  return json({ ok: true, sections: value.sections.length }, 200, headers);
}
