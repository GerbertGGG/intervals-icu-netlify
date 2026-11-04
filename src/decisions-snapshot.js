import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";
import { isIsoDate } from "./date-utils.js";

// Entscheidungsliste "Was du entscheiden musst": eigener KV-Schluessel, getrennt von der Tagesaussage (dashboard:coach).
// Wer die Liste schreibt (save_decisions, PUT /api/decisions), kann die Tagesaussage nicht anfassen, und umgekehrt.
//
// Body: { fetchedAt: ISO-Zeitstempel,
//         decisions: [{ id?: kebab-case (stabil, fehlt sie, kommt sie aus dem Titel), title: string (max 80, mit Ja/Nein/Option B beantwortbar),
//                       recommendation: string (max 160), reason: string (max 200), alternatives?: string[] (max 3 je max 80),
//                       deadline: YYYY-MM-DD, ifNoAnswer: string (max 160),
//                       answer?: string (max 200), answeredAt?: ISO-Zeitstempel }] (max 5) }
// Die ganze Liste ersetzt die gespeicherte; fehlende Punkte sind danach weg, [] blendet die Karte aus. Antwortfelder (answer,
// answeredAt) bleiben per id erhalten, solange der Aufruf sie nicht ausdruecklich setzt. Nach Frist sortiert das der Worker.
// Gespeichert wird { fetchedAt, decisions }.

export const DECISIONS_KV_KEY = "dashboard:decisions";
// Schluessel der Tagesaussage (wie COACH_KV_KEY in coach-snapshot.js; hier nicht importiert, damit es keinen Import-Kreis gibt)
const COACH_KEY = "dashboard:coach";
const MAX_BODY_BYTES = 40_000;
const MAX_DECISIONS = 5;
const MAX_ID = 60;
const MAX_TITLE = 80;
const MAX_REC = 160;
const MAX_REASON = 200;
const MAX_ALT = 80;
const MAX_ALTS = 3;
const MAX_IF = 160;
export const MAX_DEC_ANSWER = 200;
const ID_RE = /^[a-z0-9]+(-[a-z0-9]+)*$/;

// Echtes Kalenderdatum (isIsoDate prueft nur das Format)
const isRealDate = (d) => {
  if (!isIsoDate(d)) return false;
  const ms = Date.parse(d + "T00:00:00Z");
  return Number.isFinite(ms) && new Date(ms).toISOString().slice(0, 10) === d;
};

// Titel -> kebab-case-id (Umlaute aufgeloest, max. 60 Zeichen)
export function slugId(title) {
  const s = String(title ?? "").toLowerCase().replace(/ä/g, "ae").replace(/ö/g, "oe").replace(/ü/g, "ue").replace(/ß/g, "ss")
    .replace(/[^a-z0-9]+/g, "-").replace(/^-+|-+$/g, "").slice(0, MAX_ID).replace(/-+$/g, "");
  return s || "punkt";
}

function uniqueId(base, used) {
  let id = base, n = 2;
  while (used.has(id)) { const suffix = `-${n++}`; id = base.slice(0, MAX_ID - suffix.length) + suffix; }
  return id;
}

// Gueltige Liste ohne Antwort-Merge: { value: { fetchedAt, decisions } } oder { error }. answer/answeredAt nur, wenn der Aufruf sie setzt
// (answerSet: true = ausdruecklich gesetzt, auch leer = Antwort entfernen).
export function validateDecisions(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  let list = body?.decisions;
  if (typeof list === "string" && list.trim().startsWith("[")) {
    try { list = JSON.parse(list); } catch { return { error: "decisions ist kein gültiges JSON" }; }
  }
  if (!Array.isArray(list)) return { error: "decisions muss eine Liste sein (leer = Karte ausblenden)" };
  if (list.length > MAX_DECISIONS) return { error: `höchstens ${MAX_DECISIONS} decisions` };
  const text = (v, name, max) => {
    if (v != null && typeof v !== "string") return { error: `decisions.${name} muss ein Text sein` };
    const t = (v ?? "").trim();
    if (!t) return { error: `decisions.${name} fehlt` };
    if (t.length > max) return { error: `decisions.${name} länger als ${max} Zeichen` };
    return { value: t };
  };
  const out = [];
  const used = new Set();
  const givenIds = list.map((x) => (typeof x?.id === "string" ? x.id.trim() : ""));
  for (const id of givenIds) if (id) used.add(id);
  if (used.size !== givenIds.filter(Boolean).length) return { error: "decisions.id doppelt" };
  for (let i = 0; i < list.length; i++) {
    const x = list[i];
    if (!x || typeof x !== "object" || Array.isArray(x)) return { error: "decisions braucht Objekte { id?, title, recommendation, reason, deadline, ifNoAnswer }" };
    if (x.id != null && typeof x.id !== "string") return { error: "decisions.id muss ein Text sein" };
    const f = {};
    for (const [k, max] of [["title", MAX_TITLE], ["recommendation", MAX_REC], ["reason", MAX_REASON], ["ifNoAnswer", MAX_IF]]) {
      const r = text(x[k], k, max);
      if (r.error) return r;
      f[k] = r.value;
    }
    let id = givenIds[i];
    if (id) {
      if (id.length > MAX_ID || !ID_RE.test(id)) return { error: `decisions.id muss kebab-case sein (a-z, 0-9, Bindestrich, max. ${MAX_ID} Zeichen)` };
    } else {
      id = uniqueId(slugId(f.title), used);
      used.add(id);
    }
    const deadline = typeof x.deadline === "string" ? x.deadline.trim() : "";
    if (!isRealDate(deadline)) return { error: "decisions.deadline muss YYYY-MM-DD sein" };
    if (x.alternatives != null && !Array.isArray(x.alternatives)) return { error: "decisions.alternatives muss eine Liste sein" };
    const alternatives = (x.alternatives ?? []).map((a) => String(a ?? "").trim()).filter(Boolean);
    if (alternatives.length > MAX_ALTS) return { error: `höchstens ${MAX_ALTS} decisions.alternatives` };
    if (alternatives.some((a) => a.length > MAX_ALT)) return { error: `decisions.alternatives länger als ${MAX_ALT} Zeichen` };
    const item = { id, ...f, alternatives, deadline };
    if (x.answer !== undefined) {
      if (x.answer != null && typeof x.answer !== "string") return { error: "decisions.answer muss ein Text sein" };
      const answer = (x.answer ?? "").trim();
      if (answer.length > MAX_DEC_ANSWER) return { error: `decisions.answer länger als ${MAX_DEC_ANSWER} Zeichen` };
      item.answerSet = true;
      item.answer = answer || null;
      if (answer) {
        const ms = Date.parse(x.answeredAt);
        item.answeredAt = Number.isFinite(ms) ? new Date(ms).toISOString() : null;
      }
    }
    out.push(item);
  }
  out.sort((a, b) => a.deadline.localeCompare(b.deadline));
  return { value: { fetchedAt: new Date(fetchedAt).toISOString(), decisions: out } };
}

// Antworten aus dem gespeicherten Stand per id uebernehmen, solange der Aufruf sie nicht ausdruecklich setzt. Fehlende Punkte
// im Aufruf sind weg.
export function mergeDecisions(prev, next, nowMs = Date.now()) {
  const old = new Map((Array.isArray(prev?.decisions) ? prev.decisions : []).map((x) => [x.id, x]));
  const decisions = next.decisions.map(({ answerSet, answer, answeredAt, ...rest }) => {
    if (answerSet) return answer ? { ...rest, answer, answeredAt: answeredAt ?? new Date(nowMs).toISOString() } : rest;
    const o = old.get(rest.id);
    return o?.answer ? { ...rest, answer: o.answer, answeredAt: o.answeredAt ?? null } : rest;
  });
  return { fetchedAt: next.fetchedAt, decisions };
}

// Antwort aus dem Dashboard: aendert nur Antwortfelder des Punkts mit dieser id.
export function applyDecisionAnswer(stored, body, nowMs = Date.now()) {
  const list = Array.isArray(stored?.decisions) ? stored.decisions : [];
  const id = typeof body?.id === "string" ? body.id : "";
  const idx = list.findIndex((x) => x.id === id);
  if (idx < 0) return { error: "Entscheidung nicht (mehr) offen" };
  const answer = typeof body?.answer === "string" ? body.answer.trim() : "";
  if (!answer) return { error: "answer fehlt" };
  if (answer.length > MAX_DEC_ANSWER) return { error: `answer länger als ${MAX_DEC_ANSWER} Zeichen` };
  const decisions = list.map((x, i) => (i === idx ? { ...x, answer, answeredAt: new Date(nowMs).toISOString() } : x));
  return { value: { ...stored, decisions } };
}

// Einmalige, wiederholbare Migration: Punkte aus dashboard:coach.decisions in den eigenen Schluessel uebernehmen, danach das Feld
// aus dashboard:coach entfernen. Schon vorhandene ids im neuen Schluessel bleiben unveraendert (nichts doppelt, nichts verloren).
// Zuerst wird der neue Schluessel geschrieben, dann das alte Feld entfernt: bricht es dazwischen ab, holt der naechste Lauf es nach.
// Gibt den (ggf. migrierten) Coach-Stand zurueck.
export async function migrateDecisions(env, coach = undefined) {
  const c = coach === undefined ? await readKvJson(env, COACH_KEY).catch(() => null) : coach;
  if (!c || !Object.hasOwn(c, "decisions")) return c;
  const legacy = Array.isArray(c.decisions) ? c.decisions : [];
  const stored = await readKvJson(env, DECISIONS_KV_KEY).catch(() => null);
  const have = Array.isArray(stored?.decisions) ? stored.decisions : [];
  const used = new Set(have.map((x) => x.id));
  const added = [];
  for (const x of legacy) {
    if (!x || typeof x !== "object") continue;
    const id = typeof x.id === "string" && ID_RE.test(x.id) ? x.id : slugId(x.title);
    // gleicher Punkt schon da (id oder gleicher Titel): nicht noch einmal anlegen
    if (used.has(id) || have.some((h) => h.title === x.title)) continue;
    used.add(id);
    added.push({ ...x, id });
  }
  if (added.length || !stored) {
    const decisions = [...have, ...added].sort((a, b) => String(a.deadline).localeCompare(String(b.deadline)));
    await writeKvJson(env, DECISIONS_KV_KEY, { fetchedAt: stored?.fetchedAt ?? c.fetchedAt ?? new Date().toISOString(), decisions });
  }
  const { decisions: _drop, ...rest } = c;
  await writeKvJson(env, COACH_KEY, rest);
  return rest;
}

// Liste lesen; migriert vorher, falls dashboard:coach noch ein decisions-Feld hat (coach = schon gelesener Stand, spart ein Lesen)
export async function readDecisions(env, coach = undefined) {
  await migrateDecisions(env, coach).catch(() => {});
  const stored = await readKvJson(env, DECISIONS_KV_KEY).catch(() => null);
  return Array.isArray(stored?.decisions) ? stored.decisions : [];
}

// Gemeinsamer Schreibweg fuer save_decisions und PUT /api/decisions: genau ein Schreibvorgang auf dashboard:decisions
export async function saveDecisions(env, args) {
  const { value, error } = validateDecisions(args);
  if (error) throw new Error(error);
  await migrateDecisions(env);
  const merged = mergeDecisions(await readKvJson(env, DECISIONS_KV_KEY), value);
  await writeKvJson(env, DECISIONS_KV_KEY, merged);
  return { ok: true, fetchedAt: merged.fetchedAt, decisions: merged.decisions.map((x) => ({ id: x.id, title: x.title, deadline: x.deadline })) };
}

async function readBody(req, env, method, headers) {
  if (req.method !== method) return { res: json({ ok: false, error: `Nur ${method} erlaubt` }, 405, { ...headers, allow: method }) };
  if (!env?.KV) return { res: json({ ok: false, error: "KV nicht verfügbar" }, 503, headers) };
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return { res: json({ ok: false, error: "Body zu groß" }, 413, headers) };
  try { return { body: JSON.parse(text) }; } catch { return { res: json({ ok: false, error: "Kein gültiges JSON" }, 400, headers) }; }
}

function gate(req, env, isAuthorized, headers) {
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  return null;
}

// PUT /api/decisions (Bearer DASHBOARD_TOKEN), wie das MCP-Tool save_decisions
export async function handleDecisionsRequest(req, env, isAuthorized) {
  const headers = { "cache-control": "no-store" };
  const denied = gate(req, env, isAuthorized, headers);
  if (denied) return denied;
  const { body, res } = await readBody(req, env, "PUT", headers);
  if (res) return res;
  try {
    return json(await saveDecisions(env, body), 200, headers);
  } catch (e) {
    return json({ ok: false, error: String(e?.message ?? e) }, 400, headers);
  }
}

// POST /api/coach/decision { id, answer } aus dem Dashboard: aendert nur die Antwortfelder dieses Punkts
export async function handleDecisionAnswerRequest(req, env, isAuthorized) {
  const headers = { "cache-control": "no-store" };
  const denied = gate(req, env, isAuthorized, headers);
  if (denied) return denied;
  const { body, res } = await readBody(req, env, "POST", headers);
  if (res) return res;
  await migrateDecisions(env);
  const { value, error } = applyDecisionAnswer(await readKvJson(env, DECISIONS_KV_KEY).catch(() => null), body);
  if (error) return json({ ok: false, error }, 409, headers);
  await writeKvJson(env, DECISIONS_KV_KEY, value);
  return json({ ok: true }, 200, headers);
}
