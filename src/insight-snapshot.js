import { readKvJson } from "./kv.js";

// Erkenntnisse: Auffaelligkeiten, die der Coach selbst in den Daten findet und pflegt (MCP-Tools save_insight, archive_insight).
// Das Dashboard zeigt sie als Feed, ohne eigene Bewertung; get_coach liefert die aktiven als Kurzfassung.
//
// save_insight (Upsert ueber id): { id: kebab-case (max 60), title (max 60), text (max 280), action?: string | null (max 160),
//   strength: "beobachtung" | "schwach" | "mittel" | "stark", status: "neu" | "bestätigt" | "abgeschwächt" | "widerlegt",
//   discipline: "lauf" | "rad" | "schwimmen" | "kraft" | "erholung" | "alle", evidence (max 200),
//   chart?: null | { type: "line" | "bar", unit (max 20), points: [{ x (max 20), y: Zahl }] (1-30), refLine?: Zahl | null,
//                    better: "higher" | "lower" }, fetchedAt: ISO-Zeitstempel }
// createdAt kommt beim ersten Speichern dazu, updatedAt bei jedem. Jede Aenderung von status oder strength (und der erste
// Stand) landet mit Datum in history (neueste zuerst, max 10). Speichern eines archivierten Insights holt ihn zurueck.
// archive_insight({ id }) blendet ihn aus (archived: true, archivedAt), geloescht wird nichts.

export const INSIGHTS_KV_KEY = "dashboard:insights";
export const INSIGHT_STRENGTH = ["beobachtung", "schwach", "mittel", "stark"];
export const INSIGHT_STATUS = ["neu", "bestätigt", "abgeschwächt", "widerlegt"];
export const INSIGHT_DISCIPLINES = ["lauf", "rad", "schwimmen", "kraft", "erholung", "alle"];
const ID_RE = /^[a-z0-9äöüß]+(?:-[a-z0-9äöüß]+)*$/;
const MAX_ID = 60;
const MAX_TITLE = 60;
const MAX_TEXT = 280;
const MAX_ACTION = 160;
const MAX_EVIDENCE = 200;
const MAX_UNIT = 20;
const MAX_X = 20;
const MAX_POINTS = 30;
const MAX_HISTORY = 10;
const MAX_INSIGHTS = 100; // aelteste archivierte fallen zuerst heraus
const SHORT_TEXT = 140;

// Clients mit veraltetem Tool-Schema schicken Objekte manchmal als JSON-Text
function parseJsonText(v) {
  if (typeof v !== "string" || !/^\s*[[{]/.test(v)) return v;
  try { return JSON.parse(v); } catch { return v; }
}

function reqText(v, name, max) {
  const t = typeof v === "string" ? v.trim() : "";
  if (!t) return { error: `${name} fehlt` };
  if (t.length > max) return { error: `${name} länger als ${max} Zeichen` };
  return { value: t };
}

function oneOf(v, name, list) {
  const t = String(v ?? "").trim().toLowerCase();
  if (!list.includes(t)) return { error: `${name} muss ${list.join(", ")} sein` };
  return { value: t };
}

function validateId(v) {
  const id = typeof v === "string" ? v.trim() : "";
  if (!id) return { error: "id fehlt" };
  if (id.length > MAX_ID || !ID_RE.test(id)) return { error: `id muss kebab-case sein (a-z, 0-9, Bindestriche, max. ${MAX_ID} Zeichen)` };
  return { value: id };
}

function validateChart(c) {
  c = parseJsonText(c);
  if (c == null) return { value: null };
  if (typeof c !== "object" || Array.isArray(c)) return { error: "chart muss ein Objekt { type, unit, points, refLine, better } oder null sein" };
  const type = oneOf(c.type, "chart.type", ["line", "bar"]);
  if (type.error) return type;
  if (c.unit != null && typeof c.unit !== "string") return { error: "chart.unit muss ein Text sein" };
  const unit = (c.unit ?? "").trim();
  if (unit.length > MAX_UNIT) return { error: `chart.unit länger als ${MAX_UNIT} Zeichen` };
  const better = oneOf(c.better, "chart.better", ["higher", "lower"]);
  if (better.error) return better;
  if (!Array.isArray(c.points) || !c.points.length) return { error: "chart.points muss eine Liste mit mind. einem Punkt sein" };
  if (c.points.length > MAX_POINTS) return { error: `höchstens ${MAX_POINTS} chart.points` };
  const points = [];
  for (const p of c.points) {
    const x = String(p?.x ?? "").trim(), y = p?.y == null || p.y === "" ? NaN : Number(p.y);
    if (!x || x.length > MAX_X) return { error: `chart.points.x fehlt oder länger als ${MAX_X} Zeichen` };
    if (!Number.isFinite(y)) return { error: "chart.points.y muss eine Zahl sein" };
    points.push({ x, y });
  }
  const refLine = c.refLine == null || c.refLine === "" ? null : Number(c.refLine);
  if (refLine != null && !Number.isFinite(refLine)) return { error: "chart.refLine muss eine Zahl oder null sein" };
  return { value: { type: type.value, unit, points, refLine, better: better.value } };
}

export function validateInsight(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const checks = {
    id: validateId(body?.id),
    title: reqText(body?.title, "title", MAX_TITLE),
    text: reqText(body?.text, "text", MAX_TEXT),
    strength: oneOf(body?.strength, "strength", INSIGHT_STRENGTH),
    status: oneOf(String(body?.status ?? "").replace("bestaetigt", "bestätigt").replace("abgeschwaecht", "abgeschwächt"), "status", INSIGHT_STATUS),
    discipline: oneOf(body?.discipline, "discipline", INSIGHT_DISCIPLINES),
    evidence: reqText(body?.evidence, "evidence", MAX_EVIDENCE),
    chart: validateChart(body?.chart),
  };
  for (const r of Object.values(checks)) if (r.error) return { error: r.error };
  if (body?.action != null && typeof body.action !== "string") return { error: "action muss ein Text oder null sein" };
  const action = (body?.action ?? "").trim();
  if (action.length > MAX_ACTION) return { error: `action länger als ${MAX_ACTION} Zeichen` };
  const value = Object.fromEntries(Object.entries(checks).map(([k, r]) => [k, r.value]));
  return { value: { ...value, action: action || null, fetchedAt: new Date(fetchedAt).toISOString() } };
}

export function validateArchive(body) {
  return validateId(body?.id);
}

function listOf(stored) {
  return Array.isArray(stored?.items) ? stored.items : [];
}

// Upsert: neuer Insight vorn, bestehender wird ersetzt (createdAt und history bleiben). Gibt die neue Liste zurueck.
export function upsertInsight(stored, insight) {
  const items = listOf(stored);
  const prev = items.find((x) => x?.id === insight.id);
  const at = insight.fetchedAt, date = at.slice(0, 10);
  let history = Array.isArray(prev?.history) ? prev.history : [];
  if (!prev || prev.status !== insight.status || prev.strength !== insight.strength) {
    history = [{ date, at, status: insight.status, strength: insight.strength }, ...history].slice(0, MAX_HISTORY);
  }
  const next = { ...insight, createdAt: prev?.createdAt ?? at, updatedAt: at, archived: false, archivedAt: null, history };
  const rest = items.filter((x) => x?.id !== insight.id);
  // Platz schaffen: zuerst die am laengsten archivierten, dann die aeltesten aktiven
  const byAge = (a, b) => String(a.archivedAt ?? a.updatedAt).localeCompare(String(b.archivedAt ?? b.updatedAt));
  while (rest.length >= MAX_INSIGHTS) {
    const pool = rest.some((x) => x.archived) ? rest.filter((x) => x.archived) : rest;
    const oldest = [...pool].sort(byAge)[0];
    rest.splice(rest.indexOf(oldest), 1);
  }
  return { items: [next, ...rest] };
}

export function archiveInsight(stored, id, nowIso = new Date().toISOString()) {
  const items = listOf(stored);
  const hit = items.find((x) => x?.id === id);
  if (!hit) return { error: `Kein Insight mit id ${id}` };
  if (hit.archived) return { value: stored, already: true };
  return { value: { items: items.map((x) => (x.id === id ? { ...x, archived: true, archivedAt: nowIso } : x)) } };
}

const byUpdatedDesc = (a, b) => String(b.updatedAt).localeCompare(String(a.updatedAt));

// Fuers Dashboard: aktive und archivierte, jeweils neueste zuerst
export function splitInsights(stored) {
  const items = listOf(stored);
  return {
    active: items.filter((x) => !x.archived).sort(byUpdatedDesc),
    archived: items.filter((x) => x.archived).sort(byUpdatedDesc),
  };
}

// Fuer get_coach: nur die aktiven, kurz
export function insightsForCoach(stored) {
  return splitInsights(stored).active.map((x) => ({
    id: x.id, title: x.title, discipline: x.discipline, strength: x.strength, status: x.status, updatedAt: x.updatedAt,
    text: x.text.length > SHORT_TEXT ? x.text.slice(0, SHORT_TEXT - 1).trimEnd() + "…" : x.text,
  }));
}

export async function readInsights(env) {
  return readKvJson(env, INSIGHTS_KV_KEY).catch(() => null);
}
