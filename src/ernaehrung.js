import { readKvJson, writeKvJson } from "./kv.js";
import { isIsoDate, isoDateBerlin, listIsoDaysInclusive } from "./date-utils.js";

// Gewicht und Ernährung (Abnehmziel). Zwei getrennte Speicher im KV:
//  - ERNAEHRUNG_KV_KEY: Aussage des Coaches (save_ernaehrung): { fetchedAt, headline, text, weekLine, goal }
//  - WEIGHT_KV_KEY: Gewichtseinträge aus Yazio (Sync in yazio-sync.js), { fetchedAt, entries: [{ date, kg }] }
// Das Dashboard zeigt beides, rechnet aber nichts Wertendes: weightTrend7d ist nur das gleitende 7-Tage-Mittel.
//
// Body von save_ernaehrung: { fetchedAt: ISO-Zeitstempel,
//   headline?: string (max 80), text?: string (max 2000, Tagesaussage), weekLine?: string (max 300, Wochenzeile),
//   goal?: { targetKg: Zahl, byDate: YYYY-MM-DD } | null }
// Mindestens eines von headline, text, weekLine, goal. goal bleibt gespeichert, wenn es fehlt; null entfernt es.

export const ERNAEHRUNG_KV_KEY = "dashboard:ernaehrung";
export const WEIGHT_KV_KEY = "yazio:weight";
export const WEIGHT_DAYS = 90;
const MAX_HEADLINE = 80;
const MAX_TEXT = 2000;
const MAX_WEEKLINE = 300;
const MIN_KG = 30;
const MAX_KG = 300;

const GOAL_HINT = " (ohne goal speichern, um die Aussage trotzdem zu sichern)";

// undefined = Feld fehlt (Ziel bleibt), null = entfernen, sonst das neue Ziel.
function validateGoal(goal) {
  if (goal === undefined) return { value: undefined };
  if (goal === null) return { value: null };
  if (typeof goal === "string" && goal.trim().startsWith("{")) {
    try { goal = JSON.parse(goal); } catch { return { error: "goal ist kein gültiges JSON-Objekt" + GOAL_HINT }; }
  }
  if (typeof goal !== "object" || Array.isArray(goal)) return { error: "goal muss ein Objekt { targetKg, byDate } sein" + GOAL_HINT };
  const kg = typeof goal.targetKg === "string" ? Number(goal.targetKg.replace(",", ".")) : goal.targetKg;
  if (goal.targetKg == null || goal.targetKg === "" || !Number.isFinite(kg) || kg < MIN_KG || kg > MAX_KG) return { error: `goal.targetKg muss eine Zahl von ${MIN_KG} bis ${MAX_KG} sein` + GOAL_HINT };
  const byDate = typeof goal.byDate === "string" ? goal.byDate.trim() : "";
  if (!isIsoDate(byDate)) return { error: "goal.byDate muss ein Datum YYYY-MM-DD sein" + GOAL_HINT };
  return { value: { targetKg: Math.round(kg * 10) / 10, byDate } };
}

export function validateErnaehrung(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  for (const k of ["headline", "text", "weekLine"]) if (body?.[k] != null && typeof body[k] !== "string") return { error: `${k} muss ein Text sein` };
  const headline = (body?.headline ?? "").trim();
  if (headline.length > MAX_HEADLINE) return { error: `headline länger als ${MAX_HEADLINE} Zeichen` };
  const text = (body?.text ?? "").trim();
  if (text.length > MAX_TEXT) return { error: `text länger als ${MAX_TEXT} Zeichen` };
  const weekLine = (body?.weekLine ?? "").trim();
  if (weekLine.length > MAX_WEEKLINE) return { error: `weekLine länger als ${MAX_WEEKLINE} Zeichen` };
  const goal = validateGoal(body?.goal);
  if (goal.error) return { error: goal.error };
  if (!headline && !text && !weekLine && goal.value === undefined) return { error: "headline, text, weekLine oder goal fehlt" };
  const value = { fetchedAt: new Date(fetchedAt).toISOString(), headline: headline || null, text: text || null, weekLine: weekLine || null };
  // goal nur, wenn mitgeschickt; mergeErnaehrung unterscheidet so "fehlt" von "entfernen"
  if (goal.value !== undefined) value.goal = goal.value;
  return { value };
}

// Neue Aussage auf die gespeicherte legen: das Ziel bleibt, solange es nicht mitkommt.
export function mergeErnaehrung(prev, next) {
  const { goal, ...rest } = next;
  return { ...rest, goal: goal === undefined ? (prev?.goal ?? null) : goal };
}

export async function readErnaehrung(env) {
  return readKvJson(env, ERNAEHRUNG_KV_KEY).catch(() => null);
}

// Gewichtseinträge säubern: gültiges Datum, kg im Bereich, je Tag ein Wert (der letzte gewinnt), aufsteigend nach Datum.
export function normalizeWeights(list) {
  const byDay = new Map();
  for (const e of Array.isArray(list) ? list : []) {
    const date = String(e?.date ?? "").slice(0, 10);
    const kg = Number(e?.kg);
    if (!isIsoDate(date) || !Number.isFinite(kg) || kg < MIN_KG || kg > MAX_KG) continue;
    byDay.set(date, Math.round(kg * 10) / 10);
  }
  return [...byDay].map(([date, kg]) => ({ date, kg })).sort((a, b) => a.date.localeCompare(b.date));
}

// Nur die letzten WEIGHT_DAYS Tage bis einschließlich today.
export function lastDays(entries, today, days = WEIGHT_DAYS) {
  const from = new Date(Date.parse(today) - (days - 1) * 86400000).toISOString().slice(0, 10);
  return entries.filter((e) => e.date >= from && e.date <= today);
}

// 7-Tage-Mittel pro Tag: Mittel aller Einträge der 7 Tage bis einschließlich diesem Tag (Fenster nach Kalendertagen, nicht
// nach Anzahl Einträgen). Es gibt einen Wert für jeden Tag vom ersten Eintrag bis zum letzten; Tage, in deren Fenster
// kein Eintrag liegt, bleiben aus.
export function weightTrend7d(entries) {
  const list = normalizeWeights(entries);
  if (!list.length) return [];
  const byDay = new Map(list.map((e) => [e.date, e.kg]));
  const out = [];
  for (const day of listIsoDaysInclusive(list[0].date, list[list.length - 1].date)) {
    const t = Date.parse(day);
    const win = [];
    for (let i = 0; i < 7; i++) {
      const v = byDay.get(new Date(t - i * 86400000).toISOString().slice(0, 10));
      if (v != null) win.push(v);
    }
    if (win.length) out.push({ date: day, kg: Math.round((win.reduce((a, b) => a + b, 0) / win.length) * 100) / 100 });
  }
  return out;
}

// Neue Yazio-Einträge auf die gespeicherten legen (neue Werte gewinnen), alles außerhalb von 90 Tagen fällt weg.
export function mergeWeights(prev, fresh, today) {
  return lastDays(normalizeWeights([...(prev ?? []), ...(fresh ?? [])]), today);
}

export async function readWeights(env) {
  const stored = await readKvJson(env, WEIGHT_KV_KEY).catch(() => null);
  return normalizeWeights(stored?.entries);
}

export async function storeWeights(env, fresh, today = isoDateBerlin()) {
  const prev = await readWeights(env);
  const entries = mergeWeights(prev, fresh, today);
  await writeKvJson(env, WEIGHT_KV_KEY, { fetchedAt: new Date().toISOString(), entries });
  return entries;
}

// Das Feld "ernaehrung" für get_coach und Dashboard:
//  { coach, weight: [{ date, kg }], weightTrend7d: [{ date, kg }], weightSource: "yazio" | "intervals" | null }
// Quelle: Yazio-Einträge aus dem KV; sind es keine, die weight-Werte aus Intervals-Wellness (wellness: Liste oder
// async Funktion, die sie liefert; Einträge mit date/weight oder id/weight).
// Fehler in einem Teil machen daraus nie einen Fehler: coach und Gewicht fallen einzeln auf null bzw. [] zurück.
export async function buildErnaehrung(env, today, wellness = []) {
  const coach = await readErnaehrung(env);
  let yazio = [];
  try { yazio = lastDays(await readWeights(env), today); } catch { yazio = []; }
  let weight = yazio;
  let weightSource = yazio.length ? "yazio" : null;
  if (!yazio.length) {
    let list = [];
    try { list = typeof wellness === "function" ? await wellness() : wellness; } catch { list = []; }
    const fromWellness = lastDays(normalizeWeights((Array.isArray(list) ? list : []).map((w) => ({ date: w?.date ?? w?.id, kg: w?.weight }))), today);
    if (fromWellness.length) { weight = fromWellness; weightSource = "intervals"; }
  }
  return { coach: coach ?? null, weight, weightTrend7d: weightTrend7d(weight), weightSource };
}
