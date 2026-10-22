import { readKvJson } from "./kv.js";
import { isIsoDate, isoDate, mondayOnOrBefore } from "./date-utils.js";
import { activityDay } from "./activity-utils.js";

// Laufendes Trainings-Experiment und die wichtigsten Punkte aus dem Athletenprofil. Der Coach schreibt beides per MCP-Tool
// (save_experiment, save_profil) und liest es mit get_coach; das Dashboard zeigt es nur an, ohne eigene Bewertung.
// Jeder Aufruf ueberschreibt den gespeicherten Stand.
//
// Experiment: { fetchedAt: ISO-Zeitstempel, title: string (max 80), hypothesis: string (max 200),
//               status: "vorgeschlagen" | "läuft" | "ausgewertet", startDate: YYYY-MM-DD | null,
//               minWeeks: 1-52, targetDaysPerWeek: 1-7,
//               metrics: [{ key: lauf_hf130 | rad_hf165w | schwimm_amstueck | schwimm_100m, label (max 60), unit (max 20),
//                           baseline, baselineDate, current, currentDate (Zahl bzw. YYYY-MM-DD oder null),
//                           better: "lower" | "higher" }],
//               verdict: string (max 200) | null }
// Profil: { fetchedAt, updatedAt: YYYY-MM-DD, highlights: [{ text (max 140), strength: "mittel" | "schwach" }] (max 5) }
//
// Die Trainingstage je Woche (computeTrainingDays) rechnet der Worker live aus den Intervals-Aktivitaeten.

export const EXPERIMENT_KV_KEY = "dashboard:experiment";
export const PROFIL_KV_KEY = "dashboard:profil";
export const EXPERIMENT_STATUS = ["vorgeschlagen", "läuft", "ausgewertet"];
// Bekannte Messgroessen mit ihrer Richtung (Standard, wenn better fehlt)
export const EXPERIMENT_METRICS = { lauf_hf130: "lower", rad_hf165w: "lower", schwimm_amstueck: "higher", schwimm_100m: "lower" };
export const PROFIL_STRENGTH = ["mittel", "schwach"];
const MAX_TITLE = 80;
const MAX_HYPOTHESIS = 200;
const MAX_VERDICT = 200;
const MAX_LABEL = 60;
const MAX_UNIT = 20;
const MAX_WEEKS = 52;
const MAX_HIGHLIGHTS = 5;
const MAX_HIGHLIGHT = 140;
const TRAINING_DAYS_WEEKS = 8;

// Clients mit veraltetem Tool-Schema schicken Listen und Objekte manchmal als JSON-Text
function parseJsonText(v) {
  if (typeof v !== "string" || !/^\s*[[{]/.test(v)) return v;
  try { return JSON.parse(v); } catch { return v; }
}

function optText(v, name, max) {
  if (v == null) return { value: null };
  if (typeof v !== "string") return { error: `${name} muss ein Text sein` };
  const t = v.trim();
  if (t.length > max) return { error: `${name} länger als ${max} Zeichen` };
  return { value: t || null };
}

function optNumber(v, name) {
  if (v == null || v === "") return { value: null };
  const n = Number(v);
  if (!Number.isFinite(n)) return { error: `${name} muss eine Zahl oder null sein` };
  return { value: n };
}

function optDate(v, name) {
  if (v == null || v === "") return { value: null };
  if (!isIsoDate(v)) return { error: `${name} muss ein Datum YYYY-MM-DD oder null sein` };
  return { value: String(v) };
}

function validateMetric(m, seen) {
  if (m == null || typeof m !== "object" || Array.isArray(m)) return { error: "metrics: jeder Eintrag muss ein Objekt sein" };
  const key = String(m.key ?? "").trim();
  if (!(key in EXPERIMENT_METRICS)) return { error: `metrics.key muss ${Object.keys(EXPERIMENT_METRICS).join(", ")} sein` };
  if (seen.has(key)) return { error: `metrics: ${key} doppelt` };
  seen.add(key);
  const label = optText(m.label, "metrics.label", MAX_LABEL);
  if (label.error) return label;
  const unit = optText(m.unit, "metrics.unit", MAX_UNIT);
  if (unit.error) return unit;
  const better = m.better == null || m.better === "" ? EXPERIMENT_METRICS[key] : String(m.better).trim().toLowerCase();
  if (better !== "lower" && better !== "higher") return { error: "metrics.better muss lower oder higher sein" };
  const out = { key, label: label.value ?? key, unit: unit.value ?? "", better };
  for (const f of ["baseline", "current"]) {
    const n = optNumber(m[f], `metrics.${f}`);
    if (n.error) return n;
    const d = optDate(m[`${f}Date`], `metrics.${f}Date`);
    if (d.error) return d;
    out[f] = n.value;
    out[`${f}Date`] = d.value;
  }
  return { value: out };
}

export function validateExperiment(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const title = typeof body?.title === "string" ? body.title.trim() : "";
  if (!title) return { error: "title fehlt" };
  if (title.length > MAX_TITLE) return { error: `title länger als ${MAX_TITLE} Zeichen` };
  const hypothesis = typeof body?.hypothesis === "string" ? body.hypothesis.trim() : "";
  if (!hypothesis) return { error: "hypothesis fehlt" };
  if (hypothesis.length > MAX_HYPOTHESIS) return { error: `hypothesis länger als ${MAX_HYPOTHESIS} Zeichen` };
  const status = String(body?.status ?? "").trim().toLowerCase().replace("laeuft", "läuft");
  if (!EXPERIMENT_STATUS.includes(status)) return { error: `status muss ${EXPERIMENT_STATUS.join(", ")} sein` };
  const startDate = optDate(body?.startDate, "startDate");
  if (startDate.error) return { error: startDate.error };
  const minWeeks = Number(body?.minWeeks);
  if (!(Number.isInteger(minWeeks) && minWeeks >= 1 && minWeeks <= MAX_WEEKS)) return { error: `minWeeks muss eine ganze Zahl von 1 bis ${MAX_WEEKS} sein` };
  const targetDays = Number(body?.targetDaysPerWeek);
  if (!(Number.isInteger(targetDays) && targetDays >= 1 && targetDays <= 7)) return { error: "targetDaysPerWeek muss eine ganze Zahl von 1 bis 7 sein" };
  const rawMetrics = parseJsonText(body?.metrics) ?? [];
  if (!Array.isArray(rawMetrics)) return { error: "metrics muss eine Liste sein" };
  const seen = new Set(), metrics = [];
  for (const m of rawMetrics) {
    const r = validateMetric(m, seen);
    if (r.error) return { error: r.error };
    metrics.push(r.value);
  }
  const verdict = optText(body?.verdict, "verdict", MAX_VERDICT);
  if (verdict.error) return { error: verdict.error };
  return {
    value: { fetchedAt: new Date(fetchedAt).toISOString(), title, hypothesis, status, startDate: startDate.value, minWeeks, targetDaysPerWeek: targetDays, metrics, verdict: verdict.value },
  };
}

export function validateProfil(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  if (!isIsoDate(body?.updatedAt)) return { error: "updatedAt (YYYY-MM-DD) fehlt" };
  const raw = parseJsonText(body?.highlights);
  if (!Array.isArray(raw)) return { error: "highlights muss eine Liste sein" };
  if (raw.length > MAX_HIGHLIGHTS) return { error: `höchstens ${MAX_HIGHLIGHTS} highlights` };
  const highlights = [];
  for (const h of raw) {
    const text = typeof h?.text === "string" ? h.text.trim() : "";
    if (!text) return { error: "highlights.text fehlt" };
    if (text.length > MAX_HIGHLIGHT) return { error: `highlights.text länger als ${MAX_HIGHLIGHT} Zeichen` };
    const strength = String(h?.strength ?? "").trim().toLowerCase();
    if (!PROFIL_STRENGTH.includes(strength)) return { error: `highlights.strength muss ${PROFIL_STRENGTH.join(" oder ")} sein` };
    highlights.push({ text, strength });
  }
  return { value: { fetchedAt: new Date(fetchedAt).toISOString(), updatedAt: String(body.updatedAt), highlights } };
}

export async function readExperiment(env) {
  return readKvJson(env, EXPERIMENT_KV_KEY).catch(() => null);
}

export async function readProfil(env) {
  return readKvJson(env, PROFIL_KV_KEY).catch(() => null);
}

// ISO-Kalenderwoche eines Montags (YYYY-MM-DD)
function isoWeekNumber(monday) {
  const thu = new Date(Date.parse(monday + "T00:00:00Z") + 3 * 86400000);
  const jan1 = Date.UTC(thu.getUTCFullYear(), 0, 1);
  return Math.floor((thu.getTime() - jan1) / 86400000 / 7) + 1;
}

// Trainingstage je ISO-Woche (letzte 8 Wochen inkl. der laufenden): Anzahl der Tage mit mindestens einer Aktivitaet,
// ueber alle Sportarten (auch WeightTraining aus EGYM). Aktivitaeten mit icu_ignore_time zaehlen nicht.
export function computeTrainingDays(activities, todayIso, weeks = TRAINING_DAYS_WEEKS) {
  const thisMonday = mondayOnOrBefore(todayIso);
  const mondays = [];
  for (let i = weeks - 1; i >= 0; i--) mondays.push(isoDate(new Date(Date.parse(thisMonday + "T00:00:00Z") - i * 7 * 86400000)));
  const days = new Map(mondays.map((m) => [m, new Set()]));
  for (const a of activities ?? []) {
    if (a?.icu_ignore_time) continue;
    const day = activityDay(a);
    if (!isIsoDate(day) || day > todayIso) continue;
    days.get(mondayOnOrBefore(day))?.add(day);
  }
  return mondays.map((week) => ({ week, kw: isoWeekNumber(week), days: days.get(week).size, complete: week !== thisMonday }));
}
