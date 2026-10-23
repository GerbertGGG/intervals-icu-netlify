import { readKvJson } from "./kv.js";
import { isIsoDate, isoDateBerlin, mondayOnOrBefore } from "./date-utils.js";

// Wochenziele, die der Coach per MCP-Tool save_targets setzt: Ausdauerstunden je ISO-Woche (Laufen, Rad, Schwimmen),
// Kraftsaetze in 2 Wochen (rollierend) und die Schwelle fuer "Wochen in Folge" in Prozent des Wochenziels.
// Jeder Aufruf haengt einen Eintrag an die Historie an (nichts wird ueberschrieben). Fuer einen Tag gilt der zuletzt
// gespeicherte Eintrag mit validFrom <= Tag, sonst der Startwert (DEFAULT_TARGET). Das Dashboard und get_coach zeigen
// die Ziele nur an und rechnen Ist gegen Ziel, ohne eigene Bewertung.
//
// Eintrag: { enduranceHoursPerWeek: 1-20 in 0,25er-Schritten, strengthSets2w: 0-300 (ganze Zahl), streakThresholdPct: 50-100
//            (Standard 90), reason (max 200), validFrom: YYYY-MM-DD (Standard: Montag der laufenden Woche, Berlin),
//            fetchedAt: ISO-Zeitstempel }
// KV: { items: [Eintrag], aelteste zuerst, max MAX_ITEMS }

export const TARGETS_KV_KEY = "dashboard:targets";
// Startwert, solange der Coach nichts gespeichert hat (und fuer Tage vor seinem ersten Eintrag)
export const DEFAULT_TARGET = { enduranceHoursPerWeek: 5, strengthSets2w: 90, streakThresholdPct: 90, validFrom: "2026-10-19", reason: "Startwert Block A", fetchedAt: null, default: true };
const MAX_REASON = 200;
const MAX_ITEMS = 100;
export const COACH_HISTORY = 10;

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  return Number.isFinite(n) ? n : null;
}

export function validateTargets(body, todayIso = isoDateBerlin()) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const h = num(body?.enduranceHoursPerWeek);
  if (h == null || h < 1 || h > 20 || Math.round(h * 4) !== h * 4) return { error: "enduranceHoursPerWeek muss 1 bis 20 sein, in Schritten von 0,25" };
  const sets = num(body?.strengthSets2w);
  if (sets == null || !Number.isInteger(sets) || sets < 0 || sets > 300) return { error: "strengthSets2w muss eine ganze Zahl von 0 bis 300 sein" };
  const pct = body?.streakThresholdPct == null || body.streakThresholdPct === "" ? DEFAULT_TARGET.streakThresholdPct : num(body.streakThresholdPct);
  if (pct == null || pct < 50 || pct > 100) return { error: "streakThresholdPct muss 50 bis 100 sein" };
  const reason = typeof body?.reason === "string" ? body.reason.trim() : "";
  if (!reason) return { error: "reason fehlt" };
  if (reason.length > MAX_REASON) return { error: `reason länger als ${MAX_REASON} Zeichen` };
  const validFrom = body?.validFrom == null || body.validFrom === "" ? mondayOnOrBefore(todayIso) : String(body.validFrom);
  if (!isIsoDate(validFrom) || Number.isNaN(Date.parse(validFrom + "T00:00:00Z"))) return { error: "validFrom muss ein Datum YYYY-MM-DD sein" };
  return { value: { enduranceHoursPerWeek: h, strengthSets2w: sets, streakThresholdPct: pct, reason, validFrom, fetchedAt: new Date(fetchedAt).toISOString() } };
}

// Haengt einen Eintrag an (aelteste zuerst); bei mehr als MAX_ITEMS fallen die aeltesten heraus
export function appendTarget(stored, entry) {
  const items = Array.isArray(stored?.items) ? stored.items : [];
  return { items: [...items, entry].slice(-MAX_ITEMS) };
}

// Alle Eintraege in Speicher-Reihenfolge, der Startwert vorne
export function targetHistory(stored) {
  return [DEFAULT_TARGET, ...(Array.isArray(stored?.items) ? stored.items : [])];
}

// Gilt an einem Tag: der zuletzt gespeicherte Eintrag mit validFrom <= Tag, sonst der Startwert
export function targetOn(history, dayIso) {
  for (let i = history.length - 1; i >= 0; i--) if (history[i].validFrom <= dayIso) return history[i];
  return history[0] ?? DEFAULT_TARGET;
}

const pub = (t) => ({ enduranceHoursPerWeek: t.enduranceHoursPerWeek, strengthSets2w: t.strengthSets2w, streakThresholdPct: t.streakThresholdPct ?? DEFAULT_TARGET.streakThresholdPct, validFrom: t.validFrom, reason: t.reason, savedAt: t.fetchedAt ?? null, ...(t.default ? { default: true } : {}) });

// Fuer Dashboard und get_coach: was heute gilt, was vorher galt (anderer Wert), geplante Eintraege (validFrom in der Zukunft)
// und die Historie, neueste zuerst
export function targetsView(stored, todayIso = isoDateBerlin(), limit = null) {
  const history = targetHistory(stored);
  const current = targetOn(history, todayIso);
  const idx = history.lastIndexOf(current);
  const previous = history.slice(0, idx).reverse().find((t) => t.validFrom <= todayIso && (t.enduranceHoursPerWeek !== current.enduranceHoursPerWeek || t.strengthSets2w !== current.strengthSets2w)) ?? null;
  const upcoming = history.slice(idx + 1).filter((t) => t.validFrom > todayIso).map(pub);
  const list = [...history].reverse().map(pub);
  return { current: pub(current), previous: previous ? pub(previous) : null, upcoming, history: limit ? list.slice(0, limit) : list };
}

export async function readTargets(env) {
  return readKvJson(env, TARGETS_KV_KEY).catch(() => null);
}
