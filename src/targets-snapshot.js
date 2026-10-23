import { readKvJson } from "./kv.js";
import { isIsoDate, isoDateBerlin, mondayOnOrBefore } from "./date-utils.js";

// Aktueller Rahmen, den der Coach per MCP-Tool save_targets setzt: Ausdauerstunden je ISO-Woche (Laufen, Rad, Schwimmen),
// Kraftsaetze in 2 Wochen (rollierend), die Schwelle fuer "Wochen in Folge" in Prozent des Wochenziels, die erlaubte Steigerung
// gegenueber der Referenzwoche, Intensitaet je Sportart, Leitplanken und was nicht erlaubt ist.
// Jeder Aufruf haengt einen Eintrag an die Historie an (nichts wird ueberschrieben). Fuer einen Tag gilt der zuletzt
// gespeicherte Eintrag mit validFrom <= Tag, sonst der Startwert (DEFAULT_TARGET). Das Dashboard und get_coach zeigen
// die Ziele nur an und rechnen Ist gegen Ziel, ohne eigene Bewertung.
// Fehlende Felder uebernimmt ein neuer Eintrag von dem Eintrag, der an seinem validFrom gilt; der Coach schickt nur, was sich
// aendert. intensity wird je Sportart ersetzt (text "" streicht die Sportart), guardrails und notAllowed als ganze Liste.
//
// Eintrag: { enduranceHoursPerWeek: 1-20 in 0,25er-Schritten, strengthSets2w: 0-300 (ganze Zahl), streakThresholdPct: 50-100
//            (Standard 90), maxIncreasePct: 0-50 (Standard 10), intensity: [{ sport: lauf|rad|schwimmen|kraft, text (max 160), short (optional, max 40: Kurzfassung fuers Widget) }]
//            (max 6), guardrails: [Text (max 160)] (max 8), notAllowed: [Text (max 80)] (max 8), changed: [geaenderte Felder],
//            reason (max 200), validFrom: YYYY-MM-DD (Standard: Montag der laufenden Woche, Berlin), fetchedAt: ISO-Zeitstempel }
// KV: { items: [Eintrag], aelteste zuerst, max MAX_ITEMS }

export const TARGETS_KV_KEY = "dashboard:targets";
// Startwert, solange der Coach nichts gespeichert hat (und fuer Tage vor seinem ersten Eintrag)
export const DEFAULT_TARGET = {
  enduranceHoursPerWeek: 5, strengthSets2w: 90, streakThresholdPct: 90, maxIncreasePct: 10,
  intensity: [
    { sport: "lauf", text: "1× Hügelsprints am Ende eines lockeren Laufs, 4× 8 s bis 8× 10 s; nicht in Entlastungswochen" },
    { sport: "rad", text: "1× HIIT pro Woche ab Woche 2, z. B. 2× 8× 30/30 oder 3–4× 4 min bei 265–270 W; in Entlastungswochen halbiert" },
  ],
  guardrails: ["Kein HIIT/Sprints 2 Tage vor Blocktest", "Hüft-/Leistenschmerz bei Sprints: sofort abbrechen, 1 Woche aussetzen", "Krankheitswochen zählen nicht als verfehlt"],
  notAllowed: ["Sweet Spot", "Renntempo-Läufe", "Koppelläufe", "andere Laufintervalle"],
  validFrom: "2026-10-19", reason: "Startwert aus Blockplan A", fetchedAt: null, default: true,
};
export const INTENSITY_SPORTS = ["lauf", "rad", "schwimmen", "kraft"];
const FIELDS = ["enduranceHoursPerWeek", "strengthSets2w", "streakThresholdPct", "maxIncreasePct", "intensity", "guardrails", "notAllowed"];
const MAX_REASON = 200, MAX_INTENSITY = 6, MAX_INTENSITY_TEXT = 160, MAX_INTENSITY_SHORT = 40, MAX_GUARDRAILS = 8, MAX_GUARDRAIL_TEXT = 160, MAX_NOT_ALLOWED = 8, MAX_NOT_ALLOWED_TEXT = 80;
const MAX_ITEMS = 100;
export const COACH_HISTORY = 10;

function num(v) {
  if (v == null || v === "") return null;
  const n = Number(typeof v === "string" ? v.replace(",", ".") : v);
  return Number.isFinite(n) ? n : null;
}

const given = (v) => v != null && v !== "";
const sortIntensity = (list) => [...list].sort((a, b) => INTENSITY_SPORTS.indexOf(a.sport) - INTENSITY_SPORTS.indexOf(b.sport));

function textList(v, name, maxItems, maxLen) {
  if (!Array.isArray(v)) return { error: `${name} muss eine Liste von Texten sein` };
  const out = [];
  for (const x of v) {
    const t = typeof x === "string" ? x.trim() : "";
    if (!t) return { error: `${name}: leerer Eintrag` };
    if (t.length > maxLen) return { error: `${name}: Eintrag länger als ${maxLen} Zeichen` };
    out.push(t);
  }
  if (out.length > maxItems) return { error: `${name}: höchstens ${maxItems} Einträge` };
  return { value: out };
}

// Intensitaet je Sportart: geschickte Sportarten ersetzen ihre bisherigen Eintraege, text "" streicht die Sportart, der Rest bleibt
function mergeIntensity(v, base) {
  if (!Array.isArray(v)) return { error: "intensity muss eine Liste { sport, text } sein" };
  const sent = [];
  for (const x of v) {
    const sport = typeof x?.sport === "string" ? x.sport.trim().toLowerCase() : "";
    if (!INTENSITY_SPORTS.includes(sport)) return { error: `intensity.sport muss ${INTENSITY_SPORTS.join(", ")} sein` };
    const text = typeof x?.text === "string" ? x.text.trim() : "";
    if (text.length > MAX_INTENSITY_TEXT) return { error: `intensity.text länger als ${MAX_INTENSITY_TEXT} Zeichen` };
    const short = typeof x?.short === "string" ? x.short.trim() : "";
    if (short.length > MAX_INTENSITY_SHORT) return { error: `intensity.short länger als ${MAX_INTENSITY_SHORT} Zeichen` };
    sent.push(short ? { sport, text, short } : { sport, text });
  }
  const sports = new Set(sent.map((x) => x.sport));
  const out = sortIntensity([...(base ?? []).filter((x) => !sports.has(x.sport)), ...sent.filter((x) => x.text)]);
  if (out.length > MAX_INTENSITY) return { error: `intensity: höchstens ${MAX_INTENSITY} Einträge` };
  return { value: out };
}

// base = Eintrag, der bisher an validFrom gilt (Standard: Startwert); fehlende Felder kommen von dort
export function validateTargets(body, todayIso = isoDateBerlin(), stored = null) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const reason = typeof body?.reason === "string" ? body.reason.trim() : "";
  if (!reason) return { error: "reason fehlt" };
  if (reason.length > MAX_REASON) return { error: `reason länger als ${MAX_REASON} Zeichen` };
  const validFrom = body?.validFrom == null || body.validFrom === "" ? mondayOnOrBefore(todayIso) : String(body.validFrom);
  if (!isIsoDate(validFrom) || Number.isNaN(Date.parse(validFrom + "T00:00:00Z"))) return { error: "validFrom muss ein Datum YYYY-MM-DD sein" };
  const base = targetOn(targetHistory(stored), validFrom);
  const v = {};
  if (given(body?.enduranceHoursPerWeek)) {
    const h = num(body.enduranceHoursPerWeek);
    if (h == null || h < 1 || h > 20 || Math.round(h * 4) !== h * 4) return { error: "enduranceHoursPerWeek muss 1 bis 20 sein, in Schritten von 0,25" };
    v.enduranceHoursPerWeek = h;
  }
  if (given(body?.strengthSets2w)) {
    const sets = num(body.strengthSets2w);
    if (sets == null || !Number.isInteger(sets) || sets < 0 || sets > 300) return { error: "strengthSets2w muss eine ganze Zahl von 0 bis 300 sein" };
    v.strengthSets2w = sets;
  }
  if (given(body?.streakThresholdPct)) {
    const pct = num(body.streakThresholdPct);
    if (pct == null || pct < 50 || pct > 100) return { error: "streakThresholdPct muss 50 bis 100 sein" };
    v.streakThresholdPct = pct;
  }
  if (given(body?.maxIncreasePct)) {
    const inc = num(body.maxIncreasePct);
    if (inc == null || inc < 0 || inc > 50) return { error: "maxIncreasePct muss 0 bis 50 sein" };
    v.maxIncreasePct = inc;
  }
  if (body?.intensity != null) {
    const r = mergeIntensity(body.intensity, base.intensity);
    if (r.error) return r;
    v.intensity = r.value;
  }
  for (const [key, max, len] of [["guardrails", MAX_GUARDRAILS, MAX_GUARDRAIL_TEXT], ["notAllowed", MAX_NOT_ALLOWED, MAX_NOT_ALLOWED_TEXT]]) {
    if (body?.[key] == null) continue;
    const r = textList(body[key], key, max, len);
    if (r.error) return r;
    v[key] = r.value;
  }
  const changed = FIELDS.filter((k) => k in v && JSON.stringify(v[k]) !== JSON.stringify(base[k] ?? null));
  const full = Object.fromEntries(FIELDS.map((k) => [k, k in v ? v[k] : base[k] ?? DEFAULT_TARGET[k]]));
  return { value: { ...full, changed, reason, validFrom, fetchedAt: new Date(fetchedAt).toISOString() } };
}

// Haengt einen Eintrag an (aelteste zuerst); bei mehr als MAX_ITEMS fallen die aeltesten heraus
export function appendTarget(stored, entry) {
  const items = Array.isArray(stored?.items) ? stored.items : [];
  return { items: [...items, entry].slice(-MAX_ITEMS) };
}

// Alle Eintraege in Speicher-Reihenfolge, der Startwert vorne
// Aeltere Eintraege ohne die spaeteren Felder (maxIncreasePct, intensity, ...) erben sie wie ein neuer Eintrag
export function targetHistory(stored) {
  const out = [DEFAULT_TARGET];
  for (const t of Array.isArray(stored?.items) ? stored.items : []) {
    if (FIELDS.every((k) => k in t)) { out.push(t); continue; }
    const base = targetOn(out, t.validFrom);
    out.push({ ...Object.fromEntries(FIELDS.map((k) => [k, base[k] ?? DEFAULT_TARGET[k]])), ...t });
  }
  return out;
}

// Gilt an einem Tag: der zuletzt gespeicherte Eintrag mit validFrom <= Tag, sonst der Startwert
export function targetOn(history, dayIso) {
  for (let i = history.length - 1; i >= 0; i--) if (history[i].validFrom <= dayIso) return history[i];
  return history[0] ?? DEFAULT_TARGET;
}

const pub = (t) => ({
  enduranceHoursPerWeek: t.enduranceHoursPerWeek, strengthSets2w: t.strengthSets2w, streakThresholdPct: t.streakThresholdPct ?? DEFAULT_TARGET.streakThresholdPct,
  maxIncreasePct: t.maxIncreasePct ?? DEFAULT_TARGET.maxIncreasePct, intensity: t.intensity ?? [], guardrails: t.guardrails ?? [], notAllowed: t.notAllowed ?? [],
  ...(Array.isArray(t.changed) ? { changed: t.changed } : {}),
  validFrom: t.validFrom, reason: t.reason, savedAt: t.fetchedAt ?? null, ...(t.default ? { default: true } : {}),
});

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
