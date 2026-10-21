import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";
import { isoDateBerlin } from "./date-utils.js";

// Tagesaussage des Coaching-Tasks. Der Task schickt sie per PUT /api/coach (Bearer DASHBOARD_TOKEN), per MCP-Tool
// save_coach oder als coach.json auf den GitHub-Branch; das Dashboard zeigt sie als Karte, das Widget als Hinweiszeile.
//
// Body: { fetchedAt: ISO-Zeitstempel,
//         note: string (max 160, Hinweis fuer heute, steht im Widget),
//         headline?: string (max 80, Kurzurteil),
//         text?: string (max 8000, ganzer Bericht fuers Dashboard),
//         go?: "hart" | "locker" | "frei" (Urteil des Coaches: Qualitaet erlaubt, nur locker, frei/Ruhetag),
//         blockers?: string[] (max 3 je max 60, was heute dagegen spricht, z. B. "Hüfte zwickt", "Schlaf 5 h"),
//         question?: string (max 300, Rueckfrage des Coaches an Gerbert),
//         answer?: string (max 1000, seine Antwort), answeredAt?: ISO-Zeitstempel,
//         idea?: { text: string (max 240), angle?: 1-8 (Blickwinkel), sport?: "lauf" | "rad" | "schwimmen" | "kraft" | "alle" } }
// Die Antwort kommt entweder aus dem Dashboard (POST /api/coach/answer) oder aus dem Chat mit dem Coach (save_coach mit
// answer). Der Coach liest sie am naechsten Morgen per MCP-Tool get_coach, bevor er die neue Aussage schreibt.
//
// Idee der Woche (idea): eine freiwillige Anregung, die der Coach montags mitschickt. Sie bleibt ueber die taeglichen
// Updates stehen (mergeCoach): ohne idea unveraendert, leerer text entfernt sie. Jede neue Idee kommt vorn in die
// Historie (ideas, max. 12, je Tag ein Eintrag), damit der Coach sich nicht wiederholt.

export const COACH_KV_KEY = "dashboard:coach";
export const COACH_MAX_AGE_HOURS = 36;
const MAX_BODY_BYTES = 40_000;
const MAX_NOTE = 160;
const MAX_HEADLINE = 80;
const MAX_TEXT = 8000;
const MAX_QUESTION = 300;
const MAX_ANSWER = 1000;
export const COACH_GO = ["hart", "locker", "frei"];
const MAX_BLOCKERS = 3;
const MAX_BLOCKER = 60;
const MAX_IDEA = 240;
const MAX_IDEAS = 12;
export const IDEA_SPORTS = ["lauf", "rad", "schwimmen", "kraft", "alle"];
const IDEA_HINT = " (ohne idea speichern, um die Tagesaussage trotzdem zu sichern)";

// undefined = Feld fehlt (Idee bleibt), null = leerer text (Idee entfernen), sonst die neue Idee.
function validateIdea(idea) {
  if (idea == null) return { value: undefined };
  // Clients mit veraltetem Tool-Schema schicken das Objekt als JSON-Text
  if (typeof idea === "string" && idea.trim().startsWith("{")) {
    try { idea = JSON.parse(idea); } catch { return { error: "idea ist kein gültiges JSON-Objekt" + IDEA_HINT }; }
  }
  if (typeof idea !== "object" || Array.isArray(idea)) return { error: "idea muss ein Objekt { text, angle?, sport? } sein" + IDEA_HINT };
  if (idea.text != null && typeof idea.text !== "string") return { error: "idea.text muss ein Text sein" + IDEA_HINT };
  const text = (idea.text ?? "").trim();
  if (!text) return { value: null };
  if (text.length > MAX_IDEA) return { error: `idea.text länger als ${MAX_IDEA} Zeichen` + IDEA_HINT };
  const angle = idea.angle == null || idea.angle === "" ? null : Number(idea.angle);
  if (angle != null && !(Number.isInteger(angle) && angle >= 1 && angle <= 8)) return { error: "idea.angle muss eine ganze Zahl von 1 bis 8 sein" + IDEA_HINT };
  const sport = idea.sport == null || idea.sport === "" ? null : String(idea.sport).trim().toLowerCase();
  if (sport != null && !IDEA_SPORTS.includes(sport)) return { error: `idea.sport muss ${IDEA_SPORTS.join(", ")} sein` + IDEA_HINT };
  return { value: { text, angle, sport } };
}

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
  const go = body?.go == null || body.go === "" ? null : String(body.go).trim().toLowerCase();
  if (go != null && !COACH_GO.includes(go)) return { error: `go muss ${COACH_GO.join(", ")} sein` };
  if (body?.blockers != null && !Array.isArray(body.blockers)) return { error: "blockers muss eine Liste sein" };
  const blockers = (body?.blockers ?? []).map((x) => String(x ?? "").trim()).filter(Boolean);
  if (blockers.length > MAX_BLOCKERS) return { error: `höchstens ${MAX_BLOCKERS} blockers` };
  if (blockers.some((x) => x.length > MAX_BLOCKER)) return { error: `blocker länger als ${MAX_BLOCKER} Zeichen` };
  const question = typeof body?.question === "string" ? body.question.trim() : "";
  if (question.length > MAX_QUESTION) return { error: `question länger als ${MAX_QUESTION} Zeichen` };
  const answer = typeof body?.answer === "string" ? body.answer.trim() : "";
  if (answer.length > MAX_ANSWER) return { error: `answer länger als ${MAX_ANSWER} Zeichen` };
  if (answer && !question) return { error: "answer ohne question" };
  const answeredMs = Date.parse(body?.answeredAt);
  const answeredAt = answer ? new Date(Number.isFinite(answeredMs) ? answeredMs : Date.now()).toISOString() : null;
  const idea = validateIdea(body?.idea);
  if (idea.error) return { error: idea.error };
  const value = { fetchedAt: new Date(fetchedAt).toISOString(), note, headline: headline || null, go, blockers, text: text || null, question: question || null, answer: answer || null, answeredAt };
  // idea nur, wenn mitgeschickt; mergeCoach unterscheidet so "fehlt" von "entfernen"
  if (idea.value !== undefined) value.idea = idea.value;
  return { value };
}

// Neue Tagesaussage auf die gespeicherte legen: Idee und Historie bleiben, solange kein idea mitkommt.
export function mergeCoach(prev, next) {
  const { idea, ...rest } = next;
  let current = prev?.idea ?? null;
  let ideas = Array.isArray(prev?.ideas) ? prev.ideas : [];
  if (idea === null) current = null;
  else if (idea) {
    current = { ...idea, createdAt: next.fetchedAt };
    // Ein zweites Speichern am selben Tag (Berlin) ersetzt den Eintrag statt einen weiteren anzulegen.
    const date = isoDateBerlin(new Date(next.fetchedAt));
    ideas = [{ date, ...idea }, ...ideas.filter((x) => x?.date !== date)].slice(0, MAX_IDEAS);
  }
  return { ...rest, idea: current, ideas };
}

// Antwort aus dem Dashboard: haengt sie an die aktuelle Aussage, ohne Urteil und Bericht des Coaches zu veraendern.
export function applyAnswer(coach, body, nowMs = Date.now()) {
  if (!coach?.question) return { error: "Keine offene Frage des Coaches" };
  const answer = typeof body?.answer === "string" ? body.answer.trim() : "";
  if (!answer) return { error: "answer fehlt" };
  if (answer.length > MAX_ANSWER) return { error: `answer länger als ${MAX_ANSWER} Zeichen` };
  return { value: { ...coach, answer, answeredAt: new Date(nowMs).toISOString() } };
}

export async function readCoach(env) {
  return readKvJson(env, COACH_KV_KEY).catch(() => null);
}

// Das Widget zeigt nur eine frische Aussage; eine von gestern Mittag wuerde heute falsch wirken.
export function isFreshCoach(coach, nowMs) {
  const at = Date.parse(coach?.fetchedAt);
  return Number.isFinite(at) && Number.isFinite(nowMs) && nowMs - at >= 0 && nowMs - at <= COACH_MAX_AGE_HOURS * 3600_000;
}

export async function handleCoachAnswerRequest(req, env, isAuthorized) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  if (req.method !== "POST") return json({ ok: false, error: "Nur POST erlaubt" }, 405, { ...headers, allow: "POST" });
  if (!env?.KV) return json({ ok: false, error: "KV nicht verfügbar" }, 503, headers);
  const text = await req.text();
  if (text.length > MAX_BODY_BYTES) return json({ ok: false, error: "Body zu groß" }, 413, headers);
  let body;
  try {
    body = JSON.parse(text);
  } catch {
    return json({ ok: false, error: "Kein gültiges JSON" }, 400, headers);
  }
  const { value, error } = applyAnswer(await readCoach(env), body);
  if (error) return json({ ok: false, error }, 409, headers);
  await writeKvJson(env, COACH_KV_KEY, value);
  return json({ ok: true, answeredAt: value.answeredAt }, 200, headers);
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
  await writeKvJson(env, COACH_KV_KEY, mergeCoach(await readCoach(env), value));
  return json({ ok: true, chars: value.note.length }, 200, headers);
}
