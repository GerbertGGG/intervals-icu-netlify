import { json } from "./http-helpers.js";
import { readKvJson, writeKvJson } from "./kv.js";
import { isoDateBerlin, isIsoDate, mondayOnOrBefore } from "./date-utils.js";

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
//         idea?: { text: string (max 240), angle?: 1-8 (Blickwinkel), sport?: "lauf" | "rad" | "schwimmen" | "kraft" | "alle" },
//         week?: { light: "gruen" | "gelb" | "rot", text?: string (max 160) },
//         week_plan?: { week?: Montag YYYY-MM-DD, basis?: string (max 160), deload?: boolean,
//                       targets: [{ sport: "lauf" | "rad" | "schwimmen" | "kraft", time_min: Zahl, tss_max?: Zahl | null }],
//                       days?: [{ day: string (max 12), text: string (max 120) }] } }
// Die Antwort kommt entweder aus dem Dashboard (POST /api/coach/answer) oder aus dem Chat mit dem Coach (save_coach mit
// answer). Der Coach liest sie am naechsten Morgen per MCP-Tool get_coach, bevor er die neue Aussage schreibt.
//
// Idee der Woche (idea): eine freiwillige Anregung, die der Coach montags mitschickt. Sie bleibt ueber die taeglichen
// Updates stehen (mergeCoach): ohne idea unveraendert, leerer text entfernt sie. Jede neue Idee kommt vorn in die
// Historie (ideas, max. 12, je Tag ein Eintrag), damit der Coach sich nicht wiederholt.
//
// Wochenampel (week): das Urteil des Coaches zur letzten vollen Woche, montags mitgeschickt. Das Dashboard zeigt sie in der
// Montagsbilanz, rechnet sie aber nicht selbst. Sie bleibt wie die Idee ueber die taeglichen Updates stehen; leeres light
// entfernt sie.
//
// Wochenvorschlag (week_plan): der Wochenrahmen des Coaches, nur montags gespeichert (an anderen Tagen Fehler). Das Dashboard
// zeigt ihn bis zum naechsten Montag, ohne Abhaken und ohne Abgleich mit den Einheiten. Er bleibt ueber die taeglichen Updates
// stehen; {} entfernt ihn. deload: true markiert die Woche als Entlastung (deloadWeeks, max. 26 Montage): Nur diese Wochen
// lassen die Leitplanken im Trainingsblock aus, einen festen Rhythmus gibt es nicht.

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
export const WEEK_LIGHTS = ["gruen", "gelb", "rot"];
const MAX_WEEK_TEXT = 160;
const IDEA_HINT = " (ohne idea speichern, um die Tagesaussage trotzdem zu sichern)";
export const PLAN_SPORTS = ["lauf", "rad", "schwimmen", "kraft"];
const MAX_PLAN_BASIS = 160;
const MAX_PLAN_DAYS = 14;
const MAX_PLAN_DAY = 12;
const MAX_PLAN_DAY_TEXT = 120;
const MAX_PLAN_MIN = 3000;
const MAX_PLAN_TSS = 2000;
const MAX_DELOAD_WEEKS = 26;
const PLAN_HINT = " (ohne week_plan speichern, um die Tagesaussage trotzdem zu sichern)";

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

// Wie validateIdea: undefined = Feld fehlt (Ampel bleibt), null = leeres light (entfernen), sonst die neue Ampel.
function validateWeek(week) {
  if (week == null) return { value: undefined };
  if (typeof week === "string" && week.trim().startsWith("{")) {
    try { week = JSON.parse(week); } catch { return { error: "week ist kein gültiges JSON-Objekt" }; }
  }
  if (typeof week !== "object" || Array.isArray(week)) return { error: "week muss ein Objekt { light, text? } sein" };
  const light = week.light == null ? "" : String(week.light).trim().toLowerCase().replace("ü", "ue");
  if (!light) return { value: null };
  if (!WEEK_LIGHTS.includes(light)) return { error: `week.light muss ${WEEK_LIGHTS.join(", ")} sein` };
  if (week.text != null && typeof week.text !== "string") return { error: "week.text muss ein Text sein" };
  const text = (week.text ?? "").trim();
  if (text.length > MAX_WEEK_TEXT) return { error: `week.text länger als ${MAX_WEEK_TEXT} Zeichen` };
  return { value: { light, text: text || null } };
}

// Wie validateIdea: undefined = Feld fehlt (Vorschlag bleibt), null = {} (entfernen), sonst der neue Wochenvorschlag.
function validateWeekPlan(plan, fetchedAt) {
  if (plan == null) return { value: undefined };
  if (typeof plan === "string" && plan.trim().startsWith("{")) {
    try { plan = JSON.parse(plan); } catch { return { error: "week_plan ist kein gültiges JSON-Objekt" + PLAN_HINT }; }
  }
  if (typeof plan !== "object" || Array.isArray(plan)) return { error: "week_plan muss ein Objekt { week, basis, deload, targets, days? } sein" + PLAN_HINT };
  if (!Object.keys(plan).length) return { value: null };
  const today = isoDateBerlin(new Date(fetchedAt));
  if (mondayOnOrBefore(today) !== today) return { error: "week_plan nur montags speichern" + PLAN_HINT };
  const week = plan.week == null || plan.week === "" ? today : String(plan.week).trim();
  if (!isIsoDate(week) || mondayOnOrBefore(week) !== week) return { error: "week_plan.week muss ein Montag (YYYY-MM-DD) sein" + PLAN_HINT };
  if (plan.basis != null && typeof plan.basis !== "string") return { error: "week_plan.basis muss ein Text sein" + PLAN_HINT };
  const basis = (plan.basis ?? "").trim();
  if (basis.length > MAX_PLAN_BASIS) return { error: `week_plan.basis länger als ${MAX_PLAN_BASIS} Zeichen` + PLAN_HINT };
  if (plan.deload != null && typeof plan.deload !== "boolean") return { error: "week_plan.deload muss true oder false sein" + PLAN_HINT };
  if (!Array.isArray(plan.targets) || !plan.targets.length) return { error: "week_plan.targets muss eine Liste mit mind. einer Sportart sein" + PLAN_HINT };
  const targets = [];
  for (const t of plan.targets) {
    const sport = String(t?.sport ?? "").trim().toLowerCase();
    if (!PLAN_SPORTS.includes(sport)) return { error: `week_plan.targets.sport muss ${PLAN_SPORTS.join(", ")} sein` + PLAN_HINT };
    if (targets.some((x) => x.sport === sport)) return { error: `week_plan.targets: ${sport} doppelt` + PLAN_HINT };
    const time = Number(t.time_min);
    if (t.time_min == null || !Number.isFinite(time) || time < 0 || time > MAX_PLAN_MIN) return { error: `week_plan.targets.time_min muss eine Zahl von 0 bis ${MAX_PLAN_MIN} sein` + PLAN_HINT };
    const tss = t.tss_max == null || t.tss_max === "" ? null : Number(t.tss_max);
    if (tss != null && !(Number.isFinite(tss) && tss >= 0 && tss <= MAX_PLAN_TSS)) return { error: `week_plan.targets.tss_max muss eine Zahl von 0 bis ${MAX_PLAN_TSS} oder null sein` + PLAN_HINT };
    targets.push({ sport, time_min: Math.round(time), tss_max: tss == null ? null : Math.round(tss) });
  }
  if (plan.days != null && !Array.isArray(plan.days)) return { error: "week_plan.days muss eine Liste sein" + PLAN_HINT };
  const days = (plan.days ?? []).map((x) => ({ day: String(x?.day ?? "").trim(), text: String(x?.text ?? "").trim() })).filter((x) => x.day || x.text);
  if (days.length > MAX_PLAN_DAYS) return { error: `höchstens ${MAX_PLAN_DAYS} week_plan.days` + PLAN_HINT };
  if (days.some((x) => !x.day || !x.text)) return { error: "week_plan.days braucht je day und text" + PLAN_HINT };
  if (days.some((x) => x.day.length > MAX_PLAN_DAY || x.text.length > MAX_PLAN_DAY_TEXT)) return { error: `week_plan.days: day max. ${MAX_PLAN_DAY}, text max. ${MAX_PLAN_DAY_TEXT} Zeichen` + PLAN_HINT };
  return { value: { week, basis: basis || null, deload: plan.deload === true, targets, days } };
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
  const week = validateWeek(body?.week);
  if (week.error) return { error: week.error };
  const weekPlan = validateWeekPlan(body?.week_plan, fetchedAt);
  if (weekPlan.error) return { error: weekPlan.error };
  const value = { fetchedAt: new Date(fetchedAt).toISOString(), note, headline: headline || null, go, blockers, text: text || null, question: question || null, answer: answer || null, answeredAt };
  // idea nur, wenn mitgeschickt; mergeCoach unterscheidet so "fehlt" von "entfernen"
  if (idea.value !== undefined) value.idea = idea.value;
  if (week.value !== undefined) value.week = week.value;
  if (weekPlan.value !== undefined) value.week_plan = weekPlan.value;
  return { value };
}

// Neue Tagesaussage auf die gespeicherte legen: Idee, Historie, Wochenampel und Wochenvorschlag bleiben, solange sie nicht
// mitkommen.
// Die Antwort bleibt, solange der Coach dieselbe question noch einmal schickt (z. B. zweites Update am Sonntag); erst eine
// neue question (oder keine mehr) entfernt sie.
export function mergeCoach(prev, next) {
  const { idea, week, week_plan: weekPlan, ...rest } = next;
  if (!rest.answer && rest.question && rest.question === prev?.question && prev?.answer) {
    rest.answer = prev.answer;
    rest.answeredAt = prev.answeredAt ?? null;
  }
  const weekLight = week === undefined ? (prev?.week ?? null) : week && { ...week, createdAt: next.fetchedAt };
  let current = prev?.idea ?? null;
  let ideas = Array.isArray(prev?.ideas) ? prev.ideas : [];
  if (idea === null) current = null;
  else if (idea) {
    current = { ...idea, createdAt: next.fetchedAt };
    // Ein zweites Speichern am selben Tag (Berlin) ersetzt den Eintrag statt einen weiteren anzulegen.
    const date = isoDateBerlin(new Date(next.fetchedAt));
    ideas = [{ date, ...idea }, ...ideas.filter((x) => x?.date !== date)].slice(0, MAX_IDEAS);
  }
  // Entlastungswochen nur, wie der Coach sie markiert: ein neuer Vorschlag setzt oder streicht seine Woche
  let deloadWeeks = Array.isArray(prev?.deloadWeeks) ? prev.deloadWeeks : [];
  if (weekPlan) deloadWeeks = [...(weekPlan.deload ? [weekPlan.week] : []), ...deloadWeeks.filter((x) => x !== weekPlan.week)].slice(0, MAX_DELOAD_WEEKS);
  const plan = weekPlan === undefined ? (prev?.week_plan ?? null) : weekPlan && { ...weekPlan, createdAt: next.fetchedAt };
  return { ...rest, idea: current, ideas, week: weekLight, week_plan: plan, deloadWeeks };
}

// Antwort aus dem Dashboard: haengt sie an die aktuelle Aussage, ohne Urteil und Bericht des Coaches zu veraendern.
export function applyAnswer(coach, body, nowMs = Date.now()) {
  if (!coach?.question) return { error: "Keine offene Frage des Coaches" };
  const answer = typeof body?.answer === "string" ? body.answer.trim() : "";
  if (!answer) return { error: "answer fehlt" };
  if (answer.length > MAX_ANSWER) return { error: `answer länger als ${MAX_ANSWER} Zeichen` };
  return { value: { ...coach, answer, answeredAt: new Date(nowMs).toISOString() } };
}

// Zeiten des Wochenvorschlags aus dem Dashboard anpassen (Minuten je Sportart). Nur vorhandene Sportarten, nur time_min; Basis,
// Tage und TSS bleiben. Die Zahlen des Coaches bleiben in original (nur beim ersten Anpassen gesetzt), editedAt zeigt die Änderung.
// Der naechste week_plan des Coaches ersetzt alles.
export function applyPlanEdit(coach, body, nowMs = Date.now()) {
  const plan = coach?.week_plan;
  if (!plan?.targets?.length) return { error: "Kein Wochenvorschlag vorhanden" };
  if (!Array.isArray(body?.targets) || !body.targets.length) return { error: "targets muss eine Liste { sport, time_min } sein" };
  const sent = new Map();
  for (const x of body.targets) {
    const sport = typeof x?.sport === "string" ? x.sport.trim().toLowerCase() : "";
    if (!plan.targets.some((t) => t.sport === sport)) return { error: `${sport || "sport"} steht nicht im Wochenvorschlag` };
    if (sent.has(sport)) return { error: `${sport} doppelt` };
    const min = typeof x.time_min === "string" ? Number(x.time_min.replace(",", ".")) : x.time_min;
    if (x.time_min == null || x.time_min === "" || !Number.isFinite(min) || min < 0 || min > MAX_PLAN_MIN) return { error: `time_min muss eine Zahl von 0 bis ${MAX_PLAN_MIN} sein` };
    sent.set(sport, Math.round(min));
  }
  const original = plan.original ?? plan.targets.map(({ sport, time_min }) => ({ sport, time_min }));
  const targets = plan.targets.map((t) => (sent.has(t.sport) ? { ...t, time_min: sent.get(t.sport) } : t));
  return { value: { ...coach, week_plan: { ...plan, targets, original, editedAt: new Date(nowMs).toISOString() } } };
}

// Vom Nutzer angepasste Zeiten des Wochenvorschlags (editedAt) gelten als Zeitziel der Woche im ganzen Dashboard: Wochenkarte, Ringe,
// Resttag-Tempo, Widget und Wochensummen. Ohne Anpassung bleibt das Ziel aus dem Kalender. TSS und Kraft bleiben beim Coach.
const PLAN_TO_SPORT = { lauf: "run", rad: "bike", schwimmen: "swim" };
const PLAN_TO_DISC = { lauf: "lauf", rad: "rad", schwimmen: "schwimmen" };
export function applyEditedPlan(weeks, weekTotals, plan) {
  if (!plan?.editedAt || !plan.week || !plan.targets?.length) return { weeks, weekTotals };
  const endur = plan.targets.filter((t) => PLAN_TO_SPORT[t.sport] && Number.isFinite(Number(t.time_min)));
  if (!endur.length) return { weeks, weekTotals };
  const newWeeks = weeks.map((w) => {
    if (w.weekStart !== plan.week) return w;
    const bySport = { ...w.bySport };
    for (const t of endur) bySport[PLAN_TO_SPORT[t.sport]] = { ...bySport[PLAN_TO_SPORT[t.sport]], targetMin: Math.round(Number(t.time_min)) || null };
    const targetMin = Object.values(bySport).reduce((a, b) => a + (b.targetMin ?? 0), 0);
    return { ...w, bySport, targetMin: targetMin || null };
  });
  const total = plan.targets.filter((t) => PLAN_TO_DISC[t.sport]).reduce((a, t) => a + (Number(t.time_min) || 0), 0);
  let totals = weekTotals;
  if (weekTotals?.weeks?.some((x) => x.week === plan.week)) {
    const list = weekTotals.weeks.map((x) => (x.week === plan.week ? { ...x, goalHours: Math.round((total / 60) * 100) / 100 } : x));
    const cur = list[list.length - 1];
    totals = { ...weekTotals, weeks: list, goals: cur.week === plan.week ? { ...weekTotals.goals, enduranceHours: cur.goalHours } : weekTotals.goals };
  }
  return { weeks: newWeeks, weekTotals: totals };
}

export async function handleCoachPlanRequest(req, env, isAuthorized) {
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
  const { value, error } = applyPlanEdit(await readCoach(env), body);
  if (error) return json({ ok: false, error }, 409, headers);
  await writeKvJson(env, COACH_KV_KEY, value);
  return json({ ok: true, editedAt: value.week_plan.editedAt }, 200, headers);
}

export async function readCoach(env) {
  return readKvJson(env, COACH_KV_KEY).catch(() => null);
}

// Das Widget zeigt nur eine frische Aussage; eine von gestern Mittag wuerde heute falsch wirken.
// Etwas Vorlauf erlaubt: Der Coach setzt fetchedAt selbst und liegt dabei schon mal in der Zukunft (z. B. Ortszeit
// mit "Z"). Ohne Vorlauf gaelte seine Aussage bis dahin nicht als frisch und es stuende "Noch keine Coach-Auswertung" da.
const COACH_MAX_AHEAD_HOURS = 3;
export function isFreshCoach(coach, nowMs) {
  const at = Date.parse(coach?.fetchedAt);
  return Number.isFinite(at) && Number.isFinite(nowMs) && nowMs - at >= -COACH_MAX_AHEAD_HOURS * 3600_000 && nowMs - at <= COACH_MAX_AGE_HOURS * 3600_000;
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
