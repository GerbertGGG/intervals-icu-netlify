import { readKvJson, writeKvJson } from "./kv.js";
import { isIsoDate, isoDateBerlin } from "./date-utils.js";
import { json } from "./http-helpers.js";
import { gate, readBody } from "./decisions-snapshot.js";

// Tests des Coaches (Ansicht "Tests" im Dashboard): mehrere Tests gleichzeitig, dazu Kandidaten und abgeschlossene Tests mit
// "Gelernt". Der Coach schreibt je Aufruf einen Test per save_test (Upsert über id); get_coach liefert sie unter tests.
// Das Urteil eines Tests steht nur hier; die Wissensbasis-Karte leitet "Du machst" und "Bei dir" daraus ab (applyTestsToWissensbasis),
// gespeichert wird dort nichts doppelt. Das Dashboard bewertet nichts.
//
// KV: dashboard:tests = { fetchedAt, tests: Test[] }, höchstens 40 (zuerst fallen die ältesten mit status "verworfen" weg).
// Test: { id, title (80), kind: test|paket|begleiter, discipline, block (20), blockGoal (140), question (160), studyRefs? (max 6),
//         insightRefs? (max 6), change (160), startDate?, checkDate?, metrics (1-4), expectation?, load: mehr|neutral,
//         adherence? { rule (120), minPct 0-100, actualPct|null }, abort? (140), status: kandidat|laeuft|urteil|gelernt|verworfen,
//         result?: bestaetigt|widerlegt|unklar|gestoert|abgebrochen, confoundedWith? (Test-ids), learned? (200),
//         proposedBy: coach|gerbert, decidedAt?, updatedAt (Worker) }
// Das alte dashboard:experiment bleibt unverändert lesbar (save_experiment).

export const TESTS_KV_KEY = "dashboard:tests";
export const MAX_TESTS = 40;
export const TEST_KIND = ["test", "paket", "begleiter"];
export const TEST_DISCIPLINE = ["lauf", "rad", "schwimmen", "kraft", "erholung", "ernaehrung", "alle"];
export const TEST_LOAD = ["mehr", "neutral"];
export const TEST_STATUS = ["kandidat", "laeuft", "urteil", "gelernt", "verworfen"];
export const TEST_RESULT = ["bestaetigt", "widerlegt", "unklar", "gestoert", "abgebrochen"];
export const TEST_PROPOSED_BY = ["coach", "gerbert"];
const BETTER = ["lower", "higher"];
// Bekannte Messgrößen mit ihrer Richtung (Standard, wenn better fehlt); eigene keys brauchen better
const KNOWN_METRICS = { lauf_hf130: "lower", rad_hf165w: "lower", schwimm_amstueck: "higher", schwimm_100m: "lower" };
const ID_RE = /^[a-z0-9äöüß]+(?:-[a-z0-9äöüß]+)*$/;
const MAX_REFS = 6;
const MAX_ID = 60;
const MAX_METRICS = 4;
const MAX_HEAVY_RUNNING = 2;
const STATUS_ORDER = ["laeuft", "kandidat", "urteil", "gelernt"];

const isObj = (v) => v != null && typeof v === "object" && !Array.isArray(v);
const parseJsonText = (v) => {
  if (typeof v !== "string" || !/^\s*[[{]/.test(v)) return v;
  try { return JSON.parse(v); } catch { return v; }
};
const blank = (v) => v == null || v === "";

function reqText(v, name, max) {
  const t = typeof v === "string" ? v.trim() : "";
  if (!t) return { error: `${name} fehlt` };
  if (t.length > max) return { error: `${name} länger als ${max} Zeichen` };
  return { value: t };
}

function optText(v, name, max) {
  if (blank(v)) return { value: null };
  if (typeof v !== "string") return { error: `${name} muss ein Text sein` };
  const t = v.trim();
  if (t.length > max) return { error: `${name} länger als ${max} Zeichen` };
  return { value: t || null };
}

function oneOf(v, name, list) {
  const t = String(v ?? "").trim().toLowerCase();
  if (!list.includes(t)) return { error: `${name} muss ${list.join(" | ")} sein` };
  return { value: t };
}

function optDate(v, name) {
  if (blank(v)) return { value: null };
  if (!isIsoDate(v)) return { error: `${name} muss ein Datum YYYY-MM-DD oder null sein` };
  return { value: String(v) };
}

function optNumber(v, name) {
  if (blank(v)) return { value: null };
  const n = Number(v);
  if (!Number.isFinite(n)) return { error: `${name} muss eine Zahl oder null sein` };
  return { value: n };
}

function validateId(v, name = "id") {
  const id = typeof v === "string" ? v.trim() : "";
  if (!id) return { error: `${name} fehlt` };
  if (id.length > MAX_ID || !ID_RE.test(id)) return { error: `${name} muss kebab-case sein (a-z, 0-9, Bindestriche, max. ${MAX_ID} Zeichen)` };
  return { value: id };
}

// Liste von ids (Studien-, Erkenntnis-, Test-ids); unbekannte ids sind erlaubt, Prüfung als Warnung
function refList(v, name) {
  v = parseJsonText(v);
  if (blank(v)) return { value: [] };
  if (!Array.isArray(v)) return { error: `${name} muss eine Liste sein` };
  if (v.length > MAX_REFS) return { error: `${name}: mehr als ${MAX_REFS} Einträge` };
  const out = [];
  for (const [i, x] of v.entries()) {
    const t = typeof x === "string" ? x.trim() : "";
    if (!t) return { error: `${name}[${i}] fehlt oder ist kein Text` };
    if (t.length > MAX_ID) return { error: `${name}[${i}] länger als ${MAX_ID} Zeichen` };
    if (!out.includes(t)) out.push(t);
  }
  return { value: out };
}

function validateMetric(m, i, seen) {
  const at = `metrics[${i}]`;
  if (!isObj(m)) return { error: `${at} muss ein Objekt sein` };
  const key = typeof m.key === "string" ? m.key.trim() : "";
  if (!key) return { error: `${at}.key fehlt` };
  if (key.length > 40) return { error: `${at}.key länger als 40 Zeichen` };
  if (seen.has(key)) return { error: `metrics: ${key} doppelt` };
  seen.add(key);
  const label = reqText(m.label, `${at}.label`, 60);
  if (label.error) return label;
  const unit = optText(m.unit, `${at}.unit`, 20);
  if (unit.error) return unit;
  let better = blank(m.better) ? KNOWN_METRICS[key] : String(m.better).trim().toLowerCase();
  if (!BETTER.includes(better)) return { error: `${at}.better muss ${BETTER.join(" | ")} sein${blank(m.better) ? " (bei eigenem key Pflicht)" : ""}` };
  const out = { key, label: label.value, unit: unit.value ?? "", better };
  for (const f of ["baseline", "current"]) {
    const n = optNumber(m[f], `${at}.${f}`);
    if (n.error) return n;
    const d = optDate(m[`${f}Date`], `${at}.${f}Date`);
    if (d.error) return d;
    out[f] = n.value;
    out[`${f}Date`] = d.value;
  }
  if (m.provisional === true || m.provisional === "true") out.provisional = true;
  return { value: out };
}

function validateExpectation(e) {
  e = parseJsonText(e);
  if (e == null) return { value: null };
  if (!isObj(e)) return { error: "expectation muss ein Objekt { confirmed, unclear, refuted } sein" };
  const out = {};
  for (const k of ["confirmed", "unclear", "refuted"]) {
    const t = reqText(e[k], `expectation.${k}`, 120);
    if (t.error) return t;
    out[k] = t.value;
  }
  return { value: out };
}

function validateAdherence(a) {
  a = parseJsonText(a);
  if (a == null) return { value: null };
  if (!isObj(a)) return { error: "adherence muss ein Objekt { rule, minPct, actualPct } sein" };
  const rule = reqText(a.rule, "adherence.rule", 120);
  if (rule.error) return rule;
  const minPct = Number(a.minPct);
  if (blank(a.minPct) || !Number.isFinite(minPct) || minPct < 0 || minPct > 100) return { error: "adherence.minPct muss eine Zahl von 0 bis 100 sein" };
  const actual = optNumber(a.actualPct, "adherence.actualPct");
  if (actual.error) return actual;
  if (actual.value != null && (actual.value < 0 || actual.value > 100)) return { error: "adherence.actualPct muss 0 bis 100 oder null sein" };
  return { value: { rule: rule.value, minPct, actualPct: actual.value } };
}

export function validateTest(body) {
  const fetchedAt = Date.parse(body?.fetchedAt);
  if (!Number.isFinite(fetchedAt)) return { error: "fetchedAt (ISO-Zeitstempel) fehlt" };
  const checks = {
    id: validateId(body?.id),
    title: reqText(body?.title, "title", 80),
    kind: oneOf(body?.kind, "kind", TEST_KIND),
    discipline: oneOf(body?.discipline, "discipline", TEST_DISCIPLINE),
    block: reqText(body?.block, "block", 20),
    blockGoal: reqText(body?.blockGoal, "blockGoal", 140),
    question: reqText(body?.question, "question", 160),
    change: reqText(body?.change, "change", 160),
    load: oneOf(body?.load, "load", TEST_LOAD),
    status: oneOf(body?.status, "status", TEST_STATUS),
    proposedBy: oneOf(body?.proposedBy, "proposedBy", TEST_PROPOSED_BY),
    studyRefs: refList(body?.studyRefs, "studyRefs"),
    insightRefs: refList(body?.insightRefs, "insightRefs"),
    confoundedWith: refList(body?.confoundedWith, "confoundedWith"),
    startDate: optDate(body?.startDate, "startDate"),
    checkDate: optDate(body?.checkDate, "checkDate"),
    decidedAt: optDate(body?.decidedAt, "decidedAt"),
    abort: optText(body?.abort, "abort", 140),
    learned: optText(body?.learned, "learned", 200),
    expectation: validateExpectation(body?.expectation),
    adherence: validateAdherence(body?.adherence),
  };
  for (const r of Object.values(checks)) if (r.error) return { error: r.error };
  const v = Object.fromEntries(Object.entries(checks).map(([k, r]) => [k, r.value]));

  const rawMetrics = parseJsonText(body?.metrics);
  if (!Array.isArray(rawMetrics) || rawMetrics.length < 1) return { error: "metrics fehlt oder ist leer (1–4 Messgrößen)" };
  if (rawMetrics.length > MAX_METRICS) return { error: `metrics: mehr als ${MAX_METRICS} Messgrößen` };
  const seen = new Set(), metrics = [];
  for (const [i, m] of rawMetrics.entries()) {
    const r = validateMetric(m, i, seen);
    if (r.error) return { error: r.error };
    metrics.push(r.value);
  }

  const resultGiven = !blank(body?.result);
  let result = null;
  if (resultGiven) {
    const r = oneOf(body.result, "result", TEST_RESULT);
    if (r.error) return r;
    result = r.value;
  }
  const { status } = v;
  if (status === "laeuft") {
    for (const f of ["startDate", "checkDate", "decidedAt"]) if (!v[f]) return { error: `${f} fehlt (Pflicht bei status laeuft; ohne Gerberts Ja kein laeuft)` };
    if (!v.expectation) return { error: "expectation fehlt (Pflicht bei status laeuft: Grenzen vorher festlegen)" };
  }
  if (status === "urteil" || status === "gelernt") {
    if (!result) return { error: `result fehlt (Pflicht bei status ${status})` };
  } else if (result) {
    return { error: `result nur bei status urteil oder gelernt (nicht bei ${status})` };
  }
  if (status === "gelernt") {
    if (!v.learned) return { error: "learned fehlt (Pflicht bei status gelernt)" };
  } else if (v.learned) {
    return { error: `learned nur bei status gelernt (nicht bei ${status})` };
  }
  // Einhaltung unter der Mindestquote: höchstens "unklar"
  const ad = v.adherence;
  if (ad && ad.actualPct != null && ad.actualPct < ad.minPct && (result === "bestaetigt" || result === "widerlegt")) {
    return { error: `result ${result} nicht möglich: Einhaltung ${ad.actualPct} % liegt unter der Mindestquote ${ad.minPct} % (höchstens unklar)` };
  }
  if (v.confoundedWith.includes(v.id)) return { error: "confoundedWith darf den Test selbst nicht enthalten" };

  const iso = new Date(fetchedAt).toISOString();
  const value = {
    id: v.id, title: v.title, kind: v.kind, discipline: v.discipline, block: v.block, blockGoal: v.blockGoal, question: v.question,
    studyRefs: v.studyRefs, insightRefs: v.insightRefs, change: v.change, startDate: v.startDate, checkDate: v.checkDate,
    metrics, expectation: v.expectation, load: v.load, adherence: ad, abort: v.abort, status, result,
    confoundedWith: v.confoundedWith, learned: v.learned, proposedBy: v.proposedBy, decidedAt: v.decidedAt, updatedAt: iso,
  };
  return { value, fetchedAt: iso };
}

function listOf(stored) {
  return Array.isArray(stored?.tests) ? stored.tests : [];
}

// Upsert über id; ein älterer fetchedAt überschreibt nie einen neueren (gleicher Test). Platz schaffen: zuerst die ältesten
// verworfenen; ist nichts Verworfenes da, Fehler.
export function upsertTest(stored, test, fetchedAt) {
  const tests = listOf(stored);
  const prev = tests.find((t) => t?.id === test.id);
  if (prev?.updatedAt && Date.parse(prev.updatedAt) > Date.parse(fetchedAt)) {
    return { error: `fetchedAt ${fetchedAt} ist älter als der gespeicherte Stand dieses Tests (${prev.updatedAt}); nicht überschrieben` };
  }
  const rest = tests.filter((t) => t?.id !== test.id);
  while (rest.length >= MAX_TESTS) {
    const dropped = rest.filter((t) => t.status === "verworfen").sort((a, b) => String(a.updatedAt).localeCompare(String(b.updatedAt)))[0];
    if (!dropped) return { error: `Höchstens ${MAX_TESTS} Tests und keiner ist verworfen; erst einen Test mit status verworfen markieren` };
    rest.splice(rest.indexOf(dropped), 1);
  }
  const next = [test, ...rest];
  const latest = next.reduce((m, t) => (String(t.updatedAt) > m ? String(t.updatedAt) : m), fetchedAt);
  return { value: { fetchedAt: latest, tests: next } };
}

// Warnungen über den ganzen Bestand (kein Fehler): mehr als zwei laufende Mehrbelastungs-Tests; zwei laufende test/paket mit
// gleichem metrics.key ohne gegenseitiges confoundedWith
export function testWarnings(tests) {
  const running = listOf({ tests }).filter((t) => t.status === "laeuft");
  const out = [];
  const heavy = running.filter((t) => t.load === "mehr");
  if (heavy.length > MAX_HEAVY_RUNNING) {
    out.push(`${heavy.length} laufende Tests mit Mehrbelastung (${heavy.map((t) => t.id).join(", ")}); mehr als ${MAX_HEAVY_RUNNING} gleichzeitig`);
  }
  const judged = running.filter((t) => t.kind === "test" || t.kind === "paket");
  for (let i = 0; i < judged.length; i++) {
    for (let j = i + 1; j < judged.length; j++) {
      const a = judged[i], b = judged[j];
      const shared = a.metrics.map((m) => m.key).filter((k) => b.metrics.some((m) => m.key === k));
      if (!shared.length) continue;
      const mutual = (a.confoundedWith ?? []).includes(b.id) && (b.confoundedWith ?? []).includes(a.id);
      if (!mutual) out.push(`${a.id} und ${b.id} teilen die Messgröße ${shared.join(", ")} ohne gegenseitiges confoundedWith`);
    }
  }
  return out;
}

// Unbekannte Verweise (Warnung, kein Fehler). knownStudyIds / knownInsightIds: Mengen der vorhandenen ids
export function refWarnings(test, knownStudyIds, knownInsightIds) {
  const out = [];
  for (const id of test.studyRefs ?? []) if (!knownStudyIds.has(id)) out.push(`studyRefs: Wissensbasis-Punkt ${id} nicht gefunden`);
  for (const id of test.insightRefs ?? []) if (!knownInsightIds.has(id)) out.push(`insightRefs: Erkenntnis ${id} nicht gefunden`);
  return out;
}

export async function readTests(env) {
  return readKvJson(env, TESTS_KV_KEY).catch(() => null);
}

export async function writeTests(env, value) {
  await writeKvJson(env, TESTS_KV_KEY, value);
}

const byCheckDate = (a, b) => String(a.checkDate ?? "9999").localeCompare(String(b.checkDate ?? "9999"));

// Für get_coach und Dashboard: alle außer verworfen, sortiert laeuft, kandidat, urteil, gelernt; innerhalb nach checkDate
export function testsForCoach(stored) {
  return listOf(stored)
    .filter((t) => t.status !== "verworfen")
    .sort((a, b) => STATUS_ORDER.indexOf(a.status) - STATUS_ORDER.indexOf(b.status) || byCheckDate(a, b));
}

// Zuordnung Test -> "Bei dir" der Wissensbasis (Abschnitt 4 des Auftrags)
export function atYouFromTest(test) {
  if (test.status === "kandidat") return { atYou: "nicht_getestet", candidate: true };
  if (test.status === "laeuft") return { atYou: "laeuft" };
  const begleiter = test.kind === "begleiter";
  if (test.result === "bestaetigt") return { atYou: begleiter ? "stoert_nicht" : "gewirkt" };
  if (test.result === "widerlegt") return { atYou: begleiter ? "stoert" : "nicht_gewirkt" };
  return { atYou: "offen" };
}

const LINK_PRIORITY = { laeuft: 0, urteil: 1, gelernt: 1, kandidat: 2 };

// Wissensbasis fürs Dashboard: Punkte mit verknüpftem Test (link {type:test} oder studyRefs eines Tests) bekommen
// doing = change des Tests, atYou aus dem Test und tests = [{ id, title, status }]; gespeicherte practice/atYou werden dann
// ignoriert. Ohne Test: gespeichertes atYou, aber "gewirkt" gibt es nie ohne Test-Ergebnis (wird zu "offen").
// Studienlage "einig" gilt nur mit counterCheckedAt (sonst "eine_seite"). Gespeichert wird nichts davon.
export function applyTestsToWissensbasis(w, tests) {
  if (!w || !Array.isArray(w.sections)) return w;
  const live = (tests ?? []).filter((t) => t && t.status !== "verworfen");
  return {
    ...w,
    sections: w.sections.map((s) => ({
      ...s,
      items: s.items.map((it) => {
        const linked = live.filter((t) => (it.id && (t.studyRefs ?? []).includes(it.id)) || (it.link?.type === "test" && it.link.id === t.id));
        const out = { ...it };
        if (out.consensus === "einig" && !out.counterCheckedAt) out.consensus = "eine_seite";
        if (linked.length) {
          const main = [...linked].sort((a, b) => (LINK_PRIORITY[a.status] ?? 9) - (LINK_PRIORITY[b.status] ?? 9) || String(b.updatedAt).localeCompare(String(a.updatedAt)))[0];
          const d = atYouFromTest(main);
          out.practice = main.change;
          out.atYou = d.atYou;
          if (d.candidate) out.candidate = true;
          out.tests = linked.map((t) => ({ id: t.id, title: t.title, status: t.status }));
        } else if (out.atYou === "gewirkt") {
          out.atYou = "offen";
        }
        return out;
      }),
    })),
  };
}

// Ein freigegebener oder abgelehnter Test (decidedAt) bleibt, wie Gerbert entschieden hat: save_test mit status kandidat
// darf ihn nicht zurücksetzen. Rückgabe: Warntext oder null.
export function resetGuard(stored, test) {
  if (test.status !== "kandidat") return null;
  const prev = listOf(stored).find((t) => t?.id === test.id);
  if (!prev?.decidedAt) return null;
  return `${test.id}: von Gerbert am ${prev.decidedAt} entschieden (status ${prev.status}); status kandidat ignoriert, nichts gespeichert`;
}

const addDays = (iso, n) => new Date(Date.parse(iso + "T12:00:00Z") + n * 86400000).toISOString().slice(0, 10);
// Nächster Montag; ist heute Montag, heute
export function nextMonday(iso) {
  const day = new Date(iso + "T12:00:00Z").getUTCDay();
  return addDays(iso, (8 - day) % 7);
}

// Gerberts Entscheidung über einen Kandidaten (POST /api/tests/:id/decision). Schreibt denselben Datensatz wie save_test
// (gleiche Validierung über validateTest); decidedAt setzt der Worker. Rückgabe { value (neuer Bestand), test, warnings }
// oder { error, status }.
export function applyTestDecision(stored, id, body, todayIso = isoDateBerlin(), nowIso = new Date().toISOString()) {
  const prev = listOf(stored).find((t) => t?.id === id);
  if (!prev) return { error: `Test ${id} nicht gefunden`, status: 404 };
  if (!isObj(body)) return { error: "Body muss ein Objekt sein", status: 400 };
  const action = body.action;
  if (!["start", "reject", "later"].includes(action)) return { error: "action muss start, reject oder later sein", status: 400 };
  if (prev.status !== "kandidat") return { error: `Test ${id} ist kein Kandidat (status ${prev.status})`, status: 409 };
  const base = { ...prev, fetchedAt: nowIso };
  let next;
  if (action === "reject") next = { ...base, status: "verworfen", decidedAt: todayIso };
  else if (action === "later") {
    if (blank(body.block)) return { error: "block fehlt (Pflicht bei later)", status: 400 };
    next = { ...base, block: body.block };
  } else {
    const startDate = blank(body.startDate) ? nextMonday(todayIso) : body.startDate;
    const checkDate = blank(body.checkDate) ? (prev.checkDate ?? (isIsoDate(startDate) ? addDays(startDate, 56) : null)) : body.checkDate;
    if (isIsoDate(startDate) && isIsoDate(checkDate) && checkDate < startDate) return { error: "checkDate liegt vor startDate", status: 400 };
    next = {
      ...base, status: "laeuft", startDate, checkDate, decidedAt: todayIso,
      expectation: body.expectation !== undefined ? body.expectation : prev.expectation,
      adherence: body.adherence !== undefined ? body.adherence : prev.adherence,
      ...(blank(body.block) ? {} : { block: body.block }),
    };
  }
  const { value, fetchedAt, error } = validateTest(next);
  if (error) return { error, status: 400 };
  const up = upsertTest(stored, value, fetchedAt);
  if (up.error) return { error: up.error, status: 409 };
  return { value: up.value, test: value, warnings: testWarnings(up.value.tests) };
}

export async function handleTestDecisionRequest(req, env, isAuthorized, id) {
  const headers = { "cache-control": "no-store" };
  const denied = gate(req, env, isAuthorized, headers);
  if (denied) return denied;
  const { body, res } = await readBody(req, env, "POST", headers);
  if (res) return res;
  const r = applyTestDecision(await readTests(env), id, body);
  if (r.error) return json({ ok: false, error: r.error }, r.status, headers);
  await writeTests(env, r.value);
  return json({ ok: true, test: r.test, warnings: r.warnings }, 200, headers);
}
