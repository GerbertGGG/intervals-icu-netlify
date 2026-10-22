// Testet src/coach-snapshot.js. Ausführen: node test/coach.test.mjs
import assert from "node:assert/strict";
import { validateCoach, mergeCoach, isFreshCoach, handleCoachRequest, handleCoachAnswerRequest, applyAnswer } from "../src/coach-snapshot.js";

assert.ok(validateCoach({ fetchedAt: "kaputt", note: "x" }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "  " }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", headline: "y".repeat(81) }).error);
const ok = validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: " Locker bleiben. " });
assert.deepEqual(ok.value, { fetchedAt: "2026-10-11T05:00:00.000Z", note: "Locker bleiben.", headline: null, go: null, blockers: [], text: null, question: null, answer: null, answeredAt: null });
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", question: "q".repeat(301) }).error);
// Urteil des Coaches: go (hart/locker/frei) und bis zu 3 kurze Blocker
const g = validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", go: " Locker ", blockers: [" Hüfte zwickt ", "", "Schlaf 5 h"] }).value;
assert.equal(g.go, "locker"); assert.deepEqual(g.blockers, ["Hüfte zwickt", "Schlaf 5 h"]);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", go: "vollgas" }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", blockers: "Hüfte" }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", blockers: ["a", "b", "c", "d"] }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", blockers: ["b".repeat(61)] }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", answer: "ja" }).error);
const qa = validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", question: " Knie? ", answer: " gut ", answeredAt: "2026-10-11T07:00:00Z" }).value;
assert.equal(qa.question, "Knie?");
assert.equal(qa.answer, "gut");
assert.equal(qa.answeredAt, "2026-10-11T07:00:00.000Z");
assert.ok(applyAnswer({ note: "x", question: null }, { answer: "a" }).error);
assert.ok(applyAnswer({ note: "x", question: "q" }, { answer: "  " }).error);
assert.deepEqual(applyAnswer({ note: "x", question: "q" }, { answer: " a " }, Date.parse("2026-10-11T08:00:00Z")).value, { note: "x", question: "q", answer: "a", answeredAt: "2026-10-11T08:00:00.000Z" });

const at = "2026-10-11T05:00:00Z", t0 = Date.parse(at);
assert.equal(isFreshCoach({ fetchedAt: at }, t0 + 35 * 3600_000), true);
assert.equal(isFreshCoach({ fetchedAt: at }, t0 + 37 * 3600_000), false);
assert.equal(isFreshCoach({ fetchedAt: at }, t0 - 1000), false);
assert.equal(isFreshCoach(null, t0), false);

const kv = new Map();
const env = { DASHBOARD_TOKEN: "t", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const auth = (req) => req.headers.get("authorization") === "Bearer t";
const put = (body, token = "t") => handleCoachRequest(new Request("https://x/api/coach", { method: "PUT", headers: { authorization: "Bearer " + token }, body: JSON.stringify(body) }), env, auth);
assert.equal((await put({ fetchedAt: at, note: "x" }, "falsch")).status, 401);
assert.equal((await put({ fetchedAt: at })).status, 400);
assert.equal((await put({ fetchedAt: at, note: "Ruhe." })).status, 200);
assert.equal(JSON.parse(kv.get("dashboard:coach")).note, "Ruhe.");
const answer = (body, token = "t") => handleCoachAnswerRequest(new Request("https://x/api/coach/answer", { method: "POST", headers: { authorization: "Bearer " + token }, body: JSON.stringify(body) }), env, auth);
assert.equal((await answer({ answer: "a" })).status, 409); // keine Frage offen
assert.equal((await put({ fetchedAt: at, note: "Ruhe.", question: "Wie ist die Hüfte?" })).status, 200);
assert.equal((await answer({ answer: "a" }, "falsch")).status, 401);
assert.equal((await answer({ answer: "Hüfte ruhig, 1 von 10" })).status, 200);
const saved = JSON.parse(kv.get("dashboard:coach"));
assert.equal(saved.answer, "Hüfte ruhig, 1 von 10");
assert.equal(saved.note, "Ruhe.");

// Idee der Woche: Validierung
const base = { fetchedAt: "2026-10-12T05:00:00Z", note: "x" };
assert.equal("idea" in validateCoach(base).value, false); // Feld fehlt
assert.equal(validateCoach({ ...base, idea: { text: "  " } }).value.idea, null); // leerer Text = entfernen
assert.deepEqual(validateCoach({ ...base, idea: { text: " Barfuß 5 min auf Rasen ", angle: 3, sport: " Lauf " } }).value.idea, { text: "Barfuß 5 min auf Rasen", angle: 3, sport: "lauf" });
assert.match(validateCoach({ ...base, idea: { text: "i".repeat(241) } }).error, /240.*ohne idea/);
assert.match(validateCoach({ ...base, idea: { text: "i", angle: 9 } }).error, /angle/);
assert.match(validateCoach({ ...base, idea: { text: "i", angle: 2.5 } }).error, /angle/);
assert.match(validateCoach({ ...base, idea: { text: "i", sport: "tennis" } }).error, /sport/);
assert.ok(validateCoach({ ...base, idea: "Barfuß laufen" }).error);
assert.deepEqual(validateCoach({ ...base, idea: '{"text":"Rad ohne Uhr","sport":"rad"}' }).value.idea, { text: "Rad ohne Uhr", angle: null, sport: "rad" }); // JSON-Text
assert.ok(validateCoach({ ...base, idea: '{"text":' }).error);
// mergeCoach: Update ohne idea laesst Idee und Historie stehen
const v = (fetchedAt, idea) => validateCoach({ fetchedAt, note: "x", ...(idea !== undefined ? { idea } : {}) }).value;
let m = mergeCoach(null, v("2026-10-12T05:00:00Z", { text: "Idee A", sport: "rad" }));
assert.deepEqual(m.idea, { text: "Idee A", angle: null, sport: "rad", createdAt: "2026-10-12T05:00:00.000Z" });
assert.deepEqual(m.ideas, [{ date: "2026-10-12", text: "Idee A", angle: null, sport: "rad" }]);
m = mergeCoach(m, v("2026-10-13T05:00:00Z"));
assert.equal(m.idea.text, "Idee A"); assert.equal(m.ideas.length, 1); assert.equal(m.fetchedAt, "2026-10-13T05:00:00.000Z");
// Gleicher Tag (Berlin) ersetzt den Historieneintrag; 23:30 UTC ist in Berlin schon der naechste Tag
m = mergeCoach(m, v("2026-10-19T05:00:00Z", { text: "Idee B" }));
m = mergeCoach(m, v("2026-10-19T09:00:00Z", { text: "Idee B korrigiert", angle: 2 }));
assert.deepEqual(m.ideas.map((x) => x.text), ["Idee B korrigiert", "Idee A"]);
assert.equal(m.idea.createdAt, "2026-10-19T09:00:00.000Z");
m = mergeCoach(m, v("2026-10-19T23:30:00Z", { text: "Idee C" }));
assert.equal(m.ideas[0].date, "2026-10-20"); assert.equal(m.ideas.length, 3);
// Leerer Text entfernt die aktuelle Idee, die Historie bleibt
m = mergeCoach(m, v("2026-10-21T05:00:00Z", { text: "" }));
assert.equal(m.idea, null); assert.equal(m.ideas.length, 3);
// Historie bei 12 gekappt, neueste zuerst
for (let k = 0; k < 15; k++) m = mergeCoach(m, v(new Date(Date.parse("2026-11-02T05:00:00Z") + k * 7 * 86400_000).toISOString(), { text: `W${k}` }));
assert.equal(m.ideas.length, 12); assert.equal(m.ideas[0].text, "W14"); assert.equal(m.ideas[11].text, "W3");
// PUT /api/coach: auch hier bleibt die Idee ohne idea stehen, die Antwort aus dem Dashboard laesst sie in Ruhe
assert.equal((await put({ fetchedAt: "2026-10-12T05:00:00Z", note: "a", idea: { text: "Rad ohne Uhr", sport: "rad" } })).status, 200);
assert.equal((await put({ fetchedAt: "2026-10-13T05:00:00Z", note: "b", question: "Knie?" })).status, 200);
assert.equal((await put({ fetchedAt: "2026-10-13T06:00:00Z", note: "b", idea: { text: "i", sport: "golf" } })).status, 400);
assert.equal((await answer({ answer: "gut" })).status, 200);
const kept = JSON.parse(kv.get("dashboard:coach"));
assert.equal(kept.note, "b"); assert.equal(kept.idea.text, "Rad ohne Uhr"); assert.equal(kept.ideas.length, 1);
// Antwort bleibt bei derselben question (zweites Update am Sonntag), verschwindet erst mit einer neuen
const multi = "Mo 1 h\nMi 1,5 h abends\nDo weg\nSa 3 h";
assert.equal((await put({ fetchedAt: "2026-10-25T06:00:00Z", note: "c", question: "Wie viel Zeit hast du nächste Woche?" })).status, 200);
assert.equal((await answer({ answer: multi })).status, 200);
assert.equal((await put({ fetchedAt: "2026-10-25T17:00:00Z", note: "d", question: "Wie viel Zeit hast du nächste Woche?" })).status, 200);
const sunday = JSON.parse(kv.get("dashboard:coach"));
assert.equal(sunday.note, "d"); assert.equal(sunday.answer, multi); assert.ok(sunday.answeredAt);
assert.equal((await put({ fetchedAt: "2026-10-26T06:00:00Z", note: "e", question: "Wie lief der lange Lauf?" })).status, 200);
const monday = JSON.parse(kv.get("dashboard:coach"));
assert.equal(monday.answer, null); assert.equal(monday.answeredAt, null);
console.log("coach tests ok");

// Wochenampel: Validierung und Merge (bleibt ohne week stehen, leeres light entfernt sie)
assert.equal("week" in validateCoach(base).value, false);
assert.deepEqual(validateCoach({ ...base, week: { light: " Grün ", text: " Woche passt " } }).value.week, { light: "gruen", text: "Woche passt" });
assert.equal(validateCoach({ ...base, week: { light: "" } }).value.week, null);
assert.match(validateCoach({ ...base, week: { light: "blau" } }).error, /light/);
assert.match(validateCoach({ ...base, week: { light: "rot", text: "t".repeat(161) } }).error, /160/);
assert.deepEqual(validateCoach({ ...base, week: '{"light":"gelb"}' }).value.week, { light: "gelb", text: null });
const w = (fetchedAt, week) => validateCoach({ fetchedAt, note: "x", ...(week !== undefined ? { week } : {}) }).value;
let mw = mergeCoach(null, w("2026-10-19T05:00:00Z"));
assert.equal(mw.week, null);
mw = mergeCoach(mw, w("2026-10-19T06:00:00Z", { light: "gelb", text: "Laufumfang stark eingebrochen" }));
assert.deepEqual(mw.week, { light: "gelb", text: "Laufumfang stark eingebrochen", createdAt: "2026-10-19T06:00:00.000Z" });
mw = mergeCoach(mw, w("2026-10-20T05:00:00Z"));
assert.equal(mw.week.light, "gelb");
mw = mergeCoach(mw, w("2026-10-21T05:00:00Z", { light: "" }));
assert.equal(mw.week, null);

// Wochenvorschlag: nur montags, bleibt ohne week_plan stehen, {} entfernt ihn; deload füllt deloadWeeks
const mon = { fetchedAt: "2026-10-26T06:00:00Z", note: "x" }; // Montag (Berlin)
const plan = { week: "2026-10-26", basis: " ca. 5 h verfügbar ", deload: false, targets: [{ sport: "Lauf", time_min: 150, tss_max: 160 }, { sport: "schwimmen", time_min: 45 }], days: [{ day: "Mo", text: "Lauf locker 50 min" }] };
assert.equal("week_plan" in validateCoach(mon).value, false);
assert.deepEqual(validateCoach({ ...mon, week_plan: plan }).value.week_plan, { week: "2026-10-26", basis: "ca. 5 h verfügbar", deload: false, targets: [{ sport: "lauf", time_min: 150, tss_max: 160 }, { sport: "schwimmen", time_min: 45, tss_max: null }], days: [{ day: "Mo", text: "Lauf locker 50 min" }] });
assert.equal(validateCoach({ ...mon, week_plan: {} }).value.week_plan, null);
assert.equal(validateCoach({ ...mon, week_plan: { targets: [{ sport: "rad", time_min: 60 }] } }).value.week_plan.week, "2026-10-26"); // ohne week: heutiger Montag
assert.match(validateCoach({ ...mon, fetchedAt: "2026-10-27T06:00:00Z", week_plan: plan }).error, /nur montags.*ohne week_plan/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, week: "2026-10-27" } }).error, /Montag/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, targets: [] } }).error, /targets/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, targets: [{ sport: "golf", time_min: 1 }] } }).error, /sport/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, targets: [{ sport: "rad", time_min: 1 }, { sport: "rad", time_min: 2 }] } }).error, /doppelt/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, targets: [{ sport: "rad" }] } }).error, /time_min/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, deload: "ja" } }).error, /deload/);
assert.match(validateCoach({ ...mon, week_plan: { ...plan, days: [{ day: "Mo" }] } }).error, /days/);
let mp = mergeCoach(null, validateCoach({ ...mon, week_plan: { ...plan, deload: true } }).value);
assert.equal(mp.week_plan.createdAt, "2026-10-26T06:00:00.000Z"); assert.deepEqual(mp.deloadWeeks, ["2026-10-26"]);
mp = mergeCoach(mp, validateCoach({ ...mon, fetchedAt: "2026-10-27T06:00:00Z" }).value);
assert.equal(mp.week_plan.week, "2026-10-26"); assert.deepEqual(mp.deloadWeeks, ["2026-10-26"]); // Dienstag: bleibt stehen
mp = mergeCoach(mp, validateCoach({ ...mon, fetchedAt: "2026-10-26T09:00:00Z", week_plan: plan }).value);
assert.deepEqual(mp.deloadWeeks, []); // Korrektur am Montag: doch keine Entlastung
mp = mergeCoach(mp, validateCoach({ ...mon, fetchedAt: "2026-10-28T06:00:00Z", week_plan: {} }).value);
assert.equal(mp.week_plan, null);
console.log("coach week_plan ok");
