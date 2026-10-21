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
console.log("coach tests ok");
