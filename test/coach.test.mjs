// Testet src/coach-snapshot.js. Ausführen: node test/coach.test.mjs
import assert from "node:assert/strict";
import { validateCoach, isFreshCoach, handleCoachRequest, handleCoachAnswerRequest, applyAnswer } from "../src/coach-snapshot.js";

assert.ok(validateCoach({ fetchedAt: "kaputt", note: "x" }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "  " }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", headline: "y".repeat(81) }).error);
const ok = validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: " Locker bleiben. " });
assert.deepEqual(ok.value, { fetchedAt: "2026-10-11T05:00:00.000Z", note: "Locker bleiben.", headline: null, text: null, question: null, answer: null, answeredAt: null });
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", question: "q".repeat(301) }).error);
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
console.log("coach tests ok");
