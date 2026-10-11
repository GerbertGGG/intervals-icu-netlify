// Testet src/coach-snapshot.js. Ausführen: node test/coach.test.mjs
import assert from "node:assert/strict";
import { validateCoach, isFreshCoach, handleCoachRequest } from "../src/coach-snapshot.js";

assert.ok(validateCoach({ fetchedAt: "kaputt", note: "x" }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "  " }).error);
assert.ok(validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: "x", headline: "y".repeat(81) }).error);
const ok = validateCoach({ fetchedAt: "2026-10-11T05:00:00Z", note: " Locker bleiben. " });
assert.deepEqual(ok.value, { fetchedAt: "2026-10-11T05:00:00.000Z", note: "Locker bleiben.", headline: null, text: null });

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
console.log("coach tests ok");
