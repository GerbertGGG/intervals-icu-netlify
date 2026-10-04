import test from "node:test";
import assert from "node:assert/strict";
import { handleEgymDebugRequest } from "../src/egym-debug.js";
import { hasEgymCredentials } from "../src/egym-client.js";

const req = (token) => new Request("https://x/api/egym-debug", { headers: token ? { authorization: `Bearer ${token}` } : {} });

test("egym-debug verlangt Token und Zugangsdaten", async () => {
  assert.equal((await handleEgymDebugRequest(req(), {})).status, 503);
  assert.equal((await handleEgymDebugRequest(req(), { DASHBOARD_TOKEN: "t" })).status, 401);
  assert.equal((await handleEgymDebugRequest(req("t"), { DASHBOARD_TOKEN: "t" })).status, 503);
});

test("hasEgymCredentials braucht alle drei Variablen", () => {
  assert.equal(hasEgymCredentials({ EGYM_BRAND: "a", EGYM_USERNAME: "b" }), false);
  assert.equal(hasEgymCredentials({ EGYM_BRAND: "a", EGYM_USERNAME: "b", EGYM_PASSWORD: "c" }), true);
});
