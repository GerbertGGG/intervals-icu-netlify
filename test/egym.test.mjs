import test from "node:test";
import assert from "node:assert/strict";
import { handleEgymDebugRequest } from "../src/egym-debug.js";
import { hasEgymCredentials, backendFromPlist } from "../src/egym-client.js";

const req = (token) => new Request("https://x/api/egym-debug", { headers: token ? { authorization: `Bearer ${token}` } : {} });

test("egym-debug verlangt Token und Zugangsdaten", async () => {
  assert.equal((await handleEgymDebugRequest(req(), {})).status, 503);
  assert.equal((await handleEgymDebugRequest(req(), { DASHBOARD_TOKEN: "t" })).status, 401);
  assert.equal((await handleEgymDebugRequest(req("t"), { DASHBOARD_TOKEN: "t" })).status, 503);
});

test("hasEgymCredentials braucht E-Mail und Passwort", () => {
  assert.equal(hasEgymCredentials({ EGYM_USERNAME: "b" }), false);
  assert.equal(hasEgymCredentials({ EGYM_USERNAME: "b", EGYM_PASSWORD: "c" }), true);
});

test("Backend-Adresse aus XML- und Binaer-Plist", () => {
  assert.equal(backendFromPlist("<dict><key>NGBackendAddressKey</key>\n<string>https://x.netpulse.com/</string></dict>"), "https://x.netpulse.com/");
  assert.equal(backendFromPlist("bplist00\u0001https://one.netpulse.com\u0002https://studio.netpulse.com\u0003"), "https://studio.netpulse.com");
  assert.equal(backendFromPlist("nichts"), null);
});

test("egym-debug akzeptiert das Token auch als ?token=", async () => {
  const q = (t) => new Request(`https://x/api/egym-debug?token=${t}`);
  assert.equal((await handleEgymDebugRequest(q("falsch"), { DASHBOARD_TOKEN: "t" })).status, 401);
  assert.equal((await handleEgymDebugRequest(q("t"), { DASHBOARD_TOKEN: "t" })).status, 503);
});
