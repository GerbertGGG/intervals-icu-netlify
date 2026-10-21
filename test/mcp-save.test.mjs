// Testet die Schreib-Tools des MCP-Servers. Ausführen: node test/mcp-save.test.mjs
import assert from "node:assert/strict";
import { handleMcpRequest } from "../src/mcp-server.js";

const kv = new Map([["mcp:token:gut", "1"]]);
const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const log = console.log; console.log = () => {};
const rpc = async (method, params, token = "gut") => {
  const res = await handleMcpRequest(new Request("https://x/mcp", { method: "POST", headers: { authorization: "Bearer " + token }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }), new URL("https://x/mcp"), env);
  return { status: res.status, body: await res.json() };
};

const list = await rpc("tools/list");
assert.ok(["save_runalyze_snapshot", "save_studie", "save_coach"].every((n) => list.body.result.tools.some((t) => t.name === n)));

const snap = { fetchedAt: "2026-10-02T05:00:00Z", vdot: 40, prognosis: [{ distanceKm: 5, seconds: 1500 }], races: [] };
assert.equal((await rpc("tools/call", { name: "save_runalyze_snapshot", arguments: snap }, "schlecht")).status, 401);
let r = await rpc("tools/call", { name: "save_runalyze_snapshot", arguments: snap });
assert.ok(!r.body.result.isError);
assert.equal(JSON.parse(kv.get("dashboard:runalyze")).vdot, 40);

r = await rpc("tools/call", { name: "save_runalyze_snapshot", arguments: { fetchedAt: "kaputt" } });
assert.equal(r.body.result.isError, true);
assert.equal(JSON.parse(kv.get("dashboard:runalyze")).vdot, 40);

r = await rpc("tools/call", { name: "save_studie", arguments: { fetchedAt: snap.fetchedAt, text: "t" } });
assert.equal(r.body.result.isError, true); // source fehlt
r = await rpc("tools/call", { name: "save_studie", arguments: { fetchedAt: snap.fetchedAt, text: "t", source: "Autor 2026" } });
assert.ok(!r.body.result.isError);
assert.equal(JSON.parse(kv.get("dashboard:studie")).source, "Autor 2026");
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: snap.fetchedAt } });
assert.equal(r.body.result.isError, true); // note fehlt
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: snap.fetchedAt, note: "x".repeat(161) } });
assert.equal(r.body.result.isError, true); // note zu lang
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: snap.fetchedAt, note: "Heute locker bleiben.", headline: "Ruhe" } });
assert.ok(!r.body.result.isError);
assert.equal(JSON.parse(kv.get("dashboard:coach")).note, "Heute locker bleiben.");
assert.ok(list.body.result.tools.some((t) => t.name === "get_coach"));
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: snap.fetchedAt, note: "Locker.", question: "Hüfte nach den Sprints, 0–10?" } });
assert.ok(!r.body.result.isError);
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
assert.ok(!r.body.result.isError);
const got = JSON.parse(r.body.result.content[0].text);
assert.equal(got.question, "Hüfte nach den Sprints, 0–10?");
assert.equal(got.answer, null);
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: snap.fetchedAt, note: "Locker.", answer: "2" } });
assert.equal(r.body.result.isError, true); // Antwort ohne Frage
// Idee der Woche ueber save_coach / get_coach
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
assert.equal(JSON.parse(r.body.result.content[0].text).idea, null); // aeltere Aussage ohne idea
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-19T05:00:00Z", note: "Montag.", idea: { text: "Schwimm 4 Bahnen nur mit Beinen", angle: 4, sport: "schwimmen" } } });
assert.ok(!r.body.result.isError);
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-20T05:00:00Z", note: "Dienstag." } });
assert.ok(!r.body.result.isError);
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
const gi = JSON.parse(r.body.result.content[0].text);
assert.equal(gi.note, "Dienstag.");
assert.deepEqual(gi.idea, { text: "Schwimm 4 Bahnen nur mit Beinen", angle: 4, sport: "schwimmen", createdAt: "2026-10-19T05:00:00.000Z" });
assert.deepEqual(gi.ideas, [{ date: "2026-10-19", text: "Schwimm 4 Bahnen nur mit Beinen", angle: 4, sport: "schwimmen" }]);
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-21T05:00:00Z", note: "Mittwoch.", idea: { text: "x".repeat(241) } } });
assert.equal(r.body.result.isError, true);
assert.match(r.body.result.content[0].text, /idea\.text länger als 240/);
assert.equal(JSON.parse(kv.get("dashboard:coach")).note, "Dienstag."); // nichts geschrieben
const schema = list.body.result.tools.find((t) => t.name === "save_coach").inputSchema.properties.idea;
assert.deepEqual(schema.properties.sport.enum, ["lauf", "rad", "schwimmen", "kraft", "alle"]);
console.log = log;
console.log("mcp-save tests ok");
