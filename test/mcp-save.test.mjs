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
assert.ok(["save_runalyze_snapshot", "save_studie"].every((n) => list.body.result.tools.some((t) => t.name === n)));

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
console.log = log;
console.log("mcp-save tests ok");
