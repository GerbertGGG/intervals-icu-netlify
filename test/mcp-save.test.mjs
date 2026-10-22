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
// Sonntag: Frage nach der Zeit, Antwort im Dashboard, zweites save_coach mit derselben Frage; Montag: Wochenvorschlag
const q = "Wie viel Zeit hast du nächste Woche (gern mit Tagen)?";
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-25T06:30:00Z", note: "Sonntag.", question: q } });
assert.ok(!r.body.result.isError);
kv.set("dashboard:coach", JSON.stringify({ ...JSON.parse(kv.get("dashboard:coach")), answer: "Mo 1 h, Mi 1,5 h abends, Do weg, Sa 3 h", answeredAt: "2026-10-25T18:00:00Z" }));
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-25T19:00:00Z", note: "Sonntag Abend.", question: q } });
assert.ok(!r.body.result.isError);
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
let gc = JSON.parse(r.body.result.content[0].text);
assert.equal(gc.answer, "Mo 1 h, Mi 1,5 h abends, Do weg, Sa 3 h"); assert.equal(gc.week_plan, null); assert.deepEqual(gc.deloadWeeks, []);
const weekPlan = { week: "2026-10-26", basis: "ca. 5 h verfügbar, Erholung grün → +8 %", deload: false,
  targets: [{ sport: "lauf", time_min: 150, tss_max: 160 }, { sport: "rad", time_min: 90, tss_max: 80 }, { sport: "schwimmen", time_min: 45, tss_max: null }],
  days: [{ day: "Mo", text: "Lauf locker 50 min" }, { day: "Sa", text: "Langer Lauf 100 min" }] };
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-26T06:30:00Z", note: "Montag.", question: "Wie lief der lange Lauf?", week_plan: weekPlan } });
assert.ok(!r.body.result.isError);
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
gc = JSON.parse(r.body.result.content[0].text);
assert.equal(gc.answer, null); // neue Frage
assert.deepEqual(gc.week_plan, { ...weekPlan, createdAt: "2026-10-26T06:30:00.000Z" });
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-27T06:30:00Z", note: "Dienstag.", week_plan: weekPlan } });
assert.equal(r.body.result.isError, true); assert.match(r.body.result.content[0].text, /nur montags/);
r = await rpc("tools/call", { name: "save_coach", arguments: { fetchedAt: "2026-10-27T06:31:00Z", note: "Dienstag.", week_plan: {} } });
assert.ok(!r.body.result.isError);
assert.equal(JSON.parse((await rpc("tools/call", { name: "get_coach", arguments: {} })).body.result.content[0].text).week_plan, null);
console.log = log;
console.log("mcp-save tests ok");
