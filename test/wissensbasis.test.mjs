// Testet die Wissensbasis (Validierung, Speichern, get_coach). Ausführen: node test/wissensbasis.test.mjs
import assert from "node:assert/strict";
import { validateWissensbasis, saveWissensbasis, WISSENSBASIS_KV_KEY } from "../src/wissensbasis-snapshot.js";
import { handleMcpRequest } from "../src/mcp-server.js";

const item = { text: "Punkt", evidence: "mittel", fit: "hoch", source: "Quelle 2025", isNew: true };
const good = { fetchedAt: "2026-11-05T08:00:00+01:00", updatedAt: "2026-11-05", folderCheckedAt: "2026-11-05", sections: [{ title: "HIIT", items: [item] }], open: ["offen"] };

// gültiger Stand
const ok = validateWissensbasis(good);
assert.ok(!ok.error, ok.error);
assert.equal(ok.value.fetchedAt, "2026-11-05T07:00:00.000Z");
assert.equal(ok.value.sections[0].items[0].isNew, true);

// Abgleich-Felder: alle gesetzt gültig, alter Stand gültig, ungültige Werte abgelehnt
const full = { ...item, consensus: "umstritten", counterCheckedAt: "2026-11-05", practice: "2x/Woche", match: "luecke", atYou: "laeuft", link: { type: "insight", id: "x-1" }, next: "Test starten" };
const fv = validateWissensbasis({ ...good, sections: [{ title: "A", items: [full] }] });
assert.ok(!fv.error, fv.error);
assert.deepEqual(fv.value.sections[0].items[0].link, { type: "insight", id: "x-1" });
assert.equal(fv.value.sections[0].items[0].atYou, "laeuft");
assert.ok(!validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ text: "t", evidence: "stark", fit: "hoch" }] }] }).error);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...full, match: "egal" }] }] }).error, /match/);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...full, link: { type: "foo", id: "a" } }] }] }).error, /link\.type/);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...full, practice: "x".repeat(161) }] }] }).error, /practice/);

// zu viele Abschnitte / Punkte
const sec = { title: "A", items: [item] };
assert.match(validateWissensbasis({ ...good, sections: Array(13).fill(sec) }).error, /13|mehr als 12/);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: Array(9).fill(item) }] }).error, /mehr als 8/);
assert.ok(validateWissensbasis({ ...good, sections: Array(12).fill(sec) }).value);

// ungültige Evidenz / Übertragbarkeit / Längen / Pflichtfelder
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, evidence: "super" }] }] }).error, /evidence/);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, fit: "?" }] }] }).error, /fit/);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, text: "x".repeat(221) }] }] }).error, /220/);
assert.match(validateWissensbasis({ ...good, updatedAt: "gestern" }).error, /updatedAt/);
assert.match(validateWissensbasis({ ...good, open: Array(7).fill("x") }).error, /open/);
assert.match(validateWissensbasis({ ...good, fetchedAt: undefined }).error, /fetchedAt/);

// älterer fetchedAt überschreibt nie einen neueren
const kv = new Map([["mcp:token:gut", "1"]]);
const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
assert.ok(!(await saveWissensbasis(env, good)).error);
const older = await saveWissensbasis(env, { ...good, fetchedAt: "2026-11-04T08:00:00Z", updatedAt: "2026-11-04" });
assert.match(older.error, /älter/);
assert.equal(JSON.parse(kv.get(WISSENSBASIS_KV_KEY)).updatedAt, "2026-11-05");

// MCP: save_wissensbasis + get_coach
const log = console.log; console.log = () => {};
const rpc = async (method, params) => {
  const res = await handleMcpRequest(new Request("https://x/mcp", { method: "POST", headers: { authorization: "Bearer gut" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }), new URL("https://x/mcp"), env);
  return res.json();
};
const list = await rpc("tools/list");
assert.ok(list.result.tools.some((t) => t.name === "save_wissensbasis"));
let r = await rpc("tools/call", { name: "save_wissensbasis", arguments: { ...good, fetchedAt: "2026-11-06T08:00:00Z", updatedAt: "2026-11-06" } });
assert.ok(!r.result.isError, JSON.stringify(r));
r = await rpc("tools/call", { name: "save_wissensbasis", arguments: { ...good, sections: [{ title: "A", items: [{ ...item, evidence: "x" }] }] } });
assert.equal(r.result.isError, true);
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
console.log = log;
const coach = JSON.parse(r.result.content[0].text);
assert.equal(coach.wissensbasis.updatedAt, "2026-11-06");
console.log("wissensbasis ok");
