// Testet das Archiv der Wissensbasis-Punkte. Ausführen: node test/wissensbasis-archiv.test.mjs
import assert from "node:assert/strict";
import { saveWissensbasis, WISSENSBASIS_KV_KEY } from "../src/wissensbasis-snapshot.js";
import { WB_ARCHIV_KV_KEY, itemId, visibleWissensbasis, archivForCoach, MAX_ARCHIV, applyArchive } from "../src/wissensbasis-archiv.js";
import { applyTestsToWissensbasis } from "../src/tests-snapshot.js";
import { handleMcpRequest } from "../src/mcp-server.js";
import worker from "../src/index.js";

const kv = new Map([["mcp:token:gut", "1"]]);
let puts = 0;
const env = { DASHBOARD_TOKEN: "tok", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => { puts++; kv.set(k, v); } } };
const omega = { text: "Omega-3 bringt Ausdauersportlern keinen messbaren Leistungsvorteil.", evidence: "schwach", fit: "gering", source: "Thielecke & Blannin 2020", forYou: "keine Empfehlung" };
const hiit = { text: "HIIT zweimal pro Woche reicht.", evidence: "stark", fit: "hoch", source: "Quelle 2025", id: "hiit-2x", forYou: "ok", atYou: "laeuft" };
const body = { fetchedAt: "2026-11-05T08:00:00Z", updatedAt: "2026-11-05", sections: [{ title: "Supplemente", items: [omega] }, { title: "HIIT", items: [hiit] }] };
assert.ok(!(await saveWissensbasis(env, body)).error);
const stored = kv.get(WISSENSBASIS_KV_KEY);

const call = (path, method = "POST", payload, auth = "Bearer tok") => worker.fetch(new Request("https://x" + path, { method, headers: { authorization: auth }, ...(payload !== undefined ? { body: JSON.stringify(payload) } : {}) }), env, { waitUntil() {} });

// Punkt ohne id: abgeleitete id bleibt über zwei Lesevorgänge gleich, gespeichert wird sie nicht
const aid = itemId(omega);
assert.match(aid, /^wb-[0-9a-f]{8}$/);
assert.equal(itemId(JSON.parse(stored).sections[0].items[0]), aid);
assert.equal(itemId({ ...omega, text: omega.text.toUpperCase() + "  " }), aid);
assert.equal(itemId(hiit), "hiit-2x");
assert.equal(JSON.parse(stored).sections[0].items[0].id, undefined);

// Auth, Methode, unbekannte id
assert.equal((await call(`/api/wissensbasis/${aid}/archive`, "POST", {}, "Bearer falsch")).status, 401);
assert.equal((await call(`/api/wissensbasis/${aid}/archive`, "GET")).status, 405);
assert.equal((await call(`/api/wissensbasis/gibt-es-nicht/archive`, "POST", {})).status, 404);
assert.equal((await call(`/api/wissensbasis/gibt-es-nicht/restore`, "POST", {})).status, 404);
assert.equal((await call(`/api/wissensbasis/${aid}/archive`, "POST", { reason: "x".repeat(161) })).status, 400);
assert.equal(kv.has(WB_ARCHIV_KV_KEY), false);

// Archivieren mit Grund; Wissensbasis bleibt unverändert; zweimal = idempotent ohne Schreiben
const reason = "Hersteller finanziert, Leistung nicht besser: nehmen wir nicht.";
let r = await call(`/api/wissensbasis/${aid}/archive`, "POST", { reason });
assert.equal(r.status, 200);
const putsAfter = puts;
r = await call(`/api/wissensbasis/${aid}/archive`, "POST", { reason: "anders" });
assert.equal(r.status, 200); assert.equal((await r.json()).already, true);
assert.equal(puts, putsAfter);
const arch = JSON.parse(kv.get(WB_ARCHIV_KV_KEY));
assert.equal(arch.items.length, 1);
assert.deepEqual(Object.keys(arch.items[0]).sort(), ["archivedAt", "id", "reason", "source", "title"]);
assert.equal(arch.items[0].title, omega.text.slice(0, 80));
assert.equal(arch.items[0].reason, reason);
assert.match(arch.items[0].archivedAt, /^\d{4}-\d{2}-\d{2}$/);
assert.equal(kv.get(WISSENSBASIS_KV_KEY), stored);

// Anzeige: weg aus Abschnitten (leere Abschnitte entfallen), steht im Archiv; Lesen schreibt nichts
const p0 = puts;
const vis = applyTestsToWissensbasis(visibleWissensbasis(JSON.parse(stored), arch, true), []);
assert.deepEqual(vis.sections.map((s) => s.title), ["HIIT"]);
assert.equal(vis.sections[0].items[0].aid, "hiit-2x");
assert.equal(archivForCoach(arch)[0].id, aid);
assert.equal(puts, p0);

// save_wissensbasis mit demselben Punkt: bleibt archiviert, Warnung in der Antwort
const log = console.log; console.log = () => {};
const rpc = async (name, args) => (await (await handleMcpRequest(new Request("https://x/mcp", { method: "POST", headers: { authorization: "Bearer gut" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } }) }), new URL("https://x/mcp"), env)).json()).result;
const sv = await rpc("save_wissensbasis", { ...body, fetchedAt: "2026-11-06T08:00:00Z", updatedAt: "2026-11-06" });
assert.ok(!sv.isError, JSON.stringify(sv));
const svj = JSON.parse(sv.content[0].text);
assert.ok(svj.warnings.some((w) => w === `id ${aid} ist archiviert, wird nicht angezeigt`), JSON.stringify(svj));
assert.ok(!svj.warnings.some((w) => w.includes("hiit-2x ist archiviert")));
assert.equal(JSON.parse(kv.get(WISSENSBASIS_KV_KEY)).sections[0].items[0].text, omega.text); // gespeichert
assert.equal(JSON.parse(kv.get(WB_ARCHIV_KV_KEY)).items.length, 1);

// get_coach: wissensbasisArchiv, wissensbasis nur sichtbare Punkte
const gc = JSON.parse((await rpc("get_coach", {})).content[0].text);
console.log = log;
assert.deepEqual(gc.wissensbasisArchiv, [{ id: aid, title: omega.text.slice(0, 80), source: omega.source, archivedAt: arch.items[0].archivedAt, reason }]);
assert.deepEqual(gc.wissensbasis.sections.map((s) => s.title), ["HIIT"]);

// studyRefs / link auf archivierten Punkt: keine Warnung (refWarnings nutzt die ungefilterte Wissensbasis), Dashboard löst über wissensbasisArchiv auf
const { refWarnings } = await import("../src/tests-snapshot.js");
const ids = new Set(JSON.parse(kv.get(WISSENSBASIS_KV_KEY)).sections.flatMap((s) => s.items.map((i) => i.id).filter(Boolean)));
assert.deepEqual(refWarnings({ studyRefs: ["hiit-2x"] }, ids, new Set()), []);

// Wiederherstellen: wieder da; zweimal = idempotent
r = await call(`/api/wissensbasis/${aid}/restore`, "POST");
assert.equal(r.status, 200); assert.equal((await r.json()).already, false);
assert.equal(JSON.parse(kv.get(WB_ARCHIV_KV_KEY)).items.length, 0);
assert.deepEqual(visibleWissensbasis(JSON.parse(kv.get(WISSENSBASIS_KV_KEY)), JSON.parse(kv.get(WB_ARCHIV_KV_KEY)), true).sections.map((s) => s.title), ["Supplemente", "HIIT"]);
r = await call(`/api/wissensbasis/${aid}/restore`, "POST");
assert.equal(r.status, 200); assert.equal((await r.json()).already, true);

// Archiv hält höchstens 100 Einträge, der älteste fällt weg
let a = null;
const many = { sections: [{ title: "A", items: Array.from({ length: 101 }, (_, i) => ({ text: `Punkt ${i}`, evidence: "mittel", fit: "hoch", id: `p-${i}` })) }] };
for (let i = 0; i < 101; i++) a = applyArchive(many, a, `p-${i}`, {}, "2026-11-06").value;
assert.equal(a.items.length, MAX_ARCHIV);
assert.equal(a.items[0].id, "p-1");

// Erkenntnis archivieren (vorhandene Archivierung)
kv.set("dashboard:insights", JSON.stringify({ items: [{ id: "i-1", title: "T", archived: false }] }));
assert.equal((await call("/api/insights/gibt-es-nicht/archive")).status, 404);
assert.equal((await call("/api/insights/i-1/archive")).status, 200);
assert.equal(JSON.parse(kv.get("dashboard:insights")).items[0].archived, true);
assert.equal((await (await call("/api/insights/i-1/archive")).json()).already, true);
console.log("wissensbasis-archiv ok");
