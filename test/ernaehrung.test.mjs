// Testet src/ernaehrung.js und die MCP-Tools save_ernaehrung / get_coach (ernaehrung). Ausführen: node test/ernaehrung.test.mjs
import assert from "node:assert/strict";
import { validateErnaehrung, mergeErnaehrung, normalizeWeights, weightTrend7d, mergeWeights, buildErnaehrung, WEIGHT_KV_KEY } from "../src/ernaehrung.js";
import { handleMcpRequest } from "../src/mcp-server.js";

const at = "2026-10-29T05:00:00Z";
// Validierung
assert.ok(validateErnaehrung({ headline: "x" }).error); // fetchedAt fehlt
assert.ok(validateErnaehrung({ fetchedAt: at }).error); // nichts zu speichern
assert.ok(validateErnaehrung({ fetchedAt: at, headline: "h".repeat(81) }).error);
assert.ok(validateErnaehrung({ fetchedAt: at, text: "t".repeat(2001) }).error);
assert.ok(validateErnaehrung({ fetchedAt: at, weekLine: "w".repeat(301) }).error);
assert.ok(validateErnaehrung({ fetchedAt: at, text: 5 }).error);
assert.ok(validateErnaehrung({ fetchedAt: at, goal: { targetKg: 10, byDate: "2026-12-31" } }).error);
assert.ok(validateErnaehrung({ fetchedAt: at, goal: { targetKg: 80, byDate: "bald" } }).error);
assert.ok(validateErnaehrung({ fetchedAt: at, goal: "kaputt" }).error);
assert.deepEqual(validateErnaehrung({ fetchedAt: at, headline: " Kurz ", text: " Text ", weekLine: " Woche " }).value, { fetchedAt: "2026-10-29T05:00:00.000Z", headline: "Kurz", text: "Text", weekLine: "Woche" });
assert.ok(validateErnaehrung({ fetchedAt: at, headline: "h".repeat(80), text: "t".repeat(2000), weekLine: "w".repeat(300) }).value);
assert.deepEqual(validateErnaehrung({ fetchedAt: at, goal: { targetKg: "82,5", byDate: "2026-12-31" } }).value.goal, { targetKg: 82.5, byDate: "2026-12-31" });

// Ziel bleibt, wenn es fehlt; null entfernt es
const withGoal = mergeErnaehrung(null, validateErnaehrung({ fetchedAt: at, text: "a", goal: { targetKg: 80, byDate: "2026-12-31" } }).value);
assert.deepEqual(withGoal.goal, { targetKg: 80, byDate: "2026-12-31" });
assert.deepEqual(mergeErnaehrung(withGoal, validateErnaehrung({ fetchedAt: at, text: "b" }).value).goal, { targetKg: 80, byDate: "2026-12-31" });
assert.equal(mergeErnaehrung(withGoal, validateErnaehrung({ fetchedAt: at, text: "b", goal: null }).value).goal, null);
assert.equal(mergeErnaehrung(null, validateErnaehrung({ fetchedAt: at, text: "b" }).value).goal, null);

// Trend: Mittel der 7 Kalendertage bis zum Tag; Lücken im Fenster zählen nicht; ungültige Werte fallen raus
assert.deepEqual(weightTrend7d([]), []);
assert.deepEqual(weightTrend7d(null), []);
const w = [{ date: "2026-10-01", kg: 90 }, { date: "2026-10-02", kg: 88 }, { date: "2026-10-05", kg: 86 }, { date: "2026-10-09", kg: 84 }, { date: "2026-10-03", kg: 0 }, { date: "x", kg: 80 }];
const tr = Object.fromEntries(weightTrend7d(w).map((x) => [x.date, x.kg]));
assert.equal(tr["2026-10-01"], 90);
assert.equal(tr["2026-10-02"], 89);
assert.equal(tr["2026-10-05"], 88); // (90+88+86)/3
assert.equal(tr["2026-10-07"], 88); // Fenster 01.-07.
assert.equal(tr["2026-10-08"], 87); // 01. fällt raus: (88+86)/2
assert.equal(tr["2026-10-09"], 85); // Fenster 03.-09.: (86+84)/2
assert.equal(tr["2026-10-10"], undefined); // die Reihe endet am letzten Eintrag
assert.deepEqual(normalizeWeights([{ date: "2026-10-01T06:00:00Z", kg: "90.04" }, { date: "2026-10-01", kg: 91 }]), [{ date: "2026-10-01", kg: 91 }]);

// 90 Tage, neue Werte gewinnen
const old = [{ date: "2026-07-01", kg: 95 }, { date: "2026-10-28", kg: 85 }];
assert.deepEqual(mergeWeights(old, [{ date: "2026-10-28", kg: 84.5 }, { date: "2026-10-29", kg: 84 }], "2026-10-29"), [{ date: "2026-10-28", kg: 84.5 }, { date: "2026-10-29", kg: 84 }]);

// buildErnaehrung: leer, Yazio, Fallback Intervals, kaputter Speicher
const kvEnv = (init = {}) => { const m = new Map(Object.entries(init)); return { m, env: { KV: { get: async (k) => m.get(k) ?? null, put: async (k, v) => void m.set(k, v) } } }; };
assert.deepEqual(await buildErnaehrung(kvEnv().env, "2026-10-29"), { coach: null, weight: [], weightTrend7d: [], weightSource: null });
let e = await buildErnaehrung(kvEnv({ [WEIGHT_KV_KEY]: JSON.stringify({ entries: [{ date: "2026-10-28", kg: 85 }, { date: "2026-10-29", kg: 84 }] }) }).env, "2026-10-29", () => { throw new Error("nicht nötig"); });
assert.equal(e.weightSource, "yazio"); assert.deepEqual(e.weight.map((x) => x.kg), [85, 84]); assert.equal(e.weightTrend7d[1].kg, 84.5);
e = await buildErnaehrung(kvEnv().env, "2026-10-29", async () => [{ id: "2026-10-28", weight: 85 }, { id: "2026-10-29" }]);
assert.equal(e.weightSource, "intervals"); assert.deepEqual(e.weight, [{ date: "2026-10-28", kg: 85 }]);
e = await buildErnaehrung(kvEnv().env, "2026-10-29", async () => { throw new Error("Intervals down"); });
assert.equal(e.weightSource, null);

// MCP: save_ernaehrung und get_coach
const kv = new Map([["mcp:token:gut", "1"]]);
const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } }; // ohne ATHLETE_ID: Intervals/Yazio nicht erreichbar
const log = console.log; console.log = () => {};
const rpc = async (name, args) => {
  const res = await handleMcpRequest(new Request("https://x/mcp", { method: "POST", headers: { authorization: "Bearer gut" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } }) }), new URL("https://x/mcp"), env);
  return (await res.json()).result;
};
const coach = async () => JSON.parse((await rpc("get_coach", {})).content[0].text);
let r = await rpc("save_ernaehrung", { fetchedAt: at });
assert.equal(r.isError, true);
assert.equal(kv.has("dashboard:ernaehrung"), false); // nichts geschrieben
let gc = await coach();
assert.deepEqual(gc.ernaehrung, { coach: null, weight: [], weightTrend7d: [], weightSource: null }); // leerer Zustand
r = await rpc("save_ernaehrung", { fetchedAt: at, headline: "Auf Kurs", text: "Heute 2200 kcal.", weekLine: "Woche ok", goal: { targetKg: 80, byDate: "2026-12-31" } });
assert.ok(!r.isError);
r = await rpc("save_ernaehrung", { fetchedAt: "2026-10-30T05:00:00Z", text: "Neuer Tag." });
assert.ok(!r.isError);
gc = await coach();
assert.equal(gc.ernaehrung.coach.text, "Neuer Tag.");
assert.equal(gc.ernaehrung.coach.headline, null);
assert.deepEqual(gc.ernaehrung.coach.goal, { targetKg: 80, byDate: "2026-12-31" }); // Ziel bleibt
// bestehende get_coach-Felder bleiben
assert.ok("week_totals" in gc && "targets" in gc && "ideas" in gc);
// kaputter Wert im KV macht get_coach nicht kaputt
kv.set("dashboard:ernaehrung", "{kaputt"); kv.set(WEIGHT_KV_KEY, "{kaputt");
gc = await coach();
assert.deepEqual(gc.ernaehrung, { coach: null, weight: [], weightTrend7d: [], weightSource: null });
console.log = log;
console.log("ernaehrung ok");
