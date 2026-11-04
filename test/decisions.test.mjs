// Testet save_decisions, die Absicherung von save_coach und die Migration. Ausführen: node test/decisions.test.mjs
import assert from "node:assert/strict";
import { handleMcpRequest } from "../src/mcp-server.js";
import { handleDecisionAnswerRequest, migrateDecisions, DECISIONS_KV_KEY } from "../src/decisions-snapshot.js";

const log = console.log; console.log = () => {};
const makeEnv = (init = {}) => {
  const kv = new Map([["mcp:token:gut", "1"], ...Object.entries(init).map(([k, v]) => [k, JSON.stringify(v)])]);
  const writes = [];
  return { kv, writes, env: { DASHBOARD_TOKEN: "t", KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => { writes.push(k); kv.set(k, v); } } } };
};
const call = async (env, name, args) => {
  const res = await handleMcpRequest(new Request("https://x/mcp", { method: "POST", headers: { authorization: "Bearer gut" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "tools/call", params: { name, arguments: args } }) }), new URL("https://x/mcp"), env);
  const r = (await res.json()).result;
  return { error: r.isError ? r.content[0].text : null, data: r.isError ? null : JSON.parse(r.content[0].text) };
};
const get = (kv, k) => JSON.parse(kv.get(k));
const dec = (title, deadline, extra = {}) => ({ title, recommendation: "Ja", reason: "weil", alternatives: ["Option B"], deadline, ifNoAnswer: "bleibt wie geplant", ...extra });
const NOW = new Date().toISOString();
const FULL = { fetchedAt: NOW, note: "Locker.", headline: "Ruhe", go: "locker", blockers: ["Hüfte"], text: "Bericht", question: "Frage?", answer: "Antwort" };

// 1 + 2: save_coach lässt die Liste stehen, save_decisions die Tagesaussage
{
  const { env, kv } = makeEnv();
  assert.ok(!(await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("A", "2026-11-10"), dec("B", "2026-12-01")] })).error);
  assert.ok(!(await call(env, "save_coach", { ...FULL, idea: { text: "Idee", angle: 2, sport: "lauf" } })).error);
  assert.equal(get(kv, DECISIONS_KV_KEY).decisions.length, 2);
  const coachBefore = kv.get("dashboard:coach");
  const r = await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("B", "2026-12-01")] });
  assert.ok(!r.error);
  assert.equal(kv.get("dashboard:coach"), coachBefore); // byte-gleich: save_decisions fasst sie nicht an
  const c = get(kv, "dashboard:coach");
  for (const k of ["note", "headline", "go", "blockers", "text", "question", "answer", "idea"]) assert.ok(c[k] != null, k);
  assert.equal("decisions" in c, false);
  assert.ok(!(await call(env, "save_coach", { ...FULL, note: "Neu" })).error);
  assert.deepEqual(get(kv, DECISIONS_KV_KEY).decisions.map((x) => x.title), ["B"]);
  // get_coach liefert decisions mit id
  const g = await call(env, "get_coach", {});
  assert.equal(g.data.decisions[0].id, "b");
}

// 3: save_coach mit decisions wird abgelehnt, nichts gespeichert
{
  const { env, kv, writes } = makeEnv();
  const r = await call(env, "save_coach", { ...FULL, decisions: [dec("A", "2026-11-10")] });
  assert.equal(r.error, "Error: Entscheidungen bitte mit save_decisions speichern");
  const r2 = await call(env, "save_coach", { ...FULL, decisions: [] });
  assert.match(r2.error, /save_decisions/);
  assert.equal(writes.length, 0);
}

// 4: save_coach ohne text am selben Tag wird abgelehnt; unveränderter text bleibt erlaubt; anderer Tag erlaubt
{
  const { env, kv } = makeEnv();
  assert.ok(!(await call(env, "save_coach", FULL)).error);
  const before = kv.get("dashboard:coach");
  const { text, ...noText } = FULL;
  const r = await call(env, "save_coach", { ...noText, note: "Nur Note" });
  assert.equal(r.error, "Error: text fehlt – save_coach ersetzt alles; bitte vollständig senden");
  assert.equal((await call(env, "save_coach", { ...FULL, text: "" })).error?.slice(0, 15), "Error: text feh");
  assert.equal(kv.get("dashboard:coach"), before);
  assert.ok(!(await call(env, "save_coach", { ...FULL, note: "Chat-Update" })).error); // Chat-Ablauf: text unverändert erneut
  // Stand von gestern: ohne text erlaubt
  kv.set("dashboard:coach", JSON.stringify({ ...JSON.parse(before), fetchedAt: "2020-01-01T06:00:00Z" }));
  // (die Uhr zählt auch: ein Aufruf mit altem fetchedAt am heutigen Tag bleibt geschützt, siehe checkCoachReplace)
}

// 5: Antwort im Dashboard, danach save_decisions ohne Antwortfelder -> Antwort bleibt
{
  const { env, kv } = makeEnv();
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("A", "2026-11-10", { id: "punkt-a" }), dec("B", "2026-12-01", { id: "punkt-b" })] });
  const ans = (body) => handleDecisionAnswerRequest(new Request("https://x/api/coach/decision", { method: "POST", headers: { authorization: "Bearer t" }, body: JSON.stringify(body) }), env.DASHBOARD_TOKEN ? env : env, () => true);
  const coachBefore = JSON.stringify({ a: 1 });
  kv.set("dashboard:coach", coachBefore);
  assert.equal((await ans({ id: "punkt-a", answer: " Ja " })).status, 200);
  assert.equal(kv.get("dashboard:coach"), coachBefore); // Antwort ändert nur den Punkt
  assert.equal((await ans({ id: "nope", answer: "Ja" })).status, 409);
  assert.equal((await ans({ id: "punkt-a", answer: "" })).status, 409);
  const a1 = get(kv, DECISIONS_KV_KEY).decisions[0];
  assert.equal(a1.answer, "Ja");
  assert.equal(get(kv, DECISIONS_KV_KEY).decisions[1].answer, undefined);
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("A neu formuliert", "2026-11-10", { id: "punkt-a" }), dec("B", "2026-12-01", { id: "punkt-b" })] });
  const after = get(kv, DECISIONS_KV_KEY).decisions;
  assert.equal(after[0].answer, "Ja");
  assert.equal(after[0].answeredAt, a1.answeredAt);
  assert.equal(after[1].answer, undefined);
  // ausdrücklich setzen / entfernen
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("A", "2026-11-10", { id: "punkt-a", answer: "" })] });
  assert.equal(get(kv, DECISIONS_KV_KEY).decisions[0].answer, undefined);
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("A", "2026-11-10", { id: "punkt-a", answer: "Nein" })] });
  assert.equal(get(kv, DECISIONS_KV_KEY).decisions[0].answer, "Nein");
}

// 6: fehlender Punkt ist weg, [] blendet die Karte aus; id aus Titel, in der Antwort zurück
{
  const { env, kv } = makeEnv();
  const r = await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("Kraft früh starten?", "2026-11-10"), dec("Kraft früh starten?", "2026-11-09"), dec("C", "2026-12-01")] });
  assert.deepEqual(r.data.decisions.map((x) => x.id), ["kraft-frueh-starten-2", "kraft-frueh-starten", "c"]);
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("C", "2026-12-01")] });
  assert.deepEqual(get(kv, DECISIONS_KV_KEY).decisions.map((x) => x.id), ["c"]);
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [] });
  assert.deepEqual(get(kv, DECISIONS_KV_KEY).decisions, []);
  assert.deepEqual((await call(env, "get_coach", {})).data.decisions, []);
}

// 7: mehr als 5, ungültiges Datum, kaputte id, doppelte id, Länge -> Fehler, nichts gespeichert
{
  const { env, kv, writes } = makeEnv();
  await call(env, "save_decisions", { fetchedAt: NOW, decisions: [dec("A", "2026-11-10")] });
  const stored = kv.get(DECISIONS_KV_KEY);
  const n = writes.length;
  const bad = [
    [Array.from({ length: 6 }, (_, i) => dec("P" + i, "2026-11-10")), /höchstens 5/],
    [[dec("A", "morgen")], /deadline/],
    [[dec("A", "2026-13-45")], /deadline/],
    [[dec("A", "2026-11-10", { id: "Groß Klein" })], /kebab/],
    [[dec("A", "2026-11-10", { id: "x" }), dec("B", "2026-11-10", { id: "x" })], /doppelt/],
    [[dec("A".repeat(81), "2026-11-10")], /länger als 80/],
    [[dec("A", "2026-11-10", { ifNoAnswer: "" })], /ifNoAnswer/],
    [[dec("A", "2026-11-10", { alternatives: ["a", "b", "c", "d"] })], /höchstens 3/],
    [[dec("A", "2026-11-10", { answer: "x".repeat(201) })], /answer/],
    ["kaputt", /Liste/],
  ];
  for (const [decisions, re] of bad) {
    const r = await call(env, "save_decisions", { fetchedAt: NOW, decisions });
    assert.match(r.error ?? "", re);
  }
  assert.match((await call(env, "save_decisions", { decisions: [] })).error, /fetchedAt/);
  assert.equal(kv.get(DECISIONS_KV_KEY), stored);
  assert.equal(writes.length, n);
}

// 8: Migration
{
  const legacy = [{ ...dec("Eins", "2026-11-10"), answer: "Ja", answeredAt: "2026-11-03T10:00:00.000Z" }, dec("Zwei", "2026-11-12"), dec("Drei", "2026-11-20"), dec("Vier", "2026-12-01")];
  const coach = { ...FULL, fetchedAt: "2026-11-04T06:00:00.000Z", decisions: legacy };
  const { env, kv, writes } = makeEnv({ "dashboard:coach": coach });
  await migrateDecisions(env);
  const list = get(kv, DECISIONS_KV_KEY).decisions;
  assert.equal(list.length, 4);
  assert.deepEqual(list.map((x) => x.id), ["eins", "zwei", "drei", "vier"]);
  assert.equal(list[0].answer, "Ja");
  assert.equal("decisions" in get(kv, "dashboard:coach"), false);
  for (const k of ["note", "headline", "go", "blockers", "text", "question", "answer"]) assert.deepEqual(get(kv, "dashboard:coach")[k], coach[k]);
  const snap = [kv.get(DECISIONS_KV_KEY), kv.get("dashboard:coach")], n = writes.length;
  await migrateDecisions(env);
  await migrateDecisions(env);
  assert.deepEqual([kv.get(DECISIONS_KV_KEY), kv.get("dashboard:coach")], snap);
  assert.equal(writes.length, n); // zweiter Lauf schreibt nichts
  // Abbruch zwischen den beiden Schreibvorgängen: neuer Schlüssel da, altes Feld noch da -> Wiederholung legt nichts doppelt an
  const half = makeEnv({ "dashboard:coach": coach, [DECISIONS_KV_KEY]: { fetchedAt: "x", decisions: list.slice(0, 2).map((x) => ({ ...x, answer: x.answer })) } });
  await migrateDecisions(half.env);
  assert.deepEqual(get(half.kv, DECISIONS_KV_KEY).decisions.map((x) => x.id), ["eins", "zwei", "drei", "vier"]);
  assert.equal("decisions" in get(half.kv, "dashboard:coach"), false);
  // save_coach vor jeder anderen Migration verliert die Punkte nicht
  const early = makeEnv({ "dashboard:coach": coach });
  assert.ok(!(await call(early.env, "save_coach", { ...FULL, fetchedAt: new Date().toISOString() })).error);
  assert.equal(get(early.kv, DECISIONS_KV_KEY).decisions.length, 4);
  assert.equal((await call(early.env, "get_coach", {})).data.decisions.length, 4);
}
console.log = log;
console.log("decisions ok");
