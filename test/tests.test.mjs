// Testet die Tests des Coaches (Validierung, Warnungen, Upsert, Statuszuordnung, Sperren, MCP). Ausführen: node test/tests.test.mjs
import assert from "node:assert/strict";
import { validateTest, upsertTest, testWarnings, refWarnings, testsForCoach, atYouFromTest, applyTestsToWissensbasis, TESTS_KV_KEY, MAX_TESTS, applyTestDecision, nextMonday, handleTestDecisionRequest } from "../src/tests-snapshot.js";
import { validateWissensbasis, wissensbasisWarnings } from "../src/wissensbasis-snapshot.js";
import { isoDateBerlin } from "../src/date-utils.js";
import { handleMcpRequest } from "../src/mcp-server.js";

const metric = { key: "lauf_hf130", label: "Pace bei HF 130", unit: "s/km", baseline: 400, baselineDate: "2026-10-16", current: null, currentDate: null };
const kand = { fetchedAt: "2026-11-05T08:00:00Z", id: "dritter-lauf", title: "Dritter Lauf, locker unter 128", kind: "test", discipline: "lauf", block: "A", blockGoal: "Aerobe Basis", question: "Verbessert ein dritter lockerer Lauf die Pace bei HF 130?", change: "Dritter Lauf pro Woche unter HF 128", metrics: [metric], load: "mehr", status: "kandidat", proposedBy: "coach" };
const laeuft = { ...kand, status: "laeuft", startDate: "2026-11-09", checkDate: "2026-12-20", decidedAt: "2026-11-05", expectation: { confirmed: "−10 s/km", unclear: "−4 bis −9 s/km", refuted: "keine Veränderung" } };

// gültig: Kandidat, laufend, Defaults, Rückgabe-Form
const k = validateTest(kand);
assert.ok(!k.error, k.error);
assert.equal(k.value.updatedAt, "2026-11-05T08:00:00.000Z");
assert.deepEqual(k.value.studyRefs, []);
assert.equal(k.value.metrics[0].better, "lower"); // bekannter key: Richtung aus dem key
assert.ok(!validateTest(laeuft).error);
// eigener key: better ist Pflicht; key frei
assert.match(validateTest({ ...kand, metrics: [{ key: "eigene-groesse", label: "x" }] }).error, /better/);
assert.ok(!validateTest({ ...kand, metrics: [{ key: "eigene-groesse", label: "x", better: "higher" }] }).error);
// Listen auch als JSON-Text
assert.deepEqual(validateTest({ ...kand, studyRefs: JSON.stringify(["a", "b"]) }).value.studyRefs, ["a", "b"]);

// Pflichtfelder und Grenzen
for (const [bad, re] of [
  [{ id: "Gross" }, /id/], [{ id: "x".repeat(61) }, /id/], [{ title: "t".repeat(81) }, /title/], [{ kind: "x" }, /kind/], [{ discipline: "x" }, /discipline/],
  [{ block: "" }, /block/], [{ block: "b".repeat(21) }, /block/], [{ blockGoal: "x".repeat(141) }, /blockGoal/], [{ question: "x".repeat(161) }, /question/],
  [{ change: "" }, /change/], [{ load: "viel" }, /load/], [{ status: "fertig" }, /status/], [{ proposedBy: "ich" }, /proposedBy/],
  [{ metrics: [] }, /metrics/], [{ metrics: Array(5).fill(0).map((_, i) => ({ ...metric, key: "k" + i })) }, /metrics/],
  [{ metrics: [metric, metric] }, /doppelt/], [{ metrics: [{ ...metric, key: "k".repeat(41) }] }, /key/],
  [{ studyRefs: Array(7).fill("a") }, /studyRefs/], [{ insightRefs: "x" }, /insightRefs/], [{ startDate: "5.11.2026" }, /startDate/],
  [{ abort: "x".repeat(141) }, /abort/], [{ adherence: { rule: "r", minPct: 120 } }, /minPct/], [{ fetchedAt: "kaputt" }, /fetchedAt/],
  [{ confoundedWith: ["dritter-lauf"] }, /confoundedWith/],
]) assert.match(validateTest({ ...kand, ...bad }).error ?? "", re, JSON.stringify(bad));

// Pflichtfelder je status
for (const f of ["startDate", "checkDate", "decidedAt", "expectation"]) assert.match(validateTest({ ...laeuft, [f]: null }).error ?? "", new RegExp(f), f);
assert.match(validateTest({ ...kand, status: "urteil" }).error, /result fehlt/);
assert.match(validateTest({ ...kand, status: "laeuft", ...laeuft, result: "bestaetigt" }).error, /result nur bei/);
assert.match(validateTest({ ...laeuft, status: "gelernt", result: "bestaetigt" }).error, /learned fehlt/);
assert.match(validateTest({ ...laeuft, learned: "x" }).error, /learned nur bei/);
assert.match(validateTest({ ...laeuft, status: "urteil", result: "super" }).error, /result/);
const done = { ...laeuft, status: "gelernt", result: "bestaetigt", learned: "Bei dir wirkt ein dritter lockerer Lauf." };
assert.ok(!validateTest(done).error);
// Einhaltung unter Mindestquote: höchstens unklar
const ad = { rule: "mind. 2 von 3 Läufen unter HF 128", minPct: 70 };
assert.match(validateTest({ ...done, adherence: { ...ad, actualPct: 50 } }).error, /Mindestquote/);
assert.ok(!validateTest({ ...done, result: "unklar", adherence: { ...ad, actualPct: 50 } }).error);
assert.ok(!validateTest({ ...done, adherence: { ...ad, actualPct: 80 } }).error);

// Upsert: ersetzen statt doppeln, älterer fetchedAt überschreibt nie, 40er-Grenze
const t1 = validateTest(kand), t1b = validateTest({ ...kand, fetchedAt: "2026-11-06T08:00:00Z", title: "Neu" });
let st = upsertTest(null, t1.value, t1.fetchedAt).value;
st = upsertTest(st, t1b.value, t1b.fetchedAt).value;
assert.equal(st.tests.length, 1);
assert.equal(st.tests[0].title, "Neu");
assert.match(upsertTest(st, t1.value, t1.fetchedAt).error, /älter/);
const mk = (i, status = "kandidat") => ({ ...t1.value, id: "t" + i, status, updatedAt: `2026-10-${String(10 + (i % 18)).padStart(2, "0")}T00:00:00.000Z` });
const full = { fetchedAt: "x", tests: Array.from({ length: MAX_TESTS }, (_, i) => mk(i)) };
assert.match(upsertTest(full, mk(99), "2026-11-05T00:00:00.000Z").error, /40/);
full.tests[5] = mk(5, "verworfen"); full.tests[7] = { ...mk(7, "verworfen"), updatedAt: "2026-09-01T00:00:00.000Z" };
const rep = upsertTest(full, mk(99), "2026-11-05T00:00:00.000Z").value;
assert.equal(rep.tests.length, MAX_TESTS);
assert.ok(!rep.tests.some((t) => t.id === "t7") && rep.tests.some((t) => t.id === "t5")); // ältester verworfener fällt zuerst weg

// Warnungen: > 2 laufende Mehrbelastung; gemeinsame Messgröße ohne gegenseitiges confoundedWith
const run = (id, over = {}) => validateTest({ ...laeuft, id, ...over }).value;
const a = run("a"), b = run("b", { metrics: [{ ...metric, key: "rad_hf165w", label: "R" }] }), c = run("c", { metrics: [{ ...metric, key: "schwimm_100m", label: "S" }] });
assert.equal(testWarnings([a, b]).length, 0);
assert.match(testWarnings([a, b, c]).join(" "), /3 laufende Tests mit Mehrbelastung/);
assert.equal(testWarnings([a, b, run("c", { load: "neutral", metrics: [{ ...metric, key: "schwimm_100m", label: "S" }] })]).length, 0);
const a2 = run("a2");
assert.match(testWarnings([a, a2]).join(" "), /teilen die Messgröße lauf_hf130/);
assert.equal(testWarnings([{ ...a, confoundedWith: ["a2"] }, { ...a2, confoundedWith: ["a"] }]).length, 0);
assert.match(testWarnings([{ ...a, confoundedWith: ["a2"] }, a2]).join(" "), /teilen/); // nur einseitig reicht nicht
assert.equal(testWarnings([a, { ...a2, kind: "begleiter" }]).length, 0); // Begleiter urteilt nicht
// unbekannte Verweise: Warnung, kein Fehler
const withRefs = validateTest({ ...kand, studyRefs: ["gibt", "fehlt"], insightRefs: ["x"] }).value;
assert.deepEqual(refWarnings(withRefs, new Set(["gibt"]), new Set(["x"])), ["studyRefs: Wissensbasis-Punkt fehlt nicht gefunden"]);

// get_coach-Sortierung: ohne verworfen; laeuft, kandidat, urteil, gelernt; innerhalb nach checkDate
const sorted = testsForCoach({ tests: [
  { id: "g", status: "gelernt" }, { id: "k", status: "kandidat" }, { id: "v", status: "verworfen" }, { id: "l2", status: "laeuft", checkDate: "2026-12-30" },
  { id: "u", status: "urteil" }, { id: "l1", status: "laeuft", checkDate: "2026-12-01" },
] }).map((t) => t.id);
assert.deepEqual(sorted, ["l1", "l2", "k", "u", "g"]);

// Statuszuordnung Test -> "Bei dir"
assert.deepEqual(atYouFromTest({ status: "kandidat" }), { atYou: "nicht_getestet", candidate: true });
assert.equal(atYouFromTest({ status: "laeuft" }).atYou, "laeuft");
for (const [result, kind, want] of [
  ["bestaetigt", "test", "gewirkt"], ["widerlegt", "paket", "nicht_gewirkt"], ["unklar", "test", "offen"], ["gestoert", "test", "offen"], ["abgebrochen", "paket", "offen"],
  ["bestaetigt", "begleiter", "stoert_nicht"], ["widerlegt", "begleiter", "stoert"], ["unklar", "begleiter", "offen"],
]) assert.equal(atYouFromTest({ status: "urteil", kind, result }).atYou, want, `${kind}/${result}`);

// Wissensbasis: id, link.type test, doppelte id; Verknüpfung über studyRefs und link; Sperren
const wbItem = { text: "Punkt", evidence: "mittel", fit: "hoch" };
const wbBody = (items) => ({ fetchedAt: "2026-11-05T08:00:00Z", updatedAt: "2026-11-05", sections: [{ title: "A", items }] });
assert.ok(!validateWissensbasis(wbBody([{ ...wbItem, id: "dritter-lauf-studie", link: { type: "test", id: "dritter-lauf" } }])).error);
assert.match(validateWissensbasis(wbBody([{ ...wbItem, id: "Gross" }])).error, /\.id/);
assert.match(validateWissensbasis(wbBody([{ ...wbItem, id: "a" }, { ...wbItem, id: "a" }])).error, /doppelt/);
const wb = validateWissensbasis(wbBody([
  { ...wbItem, id: "s1", practice: "alt", atYou: "gewirkt", consensus: "einig" },          // verknüpft über studyRefs
  { ...wbItem, id: "s2", link: { type: "test", id: "dritter-lauf" }, atYou: "nicht_gewirkt" }, // verknüpft über link
  { ...wbItem, id: "s3", atYou: "gewirkt", consensus: "einig", counterCheckedAt: "2026-11-01" }, // ohne Test
  { ...wbItem, id: "s4", atYou: "laeuft", practice: "gespeichert" },
])).value;
const tGelernt = validateTest({ ...done, studyRefs: ["s1"] }).value;
const out = applyTestsToWissensbasis(wb, [tGelernt]).sections[0].items;
assert.equal(out[0].atYou, "gewirkt"); assert.equal(out[0].practice, tGelernt.change); assert.deepEqual(out[0].tests, [{ id: tGelernt.id, title: tGelernt.title, status: "gelernt" }]);
assert.equal(out[0].consensus, "eine_seite"); // einig nur mit counterCheckedAt
assert.equal(out[1].atYou, "gewirkt"); // link type test, atYou aus dem Test, gespeichertes nicht_gewirkt ignoriert
assert.equal(out[2].atYou, "offen"); assert.equal(out[2].consensus, "einig"); // gewirkt nie ohne Test-Ergebnis
assert.equal(out[3].atYou, "laeuft"); assert.equal(out[3].practice, "gespeichert"); // ohne Test: gespeichert wie bisher
const cand = applyTestsToWissensbasis(wb, [validateTest({ ...kand, studyRefs: ["s4"] }).value]).sections[0].items[3];
assert.equal(cand.atYou, "nicht_getestet"); assert.equal(cand.candidate, true);
const verw = applyTestsToWissensbasis(wb, [{ ...tGelernt, status: "verworfen" }]).sections[0].items[0];
assert.equal(verw.tests, undefined); // verworfene Tests verknüpfen nicht
const ww = wissensbasisWarnings(wb, [tGelernt]);
assert.equal(ww.length, 2);
assert.match(ww.join(" "), /s1: Studienlage einig ohne counterCheckedAt/);
assert.match(ww.join(" "), /s3: atYou gewirkt gibt es nur über ein Test-Ergebnis/);

// MCP: save_test, Warnungen, get_coach.tests, save_wissensbasis mit Warnungen, älterer fetchedAt, Altbestand experiment bleibt
const kv = new Map([["mcp:token:gut", "1"]]);
const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const log = console.log; console.log = () => {};
const rpc = async (method, params) => {
  const res = await handleMcpRequest(new Request("https://x/mcp", { method: "POST", headers: { authorization: "Bearer gut" }, body: JSON.stringify({ jsonrpc: "2.0", id: 1, method, params }) }), new URL("https://x/mcp"), env);
  return res.json();
};
const call = async (name, args) => { const r = await rpc("tools/call", { name, arguments: args }); return r.result.isError ? { error: r.result.content[0].text } : JSON.parse(r.result.content[0].text); };
const tools = (await rpc("tools/list")).result.tools;
assert.ok(tools.some((t) => t.name === "save_test"));
assert.match(tools.find((t) => t.name === "save_experiment").description, /Veraltet, bitte save_test nutzen/);
assert.ok(tools.find((t) => t.name === "save_wissensbasis").inputSchema.properties.sections.items.properties.items.items.properties.id);
kv.set("dashboard:experiment", JSON.stringify({ fetchedAt: "2026-10-22T06:00:00.000Z", title: "Alt", hypothesis: "h", status: "läuft", startDate: null, minWeeks: 6, targetDaysPerWeek: 5, metrics: [], verdict: null }));
assert.deepEqual((await call("save_wissensbasis", wbBody([{ ...wbItem, id: "studie-eins" }]))).warnings, undefined);
let r = await call("save_test", { ...laeuft, id: "t-eins", studyRefs: ["studie-eins", "weg"], insightRefs: ["nix"] });
assert.equal(r.ok, true); assert.equal(r.id, "t-eins");
assert.deepEqual(r.warnings.filter((w) => /nicht gefunden/.test(w)), ["studyRefs: Wissensbasis-Punkt weg nicht gefunden", "insightRefs: Erkenntnis nix nicht gefunden"]);
r = await call("save_test", { ...laeuft, id: "t-zwei", checkDate: "2026-12-21" });
assert.match(r.warnings.join(" "), /t-zwei und t-eins teilen die Messgröße/);
r = await call("save_test", { ...laeuft, id: "t-drei", checkDate: "2026-12-22", metrics: [{ ...metric, key: "rad_hf165w", label: "R" }] });
assert.match(r.warnings.join(" "), /3 laufende Tests mit Mehrbelastung/);
assert.ok((await call("save_test", { ...laeuft, id: "t-eins", fetchedAt: "2026-11-04T08:00:00Z" })).error.match(/älter/));
assert.ok((await call("save_test", { ...kand, id: "t-x", status: "urteil" })).error.match(/result/));
const coach = await call("get_coach", {});
assert.deepEqual(coach.tests.map((t) => t.id), ["t-eins", "t-zwei", "t-drei"]);
assert.equal(coach.tests[0].studyRefs[0], "studie-eins");
assert.equal(coach.experiment.title, "Alt"); // Altbestand unverändert
// verworfen verschwindet aus get_coach
await call("save_test", { ...kand, id: "t-drei", status: "verworfen", fetchedAt: "2026-11-06T08:00:00Z" });
assert.deepEqual((await call("get_coach", {})).tests.map((t) => t.id), ["t-eins", "t-zwei"]);
// save_wissensbasis meldet Sperren als Warnung
const w2 = await call("save_wissensbasis", { ...wbBody([{ ...wbItem, id: "studie-zwei", atYou: "gewirkt", consensus: "einig" }]), fetchedAt: "2026-11-07T08:00:00Z" });
assert.equal(w2.ok, true); assert.equal(w2.warnings.length, 2);

// Gerberts Entscheidung über Kandidaten (POST /api/tests/:id/decision)
console.log = log;
assert.equal(nextMonday("2026-11-05"), "2026-11-09"); assert.equal(nextMonday("2026-11-09"), "2026-11-09"); assert.equal(nextMonday("2026-11-08"), "2026-11-09");
const exp = { confirmed: "−10 s/km", unclear: "−4 bis −9", refuted: "keine" };
const cand1 = validateTest(kand).value;
const store0 = { fetchedAt: cand1.updatedAt, tests: [cand1] };
const T = "2026-11-05", NOW = "2026-11-05T09:00:00.000Z";
// Start: Defaults (nächster Montag, +8 Wochen), decidedAt heute, proposedBy bleibt
let d = applyTestDecision(store0, "dritter-lauf", { action: "start", expectation: exp }, T, NOW);
assert.ok(!d.error, d.error);
assert.equal(d.test.status, "laeuft"); assert.equal(d.test.startDate, "2026-11-09"); assert.equal(d.test.checkDate, "2027-01-04");
assert.equal(d.test.decidedAt, T); assert.equal(d.test.proposedBy, "coach"); assert.deepEqual(d.test.expectation, exp);
assert.equal(d.value.tests[0].status, "laeuft");
// Eigene Daten, Block, Einhaltung
d = applyTestDecision(store0, "dritter-lauf", { action: "start", startDate: "2026-11-16", checkDate: "2026-12-20", expectation: exp, block: "B", adherence: { rule: "2 von 3 Läufen", minPct: 66 } }, T, NOW);
assert.equal(d.test.block, "B"); assert.equal(d.test.checkDate, "2026-12-20"); assert.equal(d.test.adherence.minPct, 66);
// Vorgabe des Tests als Prüfdatum, Erwartung vom Coach vorgeschlagen
const store1 = { tests: [{ ...cand1, checkDate: "2026-12-30", expectation: exp }] };
d = applyTestDecision(store1, "dritter-lauf", { action: "start" }, T, NOW);
assert.equal(d.test.checkDate, "2026-12-30"); assert.deepEqual(d.test.expectation, exp);
// Pflichtfelder und Fehler
assert.match(applyTestDecision(store0, "dritter-lauf", { action: "start" }, T, NOW).error, /expectation/);
assert.match(applyTestDecision(store0, "dritter-lauf", { action: "start", expectation: exp, startDate: "11.11." }, T, NOW).error, /startDate/);
assert.match(applyTestDecision(store0, "dritter-lauf", { action: "start", expectation: exp, startDate: "2026-12-01", checkDate: "2026-11-20" }, T, NOW).error, /vor startDate/);
assert.match(applyTestDecision(store0, "dritter-lauf", { action: "start", expectation: { confirmed: "a" } }, T, NOW).error, /expectation/);
assert.equal(applyTestDecision(store0, "weg", { action: "reject" }, T, NOW).status, 404);
assert.equal(applyTestDecision(store0, "dritter-lauf", { action: "foo" }, T, NOW).status, 400);
assert.equal(applyTestDecision(d.value, "dritter-lauf", { action: "reject" }, T, NOW).status, 409); // läuft schon
// Ablehnen: verworfen, blendet aus
d = applyTestDecision(store0, "dritter-lauf", { action: "reject" }, T, NOW);
assert.equal(d.test.status, "verworfen"); assert.equal(d.test.decidedAt, T); assert.equal(testsForCoach(d.value).length, 0);
// Später: bleibt Kandidat, nur block; ohne block Fehler
d = applyTestDecision(store0, "dritter-lauf", { action: "later", block: "Block C" }, T, NOW);
assert.equal(d.test.status, "kandidat"); assert.equal(d.test.block, "Block C"); assert.equal(d.test.decidedAt, null);
assert.match(applyTestDecision(store0, "dritter-lauf", { action: "later" }, T, NOW).error, /block/);
assert.match(applyTestDecision(store0, "dritter-lauf", { action: "later", block: "x".repeat(21) }, T, NOW).error, /block/);
// Warnung: dritter laufender Test mit Mehrbelastung
const heavy = (id) => validateTest({ ...laeuft, id, metrics: [{ ...metric, key: "k-" + id, better: "lower" }] }).value;
d = applyTestDecision({ tests: [heavy("a"), heavy("b"), cand1] }, "dritter-lauf", { action: "start", expectation: exp }, T, NOW);
assert.equal(d.warnings.length, 1); assert.match(d.warnings[0], /3 laufende Tests mit Mehrbelastung/);

// Endpunkt: Token, Methode, Schreiben in KV, Schutz vor Zurücksetzen durch save_test
const kv2 = new Map([[TESTS_KV_KEY, JSON.stringify(store0)]]);
const env2 = { DASHBOARD_TOKEN: "tok", KV: { get: async (k) => kv2.get(k) ?? null, put: async (k, v) => void kv2.set(k, v) } };
const auth = (req, e) => req.headers.get("authorization") === "Bearer " + e.DASHBOARD_TOKEN;
const post = (body, token = "tok", method = "POST") => new Request("https://x/api/tests/dritter-lauf/decision", { method, headers: { authorization: "Bearer " + token }, body: method === "POST" ? JSON.stringify(body) : undefined });
assert.equal((await handleTestDecisionRequest(post({ action: "reject" }, "falsch"), env2, auth, "dritter-lauf")).status, 401);
assert.equal((await handleTestDecisionRequest(post({}, "tok", "GET"), env2, auth, "dritter-lauf")).status, 405);
assert.equal((await handleTestDecisionRequest(post({ action: "start" }), env2, auth, "dritter-lauf")).status, 400);
assert.equal(JSON.parse(kv2.get(TESTS_KV_KEY)).tests[0].status, "kandidat"); // Fehler schreibt nichts
const ok = await handleTestDecisionRequest(post({ action: "start", expectation: exp }), env2, auth, "dritter-lauf");
assert.equal(ok.status, 200); assert.equal((await ok.json()).test.status, "laeuft");
const saved = JSON.parse(kv2.get(TESTS_KV_KEY)).tests[0];
assert.equal(saved.status, "laeuft"); assert.equal(saved.decidedAt, isoDateBerlin());
// save_test mit kandidat überschreibt die Freigabe nicht (auch mit neuerem fetchedAt), warnt
for (const [k, v] of kv2) kv.set(k, v);
console.log = () => {};
const back = await call("save_test", { ...kand, fetchedAt: "2099-01-01T00:00:00Z" });
console.log = log;
assert.equal(back.ignored, true); assert.match(back.warnings[0], /entschieden/);
assert.equal(JSON.parse(kv.get(TESTS_KV_KEY)).tests.find((t) => t.id === "dritter-lauf").status, "laeuft");
// ein Test ohne decidedAt lässt sich weiter als Kandidat speichern
console.log = () => {};
assert.equal((await call("save_test", { ...kand, id: "anderer", fetchedAt: "2026-11-06T08:00:00Z" })).ok, true);
console.log = log;
console.log("tests ok");
