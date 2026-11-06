// Testet die Wissensbasis (Validierung, Speichern, get_coach). Ausführen: node test/wissensbasis.test.mjs
import assert from "node:assert/strict";
import { validateWissensbasis, wissensbasisWarnings, saveWissensbasis, WISSENSBASIS_KV_KEY } from "../src/wissensbasis-snapshot.js";
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

// forYou (max. 200) und addedAt (YYYY-MM-DD) je Punkt; alter Stand ohne beides bleibt gültig, Warnung bei fehlendem forYou
const fy = validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, forYou: "  Studie sagt X, für dich heißt das Y.  ", addedAt: "2026-11-02" }] }] });
assert.ok(!fy.error, fy.error);
assert.equal(fy.value.sections[0].items[0].forYou, "Studie sagt X, für dich heißt das Y.");
assert.equal(fy.value.sections[0].items[0].addedAt, "2026-11-02");
assert.equal(ok.value.sections[0].items[0].forYou, undefined);
assert.ok(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, forYou: "x".repeat(200) }] }] }).value);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, forYou: "x".repeat(201) }] }] }).error, /forYou.*200/);
assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, addedAt: "gestern" }] }] }).error, /addedAt/);
assert.deepEqual(wissensbasisWarnings(fy.value, []), []);
assert.match(wissensbasisWarnings(ok.value, [])[0], /forYou fehlt/);

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
assert.ok(JSON.parse(r.result.content[0].text).ok === true);
assert.match(JSON.parse(r.result.content[0].text).warnings.join(" "), /forYou fehlt/); // Warnung, kein Fehler
r = await rpc("tools/call", { name: "save_wissensbasis", arguments: { ...good, sections: [{ title: "A", items: [{ ...item, evidence: "x" }] }] } });
assert.equal(r.result.isError, true);
r = await rpc("tools/call", { name: "get_coach", arguments: {} });
console.log = log;
const coach = JSON.parse(r.result.content[0].text);
assert.equal(coach.wissensbasis.sections[0].items[0].isNew, true);
assert.equal(coach.wissensbasis.updatedAt, "2026-11-06");
console.log("wissensbasis ok");

// link targets: nur bekannte Rahmenfelder
{
  const ok = validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, link: { type: "targets", id: "maxIncreasePct" } }] }] });
  assert.deepEqual(ok.value.sections[0].items[0].link, { type: "targets", id: "maxIncreasePct" });
  assert.match(validateWissensbasis({ ...good, sections: [{ title: "A", items: [{ ...item, link: { type: "targets", id: "foo" } }] }] }).error, /targets/);
}

// ---- Übernahme (adoption) ----
{
  const { mergeAdoption, wissenForCoach, applyWissenAdoption } = await import("../src/wissensbasis-snapshot.js");
  const kv = new Map();
  const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => { kv.set(k, v); } } };
  const ad = { rule: "Mind. 5 g KH/kg an Schlüsseltagen.", conditions: "Intervall- und Longrun-Tage" };
  const mk = (extra = {}, at = "2026-11-05T08:00:00Z") => ({ ...good, fetchedAt: at, sections: [{ title: "Ernährung im Training", items: [{ ...item, text: "KH-Aufnahme", adoption: ad, ...extra }, { text: "Reines Wissen", evidence: "stark", fit: "hoch" }] }] });
  // ohne id: aus dem Text; state offen; Punkt ohne adoption bleibt ohne
  let r = await saveWissensbasis(env, mk());
  const [a0, b0] = r.value.sections[0].items;
  assert.equal(a0.id, "kh-aufnahme"); assert.equal(a0.adoption.state, "offen"); assert.equal(b0.adoption, undefined);
  assert.deepEqual(wissenForCoach(r.value), []);
  // Knopf
  const dec = applyWissenAdoption(JSON.parse(kv.get(WISSENSBASIS_KV_KEY)), "kh-aufnahme", { state: "uebernommen" }, "2026-11-06");
  kv.set(WISSENSBASIS_KV_KEY, JSON.stringify(dec.value));
  assert.equal(applyWissenAdoption(dec.value, "reines", { state: "uebernommen" }).status, 404);
  assert.equal(applyWissenAdoption(dec.value, "kh-aufnahme", { state: "x" }).status, 400);
  // Coach speichert ganzen Stand mit absichtlich falschem state: bleibt uebernommen, decidedAt bleibt
  r = await saveWissensbasis(env, mk({ adoption: { ...ad, state: "abgelehnt", decidedAt: "2020-01-01" } }, "2026-11-06T08:00:00Z"));
  assert.deepEqual(r.value.sections[0].items[0].adoption, { state: "uebernommen", rule: ad.rule, conditions: ad.conditions, decidedAt: "2026-11-06" });
  assert.deepEqual(wissenForCoach(r.value), [{ id: "kh-aufnahme", title: "KH-Aufnahme", topic: "Ernährung im Training", state: "uebernommen", rule: ad.rule, conditions: ad.conditions, decidedAt: "2026-11-06" }]);
  // Regeltext geändert: state bleibt, ruleChanged; erneute Entscheidung löscht es
  r = await saveWissensbasis(env, mk({ id: "kh-aufnahme", adoption: { ...ad, rule: "Neu." } }, "2026-11-07T08:00:00Z"));
  assert.equal(r.value.sections[0].items[0].adoption.state, "uebernommen");
  assert.equal(r.value.sections[0].items[0].adoption.ruleChanged, true);
  const re = applyWissenAdoption(r.value, "kh-aufnahme", { state: "offen" });
  assert.deepEqual(re.value.sections[0].items[0].adoption, { rule: "Neu.", conditions: ad.conditions, state: "offen" });
  // Validierung
  assert.match(validateWissensbasis(mk({ adoption: { rule: "x".repeat(201), conditions: "c" } })).error, /rule/);
  assert.match(validateWissensbasis(mk({ adoption: { rule: "r", conditions: "x".repeat(161) } })).error, /conditions/);
  assert.match(validateWissensbasis(mk({ adoption: { rule: "r" } })).error, /conditions/);
  // alter Stand ohne adoption lädt und speichert
  assert.ok(mergeAdoption(null, validateWissensbasis(good).value));
  console.log("wissensbasis adoption ok");
}

// ---- Prüfauftrag „Coach, prüf das“ (review) ----
{
  const { applyWissenReview, wissenAnfragen } = await import("../src/wissensbasis-snapshot.js");
  const kv = new Map();
  const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => { kv.set(k, v); } } };
  const mk = (extra = {}, at = "2026-11-05T08:00:00Z") => ({ ...good, fetchedAt: at, sections: [{ title: "Ernährung", items: [{ text: "Punkt A", evidence: "schwach", fit: "gering", ...extra }] }] });
  let r = await saveWissensbasis(env, mk());
  const { itemId } = await import("../src/wissensbasis-archiv.js");
  const pid = itemId(r.value.sections[0].items[0]);
  const ask = applyWissenReview(r.value, pid, { note: "stimmt das?" }, "2026-11-06T09:00:00Z");
  kv.set(WISSENSBASIS_KV_KEY, JSON.stringify(ask.value));
  assert.deepEqual(wissenAnfragen(ask.value).map((x) => [x.id, x.note]), [[pid, "stimmt das?"]]);
  assert.equal(applyWissenReview(ask.value, "nix", {}).status, 404);
  // ganzer Stand ohne review: Anfrage bleibt
  r = await saveWissensbasis(env, mk({}, "2026-11-06T10:00:00Z"));
  assert.equal(r.value.sections[0].items[0].review.state, "angefragt");
  // Coach antwortet, versucht requestedAt/state zu fälschen: bleibt, verdict gesetzt
  r = await saveWissensbasis(env, mk({ review: { verdict: "verworfen", reason: "Querschnitt", state: "angefragt", requestedAt: "2000-01-01", note: "x" } }, "2026-11-06T11:00:00Z"));
  assert.deepEqual(r.value.sections[0].items[0].review, { requestedAt: "2026-11-06T09:00:00Z", note: "stimmt das?", state: "geprueft", verdict: "verworfen", reason: "Querschnitt" });
  assert.deepEqual(wissenAnfragen(r.value), []);
  assert.equal(applyWissenReview(r.value, pid, {}).status, 409);
  // Rücknahme
  const w = applyWissenReview(ask.value, pid, { withdraw: true });
  assert.equal(w.value.sections[0].items[0].review, undefined);
  assert.match(validateWissensbasis(mk({ review: { verdict: "egal" } })).error, /verdict/);
  console.log("wissensbasis review ok");
}
