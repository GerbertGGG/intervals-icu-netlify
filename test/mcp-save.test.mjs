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
// Experiment und Profil: eigene Schreib-Tools, get_coach liefert beides mit (null, solange nichts gespeichert ist)
assert.ok(["save_experiment", "save_profil"].every((n) => list.body.result.tools.some((t) => t.name === n)));
assert.equal(gc.experiment, null); assert.equal(gc.profil, null);
assert.equal(gc.week_totals, null); // ohne Intervals-Zugang keine Wochensummen, get_coach klappt trotzdem
const experiment = { fetchedAt: "2026-10-26T06:30:00Z", title: "Trainingstage", hypothesis: "Fünf statt drei Trainingstage pro Woche verbessern die aerobe Basis in allen drei Sportarten.",
  status: "läuft", startDate: "2026-10-19", minWeeks: 6, targetDaysPerWeek: 5,
  metrics: [
    { key: "lauf_hf130", label: "Lauf bei Puls 130", unit: "s/km", baseline: 412, baselineDate: "2026-10-15", current: 401, currentDate: "2026-10-25", better: "lower" },
    { key: "rad_hf165w", label: "Ø-Puls bei 165 W", unit: "bpm", baseline: 138, baselineDate: "2026-10-14", current: null, currentDate: null, better: "lower" },
    { key: "schwimm_amstueck", label: "Am Stück", unit: "m", baseline: 400, baselineDate: "2026-10-16", current: 500, currentDate: "2026-10-23", better: "higher" },
    { key: "schwimm_100m", label: "100 m", unit: "s/100 m", baseline: 135, baselineDate: "2026-10-16", current: 138, currentDate: "2026-10-23", better: "lower" },
  ], verdict: null };
r = await rpc("tools/call", { name: "save_experiment", arguments: experiment });
assert.ok(!r.body.result.isError, r.body.result.content[0].text);
r = await rpc("tools/call", { name: "save_experiment", arguments: { ...experiment, hypothesis: "h".repeat(201) } });
assert.equal(r.body.result.isError, true); assert.match(r.body.result.content[0].text, /hypothesis länger als 200/);
r = await rpc("tools/call", { name: "save_experiment", arguments: { ...experiment, metrics: [{ key: "lauf_vo2", label: "x", unit: "x", better: "lower" }] } });
assert.equal(r.body.result.isError, true);
r = await rpc("tools/call", { name: "save_profil", arguments: { fetchedAt: "2026-10-26T06:30:00Z", updatedAt: "2026-10-26", highlights: [{ text: "Lange lockere Läufe senken die Pace bei Puls 130.", strength: "mittel" }, { text: "Krafttraining hilft der Hüfte.", strength: "schwach" }] } });
assert.ok(!r.body.result.isError);
r = await rpc("tools/call", { name: "save_profil", arguments: { fetchedAt: "2026-10-26T06:30:00Z", updatedAt: "2026-10-26", highlights: [{ text: "t".repeat(141), strength: "mittel" }] } });
assert.equal(r.body.result.isError, true);
gc = JSON.parse((await rpc("tools/call", { name: "get_coach", arguments: {} })).body.result.content[0].text);
assert.equal(gc.note, "Dienstag."); // Coach-Felder unverändert
assert.deepEqual(gc.experiment, { ...experiment, fetchedAt: "2026-10-26T06:30:00.000Z" });
assert.equal(gc.profil.highlights.length, 2); assert.equal(gc.profil.updatedAt, "2026-10-26");
// Ueberschreiben statt mergen
r = await rpc("tools/call", { name: "save_experiment", arguments: { ...experiment, fetchedAt: "2026-12-01T06:30:00Z", status: "ausgewertet", verdict: "stützt die Hypothese" } });
assert.ok(!r.body.result.isError);
assert.equal(JSON.parse(kv.get("dashboard:experiment")).verdict, "stützt die Hypothese");
// Erkenntnisse: save_insight (Upsert), archive_insight, get_coach.insights
assert.ok(["save_insight", "archive_insight"].every((n) => list.body.result.tools.some((t) => t.name === n)));
assert.deepEqual(gc.insights, []);
const ins = { id: "trainingstage-vs-umfang", title: "Mehr Tage, nicht mehr Minuten", text: "Fünf kurze Tage bringen dir mehr als drei lange.", action: null, strength: "schwach", status: "neu", discipline: "lauf", evidence: "107 Läufe", chart: null, fetchedAt: "2026-12-01T06:30:00Z" };
r = await rpc("tools/call", { name: "save_insight", arguments: ins });
assert.ok(!r.body.result.isError, r.body.result.content[0].text);
r = await rpc("tools/call", { name: "save_insight", arguments: { ...ins, status: "bestätigt", fetchedAt: "2026-12-08T06:30:00Z" } });
assert.equal(JSON.parse(r.body.result.content[0].text).history.length, 2);
r = await rpc("tools/call", { name: "save_insight", arguments: { ...ins, title: "t".repeat(61) } });
assert.equal(r.body.result.isError, true); assert.match(r.body.result.content[0].text, /title länger als 60/);
gc = JSON.parse((await rpc("tools/call", { name: "get_coach", arguments: {} })).body.result.content[0].text);
assert.deepEqual(gc.insights.map((x) => [x.id, x.status]), [["trainingstage-vs-umfang", "bestätigt"]]);
assert.ok(gc.experiment); // bestehende Felder bleiben
r = await rpc("tools/call", { name: "archive_insight", arguments: { id: "trainingstage-vs-umfang" } });
assert.ok(!r.body.result.isError);
assert.equal(JSON.parse(kv.get("dashboard:insights")).items.length, 1); // nicht geloescht
gc = JSON.parse((await rpc("tools/call", { name: "get_coach", arguments: {} })).body.result.content[0].text);
assert.deepEqual(gc.insights, []);
r = await rpc("tools/call", { name: "archive_insight", arguments: { id: "gibt-es-nicht" } });
assert.equal(r.body.result.isError, true);
// Wochenziele: save_targets hängt an (überschreibt nicht), get_coach liefert targets mit Historie (max. 10, neueste zuerst)
assert.ok(list.body.result.tools.some((t) => t.name === "save_targets"));
gc = JSON.parse((await rpc("tools/call", { name: "get_coach", arguments: {} })).body.result.content[0].text);
assert.deepEqual([gc.targets.current.enduranceHoursPerWeek, gc.targets.current.default, gc.targets.history.length], [5, true, 1]); // Startwert
const tgt = { fetchedAt: "2026-10-23T06:00:00Z", enduranceHoursPerWeek: 5.5, strengthSets2w: 90, reason: "6 Wochen ohne gelbe Signale", validFrom: "2099-11-02" };
r = await rpc("tools/call", { name: "save_targets", arguments: tgt });
assert.ok(!r.body.result.isError, r.body.result.content[0].text);
r = await rpc("tools/call", { name: "save_targets", arguments: { ...tgt, enduranceHoursPerWeek: 5.3 } });
assert.equal(r.body.result.isError, true); assert.match(r.body.result.content[0].text, /Schritten von 0,25/);
for (let i = 0; i < 11; i++) await rpc("tools/call", { name: "save_targets", arguments: { ...tgt, reason: `r${i}`, validFrom: "2026-10-19", enduranceHoursPerWeek: 5 } });
assert.equal(JSON.parse(kv.get("dashboard:targets")).items.length, 12); // nichts überschrieben
gc = JSON.parse((await rpc("tools/call", { name: "get_coach", arguments: {} })).body.result.content[0].text);
assert.equal(gc.targets.history.length, 10); assert.equal(gc.targets.history[0].reason, "r10");
assert.deepEqual([gc.targets.current.reason, gc.targets.upcoming.length], ["r10", 0]); // neuester Eintrag mit validFrom <= heute
console.log = log;
console.log("mcp-save tests ok");
