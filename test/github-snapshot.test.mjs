// Testet src/github-snapshot.js mit gemocktem fetch. Ausführen: node test/github-snapshot.test.mjs
import assert from "node:assert/strict";
import { syncSnapshotsFromGithub } from "../src/github-snapshot.js";

const kv = new Map();
const env = { KV: { get: async (k) => kv.get(k) ?? null, put: async (k, v) => void kv.set(k, v) } };
const snap = (at) => ({ fetchedAt: at, vdot: 40, prognosis: [{ distanceKm: 5, seconds: 1500 }], races: [] });
let files = { "runalyze.json": JSON.stringify(snap("2026-10-01T05:00:00Z")) };
const urls = [];
globalThis.fetch = async (url) => {
  urls.push(String(url));
  const f = String(url).split("/").pop();
  return files[f] ? new Response(files[f]) : new Response("nope", { status: 404 });
};

let r = await syncSnapshotsFromGithub(env);
assert.deepEqual(r.map((x) => x.status), ["übernommen", "keine Datei", "keine Datei"]);
assert.ok(urls[0].endsWith("/GerbertGGG/intervals-icu-netlify/snapshots/runalyze.json"));
assert.equal(JSON.parse(kv.get("dashboard:runalyze")).vdot, 40);

// gleicher Stand: nichts überschreiben
assert.equal((await syncSnapshotsFromGithub(env))[0].status, "aktuell");

// älterer Stand überschreibt einen neueren PUT nicht
kv.set("dashboard:runalyze", JSON.stringify({ ...snap("2026-10-02T05:00:00Z"), vdot: 41 }));
assert.equal((await syncSnapshotsFromGithub(env))[0].status, "aktuell");
assert.equal(JSON.parse(kv.get("dashboard:runalyze")).vdot, 41);

// kaputte Datei: Fehler, KV unverändert, kein Throw
files["runalyze.json"] = "{kaputt";
files["studie.json"] = JSON.stringify({ fetchedAt: "2026-10-01T05:00:00Z", text: "x", source: "Autor 2026" });
const origErr = console.error; console.error = () => {};
r = await syncSnapshotsFromGithub(env);
console.error = origErr;
assert.deepEqual(r.map((x) => x.status), ["Fehler", "übernommen", "keine Datei"]);
assert.equal(JSON.parse(kv.get("dashboard:runalyze")).vdot, 41);
assert.equal(JSON.parse(kv.get("dashboard:studie")).source, "Autor 2026");

// coach.json ohne idea: die gespeicherte Idee der Woche bleibt stehen
kv.set("dashboard:coach", JSON.stringify({ fetchedAt: "2026-10-19T05:00:00.000Z", note: "Mo", idea: { text: "Idee", angle: null, sport: null, createdAt: "2026-10-19T05:00:00.000Z" }, ideas: [{ date: "2026-10-19", text: "Idee", angle: null, sport: null }] }));
files["coach.json"] = JSON.stringify({ fetchedAt: "2026-10-20T05:00:00Z", note: "Di" });
r = await syncSnapshotsFromGithub(env);
assert.equal(r[2].status, "übernommen");
const c = JSON.parse(kv.get("dashboard:coach"));
assert.equal(c.note, "Di"); assert.equal(c.idea.text, "Idee"); assert.equal(c.ideas.length, 1);
console.log("github-snapshot tests ok");
