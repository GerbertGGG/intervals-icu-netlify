// Testet src/insight-snapshot.js (Erkenntnisse des Coaches). Ausführen: node test/insight.test.mjs
import assert from "node:assert/strict";
import { validateInsight, upsertInsight, archiveInsight, splitInsights, insightsForCoach } from "../src/insight-snapshot.js";

const base = { id: "trainingstage-vs-umfang", title: "Mehr Tage, nicht mehr Minuten", text: "Deine Pace bei Puls 130 wird in Wochen mit fünf Trainingstagen besser als in Wochen mit gleicher Zeit auf drei Tage verteilt.",
  action: "Lieber kurze Einheiten verteilen als eine lange anhängen.", strength: "schwach", status: "neu", discipline: "lauf", evidence: "107 Läufe, Jan 2025–Okt 2026",
  chart: { type: "line", unit: "s/km", points: [{ x: "KW 38", y: 415 }, { x: "KW 40", y: 409 }, { x: "KW 42", y: 401 }], refLine: 405, better: "lower" }, fetchedAt: "2026-10-20T06:30:00Z" };
const v = validateInsight(base).value;
assert.equal(v.fetchedAt, "2026-10-20T06:30:00.000Z"); assert.equal(v.chart.points.length, 3); assert.equal(v.action, base.action);
assert.equal(validateInsight({ ...base, status: "bestaetigt" }).value.status, "bestätigt");
assert.equal(validateInsight({ ...base, action: null, chart: null }).value.action, null);
assert.equal(validateInsight({ ...base, chart: JSON.stringify(base.chart) }).value.chart.type, "line");
for (const bad of [{ id: "Trainingstage vs Umfang" }, { id: "a--b" }, { id: "x".repeat(61) }, { title: "t".repeat(61) }, { text: "t".repeat(281) }, { text: "" },
  { action: "a".repeat(161) }, { strength: "sehr stark" }, { status: "offen" }, { discipline: "triathlon" }, { evidence: "e".repeat(201) }, { evidence: "" }, { fetchedAt: "gestern" },
  { chart: { ...base.chart, type: "pie" } }, { chart: { ...base.chart, points: [] } }, { chart: { ...base.chart, points: Array(31).fill({ x: "a", y: 1 }) } },
  { chart: { ...base.chart, points: [{ x: "a", y: "viel" }] } }, { chart: { ...base.chart, better: "mehr" } }, { chart: { ...base.chart, refLine: "hoch" } }])
  assert.ok(validateInsight({ ...base, ...bad }).error, JSON.stringify(bad));

// Upsert: createdAt bleibt, updatedAt neu, history nur bei Aenderung von status oder strength
let st = upsertInsight(null, v);
assert.equal(st.items[0].createdAt, "2026-10-20T06:30:00.000Z"); assert.deepEqual(st.items[0].history, [{ date: "2026-10-20", at: "2026-10-20T06:30:00.000Z", status: "neu", strength: "schwach" }]);
st = upsertInsight(st, validateInsight({ ...base, text: "Neu formuliert.", fetchedAt: "2026-10-21T06:30:00Z" }).value);
assert.equal(st.items.length, 1); assert.equal(st.items[0].text, "Neu formuliert."); assert.equal(st.items[0].history.length, 1);
assert.equal(st.items[0].createdAt, "2026-10-20T06:30:00.000Z"); assert.equal(st.items[0].updatedAt, "2026-10-21T06:30:00.000Z");
st = upsertInsight(st, validateInsight({ ...base, status: "bestätigt", strength: "mittel", fetchedAt: "2026-10-22T06:30:00Z" }).value);
assert.deepEqual(st.items[0].history.map((h) => [h.date, h.status, h.strength]), [["2026-10-22", "bestätigt", "mittel"], ["2026-10-20", "neu", "schwach"]]);
for (let i = 0; i < 12; i++) st = upsertInsight(st, validateInsight({ ...base, strength: i % 2 ? "mittel" : "stark", fetchedAt: `2026-11-${String(i + 1).padStart(2, "0")}T06:00:00Z` }).value);
assert.equal(st.items[0].history.length, 10); // max. 10

const two = validateInsight({ ...base, id: "schlaf-und-hrv", title: "Schlaf unter 6 h drückt die HRV", discipline: "erholung", strength: "mittel", status: "neu", chart: { type: "bar", unit: "ms", points: [{ x: "< 6 h", y: 41 }, { x: "6–7 h", y: 47 }, { x: "> 7 h", y: 52 }], refLine: null, better: "higher" }, fetchedAt: "2026-11-20T06:00:00Z" }).value;
const three = validateInsight({ ...base, id: "schwimmen-am-stueck", title: "Am Stück wächst schneller als Tempo", discipline: "schwimmen", strength: "beobachtung", status: "widerlegt", chart: null, action: null, fetchedAt: "2026-11-15T06:00:00Z" }).value;
st = upsertInsight(upsertInsight(st, two), three);
assert.deepEqual(splitInsights(st).active.map((x) => x.id), ["schlaf-und-hrv", "schwimmen-am-stueck", "trainingstage-vs-umfang"]);

// Archivieren blendet aus, loescht nicht; erneutes Speichern holt zurueck
assert.ok(archiveInsight(st, "gibt-es-nicht").error);
st = archiveInsight(st, "schwimmen-am-stueck", "2026-11-21T06:00:00.000Z").value;
let sp = splitInsights(st);
assert.deepEqual(sp.active.map((x) => x.id), ["schlaf-und-hrv", "trainingstage-vs-umfang"]);
assert.deepEqual(sp.archived.map((x) => [x.id, x.archivedAt]), [["schwimmen-am-stueck", "2026-11-21T06:00:00.000Z"]]);
assert.equal(archiveInsight(st, "schwimmen-am-stueck").already, true);
const coach = insightsForCoach(st);
assert.deepEqual(Object.keys(coach[0]), ["id", "title", "discipline", "strength", "status", "updatedAt", "text"]);
assert.ok(coach.every((x) => x.text.length <= 140));
st = upsertInsight(st, validateInsight({ ...base, id: "schwimmen-am-stueck", discipline: "schwimmen", status: "abgeschwächt", fetchedAt: "2026-11-22T06:00:00Z" }).value);
sp = splitInsights(st);
assert.equal(sp.archived.length, 0); assert.equal(sp.active[0].id, "schwimmen-am-stueck"); assert.equal(sp.active[0].archivedAt, null);
console.log("insight tests ok");
