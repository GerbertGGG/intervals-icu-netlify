// Rechnet den Export (src/export-calc.js) und schreibt ihn nach D1. Laeuft in GitHub Actions (scripts/export-to-d1.mjs,
// .github/workflows/export-d1.yml), nicht im Worker. Wiederverwendet die Funktionen des Workers; hier stehen nur Abruf,
// Abgleich mit D1 und Schreiben.
//
// d1(sql, params) -> Ergebniszeilen (Array von Objekten). In Actions die Cloudflare-REST-API, im Test node:sqlite.
// Schreibt nur neue oder geaenderte Zeilen (Hash je Zeile) und loescht Laeufe, die es in Intervals im Bereich nicht mehr gibt.
import { createHash } from "node:crypto";
import { activityDay, isRun, isTreadmill } from "../src/activity-utils.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, fetchIntervalsWellnessRange } from "../src/intervals-client.js";
import { isoDate } from "../src/date-utils.js";
import { readWeights } from "../src/ernaehrung.js";
import { runRow, computeRunEntry, entryFromRow, streamSkipOf, buildDayRows, raceContext } from "../src/export-calc.js";
import { METHOD_VERSION, TABLES, schemaStatements, missingColumnStatements, encodeRow, decodeRow } from "../src/export-schema.js";

const addDays = (iso, n) => isoDate(new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000));
const daysBetween = (a, b) => Math.round((Date.parse(b + "T00:00:00Z") - Date.parse(a + "T00:00:00Z")) / 86400000);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
export const WEATHER_RETRY_DAYS = 14; // Open-Meteo-Archiv hinkt hinterher: Laeufe ohne Wetter so lange neu rechnen
const SLICE_DAYS = 180; // Intervals-Abrufe in Scheiben, damit eine Antwort nicht beliebig gross wird
const ROWS_PER_STATEMENT = 40; // Zeilen je INSERT (D1: hoechstens 100 KB je SQL-Anweisung)

// Fehlermeldung fuers Log ohne Inhalt: Das Repo und damit die Action-Logs sind oeffentlich. Fehler aus intervals-client.js
// tragen den Antworttext der API ("activity streams 500: <Body>"), D1-Fehler koennen SQL mit Datenwerten enthalten. Erlaubt
// sind nur eigene Meldungen ohne Daten (err.safe) und "<Quelle> HTTP <Status>"; sonst nur der Fehlertyp.
export function safeError(e) {
  const m = String(e?.message ?? "");
  if (e?.safe === true) return m;
  const st = m.match(/^([A-Za-z][A-Za-z ]{0,40}?) (\d{3})\b/);
  return st ? `${st[1]} HTTP ${st[2]}` : String(e?.name || "Error");
}
export const safeErr = (message) => Object.assign(new Error(message), { safe: true });

// SQL-Literal (Upsert mit Literalen statt Parametern: D1 erlaubt nur 100 gebundene Parameter je Anweisung)
export function sqlLiteral(v) {
  if (v == null) return "NULL";
  if (typeof v === "number") return Number.isFinite(v) ? String(v) : "NULL";
  if (typeof v === "boolean") return v ? "1" : "0";
  return `'${String(v).replace(/\0/g, "").replace(/'/g, "''")}'`;
}
const q = (c) => `"${c}"`;
export const rowHash = (values) => createHash("sha1").update(JSON.stringify(values)).digest("hex");

export async function ensureSchema(d1) {
  for (const sql of schemaStatements()) await d1(sql);
  for (const name of Object.keys(TABLES)) {
    const cols = (await d1(`PRAGMA table_info(${name})`)).map((r) => r.name);
    for (const sql of missingColumnStatements(name, cols)) await d1(sql);
  }
}

async function upsert(d1, table, rows, syncedAt) {
  const { key, columns } = TABLES[table];
  const all = [...columns, "hash", "syncedAt"];
  const set = all.filter((c) => c !== key).map((c) => `${q(c)} = excluded.${q(c)}`).join(", ");
  for (let i = 0; i < rows.length; i += ROWS_PER_STATEMENT) {
    const values = rows.slice(i, i + ROWS_PER_STATEMENT).map(({ values, hash }) => `(${[...values, hash, syncedAt].map(sqlLiteral).join(", ")})`);
    await d1(`INSERT INTO ${table} (${all.map(q).join(", ")}) VALUES ${values.join(", ")} ON CONFLICT(${q(key)}) DO UPDATE SET ${set} WHERE ${table}."hash" IS NOT excluded."hash"`);
  }
}

// Intervals in Scheiben abrufen und zusammenfuegen
async function sliced(fetcher, from, to) {
  const out = [];
  for (let d = from; d <= to; d = addDays(d, SLICE_DAYS)) {
    const end = addDays(d, SLICE_DAYS - 1) < to ? addDays(d, SLICE_DAYS - 1) : to;
    const part = await fetcher(d, end);
    if (Array.isArray(part)) out.push(...part);
  }
  return out;
}

// Erster Tag mit Aktivitaet in Intervals: rueckwaerts in Jahresschritten, bis zwei Jahre in Folge leer sind
export async function findFirstDay(env, today) {
  let earliest = null, empty = 0;
  for (let end = today; end >= "2000-01-01" && empty < 2; end = addDays(end, -365)) {
    const days = ((await fetchIntervalsActivities(env, addDays(end, -364), end)) ?? []).map(activityDay).filter(Boolean).sort();
    if (days.length) { earliest = days[0]; empty = 0; } else empty++;
  }
  return earliest ?? today;
}

// Ob eine gespeicherte Zeile die Zeitreihen-Rechnung wiederverwenden darf: gleiche Methode, fertig gerechnet, gleicher
// Uebersprung-Grund (Name/Tags koennen ihn aendern), und kein fehlendes Wetter in den ersten Tagen
function reusable(prev, a, today, ctx) {
  if (!prev || prev.methodVersion !== METHOD_VERSION || prev.streamStatus === "pending") return false;
  // "gps" setzt erst die Rechnung (Schutzregel), streamSkipOf kennt es nicht: zaehlt wie "kein Uebersprung"
  const prevSkip = prev.streamSkip === "gps" ? null : prev.streamSkip ?? null;
  if (prevSkip !== streamSkipOf(a, ctx)) return false;
  if (!prev.weatherSrc && !isTreadmill(a) && daysBetween(activityDay(a), today) <= WEATHER_RETRY_DAYS) return false;
  return true;
}

// Haupteinstieg. env: { INTERVALS_API_KEY, ATHLETE_ID, KV? } (KV: Yazio-Gewicht und Zwischenspeicher fuers Wetter).
// from/to: Bereich (Laeufe und Tage); delayMs: Pause zwischen zwei gerechneten Laeufen (Rate-Limits Intervals/Open-Meteo).
export async function syncExport({ env, d1, from, to, today, delayMs = 0, log = console.log, now = () => new Date().toISOString() }) {
  await ensureSchema(d1);
  const syncedAt = now();
  const stats = { from, to, runs: 0, computed: 0, reused: 0, pending: 0, failed: 0, runsWritten: 0, runsDeleted: 0, days: 0, daysWritten: 0 };

  // Kalender zuerst: Renntage fuer die Laeufe (raceContext), Krankheit fuer die Tage (ab 60 Tage vor dem Bereich)
  const events = await sliced((a, b) => fetchIntervalsEvents(env, a, b), addDays(from, -60), to);
  const ctx = raceContext(events);

  // Laeufe
  const acts = await sliced((a, b) => fetchIntervalsActivities(env, a, b), from, to);
  const runs = acts.filter((a) => isRun(a) && a?.id != null && activityDay(a) >= from && activityDay(a) <= to);
  const prevRows = new Map((await d1(`SELECT * FROM runs WHERE "date" >= ?1 AND "date" <= ?2`, [from, to])).map((r) => [String(r.id), r]));
  const changed = [];
  for (const a of runs) {
    const prev = prevRows.get(String(a.id));
    const prevDecoded = prev ? { ...decodeRow(prev, TABLES.runs.columns), hash: prev.hash } : null;
    let entry = null;
    if (reusable(prevDecoded, a, today, ctx)) { entry = entryFromRow(prevDecoded); stats.reused++; }
    else {
      try {
        entry = await computeRunEntry(env, a, ctx);
        stats.computed++;
      } catch (e) {
        stats.failed = (stats.failed ?? 0) + 1;
        log(`Lauf nicht abrufbar (${safeError(e)}), bleibt pending`); // ohne ID und ohne Antworttext (oeffentliches Log)
      }
      if (delayMs) await sleep(delayMs);
    }
    const row = runRow(a, entry, ctx);
    if (row.streamStatus === "pending") stats.pending++;
    const values = encodeRow(row, TABLES.runs.columns);
    const hash = rowHash(values);
    if (prev?.hash !== hash) changed.push({ values, hash });
  }
  stats.runs = runs.length;
  await upsert(d1, "runs", changed, syncedAt);
  stats.runsWritten = changed.length;
  const ids = new Set(runs.map((a) => String(a.id)));
  const gone = [...prevRows.keys()].filter((id) => !ids.has(id));
  for (let i = 0; i < gone.length; i += 100) await d1(`DELETE FROM runs WHERE "id" IN (${gone.slice(i, i + 100).map(sqlLiteral).join(", ")})`);
  stats.runsDeleted = gone.length;

  // Tage
  const wellness = await sliced((a, b) => fetchIntervalsWellnessRange(env, a, b), from, to);
  const weights = await readWeights(env).catch(() => []);
  const dayList = [];
  for (let d = from; d <= to; d = addDays(d, 1)) dayList.push(d);
  const prevDays = new Map((await d1(`SELECT * FROM days WHERE "date" >= ?1 AND "date" <= ?2`, [from, to])).map((r) => [r.date, r]));
  const dayChanged = [];
  for (const row of buildDayRows(dayList, { wellness, activities: acts, events, weights })) {
    const prev = prevDays.get(row.date);
    // Yazio-Gewicht liegt im KV nur 90 Tage: ein frueher gespeicherter Wert bleibt, wenn heute keiner mehr kommt
    if (row.weightKg == null && prev?.weightKg != null) row.weightKg = prev.weightKg;
    const values = encodeRow(row, TABLES.days.columns);
    const hash = rowHash(values);
    if (prev?.hash !== hash) dayChanged.push({ values, hash });
  }
  stats.days = dayList.length;
  await upsert(d1, "days", dayChanged, syncedAt);
  stats.daysWritten = dayChanged.length;

  await d1(`INSERT INTO export_meta ("key", "value") VALUES ('lastSync', ?1) ON CONFLICT("key") DO UPDATE SET "value" = excluded."value"`,
    [JSON.stringify({ at: syncedAt, from, to, methodVersion: METHOD_VERSION, runs: stats.runs, pending: stats.pending })]);
  return stats;
}
