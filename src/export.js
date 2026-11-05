// Export fuer die Langzeitanalyse des Coaches (DOKUMENTATION.md, Teil B 13): eine Zeile pro Lauf und eine Zeile pro Tag.
// Der Worker liest die Zeilen NUR aus D1 (Binding EXPORT_DB). Gerechnet und geschrieben werden sie taeglich in GitHub Actions
// (scripts/export-sync.mjs, src/export-calc.js), weil die Auswertung der Zeitreihen die 10 ms CPU des kostenlosen Plans
// sprengt. Kein Abruf von Intervals, keine Auswertung, kein Cache hier.
// Zugaenge: MCP get_run_export / get_day_export / get_export_stats (mcp-server.js), HTTP /api/export/runs|days|rides|swims|stats (index.js).
import { json } from "./http-helpers.js";
import { isIsoDate, isoDate, isoDateBerlin } from "./date-utils.js";
import { isAuthorized } from "./dashboard.js";
import { METHOD_VERSION, RUN_COLUMNS, DAY_COLUMNS, RIDE_COLUMNS, SWIM_COLUMNS, decodeRow, isTextColumn, isBoolColumn } from "./export-schema.js";

export { METHOD_VERSION, RUN_COLUMNS, DAY_COLUMNS, RIDE_COLUMNS, SWIM_COLUMNS };
// Aktivitaets-Tabellen mit gleicher Abfrage (nach Startzeit, cursor "<startLocal>|<id>")
export const ACTIVITY_TABLES = { runs: RUN_COLUMNS, rides: RIDE_COLUMNS, swims: SWIM_COLUMNS };
export const DEFAULT_LIMIT = 200;
export const MAX_LIMIT = 500;

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const addDays = (iso, n) => isoDate(new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000));

function dbOf(env) {
  const db = env?.EXPORT_DB;
  if (!db || typeof db.prepare !== "function") throw new Error("D1-Datenbank EXPORT_DB ist nicht eingerichtet (wrangler.toml, DOKUMENTATION.md 13.4).");
  return db;
}
const all = async (db, sql, ...params) => (await db.prepare(sql).bind(...params).all())?.results ?? [];
const first = async (db, sql, ...params) => (await all(db, sql, ...params))[0] ?? null;

// Stand der letzten Synchronisation aus GitHub Actions (export_meta.lastSync)
async function lastSync(db) {
  const r = await first(db, `SELECT "value" FROM export_meta WHERE "key" = 'lastSync'`).catch(() => null);
  try { return r?.value ? JSON.parse(r.value) : null; } catch { return null; }
}

// Bereich und Seitengroesse. Standard: erster Tag in der Tabelle bis gestern (Berlin).
async function resolveArgs(db, table, args = {}) {
  for (const k of ["from", "to"]) if (args[k] != null && args[k] !== "" && !isIsoDate(args[k])) throw new Error(`${k} muss YYYY-MM-DD sein`);
  const yesterday = addDays(isoDateBerlin(), -1);
  const from = isIsoDate(args.from) ? args.from : (await first(db, `SELECT MIN("date") AS d FROM ${table}`))?.d ?? yesterday;
  const to = isIsoDate(args.to) ? args.to : yesterday;
  if (from > to) throw new Error(`from (${from}) liegt nach to (${to})`);
  const limit = Math.max(1, Math.min(MAX_LIMIT, Math.floor(num(args.limit) ?? DEFAULT_LIMIT)));
  return { from, to, limit, cursor: args.cursor ? String(args.cursor) : null };
}

const syncInfo = (s) => ({ syncedAt: s?.at ?? null, syncedFrom: s?.from ?? null, syncedTo: s?.to ?? null });

// Aktivitaeten (runs, rides, swims) nach Startzeit, cursor = "<startLocal>|<id>" der letzten Zeile
export const buildRunExport = (env, args = {}) => buildActivityExport(env, "runs", args);
export const buildRideExport = (env, args = {}) => buildActivityExport(env, "rides", args);
export const buildSwimExport = (env, args = {}) => buildActivityExport(env, "swims", args);
export async function buildActivityExport(env, table, args = {}) {
  const columns = ACTIVITY_TABLES[table];
  if (!columns) throw new Error(`unbekannte Tabelle ${table}`);
  const db = dbOf(env);
  const { from, to, limit, cursor } = await resolveArgs(db, table, args);
  const [cStart, ...cId] = cursor ? cursor.split("|") : [];
  const after = cursor ? ` AND ("startLocal" > ?3 OR ("startLocal" = ?3 AND "id" > ?4))` : "";
  const params = cursor ? [from, to, cStart, cId.join("|"), limit + 1] : [from, to, limit + 1];
  const lim = cursor ? "?5" : "?3";
  const rows = await all(db, `SELECT * FROM ${table} WHERE "date" >= ?1 AND "date" <= ?2${after} ORDER BY "startLocal", "id" LIMIT ${lim}`, ...params);
  const page = rows.slice(0, limit).map((r) => decodeRow(r, columns));
  const pendingCol = table === "swims" ? "lapsStatus" : "streamStatus";
  const stats = await first(db, `SELECT COUNT(*) AS n, SUM(CASE WHEN "${pendingCol}" = 'pending' THEN 1 ELSE 0 END) AS p FROM ${table} WHERE "date" >= ?1 AND "date" <= ?2`, from, to);
  const last = page[page.length - 1];
  return {
    methodVersion: METHOD_VERSION, from, to, total: stats?.n ?? 0, count: page.length, pending: stats?.p ?? 0, ...syncInfo(await lastSync(db)),
    nextCursor: rows.length > limit ? `${last.startLocal}|${last.id}` : null, rows: page,
  };
}

// Tage, cursor = Datum des naechsten Tages. Nur Tage, die D1 kennt (das Skript schreibt jeden Tag im Bereich).
export async function buildDayExport(env, args = {}) {
  const db = dbOf(env);
  const { from, to, limit, cursor } = await resolveArgs(db, "days", args);
  const start = isIsoDate(cursor) && cursor > from ? cursor : from;
  const rows = await all(db, `SELECT * FROM days WHERE "date" >= ?1 AND "date" <= ?2 ORDER BY "date" LIMIT ?3`, start, to, limit + 1);
  const page = rows.slice(0, limit).map((r) => decodeRow(r, DAY_COLUMNS));
  const stats = await first(db, `SELECT COUNT(*) AS n FROM days WHERE "date" >= ?1 AND "date" <= ?2`, from, to);
  return {
    methodVersion: METHOD_VERSION, from, to, total: stats?.n ?? 0, count: page.length, pending: 0, ...syncInfo(await lastSync(db)),
    nextCursor: rows.length > limit ? addDays(page[page.length - 1].date, 1) : null, rows: page,
  };
}

// ---------------------------------------------------------------------------------------------------------------------
// Zusammenfassung (get_export_stats): Kennzahlen je Woche, Monat, Quartal oder Jahr statt hunderter Zeilen
// ---------------------------------------------------------------------------------------------------------------------

export const STATS_TABLES = { runs: RUN_COLUMNS, rides: RIDE_COLUMNS, swims: SWIM_COLUMNS, days: DAY_COLUMNS };
export const STATS_GROUPS = ["none", "week", "month", "quarter", "year"];
export const STATS_MAX_METRICS = 8;
const STATS_MAX_ROWS = 20000;
// Filter nur auf Einordnungs-Spalten (keine Namen, Tags, Ids, Datumswerte)
const STATS_NO_FILTER = new Set(["id", "date", "startLocal", "endLocal", "name", "tags", "type", "calendar", "brickRideId"]);
const isNumericColumn = (c) => !isTextColumn(c) && !isBoolColumn(c) && c !== "methodVersion";
const isFilterColumn = (c) => !STATS_NO_FILTER.has(c) && (isTextColumn(c) || isBoolColumn(c));

// Gruppenschluessel aus dem Datum: Woche = Montag (ISO), Monat 2026-11, Quartal 2026-Q4, Jahr 2026
export function groupKeyOf(date, groupBy) {
  if (groupBy === "none") return "alle";
  if (groupBy === "year") return date.slice(0, 4);
  if (groupBy === "month") return date.slice(0, 7);
  if (groupBy === "quarter") return `${date.slice(0, 4)}-Q${Math.floor((Number(date.slice(5, 7)) - 1) / 3) + 1}`;
  const wd = new Date(date + "T00:00:00Z").getUTCDay(); // 0 = Sonntag
  return addDays(date, -((wd + 6) % 7));
}

const r2 = (v) => (v == null ? null : Math.round(v * 100) / 100);
export function describeValues(xs) {
  const v = xs.filter((x) => x != null && Number.isFinite(x)).sort((a, b) => a - b);
  const n = v.length;
  if (!n) return { n: 0, mean: null, median: null, min: null, max: null, sd: null };
  const mean = v.reduce((a, b) => a + b, 0) / n, m = n >> 1;
  const sd = n > 1 ? Math.sqrt(v.reduce((a, b) => a + (b - mean) ** 2, 0) / (n - 1)) : null;
  return { n, mean: r2(mean), median: r2(n % 2 ? v[m] : (v[m - 1] + v[m]) / 2), min: r2(v[0]), max: r2(v[n - 1]), sd: r2(sd) };
}

function statsArgs(args = {}) {
  const table = String(args.table ?? "");
  const columns = STATS_TABLES[table];
  if (!columns) throw new Error(`table muss eine von ${Object.keys(STATS_TABLES).join(", ")} sein`);
  const groupBy = args.groupBy == null || args.groupBy === "" ? "month" : String(args.groupBy);
  if (!STATS_GROUPS.includes(groupBy)) throw new Error(`groupBy muss eine von ${STATS_GROUPS.join(", ")} sein`);
  const metrics = Array.isArray(args.metrics) ? [...new Set(args.metrics.map(String))] : [];
  if (!metrics.length || metrics.length > STATS_MAX_METRICS) throw new Error(`metrics: 1 bis ${STATS_MAX_METRICS} Spalten`);
  for (const c of metrics) if (!columns.includes(c) || !isNumericColumn(c)) throw new Error(`metrics: ${c} ist keine Zahlenspalte von ${table}`);
  const where = [], params = [];
  const filter = args.filter && typeof args.filter === "object" && !Array.isArray(args.filter) ? args.filter : {};
  for (const [c, raw] of Object.entries(filter)) {
    if (!columns.includes(c) || !isFilterColumn(c)) throw new Error(`filter: ${c} ist nicht filterbar (nur Einordnungs-Spalten wie runClass, treadmill, trainer, testKind)`);
    const vals = (Array.isArray(raw) ? raw : [raw]).map((v) => (isBoolColumn(c) && v != null ? (v === true || v === 1 || v === "true" ? 1 : 0) : v));
    if (!vals.length || vals.length > 10) throw new Error(`filter: ${c} braucht 1 bis 10 Werte`);
    const nonNull = vals.filter((v) => v != null), parts = [];
    if (nonNull.length) { parts.push(`"${c}" IN (${nonNull.map(() => "?").join(", ")})`); params.push(...nonNull.map((v) => (isBoolColumn(c) ? v : String(v)))); }
    if (nonNull.length < vals.length) parts.push(`"${c}" IS NULL`);
    where.push(`(${parts.join(" OR ")})`);
  }
  for (const [key, op] of [["min", ">="], ["max", "<="]]) {
    const o = args[key] && typeof args[key] === "object" && !Array.isArray(args[key]) ? args[key] : {};
    for (const [c, v] of Object.entries(o)) {
      if (!columns.includes(c) || !isNumericColumn(c)) throw new Error(`${key}: ${c} ist keine Zahlenspalte von ${table}`);
      if (!Number.isFinite(Number(v))) throw new Error(`${key}: ${c} braucht eine Zahl`);
      where.push(`"${c}" ${op} ?`); params.push(Number(v));
    }
  }
  return { table, groupBy, metrics, filter, min: args.min ?? null, max: args.max ?? null, where, params };
}

// Liest nur Datum und die gewaehlten Spalten. n = Zeilen mit Wert (null zaehlt nicht), sd = Stichproben-Standardabweichung.
export async function buildExportStats(env, args = {}) {
  const a = statsArgs(args);
  const db = dbOf(env);
  const { from, to } = await resolveArgs(db, a.table, { from: args.from, to: args.to });
  const cols = a.metrics.map((c) => `"${c}"`).join(", ");
  const where = [`"date" >= ?`, `"date" <= ?`, ...a.where].join(" AND ");
  const rows = await all(db, `SELECT "date", ${cols} FROM ${a.table} WHERE ${where} ORDER BY "date" LIMIT ${STATS_MAX_ROWS + 1}`, from, to, ...a.params);
  if (rows.length > STATS_MAX_ROWS) throw new Error(`Zu viele Zeilen (über ${STATS_MAX_ROWS}); Zeitraum oder Filter enger fassen`);
  const groups = new Map();
  for (const r of rows) {
    const k = groupKeyOf(r.date, a.groupBy);
    const g = groups.get(k) ?? { key: k, from: r.date, to: r.date, rows: 0, values: Object.fromEntries(a.metrics.map((c) => [c, []])) };
    g.to = r.date; g.rows++;
    for (const c of a.metrics) if (r[c] != null) g.values[c].push(Number(r[c]));
    groups.set(k, g);
  }
  return {
    methodVersion: METHOD_VERSION, table: a.table, groupBy: a.groupBy, from, to, filter: a.filter, min: a.min, max: a.max, metrics: a.metrics,
    rows: rows.length, ...syncInfo(await lastSync(db)),
    groups: [...groups.values()].map((g) => ({ key: g.key, from: g.from, to: g.to, rows: g.rows, stats: Object.fromEntries(a.metrics.map((c) => [c, describeValues(g.values[c])])) })),
  };
}

// ---------------------------------------------------------------------------------------------------------------------
// Ausgabeformate und HTTP
// ---------------------------------------------------------------------------------------------------------------------

const cell = (v) => {
  if (v == null) return "";
  const s = Array.isArray(v) ? v.join("|") : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
export function toCsv(rows, columns) {
  return [columns.join(","), ...rows.map((r) => columns.map((c) => cell(r[c])).join(","))].join("\n") + "\n";
}
// table: Spaltennamen einmal, Zeilen als Arrays (kompakt fuer den Coach)
export function toTable(result, columns) {
  return { ...result, columns, rows: result.rows.map((r) => columns.map((c) => r[c] ?? null)) };
}

export async function handleExportRequest(req, env, kind) {
  if (req.method !== "GET") return json({ ok: false, error: "Nur GET erlaubt" }, 405, { allow: "GET" });
  if (!isAuthorized(req, env)) return json({ ok: false, error: "unauthorized" }, 401);
  const p = new URL(req.url).searchParams;
  if (kind === "stats") {
    // ?table=runs&metrics=paceBand120_128,decoup&groupBy=quarter&filter={"runClass":["base","long"]}&min={"movingMin":60}
    const obj = (k) => { const v = p.get(k); if (!v) return undefined; try { return JSON.parse(v); } catch { throw new Error(`${k} muss JSON sein`); } };
    try {
      return json(await buildExportStats(env, {
        table: p.get("table"), groupBy: p.get("groupBy"), from: p.get("from"), to: p.get("to"),
        metrics: (p.get("metrics") ?? "").split(",").map((x) => x.trim()).filter(Boolean), filter: obj("filter"), min: obj("min"), max: obj("max"),
      }));
    } catch (e) {
      return json({ ok: false, error: String(e?.message ?? e) }, 400);
    }
  }
  const args = { from: p.get("from"), to: p.get("to"), cursor: p.get("cursor"), limit: p.get("limit") };
  for (const k of ["from", "to"]) if (args[k] && !isIsoDate(args[k])) return json({ ok: false, error: `${k} muss YYYY-MM-DD sein` }, 400);
  let result;
  try {
    result = ACTIVITY_TABLES[kind] ? await buildActivityExport(env, kind, args) : await buildDayExport(env, args);
  } catch (e) {
    return json({ ok: false, error: String(e?.message ?? e) }, 502);
  }
  const columns = ACTIVITY_TABLES[kind] ?? DAY_COLUMNS;
  const format = p.get("format");
  if (format === "csv") {
    return new Response(toCsv(result.rows, columns), {
      headers: {
        "content-type": "text/csv; charset=utf-8",
        "x-next-cursor": result.nextCursor ?? "", "x-pending": String(result.pending), "x-method-version": String(METHOD_VERSION),
        "x-synced-at": result.syncedAt ?? "",
      },
    });
  }
  return json(format === "table" ? toTable(result, columns) : result);
}
