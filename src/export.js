// Export fuer die Langzeitanalyse des Coaches (DOKUMENTATION.md, Teil B 13): eine Zeile pro Lauf und eine Zeile pro Tag.
// Der Worker liest die Zeilen NUR aus D1 (Binding EXPORT_DB). Gerechnet und geschrieben werden sie taeglich in GitHub Actions
// (scripts/export-sync.mjs, src/export-calc.js), weil die Auswertung der Zeitreihen die 10 ms CPU des kostenlosen Plans
// sprengt. Kein Abruf von Intervals, keine Auswertung, kein Cache hier.
// Zugaenge: MCP get_run_export / get_day_export (mcp-server.js), HTTP /api/export/runs|days (index.js).
import { json } from "./http-helpers.js";
import { isIsoDate, isoDate, isoDateBerlin } from "./date-utils.js";
import { isAuthorized } from "./dashboard.js";
import { METHOD_VERSION, RUN_COLUMNS, DAY_COLUMNS, decodeRow } from "./export-schema.js";

export { METHOD_VERSION, RUN_COLUMNS, DAY_COLUMNS };
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

// Laeufe nach Startzeit, cursor = "<startLocal>|<id>" der letzten Zeile
export async function buildRunExport(env, args = {}) {
  const db = dbOf(env);
  const { from, to, limit, cursor } = await resolveArgs(db, "runs", args);
  const [cStart, ...cId] = cursor ? cursor.split("|") : [];
  const after = cursor ? ` AND ("startLocal" > ?3 OR ("startLocal" = ?3 AND "id" > ?4))` : "";
  const params = cursor ? [from, to, cStart, cId.join("|"), limit + 1] : [from, to, limit + 1];
  const lim = cursor ? "?5" : "?3";
  const rows = await all(db, `SELECT * FROM runs WHERE "date" >= ?1 AND "date" <= ?2${after} ORDER BY "startLocal", "id" LIMIT ${lim}`, ...params);
  const page = rows.slice(0, limit).map((r) => decodeRow(r, RUN_COLUMNS));
  const stats = await first(db, `SELECT COUNT(*) AS n, SUM(CASE WHEN "streamStatus" = 'pending' THEN 1 ELSE 0 END) AS p FROM runs WHERE "date" >= ?1 AND "date" <= ?2`, from, to);
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
  const args = { from: p.get("from"), to: p.get("to"), cursor: p.get("cursor"), limit: p.get("limit") };
  for (const k of ["from", "to"]) if (args[k] && !isIsoDate(args[k])) return json({ ok: false, error: `${k} muss YYYY-MM-DD sein` }, 400);
  let result;
  try {
    result = kind === "runs" ? await buildRunExport(env, args) : await buildDayExport(env, args);
  } catch (e) {
    return json({ ok: false, error: String(e?.message ?? e) }, 502);
  }
  const columns = kind === "runs" ? RUN_COLUMNS : DAY_COLUMNS;
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
