// Aufruf in GitHub Actions (.github/workflows/export-d1.yml): rechnet den Export und schreibt ihn nach D1.
//   node scripts/export-to-d1.mjs
// Umgebung (Repo-Secrets): INTERVALS_API_KEY, ATHLETE_ID, CLOUDFLARE_API_TOKEN (minimal "Account > D1 > Edit" fuer dieses
// Konto; mit "Workers KV Storage: Read" zusaetzlich das Yazio-Gewicht aus dem KV, sonst nur Intervals), CLOUDFLARE_ACCOUNT_ID. Optional: EXPORT_FROM, EXPORT_TO (YYYY-MM-DD), EXPORT_FULL=true (ganze Historie),
// EXPORT_DAYS (Standardbereich, 30), D1_DATABASE_ID (sonst aus wrangler.toml), EXPORT_DELAY_MS (Pause je Lauf, 150).
// Das Repo ist oeffentlich, die Logs auch: ausgegeben werden nur Bereich, Zaehler, HTTP-Status und Fehlercodes, nie
// Datenzeilen, Gesundheitswerte, Aktivitaets-IDs oder Antworttexte (safeError in export-sync.mjs).
import { readFileSync } from "node:fs";
import { isIsoDate, isoDate, isoDateBerlin } from "../src/date-utils.js";
import { syncExport, findFirstDay, safeError, safeErr } from "./export-sync.mjs";

const CF = "https://api.cloudflare.com/client/v4";
const addDays = (iso, n) => isoDate(new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000));

function need(name) {
  const v = process.env[name];
  if (!v) throw safeErr(`Umgebungsvariable ${name} fehlt (Repo-Secret anlegen, DOKUMENTATION.md 13.4)`);
  return v;
}

// IDs aus wrangler.toml: D1 (Binding EXPORT_DB, nur wenn der Block einkommentiert ist) und KV (Binding KV)
function tomlIds() {
  const toml = readFileSync(new URL("../wrangler.toml", import.meta.url), "utf8");
  const block = (header, binding) => toml.split(/^\[\[/m).find((b) => b.startsWith(header) && new RegExp(`^binding\\s*=\\s*"${binding}"`, "m").test(b)) ?? "";
  const field = (b, k) => b.match(new RegExp(`^${k}\\s*=\\s*"([^"]+)"`, "m"))?.[1] ?? null;
  return { d1: field(block("d1_databases]]", "EXPORT_DB"), "database_id"), kv: field(block("kv_namespaces]]", "KV"), "id") };
}

async function cfFetch(path, init = {}) {
  const r = await fetch(`${CF}${path}`, { ...init, headers: { Authorization: `Bearer ${need("CLOUDFLARE_API_TOKEN")}`, "Content-Type": "application/json", ...(init.headers ?? {}) } });
  return r;
}

function d1Client(account, db) {
  return async (sql, params = []) => {
    for (let attempt = 0; ; attempt++) {
      const r = await cfFetch(`/accounts/${account}/d1/database/${db}/query`, { method: "POST", body: JSON.stringify({ sql, params }) });
      const body = await r.json().catch(() => null);
      if (r.ok && body?.success) return body.result?.[0]?.results ?? [];
      if (attempt < 3 && (r.status === 429 || r.status >= 500)) { await new Promise((res) => setTimeout(res, 1000 * 2 ** attempt)); continue; }
      // Nur Status und Fehlercodes: die Meldungstexte von D1 koennen SQL mit Datenwerten enthalten
      const codes = (body?.errors ?? []).map((e) => e?.code).filter((c) => Number.isFinite(Number(c)));
      throw safeErr(`D1 HTTP ${r.status}${codes.length ? ` (Code ${codes.join(", ")})` : ""}`);
    }
  };
}

// KV-Ersatz fuer die wiederverwendeten Worker-Funktionen: "yazio:weight" liest aus dem echten KV (nur lesen), alles andere
// (Open-Meteo-Tage aus weather.js) bleibt im Speicher dieses Laufs.
function kvShim(account, ns) {
  const mem = new Map();
  return {
    async get(key) {
      if (key !== "yazio:weight") return mem.get(key) ?? null;
      if (!ns) return null;
      const r = await cfFetch(`/accounts/${account}/storage/kv/namespaces/${ns}/values/${encodeURIComponent(key)}`);
      if (r.status === 404) return null;
      if (!r.ok) { console.log(`KV ${key}: HTTP ${r.status} (Token ohne KV-Leserecht?), Gewicht nur aus Intervals`); return null; }
      return r.text();
    },
    async put(key, value) { if (key !== "yazio:weight") mem.set(key, value); },
  };
}

async function main() {
const account = need("CLOUDFLARE_ACCOUNT_ID");
const ids = tomlIds();
const dbId = process.env.D1_DATABASE_ID || ids.d1;
if (!dbId) throw safeErr("Keine D1-Datenbank: [[d1_databases]] mit binding EXPORT_DB in wrangler.toml einkommentieren (oder D1_DATABASE_ID setzen)");
const env = { INTERVALS_API_KEY: need("INTERVALS_API_KEY"), ATHLETE_ID: need("ATHLETE_ID"), KV: kvShim(account, ids.kv) };
const today = isoDateBerlin(), yesterday = addDays(today, -1);
const full = /^(1|true|yes)$/i.test(process.env.EXPORT_FULL ?? "");
const from = isIsoDate(process.env.EXPORT_FROM) ? process.env.EXPORT_FROM : full ? await findFirstDay(env, today) : addDays(today, -(Number(process.env.EXPORT_DAYS) || 30));
const to = isIsoDate(process.env.EXPORT_TO) ? process.env.EXPORT_TO : yesterday;
if (from > to) throw safeErr(`from ${from} liegt nach to ${to}`);

const t0 = Date.now();
console.log(`Export ${from} .. ${to}${full ? " (ganze Historie)" : ""}`);
const stats = await syncExport({ env, d1: d1Client(account, dbId), from, to, today, delayMs: Number(process.env.EXPORT_DELAY_MS ?? 150) });
console.log(JSON.stringify({ ...stats, seconds: Math.round((Date.now() - t0) / 1000) }));
if (process.env.GITHUB_STEP_SUMMARY) {
  const { appendFileSync } = await import("node:fs");
  appendFileSync(process.env.GITHUB_STEP_SUMMARY, `### Export nach D1\n\n| | |\n|---|---|\n${Object.entries(stats).map(([k, v]) => `| ${k} | ${v} |`).join("\n")}\n| Sekunden | ${Math.round((Date.now() - t0) / 1000)} |\n`);
}
}

// Abbruch ohne Stack und ohne Inhalt der Fehlermeldung (oeffentliches Log)
main().catch((e) => {
  console.error(`Export fehlgeschlagen: ${safeError(e)}`);
  process.exitCode = 1;
});
