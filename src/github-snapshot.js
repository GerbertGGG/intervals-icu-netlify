import { readKvJson, writeKvJson } from "./kv.js";
import { RUNALYZE_KV_KEY, validateSnapshot } from "./runalyze-snapshot.js";
import { STUDIE_KV_KEY, validateStudie } from "./studie-snapshot.js";

// Fallback fuer PUT /api/runalyze und /api/studie: Der Coaching-Task erreicht den Worker nicht immer
// (Egress-Policy), GitHub aber schon. Er committet die Snapshots deshalb als JSON auf den Branch
// "snapshots" (nicht main, damit kein Deploy ausgeloest wird), und der Cron holt sie von dort ins KV.
//
// Dateien auf dem Branch: runalyze.json, studie.json (gleicher Body wie bei den PUT-Endpunkten).
// Optionale Worker-Variablen: GITHUB_REPO ("owner/repo"), SNAPSHOT_BRANCH, GITHUB_TOKEN (nur bei privatem Repo).

const DEFAULT_REPO = "GerbertGGG/intervals-icu-netlify";
const DEFAULT_BRANCH = "snapshots";
const MAX_BYTES = 60_000;

const SOURCES = [
  { file: "runalyze.json", kvKey: RUNALYZE_KV_KEY, validate: validateSnapshot },
  { file: "studie.json", kvKey: STUDIE_KV_KEY, validate: validateStudie },
];

async function syncOne(env, { file, kvKey, validate }) {
  const repo = env?.GITHUB_REPO || DEFAULT_REPO;
  const branch = env?.SNAPSHOT_BRANCH || DEFAULT_BRANCH;
  const headers = { "user-agent": "intervals-icu-worker" };
  if (env?.GITHUB_TOKEN) headers.authorization = `Bearer ${env.GITHUB_TOKEN}`;
  const res = await fetch(`https://raw.githubusercontent.com/${repo}/${branch}/${file}`, { headers });
  if (res.status === 404) return { file, status: "keine Datei" };
  if (!res.ok) throw new Error(`${file}: GitHub antwortet ${res.status}`);
  const text = await res.text();
  if (text.length > MAX_BYTES) throw new Error(`${file}: Datei zu groß`);
  const { value, error } = validate(JSON.parse(text));
  if (error) throw new Error(`${file}: ${error}`);
  const current = await readKvJson(env, kvKey);
  // Nie einen neueren PUT-Snapshot mit einem aelteren Stand ueberschreiben.
  if (current?.fetchedAt && Date.parse(current.fetchedAt) >= Date.parse(value.fetchedAt)) return { file, status: "aktuell" };
  await writeKvJson(env, kvKey, value);
  return { file, status: "übernommen" };
}

export async function syncSnapshotsFromGithub(env) {
  const results = [];
  for (const src of SOURCES) {
    try {
      results.push(await syncOne(env, src));
    } catch (e) {
      console.error("github snapshot sync failed", { file: src.file, error: String(e?.message ?? e) });
      results.push({ file: src.file, status: "Fehler" });
    }
  }
  return results;
}
