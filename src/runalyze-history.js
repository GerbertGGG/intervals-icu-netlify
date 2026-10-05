import { readKvJson, writeKvJson } from "./kv.js";
import { predictRaceTimesFromVdot } from "./vdot.js";
import { readRunalyzeSnapshot } from "./runalyze-snapshot.js";

// Verlauf der Halbmarathon-Zeit: Das Dashboard kennt vom Runalyze-Snapshot nur den neuesten Stand.
// Fuer "wie sinkt meine Zeit von Woche zu Woche" wird je Tag ein Eintrag aus dem Snapshot festgehalten
// (VDOT-Rechnung nach Daniels und Runalyze-Prognose), ein neuer Snapshot am selben Tag ersetzt den alten.
export const RUNALYZE_HISTORY_KV_KEY = "dashboard:runalyze-history";
const MAX_ENTRIES = 400;
const HM_KM = 21.0975;

export function historyEntryFromSnapshot(snapshot) {
  if (!snapshot?.fetchedAt) return null;
  const date = String(snapshot.fetchedAt).slice(0, 10);
  const vdot = snapshot.vdot ?? null;
  const hmVdotSecs = vdot != null ? predictRaceTimesFromVdot(vdot)?.find((x) => x.key === "hm")?.seconds ?? null : null;
  const prog = (km) => {
    const v = (snapshot.prognosis ?? []).find((p) => Math.abs(p.distanceKm - km) / km <= 0.01)?.seconds;
    return v != null ? Math.round(v) : null;
  };
  const entry = { date, vdot, hmVdotSecs: hmVdotSecs != null ? Math.round(hmVdotSecs) : null, hmProgSecs: prog(HM_KM), p5Secs: prog(5), p10Secs: prog(10), marathonShape: snapshot.marathonShape ?? null };
  if (vdot == null && entry.hmProgSecs == null && entry.p5Secs == null && entry.p10Secs == null && entry.marathonShape == null) return null;
  return entry;
}

export function upsertHistory(history, entry) {
  const list = Array.isArray(history) ? history.filter((e) => e?.date) : [];
  if (!entry) return list;
  return [...list.filter((e) => e.date !== entry.date), entry].sort((a, b) => a.date.localeCompare(b.date)).slice(-MAX_ENTRIES);
}

export async function readRunalyzeHistory(env) {
  const h = await readKvJson(env, RUNALYZE_HISTORY_KV_KEY).catch(() => null);
  return Array.isArray(h) ? h : [];
}

// Vom Cron aufgerufen (nach dem Holen der Snapshots): schreibt nur, wenn sich etwas geaendert hat.
export async function recordRunalyzeHistory(env) {
  const snapshot = await readRunalyzeSnapshot(env);
  const entry = historyEntryFromSnapshot(snapshot);
  if (!entry) return false;
  const old = await readRunalyzeHistory(env);
  const next = upsertHistory(old, entry);
  if (JSON.stringify(next) === JSON.stringify(old)) return false;
  await writeKvJson(env, RUNALYZE_HISTORY_KV_KEY, next);
  return true;
}
