export function mustEnv(env, key) {
  const v = env?.[key];
  if (!v) throw new Error(`Missing env: ${key}`);
  return String(v);
}

// Feature flag: Intervals.icu-Sync (/sync, /backfill-profile) und die davon
// abhängigen Auswertungen (Weekly Progress, Recovery Note, Formanalyse) sind
// bewusst deaktiviert. Zum Reaktivieren: INTERVALS_ENABLED=true als Worker-
// Var/Secret setzen. Unset/alles andere = deaktiviert.
export function isIntervalsEnabled(env) {
  const raw = env?.INTERVALS_ENABLED;
  return String(raw ?? "").toLowerCase() === "true";
}

export function hasKv(env) {
  return Boolean(
    env?.KV &&
    typeof env.KV.get === "function" &&
    typeof env.KV.put === "function",
  );
}

export async function readKvJson(env, key) {
  if (!hasKv(env)) return null;
  const raw = await env.KV.get(key);
  if (!raw) return null;
  try {
    return JSON.parse(raw);
  } catch {
    return null;
  }
}

export async function writeKvJson(env, key, value) {
  if (!hasKv(env)) return;
  await env.KV.put(key, JSON.stringify(value));
}
