export function mustEnv(env, key) {
  const v = env?.[key];
  if (!v) throw new Error(`Missing env: ${key}`);
  return String(v);
}

// Feature flag: Die manuelle Route /sync ist bewusst deaktiviert. Zum Aktivieren:
// INTERVALS_ENABLED=true als Worker-Var/Secret setzen. Unset/alles andere = deaktiviert.
// Der Cron-Sync der Yazio-Werte ist davon unabhängig.
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
