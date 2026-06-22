export function mustEnv(env, key) {
  const v = env?.[key];
  if (!v) throw new Error(`Missing env: ${key}`);
  return String(v);
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
