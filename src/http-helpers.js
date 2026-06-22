export function parseBooleanParam(searchParams, key) {
  return (searchParams.get(key) || "").toLowerCase() === "true";
}

export function getSearchParamAny(searchParams, keys) {
  for (const key of keys) {
    const direct = searchParams.get(key);
    if (direct) return direct;
  }

  const lowerMap = new Map();
  for (const [key, value] of searchParams.entries()) {
    if (!value) continue;
    const normalizedKey = String(key || "").toLowerCase();
    if (!lowerMap.has(normalizedKey)) lowerMap.set(normalizedKey, value);
  }

  for (const key of keys) {
    const value = lowerMap.get(String(key).toLowerCase());
    if (value) return value;
  }
  return "";
}

export function json(o, status = 200) {
  return new Response(JSON.stringify(o, null, 2), {
    status,
    headers: { "content-type": "application/json; charset=utf-8" },
  });
}

export function clampInt(x, min, max) {
  const n = Number(x);
  return Number.isFinite(n) ? Math.max(min, Math.min(max, Math.floor(n))) : min;
}
