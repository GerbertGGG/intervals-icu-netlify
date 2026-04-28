export const WATCHFACE_PREFLIGHT_HEADERS = {
  "access-control-allow-origin": "*",
  "access-control-allow-methods": "GET, OPTIONS",
  "access-control-allow-headers": "content-type",
  "access-control-max-age": "86400",
};

export const WATCHFACE_JSON_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "cache-control": "no-store",
  "access-control-allow-origin": "*",
};

export const WATCHFACE_ERROR_HEADERS = {
  "content-type": "application/json; charset=utf-8",
  "access-control-allow-origin": "*",
};

const REPORT_VERBOSITY_VALUES = new Set(["coach", "diagnose", "debug"]);

export function parseBooleanParam(searchParams, key) {
  return (searchParams.get(key) || "").toLowerCase() === "true";
}

export function parseReportVerbosity(searchParams, { debug = false } = {}) {
  const raw = String(searchParams.get("verbosity") || "").trim().toLowerCase();
  if (REPORT_VERBOSITY_VALUES.has(raw)) return raw;
  return "coach";
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
