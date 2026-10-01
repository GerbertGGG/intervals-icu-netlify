// MCP (Model Context Protocol) endpoint: exposes read-only Intervals.icu data
// (planned workouts, past activities, wellness) and the Yazio nutrition diary as
// tools so a Claude chat can be added as a custom connector against this Worker,
// reusing the existing intervals-client.js / yazio-client.js fetchers instead of
// second integrations. Die einzigen schreibenden Tools sind save_runalyze_snapshot und
// save_studie: Sie legen das Dashboard-Snapshot im KV ab (Weg fuer Sitzungen, die weder den
// Worker per HTTP noch GitHub erreichen, aber diesen Connector nutzen).
import { json } from "./http-helpers.js";
import { isIsoDate, isoDate, listIsoDaysInclusive } from "./date-utils.js";
import { isValidAccessToken } from "./mcp-oauth.js";
import { fetchIntervalsEvents, fetchIntervalsActivities, fetchIntervalsWellnessRange } from "./intervals-client.js";
import { hasKv, writeKvJson } from "./kv.js";
import { RUNALYZE_KV_KEY, validateSnapshot } from "./runalyze-snapshot.js";
import { STUDIE_KV_KEY, validateStudie } from "./studie-snapshot.js";
import { hasYazioCredentials, fetchYazioDailyNutrition, fetchYazioDailyGoalKcal } from "./yazio-client.js";

// get_nutrition fetches one Yazio API round-trip per day in the range (Yazio has
// no range endpoint), sequentially to stay gentle on Yazio's rate limiting - so the
// range is capped to keep a single MCP call within the Worker's execution time.
const MAX_NUTRITION_DAYS = 31;

const PROTOCOL_VERSION = "2025-06-18";

const TOOLS = [
  {
    name: "get_planned_workouts",
    description: "Geplante Workouts/Events aus dem Intervals.icu-Trainingskalender für einen Datumsbereich.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute." },
        newest: { type: "string", description: "Enddatum ISO YYYY-MM-DD, Standard: heute + 14 Tage." },
      },
    },
  },
  {
    name: "get_recent_activities",
    description: "Abgeschlossene Trainingsaktivitäten aus Intervals.icu für einen Datumsbereich.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute - 14 Tage." },
        newest: { type: "string", description: "Enddatum ISO YYYY-MM-DD, Standard: heute." },
      },
    },
  },
  {
    name: "get_wellness",
    description: "Tägliche Wellness-Daten (HRV, Ruhepuls, Schlaf, CTL/ATL/Form) aus Intervals.icu für einen Datumsbereich.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute - 14 Tage." },
        newest: { type: "string", description: "Enddatum ISO YYYY-MM-DD, Standard: heute." },
      },
    },
  },
  {
    name: "get_nutrition",
    description:
      "Ernährungstagebuch aus Yazio für einen Datumsbereich, Tag für Tag: Tagessummen (Kalorien, Protein, Fett, Kohlenhydrate, Kalorienziel) sowie die einzelnen gegessenen Gerichte/Lebensmittel mit Name, Mahlzeit und Nährwerten.",
    inputSchema: {
      type: "object",
      properties: {
        oldest: { type: "string", description: "Startdatum ISO YYYY-MM-DD, Standard: heute - 14 Tage." },
        newest: { type: "string", description: `Enddatum ISO YYYY-MM-DD, Standard: heute. Bereich max. ${MAX_NUTRITION_DAYS} Tage.` },
      },
    },
  },
  {
    name: "save_runalyze_snapshot",
    description:
      "Schreibt den Runalyze-Snapshot (VDOT, Prognose, Rennen) fürs Dashboard. fetchedAt bei jedem Aufruf auf die aktuelle Zeit setzen. Nur Werte aus Runalyze übergeben, nichts erfinden.",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        vdot: { type: "number", description: "effectiveVO2max aus Runalyze get_calculations (optional)." },
        prognosis: {
          type: "array",
          description: "Aus get_prognosis: distance -> distanceKm, prognosis_seconds -> seconds.",
          items: { type: "object", properties: { distanceKm: { type: "number" }, seconds: { type: "number" } } },
        },
        races: {
          type: "array",
          description: "Aus get_historical_races: date, name, official_distance -> officialDistanceKm, official_time -> officialTimeSec.",
          items: {
            type: "object",
            properties: { date: { type: "string" }, name: { type: "string" }, officialDistanceKm: { type: "number" }, officialTimeSec: { type: "number" } },
          },
        },
      },
      required: ["fetchedAt"],
    },
  },
  {
    name: "save_studie",
    description: "Schreibt den Studien-Check der Woche fürs Dashboard (nur mit belastbarer Quelle).",
    inputSchema: {
      type: "object",
      properties: {
        fetchedAt: { type: "string", description: "Aktueller Zeitpunkt, ISO-8601." },
        text: { type: "string", description: "Text des Abschnitts, max. 6000 Zeichen." },
        source: { type: "string", description: "Quelle (Autor/Journal/Jahr)." },
        sourceUrl: { type: "string", description: "https-Link zur Quelle (optional)." },
        week: { type: "string", description: "Datum YYYY-MM-DD (optional)." },
        title: { type: "string", description: "Titel (optional)." },
      },
      required: ["fetchedAt", "text", "source"],
    },
  },
];

function resolveRange(args, { pastDefaultDays, futureDefaultDays }) {
  const oldest = isIsoDate(args?.oldest) ? args.oldest : isoDate(new Date(Date.now() - pastDefaultDays * 86400000));
  const newest = isIsoDate(args?.newest) ? args.newest : isoDate(new Date(Date.now() + futureDefaultDays * 86400000));
  return { oldest, newest };
}

async function saveSnapshot(env, args, validate, kvKey) {
  if (!hasKv(env)) throw new Error("KV nicht verfügbar");
  const { value, error } = validate(args);
  if (error) throw new Error(error);
  await writeKvJson(env, kvKey, value);
  return { ok: true, fetchedAt: value.fetchedAt };
}

async function callTool(env, name, args) {
  if (name === "save_runalyze_snapshot") return saveSnapshot(env, args, validateSnapshot, RUNALYZE_KV_KEY);
  if (name === "save_studie") return saveSnapshot(env, args, validateStudie, STUDIE_KV_KEY);
  if (name === "get_planned_workouts") {
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 0, futureDefaultDays: 14 });
    return fetchIntervalsEvents(env, oldest, newest);
  }
  if (name === "get_recent_activities") {
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 14, futureDefaultDays: 0 });
    return fetchIntervalsActivities(env, oldest, newest);
  }
  if (name === "get_wellness") {
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 14, futureDefaultDays: 0 });
    return fetchIntervalsWellnessRange(env, oldest, newest);
  }
  if (name === "get_nutrition") {
    if (!hasYazioCredentials(env)) throw new Error("Yazio ist nicht konfiguriert (YAZIO_USERNAME/YAZIO_PASSWORD fehlen).");
    const { oldest, newest } = resolveRange(args, { pastDefaultDays: 14, futureDefaultDays: 0 });
    const days = listIsoDaysInclusive(oldest, newest);
    if (days.length > MAX_NUTRITION_DAYS) throw new Error(`Zeitraum zu groß: max. ${MAX_NUTRITION_DAYS} Tage.`);

    const results = [];
    for (const day of days) {
      const [nutrition, goalKcal] = await Promise.all([fetchYazioDailyNutrition(env, day), fetchYazioDailyGoalKcal(env, day)]);
      results.push({
        date: day,
        energyKcal: Math.round(nutrition.energyKcal),
        proteinG: Math.round(nutrition.proteinG),
        fatG: Math.round(nutrition.fatG),
        carbG: Math.round(nutrition.carbG),
        goalKcal,
        items: nutrition.items.map((i) => ({
          name: i.name,
          daytime: i.daytime,
          amountG: i.amountG ?? null,
          energyKcal: Math.round(i.energyKcal),
          proteinG: Math.round(i.proteinG * 10) / 10,
          fatG: Math.round(i.fatG * 10) / 10,
          carbG: Math.round(i.carbG * 10) / 10,
        })),
      });
    }
    return results;
  }
  throw new Error(`Unknown tool: ${name}`);
}

function rpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}

function rpcError(id, code, message) {
  return { jsonrpc: "2.0", id, error: { code, message } };
}

async function handleMcpRequestInner(req, url, env, info) {
  if (req.method === "GET") return new Response("MCP endpoint requires POST", { status: 405 });

  const authMatch = (req.headers.get("Authorization") || "").match(/^Bearer\s+(.+)$/i);
  const token = authMatch ? authMatch[1] : null;
  info.hasToken = Boolean(token);
  if (!(await isValidAccessToken(env, token))) {
    return json({ error: "unauthorized" }, 401, {
      "WWW-Authenticate": `Bearer resource_metadata="${url.origin}/.well-known/oauth-protected-resource"`,
    });
  }

  let body;
  try {
    body = await req.json();
  } catch {
    return json(rpcError(null, -32700, "Parse error"), 400);
  }

  const { id, method, params } = body || {};
  info.rpcMethod = method;

  if (method === "initialize") {
    return json(
      rpcResult(id, {
        protocolVersion: PROTOCOL_VERSION,
        capabilities: { tools: {} },
        serverInfo: { name: "intervals-icu-mcp", version: "1.0.0" },
      }),
    );
  }

  if (method === "notifications/initialized" || method === "ping") {
    return new Response(null, { status: 202 });
  }

  if (method === "tools/list") {
    return json(rpcResult(id, { tools: TOOLS }));
  }

  if (method === "tools/call") {
    const toolName = params?.name;
    const args = params?.arguments || {};
    try {
      const data = await callTool(env, toolName, args);
      return json(rpcResult(id, { content: [{ type: "text", text: JSON.stringify(data, null, 2) }] }));
    } catch (e) {
      return json(
        rpcResult(id, {
          content: [{ type: "text", text: `Error: ${String(e?.message ?? e)}` }],
          isError: true,
        }),
      );
    }
  }

  return json(rpcError(id, -32601, `Method not found: ${method}`));
}

// Diagnostic log line per MCP request (visible in Workers Logs): lets us see which
// RPC method the client sent and which HTTP status the Worker answered with, since
// the Cloudflare log list only shows "Ok" for the Worker outcome, not the status.
export async function handleMcpRequest(req, url, env) {
  const info = { httpMethod: req.method, hasToken: false, rpcMethod: null };
  let res;
  try {
    res = await handleMcpRequestInner(req, url, env, info);
    return res;
  } finally {
    console.log("mcp", JSON.stringify({ ...info, status: res?.status ?? "exception", protocol: req.headers.get("mcp-protocol-version"), ua: req.headers.get("user-agent") }));
  }
}
