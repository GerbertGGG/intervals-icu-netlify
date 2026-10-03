import { isoDate } from "./date-utils.js";
import { handleSyncRequest, withWorkerErrorBoundary } from "./request-handlers.js";
import { syncRange } from "./sync.js";
import { handleAuthorizeRequest, handleTokenRequest, handleAuthServerMetadata, handleProtectedResourceMetadata } from "./mcp-oauth.js";
import { handleMcpRequest } from "./mcp-server.js";
import { handleDashboardRequest, isAuthorized } from "./dashboard.js";
import { handleRunalyzeSnapshotRequest } from "./runalyze-snapshot.js";
import { handleStudieRequest } from "./studie-snapshot.js";
import { syncSnapshotsFromGithub } from "./github-snapshot.js";
import { recordRunalyzeHistory } from "./runalyze-history.js";
import { handleWidgetRequest } from "./widget.js";

function getBerlinHourFromScheduledEvent(event) {
  const t = Number(event?.scheduledTime);
  if (!Number.isFinite(t)) return null;
  const hour = Number(
    new Intl.DateTimeFormat("en-GB", { hour: "2-digit", hour12: false, timeZone: "Europe/Berlin" }).format(new Date(t)),
  );
  return Number.isFinite(hour) ? hour : null;
}

// Yazio is synced on every cron tick (15 min) between 07:00 and 23:59 Berlin, so the
// diary can be followed over the day. The extra ":58" cron entry ("58 21-22 * * *")
// fires at :58 past both UTC 21 and 22 to cover the CEST/CET boundary; the one landing
// on 23:58 Berlin time is the end-of-day sync that captures the day's final totals
// (the 15-min grid itself stops at 23:45), so DST transitions don't need a cron swap.
function isYazioWindowBerlin(event) {
  const hour = getBerlinHourFromScheduledEvent(event);
  return Number.isFinite(hour) && hour >= 7 && hour <= 23;
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") return new Response("ok");

    if (url.pathname === "/sync") {
      return handleSyncRequest(url, env, ctx, { syncRange });
    }

    // Read-only Daten für public/dashboard (Token-geschützt, siehe src/dashboard.js).
    if (url.pathname === "/api/dashboard") {
      return withWorkerErrorBoundary(() => handleDashboardRequest(req, env));
    }

    // Runalyze-Snapshot (Prognose + Rennen) aus einer Sitzung mit Runalyze-MCP, siehe src/runalyze-snapshot.js.
    if (url.pathname === "/api/runalyze") {
      return withWorkerErrorBoundary(() => handleRunalyzeSnapshotRequest(req, env, isAuthorized));
    }

    // Kompakte Daten für das iOS-Widget (Scriptable), siehe src/widget.js.
    if (url.pathname === "/api/widget") {
      return withWorkerErrorBoundary(() => handleWidgetRequest(req, env));
    }

    // Studien-Check der Woche aus dem Coaching-Bericht, siehe src/studie-snapshot.js.
    if (url.pathname === "/api/studie") {
      return withWorkerErrorBoundary(() => handleStudieRequest(req, env, isAuthorized));
    }

    // MCP custom connector (see src/mcp-oauth.js, src/mcp-server.js): lets a Claude
    // chat read planned workouts / activities / wellness from Intervals.icu and the
    // nutrition diary from Yazio.
    if (url.pathname === "/.well-known/oauth-authorization-server") {
      return handleAuthServerMetadata(url);
    }

    if (url.pathname === "/.well-known/oauth-protected-resource") {
      return handleProtectedResourceMetadata(url);
    }

    if (url.pathname === "/authorize") {
      return withWorkerErrorBoundary(() => handleAuthorizeRequest(req, url, env));
    }

    if (url.pathname === "/token") {
      return withWorkerErrorBoundary(() => handleTokenRequest(req, env));
    }

    if (url.pathname === "/mcp") {
      return withWorkerErrorBoundary(() => handleMcpRequest(req, url, env));
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // Runalyze-/Studien-Snapshots, die der Coaching-Task per GitHub-Branch liefert (siehe github-snapshot.js).
    // Danach den Tageseintrag im Halbmarathon-Zeitverlauf festhalten (siehe runalyze-history.js).
    ctx.waitUntil(
      syncSnapshotsFromGithub(env)
        .then(() => recordRunalyzeHistory(env))
        .catch((e) => console.error("runalyze history failed", String(e?.message ?? e))),
    );

    if (isYazioWindowBerlin(event)) {
      const yazioDay = isoDate(new Date());
      // Morning ticks (07:xx) also re-sync yesterday: entries added after the 23:58 run get picked up.
      const yazioFrom = getBerlinHourFromScheduledEvent(event) === 7 ? isoDate(new Date(Date.now() - 86400000)) : yazioDay;
      ctx.waitUntil(
        syncRange(env, yazioFrom, yazioDay, true, false, { includeYazio: true }).catch((e) => {
          console.error("yazio sync failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
        }),
      );
    }
  },
};
