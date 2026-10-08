import { isoDate } from "./date-utils.js";
import { json } from "./http-helpers.js";
import { syncYazioRange } from "./yazio-sync.js";
import { handleAuthorizeRequest, handleTokenRequest, handleAuthServerMetadata, handleProtectedResourceMetadata } from "./mcp-oauth.js";
import { handleMcpRequest } from "./mcp-server.js";
import { handleDashboardRequest, isAuthorized } from "./dashboard.js";
import { handleRunalyzeSnapshotRequest } from "./runalyze-snapshot.js";
import { handleStudieRequest } from "./studie-snapshot.js";
import { syncSnapshotsFromGithub } from "./github-snapshot.js";
import { recordRunalyzeHistory } from "./runalyze-history.js";
import { handleWidgetRequest } from "./widget.js";
import { syncEgymToIntervals } from "./egym-intervals-sync.js";

async function withWorkerErrorBoundary(fn) {
  try {
    return await fn();
  } catch (e) {
    return json({ ok: false, error: "Worker exception", message: String(e?.message ?? e) }, 500);
  }
}

function berlinHour(event) {
  const t = Number(event?.scheduledTime);
  if (!Number.isFinite(t)) return null;
  return Number(new Intl.DateTimeFormat("en-GB", { hour: "2-digit", hour12: false, timeZone: "Europe/Berlin" }).format(new Date(t)));
}

export default {
  async fetch(req, env) {
    const url = new URL(req.url);
    const route = (fn) => withWorkerErrorBoundary(fn);

    switch (url.pathname) {
      case "/":
        return new Response("ok");
      // Daten fuer public/dashboard (Token-geschuetzt, siehe dashboard.js).
      case "/api/dashboard":
        return route(() => handleDashboardRequest(req, env));
      // Kompakte Daten fuer das iOS-Widget (Scriptable), siehe widget.js.
      case "/api/widget":
        return route(() => handleWidgetRequest(req, env));
      // EGYM-Kraft nach Intervals schreiben, von Hand ausloesbar (Token-geschuetzt); liefert Ergebnis oder die genaue Fehlermeldung.
      case "/api/egym-sync":
        return route(async () => {
          if (req.method !== "POST") return json({ ok: false, error: "Nur POST erlaubt" }, 405, { allow: "POST" });
          if (!isAuthorized(req, env)) return json({ ok: false, error: "unauthorized" }, 401);
          try {
            return json({ ok: true, ...(await syncEgymToIntervals(env)) });
          } catch (e) {
            return json({ ok: false, error: String(e?.message ?? e) }, 502);
          }
        });
      // Runalyze-Snapshot und Studien-Check, geschrieben aus einer Coaching-Sitzung.
      case "/api/runalyze":
        return route(() => handleRunalyzeSnapshotRequest(req, env, isAuthorized));
      case "/api/studie":
        return route(() => handleStudieRequest(req, env, isAuthorized));
      // MCP-Connector (mcp-oauth.js, mcp-server.js): Claude liest Intervals/Yazio und schreibt die Snapshots.
      case "/.well-known/oauth-authorization-server":
        return handleAuthServerMetadata(url);
      case "/.well-known/oauth-protected-resource":
        return handleProtectedResourceMetadata(url);
      case "/authorize":
        return route(() => handleAuthorizeRequest(req, url, env));
      case "/token":
        return route(() => handleTokenRequest(req, env));
      case "/mcp":
        return route(() => handleMcpRequest(req, url, env));
      default:
        return new Response("Not found", { status: 404 });
    }
  },

  async scheduled(event, env, ctx) {
    // Runalyze-/Studien-Snapshots, die der Coaching-Task per GitHub-Branch liefert, danach der Tageseintrag im Zeitverlauf.
    ctx.waitUntil(
      syncSnapshotsFromGithub(env)
        .then(() => recordRunalyzeHistory(env))
        .catch((e) => console.error("snapshot sync failed", String(e?.message ?? e))),
    );

    // Yazio alle 15 Minuten von 07:00 bis 23:59 Berlin (der Eintrag "58 21-22" in wrangler.toml
    // liefert den Tagesabschluss um 23:58, unabhaengig von Sommer-/Winterzeit).
    const hour = berlinHour(event);
    if (!Number.isFinite(hour) || hour < 7 || hour > 23) return;
    const today = isoDate(new Date());
    // Der erste Tick des Tages holt auch gestern nach (Eintraege nach dem 23:58-Lauf).
    const from = hour === 7 ? isoDate(new Date(Date.now() - 86400000)) : today;
    // EGYM-Krafteinheiten einmal pro Stunde nach Intervals schreiben (EGYM-Abruf ist langsam, der Abgleich ist wiederholbar)
    if (new Date(Number(event?.scheduledTime)).getUTCMinutes() < 15) {
      ctx.waitUntil(syncEgymToIntervals(env).catch((e) => console.error("egym to intervals failed", String(e?.message ?? e))));
    }
    ctx.waitUntil(
      syncYazioRange(env, from, today).catch((e) => console.error("yazio sync failed", String(e?.message ?? e))),
    );
  },
};
