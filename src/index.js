import { isoDate } from "./date-utils.js";
import { handleSyncRequest, handleBackfillProfileRequest, withWorkerErrorBoundary } from "./request-handlers.js";
import { syncRange } from "./sync.js";

function getBerlinHourFromScheduledEvent(event) {
  const t = Number(event?.scheduledTime);
  if (!Number.isFinite(t)) return null;
  const hour = Number(
    new Intl.DateTimeFormat("en-GB", { hour: "2-digit", hour12: false, timeZone: "Europe/Berlin" }).format(new Date(t)),
  );
  return Number.isFinite(hour) ? hour : null;
}

function getBerlinMinuteFromScheduledEvent(event) {
  const t = Number(event?.scheduledTime);
  if (!Number.isFinite(t)) return null;
  const minute = Number(
    new Intl.DateTimeFormat("en-GB", { minute: "2-digit", hour12: false, timeZone: "Europe/Berlin" }).format(new Date(t)),
  );
  return Number.isFinite(minute) ? minute : null;
}

function isScheduledWindowBerlin(event) {
  const hour = getBerlinHourFromScheduledEvent(event);
  return Number.isFinite(hour) && hour >= 7 && hour <= 21;
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") return new Response("ok");

    if (url.pathname === "/sync") {
      return handleSyncRequest(url, env, ctx, { syncRange });
    }

    if (url.pathname === "/backfill-profile") {
      return withWorkerErrorBoundary(() => handleBackfillProfileRequest(url, env, { syncRange }));
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    // Cron fires every 30 min, but we only sync/write 07:00–21:00 Berlin time.
    if (!isScheduledWindowBerlin(event)) return;

    const today = isoDate(new Date());
    const berlinHour = getBerlinHourFromScheduledEvent(event);
    const berlinMinute = getBerlinMinuteFromScheduledEvent(event);
    // The window starts at 07:00 and the cron never fires between 21:00 and 07:00,
    // so the first run of the day also re-syncs yesterday to catch late-evening runs.
    const isFirstRunOfDay = berlinHour === 7 && berlinMinute !== null && berlinMinute < 30;
    const oldest = isFirstRunOfDay ? isoDate(new Date(Date.now() - 86400000)) : today;

    ctx.waitUntil(
      syncRange(env, oldest, today, true, false, {}).catch((e) => {
        console.error("scheduled sync failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
      }),
    );
  },
};
