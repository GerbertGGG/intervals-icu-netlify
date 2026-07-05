import { isoDate, isMondayIso } from "./date-utils.js";
import { handleSyncRequest, handleBackfillProfileRequest, handleWeeklyProgressRequest, handleGoalRequest, handleStatusRequest, handleRecentFormAnalysisRequest, handleReportEmailRequest, withWorkerErrorBoundary } from "./request-handlers.js";
import { syncRange } from "./sync.js";
import { buildWeeklyProgressReport } from "./weekly-progress.js";
import { buildRecentFormAnalysis } from "./form-analysis.js";
import { recordSyncSuccess, recordSyncError } from "./sync-status.js";
import { writeDailyRecoveryNote } from "./recovery-note.js";
import { sendRecentFormReportEmail } from "./email.js";

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

// Yazio's diary for the day is effectively final by ~23:58, so it's fetched exactly
// once here instead of on every 30-min daytime tick (see the `includeYazio` gate in
// sync.js) - far less load on Yazio's unofficial/rate-limit-prone API for the same
// end-of-day totals. wrangler.toml's second cron entry ("58 21-22 * * *") fires at
// :58 past both UTC 21 and 22 to cover the CEST/CET boundary; only the firing that
// actually lands on 23:58 Berlin time runs the job below, so DST transitions don't
// need a seasonal cron-line swap.
function isNightlyYazioWindowBerlin(event) {
  const hour = getBerlinHourFromScheduledEvent(event);
  const minute = getBerlinMinuteFromScheduledEvent(event);
  return hour === 23 && Number.isFinite(minute) && minute >= 55;
}

export default {
  async fetch(req, env, ctx) {
    const url = new URL(req.url);

    if (url.pathname === "/") return new Response("ok");

    if (url.pathname === "/sync") {
      return handleSyncRequest(url, env, ctx, { syncRange });
    }

    if (url.pathname === "/backfill-profile") {
      return withWorkerErrorBoundary(() => handleBackfillProfileRequest(url, env, ctx, { syncRange }));
    }

    if (url.pathname === "/weekly-progress") {
      return withWorkerErrorBoundary(() => handleWeeklyProgressRequest(url, env, ctx, { buildWeeklyProgressReport }));
    }

    if (url.pathname === "/goal") {
      return withWorkerErrorBoundary(() => handleGoalRequest(req, url, env, ctx));
    }

    if (url.pathname === "/status") {
      return withWorkerErrorBoundary(() => handleStatusRequest(url, env, ctx));
    }

    if (url.pathname === "/api/analysis/recent-form") {
      return withWorkerErrorBoundary(() => handleRecentFormAnalysisRequest(url, env, ctx, { buildRecentFormAnalysis }));
    }

    if (url.pathname === "/report-email") {
      return withWorkerErrorBoundary(() => handleReportEmailRequest(url, env, ctx, { sendRecentFormReportEmail }));
    }

    return new Response("Not found", { status: 404 });
  },

  async scheduled(event, env, ctx) {
    if (isNightlyYazioWindowBerlin(event)) {
      const today = isoDate(new Date());
      ctx.waitUntil(
        syncRange(env, today, today, true, false, { includeYazio: true }).catch((e) => {
          console.error("nightly yazio sync failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
        }),
      );
      return;
    }

    // Cron fires every 30 min, but we only sync/write 07:00–21:00 Berlin time.
    if (!isScheduledWindowBerlin(event)) return;

    const today = isoDate(new Date());
    const berlinHour = getBerlinHourFromScheduledEvent(event);
    const berlinMinute = getBerlinMinuteFromScheduledEvent(event);
    const isFirstRunOfDay = berlinHour === 7 && berlinMinute !== null && berlinMinute < 30;
    // Re-sync the last 2 days on every tick (not just the first run of the day), so a
    // "#novdot" tag added retroactively to yesterday's or the day-before's training is
    // picked up within the next 30-minute cycle instead of only at tomorrow's 07:00 run.
    const oldest = isoDate(new Date(Date.now() - 2 * 86400000));

    ctx.waitUntil(
      syncRange(env, oldest, today, true, false, {})
        .then(() => recordSyncSuccess(env))
        .catch((e) => {
          console.error("scheduled sync failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
          return recordSyncError(env, e?.message ?? String(e));
        }),
    );

    if (isFirstRunOfDay && isMondayIso(today)) {
      ctx.waitUntil(
        buildWeeklyProgressReport(env, today, { write: true }).catch((e) => {
          console.error("weekly progress job failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
        }),
      );

      // Independent of the weekly progress note above: mail the raw recent-form
      // JSON for manual analysis. A failure here must never block the report.
      ctx.waitUntil(
        sendRecentFormReportEmail(env, today).catch((e) => {
          console.error("recent-form report email failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
        }),
      );
    }

    if (isFirstRunOfDay) {
      ctx.waitUntil(
        writeDailyRecoveryNote(env, today).catch((e) => {
          console.error("daily recovery note failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
        }),
      );
    }
  },
};
