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
    // Cron fires every 30 min, but we only sync/write 07:00–21:00 Berlin time.
    if (!isScheduledWindowBerlin(event)) return;

    const today = isoDate(new Date());
    const berlinHour = getBerlinHourFromScheduledEvent(event);
    const berlinMinute = getBerlinMinuteFromScheduledEvent(event);
    const isFirstRunOfDay = berlinHour === 7 && berlinMinute !== null && berlinMinute < 30;
    // First tick of the 21:00 Berlin hour, same "first tick" pattern as isFirstRunOfDay
    // above. No separate wrangler.toml cron entry needed for this: the existing
    // */30 5-20 * * * (UTC) trigger already lands on a tick at Berlin 21:00 in both CET
    // (UTC 20:00) and CEST (UTC 19:00), so reusing it avoids a second, DST-fragile
    // fixed-UTC cron and a risk of double-firing this job.
    const isFirstEveningRunOfDay = berlinHour === 21 && berlinMinute !== null && berlinMinute < 30;
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

    if (isFirstEveningRunOfDay) {
      ctx.waitUntil(
        writeDailyRecoveryNote(env, today).catch((e) => {
          console.error("daily recovery note failed", { athlete: env?.ATHLETE_ID, error: String(e?.message ?? e) });
        }),
      );
    }
  },
};
