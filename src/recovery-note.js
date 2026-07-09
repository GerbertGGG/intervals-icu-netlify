import { buildRecentFormAnalysis } from "./form-analysis.js";
import { upsertIntervalsNote } from "./intervals-client.js";

const RECOVERY_NOTE_DAYS = 28; // matches buildRecentFormAnalysis's own default window
// hrDrift needs one extra fetch (GET .../streams) per qualifying run - this cap keeps
// that to the last handful of easy/long runs (enough for the 14-day aerobic-decoupling
// window assessRecoveryStatus looks at) since writeDailyRecoveryNote itself only runs
// once/day (see index.js's isFirstRunOfDay guard), not on every 30-min scheduled tick.
const RECOVERY_NOTE_HR_DRIFT_CAP = 8;

const STATUS_EMOJI = { "grün": "🟢", "gelb": "🟡", rot: "🔴" };
const STATUS_COLOR = { "grün": "green", "gelb": "orange", rot: "red" };

function buildRecoveryNoteText(assessment) {
  const emoji = STATUS_EMOJI[assessment.status] ?? "⚪";
  const parts = [assessment.summary];
  if (assessment.details?.length) {
    parts.push(`Werte:\n${assessment.details.map((d) => `- ${d}`).join("\n")}`);
  }
  if (assessment.goalText) parts.push(assessment.goalText);
  parts.push(`Empfehlung: ${assessment.recommendation}`);
  return {
    name: `Formcheck ${emoji}`,
    description: parts.join("\n\n"),
  };
}

// Reuses buildRecentFormAnalysis directly (the same call /api/analysis/recent-form
// makes) instead of a self-fetch, since this already runs in-process inside the daily
// scheduled() job. Errors (missing data, failed PUT) are left to bubble up so the
// caller's existing ctx.waitUntil(...).catch(...) logging applies, same as the other
// scheduled jobs in index.js. Written as its own NOTE calendar event (same mechanism
// as the weekly "Wochenvergleich" note) instead of the wellness comments field, so the
// Ampel shows up as its own color-coded tile on the calendar.
export async function writeDailyRecoveryNote(env, todayIso) {
  const { assessment } = await buildRecentFormAnalysis(env, todayIso, {
    days: RECOVERY_NOTE_DAYS,
    includeHrDrift: true,
    hrDriftCap: RECOVERY_NOTE_HR_DRIFT_CAP,
  });
  const { name, description } = buildRecoveryNoteText(assessment);
  await upsertIntervalsNote(env, {
    dayIso: todayIso,
    externalId: `formcheck-${todayIso}`,
    name,
    description,
    color: STATUS_COLOR[assessment.status] ?? "blue",
  });
  return { status: assessment.status, flags: assessment.flags, name, description };
}
