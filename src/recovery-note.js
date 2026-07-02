import { buildRecentFormAnalysis } from "./form-analysis.js";
import { upsertIntervalsNote } from "./intervals-client.js";

const RECOVERY_NOTE_DAYS = 28; // matches buildRecentFormAnalysis's own default window

const STATUS_EMOJI = { "grün": "🟢", "gelb": "🟡", rot: "🔴" };
const STATUS_COLOR = { "grün": "green", "gelb": "orange", rot: "red" };

function buildRecoveryNoteText(assessment) {
  const emoji = STATUS_EMOJI[assessment.status] ?? "⚪";
  return {
    name: `Formcheck ${emoji}`,
    description: `${assessment.summary}\n\nEmpfehlung: ${assessment.recommendation}`,
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
  const { assessment } = await buildRecentFormAnalysis(env, todayIso, { days: RECOVERY_NOTE_DAYS });
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
