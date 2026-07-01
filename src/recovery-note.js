import { buildRecentFormAnalysis } from "./form-analysis.js";
import { upsertWellnessComment } from "./intervals-client.js";

const RECOVERY_NOTE_DAYS = 28; // matches buildRecentFormAnalysis's own default window
const RECOVERY_NOTE_MAX_LENGTH = 300;
// Stable across runs (unlike the status emoji), so re-runs can find and replace their
// own line in upsertWellnessComment regardless of that day's status.
const RECOVERY_NOTE_MARKER = "Formcheck:";

const STATUS_EMOJI = { "grün": "🟢", "gelb": "🟡", rot: "🔴" };

function truncate(text, maxLength) {
  const chars = Array.from(text);
  if (chars.length <= maxLength) return text;
  return `${chars.slice(0, maxLength - 1).join("")}…`;
}

function buildRecoveryNoteLine(assessment) {
  const emoji = STATUS_EMOJI[assessment.status] ?? "⚪";
  const line = `${emoji} ${RECOVERY_NOTE_MARKER} ${assessment.summary} Empfehlung: ${assessment.recommendation}`;
  return truncate(line, RECOVERY_NOTE_MAX_LENGTH);
}

// Reuses buildRecentFormAnalysis directly (the same call /api/analysis/recent-form
// makes) instead of a self-fetch, since this already runs in-process inside the daily
// scheduled() job. Errors (missing data, failed PUT) are left to bubble up so the
// caller's existing ctx.waitUntil(...).catch(...) logging applies, same as the other
// scheduled jobs in index.js.
export async function writeDailyRecoveryNote(env, todayIso) {
  const { assessment } = await buildRecentFormAnalysis(env, todayIso, { days: RECOVERY_NOTE_DAYS });
  const line = buildRecoveryNoteLine(assessment);
  await upsertWellnessComment(env, todayIso, RECOVERY_NOTE_MARKER, line);
  return { status: assessment.status, flags: assessment.flags, line };
}
