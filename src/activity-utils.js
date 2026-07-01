export function activityDay(a) {
  return String(a?.start_date_local || a?.start_date || "").slice(0, 10);
}

export function activityLoad(a) {
  const v = Number(a?.icu_training_load ?? a?.training_load ?? a?.load ?? 0);
  return Number.isFinite(v) && v > 0 ? v : 0;
}

export function isRun(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return (
    t === "run" ||
    t === "running" ||
    t.includes("run") ||
    t.includes("laufen") ||
    t.includes("treadmill")
  );
}

export function isTreadmill(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t === "virtualrun" || t.includes("treadmill");
}

export function isRaceActivity(activity) {
  if (!activity || !isRun(activity)) return false;
  const tags = Array.isArray(activity?.tags) ? activity.tags : [];
  if (
    tags.some((tag) =>
      String(tag || "")
        .trim()
        .toLowerCase()
        .startsWith("race:"),
    )
  )
    return true;
  const cat = String(activity?.category || "")
    .trim()
    .toUpperCase();
  if (cat === "RACE" || cat === "RACE_A" || cat === "A_RACE") return true;
  const title = String(activity?.name || activity?.title || "").toLowerCase();
  return /\b(race|wettkampf|competition)\b/.test(title);
}

// Marks interval/repeat sessions (e.g. "#intervalle", "#intervals", "interval:vo2") so
// VDOT estimation can exclude them: averaging pace/HR over the whole activity dilutes
// both with recovery jog/walk segments and skews the estimate (see vdot.js).
export function isIntervalActivity(activity) {
  const tags = Array.isArray(activity?.tags) ? activity.tags : [];
  return tags.some((tag) =>
    String(tag || "")
      .trim()
      .toLowerCase()
      .replace(/^#/, "")
      .startsWith("interval"),
  );
}

// Manual opt-out tag (e.g. "#novdot") to fully exclude an activity from every VDOT
// computation (race detection, training estimate, today's-run field) – e.g. a sick run,
// a stroller/pram run, or a treadmill test that shouldn't influence fitness estimates.
// Tags are read live from Intervals.icu on each sync, so tagging a past activity
// retroactively and re-syncing that day picks it up immediately.
export function isVdotExcluded(activity) {
  const tags = Array.isArray(activity?.tags) ? activity.tags : [];
  return tags.some(
    (tag) =>
      String(tag || "")
        .trim()
        .toLowerCase()
        .replace(/^#/, "") === "novdot",
  );
}
