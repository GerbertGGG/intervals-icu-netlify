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
