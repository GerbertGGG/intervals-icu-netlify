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

// Bike/Smarttrainer classification (doc.txt 6.1): type contains ride/bike/cycling/
// rad/velo. VirtualRide (smart trainer) counts as a ride like an outdoor ride.
export function isBike(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t.includes("ride") || t.includes("bike") || t.includes("cycling") || t.includes("rad") || t.includes("velo");
}

// Sport-agnostic on purpose: every current caller already gates by isRun(...)
// itself (form-analysis.js, vdot.js) where a run-only race matters, so dropping the
// isRun requirement here doesn't change any existing result - it just lets ride
// records (buildRideRecord in form-analysis.js) reuse the same race signal.
export function isRaceActivity(activity) {
  if (!activity) return false;
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

// Auto-detected repeat/interval structure via a common rep-count notation ("5x1000",
// "4×1 km", "3x10'"), on top of the tag-based signal below - a manual tag is easy to
// forget on an actual interval session, so this text-only signal (no extra API cost)
// catches it too. form-analysis.js additionally upgrades this to a "structure"-based
// signal for isInterval runs once the real rep data is fetched (opt-in intervalSplits).
const INTERVAL_TEXT_PATTERN = /\b\d+\s*[x×]\s*\d+/i;

function detectIntervalSource(activity) {
  const tags = Array.isArray(activity?.tags) ? activity.tags : [];
  const tagged = tags.some((tag) =>
    String(tag || "")
      .trim()
      .toLowerCase()
      .replace(/^#/, "")
      .startsWith("interval"),
  );
  if (tagged) return "tag";
  const text = `${activity?.name ?? ""} ${activity?.description ?? ""}`.toLowerCase();
  if (text.includes("intervall") || text.includes("interval") || INTERVAL_TEXT_PATTERN.test(text)) return "text";
  return null;
}

// Marks interval/repeat sessions so VDOT estimation can exclude them: averaging
// pace/HR over the whole activity dilutes both with recovery jog/walk segments and
// skews the estimate (see vdot.js).
export function isIntervalActivity(activity) {
  return detectIntervalSource(activity) != null;
}

// Exposes *how* isIntervalActivity decided (tag vs. text-pattern), for a JSON
// consumer that wants to judge how trustworthy the flag is. See form-analysis.js's
// enrichRunsWithIntervalSplits for the "structure"-confirmed upgrade.
export function intervalDetectionSource(activity) {
  return detectIntervalSource(activity);
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
