export function activityDay(a) {
  return String(a?.start_date_local || a?.start_date || "").slice(0, 10);
}

export function activityLoad(a) {
  const v = Number(a?.icu_training_load ?? a?.training_load ?? a?.load ?? 0);
  return Number.isFinite(v) && v > 0 ? v : 0;
}

// Last für Wochenrahmen und TSS-Obergrenzen: beim Rad aus der Leistung (power_load), sonst wie Intervals.
// Intervals rechnet bei dir den Load aus dem Puls (HRSS), die Obergrenzen sind aber in Leistungs-TSS geplant; die HRSS
// unterschätzt lockere Fahrten um ~40 % und HIIT um ~60 % und sinkt, wenn bei gleicher Leistung der Puls fällt.
// CTL/ATL bleiben bei activityLoad, damit sie zur Fitness-Kurve von Intervals passen.
export function sessionLoad(a) {
  const p = Number(a?.power_load);
  return isBike(a) && Number.isFinite(p) && p > 0 ? p : activityLoad(a);
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

// Laufband: VirtualRun, Typ mit "treadmill" oder als Trainer markiert (wie block-inputs.js). Nur fuer den Export (export.js).
export function isTreadmill(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t === "virtualrun" || t.includes("treadmill") || a?.trainer === true;
}

// Renn-Signal ohne Kalender/Runalyze (Stand vor #976): Tag "race:…", Kategorie RACE/RACE_A/A_RACE, sub_type RACE, race-Flag
// von Intervals. Woerter im Namen zaehlen nicht. Nur fuer den Export (export.js), classifyRun bleibt wie er ist.
export function isRaceActivity(a) {
  if (!a) return false;
  const tags = Array.isArray(a?.tags) ? a.tags : [];
  if (tags.some((tag) => String(tag || "").trim().toLowerCase().startsWith("race:"))) return true;
  if (["RACE", "RACE_A", "A_RACE"].includes(String(a?.category || "").trim().toUpperCase())) return true;
  if (String(a?.sub_type ?? "").toUpperCase() === "RACE" || a?.race === true) return true;
  return false;
}

// Bike/Smarttrainer classification (doc.txt 6.1): type contains ride/bike/cycling/
// rad/velo. VirtualRide (smart trainer) counts as a ride like an outdoor ride.
export function isBike(a) {
  const t = String(a?.type ?? "").toLowerCase();
  return t.includes("ride") || t.includes("bike") || t.includes("cycling") || t.includes("rad") || t.includes("velo");
}

// Markiert Intervall-Einheiten per Tag (z. B. "#intervalle", "interval:vo2"). Bewusst nur Tags.
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

// Intervall-Erkennung per Text: Wiederholungsschreibweise ("5x1000", "4×1 km") oder das Wort selbst.
const INTERVAL_TEXT_PATTERN = /\b\d+\s*[x×]\s*\d+/i;

export function hasIntervalTextSignal(activity) {
  const text = `${activity?.name ?? ""} ${activity?.description ?? ""}`.toLowerCase();
  return /\bintervall?\b/.test(text) || INTERVAL_TEXT_PATTERN.test(text);
}
