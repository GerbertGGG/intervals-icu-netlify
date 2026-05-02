import { isIsoDate } from "./date-utils.js";

export function normalizeEventCategory(category) {
  return String(category ?? "").toUpperCase().trim();
}

export function isARaceCategory(category) {
  const cat = normalizeEventCategory(category);
  if (!cat) return false;

  const compact = cat.replace(/[^A-Z0-9]/g, "");

  return (
    cat === "RACE_A" ||
    cat === "A_RACE" ||
    cat === "A-RACE" ||
    cat === "RACE A" ||
    cat === "A" ||
    compact === "RACEA" ||
    compact === "ARACE"
  );
}

export function isARaceEvent(event) {
  if (!event || typeof event !== "object") return false;
  if (isARaceCategory(event?.category)) return true;

  const priorityFields = [
    event?.priority,
    event?.racePriority,
    event?.race_priority,
    event?.raceCategory,
    event?.race_category,
    event?.importance,
    event?.targetLevel,
    event?.goalPriority,
  ];
  const hasAPriority = priorityFields
    .map((v) => normalizeEventCategory(v))
    .some((v) => v === "A" || v === "RACE_A" || v === "A_RACE" || v === "A-RACE");
  if (!hasAPriority) return false;

  return [event?.type, event?.eventType, event?.event_type, event?.discipline]
    .map((v) => normalizeEventCategory(v))
    .some((v) => v === "RACE" || v.includes("RACE"));
}

export function isLifeEventCategory(category) {
  const cat = normalizeEventCategory(category);
  return cat === "SICK" || cat === "INJURED" || cat === "HOLIDAY";
}

export function isLifeEventActiveOnDay(event, dayIso) {
  const startIso = String(event?.start_date_local || event?.start_date || "").slice(0, 10);
  if (!isIsoDate(startIso) || !isIsoDate(dayIso)) return false;

  const endIsoRaw = String(event?.end_date_local || event?.end_date || "").slice(0, 10);
  if (!isIsoDate(endIsoRaw)) return dayIso === startIso;
  return dayIso >= startIso && dayIso < endIsoRaw;
}

export function getLifeEventEffect(activeLifeEvent) {
  const category = normalizeEventCategory(activeLifeEvent?.category);

  if (category === "SICK" || category === "INJURED") {
    return {
      active: true,
      category,
      runFloorFactor: 0,
      allowKeys: false,
      freezeProgression: true,
      freezeFloorIncrease: true,
      ignoreRunFloorGap: true,
      overlayMode: "LIFE_EVENT_STOP",
      reason: `${category}: kompletter Freeze`,
      event: activeLifeEvent,
    };
  }

  if (category === "HOLIDAY") {
    return {
      active: true,
      category,
      runFloorFactor: 0.6,
      allowKeys: false,
      freezeProgression: true,
      freezeFloorIncrease: true,
      ignoreRunFloorGap: true,
      overlayMode: "LIFE_EVENT_HOLIDAY",
      reason: "HOLIDAY: RunFloor reduziert + Keys/Progression pausiert",
      event: activeLifeEvent,
    };
  }

  return {
    active: false,
    category: null,
    runFloorFactor: 1,
    allowKeys: null,
    freezeProgression: false,
    freezeFloorIncrease: false,
    ignoreRunFloorGap: false,
    overlayMode: null,
    reason: null,
    event: null,
  };
}

export function getLifeEventCategoryLabel(category) {
  const cat = normalizeEventCategory(category);
  if (cat === "SICK") return "krank";
  if (cat === "INJURED") return "verletzt";
  if (cat === "HOLIDAY") return "Urlaub";
  return cat || "unbekannt";
}

export function parseLifeEventBoundary(event, field) {
  const value = String(event?.[field] || "").slice(0, 10);
  return isIsoDate(value) ? value : null;
}

export function inferRaceDistanceLabel(distanceM) {
  const d = Number(distanceM);
  if (!Number.isFinite(d) || d <= 0) return "10k";
  if (Math.abs(d - 5000) <= 600) return "5k";
  if (Math.abs(d - 10000) <= 900) return "10k";
  if (Math.abs(d - 21097) <= 1500) return "HM";
  if (Math.abs(d - 42195) <= 2500) return "M";
  return d < 7500 ? "5k" : d < 15500 ? "10k" : d < 32000 ? "HM" : "M";
}

export function formatEventDistance(dist) {
  if (!dist) return "n/a";
  const s = String(dist).toLowerCase();
  if (s === "5k") return "5 km";
  if (s === "10k") return "10 km";
  if (s === "hm") return "HM";
  if (s === "m") return "Marathon";
  return String(dist);
}
