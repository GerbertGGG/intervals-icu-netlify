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
