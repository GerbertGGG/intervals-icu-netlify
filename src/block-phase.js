// Erkennung von Renndistanz und Triathlon-Format aus Kalendereinträgen (Intervals.icu).

export function normalizeEventDistance(value) {
  const s = String(value || "").toLowerCase().trim();
  if (!s) return null;
  if (s.includes("5k") || s.includes("5 km") || s.includes("5km")) return "5k";
  if (s.includes("10k") || s.includes("10 km") || s.includes("10km")) return "10k";
  if (s.includes("half") || s.includes("hm") || s.includes("halb")) return "hm";
  if (s.includes("marathon") || s === "m" || s.includes("42")) return "m";
  const numeric = Number(s.replace(/[^0-9.]/g, ""));
  if (Number.isFinite(numeric) && numeric > 0) {
    const meters = numeric < 1000 ? numeric * 1000 : numeric;
    if (meters >= 4900 && meters <= 5100) return "5k";
    if (meters >= 9500 && meters <= 10500) return "10k";
    if (meters >= 20500 && meters <= 21500) return "hm";
    if (meters >= 41000 && meters <= 43000) return "m";
  }
  return null;
}

// Intervals.icu kennt keinen Triathlon als Rennart: Der Eintrag heisst z. B. "Triathlon", die
// Variante steht in der Beschreibung ("Mitteldistanz"). Die Lauf-Distanz des Formats steuert die
// Blocklaenge (Sprint ~5k, Olympisch ~10k, Mittel ~Halbmarathon, Lang ~Marathon).
const TRIATHLON_FORMATS = [
  { key: "sprint", label: "Sprint", re: /sprint/, swimKm: 0.75, bikeKm: 20, runKm: 5, run: "5k" },
  { key: "olympic", label: "Olympische Distanz", re: /olymp|kurzdistanz|standard/, swimKm: 1.5, bikeKm: 40, runKm: 10, run: "10k" },
  { key: "middle", label: "Mitteldistanz", re: /mittel|70\.3|half\s*iron|halb\s*iron/, swimKm: 1.9, bikeKm: 90, runKm: 21.0975, run: "hm" },
  { key: "long", label: "Langdistanz", re: /lang|ironman|140\.6|full\s*iron/, swimKm: 3.8, bikeKm: 180, runKm: 42.195, run: "m" },
];

export function parseTriathlonEvent(event) {
  if (!event) return null;
  const head = `${event?.name ?? ""} ${event?.type ?? ""}`.toLowerCase();
  if (!/triathlon|\btri\b|ironman|70\.3/.test(head)) return null;
  const text = `${head} ${event?.description ?? ""}`.toLowerCase();
  const fmt = TRIATHLON_FORMATS.find((f) => f.re.test(text)) ?? TRIATHLON_FORMATS[1];
  const desc = String(event?.description ?? "");
  const toSecs = (t) => { const p = t.split(":").map(Number); return p.length === 3 ? p[0] * 3600 + p[1] * 60 + p[2] : p[0] * 60 + p[1]; };
  const time = (word) => { const m = desc.match(new RegExp(`\\b(?:${word})\\b\\s*[:=]?\\s*(\\d{1,2}:\\d{2}(?::\\d{2})?)`, "i")); return m ? toSecs(m[1]) : null; };
  const runTargetSecs = time("lauf(?:en)?|run");
  const swimTargetSecs = time("schwimm(?:en)?|swim");
  const bikeTargetSecs = time("rad|bike");
  const w = desc.match(/\b(?:rad|bike)\b[^\n\d]*(?:\d{1,2}:\d{2}(?::\d{2})?)?[^\n\d]*(\d{2,3})\s*(?:w|watt)\b/i);
  const bikeTargetWatts = w ? Number(w[1]) : null;
  return { format: fmt.key, label: fmt.label, swimKm: fmt.swimKm, bikeKm: fmt.bikeKm, runKm: fmt.runKm, runDistance: fmt.run, runTargetSecs, swimTargetSecs, bikeTargetSecs, bikeTargetWatts };
}

export function getEventDistanceFromEvent(event) {
  if (!event) return null;
  const tri = parseTriathlonEvent(event);
  if (tri) return tri.runDistance;
  const raw = event?.distance ?? event?.distance_target ?? null;
  const fromField = normalizeEventDistance(raw);
  if (fromField) return fromField;
  const name = String(event?.name ?? "");
  const type = String(event?.type ?? "");
  return normalizeEventDistance(`${name} ${type}`);
}
