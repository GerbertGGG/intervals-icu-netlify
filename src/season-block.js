import { diffDays } from "./date-utils.js";

const addDays = (iso, n) => new Date(new Date(iso + "T00:00:00Z").getTime() + n * 86400000).toISOString().slice(0, 10);

// Trainingsblock aus dem Intervals.icu-Kalender: ein Eintrag der Kategorie "Saison" (SEASON_START), optional auch
// PLAN/NOTE, dessen Name mit "Block" beginnt, z.B. "Block Grundlage (12 Wochen)". Die Dauer steht im Namen
// ("12 Wochen") oder ergibt sich aus end_date_local; die Beschreibung enthaelt das Ziel (1. Absatz) und Hinweise/Checkpoints.
const dayOf = (v) => String(v ?? "").slice(0, 10);

export function isSeasonBlockEvent(e) {
  if (!e || typeof e !== "object") return false;
  const cat = String(e.category ?? "").toUpperCase();
  if (cat.includes("SEASON")) return true;
  return ["PLAN", "NOTE", "TARGET"].includes(cat) && /^\s*block\b/i.test(String(e.name ?? ""));
}

export function parseBlockWeeks(name) {
  const m = String(name ?? "").match(/(\d{1,2})\s*(?:wochen?|wo\b|weeks?|w\b)/i);
  return m ? Number(m[1]) : null;
}

function toBlock(e, todayIso) {
  const start = dayOf(e.start_date_local || e.start_date);
  if (!start) return null;
  const endRaw = dayOf(e.end_date_local || e.end_date);
  const nameWeeks = parseBlockWeeks(e.name);
  const weeks = nameWeeks ?? (endRaw && endRaw > start ? Math.round(diffDays(start, endRaw) / 7) : null);
  const end = weeks ? addDays(start, weeks * 7 - 1) : endRaw || null;
  const paras = String(e.description ?? "").split(/\n\s*\n/).map((p) => p.trim()).filter(Boolean);
  const status = todayIso < start ? "upcoming" : end && todayIso > end ? "done" : "active";
  const weekNo = status === "active" ? Math.floor(diffDays(start, todayIso) / 7) + 1 : null;
  return {
    name: String(e.name ?? "Block").replace(/\s*\(\s*\d{1,2}\s*[A-Za-z]+\s*\)\s*$/, "").trim() || "Block",
    fullName: e.name ?? null,
    start,
    end,
    weeks,
    weekNo,
    daysToStart: status === "upcoming" ? diffDays(todayIso, start) : null,
    daysLeft: status === "active" && end ? diffDays(todayIso, end) : null,
    status,
    goal: paras[0] ?? null,
    notes: paras.slice(1),
  };
}

// Aktiver Block, sonst der naechste kommende, sonst der zuletzt beendete (maximal 14 Tage her).
export function resolveSeasonBlock(events, todayIso) {
  const blocks = (Array.isArray(events) ? events : []).filter(isSeasonBlockEvent).map((e) => toBlock(e, todayIso)).filter(Boolean);
  const seen = new Set();
  const uniq = blocks.filter((b) => (seen.has(b.start + b.name) ? false : seen.add(b.start + b.name)));
  const active = uniq.filter((b) => b.status === "active").sort((a, b) => b.start.localeCompare(a.start))[0];
  if (active) return active;
  const next = uniq.filter((b) => b.status === "upcoming").sort((a, b) => a.start.localeCompare(b.start))[0];
  if (next) return next;
  return uniq.filter((b) => b.status === "done" && b.end && diffDays(b.end, todayIso) <= 14).sort((a, b) => b.end.localeCompare(a.end))[0] ?? null;
}
