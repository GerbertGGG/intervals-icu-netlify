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

// ---------- Fortschritt der Block-Ziele ----------
// Welche Ziele verfolgt werden, steht im Text des Eintrags (Stichworte): Decoupling, Kraft, ACWR.
// Schwellen werden aus dem Text gelesen ("unter 8%", "≥32-35%"), sonst gelten die Vorgaben unten.
const ACWR_BAND = { lo: 0.8, hi: 1.3 };
const ACWR_DAYS_SHARE = 0.8; // "Korridor gehalten": mindestens 80 % der Tage
const STRENGTH_PER_WEEK = 2; // Kraft-Konsistenz: Einheiten pro Woche
const r1 = (v) => Math.round(v * 10) / 10;
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const numOf = (s) => Number(String(s).replace(",", "."));

export function parseBlockTargets(text) {
  const t = String(text ?? "");
  const has = (re) => re.test(t);
  const dec = t.match(/decoupling[^\d]{0,25}(\d+(?:[.,]\d+)?)\s*%/i);
  return {
    decoupling: has(/decoupling/i) ? { max: dec ? numOf(dec[1]) : 8 } : null,
    strength: has(/kraft/i) ? { perWeek: STRENGTH_PER_WEEK } : null,
    acwr: has(/acwr/i) ? { ...ACWR_BAND, share: ACWR_DAYS_SHARE } : null,
  };
}

// Fenster: laeuft der Block, ab Blockstart. Davor (oder bei Block-Ende) zaehlt der "Ausgangswert" der letzten 8 Wochen.
function windowOf(block, todayIso) {
  if (block.status === "active" || block.status === "done") {
    const end = block.end && block.end < todayIso ? block.end : todayIso;
    return { from: block.start, to: end, baseline: false };
  }
  return { from: addDays(todayIso, -55), to: todayIso, baseline: true };
}
const inWin = (d, w) => d >= w.from && d <= w.to;

function statusOf(ok, warn) {
  return ok ? "ok" : warn ? "warn" : "bad";
}

// input: { longRuns:[{date,decoupling}], strength:[{date,minutes}], acwrDays:[{date,acwr}]
export function buildBlockGoals(block, input, todayIso) {
  const targets = parseBlockTargets(`${block.goal ?? ""}\n${(block.notes ?? []).join("\n")}`);
  const w = windowOf(block, todayIso);
  const basis = w.baseline ? "letzte 8 Wochen" : "seit Blockstart";
  const goals = [];

  if (targets.decoupling) {
    const runs = (input.longRuns ?? []).filter((r) => inWin(r.date, w) && Number.isFinite(r.decoupling));
    const avg = mean(runs.map((r) => r.decoupling));
    goals.push({
      key: "decoupling", label: "Decoupling lange Läufe", target: `Ø unter ${targets.decoupling.max} %`, unit: "%",
      value: avg != null ? r1(avg) : null, count: runs.length, basis,
      series: runs.sort((a, b) => a.date.localeCompare(b.date)).map((r) => ({ date: r.date, value: r1(r.decoupling) })),
      max: targets.decoupling.max,
      status: avg == null ? "none" : w.baseline ? "base" : statusOf(avg <= targets.decoupling.max, avg <= targets.decoupling.max + 2),
      note: avg == null ? "Noch kein langer Lauf mit Decoupling-Wert." : `${runs.length} lange${runs.length === 1 ? "r Lauf" : " Läufe"}`,
    });
  }

  if (targets.strength) {
    const weeks = [];
    for (let s = w.from; s <= w.to; s = addDays(s, 7)) {
      const e = addDays(s, 6);
      const count = (input.strength ?? []).filter((x) => x.date >= s && x.date <= e).length;
      weeks.push({ start: s, count, done: e <= w.to && e < todayIso, hit: count >= targets.strength.perWeek });
    }
    const full = weeks.filter((x) => x.done);
    const hits = full.filter((x) => x.hit).length;
    const openWeek = weeks.find((x) => !x.done) ?? null;
    goals.push({
      key: "strength", label: "Kraft-Konsistenz", target: `jede Woche ${targets.strength.perWeek}× Kraft`, unit: "Wochen",
      value: full.length ? hits : null, of: full.length, basis, weeks: weeks.slice(-12),
      status: w.baseline ? (full.length ? "base" : "none") : !full.length ? "none" : statusOf(hits / full.length >= 0.8, hits / full.length >= 0.6),
      note: openWeek ? `Diese Woche: ${openWeek.count} von ${targets.strength.perWeek}` : null,
    });
  }

  if (targets.acwr) {
    const days = (input.acwrDays ?? []).filter((x) => inWin(x.date, w) && Number.isFinite(x.acwr));
    const inBand = days.filter((x) => x.acwr >= targets.acwr.lo && x.acwr <= targets.acwr.hi).length;
    const share = days.length ? inBand / days.length : null;
    goals.push({
      key: "acwr", label: "ACWR im Korridor", target: `≥ ${Math.round(targets.acwr.share * 100)} % der Tage in ${targets.acwr.lo}–${targets.acwr.hi}`, unit: "%",
      value: share != null ? Math.round(share * 100) : null, count: days.length, basis,
      status: share == null ? "none" : w.baseline ? "base" : statusOf(share >= targets.acwr.share, share >= targets.acwr.share - 0.15),
      note: share == null ? "Keine ACWR-Daten." : `${inBand} von ${days.length} Tagen im Korridor`,
    });
  }

  return goals;
}
