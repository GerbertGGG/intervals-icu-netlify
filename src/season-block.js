import { diffDays, mondayOnOrBefore } from "./date-utils.js";

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

// Regeln des Coaches fuer den Block: ein Abschnitt, der mit einer Zeile "Regeln", "Trainingsregeln" oder "Fixpunkte" beginnt
// (Zusatz in Klammern und Doppelpunkt erlaubt, z.B. "FIXPUNKTE (meine Zusagen)"); jede folgende Zeile ist eine Regel.
// Ist die erste Regel ein Spiegelstrich, zaehlen nur Spiegelstriche, ein Satz danach gehoert nicht mehr dazu.
// Der Abschnitt endet an der ersten Leerzeile nach einer Regel.
const RULES_HEAD = /^(?:(?:trainings)?regeln|fixpunkte)\s*(?:\([^)]*\))?\s*:?$/i;
const BULLET = /^(?:[-•*–]|\d+[.)])\s*/;
export function parseBlockRules(text) {
  const lines = String(text ?? "").replace(/<br\s*\/?>/gi, "").split("\n").map((l) => l.trim());
  const at = lines.findIndex((l) => RULES_HEAD.test(l));
  if (at < 0) return [];
  const out = [];
  let bullets = null;
  for (const l of lines.slice(at + 1)) {
    if (!l) { if (out.length) break; continue; }
    if (bullets == null) bullets = BULLET.test(l);
    else if (bullets && !BULLET.test(l)) break;
    out.push(l.replace(BULLET, ""));
  }
  return out.filter(Boolean);
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
    rules: parseBlockRules(e.description),
  };
}

// Aktiver Block, sonst der naechste kommende, sonst der zuletzt beendete (maximal 14 Tage her).
export function resolveSeasonBlock(events, todayIso) {
  const blocks = (Array.isArray(events) ? events : []).filter(isSeasonBlockEvent).map((e) => toBlock(e, todayIso)).filter(Boolean);
  const seen = new Set();
  const uniq = blocks.filter((b) => (seen.has(b.start + b.name) ? false : seen.add(b.start + b.name)));
  const withNext = (b) => {
    const n = uniq.filter((x) => x.start > b.start).sort((x, y) => x.start.localeCompare(y.start))[0];
    return n ? { ...b, next: { name: n.name, start: n.start } } : b;
  };
  const active = uniq.filter((b) => b.status === "active").sort((a, b) => b.start.localeCompare(a.start))[0];
  if (active) return withNext(active);
  const next = uniq.filter((b) => b.status === "upcoming").sort((a, b) => a.start.localeCompare(b.start))[0];
  if (next) return withNext(next);
  const done = uniq.filter((b) => b.status === "done" && b.end && diffDays(b.end, todayIso) <= 14).sort((a, b) => b.end.localeCompare(a.end))[0];
  return done ? withNext(done) : null;
}

// ---------- Fortschritt der Block-Ziele ----------
// Welche Ziele verfolgt werden, steht im Text des Eintrags (Stichworte): Decoupling, Kraft, ACWR.
// Schwellen werden aus dem Text gelesen ("unter 8%", "≥32-35%"), sonst gelten die Vorgaben unten.
const ACWR_BAND = { lo: 0.8, hi: 1.3 };
const ACWR_DAYS_SHARE = 0.8; // "Korridor gehalten": mindestens 80 % der Tage
const STRENGTH_WEEKS_SHARE = 0.75; // Kraft-Konsistenz: mindestens 75 % der Wochen mit erreichtem Satz-Ziel (nicht jede Woche)
const STRENGTH_DAYS_PER_WEEK = 2; // nur Rückfall, wenn keine Satzzahlen vorliegen (kein EGYM): Kraft-Tage pro Woche
const DEFAULT_WEEKLY_SETS = 60;
const r1 = (v) => Math.round(v * 10) / 10;
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const numOf = (s) => Number(String(s).replace(",", "."));

// Abschlussziele je Sportart (Block A): je ein Absatz bzw. Listenpunkt "1. Lauf: …", "2. Rad (Rolle): …", "3. Schwimmen: …"
// mit Vergleichseinheiten ("vergleichbar", "Vergleichseinheiten", "Blocktest") bzw. einem Test (2× 500 m). Leitplanken stehen
// als Stichworte im Text (Kraft "8 von 9 Belastungswochen", "Ramp Rate … ≤5", "Pause über 7 Tage"). Muss-Kriterium: "Muss ≤8 %"
// im Absatz der Sportart, sonst gilt ein allgemeines ("bei 8 % … Muss-Kriterium") fuer beide Decoupling-Ziele.
const num1 = (m, fb) => (m ? numOf(m[1]) : fb);
// Termine der Blocktests aus dem Satz mit "Basistest"/"Blocktest" als "MM-DD" (Jahr ergibt sich aus dem Blockstart)
export function parseTestDates(p) {
  const m = String(p ?? "").match(/basistest\s*(\d{1,2}\.\d{1,2}\.[^)\n]*)/i);
  if (!m) return [];
  return [...m[1].matchAll(/(\d{1,2})\.(\d{1,2})\./g)].map((x) => `${x[2].padStart(2, "0")}-${x[1].padStart(2, "0")}`);
}
const LE = "(?:≤|<=|<|unter|max(?:imal)?)";
function parseSportGoals(t) {
  const paras = t.split(/\n\s*\n|\n(?=\s*\d+\.\s)/);
  const para = (...res) => paras.find((p) => res.every((re) => re.test(p))) ?? null;
  const mustAll = t.match(/(\d+(?:[.,]\d+)?)\s*%[^\n]{0,50}muss/i);
  const mustOf = (p) => num1(p.match(new RegExp(`muss\\s*:?\\s*${LE}?\\s*(\\d+(?:[.,]\\d+)?)\\s*%`, "i")), mustAll ? numOf(mustAll[1]) : null);
  const lastN = (p) => num1(p.match(/letzten\s*(\d+)/i), 3);
  const decMax = (p) => num1(p.match(new RegExp(`ziel(?:wert)?\\s*:?\\s*${LE}\\s*(\\d+(?:[.,]\\d+)?)\\s*%`, "i")) ?? p.match(new RegExp(`decoupling[^\\n\\d]{0,40}?${LE}\\s*(\\d+(?:[.,]\\d+)?)\\s*%`, "i")), 5);
  const cmp = /vergleich|test/i;
  const runP = para(/\blauf/i, /decoupling/i, cmp);
  const bikeP = para(/\brad\b|rolle/i, /decoupling/i, cmp);
  const swimP = para(/schwimm/i, /\d\s*[×x]\s*\d{2,4}\s*m\b/i);
  const out = {};
  if (runP) {
    const max = decMax(runP), must = mustOf(runP);
    out.run = { max, must: must != null ? Math.max(max, must) : null, lastN: lastN(runP), minMin: num1(runP.match(/(?:≥|>=|ab|mind(?:estens)?\.?)\s*(\d+)\s*min/i), 60), pace: /pace/i.test(runP) };
    // Blocktest-Modus, sobald der Lauf-Absatz Blocktests nennt: Das Ziel wird nur an Laeufen gemessen, die der Nutzer als
    // "Blocktest"/"Basistest" benennt (Decoupling der ersten 60 min und stabile Dauer, laengstes Fenster 0–60, 0–75 … mit
    // Decoupling ≤ stableMax). Termine im Text ("Basistest 25.10., dann 08.11.") sind optional und nur Anzeige.
    const tests = parseTestDates(runP);
    if (tests.length || TEST_RE.test(runP)) {
      out.run.tests = tests;
      out.run.stableMax = num1(runP.match(new RegExp(`stabile\\s*dauer[^\\n]*?decoupling\\s*${LE}\\s*(\\d+(?:[.,]\\d+)?)\\s*%`, "i")), 5);
    }
    // HF-Band des Blocktests, auch ohne Termine (fuer die Pruefung des Block-Eintrags)
    const band = runP.match(/hf-?band\s*(\d{2,3})\s*[–-]\s*(\d{2,3})/i);
    if (band) out.run.band = [Number(band[1]), Number(band[2])];
  }
  if (bikeP) {
    const max = decMax(bikeP), must = mustOf(bikeP);
    // Messfenster ("60-min-Messfenster") nach dem Einrollen ("10 min Einrollen") bei fester Leistung ("fester Leistung 165 W")
    const win = num1(bikeP.match(/(\d+)\s*-?\s*min\s*-?\s*messfenster/i), null);
    const warm = num1(bikeP.match(/(\d+)\s*min\s*einrollen/i), null);
    const watts = num1(bikeP.match(/fest(?:e|er|en)?\s*leistung\s*(?:von\s*)?(\d{2,3})\s*w\b/i), null);
    // FTP-Test nur, wenn er im Block ansteht ("FTP-Test in Woche 1"); eine Referenz wie "FTP 252 W (Rampentest 12.10.)" nicht
    out.bike = { max, must: must != null ? Math.max(max, must) : null, lastN: lastN(bikeP), minMin: win ?? num1(bikeP.match(/(\d+)\s*min\b/i), 75), firstMin: win ?? num1(bikeP.match(/ersten\s*(\d+)\s*min/i), null), ftpTest: /ftp-?test[^\n]{0,30}woche/i.test(bikeP) };
    if (warm != null) out.bike.warmupMin = warm;
    if (watts != null) out.bike.watts = watts;
  }
  if (swimP) {
    const reps = swimP.match(/(\d)\s*[×x]\s*(\d{2,4})\s*m\b/i);
    out.swim = {
      reps: Number(reps[1]), distM: Number(reps[2]),
      maxPauseSec: num1(swimP.match(/(?:max(?:imal)?|höchstens)\s*(\d+(?:[.,]\d+)?)\s*min\s*pause/i), 1) * 60,
      maxSlowerPer100: num1(swimP.match(/(\d+(?:[.,]\d+)?)\s*s(?:ek(?:unden)?)?\s*(?:pro|\/|je)\s*100/i), 5),
    };
  }
  return out;
}

// Satz-Ziel Kraft aus dem Blocktext: Zahl vor "Sätze" in einer Zeile mit "Kraft" ("Kraft ≥70 Sätze pro Woche"),
// sonst die erste "N Sätze" im Text; null, wenn keine dasteht. Gilt auch für die Kraft-Karte (egym-widget.js).
// Ein Ziel über mehrere Wochen ("Kraft ≥90 Sätze in 2 Wochen", meist in den Leitplanken) geht einer Zahl ohne Zeitraum
// vor (etwa "Kraft ≥60 Sätze" in den Standardregeln); weeks = Länge des gleitenden Fensters, sonst 1.
export function parseStrengthTarget(text) {
  const t = String(text ?? "");
  const per = t.match(/kraft[^\n]*?(\d+)\s*(?:sätze|saetze)\s*(?:in|pro|je|alle)\s*(\d+)\s*wochen/i);
  if (per && Number(per[1]) > 0 && Number(per[2]) > 0) return { sets: Number(per[1]), weeks: Number(per[2]) };
  const m = t.match(/kraft[^\n]*?(\d+)\s*(?:sätze|saetze)/i) ?? t.match(/(\d+)\s*(?:sätze|saetze)/i);
  const n = m ? Number(m[1]) : NaN;
  return n > 0 ? { sets: n, weeks: 1 } : null;
}
export const parseStrengthSets = (text) => parseStrengthTarget(text)?.sets ?? null;

// Satz-Ziel des laufenden Blocks (nur aktiv, ein kommender Block gilt noch nicht)
export function activeBlockStrengthTarget(events, todayIso) {
  const b = resolveSeasonBlock(events, todayIso);
  const t = b?.status === "active" ? parseStrengthTarget([b.goal, ...(b.notes ?? [])].filter(Boolean).join("\n\n")) : null;
  return t ? { ...t, start: b.start } : null;
}
export const activeBlockStrengthSets = (events, todayIso) => activeBlockStrengthTarget(events, todayIso)?.sets ?? null;

export function parseBlockTargets(text) {
  const t = String(text ?? "");
  const has = (re) => re.test(t);
  const sport = parseSportGoals(t);
  const dec = t.match(/decoupling[^\d]{0,25}(\d+(?:[.,]\d+)?)\s*%/i);
  // "x von y Belastungswochen" gibt es auch bei der Intensitaet: fuer Kraft zuerst die Zeile mit "Kraft"
  const loadWeeks = t.match(/kraft[^\n]*?(\d+)\s*von\s*(\d+)\s*belastungswochen/i) ?? t.match(/(\d+)\s*von\s*(\d+)\s*belastungswochen/i);
  const intLine = t.match(/intensit(?:ä|ae|a)t[^\n]*?(\d+)\s*von\s*(\d+)\s*belastungswochen[^\n]*/i);
  const intFrom = intLine?.[0].match(/ab\s*woche\s*(\d+)/i);
  const intKinds = intLine ? [/hiit/i.test(intLine[0]) ? "bikeHiit" : null, /hügel|huegel|sprint/i.test(intLine[0]) ? "hillSprints" : null].filter(Boolean) : [];
  const st = parseStrengthTarget(t), sets = st?.sets ?? null, periodWeeks = st?.weeks ?? 1;
  const ramp = t.match(/ramp\s*-?\s*rate[^\d\n]{0,40}(\d+(?:[.,]\d+)?)/i);
  const pause = t.match(/pause\s*(?:über|von mehr als|länger als|>)\s*(\d+)\s*tag/i);
  return {
    ...sport,
    // Alte Form (nur Stichworte): Decoupling langer Laeufe, solange kein Lauf-Absatz mit vergleichbaren Laeufen da ist
    decoupling: has(/decoupling/i) && !sport.run && !sport.bike ? { max: dec ? numOf(dec[1]) : 8 } : null,
    strength: has(/kraft/i) ? (loadWeeks ? { need: Number(loadWeeks[1]), of: Number(loadWeeks[2]), sets } : periodWeeks > 1 ? { sets, periodWeeks } : { share: STRENGTH_WEEKS_SHARE, sets }) : null,
    acwr: has(/acwr/i) ? { ...ACWR_BAND, share: ACWR_DAYS_SHARE } : null,
    ramp: ramp ? { max: numOf(ramp[1]) } : null,
    pause: pause ? { maxDays: Number(pause[1]) } : null,
    intensity: intKinds.length ? { need: Number(intLine[1]), of: Number(intLine[2]), fromWeek: intFrom ? Number(intFrom[1]) : 1, kinds: intKinds, sickOk: /krank/i.test(intLine[0]) } : null,
    // HF-Deckel fuer lockere Laeufe: "HF-Deckel 128", sonst die Zeile "Lauf-Grundlage unter HF 128", erst danach irgendein
    // "Grundlage… unter HF" (sonst gewinnt ein frueherer Satz wie "Grundlagenlauf ab 60 min unter HF 125 als Trend")
    hrCap: num1(t.match(/hf-?(?:deckel|obergrenze)\s*:?\s*(\d{3})/i) ?? t.match(/lauf-?grundlage[^\n]{0,40}?(?:unter|≤|<=|<)\s*hf\s*(\d{3})/i) ?? t.match(/grundlage[^\n]{0,40}?(?:unter|≤|<=|<)\s*hf\s*(\d{3})/i), null),
  };
}

// Was das Dashboard aus dem Block-Eintrag gelesen hat, und Widersprueche darin. Der Text steuert Deckel, Testband und
// Erkennung der Blocktests; ein Tippfehler dort verfaelscht still die Auswertung (z. B. Deckel 125 statt 128).
export function checkBlockText(text, targets = parseBlockTargets(text)) {
  const t = String(text ?? "");
  const hints = [];
  const limits = [...new Set([...t.matchAll(/(?:unter|≤|<=|<|bis)\s*hf\s*(\d{3})/gi)].map((m) => Number(m[1])))].sort((a, b) => a - b);
  if (limits.length > 1) hints.push(`Verschiedene HF-Grenzen im Text: ${limits.join(", ")} – als Deckel gezählt wird ${targets.hrCap ?? "keine"} (Zeile „Lauf-Grundlage unter HF …“).`);
  const band = targets.run?.band ?? null;
  if (band && targets.hrCap != null && band[1] > targets.hrCap) hints.push(`Testband ${band[0]}–${band[1]} reicht über den HF-Deckel ${targets.hrCap}.`);
  return { hrCap: targets.hrCap ?? null, band, tests: targets.run?.tests ?? [], testsByName: Array.isArray(targets.run?.tests), hints };
}

// Fenster: laeuft der Block, ab Blockstart. Davor (oder bei Block-Ende) zaehlt der "Ausgangswert" der letzten 8 Wochen.
export function windowOf(block, todayIso) {
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

// ---------- Abschlussziele: vergleichbare Einheiten, Schwimmtest, Leitplanken ----------
// Vergleichbar heisst beim Laufen: locker (Grundlage/Long Slow, kein Tempo, kein Rennen), draussen, mind. minMin Minuten,
// flach (hoechstens FLAT_M_PER_KM Hoehenmeter je km) und kein Hitzetag. Auf der Rolle: mind. minMin − 5 Minuten,
// gleichmaessige Leistung (VI ≤ 1,05) und locker (IF ≤ 0,80). Raumtemperatur und Ventilator prueft der Worker nicht,
// die Temperatur des Radcomputers wird nur angezeigt.
export const FLAT_M_PER_KM = 10;
export const HEAT_C = 22;
const RIDE_MAX_VI = 1.05;
const RIDE_MAX_IF = 80;
const RIDE_MIN_SLACK = 5;
const RECOVERY_EVERY = 4; // 3:1 – jede 4. Blockwoche ist Entlastung, die anderen sind Belastungswochen

export function isComparableRun(r, cfg) {
  return Boolean(r.easy) && !r.treadmill && !r.heat && (r.durationMin ?? 0) >= cfg.minMin && (r.elevPerKm == null || r.elevPerKm <= FLAT_M_PER_KM);
}
const RIDE_WATTS_TOL = 0.06; // feste Leistung: Messfenster hoechstens 6 % daneben
export function isComparableRide(r, cfg) {
  const w = r.mainWatts ?? r.watts;
  const wattsOk = !cfg.watts || !Number.isFinite(w) || Math.abs(w - cfg.watts) <= cfg.watts * RIDE_WATTS_TOL;
  return Boolean(r.easy) && Boolean(r.indoor) && wattsOk && (r.durationMin ?? 0) >= cfg.minMin - RIDE_MIN_SLACK && (r.vi == null || r.vi <= RIDE_MAX_VI) && (r.intensity == null || r.intensity <= RIDE_MAX_IF);
}
// Warum ein lockerer Lauf nicht zaehlt (fuer den Hinweis): erster Grund
function runExclusion(r, cfg) {
  if (r.treadmill) return "Laufband";
  if ((r.durationMin ?? 0) < cfg.minMin) return `unter ${cfg.minMin} min`;
  if (r.elevPerKm != null && r.elevPerKm > FLAT_M_PER_KM) return "nicht flach";
  if (r.heat) return "Hitze";
  return null;
}

const fmtSecs = (s) => `${Math.floor(s / 60)}:${String(Math.round(s % 60)).padStart(2, "0")}`;
const countBy = (xs) => Object.entries(xs.reduce((a, x) => ({ ...a, [x]: (a[x] ?? 0) + 1 }), {})).map(([k, n]) => `${n}× ${k}`).join(", ");

function decouplingGoal({ key, sport, label, sessions, cfg, w, basis, comparable, extraNotes = [], extra = {} }) {
  const comp = sessions.filter((x) => inWin(x.date, w) && comparable(x) && Number.isFinite(x.decoupling)).sort((a, b) => a.date.localeCompare(b.date));
  const n = cfg.lastN, last = comp.slice(-n), first = comp.slice(0, n);
  const avg = mean(last.map((x) => x.decoupling));
  const must = cfg.must ?? null;
  const decOk = avg != null && avg <= cfg.max;
  const decMust = avg != null && (must != null ? avg <= must : avg <= cfg.max + 2);
  // Pace: letzte n gegen die ersten n Vergleichslaeufe, erst wenn es mehr als n gibt (sonst waeren es dieselben Laeufe)
  let pace = null;
  if (cfg.pace && comp.length > n) {
    const f = mean(first.map((x) => x.paceSecPerKm).filter(Number.isFinite)), l = mean(last.map((x) => x.paceSecPerKm).filter(Number.isFinite));
    if (f != null && l != null) pace = { first: Math.round(f), last: Math.round(l), ok: l <= f };
  }
  const status = avg == null ? "none" : w.baseline ? "base" : pace?.ok === false ? (decMust ? "warn" : "bad") : statusOf(decOk, decMust);
  const notes = [];
  if (avg != null) notes.push(comp.length < n ? `erst ${comp.length} von ${n} Vergleichseinheiten` : `Ø der letzten ${n}`);
  if (pace) notes.push(`Pace ${fmtSecs(pace.last)} statt ${fmtSecs(pace.first)} /km${pace.ok ? "" : " – langsamer als zu Blockbeginn"}`);
  return {
    key, group: "ziel", sport, label, unit: "%", basis,
    target: `Ø ≤ ${cfg.max} %${must != null && must > cfg.max ? ` (Muss ≤ ${must} %)` : ""} · letzte ${n} vergleichbare${cfg.pace ? " · Pace nicht langsamer" : ""}`,
    value: avg != null ? r1(avg) : null, count: comp.length, max: cfg.max, must, lastN: n, pace,
    series: comp.slice(-10).map((x) => ({ date: x.date, value: r1(x.decoupling), ...(x.paceSecPerKm ? { pace: fmtSecs(x.paceSecPerKm) } : {}), ...(Number.isFinite(x.mainWatts ?? x.watts) ? { watts: Math.round(x.mainWatts ?? x.watts) } : {}), ...(Number.isFinite(x.mainHr) ? { hr: x.mainHr } : {}), ...(x.noWarmup ? { noWarmup: true } : {}) })),
    status,
    // Ziel erfuellt: Muss-Kriterium (ohne Muss der Zielwert) ueber volle n Einheiten, Pace nicht langsamer
    pass: !w.baseline && comp.length >= n && (must != null ? avg <= must : decOk) && pace?.ok !== false,
    note: avg == null ? `Noch keine vergleichbare Einheit mit Decoupling-Wert.` : notes.join(" · "),
    hints: extraNotes.filter(Boolean),
    ...extra,
  };
}

// Schwimmtest (z. B. 2× 500 m) aus den Intervallen einer Einheit: die ersten zwei aufeinanderfolgenden Arbeitsintervalle
// mit ±10 % der Testdistanz. Pause = Abstand zwischen Ende des ersten und Start des zweiten.
export function swimTestFromIntervals(dto, distM) {
  const list = (Array.isArray(dto?.icu_intervals) ? dto.icu_intervals : []).filter((i) => String(i?.type ?? "").toUpperCase() !== "RECOVERY" && Number(i?.distance) > 0);
  const near = (i) => Math.abs(Number(i.distance) - distM) <= distM * 0.1;
  for (let k = 0; k + 1 < list.length; k++) {
    const a = list[k], b = list[k + 1];
    if (!near(a) || !near(b)) continue;
    const t1 = Number(a.moving_time ?? a.elapsed_time), t2 = Number(b.moving_time ?? b.elapsed_time);
    const pause = Number.isFinite(Number(b.start_time)) && Number.isFinite(Number(a.end_time)) ? Number(b.start_time) - Number(a.end_time) : null;
    if (t1 > 0 && t2 > 0) return { t1Sec: Math.round(t1 * (distM / Number(a.distance))), t2Sec: Math.round(t2 * (distM / Number(b.distance))), pauseSec: pause != null && pause >= 0 ? Math.round(pause) : null, source: "intervalle" };
  }
  return null;
}
// Oder als Text in der Beschreibung/im Namen: "2x500: 9:40 / 9:52, Pause 45 s" (erste zwei Zeiten = die zwei Strecken)
export function swimTestFromText(text, distM) {
  const t = String(text ?? "");
  if (!new RegExp(`(?<!\\d)${distM}(?!\\d)`).test(t)) return null;
  const body = t.replace(/\d\s*[×x]\s*\d{2,4}\s*m?/gi, " ");
  const pm = body.match(/pause\s*:?\s*(?:(\d{1,2}):(\d{2})|(\d+)\s*(s|sek|sekunden|min))/i);
  const rest = pm ? body.replace(pm[0], " ") : body;
  const times = [...rest.matchAll(/\b(\d{1,2}):(\d{2})\b/g)].map((m) => Number(m[1]) * 60 + Number(m[2]));
  if (times.length < 2) return null;
  const pauseSec = pm ? (pm[1] != null ? Number(pm[1]) * 60 + Number(pm[2]) : /min/i.test(pm[4]) ? Number(pm[3]) * 60 : Number(pm[3])) : null;
  return { t1Sec: times[0], t2Sec: times[1], pauseSec, source: "text" };
}

function swimGoal(tests, cfg, w, basis) {
  const per100 = cfg.distM / 100;
  const list = tests.filter((x) => inWin(x.date, w) && x.t1Sec > 0 && x.t2Sec > 0).sort((a, b) => a.date.localeCompare(b.date)).map((x) => {
    const slower = (x.t2Sec - x.t1Sec) / per100;
    const pauseOk = x.pauseSec == null ? null : x.pauseSec <= cfg.maxPauseSec;
    return { ...x, slowerPer100: r1(slower), pauseOk, pass: slower <= cfg.maxSlowerPer100 && pauseOk !== false };
  });
  const latest = list[list.length - 1] ?? null;
  const anyPass = list.some((x) => x.pass);
  return {
    key: "swimTest", group: "ziel", sport: "swim", label: "Schwimmen: Dauer, saubere Technik", unit: "s/100 m", basis,
    target: `${cfg.reps}× ${cfg.distM} m · Pause ≤ ${cfg.maxPauseSec / 60} min · 2. höchstens +${cfg.maxSlowerPer100} s/100 m`,
    value: latest ? latest.slowerPer100 : null, count: list.length, maxSlowerPer100: cfg.maxSlowerPer100,
    tests: list.slice(-6).map((x) => ({ date: x.date, t1: fmtSecs(x.t1Sec), t2: fmtSecs(x.t2Sec), pauseSec: x.pauseSec, slowerPer100: x.slowerPer100, pass: x.pass })),
    status: !latest ? "none" : w.baseline ? "base" : statusOf(latest.pass, anyPass),
    pass: !w.baseline && anyPass,
    note: !latest ? `Noch kein Test (${cfg.reps}× ${cfg.distM} m) – Zeiten in die Beschreibung schreiben, z. B. „${cfg.reps}x${cfg.distM}: 9:40 / 9:50, Pause 45 s“.`
      : `${fmtSecs(latest.t1Sec)} / ${fmtSecs(latest.t2Sec)}${latest.pauseSec != null ? ` · Pause ${latest.pauseSec} s` : " · Pause unbekannt"}${anyPass && !latest.pass ? " · früher schon bestanden" : ""}`,
  };
}

const weeksOf = (block, w, todayIso) => {
  const out = [];
  for (let s = w.from, i = 0; s <= w.to; s = addDays(s, 7), i++) {
    const e = addDays(s, 6);
    out.push({ start: s, end: e, idx: i, done: e <= w.to && e < todayIso, load: w.baseline || (i + 1) % RECOVERY_EVERY !== 0 });
  }
  return out;
};

function rampGoal(ctlDays, cfg, block, w, basis, todayIso) {
  const ctl = Object.fromEntries(ctlDays.filter((x) => Number.isFinite(x.ctl)).map((x) => [x.date, x.ctl]));
  const at = (d) => { for (let k = 0; k < 4; k++) { const v = ctl[addDays(d, -k)]; if (v != null) return v; } return null; };
  const weeks = weeksOf(block, w, todayIso).map((x) => {
    const a = at(addDays(x.start, -1)), b = at(x.done ? x.end : todayIso);
    return { start: x.start, load: x.load, done: x.done, ramp: a != null && b != null ? r1(b - a) : null };
  });
  const full = weeks.filter((x) => x.done && x.load && x.ramp != null);
  const over = full.filter((x) => x.ramp > cfg.max);
  const maxRamp = full.length ? Math.max(...full.map((x) => x.ramp)) : null;
  const open = weeks.find((x) => !x.done);
  // Wie viel Wochenlast die Grenze erlaubt: CTL steigt je Woche um etwa (Tageslast − CTL) × (1 − e^(−7/42)).
  // Bei niedriger CTL greift die Grenze erst bei einem Vielfachen der Erhaltungslast (7 × CTL).
  const ctlNow = at(todayIso);
  const allowed = ctlNow > 0 ? 1 + cfg.max / ((1 - Math.exp(-7 / 42)) * ctlNow) : null;
  return {
    hints: allowed != null && allowed > 1.5 ? [`Bei CTL ${Math.round(ctlNow)} erlaubt Ramp ${cfg.max} etwa das ${String(Math.round(allowed * 10) / 10).replace(".", ",")}-Fache der Erhaltungslast (7 × CTL) – die Regel „höchstens +10 % pro Woche“ greift viel früher.`] : [],
    key: "ramp", group: "leitplanke", label: "Ramp Rate", unit: "CTL/Woche", basis,
    target: `≤ ${cfg.max} je Belastungswoche`, value: maxRamp, max: cfg.max, weeks: weeks.slice(-12),
    status: !full.length ? "none" : w.baseline ? "base" : over.length ? "bad" : "ok",
    note: [full.length ? `höchste ${String(maxRamp).replace(".", ",")}${over.length ? ` · ${over.length}× über ${cfg.max}` : ""}` : "Noch keine abgeschlossene Belastungswoche.", open?.ramp != null ? `diese Woche bisher ${open.ramp > 0 ? "+" : ""}${String(open.ramp).replace(".", ",")}` : null].filter(Boolean).join(" · "),
  };
}

function pauseGoal(activityDays, breaks, cfg, w, basis, todayIso) {
  const days = [...new Set(activityDays.filter((d) => inWin(d, { from: addDays(w.from, -1), to: w.to })))].sort();
  const planned = (from, to) => breaks.some((b) => b.from <= to && b.to >= from);
  const gaps = [];
  let prev = addDays(w.from, -1);
  // Der laufende Tag zaehlt nicht als Luecke (es kann noch trainiert werden)
  const end = w.to >= todayIso ? todayIso : addDays(w.to, 1);
  for (const d of [...days.filter((x) => x < end), end]) {
    const len = diffDays(prev, d) - 1;
    if (len > 0) gaps.push({ from: addDays(prev, 1), to: addDays(d, -1), days: len, open: d === end });
    prev = d;
  }
  const unplanned = gaps.filter((g) => !planned(g.from, g.to));
  const longest = unplanned.reduce((m, g) => Math.max(m, g.days), 0);
  const broken = unplanned.filter((g) => g.days > cfg.maxDays);
  const openGap = unplanned.find((g) => g.open);
  return {
    key: "pause", group: "leitplanke", label: "Keine lange Pause", unit: "Tage", basis,
    target: `keine ungeplante Pause über ${cfg.maxDays} Tage`, value: longest, max: cfg.maxDays,
    status: w.baseline ? "base" : broken.length ? "bad" : openGap && openGap.days >= cfg.maxDays - 2 ? "warn" : "ok",
    note: [`längste Lücke ${longest} Tag${longest === 1 ? "" : "e"}`, openGap ? `seit ${openGap.days} Tag${openGap.days === 1 ? "" : "en"} kein Training` : null, broken.length ? `${broken.length}× über ${cfg.maxDays} Tage` : null].filter(Boolean).join(" · "),
    hints: ["Geplante Pausen als Notiz „Pause“ oder „Urlaub“ in den Kalender eintragen, dann zählen sie nicht."],
  };
}

// Intensitaets-Leitplanke: je Belastungswoche ab fromWeek muessen alle Arten (Rad-HIIT, Huegelsprints) mind. 1× vorkommen,
// in need von of Wochen. Wochen mit Krankheit (Kalender) zaehlen nicht als verfehlt, wenn der Zieltext das sagt.
const BIKE_HIIT = /hiit|\bhit\b|30\s*\/\s*30|\b\d\s*[x×]\s*4\s*min|vo2/i;
const HILL_SPRINT = /hügel|huegel|\bhill|berg\s*-?sprint|\bsprints?\b/i;
const INT_KIND_DE = { bikeHiit: "Rad-HIIT", hillSprints: "Hügelsprints" };
// sport "bike"/"run", text: Name/Beschreibung der Einheit samt geplantem Workout. Rad ohne Stichwort: mind. 4 min in Z5 (kein Test).
export function intensityKindOf(sport, text, z5Secs = null) {
  if (sport === "bike" && (BIKE_HIIT.test(text) || (z5Secs != null && z5Secs >= 240 && !/ftp|rampe|test/i.test(text)))) return "bikeHiit";
  if (sport === "run" && HILL_SPRINT.test(text)) return "hillSprints";
  return null;
}

function intensityGoal(sessions, sickDays, cfg, block, w, basis, todayIso) {
  const sick = new Set(sickDays);
  const weeks = weeksOf(block, w, todayIso).map((x) => {
    const inWeek = sessions.filter((y) => y.date >= x.start && y.date <= x.end);
    const has = Object.fromEntries(cfg.kinds.map((k) => [k, inWeek.filter((y) => y.kind === k).length]));
    const hit = cfg.kinds.every((k) => has[k] > 0);
    let ill = false;
    for (let d = x.start; d <= x.end; d = addDays(d, 1)) if (sick.has(d)) ill = true;
    const early = !w.baseline && x.idx + 1 < cfg.fromWeek;
    return { start: x.start, done: x.done, load: x.load && !early, skip: early ? `Woche ${x.idx + 1}, zählt erst ab Woche ${cfg.fromWeek}` : x.load ? null : "Entlastungswoche, zählt nicht", has, hit, sick: ill };
  });
  // Krankheitswoche ohne Ziel faellt heraus; mit Ziel zaehlt sie normal
  const full = weeks.filter((x) => x.done && x.load && (x.hit || !(cfg.sickOk && x.sick)));
  const hits = full.filter((x) => x.hit).length;
  const allowed = Math.max(0, cfg.of - cfg.need), misses = full.length - hits;
  const open = weeks.find((x) => !x.done) ?? null;
  const what = cfg.kinds.map((k) => INT_KIND_DE[k]).join(" und ");
  return {
    key: "intensity", group: "leitplanke", label: "Intensität", unit: "Wochen", basis,
    target: `${what} je 1× in ${cfg.need} von ${cfg.of} Belastungswochen${cfg.fromWeek > 1 ? ` ab Woche ${cfg.fromWeek}` : ""}`,
    value: full.length ? hits : null, of: full.length, needWeeks: cfg.need, ofWeeks: cfg.of, kinds: cfg.kinds, weeks: weeks.slice(-12),
    status: w.baseline ? (full.length ? "base" : "none") : !full.length ? "none" : misses > allowed ? "bad" : misses === allowed && allowed > 0 ? "warn" : "ok",
    note: [open ? (open.skip ? `Diese Woche: ${open.skip}` : `Diese Woche: ${cfg.kinds.map((k) => `${INT_KIND_DE[k]} ${open.has[k] ? "✓" : "offen"}`).join(", ")}`) : null, misses ? `${misses} von ${allowed} erlaubten Fehlwochen` : null, weeks.some((x) => x.done && x.sick && !x.hit && cfg.sickOk) ? "Krankheitswoche ausgenommen" : null].filter(Boolean).join(" · ") || null,
    hints: cfg.sickOk ? ["Krankheit als Kalendereintrag „Krank“ (oder Kategorie Krankheit/Verletzung) eintragen, dann zählt die Woche nicht als verfehlt."] : [],
  };
}

// ---------- Lauf mit Blocktests: Basis (Decoupling 0–60 min) und Haltbarkeit (stabile Dauer) ----------
// Wann ein Blocktest ist, entscheidet der Nutzer: Ein Lauf zaehlt nur als Test, wenn er so heisst ("Blocktest"/"Basistest"
// im Namen oder in der Beschreibung der Einheit bzw. des gekoppelten Workouts, r.test aus dashboard.js). Ein langer Lauf
// ist nicht automatisch ein Test, auch nicht an einem im Text genannten Termin. Der erste Test im Block ist der Basistest.
// Jeder Lauf traegt seine Auswertung aus den Zeitreihen (analysis, siehe block-inputs.js): dec60, pace60, windows
// [{min, value}], stableMin (0 = unter 60 min). Grundlagenlaeufe ab 60 min ausserhalb der Tests sind nur Trend, nicht Ziel.
// planned: kommende Testtage (geplante Workouts "Blocktest" im Kalender, sonst Termine aus dem Block-Text) – nur Anzeige.
export const TEST_RE = /blocktest|basistest/i;
const PACE_NOISE_SEC = 5; // Pace-Unterschiede unter 5 s/km sind Rauschen
export function testDatesOf(block, tests) {
  const y0 = Number(block.start.slice(0, 4));
  return (tests ?? []).map((md) => { const iso = `${y0}-${md}`; return iso < addDays(block.start, -7) ? `${y0 + 1}-${md}` : iso; });
}
const fmtMin = (m) => (m == null ? "–" : m === 0 ? "unter 60 min" : `${m} min`);
function blocktestGoals(block, runs, cfg, w, basis, todayIso, plannedTests = []) {
  const planned = [...new Set([...plannedTests, ...testDatesOf(block, cfg.tests)])].sort();
  const usable = runs.filter((r) => inWin(r.date, w) && r.easy && !r.treadmill && Number.isFinite(r.analysis?.dec60));
  const marked = usable.filter((r) => r.test || TEST_RE.test(r.name ?? "")).sort((a, b) => a.date.localeCompare(b.date));
  const used = new Set(marked);
  const tests = marked.map((r, i) => {
    const a = r.analysis;
    return { date: r.date, planned: r.date, label: i === 0 ? "Basistest" : `Test ${i}`, dec60: a.dec60, pace60: a.pace60 != null ? fmtSecs(a.pace60) : null, pace60Sec: a.pace60 ?? null, stableMin: a.stableMin, windows: a.windows, durationMin: r.durationMin, overCapMin: a.overCapMin ?? null, heat: Boolean(r.heat), hilly: r.elevPerKm != null && r.elevPerKm > FLAT_M_PER_KM };
  });
  const nextTest = planned.find((p) => p >= todayIso && !tests.some((t) => t.date === p)) ?? null;
  const n = cfg.lastN, last = tests.slice(-n), baseT = tests[0] ?? null, lastT = tests[tests.length - 1] ?? null;
  const avg = mean(last.map((t) => t.dec60));
  const must = cfg.must ?? null;
  const paceOk = baseT && lastT && lastT !== baseT && baseT.pace60Sec != null && lastT.pace60Sec != null ? lastT.pace60Sec <= baseT.pace60Sec + PACE_NOISE_SEC : null;
  const decOk = avg != null && avg <= cfg.max, decMust = avg != null && (must != null ? avg <= must : avg <= cfg.max + 2);
  const trend = usable.filter((r) => !used.has(r) && (r.durationMin ?? 0) >= cfg.minMin);
  const nextTxt = nextTest ? `nächster Test ${nextTest.slice(8, 10)}.${nextTest.slice(5, 7)}.` : null;
  const conds = tests.filter((t) => t.heat || t.hilly).map((t) => `${t.date.slice(8, 10)}.${t.date.slice(5, 7)}. ${t.heat ? "Hitze" : "nicht flach"}`);
  const shared = { group: "ziel", sport: "run", basis, tests, nextTest, count: tests.length, lastN: n };
  const base = {
    key: "runDecoupling", ...shared, label: "Lauf: Basis (0–60 min)", unit: "%",
    target: `Ø ≤ ${cfg.max} %${must != null && must > cfg.max ? ` (Muss ≤ ${must} %)` : ""} · Decoupling 0–60 min der letzten ${n} Blocktests · Pace 0–60 nicht langsamer als im Basistest`,
    value: avg != null ? r1(avg) : null, max: cfg.max, must, pace: paceOk == null ? null : { first: baseT.pace60Sec, last: lastT.pace60Sec, ok: paceOk },
    series: tests.map((t) => ({ date: t.date, value: r1(t.dec60), ...(t.pace60 ? { pace: t.pace60 } : {}) })),
    status: avg == null ? "none" : w.baseline ? "base" : paceOk === false ? (decMust ? "warn" : "bad") : statusOf(decOk, decMust),
    pass: !w.baseline && tests.length >= n && (must != null ? avg <= must : decOk) && paceOk !== false,
    note: avg == null ? `Noch kein Blocktest${nextTxt ? ` – ${nextTxt}` : "."}` : [tests.length < n ? `erst ${tests.length} von ${n} Tests` : `Ø der letzten ${n} Tests`, paceOk === false ? `Pace 0–60 ${lastT.pace60} statt ${baseT.pace60} /km – langsamer als im Basistest` : null, nextTxt].filter(Boolean).join(" · "),
    hints: [trend.length ? `Trend (zählt nicht fürs Ziel): ${trend.length} Grundlagenl${trend.length === 1 ? "auf" : "äufe"} ab ${cfg.minMin} min, Ø Decoupling 0–60 ${String(r1(mean(trend.map((r) => r.analysis.dec60)))).replace(".", ",")} %` : null, conds.length ? `Bedingungen: ${conds.join(", ")}` : null].filter(Boolean),
  };
  // Was der naechste Test fuers Ziel braucht: gemessen wird das Mittel der letzten n Tests
  if (tests.length) {
    const f1 = (v) => String(r1(v)).replace(".", ",");
    const prev = tests.slice(-(n - 1)), sum = prev.reduce((x, t) => x + t.dec60, 0);
    const need = (lim) => lim * n - sum;
    base.hints.push(tests.length < n - 1
      ? `Fürs Ziel zählt das Mittel der letzten ${n} Tests – bisher ${tests.length}.`
      : need(cfg.max) < 0 ? `Mit dem nächsten Test bleibt das Mittel der letzten ${n} über ${cfg.max} %${must != null && need(must) >= 0 ? `; fürs Muss braucht er ≤ ${f1(need(must))} %` : ""}.`
      : `Der nächste Test braucht ≤ ${f1(need(cfg.max))} % fürs Ziel${must != null ? `, ≤ ${f1(need(must))} % fürs Muss` : ""} (Mittel der letzten ${n}).`);
  }
  const grow = baseT && lastT && lastT !== baseT ? lastT.stableMin > baseT.stableMin : null;
  const stable = {
    key: "runStable", ...shared, label: "Lauf: stabile Dauer", unit: "min", stableMax: cfg.stableMax,
    target: `im letzten Blocktest länger als im Basistest · längstes Fenster ab Start (0–60, 0–75 …) mit Decoupling ≤ ${cfg.stableMax} %`,
    value: lastT ? lastT.stableMin : null, baseValue: baseT ? baseT.stableMin : null,
    status: !lastT ? "none" : w.baseline ? "base" : grow == null ? "none" : grow ? "ok" : lastT.stableMin === baseT.stableMin ? "warn" : "bad",
    pass: !w.baseline && grow === true,
    note: !lastT ? `Noch kein Blocktest${nextTxt ? ` – ${nextTxt}` : "."}` : grow == null ? `Basistest: ${fmtMin(baseT.stableMin)} – verglichen wird ab dem nächsten Test${nextTxt ? ` (${nextTxt})` : ""}` : `${fmtMin(lastT.stableMin)} statt ${fmtMin(baseT.stableMin)} im Basistest${nextTxt ? ` · ${nextTxt}` : ""}`,
    hints: [],
  };
  return [base, stable];
}

// input: { longRuns:[{date,decoupling}], strength:[{date,sets?}] (Kraft-Tage, mit Sätzen wenn bekannt), weeklyGoalSets, strengthSource:{egym,egymAt}, acwrDays:[{date,acwr}],
//   runs:[{date,durationMin,decoupling,paceSecPerKm,elevPerKm,heat,easy,treadmill}], rides:[{date,durationMin,decoupling,watts,mainWatts,noWarmup,vi,intensity,indoor,easy,hasHr}],
//   im Blocktest-Modus runs zusaetzlich mit {id,name,analysis} (analyzeRunStreams),
//   swimTests:[{date,t1Sec,t2Sec,pauseSec}], ftpTest:{date}|null, ftp, ctlDays:[{date,ctl}], activityDays:[date], plannedBreaks:[{from,to}],
//   intensity:[{date,kind}] (kind aus intensityKindOf), sickDays:[date] }
export function buildBlockGoals(block, input, todayIso) {
  return buildBlockGoalsFull(block, input, todayIso).goals;
}

export function buildBlockGoalsFull(block, input, todayIso) {
  const targets = parseBlockTargets([block.goal, ...(block.notes ?? [])].filter(Boolean).join("\n\n"));
  const w = windowOf(block, todayIso);
  const basis = w.baseline ? "letzte 8 Wochen" : "seit Blockstart";
  const goals = [];

  if (Array.isArray(targets.run?.tests)) goals.push(...blocktestGoals(block, input.runs ?? [], targets.run, w, basis, todayIso, input.plannedTests ?? []));
  else if (targets.run) {
    const cfg = targets.run;
    const easy = (input.runs ?? []).filter((r) => inWin(r.date, w) && r.easy);
    const excluded = easy.map((r) => runExclusion(r, cfg)).filter(Boolean);
    const noDec = easy.filter((r) => isComparableRun(r, cfg) && !Number.isFinite(r.decoupling)).length;
    goals.push(decouplingGoal({
      key: "runDecoupling", sport: "run", label: "Lauf: aerobe Stabilität", sessions: input.runs ?? [], cfg, w, basis,
      comparable: (r) => isComparableRun(r, cfg),
      extraNotes: [excluded.length ? `nicht vergleichbar: ${countBy(excluded)}` : null, noDec ? `${noDec}× ohne Decoupling-Wert` : null],
    }));
  }

  if (targets.bike) {
    const cfg = targets.bike;
    const cand = (input.rides ?? []).filter((r) => inWin(r.date, w) && isComparableRide(r, cfg));
    const noHr = cand.filter((r) => !Number.isFinite(r.decoupling) && r.hasHr === false).length;
    const g = decouplingGoal({
      key: "bikeDecoupling", sport: "bike", label: "Rad (Rolle): aerobe Stabilität", sessions: input.rides ?? [], cfg, w, basis,
      comparable: (r) => isComparableRide(r, cfg),
      extraNotes: [noHr ? `${noHr}× ohne Puls – ohne Pulsgurt gibt es kein Decoupling` : null],
      extra: { ftp: input.ftp ?? null, ftpTest: input.ftpTest ?? null },
    });
    const last = g.series.slice(-g.lastN);
    // Temperatur des Radcomputers bleibt draussen: der Sensor misst Koerperwaerme mit
    const watts = last.map((x) => x.watts).filter(Number.isFinite);
    const span = (xs, u) => (xs.length ? (Math.min(...xs) === Math.max(...xs) ? `${Math.min(...xs)} ${u}` : `${Math.min(...xs)}–${Math.max(...xs)} ${u}`) : null);
    if (watts.length) g.hints.unshift(`Leistung im Messfenster ${span(watts, "W")}`);
    if (cfg.firstMin && cfg.warmupMin) g.hints.push(`Messfenster: ${cfg.firstMin} min nach ${cfg.warmupMin} min Einrollen${cfg.watts ? `, Leistung ${cfg.watts} W ±${Math.round(RIDE_WATTS_TOL * 100)} %` : ""}`);
    const noWarm = last.filter((x) => x.noWarmup).length;
    if (noWarm) g.hints.push(`${noWarm}× ohne Einrollen gemessen (Fenster ab Minute 0)`);
    if (cfg.ftpTest && !w.baseline) g.hints.push(input.ftpTest ? `FTP-Test am ${input.ftpTest.date.slice(8, 10)}.${input.ftpTest.date.slice(5, 7)}.${input.ftp ? ` · FTP ${input.ftp} W` : ""}` : (block.weekNo ?? 1) > 1 || block.status === "done" ? "Kein FTP-Test in Woche 1 gefunden." : "FTP-Test in Woche 1 steht an.");
    goals.push(g);
  }

  if (targets.swim) goals.push(swimGoal(input.swimTests ?? [], targets.swim, w, basis));


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
    // Wochenziel in Sätzen (EGYM). Ohne Satzzahlen (kein EGYM) zählen Kraft-Tage, dann gilt STRENGTH_DAYS_PER_WEEK.
    // Leitplanke "8 von 9 Belastungswochen": Entlastungswochen (jede 4. Blockwoche) zählen nicht; es darf höchstens
    // (of − need) Belastungswochen ohne Ziel geben. Ziel über mehrere Wochen ("≥90 Sätze in 2 Wochen"): feste Abschnitte ab
    // Blockstart (Woche 1–2, 3–4, …); weeks sind dann diese Abschnitte, und jeder abgeschlossene muss das Ziel erreichen.
    const st = targets.strength, perLoad = st.need != null, P = st.periodWeeks > 1 ? st.periodWeeks : 1;
    const goalSets = st.sets ? st.sets : Number.isFinite(input.weeklyGoalSets) && input.weeklyGoalSets > 0 ? input.weeklyGoalSets : DEFAULT_WEEKLY_SETS;
    const setsMode = (input.strength ?? []).some((x) => Number.isFinite(x.sets));
    const need = setsMode ? goalSets : STRENGTH_DAYS_PER_WEEK;
    const countIn = (from, to) => {
      const xs = (input.strength ?? []).filter((y) => y.date >= from && y.date <= to);
      return setsMode ? xs.reduce((a, y) => a + (Number.isFinite(y.sets) ? y.sets : 0), 0) : new Set(xs.map((y) => y.date)).size;
    };
    const bw = weeksOf(block, w, todayIso);
    const spans = P > 1 ? Array.from({ length: Math.ceil(bw.length / P) }, (_, i) => bw.slice(i * P, i * P + P)).map((c) => ({ start: c[0].start, end: addDays(c[0].start, 7 * P - 1), done: c.length === P && c[c.length - 1].done, load: true })) : bw;
    const weeks = spans.map((x) => {
      const count = countIn(x.start, x.end);
      return { start: x.start, count, done: x.done, hit: count >= need, load: perLoad ? x.load : true, ...(P > 1 ? { end: x.end } : {}) };
    });
    const full = weeks.filter((x) => x.done && x.load);
    const hits = full.filter((x) => x.hit).length;
    const openWeek = weeks.find((x) => !x.done) ?? null;
    // Kalenderwoche (Mo–So) bis heute, wie die Kraft-Karte im Dashboard; die Blockwochen beginnen am Starttag des Blocks
    const monday = mondayOnOrBefore(todayIso);
    // Mit Abschnitten: der laufende Abschnitt bis heute
    const curSpan = P > 1 ? weeks.find((x) => x.start <= todayIso && todayIso <= x.end) : null;
    const calWeek = curSpan ? { start: curSpan.start, end: curSpan.end, count: countIn(curSpan.start, todayIso) } : { start: monday, count: countIn(monday, todayIso) };
    const unit = setsMode ? "Sätze" : "× Kraft";
    const what = setsMode ? `≥ ${need} Sätze` : `${need}× Kraft`;
    let status, target;
    if (P > 1) {
      target = `${what} je ${P} Wochen (ab Blockstart)`;
      status = w.baseline ? (full.length ? "base" : "none") : !full.length ? "none" : hits === full.length ? "ok" : "bad";
    } else if (perLoad) {
      const allowed = Math.max(0, st.of - st.need), misses = full.length - hits;
      target = `${what} in ${st.need} von ${st.of} Belastungswochen`;
      status = w.baseline ? (full.length ? "base" : "none") : !full.length ? "none" : misses > allowed ? "bad" : misses === allowed && allowed > 0 ? "warn" : "ok";
    } else {
      target = `${what} in ${Math.round(st.share * 100)} % der Wochen`;
      status = w.baseline ? (full.length ? "base" : "none") : !full.length ? "none" : statusOf(hits / full.length >= st.share, hits / full.length >= st.share - 0.15);
    }
    goals.push({
      key: "strength", ...(perLoad || P > 1 ? { group: "leitplanke" } : {}), label: perLoad || P > 1 ? "Kraft" : "Kraft-Konsistenz", target, unit: "Wochen",
      value: full.length ? hits : null, of: full.length, basis, weeks: weeks.slice(-12), calWeek, goalSets: setsMode ? need : null, weekGoal: need, countUnit: unit,
      ...(perLoad ? { needWeeks: st.need, ofWeeks: st.of } : {}), ...(P > 1 ? { periodWeeks: P } : {}),
      status,
      egym: Boolean(input.strengthSource?.egym), egymAt: input.strengthSource?.egymAt ?? null,
      note: [openWeek ? `${P > 1 ? "Dieser Abschnitt" : "Diese Woche"}: ${openWeek.count} von ${need}${setsMode ? " Sätzen" : ""}${openWeek.load ? "" : " (Entlastungswoche)"}` : null, perLoad && full.length > hits ? `${full.length - hits} von ${Math.max(0, st.of - st.need)} erlaubten Fehlwochen` : null].filter(Boolean).join(" · ") || null,
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

  if (targets.ramp) goals.push(rampGoal(input.ctlDays ?? [], targets.ramp, block, w, basis, todayIso));
  if (targets.intensity) goals.push(intensityGoal(input.intensity ?? [], input.sickDays ?? [], targets.intensity, block, w, basis, todayIso));
  if (targets.pause) goals.push(pauseGoal(input.activityDays ?? [], input.plannedBreaks ?? [], targets.pause, w, basis, todayIso));

  for (const g of goals) if (g.group === "ziel") g.course = goalCourse(g, w);
  return { goals };
}

// Kurs der Abschlussziele: Erster Messwert → letzter → Ziel, dazu die Richtung (besser/schlechter/gleich) und die nächste
// Messung. Nur der Verlauf, keine Stufe und keine Entscheidung (die trifft der Coach). Hauptkurve ist die Leistung bei gleichem
// Puls bzw. der Puls bei gleicher Leistung: Unter einem strengen HF-Deckel ist das Decoupling oft schon niedrig, der
// Fortschritt zeigt sich dann im Tempo (Lauf, Pace 0–60 im HF-Band) bzw. im Puls bei fester Leistung (Rad).
// open = noch kein Vergleich (unter 2 Messungen).
function goalCourse(g, w) {
  const dirOf = (a, b, lowerBetter = true) => (a == null || b == null ? null : a === b ? "flat" : (b < a) === lowerBetter ? "better" : "worse");
  const out = (open, extra) => ({ open, baseline: Boolean(w?.baseline), ...extra });
  if (g.key === "runDecoupling" || g.key === "bikeDecoupling") {
    const s = g.series ?? [];
    const next = g.key === "runDecoupling" ? (g.nextTest ?? null) : null;
    const nextText = g.key === "runDecoupling" ? (g.nextTest ? `nächster Blocktest ${g.nextTest.slice(8, 10)}.${g.nextTest.slice(5, 7)}.` : g.tests ? null : "nächster langer Grundlagenlauf") : "nächste Rolle mit Messfenster";
    if (!s.length) return out(true, { from: null, to: null, target: g.max, unit: "%", dir: null, main: null, next, nextText });
    const first = s[0], last = s[s.length - 1];
    let main = null, mainDir = null;
    if (g.key === "runDecoupling" && g.tests?.length) {
      const a = g.tests[0], b = g.tests[g.tests.length - 1];
      if (a.pace60 && b.pace60) { main = `Pace 0–60 ${a.pace60} → ${b.pace60} /km`; mainDir = a === b ? null : dirOf(a.pace60Sec, b.pace60Sec); }
    } else if (g.key === "bikeDecoupling") {
      const hs = s.filter((x) => Number.isFinite(x.hr));
      if (hs.length) { const a = hs[0], b = hs[hs.length - 1]; main = `Puls bei ${b.watts ?? "fester"} W ${a.hr} → ${b.hr}`; mainDir = a === b ? null : dirOf(a.hr, b.hr); }
    }
    const dir = s.length > 1 ? dirOf(first.value, g.value) : null;
    return out(s.length < 2, { from: first.value, to: g.value, target: g.max, unit: "%", dir, main, mainDir, next, nextText });
  }
  if (g.key === "runStable") {
    const n = g.tests?.length ?? 0;
    const nextText = g.nextTest ? `nächster Blocktest ${g.nextTest.slice(8, 10)}.${g.nextTest.slice(5, 7)}.` : null;
    if (n < 2) return out(true, { from: g.baseValue ?? null, to: null, target: "länger als Basistest", unit: "min", dir: null, next: g.nextTest ?? null, nextText });
    const dir = dirOf(g.baseValue, g.value, false);
    return out(false, { from: g.baseValue, to: g.value, target: "länger als Basistest", unit: "min", dir, next: g.nextTest ?? null, nextText });
  }
  if (g.key === "swimTest") {
    const t = g.tests ?? [];
    if (!t.length) return out(true, { from: null, to: null, target: g.maxSlowerPer100, unit: "s/100 m", dir: null, nextText: "nächster Schwimmtest" });
    const a = t[0], b = t[t.length - 1], dir = t.length > 1 ? dirOf(a.slowerPer100, b.slowerPer100) : null;
    return out(false, { from: a.slowerPer100, to: b.slowerPer100, target: g.maxSlowerPer100, unit: "s/100 m", dir, nextText: b.pass ? null : "nächster Schwimmtest" });
  }
  return null;
}
