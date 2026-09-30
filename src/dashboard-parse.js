// Tolerantes Auslesen der Freitext-Kommentare für das Dashboard. Rohtexte verlassen den Worker
// nicht: Es gehen nur die erkannten Felder (Heißhunger) bzw. kurze Textausschnitte (Hüfte) raus.

const clean = (s) => (s == null ? null : String(s).replace(/\s+/g, " ").trim().replace(/[.\s]+$/, "") || null);

// Beispiel: "HH 16:30, Stärke 4, Schokolade, davor 12:00 Salat, Auslöser Müdigkeit".
// Mehrere Einträge pro Kommentar beginnen jeweils mit "HH". Was sich nicht lesen lässt, bleibt null
// (der Eintrag wird trotzdem gezählt, damit nichts stillschweigend verschwindet).
export function parseCravings(dateIso, comment) {
  const text = String(comment ?? "");
  const starts = [...text.matchAll(/(?:^|[^\p{L}\d])(HH)(?![\p{L}\d])/giu)].map((m) => m.index + m[0].toLowerCase().indexOf("hh"));
  return starts.map((from, i) => {
    const chunk = text.slice(from, starts[i + 1] ?? text.length);
    const t = chunk.match(/HH\s*[:\-]?\s*(\d{1,2})(?:\s*[:.h]\s*(\d{2}))?/i);
    let hour = null;
    let time = null;
    if (t) {
      const h = Number(t[1]);
      const m = t[2] != null ? Number(t[2]) : 0;
      if (h >= 0 && h <= 24 && m >= 0 && m < 60) {
        hour = h + m / 60;
        time = `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}`;
      }
    }
    const s = chunk.match(/St[äa]rke\s*[:=]?\s*(\d{1,2})/i);
    const trigger = clean(chunk.match(/Ausl[öo]ser\s*[:=]?\s*([^,;\n]+)/i)?.[1]);
    const before = clean(chunk.match(/davor\s*[:=]?\s*([^,;\n]+)/i)?.[1]);
    const what = chunk
      .replace(/HH\s*[:\-]?\s*\d{1,2}(?:\s*[:.h]\s*\d{2})?/i, "")
      .replace(/St[äa]rke\s*[:=]?\s*\d{1,2}/i, "")
      .replace(/davor\s*[:=]?\s*[^,;\n]+/i, "")
      .replace(/Ausl[öo]ser\s*[:=]?\s*[^,;\n]+/i, "")
      .split(/[,;\n]/)
      .map(clean)
      .find(Boolean) ?? null;
    return { date: dateIso, time, hour, strength: s ? Number(s[1]) : null, what, before, trigger };
  });
}

const HIP_TERM = /(h[üu]e?ft\w*|leiste\w*|knie\w*|\bhip\b|groin)/gi;
const HIP_DRILL = /^(kniebeuge\w*|kniehebe\w*|knieheben|kniestand|knieend\w*)$/i;
const NEGATION = /(kein|keine|keinen|keiner|ohne|nicht|schmerzfrei)\W+(?:\w+\W+){0,2}$/i;

// Hinweise auf Hüft-, Leisten- oder Knieprobleme. Bewusst grob: Wörter wie Kniebeuge und
// Verneinungen ("kein Knieschmerz") werden übergangen, alles andere wird mit Textausschnitt gezeigt.
export function findHipFlags(text) {
  const s = String(text ?? "");
  const out = [];
  for (const m of s.matchAll(HIP_TERM)) {
    if (HIP_DRILL.test(m[0])) continue;
    if (NEGATION.test(s.slice(Math.max(0, m.index - 30), m.index).split(/[,;.!\n]/).pop())) continue;
    const from = Math.max(0, m.index - 50);
    out.push({ term: m[0], snippet: clean(s.slice(from, Math.min(s.length, m.index + m[0].length + 50))) });
  }
  return out;
}

// Zerlegt ein geplantes Workout in Blöcke [{ secs, pct }] für die Grafik (Breite = Dauer, Höhe = Intensität).
// Bevorzugt das strukturierte workout_doc von Intervals.icu, sonst der Beschreibungstext im
// Intervals-Workout-Format: "-15m 75% Pace", "3x" gefolgt von "-"-Zeilen bis zur Leerzeile.
// Fließtext wird ignoriert. Distanz-Schritte (km) werden grob mit 6:00 min/km in Zeit umgerechnet.
const ZONE_PCT = { 1: 55, 2: 70, 3: 85, 4: 98, 5: 110, 6: 125, 7: 150 };
const MAX_BLOCKS = 200;

function durationSecs(text) {
  let secs = 0;
  let found = false;
  for (const m of text.matchAll(/(\d+(?:[.,]\d+)?)\s*(h|min|mtr|km|m|s)(?![a-z])/gi)) {
    const v = Number(m[1].replace(",", "."));
    const u = m[2].toLowerCase();
    found = true;
    if (u === "h") secs += v * 3600;
    else if (u === "m" || u === "min") secs += v * 60;
    else if (u === "s") secs += v;
    else if (u === "km") secs += v * 360;
    else if (u === "mtr") secs += (v / 1000) * 360;
  }
  return found ? secs : null;
}

function intensityPct(text) {
  const range = text.match(/(\d+(?:[.,]\d+)?)(?:\s*-\s*(\d+(?:[.,]\d+)?))?\s*%/);
  if (range) {
    const a = Number(range[1].replace(",", "."));
    const b = range[2] != null ? Number(range[2].replace(",", ".")) : a;
    return (a + b) / 2;
  }
  const z = text.match(/\bZ([1-7])\b/i);
  return z ? ZONE_PCT[Number(z[1])] : null;
}

function flattenDoc(steps, out, depth = 0) {
  for (const s of Array.isArray(steps) ? steps : []) {
    if (out.length >= MAX_BLOCKS || depth > 3) return;
    if (Array.isArray(s?.steps) && s.steps.length) {
      const reps = Math.min(Math.max(Math.round(Number(s.reps) || 1), 1), 50);
      for (let i = 0; i < reps; i++) flattenDoc(s.steps, out, depth + 1);
    } else {
      const secs = Number(s?.duration);
      const t = s?.pace ?? s?.power ?? s?.hr ?? s?.target;
      const value = t?.value != null ? Number(t.value) : t?.start != null && t?.end != null ? (Number(t.start) + Number(t.end)) / 2 : null;
      const pct = value != null && String(t?.units ?? "").includes("%") ? value : null;
      if (Number.isFinite(secs) && secs > 0) out.push({ secs: Math.round(secs), pct: Number.isFinite(pct) ? pct : null });
    }
  }
}

export function parseWorkoutSteps(description, workoutDoc) {
  const fromDoc = [];
  flattenDoc(workoutDoc?.steps, fromDoc);
  if (fromDoc.length) return fromDoc;

  const out = [];
  const lines = String(description ?? "").split(/\r?\n/);
  let group = null; // { reps, steps }
  const flush = () => {
    if (group) for (let i = 0; i < group.reps && out.length < MAX_BLOCKS; i++) out.push(...group.steps);
    group = null;
  };
  for (const raw of lines) {
    const line = raw.trim();
    if (!line) { flush(); continue; }
    const rep = line.match(/^(\d{1,2})\s*x\b\s*(.*)$/i);
    if (rep && !rep[2].startsWith("-")) { flush(); group = { reps: Math.min(Number(rep[1]), 50), steps: [] }; continue; }
    const step = line.match(/^-\s*(.+)$/);
    if (!step) continue;
    const secs = durationSecs(step[1]);
    if (!secs || secs <= 0) continue;
    const block = { secs: Math.round(secs), pct: intensityPct(step[1]) };
    if (group) group.steps.push(block);
    else out.push(block);
  }
  flush();
  return out.slice(0, MAX_BLOCKS);
}
