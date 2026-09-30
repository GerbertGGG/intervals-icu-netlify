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
