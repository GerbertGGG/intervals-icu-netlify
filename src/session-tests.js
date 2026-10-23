// Test-Erkennung aus der tatsaechlich absolvierten Aktivitaet (nicht aus dem Namen oder dem geplanten Workout):
//   runBlocktest – Run am Blocktest-Termin (±testDateTolDays), mindestens runMinMin; mehrere im Fenster: der laengste.
//   runTrend     – Run, Bewegungszeit ≥ runMinMin, Ø-HF unter dem HF-Deckel des Blocks, kein Blocktest.
//   bikeCompare  – Ride/VirtualRide: Einrollen (icu_warmup_time, sonst angenommen), danach Messfenster bikeWindowMin mit
//                  Ø-Leistung = Vergleichsleistung ± bikeWattsTol und VI ≤ bikeMaxVi. Intervalle/HIIT fallen ueber VI und
//                  Leistung heraus. Beim Rad gibt es keine Blocktests.
// Termine, HF-Deckel und Vergleichsleistung kommen aus dem SEASON_START-Eintrag (testConfigOf), sonst aus TEST_FALLBACK.
// Der Name wirkt nur als Override fuer Grenzfaelle (nameOverride). Jede Einheit traegt testReason.

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const addDays = (iso, n) => new Date(new Date(iso + "T00:00:00Z").getTime() + n * 86400000).toISOString().slice(0, 10);
const daysApart = (a, b) => Math.abs(Math.round((Date.parse(a + "T00:00:00Z") - Date.parse(b + "T00:00:00Z")) / 86400000));
const ddmm = (iso) => `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
const de = (v, d = 0) => String(Math.round(v * 10 ** d) / 10 ** d).replace(".", ",");

export const TEST_RULES = {
  testDateTolDays: 2, // Blocktest verschoben (Ampel rot, krank)
  runMinMin: 60, // Blocktest und Trendlauf
  bikeWindowMin: 60, // Messfenster nach dem Einrollen
  bikeAssumedWarmupSec: 600, // icu_warmup_time = 0: die ersten 10 min gelten als Einrollen
  bikeMainSlackSec: 60, // Messtoleranz der Bewegungszeit beim Hauptteil
  bikeWattsTol: 5, // W um die Vergleichsleistung
  bikeMaxVi: 1.05,
  decTotalTolMin: 5, // Intervals-Decoupling nur bei Gesamtdauer ≈ Einrollen + Messfenster
  warmupMatchTolSec: 60, // und icu_warmup_time ≈ Einrolldauer
};
export const TEST_FALLBACK = { testDates: ["2026-10-23", "2026-11-08", "2026-12-06", "2027-01-03"], hrCap: 128, bikeWatts: 165 };
export const TEST_CATEGORY_DE = { runBlocktest: "Lauf-Blocktest", runTrend: "Lauf-Trend", bikeCompare: "Rad-Vergleich" };

// Override ueber den Namen: "kein Test" schliesst aus, "Blocktest"/"Basistest" erzwingt den Lauf-Blocktest, "Vergleich" den
// Rad-Vergleich. Gilt nur fuer die passende Sportart (ein Rad heisst nie Blocktest).
export function nameOverride(name) {
  const n = String(name ?? "");
  if (/kein\s*test/i.test(n)) return "exclude";
  if (/blocktest|basistest/i.test(n)) return "runBlocktest";
  if (/vergleich/i.test(n)) return "bikeCompare";
  return null;
}
export const isTestRun = (a) => String(a?.type ?? "").toLowerCase() === "run";
export const isTestRide = (a) => /^(ride|virtualride)$/i.test(String(a?.type ?? ""));

// Termine aus der Zeile mit "Blocktests" (z. B. "Blocktests (langer Lauf): Basistest Fr 23.10., dann So 08.11., …"),
// sonst aus parseBlockTargets (run.tests, "MM-DD"). Jahr: aus dem Text, sonst ab Blockstart (frueher als eine Woche davor: Folgejahr).
export function testConfigOf(text, blockStart, targets = {}, block = null) {
  const t = String(text ?? "");
  const warnings = [];
  const y0 = Number(String(blockStart).slice(0, 4));
  const isoOf = (d, m, y) => {
    const md = `${String(m).padStart(2, "0")}-${String(d).padStart(2, "0")}`;
    if (y) return `${String(y).length === 2 ? 2000 + Number(y) : y}-${md}`;
    const iso = `${y0}-${md}`;
    return iso < addDays(blockStart, -7) ? `${y0 + 1}-${md}` : iso;
  };
  const line = t.split("\n").find((l) => /blocktests?\b/i.test(l) && /\d{1,2}\.\d{1,2}\./.test(l));
  let testDates = line ? [...line.matchAll(/(?<!\d)(\d{1,2})\.(\d{1,2})\.(\d{4}|\d{2}(?!\d))?/g)].map((m) => isoOf(m[1], m[2], m[3])) : [];
  let testSource = "text";
  if (!testDates.length && targets.run?.tests?.length) testDates = targets.run.tests.map((md) => isoOf(Number(md.slice(3)), Number(md.slice(0, 2))));
  if (!testDates.length) {
    testSource = "fallback";
    testDates = TEST_FALLBACK.testDates.filter((d) => d >= addDays(blockStart, -7) && (!block?.end || d <= block.end));
    warnings.push(`Blocktest-Termine nicht im Block-Eintrag gefunden (Abschnitt „Blocktests“), Fallback ${testDates.map(ddmm).join(", ") || "keine"}`);
  }
  const hrCap = num(targets.hrCap) ?? TEST_FALLBACK.hrCap;
  if (num(targets.hrCap) == null) warnings.push(`HF-Deckel nicht im Block-Eintrag gefunden („HF-DECKEL“), Fallback ${TEST_FALLBACK.hrCap}`);
  const bikeWatts = num(targets.bike?.watts) ?? TEST_FALLBACK.bikeWatts;
  if (num(targets.bike?.watts) == null) warnings.push(`Vergleichsleistung nicht im Block-Eintrag gefunden, Fallback ${TEST_FALLBACK.bikeWatts} W`);
  for (const w of warnings) console.warn("test config:", w);
  return { testDates: [...new Set(testDates)].sort(), testSource, hrCap, bikeWatts, warnings };
}

// Laeufe klassifizieren. acts: Roh-Aktivitaeten (alle Sportarten); Ergebnis: Map id -> {category, testReason, testDate?}
export function classifyRuns(acts, cfg, rules = TEST_RULES) {
  const out = new Map();
  const runs = (acts ?? []).filter((a) => isTestRun(a) && a?.id != null);
  const dayOf = (a) => String(a.start_date_local ?? "").slice(0, 10);
  const minOf = (a) => (num(a.moving_time) ?? 0) / 60;
  const ov = (a) => nameOverride(a.name);
  const eligible = runs.filter((a) => ov(a) !== "exclude");
  for (const a of runs) if (ov(a) === "exclude") out.set(a.id, { category: null, testReason: "Name „kein Test“: ausgeschlossen" });
  // Override "Blocktest"/"Basistest": zaehlt immer, belegt den Termin im Fenster
  const forced = eligible.filter((a) => ov(a) === "runBlocktest");
  for (const a of forced) {
    const near = cfg.testDates.find((d) => daysApart(d, dayOf(a)) <= rules.testDateTolDays) ?? null;
    out.set(a.id, { category: "runBlocktest", testDate: near ?? dayOf(a), testReason: `Run, Name „${String(a.name).trim()}“ (Override), ${Math.round(minOf(a))} min` });
  }
  for (const d of cfg.testDates) {
    if (forced.some((a) => daysApart(d, dayOf(a)) <= rules.testDateTolDays)) continue;
    const cand = eligible.filter((a) => !out.has(a.id) && daysApart(d, dayOf(a)) <= rules.testDateTolDays && minOf(a) >= rules.runMinMin);
    if (!cand.length) continue;
    const best = cand.sort((x, y) => minOf(y) - minOf(x))[0];
    const shift = dayOf(best) !== d ? `, ${ddmm(dayOf(best))} statt ${ddmm(d)}` : "";
    out.set(best.id, { category: "runBlocktest", testDate: d, testReason: `Run am Blocktest-Termin ${ddmm(d)} (±${rules.testDateTolDays} Tage${shift}), ${Math.round(minOf(best))} min${cand.length > 1 ? `, längster von ${cand.length}` : ""}` });
  }
  for (const a of eligible) {
    if (out.has(a.id)) continue;
    const hr = num(a.average_heartrate), m = minOf(a);
    if (m >= rules.runMinMin && hr != null && hr < cfg.hrCap) out.set(a.id, { category: "runTrend", testReason: `Run ≥${rules.runMinMin} min (${Math.round(m)} min), Ø-HF ${Math.round(hr)} < ${cfg.hrCap}` });
  }
  return out;
}

// Rad-Messfenster aus den Zeitreihen: [startSec, startSec + windowSec). Ø-Leistung, NP (30-s-Mittel hoch 4) und VI ueber alle
// Proben im Fenster; Puls und Decoupling nur aus Proben mit HF. Decoupling % = (Eff. Haelfte 1 / Eff. Haelfte 2 − 1) × 100,
// Effizienz = Ø-Watt / Ø-HF, Haelften nach Zeit.
export function rideWindowStats(streams, startSec, windowSec) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const w = get("watts"), time = get("time"), hr = get("heartrate");
  if (!Array.isArray(w) || !w.length) return null;
  const tOf = (i) => (Array.isArray(time) && Number.isFinite(time[i]) ? time[i] : i);
  const endSec = startSec + windowSec, midSec = startSec + windowSec / 2;
  const ws = [], halves = [[[], []], [[], []]];
  for (let i = 0; i < w.length; i++) {
    const t = tOf(i);
    if (t < startSec || t >= endSec) continue;
    const p = Number(w[i]) || 0;
    ws.push(p);
    if (Array.isArray(hr) && hr[i] > 40) { const h = halves[t < midSec ? 0 : 1]; h[0].push(p); h[1].push(hr[i]); }
  }
  if (!ws.length) return null;
  const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
  const roll = [];
  for (let i = 0, s = 0; i < ws.length; i++) { s += ws[i]; if (i >= 30) s -= ws[i - 30]; if (i >= 29) roll.push(s / 30); }
  const avg = mean(ws), np = roll.length ? mean(roll.map((x) => x ** 4)) ** 0.25 : avg;
  const eff = (h) => (h[0].length >= 60 ? mean(h[0]) / mean(h[1]) : null);
  const e1 = eff(halves[0]), e2 = eff(halves[1]);
  const hs = [...halves[0][1], ...halves[1][1]];
  const lastT = tOf(w.length - 1);
  return {
    windowMin: Math.round((Math.min(endSec, lastT + 1) - startSec) / 60),
    avgWatts: Math.round(avg), vi: avg > 0 ? Math.round((np / avg) * 100) / 100 : null,
    hr: hs.length >= 60 ? Math.round(mean(hs)) : null,
    decoupling: e1 && e2 ? Math.round((e1 / e2 - 1) * 1000) / 10 : null,
  };
}

// Einrollen und Messfenster einer Fahrt (ohne Zeitreihen). Mit Override "Vergleich" und zu kurzem Hauptteil (z. B. 60 min ohne
// Einrollen wie am 19.10.2026) beginnt das Fenster bei 0 (noWarmup).
export function rideWindowOf(a, rules = TEST_RULES) {
  const icuWarm = num(a?.icu_warmup_time) ?? 0;
  const total = num(a?.moving_time) ?? 0, cool = num(a?.icu_cooldown_time) ?? 0;
  const forced = nameOverride(a?.name) === "bikeCompare";
  let warmSec = icuWarm > 0 ? icuWarm : rules.bikeAssumedWarmupSec, assumed = !(icuWarm > 0), noWarmup = false;
  let mainSec = total - warmSec - cool;
  if (forced && mainSec < rules.bikeWindowMin * 60 - rules.bikeMainSlackSec && icuWarm === 0) { warmSec = 0; assumed = false; noWarmup = true; mainSec = total - cool; }
  return { warmSec, assumed, noWarmup, mainSec, totalSec: total, forced };
}

// Rad klassifizieren. stats: rideWindowStats fuer das Fenster aus rideWindowOf (null = Zeitreihen fehlen).
export function classifyRide(a, stats, cfg, rules = TEST_RULES) {
  if (!isTestRide(a)) return null;
  const ov = nameOverride(a?.name);
  if (ov === "exclude") return { category: null, testReason: "Name „kein Test“: ausgeschlossen" };
  const win = rideWindowOf(a, rules);
  const mainMin = Math.round(win.mainSec / 60);
  const warmTxt = win.noWarmup ? "ohne Einrollen" : win.assumed ? "Einrollen 10 min angenommen (icu_warmup_time 0)" : `Einrollen ${Math.round(win.warmSec / 60)} min`;
  const base = { warmupSec: win.warmSec, warmupAssumed: win.assumed, noWarmup: win.noWarmup, mainMin };
  const longEnough = win.mainSec >= rules.bikeWindowMin * 60 - rules.bikeMainSlackSec;
  if (!win.forced && !longEnough) return { ...base, category: null, testReason: `Ride: Hauptteil ${mainMin} min < ${rules.bikeWindowMin} min (${warmTxt})` };
  if (!stats) return { ...base, category: null, testReason: "Ride: Zeitreihen noch nicht geladen" };
  const wattsOk = Math.abs(stats.avgWatts - cfg.bikeWatts) <= rules.bikeWattsTol, viOk = stats.vi != null && stats.vi <= rules.bikeMaxVi;
  const facts = `Hauptteil ${mainMin} min, ${stats.avgWatts} W, VI ${stats.vi != null ? de(stats.vi, 2) : "–"}`;
  if (!win.forced && !(wattsOk && viOk)) {
    const why = [!wattsOk ? `${stats.avgWatts} W nicht ${cfg.bikeWatts} ±${rules.bikeWattsTol} W` : null, !viOk ? `VI ${stats.vi != null ? de(stats.vi, 2) : "–"} > ${de(rules.bikeMaxVi, 2)}` : null].filter(Boolean).join(", ");
    return { ...base, category: null, mainWatts: stats.avgWatts, vi: stats.vi, testReason: `Ride: ${why} (${facts})` };
  }
  // Decoupling: Intervals-Wert nur, wenn er genau das Messfenster abdeckt; sonst selbst aus den Zeitreihen
  const icuDec = num(a?.decoupling), icuWarm = num(a?.icu_warmup_time) ?? 0;
  const fits = Math.abs(win.totalSec - (win.warmSec + rules.bikeWindowMin * 60)) <= rules.decTotalTolMin * 60 && Math.abs(icuWarm - win.warmSec) <= rules.warmupMatchTolSec;
  const useIcu = fits && icuDec != null;
  const decoupling = useIcu ? Math.round(icuDec * 10) / 10 : stats.decoupling;
  const decSource = decoupling == null ? null : useIcu ? "intervals" : "streams";
  const decTxt = decSource === "intervals" ? "Decoupling von Intervals" : decSource === "streams" ? "Decoupling selbst gerechnet" : "kein Decoupling (Puls fehlt)";
  return {
    ...base, category: "bikeCompare", mainWatts: stats.avgWatts, mainHr: stats.hr, vi: stats.vi, decoupling, decSource,
    testReason: `Ride${win.forced ? `, Name „${String(a.name).trim()}“ (Override)` : ""}: ${facts}, ${warmTxt}, ${decTxt}`,
  };
}
