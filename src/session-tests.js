// Test-Erkennung aus der tatsaechlich absolvierten Aktivitaet (nicht aus dem Namen oder dem geplanten Workout):
//   runBlocktest – Run am Blocktest-Termin (±testDateTolDays), mindestens runMinMin; mehrere im Fenster: der laengste.
//   runBlocktest – auch jeder andere Run ab runAutoMin (65 min): zaehlt automatisch als Test, ohne vorherige Entscheidung
//                  (5 min Einlaufen, Messfenster Minute 5–65, s. RUN_RULES in block-inputs.js).
//   runTrend     – Run, Bewegungszeit ≥ runMinMin, Ø-HF unter dem HF-Deckel des Blocks, kein Blocktest.
//   bikeCompare  – Ride/VirtualRide laenger als Einrollen + Messfenster (bikeAutoWarmupSec + bikeWindowMin, minus Toleranz):
//                  die ersten 10 min sind automatisch das Einrollen (egal ob Intervals einen Warmup-Block erkennt), danach
//                  Messfenster bikeWindowMin mit
//                  Ø-Leistung = Vergleichsleistung ± bikeWattsTol und VI ≤ bikeMaxVi. Intervalle/HIIT fallen ueber VI und
//                  Leistung heraus. Beim Rad zaehlt "Blocktest" im Namen wie "Vergleich" (Override); fehlt der Warmup-Block, gilt das erste Intervall aus interval_summary als Einrollen.
// Termine, HF-Deckel und Vergleichsleistung kommen aus dem SEASON_START-Eintrag (testConfigOf), sonst aus TEST_FALLBACK.
// Der Name wirkt nur als Override fuer Grenzfaelle (nameOverride). Jede Einheit traegt testReason.
// baseline (nur Rad): Ausgangswert, wird angezeigt, zaehlt aber nicht ins Mittel fuers Blockziel – Fahrt ohne Einrollen
// (z. B. 19.10.2026 nach Krankheit, "nur Orientierung") oder "Ausgangswert" im Namen.
// trend (nur Rad, classifyRideTrend unten): lockere, gleichmaessige Rollenfahrt als Trendpunkt (Watt pro Puls, Decoupling), kein Test,
// zaehlt nie fuers Ziel; kann zusaetzlich zu bikeCompare/baseline gelten.

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const addDays = (iso, n) => new Date(new Date(iso + "T00:00:00Z").getTime() + n * 86400000).toISOString().slice(0, 10);
const daysApart = (a, b) => Math.abs(Math.round((Date.parse(a + "T00:00:00Z") - Date.parse(b + "T00:00:00Z")) / 86400000));
const ddmm = (iso) => `${iso.slice(8, 10)}.${iso.slice(5, 7)}.`;
const de = (v, d = 0) => String(Math.round(v * 10 ** d) / 10 ** d).replace(".", ",");

export const TEST_RULES = {
  testDateTolDays: 2, // Blocktest verschoben (Ampel rot, krank)
  runMinMin: 60, // Blocktest und Trendlauf
  bikeWindowMin: 60, // Messfenster nach dem Einrollen
  runAutoMin: 65, // Run ab 65 min (5 min Einlaufen + 60 min Messfenster): zaehlt automatisch als Blocktest
  bikeAutoWarmupSec: 600, // Rad: die ersten 10 min sind das Einrollen, sobald die Fahrt fuers Messfenster lang genug ist
  bikeMinWarmupSec: 300, // Warmup-Block in Intervals bei benannten Tests; kuerzer: ohne Einrollen (Fenster ab 0, nur Ausgangswert)
  bikeMainSlackSec: 60, // Messtoleranz der Bewegungszeit beim Hauptteil
  bikeWattsTol: 5, // W um die Vergleichsleistung
  bikeMaxVi: 1.05,
  decTotalTolMin: 5, // Intervals-Decoupling nur bei Gesamtdauer ≈ Einrollen + Messfenster
  warmupMatchTolSec: 60, // und icu_warmup_time ≈ Einrolldauer
  // Trendpunkt Rad (lockere, gleichmaessige Rollenfahrt, kein Test): Watt pro Puls und Decoupling, nur innerhalb desselben Ablaufs verglichen
  trendMinMin: 45, // Bewegungszeit
  trendWattsTol: 10, // W um die Vergleichsleistung (Ø im Messfenster)
  trendMaxVi: 1.05, // gleichmaessig: keine HIIT
  trendWarmupSec: 300, // icu_warmup_time ab hier: Ablauf mit_einrollen, Fenster ab Ende des Warmup-Blocks
  trendNoWarmupStartSec: 600, // ohne Einrollen: Fenster ab Minute 10
};
export const TEST_FALLBACK = { testDates: ["2026-10-23", "2026-11-08", "2026-12-06", "2027-01-03"], hrCap: 128, bikeWatts: 165 };
export const TEST_CATEGORY_DE = { runBlocktest: "Lauf-Blocktest", runTrend: "Lauf-Trend", bikeCompare: "Rad-Vergleich", trend: "Rad-Trendpunkt" };

// Override ueber den Namen: "kein Test" schliesst aus, "Blocktest"/"Basistest" erzwingt den Lauf-Blocktest, "Vergleich" den
// Rad-Vergleich, "Ausgangswert" den Rad-Vergleich als baseline. Gilt nur fuer die passende Sportart (beim Rad erzwingt auch "Blocktest" den Rad-Vergleich).
export function nameOverride(name) {
  const n = String(name ?? "");
  if (/kein\s*test/i.test(n)) return "exclude";
  if (/blocktest|basistest/i.test(n)) return "runBlocktest";
  if (/vergleich|ausgangswert/i.test(n)) return "bikeCompare";
  return null;
}
// Intervals-Dauer aus interval_summary ("1x 10m1s 132w", "1x 1h 164w") in Sekunden
const summarySecs = (line) => {
  const m = String(line ?? "").match(/^\s*\d+x\s+(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?/i);
  return m ? (Number(m[1] ?? 0) * 3600 + Number(m[2] ?? 0) * 60 + Number(m[3] ?? 0)) : 0;
};
export const isBaselineName = (name) => /ausgangswert/i.test(String(name ?? "")) && !/kein\s*test/i.test(String(name ?? ""));
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
    if (m >= rules.runAutoMin) { out.set(a.id, { category: "runBlocktest", testDate: dayOf(a), testReason: `Run ab ${rules.runAutoMin} min (${Math.round(m)} min): zählt automatisch als Test` }); continue; }
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
    ef: hs.length >= 60 ? Math.round((avg / mean(hs)) * 100) / 100 : null, // Watt pro Puls: Ø-Leistung / Ø-HF (ungerundet gerechnet)
    decoupling: e1 && e2 ? Math.round((e1 / e2 - 1) * 1000) / 10 : null,
  };
}

// Einrollen und Messfenster einer Fahrt (ohne Zeitreihen). Ohne Warmup-Block in Intervals (icu_warmup_time < bikeMinWarmupSec,
// z. B. 19.10.2026) beginnt das Fenster bei 0 (noWarmup); die Fahrt kann dann nur Ausgangswert (baseline) sein.
export function rideWindowOf(a, rules = TEST_RULES) {
  const icuWarm = num(a?.icu_warmup_time) ?? 0;
  const total = num(a?.moving_time) ?? 0, cool = num(a?.icu_cooldown_time) ?? 0;
  const ov = nameOverride(a?.name);
  const forced = ov === "bikeCompare" || ov === "runBlocktest"; // beim Rad zaehlt auch "Blocktest" im Namen
  // Lang genug fuer Einrollen + Messfenster: die ersten bikeAutoWarmupSec sind automatisch das Einrollen, ohne Namen und
  // ohne Warmup-Block in Intervals. Benannte Tests mit erkanntem Warmup behalten dessen Dauer.
  const autoLong = total - cool >= rules.bikeAutoWarmupSec + rules.bikeWindowMin * 60 - rules.bikeMainSlackSec;
  // Hat Intervals keinen Warmup-Block erkannt (Einheit am Geraet gefahren), steht das Einrollen als erstes Intervall vor dem
  // Hauptteil in interval_summary – nur fuer benannte Tests
  let warmFromSummary = 0;
  if (forced && icuWarm < rules.bikeMinWarmupSec && Array.isArray(a?.interval_summary)) {
    const secs = a.interval_summary.map(summarySecs);
    if (secs.length > 1 && secs[0] >= rules.bikeMinWarmupSec && secs.slice(1).some((x) => x >= rules.bikeWindowMin * 60 - rules.bikeMainSlackSec)) warmFromSummary = secs[0];
  }
  const detected = warmFromSummary || (icuWarm >= rules.bikeMinWarmupSec ? icuWarm : 0);
  const auto = autoLong && !(forced && detected);
  const noWarmup = !auto && !detected, warmSec = auto ? rules.bikeAutoWarmupSec : detected;
  return { warmSec, noWarmup, auto, mainSec: total - warmSec - cool, totalSec: total, forced };
}

// Rad klassifizieren. stats: rideWindowStats fuer das Fenster aus rideWindowOf (null = Zeitreihen fehlen).
export function classifyRide(a, stats, cfg, rules = TEST_RULES) {
  if (!isTestRide(a)) return null;
  const ov = nameOverride(a?.name);
  if (ov === "exclude") return { category: null, testReason: "Name „kein Test“: ausgeschlossen" };
  const win = rideWindowOf(a, rules);
  const mainMin = Math.round(win.mainSec / 60);
  const warmTxt = win.noWarmup ? `ohne Einrollen (icu_warmup_time ${Math.round(num(a?.icu_warmup_time) ?? 0)} s)` : `Einrollen ${Math.round(win.warmSec / 60)} min${win.auto ? " (automatisch, erste 10 min)" : ""}`;
  const base = { warmupSec: win.warmSec, noWarmup: win.noWarmup, mainMin };
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
  const baseline = win.noWarmup || isBaselineName(a?.name);
  return {
    ...base, category: "bikeCompare", baseline, mainWatts: stats.avgWatts, mainHr: stats.hr, vi: stats.vi, decoupling, decSource,
    testReason: `Ride${win.forced ? `, Name „${String(a.name).trim()}“ (Override)` : ""}: ${facts}, ${warmTxt}, ${decTxt}${baseline ? ", Ausgangswert (baseline, zählt nicht fürs Ziel)" : ""}`,
  };
}

// ---------- Rad-Trendpunkt (Kategorie trend) ----------
// Jede lockere, gleichmaessige Rollenfahrt ist ein Trendpunkt (Watt pro Puls, Decoupling), auch ohne Einrollen und ohne
// Testprotokoll. Verglichen wird nur innerhalb desselben Ablaufs (ablauf: mit_einrollen | ohne_einrollen). Unabhaengig von
// classifyRide: eine Vergleichseinheit oder ein Ausgangswert kann zusaetzlich Trendpunkt sein; die Zielbewertung bleibt unberuehrt.
// Bedingungen: Ride/VirtualRide auf der Rolle (trainer), Bewegungszeit ≥ trendMinMin, kein Blocktest/FTP-/Rampentest (Name),
// Pulsdaten, im Messfenster VI ≤ trendMaxVi und Ø-Leistung = Vergleichsleistung ± trendWattsTol. "kein Test" im Namen schliesst aus.
// Messfenster: mit Einrollen (icu_warmup_time ≥ trendWarmupSec) ab Ende des Warmup-Blocks bis Ende, sonst ab Minute 10 bis Ende.
const TREND_TEST_NAME = /blocktest|basistest|\bftp\b|rampe|ramp\s*test|rampentest/i;
const isIndoor = (a) => a?.trainer === true || /^virtualride$/i.test(String(a?.type ?? ""));

// Vorpruefung ohne Zeitreihen: ob die Fahrt Trendpunkt sein kann und wo das Messfenster beginnt
export function rideTrendWindowOf(a, rules = TEST_RULES) {
  if (!isTestRide(a)) return null;
  const warm = num(a?.icu_warmup_time) ?? 0, moving = num(a?.moving_time) ?? 0;
  const ablauf = warm >= rules.trendWarmupSec ? "mit_einrollen" : "ohne_einrollen";
  const windowStartS = ablauf === "mit_einrollen" ? Math.round(warm) : rules.trendNoWarmupStartSec;
  const hasHr = a?.has_heartrate === true || num(a?.average_heartrate) != null;
  const durationMin = Math.round(moving / 60);
  let reason = null;
  if (nameOverride(a?.name) === "exclude") reason = "Name „kein Test“: ausgeschlossen";
  else if (!isIndoor(a)) reason = "nicht auf der Rolle (trainer)";
  else if (moving < rules.trendMinMin * 60) reason = `${durationMin} min < ${rules.trendMinMin} min`;
  else if (TREND_TEST_NAME.test(String(a?.name ?? ""))) reason = `Test im Namen („${String(a.name).trim()}“): kein Trendpunkt`;
  else if (!hasHr) reason = "keine Pulsdaten";
  return { ok: !reason, reason, ablauf, windowStartS, durationMin };
}

// Werte im Messfenster: Start windowStartS bis Ende der Zeitreihe (gleiche Rechnung wie rideWindowStats)
export function rideTrendStats(streams, windowStartS) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const w = get("watts"), time = get("time");
  if (!Array.isArray(w) || !w.length) return null;
  const lastT = Array.isArray(time) && Number.isFinite(time[w.length - 1]) ? time[w.length - 1] : w.length - 1;
  if (lastT + 1 <= windowStartS) return null;
  return rideWindowStats(streams, windowStartS, lastT + 1 - windowStartS);
}

// Trendpunkt klassifizieren. stats: rideTrendStats (null = Zeitreihen fehlen). Ergebnis: null (kein Rad) oder
// { trend: boolean, testReason, ablauf, windowStartS, … } mit den Werten, wenn trend.
export function classifyRideTrend(a, stats, cfg, rules = TEST_RULES) {
  const pre = rideTrendWindowOf(a, rules);
  if (!pre) return null;
  const head = { ablauf: pre.ablauf, windowStartS: pre.windowStartS };
  const abl = pre.ablauf === "mit_einrollen" ? `mit Einrollen (Fenster ab Minute ${de(pre.windowStartS / 60, 1)})` : `ohne Einrollen (Fenster ab Minute ${pre.windowStartS / 60})`;
  if (!pre.ok) return { ...head, trend: false, testReason: `Trend: ${pre.reason}` };
  if (!stats) return { ...head, trend: false, testReason: "Trend: Zeitreihen noch nicht geladen" };
  const wattsOk = Math.abs(stats.avgWatts - cfg.bikeWatts) <= rules.trendWattsTol, viOk = stats.vi != null && stats.vi <= rules.trendMaxVi;
  const facts = `${pre.durationMin} min, Fenster ${stats.windowMin} min, ${stats.avgWatts} W, VI ${stats.vi != null ? de(stats.vi, 2) : "–"}`;
  const why = [!wattsOk ? `${stats.avgWatts} W nicht ${cfg.bikeWatts} ±${rules.trendWattsTol} W` : null, !viOk ? `VI ${stats.vi != null ? de(stats.vi, 2) : "–"} > ${de(rules.trendMaxVi, 2)}` : null, stats.hr == null || stats.ef == null ? "Puls im Fenster fehlt" : null].filter(Boolean);
  if (why.length) return { ...head, trend: false, testReason: `Trend: ${why.join(", ")} (${facts})` };
  return {
    ...head, trend: true, durationMin: pre.durationMin, windowMin: stats.windowMin, avgWatts: stats.avgWatts, avgHr: stats.hr, vi: stats.vi, ef: stats.ef, decoupling: stats.decoupling,
    testReason: `Trendpunkt: Rolle, ${facts}, ${abl}, ${cfg.bikeWatts} ±${rules.trendWattsTol} W, VI ≤ ${de(rules.trendMaxVi, 2)}`,
  };
}

// Trendpunkte fuer Anzeige und get_coach: aus den Fahrten (block-inputs.js, rec.trend), aelteste zuerst, je Ablauf getrennt.
// Keine Bewertung, kein Mittel gegen das Ziel.
export const RIDE_TREND_KEEP = 8;
export const RIDE_TREND_NOTE = "Trend, kein Test. Bewertet wird nur am Blocktest.";
const TREND_POINT_KEYS = ["date", "ablauf", "durationMin", "windowStartS", "windowMin", "avgWatts", "avgHr", "vi", "ef", "decoupling", "testReason"];
export function rideTrendPoints(rides, keep = RIDE_TREND_KEEP) {
  return (rides ?? []).map((r) => r?.trend).filter((t) => t && t.date)
    .sort((a, b) => a.date.localeCompare(b.date))
    .map((t) => Object.fromEntries(TREND_POINT_KEYS.map((k) => [k, t[k] ?? null])))
    .slice(-keep);
}
export function rideTrendView(rides, keep = RIDE_TREND_KEEP) {
  const all = (rides ?? []).filter((r) => r?.trend);
  if (!all.length) return null;
  const groups = [["ohne_einrollen", "ohne Einrollen"], ["mit_einrollen", "mit Einrollen"]]
    .map(([ablauf, label]) => ({ ablauf, label, points: rideTrendPoints(all.filter((r) => r.trend.ablauf === ablauf), keep) }))
    .filter((g) => g.points.length);
  return { groups, note: RIDE_TREND_NOTE };
}
