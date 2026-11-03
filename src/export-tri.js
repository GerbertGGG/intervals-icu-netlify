// Export fuer die Triathlon-Auswertung (DOKUMENTATION.md, Teil B 13.6–13.8): Radfahrten, Koppellaeufe, Schwimmeinheiten.
// Laeuft wie export-calc.js nur in GitHub Actions (scripts/export-sync.mjs), nie im Worker. Nur Rohwerte und Kennzahlen, keine
// Bewertung (CLAUDE.md). Wiederverwendet: rideWindowOf/rideWindowStats/classifyRide (session-tests.js, Rad-Vergleichseinheit wie
// im Dashboard), decouplingFromStreams/runMovingAxis (block-inputs.js), intensityKindOf (season-block.js), sessionLoad,
// swimTimeOf, swimTestFromText/swimTestFromIntervals.
import { activityDay, sessionLoad, isBike, isRun, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivityStreamsStatus, fetchIntervalsActivityIntervals } from "./intervals-client.js";
import { decouplingFromStreams, runMovingAxis } from "./block-inputs.js";
import { rideWindowOf, rideWindowStats, classifyRide, isTestRide, TEST_FALLBACK, TEST_RULES } from "./session-tests.js";
import { intensityKindOf, swimTestFromText, swimTestFromIntervals } from "./season-block.js";
import { swimTimeOf } from "./swim-time.js";
import { weatherOf, EXPORT_RUN_RULES } from "./export-calc.js";
import { METHOD_VERSION, RIDE_METRIC_COLUMNS } from "./export-schema.js";

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);
const r2 = (v) => (v == null ? null : Math.round(v * 100) / 100);
const r3 = (v) => (v == null ? null : Math.round(v * 1000) / 1000);
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const median = (xs) => { const s = [...xs].sort((a, b) => a - b), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const tsOf = (local) => (local ? Date.parse(String(local).slice(0, 19) + "Z") : NaN); // lokale Zeit ohne Zone, nur fuer Abstaende
const tags = (a) => (Array.isArray(a?.tags) ? a.tags.map(String) : []);

// ---------------------------------------------------------------------------------------------------------------------
// Radfahrten
// ---------------------------------------------------------------------------------------------------------------------

export const RIDE_RULES = {
  MIN_MOVING_S: 20 * 60,
  DEFAULT_WARMUP_S: 600, // ohne icu_warmup_time: die ersten 10 min sind Einrollen
  WIN_S: 30, STEP_S: 5,
  CV_LOOKBACK_S: 120, CV_MAX: 0.10, // gleichmaessig: Variationskoeffizient der Leistung in den 2 min vor dem Fenster < 10 %
  BAND: [160, 170], BAND_MIN_S: 300,
  PW_HR: [100, 150], PW_AT: [120, 130], PW_MIN_N: 20,
  MIN_HR: 40, MIN_HR_SHARE: 0.8,
  WINDOW_S: 60 * 60, // decoup60: die ersten 60 min nach dem Einrollen
};
const RIDE_STREAMS = ["time", "watts", "heartrate", "distance", "velocity_smooth"];

export const isIndoorRide = (a) => a?.trainer === true || /virtual/i.test(String(a?.type ?? ""));
export const hasPower = (a) => num(a?.icu_average_watts) > 0 || num(a?.average_watts) > 0;

// Text fuer intensityKindOf: Name/Beschreibung der Fahrt und des gekoppelten Workouts (wie dashboard.js)
const textOf = (a, eventsById) => { const e = eventsById?.get(String(a?.paired_event_id)); return [a?.name, a?.description, e?.name, e?.description].filter(Boolean).join(" "); };
export function rideIntensityOf(a, eventsById = null) {
  const z5 = Array.isArray(a?.icu_zone_times) ? num(a.icu_zone_times.find((z) => z?.id === "Z5")?.secs) : null;
  return intensityKindOf("bike", textOf(a, eventsById), z5);
}

// Grund ohne Zeitreihen-Kennzahlen (null = wird gerechnet)
export function rideSkipOf(a, eventsById = null) {
  if (!hasPower(a)) return "noPower";
  if ((num(a?.moving_time) ?? 0) < RIDE_RULES.MIN_MOVING_S) return "short";
  if (rideIntensityOf(a, eventsById) || isIntervalActivity(a) || hasIntervalTextSignal(a)) return "interval";
  if (!(a?.has_heartrate === true || num(a?.average_heartrate) != null)) return "noHr";
  return null;
}

// Kennzahlen aus den Zeitreihen nach dem Einrollen (warmSec). Zeitachse ohne Stillstand (runMovingAxis mit Distanz bzw.
// aufsummiertem Tempo, Stehen unter 0,5 m/s); ohne Distanz zaehlen Luecken ueber 5 s als 1 s.
export function analyzeRideStreams(streams, warmSec, rules = RIDE_RULES) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const w = get("watts"), hr = get("heartrate"), time = get("time"), vel = get("velocity_smooth");
  let dist = get("distance");
  if (!Array.isArray(w) || !Array.isArray(hr) || w.length < 2) return null;
  const n = Math.min(w.length, hr.length);
  const elapsed = Array.from({ length: n }, (_, i) => (Array.isArray(time) && Number.isFinite(time[i]) ? time[i] : i));
  if (!Array.isArray(dist) || dist.length < n) {
    if (Array.isArray(vel) && vel.length >= n) { dist = [0]; for (let i = 1; i < n; i++) dist.push(dist[i - 1] + (Number(vel[i]) || 0) * Math.max(0, elapsed[i] - elapsed[i - 1])); }
    else dist = null;
  }
  let mt;
  if (dist) mt = runMovingAxis(elapsed, dist.slice(0, n).map((x) => Number(x) || 0), EXPORT_RUN_RULES);
  else { mt = [0]; for (let i = 1; i < n; i++) { const dt = elapsed[i] - elapsed[i - 1]; mt.push(mt[i - 1] + (dt <= 0 ? 0 : dt > 5 ? 1 : dt)); } }
  const dur = Math.floor(mt[n - 1]);
  if (dur < 60) return null;
  // Raster je Sekunde Bewegungszeit; Unterbrechung = verlorene Zeit (Stehen, Pause)
  const P = new Float64Array(dur + 1), H = new Float64Array(dur + 1), Hok = new Uint8Array(dur + 1), brk = new Uint8Array(dur + 2);
  for (let i = 1; i < n; i++) if ((elapsed[i] - elapsed[i - 1]) - (mt[i] - mt[i - 1]) >= 1) brk[Math.min(dur + 1, Math.ceil(mt[i]))] = 1;
  for (let s = 0, j = 0; s <= dur; s++) {
    while (j < n - 1 && mt[j + 1] <= s) j++;
    P[s] = Math.max(0, Number(w[j]) || 0);
    const h = Number(hr[j]);
    if (hr[j] != null && h > rules.MIN_HR) { H[s] = h; Hok[s] = 1; }
  }
  const PS = new Float64Array(dur + 2), PQ = new Float64Array(dur + 2), HS = new Float64Array(dur + 2), HC = new Uint32Array(dur + 2), BS = new Uint32Array(dur + 3);
  for (let s = 0; s <= dur; s++) { PS[s + 1] = PS[s] + P[s]; PQ[s + 1] = PQ[s] + P[s] * P[s]; HS[s + 1] = HS[s] + H[s]; HC[s + 1] = HC[s] + Hok[s]; }
  for (let s = 0; s <= dur + 1; s++) BS[s + 1] = BS[s] + brk[s];
  const broken = (a, b) => BS[b + 1] - BS[a + 1] > 0; // Unterbrechung in (a, b]
  const pMean = (a, b) => (PS[b] - PS[a]) / (b - a);
  const cv = (a, b) => { const m = pMean(a, b); if (!(m > 0)) return Infinity; const v = (PQ[b] - PQ[a]) / (b - a) - m * m; return Math.sqrt(Math.max(0, v)) / m; };
  const start = Math.max(0, Math.round(warmSec));
  const W = rules.WIN_S, L = rules.CV_LOOKBACK_S, wins = [];
  for (let s = Math.max(start, L); s + W <= dur; s += rules.STEP_S) {
    if (broken(s - L, s + W)) continue;
    if (HC[s + W] - HC[s] < W * rules.MIN_HR_SHARE) continue;
    if (!(cv(s - L, s) < rules.CV_MAX)) continue;
    const p = pMean(s, s + W);
    if (!(p > 0)) continue;
    wins.push({ s, p, hr: (HS[s + W] - HS[s]) / (HC[s + W] - HC[s]) });
  }
  const covered = (sel) => { let c = 0; for (let i = 0; i < sel.length; i++) c += i + 1 < sel.length ? Math.min(W, sel[i + 1].s - sel[i].s) : W; return c; };
  const out = { movingSec: dur };
  const band = wins.filter((x) => x.p >= rules.BAND[0] && x.p <= rules.BAND[1]);
  out.secAt160_170 = covered(band);
  out.hrAt160_170 = out.secAt160_170 >= rules.BAND_MIN_S ? r1(median(band.map((x) => x.hr))) : null;
  // Regression Leistung gegen Puls
  const reg = wins.filter((x) => x.hr >= rules.PW_HR[0] && x.hr <= rules.PW_HR[1]);
  out.pwN = reg.length;
  out.powerAt120 = out.powerAt130 = out.pwSlope = out.pwR2 = null;
  if (reg.length >= rules.PW_MIN_N) {
    const xs = reg.map((x) => x.hr), ys = reg.map((x) => x.p), mx = mean(xs), my = mean(ys);
    let sxx = 0, syy = 0, sxy = 0;
    for (let i = 0; i < xs.length; i++) { sxx += (xs[i] - mx) ** 2; syy += (ys[i] - my) ** 2; sxy += (xs[i] - mx) * (ys[i] - my); }
    if (sxx > 0) {
      const b = sxy / sxx;
      out.pwSlope = r2(b);
      out.powerAt120 = r1(my + b * (rules.PW_AT[0] - mx));
      out.powerAt130 = r1(my + b * (rules.PW_AT[1] - mx));
      out.pwR2 = syy > 0 ? r3((sxy * sxy) / (sxx * syy)) : null;
    }
  }
  // EF = NP / Ø-Puls nach dem Einrollen (NP: 30-s-Mittel hoch 4)
  if (dur - start >= 60) {
    const roll = []; for (let s = start + 29; s <= dur; s++) roll.push(pMean(s - 29, s + 1));
    const np = roll.length ? mean(roll.map((x) => x ** 4)) ** 0.25 : null;
    const hc = HC[dur + 1] - HC[start];
    out.ef = np != null && hc >= 60 ? r2(np / ((HS[dur + 1] - HS[start]) / hc)) : null;
  } else out.ef = null;
  return out;
}

const endLocalOf = (a) => {
  const t = tsOf(a?.start_date_local), e = num(a?.elapsed_time) ?? num(a?.moving_time);
  return Number.isFinite(t) && e != null ? new Date(t + e * 1000).toISOString().slice(0, 19) : null;
};

// Rechnet eine Fahrt: Wetter (nur draussen), Zeitreihen (wenn Leistung und mind. 20 min), Rad-Vergleichseinheit wie im
// Dashboard (rideWindowOf/rideWindowStats/classifyRide mit der Vergleichsleistung aus TEST_FALLBACK). Wirft bei Abruffehlern.
export async function computeRideEntry(env, a, { eventsById = null, testCfg = TEST_FALLBACK } = {}) {
  const skip = rideSkipOf(a, eventsById);
  const weather = isIndoorRide(a) ? null : await weatherOf(env, a);
  if (skip === "noPower" || skip === "short") return { streamStatus: "skip", streamSkip: skip, metrics: null, weather, testKind: null };
  const st = await fetchIntervalsActivityStreamsStatus(env, a.id, RIDE_STREAMS);
  if (st.status === 404) return { streamStatus: "none", streamSkip: skip, metrics: null, weather, testKind: null };
  const streams = st.data;
  // Vergleichseinheit/Ausgangswert: dasselbe Fenster und dieselbe Entscheidung wie das Dashboard (7.9)
  let testKind = null, mainHr = null, testDecoup = null;
  const win = rideWindowOf(a);
  if (isTestRide(a) && (win.forced || win.mainSec >= TEST_RULES.bikeWindowMin * 60 - TEST_RULES.bikeMainSlackSec)) {
    const c = classifyRide(a, rideWindowStats(streams, win.warmSec, TEST_RULES.bikeWindowMin * 60), testCfg);
    if (c?.category === "bikeCompare") { testKind = c.baseline ? "baseline" : "compare"; mainHr = c.mainHr ?? null; testDecoup = c.decoupling ?? null; }
  }
  if (skip) return { streamStatus: "skip", streamSkip: skip, metrics: testKind ? { mainHr, decoup60: testDecoup } : null, weather, testKind };
  // Einrollen: bei Vergleichseinheiten das Fenster des Dashboards, sonst icu_warmup_time bzw. 10 min
  const icuWarm = num(a?.icu_warmup_time) ?? 0;
  const warmSec = testKind ? win.warmSec : icuWarm > 0 ? icuWarm : RIDE_RULES.DEFAULT_WARMUP_S;
  const m = analyzeRideStreams(streams, warmSec);
  if (!m) return { streamStatus: "none", streamSkip: null, metrics: null, weather, testKind };
  const cool = num(a?.icu_cooldown_time) ?? 0;
  m.decoup = decouplingFromStreams(streams, "watts", null, { warmupSec: warmSec, cooldownSec: cool });
  m.decoup60 = testKind ? testDecoup : rideWindowStats(streams, warmSec, RIDE_RULES.WINDOW_S)?.decoupling ?? null;
  m.mainHr = mainHr;
  m.warmUsedS = warmSec;
  return { streamStatus: "ok", streamSkip: null, metrics: m, weather, testKind };
}

export function rideEntryFromRow(r) {
  const metrics = {};
  for (const c of RIDE_METRIC_COLUMNS) metrics[c] = r?.[c] ?? null;
  return {
    streamStatus: r.streamStatus, streamSkip: r.streamSkip ?? null, metrics, testKind: r.testKind ?? null,
    weather: r.weatherSrc ? { tempC: r.tempC ?? null, humidity: r.humidity ?? null, windMs: r.windMs ?? null, src: r.weatherSrc } : null,
  };
}

export function rideRow(a, entry, eventsById = null) {
  const km = (num(a?.distance) ?? 0) / 1000, gain = num(a?.total_elevation_gain), load = sessionLoad(a);
  const avg = num(a?.icu_average_watts) ?? num(a?.average_watts), np = num(a?.icu_weighted_avg_watts), ifPct = num(a?.icu_intensity);
  const skip = rideSkipOf(a, eventsById), w = entry?.weather ?? null;
  const row = {
    id: a?.id ?? null, date: activityDay(a) || null, startLocal: a?.start_date_local ?? null, endLocal: endLocalOf(a), type: a?.type ?? null,
    name: a?.name ?? null, tags: tags(a), trainer: isIndoorRide(a),
    movingMin: num(a?.moving_time) != null ? r1(num(a.moving_time) / 60) : null, distKm: km > 0 ? r2(km) : null,
    elevPerKm: km > 0 && gain != null ? r1(gain / km) : null,
    avgPower: avg != null && avg > 0 ? Math.round(avg) : null, np: np != null && np > 0 ? Math.round(np) : null,
    if: ifPct != null && ifPct > 0 ? r3(ifPct > 3 ? ifPct / 100 : ifPct) : null,
    vi: num(a?.icu_variability_index) ?? (np && avg ? r2(np / avg) : null),
    ftp: num(a?.icu_ftp), avgHr: num(a?.average_heartrate), maxHr: num(a?.max_heartrate), avgCadence: num(a?.average_cadence) != null ? Math.round(num(a.average_cadence)) : null,
    load: load > 0 ? r1(load) : null, rpe: num(a?.icu_rpe) ?? num(a?.perceived_exertion), feel: num(a?.feel), warmupS: num(a?.icu_warmup_time),
    intensity: rideIntensityOf(a, eventsById), testKind: entry?.testKind ?? null,
    tempC: w?.tempC ?? null, humidity: w?.humidity ?? null, windMs: w?.windMs ?? null, weatherSrc: w?.src ?? null,
  };
  if (!entry) Object.assign(row, { streamStatus: "pending", streamSkip: skip });
  else Object.assign(row, { streamStatus: entry.streamStatus, streamSkip: entry.streamSkip ?? null });
  for (const c of RIDE_METRIC_COLUMNS) row[c] = entry?.metrics?.[c] ?? null;
  row.methodVersion = METHOD_VERSION;
  return row;
}

// ---------------------------------------------------------------------------------------------------------------------
// Koppellaeufe
// ---------------------------------------------------------------------------------------------------------------------

export const BRICK_MAX_GAP_MIN = 30;
const BRICK_OVERLAP_MIN = 5; // Uhrzeiten von Radcomputer und Uhr weichen ab: bis 5 min Ueberlappung zaehlt als Abstand 0

// Lauf, der hoechstens 30 min nach dem Ende einer Radfahrt beginnt. Bei mehreren Fahrten die zuletzt beendete.
// Ergebnis: Map Lauf-ID -> { brickRideId, brickGapMin, brickRideMin, brickRideLoad }
export function brickMap(activities) {
  const list = Array.isArray(activities) ? activities : [];
  const rides = list.filter(isBike).map((a) => ({ a, end: tsOf(a?.start_date_local) + ((num(a?.elapsed_time) ?? num(a?.moving_time) ?? 0) * 1000) })).filter((x) => Number.isFinite(x.end));
  const out = new Map();
  for (const r of list.filter((x) => isRun(x) && x?.id != null)) {
    const start = tsOf(r.start_date_local);
    if (!Number.isFinite(start)) continue;
    let best = null;
    for (const x of rides) {
      const gapMin = (start - x.end) / 60000;
      if (gapMin < -BRICK_OVERLAP_MIN || gapMin > BRICK_MAX_GAP_MIN) continue;
      if (!best || x.end > best.end) best = { ...x, gapMin };
    }
    if (best) {
      const load = sessionLoad(best.a);
      out.set(String(r.id), {
        brickRideId: best.a.id ?? null, brickGapMin: r1(Math.max(0, best.gapMin)),
        brickRideMin: num(best.a.moving_time) != null ? r1(num(best.a.moving_time) / 60) : null, brickRideLoad: load > 0 ? r1(load) : null,
      });
    }
  }
  return out;
}

// ---------------------------------------------------------------------------------------------------------------------
// Schwimmen
// ---------------------------------------------------------------------------------------------------------------------

export const isSwim = (a) => /swim/i.test(String(a?.type ?? ""));
// Schwimmtest 2× 500 m wie im Blockziel (7.9): bestanden bei Pause ≤ 60 s und zweite 500 m hoechstens 5 s/100 m langsamer
export const SWIM_TEST = { distM: 500, maxPauseSec: 60, maxSlowerPer100: 5 };

// Laps holen (Bewegungszeit ohne Pausen, Test aus den Intervallen). Wirft bei Abruffehlern (ausser 404).
export async function computeSwimEntry(env, a) {
  let dto = null, lapsStatus = "none";
  try { dto = await fetchIntervalsActivityIntervals(env, a.id); lapsStatus = Array.isArray(dto?.icu_intervals) && dto.icu_intervals.length ? "ok" : "none"; }
  catch (e) { if (!/\b404\b/.test(String(e?.message ?? ""))) throw e; }
  const withLaps = dto?.icu_intervals ? { ...a, icu_intervals: dto.icu_intervals } : a;
  const movingSec = swimTimeOf(withLaps).sec;
  const text = `${a?.name ?? ""}\n${a?.description ?? ""}`;
  const test = swimTestFromText(text, SWIM_TEST.distM) ?? (dto ? swimTestFromIntervals(dto, SWIM_TEST.distM) : null);
  return { lapsStatus, movingSec, test };
}

export function swimEntryFromRow(r) {
  return {
    lapsStatus: r.lapsStatus ?? "none", movingSec: num(r.movingMin) != null ? num(r.movingMin) * 60 : null,
    test: r.test500a != null ? { t1Sec: r.test500a, t2Sec: r.test500b, pauseSec: r.testPauseS } : null,
  };
}

export function swimRow(a, entry) {
  const distanceM = num(a?.distance), load = sessionLoad(a);
  const movingSec = entry?.movingSec ?? swimTimeOf(a).sec;
  const t = entry?.test ?? null;
  const slower = t?.t1Sec > 0 && t?.t2Sec > 0 ? (t.t2Sec - t.t1Sec) / (SWIM_TEST.distM / 100) : null;
  const pool = /open\s*water/i.test(String(a?.type ?? "")) ? "open" : num(a?.pool_length) > 0 ? "pool" : null;
  return {
    id: a?.id ?? null, date: activityDay(a) || null, startLocal: a?.start_date_local ?? null, type: a?.type ?? null, name: a?.name ?? null,
    pool, poolLengthM: num(a?.pool_length), distanceM: distanceM > 0 ? Math.round(distanceM) : null,
    movingMin: movingSec > 0 ? r1(movingSec / 60) : null,
    pace100: movingSec > 0 && distanceM > 0 ? Math.round(movingSec / (distanceM / 100)) : null,
    avgHr: num(a?.average_heartrate), load: load > 0 ? r1(load) : null, rpe: num(a?.icu_rpe) ?? num(a?.perceived_exertion), feel: num(a?.feel),
    test500a: t?.t1Sec ?? null, test500b: t?.t2Sec ?? null, testPauseS: t?.pauseSec ?? null,
    testPassed: slower == null ? null : slower <= SWIM_TEST.maxSlowerPer100 && !(t.pauseSec != null && t.pauseSec > SWIM_TEST.maxPauseSec),
    lapsStatus: entry ? entry.lapsStatus : "pending",
    methodVersion: METHOD_VERSION,
  };
}
