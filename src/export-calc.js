// Rechnung des Exports (DOKUMENTATION.md, Teil B 13): Kennzahlen aus den Intervals-Zeitreihen, Wetter, Zeile pro Lauf und pro
// Tag. Laeuft NICHT im Worker (10 ms CPU im kostenlosen Plan), sondern in GitHub Actions (scripts/export-sync.mjs), das die
// Zeilen nach D1 schreibt; der Worker liest sie nur (export.js). Nur Rohwerte und Kennzahlen, keine Bewertung (CLAUDE.md).
import { activityDay, sessionLoad, isRun, isBike, isTreadmill, isRaceActivity, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivityMap, fetchIntervalsActivityStreamsStatus } from "./intervals-client.js";
import { runMovingAxis, cleanRunDistance, RUN_RULES } from "./block-inputs.js";
import { classifyRun, buildRaceDays } from "./dashboard.js";
import { isARaceEvent } from "./event-utils.js";
import { averageNativeWeather, extractStartLatLng, openMeteoDay, pickOpenMeteoValues, activityStartHourIso, dewPointC } from "./weather.js";
import { isStrengthActivity, strengthSetsOf } from "./week-totals.js";
import { swimTimeOf } from "./swim-time.js";
import { isIsoDate, isoDate } from "./date-utils.js";
import { METHOD_VERSION, METRIC_COLUMNS } from "./export-schema.js";

export { METHOD_VERSION, METRIC_COLUMNS };

export const EXPORT_RUN_RULES = { ...RUN_RULES, SPARSE_MAX_GAP_S: 12 };

export const EXPORT_RULES = {
  SKIP_S: RUN_RULES.DECOUPLING_SKIP_START_S, // 300 s Startausblendung wie 6.5
  WIN_S: 30, // Fensterlaenge
  STEP_S: 5, // Fenster gleiten in 5-s-Schritten
  MIN_HR_SHARE: 0.8, // Fenster braucht Puls in mindestens 80 % seiner Sekunden
  BANDS: [[125, 135], [120, 128]],
  BAND_MIN_S: 300, // Pace im Band erst ab 300 s im Band
  EFF_HR: [110, 150], // Effizienz-Regression nur mit Fenstern in diesem Pulsbereich
  EFF_AT: 130,
  EFF_MIN_N: 20,
  FLAT_GRADE: 0.02, // flach = Betrag der Steigung unter 2 %
  ALT_SMOOTH_S: 15, // Hoehe vor der Steigung +-15 s geglaettet (Rauschen des Barometers)
  DRIFT_PACE_TOL: 0.03, // HF-Drift nur bei Pace-Abweichung erstes/letztes Drittel unter 3 %
  MIN_MOVING_S: 20 * 60,
  MIN_HR: 40,
  GPS_MAX_SHARE: 0.1, // entfernt die GPS-Bereinigung mehr als 10 % der Distanz: streamStatus skip, streamSkip "gps"
};

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);
const r2 = (v) => (v == null ? null : Math.round(v * 100) / 100);
const r3 = (v) => (v == null ? null : Math.round(v * 1000) / 1000);
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const median = (xs) => { const s = [...xs].sort((a, b) => a - b), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const addDays = (iso, n) => isoDate(new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000));

// ---------------------------------------------------------------------------------------------------------------------
// Kennzahlen aus den Zeitreihen
// ---------------------------------------------------------------------------------------------------------------------

const bandKey = ([lo, hi]) => `${lo}_${hi}`;

// streams: Liste { type, data } von Intervals (oder Objekt type -> data). null ohne Puls oder Distanz/Tempo.
export function analyzeExportStreams(streams, rules = EXPORT_RULES) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const hr = get("heartrate"), time = get("time"), vel = get("velocity_smooth");
  const altRaw = get("fixed_altitude") ?? get("altitude");
  let rawDist = get("distance");
  if (!Array.isArray(hr) || hr.length < 2) return null;
  const n = hr.length;
  const elapsed = Array.from({ length: n }, (_, i) => (Array.isArray(time) && Number.isFinite(time[i]) ? time[i] : i));
  if (!Array.isArray(rawDist) || rawDist.length < n) {
    if (!Array.isArray(vel)) return null;
    rawDist = [0];
    for (let i = 1; i < n; i++) rawDist.push(rawDist[i - 1] + (Number(vel[i]) || 0) * Math.max(0, elapsed[i] - elapsed[i - 1]));
  }
  rawDist = rawDist.slice(0, n).map((x) => Number(x) || 0);
  // 1. Bewegungszeit, 2. GPS-Bereinigung (wie 6.5)
  // Regeln wie 6.5, dazu Aufzeichnungsluecken bis 12 s als Bewegung (aeltere Uhren mit intelligenter Aufzeichnung, z. B. 2022:
  // Proben alle 5–10 s; als Auto-Pause gezaehlt landete die Strecke auf 1 s und die GPS-Bereinigung entfernte 60–70 %)
  const mt = runMovingAxis(elapsed, rawDist, EXPORT_RUN_RULES);
  const { dist, gps } = cleanRunDistance(mt, rawDist, EXPORT_RUN_RULES);
  const dur = Math.floor(mt[n - 1]);

  // Raster je Sekunde Bewegungszeit: Distanz linear, Puls und Hoehe von der letzten Probe davor.
  // Unterbrechung (Auto-Pause oder Stehen) = Probe, bei der vergangene Zeit verloren ging; Fenster darueber sind ungueltig.
  // Puls und Hoehe als Praefixsummen (Summe und Anzahl gueltiger Sekunden): jedes Fenster kostet so O(1) statt O(30) (CPU-Limit 10 ms)
  const D = new Float64Array(dur + 1), H = new Float64Array(dur + 1), Hok = new Uint8Array(dur + 1), A = new Float64Array(dur + 1), Aok = new Uint8Array(dur + 1), brk = new Uint8Array(dur + 2);
  const hasAlt = Array.isArray(altRaw) && altRaw.some((x) => num(x) != null);
  for (let i = 1; i < n; i++) if ((elapsed[i] - elapsed[i - 1]) - (mt[i] - mt[i - 1]) >= 1) brk[Math.min(dur + 1, Math.ceil(mt[i]))] = 1;
  for (let s = 0, j = 0; s <= dur; s++) {
    while (j < n - 1 && mt[j + 1] <= s) j++;
    const span = j < n - 1 ? mt[j + 1] - mt[j] : 0;
    const f = span > 0 ? Math.min(1, Math.max(0, (s - mt[j]) / span)) : 0;
    D[s] = dist[j] + (span > 0 ? f * (dist[j + 1] - dist[j]) : 0);
    const h = Number(hr[j]);
    if (hr[j] != null && h > rules.MIN_HR) { H[s] = h; Hok[s] = 1; }
    if (hasAlt) { const a = altRaw[j]; if (a != null && a !== "" && Number.isFinite(Number(a))) { A[s] = Number(a); Aok[s] = 1; } }
  }
  const HS = new Float64Array(dur + 2), HC = new Uint32Array(dur + 2);
  for (let s = 0; s <= dur; s++) { HS[s + 1] = HS[s] + H[s]; HC[s + 1] = HC[s] + Hok[s]; }
  const hrCount = (a, b) => HC[b] - HC[a]; // gueltige Sekunden in [a, b)
  const hrMean = (a, b) => (HC[b] - HC[a] > 0 ? (HS[b] - HS[a]) / (HC[b] - HC[a]) : null);
  const brkSum = new Uint32Array(dur + 3);
  for (let s = 0; s <= dur + 1; s++) brkSum[s + 1] = brkSum[s] + brk[s];
  const brokenIn = (a, b) => brkSum[b + 1] - brkSum[a + 1] > 0; // Unterbrechung in (a, b]
  let As = null;
  if (hasAlt) {
    As = new Array(dur + 1).fill(null);
    const k = rules.ALT_SMOOTH_S, AS = new Float64Array(dur + 2), AC = new Uint32Array(dur + 2);
    for (let s = 0; s <= dur; s++) { AS[s + 1] = AS[s] + A[s]; AC[s + 1] = AC[s] + Aok[s]; }
    for (let s = 0; s <= dur; s++) { const lo = Math.max(0, s - k), hi = Math.min(dur, s + k) + 1, c = AC[hi] - AC[lo]; As[s] = c > 0 ? (AS[hi] - AS[lo]) / c : null; }
  }

  // Gleitende 30-s-Fenster ab Minute 5
  const W = rules.WIN_S, wins = [];
  for (let w = rules.SKIP_S; w + W <= dur; w += rules.STEP_S) {
    if (brokenIn(w, w + W)) continue;
    if (hrCount(w, w + W) < W * rules.MIN_HR_SHARE) continue;
    const dd = D[w + W] - D[w];
    if (!(dd > 0)) continue;
    let flat = null;
    if (As && As[w] != null && As[w + W] != null) flat = Math.abs((As[w + W] - As[w]) / dd) < rules.FLAT_GRADE;
    wins.push({ w, v: dd / W, hr: hrMean(w, w + W), flat });
  }

  const rawM = rawDist[n - 1] - rawDist[0];
  const out = { gpsCorrectedM: gps.correctedM, gpsShare: rawM > 0 ? gps.correctedM / rawM : null };
  // Pace im HF-Band: Median der Fenster-Geschwindigkeiten; Zeit im Band = Vereinigung der Fenstersekunden
  // Fenster sind nach Start sortiert und gleich lang: Vereinigung = Summe der Abstaende (hoechstens W) plus W fuer das letzte
  const covered = (sel) => { let c = 0; for (let i = 0; i < sel.length; i++) c += i + 1 < sel.length ? Math.min(W, sel[i + 1].w - sel[i].w) : W; return c; };
  const paceOf = (sel) => (sel.length && covered(sel) >= rules.BAND_MIN_S ? Math.round(1000 / median(sel.map((x) => x.v))) : null);
  for (const band of rules.BANDS) {
    const sel = wins.filter((x) => x.hr >= band[0] && x.hr <= band[1]);
    out[`paceBand${bandKey(band)}`] = paceOf(sel);
    out[`secBand${bandKey(band)}`] = covered(sel);
    out[`paceBand${bandKey(band)}Flat`] = hasAlt ? paceOf(sel.filter((x) => x.flat === true)) : null;
  }

  // Effizienz-Regression: Geschwindigkeit (m/min) gegen HF, nur Fenster mit HF im Bereich
  const ef = wins.filter((x) => x.hr >= rules.EFF_HR[0] && x.hr <= rules.EFF_HR[1]);
  out.effAt130 = out.effSlope = out.effR2 = null;
  out.effN = ef.length;
  if (ef.length >= rules.EFF_MIN_N) {
    const xs = ef.map((x) => x.hr), ys = ef.map((x) => x.v * 60), mx = mean(xs), my = mean(ys);
    let sxx = 0, syy = 0, sxy = 0;
    for (let i = 0; i < xs.length; i++) { sxx += (xs[i] - mx) ** 2; syy += (ys[i] - my) ** 2; sxy += (xs[i] - mx) * (ys[i] - my); }
    if (sxx > 0) {
      const b = sxy / sxx;
      out.effSlope = r3(b);
      out.effAt130 = r1(my + b * (rules.EFF_AT - mx));
      out.effR2 = syy > 0 ? r3((sxy * sxy) / (sxx * syy)) : null;
    }
  }

  // Decoupling wie 6.5: Minute 5 bis Ende in zwei Haelften, Effizienz = (Distanzzuwachs / Zeit) / mittlere HF
  const effHalf = (a, b) => (b > a && hrCount(a, b + 1) >= 60 && D[b] > D[a] ? ((D[b] - D[a]) / (b - a)) / hrMean(a, b + 1) : null);
  const S = rules.SKIP_S, mid = Math.floor((S + dur) / 2);
  const e1 = dur > S ? effHalf(S, mid) : null, e2 = dur > S ? effHalf(mid, dur) : null;
  out.decoup = e1 && e2 ? Math.round((e1 / e2 - 1) * 1000) / 10 : null;

  // HF-Drift: Oe-HF letztes minus erstes Drittel (Minute 5 bis Ende), nur wenn die Pace beider Drittel unter 3 % abweicht
  out.hrDriftBpm = null;
  if (dur - S >= 3 * 60) {
    const t1 = S + Math.floor((dur - S) / 3), t2 = S + Math.floor((2 * (dur - S)) / 3);
    const v1 = (D[t1] - D[S]) / (t1 - S), v3 = (D[dur] - D[t2]) / (dur - t2);
    const h = (a, b) => (hrCount(a, b) >= 60 ? hrMean(a, b) : null);
    const h1 = h(S, t1), h3 = h(t2, dur);
    if (v1 > 0 && v3 > 0 && h1 != null && h3 != null && Math.abs(v3 / v1 - 1) < rules.DRIFT_PACE_TOL) out.hrDriftBpm = r1(h3 - h1);
  }

  // Gelaende: Anteil der gueltigen Fenster ab Minute 5 mit Steigung unter 2 %
  const graded = wins.filter((x) => x.flat != null);
  out.gradeShareFlat = graded.length ? r2(graded.filter((x) => x.flat).length / graded.length) : null;
  out.movingSec = dur;
  return out;
}

// ---------------------------------------------------------------------------------------------------------------------
// Zeile pro Lauf
// ---------------------------------------------------------------------------------------------------------------------

// Renntage aus dem Intervals-Kalender. raceDays: Kontext fuer classifyRun wie im Dashboard (buildRaceDays, mit Distanzabgleich
// auf 15 %). raceDaySet: jeder Tag mit einem Renn-Eintrag (Kategorie RACE, RACE_A/B/C, A_RACE bzw. A-Rennen), OHNE
// Distanzabgleich: ein abgebrochenes Rennen (03.10.2026, Halbmarathon nach 15,2 km beendet, Eintrag 21,1 km) ist trotzdem
// ein Rennen. events: Kalender-Eintraege des Bereichs (fetchIntervalsEvents).
export const isRaceEvent = (e) => isARaceEvent(e) || /^(RACE(_[A-C])?|A_RACE)$/.test(String(e?.category ?? "").trim().toUpperCase());
export function raceContext(events) {
  const list = Array.isArray(events) ? events : [];
  const raceDaySet = new Set(list.filter(isRaceEvent).map((e) => String(e?.start_date_local || e?.start_date || "").slice(0, 10)).filter(Boolean));
  return { raceDays: buildRaceDays(list), raceDaySet };
}

const isRaceRow = (a, ctx) => isRaceActivity(a) || Boolean(ctx?.raceDaySet?.has(activityDay(a))) || classifyRun(a, ctx) === "race";

// Grund, warum ein Lauf keine Zeitreihen-Kennzahlen bekommt (null = wird gerechnet). ctx: raceContext(events)
export function streamSkipOf(a, ctx = null) {
  if (isTreadmill(a)) return "treadmill";
  if (isRaceRow(a, ctx)) return "race";
  if (isIntervalActivity(a) || hasIntervalTextSignal(a)) return "interval";
  if ((num(a?.moving_time) ?? 0) < EXPORT_RULES.MIN_MOVING_S) return "short";
  if (!(a?.has_heartrate === true || num(a?.average_heartrate) != null)) return "noHr";
  return null;
}

export function baseRunRow(a, ctx = null) {
  const km = (num(a?.distance) ?? 0) / 1000, gain = num(a?.total_elevation_gain), load = sessionLoad(a);
  const tags = Array.isArray(a?.tags) ? a.tags.map(String) : [];
  return {
    id: a?.id ?? null, date: activityDay(a) || null, startLocal: a?.start_date_local ?? null, type: a?.type ?? null, name: a?.name ?? null,
    tags, treadmill: isTreadmill(a), race: isRaceRow(a, ctx), interval: isIntervalActivity(a) || hasIntervalTextSignal(a),
    runClass: isRaceRow(a, ctx) ? "race" : classifyRun(a, ctx), distKm: km > 0 ? r2(km) : null, movingMin: num(a?.moving_time) != null ? r1(num(a.moving_time) / 60) : null,
    elevPerKm: km > 0 && gain != null ? r1(gain / km) : null, avgHr: num(a?.average_heartrate), maxHr: num(a?.max_heartrate),
    rpe: num(a?.icu_rpe) ?? num(a?.perceived_exertion), feel: num(a?.feel), load: load > 0 ? r1(load) : null,
    // Koppellauf (export-tri.js, brickMap): ctx.bricks kommt aus der Action; ohne Kontext false
    ...brickCols(ctx?.bricks?.get(String(a?.id))),
  };
}
const brickCols = (b) => ({
  brick: Boolean(b), brickRideId: b?.brickRideId ?? null, brickGapMin: b?.brickGapMin ?? null, brickRideMin: b?.brickRideMin ?? null, brickRideLoad: b?.brickRideLoad ?? null,
});

// Zeile aus Stammdaten und Eintrag; entry null = noch nicht berechnet (pending). ctx: raceContext(events)
export function runRow(a, entry, ctx = null) {
  const row = baseRunRow(a, ctx);
  const skip = streamSkipOf(a, ctx);
  const w = entry?.weather ?? null;
  Object.assign(row, { tempC: w?.tempC ?? null, humidity: w?.humidity ?? null, dewPointC: dewPointC(w?.tempC, w?.humidity), windMs: w?.windMs ?? null, weatherSrc: w?.src ?? null });
  if (skip === "treadmill") Object.assign(row, { streamStatus: "skip", streamSkip: skip });
  else if (!entry) Object.assign(row, { streamStatus: "pending", streamSkip: skip });
  else Object.assign(row, { streamStatus: entry.streamStatus, streamSkip: entry.streamSkip ?? null });
  for (const c of METRIC_COLUMNS) row[c] = entry?.metrics?.[c] ?? null;
  row.methodVersion = METHOD_VERSION;
  return row;
}

// Wetter ueber Open-Meteo (Tageswerte je gerundetem Punkt; env.KV dient nur als Zwischenspeicher, im Skript im Speicher)
async function resolveWeather(env, a, latlng) {
  const day = await openMeteoDay(env, latlng.lat, latlng.lng, activityDay(a), { left: Infinity });
  if (!day) return null;
  const v = pickOpenMeteoValues(day, activityStartHourIso(a));
  return v ? { ...v, src: "openmeteo" } : null;
}

const STREAM_TYPES = ["time", "heartrate", "distance", "velocity_smooth", "altitude", "fixed_altitude"];

// Wetter einer Aktivitaet draussen: Karte (Startpunkt, Intervals-Wetter aus den Kartenpunkten), sonst Open-Meteo am Startpunkt.
// Auch fuer Radfahrten (export-tri.js). Intervals-Wetter hat keinen Wind in den Kartenpunkten (windMs dann null).
export async function weatherOf(env, a) {
  const map = await fetchIntervalsActivityMap(env, a.id);
  const latlng = map.status === 200 ? extractStartLatLng(map.data) : null;
  const native = map.status === 200 ? averageNativeWeather(map.data) : null;
  return native ? { ...native, windMs: null, src: "intervals" } : latlng ? await resolveWeather(env, a, latlng) : null;
}

// Rechnet einen Lauf: Karte (Startpunkt, Intervals-Wetter), sonst Open-Meteo, und ohne streamSkip die Zeitreihen.
// Eintrag: { streamStatus: ok|none|skip, streamSkip, metrics, weather: {tempC, humidity, windMs, src}|null }. Wirft bei
// Abruffehlern (ausser 404); der Aufrufer schreibt den Lauf dann als pending.
export async function computeRunEntry(env, a, ctx = null) {
  const skip = streamSkipOf(a, ctx);
  if (skip === "treadmill") return { streamStatus: "skip", streamSkip: skip, metrics: null, weather: null };
  const weather = await weatherOf(env, a);
  let streamStatus = "skip", metrics = null;
  if (!skip) {
    const st = await fetchIntervalsActivityStreamsStatus(env, a.id, STREAM_TYPES);
    metrics = st.status === 404 ? null : analyzeExportStreams(st.data);
    streamStatus = metrics ? "ok" : "none";
    // Schutzregel: entfernt die GPS-Bereinigung mehr als GPS_MAX_SHARE der Distanz, ist die Zeitreihe nicht belastbar. Keine
    // Kennzahlen, nur die entfernten Meter bleiben sichtbar.
    if (metrics && metrics.gpsShare != null && metrics.gpsShare > EXPORT_RULES.GPS_MAX_SHARE) {
      return { streamStatus: "skip", streamSkip: "gps", metrics: { gpsCorrectedM: metrics.gpsCorrectedM }, weather };
    }
  }
  return { streamStatus, streamSkip: skip, metrics, weather };
}

// Eintrag aus einer gespeicherten D1-Zeile (Zeitreihen nicht erneut abrufen)
export function entryFromRow(r) {
  const metrics = {};
  for (const c of METRIC_COLUMNS) metrics[c] = r?.[c] ?? null;
  return {
    streamStatus: r.streamStatus, streamSkip: r.streamSkip ?? null, metrics,
    weather: r.weatherSrc ? { tempC: r.tempC ?? null, humidity: r.humidity ?? null, windMs: r.windMs ?? null, src: r.weatherSrc } : null,
  };
}

// ---------------------------------------------------------------------------------------------------------------------
// Zeile pro Tag
// ---------------------------------------------------------------------------------------------------------------------

// Nicht-Workout-Eintraege des Kalenders (Notiz, Krank, Verletzt, Urlaub, Rennen ...) je Tag. Mehrtaegige Eintraege stehen an
// jedem Tag der Spanne (Ende im Kalender exklusiv, wie bei sick). Liefert Map date -> ["Name (KATEGORIE)", ...]
const CAL_MAX_DAYS = 120;
const SICK_NAME = /\b(OP|Operation|Verletzung)\b/i;
const isWorkoutEvent = (e) => /^WORKOUT$/i.test(String(e?.category ?? "").trim());
export function calendarByDay(events) {
  const map = new Map();
  for (const e of Array.isArray(events) ? events : []) {
    if (isWorkoutEvent(e)) continue;
    const from = String(e?.start_date_local ?? "").slice(0, 10), endRaw = String(e?.end_date_local ?? "").slice(0, 10);
    const to = endRaw > from ? addDays(endRaw, -1) : from;
    const name = String(e?.name ?? "").trim().slice(0, 60), cat = String(e?.category ?? "").trim().toUpperCase();
    const label = name ? (cat ? `${name} (${cat})` : name) : cat;
    if (!label) continue;
    const sick = /SICK|INJURED/i.test(cat) || (/NOTE/i.test(cat) && /krank/i.test(name)) || SICK_NAME.test(name);
    for (let d = from, k = 0; isIsoDate(d) && d <= to && k < CAL_MAX_DAYS; d = addDays(d, 1), k++) {
      const x = map.get(d) ?? { labels: [], sick: false };
      x.labels.push(label); x.sick = x.sick || sick;
      map.set(d, x);
    }
  }
  return map;
}

// Krankheitstage: Kategorie SICK/INJURED, Notiz "krank" oder Name mit OP/Operation/Verletzung (siehe calendarByDay)
export function sickDaysFromEvents(events) {
  const set = new Set();
  for (const [d, x] of calendarByDay(events)) if (x.sick) set.add(d);
  return set;
}

const pos = (v) => { const n = num(v); return n != null && n > 0 ? r1(n) : null; };
const scale = (v) => { const n = num(v); return n != null && n >= 1 ? n : null; };
const sportOfDay = (a) => (/swim/i.test(String(a?.type ?? "")) ? "swim" : isBike(a) ? "bike" : isRun(a) ? "run" : isStrengthActivity(a) ? "strength" : null);

export function buildDayRows(days, { wellness = [], activities = [], events = [], weights = [] } = {}) {
  const wBy = new Map((Array.isArray(wellness) ? wellness : []).map((w) => [String(w?.id ?? w?.date ?? "").slice(0, 10), w]));
  const kgBy = new Map((weights ?? []).map((w) => [w.date, w.kg]));
  const cal = calendarByDay(events);
  const agg = new Map();
  const blank = () => ({ load: { run: 0, bike: 0, swim: 0, strength: 0 }, sec: { run: 0, bike: 0, swim: 0 }, sets: 0, kcal: 0, kcalN: 0, kj: 0, kjN: 0 });
  for (const a of Array.isArray(activities) ? activities : []) {
    const d = activityDay(a), sport = sportOfDay(a);
    // Energie aller Aktivitaeten (auch Gehen, Yoga ...): calories = Schaetzung von Uhr/Intervals (die Zahl, die Yazio als Sport
    // importiert); icu_joules = mechanische Arbeit am Rad aus der Leistung (1 kJ Arbeit ≈ 1 kcal Nahrungsenergie)
    const kcal = num(a?.calories), joules = isBike(a) ? num(a?.icu_joules) : null;
    if (d && ((kcal != null && kcal > 0) || (joules != null && joules > 0))) {
      const x = agg.get(d) ?? blank();
      if (kcal != null && kcal > 0) { x.kcal += kcal; x.kcalN++; }
      if (joules != null && joules > 0) { x.kj += joules / 1000; x.kjN++; }
      agg.set(d, x);
    }
    if (!sport) continue;
    const x = agg.get(d) ?? blank();
    x.load[sport] += sessionLoad(a);
    if (sport === "swim") x.sec.swim += swimTimeOf(a).sec;
    else if (sport !== "strength") x.sec[sport] += num(a?.moving_time) ?? 0;
    else x.sets += strengthSetsOf(a);
    agg.set(d, x);
  }
  return days.map((date) => {
    const w = wBy.get(date), x = agg.get(date);
    const sleepSecs = num(w?.sleepSecs);
    return {
      date, ctl: r1(num(w?.ctl)), atl: r1(num(w?.atl)), rampRate: r1(num(w?.rampRate)),
      loadRun: x ? Math.round(x.load.run) : 0, loadBike: x ? Math.round(x.load.bike) : 0, loadSwim: x ? Math.round(x.load.swim) : 0, loadStrength: x ? Math.round(x.load.strength) : 0,
      minRun: x ? Math.round(x.sec.run / 60) : 0, minBike: x ? Math.round(x.sec.bike / 60) : 0, minSwim: x ? Math.round(x.sec.swim / 60) : 0, strengthSets: x ? x.sets : 0,
      hrv: pos(w?.hrv), restHr: scale(w?.restingHR), sleepH: sleepSecs > 0 ? r1(sleepSecs / 3600) : null, sleepScore: pos(w?.sleepScore),
      sleepQuality: scale(w?.sleepQuality), fatigue: scale(w?.fatigue), soreness: scale(w?.soreness), mood: scale(w?.mood), motivation: scale(w?.motivation),
      sick: cal.get(date)?.sick ?? false, calendar: cal.has(date) ? cal.get(date).labels.join("; ") : null, weightKg: kgBy.get(date) ?? pos(w?.weight),
      kcal: pos(w?.Calories), kcalGoal: pos(w?.CalorieGoal), protein: pos(w?.Protein), carbs: pos(w?.Carbs),
      kcalActivities: x?.kcalN ? Math.round(x.kcal) : null, kjBike: x?.kjN ? Math.round(x.kj) : null,
      methodVersion: METHOD_VERSION,
    };
  });
}

