// Export fuer die Langzeitanalyse des Coaches: eine Zeile pro Lauf und eine Zeile pro Tag (DOKUMENTATION.md, Teil B 13).
// Der Worker liefert nur Rohwerte und Kennzahlen aus den Zeitreihen, er bewertet nichts (CLAUDE.md: das Dashboard zeigt,
// der Coach entscheidet). Zugaenge: MCP get_run_export / get_day_export (mcp-server.js), HTTP /api/export/runs|days (index.js).
import { json } from "./http-helpers.js";
import { hasKv, readKvJson, writeKvJson } from "./kv.js";
import { isIsoDate, isoDate, isoDateBerlin } from "./date-utils.js";
import { activityDay, sessionLoad, isRun, isBike, isTreadmill, isRaceActivity, isIntervalActivity, hasIntervalTextSignal } from "./activity-utils.js";
import { fetchIntervalsActivities, fetchIntervalsEvents, fetchIntervalsWellnessRange, fetchIntervalsActivityMap, fetchIntervalsActivityStreamsStatus } from "./intervals-client.js";
import { runMovingAxis, cleanRunDistance, RUN_RULES } from "./block-inputs.js";
import { classifyRun, isAuthorized } from "./dashboard.js";
import { averageNativeWeather, extractStartLatLng, openMeteoDay, pickOpenMeteoValues, activityStartHourIso } from "./weather.js";
import { isStrengthActivity, strengthSetsOf } from "./week-totals.js";
import { swimTimeOf } from "./swim-time.js";
import { readWeights } from "./ernaehrung.js";

// Methode der Zeitreihen-Kennzahlen. Aendert sich die Rechnung, Zahl erhoehen: neuer KV-Schluessel, alles wird neu gerechnet.
export const METHOD_VERSION = 1;
const ENTRY_KEY = (id) => `icu:export:v${METHOD_VERSION}:${id}`;
const DONE_KEY = `icu:export:v${METHOD_VERSION}:done`; // Liste der Aktivitaets-IDs mit fertigem Eintrag (fuer pending und Cron)
const STATE_KEY = `icu:export:v${METHOD_VERSION}:state`; // { doneThrough }: bis zu diesem Tag ist alles berechnet (Cron)
const FIRST_DAY_KEY = "icu:export:firstDay"; // erster Tag mit einer Aktivitaet in Intervals

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
};

// Grenzen des kostenlosen Cloudflare-Plans (Commit 08d6aec: "[limits]" entfernt "for free-plan deploys"):
// 50 Subrequests und 10 ms CPU je Aufruf (Anfrage oder Cron-Tick). Gemessen (DOKUMENTATION.md 13.4): Zeitreihen eines
// 60-min-Laufs parsen + auswerten ≈ 4 ms warm, ≈ 13 ms beim ersten Aufruf im frischen Isolate. Darum je Aufruf hoechstens
// RUNS_PER_* Laeufe mit Zeitreihen (Variablen EXPORT_RUNS_PER_CALL / EXPORT_RUNS_PER_TICK, Tick hoechstens 2) und die
// Aktivitaeten in 60-Tage-Scheiben statt der ganzen Historie (2500 Aktivitaeten parsen ≈ 29 ms).
// Abrufe: ein Lauf kostet hoechstens 3 (Karte, Zeitreihen, Open-Meteo); Laeufe ohne Zeitreihen (Rennen, Intervall ...) nur
// Karte und Wetter, die zaehlen nicht gegen RUNS_PER_*.
export const API_FETCH_BUDGET = 30;
export const CRON_FETCH_BUDGET = 8;
export const RUNS_PER_CALL = 1;
export const RUNS_PER_TICK = 1;
export const MAX_RUNS_PER_TICK = 2;
export const SLICE_DAYS = 60; // Aktivitaetsliste in Scheiben (≈ 80 Aktivitaeten ≈ 1 ms parsen)
const MAX_SLICES_PER_CALL = 12; // Export-Seite: hoechstens 2 Jahre durchsuchen, dann nextCursor
const FIRST_DAY_EMPTY_SLICES = 6; // erster Tag: nach 6 leeren Scheiben (ein Jahr ohne Aktivitaet) gilt die Suche als fertig
const runsPer = (v, def, max = Infinity) => { const n = Math.floor(num(v) ?? def); return Math.max(0, Math.min(max, n)); };
export const DEFAULT_LIMIT = 200;
export const MAX_LIMIT = 500;
const WEATHER_RETRY_DAYS = 14; // Open-Meteo-Archiv hinkt hinterher: fehlendes Wetter so lange beim Lesen erneut fragen
const STREAM_TYPES = ["time", "heartrate", "distance", "velocity_smooth", "altitude", "fixed_altitude"];

const num = (v) => (v != null && v !== "" && Number.isFinite(Number(v)) ? Number(v) : null);
const r1 = (v) => (v == null ? null : Math.round(v * 10) / 10);
const r2 = (v) => (v == null ? null : Math.round(v * 100) / 100);
const r3 = (v) => (v == null ? null : Math.round(v * 1000) / 1000);
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);
const median = (xs) => { const s = [...xs].sort((a, b) => a - b), m = s.length >> 1; return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2; };
const addDays = (iso, n) => isoDate(new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000));
const daysBetween = (a, b) => Math.round((Date.parse(b + "T00:00:00Z") - Date.parse(a + "T00:00:00Z")) / 86400000);

// ---------------------------------------------------------------------------------------------------------------------
// Kennzahlen aus den Zeitreihen
// ---------------------------------------------------------------------------------------------------------------------

export const METRIC_COLUMNS = [
  "paceBand125_135", "secBand125_135", "paceBand120_128", "secBand120_128", "paceBand125_135Flat", "paceBand120_128Flat",
  "effAt130", "effSlope", "effR2", "effN", "decoup", "hrDriftBpm", "gradeShareFlat", "gpsCorrectedM",
];
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
  const mt = runMovingAxis(elapsed, rawDist);
  const { dist, gps } = cleanRunDistance(mt, rawDist);
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

  const out = { gpsCorrectedM: gps.correctedM };
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

export const RUN_COLUMNS = [
  "id", "date", "startLocal", "type", "name", "tags", "treadmill", "race", "interval", "runClass", "distKm", "movingMin",
  "elevPerKm", "avgHr", "maxHr", "rpe", "feel", "load", "tempC", "humidity", "windMs", "weatherSrc", "streamStatus", "streamSkip",
  ...METRIC_COLUMNS,
];

// Grund, warum ein Lauf keine Zeitreihen-Kennzahlen bekommt (null = wird gerechnet)
export function streamSkipOf(a) {
  if (isTreadmill(a)) return "treadmill";
  if (isRaceActivity(a) || classifyRun(a) === "race") return "race";
  if (isIntervalActivity(a) || hasIntervalTextSignal(a)) return "interval";
  if ((num(a?.moving_time) ?? 0) < EXPORT_RULES.MIN_MOVING_S) return "short";
  if (!(a?.has_heartrate === true || num(a?.average_heartrate) != null)) return "noHr";
  return null;
}

export function baseRunRow(a) {
  const km = (num(a?.distance) ?? 0) / 1000, gain = num(a?.total_elevation_gain), load = sessionLoad(a);
  const tags = Array.isArray(a?.tags) ? a.tags.map(String) : [];
  return {
    id: a?.id ?? null, date: activityDay(a) || null, startLocal: a?.start_date_local ?? null, type: a?.type ?? null, name: a?.name ?? null,
    tags, treadmill: isTreadmill(a), race: isRaceActivity(a) || classifyRun(a) === "race", interval: isIntervalActivity(a) || hasIntervalTextSignal(a),
    runClass: classifyRun(a), distKm: km > 0 ? r2(km) : null, movingMin: num(a?.moving_time) != null ? r1(num(a.moving_time) / 60) : null,
    elevPerKm: km > 0 && gain != null ? r1(gain / km) : null, avgHr: num(a?.average_heartrate), maxHr: num(a?.max_heartrate),
    rpe: num(a?.icu_rpe) ?? num(a?.perceived_exertion), feel: num(a?.feel), load: load > 0 ? r1(load) : null,
  };
}

// Zeile aus Stammdaten und Cache-Eintrag; entry null = noch nicht berechnet
export function runRow(a, entry) {
  const row = baseRunRow(a);
  const skip = streamSkipOf(a);
  const w = entry?.weather ?? null;
  Object.assign(row, { tempC: w?.tempC ?? null, humidity: w?.humidity ?? null, windMs: w?.windMs ?? null, weatherSrc: w?.src ?? null });
  if (skip === "treadmill") Object.assign(row, { streamStatus: "skip", streamSkip: skip });
  else if (!entry) Object.assign(row, { streamStatus: "pending", streamSkip: skip });
  else Object.assign(row, { streamStatus: entry.streamStatus, streamSkip: entry.streamSkip ?? null });
  for (const c of METRIC_COLUMNS) row[c] = entry?.metrics?.[c] ?? null;
  return row;
}

async function resolveWeather(env, a, latlng, budget) {
  const day = await openMeteoDay(env, latlng.lat, latlng.lng, activityDay(a), budget);
  if (!day) return null;
  const v = pickOpenMeteoValues(day, activityStartHourIso(a));
  return v ? { ...v, src: "openmeteo" } : null;
}

// Cache-Eintrag eines Laufs (nicht Laufband) lesen oder rechnen. undefined = Budget reicht nicht oder Fehler (bleibt pending).
// Eintrag: { streamStatus: ok|none|skip, streamSkip, metrics, weather: {tempC, humidity, windMs, src}|null, latlng, date }
export async function resolveRunEntry(env, a, budget, today = isoDateBerlin()) {
  const key = ENTRY_KEY(a.id);
  const hit = hasKv(env) ? await readKvJson(env, key).catch(() => null) : null;
  if (hit) {
    // Wetter fehlte, weil das Archiv noch leer war: in den ersten Tagen beim Lesen erneut fragen
    if (!hit.weather && hit.latlng && daysBetween(hit.date, today) <= WEATHER_RETRY_DAYS && budget.left > 0) {
      const weather = await resolveWeather(env, a, hit.latlng, budget).catch(() => null);
      if (weather) { hit.weather = weather; await writeKvJson(env, key, hit).catch(() => {}); }
    }
    return hit;
  }
  const skip = streamSkipOf(a);
  if (budget.left < (skip ? 2 : 3)) return undefined;
  if (!skip && budget.runs != null && budget.runs <= 0) return undefined; // CPU: Zeitreihen nur fuer RUNS_PER_* Laeufe
  if (!skip && budget.runs != null) budget.runs--;
  try {
    budget.left--;
    const map = await fetchIntervalsActivityMap(env, a.id);
    const latlng = map.status === 200 ? extractStartLatLng(map.data) : null;
    let weather = null;
    const native = map.status === 200 ? averageNativeWeather(map.data) : null;
    // Intervals-Wetter hat keinen Wind in den Kartenpunkten, der Wind kommt nur aus Open-Meteo (windMs sonst null)
    if (native) weather = { ...native, windMs: null, src: "intervals" };
    else if (latlng) weather = await resolveWeather(env, a, latlng, budget);
    let streamStatus = "skip", metrics = null;
    if (!skip) {
      budget.left--;
      const st = await fetchIntervalsActivityStreamsStatus(env, a.id, STREAM_TYPES);
      if (st.status === 404) streamStatus = "none";
      else {
        const m = analyzeExportStreams(st.data);
        if (m) { streamStatus = "ok"; metrics = m; } else streamStatus = "none";
        if (budget.log) budget.log.push({ id: a.id, samples: Array.isArray(st.data) ? st.data.find((x) => x?.type === "time")?.data?.length ?? null : null, movingSec: m?.movingSec ?? null });
      }
    }
    const entry = { streamStatus, streamSkip: skip, metrics, weather, latlng, date: activityDay(a) };
    if (hasKv(env)) await writeKvJson(env, key, entry);
    return entry;
  } catch (e) {
    console.warn("export run", a?.id, String(e?.message ?? e));
    return undefined;
  }
}

const runKey = (a) => `${a?.start_date_local ?? ""}|${a?.id ?? ""}`;
const sortRuns = (acts) => (Array.isArray(acts) ? acts : []).filter(isRun).sort((x, y) => runKey(x).localeCompare(runKey(y)));

async function readDone(env) {
  const v = hasKv(env) ? await readKvJson(env, DONE_KEY).catch(() => null) : null;
  return new Set(Array.isArray(v?.ids) ? v.ids.map(String) : []);
}
async function writeDone(env, set) {
  if (hasKv(env)) await writeKvJson(env, DONE_KEY, { ids: [...set] }).catch(() => {});
}

async function readState(env) {
  return (hasKv(env) ? await readKvJson(env, STATE_KEY).catch(() => null) : null) ?? {};
}

// Erster Tag mit Aktivitaet: sucht der Cron rueckwaerts in Scheiben (backfillRunExport). Bis er fertig ist, gilt der frueheste
// bisher gefundene Tag, sonst ein Jahr zurueck. known = false, solange die Suche laeuft.
export async function firstExportDay(env) {
  const hit = hasKv(env) ? await readKvJson(env, FIRST_DAY_KEY).catch(() => null) : null;
  if (isIsoDate(hit?.day)) return { day: hit.day, known: true };
  const st = await readState(env);
  return { day: isIsoDate(st.earliest) ? st.earliest : addDays(isoDateBerlin(), -365), known: false };
}

// Bereich und Seitengroesse aus den Parametern; Standard: erster Tag bis gestern
export async function resolveExportArgs(env, args = {}) {
  const yesterday = addDays(isoDateBerlin(), -1);
  const first = isIsoDate(args.from) ? null : await firstExportDay(env);
  const from = first ? first.day : args.from;
  const to = isIsoDate(args.to) ? args.to : yesterday;
  if (from > to) throw new Error(`from (${from}) liegt nach to (${to})`);
  const lim = Math.floor(num(args.limit) ?? DEFAULT_LIMIT);
  const limit = Math.max(1, Math.min(MAX_LIMIT, lim));
  return { from, to, limit, cursor: args.cursor ? String(args.cursor) : null, firstDayKnown: first ? first.known : null };
}

// Laeufe ab dem Cursor in 60-Tage-Scheiben einsammeln, bis limit + 1 Laeufe da sind, to erreicht ist oder MAX_SLICES_PER_CALL
// Scheiben gelesen sind. scannedTo = letzter durchsuchter Tag.
async function collectRuns(env, from, to, cursor, limit) {
  const cursorDay = cursor && isIsoDate(cursor.slice(0, 10)) ? cursor.slice(0, 10) : null;
  let day = cursorDay && cursorDay > from ? cursorDay : from, scannedTo = addDays(day, -1), slices = 0;
  const runs = [];
  while (day <= to && runs.length <= limit && slices < MAX_SLICES_PER_CALL) {
    const end = addDays(day, SLICE_DAYS - 1) < to ? addDays(day, SLICE_DAYS - 1) : to;
    const acts = sortRuns(await fetchIntervalsActivities(env, day, end));
    slices++;
    for (const a of acts) { const d = activityDay(a); if (d >= day && d <= end && (!cursor || runKey(a) > cursor)) runs.push(a); }
    scannedTo = end;
    day = addDays(end, 1);
  }
  return { runs, scannedTo, slices };
}

export async function buildRunExport(env, args = {}, { budget = null } = {}) {
  const { from, to, limit, cursor, firstDayKnown } = await resolveExportArgs(env, args);
  budget = budget ?? { left: API_FETCH_BUDGET, runs: runsPer(env?.EXPORT_RUNS_PER_CALL, RUNS_PER_CALL) };
  const { runs, scannedTo, slices } = await collectRuns(env, from, to, cursor, limit);
  const page = runs.slice(0, limit);
  const done = await readDone(env), before = done.size;
  const today = isoDateBerlin();
  const rows = [];
  for (const a of page) {
    if (isTreadmill(a) || a?.id == null) { rows.push(runRow(a, null)); continue; }
    const entry = await resolveRunEntry(env, a, budget, today);
    if (entry) done.add(String(a.id));
    rows.push(runRow(a, entry ?? null));
  }
  if (done.size !== before) await writeDone(env, done);
  // Weiter: mehr Laeufe als limit gefunden, oder noch nicht bis to gesucht (dann ab dem Ende der letzten Scheibe; "~" sortiert
  // hinter jede Uhrzeit des Tages)
  const nextCursor = runs.length > limit ? runKey(page[page.length - 1]) : scannedTo < to ? `${scannedTo}T~|` : null;
  const st = await readState(env);
  console.log("export runs", JSON.stringify({ from, to, rows: rows.length, slices, computed: budget.log ?? null, runsLeft: budget.runs, fetchesLeft: budget.left }));
  return {
    methodVersion: METHOD_VERSION, from, to, count: rows.length,
    pending: rows.filter((r) => r.streamStatus === "pending").length,
    backfillDoneThrough: isIsoDate(st.doneThrough) ? st.doneThrough : null, firstDayKnown,
    nextCursor, rows,
  };
}

// Cron (eigener Trigger, siehe index.js): Phase 1 sucht den ersten Tag rueckwaerts in 60-Tage-Scheiben (eine je Tick), Phase 2
// rechnet offene Laeufe nach, aelteste zuerst, hoechstens RUNS_PER_TICK Laeufe mit Zeitreihen je Tick. doneThrough merkt sich,
// bis wohin alles fertig ist. Jede Runde schreibt eine Logzeile; die CPU-Zeit des Aufrufs steht in Workers Logs (cpuTimeMs).
export async function backfillRunExport(env, { budget = null, maxChecks = 100 } = {}) {
  if (!hasKv(env)) return { skipped: "no kv" };
  budget = budget ?? { left: CRON_FETCH_BUDGET, runs: runsPer(env?.EXPORT_RUNS_PER_TICK, RUNS_PER_TICK, MAX_RUNS_PER_TICK), log: [] };
  const state = await readState(env);
  const today = isoDateBerlin(), yesterday = addDays(today, -1);
  const cachedFirst = await readKvJson(env, FIRST_DAY_KEY).catch(() => null);
  let result;
  if (!isIsoDate(cachedFirst?.day)) {
    // Phase 1: erster Tag
    const end = isIsoDate(state.scanEnd) ? state.scanEnd : today, start = addDays(end, -(SLICE_DAYS - 1));
    budget.left--;
    const days = (await fetchIntervalsActivities(env, start, end) ?? []).map(activityDay).filter(isIsoDate).sort();
    const earliest = days.length ? (isIsoDate(state.earliest) && state.earliest < days[0] ? state.earliest : days[0]) : state.earliest ?? null;
    const empty = days.length ? 0 : (state.emptySlices ?? 0) + 1;
    if (empty >= FIRST_DAY_EMPTY_SLICES || start < "2000-01-01") {
      await writeKvJson(env, FIRST_DAY_KEY, { day: earliest ?? yesterday });
      await writeKvJson(env, STATE_KEY, { ...state, earliest, scanEnd: null, emptySlices: 0 });
      result = { phase: "firstDay", firstDay: earliest ?? yesterday };
    } else {
      await writeKvJson(env, STATE_KEY, { ...state, earliest, scanEnd: addDays(start, -1), emptySlices: empty });
      result = { phase: "firstDay", scanned: `${start}..${end}`, earliest };
    }
  } else {
    // Phase 2: offene Laeufe der naechsten Scheibe
    const from = isIsoDate(state.doneThrough) ? addDays(state.doneThrough, 1) : cachedFirst.day;
    if (from > yesterday) result = { phase: "runs", processed: 0, pending: 0 };
    else {
      const to = addDays(from, SLICE_DAYS - 1) < yesterday ? addDays(from, SLICE_DAYS - 1) : yesterday;
      budget.left--;
      const runs = sortRuns(await fetchIntervalsActivities(env, from, to)).filter((a) => !isTreadmill(a) && a?.id != null && activityDay(a) >= from && activityDay(a) <= to);
      const done = await readDone(env), before = done.size;
      let processed = 0, checks = 0;
      for (const a of runs) {
        if (done.has(String(a.id))) continue;
        if (checks++ >= maxChecks || budget.left < 2) break;
        const entry = await resolveRunEntry(env, a, budget, today);
        if (entry) { done.add(String(a.id)); processed++; }
      }
      if (done.size !== before) await writeDone(env, done);
      const open = runs.filter((a) => !done.has(String(a.id)));
      const doneThrough = open.length ? addDays(activityDay(open[0]), -1) : to;
      if (doneThrough !== state.doneThrough) await writeKvJson(env, STATE_KEY, { ...state, doneThrough });
      result = { phase: "runs", slice: `${from}..${to}`, processed, pendingInSlice: open.length, doneThrough };
    }
  }
  console.log("run export backfill", JSON.stringify({ ...result, computed: budget.log, runsLeft: budget.runs, fetchesLeft: budget.left }));
  return result;
}

// ---------------------------------------------------------------------------------------------------------------------
// Zeile pro Tag
// ---------------------------------------------------------------------------------------------------------------------

export const DAY_COLUMNS = [
  "date", "ctl", "atl", "rampRate", "loadRun", "loadBike", "loadSwim", "loadStrength", "minRun", "minBike", "minSwim", "strengthSets",
  "hrv", "restHr", "sleepH", "sleepScore", "sleepQuality", "fatigue", "soreness", "mood", "motivation", "sick", "weightKg",
  "kcal", "kcalGoal", "protein", "carbs",
];

// Krankheitstage wie dashboard.js (6.2): Kategorie SICK/INJURED oder Notiz "krank", Ende im Kalender exklusiv
export function sickDaysFromEvents(events) {
  const set = new Set();
  for (const e of Array.isArray(events) ? events : []) {
    const cat = String(e?.category ?? "");
    if (!(/SICK|INJURED/i.test(cat) || (/NOTE/i.test(cat) && /krank/i.test(String(e?.name ?? ""))))) continue;
    const from = String(e.start_date_local ?? "").slice(0, 10), endRaw = String(e.end_date_local ?? "").slice(0, 10);
    const to = endRaw > from ? addDays(endRaw, -1) : from;
    for (let d = from, k = 0; isIsoDate(d) && d <= to && k < 60; d = addDays(d, 1), k++) set.add(d);
  }
  return set;
}

const pos = (v) => { const n = num(v); return n != null && n > 0 ? r1(n) : null; };
const scale = (v) => { const n = num(v); return n != null && n >= 1 ? n : null; };
const sportOfDay = (a) => (/swim/i.test(String(a?.type ?? "")) ? "swim" : isBike(a) ? "bike" : isRun(a) ? "run" : isStrengthActivity(a) ? "strength" : null);

export function buildDayRows(days, { wellness = [], activities = [], events = [], weights = [] } = {}) {
  const wBy = new Map((Array.isArray(wellness) ? wellness : []).map((w) => [String(w?.id ?? w?.date ?? "").slice(0, 10), w]));
  const kgBy = new Map((weights ?? []).map((w) => [w.date, w.kg]));
  const sick = sickDaysFromEvents(events);
  const agg = new Map();
  for (const a of Array.isArray(activities) ? activities : []) {
    const d = activityDay(a), sport = sportOfDay(a);
    if (!sport) continue;
    const x = agg.get(d) ?? { load: { run: 0, bike: 0, swim: 0, strength: 0 }, sec: { run: 0, bike: 0, swim: 0 }, sets: 0 };
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
      sick: sick.has(date), weightKg: kgBy.get(date) ?? pos(w?.weight),
      kcal: pos(w?.Calories), kcalGoal: pos(w?.CalorieGoal), protein: pos(w?.Protein), carbs: pos(w?.Carbs),
    };
  });
}

export async function buildDayExport(env, args = {}) {
  const { from, to, limit, cursor, firstDayKnown } = await resolveExportArgs(env, args);
  const start = isIsoDate(cursor) && cursor > from ? cursor : from;
  if (start > to) return { methodVersion: METHOD_VERSION, from, to, count: 0, pending: 0, nextCursor: null, rows: [] };
  const endCand = addDays(start, limit - 1), end = endCand < to ? endCand : to;
  const [wellness, activities, events, weights] = await Promise.all([
    fetchIntervalsWellnessRange(env, start, end),
    fetchIntervalsActivities(env, start, end),
    fetchIntervalsEvents(env, addDays(start, -60), end), // Krankheit, die vor der Seite begonnen hat
    readWeights(env).catch(() => []),
  ]);
  const days = [];
  for (let d = start; d <= end; d = addDays(d, 1)) days.push(d);
  const rows = buildDayRows(days, { wellness, activities, events, weights });
  return { methodVersion: METHOD_VERSION, from, to, count: rows.length, pending: 0, firstDayKnown, nextCursor: end < to ? addDays(end, 1) : null, rows };
}

// ---------------------------------------------------------------------------------------------------------------------
// Ausgabeformate und HTTP
// ---------------------------------------------------------------------------------------------------------------------

const cell = (v) => {
  if (v == null) return "";
  const s = Array.isArray(v) ? v.join("|") : String(v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
export function toCsv(rows, columns) {
  return [columns.join(","), ...rows.map((r) => columns.map((c) => cell(r[c])).join(","))].join("\n") + "\n";
}
// table: Spaltennamen einmal, Zeilen als Arrays (kompakt fuer den Coach)
export function toTable(result, columns) {
  return { ...result, columns, rows: result.rows.map((r) => columns.map((c) => r[c] ?? null)) };
}

export async function handleExportRequest(req, env, kind) {
  if (req.method !== "GET") return json({ ok: false, error: "Nur GET erlaubt" }, 405, { allow: "GET" });
  if (!isAuthorized(req, env)) return json({ ok: false, error: "unauthorized" }, 401);
  const p = new URL(req.url).searchParams;
  const args = { from: p.get("from"), to: p.get("to"), cursor: p.get("cursor"), limit: p.get("limit") };
  for (const k of ["from", "to"]) if (args[k] && !isIsoDate(args[k])) return json({ ok: false, error: `${k} muss YYYY-MM-DD sein` }, 400);
  let result;
  try {
    result = kind === "runs" ? await buildRunExport(env, args) : await buildDayExport(env, args);
  } catch (e) {
    return json({ ok: false, error: String(e?.message ?? e) }, 502);
  }
  const columns = kind === "runs" ? RUN_COLUMNS : DAY_COLUMNS;
  const format = p.get("format");
  if (format === "csv") {
    return new Response(toCsv(result.rows, columns), {
      headers: {
        "content-type": "text/csv; charset=utf-8",
        "x-next-cursor": result.nextCursor ?? "", "x-pending": String(result.pending), "x-method-version": String(METHOD_VERSION),
      },
    });
  }
  return json(format === "table" ? toTable(result, columns) : result);
}
