import { readKvJson, writeKvJson } from "./kv.js";

const OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive";
const WEATHER_KV_PREFIX = "weather:";
// ~111m at the equator - plenty precise for "was it hot", and coarse enough that
// runs starting a few meters apart (e.g. loops from the same front door) share a
// cache entry instead of each re-fetching Open-Meteo.
const COORD_DECIMALS = 3;

function roundCoord(v) {
  return Math.round(Number(v) * 10 ** COORD_DECIMALS) / 10 ** COORD_DECIMALS;
}

function round1(v) {
  return Math.round(Number(v) * 10) / 10;
}

// Averages temp/humidity across every per-point time sample the intervals.icu map
// endpoint returns for this activity. A single instantaneous reading would be more
// precise for "the exact moment of the run", but the endpoint doesn't flag which
// sample is that moment, and an average over the run's own duration is exactly what
// "was this run generally hot" needs.
function averageNativeWeather(mapData) {
  const points = Array.isArray(mapData?.weather?.points) ? mapData.weather.points : [];
  let tempSum = 0;
  let tempCount = 0;
  let humiditySum = 0;
  let humidityCount = 0;
  for (const point of points) {
    const times = Array.isArray(point?.times) ? point.times : [];
    for (const t of times) {
      const temp = Number(t?.temp);
      if (Number.isFinite(temp)) {
        tempSum += temp;
        tempCount++;
      }
      const humidity = Number(t?.humidity);
      if (Number.isFinite(humidity)) {
        humiditySum += humidity;
        humidityCount++;
      }
    }
  }
  if (tempCount === 0 || humidityCount === 0) return null;
  return { tempC: round1(tempSum / tempCount), humidityPct: round1(humiditySum / humidityCount) };
}

function extractStartLatLng(mapData) {
  const first = Array.isArray(mapData?.latlngs) ? mapData.latlngs[0] : null;
  const lat = Number(first?.[0]);
  const lng = Number(first?.[1]);
  return Number.isFinite(lat) && Number.isFinite(lng) ? { lat, lng } : null;
}

function weatherKvKey(lat, lng, dateIso) {
  return `${WEATHER_KV_PREFIX}${roundCoord(lat)}:${roundCoord(lng)}:${dateIso}`;
}

function activityWeatherKvKey(activityId) {
  return `${WEATHER_KV_PREFIX}activity:${activityId}`;
}

// A resolved run's weather never changes once found, so it's cached indefinitely,
// keyed by activity id rather than lat/lng/date - this is what lets a wide `days=`
// call spread its cost across repeated requests instead of needing every run's
// map + Open-Meteo fetch to fit in one Worker invocation's subrequest budget.
// Only ever called with a non-null weather record (see enrichRunsWithWeather in
// form-analysis.js) - a null result might just be a transient/budget failure, so
// that's deliberately left uncached to retry on the next call.
export async function readCachedActivityWeather(env, activityId) {
  return readKvJson(env, activityWeatherKvKey(activityId));
}

export async function writeCachedActivityWeather(env, activityId, weather) {
  return writeKvJson(env, activityWeatherKvKey(activityId), weather);
}

function activityGpsKvKey(activityId) {
  return `${WEATHER_KV_PREFIX}gps:${activityId}`;
}

// A completed activity's GPS track never changes either, so once /map has been
// fetched once, cache what it found (a start point, or its confirmed absence) so a
// later retry - see enrichRunsWithWeather in form-analysis.js - can skip straight to
// Open-Meteo instead of repeating the (already-successful) map fetch. Stored as
// `{ none: true }` rather than a bare null so a cache hit is distinguishable from
// "never looked up yet" (readKvJson also returns null for a missing key).
export async function readCachedActivityGps(env, activityId) {
  return readKvJson(env, activityGpsKvKey(activityId));
}

export async function writeCachedActivityGps(env, activityId, latLng) {
  return writeKvJson(env, activityGpsKvKey(activityId), latLng ?? { none: true });
}

// Past weather never changes, so this is cached indefinitely (no TTL/max-age check,
// unlike the maxHr cache in intervals-client.js) - a cache hit is always still valid.
async function fetchOpenMeteoDay(lat, lng, dateIso) {
  const url =
    `${OPEN_METEO_ARCHIVE_URL}?latitude=${roundCoord(lat)}&longitude=${roundCoord(lng)}` +
    `&start_date=${dateIso}&end_date=${dateIso}&hourly=temperature_2m,relative_humidity_2m&timezone=auto`;
  const r = await fetch(url).catch(() => null);
  if (!r?.ok) return null;
  const data = await r.json().catch(() => null);
  const times = data?.hourly?.time;
  const temps = data?.hourly?.temperature_2m;
  const humidities = data?.hourly?.relative_humidity_2m;
  if (!Array.isArray(times) || !Array.isArray(temps) || !Array.isArray(humidities)) return null;
  return { times, temps, humidities };
}

async function getOpenMeteoDay(env, lat, lng, dateIso) {
  const key = weatherKvKey(lat, lng, dateIso);
  const cached = await readKvJson(env, key);
  if (cached) return cached;
  const fetched = await fetchOpenMeteoDay(lat, lng, dateIso);
  if (fetched) await writeKvJson(env, key, fetched);
  return fetched;
}

// Picks the hourly value matching the run's local start hour; falls back to the
// day's average across all hours when the run's hour isn't available/known.
function pickOpenMeteoValues(dayData, hourIso) {
  const idx = hourIso ? dayData.times.indexOf(hourIso) : -1;
  if (idx >= 0) {
    const temp = Number(dayData.temps[idx]);
    const humidity = Number(dayData.humidities[idx]);
    if (Number.isFinite(temp) && Number.isFinite(humidity)) return { tempC: round1(temp), humidityPct: round1(humidity) };
  }
  const validTemps = dayData.temps.filter((v) => Number.isFinite(Number(v))).map(Number);
  const validHumidities = dayData.humidities.filter((v) => Number.isFinite(Number(v))).map(Number);
  if (!validTemps.length || !validHumidities.length) return null;
  return {
    tempC: round1(validTemps.reduce((a, b) => a + b, 0) / validTemps.length),
    humidityPct: round1(validHumidities.reduce((a, b) => a + b, 0) / validHumidities.length),
  };
}

// activityDay()-style local-hour extraction (see activity-utils.js): reads the hour
// straight out of start_date_local instead of re-parsing/re-zoning the timestamp.
function activityStartHourIso(activity) {
  const local = String(activity?.start_date_local || "");
  return /^\d{4}-\d{2}-\d{2}T\d{2}/.test(local) ? `${local.slice(0, 13)}:00` : null;
}

// Resolves the weather record for one run: intervals.icu's own per-point weather
// (temp + humidity together) when a freshly-fetched map endpoint has it, otherwise an
// Open-Meteo fallback keyed off the run's GPS start point + date/hour, otherwise null
// (never guessed - e.g. treadmill runs have no GPS at all).
//
// Takes `mapData` (a fresh /map response) and/or `cachedLatLng` (a previously-found
// start point, see readCachedActivityGps) rather than just one or the other: a run
// whose GPS was already found but whose Open-Meteo call previously failed for budget
// reasons (see enrichRunsWithWeather) should skip the now-redundant map fetch and go
// straight to Open-Meteo, so the caller may pass cachedLatLng with no mapData at all.
// Returns latLng alongside weather so the caller can cache the GPS point even when
// the weather lookup itself didn't (yet) succeed.
export async function resolveRunWeather(env, activity, { mapData = null, cachedLatLng = null } = {}) {
  const startLatLng = cachedLatLng ?? extractStartLatLng(mapData);
  if (!startLatLng) return { weather: null, latLng: null };

  const native = mapData ? averageNativeWeather(mapData) : null;
  if (native) return { weather: { ...native, source: "intervals.icu" }, latLng: startLatLng };

  const dateIso = String(activity?.start_date_local || activity?.start_date || "").slice(0, 10);
  if (!dateIso) return { weather: null, latLng: startLatLng };
  const dayData = await getOpenMeteoDay(env, startLatLng.lat, startLatLng.lng, dateIso).catch(() => null);
  if (!dayData) return { weather: null, latLng: startLatLng };
  const picked = pickOpenMeteoValues(dayData, activityStartHourIso(activity));
  return { weather: picked ? { ...picked, source: "open-meteo" } : null, latLng: startLatLng };
}
