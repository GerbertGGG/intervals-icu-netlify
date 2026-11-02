import { readKvJson, writeKvJson } from "./kv.js";

// Wetter eines Laufs (Teil B, Abschnitt 10). Wiederhergestellt aus dem Stand vor #976 fuer den Export (export.js), dazu
// der Wind aus Open-Meteo. Reihenfolge:
// 1. Intervals-Wetter aus den Kartenpunkten (/activity/{id}/map?weather=true): Mittelwert aller Temperatur- und
//    Feuchtewerte ueber alle Zeitproben; beide Groessen muessen da sein.
// 2. Sonst Open-Meteo-Archiv am GPS-Startpunkt: Stundenwert zur lokalen Startstunde, sonst Tagesmittel.
// Nie die Temperatur der Uhr (Koerperwaerme). Laufband: kein Wetter (entscheidet der Aufrufer).

const OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive";
// v2: mit Wind (wind_speed_10m in m/s); die alten Eintraege "weather:<lat>:<lng>:<datum>" haben keinen Wind
const DAY_KV_PREFIX = "weather:v2:";
const COORD_DECIMALS = 3; // ~111 m: Laeufe von derselben Haustuer teilen sich einen Cache-Eintrag

export const roundCoord = (v) => Math.round(Number(v) * 10 ** COORD_DECIMALS) / 10 ** COORD_DECIMALS;
const round1 = (v) => Math.round(Number(v) * 10) / 10;
const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);

// Mittel ueber alle Zeitproben aller Kartenpunkte; null, wenn Temperatur oder Feuchte fehlt
export function averageNativeWeather(mapData) {
  const temps = [], hums = [];
  for (const point of Array.isArray(mapData?.weather?.points) ? mapData.weather.points : []) {
    for (const t of Array.isArray(point?.times) ? point.times : []) {
      if (t?.temp != null && Number.isFinite(Number(t.temp))) temps.push(Number(t.temp));
      if (t?.humidity != null && Number.isFinite(Number(t.humidity))) hums.push(Number(t.humidity));
    }
  }
  if (!temps.length || !hums.length) return null;
  return { tempC: round1(mean(temps)), humidity: round1(mean(hums)) };
}

export function extractStartLatLng(mapData) {
  const first = Array.isArray(mapData?.latlngs) ? mapData.latlngs.find((p) => Array.isArray(p) && p[0] != null && p[1] != null) : null;
  const lat = Number(first?.[0]), lng = Number(first?.[1]);
  return Number.isFinite(lat) && Number.isFinite(lng) && !(lat === 0 && lng === 0) ? { lat: roundCoord(lat), lng: roundCoord(lng) } : null;
}

// Lokale Startstunde aus start_date_local ("2026-10-30T11:37:55" -> "2026-10-30T11:00"), passend zu timezone=auto
export function activityStartHourIso(activity) {
  const local = String(activity?.start_date_local || "");
  return /^\d{4}-\d{2}-\d{2}T\d{2}/.test(local) ? `${local.slice(0, 13)}:00` : null;
}

// Stundenwert zur Startstunde, sonst Tagesmittel. Wind darf fehlen (dann null), Temperatur und Feuchte nicht.
export function pickOpenMeteoValues(day, hourIso) {
  const at = (arr, i) => (arr?.[i] != null && Number.isFinite(Number(arr[i])) ? Number(arr[i]) : null);
  const idx = hourIso ? (day?.times ?? []).indexOf(hourIso) : -1;
  if (idx >= 0) {
    const t = at(day.temps, idx), h = at(day.humidities, idx);
    if (t != null && h != null) { const w = at(day.winds, idx); return { tempC: round1(t), humidity: round1(h), windMs: w != null ? round1(w) : null }; }
  }
  const all = (arr) => (Array.isArray(arr) ? arr : []).map((_, i) => at(arr, i)).filter((v) => v != null);
  const t = mean(all(day?.temps)), h = mean(all(day?.humidities)), w = mean(all(day?.winds));
  if (t == null || h == null) return null;
  return { tempC: round1(t), humidity: round1(h), windMs: w != null ? round1(w) : null };
}

// Ein Tag Open-Meteo je gerundetem Punkt, dauerhaft in KV (vergangenes Wetter aendert sich nicht). Nur gespeichert, wenn das
// Archiv schon Werte hat (es hinkt einige Tage hinterher; leere Tage werden spaeter erneut gefragt).
// budget: { left } – jede echte Abfrage kostet 1.
export async function openMeteoDay(env, lat, lng, dateIso, budget, fetchImpl = fetch) {
  const key = `${DAY_KV_PREFIX}${roundCoord(lat)}:${roundCoord(lng)}:${dateIso}`;
  const hit = await readKvJson(env, key).catch(() => null);
  if (hit) return hit;
  if (!budget || budget.left <= 0) return undefined;
  budget.left--;
  const url = `${OPEN_METEO_ARCHIVE_URL}?latitude=${roundCoord(lat)}&longitude=${roundCoord(lng)}&start_date=${dateIso}&end_date=${dateIso}` +
    "&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m&wind_speed_unit=ms&timezone=auto";
  const r = await fetchImpl(url).catch(() => null);
  if (!r?.ok) return null;
  const data = await r.json().catch(() => null);
  const h = data?.hourly;
  if (!Array.isArray(h?.time) || !Array.isArray(h?.temperature_2m)) return null;
  const day = { times: h.time, temps: h.temperature_2m, humidities: h.relative_humidity_2m ?? [], winds: h.wind_speed_10m ?? [] };
  if (day.temps.some((v) => v != null)) await writeKvJson(env, key, day).catch(() => {});
  return day;
}
