import { readKvJson, writeKvJson, hasKv } from "./kv.js";
import { fetchIntervalsActivityStreams } from "./intervals-client.js";

// Profil einer Einheit fuers Heute-Band: Leistung (Rad) bzw. Pace (Laufen) ueber die Zeit, Puls nur wenn die
// Pulsregel ihn erlaubt. Die Zeitreihen werden zu hoechstens MAX_POINTS Abschnitten gemittelt; das Ergebnis
// wird je Aktivitaet in KV gespeichert (die Zeitreihen aendern sich nicht mehr).

const MAX_POINTS = 120;
const CACHE_PREFIX = "icu:profile:v1:";
const SLOWEST_PACE = 15 * 60; // langsamer als 15:00/km gilt als Gehen/Stehen und bleibt eine Luecke

const mean = (xs) => (xs.length ? xs.reduce((a, b) => a + b, 0) / xs.length : null);

// streams: Liste von { type, data } (Intervals) oder Objekt je Typ. metric: "watts" | "pace".
// Rueckgabe: { metric, hr, durationMin, points: [[minute, wert, puls]] } oder null (zu wenig Daten).
export function buildSessionProfile(streams, metric, withHr) {
  const get = (t) => (Array.isArray(streams) ? streams.find((s) => s?.type === t)?.data : streams?.[t]) ?? null;
  const out = get(metric === "watts" ? "watts" : "velocity_smooth"), hr = withHr ? get("heartrate") : null, time = get("time");
  if (!Array.isArray(out) || out.length < 60) return null;
  const tOf = (i) => (Array.isArray(time) && Number.isFinite(time[i]) ? time[i] : i);
  const total = tOf(out.length - 1);
  if (!(total > 0)) return null;
  const step = Math.max(10, Math.ceil((total + 1) / MAX_POINTS));
  const buckets = [];
  for (let i = 0; i < out.length; i++) {
    const b = Math.floor(tOf(i) / step);
    const bucket = (buckets[b] ??= { v: [], hr: [] });
    const v = Number(out[i]);
    if (metric === "watts" ? Number.isFinite(v) && v >= 0 : v >= 0.5) bucket.v.push(v);
    const h = Array.isArray(hr) ? Number(hr[i]) : NaN;
    if (h > 40) bucket.hr.push(h);
  }
  const points = [];
  buckets.forEach((x, b) => {
    if (!x) return;
    let v = mean(x.v);
    if (v != null && metric === "pace") v = 1000 / v <= SLOWEST_PACE ? Math.round(1000 / v) : null;
    else if (v != null) v = Math.round(v);
    const h = mean(x.hr);
    points.push([Math.round((b * step) / 6) / 10, v, h != null ? Math.round(h) : null]);
  });
  if (points.filter((p) => p[1] != null).length < 3) return null;
  return { metric, hr: points.some((p) => p[2] != null), durationMin: Math.round(total / 60), points };
}

// Laedt das Profil der Aktivitaet (KV-Cache). Nur Laufen und Rad; Fehler werden geschluckt (das Band zeigt dann nur die Zahlen).
export async function loadSessionProfile(env, a, sport, withHr) {
  const metric = sport === "bike" ? "watts" : sport === "run" ? "pace" : null;
  if (!metric || a?.id == null) return null;
  const key = `${CACHE_PREFIX}${a.id}:${withHr ? "hr" : "nohr"}`;
  if (hasKv(env)) {
    const hit = await readKvJson(env, key).catch(() => null);
    if (hit) return hit.v ?? null;
  }
  try {
    const types = ["time", metric === "watts" ? "watts" : "velocity_smooth", ...(withHr ? ["heartrate"] : [])];
    const v = buildSessionProfile(await fetchIntervalsActivityStreams(env, a.id, types), metric, withHr);
    if (hasKv(env)) await writeKvJson(env, key, { v }).catch(() => {});
    return v;
  } catch (err) {
    console.warn("session profile", a.id, err);
    // Ohne Zeitreihen (z. B. manuell eingetragen) nicht bei jedem Aufruf neu fragen
    if (/ 404/.test(String(err?.message)) && hasKv(env)) await writeKvJson(env, key, { v: null }).catch(() => {});
    return null;
  }
}
