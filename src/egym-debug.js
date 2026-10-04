import { json } from "./http-helpers.js";
import { isAuthorized } from "./dashboard.js";
import { isoDateBerlin } from "./date-utils.js";
import { hasEgymCredentials, fetchEgymWorkouts, fetchEgymStrength, fetchEgymBioAge } from "./egym-client.js";

// Geschuetzter Debug-Endpoint fuer die EGYM-Anbindung: zeigt die Rohdaten der drei Endpunkte (ohne Zugangsdaten),
// damit das Kraft-Widget auf echten Feldnamen aufgebaut werden kann. Standard: Zaehlung plus je wenige Beispiele,
// mit ?full=1 die komplette Antwort. ?days=N (Standard 28, max 180) bestimmt den Zeitraum.
const addDays = (iso, n) => new Date(Date.parse(iso + "T00:00:00Z") + n * 86400000).toISOString().slice(0, 10);

const settle = async (fn) => {
  try {
    return { ok: true, data: await fn() };
  } catch (e) {
    return { ok: false, error: String(e?.message ?? e) };
  }
};

// Listen stecken je nach Endpunkt direkt oder unter einem Schluessel (z. B. strengthMeasurements); beides abdecken.
function sample(data, full) {
  if (full) return data;
  if (Array.isArray(data)) return { count: data.length, sample: data.slice(0, 2) };
  if (data && typeof data === "object") {
    const out = { keys: Object.keys(data) };
    for (const [k, v] of Object.entries(data)) out[k] = Array.isArray(v) ? { count: v.length, sample: v.slice(0, 2) } : v;
    return out;
  }
  return data;
}

export async function handleEgymDebugRequest(req, env) {
  const headers = { "cache-control": "no-store" };
  if (!env?.DASHBOARD_TOKEN) return json({ ok: false, error: "DASHBOARD_TOKEN nicht gesetzt" }, 503, headers);
  if (!isAuthorized(req, env)) return json({ ok: false, error: "Nicht autorisiert" }, 401, headers);
  if (!hasEgymCredentials(env)) return json({ ok: false, error: "EGYM_BRAND, EGYM_USERNAME und EGYM_PASSWORD nicht gesetzt" }, 503, headers);
  const params = new URL(req.url).searchParams;
  const full = params.get("full") === "1";
  const days = Math.min(180, Math.max(1, Math.floor(Number(params.get("days"))) || 28));
  const to = isoDateBerlin();
  const from = addDays(to, -days);
  const [workouts, strength, bioAge] = await Promise.all([
    settle(() => fetchEgymWorkouts(env, from, to)),
    settle(() => fetchEgymStrength(env, from, to)),
    settle(() => fetchEgymBioAge(env)),
  ]);
  const view = (r) => (r.ok ? { ok: true, data: sample(r.data, full) } : r);
  return json({ ok: true, from, to, workouts: view(workouts), strength: view(strength), bioAge: view(bioAge) }, 200, headers);
}
