import { fetchIntervalsSportSettings, loadCachedSportSettings, saveCachedSportSettings } from "./intervals-client.js";

function findSportSettings(list, typeNames) {
  if (!Array.isArray(list)) return null;
  return list.find((s) => Array.isArray(s?.types) && typeNames.some((t) => s.types.includes(t))) ?? null;
}

// Field names here are best-effort, same caveat as hrDrift/perceivedExertion/
// zoneTimes/intervalSplits elsewhere in this codebase: the live intervals.icu
// OpenAPI spec wasn't reachable at implementation time, so a handful of plausible
// field names are tried per value and anything unresolved comes back null rather
// than throwing.
function normalizeHrZones(settings) {
  const raw = settings?.hr_zones ?? settings?.HRZones ?? settings?.zones ?? null;
  if (!Array.isArray(raw) || !raw.length) return null;
  const names = settings?.hr_zone_names ?? settings?.HRZoneNames ?? null;
  const zones = raw
    .map((upperBpm, i) => ({
      zone: i + 1,
      name: (Array.isArray(names) && names[i]) || `Z${i + 1}`,
      upperBpm: Number(upperBpm) || null,
    }))
    .filter((z) => z.upperBpm != null);
  return zones.length ? zones : null;
}

function normalizePowerZones(settings, ftp) {
  const raw = settings?.power_zones ?? settings?.PowerZones ?? null;
  if (!Array.isArray(raw) || !raw.length) return null;
  const names = settings?.power_zone_names ?? settings?.PowerZoneNames ?? null;
  const zones = raw
    .map((pct, i) => {
      const p = Number(pct);
      return {
        zone: i + 1,
        name: (Array.isArray(names) && names[i]) || `Z${i + 1}`,
        upperPctFtp: Number.isFinite(p) ? p : null,
        upperWatts: Number.isFinite(p) && ftp ? Math.round((p / 100) * ftp) : null,
      };
    })
    .filter((z) => z.upperPctFtp != null);
  return zones.length ? zones : null;
}

// Resolves the athlete's configured Run/Ride sport-settings (max HR, threshold HR,
// HR zones, FTP, power zones) from a single cached call (24h TTL, same lifetime as
// resolveMaxHr's dedicated cache in vdot.js - this reads the same upstream endpoint
// more fully instead of adding a second one). Returns null if the settings can't be
// loaded at all; individual fields inside the result are null if intervals.icu has
// no value for them (e.g. athlete never configured HR zones or a Ride FTP).
export async function resolveAthleteProfile(env) {
  let list = await loadCachedSportSettings(env).catch(() => null);
  if (!list) {
    list = await fetchIntervalsSportSettings(env).catch(() => null);
    if (list) saveCachedSportSettings(env, list).catch(() => {});
  }
  if (!list) return null;

  const runSettings = findSportSettings(list, ["Run"]) ?? list[0] ?? null;
  const rideSettings = findSportSettings(list, ["Ride", "VirtualRide"]);
  const ftpRide = Number(rideSettings?.ftp) || null;

  return {
    maxHrRun: Number(runSettings?.max_hr) || null,
    lthrRun: Number(runSettings?.lthr ?? runSettings?.threshold_hr ?? runSettings?.hr_lthr) || null,
    hrZonesRun: normalizeHrZones(runSettings),
    maxHrRide: Number(rideSettings?.max_hr) || null,
    ftpRide,
    powerZonesRide: normalizePowerZones(rideSettings, ftpRide),
  };
}
