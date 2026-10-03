// Jack Daniels VDOT-Modell: Rennzeit -> VDOT, VDOT -> Trainingspaces und Rennzeit-Prognosen.
// Nur die Teile, die das Dashboard braucht (dashboard.js, runalyze-history.js).

// ─── Jack Daniels VDOT formula ────────────────────────────────────────────────
// v = m/min, t = minutes
// VO2 = -4.60 + 0.182258·v + 0.000104·v²
// %VO2max = 0.8 + 0.1894393·e^(-0.012778·t) + 0.2989558·e^(-0.1932605·t)
// VDOT = VO2 / %VO2max
export function computeVdotFromRaceTime(distanceMeters, timeSecs) {
  const dist = Number(distanceMeters);
  const secs = Number(timeSecs);
  if (!Number.isFinite(dist) || dist < 400) return null;
  if (!Number.isFinite(secs) || secs < 60) return null;

  const v = (dist / secs) * 60;
  const t = secs / 60;

  const vo2 = -4.6 + 0.182258 * v + 0.000104 * v * v;
  const pctVo2max = 0.8 + 0.1894393 * Math.exp(-0.012778 * t) + 0.2989558 * Math.exp(-0.1932605 * t);

  if (pctVo2max <= 0) return null;
  const vdot = vo2 / pctVo2max;
  if (!Number.isFinite(vdot) || vdot < 20 || vdot > 90) return null;
  return Math.round(vdot * 10) / 10;
}

// Inverts the VDOT VO2 formula to get velocity (m/min) for a given VO2 (ml/kg/min).
function velocityFromVo2(vo2) {
  const a = 0.000104;
  const b = 0.182258;
  const c = -(4.6 + vo2);
  const disc = b * b - 4 * a * c;
  if (disc < 0) return null;
  const v = (-b + Math.sqrt(disc)) / (2 * a);
  return v > 0 ? v : null;
}

function formatPacePerKm(velocityMPerMin) {
  if (!Number.isFinite(velocityMPerMin) || velocityMPerMin <= 0) return null;
  const secPerKm = Math.round(60000 / velocityMPerMin);
  const min = Math.floor(secPerKm / 60);
  const sec = secPerKm % 60;
  return `${min}:${String(sec).padStart(2, "0")}/km`;
}

// Jack Daniels training pace zones, derived as the velocity at a fixed %VDOT
// (VDOT approximates VO2max, so vo2_target = pct * vdot).
const PACE_ZONES = [
  { key: "easy", label: "Easy (E)", pct: 0.7 },
  { key: "marathon", label: "Marathon (M)", pct: 0.84 },
  { key: "threshold", label: "Threshold (T)", pct: 0.88 },
  { key: "interval", label: "Interval (I)", pct: 0.975 },
  { key: "repetition", label: "Repetition (R)", pct: 1.05 },
];

// Returns [{ key, label, pace }] pace targets per km for the given VDOT, or null.
export function paceTargetsFromVdot(vdot) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  return PACE_ZONES.map((zone) => ({
    key: zone.key,
    label: zone.label,
    pace: formatPacePerKm(velocityFromVo2(zone.pct * v)),
  }));
}

const RACE_DISTANCES = [
  { key: "5k", meters: 5000, label: "5 km" },
  { key: "10k", meters: 10000, label: "10 km" },
  { key: "hm", meters: 21097, label: "Halbmarathon" },
  { key: "m", meters: 42195, label: "Marathon" },
];

function secondsToRaceTimeString(totalSeconds) {
  if (!Number.isFinite(totalSeconds)) return null;
  const s = Math.round(totalSeconds);
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = s % 60;
  return h > 0 ? `${h}:${String(m).padStart(2, "0")}:${String(sec).padStart(2, "0")}` : `${m}:${String(sec).padStart(2, "0")}`;
}

// %VO2max depends on race duration, so predicting a race time from VDOT needs a
// fixed-point iteration: guess t -> get %VO2max(t) -> get required velocity -> get a
// better t = distance/velocity, repeat until it converges (a handful of iterations).
function predictRaceTimeSeconds(vdot, distanceMeters) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  let t = (distanceMeters / 1000) * 4; // initial guess: ~4 min/km
  for (let i = 0; i < 12; i++) {
    const pctVo2max = 0.8 + 0.1894393 * Math.exp(-0.012778 * t) + 0.2989558 * Math.exp(-0.1932605 * t);
    const vo2 = v * pctVo2max;
    const velocity = velocityFromVo2(vo2);
    if (!velocity) return null;
    t = distanceMeters / velocity;
  }
  return Number.isFinite(t) ? Math.round(t * 60) : null;
}

// Returns [{ key, label, meters, seconds, time }] predicted race times for the
// standard distances (5k/10k/HM/M) at the given VDOT, or null.
export function predictRaceTimesFromVdot(vdot) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  return RACE_DISTANCES.map((d) => {
    const secs = predictRaceTimeSeconds(v, d.meters);
    return { key: d.key, label: d.label, meters: d.meters, seconds: secs, time: secondsToRaceTimeString(secs) };
  });
}
