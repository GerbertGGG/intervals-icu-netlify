// Jack-Daniels-VDOT: Rechnungen aus Rennzeit, Pace-Bereiche und Rennzeit-Prognosen.
// Jack Daniels: v = m/min, t = Minuten
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

// Trainingsbereiche wie in den Runalyze-Lauftabellen: Anteil der Geschwindigkeit bei vVO2max
// (Geschwindigkeit, bei der die VO2 dem VDOT entspricht, grob die 11-Minuten-Pace). Je Bereich von-bis in Prozent;
// schneller = hoeherer Prozentwert. Zwischen den Bereichen liegen bewusst Luecken, wie in der Tabelle.
const PACE_ZONES = [
  { key: "easy", label: "Easy (E)", from: 59, to: 74 },
  { key: "marathon", label: "Marathon (M)", from: 75, to: 84 },
  { key: "threshold", label: "Threshold (T)", from: 88, to: 92 },
  { key: "interval", label: "Interval (I)", from: 95, to: 100 },
  { key: "repetition", label: "Repetition (R)", from: 105, to: 110 },
];

const fmtSecPerKm = (sec) => (sec != null ? `${Math.floor(sec / 60)}:${String(sec % 60).padStart(2, "0")}/km` : null);

// Returns [{ key, label, pct: [von, bis], pace, secPerKm, fastSecPerKm, slowSecPerKm, fastPace, slowPace }] je Bereich
// fuer das VDOT, oder null. pace/secPerKm = Mitte des Bereichs (fuer Workout-Schritte), fast/slow = Grenzen.
export function paceTargetsFromVdot(vdot) {
  const v = Number(vdot);
  if (!Number.isFinite(v) || v <= 0) return null;
  const vel = velocityFromVo2(v);
  if (!vel) return null;
  const base = 60000 / vel; // s/km bei 100 % vVO2max
  return PACE_ZONES.map((zone) => {
    const secPerKm = Math.round(base / ((zone.from + zone.to) / 200));
    const fastSecPerKm = Math.round(base / (zone.to / 100));
    const slowSecPerKm = Math.round(base / (zone.from / 100));
    return { key: zone.key, label: zone.label, pct: [zone.from, zone.to], pace: fmtSecPerKm(secPerKm), secPerKm, fastSecPerKm, slowSecPerKm, fastPace: fmtSecPerKm(fastSecPerKm), slowPace: fmtSecPerKm(slowSecPerKm) };
  });
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
