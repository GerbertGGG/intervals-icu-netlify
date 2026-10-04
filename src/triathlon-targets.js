import { predictRaceTimesFromVdot } from "./vdot.js";

// Ziele je Disziplin fuer den Triathlon. Vorschlaege aus den Schwellenwerten (FTP, Schwimmschwelle),
// jede Angabe in der Beschreibung des Intervals.icu-Eintrags ("Schwimmen 40:00", "Rad 215 W", "Rad 3:00:00",
// "Lauf 1:55:00") hat Vorrang. Faustwerte, kein Trainingsplan.

// Anteil der FTP und Aufschlag auf die Schwimmschwelle (CSS) je Format: Je laenger, desto lockerer.
const BIKE_IF = { sprint: 0.85, olympic: 0.8, middle: 0.72, long: 0.68 };
const SWIM_CSS_FACTOR = { sprint: 1.02, olympic: 1.04, middle: 1.06, long: 1.08 };

// Radzeit aus Leistung: Leistungsbilanz Luftwiderstand + Rollwiderstand, flache Strecke, Tri-Rad. Modellwert.
const RUN_OFF_BIKE_FACTOR = 1.06; // Laufen nach dem Rad: Halbmarathon-Zeit aus VDOT plus 6 %
function bikeSpeedMs(watts, weightKg) {
  const mass = (weightKg ?? 75) + 9, drive = watts * 0.97;
  let lo = 1, hi = 25;
  for (let i = 0; i < 40; i++) {
    const v = (lo + hi) / 2;
    const need = 0.5 * 1.2 * 0.32 * v ** 3 + 0.004 * mass * 9.81 * v;
    if (need > drive) hi = v; else lo = v;
  }
  return (lo + hi) / 2;
}

export function buildTriathlonTargets(tri, thresholds, totalTargetSecs = null, extras = {}) {
  if (!tri) return null;
  const ftp = thresholds?.bike?.ftp ?? null, css = thresholds?.swim?.thresholdPaceSecPer100m ?? null;

  let swim = null;
  if (tri.swimTargetSecs) {
    swim = { source: "eintrag", timeSecs: tri.swimTargetSecs, pacePer100m: tri.swimTargetSecs / (tri.swimKm * 10) };
  } else if (css) {
    const pace = css * (SWIM_CSS_FACTOR[tri.format] ?? 1.04);
    swim = { source: "vorschlag", timeSecs: Math.round(pace * tri.swimKm * 10), pacePer100m: pace };
  }

  let bike = null;
  if (tri.bikeTargetWatts || tri.bikeTargetSecs) {
    bike = {
      source: "eintrag",
      watts: tri.bikeTargetWatts ?? null,
      pctFtp: tri.bikeTargetWatts && ftp ? tri.bikeTargetWatts / ftp : null,
      timeSecs: tri.bikeTargetSecs ?? null,
      speedKmh: tri.bikeTargetSecs ? tri.bikeKm / (tri.bikeTargetSecs / 3600) : null,
    };
  } else if (ftp) {
    const f = BIKE_IF[tri.format] ?? 0.8;
    const watts = Math.round(ftp * f), v = bikeSpeedMs(watts, extras.weightKg);
    bike = { source: "vorschlag", watts, wattsLow: Math.round(ftp * (f - 0.03)), wattsHigh: Math.round(ftp * (f + 0.03)), pctFtp: f, timeSecs: Math.round(tri.bikeKm * 1000 / v), speedKmh: v * 3.6 };
  }

  let run = null;
  if (tri.runTargetSecs) {
    run = { source: "eintrag", timeSecs: tri.runTargetSecs, pacePerKm: tri.runTargetSecs / tri.runKm };
  } else if (extras.vdot) {
    const hm = predictRaceTimesFromVdot(extras.vdot)?.find((x) => x.key === "hm");
    if (hm?.seconds) {
      const secs = Math.round(hm.seconds * (tri.runKm / 21.0975) ** 1.06 * RUN_OFF_BIKE_FACTOR);
      run = { source: "vorschlag", timeSecs: secs, pacePerKm: secs / tri.runKm };
    }
  }
  return { swim, bike, run, totalTargetSecs };
}
