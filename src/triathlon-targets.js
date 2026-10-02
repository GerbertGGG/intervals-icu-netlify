// Ziele je Disziplin fuer den Triathlon. Vorschlaege aus den Schwellenwerten (FTP, Schwimmschwelle),
// jede Angabe in der Beschreibung des Intervals.icu-Eintrags ("Schwimmen 40:00", "Rad 215 W", "Rad 3:00:00",
// "Lauf 1:55:00") hat Vorrang. Faustwerte, kein Trainingsplan.

// Anteil der FTP und Aufschlag auf die Schwimmschwelle (CSS) je Format: Je laenger, desto lockerer.
const BIKE_IF = { sprint: 0.85, olympic: 0.8, middle: 0.72, long: 0.68 };
const SWIM_CSS_FACTOR = { sprint: 1.02, olympic: 1.04, middle: 1.06, long: 1.08 };

export function buildTriathlonTargets(tri, thresholds, totalTargetSecs = null) {
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
    bike = { source: "vorschlag", watts: Math.round(ftp * f), wattsLow: Math.round(ftp * (f - 0.03)), wattsHigh: Math.round(ftp * (f + 0.03)), pctFtp: f, timeSecs: null, speedKmh: null };
  }

  const run = tri.runTargetSecs ? { source: "eintrag", timeSecs: tri.runTargetSecs, pacePerKm: tri.runTargetSecs / tri.runKm } : null;
  return { swim, bike, run, totalTargetSecs };
}
