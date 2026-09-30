// Rennphase und phasenabhängige Ziele an EINER Stelle. Das Widget (widget.js) liest nur das Ergebnis.
// Renndatum, Distanz und Zielzeit kommen aus dem Ziel des Dashboards (Intervals-A-Rennen oder Config).

// Schwellen in Tagen bis zum Rennen (Renntag = 0, danach negativ).
export const PHASE_THRESHOLDS = {
  taperFromDays: 7,   // taper: 7 bis 3 Tage vor dem Rennen
  carbloadFromDays: 2, // carbload: 2 Tage vor dem Rennen bis einschließlich Renntag
  recoveryDays: 3,    // recovery: 3 Tage nach dem Rennen
};

// Tagesziele je Phase. TODO: Platzhalter, vom Nutzer noch nicht festgelegt - bitte durch echte Werte ersetzen.
export const NUTRITION_TARGETS = {
  normal: { proteinG: 130, kcal: 2400 },             // TODO: Protein- und Kalorienziel festlegen
  taper: { proteinG: 130, kcal: 2400 },              // TODO: wie normal, ggf. anpassen
  carbload: { carbsG: 600, proteinG: 100, kcal: 3200 }, // TODO: Carb-Loading-Ziel (g Kohlenhydrate/Tag) festlegen
  recovery: { proteinG: 140, kcal: 2400, carbsG: 350 },  // TODO: Protein-, Kalorien- und Kohlenhydratziel festlegen
};

// Reduzierte Trainingswoche (Badge statt Wochen-TSS-Ziel, keine Kraft-Zeile)
export const REDUCED_LOAD_PHASES = ["taper", "carbload"];

export const RACE_DISTANCES = {
  "5k": { km: 5, label: "5 km" },
  "10k": { km: 10, label: "10 km" },
  hm: { km: 21.0975, label: "Halbmarathon" },
  m: { km: 42.195, label: "Marathon" },
};

// Distanz aus Kilometern (Config) oder Kürzel (Intervals) ableiten; Fallback: nur die Kilometer.
export function raceDistance({ distance, distanceKm } = {}) {
  const byKey = RACE_DISTANCES[distance];
  if (byKey) return { key: distance, ...byKey };
  const km = Number(distanceKm);
  if (Number.isFinite(km) && km > 0) {
    const hit = Object.entries(RACE_DISTANCES).find(([, v]) => Math.abs(v.km - km) < 0.5);
    return hit ? { key: hit[0], ...hit[1] } : { key: null, km, label: `${km} km` };
  }
  return null;
}

const dayDiff = (fromIso, toIso) => Math.round((Date.parse(toIso + "T00:00:00Z") - Date.parse(fromIso + "T00:00:00Z")) / 86400000);

// daysToGo: Tage bis zum Rennen (0 = Renntag, negativ = danach)
export function phaseForDaysToGo(daysToGo, t = PHASE_THRESHOLDS) {
  if (daysToGo == null || !Number.isFinite(daysToGo)) return "normal";
  if (daysToGo > t.taperFromDays) return "normal";
  if (daysToGo > t.carbloadFromDays) return "taper";
  if (daysToGo >= 0) return "carbload";
  if (daysToGo >= -t.recoveryDays) return "recovery";
  return "normal";
}

export function racePhase(todayIso, raceIso) {
  const daysToGo = dayDiff(todayIso, raceIso);
  const phase = phaseForDaysToGo(daysToGo);
  return { phase, daysToGo, reducedLoad: REDUCED_LOAD_PHASES.includes(phase), targets: NUTRITION_TARGETS[phase] };
}
