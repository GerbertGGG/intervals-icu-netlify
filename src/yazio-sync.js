import { putWellnessDay } from "./intervals-client.js";
import { hasYazioCredentials, fetchYazioDailyNutrition, fetchYazioDailyGoalKcal } from "./yazio-client.js";
import { listIsoDaysInclusive } from "./date-utils.js";

// Schreibt die Yazio-Tageswerte als Zusatzfelder in den Intervals-Wellness-Eintrag;
// das Dashboard liest sie von dort (buildWellness in dashboard.js).
// Yazio-Fehler sind best effort und brechen den Lauf nicht ab.
export async function syncYazioRange(env, oldest, newest) {
  if (!hasYazioCredentials(env)) return;
  for (const day of listIsoDaysInclusive(oldest, newest)) {
    const patch = {};
    try {
      const n = await fetchYazioDailyNutrition(env, day);
      if (n) {
        patch.Calories = Math.round(n.energyKcal);
        patch.Protein = Math.round(n.proteinG * 10) / 10;
        patch.Carbs = Math.round(n.carbG * 10) / 10;
        patch.Fat = Math.round(n.fatG * 10) / 10;
      }
    } catch (e) {
      console.warn("yazio nutrition sync failed", { day, error: String(e?.message ?? e) });
    }
    // Yazios eigenes Tagesziel enthaelt schon die Kalorien importierter Workouts.
    try {
      const goalKcal = await fetchYazioDailyGoalKcal(env, day);
      if (goalKcal != null) patch.CalorieGoal = Math.round(goalKcal);
    } catch (e) {
      console.warn("yazio calorie goal sync failed", { day, error: String(e?.message ?? e) });
    }
    if (Object.keys(patch).length > 0) await putWellnessDay(env, day, patch);
  }
}
