import { isoDate, listIsoDaysInclusive } from "./date-utils.js";
import { activityDay } from "./activity-utils.js";
import { fetchIntervalsActivities, putWellnessDay } from "./intervals-client.js";
import { hasYazioCredentials, fetchYazioDailyNutrition, fetchYazioDailyGoalKcal } from "./yazio-client.js";

const FIELD_CALORIES = "Calories";
const FIELD_PROTEIN = "Protein";
const FIELD_CARBS = "Carbs";
const FIELD_FAT = "Fat";
const FIELD_CALORIE_GOAL = "CalorieGoal";

// Schreibt die Yazio-Ernährungswerte (Kalorien, Makros, Kalorienziel) als Zusatzfelder in den
// Intervals-Wellness-Eintrag; das Dashboard liest sie von dort. Alles Weitere (Block, VDOT) entfällt.
export async function syncRange(env, oldest, newest, write, debug, syncOptions = {}) {
  const { includeYazio = false } = syncOptions;
  const days = listIsoDaysInclusive(oldest, newest);
  const results = [];
  if (!includeYazio || !hasYazioCredentials(env)) return { ok: true, oldest, newest, write, days: results };

  const activities = await fetchIntervalsActivities(env, oldest, newest);

  for (const day of days) {
    const patch = {};
    let yazioDebug;

    // Best-effort: ein Fehler bei Yazio (Login, Rate-Limit) darf den restlichen Sync nicht stoppen.
    try {
      const nutrition = await fetchYazioDailyNutrition(env, day);
      if (nutrition) {
        patch[FIELD_CALORIES] = Math.round(nutrition.energyKcal);
        patch[FIELD_PROTEIN] = Math.round(nutrition.proteinG * 10) / 10;
        patch[FIELD_CARBS] = Math.round(nutrition.carbG * 10) / 10;
        patch[FIELD_FAT] = Math.round(nutrition.fatG * 10) / 10;
        if (debug) yazioDebug = { itemCount: nutrition.itemCount, skippedItems: nutrition.skippedItems, rawShape: nutrition.rawShape };
      }
    } catch (e) {
      console.warn("yazio nutrition sync failed", { day, error: String(e?.message ?? e) });
      if (debug) yazioDebug = { error: String(e?.message ?? e) };
    }

    // Kalorienziel = Yazio-Diätziel plus die an diesem Tag verbrauchten Trainingskalorien.
    // Das Diätziel ist der Boden: an Ruhetagen wird nichts addiert.
    try {
      const goalKcal = await fetchYazioDailyGoalKcal(env, day);
      if (goalKcal != null) {
        const trainingKcal = activities
          .filter((a) => activityDay(a) === day)
          .reduce((sum, a) => sum + (Number(a?.calories) || 0), 0);
        patch[FIELD_CALORIE_GOAL] = Math.round(goalKcal + trainingKcal);
      }
    } catch (e) {
      console.warn("yazio calorie goal sync failed", { day, error: String(e?.message ?? e) });
    }

    if (write && Object.keys(patch).length > 0) await putWellnessDay(env, day, patch);
    results.push({ day, patch, yazioDebug });
  }

  return { ok: true, oldest, newest, write, days: results };
}
