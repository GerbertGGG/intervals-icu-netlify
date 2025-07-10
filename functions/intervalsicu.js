export async function handler() {
  const API_KEY = process.env.INTERVALS_API_KEY;
  const athleteId = "i105857"; // deine AthleteID

  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing" }),
    };
  }

  const basicAuth = Buffer.from(`${API_KEY}:`).toString("base64");

  try {
    // Workouts holen
    const workoutsRes = await fetch(
      `https://intervals.icu/api/v1/athlete/${athleteId}/workouts`,
      {
        method: "GET",
        headers: {
          Authorization: `Basic ${basicAuth}`,
        },
      }
    );

    if (!workoutsRes.ok) {
      const text = await workoutsRes.text();
      return {
        statusCode: workoutsRes.status,
        body: JSON.stringify({ error: text }),
      };
    }
    const workouts = await workoutsRes.json();

    // Geplante Workouts holen
    const plannedRes = await fetch(
      `https://intervals.icu/api/v1/athlete/${athleteId}/planned_activities`,
      {
        method: "GET",
        headers: {
          Authorization: `Basic ${basicAuth}`,
        },
      }
    );

    if (!plannedRes.ok) {
      const text = await plannedRes.text();
      return {
        statusCode: plannedRes.status,
        body: JSON.stringify({ error: text }),
      };
    }
    const plannedActivities = await plannedRes.json();

    return {
      statusCode: 200,
      body: JSON.stringify({
        workouts,
        plannedActivities,
      }),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
}
