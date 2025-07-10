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
    const res = await fetch(
      `https://intervals.icu/api/v1/athlete/${athleteId}/workouts`,
      {
        method: "GET",
        headers: {
          Authorization: `Basic ${basicAuth}`,
        },
      }
    );

    if (!res.ok) {
      const text = await res.text();
      return {
        statusCode: res.status,
        body: JSON.stringify({ error: text }),
      };
    }

    const workouts = await res.json();
    return {
      statusCode: 200,
      body: JSON.stringify(workouts),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
}
