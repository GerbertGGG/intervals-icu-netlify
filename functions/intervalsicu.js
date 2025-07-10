export async function handler(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  const athleteId = "i105857"; // <== deine echte Athleten-ID hier

  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key not set in environment variables." })
    };
  }

  const url = `https://intervals.icu/api/v1/athletes/${athleteId}/workouts`;
  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString('base64');

  try {
    const response = await fetch(url, {
      headers: {
        Authorization: `Basic ${basicAuth}`
      }
    });

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({ error: `Intervals API error: ${response.statusText}` })
      };
    }

    const data = await response.json();

    return {
      statusCode: 200,
      body: JSON.stringify(data)
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
}
