import fetch from 'node-fetch';

export async function handler(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY; // Dein Intervals API Key als Env-Variable
  const athleteId = 'i105857'; // Deine Athlete-ID

  const headers = {
    'Authorization': 'Basic ' + Buffer.from(`${API_KEY}:`).toString('base64'),
    'Content-Type': 'application/json',
  };

  try {
    const res = await fetch(`https://intervals.icu/api/v1/athlete/${athleteId}/workouts`, { headers });

    if (!res.ok) {
      return {
        statusCode: res.status,
        body: JSON.stringify({ error: `Intervals API error: ${res.statusText}` }),
      };
    }

    const workouts = await res.json();

    return {
      statusCode: 200,
      body: JSON.stringify(workouts),
      headers: { 'Content-Type': 'application/json' },
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
}
