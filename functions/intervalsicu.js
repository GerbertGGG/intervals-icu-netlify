// netlify/functions/getWorkouts.js
import fetch from 'node-fetch';

export async function handler(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY; // Setze das in Netlify!

  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key not found in environment variables" }),
    };
  }

  const athleteId = 'i105857';
  const url = `https://intervals.icu/api/v1/athlete/${athleteId}/workouts`;

  const headers = {
    'Authorization': 'Basic ' + Buffer.from(`${API_KEY}:`).toString('base64'),
    'Content-Type': 'application/json',
  };

  try {
    const response = await fetch(url, { method: 'GET', headers });

    if (!response.ok) {
      return {
        statusCode: response.status,
        body: JSON.stringify({
          error: `Intervals API error: ${response.statusText}`,
          status: response.status
        }),
      };
    }

    const data = await response.json();

    return {
      statusCode: 200,
      body: JSON.stringify(data),
      headers: {
        'Content-Type': 'application/json',
      },
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
}

