// netlify/functions/getWorkouts.js
import axios from 'axios';

export async function handler(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  const athleteId = 'i105857'; // oder deine tatsächliche ID

  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key not found in environment variables" }),
    };
  }

  const url = `https://intervals.icu/api/v1/athlete/${athleteId}/workouts`;

  try {
    const response = await axios.get(url, {
      headers: {
        'Authorization': 'Basic ' + Buffer.from(`${API_KEY}:`).toString('base64'),
        'Content-Type': 'application/json',
      }
    });

    return {
      statusCode: 200,
      body: JSON.stringify(response.data),
      headers: {
        'Content-Type': 'application/json',
      },
    };
  } catch (error) {
    return {
      statusCode: error.response?.status || 500,
      body: JSON.stringify({
        error: `Intervals API error: ${error.response?.statusText || error.message}`,
        details: error.response?.data || null
      }),
    };
  }
}

