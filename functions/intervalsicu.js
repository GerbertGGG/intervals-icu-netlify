const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" }),
    };
  }

  const athleteId = "i105857"; // fest eingetragene Athlete-ID
  const basicAuth = Buffer.from(`${API_KEY}:`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
    "Content-Type": "application/json",
  };

  try {
    const workoutsRes = await fetch(`https://intervals.icu/api/v1/athlete/i105857/workouts`, {
      method: "GET",
      headers,
    });

    if (!workoutsRes.ok) {
      const errorText = await workoutsRes.text();
      return {
        statusCode: workoutsRes.status,
        body: JSON.stringify({ error: errorText }),
      };
    }

    const workouts = await workoutsRes.json();
    return {
      statusCode: 200,
      body: JSON.stringify({
        athleteId,
        workouts,
      }),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};
