const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" }),
    };
  }

  const basicAuth = Buffer.from(`${API_KEY}:`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
  };

  try {
    // Step 1: Check if API key is valid
    const authCheck = await fetch("https://intervals.icu/api/v1/athlete", {
      method: "GET",
      headers,
    });

    if (!authCheck.ok) {
      const authError = await authCheck.text();
      return {
        statusCode: authCheck.status,
        body: JSON.stringify({
          error: "Auth check failed",
          details: authError,
        }),
      };
    }

    const athleteData = await authCheck.json();
    const athleteId = athleteData.id;

    // Step 2: Fetch workouts
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
        athlete: {
          id: athleteId,
          name: athleteData.firstname + " " + athleteData.lastname,
        },
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

