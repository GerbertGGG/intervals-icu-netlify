const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" }),
    };
  }

  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
    "Content-Type": "application/json",
  };

  try {
    // Füge ein console.log hinzu um die URL zu prüfen
    const url = "https://intervals.icu/api/v1/athlete/i105857/activities?oldest=2025-06-01&limit=10";
    console.log("Fetching URL:", url);

    const activitiesRes = await fetch(url, { method: "GET", headers });

    if (!activitiesRes || typeof activitiesRes.ok === "undefined") {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Keine Antwort von Intervals.icu API" }),
      };
    }

    if (!activitiesRes.ok) {
      const error = await activitiesRes.text();
      return { statusCode: 500, body: JSON.stringify({ error }) };
    }

    const activities = await activitiesRes.json();
    return { statusCode: 200, body: JSON.stringify({ activities }) };
  } catch (error) {
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};
