const fetch = require("node-fetch");

exports.getEvents = async function (event, context) {
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
    const eventsRes = await fetch(
      "https://intervals.icu/api/v1/athlete/i105857/events?limit=30",
      { method: "GET", headers }
    );

    if (!eventsRes.ok) {
      const error = await eventsRes.text();
      return { statusCode: 500, body: JSON.stringify({ error }) };
    }

    const events = await eventsRes.json();
    return { statusCode: 200, body: JSON.stringify({ events }) };
  } catch (error) {
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};
