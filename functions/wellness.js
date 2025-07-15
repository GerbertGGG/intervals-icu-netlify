const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" }),
    };
  }

  // Datum aus Query-Parameter holen
  const params = event.queryStringParameters || {};
  let date = params.date;

  // Wenn kein Datum angegeben, nimm heute (im Format YYYY-MM-DD)
  if (!date) {
    const today = new Date();
    date = today.toISOString().slice(0, 10);
  }

  const url = `https://intervals.icu/api/v1/athlete/i105857/wellness/${date}`;
  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
    "Content-Type": "application/json",
  };

  try {
    const res = await fetch(url, { method: "GET", headers });

    if (!res || typeof res.ok === "undefined") {
      return {
        statusCode: 500,
        body: JSON.stringify({ error: "Keine Antwort von Intervals.icu API" }),
      };
    }

    if (!res.ok) {
      const error = await res.text();
      return { statusCode: 500, body: JSON.stringify({ error }) };
    }

    const wellness = await res.json();
    return { statusCode: 200, body: JSON.stringify(wellness) };
  } catch (error) {
    return { statusCode: 500, body: JSON.stringify({ error: error.message }) };
  }
};
