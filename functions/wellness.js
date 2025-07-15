const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" }),
    };
  }

  // Datum parsen
  const params = event.queryStringParameters || {};
  let date = params.date;

  function toISO(daysAgo = 0) {
    const d = new Date();
    d.setDate(d.getDate() - daysAgo);
    return d.toISOString().slice(0, 10);
  }

  if (!date) {
    date = toISO(0); // heute
  } else if (/^heute$/i.test(date)) {
    date = toISO(0);
  } else if (/^gestern$/i.test(date)) {
    date = toISO(1);
  } else if (/^vorgestern$/i.test(date)) {
    date = toISO(2);
  }
  // Sonst: bleibt wie übergeben (z.B. ISO-String)

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
