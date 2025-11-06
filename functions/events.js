const fetch = require("node-fetch");

exports.handler = async function(event, context) {
  // CORS Preflight-Handler für OPTIONS-Anfragen
  if (event.httpMethod === "OPTIONS") {
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS"
      },
      body: ""
    };
  }

  // API Key nur innerhalb der Funktion deklarieren!
  const API_KEY = process.env.INTERVALS_API_KEY;
  console.log("DEBUG - API_KEY gesetzt:", !!API_KEY);

  if (!API_KEY) {
    console.error("DEBUG - Kein API_KEY vorhanden!");
    return {
      statusCode: 500,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ error: "API key missing in environment variables" }),
    };
  }

  // Basic Auth nur innerhalb der Funktion deklarieren!
  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)" // <- User-Agent hinzugefügt!
  };

  const url = "https://intervals.icu/api/v1/athlete/i105857/events?limit=300";
  console.log("DEBUG - Verwende URL:", url);
  console.log("DEBUG - Authorization-Header (gekürzt):", headers.Authorization.slice(0, 14) + "...");
  console.log("DEBUG - User-Agent:", headers["User-Agent"]);

  try {
    const activitiesRes = await fetch(url, { method: "GET", headers });

    if (!activitiesRes || typeof activitiesRes.ok === "undefined") {
      console.error("DEBUG - Keine Antwort von Intervals.icu API");
      return {
        statusCode: 500,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type,Authorization",
          "Access-Control-Allow-Methods": "GET,OPTIONS",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ error: "Keine Antwort von Intervals.icu API" }),
      };
    }

    if (!activitiesRes.ok) {
      const error = await activitiesRes.text();
      console.error("DEBUG - Fehler von Intervals.icu:", error);
      return {
        statusCode: 500,
        headers: {
          "Access-Control-Allow-Origin": "*",
          "Access-Control-Allow-Headers": "Content-Type,Authorization",
          "Access-Control-Allow-Methods": "GET,OPTIONS",
          "Content-Type": "application/json"
        },
        body: JSON.stringify({ error })
      };
    }

    const activities = await activitiesRes.json();
    console.log("DEBUG - Events erfolgreich geladen! Anzahl:", Array.isArray(activities) ? activities.length : "unbekannt");
    console.log("DEBUG - Response Preview:", JSON.stringify(activities).slice(0,200));

    // Gibt ein reines Array zurück!
    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json"
      },
      body: JSON.stringify(activities)
    };
  } catch (error) {
    console.error("DEBUG - Exception:", error.message);
    return {
      statusCode: 500,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ error: error.message })
    };
  }
};
