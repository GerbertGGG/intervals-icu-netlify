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

  // Debug: Prüfe, ob der API_KEY gesetzt ist
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

  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
    "Content-Type": "application/json",
  };

  const url = "https://intervals.icu/api/v1/athlete/i105857/events?limit=30";
  // Debug: Zeige URL und Teile des Headers (nicht den kompletten Key!)
  console.log("DEBUG - Verwende URL:", url);
  console.log("DEBUG - Authorization-Header (gekürzt):", headers.Authorization.slice(0, 14) + "...");

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

    return {
      statusCode: 200,
      headers: {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "Content-Type,Authorization",
        "Access-Control-Allow-Methods": "GET,OPTIONS",
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ activities })
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
