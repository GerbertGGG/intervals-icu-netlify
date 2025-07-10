const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" })
    };
  }

  const baseUrl = "https://intervals.icu/api/v1/athlete/i105857";
  const authHeader = Buffer.from(`${API_KEY}:`).toString("base64");
  const headers = {
    Authorization: `Basic ${authHeader}`,
    "Content-Type": "application/json"
  };

  const today = new Date().toISOString().split("T")[0];
  const oldest = new Date();
  oldest.setDate(oldest.getDate() - 30);
  const oldestStr = oldest.toISOString().split("T")[0];

  try {
    const [activitiesRes, plannedRes] = await Promise.all([
      fetch(`${baseUrl}/activities?oldest=${oldestStr}&limit=50`, { headers }),
      fetch(`${baseUrl}/planned_activities`, { headers })
    ]);

    const result = { activities: [], planned: [] };

    if (activitiesRes.ok) {
      const activities = await activitiesRes.json();
      result.activities = activities.map(act => ({
        date: act.date,
        type: act.sport,
        name: act.description || act.name || "Unnamed activity",
        duration: act.moving_time
      }));
    } else {
      result.activitiesError = await activitiesRes.text();
    }

    if (plannedRes.ok) {
      const planned = await plannedRes.json();
      result.planned = planned
        .filter(p => p.planned_date && p.planned_date >= today)
        .map(p => ({
          planned_date: p.planned_date,
          type: p.type,
          name: p.name || "Unnamed workout",
          duration: p.moving_time
        }));
    } else {
      result.plannedError = await plannedRes.text();
    }

    return {
      statusCode: 200,
      body: JSON.stringify(result)
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "Failed to fetch data", details: error.message })
    };
  }
};
