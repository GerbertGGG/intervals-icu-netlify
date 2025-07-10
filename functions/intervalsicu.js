const fetch = require("node-fetch");

exports.handler = async function (event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key missing in environment variables" })
    };
  }

  const basicAuth = Buffer.from(`${API_KEY}:`).toString("base64");
  const headers = {
    Authorization: `Basic ${basicAuth}`,
    "Content-Type": "application/json"
  };

  const baseUrl = "https://intervals.icu/api/v1/athlete/i105857";

  try {
    const [activitiesRes, plannedRes] = await Promise.all([
      fetch(`${baseUrl}/activities?oldest=2024-05-01&limit=100`, {
        method: "GET",
        headers
      }),
      fetch(`${baseUrl}/planned_activities?limit=50`, {
        method: "GET",
        headers
      })
    ]);

    const [activitiesData, plannedData] = await Promise.all([
      activitiesRes.ok ? activitiesRes.json() : activitiesRes.text(),
      plannedRes.ok ? plannedRes.json() : plannedRes.text()
    ]);

    if (!activitiesRes.ok || !plannedRes.ok) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Failed to fetch data",
          activitiesError: activitiesRes.ok ? null : activitiesData,
          plannedError: plannedRes.ok ? null : plannedData
        })
      };
    }

    return {
      statusCode: 200,
      body: JSON.stringify({
        athlete: "i105857",
        activities: activitiesData,
        planned_workouts: plannedData
      })
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
};
