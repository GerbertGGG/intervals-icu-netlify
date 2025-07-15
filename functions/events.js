const fetch = require("node-fetch");

exports.handler = async function (event, context) {
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
    const [activitiesRes, eventsRes] = await Promise.all([
            fetch("https://intervals.icu/api/v1/athlete/i105857/events?limit=30", {
        method: "GET",
        headers,
      }),
    ]);

    const activitiesError = !activitiesRes.ok ? await activitiesRes.text() : null;
    const eventsError = !eventsRes.ok ? await eventsRes.text() : null;

    if (activitiesError || eventsError) {
      return {
        statusCode: 500,
        body: JSON.stringify({
          error: "Failed to fetch data",
          activitiesError,
          eventsError,
        }),
      };
    }

    const activities = await activitiesRes.json();
    const events = await eventsRes.json();

    return {
      statusCode: 200,
      body: JSON.stringify({
        activities,
        events,
      }),
    };
  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message }),
    };
  }
};
