export async function handler(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;

  if (!API_KEY) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: "API key not set in environment variables." })
    };
  }

  const url = 'https://intervals.icu/api/v1/athletes/0';
  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString('base64');

  try {
    const response = await fetch(url, {
      headers: {
        Authorization: `Basic ${basicAuth}`
      }
    });

    const data = await response.json();

    return {
      statusCode: response.status,
      body: JSON.stringify(data)
    };

  } catch (error) {
    return {
      statusCode: 500,
      body: JSON.stringify({ error: error.message })
    };
  }
}
