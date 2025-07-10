export async function handler() {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: "API key missing" }) };
  }

  const url = "https://intervals.icu/api/v1/athletes/i105857";
  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString("base64");

  try {
    const res = await fetch(url, {
      headers: {
        Authorization: `Basic ${basicAuth}`,
      },
    });

    if (!res.ok) {
      return { statusCode: res.status, body: JSON.stringify({ error: res.statusText }) };
    }

    const data = await res.json();

    return { statusCode: 200, body: JSON.stringify(data) };
  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
}
