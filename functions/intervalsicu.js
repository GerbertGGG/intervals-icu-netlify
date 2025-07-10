export async function handler(event, context) {
  const API_KEY = process.env.INTERVALS_API_KEY;
  if (!API_KEY) {
    return { statusCode: 500, body: JSON.stringify({ error: 'API key missing' }) };
  }

  // Beispiel: Profil abrufen
  const urlProfile = 'https://intervals.icu/api/v1/athletes/0';

  // Beispiel: Workouts abrufen
  const urlWorkouts = 'https://intervals.icu/api/v1/athletes/0/workouts';

  const basicAuth = Buffer.from(`API_KEY:${API_KEY}`).toString('base64');

  try {
    // Profil
    const profileRes = await fetch(urlProfile, {
      headers: { Authorization: `Basic ${basicAuth}` }
    });
    if (!profileRes.ok) {
      return { statusCode: profileRes.status, body: JSON.stringify({ error: await profileRes.text() }) };
    }
    const profile = await profileRes.json();

    // Workouts
    const workoutsRes = await fetch(urlWorkouts, {
      headers: { Authorization: `Basic ${basicAuth}` }
    });
    if (!workoutsRes.ok) {
      return { statusCode: workoutsRes.status, body: JSON.stringify({ error: await workoutsRes.text() }) };
    }
    const workouts = await workoutsRes.json();

    return {
      statusCode: 200,
      body: JSON.stringify({ profile, workouts })
    };

  } catch (err) {
    return { statusCode: 500, body: JSON.stringify({ error: err.message }) };
  }
}
