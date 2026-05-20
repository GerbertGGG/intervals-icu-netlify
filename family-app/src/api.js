export async function analysiertBrief(inhalt, apiKey) {
  if (!apiKey) {
    throw new Error('Kein API-Schlüssel. Bitte in den Einstellungen eintragen.')
  }

  const prompt = `Analysiere diesen deutschen Elternbrief oder diese Einladung.

Extrahiere:
1. Eine kurze Zusammenfassung (2-3 prägnante Sätze)
2. Alle konkreten Termine mit Datum, Uhrzeit und Beschreibung

Antworte NUR mit gültigem JSON in exakt diesem Format:
{
  "zusammenfassung": "Kurze Zusammenfassung des Inhalts",
  "termine": [
    {
      "titel": "Name des Termins",
      "datum": "YYYY-MM-DD",
      "uhrzeit": "HH:MM",
      "beschreibung": "Details zum Termin"
    }
  ]
}

Regeln:
- Wenn kein Datum erkennbar: datum = "", uhrzeit = ""
- Wenn keine Uhrzeit: uhrzeit = ""
- Aktuelles Jahr: ${new Date().getFullYear()}
- Nur echte Termine, keine generellen Aussagen

Brief/Einladung:
${inhalt}`

  const response = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    headers: {
      'x-api-key': apiKey,
      'anthropic-version': '2023-06-01',
      'content-type': 'application/json',
      'anthropic-dangerous-direct-browser-access': 'true',
    },
    body: JSON.stringify({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: 2048,
      messages: [{ role: 'user', content: prompt }],
    }),
  })

  if (!response.ok) {
    const err = await response.json().catch(() => ({}))
    throw new Error(err.error?.message || `API-Fehler ${response.status}`)
  }

  const data = await response.json()
  const text = data.content?.[0]?.text || ''
  const match = text.match(/\{[\s\S]*\}/)
  if (!match) throw new Error('Keine gültige KI-Antwort erhalten')
  return JSON.parse(match[0])
}
