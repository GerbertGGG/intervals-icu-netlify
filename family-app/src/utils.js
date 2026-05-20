export function genId() {
  return Math.random().toString(36).slice(2) + Date.now().toString(36)
}

export function heute() {
  const d = new Date()
  return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`
}

export function formatDatum(dateStr) {
  if (!dateStr) return ''
  const date = new Date(dateStr + 'T00:00:00')
  return date.toLocaleDateString('de-DE', { weekday: 'long', day: 'numeric', month: 'long', year: 'numeric' })
}

export function formatKurzdatum(dateStr) {
  if (!dateStr) return ''
  const date = new Date(dateStr + 'T00:00:00')
  return date.toLocaleDateString('de-DE', { weekday: 'short', day: 'numeric', month: 'short' })
}

export function formatMonat(dateStr) {
  if (!dateStr) return ''
  const date = new Date(dateStr + 'T00:00:00')
  return date.toLocaleDateString('de-DE', { month: 'long', year: 'numeric' })
}

export function isToday(dateStr) {
  if (!dateStr) return false
  return dateStr === heute()
}

export function isUpcoming(dateStr, days = 7) {
  if (!dateStr) return false
  const todayDate = new Date(heute() + 'T00:00:00')
  const end = new Date(todayDate)
  end.setDate(end.getDate() + days)
  const date = new Date(dateStr + 'T00:00:00')
  return date > todayDate && date <= end
}

export function isFuture(dateStr) {
  if (!dateStr) return true
  return dateStr >= heute()
}

export function sortByDate(events) {
  return [...events].sort((a, b) => {
    if (a.datum < b.datum) return -1
    if (a.datum > b.datum) return 1
    if ((a.uhrzeit || '') < (b.uhrzeit || '')) return -1
    if ((a.uhrzeit || '') > (b.uhrzeit || '')) return 1
    return 0
  })
}

export function groupByMonth(events) {
  const groups = {}
  for (const e of events) {
    const key = e.datum ? e.datum.slice(0, 7) : 'unbekannt'
    if (!groups[key]) groups[key] = []
    groups[key].push(e)
  }
  return groups
}
