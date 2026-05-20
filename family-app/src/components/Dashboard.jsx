import { useStore } from '../StoreContext.jsx'
import { formatKurzdatum, isToday, isUpcoming, sortByDate, heute } from '../utils.js'

export default function Dashboard({ onNavigate }) {
  const { data } = useStore()
  const { events, careRequests, settings } = data

  const now = heute()
  const future = sortByDate(events.filter(e => !e.datum || e.datum >= now))
  const todayEvents = future.filter(e => isToday(e.datum))
  const weekEvents = future.filter(e => isUpcoming(e.datum, 7))

  const missingPickup = future.filter(e => !e.abholung).length
  const openCare = careRequests.filter(c => c.status === 'offen').length

  const greeting = (() => {
    const h = new Date().getHours()
    if (h < 12) return 'Guten Morgen'
    if (h < 18) return 'Guten Tag'
    return 'Guten Abend'
  })()

  return (
    <div>
      <div className="mb-3">
        <h1>{greeting}, {settings.elternteilA}!</h1>
        <p className="text-muted text-sm mt-1">
          {new Date().toLocaleDateString('de-DE', { weekday: 'long', day: 'numeric', month: 'long' })}
        </p>
      </div>

      {/* Action items */}
      {missingPickup > 0 && (
        <div
          className="alert alert-warning"
          style={{ cursor: 'pointer' }}
          onClick={() => onNavigate('termine')}
        >
          ⚠️ <strong>{missingPickup} {missingPickup === 1 ? 'Termin' : 'Termine'}</strong> ohne Abholzuweisung → klicken zum Zuweisen
        </div>
      )}
      {openCare > 0 && (
        <div
          className="alert alert-info"
          style={{ cursor: 'pointer' }}
          onClick={() => onNavigate('betreuung')}
        >
          ⏳ <strong>{openCare}</strong> offene Betreuungsanfrage{openCare !== 1 ? 'n' : ''} → klicken zum Verwalten
        </div>
      )}

      {/* Stats */}
      <div className="stat-grid">
        <div className="stat-card">
          <div className="stat-value">{future.length}</div>
          <div className="stat-label">Kommende Termine</div>
        </div>
        <div className="stat-card">
          <div className="stat-value">{data.letters.length}</div>
          <div className="stat-label">Analysierte Briefe</div>
        </div>
      </div>

      {/* Today */}
      {todayEvents.length > 0 && (
        <section className="mb-3">
          <div className="section-header">
            <h2>Heute</h2>
          </div>
          {todayEvents.map(e => (
            <MiniCard key={e.id} event={e} onClick={() => onNavigate('termine')} />
          ))}
        </section>
      )}

      {/* This week */}
      {weekEvents.length > 0 && (
        <section className="mb-3">
          <div className="section-header">
            <h2>Diese Woche</h2>
            {weekEvents.length > 3 && (
              <button className="btn btn-secondary btn-sm" onClick={() => onNavigate('termine')}>
                Alle anzeigen
              </button>
            )}
          </div>
          {weekEvents.slice(0, 4).map(e => (
            <MiniCard key={e.id} event={e} onClick={() => onNavigate('termine')} />
          ))}
        </section>
      )}

      {todayEvents.length === 0 && weekEvents.length === 0 && future.length === 0 && (
        <div className="empty">
          <div className="empty-icon">📅</div>
          <p>Keine Termine eingetragen.</p>
          <p className="text-sm mt-2 text-muted">Brief hochladen oder Termin manuell hinzufügen.</p>
        </div>
      )}

      {/* Quick actions */}
      <div className="flex gap-2 mt-4">
        <button className="btn btn-primary" style={{ flex: 1 }} onClick={() => onNavigate('briefe')}>
          📄 Brief analysieren
        </button>
        <button className="btn btn-secondary" style={{ flex: 1 }} onClick={() => onNavigate('termine')}>
          + Termin
        </button>
      </div>
    </div>
  )
}

function MiniCard({ event, onClick }) {
  return (
    <div className="event-card" onClick={onClick}>
      <div className="event-card-date">
        {formatKurzdatum(event.datum)}{event.uhrzeit ? ` · ${event.uhrzeit} Uhr` : ''}
      </div>
      <div className="event-card-title">{event.titel}</div>
      <div className="event-card-meta">
        {event.kind && <span className="badge badge-gray">{event.kind}</span>}
        <span className={`badge ${event.abholung ? 'badge-green' : 'badge-yellow'}`}>
          {event.abholung ? `Abholung: ${event.abholung}` : 'Abholung offen'}
        </span>
        {event.fahrer && <span className="badge badge-blue">Fahrer: {event.fahrer}</span>}
      </div>
    </div>
  )
}
