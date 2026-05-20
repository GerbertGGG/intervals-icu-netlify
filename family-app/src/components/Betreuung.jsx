import { useState } from 'react'
import { useStore } from '../StoreContext.jsx'
import { genId, formatKurzdatum, sortByDate, isFuture, heute } from '../utils.js'

const STATUS_LABELS = {
  offen: { label: 'Gefragt – keine Antwort', badge: 'badge-yellow' },
  zugesagt: { label: 'Zugesagt ✓', badge: 'badge-green' },
  abgesagt: { label: 'Abgesagt ✗', badge: 'badge-red' },
}

export default function Betreuung() {
  const { data, update } = useStore()
  const { events, careRequests, settings } = data

  const [selectedTerminId, setSelectedTerminId] = useState('')
  const [selectedPerson, setSelectedPerson] = useState('')
  const [notizen, setNotizen] = useState('')
  const [showForm, setShowForm] = useState(false)
  const [filter, setFilter] = useState('offen') // offen | alle

  const futureEvents = sortByDate(events.filter(e => !e.datum || isFuture(e.datum)))

  const filteredRequests = careRequests.filter(c => {
    if (filter === 'offen') return c.status === 'offen'
    return true
  })

  const requestsByTermin = {}
  for (const c of filteredRequests) {
    if (!requestsByTermin[c.terminId]) requestsByTermin[c.terminId] = []
    requestsByTermin[c.terminId].push(c)
  }

  function getEvent(id) {
    return events.find(e => e.id === id)
  }

  function addRequest() {
    if (!selectedTerminId || !selectedPerson) return

    const existing = careRequests.find(
      c => c.terminId === selectedTerminId && c.person === selectedPerson
    )
    if (existing) {
      alert(`${selectedPerson} wurde für diesen Termin bereits gefragt.`)
      return
    }

    const req = {
      id: genId(),
      terminId: selectedTerminId,
      person: selectedPerson,
      status: 'offen',
      notizen,
      erstellt: new Date().toISOString(),
    }
    update(prev => ({ ...prev, careRequests: [...prev.careRequests, req] }))
    setShowForm(false)
    setSelectedTerminId('')
    setSelectedPerson('')
    setNotizen('')
    setFilter('offen')
  }

  function updateStatus(id, status) {
    update(prev => ({
      ...prev,
      careRequests: prev.careRequests.map(c =>
        c.id === id ? { ...c, status, geantwortet: new Date().toISOString() } : c
      ),
    }))
  }

  function deleteRequest(id) {
    update(prev => ({
      ...prev,
      careRequests: prev.careRequests.filter(c => c.id !== id),
    }))
  }

  const openCount = careRequests.filter(c => c.status === 'offen').length

  return (
    <div>
      {/* Summary */}
      <div className="stat-grid mb-3">
        <div className="stat-card">
          <div className="stat-value" style={{ color: openCount > 0 ? 'var(--warning)' : 'var(--success)' }}>
            {openCount}
          </div>
          <div className="stat-label">Offene Anfragen</div>
        </div>
        <div className="stat-card">
          <div className="stat-value" style={{ color: 'var(--success)' }}>
            {careRequests.filter(c => c.status === 'zugesagt').length}
          </div>
          <div className="stat-label">Zugesagt</div>
        </div>
      </div>

      {/* Add request */}
      {!showForm ? (
        <button className="btn btn-primary btn-block mb-3" onClick={() => setShowForm(true)}>
          + Person fragen
        </button>
      ) : (
        <div className="card mb-3">
          <div className="modal-header" style={{ marginBottom: '1rem' }}>
            <h2>Person anfragen</h2>
            <button className="modal-close" onClick={() => setShowForm(false)}>×</button>
          </div>

          <div className="field">
            <label className="label">Für welchen Termin?</label>
            <select
              className="select"
              value={selectedTerminId}
              onChange={e => setSelectedTerminId(e.target.value)}
            >
              <option value="">— Termin auswählen —</option>
              {futureEvents.map(e => (
                <option key={e.id} value={e.id}>
                  {e.datum ? formatKurzdatum(e.datum) + ' · ' : ''}{e.titel}
                </option>
              ))}
            </select>
          </div>

          <div className="field">
            <label className="label">Wen fragen?</label>
            <div className="person-btns">
              {[...settings.betreuungspersonen, settings.elternteilA, settings.elternteilB]
                .filter(Boolean)
                .map(p => (
                  <button
                    key={p}
                    className={`person-btn${selectedPerson === p ? ' selected' : ''}`}
                    onClick={() => setSelectedPerson(prev => prev === p ? '' : p)}
                  >
                    {p}
                  </button>
                ))}
            </div>
          </div>

          <div className="field">
            <label className="label">Notiz (optional)</label>
            <input
              className="input"
              placeholder="z.B. Abholung um 15:00 Uhr"
              value={notizen}
              onChange={e => setNotizen(e.target.value)}
            />
          </div>

          <div className="flex gap-2">
            <button
              className="btn btn-primary"
              style={{ flex: 1 }}
              disabled={!selectedTerminId || !selectedPerson}
              onClick={addRequest}
            >
              Anfrage speichern
            </button>
            <button className="btn btn-secondary" onClick={() => setShowForm(false)}>
              Abbrechen
            </button>
          </div>
        </div>
      )}

      {/* Filter */}
      <div style={{ display: 'flex', gap: 6, marginBottom: '0.875rem' }}>
        {[
          { id: 'offen', label: '⏳ Offen' },
          { id: 'alle', label: 'Alle' },
        ].map(f => (
          <button
            key={f.id}
            className={`btn btn-sm ${filter === f.id ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setFilter(f.id)}
          >
            {f.label}
          </button>
        ))}
      </div>

      {/* Requests grouped by event */}
      {Object.keys(requestsByTermin).length === 0 ? (
        <div className="empty">
          <div className="empty-icon">👵</div>
          <p>
            {filter === 'offen'
              ? 'Keine offenen Betreuungsanfragen.'
              : 'Noch keine Betreuungsanfragen.'}
          </p>
          <p className="text-sm text-muted mt-2">
            Klicke auf „Person fragen", um eine Anfrage zu erfassen.
          </p>
        </div>
      ) : (
        Object.entries(requestsByTermin).map(([terminId, requests]) => {
          const event = getEvent(terminId)
          return (
            <div key={terminId} className="card">
              <div className="mb-2">
                <p className="font-medium">{event?.titel || 'Unbekannter Termin'}</p>
                {event?.datum && (
                  <p className="text-sm text-muted">
                    {formatKurzdatum(event.datum)}{event.uhrzeit ? ` · ${event.uhrzeit} Uhr` : ''}
                  </p>
                )}
              </div>
              <div className="divider" />
              {requests.map(req => (
                <RequestItem
                  key={req.id}
                  req={req}
                  onStatus={updateStatus}
                  onDelete={deleteRequest}
                />
              ))}
            </div>
          )
        })
      )}
    </div>
  )
}

function RequestItem({ req, onStatus, onDelete }) {
  const { label, badge } = STATUS_LABELS[req.status] || STATUS_LABELS.offen
  const [confirm, setConfirm] = useState(false)

  return (
    <div className="care-item">
      <div className="care-item-info">
        <div className="flex items-center gap-2">
          <span className="font-medium">{req.person}</span>
          <span className={`badge ${badge}`}>{label}</span>
        </div>
        {req.notizen && (
          <p className="text-xs text-muted mt-1">{req.notizen}</p>
        )}
        <p className="text-xs text-muted mt-1">
          Gefragt: {new Date(req.erstellt).toLocaleDateString('de-DE')}
        </p>
      </div>

      <div className="care-item-actions" style={{ flexDirection: 'column', gap: 4 }}>
        {req.status !== 'zugesagt' && (
          <button
            className="btn btn-success btn-sm"
            onClick={() => onStatus(req.id, 'zugesagt')}
          >
            ✓ Ja
          </button>
        )}
        {req.status !== 'abgesagt' && (
          <button
            className="btn btn-danger btn-sm"
            onClick={() => onStatus(req.id, 'abgesagt')}
          >
            ✗ Nein
          </button>
        )}
        {confirm ? (
          <button className="btn btn-secondary btn-sm" onClick={() => { onDelete(req.id); setConfirm(false) }}>
            Ja, löschen
          </button>
        ) : (
          <button className="btn btn-secondary btn-sm" onClick={() => setConfirm(true)}>
            🗑
          </button>
        )}
      </div>
    </div>
  )
}
