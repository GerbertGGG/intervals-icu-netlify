import { useState } from 'react'
import { useStore } from '../StoreContext.jsx'
import { genId, formatDatum, formatKurzdatum, formatMonat, sortByDate, heute, isFuture, groupByMonth } from '../utils.js'

const EMPTY_EVENT = {
  titel: '',
  datum: '',
  uhrzeit: '',
  kind: '',
  abholung: '',
  fahrer: '',
  notizen: '',
}

export default function Termine() {
  const { data, update } = useStore()
  const { events, settings } = data

  const [filter, setFilter] = useState('alle') // alle | offen | heute
  const [modal, setModal] = useState(null) // null | { mode: 'new'|'edit', event }
  const [form, setForm] = useState(EMPTY_EVENT)
  const [confirmDelete, setConfirmDelete] = useState(null)

  const allPersons = [
    settings.elternteilA,
    settings.elternteilB,
    ...settings.betreuungspersonen,
  ].filter(Boolean)

  const filtered = (() => {
    let list = [...events]
    if (filter === 'offen') list = list.filter(e => !e.abholung)
    if (filter === 'zukunft') list = list.filter(e => isFuture(e.datum))
    return sortByDate(list)
  })()

  const grouped = groupByMonth(filtered)
  const months = Object.keys(grouped).sort()

  function openNew() {
    setForm({ ...EMPTY_EVENT, kind: settings.kinder[0] || '', datum: heute() })
    setModal({ mode: 'new' })
  }

  function openEdit(event) {
    setForm({ ...event })
    setModal({ mode: 'edit', event })
  }

  function closeModal() {
    setModal(null)
    setForm(EMPTY_EVENT)
  }

  function save() {
    if (!form.titel.trim()) return

    if (modal.mode === 'new') {
      const newEvent = {
        ...form,
        id: genId(),
        quelleId: '',
        erstellt: new Date().toISOString(),
      }
      update(prev => ({ ...prev, events: [...prev.events, newEvent] }))
    } else {
      update(prev => ({
        ...prev,
        events: prev.events.map(e => (e.id === form.id ? { ...form } : e)),
      }))
    }
    closeModal()
  }

  function deleteEvent(id) {
    update(prev => ({
      ...prev,
      events: prev.events.filter(e => e.id !== id),
      careRequests: prev.careRequests.filter(c => c.terminId !== id),
    }))
    setConfirmDelete(null)
    closeModal()
  }

  return (
    <div>
      {/* Filter tabs */}
      <div style={{ display: 'flex', gap: 6, marginBottom: '0.875rem', flexWrap: 'wrap' }}>
        {[
          { id: 'alle', label: 'Alle' },
          { id: 'zukunft', label: 'Zukünftige' },
          { id: 'offen', label: '⚠️ Ohne Abholung' },
        ].map(f => (
          <button
            key={f.id}
            className={`btn btn-sm ${filter === f.id ? 'btn-primary' : 'btn-secondary'}`}
            onClick={() => setFilter(f.id)}
          >
            {f.label}
          </button>
        ))}
        <button className="btn btn-primary btn-sm" style={{ marginLeft: 'auto' }} onClick={openNew}>
          + Termin
        </button>
      </div>

      {/* Event list */}
      {months.length === 0 ? (
        <div className="empty">
          <div className="empty-icon">📅</div>
          <p>Keine Termine{filter !== 'alle' ? ' in dieser Ansicht' : ''}.</p>
        </div>
      ) : (
        months.map(month => (
          <div key={month}>
            <div className="month-header">{formatMonat(month + '-01')}</div>
            {grouped[month].map(event => (
              <EventCard key={event.id} event={event} onClick={() => openEdit(event)} />
            ))}
          </div>
        ))
      )}

      {/* Add/Edit Modal */}
      {modal && (
        <div className="modal-backdrop" onClick={e => e.target === e.currentTarget && closeModal()}>
          <div className="modal">
            <div className="modal-header">
              <h2>{modal.mode === 'new' ? 'Neuer Termin' : 'Termin bearbeiten'}</h2>
              <button className="modal-close" onClick={closeModal}>×</button>
            </div>

            <div className="field">
              <label className="label">Titel *</label>
              <input
                className="input"
                placeholder="z.B. Kindergeburtstag Lisa"
                value={form.titel}
                onChange={e => setForm(p => ({ ...p, titel: e.target.value }))}
                autoFocus
              />
            </div>

            <div className="flex gap-2">
              <div className="field" style={{ flex: 2 }}>
                <label className="label">Datum</label>
                <input
                  className="input"
                  type="date"
                  value={form.datum}
                  onChange={e => setForm(p => ({ ...p, datum: e.target.value }))}
                />
              </div>
              <div className="field" style={{ flex: 1 }}>
                <label className="label">Uhrzeit</label>
                <input
                  className="input"
                  type="time"
                  value={form.uhrzeit}
                  onChange={e => setForm(p => ({ ...p, uhrzeit: e.target.value }))}
                />
              </div>
            </div>

            {settings.kinder.length > 1 && (
              <div className="field">
                <label className="label">Kind</label>
                <select
                  className="select"
                  value={form.kind}
                  onChange={e => setForm(p => ({ ...p, kind: e.target.value }))}
                >
                  <option value="">— auswählen —</option>
                  {settings.kinder.map(k => (
                    <option key={k} value={k}>{k}</option>
                  ))}
                </select>
              </div>
            )}

            <div className="field">
              <label className="label">Abholung: wer holt das Kind ab?</label>
              <div className="person-btns">
                {allPersons.map(p => (
                  <button
                    key={p}
                    className={`person-btn${form.abholung === p ? ' selected' : ''}`}
                    onClick={() => setForm(prev => ({ ...prev, abholung: prev.abholung === p ? '' : p }))}
                  >
                    {p}
                  </button>
                ))}
                {form.abholung && (
                  <button className="person-btn clear" onClick={() => setForm(p => ({ ...p, abholung: '' }))}>
                    ✕ Löschen
                  </button>
                )}
              </div>
            </div>

            <div className="field">
              <label className="label">Fahrer: wer fährt hin?</label>
              <div className="person-btns">
                {allPersons.map(p => (
                  <button
                    key={p}
                    className={`person-btn${form.fahrer === p ? ' selected' : ''}`}
                    onClick={() => setForm(prev => ({ ...prev, fahrer: prev.fahrer === p ? '' : p }))}
                  >
                    {p}
                  </button>
                ))}
                {form.fahrer && (
                  <button className="person-btn clear" onClick={() => setForm(p => ({ ...p, fahrer: '' }))}>
                    ✕ Löschen
                  </button>
                )}
              </div>
            </div>

            <div className="field">
              <label className="label">Notizen</label>
              <textarea
                className="textarea"
                style={{ minHeight: 80 }}
                placeholder="Adresse, Mitbringen, Hinweise…"
                value={form.notizen}
                onChange={e => setForm(p => ({ ...p, notizen: e.target.value }))}
              />
            </div>

            <div className="flex gap-2">
              <button
                className="btn btn-primary"
                style={{ flex: 1 }}
                disabled={!form.titel.trim()}
                onClick={save}
              >
                Speichern
              </button>
              {modal.mode === 'edit' && (
                <button
                  className="btn btn-danger"
                  onClick={() => setConfirmDelete(form.id)}
                >
                  Löschen
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Delete confirmation */}
      {confirmDelete && (
        <div className="modal-backdrop" onClick={() => setConfirmDelete(null)}>
          <div className="modal" style={{ maxWidth: 340 }}>
            <h2 className="mb-3">Termin löschen?</h2>
            <p className="text-sm text-muted mb-3">
              Der Termin und alle zugehörigen Betreuungsanfragen werden unwiderruflich gelöscht.
            </p>
            <div className="flex gap-2">
              <button className="btn btn-danger" style={{ flex: 1 }} onClick={() => deleteEvent(confirmDelete)}>
                Löschen
              </button>
              <button className="btn btn-secondary" style={{ flex: 1 }} onClick={() => setConfirmDelete(null)}>
                Abbrechen
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function EventCard({ event, onClick }) {
  const pastStyle = event.datum && event.datum < heute()
    ? { opacity: 0.6 }
    : {}

  return (
    <div className="event-card" style={pastStyle} onClick={onClick}>
      <div className="event-card-date">
        {event.datum ? formatKurzdatum(event.datum) : 'Kein Datum'}
        {event.uhrzeit ? ` · ${event.uhrzeit} Uhr` : ''}
      </div>
      <div className="event-card-title">{event.titel}</div>
      <div className="event-card-meta">
        {event.kind && <span className="badge badge-gray">{event.kind}</span>}
        <span className={`badge ${event.abholung ? 'badge-green' : 'badge-yellow'}`}>
          {event.abholung ? `Abholung: ${event.abholung}` : 'Abholung offen'}
        </span>
        {event.fahrer && <span className="badge badge-blue">Fahrer: {event.fahrer}</span>}
      </div>
      {event.notizen && (
        <p className="text-xs text-muted mt-2" style={{ lineHeight: 1.4 }}>
          {event.notizen.slice(0, 100)}{event.notizen.length > 100 ? '…' : ''}
        </p>
      )}
    </div>
  )
}
