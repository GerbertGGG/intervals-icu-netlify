import { useState, useRef } from 'react'
import { useStore } from '../StoreContext.jsx'
import { analysiertBrief } from '../api.js'
import { genId, heute, formatKurzdatum } from '../utils.js'

export default function BriefAnalyse({ onNavigate }) {
  const { data, update } = useStore()
  const { settings } = data

  const [text, setText] = useState('')
  const [filename, setFilename] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const [result, setResult] = useState(null)
  const [selectedTermine, setSelectedTermine] = useState(new Set())
  const [saved, setSaved] = useState(false)
  const [dragOver, setDragOver] = useState(false)
  const fileRef = useRef()

  function handleFile(file) {
    if (!file) return
    setFilename(file.name)
    const reader = new FileReader()
    reader.onload = e => setText(e.target.result)
    reader.readAsText(file, 'UTF-8')
  }

  function handleDrop(e) {
    e.preventDefault()
    setDragOver(false)
    const file = e.dataTransfer.files[0]
    if (file) handleFile(file)
  }

  async function analyse() {
    if (!text.trim()) {
      setError('Bitte zuerst Text eingeben oder Datei hochladen.')
      return
    }
    if (!settings.claudeApiKey) {
      setError('Kein Claude API-Schlüssel konfiguriert. Bitte in den Einstellungen eintragen.')
      return
    }
    setError('')
    setLoading(true)
    setResult(null)
    setSaved(false)
    try {
      const res = await analysiertBrief(text, settings.claudeApiKey)
      setResult(res)
      setSelectedTermine(new Set(res.termine.map((_, i) => i)))
    } catch (err) {
      setError(err.message)
    } finally {
      setLoading(false)
    }
  }

  function toggleTermin(idx) {
    setSelectedTermine(prev => {
      const next = new Set(prev)
      if (next.has(idx)) next.delete(idx)
      else next.add(idx)
      return next
    })
  }

  function speichern() {
    if (!result) return

    const briefId = genId()
    const newEventIds = []
    const newEvents = []

    for (const idx of selectedTermine) {
      const t = result.termine[idx]
      const id = genId()
      newEventIds.push(id)
      newEvents.push({
        id,
        titel: t.titel,
        datum: t.datum || '',
        uhrzeit: t.uhrzeit || '',
        kind: settings.kinder[0] || '',
        abholung: '',
        fahrer: '',
        notizen: t.beschreibung || '',
        quelleId: briefId,
        erstellt: new Date().toISOString(),
      })
    }

    const letter = {
      id: briefId,
      titel: filename || 'Manuell eingegeben',
      inhalt: text,
      zusammenfassung: result.zusammenfassung,
      extrahierteTermineIds: newEventIds,
      erstellt: new Date().toISOString(),
    }

    update(prev => ({
      ...prev,
      letters: [letter, ...prev.letters],
      events: [...prev.events, ...newEvents],
    }))

    setSaved(true)
    setText('')
    setFilename('')
    setResult(null)
  }

  function reset() {
    setText('')
    setFilename('')
    setResult(null)
    setError('')
    setSaved(false)
  }

  return (
    <div>
      {saved && (
        <div className="alert alert-success mb-3">
          ✅ Brief gespeichert und Termine übernommen!{' '}
          <button className="btn btn-secondary btn-sm" onClick={() => onNavigate('termine')}>
            Termine ansehen
          </button>
        </div>
      )}

      {!settings.claudeApiKey && (
        <div className="alert alert-warning mb-3">
          ⚠️ Kein Claude API-Schlüssel.{' '}
          <button className="btn btn-secondary btn-sm" onClick={() => onNavigate('einstell')}>
            Einstellungen öffnen
          </button>
        </div>
      )}

      {/* Upload area */}
      <div className="card mb-3">
        <h2 className="mb-3">Brief hochladen oder einfügen</h2>

        <div
          className={`upload-area mb-3${dragOver ? ' drag-over' : ''}`}
          onClick={() => fileRef.current?.click()}
          onDragOver={e => { e.preventDefault(); setDragOver(true) }}
          onDragLeave={() => setDragOver(false)}
          onDrop={handleDrop}
        >
          <div className="upload-icon">📂</div>
          <p className="font-medium">{filename || 'Datei hier ablegen oder klicken'}</p>
          <p className="text-sm text-muted mt-1">Textdateien (.txt, .md) oder beliebiger Text</p>
          <input
            ref={fileRef}
            type="file"
            accept=".txt,.md,.text,text/*"
            style={{ display: 'none' }}
            onChange={e => handleFile(e.target.files[0])}
          />
        </div>

        <div className="field">
          <label className="label">Text direkt einfügen</label>
          <textarea
            className="textarea"
            style={{ minHeight: 160 }}
            placeholder="Elternbrief, Einladung oder Ankündigung hier einfügen…"
            value={text}
            onChange={e => setText(e.target.value)}
          />
        </div>

        {error && <div className="alert alert-error">{error}</div>}

        <div className="flex gap-2">
          <button
            className="btn btn-primary"
            style={{ flex: 1 }}
            disabled={loading || !text.trim()}
            onClick={analyse}
          >
            {loading ? <><span className="spinner" /> Analysiere…</> : '🤖 Analysieren'}
          </button>
          {text && (
            <button className="btn btn-secondary" onClick={reset}>
              Zurücksetzen
            </button>
          )}
        </div>
      </div>

      {/* Result */}
      {result && (
        <div className="card">
          <h2 className="mb-3">Ergebnis der Analyse</h2>

          <div className="mb-3">
            <p className="label">Zusammenfassung</p>
            <p className="text-sm" style={{ lineHeight: 1.6 }}>{result.zusammenfassung}</p>
          </div>

          {result.termine.length > 0 ? (
            <>
              <p className="label mb-2">
                Erkannte Termine ({result.termine.length})
                <span className="text-muted text-xs" style={{ fontWeight: 400, marginLeft: 6 }}>
                  Auswählen, welche übernommen werden sollen
                </span>
              </p>

              {result.termine.map((t, idx) => (
                <div
                  key={idx}
                  className="care-item"
                  style={{
                    cursor: 'pointer',
                    border: selectedTermine.has(idx)
                      ? '1px solid var(--primary-border)'
                      : '1px solid transparent',
                    background: selectedTermine.has(idx) ? 'var(--primary-light)' : 'var(--g50)',
                    borderRadius: 'var(--rs)',
                  }}
                  onClick={() => toggleTermin(idx)}
                >
                  <div style={{ fontSize: '1.2rem' }}>
                    {selectedTermine.has(idx) ? '✅' : '⬜'}
                  </div>
                  <div className="care-item-info">
                    <p className="font-medium">{t.titel}</p>
                    {t.datum && (
                      <p className="text-sm text-muted">
                        {formatKurzdatum(t.datum)}{t.uhrzeit ? ` · ${t.uhrzeit} Uhr` : ''}
                      </p>
                    )}
                    {t.beschreibung && (
                      <p className="text-xs text-muted mt-1">{t.beschreibung}</p>
                    )}
                  </div>
                </div>
              ))}

              <button
                className="btn btn-primary btn-block mt-3"
                disabled={selectedTermine.size === 0}
                onClick={speichern}
              >
                {selectedTermine.size === 0
                  ? 'Keine Termine ausgewählt'
                  : `${selectedTermine.size} Termin${selectedTermine.size !== 1 ? 'e' : ''} übernehmen`}
              </button>
            </>
          ) : (
            <div className="alert alert-info">
              Keine konkreten Termine gefunden.{' '}
              <button className="btn btn-secondary btn-sm" onClick={speichern}>
                Nur Zusammenfassung speichern
              </button>
            </div>
          )}
        </div>
      )}

      {/* Previous letters */}
      {data.letters.length > 0 && (
        <div className="mt-4">
          <h2 className="mb-3">Frühere Briefe</h2>
          {data.letters.map(l => (
            <div key={l.id} className="card card-clickable">
              <div className="flex items-center justify-between">
                <p className="font-medium">{l.titel}</p>
                <span className="badge badge-gray">
                  {l.extrahierteTermineIds?.length || 0} Termin{(l.extrahierteTermineIds?.length || 0) !== 1 ? 'e' : ''}
                </span>
              </div>
              <p className="text-sm text-muted mt-1" style={{ lineHeight: 1.5 }}>{l.zusammenfassung}</p>
              <p className="text-xs text-muted mt-2">
                {new Date(l.erstellt).toLocaleDateString('de-DE')}
              </p>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
