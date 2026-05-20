import { useState } from 'react'
import { useStore } from '../StoreContext.jsx'

export default function Einstellungen() {
  const { data, update } = useStore()
  const [settings, setSettings] = useState({ ...data.settings })
  const [saved, setSaved] = useState(false)
  const [showKey, setShowKey] = useState(false)
  const [newKind, setNewKind] = useState('')
  const [newBetreuung, setNewBetreuung] = useState('')

  function save() {
    update(prev => ({ ...prev, settings }))
    setSaved(true)
    setTimeout(() => setSaved(false), 2500)
  }

  function addKind() {
    const val = newKind.trim()
    if (!val || settings.kinder.includes(val)) return
    setSettings(p => ({ ...p, kinder: [...p.kinder, val] }))
    setNewKind('')
  }

  function removeKind(name) {
    setSettings(p => ({ ...p, kinder: p.kinder.filter(k => k !== name) }))
  }

  function addBetreuung() {
    const val = newBetreuung.trim()
    if (!val || settings.betreuungspersonen.includes(val)) return
    setSettings(p => ({ ...p, betreuungspersonen: [...p.betreuungspersonen, val] }))
    setNewBetreuung('')
  }

  function removeBetreuung(name) {
    setSettings(p => ({ ...p, betreuungspersonen: p.betreuungspersonen.filter(b => b !== name) }))
  }

  function exportData() {
    const json = JSON.stringify(data, null, 2)
    const blob = new Blob([json], { type: 'application/json' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `familienorg-backup-${new Date().toISOString().slice(0, 10)}.json`
    a.click()
    URL.revokeObjectURL(url)
  }

  function importData(file) {
    const reader = new FileReader()
    reader.onload = e => {
      try {
        const imported = JSON.parse(e.target.result)
        if (imported.events && imported.settings) {
          update(() => imported)
          setSettings(imported.settings)
          alert('Daten erfolgreich importiert!')
        } else {
          alert('Ungültige Backup-Datei.')
        }
      } catch {
        alert('Fehler beim Lesen der Datei.')
      }
    }
    reader.readAsText(file)
  }

  function clearAll() {
    if (window.confirm('Alle Daten wirklich löschen? Diese Aktion kann nicht rückgängig gemacht werden.')) {
      update(() => ({
        settings: data.settings,
        events: [],
        letters: [],
        careRequests: [],
      }))
      alert('Alle Termine, Briefe und Betreuungsanfragen gelöscht.')
    }
  }

  return (
    <div>
      {saved && <div className="alert alert-success mb-3">✅ Einstellungen gespeichert.</div>}

      {/* Elternteile */}
      <div className="card mb-3">
        <h2 className="mb-3">Elternteile</h2>

        <div className="flex gap-2">
          <div className="field" style={{ flex: 1 }}>
            <label className="label">Elternteil A</label>
            <input
              className="input"
              value={settings.elternteilA}
              onChange={e => setSettings(p => ({ ...p, elternteilA: e.target.value }))}
              placeholder="z.B. Papa"
            />
          </div>
          <div className="field" style={{ flex: 1 }}>
            <label className="label">Elternteil B</label>
            <input
              className="input"
              value={settings.elternteilB}
              onChange={e => setSettings(p => ({ ...p, elternteilB: e.target.value }))}
              placeholder="z.B. Mama"
            />
          </div>
        </div>
      </div>

      {/* Kinder */}
      <div className="card mb-3">
        <h2 className="mb-3">Kinder</h2>

        <div className="tag-list mb-2">
          {settings.kinder.map(k => (
            <span key={k} className="tag">
              {k}
              <button className="tag-remove" onClick={() => removeKind(k)}>×</button>
            </span>
          ))}
        </div>

        <div className="flex gap-2">
          <input
            className="input"
            placeholder="Kind hinzufügen…"
            value={newKind}
            onChange={e => setNewKind(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && addKind()}
          />
          <button className="btn btn-secondary" onClick={addKind} disabled={!newKind.trim()}>
            +
          </button>
        </div>
      </div>

      {/* Betreuungspersonen */}
      <div className="card mb-3">
        <h2 className="mb-3">Betreuungspersonen</h2>
        <p className="text-sm text-muted mb-3">
          Wer kann als Abholung, Fahrer oder Betreuung in Frage kommen?
        </p>

        <div className="tag-list mb-2">
          {settings.betreuungspersonen.map(b => (
            <span key={b} className="tag">
              {b}
              <button className="tag-remove" onClick={() => removeBetreuung(b)}>×</button>
            </span>
          ))}
        </div>

        <div className="flex gap-2">
          <input
            className="input"
            placeholder="Person hinzufügen (z.B. Oma)…"
            value={newBetreuung}
            onChange={e => setNewBetreuung(e.target.value)}
            onKeyDown={e => e.key === 'Enter' && addBetreuung()}
          />
          <button className="btn btn-secondary" onClick={addBetreuung} disabled={!newBetreuung.trim()}>
            +
          </button>
        </div>
      </div>

      {/* Claude API Key */}
      <div className="card mb-3">
        <h2 className="mb-2">Claude API-Schlüssel</h2>
        <p className="text-sm text-muted mb-3" style={{ lineHeight: 1.5 }}>
          Wird für die KI-Analyse von Elternbriefen benötigt. Der Schlüssel wird nur
          lokal in diesem Browser gespeichert und nie an andere Server übertragen.
          Erhältlich unter{' '}
          <span style={{ color: 'var(--primary)', textDecoration: 'underline', cursor: 'default' }}>
            console.anthropic.com
          </span>
          .
        </p>

        <div className="field">
          <label className="label">API-Schlüssel</label>
          <div className="flex gap-2">
            <input
              className="input"
              type={showKey ? 'text' : 'password'}
              placeholder="sk-ant-…"
              value={settings.claudeApiKey}
              onChange={e => setSettings(p => ({ ...p, claudeApiKey: e.target.value }))}
            />
            <button className="btn btn-secondary" onClick={() => setShowKey(p => !p)}>
              {showKey ? '🙈' : '👁'}
            </button>
          </div>
        </div>

        {settings.claudeApiKey && (
          <p className="text-xs text-muted">
            ✓ Schlüssel eingetragen ({settings.claudeApiKey.slice(0, 12)}…)
          </p>
        )}
      </div>

      <button className="btn btn-primary btn-block mb-3" onClick={save}>
        Einstellungen speichern
      </button>

      {/* Data management */}
      <div className="card mb-3">
        <h2 className="mb-3">Daten</h2>

        <div className="flex gap-2 mb-2">
          <button className="btn btn-secondary" style={{ flex: 1 }} onClick={exportData}>
            📤 Backup exportieren
          </button>
          <label className="btn btn-secondary" style={{ flex: 1, justifyContent: 'center', cursor: 'pointer' }}>
            📥 Backup importieren
            <input
              type="file"
              accept=".json"
              style={{ display: 'none' }}
              onChange={e => { if (e.target.files[0]) importData(e.target.files[0]) }}
            />
          </label>
        </div>

        <div className="divider" />

        <div>
          <p className="text-sm text-muted mb-2">
            Termine: <strong>{data.events.length}</strong> ·
            Briefe: <strong>{data.letters.length}</strong> ·
            Betreuungsanfragen: <strong>{data.careRequests.length}</strong>
          </p>
          <button className="btn btn-danger btn-sm" onClick={clearAll}>
            Alle Termine und Daten löschen
          </button>
        </div>
      </div>

      <p className="text-xs text-muted" style={{ textAlign: 'center', padding: '0.5rem' }}>
        FamilienOrg · Daten bleiben lokal in diesem Browser
      </p>
    </div>
  )
}
