import { useState } from 'react'
import Dashboard from './components/Dashboard.jsx'
import BriefAnalyse from './components/BriefAnalyse.jsx'
import Termine from './components/Termine.jsx'
import Betreuung from './components/Betreuung.jsx'
import Einstellungen from './components/Einstellungen.jsx'

const VIEWS = [
  { id: 'dashboard', icon: '🏠', label: 'Übersicht' },
  { id: 'briefe',    icon: '📄', label: 'Briefe' },
  { id: 'termine',   icon: '📅', label: 'Termine' },
  { id: 'betreuung', icon: '👶', label: 'Betreuung' },
  { id: 'einstell',  icon: '⚙️', label: 'Einst.' },
]

const TITLES = {
  dashboard: '🏠 FamilienOrg',
  briefe:    '📄 Brief analysieren',
  termine:   '📅 Termine',
  betreuung: '👶 Betreuung',
  einstell:  '⚙️ Einstellungen',
}

export default function App() {
  const [view, setView] = useState('dashboard')

  return (
    <div className="app">
      <header className="header">
        <h1>{TITLES[view]}</h1>
      </header>

      <main className="main">
        {view === 'dashboard' && <Dashboard onNavigate={setView} />}
        {view === 'briefe'    && <BriefAnalyse onNavigate={setView} />}
        {view === 'termine'   && <Termine />}
        {view === 'betreuung' && <Betreuung />}
        {view === 'einstell'  && <Einstellungen />}
      </main>

      <nav className="nav">
        {VIEWS.map(v => (
          <button
            key={v.id}
            className={`nav-item${view === v.id ? ' active' : ''}`}
            onClick={() => setView(v.id)}
          >
            <span className="nav-icon">{v.icon}</span>
            {v.label}
          </button>
        ))}
      </nav>
    </div>
  )
}
