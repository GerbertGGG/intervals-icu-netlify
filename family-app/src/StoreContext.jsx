import { createContext, useContext, useState, useCallback } from 'react'

const STORAGE_KEY = 'familienorg_v1'

function defaultState() {
  return {
    settings: {
      elternteilA: 'Papa',
      elternteilB: 'Mama',
      kinder: ['Kind'],
      betreuungspersonen: ['Oma', 'Opa'],
      claudeApiKey: '',
    },
    events: [],
    letters: [],
    careRequests: [],
  }
}

function loadState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY)
    if (!raw) return defaultState()
    const stored = JSON.parse(raw)
    const def = defaultState()
    return {
      ...def,
      ...stored,
      settings: { ...def.settings, ...(stored.settings || {}) },
    }
  } catch {
    return defaultState()
  }
}

const StoreCtx = createContext(null)

export function StoreProvider({ children }) {
  const [data, setData] = useState(loadState)

  const update = useCallback((updater) => {
    setData(prev => {
      const next = typeof updater === 'function' ? updater(prev) : { ...prev, ...updater }
      try { localStorage.setItem(STORAGE_KEY, JSON.stringify(next)) } catch {}
      return next
    })
  }, [])

  return <StoreCtx.Provider value={{ data, update }}>{children}</StoreCtx.Provider>
}

export function useStore() {
  const ctx = useContext(StoreCtx)
  if (!ctx) throw new Error('useStore outside StoreProvider')
  return ctx
}
