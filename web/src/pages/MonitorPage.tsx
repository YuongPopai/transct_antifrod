import React, { useEffect, useMemo, useRef, useState } from 'react'
import { motion } from 'framer-motion'
import { recent, deleteClassification, clearClassifications, monitorSummary, listEntities, listRules } from '../lib/api'

const WS_BASE = import.meta.env.VITE_WS_BASE || 'ws://localhost:8000'

function safeStringify(obj: any) {
  try {
    return JSON.stringify(obj, null, 2)
  } catch (e) {
    try { return String(obj) } catch { return '<unserializable>' }
  }
}

function Card({ ev, onDelete }: any) {
  const entity_key = typeof ev.entity_key === 'string' ? ev.entity_key : ''
  const decision = typeof ev.decision === 'string' ? ev.decision : 'legit'
  const event_id = typeof ev.event_id === 'string' ? ev.event_id : String(ev.event_id ?? '')
  const payload = ev.payload ?? {}
  const matched_rules = Array.isArray(ev.matched_rules) ? ev.matched_rules : []

  return (
    <motion.div layout initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} className="rounded-2xl border border-zinc-800 bg-zinc-900/40 p-3">
      <div className="flex items-center justify-between">
        <div className="text-sm font-medium">{entity_key}</div>
        <div className="flex items-center space-x-2">
          <div className={`text-xs px-2 py-1 rounded-full ${decision === 'alert' ? 'bg-red-900/40 text-red-200 border border-red-800' : decision === 'invalid' ? 'bg-yellow-900/40 text-yellow-200 border border-yellow-800' : 'bg-emerald-900/30 text-emerald-200 border border-emerald-800'}`}>{decision}</div>
          <button onClick={() => onDelete(event_id)} className="text-zinc-400 hover:text-zinc-200">✕</button>
        </div>
      </div>
      <div className="mt-2 text-xs text-zinc-400">{event_id}</div>
      <pre className="mt-2 text-xs bg-zinc-950/60 border border-zinc-900 rounded-xl p-2 overflow-auto max-h-40">{safeStringify(payload)}</pre>
      {matched_rules.length ? (
        <div className="mt-2 text-xs text-zinc-300">
          matched: {matched_rules.map((r: any) => (r && r.name) ? r.name : String(r)).join(', ')}
        </div>
      ) : null}
    </motion.div>
  )
}

export default function MonitorPage() {
  const [items, setItems] = useState<any[]>([])
  const [entities, setEntities] = useState<any[]>([])
  const [selectedEntityId, setSelectedEntityId] = useState<number | null>(null)
  const [rules, setRules] = useState<any[]>([])
  const [selectedRuleIds, setSelectedRuleIds] = useState<number[]>([])
  const [summary, setSummary] = useState<any | null>(null)
  const [isFiltering, setIsFiltering] = useState(false)
  const wsRef = useRef<WebSocket | null>(null)
  const filterRefreshRef = useRef<number | null>(null)

  function sanitizeEvent(ev: any) {
    try {
      if (!ev || typeof ev !== 'object') return null
      const out: any = {}
      out.event_id = typeof ev.event_id === 'string' ? ev.event_id : String(ev.event_id ?? '')
      out.entity_key = typeof ev.entity_key === 'string' ? ev.entity_key : ''
      // prefer explicit decision, otherwise infer from matched_rules or reasons
      let decision = 'legit'
      if (typeof ev.decision === 'string' && ev.decision) {
        decision = ev.decision
      } else if (Array.isArray(ev.matched_rules) && ev.matched_rules.length) {
        decision = 'alert'
      } else if (Array.isArray(ev.reasons) && ev.reasons.some((r: any) => r && r.type === 'rule_match')) {
        decision = 'alert'
      }
      out.decision = decision
      out.payload = ev.payload ?? {}
      out.matched_rules = Array.isArray(ev.matched_rules) ? ev.matched_rules.map((r: any) => (r && (r.name || r.rule_id)) ? { rule_id: r.rule_id, name: r.name ? String(r.name) : undefined } : r) : []
      out.reasons = Array.isArray(ev.reasons) ? ev.reasons : []
      out.classified_at = ev.classified_at ?? ev.occurred_at ?? new Date().toISOString()
      return out
    } catch (e) {
      console.warn('sanitizeEvent failed', e, ev)
      return null
    }
  }

  useEffect(() => {
    // fetch entities for filtering controls
    ;(async () => {
      try {
        const e = await listEntities()
        setEntities(Array.isArray(e) ? e : [])
      } catch (err) {
        console.warn('Failed to load entities for Monitor filters', err)
      }
    })()

    // fetch recent with a couple retries to avoid a race/timing issue on first navigation
    const fetchRecent = async (attempt = 1) => {
      try {
        if (isFiltering && selectedEntityId) {
          await fetchSummary()
          return
        }
        const r = await recent(200)
        const safe = Array.isArray(r) ? r.map(sanitizeEvent).filter(Boolean) : []
        setItems(safe)
        // fetch global summary after loading recent events
        try { fetchSummary().catch(() => {}) } catch {}
      } catch (e) {
        if (attempt < 3) {
          setTimeout(() => fetchRecent(attempt + 1), attempt * 200)
        } else {
          // don't throw - leave items empty and allow ws open to refresh
          console.warn('Monitor: failed to fetch recent after retries', e)
        }
      }
    }
    fetchRecent()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  useEffect(() => {
    // when selected entity changes, load its rules
    ;(async () => {
      try {
        if (!selectedEntityId) {
          setRules([])
          setSelectedRuleIds([])
          setIsFiltering(false)
          setSummary(null)
          return
        }
        const rs = await listRules(selectedEntityId)
        setRules(Array.isArray(rs) ? rs : [])
        setSelectedRuleIds([])
        // auto-apply filter when entity selected
        setIsFiltering(true)
        await fetchSummary()
      } catch (err) {
        console.warn('Failed to load rules for selected entity', err)
      }
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedEntityId])

  useEffect(() => {
    // when selected rules change, reapply filter
    ;(async () => {
      if (!selectedEntityId) return
      setIsFiltering(true)
      await fetchSummary()
    })()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [selectedRuleIds])

  async function fetchSummary() {
    try {
      const entity = entities.find((e) => e.entity_id === selectedEntityId)
      const entityKey = entity?.entity_key
      const rids = selectedRuleIds && selectedRuleIds.length ? selectedRuleIds : undefined
      const res = await monitorSummary(200, entityKey, rids)
      if (!res) return
      const evs = Array.isArray(res.events) ? res.events.map(sanitizeEvent).filter(Boolean) : []
      setItems(evs)
      setSummary({ total: res.total, alert: res.alert, legit: res.legit, alert_pct: res.alert_pct, legit_pct: res.legit_pct })
    } catch (err) {
      console.warn('Failed to fetch monitor summary', err)
    }
  }

  useEffect(() => {
    let ws: WebSocket | null = null
    let reconnectAttempts = 0
    let buffer: any[] = []
    let flushTimer: any = null

    const connect = () => {
      try {
        ws = new WebSocket(`${WS_BASE}/ws/monitor`)
        wsRef.current = ws
        ws.onopen = () => {
          reconnectAttempts = 0
          // ping loop
          const t = setInterval(() => {
            try { ws!.send('ping') } catch (e) { console.warn('ws ping failed', e) }
          }, 15000)
          ;(ws as any)._t = t
          // ensure we have recent data after websocket opens
          ;(async () => {
            try {
              const r = await recent(200)
              const safe = Array.isArray(r) ? r.map(sanitizeEvent).filter(Boolean) : []
              setItems(safe)
              try { fetchSummary().catch(() => {}) } catch {}
            } catch (e) {
              console.warn('fetch recent on ws.open failed', e)
            }
          })()
        }
        ws.onmessage = (m) => {
          try {
            const ev = JSON.parse(m.data)
            // special control messages
            if (ev && ev.type === 'monitor_cleared') {
              setItems([])
              return
            }
            if (ev && ev.type === 'monitor_deleted') {
              setItems((prev) => prev.filter((x) => x.event_id !== ev.event_id))
              return
            }
            // validate expected shape (decision can be inferred)
            if (!ev || typeof ev !== 'object' || !ev.event_id) {
              console.warn('ws invalid message ignored', ev)
              return
            }
            if (!Array.isArray(ev.matched_rules)) ev.matched_rules = []
            if (!Array.isArray(ev.reasons)) ev.reasons = []
            // buffer messages and flush periodically to avoid UI freeze when simulator floods
            const safe = sanitizeEvent(ev)
            if (!safe) return
            buffer.push(safe)
            if (!flushTimer) {
              flushTimer = setTimeout(() => {
                setItems((prev) => {
                  try {
                    const merged = [...buffer, ...prev].slice(0, 600)
                    buffer = []
                    flushTimer = null
                    return merged
                  } catch (e) {
                    console.error('Failed to merge buffered ws messages', e)
                    buffer = []
                    flushTimer = null
                    return prev
                  }
                })
              }, 250)
            }

            // if filtering is active, schedule a server-side summary refresh (debounced)
            try {
              if (isFiltering) {
                if (filterRefreshRef.current) window.clearTimeout(filterRefreshRef.current)
                filterRefreshRef.current = window.setTimeout(() => { fetchSummary().catch(() => {}) }, 300)
              }
            } catch (e) {
              console.warn('Failed to schedule filter refresh', e)
            }
          } catch (e) {
            console.error('ws onmessage parse failed', e)
          }
        }
        ws.onclose = (ev) => {
          const t = (ws as any)._t
          if (t) clearInterval(t)
          console.warn('ws closed', ev)
          // attempt reconnect
          if (reconnectAttempts < 6) {
            reconnectAttempts += 1
            const delay = Math.min(2000 * reconnectAttempts, 10000)
            setTimeout(connect, delay)
          }
        }
        ws.onerror = (ev) => {
          console.error('ws error', ev)
        }
      } catch (e) {
        console.error('Failed to open WebSocket', e)
      }
    }

    connect()

    return () => {
      try { ws?.close() } catch {}
      if (flushTimer) clearTimeout(flushTimer)
    }
  }, [])

  const all = items
  const legit = useMemo(() => items.filter((x) => x.decision === 'legit'), [items])
  const alert = useMemo(() => items.filter((x) => x.decision === 'alert'), [items])

  const handleDelete = async (eventId: string) => {
    try {
      const res = await deleteClassification(eventId)
      if (!res || !res.ok) {
        alert('Failed to delete message')
        return
      }
      setItems((prev) => prev.filter((x) => x.event_id !== eventId))
    } catch (e: any) {
      alert(e?.message || String(e))
    }
  }

  const handleClear = async () => {
    if (!confirm('Clear all classifications from monitor?')) return
    try {
      const res = await clearClassifications()
      if (!res || !res.ok) {
        alert('Failed to clear classifications')
        return
      }
      // show deleted count to the user
      if (typeof res.deleted === 'number') {
        alert(`Cleared classifications: ${res.deleted}`)
      } else {
        alert('Cleared classifications')
      }
      setItems([])
    } catch (e: any) {
      alert(e?.message || String(e))
    }
  }

  try {
    return (
      <div>
        <div className="flex items-end justify-between mb-4">
          <div>
            <div className="text-2xl font-semibold">Realtime Monitor</div>
            <div className="text-sm text-zinc-400">Incoming events split into All / Legit / Alert</div>
          </div>
          <div className="flex items-center gap-4">
            <div>
              <div className="text-xs text-zinc-500">kept: {items.length}</div>
              {summary ? (
                <div className="mt-1 flex items-center gap-4">
                  <div className="text-sm">Alert: <span className="ml-2 text-red-400 font-semibold">{summary.alert} ({summary.alert_pct.toFixed(1)}%)</span></div>
                  <div className="text-sm">Legit: <span className="ml-2 text-emerald-400 font-semibold">{summary.legit} ({summary.legit_pct.toFixed(1)}%)</span></div>
                </div>
              ) : null}
            </div>

            <div className="flex items-center gap-3">
              <select className="text-sm bg-zinc-900 border border-zinc-800 rounded px-3 py-1.5 min-w-[180px]" value={selectedEntityId ?? ''} onChange={(e) => setSelectedEntityId(e.target.value ? Number(e.target.value) : null)}>
                <option value="">All entities</option>
                {entities.map((en) => <option key={en.entity_id} value={en.entity_id}>{en.entity_key}</option>)}
              </select>
              {rules.length ? (
                <select multiple className="text-sm bg-zinc-900 border border-zinc-800 rounded px-3 py-1.5 max-h-40 min-w-[240px]" value={selectedRuleIds.map(String)} onChange={(e) => {
                  const opts = Array.from(e.target.selectedOptions).map((o: any) => Number(o.value))
                  setSelectedRuleIds(opts)
                }}>
                  {rules.map((r: any) => <option key={r.rule_id} value={r.rule_id}>{r.name}</option>)}
                </select>
              ) : null}
              {isFiltering ? (
                <button onClick={() => { setSelectedEntityId(null); setSelectedRuleIds([]); setIsFiltering(false); setSummary(null); }} className="text-sm px-3 py-1.5 rounded-md bg-zinc-800 border border-zinc-700 hover:bg-zinc-700">Clear filter</button>
              ) : null}
            </div>

            <button onClick={handleClear} className="text-sm px-3 py-1.5 rounded-md bg-zinc-800 border border-zinc-700 hover:bg-zinc-700">Clear all</button>
          </div>
        </div>

        {items.length === 0 ? (
          <div className="rounded-xl border border-zinc-800 bg-zinc-900/40 p-8 text-center text-sm text-zinc-400">No recent events — waiting for incoming events or try "Run once" in Admin.</div>
        ) : (
          <div className="grid grid-cols-3 gap-4">
            <section>
              <div className="mb-2 text-sm text-zinc-300">All</div>
              <div className="space-y-2">
                {all.slice(0, 40).map((ev) => {
                  try {
                    return <Card key={`${ev.event_id}-${ev.classified_at}`} ev={ev} onDelete={handleDelete} />
                  } catch (e) {
                    console.error('Failed rendering event card', e, ev)
                    return <div key={String(ev?.event_id || Math.random())} className="rounded-xl border border-red-800 bg-red-900/10 p-2 text-xs text-red-200">Invalid event</div>
                  }
                })}
              </div>
            </section>
            <section>
              <div className="mb-2 text-sm text-zinc-300">Legit {summary ? (<span className="text-xs text-zinc-500">({summary.legit_pct.toFixed(1)}% of {summary.total})</span>) : null}</div>
              <div className="space-y-2">
                {legit.slice(0, 40).map((ev) => {
                  try {
                    return <Card key={`${ev.event_id}-${ev.classified_at}`} ev={ev} onDelete={handleDelete} />
                  } catch (e) {
                    console.error('Failed rendering event card', e, ev)
                    return <div key={String(ev?.event_id || Math.random())} className="rounded-xl border border-red-800 bg-red-900/10 p-2 text-xs text-red-200">Invalid event</div>
                  }
                })}
              </div>
            </section>
            <section>
              <div className="mb-2 text-sm text-zinc-300">Alert {summary ? (<span className="text-xs text-red-400">({summary.alert_pct.toFixed(1)}% of {summary.total})</span>) : null}</div>
              <div className="space-y-2">
                {alert.slice(0, 40).map((ev) => {
                  try {
                    return <Card key={`${ev.event_id}-${ev.classified_at}`} ev={ev} onDelete={handleDelete} />
                  } catch (e) {
                    console.error('Failed rendering event card', e, ev)
                    return <div key={String(ev?.event_id || Math.random())} className="rounded-xl border border-red-800 bg-red-900/10 p-2 text-xs text-red-200">Invalid event</div>
                  }
                })}
              </div>
            </section>
          </div>
        )}
      </div>
    )
  } catch (e) {
    console.error('Monitor render failed', e)
    return (
      <div className="rounded-xl border border-red-800 bg-red-900/10 p-6 text-center">
        <div className="text-sm text-red-200 mb-2">An unexpected rendering error occurred.</div>
        <div className="text-xs text-zinc-400 mb-4">You can try to clear Monitor data and resume:</div>
        <div className="flex justify-center gap-2">
          <button onClick={() => { try { setItems([]) } catch {} }} className="px-3 py-1 rounded bg-zinc-800 text-sm">Clear UI</button>
          <button onClick={() => window.location.reload()} className="px-3 py-1 rounded bg-red-700 text-sm">Refresh page</button>
        </div>
      </div>
    )
  }
}
