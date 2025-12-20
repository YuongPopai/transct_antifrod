import React, { useEffect, useRef, useState } from 'react'
import { createUser, listUsers, patchUser, simulateEvents, deleteUser, listEntities, listRules } from '../lib/api'

function Panel({ title, children }: any) {
  return (
    <div className="rounded-3xl border border-zinc-800 bg-zinc-900/40 p-4 shadow-soft">
      <div className="text-sm text-zinc-300 mb-2">{title}</div>
      {children}
    </div>
  )
}

export default function AdminUsersPage() {
  const [users, setUsers] = useState<any[]>([])
  const [form, setForm] = useState({ username: '', password: '', role: 'analyst' })

  async function removeUser(id: number) {
    if (!confirm('Are you sure you want to delete this user? This cannot be undone.')) return
    try {
      await deleteUser(id)
      await reload()
    } catch (e: any) {
      alert(e?.message || String(e))
    }
  }

  async function reload() {
    setUsers(await listUsers())
  }

  useEffect(() => {
    reload()
    ;(async () => {
      try {
        const e = await listEntities()
        setEntitiesForSim(Array.isArray(e) ? e : [])
      } catch (err) {
        console.warn('Failed to load entities for simulator', err)
      }
      try {
        const r: any = await simulateEvents('status')
        setSimRunning(!!r.running)
        if (r.last) { setLastSent(r.last.sent || 0); setLastDebug(r.last.debug || null) }
      } catch {}
    })()
  }, [])

  async function add() {
    try {
      await createUser(form)
      setForm({ username: '', password: '', role: 'analyst' })
      await reload()
    } catch (e: any) {
      alert(e?.message || String(e))
    }
  }

  const [simRunning, setSimRunning] = useState(false)
  const [lastSent, setLastSent] = useState<number | null>(null)
  const [lastDebug, setLastDebug] = useState<any | null>(null)

  const [entitiesForSim, setEntitiesForSim] = useState<any[]>([])
  const [selectedEntityForSim, setSelectedEntityForSim] = useState<number | null>(null)
  const [rulesForSim, setRulesForSim] = useState<any[]>([])
  const [selectedRuleForSim, setSelectedRuleForSim] = useState<number | null>(null)

  async function _runOnce() {
    try {
      const res: any = await simulateEvents('once', selectedEntityForSim ?? undefined, selectedRuleForSim ?? undefined)
      setLastSent(res.sent || 0)
      setLastDebug(res.debug || null)
      await reload()
    } catch (e: any) {
      alert(e?.message || String(e))
    }
  }

  async function runSimulator() {
    try {
      if (!simRunning) {
        if (!confirm('Start continuous simulation? This runs server-side and publishes messages to Kafka every ~10 seconds.')) return
        const res = await simulateEvents('start')
        if (res.ok) {
          setSimRunning(true)
          alert('Simulator started')
        } else {
          alert(res.detail || 'Failed to start simulator')
        }
      } else {
        const res = await simulateEvents('stop')
        if (res.ok) {
          setSimRunning(false)
          alert('Simulator stopped')
        } else {
          alert('Failed to stop simulator')
        }
      }
    } catch (e: any) {
      alert(e?.message || String(e))
    }
  }

  // cleanup on unmount
  useEffect(() => {
    return () => {
      if (simIntervalRef.current) clearInterval(simIntervalRef.current)
    }
  }, [])

  return (
    <div className="grid grid-cols-2 gap-4">
      <Panel title="Create analyst / admin">
        <div className="grid grid-cols-2 gap-2">
          <input placeholder="username" className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} />
          <input placeholder="password" type="password" className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} />
          <select className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })}>
            <option value="analyst">analyst</option>
            <option value="senior_analyst">senior_analyst</option>
            <option value="admin">admin</option>
          </select>
          <button onClick={add} className="rounded-xl bg-white text-zinc-950 px-3 py-2 font-medium hover:opacity-90">Create</button>
        </div>
        <div className="mt-2 text-xs text-zinc-500">Admins can manage users; analysts cannot see this tab.</div>
        <div className="mt-4">
          <div className="flex items-center gap-2">
            <select className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm" value={selectedEntityForSim ?? ''} onChange={(e) => { const v = e.target.value ? Number(e.target.value) : null; setSelectedEntityForSim(v); setSelectedRuleForSim(null); if (v) { listRules(v).then((rs) => setRulesForSim(Array.isArray(rs) ? rs : [])).catch(() => setRulesForSim([])) } else { setRulesForSim([]) } }}>
              <option value="">All entities</option>
              {entitiesForSim.map((en) => <option key={en.entity_id} value={en.entity_id}>{en.entity_key}</option>)}
            </select>
            <select className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2 text-sm" value={selectedRuleForSim ?? ''} onChange={(e) => setSelectedRuleForSim(e.target.value ? Number(e.target.value) : null)}>
              <option value="">All rules</option>
              {rulesForSim.map((r) => <option key={r.rule_id} value={r.rule_id}>{r.name}</option>)}
            </select>

            <button onClick={runSimulator} className={`rounded-xl px-3 py-2 hover:opacity-90 ${simRunning ? 'bg-red-600 text-white' : 'bg-indigo-600 text-white'}`}>
              {simRunning ? 'Stop simulator' : 'Start simulator'}
            </button>
            <button onClick={_runOnce} className="rounded-xl bg-zinc-800 text-white px-3 py-2">Run once</button>
          </div>
          {lastSent !== null && <div className="mt-2 text-xs text-zinc-400">Last run sent: {lastSent} events</div>}
          {lastDebug && (
            <details className="mt-2 text-xs text-zinc-400">
              <summary>Debug samples (first rules)</summary>
              <pre className="mt-1 p-2 rounded bg-zinc-950/60 border border-zinc-900 text-xs overflow-auto max-h-40">{JSON.stringify(lastDebug, null, 2)}</pre>
            </details>
          )}
        </div>
      </Panel>

      <Panel title="Users">
        <div className="space-y-2">
          {users.map((u) => (
            <div key={u.user_id} className="flex items-center justify-between rounded-2xl border border-zinc-900 bg-zinc-950/30 px-3 py-2">
              <div>
                <div className="text-sm font-medium">{u.username}</div>
                <div className="text-xs text-zinc-500">{u.role} • {u.is_active ? 'active' : 'disabled'}</div>
              </div>
              <div className="flex gap-2">
                <button onClick={() => patchUser(u.user_id, { is_active: !u.is_active }).then(reload)} className="text-xs px-2 py-1 rounded-xl bg-zinc-800 border border-zinc-700 hover:bg-zinc-700">
                  {u.is_active ? 'Disable' : 'Enable'}
                </button>
                <button onClick={() => removeUser(u.user_id)} className="text-xs px-2 py-1 rounded-xl bg-red-600 text-white hover:opacity-90">Delete</button>
              </div>
            </div>
          ))}
        </div>
      </Panel>
    </div>
  )
}
