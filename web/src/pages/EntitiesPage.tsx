import React, { useEffect, useState } from 'react'
import { motion } from 'framer-motion'
import { listEntities, createEntity, updateEntity, deleteEntity, listAttributes, createAttribute, deleteAttribute, getEntityDependencies } from '../lib/api'

function Panel({ title, children }: any) {
  return (
    <div className="rounded-3xl border border-zinc-800 bg-zinc-900/40 p-4 shadow-soft">
      <div className="text-sm text-zinc-300 mb-2">{title}</div>
      {children}
    </div>
  )
}

export default function EntitiesPage({ user }: any) {
  const [entities, setEntities] = useState<any[]>([])
  const [selected, setSelected] = useState<any | null>(null)
  const [attrs, setAttrs] = useState<any[]>([])
  const [form, setForm] = useState({ entity_key: '', name: '', description: '', is_active: true })
  const [attrForm, setAttrForm] = useState({ attr_key: '', name: '', type: 'string', is_required: false })

  async function reload() {
    const e = await listEntities()
    setEntities(e)
    if (selected) {
      const fresh = e.find((x: any) => x.entity_id === selected.entity_id) || null
      setSelected(fresh)
      if (fresh) setAttrs(await listAttributes(fresh.entity_id))
    }
  }

  useEffect(() => { reload() }, [])

  async function selectEntity(e: any) {
    setSelected(e)
    setAttrs(await listAttributes(e.entity_id))
    setForm({ entity_key: e.entity_key, name: e.name, description: e.description || '', is_active: e.is_active })
  }

  async function saveEntity() {
    try {
      if (!selected) {
        await createEntity(form)
      } else {
        await updateEntity(selected.entity_id, form)
      }
      setSelected(null)
      setForm({ entity_key: '', name: '', description: '', is_active: true })
      await reload()
    } catch (err: any) {
      alert(err?.message || String(err))
    }
  }

  async function removeEntity(id: number) {
    try {
      await deleteEntity(id)
      setSelected(null)
      setAttrs([])
      await reload()
    } catch (err: any) {
      // If entity has dependent objects, server returns 409 with detail object
      let msg = err?.message || String(err)
      try {
        const parsed = JSON.parse(msg)
        if (parsed.detail && typeof parsed.detail === 'object') {
          const d = parsed.detail
          const parts: string[] = []
          if (d.events) parts.push(`${d.events} events`)
          if (d.classifications) parts.push(`${d.classifications} classifications`)
          if (d.rules) parts.push(`${d.rules} rules`)
          if (d.attributes) parts.push(`${d.attributes} attributes`)
          msg = `Entity has dependent objects: ${parts.join(', ')}. Contact an admin to force delete.`
        }
      } catch (_e) {
        // leave original message
      }
      alert(msg)
    }
  }

  async function forceDelete(id: number) {
    if (user?.role !== 'admin') {
      alert('Force delete is only available to admin users')
      return
    }
    try {
      const deps = await getEntityDependencies(id)
      const parts: string[] = []
      if (deps.events) parts.push(`${deps.events} events`)
      if (deps.classifications) parts.push(`${deps.classifications} classifications`)
      if (deps.rules) parts.push(`${deps.rules} rules`)
      if (deps.attributes) parts.push(`${deps.attributes} attributes`)

      const ok = confirm(`FORCE DELETE: Are you sure you want to permanently delete this entity and all dependent objects? It has: ${parts.join(', ')}. This cannot be undone.`)
      if (!ok) return

      const res = await deleteEntity(id, true)
      if (res && res.deleted) {
        alert(`Force delete completed. Deleted: ${res.deleted.entities} entities, ${res.deleted.events} events, ${res.deleted.classifications} classifications.`)
      } else {
        alert('Force delete completed.')
      }
      setSelected(null)
      setAttrs([])
      await reload()
    } catch (err: any) {
      let msg = err?.message || String(err)
      try {
        const parsed = JSON.parse(msg)
        if (parsed.detail) msg = typeof parsed.detail === 'string' ? parsed.detail : JSON.stringify(parsed.detail)
      } catch (_e) {}
      alert(msg)
    }
  }

  async function addAttr() {
    if (!selected) return
    if (!attrForm.attr_key) { alert('attr_key is required'); return }
    // client-side duplicate check for better UX
    if (attrs.some((a) => a.attr_key === attrForm.attr_key)) {
      alert('Attribute with this key already exists')
      return
    }
    try {
      await createAttribute(selected.entity_id, attrForm)
      setAttrForm({ attr_key: '', name: '', type: 'string', is_required: false })
      setAttrs(await listAttributes(selected.entity_id))
    } catch (err: any) {
      alert(err?.message || String(err))
    }
  }

  async function removeAttr(id: number) {
    await deleteAttribute(id)
    if (!selected) return
    setAttrs(await listAttributes(selected.entity_id))
  }

  return (
    <div className="grid grid-cols-[360px_1fr] gap-4">
      <Panel title="Entities">
        <div className="space-y-2">
          {entities.map((e) => (
            <button
              key={e.entity_id}
              onClick={() => selectEntity(e)}
              className={`w-full text-left px-3 py-2 rounded-2xl border transition ${selected?.entity_id === e.entity_id ? 'border-zinc-600 bg-zinc-800/40' : 'border-zinc-900 hover:bg-zinc-900/60'}`}
            >
              <div className="text-sm font-medium">{e.name}</div>
              <div className="text-xs text-zinc-500">{e.entity_key}</div>
            </button>
          ))}
        </div>
      </Panel>

      <div className="space-y-4">
        <div className="flex items-center justify-between">
          <div className="text-sm text-zinc-400">{selected ? `Edit entity: ${selected.name}` : 'Create entity'}</div>
          <div className="flex items-center gap-2">
            <button
              onClick={() => { if (!selected) { alert('Select an entity first'); return } forceDelete(selected.entity_id) }}
              disabled={!selected || user?.role !== 'admin'}
              title={!selected ? 'Select an entity first' : user?.role !== 'admin' ? 'Only admins can force delete' : 'Force delete (admin only)'}
              className={`rounded-xl px-3 py-2 font-medium ${!selected || user?.role !== 'admin' ? 'bg-zinc-800 border border-zinc-700 text-zinc-400 cursor-not-allowed' : 'bg-red-700 text-white hover:opacity-90'}`}
            >
              Force delete
            </button>
          </div>
        </div>

        <Panel title="">
          <div className="grid grid-cols-2 gap-3">
            <div>
              <div className="text-xs text-zinc-400">entity_key</div>
              <input className="mt-1 w-full rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={form.entity_key} onChange={(e) => setForm({ ...form, entity_key: e.target.value })} />
            </div>
            <div>
              <div className="text-xs text-zinc-400">name</div>
              <input className="mt-1 w-full rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={form.name} onChange={(e) => setForm({ ...form, name: e.target.value })} />
            </div>
            <div className="col-span-2">
              <div className="text-xs text-zinc-400">description</div>
              <input className="mt-1 w-full rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={form.description} onChange={(e) => setForm({ ...form, description: e.target.value })} />
            </div>
          </div>

          <div className="mt-3 flex items-center gap-2">
            <button onClick={saveEntity} className="rounded-xl bg-white text-zinc-950 px-3 py-2 font-medium hover:opacity-90">Save</button>
            {selected && <button onClick={() => removeEntity(selected.entity_id)} className="rounded-xl bg-red-500/20 border border-red-700 px-3 py-2 hover:bg-red-500/30">Delete</button>}
            <button onClick={() => { setSelected(null); setForm({ entity_key: '', name: '', description: '', is_active: true }); setAttrs([]) }} className="rounded-xl bg-zinc-900 border border-zinc-800 px-3 py-2 hover:bg-zinc-800">Clear</button>
          </div>
        </Panel>

        <Panel title="Attributes">
          {!selected ? (
            <div className="text-sm text-zinc-500">Select an entity to manage attributes.</div>
          ) : (
            <>
              <div className="grid grid-cols-4 gap-2">
                <input placeholder="attr_key" className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={attrForm.attr_key} onChange={(e) => setAttrForm({ ...attrForm, attr_key: e.target.value })} />
                <input placeholder="name" className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={attrForm.name} onChange={(e) => setAttrForm({ ...attrForm, name: e.target.value })} />
                <select className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" value={attrForm.type} onChange={(e) => setAttrForm({ ...attrForm, type: e.target.value })}>
                  {['string','number','boolean','datetime','object','array'].map((t) => <option key={t} value={t}>{t}</option>)}
                </select>
                <button onClick={addAttr} className="rounded-xl bg-zinc-800 border border-zinc-700 px-3 py-2 hover:bg-zinc-700">Add</button>
              </div>

              <div className="mt-3 space-y-2">
                {attrs.map((a) => (
                  <div key={a.attr_id} className="flex items-center justify-between rounded-2xl border border-zinc-900 bg-zinc-950/40 px-3 py-2">
                    <div>
                      <div className="text-sm font-medium">{a.name} <span className="text-xs text-zinc-500">({a.attr_key})</span></div>
                      <div className="text-xs text-zinc-500">type: {a.type}{a.is_required ? ' • required' : ''}</div>
                    </div>
                    <button onClick={() => removeAttr(a.attr_id)} className="text-xs px-2 py-1 rounded-xl bg-red-500/10 border border-red-800 hover:bg-red-500/20">Delete</button>
                  </div>
                ))}
              </div>
            </>
          )}
        </Panel>
      </div>
    </div>
  )
}
