import React, { useEffect, useMemo, useState } from 'react'
import { listEntities, listAttributes, listRules, createRule, deleteRule, listVersions, createVersion, activateVersion, validateRule } from '../lib/api'

function Panel({ title, children }: any) {
  return (
    <div className="rounded-3xl border border-zinc-800 bg-zinc-900/40 p-4 shadow-soft">
      <div className="text-sm text-zinc-300 mb-2">{title}</div>
      {children}
    </div>
  )
}

const OPS = ['==','!=','<','<=','>','>=']

export default function RulesPage() {
  const [entities, setEntities] = useState<any[]>([])
  const [selectedEntity, setSelectedEntity] = useState<any | null>(null)
  const [attrs, setAttrs] = useState<any[]>([])
  const [rules, setRules] = useState<any[]>([])
  const [selectedRule, setSelectedRule] = useState<any | null>(null)
  const [versions, setVersions] = useState<any[]>([])

  const [newRuleName, setNewRuleName] = useState('')

  // version editor
  const [exprType, setExprType] = useState<'expr'|'jsonlogic'>('expr')
  const [exprSource, setExprSource] = useState('amount < 5 && sender != receiver')
  const [builder, setBuilder] = useState<any[]>([{ attr: '', op: '==', value: '' }])
  const [validateMsg, setValidateMsg] = useState<string>('')

  useEffect(() => {
    ;(async () => {
      const e = await listEntities()
      setEntities(e)
    })()
  }, [])

  async function selectEntity(e: any) {
    setSelectedEntity(e)
    setSelectedRule(null)
    setVersions([])
    setAttrs(await listAttributes(e.entity_id))
    setRules(await listRules(e.entity_id))
  }

  async function selectRule(r: any) {
    setSelectedRule(r)
    setVersions(await listVersions(r.rule_id))
  }

  async function addRule() {
    if (!selectedEntity || !newRuleName.trim()) return
    await createRule({ entity_id: selectedEntity.entity_id, name: newRuleName, is_active: true, priority: 100, severity: 'warn', action: 'alert' })
    setNewRuleName('')
    setRules(await listRules(selectedEntity.entity_id))
  }

  function buildJsonlogic() {
    const clauses = builder
      .filter((c: any) => c.attr && c.op)
      .map((c: any) => ({ [c.op]: [ { var: c.attr }, parseValue(c.value) ] }))
    if (!clauses.length) return { '==': [1, 0] }
    if (clauses.length === 1) return clauses[0]
    return { and: clauses }
  }

  function parseValue(v: string) {
    const s = v.trim()
    if (s === 'true') return true
    if (s === 'false') return false
    if (s === '') return ''
    const n = Number(s)
    if (!Number.isNaN(n) && s.match(/^-?\d+(\.\d+)?$/)) return n
    // string
    return s
  }

  async function onValidate() {
    setValidateMsg('')
    const sample: any = {}
    attrs.forEach((a: any) => { sample[a.attr_key] = a.type === 'number' ? 1 : a.type === 'boolean' ? true : 'x' })
    try {
      const body = exprType === 'expr'
        ? { expr_type: 'expr', expr: { lang: 'safe_expr_v1', source: exprSource }, sample_payload: sample }
        : { expr_type: 'jsonlogic', expr: buildJsonlogic(), sample_payload: sample }
      const r = await validateRule(body)
      setValidateMsg(r.ok ? '✅ Valid' : `❌ ${r.errors?.[0] || 'invalid'}`)
    } catch (e: any) {
      setValidateMsg(`❌ ${e.message}`)
    }
  }

  async function saveVersion() {
    if (!selectedRule) return
    const body = exprType === 'expr'
      ? { expr_type: 'expr', expr: { lang: 'safe_expr_v1', source: exprSource } }
      : { expr_type: 'jsonlogic', expr: buildJsonlogic() }
    await createVersion(selectedRule.rule_id, body)
    setVersions(await listVersions(selectedRule.rule_id))
  }

  return (
    <div className="grid grid-cols-[360px_1fr] gap-4">
      <Panel title="Pick entity">
        <div className="space-y-2">
          {entities.map((e) => (
            <button
              key={e.entity_id}
              onClick={() => selectEntity(e)}
              className={`w-full text-left px-3 py-2 rounded-2xl border transition ${selectedEntity?.entity_id === e.entity_id ? 'border-zinc-600 bg-zinc-800/40' : 'border-zinc-900 hover:bg-zinc-900/60'}`}
            >
              <div className="text-sm font-medium">{e.name}</div>
              <div className="text-xs text-zinc-500">{e.entity_key}</div>
            </button>
          ))}
        </div>
      </Panel>

      <div className="space-y-4">
        <Panel title={selectedEntity ? `Rules for ${selectedEntity.name}` : 'Rules'}>
          {!selectedEntity ? (
            <div className="text-sm text-zinc-500">Select an entity first.</div>
          ) : (
            <>
              <div className="flex gap-2">
                <input value={newRuleName} onChange={(e) => setNewRuleName(e.target.value)} placeholder="New rule name" className="flex-1 rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" />
                <button onClick={addRule} className="rounded-xl bg-zinc-800 border border-zinc-700 px-3 py-2 hover:bg-zinc-700">Add</button>
              </div>

              <div className="mt-3 grid grid-cols-2 gap-3">
                <div className="space-y-2">
                  {rules.map((r) => (
                    <div key={r.rule_id} className={`rounded-2xl border px-3 py-2 ${selectedRule?.rule_id === r.rule_id ? 'border-zinc-600 bg-zinc-800/40' : 'border-zinc-900 bg-zinc-950/30'}`}>
                      <button onClick={() => selectRule(r)} className="w-full text-left">
                        <div className="text-sm font-medium">{r.name}</div>
                        <div className="text-xs text-zinc-500">priority {r.priority} • {r.action}</div>
                        <div className="mt-1">
                          <span className={`px-2 py-0.5 rounded-full text-xs ${r.active_version_id ? 'bg-green-600 text-white' : 'bg-zinc-700 text-zinc-300'}`}>
                            {r.active_version_id ? 'Has active version' : 'No active version'}
                          </span>
                        </div>
                      </button>
                      <div className="mt-2 flex justify-end">
                        <button onClick={() => deleteRule(r.rule_id).then(() => listRules(selectedEntity.entity_id).then(setRules))} className="text-xs px-2 py-1 rounded-xl bg-red-500/10 border border-red-800 hover:bg-red-500/20">Delete</button>
                      </div>
                    </div>
                  ))}
                </div>

                <div>
                  {!selectedRule ? (
                    <div className="text-sm text-zinc-500">Select a rule to manage versions.</div>
                  ) : (
                    <>
                      <div className="mb-2 text-sm text-zinc-300">Versions</div>
                      <div className="space-y-2">
                        {versions.map((v) => (
                          <div key={v.rule_version_id} className="rounded-2xl border border-zinc-900 bg-zinc-950/30 px-3 py-2">
                            <div className="text-sm">v{v.version_no} • {v.expr_type}</div>
                            <pre className="mt-2 text-xs bg-zinc-950/60 border border-zinc-900 rounded-xl p-2 overflow-auto max-h-32">{JSON.stringify(v.expr, null, 2)}</pre>
                            <div className="mt-2 flex justify-end">
                              {v.is_active ? (
                                <div className="text-xs px-2 py-1 rounded-xl bg-green-600 text-white">Active</div>
                              ) : (
                                <button onClick={async () => { try { await activateVersion(selectedRule.rule_id, v.rule_version_id); setVersions(await listVersions(selectedRule.rule_id)); setRules(await listRules(selectedEntity!.entity_id)); } catch (e: any) { alert(e?.message || String(e)) } }} className="text-xs px-2 py-1 rounded-xl bg-zinc-800 border border-zinc-700 hover:bg-zinc-700">Activate</button>
                              )}
                            </div>
                          </div>
                        ))}
                      </div>
                    </>
                  )}
                </div>
              </div>
            </>
          )}
        </Panel>

        <Panel title="Rule editor (builder + custom expression)">
          {!selectedRule ? (
            <div className="text-sm text-zinc-500">Pick a rule above.</div>
          ) : (
            <>
              <div className="flex items-center gap-3 mb-3">
                <select value={exprType} onChange={(e) => setExprType(e.target.value as any)} className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2">
                  <option value="expr">Custom expression</option>
                  <option value="jsonlogic">Builder (JSONLogic)</option>
                </select>
                <button onClick={onValidate} className="rounded-xl bg-zinc-800 border border-zinc-700 px-3 py-2 hover:bg-zinc-700">Validate</button>
                {validateMsg && <div className="text-sm text-zinc-300">{validateMsg}</div>}
              </div>

              {exprType === 'expr' ? (
                <>
                  <div className="text-xs text-zinc-400">Style: JS-like (&&, ||, !). Variables = attribute keys.</div>
                  <textarea value={exprSource} onChange={(e) => setExprSource(e.target.value)} className="mt-2 w-full h-28 rounded-2xl bg-zinc-950 border border-zinc-800 p-3 font-mono text-sm" />
                  <div className="mt-2 text-xs text-zinc-500">
                    functions: len(x), lower(x), upper(x), contains(a,b), abs(x), round(x,n), startswith(s,p), endswith(s,p)
                  </div>
                </>
              ) : (
                <>
                  <div className="text-xs text-zinc-400">Builder: AND of conditions</div>
                  <div className="mt-2 space-y-2">
                    {builder.map((c: any, idx: number) => (
                      <div key={idx} className="grid grid-cols-4 gap-2">
                        <select value={c.attr} onChange={(e) => {
                          const b = [...builder]; b[idx] = { ...b[idx], attr: e.target.value }; setBuilder(b)
                        }} className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2">
                          <option value="">attr…</option>
                          {attrs.map((a: any) => <option key={a.attr_key} value={a.attr_key}>{a.attr_key}</option>)}
                        </select>
                        <select value={c.op} onChange={(e) => {
                          const b = [...builder]; b[idx] = { ...b[idx], op: e.target.value }; setBuilder(b)
                        }} className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2">
                          {OPS.map((o) => <option key={o} value={o}>{o}</option>)}
                        </select>
                        <input value={c.value} onChange={(e) => {
                          const b = [...builder]; b[idx] = { ...b[idx], value: e.target.value }; setBuilder(b)
                        }} placeholder="value" className="rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2" />
                        <button onClick={() => setBuilder(builder.filter((_: any, i: number) => i !== idx))} className="rounded-xl bg-red-500/10 border border-red-800 px-3 py-2 hover:bg-red-500/20">Remove</button>
                      </div>
                    ))}
                    <button onClick={() => setBuilder([...builder, { attr: '', op: '==', value: '' }])} className="rounded-xl bg-zinc-800 border border-zinc-700 px-3 py-2 hover:bg-zinc-700">Add condition</button>

                    <div className="mt-2">
                      <div className="text-xs text-zinc-500">Generated JSONLogic</div>
                      <pre className="mt-1 text-xs bg-zinc-950/60 border border-zinc-900 rounded-xl p-2 overflow-auto max-h-40">{JSON.stringify(buildJsonlogic(), null, 2)}</pre>
                    </div>
                  </div>
                </>
              )}

              <div className="mt-3">
                <button onClick={saveVersion} className="rounded-xl bg-white text-zinc-950 px-3 py-2 font-medium hover:opacity-90">Save as new version</button>
              </div>
            </>
          )}
        </Panel>
      </div>
    </div>
  )
}
