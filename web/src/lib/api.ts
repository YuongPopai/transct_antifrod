const API_BASE = import.meta.env.VITE_API_BASE || 'http://localhost:8000'

export type Token = string

export function getToken(): string | null {
  return localStorage.getItem('token')
}

export function setToken(t: string | null) {
  if (t) localStorage.setItem('token', t)
  else localStorage.removeItem('token')
}

async function request(path: string, opts: RequestInit = {}) {
  const token = getToken()
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(opts.headers as any),
  }
  if (token) headers['Authorization'] = `Bearer ${token}`

  const res = await fetch(`${API_BASE}${path}`, { ...opts, headers })
  if (!res.ok) {
    const text = await res.text()
    throw new Error(text || `HTTP ${res.status}`)
  }
  if (res.status === 204) return null
  return res.json()
}

export async function login(username: string, password: string): Promise<string> {
  const form = new URLSearchParams()
  form.append('username', username)
  form.append('password', password)
  const res = await fetch(`${API_BASE}/auth/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: form.toString(),
  })
  if (!res.ok) throw new Error('Bad credentials')
  const data = await res.json()
  return data.access_token
}

export async function me() {
  return request('/auth/me')
}

export async function listEntities() {
  return request('/entities')
}
export async function createEntity(body: any) {
  return request('/entities', { method: 'POST', body: JSON.stringify(body) })
}
export async function updateEntity(id: number, body: any) {
  return request(`/entities/${id}`, { method: 'PUT', body: JSON.stringify(body) })
}
export async function deleteEntity(id: number, force = false) {
  const url = `/entities/${id}` + (force ? '?force=true' : '')
  return request(url, { method: 'DELETE' })
}

export async function getEntityDependencies(id: number) {
  return request(`/entities/${id}/dependencies`)
}

export async function listAttributes(entityId: number) {
  return request(`/entities/${entityId}/attributes`)
}
export async function createAttribute(entityId: number, body: any) {
  return request(`/entities/${entityId}/attributes`, { method: 'POST', body: JSON.stringify(body) })
}
export async function updateAttribute(attrId: number, body: any) {
  return request(`/attributes/${attrId}`, { method: 'PUT', body: JSON.stringify(body) })
}
export async function deleteAttribute(attrId: number) {
  return request(`/attributes/${attrId}`, { method: 'DELETE' })
}

export async function listRules(entityId: number) {
  return request(`/entities/${entityId}/rules`)
}
export async function createRule(body: any) {
  return request(`/rules`, { method: 'POST', body: JSON.stringify(body) })
}
export async function updateRule(ruleId: number, body: any) {
  return request(`/rules/${ruleId}`, { method: 'PUT', body: JSON.stringify(body) })
}
export async function deleteRule(ruleId: number) {
  return request(`/rules/${ruleId}`, { method: 'DELETE' })
}

export async function listVersions(ruleId: number) {
  return request(`/rules/${ruleId}/versions`)
}
export async function createVersion(ruleId: number, body: any) {
  return request(`/rules/${ruleId}/versions`, { method: 'POST', body: JSON.stringify(body) })
}
export async function activateVersion(ruleId: number, versionId: number) {
  return request(`/rules/${ruleId}/activate/${versionId}`, { method: 'POST' })
}
export async function validateRule(body: any) {
  return request(`/rules/validate`, { method: 'POST', body: JSON.stringify(body) })
}

export async function recent(limit = 200) {
  return request(`/monitor/recent?limit=${limit}`)
}

export async function monitorSummary(limit = 200, entityKey?: string, ruleIds?: number[]) {
  const params = new URLSearchParams()
  params.append('limit', String(limit))
  if (entityKey) params.append('entity_key', entityKey)
  if (ruleIds && ruleIds.length) ruleIds.forEach((id) => params.append('rule_ids', String(id)))
  return request(`/monitor/summary?${params.toString()}`)
}

export async function listUsers() {
  return request('/admin/users')
}
export async function createUser(body: any) {
  return request('/admin/users', { method: 'POST', body: JSON.stringify(body) })
}
export async function patchUser(userId: number, body: any) {
  return request(`/admin/users/${userId}`, { method: 'PATCH', body: JSON.stringify(body) })
}
export async function deleteUser(userId: number) {
  return request(`/admin/users/${userId}`, { method: 'DELETE' })
}
export async function simulateEvents(action?: 'start' | 'stop' | 'once' | 'status', entityId?: number | null, ruleId?: number | null) {
  const method = action === 'status' ? 'GET' : 'POST'
  let url = action ? `/admin/simulate_events?action=${action}` : '/admin/simulate_events'
  if (entityId) url += `&entity_id=${entityId}`
  if (ruleId) url += `&rule_id=${ruleId}`
  return request(url, { method })
}

export async function deleteClassification(eventId: string) {
  return request(`/monitor/${eventId}`, { method: 'DELETE' })
}

export async function clearClassifications() {
  return request(`/monitor/clear`, { method: 'DELETE' })
}
