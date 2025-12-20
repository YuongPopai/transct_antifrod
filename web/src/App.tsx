import React, { useEffect, useState } from 'react'
import { Routes, Route, Navigate, useNavigate } from 'react-router-dom'
import { getToken, me, setToken } from './lib/api'
import LoginPage from './pages/LoginPage'
import MonitorPage from './pages/MonitorPage'
import EntitiesPage from './pages/EntitiesPage'
import RulesPage from './pages/RulesPage'
import AdminUsersPage from './pages/AdminUsersPage'
import Shell from './components/Shell'

export default function App() {
  const [loading, setLoading] = useState(true)
  const [user, setUser] = useState<any>(null)

  useEffect(() => {
    ;(async () => {
      const t = getToken()
      if (!t) {
        setLoading(false)
        setUser(null)
        return
      }
      try {
        const u = await me()
        setUser(u)
      } catch {
        setToken(null)
        setUser(null)
      } finally {
        setLoading(false)
      }
    })()
  }, [])

  if (loading) return <div className="min-h-screen grid place-items-center text-zinc-400">Loading…</div>

  return (
    <Routes>
      <Route path="/login" element={<LoginPage onLogin={setUser} />} />
      <Route
        path="/*"
        element={
          user ? (
            <Shell user={user} onLogout={() => { setToken(null); setUser(null) }}>
              <Routes>
                <Route path="/" element={<Navigate to="/monitor" replace />} />
                <Route path="/monitor" element={<MonitorPage />} />
                <Route path="/entities" element={<EntitiesPage user={user} />} />
                <Route path="/rules" element={<RulesPage />} />
                <Route path="/admin/users" element={user.role === 'admin' ? <AdminUsersPage /> : <Navigate to="/monitor" replace />} />
                <Route path="*" element={<Navigate to="/monitor" replace />} />
              </Routes>
            </Shell>
          ) : (
            <Navigate to="/login" replace />
          )
        }
      />
    </Routes>
  )
}
