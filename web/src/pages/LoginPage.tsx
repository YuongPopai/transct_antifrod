import React, { useState } from 'react'
import { motion } from 'framer-motion'
import { useNavigate } from 'react-router-dom'
import { login, me, setToken } from '../lib/api'

export default function LoginPage({ onLogin }: any) {
  const [username, setUsername] = useState('admin')
  const [password, setPassword] = useState('admin')
  const [error, setError] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const navigate = useNavigate()

  async function onSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    setLoading(true)
    try {
      const t = await login(username, password)
      setToken(t)
      const u = await me()
      onLogin(u)
      // redirect to main app after successful login
      navigate('/monitor', { replace: true })
    } catch (err: any) {
      setError(err.message || 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen grid place-items-center p-6">
      <motion.div
        initial={{ opacity: 0, y: 18 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="w-full max-w-md rounded-3xl shadow-soft border border-zinc-800 bg-zinc-900/40 backdrop-blur p-6"
      >
        <div className="mb-4">
          <div className="text-2xl font-semibold">Welcome back</div>
          <div className="text-sm text-zinc-400">Sign in to manage entities, rules, and monitor events.</div>
        </div>

        <form onSubmit={onSubmit} className="space-y-3">
          <div>
            <label className="text-xs text-zinc-400">Username</label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="mt-1 w-full rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2 outline-none focus:border-zinc-600"
            />
          </div>
          <div>
            <label className="text-xs text-zinc-400">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="mt-1 w-full rounded-xl bg-zinc-950 border border-zinc-800 px-3 py-2 outline-none focus:border-zinc-600"
            />
          </div>

          {error && <div className="text-sm text-red-400">{error}</div>}

          <button
            disabled={loading}
            className="w-full rounded-xl bg-white text-zinc-950 font-medium px-3 py-2 hover:opacity-90 transition disabled:opacity-60"
          >
            {loading ? 'Signing in…' : 'Sign in'}
          </button>
        </form>

        <div className="mt-4 text-xs text-zinc-500">
          Default demo credentials: <span className="text-zinc-200">admin/admin</span>
        </div>
      </motion.div>
    </div>
  )
}
