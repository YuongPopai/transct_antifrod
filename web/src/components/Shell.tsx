import React from 'react'
import { NavLink, useLocation } from 'react-router-dom'
import { motion } from 'framer-motion'
import { Activity, Boxes, Gavel, Users, LogOut } from 'lucide-react'
import ErrorBoundary from './ErrorBoundary'

function Item({ to, icon: Icon, label }: any) {
  return (
    <NavLink
      to={to}
      className={({ isActive }) =>
        `flex items-center gap-2 px-3 py-2 rounded-xl transition ${isActive ? 'bg-zinc-800 text-white' : 'text-zinc-300 hover:bg-zinc-900/60'}`
      }
    >
      <Icon size={16} />
      <span className="text-sm">{label}</span>
    </NavLink>
  )
}

export default function Shell({ user, onLogout, children }: any) {
  return (
    <div className="min-h-screen grid grid-cols-[260px_1fr]">
      <aside className="p-4 border-r border-zinc-900 bg-gradient-to-b from-zinc-950 to-zinc-950">
        <motion.div
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.4 }}
          className="mb-4"
        >
          <div className="text-lg font-semibold">Antifraud Platform</div>
          <div className="text-xs text-zinc-500">signed in as <span className="text-zinc-300">{user.username}</span> • {user.role}</div>
        </motion.div>

        <div className="space-y-1">
          <Item to="/monitor" icon={Activity} label="Monitor" />
          <Item to="/entities" icon={Boxes} label="Entities" />
          <Item to="/rules" icon={Gavel} label="Rules" />
          {user.role === 'admin' && <Item to="/admin/users" icon={Users} label="Admin" />}
        </div>

        <div className="mt-6">
          <button
            onClick={onLogout}
            className="w-full flex items-center justify-center gap-2 px-3 py-2 rounded-xl bg-zinc-900 hover:bg-zinc-800 transition"
          >
            <LogOut size={16} />
            <span className="text-sm">Logout</span>
          </button>
        </div>
      </aside>

      <main className="p-6">
        <ErrorBoundary>
          {children}
        </ErrorBoundary>
      </main>
    </div>
  )
}
