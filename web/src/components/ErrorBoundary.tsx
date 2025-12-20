import React from 'react'

export default class ErrorBoundary extends React.Component<any, { hasError: boolean; error?: Error; countdown?: number; autoRefreshActive?: boolean }> {
  _timeout?: number | null
  _interval?: number | null

  constructor(props: any) {
    super(props)
    this.state = { hasError: false, countdown: 3, autoRefreshActive: true }
    this._timeout = null
    this._interval = null
  }

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error: error, countdown: 3, autoRefreshActive: true }
  }

  componentDidCatch(error: Error, info: any) {
    // eslint-disable-next-line no-console
    console.error('ErrorBoundary caught error', error, info)
    // start auto-refresh countdown
    this.startAutoRefresh()
  }

  componentWillUnmount() {
    this.clearTimers()
  }

  startAutoRefresh() {
    try {
      if (this._interval) return
      this._interval = window.setInterval(() => {
        this.setState((s: any) => {
          const next = (s.countdown ?? 0) - 1
          if (next <= 0) {
            // give a tiny delay then reload
            this.clearTimers()
            try { window.location.reload() } catch { /* ignore */ }
            return { countdown: 0 }
          }
          return { countdown: next }
        })
      }, 1000)
      // safety timeout to ensure reload happens even if interval missed
      this._timeout = window.setTimeout(() => {
        this.clearTimers()
        try { window.location.reload() } catch { /* ignore */ }
      }, 5000)
    } catch (e) {
      console.error('startAutoRefresh failed', e)
    }
  }

  clearTimers() {
    if (this._interval) {
      clearInterval(this._interval)
      this._interval = null
    }
    if (this._timeout) {
      clearTimeout(this._timeout)
      this._timeout = null
    }
  }

  cancelAutoRefresh() {
    this.clearTimers()
    this.setState({ autoRefreshActive: false })
  }

  render() {
    if (this.state.hasError) {
      const cd = this.state.countdown ?? 0
      const active = this.state.autoRefreshActive
      return (
        <div className="rounded-xl border border-red-800 bg-red-900/20 p-6 text-center text-sm text-red-200">
          <div className="mb-2">An error occurred rendering this view.</div>
          {active ? (
            <div className="text-xs text-zinc-300 mb-3">Auto-refreshing in {cd} second{cd === 1 ? '' : 's'}…</div>
          ) : (
            <div className="text-xs text-zinc-300 mb-3">Auto-refresh cancelled. You can refresh the page manually.</div>
          )}
          <div className="flex justify-center gap-2">
            <button onClick={() => { this.clearTimers(); try { window.location.reload() } catch {} }} className="px-3 py-1 rounded bg-zinc-800 text-sm">Refresh now</button>
            {active ? (
              <button onClick={() => this.cancelAutoRefresh()} className="px-3 py-1 rounded bg-zinc-700 text-sm">Cancel auto-refresh</button>
            ) : (
              <button onClick={() => { this.setState({ autoRefreshActive: true, countdown: 3 }); this.startAutoRefresh() }} className="px-3 py-1 rounded bg-zinc-800 text-sm">Enable auto-refresh</button>
            )}
          </div>
        </div>
      )
    }

    return this.props.children
  }
}
