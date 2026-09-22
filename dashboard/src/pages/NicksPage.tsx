import { useCallback, useEffect, useState, type CSSProperties, type ReactNode } from 'react'
import {
  Pencil,
  Activity,
  RefreshCw,
  Sparkles,
  ChevronDown,
  ChevronUp,
  Copy,
  Check,
  RotateCw,
  Zap,
  TrendingUp,
  RotateCcw,
  Gauge,
  ShieldAlert,
  ExternalLink,
  PlayCircle,
} from 'lucide-react'
import { fetchAPI } from '../api/client'
import { useTranslation } from '../i18n/useTranslation'
import type { TranslationKey } from '../i18n/translations'
import { Card, CardHeader, CardTitle, CardDescription, CardContent, CardFooter } from '../components/ui/card'
import { Badge } from '../components/ui/badge'

export interface NickMetrics {
  worker_id: string
  current_concurrency: number
  peak_concurrency: number
  max_concurrency_limit: number
  current_rpm: number
  peak_rpm: number
  rpm_5m_avg: number
  total_requests: number
  successful_requests: number
  failed_requests: number
  success_rate_percent: number
  avg_latency_ms: number
  last_latency_ms: number
  last_request_at: number | null
  last_error: string | null
  last_error_at: number | null
  uptime_seconds: number
}

interface NickWorker {
  profile_id: string | null
  project_id: string
  available: boolean
  in_flight: number
  chat_session: boolean
  flow_key_present: boolean
  /** > 0 while this account is out of the video rotation for MODEL_ACCESS_DENIED. */
  video_denied_for_s?: number
}

interface NickApi {
  id: string
  status: 'ok' | 'need' | 'blocked'
  reason: string
}

interface Nick {
  id: string
  label: string
  project_id: string
  detected_project_id?: string
  proxy_url: string
  proxy_display: string
  has_proxy: boolean
  note: string
  enabled: boolean
  chrome_running: boolean
  pid: number | null
  data_dir: string
  bridge_port: number | null
  worker: NickWorker | null
  connected: boolean
  apis?: NickApi[]
  next?: string | null
  metrics?: NickMetrics
}

interface NickDraft {
  id: string
  label: string
  project_id: string
  proxy_url: string
  note: string
  enabled: boolean
  old_id?: string
}

interface ProxyCheck {
  ok: boolean
  proxy?: string
  egress_ip?: string
  error?: string
}

interface ProxyHealthRecord {
  proxy_url: string
  masked: string
  alive: boolean
  google_clean: boolean
  labs_accessible: boolean
  recaptcha_clean: boolean
  latency_ms: number
  egress_ip: string | null
  status: string
  error: string | null
  last_checked: number
  assigned_accounts: string[]
  in_pool: boolean
  quarantined: boolean
  consecutive_failures: number
}

interface ProxyHealthReport {
  ok: boolean
  summary: {
    total: number
    healthy: number
    warning: number
    dead: number
    blocked: number
    quarantined: number
    checking: boolean
    last_check_time: number
    interval_seconds: number
    daemon_running: boolean
  }
  proxies: ProxyHealthRecord[]
}

type AuthVerdict = 'SIGNED_OUT' | 'ACCOUNT_BLOCKED' | 'RECOVERED' | 'OK' | 'NO_EVIDENCE'

interface NickAuth {
  nick_id: string
  enabled: boolean
  verdict: AuthVerdict
  advice: string
  samples: number
  ok: number
  unauthorized: number
  gen_unauthorized: number
  last_ok_at: string | null
  last_unauthorized_at: string | null
  auth_strikes: number
  parked_for_s: number
  open_incidents: { id: string; message: string; created_at?: number }[]
  needs_attention: boolean
}

interface AuthReport {
  window_s: number
  generated_at: string | null
  netlog_available: boolean
  counts: { total: number; needs_attention: number; signed_out: number; blocked: number; disabled: number }
  nicks: NickAuth[]
}

const VERDICT_STYLE: Record<AuthVerdict, string> = {
  SIGNED_OUT: 'bg-rose-500/10 text-rose-400 border-rose-500/30',
  ACCOUNT_BLOCKED: 'bg-orange-500/10 text-orange-300 border-orange-500/30',
  RECOVERED: 'bg-amber-500/10 text-amber-300 border-amber-500/30',
  OK: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/20',
  NO_EVIDENCE: 'bg-white/5 text-[var(--muted)] border-[var(--border)]',
}

function verdictKey(v: AuthVerdict): TranslationKey {
  return `nicks.auth.verdict.${v}` as TranslationKey
}

function shortTs(ts: string | null): string {
  if (!ts) return '—'
  const d = new Date(ts)
  return Number.isNaN(d.getTime()) ? ts : d.toLocaleTimeString()
}

const EMPTY_DRAFT: NickDraft = {
  id: '',
  label: '',
  project_id: '',
  proxy_url: '',
  note: '',
  enabled: true,
}

const fieldStyle: CSSProperties = {
  background: 'var(--card)',
  color: 'var(--text)',
  border: '1px solid var(--border)',
}

function ActionBtn({
  children,
  onClick,
  disabled,
  tone = 'default',
  title,
}: {
  children: ReactNode
  onClick?: () => void
  disabled?: boolean
  tone?: 'default' | 'primary' | 'danger'
  title?: string
}) {
  const bg = tone === 'primary' ? 'var(--accent-dim)' : 'transparent'
  const color = tone === 'danger' ? 'var(--red)' : 'var(--text)'
  return (
    <button
      type="button"
      translate="no"
      onClick={onClick}
      disabled={disabled}
      title={title}
      className="notranslate text-[11px] px-2.5 py-1 rounded disabled:opacity-40 inline-flex items-center justify-center transition-colors hover:brightness-110 cursor-pointer disabled:cursor-not-allowed"
      style={{ background: bg, color, border: '1px solid var(--border)' }}
    >
      {children}
    </button>
  )
}

function Field({
  label,
  children,
}: {
  label: string
  children: ReactNode
}) {
  return (
    <label className="flex flex-col gap-1 text-[11px]" style={{ color: 'var(--muted)' }}>
      <span className="tracking-wide uppercase font-medium">{label}</span>
      {children}
    </label>
  )
}

function inputClass() {
  return 'w-full text-xs px-2.5 py-1.5 rounded outline-none transition-colors focus:border-[var(--accent)]'
}

export default function NicksPage() {
  const { t } = useTranslation()
  const [nicks, setNicks] = useState<Nick[]>([])
  const [loading, setLoading] = useState(true)
  const [editing, setEditing] = useState<string | 'new' | null>(null)
  const [draft, setDraft] = useState<NickDraft>(EMPTY_DRAFT)
  const [busy, setBusy] = useState<string | null>(null)
  const [message, setMessage] = useState<{ tone: 'ok' | 'err'; text: string } | null>(null)
  const [checks, setChecks] = useState<Record<string, ProxyCheck>>({})
  const [health, setHealth] = useState<ProxyHealthReport | null>(null)
  const [healthChecking, setHealthChecking] = useState(false)
  const [showHealthTable, setShowHealthTable] = useState(false)
  const [copiedProxy, setCopiedProxy] = useState<string | null>(null)
  const [auth, setAuth] = useState<AuthReport | null>(null)
  const [showAllAuth, setShowAllAuth] = useState(false)
  // Saving is tracked apart from `busy`: a save no longer holds the page.
  const [saving, setSaving] = useState<string | null>(null)

  const load = useCallback(() => {
    return fetchAPI<{ accounts: Nick[] }>('/api/accounts')
      .then(r => setNicks(r.accounts))
      .catch(err => setMessage({ tone: 'err', text: t('nicks.error', { msg: String((err as Error).message || err) }) }))
  }, [t])

  const loadHealth = useCallback(() => {
    return fetchAPI<ProxyHealthReport>('/api/accounts/proxy-health')
      .then(res => {
        if (res?.ok) setHealth(res)
      })
      .catch(() => {})
  }, [])

  const loadAuth = useCallback(() => {
    return fetchAPI<AuthReport>('/api/accounts/auth-report')
      .then(res => setAuth(res))
      .catch(() => {})
  }, [])

  useEffect(() => {
    let cancelled = false
    Promise.all([load(), loadHealth(), loadAuth()]).finally(() => {
      if (!cancelled) setLoading(false)
    })
    const id = setInterval(() => {
      if (!cancelled) {
        load()
        loadHealth()
      }
    }, 4000)
    // The auth verdict reads a log tail; every 12s is plenty for a 60m window.
    const authId = setInterval(() => {
      if (!cancelled) loadAuth()
    }, 12000)
    return () => {
      cancelled = true
      clearInterval(id)
      clearInterval(authId)
    }
  }, [load, loadHealth, loadAuth])

  const clusterConcurrency = nicks.reduce((acc, n) => acc + (n.metrics?.current_concurrency || 0), 0)
  const clusterPeakConcurrency = Math.max(0, ...nicks.map(n => n.metrics?.peak_concurrency || 0))
  const clusterMaxLimit = nicks.reduce((acc, n) => acc + (n.metrics?.max_concurrency_limit || 20), 0)
  const clusterRPM = nicks.reduce((acc, n) => acc + (n.metrics?.current_rpm || 0), 0)
  const clusterPeakRPM = nicks.reduce((acc, n) => acc + (n.metrics?.peak_rpm || 0), 0)
  const clusterRequests = nicks.reduce((acc, n) => acc + (n.metrics?.total_requests || 0), 0)

  function flash(tone: 'ok' | 'err', text: string) {
    setMessage({ tone, text })
    setTimeout(() => {
      setMessage(m => (m?.text === text ? null : m))
    }, 6000)
  }

  async function checkAllProxies() {
    setHealthChecking(true)
    try {
      const res = await fetchAPI<ProxyHealthReport>('/api/accounts/proxy-health/check-all', { method: 'POST' })
      if (res?.ok) {
        setHealth(res)
        flash('ok', `${t('nicks.checkAllProxies')}: ${res.summary.healthy}/${res.summary.total} ${t('nicks.healthy')}`)
      }
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setHealthChecking(false)
    }
  }

  async function resetMetrics() {
    try {
      await fetchAPI('/api/accounts/metrics/reset', { method: 'POST' })
      flash('ok', 'Đã đặt lại chỉ số Concurrency và RPM đỉnh!')
      load()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    }
  }

  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === 'Escape' && editing !== null && busy === null) {
        setEditing(null)
      }
    }
    window.addEventListener('keydown', handleKeyDown)
    return () => window.removeEventListener('keydown', handleKeyDown)
  }, [editing, busy])

  async function checkSingleProxy(proxyUrl: string, rowIdx: number) {
    setBusy(`check-row:${rowIdx}`)
    try {
      const res = await fetchAPI<ProxyCheck>('/api/accounts/check-proxy', {
        method: 'POST',
        body: JSON.stringify({ proxy_url: proxyUrl }),
      })
      if (res.ok) {
        flash('ok', `${proxyUrl.split('@').pop() || proxyUrl}: ${t('nicks.status.egress', { ip: res.egress_ip || 'OK' })}`)
      } else {
        flash('err', `${proxyUrl.split('@').pop() || proxyUrl}: ${res.error || t('nicks.status.proxyFail')}`)
      }
      await loadHealth()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }


  async function startEdit(nick: Nick) {
    setBusy(`edit:${nick.id}`)
    try {
      const full = await fetchAPI<Nick>(`/api/accounts/${encodeURIComponent(nick.id)}?reveal=true`)
      setDraft({
        id: full.id,
        old_id: full.id,
        label: full.label,
        project_id: full.project_id,
        proxy_url: full.proxy_url,
        note: full.note,
        enabled: full.enabled,
      })
      setEditing(nick.id)
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  function startNew() {
    setDraft({ ...EMPTY_DRAFT })
    setEditing('new')
  }

  async function save() {
    const id = draft.id.trim()
    if (!id) {
      flash('err', t('nicks.error', { msg: t('nicks.field.id') }))
      return
    }
    const payload = {
      id,
      old_id: editing !== 'new' ? draft.old_id : undefined,
      label: draft.label.trim() || id,
      project_id: draft.project_id.trim(),
      proxy_url: draft.proxy_url.trim(),
      note: draft.note,
      enabled: draft.enabled,
    }
    // Adding a nick verifies a pool proxy, which takes seconds. Holding the
    // modal and the global `busy` lock for that looked like a frozen page, so
    // the dialog closes now and the request finishes in the background.
    setEditing(null)
    setSaving(id)
    flash('ok', t('nicks.savingBg', { id }))
    try {
      await fetchAPI<Nick>('/api/accounts', { method: 'POST', body: JSON.stringify(payload) })
      flash('ok', t('nicks.saved', { id }))
      await load()
      loadHealth()
      loadAuth()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setSaving(null)
    }
  }

  async function applyDetectedProject(nickId: string, projectId: string) {
    setBusy(`sync:${nickId}`)
    try {
      await fetchAPI<Nick>(`/api/accounts/${encodeURIComponent(nickId)}/sync-project`, {
        method: 'POST',
        body: JSON.stringify({ project_id: projectId }),
      })
      flash('ok', t('nicks.saved', { id: nickId }))
      await load()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function rotateProxy(nickId: string) {
    setBusy(`rotate:${nickId}`)
    try {
      const res = await fetchAPI<{ ok: boolean; proxy?: string; error?: string }>(
        `/api/accounts/${encodeURIComponent(nickId)}/rotate-proxy`,
        { method: 'POST' }
      )
      if (res.ok) {
        flash('ok', `${t('nicks.rotateFromPool')}: ${res.proxy || 'OK'}`)
        await load()
        if (editing === nickId) {
          const full = await fetchAPI<Nick>(`/api/accounts/${encodeURIComponent(nickId)}?reveal=true`)
          setDraft(d => ({ ...d, proxy_url: full.proxy_url }))
        }
      } else {
        flash('err', res.error || t('nicks.status.proxyFail'))
      }
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function remove(id: string) {
    if (!window.confirm(t('nicks.confirmDelete', { id }))) return
    setBusy(`del:${id}`)
    try {
      await fetchAPI(`/api/accounts/${encodeURIComponent(id)}`, { method: 'DELETE' })
      if (editing === id) setEditing(null)
      await load()
      loadHealth()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function check(id: string, proxyUrl?: string) {
    setBusy(`check:${id}`)
    try {
      const path = id === 'new'
        ? '/api/accounts/check-proxy'
        : `/api/accounts/${encodeURIComponent(id)}/check-proxy`
      const result = await fetchAPI<ProxyCheck>(path, {
        method: 'POST',
        body: JSON.stringify({ proxy_url: proxyUrl || '' }),
      })
      setChecks(prev => ({ ...prev, [id]: result }))
      if (!result.ok) flash('err', t('nicks.error', { msg: result.error || t('nicks.status.proxyFail') }))
    } catch (err) {
      const text = String((err as Error).message || err)
      setChecks(prev => ({ ...prev, [id]: { ok: false, error: text } }))
      flash('err', t('nicks.error', { msg: text }))
    } finally {
      setBusy(null)
    }
  }

  async function launch(id: string) {
    setBusy(`launch:${id}`)
    try {
      await fetchAPI(`/api/accounts/${encodeURIComponent(id)}/launch`, { method: 'POST' })
      flash('ok', t('nicks.launched', { id }))
      await load()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function stop(id: string) {
    setBusy(`stop:${id}`)
    try {
      await fetchAPI(`/api/accounts/${encodeURIComponent(id)}/stop`, { method: 'POST' })
      flash('ok', t('nicks.stopped', { id }))
      await load()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function focusNick(id: string) {
    setBusy(`focus:${id}`)
    try {
      await fetchAPI(`/api/accounts/${encodeURIComponent(id)}/focus`, { method: 'POST' })
      flash('ok', t('nicks.focused', { id }))
      await load()
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function reloadTab(id: string) {
    setBusy(`reload:${id}`)
    try {
      await fetchAPI(`/api/accounts/${encodeURIComponent(id)}/reload-tab`, { method: 'POST' })
      flash('ok', t('nicks.reloadedTab', { id }))
      await Promise.all([load(), loadAuth()])
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  async function reenable(id: string) {
    setBusy(`enable:${id}`)
    try {
      await fetchAPI(`/api/accounts/${encodeURIComponent(id)}/enable`, { method: 'POST' })
      flash('ok', t('nicks.reenabled', { id }))
      await Promise.all([load(), loadAuth()])
    } catch (err) {
      flash('err', t('nicks.error', { msg: String((err as Error).message || err) }))
    } finally {
      setBusy(null)
    }
  }

  function copyText(text: string, id: string) {
    navigator.clipboard.writeText(text).then(() => {
      setCopiedProxy(id)
      setTimeout(() => setCopiedProxy(null), 2000)
    })
  }

  const activeEditingNick = editing !== 'new' && editing !== null ? nicks.find(n => n.id === editing) : null
  const detectedProjectId = activeEditingNick?.detected_project_id || activeEditingNick?.worker?.project_id

  return (
    <div className="flex flex-col gap-4 max-w-5xl pb-12">
      {/* Top Header */}
      <div className="flex items-start gap-3 justify-between">
        <div>
          <h1 className="m-0 text-lg font-semibold" style={{ color: 'var(--text)' }}>{t('nicks.title')}</h1>
          <p className="text-[11px] mt-1 leading-relaxed" style={{ color: 'var(--muted)' }}>{t('nicks.intro')}</p>
        </div>
        <div className="flex items-center gap-2">
          {saving && (
            <span className="text-[11px] inline-flex items-center gap-1" style={{ color: 'var(--muted)' }}>
              <RefreshCw className="w-3 h-3 animate-spin" />
              {saving}
            </span>
          )}
          <ActionBtn tone="primary" onClick={startNew} disabled={busy !== null}>
            + {t('nicks.add')}
          </ActionBtn>
        </div>
      </div>

      {message && (
        <div
          className="text-xs px-3 py-2 rounded flex items-center gap-2"
          style={{
            background: message.tone === 'err' ? 'rgba(239, 68, 68, 0.1)' : 'rgba(34, 197, 94, 0.1)',
            color: message.tone === 'err' ? 'var(--red)' : 'var(--green)',
            border: `1px solid ${message.tone === 'err' ? 'rgba(239, 68, 68, 0.3)' : 'rgba(34, 197, 94, 0.3)'}`,
          }}
        >
          <span>{message.text}</span>
        </div>
      )}

      {/* 401 / auth watch — the only place that says which nick is signed out */}
      <AuthPanel
        report={auth}
        busy={busy}
        t={t}
        showAll={showAllAuth}
        onToggleAll={() => setShowAllAuth(v => !v)}
        onRefresh={loadAuth}
        onFocus={focusNick}
        onReloadTab={reloadTab}
        onReenable={reenable}
      />

      {/* Proxy Health Monitor Widget */}
      <Card className="py-3 px-4 border border-[var(--border)]">
        <div className="flex flex-wrap items-center justify-between gap-3">
          <div className="flex items-center gap-2">
            <Activity className="w-4 h-4 text-emerald-400 shrink-0" />
            <div>
              <div className="text-xs font-semibold uppercase tracking-wide" style={{ color: 'var(--text)' }}>
                {t('nicks.proxyHealth')}
              </div>
              <div className="text-[11px]" style={{ color: 'var(--muted)' }}>
                {t('nicks.proxyHealthDesc')}
              </div>
            </div>
          </div>

          <div className="flex flex-wrap items-center gap-2 ml-auto">
            {health && (
              <div className="flex items-center gap-1.5 text-[11px] font-mono">
                <span className="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
                  🟢 {health.summary.healthy} {t('nicks.healthy')}
                </span>
                {health.summary.warning > 0 && (
                  <span className="px-2 py-0.5 rounded bg-amber-500/10 text-amber-400 border border-amber-500/20">
                    🟡 {health.summary.warning} {t('nicks.warning')}
                  </span>
                )}
                {(health.summary.dead > 0 || health.summary.blocked > 0) && (
                  <span className="px-2 py-0.5 rounded bg-rose-500/10 text-rose-400 border border-rose-500/20">
                    🔴 {health.summary.dead + health.summary.blocked} {t('nicks.dead')}
                  </span>
                )}
                {health.summary.quarantined > 0 && (
                  <span className="px-2 py-0.5 rounded bg-purple-500/10 text-purple-400 border border-purple-500/20">
                    🛡️ {health.summary.quarantined} {t('nicks.quarantined')}
                  </span>
                )}
              </div>
            )}

            <ActionBtn onClick={checkAllProxies} disabled={healthChecking || busy !== null} tone="default">
              <RefreshCw className={`w-3 h-3 inline mr-1 ${healthChecking ? 'animate-spin' : ''}`} />
              {healthChecking ? t('nicks.checkingAll') : t('nicks.checkAllProxies')}
            </ActionBtn>

            <button
              type="button"
              onClick={() => setShowHealthTable(v => !v)}
              className="text-[11px] px-2 py-1 rounded text-[var(--muted)] hover:text-[var(--text)] transition-colors inline-flex items-center gap-1 cursor-pointer"
            >
              <span>{showHealthTable ? t('nicks.hideHealth') : t('nicks.viewHealth')}</span>
              {showHealthTable ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
            </button>
          </div>
        </div>

        {/* Detailed Proxy Health Table */}
        {showHealthTable && health && (
          <div className="mt-3 pt-3 border-t border-[var(--border)] overflow-x-auto">
            <table className="w-full text-left text-[11px] border-collapse">
              <thead>
                <tr className="text-[var(--muted)] border-b border-[var(--border)]">
                  <th className="pb-1.5 font-medium">{t('nicks.statusCol')}</th>
                  <th className="pb-1.5 font-medium">{t('nicks.proxyCol')}</th>
                  <th className="pb-1.5 font-medium">{t('nicks.egressCol')}</th>
                  <th className="pb-1.5 font-medium">{t('nicks.latency')}</th>
                  <th className="pb-1.5 font-medium">{t('nicks.assignedNick')}</th>
                  <th className="pb-1.5 font-medium">{t('nicks.detailsCol')}</th>
                  <th className="pb-1.5 font-medium text-right">{t('nicks.actionsCol')}</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-[var(--border)] font-mono">
                {health.proxies.map((item, idx) => {
                  const isClean = item.status === 'CLEAN'
                  const isBlocked = item.status.includes('CAPTCHA') || item.status.includes('RECAPTCHA')
                  const isDead = item.status === 'DEAD' || item.status.startsWith('HTTP_') || item.status.includes('FAILED')
                  const isQuarantined = item.quarantined

                  return (
                    <tr key={idx} className="hover:bg-white/[0.02]">
                      <td className="py-2 pr-2">
                        {isQuarantined ? (
                          <span className="px-1.5 py-0.5 rounded text-[10px] bg-purple-500/20 text-purple-300 font-semibold">
                            🛡️ CÁCH LY
                          </span>
                        ) : isClean ? (
                          <span className="px-1.5 py-0.5 rounded text-[10px] bg-emerald-500/20 text-emerald-300 font-semibold">
                            🟢 CLEAN
                          </span>
                        ) : isBlocked ? (
                          <span className="px-1.5 py-0.5 rounded text-[10px] bg-amber-500/20 text-amber-300 font-semibold">
                            ⚠️ CAPTCHA
                          </span>
                        ) : isDead ? (
                          <span className="px-1.5 py-0.5 rounded text-[10px] bg-rose-500/20 text-rose-300 font-semibold">
                            🔴 DEAD
                          </span>
                        ) : (
                          <span className="px-1.5 py-0.5 rounded text-[10px] bg-gray-500/20 text-gray-400">
                            ⚪ {item.status}
                          </span>
                        )}
                      </td>
                      <td className="py-2 pr-2 font-mono text-[var(--text)]">
                        <div className="flex items-center gap-1.5">
                          <span>{item.masked}</span>
                          <button
                            type="button"
                            onClick={() => copyText(item.proxy_url, `p-${idx}`)}
                            title={t('nicks.copyProxy')}
                            className="text-[var(--muted)] hover:text-[var(--text)]"
                          >
                            {copiedProxy === `p-${idx}` ? <Check className="w-3 h-3 text-emerald-400" /> : <Copy className="w-3 h-3" />}
                          </button>
                        </div>
                      </td>
                      <td className="py-2 pr-2 text-[var(--muted)]">
                        {item.egress_ip || '—'}
                      </td>
                      <td className="py-2 pr-2">
                        <span
                          style={{
                            color: item.latency_ms > 0 && item.latency_ms < 600
                              ? 'var(--green)'
                              : item.latency_ms >= 600
                                ? 'var(--yellow)'
                                : 'var(--muted)',
                          }}
                        >
                          {item.latency_ms > 0 ? `${item.latency_ms}ms` : '—'}
                        </span>
                      </td>
                      <td className="py-2 pr-2 text-[var(--text)]">
                        {item.assigned_accounts.length > 0 ? (
                          <div className="flex flex-wrap gap-1">
                            {item.assigned_accounts.map(acc => (
                              <span key={acc} className="px-1.5 py-0.2 rounded text-[10px] bg-blue-500/10 text-blue-400 border border-blue-500/20 font-sans">
                                {acc}
                              </span>
                            ))}
                          </div>
                        ) : (
                          <span className="text-[var(--muted)] font-sans text-[10px]">—</span>
                        )}
                      </td>
                      <td className="py-2 pr-2 text-[var(--muted)] max-w-xs truncate font-sans text-[10px]">
                        {item.error || (isClean ? 'OK (Google + Labs + reCAPTCHA)' : '—')}
                      </td>
                      <td className="py-2 text-right">
                        <ActionBtn
                          onClick={() => checkSingleProxy(item.proxy_url, idx)}
                          disabled={busy !== null}
                          title={t('nicks.checkSingle')}
                        >
                          {busy === `check-row:${idx}` ? (
                            <RefreshCw className="w-3 h-3 animate-spin inline" />
                          ) : (
                            t('nicks.checkSingle')
                          )}
                        </ActionBtn>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        )}
      </Card>

      {/* Real-time Telemetry & Cluster Concurrency Header */}
      {nicks.some(n => n.metrics) && (
        <Card className="p-3 border border-[var(--border)] bg-gradient-to-r from-blue-950/20 via-black/30 to-purple-950/20">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="flex items-center gap-2">
              <Gauge className="w-4 h-4 text-cyan-400 shrink-0" />
              <div>
                <div className="text-xs font-semibold text-[var(--text)] flex items-center gap-1.5">
                  <span>Hạ tầng Tải trọng & RPM Cụm Nick</span>
                  <span className="text-[10px] px-1.5 py-0.2 rounded bg-cyan-500/20 text-cyan-300 font-mono">
                    Max 20/nick · 3 nick = 60 song song
                  </span>
                </div>
                <div className="text-[11px] text-[var(--muted)]">
                  Đo lường độ đồng thời (Concurrency) và tốc độ request/phút (RPM) thực tế
                </div>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-2 text-xs font-mono">
              <div className="px-2.5 py-1 rounded bg-black/40 border border-[var(--border)] flex items-center gap-1.5">
                <Zap className="w-3.5 h-3.5 text-amber-400 shrink-0" />
                <span className="text-[var(--muted)]">Concurrency:</span>
                <span className={`font-bold ${clusterConcurrency > 0 ? 'text-emerald-400' : 'text-[var(--text)]'}`}>
                  {clusterConcurrency}
                </span>
                <span className="text-[var(--muted)]">/ Đỉnh:</span>
                <span className="text-amber-300 font-bold">{clusterPeakConcurrency}</span>
                <span className="text-[var(--muted)] text-[10px]">(Cụm: {clusterMaxLimit})</span>
              </div>

              <div className="px-2.5 py-1 rounded bg-black/40 border border-[var(--border)] flex items-center gap-1.5">
                <TrendingUp className="w-3.5 h-3.5 text-cyan-400 shrink-0" />
                <span className="text-[var(--muted)]">RPM Cụm:</span>
                <span className={`font-bold ${clusterRPM > 0 ? 'text-cyan-400' : 'text-[var(--text)]'}`}>
                  {clusterRPM}
                </span>
                <span className="text-[var(--muted)]">/ Đỉnh:</span>
                <span className="text-purple-300 font-bold">{clusterPeakRPM} req/m</span>
              </div>

              <div className="px-2.5 py-1 rounded bg-black/40 border border-[var(--border)] flex items-center gap-1.5 text-[var(--muted)]">
                <span>Tổng: {clusterRequests} reqs</span>
              </div>

              <ActionBtn onClick={resetMetrics} disabled={busy !== null} tone="default" title="Đặt lại thống kê đỉnh">
                <RotateCcw className="w-3 h-3 inline mr-1" />
                Reset Đỉnh
              </ActionBtn>
            </div>
          </div>
        </Card>
      )}

      {/* Accounts Grid */}
      {loading ? (
        <div className="text-xs" style={{ color: 'var(--muted)' }}>{t('nicks.loading')}</div>
      ) : nicks.length === 0 ? (
        <div className="text-xs" style={{ color: 'var(--muted)' }}>{t('nicks.empty')}</div>
      ) : (
        <div className="grid gap-4" style={{ gridTemplateColumns: 'repeat(auto-fill, minmax(340px, 1fr))' }}>
          {nicks.map(nick => (
            <NickCard
              key={nick.id}
              nick={nick}
              auth={auth?.nicks.find(a => a.nick_id === nick.id)}
              check={checks[nick.id]}
              busy={busy}
              t={t}
              onFocus={() => focusNick(nick.id)}
              onEdit={() => startEdit(nick)}
              onDelete={() => remove(nick.id)}
              onCheck={() => check(nick.id)}
              onLaunch={() => launch(nick.id)}
              onStop={() => stop(nick.id)}
              onApplyProject={applyDetectedProject}
              onRotateProxy={rotateProxy}
            />
          ))}
        </div>
      )}

      {/* Edit / Add Modal Dialog */}
      {editing !== null && (
        <div
          className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/60 backdrop-blur-xs animate-in fade-in"
          onClick={e => {
            if (e.target === e.currentTarget && busy === null) setEditing(null)
          }}
        >
          <div className="w-full max-w-xl max-h-[90vh] overflow-y-auto">
            <Card className="py-5 px-6 shadow-2xl border border-[var(--border)]">
              <CardHeader className="p-0 pb-4">
                <div className="flex items-center justify-between">
                  <CardTitle className="text-base font-semibold flex items-center gap-2" style={{ color: 'var(--text)' }}>
                    <Pencil className="w-4 h-4 text-[var(--accent)]" />
                    {editing === 'new' ? t('nicks.add') : t('nicks.editAccount')}
                  </CardTitle>
                  <button
                    type="button"
                    onClick={() => setEditing(null)}
                    disabled={busy !== null}
                    className="text-xs text-[var(--muted)] hover:text-[var(--text)] px-2 py-1 rounded"
                  >
                    ✕
                  </button>
                </div>
                <CardDescription className="text-xs mt-1">{t('nicks.editHint')}</CardDescription>
              </CardHeader>

              <CardContent className="p-0 flex flex-col gap-3.5">
                <div className="grid gap-3" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(240px, 1fr))' }}>
                  <Field label={t('nicks.field.id')}>
                    <input
                      className={inputClass()}
                      style={fieldStyle}
                      value={draft.id}
                      placeholder={t('nicks.placeholder.id')}
                      onChange={e => setDraft(d => ({ ...d, id: e.target.value }))}
                    />
                  </Field>

                  <Field label={t('nicks.field.label')}>
                    <input
                      className={inputClass()}
                      style={fieldStyle}
                      value={draft.label}
                      placeholder="Display label / Nickname"
                      onChange={e => setDraft(d => ({ ...d, label: e.target.value }))}
                    />
                  </Field>
                </div>

                <Field label={t('nicks.field.project')}>
                  <div className="flex flex-col gap-1.5">
                    <input
                      className={inputClass()}
                      style={fieldStyle}
                      value={draft.project_id}
                      placeholder={t('nicks.placeholder.project')}
                      onChange={e => setDraft(d => ({ ...d, project_id: e.target.value }))}
                    />
                    {detectedProjectId && detectedProjectId !== draft.project_id && (
                      <div className="flex items-center justify-between p-2 rounded text-xs bg-amber-500/10 border border-amber-500/30 text-amber-300">
                        <span className="truncate mr-2 flex items-center gap-1">
                          <Sparkles className="w-3 h-3 text-amber-400 shrink-0" />
                          <span>{t('nicks.detectedProject')}:</span>
                          <code className="font-mono font-semibold">{detectedProjectId}</code>
                        </span>
                        <ActionBtn
                          tone="primary"
                          onClick={() => setDraft(d => ({ ...d, project_id: detectedProjectId }))}
                        >
                          {t('nicks.applyDetectedProject')}
                        </ActionBtn>
                      </div>
                    )}
                  </div>
                </Field>

                <Field label={t('nicks.field.proxy')}>
                  <div className="flex flex-col gap-1.5">
                    <input
                      className={inputClass()}
                      style={fieldStyle}
                      value={draft.proxy_url}
                      placeholder={t('nicks.placeholder.proxy')}
                      autoComplete="off"
                      spellCheck={false}
                      onChange={e => setDraft(d => ({ ...d, proxy_url: e.target.value }))}
                    />
                    <div className="flex items-center gap-2">
                      <ActionBtn
                        onClick={() => check(editing === 'new' ? 'new' : draft.id, draft.proxy_url)}
                        disabled={busy !== null || !draft.proxy_url.trim()}
                      >
                        {busy?.startsWith('check:') ? t('nicks.checking') : t('nicks.checkProxy')}
                      </ActionBtn>
                      {editing !== 'new' && (
                        <ActionBtn
                          onClick={() => rotateProxy(draft.id)}
                          disabled={busy !== null}
                          title="Lấy proxy sạch tiếp theo từ proxy pool"
                        >
                          <RotateCw className="w-3 h-3 inline mr-1" />
                          {t('nicks.rotateFromPool')}
                        </ActionBtn>
                      )}
                      {checks[editing === 'new' ? 'new' : draft.id] && (
                        <span
                          className="text-xs ml-auto font-mono"
                          style={{ color: checks[editing === 'new' ? 'new' : draft.id].ok ? 'var(--green)' : 'var(--red)' }}
                        >
                          {checks[editing === 'new' ? 'new' : draft.id].ok
                            ? t('nicks.status.egress', { ip: checks[editing === 'new' ? 'new' : draft.id].egress_ip || '' })
                            : (checks[editing === 'new' ? 'new' : draft.id].error || t('nicks.status.proxyFail'))}
                        </span>
                      )}
                    </div>
                  </div>
                </Field>

                <Field label={t('nicks.field.note')}>
                  <input
                    className={inputClass()}
                    style={fieldStyle}
                    value={draft.note}
                    placeholder="Ghi chú về nick (ví dụ: Google account 1, Viettel sticky)..."
                    onChange={e => setDraft(d => ({ ...d, note: e.target.value }))}
                  />
                </Field>

                <label className="flex items-center gap-2 text-xs cursor-pointer select-none" style={{ color: 'var(--text)' }}>
                  <input
                    type="checkbox"
                    checked={draft.enabled}
                    onChange={e => setDraft(d => ({ ...d, enabled: e.target.checked }))}
                  />
                  <span>{t('nicks.field.enabled')}</span>
                </label>
              </CardContent>

              <CardFooter className="p-0 pt-5 flex items-center justify-end gap-2 border-t border-[var(--border)] mt-4">
                <ActionBtn onClick={() => setEditing(null)} disabled={busy !== null}>
                  {t('nicks.cancel')}
                </ActionBtn>
                <ActionBtn tone="primary" onClick={save} disabled={busy !== null}>
                  {busy === 'save' ? t('nicks.saving') : t('nicks.save')}
                </ActionBtn>
              </CardFooter>
            </Card>
          </div>
        </div>
      )}
    </div>
  )
}

function AuthPanel({
  report,
  busy,
  t,
  showAll,
  onToggleAll,
  onRefresh,
  onFocus,
  onReloadTab,
  onReenable,
}: {
  report: AuthReport | null
  busy: string | null
  t: (key: TranslationKey, params?: Record<string, string | number>) => string
  showAll: boolean
  onToggleAll: () => void
  onRefresh: () => void
  onFocus: (id: string) => void
  onReloadTab: (id: string) => void
  onReenable: (id: string) => void
}) {
  if (!report) return null
  const locked = busy !== null
  const problems = report.nicks.filter(n => n.needs_attention)
  const rows = showAll ? report.nicks : problems
  const windowMin = Math.round(report.window_s / 60)

  return (
    <Card className="py-3 px-4 border border-[var(--border)]">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <ShieldAlert className={`w-4 h-4 shrink-0 ${problems.length ? 'text-rose-400' : 'text-emerald-400'}`} />
          <div>
            <div className="text-xs font-semibold uppercase tracking-wide" style={{ color: 'var(--text)' }}>
              {t('nicks.auth.title')}
            </div>
            <div className="text-[11px]" style={{ color: 'var(--muted)' }}>
              {t('nicks.auth.desc')} · {t('nicks.auth.window', { n: windowMin })}
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center gap-2 ml-auto text-[11px] font-mono">
          {problems.length > 0 ? (
            <span className="px-2 py-0.5 rounded bg-rose-500/10 text-rose-400 border border-rose-500/20">
              🔴 {t('nicks.auth.needsAttention', { n: problems.length })}
            </span>
          ) : (
            <span className="px-2 py-0.5 rounded bg-emerald-500/10 text-emerald-400 border border-emerald-500/20">
              🟢 {t('nicks.auth.allOk')}
            </span>
          )}
          <ActionBtn onClick={onRefresh} disabled={locked} tone="default">
            <RefreshCw className="w-3 h-3 inline mr-1" />
            {t('nicks.auth.refresh')}
          </ActionBtn>
          <button
            type="button"
            onClick={onToggleAll}
            className="text-[11px] px-2 py-1 rounded text-[var(--muted)] hover:text-[var(--text)] transition-colors inline-flex items-center gap-1 cursor-pointer"
          >
            <span>{showAll ? t('nicks.auth.hideAll') : t('nicks.auth.showAll')}</span>
            {showAll ? <ChevronUp className="w-3 h-3" /> : <ChevronDown className="w-3 h-3" />}
          </button>
        </div>
      </div>

      {rows.length > 0 && (
        <div className="mt-3 flex flex-col gap-1.5">
          {rows.map(row => (
            <div
              key={row.nick_id}
              className="flex flex-wrap items-center gap-2 px-2 py-1.5 rounded border border-[var(--border)] bg-black/20"
            >
              <span className={`px-1.5 py-0.5 rounded text-[10px] border font-medium shrink-0 ${VERDICT_STYLE[row.verdict]}`}>
                {t(verdictKey(row.verdict))}
              </span>
              <span className="text-[11px] font-mono truncate max-w-[220px]" style={{ color: 'var(--text)' }} title={row.nick_id}>
                {row.nick_id}
              </span>
              {!row.enabled && (
                <span className="px-1.5 py-0.5 rounded text-[10px] bg-rose-500/10 text-rose-400 border border-rose-500/30">
                  {t('nicks.auth.disabled')}
                </span>
              )}
              {row.auth_strikes > 0 && (
                <span className="px-1.5 py-0.5 rounded text-[10px] bg-amber-500/10 text-amber-300 border border-amber-500/30">
                  {t('nicks.auth.strikes', { n: row.auth_strikes })}
                </span>
              )}
              <span className="text-[10px] font-mono" style={{ color: 'var(--muted)' }}>
                {row.samples === 0
                  ? t('nicks.auth.noEvidenceHint')
                  : t('nicks.auth.counts', { ok: row.ok, bad: row.unauthorized })}
                {row.last_unauthorized_at ? ` · ${t('nicks.auth.lastBad', { ts: shortTs(row.last_unauthorized_at) })}` : ''}
              </span>

              <div className="flex items-center gap-1.5 ml-auto">
                <ActionBtn tone="primary" onClick={() => onFocus(row.nick_id)} disabled={locked} title={t('nicks.focusHint')}>
                  <ExternalLink className="w-3 h-3 inline mr-1" />
                  {busy === `focus:${row.nick_id}` ? '…' : t('nicks.focus')}
                </ActionBtn>
                <ActionBtn onClick={() => onReloadTab(row.nick_id)} disabled={locked}>
                  <RotateCw className="w-3 h-3 inline mr-1" />
                  {busy === `reload:${row.nick_id}` ? '…' : t('nicks.reloadTab')}
                </ActionBtn>
                {!row.enabled && (
                  <ActionBtn onClick={() => onReenable(row.nick_id)} disabled={locked}>
                    <PlayCircle className="w-3 h-3 inline mr-1" />
                    {busy === `enable:${row.nick_id}` ? '…' : t('nicks.reenable')}
                  </ActionBtn>
                )}
              </div>

              {(row.verdict === 'SIGNED_OUT' || row.verdict === 'ACCOUNT_BLOCKED') && (
                <div className="w-full text-[10px] pl-0.5" style={{ color: 'var(--muted)' }}>
                  {row.advice}
                  {row.open_incidents.length > 0 ? ` · ${row.open_incidents[0].message}` : ''}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </Card>
  )
}

function NickCard({
  nick,
  auth,
  check,
  busy,
  t,
  onFocus,
  onEdit,
  onDelete,
  onCheck,
  onLaunch,
  onStop,
  onApplyProject,
  onRotateProxy,
}: {
  nick: Nick
  auth?: NickAuth
  check?: ProxyCheck
  busy: string | null
  t: (key: TranslationKey, params?: Record<string, string | number>) => string
  onFocus: () => void
  onEdit: () => void
  onDelete: () => void
  onCheck: () => void
  onLaunch: () => void
  onStop: () => void
  onApplyProject: (nickId: string, projectId: string) => void
  onRotateProxy: (nickId: string) => void
}) {
  const locked = busy !== null
  const detectedProjectId = nick.detected_project_id || nick.worker?.project_id
  const needsProject = !nick.project_id && !!detectedProjectId

  return (
    <Card className="py-4 px-4 gap-3 h-full flex flex-col justify-between border border-[var(--border)]">
      <CardHeader className="p-0">
        <div className="flex items-center justify-between">
          <CardTitle className="text-sm font-semibold flex items-center gap-1.5" style={{ color: 'var(--text)' }}>
            <span>{nick.label || nick.id}</span>
            <button
              type="button"
              onClick={onEdit}
              disabled={locked}
              title={t('nicks.editHint')}
              className="text-[var(--muted)] hover:text-[var(--accent)] transition-colors p-0.5 rounded cursor-pointer disabled:opacity-40"
            >
              <Pencil className="w-3 h-3 inline" />
            </button>
          </CardTitle>
          <div className="flex items-center gap-1.5">
            {auth && auth.verdict !== 'OK' && auth.verdict !== 'NO_EVIDENCE' && (
              <span
                title={auth.advice}
                className={`px-1.5 py-0.5 rounded text-[10px] border font-medium ${VERDICT_STYLE[auth.verdict]}`}
              >
                {t(verdictKey(auth.verdict))}
              </span>
            )}
            {!!nick.worker?.video_denied_for_s && (
              <span
                title={t('nicks.videoDenied.hint')}
                className="px-1.5 py-0.5 rounded text-[10px] border font-medium"
                style={{ color: 'var(--red)', borderColor: 'var(--red)' }}
              >
                {t('nicks.videoDenied.badge')}
              </span>
            )}
            <Badge variant="outline">{nick.enabled ? t('nicks.field.enabled') : t('common.dash')}</Badge>
          </div>
        </div>
        <CardDescription className="text-[11px] font-mono truncate" title={nick.id}>
          {nick.id}
        </CardDescription>
      </CardHeader>

      <CardContent className="p-0 flex-1 flex flex-col gap-2">
        <div className="flex flex-col gap-1.5 text-[11px]" style={{ color: 'var(--muted)' }}>
          {/* Status Row */}
          <div className="flex items-center gap-1.5">
            <span
              className="w-1.5 h-1.5 rounded-full"
              style={{ background: nick.connected ? 'var(--green)' : 'var(--muted)' }}
            />
            {nick.connected ? t('nicks.status.connected') : t('nicks.status.disconnected')}
            {nick.worker ? ` · ${t('nicks.status.inFlight', { n: nick.worker.in_flight })}` : ''}
          </div>

          <div className="flex items-center gap-1.5">
            <span
              className="w-1.5 h-1.5 rounded-full"
              style={{ background: nick.chrome_running ? 'var(--green)' : 'var(--muted)' }}
            />
            {nick.chrome_running ? t('nicks.status.chromeOn') : t('nicks.status.chromeOff')}
            {nick.pid ? ` · pid ${nick.pid}` : ''}
          </div>

          {/* Project UUID - Clickable to edit */}
          <div
            onClick={onEdit}
            title={t('nicks.clickToEditProject')}
            className="group/uuid flex items-center justify-between cursor-pointer hover:bg-white/[0.04] p-1 rounded transition-colors -mx-1"
          >
            <div>
              {t('nicks.field.project')}:{' '}
              <span className="font-mono" style={{ color: nick.project_id ? 'var(--text)' : 'var(--muted)' }}>
                {nick.project_id || t('common.dash')}
              </span>
            </div>
            <Pencil className="w-2.5 h-2.5 opacity-0 group-hover/uuid:opacity-70 text-[var(--muted)] shrink-0" />
          </div>

          {/* Quick Apply Detected Project Banner */}
          {needsProject && (
            <div className="flex items-center justify-between p-2 rounded text-xs bg-amber-500/10 border border-amber-500/30 text-amber-300 my-1">
              <div className="flex items-center gap-1.5 truncate mr-2">
                <Sparkles className="w-3.5 h-3.5 shrink-0 text-amber-400" />
                <span className="truncate">
                  {t('nicks.detectedProject')}: <code className="font-mono font-semibold">{detectedProjectId}</code>
                </span>
              </div>
              <ActionBtn
                tone="primary"
                onClick={() => onApplyProject(nick.id, detectedProjectId!)}
                disabled={locked}
              >
                {t('nicks.apply')}
              </ActionBtn>
            </div>
          )}

          {/* Proxy Info */}
          <div className="break-all flex items-center justify-between gap-2">
            <div>
              {t('nicks.field.proxy')}:{' '}
              <span className="font-mono" style={{ color: 'var(--text)' }}>
                {nick.proxy_display || t('nicks.status.noProxy')}
              </span>
            </div>
            <ActionBtn
              onClick={() => onRotateProxy(nick.id)}
              disabled={locked}
              title={t('nicks.rotateFromPool')}
            >
              <RotateCw className="w-3 h-3" />
            </ActionBtn>
          </div>

          {check && (
            <div className="font-mono" style={{ color: check.ok ? 'var(--green)' : 'var(--red)' }}>
              {check.ok
                ? t('nicks.status.egress', { ip: check.egress_ip || '' })
                : (check.error || t('nicks.status.proxyFail'))}
            </div>
          )}

          {nick.note && <div className="text-[11px] italic" style={{ color: 'var(--muted)' }}>{nick.note}</div>}

          {/* Real-time Concurrency & RPM Telemetry Widget */}
          {nick.metrics && (
            <div className="p-2 rounded border border-[var(--border)] bg-black/25 flex flex-col gap-1.5 my-1 font-mono text-[10px]">
              {/* Concurrency Row */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-1 text-[var(--muted)]">
                  <Zap className="w-3 h-3 text-amber-400 shrink-0" />
                  <span>{t('nicks.metrics.concurrency')}:</span>
                </div>
                <div className="flex items-center gap-1.5 font-semibold">
                  <span className={nick.metrics.current_concurrency > 0 ? 'text-emerald-400 font-bold' : 'text-[var(--text)]'}>
                    {nick.metrics.current_concurrency}
                  </span>
                  <span className="text-[var(--muted)]">/ {t('nicks.metrics.peak')}:</span>
                  <span className="text-amber-300 font-bold">{nick.metrics.peak_concurrency}</span>
                  <span className="text-[var(--muted)] text-[9px]">({t('nicks.metrics.limit')}: {nick.metrics.max_concurrency_limit || 20})</span>
                </div>
              </div>

              {/* Concurrency Visual Meter */}
              <div className="w-full bg-white/10 rounded-full h-1.5 overflow-hidden flex">
                <div
                  className="bg-emerald-400 h-full transition-all duration-300"
                  style={{ width: `${Math.min(100, (nick.metrics.current_concurrency / (nick.metrics.max_concurrency_limit || 20)) * 100)}%` }}
                  title={`Đang chạy: ${nick.metrics.current_concurrency}`}
                />
                <div
                  className="bg-amber-400/40 h-full transition-all duration-300"
                  style={{
                    width: `${Math.max(0, Math.min(100, ((nick.metrics.peak_concurrency - nick.metrics.current_concurrency) / (nick.metrics.max_concurrency_limit || 20)) * 100))}%`
                  }}
                  title={`Đỉnh: ${nick.metrics.peak_concurrency}`}
                />
              </div>

              {/* RPM Row */}
              <div className="flex items-center justify-between pt-0.5">
                <div className="flex items-center gap-1 text-[var(--muted)]">
                  <TrendingUp className="w-3 h-3 text-cyan-400 shrink-0" />
                  <span>{t('nicks.metrics.rpm')}:</span>
                </div>
                <div className="flex items-center gap-1.5 font-semibold">
                  <span className={nick.metrics.current_rpm > 0 ? 'text-cyan-400 font-bold' : 'text-[var(--text)]'}>
                    {nick.metrics.current_rpm} req/m
                  </span>
                  <span className="text-[var(--muted)]">/ {t('nicks.metrics.peak')}:</span>
                  <span className="text-purple-300 font-bold">{nick.metrics.peak_rpm}</span>
                </div>
              </div>

              {/* Requests & Latency row */}
              <div className="flex items-center justify-between text-[9px] text-[var(--muted)] pt-1 border-t border-white/5">
                <span>{nick.metrics.total_requests} reqs · {nick.metrics.success_rate_percent}% ok</span>
                <span>{nick.metrics.avg_latency_ms ? `${nick.metrics.avg_latency_ms}ms avg` : '—'}</span>
              </div>
            </div>
          )}

          <ApiChecklist nick={nick} t={t} />
        </div>
      </CardContent>

      <CardFooter className="p-0 pt-3 flex flex-wrap gap-2 border-t border-[var(--border)] mt-2">
        {/* Prominent Edit Button */}
        <ActionBtn onClick={onEdit} disabled={locked} tone="default" title={t('nicks.editHint')}>
          <Pencil className="w-3 h-3 inline mr-1" />
          {t('nicks.edit')}
        </ActionBtn>

        <ActionBtn onClick={onCheck} disabled={locked || !nick.has_proxy}>
          {busy === `check:${nick.id}` ? t('nicks.checking') : t('nicks.checkProxy')}
        </ActionBtn>

        <ActionBtn onClick={onFocus} disabled={locked} tone="default" title={t('nicks.focusHint')}>
          <ExternalLink className="w-3 h-3 inline mr-1" />
          {busy === `focus:${nick.id}` ? '…' : t('nicks.focus')}
        </ActionBtn>

        {nick.chrome_running ? (
          <ActionBtn onClick={onStop} disabled={locked}>
            {busy === `stop:${nick.id}` ? '…' : t('nicks.stop')}
          </ActionBtn>
        ) : (
          <ActionBtn tone="primary" onClick={onLaunch} disabled={locked}>
            {busy === `launch:${nick.id}` ? t('nicks.launching') : t('nicks.launch')}
          </ActionBtn>
        )}

        <ActionBtn tone="danger" onClick={onDelete} disabled={locked}>
          {t('nicks.delete')}
        </ActionBtn>
      </CardFooter>
    </Card>
  )
}

const API_LABEL: Record<string, TranslationKey> = {
  image: 'nicks.api.image',
  upload: 'nicks.api.upload',
  t2v: 'nicks.api.t2v',
  i2v: 'nicks.api.i2v',
  r2v: 'nicks.api.r2v',
  upscale: 'nicks.api.upscale',
  chain: 'nicks.api.chain',
  omni: 'nicks.api.omni',
}

const REASON_TEXT: Record<string, TranslationKey> = {
  ok: 'nicks.reason.ok',
  need_chrome: 'nicks.reason.need_chrome',
  need_proxy: 'nicks.reason.need_proxy',
  need_project: 'nicks.reason.need_project',
  need_extension: 'nicks.reason.need_extension',
  need_ingredients: 'nicks.reason.need_ingredients',
  blocked_unported: 'nicks.reason.blocked_unported',
  blocked_chain: 'nicks.reason.blocked_chain',
  degraded_chain: 'nicks.reason.degraded_chain',
  need_relogin: 'nicks.reason.need_relogin',
  disabled: 'nicks.reason.disabled',
}

function reasonText(
  t: (key: TranslationKey, params?: Record<string, string | number>) => string,
  reason: string,
) {
  const key = REASON_TEXT[reason]
  return key ? t(key) : reason
}

function apiLabel(
  t: (key: TranslationKey, params?: Record<string, string | number>) => string,
  id: string,
) {
  const key = API_LABEL[id]
  return key ? t(key) : id
}

function ApiChecklist({
  nick,
  t,
}: {
  nick: Nick
  t: (key: TranslationKey, params?: Record<string, string | number>) => string
}) {
  const apis = nick.apis || []
  if (apis.length === 0) return null
  const next = nick.next
  return (
    <div className="mt-2 pt-2" style={{ borderTop: '1px solid var(--border)' }}>
      <div className="tracking-wide uppercase mb-1.5" style={{ color: 'var(--muted)' }}>
        {t('nicks.apis')}
      </div>
      {next ? (
        <div className="mb-1.5 font-medium" style={{ color: 'var(--yellow)' }}>
          {t('nicks.next')}: {reasonText(t, next)}
        </div>
      ) : (
        <div className="mb-1.5 font-medium" style={{ color: 'var(--green)' }}>{t('nicks.ready')}</div>
      )}
      <div className="flex flex-col gap-1">
        {apis.map(api => {
          const color = api.status === 'ok'
            ? 'var(--green)'
            : api.status === 'blocked'
              ? 'var(--muted)'
              : 'var(--yellow)'
          return (
            <div key={api.id} className="flex items-start gap-1.5">
              <span className="w-1.5 h-1.5 rounded-full mt-1 shrink-0" style={{ background: color }} />
              <span>
                <span style={{ color: 'var(--text)' }}>{apiLabel(t, api.id)}</span>
                {' · '}
                <span style={{ color }}>{reasonText(t, api.reason)}</span>
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
