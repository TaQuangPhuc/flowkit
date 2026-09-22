import { useState, useEffect } from 'react'
import {
  CheckCircle2,
  RefreshCw,
  ShieldAlert,
  Activity,
  Server,
  Film,
  Camera,
  Layers,
  Users,
  Radio,
  Check
} from 'lucide-react'
import { fetchAPI } from '../api/client'

interface Incident {
  id: string
  module: string
  job_id: string | null
  sub_id: string | null
  severity: 'INFO' | 'WARNING' | 'CRITICAL' | 'HEALED'
  error_code: string
  message: string
  root_cause: string
  action_taken: string
  status: 'OPEN' | 'AUTO_HEALING' | 'RESOLVED' | 'FAILED'
  retry_count: number
  created_at: number
  resolved_at: number | null
}

interface IncidentSummary {
  ok: boolean
  system_health: 'HEALTHY' | 'WARNING' | 'CRITICAL'
  unresolved_count: number
  critical_count: number
  healed_24h_count: number
  modules: {
    [key: string]: {
      status: 'HEALTHY' | 'WARNING' | 'CRITICAL'
      open_incidents: number
    }
  }
  timestamp: number
}

export default function IncidentsPage() {
  const [summary, setSummary] = useState<IncidentSummary | null>(null)
  const [incidents, setIncidents] = useState<Incident[]>([])
  const [filterModule, setFilterModule] = useState<string>('all')
  const [filterStatus, setFilterStatus] = useState<string>('all')
  const [isLoading, setIsLoading] = useState<boolean>(true)
  const [isSweeping, setIsSweeping] = useState<boolean>(false)

  const loadData = async () => {
    try {
      const [sumRes, listRes] = await Promise.all([
        fetchAPI<IncidentSummary>('/api/system/incidents/summary'),
        fetchAPI<{ ok: boolean; incidents: Incident[] }>('/api/system/incidents?limit=100')
      ])
      setSummary(sumRes)
      setIncidents(listRes.incidents || [])
    } catch (err) {
      console.error('Failed to load incidents data:', err)
    } finally {
      setIsLoading(false)
    }
  }

  useEffect(() => {
    loadData()
    const timer = setInterval(loadData, 10000)
    return () => clearInterval(timer)
  }, [])

  const handleManualSweep = async () => {
    setIsSweeping(true)
    try {
      await fetchAPI('/api/system/incidents/sweep', { method: 'POST' })
      await loadData()
    } catch (err) {
      console.error('Sweep failed:', err)
    } finally {
      setIsSweeping(false)
    }
  }

  const handleResolve = async (id: string) => {
    try {
      await fetchAPI(`/api/system/incidents/${id}/resolve`, {
        method: 'POST',
        body: JSON.stringify({ action_taken: 'ADMIN_MANUAL_RESOLVE' })
      })
      await loadData()
    } catch (err) {
      console.error('Resolve failed:', err)
    }
  }

  const filteredIncidents = incidents.filter(inc => {
    if (filterModule !== 'all' && inc.module !== filterModule) return false
    if (filterStatus === 'unresolved' && !['OPEN', 'AUTO_HEALING'].includes(inc.status)) return false
    if (filterStatus === 'healed' && inc.severity !== 'HEALED') return false
    if (filterStatus === 'critical' && inc.severity !== 'CRITICAL') return false
    return true
  })

  const moduleIcons: Record<string, any> = {
    lookbook: Camera,
    tvc: Film,
    batch: Layers,
    worker: Users,
    proxy: Radio
  }

  const moduleNames: Record<string, string> = {
    lookbook: 'Lookbook Thời Trang',
    tvc: 'Auto TVC Studio',
    batch: 'Hàng Đợi Batch Queue',
    worker: 'Chrome Workers',
    proxy: 'Residential Proxy Pool'
  }

  return (
    <div className="max-w-6xl mx-auto space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-xl font-bold flex items-center gap-2.5">
            <ShieldAlert className="w-5 h-5 text-amber-500" />
            Trung Tâm Giám Sát & Tự Phục Hồi (Central Watchdog)
          </h1>
          <p className="text-xs text-muted-foreground mt-0.5">
            Tự động quét định kỳ 30s toàn bộ 5 module, tự phục hồi lỗi ngầm (Self-Healing) và lưu vết SQLite bền vững.
          </p>
        </div>

        <button
          onClick={handleManualSweep}
          disabled={isSweeping}
          className="flex items-center gap-2 px-3 py-1.5 rounded text-xs font-medium bg-amber-500/10 text-amber-500 hover:bg-amber-500/20 border border-amber-500/30 transition-all disabled:opacity-50"
        >
          <RefreshCw className={`w-3.5 h-3.5 ${isSweeping ? 'animate-spin' : ''}`} />
          {isSweeping ? 'Đang Quét Hệ Thống...' : 'Quét & Tự Sửa Ngay (Sweep)'}
        </button>
      </div>

      {/* 5 Module Health Cards */}
      <div className="grid grid-cols-5 gap-3">
        {['lookbook', 'tvc', 'batch', 'worker', 'proxy'].map(modKey => {
          const Icon = moduleIcons[modKey] || Server
          const modData = summary?.modules?.[modKey]
          const isHealthy = modData?.status === 'HEALTHY'
          const isCrit = modData?.status === 'CRITICAL'
          const openCount = modData?.open_incidents || 0

          return (
            <div
              key={modKey}
              onClick={() => setFilterModule(filterModule === modKey ? 'all' : modKey)}
              className={`p-3 rounded-lg border cursor-pointer transition-all ${
                filterModule === modKey ? 'ring-2 ring-accent' : ''
              }`}
              style={{
                background: 'var(--surface)',
                borderColor: isCrit ? 'var(--red)' : isHealthy ? 'var(--border)' : 'var(--amber)'
              }}
            >
              <div className="flex items-center justify-between mb-2">
                <Icon className="w-4 h-4 text-muted-foreground" />
                <span
                  className={`text-[9px] px-1.5 py-0.5 rounded font-semibold uppercase ${
                    isCrit
                      ? 'bg-red-500/10 text-red-500 border border-red-500/20'
                      : isHealthy
                      ? 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/20'
                      : 'bg-amber-500/10 text-amber-500 border border-amber-500/20'
                  }`}
                >
                  {isHealthy ? 'Khỏe Mạnh' : isCrit ? 'Nghiêm Trọng' : 'Cảnh Báo'}
                </span>
              </div>
              <div className="text-xs font-semibold truncate">{moduleNames[modKey]}</div>
              <div className="text-[10px] text-muted-foreground mt-1">
                {openCount > 0 ? `${openCount} sự cố đang mở` : 'Hoạt động ổn định'}
              </div>
            </div>
          )
        })}
      </div>

      {/* Summary KPI Banner */}
      <div
        className="p-4 rounded-lg border flex items-center justify-between"
        style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}
      >
        <div className="flex items-center gap-6">
          <div>
            <div className="text-[10px] text-muted-foreground uppercase font-bold tracking-wider">Trạng Thái Toàn Hệ Thống</div>
            <div className="text-sm font-bold mt-0.5 flex items-center gap-2">
              <span
                className="w-2 h-2 rounded-full animate-pulse"
                style={{
                  background:
                    summary?.system_health === 'HEALTHY'
                      ? 'var(--green)'
                      : summary?.system_health === 'CRITICAL'
                      ? 'var(--red)'
                      : 'var(--amber)'
                }}
              />
              {summary?.system_health === 'HEALTHY'
                ? 'TẤT CẢ MODULE KHỎE MẠNH'
                : summary?.system_health === 'CRITICAL'
                ? 'CÓ SỰ CỐ CẦN CHÚ Ý'
                : 'CẢNH BÁO TÁC VỤ TREO'}
            </div>
          </div>

          <div className="h-8 w-px bg-border" />

          <div>
            <div className="text-[10px] text-muted-foreground uppercase font-bold tracking-wider">Sự Cố Đang Mở</div>
            <div className="text-sm font-bold mt-0.5 text-amber-500">{summary?.unresolved_count || 0} sự cố</div>
          </div>

          <div className="h-8 w-px bg-border" />

          <div>
            <div className="text-[10px] text-muted-foreground uppercase font-bold tracking-wider">Đã Tự Sửa 24h</div>
            <div className="text-sm font-bold mt-0.5 text-emerald-500">{summary?.healed_24h_count || 0} lần thành công</div>
          </div>
        </div>

        {/* Filters */}
        <div className="flex items-center gap-2">
          {['all', 'unresolved', 'healed', 'critical'].map(st => (
            <button
              key={st}
              onClick={() => setFilterStatus(st)}
              className={`px-2.5 py-1 rounded text-xs transition-colors ${
                filterStatus === st ? 'bg-card border text-text' : 'text-muted hover:text-text'
              }`}
              style={{ borderColor: filterStatus === st ? 'var(--accent)' : 'transparent' }}
            >
              {st === 'all'
                ? 'Tất cả'
                : st === 'unresolved'
                ? 'Chưa giải quyết'
                : st === 'healed'
                ? 'Đã tự sửa'
                : 'Nghiêm trọng'}
            </button>
          ))}
        </div>
      </div>

      {/* Incident Timeline List */}
      <div className="space-y-2.5">
        <h2 className="text-xs font-semibold text-muted-foreground uppercase tracking-wider flex items-center gap-2">
          <Activity className="w-3.5 h-3.5" />
          Nhật Ký Sự Cố & Hành Động Tự Phục Hồi ({filteredIncidents.length})
        </h2>

        {isLoading ? (
          <div className="py-12 text-center text-xs text-muted-foreground">Đang tải nhật ký sự cố...</div>
        ) : filteredIncidents.length === 0 ? (
          <div className="py-12 text-center border rounded-lg" style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}>
            <CheckCircle2 className="w-8 h-8 text-emerald-500 mx-auto mb-2 opacity-80" />
            <div className="text-xs font-semibold">Không có sự cố nào phù hợp bộ lọc</div>
            <p className="text-[11px] text-muted-foreground mt-0.5">Hệ thống đang chạy trơn tru với 0 lỗi phát sinh.</p>
          </div>
        ) : (
          filteredIncidents.map(inc => {
            const isHealed = inc.severity === 'HEALED'
            const isCrit = inc.severity === 'CRITICAL'
            const isOpen = ['OPEN', 'AUTO_HEALING'].includes(inc.status)

            return (
              <div
                key={inc.id}
                className="p-3.5 rounded-lg border transition-all"
                style={{
                  background: 'var(--surface)',
                  borderColor: isCrit ? 'rgba(239, 68, 68, 0.4)' : isHealed ? 'rgba(16, 185, 129, 0.3)' : 'var(--border)'
                }}
              >
                <div className="flex items-start justify-between gap-4">
                  <div className="space-y-1 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span
                        className={`text-[9px] px-1.5 py-0.5 rounded font-bold uppercase ${
                          isCrit
                            ? 'bg-red-500/10 text-red-500 border border-red-500/30'
                            : isHealed
                            ? 'bg-emerald-500/10 text-emerald-500 border border-emerald-500/30'
                            : 'bg-amber-500/10 text-amber-500 border border-amber-500/30'
                        }`}
                      >
                        {inc.severity}
                      </span>

                      <span className="text-[10px] px-1.5 py-0.5 rounded bg-card border border-border font-mono">
                        {inc.module.toUpperCase()}
                      </span>

                      {inc.job_id && (
                        <span className="text-[10px] px-1.5 py-0.5 rounded bg-card border border-border font-mono text-muted-foreground">
                          Job: #{inc.job_id.slice(0, 8)}
                        </span>
                      )}

                      <span className="text-[10px] font-mono text-muted-foreground">
                        {inc.error_code}
                      </span>

                      <span className="text-[10px] text-muted-foreground ml-auto">
                        {new Date(inc.created_at * 1000).toLocaleTimeString()} · {new Date(inc.created_at * 1000).toLocaleDateString()}
                      </span>
                    </div>

                    <div className="text-xs font-medium text-text pt-0.5">{inc.message}</div>

                    {inc.root_cause && (
                      <div className="text-[11px] text-muted-foreground font-mono bg-card/60 p-2 rounded border border-border/50 mt-1.5">
                        <span className="font-semibold text-text">Nguyên nhân: </span>
                        {inc.root_cause}
                      </div>
                    )}

                    {inc.action_taken && (
                      <div className="text-[11px] text-emerald-400 font-medium flex items-center gap-1.5 pt-1">
                        <Check className="w-3 h-3" />
                        Đã tự phục hồi: {inc.action_taken}
                      </div>
                    )}
                  </div>

                  {isOpen && (
                    <button
                      onClick={() => handleResolve(inc.id)}
                      className="px-2 py-1 rounded text-[10px] bg-card border border-border hover:border-accent text-text transition-colors flex-shrink-0"
                    >
                      Đóng sự cố
                    </button>
                  )}
                </div>
              </div>
            )
          })
        )}
      </div>
    </div>
  )
}
