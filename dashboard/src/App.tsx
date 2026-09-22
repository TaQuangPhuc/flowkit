import { useState, useEffect } from 'react'
import { BrowserRouter, NavLink, Routes, Route, useLocation, useParams, useSearchParams } from 'react-router-dom'
import { LayoutDashboard, FolderOpen, Film, ScrollText, BookOpen, Users, ShieldAlert } from 'lucide-react'
import { TooltipProvider } from '@/components/ui/tooltip'
import { WebSocketProvider } from './api/WebSocketContext'
import { useWebSocketContext } from './api/useWebSocketContext'
import { LanguageProvider } from './i18n/LanguageContext'
import { useTranslation } from './i18n/useTranslation'
import { LANGS, LANG_LABELS, type Lang } from './i18n/translations'
import type { TranslationKey } from './i18n/translations'
import { fetchAPI } from './api/client'
import type { Project } from './types'
import DashboardPage from './pages/DashboardPage'
import ProjectsPage from './pages/ProjectsPage'
import LogsPage from './pages/LogsPage'
import GalleryPage from './pages/GalleryPage'
import GuidePage from './pages/GuidePage'
import NicksPage from './pages/NicksPage'
import IncidentsPage from './pages/IncidentsPage'

const NAV: { to: string; icon: typeof LayoutDashboard; labelKey: TranslationKey; exact: boolean }[] = [
  { to: '/', icon: LayoutDashboard, labelKey: 'nav.dashboard', exact: true },
  { to: '/projects', icon: FolderOpen, labelKey: 'nav.projects', exact: false },
  { to: '/gallery', icon: Film, labelKey: 'nav.gallery', exact: false },
  { to: '/logs', icon: ScrollText, labelKey: 'nav.logs', exact: false },
  { to: '/nicks', icon: Users, labelKey: 'nav.nicks', exact: false },
  { to: '/incidents', icon: ShieldAlert, labelKey: 'nav.incidents', exact: false },
  { to: '/guide', icon: BookOpen, labelKey: 'nav.guide', exact: false },
]

const BREADCRUMB_TAB_KEY: Record<string, TranslationKey> = {
  overview: 'app.breadcrumbTab.overview',
  characters: 'app.breadcrumbTab.characters',
  videos: 'app.breadcrumbTab.videos',
  pipeline: 'app.breadcrumbTab.pipeline',
}

function useClock() {
  const [now, setNow] = useState(() => new Date())
  useEffect(() => {
    const id = setInterval(() => setNow(new Date()), 1000)
    return () => clearInterval(id)
  }, [])
  return now
}

function useBreadcrumbs() {
  const { t } = useTranslation()
  const loc = useLocation()
  const { id } = useParams<{ id?: string }>()
  const [searchParams] = useSearchParams()
  const [projectName, setProjectName] = useState<string | null>(null)

  useEffect(() => {
    if (!id) return
    fetchAPI<Project>(`/api/projects/${id}`).then(p => setProjectName(p.name)).catch(() => setProjectName(null))
  }, [id])

  const crumbs: string[] = []
  if (loc.pathname === '/') crumbs.push(t('app.breadcrumb.dashboard'))
  else if (loc.pathname.startsWith('/projects')) {
    crumbs.push(t('app.breadcrumb.projects'))
    if (id) {
      crumbs.push(projectName ?? '…')
      const tab = searchParams.get('tab')
      const tabKey = tab ? BREADCRUMB_TAB_KEY[tab] : undefined
      if (tabKey) crumbs.push(t(tabKey))
    }
  } else if (loc.pathname.startsWith('/gallery')) crumbs.push(t('app.breadcrumb.gallery'))
  else if (loc.pathname.startsWith('/logs')) crumbs.push(t('app.breadcrumb.logs'))
  else if (loc.pathname.startsWith('/nicks')) crumbs.push(t('app.breadcrumb.nicks'))
  else if (loc.pathname.startsWith('/incidents')) crumbs.push(t('nav.incidents'))
  else if (loc.pathname.startsWith('/guide')) crumbs.push(t('app.breadcrumb.guide'))

  return crumbs
}

function LanguageSwitcher() {
  const { lang, setLang } = useTranslation()
  return (
    <select
      value={lang}
      onChange={e => setLang(e.target.value as Lang)}
      className="text-[10px] px-2 py-1 rounded outline-none w-full"
      style={{ background: 'var(--card)', color: 'var(--text)', border: '1px solid var(--border)' }}
    >
      {LANGS.map(l => (
        <option key={l} value={l}>{LANG_LABELS[l]}</option>
      ))}
    </select>
  )
}

function Sidebar() {
  const { t } = useTranslation()
  const { worker } = useWebSocketContext()
  const [health, setHealth] = useState<{ extension_connected: boolean } | null>(null)

  useEffect(() => {
    fetchAPI<{ extension_connected: boolean }>('/health').then(setHealth).catch(() => setHealth(null))
  }, [])

  return (
    <aside className="w-52 flex-shrink-0 flex flex-col border-r" style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}>
      <div className="px-4 py-4 flex items-center gap-2.5 border-b" style={{ borderColor: 'var(--border)' }}>
        <span className="w-[22px] h-[22px] rounded flex items-center justify-center text-xs font-bold" style={{ background: 'var(--accent)', color: 'var(--bg)' }}>F</span>
        <div className="flex flex-col">
          <span className="text-xs font-bold tracking-widest">{t('app.brandName')}</span>
          <span className="text-[9px] tracking-wide" style={{ color: 'var(--muted)' }}>{t('app.brandTag')}</span>
        </div>
      </div>

      <nav className="flex flex-col gap-0.5 px-2.5 py-3">
        {NAV.map(({ to, icon: Icon, labelKey, exact }) => (
          <NavLink
            key={to}
            to={to}
            end={exact}
            className="flex items-center gap-2.5 px-2.5 py-2 rounded text-xs transition-colors hover:opacity-90"
            style={({ isActive }) => ({
              background: isActive ? 'var(--card)' : 'transparent',
              color: isActive ? 'var(--text)' : 'var(--muted)',
              borderLeft: `2px solid ${isActive ? 'var(--accent)' : 'transparent'}`,
            })}
          >
            <Icon size={13} />
            {t(labelKey)}
          </NavLink>
        ))}
      </nav>

      <div className="mt-auto px-4 py-3.5 border-t flex flex-col gap-2.5" style={{ borderColor: 'var(--border)' }}>
        <LanguageSwitcher />
        <div className="flex items-center justify-between text-[10px] tracking-wide" style={{ color: 'var(--muted)' }}>
          <span>{t('app.workers')}</span>
          <span style={{ color: 'var(--text)' }}>{worker ? `${worker.active}/${worker.active + worker.slots}` : '—'}</span>
        </div>
        <div className="flex items-center gap-1.5 text-[10px]" style={{ color: 'var(--muted)' }}>
          <span
            className="w-1.5 h-1.5 rounded-full"
            style={{ background: health?.extension_connected ? 'var(--green)' : 'var(--red)' }}
          />
          {health?.extension_connected ? t('app.extensionConnected') : health ? t('app.extensionDisconnected') : t('app.extensionChecking')}
        </div>
      </div>
    </aside>
  )
}

function IncidentIndicator() {
  const [summary, setSummary] = useState<{ unresolved_count: number; system_health: string } | null>(null)

  useEffect(() => {
    const check = () => {
      fetchAPI<{ unresolved_count: number; system_health: string }>('/api/system/incidents/summary')
        .then(setSummary)
        .catch(() => {})
    }
    check()
    const id = setInterval(check, 10000)
    return () => clearInterval(id)
  }, [])

  if (!summary) return null
  const hasErrors = summary.unresolved_count > 0

  return (
    <NavLink
      to="/incidents"
      className="flex items-center gap-1.5 px-2.5 py-1 rounded border transition-colors hover:opacity-90"
      style={{
        borderColor: hasErrors ? (summary.system_health === 'CRITICAL' ? 'var(--red)' : 'var(--amber)') : 'var(--border)',
        color: hasErrors ? (summary.system_health === 'CRITICAL' ? 'var(--red)' : 'var(--amber)') : 'var(--muted)'
      }}
    >
      <ShieldAlert size={12} />
      <span>{hasErrors ? `${summary.unresolved_count} sự cố` : 'Hệ thống OK'}</span>
    </NavLink>
  )
}

function Header() {
  const { t } = useTranslation()
  const { isConnected } = useWebSocketContext()
  const crumbs = useBreadcrumbs()
  const clock = useClock()

  return (
    <header className="flex items-center gap-4 px-5 h-13 flex-shrink-0 border-b" style={{ background: 'var(--surface)', borderColor: 'var(--border)' }}>
      <div className="flex items-center gap-2 text-[11px] tracking-wide" style={{ color: 'var(--muted)' }}>
        <span style={{ color: 'var(--accent)' }}>{t('app.breadcrumbRoot')}</span>
        {crumbs.map((c, i) => (
          <span key={i} className="flex items-center gap-2">
            <span>/</span>
            <span style={{ color: i === crumbs.length - 1 ? 'var(--text)' : 'var(--muted)' }}>{c}</span>
          </span>
        ))}
      </div>
      <span className="ml-auto" />
      <div className="flex items-center gap-3 text-[10px]" style={{ color: 'var(--muted)' }}>
        <IncidentIndicator />
        <span className="tracking-wide">{clock.toLocaleTimeString()}</span>
        <span className="flex items-center gap-1.5 px-2.5 py-1 rounded border" style={{ borderColor: 'var(--border)', color: isConnected ? 'var(--green)' : 'var(--red)' }}>
          <span
            className="w-1.5 h-1.5 rounded-full"
            style={{ background: isConnected ? 'var(--green)' : 'var(--red)', animation: isConnected ? 'pulse 2s ease-in-out infinite' : 'none' }}
          />
          {isConnected ? t('app.wsLive') : t('app.wsDisconnected')}
        </span>
      </div>
    </header>
  )
}

function Layout() {
  return (
    <div className="flex h-screen overflow-hidden" style={{ background: 'var(--bg)', color: 'var(--text)' }}>
      <Sidebar />
      <div className="flex flex-col flex-1 overflow-hidden">
        <Header />
        <main className="flex-1 overflow-auto p-5">
          <Routes>
            <Route path="/" element={<DashboardPage />} />
            <Route path="/projects" element={<ProjectsPage />} />
            <Route path="/projects/:id" element={<ProjectsPage />} />
            <Route path="/gallery" element={<GalleryPage />} />
            <Route path="/logs" element={<LogsPage />} />
            <Route path="/nicks" element={<NicksPage />} />
            <Route path="/incidents" element={<IncidentsPage />} />
            <Route path="/guide" element={<GuidePage />} />
          </Routes>
        </main>
      </div>
    </div>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <LanguageProvider>
        <WebSocketProvider>
          <TooltipProvider>
            <Layout />
          </TooltipProvider>
        </WebSocketProvider>
      </LanguageProvider>
    </BrowserRouter>
  )
}
