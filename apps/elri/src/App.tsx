// The Internal Shell: Spaces, interfaces, and the device panels that stay
// reachable whatever a Space-defined interface does. Keyboard first: every
// control is a real button or link, every panel a landmark.

import { create } from '@bufbuild/protobuf'
import { ListChangesRequestSchema } from '@elri/proto/elri/v1/change/change_pb.ts'
import { GetInfoRequestSchema } from '@elri/proto/elri/v1/core/core_pb.ts'
import type { Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import { ListSpacesRequestSchema } from '@elri/proto/elri/v1/space/space_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import { BackupPanel } from './BackupPanel.tsx'
import { ChangesPanel, needsAttention } from './ChangesPanel.tsx'
import { ChatPanel } from './ChatPanel.tsx'
import { type CoreClients, connect, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'
import { GrantsPanel } from './GrantsPanel.tsx'
import { InterfaceList } from './InterfaceList.tsx'
import { InterfaceView } from './InterfaceView.tsx'
import { RemotePanel } from './RemotePanel.tsx'
import { SettingsPanel } from './SettingsPanel.tsx'
import { SpaceList } from './SpaceList.tsx'
import { SpaceView } from './SpaceView.tsx'
import { WorkflowsPanel } from './WorkflowsPanel.tsx'

interface Info {
  version: string
  deviceId: string
}

type Panel =
  | 'space'
  | 'chat'
  | 'changes'
  | 'grants'
  | 'workflows'
  | 'backup'
  | 'remote'
  | 'settings'

const DEVICE_PANELS: { id: Panel; label: string; needsSpace: boolean }[] = [
  { id: 'chat', label: 'Chat', needsSpace: true },
  { id: 'changes', label: 'Changes & rejections', needsSpace: true },
  { id: 'grants', label: 'Scripts & grants', needsSpace: false },
  { id: 'workflows', label: 'Workflows', needsSpace: true },
  { id: 'remote', label: 'Server & sync', needsSpace: false },
  { id: 'backup', label: 'Backups', needsSpace: false },
  { id: 'settings', label: 'Settings', needsSpace: false },
]

export function App() {
  const [clients, setClients] = useState<CoreClients | null>(null)
  const [info, setInfo] = useState<Info | null>(null)
  const [spaces, setSpaces] = useState<Space[]>([])
  const [current, setCurrent] = useState<Space | null>(null)
  const [panel, setPanel] = useState<Panel>('space')
  const [iface, setIface] = useState<string | null>(null)
  const [attention, setAttention] = useState(0)
  const [notices, setNotices] = useState<{ id: number; text: string }[]>([])
  const [error, setError] = useState<string | null>(null)

  const refreshSpaces = useCallback(async (c: CoreClients) => {
    const res = await c.spaces.listSpaces(create(ListSpacesRequestSchema))
    setSpaces(res.spaces)
    return res.spaces
  }, [])

  const countAttention = useCallback(async (c: CoreClients, space: Space | null) => {
    if (!space) {
      setAttention(0)
      return
    }
    try {
      const res = await c.changes.listChanges(
        create(ListChangesRequestSchema, { spaceId: space.id, limit: 200 }),
      )
      setAttention(res.changes.filter(needsAttention).length)
    } catch {
      setAttention(0)
    }
  }, [])

  useEffect(() => {
    let cancelled = false
    connect()
      .then(async (c) => {
        const res = await c.core.getInfo(create(GetInfoRequestSchema))
        if (cancelled) return
        setClients(c)
        setInfo({ version: res.version, deviceId: res.deviceId })
        const list = await refreshSpaces(c)
        if (list.length > 0) setCurrent(list[0])
      })
      .catch((e: unknown) => setError(errorText(e)))
    return () => {
      cancelled = true
    }
  }, [refreshSpaces])

  useEffect(() => {
    if (!clients) return
    void countAttention(clients, current)
    const recount = coalesce(() => void countAttention(clients, current))
    return watch(clients, [], (ev) => {
      if (ev.event.case === 'space' || ev.event.case === 'lagged') void refreshSpaces(clients)
      if (ev.event.case === 'change' || ev.event.case === 'lagged') recount()
      if (ev.event.case === 'notice') {
        const n = ev.event.value
        setNotices((cur) =>
          [...cur, { id: Date.now() + cur.length, text: `${n.source}: ${n.text}` }].slice(-5),
        )
      }
    })
  }, [clients, current, refreshSpaces, countAttention])

  if (error) {
    return (
      <main className="main">
        <h1>elri</h1>
        <p className="error" role="alert">
          {error}
        </p>
      </main>
    )
  }
  if (!clients || !info) {
    return (
      <main className="main">
        <h1>elri</h1>
        <p className="muted">connecting to core…</p>
      </main>
    )
  }

  const devicePanel = (id: Panel) => {
    switch (id) {
      case 'changes':
        return current ? (
          <ChangesPanel key={current.id} clients={clients} spaceId={current.id} />
        ) : null
      case 'grants':
        return <GrantsPanel clients={clients} spaceId={current?.id ?? null} />
      case 'workflows':
        return current ? (
          <WorkflowsPanel key={current.id} clients={clients} spaceId={current.id} />
        ) : null
      case 'backup':
        return <BackupPanel clients={clients} />
      case 'settings':
        return <SettingsPanel clients={clients} spaces={spaces} />
      case 'chat':
        return current ? (
          <ChatPanel
            key={current.id}
            clients={clients}
            spaceId={current.id}
            onOpenSettings={() => setPanel('settings')}
          />
        ) : null
      case 'remote':
        return <RemotePanel clients={clients} onChanged={() => refreshSpaces(clients)} />
      default:
        return null
    }
  }

  return (
    <div className="shell">
      <a className="skip-link" href="#main">
        Skip to content
      </a>
      <aside className="sidebar">
        <h1>elri</h1>
        <SpaceList
          clients={clients}
          spaces={spaces}
          current={panel === 'space' ? current : null}
          onSelect={(s) => {
            setCurrent(s)
            setIface(null)
            setPanel('space')
          }}
          onChanged={() => refreshSpaces(clients)}
        />
        {current && panel === 'space' && (
          <InterfaceList
            clients={clients}
            spaceId={current.id}
            current={iface}
            onSelect={setIface}
          />
        )}
        <nav className="section" aria-label="Device" style={{ marginTop: 24 }}>
          <h3>Device</h3>
          <ul className="list">
            {DEVICE_PANELS.map((p) => (
              <li key={p.id} className={panel === p.id ? 'active' : ''}>
                <button
                  type="button"
                  className="linklike"
                  aria-current={panel === p.id ? 'page' : undefined}
                  disabled={p.needsSpace && !current}
                  onClick={() => setPanel(p.id)}
                >
                  {p.label}
                  {p.id === 'changes' && attention > 0 && (
                    <span className="badge attention">
                      {attention}
                      <span className="visually-hidden"> need attention</span>
                    </span>
                  )}
                </button>
              </li>
            ))}
          </ul>
          <p className="muted" style={{ marginTop: 12 }}>
            core {info.version}
            <br />
            <code>{info.deviceId}</code>
          </p>
        </nav>
        {notices.length > 0 && (
          <section className="section" aria-label="Notices" aria-live="polite">
            <h3>Notices</h3>
            <ul className="stack" style={{ margin: 0, paddingLeft: 16 }}>
              {notices.map((n) => (
                <li key={n.id} className="muted" style={{ fontSize: 12 }}>
                  {n.text}
                </li>
              ))}
            </ul>
            <button type="button" onClick={() => setNotices([])} style={{ marginTop: 8 }}>
              Clear
            </button>
          </section>
        )}
      </aside>
      {panel !== 'space' ? (
        panel === 'remote' ? (
          devicePanel('remote')
        ) : (
          <main id="main" className="main" style={{ gridColumn: '2 / span 2' }}>
            {devicePanel(panel)}
          </main>
        )
      ) : current && iface ? (
        <main id="main" className="main" style={{ gridColumn: '2 / span 2' }}>
          <InterfaceView key={iface} clients={clients} spaceId={current.id} interfaceId={iface} />
        </main>
      ) : current ? (
        <SpaceView key={current.id} clients={clients} space={current} />
      ) : (
        <main id="main" className="main">
          <p className="muted">Create a Space to begin.</p>
        </main>
      )}
    </div>
  )
}
