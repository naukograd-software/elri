import { create } from '@bufbuild/protobuf'
import { GetInfoRequestSchema } from '@elri/proto/elri/v1/core/core_pb.ts'
import type { Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import { ListSpacesRequestSchema } from '@elri/proto/elri/v1/space/space_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import { type CoreClients, connect, errorText } from './core.ts'
import { watch } from './events.ts'
import { RemotePanel } from './RemotePanel.tsx'
import { SpaceList } from './SpaceList.tsx'
import { SpaceView } from './SpaceView.tsx'

interface Info {
  version: string
  deviceId: string
}

export function App() {
  const [clients, setClients] = useState<CoreClients | null>(null)
  const [info, setInfo] = useState<Info | null>(null)
  const [spaces, setSpaces] = useState<Space[]>([])
  const [current, setCurrent] = useState<Space | null>(null)
  const [panel, setPanel] = useState<'space' | 'remote'>('space')
  const [error, setError] = useState<string | null>(null)

  const refreshSpaces = useCallback(async (c: CoreClients) => {
    const res = await c.spaces.listSpaces(create(ListSpacesRequestSchema))
    setSpaces(res.spaces)
    return res.spaces
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
    return watch(clients, [], (ev) => {
      if (ev.event.case === 'space' || ev.event.case === 'lagged') void refreshSpaces(clients)
    })
  }, [clients, refreshSpaces])

  if (error) {
    return (
      <main className="main">
        <h1>elri</h1>
        <p className="error">{error}</p>
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

  return (
    <div className="shell">
      <aside className="sidebar">
        <h1>elri</h1>
        <SpaceList
          clients={clients}
          spaces={spaces}
          current={panel === 'space' ? current : null}
          onSelect={(s) => {
            setCurrent(s)
            setPanel('space')
          }}
          onChanged={() => refreshSpaces(clients)}
        />
        <div className="section" style={{ marginTop: 24 }}>
          <h3>Device</h3>
          <button type="button" onClick={() => setPanel('remote')}>
            Server & sync
          </button>
          <p className="muted" style={{ marginTop: 12 }}>
            core {info.version}
            <br />
            <code>{info.deviceId}</code>
          </p>
        </div>
      </aside>
      {panel === 'remote' ? (
        <RemotePanel clients={clients} onChanged={() => refreshSpaces(clients)} />
      ) : current ? (
        <SpaceView key={current.id} clients={clients} space={current} />
      ) : (
        <main className="main">
          <p className="muted">Create a Space to begin.</p>
        </main>
      )}
    </div>
  )
}
