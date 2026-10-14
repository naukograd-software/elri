import { create } from '@bufbuild/protobuf'
import {
  ConnectRequestSchema,
  CreateRemoteSpaceRequestSchema,
  DisconnectRequestSchema,
  GetStatusRequestSchema,
  type GetStatusResponse,
  JoinSpaceRequestSchema,
  ListRemoteSpacesRequestSchema,
  type RemoteSpace,
} from '@elri/proto/elri/v1/remote/remote_pb.ts'
import { type FormEvent, useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'

interface Props {
  clients: CoreClients
  onChanged: () => Promise<unknown>
}

export function RemotePanel({ clients, onChanged }: Props) {
  const [status, setStatus] = useState<GetStatusResponse | null>(null)
  const [remoteSpaces, setRemoteSpaces] = useState<RemoteSpace[]>([])
  const [url, setUrl] = useState('ws://127.0.0.1:8710/')
  const [token, setToken] = useState('')
  const [newName, setNewName] = useState('')
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const st = await clients.remote.getStatus(create(GetStatusRequestSchema))
      setStatus(st)
      if (st.configured) {
        const res = await clients.remote.listRemoteSpaces(create(ListRemoteSpacesRequestSchema))
        setRemoteSpaces(res.spaces)
      } else {
        setRemoteSpaces([])
      }
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients])

  useEffect(() => {
    void refresh()
    const timer = setInterval(() => void refresh(), 3000)
    return () => clearInterval(timer)
  }, [refresh])

  const run = async (f: () => Promise<unknown>) => {
    try {
      await f()
      setError(null)
      await refresh()
      await onChanged()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const handleConnect = (ev: FormEvent) => {
    ev.preventDefault()
    void run(async () => {
      await clients.remote.connect(
        create(ConnectRequestSchema, { url: url.trim(), token: token.trim() }),
      )
      setToken('')
    })
  }

  const handleCreate = (ev: FormEvent) => {
    ev.preventDefault()
    if (!newName.trim()) return
    void run(async () => {
      await clients.remote.createRemoteSpace(
        create(CreateRemoteSpaceRequestSchema, { name: newName.trim() }),
      )
      setNewName('')
    })
  }

  return (
    <main className="main" style={{ gridColumn: '2 / span 2' }}>
      <h2>Server & sync</h2>
      {error && <p className="error">{error}</p>}
      {status?.configured ? (
        <div className="section">
          <p>
            <strong>{status.userName}</strong> at <code>{status.url}</code> ·{' '}
            <span className={status.connected ? '' : 'error'}>
              {status.connected ? 'connected' : 'offline'}
            </span>
          </p>
          <table>
            <thead>
              <tr>
                <th>Space</th>
                <th>Cursor</th>
                <th>Pending</th>
                <th>Rejected</th>
              </tr>
            </thead>
            <tbody>
              {status.spaces.map((s) => (
                <tr key={s.spaceId}>
                  <td>
                    <code>{s.spaceId}</code>
                  </td>
                  <td>{s.cursor.toString()}</td>
                  <td>{s.pending}</td>
                  <td className={s.rejected > 0 ? 'error' : ''}>{s.rejected}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <button
            type="button"
            style={{ marginTop: 12 }}
            onClick={() => run(() => clients.remote.disconnect(create(DisconnectRequestSchema)))}
          >
            Disconnect
          </button>
        </div>
      ) : (
        <form onSubmit={handleConnect} className="stack section" style={{ maxWidth: 480 }}>
          <input
            value={url}
            onChange={(e) => setUrl(e.target.value)}
            placeholder="ws://host:port/"
          />
          <input
            value={token}
            onChange={(e) => setToken(e.target.value)}
            placeholder="server token"
            type="password"
          />
          <button type="submit" className="primary" disabled={!url.trim() || !token.trim()}>
            Connect
          </button>
        </form>
      )}

      {status?.configured && (
        <div className="section">
          <h3>Spaces on the server</h3>
          {remoteSpaces.length === 0 && <p className="muted">None visible yet.</p>}
          <ul className="list">
            {remoteSpaces.map((r) => (
              <li key={r.space?.id}>
                <span>
                  {r.space?.name} <code>{r.space?.id}</code>
                </span>
                {r.joined ? (
                  <span className="badge">joined</span>
                ) : (
                  <button
                    type="button"
                    onClick={() =>
                      run(() =>
                        clients.remote.joinSpace(
                          create(JoinSpaceRequestSchema, { spaceId: r.space?.id ?? '' }),
                        ),
                      )
                    }
                  >
                    Join
                  </button>
                )}
              </li>
            ))}
          </ul>
          <form onSubmit={handleCreate} className="row" style={{ marginTop: 8, maxWidth: 480 }}>
            <input
              value={newName}
              onChange={(e) => setNewName(e.target.value)}
              placeholder="New shared Space"
              style={{ flex: 1 }}
            />
            <button type="submit" disabled={!newName.trim()}>
              Create
            </button>
          </form>
        </div>
      )}
    </main>
  )
}
