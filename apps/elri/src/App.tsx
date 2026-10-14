import { create } from '@bufbuild/protobuf'
import { GetInfoRequestSchema } from '@elri/proto/elri/v1/core/core_pb.ts'
import type { Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import {
  CreateSpaceRequestSchema,
  ListSpacesRequestSchema,
} from '@elri/proto/elri/v1/space/space_pb.ts'
import { type FormEvent, useCallback, useEffect, useState } from 'react'
import { type CoreClients, connect } from './core.ts'

interface Info {
  version: string
  deviceId: string
}

export function App() {
  const [clients, setClients] = useState<CoreClients | null>(null)
  const [info, setInfo] = useState<Info | null>(null)
  const [spaces, setSpaces] = useState<Space[]>([])
  const [name, setName] = useState('')
  const [error, setError] = useState<string | null>(null)

  const fail = useCallback((e: unknown) => setError(e instanceof Error ? e.message : String(e)), [])

  const refresh = useCallback(async (c: CoreClients) => {
    const res = await c.spaces.listSpaces(create(ListSpacesRequestSchema))
    setSpaces(res.spaces)
  }, [])

  useEffect(() => {
    let cancelled = false
    connect()
      .then(async (c) => {
        const res = await c.core.getInfo(create(GetInfoRequestSchema))
        if (cancelled) return
        setClients(c)
        setInfo({ version: res.version, deviceId: res.deviceId })
        await refresh(c)
      })
      .catch(fail)
    return () => {
      cancelled = true
    }
  }, [refresh, fail])

  const handleCreate = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!clients || !name.trim()) return
    try {
      await clients.spaces.createSpace(create(CreateSpaceRequestSchema, { name: name.trim() }))
      setName('')
      await refresh(clients)
    } catch (e) {
      fail(e)
    }
  }

  return (
    <main style={{ fontFamily: 'system-ui', padding: 24, maxWidth: 640 }}>
      <h1>elri</h1>
      {error && <p style={{ color: 'crimson' }}>{error}</p>}
      {info ? (
        <p style={{ color: '#666' }}>
          core {info.version} · device {info.deviceId}
        </p>
      ) : (
        !error && <p>connecting to core…</p>
      )}
      <h2>Spaces</h2>
      {spaces.length === 0 ? (
        <p>No Spaces yet.</p>
      ) : (
        <ul>
          {spaces.map((s) => (
            <li key={s.id}>
              <strong>{s.name}</strong> <code style={{ color: '#888' }}>{s.id}</code>
            </li>
          ))}
        </ul>
      )}
      <form onSubmit={handleCreate} style={{ display: 'flex', gap: 8 }}>
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="New Space name"
          disabled={!clients}
        />
        <button type="submit" disabled={!clients || !name.trim()}>
          Create
        </button>
      </form>
    </main>
  )
}
