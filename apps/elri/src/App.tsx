import { create } from '@bufbuild/protobuf'
import { GetInfoRequestSchema } from '@elri/proto/elri/v1/core/core_pb.ts'
import { useEffect, useState } from 'react'
import { connect } from './core.ts'

interface Info {
  version: string
  commit: string
  kind: string
  deviceId: string
}

export function App() {
  const [info, setInfo] = useState<Info | null>(null)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    let cancelled = false
    connect()
      .then(async ({ core }) => {
        const res = await core.getInfo(create(GetInfoRequestSchema))
        if (!cancelled) {
          setInfo({
            version: res.version,
            commit: res.commit,
            kind: String(res.kind),
            deviceId: res.deviceId,
          })
        }
      })
      .catch((e: unknown) => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e))
      })
    return () => {
      cancelled = true
    }
  }, [])

  return (
    <main style={{ fontFamily: 'system-ui', padding: 24 }}>
      <h1>elri</h1>
      {error && <p style={{ color: 'crimson' }}>{error}</p>}
      {info ? (
        <dl>
          <dt>version</dt>
          <dd>{info.version}</dd>
          <dt>commit</dt>
          <dd>{info.commit || '—'}</dd>
          <dt>endpoint</dt>
          <dd>{info.kind}</dd>
          <dt>device</dt>
          <dd>{info.deviceId}</dd>
        </dl>
      ) : (
        !error && <p>connecting to core…</p>
      )}
    </main>
  )
}
