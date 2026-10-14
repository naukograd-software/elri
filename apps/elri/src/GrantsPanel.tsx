// Executable code on this device: the scripts of the current Space, the
// grants that let a version run here, and the device policy. Code never
// runs because it appeared in a shared Space; it runs because this device
// said so, and the consent can be taken back.

import { create } from '@bufbuild/protobuf'
import type { Grant, Script } from '@elri/proto/elri/v1/script/domain_pb.ts'
import {
  GetPolicyRequestSchema,
  GrantScriptRequestSchema,
  ListGrantsRequestSchema,
  ListScriptsRequestSchema,
  RevokeGrantRequestSchema,
  SetPolicyRequestSchema,
} from '@elri/proto/elri/v1/script/script_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'

interface Props {
  clients: CoreClients
  spaceId: string | null
}

export function GrantsPanel({ clients, spaceId }: Props) {
  const [scripts, setScripts] = useState<Script[]>([])
  const [grants, setGrants] = useState<Grant[]>([])
  const [autoBenign, setAutoBenign] = useState(true)
  const [maxConcurrent, setMaxConcurrent] = useState(4)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const policy = await clients.grants.getPolicy(create(GetPolicyRequestSchema))
      setAutoBenign(policy.autoGrantBenign)
      setMaxConcurrent(policy.maxConcurrentScripts)
      const g = await clients.grants.listGrants(
        create(ListGrantsRequestSchema, { spaceId: spaceId ?? '', includeRevoked: false }),
      )
      setGrants(g.grants)
      if (spaceId) {
        const s = await clients.scripts.listScripts(create(ListScriptsRequestSchema, { spaceId }))
        setScripts(s.scripts)
      } else {
        setScripts([])
      }
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId])

  useEffect(() => {
    void refresh()
    const reload = coalesce(() => void refresh())
    return watch(clients, spaceId ? [spaceId] : [], (ev) => {
      if (ev.event.case === 'object' || ev.event.case === 'lagged') reload()
    })
  }, [clients, spaceId, refresh])

  const act = async (f: () => Promise<unknown>) => {
    try {
      await f()
      setError(null)
      await refresh()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const grantFor = (s: Script) => grants.find((g) => g.scriptId === s.id && g.version === s.version)

  return (
    <section aria-labelledby="grants-heading" className="stack">
      <h2 id="grants-heading" style={{ margin: 0 }}>
        Scripts & grants
      </h2>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <div className="section">
        <h3>Device policy</h3>
        <label className="row">
          <input
            type="checkbox"
            checked={autoBenign}
            onChange={(e) =>
              act(() =>
                clients.grants.setPolicy(
                  create(SetPolicyRequestSchema, {
                    autoGrantBenign: e.target.checked,
                    maxConcurrentScripts: 0,
                  }),
                ),
              )
            }
          />
          grant benign scripts without asking (they touch only their own Space)
        </label>
        <label className="row" style={{ marginTop: 8 }}>
          script runs at once
          <input
            type="number"
            min={1}
            max={64}
            value={maxConcurrent}
            style={{ width: 80 }}
            onChange={(e) => setMaxConcurrent(Number(e.target.value))}
            onBlur={() =>
              act(() =>
                clients.grants.setPolicy(
                  create(SetPolicyRequestSchema, {
                    autoGrantBenign: autoBenign,
                    maxConcurrentScripts: Math.max(1, Math.min(64, maxConcurrent)),
                  }),
                ),
              )
            }
          />
        </label>
      </div>
      <div className="section">
        <h3>Scripts of this Space</h3>
        {!spaceId && <p className="muted">Select a Space to see its scripts.</p>}
        {spaceId && scripts.length === 0 && <p className="muted">No scripts here.</p>}
        {scripts.length > 0 && (
          <table>
            <thead>
              <tr>
                <th scope="col">Script</th>
                <th scope="col">Needs</th>
                <th scope="col">This version</th>
                <th scope="col">
                  <span className="visually-hidden">Actions</span>
                </th>
              </tr>
            </thead>
            <tbody>
              {scripts.map((s) => {
                const g = grantFor(s)
                const m = s.manifest
                const needs = [
                  ...(m?.reads ?? []).map((x) => `reads Space ${x}`),
                  ...(m?.writes ?? []).map((x) => `writes Space ${x}`),
                  ...(m?.extensions ?? []).map((x) => `calls extension ${x}`),
                  ...(m?.http ? ['calls out over HTTP'] : []),
                  ...(m?.ai ? ['uses an AI provider'] : []),
                ]
                return (
                  <tr key={s.id}>
                    <td>
                      {s.name}
                      <br />
                      <code>{s.id}</code>
                      <br />
                      <span className="muted">version </span>
                      <code>{s.version}</code>
                    </td>
                    <td className="muted">
                      {needs.length ? (
                        <ul style={{ margin: 0, paddingLeft: 16 }}>
                          {needs.map((n) => (
                            <li key={n}>{n}</li>
                          ))}
                        </ul>
                      ) : (
                        'only its own Space'
                      )}
                    </td>
                    <td>
                      {g ? (
                        <span className="badge">granted{g.benign ? ' (benign)' : ''}</span>
                      ) : (
                        <span className="badge">not granted</span>
                      )}
                    </td>
                    <td>
                      {g ? (
                        <button
                          type="button"
                          className="danger"
                          onClick={() =>
                            act(() =>
                              clients.grants.revokeGrant(
                                create(RevokeGrantRequestSchema, { id: g.id }),
                              ),
                            )
                          }
                        >
                          Revoke
                        </button>
                      ) : (
                        <button
                          type="button"
                          onClick={() =>
                            act(() =>
                              clients.grants.grantScript(
                                create(GrantScriptRequestSchema, {
                                  spaceId: s.spaceId,
                                  scriptId: s.id,
                                  expectedVersion: s.version,
                                }),
                              ),
                            )
                          }
                        >
                          Allow this version
                        </button>
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </table>
        )}
      </div>
      <div className="section">
        <h3>Grants on this device</h3>
        {grants.length === 0 ? (
          <p className="muted">None.</p>
        ) : (
          <ul className="list">
            {grants.map((g) => (
              <li key={g.id}>
                <span>
                  script <code>{g.scriptId}</code> version <code>{g.version.slice(0, 12)}</code>
                  {g.benign && <span className="badge">benign</span>}
                </span>
                <button
                  type="button"
                  className="danger"
                  onClick={() =>
                    act(() =>
                      clients.grants.revokeGrant(create(RevokeGrantRequestSchema, { id: g.id })),
                    )
                  }
                >
                  Revoke
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  )
}
