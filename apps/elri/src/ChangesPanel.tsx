// Changes of a Space, newest first: what is saved locally, queued, waiting,
// accepted, rejected or paused. A rejection stays here until the user
// retries or discards it; nothing is lost on its own.

import { create } from '@bufbuild/protobuf'
import {
  DiscardChangeRequestSchema,
  ListChangesRequestSchema,
  RetryChangeRequestSchema,
} from '@elri/proto/elri/v1/change/change_pb.ts'
import { type Change, ChangeStatus } from '@elri/proto/elri/v1/change/domain_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'

interface Props {
  clients: CoreClients
  spaceId: string
}

const STATUS: Record<number, string> = {
  [ChangeStatus.LOCAL]: 'saved locally',
  [ChangeStatus.QUEUED]: 'queued',
  [ChangeStatus.WAITING]: 'waiting',
  [ChangeStatus.ACCEPTED]: 'accepted',
  [ChangeStatus.REJECTED]: 'rejected',
  [ChangeStatus.PAUSED]: 'paused',
}

export function needsAttention(c: Change): boolean {
  return c.status === ChangeStatus.REJECTED || c.status === ChangeStatus.PAUSED
}

export function ChangesPanel({ clients, spaceId }: Props) {
  const [changes, setChanges] = useState<Change[]>([])
  const [onlyAttention, setOnlyAttention] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const res = await clients.changes.listChanges(
        create(ListChangesRequestSchema, { spaceId, limit: 200 }),
      )
      setChanges(res.changes)
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId])

  useEffect(() => {
    void refresh()
    const reload = coalesce(() => void refresh())
    return watch(clients, [spaceId], (ev) => {
      if (ev.event.case === 'change' || ev.event.case === 'lagged') reload()
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

  const shown = onlyAttention ? changes.filter(needsAttention) : changes
  return (
    <section aria-labelledby="changes-heading">
      <div className="row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
        <h2 id="changes-heading" style={{ margin: 0 }}>
          Changes
        </h2>
        <label className="row muted">
          <input
            type="checkbox"
            checked={onlyAttention}
            onChange={(e) => setOnlyAttention(e.target.checked)}
          />
          only rejected and paused
        </label>
      </div>
      <p className="muted">
        A rejected Change keeps its work on this device. Retry sends it again once the cause is
        fixed; discard drops it and lets the Changes that waited on it continue.
      </p>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {shown.length === 0 ? (
        <p className="muted">{onlyAttention ? 'Nothing needs attention.' : 'No changes yet.'}</p>
      ) : (
        <table>
          <thead>
            <tr>
              <th scope="col">When</th>
              <th scope="col">Who</th>
              <th scope="col">Status</th>
              <th scope="col">Objects</th>
              <th scope="col">Why</th>
              <th scope="col">
                <span className="visually-hidden">Actions</span>
              </th>
            </tr>
          </thead>
          <tbody>
            {shown.map((c) => (
              <tr key={c.id} className={needsAttention(c) ? 'attention' : undefined}>
                <td className="muted">{new Date(Number(c.createdAtMs)).toLocaleString()}</td>
                <td className="muted">{c.actor || 'user'}</td>
                <td>{STATUS[c.status] ?? '?'}</td>
                <td className="muted">{c.parts.length}</td>
                <td className={needsAttention(c) ? 'error' : 'muted'}>{c.rejection}</td>
                <td>
                  {needsAttention(c) && (
                    <div className="row">
                      <button
                        type="button"
                        onClick={() =>
                          act(() =>
                            clients.changes.retryChange(
                              create(RetryChangeRequestSchema, { spaceId, id: c.id }),
                            ),
                          )
                        }
                      >
                        Retry
                      </button>
                      <button
                        type="button"
                        className="danger"
                        onClick={() =>
                          act(() =>
                            clients.changes.discardChange(
                              create(DiscardChangeRequestSchema, { spaceId, id: c.id }),
                            ),
                          )
                        }
                      >
                        Discard
                      </button>
                    </div>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </section>
  )
}
