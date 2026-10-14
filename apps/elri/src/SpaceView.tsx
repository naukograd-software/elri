import { create } from '@bufbuild/protobuf'
import { type Object$ as Obj, ObjectKind } from '@elri/proto/elri/v1/object/domain_pb.ts'
import {
  CreateObjectRequestSchema,
  ListObjectsRequestSchema,
} from '@elri/proto/elri/v1/object/object_pb.ts'
import type { Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import { type FormEvent, useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'
import { ObjectInspector } from './ObjectInspector.tsx'
import { textOf } from './values.ts'

interface Props {
  clients: CoreClients
  space: Space
}

const KIND_LABEL: Record<number, string> = {
  [ObjectKind.ENTITY]: 'entity',
  [ObjectKind.RELATION]: 'relation',
  [ObjectKind.LAYER]: 'layer',
}

function title(o: Obj): string {
  const t = o.fields.title ?? o.fields.name
  return t ? textOf(t) : o.id.slice(0, 8)
}

export function SpaceView({ clients, space }: Props) {
  const [objects, setObjects] = useState<Obj[]>([])
  const [selected, setSelected] = useState<Obj | null>(null)
  const [newTitle, setNewTitle] = useState('')
  const [newType, setNewType] = useState('')
  const [showDeleted, setShowDeleted] = useState(false)
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const res = await clients.objects.listObjects(
        create(ListObjectsRequestSchema, {
          spaceId: space.id,
          includeDeleted: showDeleted,
          limit: 200,
        }),
      )
      setObjects(res.objects)
      setSelected((cur) => (cur ? (res.objects.find((o) => o.id === cur.id) ?? null) : null))
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, space.id, showDeleted])

  useEffect(() => {
    void refresh()
    const reload = coalesce(() => void refresh())
    return watch(clients, [space.id], (ev) => {
      if (ev.event.case === 'object' || ev.event.case === 'lagged') reload()
    })
  }, [clients, space.id, refresh])

  const handleCreate = async (ev: FormEvent) => {
    ev.preventDefault()
    try {
      const res = await clients.objects.createObject(
        create(CreateObjectRequestSchema, {
          spaceId: space.id,
          kind: ObjectKind.ENTITY,
          types: newType.trim() ? [newType.trim()] : [],
          fields: newTitle.trim()
            ? { title: { value: { case: 'string', value: newTitle.trim() } } }
            : {},
        }),
      )
      setNewTitle('')
      await refresh()
      if (res.object) setSelected(res.object)
    } catch (e) {
      setError(errorText(e))
    }
  }

  return (
    <>
      <main className="main">
        <div className="row" style={{ justifyContent: 'space-between', marginBottom: 16 }}>
          <h2 style={{ margin: 0 }}>{space.name}</h2>
          <label className="row muted">
            <input
              type="checkbox"
              checked={showDeleted}
              onChange={(e) => setShowDeleted(e.target.checked)}
            />
            show deleted
          </label>
        </div>
        <form onSubmit={handleCreate} className="row" style={{ marginBottom: 16 }}>
          <input
            value={newTitle}
            onChange={(e) => setNewTitle(e.target.value)}
            placeholder="Quick capture: title"
            style={{ flex: 1 }}
          />
          <input
            value={newType}
            onChange={(e) => setNewType(e.target.value)}
            placeholder="type (optional)"
            style={{ width: 140 }}
          />
          <button type="submit" className="primary">
            Create
          </button>
        </form>
        {error && <p className="error">{error}</p>}
        {objects.length === 0 ? (
          <p className="muted">Nothing here yet.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th>Title</th>
                <th>Kind</th>
                <th>Types</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {objects.map((o) => (
                <tr
                  key={o.id}
                  onClick={() => setSelected(o)}
                  style={{
                    cursor: 'pointer',
                    opacity: o.deletedAtMs !== undefined ? 0.5 : 1,
                    background:
                      selected?.id === o.id
                        ? 'color-mix(in srgb, var(--accent) 12%, transparent)'
                        : undefined,
                  }}
                >
                  <td>{title(o)}</td>
                  <td className="muted">{KIND_LABEL[o.kind] ?? '?'}</td>
                  <td className="muted">{o.types.join(', ')}</td>
                  <td className="muted">{new Date(Number(o.updatedAtMs)).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </main>
      <aside className="inspector">
        {selected ? (
          <ObjectInspector
            key={`${selected.id}:${selected.updatedAtMs}`}
            clients={clients}
            object={selected}
            onChanged={refresh}
          />
        ) : (
          <p className="muted">Select an object to inspect it.</p>
        )}
      </aside>
    </>
  )
}
