// A Space: its Objects as a list with quick capture and search, the
// Contexts that narrow the list, and its definitions. Selecting an Object
// opens the universal inspector.

import { create } from '@bufbuild/protobuf'
import type { Context } from '@elri/proto/elri/v1/context/context_pb.ts'
import {
  ListContextsRequestSchema,
  ResolveContextRequestSchema,
} from '@elri/proto/elri/v1/context/context_pb.ts'
import { type Object$ as Obj, ObjectKind } from '@elri/proto/elri/v1/object/domain_pb.ts'
import {
  CreateObjectRequestSchema,
  ListObjectsRequestSchema,
} from '@elri/proto/elri/v1/object/object_pb.ts'
import { SearchRequestSchema } from '@elri/proto/elri/v1/search/search_pb.ts'
import type { Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import { type FormEvent, useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { DefinitionsPanel } from './DefinitionsPanel.tsx'
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

export function title(o: Obj): string {
  const t = o.fields.title ?? o.fields.name ?? o.fields.$name
  return t ? textOf(t) : o.id.slice(0, 8)
}

function isHypothesis(o: Obj): boolean {
  const h = o.fields.$hypothesis
  return h?.value.case === 'bool' && h.value.value
}

type Tab = 'objects' | 'definitions'

export function SpaceView({ clients, space }: Props) {
  const [tab, setTab] = useState<Tab>('objects')
  const [objects, setObjects] = useState<Obj[]>([])
  const [selected, setSelected] = useState<Obj | null>(null)
  const [newTitle, setNewTitle] = useState('')
  const [newType, setNewType] = useState('')
  const [showDeleted, setShowDeleted] = useState(false)
  const [query, setQuery] = useState('')
  const [searching, setSearching] = useState(false)
  const [partial, setPartial] = useState(false)
  const [contexts, setContexts] = useState<Context[]>([])
  const [context, setContext] = useState<string>('')
  const [missing, setMissing] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      if (query.trim()) {
        const res = await clients.search.search(
          create(SearchRequestSchema, { spaceId: space.id, query: query.trim(), limit: 100 }),
        )
        setObjects(res.hits.flatMap((h) => (h.object ? [h.object] : [])))
        setPartial(res.watermark?.partial ?? false)
        setSearching(true)
      } else if (context) {
        const res = await clients.contexts.resolveContext(
          create(ResolveContextRequestSchema, { spaceId: space.id, id: context, limit: 500 }),
        )
        setObjects(res.objects)
        setMissing(res.missingSpaceIds)
        setSearching(false)
      } else {
        const res = await clients.objects.listObjects(
          create(ListObjectsRequestSchema, {
            spaceId: space.id,
            includeDeleted: showDeleted,
            limit: 500,
          }),
        )
        setObjects(res.objects)
        setMissing([])
        setSearching(false)
      }
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, space.id, showDeleted, query, context])

  useEffect(() => {
    setSelected((cur) => (cur ? (objects.find((o) => o.id === cur.id) ?? null) : null))
  }, [objects])

  const loadContexts = useCallback(async () => {
    try {
      const res = await clients.contexts.listContexts(
        create(ListContextsRequestSchema, { spaceId: space.id }),
      )
      setContexts(res.contexts)
    } catch {
      setContexts([])
    }
  }, [clients, space.id])

  useEffect(() => {
    void refresh()
    void loadContexts()
    const reload = coalesce(() => {
      void refresh()
      void loadContexts()
    })
    return watch(clients, [space.id], (ev) => {
      if (ev.event.case === 'object' || ev.event.case === 'lagged') reload()
    })
  }, [clients, space.id, refresh, loadContexts])

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
      <main id="main" className="main">
        <div className="row" style={{ justifyContent: 'space-between', marginBottom: 12 }}>
          <h2 style={{ margin: 0 }}>{space.name}</h2>
          <nav aria-label="Space sections" className="row">
            <button
              type="button"
              aria-current={tab === 'objects' ? 'page' : undefined}
              className={tab === 'objects' ? 'primary' : undefined}
              onClick={() => setTab('objects')}
            >
              Objects
            </button>
            <button
              type="button"
              aria-current={tab === 'definitions' ? 'page' : undefined}
              className={tab === 'definitions' ? 'primary' : undefined}
              onClick={() => setTab('definitions')}
            >
              Definitions
            </button>
          </nav>
        </div>
        {tab === 'definitions' ? (
          <DefinitionsPanel clients={clients} spaceId={space.id} />
        ) : (
          <>
            <form onSubmit={handleCreate} className="row" style={{ marginBottom: 8 }}>
              <label htmlFor="capture-title" className="visually-hidden">
                quick capture title
              </label>
              <input
                id="capture-title"
                value={newTitle}
                onChange={(e) => setNewTitle(e.target.value)}
                placeholder="Quick capture: title"
                style={{ flex: 1 }}
              />
              <label htmlFor="capture-type" className="visually-hidden">
                type id
              </label>
              <input
                id="capture-type"
                value={newType}
                onChange={(e) => setNewType(e.target.value)}
                placeholder="type (optional)"
                style={{ width: 160 }}
              />
              <button type="submit" className="primary">
                Create
              </button>
            </form>
            <div className="row" style={{ marginBottom: 12, flexWrap: 'wrap' }}>
              <label htmlFor="search" className="visually-hidden">
                search
              </label>
              <input
                id="search"
                type="search"
                value={query}
                onChange={(e) => setQuery(e.target.value)}
                placeholder="Search text (AND, OR, quotes, -word)"
                style={{ flex: 1, minWidth: 200 }}
              />
              <label className="row muted">
                context
                <select
                  value={context}
                  onChange={(e) => setContext(e.target.value)}
                  disabled={searching}
                >
                  <option value="">whole Space</option>
                  {contexts.map((c) => (
                    <option key={c.id} value={c.id}>
                      {c.name}
                    </option>
                  ))}
                </select>
              </label>
              <label className="row muted">
                <input
                  type="checkbox"
                  checked={showDeleted}
                  onChange={(e) => setShowDeleted(e.target.checked)}
                  disabled={searching || Boolean(context)}
                />
                show deleted
              </label>
            </div>
            {partial && (
              <p className="muted" role="status">
                This Space is still downloading: results may be incomplete.
              </p>
            )}
            {missing.length > 0 && (
              <p className="muted" role="status">
                The context names {missing.length} Space(s) this device does not have.
              </p>
            )}
            {error && (
              <p className="error" role="alert">
                {error}
              </p>
            )}
            {objects.length === 0 ? (
              <p className="muted">{searching ? 'No matches.' : 'Nothing here yet.'}</p>
            ) : (
              <table>
                <thead>
                  <tr>
                    <th scope="col">Title</th>
                    <th scope="col">Kind</th>
                    <th scope="col">Types</th>
                    <th scope="col">Updated</th>
                  </tr>
                </thead>
                <tbody>
                  {objects.map((o) => (
                    <tr
                      key={o.id}
                      className={`selectable${selected?.id === o.id ? ' selected' : ''}`}
                      style={{ opacity: o.deletedAtMs !== undefined ? 0.5 : 1 }}
                    >
                      <td>
                        <button type="button" className="linklike" onClick={() => setSelected(o)}>
                          <span>
                            {title(o)}
                            {isHypothesis(o) && <span className="badge"> AI hypothesis</span>}
                          </span>
                        </button>
                      </td>
                      <td className="muted">{KIND_LABEL[o.kind] ?? '?'}</td>
                      <td className="muted">{o.types.join(', ')}</td>
                      <td className="muted">{new Date(Number(o.updatedAtMs)).toLocaleString()}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </>
        )}
      </main>
      <aside className="inspector" aria-label="Inspector">
        {selected ? (
          <ObjectInspector
            key={`${selected.id}:${selected.updatedAtMs}`}
            clients={clients}
            object={selected}
            onChanged={refresh}
            onSelect={(o) => setSelected(o)}
          />
        ) : (
          <p className="muted">Select an object to inspect it.</p>
        )}
      </aside>
    </>
  )
}
