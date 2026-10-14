// Draws an interface tree from the fixed catalog. The core evaluates every
// binding and runs every handler; this file only turns node values into
// elements and reports events back.

import { create } from '@bufbuild/protobuf'
import type { Interface, Node } from '@elri/proto/elri/v1/ui/domain_pb.ts'
import {
  DispatchRequestSchema,
  EvaluateRequestSchema,
  GetInterfaceRequestSchema,
} from '@elri/proto/elri/v1/ui/ui_pb.ts'
import { type ReactNode, useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'

interface Props {
  clients: CoreClients
  spaceId: string
  interfaceId: string
}

type Json = Record<string, unknown>

interface Evaluated {
  values: Map<string, Json>
  errors: Map<string, Record<string, string>>
}

export function InterfaceView({ clients, spaceId, interfaceId }: Props) {
  const [iface, setIface] = useState<Interface | null>(null)
  const [state, setState] = useState<Json>({})
  const [evaluated, setEvaluated] = useState<Evaluated | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [logs, setLogs] = useState<string[]>([])

  const load = useCallback(async () => {
    try {
      const res = await clients.interfaces.getInterface(
        create(GetInterfaceRequestSchema, { spaceId, id: interfaceId, selected: true }),
      )
      if (res.interface) {
        setIface(res.interface)
        setState((cur) => (Object.keys(cur).length ? cur : parseJson(res.interface?.stateJson)))
      }
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId, interfaceId])

  const evaluate = useCallback(async () => {
    if (!iface) return
    try {
      const res = await clients.interfaces.evaluate(
        create(EvaluateRequestSchema, {
          spaceId,
          id: iface.id,
          stateJson: JSON.stringify(state),
        }),
      )
      const values = new Map<string, Json>()
      const errors = new Map<string, Record<string, string>>()
      for (const n of res.nodes) {
        values.set(n.nodeId, parseJson(n.valuesJson))
        if (Object.keys(n.errors).length) errors.set(n.nodeId, n.errors)
      }
      setEvaluated({ values, errors })
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId, iface, state])

  useEffect(() => {
    void load()
    const reload = coalesce(() => void load())
    return watch(clients, [spaceId], (ev) => {
      if (ev.event.case === 'object' && ev.event.value.objectId === interfaceId) reload()
    })
  }, [clients, spaceId, interfaceId, load])

  useEffect(() => {
    void evaluate()
    const reload = coalesce(() => void evaluate())
    return watch(clients, [spaceId], (ev) => {
      if (ev.event.case === 'object' || ev.event.case === 'lagged') reload()
    })
  }, [clients, spaceId, evaluate])

  const dispatch = async (nodeId: string, event: string, payload: unknown) => {
    if (!iface) return
    try {
      const res = await clients.interfaces.dispatch(
        create(DispatchRequestSchema, {
          spaceId,
          id: iface.id,
          nodeId,
          event,
          stateJson: JSON.stringify(state),
          payloadJson: JSON.stringify(payload ?? null),
        }),
      )
      if (res.error) setError(res.error)
      else setError(null)
      if (res.logs.length) setLogs((l) => [...l, ...res.logs].slice(-50))
      setState(parseJson(res.stateJson))
    } catch (e) {
      setError(errorText(e))
    }
  }

  const setLocal = (name: string, value: unknown) => {
    setState((cur) => ({ ...cur, [name]: value }))
  }

  if (!iface) return <p className="muted">loading interface…</p>
  const byParent = new Map<string, Node[]>()
  for (const n of iface.nodes) {
    const list = byParent.get(n.parentId) ?? []
    list.push(n)
    byParent.set(n.parentId, list)
  }
  const ctx: RenderCtx = {
    byParent,
    values: evaluated?.values ?? new Map(),
    errors: evaluated?.errors ?? new Map(),
    dispatch,
    setLocal,
    state,
  }
  return (
    <div className="stack">
      {error && <p className="error">{error}</p>}
      {(byParent.get('') ?? []).map((n) => renderNode(n, ctx))}
      {logs.length > 0 && (
        <details>
          <summary className="muted">logs</summary>
          <pre style={{ fontSize: 12 }}>{logs.join('\n')}</pre>
        </details>
      )}
    </div>
  )
}

interface RenderCtx {
  byParent: Map<string, Node[]>
  values: Map<string, Json>
  errors: Map<string, Record<string, string>>
  dispatch: (nodeId: string, event: string, payload: unknown) => Promise<void>
  setLocal: (name: string, value: unknown) => void
  state: Json
}

function parseJson(s: string | undefined): Json {
  if (!s) return {}
  try {
    const v: unknown = JSON.parse(s)
    return v && typeof v === 'object' && !Array.isArray(v) ? (v as Json) : {}
  } catch {
    return {}
  }
}

function text(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (typeof v === 'string') return v
  if (typeof v === 'number' || typeof v === 'boolean') return String(v)
  return JSON.stringify(v)
}

function renderNode(n: Node, ctx: RenderCtx): ReactNode {
  const p = ctx.values.get(n.id) ?? parseJson(n.propsJson)
  const errs = ctx.errors.get(n.id)
  const children = (ctx.byParent.get(n.id) ?? []).map((c) => renderNode(c, ctx))
  const errorNote = errs ? (
    <span className="error" title={Object.values(errs).join('\n')}>
      {' '}
      ⚠
    </span>
  ) : null
  const handlers = parseJson(n.handlersJson)
  const has = (ev: string) => typeof handlers[ev] === 'string'
  switch (n.type) {
    case 'column':
      return (
        <div key={n.id} className="stack" style={{ gap: text(p.gap) || 8 }}>
          {children}
          {errorNote}
        </div>
      )
    case 'row':
      return (
        <div key={n.id} className="row" style={{ gap: text(p.gap) || 8, flexWrap: 'wrap' }}>
          {children}
          {errorNote}
        </div>
      )
    case 'card':
      return (
        <div
          key={n.id}
          style={{ border: '1px solid var(--line)', borderRadius: 8, padding: 12 }}
          className="stack"
        >
          {p.title !== undefined && <strong>{text(p.title)}</strong>}
          {children}
          {errorNote}
        </div>
      )
    case 'text': {
      const Tag = p.heading ? 'h2' : 'p'
      return (
        <Tag key={n.id} className={p.muted ? 'muted' : undefined} style={{ margin: 0 }}>
          {text(p.text)}
          {errorNote}
        </Tag>
      )
    }
    case 'button':
      return (
        <button
          key={n.id}
          type="button"
          className={p.primary ? 'primary' : undefined}
          disabled={Boolean(p.disabled) || !has('click')}
          onClick={() => void ctx.dispatch(n.id, 'click', p.payload ?? null)}
        >
          {text(p.label) || 'button'}
          {errorNote}
        </button>
      )
    case 'input': {
      const name = text(p.state)
      const value = name ? text(ctx.state[name]) : text(p.value)
      return (
        <input
          key={n.id}
          value={value}
          placeholder={text(p.placeholder)}
          onChange={(e) => {
            if (name) ctx.setLocal(name, e.target.value)
            if (has('change')) void ctx.dispatch(n.id, 'change', e.target.value)
          }}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && has('submit')) void ctx.dispatch(n.id, 'submit', value)
          }}
        />
      )
    }
    case 'checkbox': {
      const name = text(p.state)
      const checked = name ? Boolean(ctx.state[name]) : Boolean(p.checked)
      return (
        <label key={n.id} className="row">
          <input
            type="checkbox"
            checked={checked}
            onChange={(e) => {
              if (name) ctx.setLocal(name, e.target.checked)
              if (has('change')) void ctx.dispatch(n.id, 'change', e.target.checked)
            }}
          />
          {text(p.label)}
          {errorNote}
        </label>
      )
    }
    case 'list': {
      const items = Array.isArray(p.items) ? (p.items as unknown[]) : []
      const label = text(p.label) || 'title'
      return (
        <ul key={n.id} className="list">
          {items.map((it, i) => {
            const obj = it && typeof it === 'object' ? (it as Json) : { value: it }
            const fields = obj.fields && typeof obj.fields === 'object' ? (obj.fields as Json) : obj
            const key = text(obj.id) || String(i)
            return (
              <li key={key}>
                <button
                  type="button"
                  style={{ all: 'unset', cursor: has('select') ? 'pointer' : 'default', flex: 1 }}
                  onClick={() => has('select') && void ctx.dispatch(n.id, 'select', it)}
                >
                  {text(fields[label] ?? obj.value ?? key)}
                </button>
              </li>
            )
          })}
          {items.length === 0 && <li className="muted">nothing</li>}
          {errorNote}
        </ul>
      )
    }
    case 'table': {
      const rows = Array.isArray(p.rows) ? (p.rows as unknown[]) : []
      const columns = Array.isArray(p.columns) ? (p.columns as unknown[]).map(text) : []
      return (
        <table key={n.id}>
          <thead>
            <tr>
              {columns.map((c) => (
                <th key={c}>{c}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => {
              const obj = r && typeof r === 'object' ? (r as Json) : {}
              const fields =
                obj.fields && typeof obj.fields === 'object' ? (obj.fields as Json) : obj
              return (
                <tr
                  key={text(obj.id) || String(i)}
                  onClick={() => has('select') && void ctx.dispatch(n.id, 'select', r)}
                  style={{ cursor: has('select') ? 'pointer' : 'default' }}
                >
                  {columns.map((c) => (
                    <td key={c}>{text(fields[c] ?? obj[c])}</td>
                  ))}
                </tr>
              )
            })}
          </tbody>
          {errorNote}
        </table>
      )
    }
    default:
      return (
        <p key={n.id} className="error">
          unknown node type {n.type}
        </p>
      )
  }
}
