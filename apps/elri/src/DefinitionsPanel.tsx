// Definitions of a Space: fields with a fixed kind, and types composed of
// them. A user shapes a domain here without touching the core.

import { create } from '@bufbuild/protobuf'
import {
  CreateFieldDefinitionRequestSchema,
  CreateTypeDefinitionRequestSchema,
  ListFieldDefinitionsRequestSchema,
  ListTypeDefinitionsRequestSchema,
} from '@elri/proto/elri/v1/definition/definition_pb.ts'
import {
  type FieldDefinition,
  FieldKind,
  type TypeDefinition,
} from '@elri/proto/elri/v1/definition/domain_pb.ts'
import { type FormEvent, useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { coalesce, watch } from './events.ts'

interface Props {
  clients: CoreClients
  spaceId: string
}

export const FIELD_KINDS: { kind: FieldKind; label: string }[] = [
  { kind: FieldKind.STRING, label: 'string' },
  { kind: FieldKind.TEXT, label: 'text' },
  { kind: FieldKind.RICH_TEXT, label: 'rich text' },
  { kind: FieldKind.INT, label: 'integer' },
  { kind: FieldKind.DOUBLE, label: 'number' },
  { kind: FieldKind.BOOL, label: 'boolean' },
  { kind: FieldKind.TIME, label: 'time' },
  { kind: FieldKind.REF, label: 'reference' },
  { kind: FieldKind.LIST, label: 'list' },
  { kind: FieldKind.MAP, label: 'map' },
]

export function kindLabel(k: FieldKind): string {
  return FIELD_KINDS.find((x) => x.kind === k)?.label ?? '?'
}

export function DefinitionsPanel({ clients, spaceId }: Props) {
  const [fields, setFields] = useState<FieldDefinition[]>([])
  const [types, setTypes] = useState<TypeDefinition[]>([])
  const [fieldName, setFieldName] = useState('')
  const [fieldKind, setFieldKind] = useState<FieldKind>(FieldKind.STRING)
  const [typeName, setTypeName] = useState('')
  const [typeFields, setTypeFields] = useState<string[]>([])
  const [error, setError] = useState<string | null>(null)

  const refresh = useCallback(async () => {
    try {
      const f = await clients.definitions.listFieldDefinitions(
        create(ListFieldDefinitionsRequestSchema, { spaceId }),
      )
      const t = await clients.definitions.listTypeDefinitions(
        create(ListTypeDefinitionsRequestSchema, { spaceId }),
      )
      setFields(f.definitions)
      setTypes(t.definitions)
      setError(null)
    } catch (e) {
      setError(errorText(e))
    }
  }, [clients, spaceId])

  useEffect(() => {
    void refresh()
    const reload = coalesce(() => void refresh())
    return watch(clients, [spaceId], (ev) => {
      if (ev.event.case === 'object' || ev.event.case === 'lagged') reload()
    })
  }, [clients, spaceId, refresh])

  const addField = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!fieldName.trim()) return
    try {
      await clients.definitions.createFieldDefinition(
        create(CreateFieldDefinitionRequestSchema, {
          spaceId,
          name: fieldName.trim(),
          description: '',
          kind: fieldKind,
        }),
      )
      setFieldName('')
      setError(null)
      await refresh()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const addType = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!typeName.trim()) return
    try {
      await clients.definitions.createTypeDefinition(
        create(CreateTypeDefinitionRequestSchema, {
          spaceId,
          name: typeName.trim(),
          description: '',
          fields: typeFields.map((fieldId) => ({ fieldId, required: false })),
        }),
      )
      setTypeName('')
      setTypeFields([])
      setError(null)
      await refresh()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const fieldName_ = (id: string) => fields.find((f) => f.id === id)?.name ?? id.slice(0, 8)

  return (
    <section aria-labelledby="defs-heading" className="stack">
      <h2 id="defs-heading" style={{ margin: 0 }}>
        Definitions
      </h2>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <div className="section">
        <h3>Fields</h3>
        {fields.length === 0 ? (
          <p className="muted">No fields defined; Objects may still carry ad-hoc field ids.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col">Kind</th>
                <th scope="col">Id</th>
              </tr>
            </thead>
            <tbody>
              {fields.map((f) => (
                <tr key={f.id}>
                  <td>{f.name}</td>
                  <td className="muted">{kindLabel(f.kind)}</td>
                  <td>
                    <code>{f.id}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <form onSubmit={addField} className="row" style={{ marginTop: 8 }}>
          <label htmlFor="new-field-name" className="visually-hidden">
            field name
          </label>
          <input
            id="new-field-name"
            value={fieldName}
            onChange={(e) => setFieldName(e.target.value)}
            placeholder="new field"
            style={{ flex: 1 }}
          />
          <label htmlFor="new-field-kind" className="visually-hidden">
            field kind
          </label>
          <select
            id="new-field-kind"
            value={fieldKind}
            onChange={(e) => setFieldKind(Number(e.target.value) as FieldKind)}
          >
            {FIELD_KINDS.map((k) => (
              <option key={k.kind} value={k.kind}>
                {k.label}
              </option>
            ))}
          </select>
          <button type="submit" disabled={!fieldName.trim()}>
            Add field
          </button>
        </form>
      </div>
      <div className="section">
        <h3>Types</h3>
        {types.length === 0 ? (
          <p className="muted">No types defined.</p>
        ) : (
          <table>
            <thead>
              <tr>
                <th scope="col">Name</th>
                <th scope="col">Fields</th>
                <th scope="col">Id</th>
              </tr>
            </thead>
            <tbody>
              {types.map((t) => (
                <tr key={t.id}>
                  <td>{t.name}</td>
                  <td className="muted">{t.fields.map((f) => fieldName_(f.fieldId)).join(', ')}</td>
                  <td>
                    <code>{t.id}</code>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
        <form onSubmit={addType} className="stack" style={{ marginTop: 8 }}>
          <div className="row">
            <label htmlFor="new-type-name" className="visually-hidden">
              type name
            </label>
            <input
              id="new-type-name"
              value={typeName}
              onChange={(e) => setTypeName(e.target.value)}
              placeholder="new type"
              style={{ flex: 1 }}
            />
            <button type="submit" disabled={!typeName.trim()}>
              Add type
            </button>
          </div>
          {fields.length > 0 && (
            <fieldset className="row" style={{ flexWrap: 'wrap', border: 0, padding: 0 }}>
              <legend className="muted">fields of the type</legend>
              {fields.map((f) => (
                <label key={f.id} className="row">
                  <input
                    type="checkbox"
                    checked={typeFields.includes(f.id)}
                    onChange={(e) =>
                      setTypeFields((cur) =>
                        e.target.checked ? [...cur, f.id] : cur.filter((x) => x !== f.id),
                      )
                    }
                  />
                  {f.name}
                </label>
              ))}
            </fieldset>
          )}
        </form>
      </div>
    </section>
  )
}
