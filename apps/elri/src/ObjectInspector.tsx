import { create } from '@bufbuild/protobuf'
import { type Object$ as Obj, ObjectKind } from '@elri/proto/elri/v1/object/domain_pb.ts'
import {
  DeleteObjectRequestSchema,
  UpdateObjectRequestSchema,
} from '@elri/proto/elri/v1/object/object_pb.ts'
import { type FormEvent, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { kindOf, parseValue, textOf, VALUE_KINDS, type ValueKind } from './values.ts'

interface Props {
  clients: CoreClients
  object: Obj
  onChanged: () => Promise<unknown>
}

export function ObjectInspector({ clients, object, onChanged }: Props) {
  const [fieldId, setFieldId] = useState('')
  const [fieldKind, setFieldKind] = useState<ValueKind>('string')
  const [fieldText, setFieldText] = useState('')
  const [typeId, setTypeId] = useState('')
  const [error, setError] = useState<string | null>(null)
  const deleted = object.deletedAtMs !== undefined

  const update = async (patch: {
    setFields?: Record<string, ReturnType<typeof parseValue>>
    removeFields?: string[]
    addTypes?: string[]
    removeTypes?: string[]
  }) => {
    try {
      const setFields: Record<string, Exclude<ReturnType<typeof parseValue>, string>> = {}
      for (const [k, v] of Object.entries(patch.setFields ?? {})) {
        if (typeof v === 'string') {
          setError(`${k}: ${v}`)
          return
        }
        setFields[k] = v
      }
      await clients.objects.updateObject(
        create(UpdateObjectRequestSchema, {
          spaceId: object.spaceId,
          id: object.id,
          setFields,
          removeFields: patch.removeFields ?? [],
          addTypes: patch.addTypes ?? [],
          removeTypes: patch.removeTypes ?? [],
        }),
      )
      setError(null)
      await onChanged()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const handleSetField = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!fieldId.trim()) return
    await update({ setFields: { [fieldId.trim()]: parseValue(fieldKind, fieldText) } })
    setFieldId('')
    setFieldText('')
  }

  const handleAddType = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!typeId.trim()) return
    await update({ addTypes: [typeId.trim()] })
    setTypeId('')
  }

  const handleDelete = async () => {
    try {
      await clients.objects.deleteObject(
        create(DeleteObjectRequestSchema, { spaceId: object.spaceId, id: object.id }),
      )
      await onChanged()
    } catch (e) {
      setError(errorText(e))
    }
  }

  return (
    <div className="stack">
      <div>
        <h3>Object</h3>
        <code>{object.id}</code>
        <p className="muted">
          {object.kind === ObjectKind.RELATION
            ? 'relation'
            : object.kind === ObjectKind.LAYER
              ? `layer over ${object.targetId ?? '?'}`
              : 'entity'}
          {deleted && ' · deleted'}
        </p>
      </div>

      <div className="section">
        <h3>Types</h3>
        <div className="row" style={{ flexWrap: 'wrap' }}>
          {object.types.map((t) => (
            <span key={t} className="badge">
              {t}{' '}
              <button
                type="button"
                style={{ all: 'unset', cursor: 'pointer' }}
                title="remove type"
                onClick={() => update({ removeTypes: [t] })}
              >
                ×
              </button>
            </span>
          ))}
        </div>
        <form onSubmit={handleAddType} className="row" style={{ marginTop: 8 }}>
          <input
            value={typeId}
            onChange={(e) => setTypeId(e.target.value)}
            placeholder="add type"
            style={{ flex: 1, minWidth: 0 }}
          />
          <button type="submit" disabled={!typeId.trim()}>
            Add
          </button>
        </form>
      </div>

      <div className="section">
        <h3>Fields</h3>
        <table>
          <tbody>
            {Object.entries(object.fields)
              .sort(([a], [b]) => a.localeCompare(b))
              .map(([k, v]) => (
                <tr key={k}>
                  <td>
                    <strong>{k}</strong>
                    <br />
                    <span className="muted">{kindOf(v)}</span>
                  </td>
                  <td>{textOf(v)}</td>
                  <td>
                    <button
                      type="button"
                      className="danger"
                      title="remove field"
                      onClick={() => update({ removeFields: [k] })}
                    >
                      ×
                    </button>
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
        <form onSubmit={handleSetField} className="stack" style={{ marginTop: 8 }}>
          <div className="row">
            <input
              value={fieldId}
              onChange={(e) => setFieldId(e.target.value)}
              placeholder="field id"
              style={{ flex: 1, minWidth: 0 }}
            />
            <select value={fieldKind} onChange={(e) => setFieldKind(e.target.value as ValueKind)}>
              {VALUE_KINDS.map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </div>
          <div className="row">
            <input
              value={fieldText}
              onChange={(e) => setFieldText(e.target.value)}
              placeholder="value"
              style={{ flex: 1, minWidth: 0 }}
            />
            <button type="submit" disabled={!fieldId.trim()}>
              Set
            </button>
          </div>
        </form>
      </div>

      {object.participants.length > 0 && (
        <div className="section">
          <h3>Participants</h3>
          <table>
            <tbody>
              {object.participants.map((p) => (
                <tr key={p.slot}>
                  <td>{p.slot}</td>
                  <td>
                    <code>{p.objectId}</code>
                  </td>
                  <td className="muted">{p.roleId}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {error && <p className="error">{error}</p>}
      {!deleted && (
        <button type="button" className="danger" onClick={handleDelete}>
          Delete object
        </button>
      )}
    </div>
  )
}
