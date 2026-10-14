// The universal inspector: one Object's types, fields, participants,
// placements and layers, and where a write goes. A hypothesis shows who
// proposed it and takes a review; reserved fields are shown, not edited.

import { create } from '@bufbuild/protobuf'
import {
  type Hypothesis,
  HypothesisStatus,
  type Object$ as Obj,
  ObjectKind,
  type Value,
} from '@elri/proto/elri/v1/object/domain_pb.ts'
import {
  CreateObjectRequestSchema,
  DeleteObjectRequestSchema,
  GetObjectRequestSchema,
  ListObjectsRequestSchema,
  ReviewHypothesisRequestSchema,
  UpdateObjectRequestSchema,
} from '@elri/proto/elri/v1/object/object_pb.ts'
import { type FormEvent, useCallback, useEffect, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'
import { kindOf, parseValue, textOf, VALUE_KINDS, type ValueKind } from './values.ts'

interface Props {
  clients: CoreClients
  object: Obj
  onChanged: () => Promise<unknown>
  onSelect?: (o: Obj) => void
}

const STATUS: Record<number, string> = {
  [HypothesisStatus.PROPOSED]: 'proposed',
  [HypothesisStatus.CONFIRMED]: 'confirmed',
  [HypothesisStatus.REJECTED]: 'rejected',
}

function hypothesisOf(o: Obj): Hypothesis | null {
  const h = o.fields.$hypothesis
  if (!(h?.value.case === 'bool' && h.value.value)) return null
  const text = (k: string) => {
    const v = o.fields[k]
    return v ? textOf(v) : ''
  }
  const statusV = o.fields.$status
  return {
    $typeName: 'elri.v1.object.Hypothesis',
    agent: text('$agent'),
    model: text('$model'),
    sources: [],
    basis: text('$basis'),
    status: statusV?.value.case === 'int' ? (Number(statusV.value.value) as HypothesisStatus) : 1,
    reviewedBy: text('$reviewed_by'),
    reviewedAtMs: 0n,
    note: text('$note'),
  }
}

export function ObjectInspector({ clients, object, onChanged, onSelect }: Props) {
  const [fieldId, setFieldId] = useState('')
  const [fieldKind, setFieldKind] = useState<ValueKind>('string')
  const [fieldText, setFieldText] = useState('')
  const [typeId, setTypeId] = useState('')
  const [slot, setSlot] = useState('')
  const [participant, setParticipant] = useState('')
  const [role, setRole] = useState('')
  const [layers, setLayers] = useState<Obj[]>([])
  const [destination, setDestination] = useState<string>('self')
  const [editing, setEditing] = useState<{ id: string; text: string } | null>(null)
  const [note, setNote] = useState('')
  const [error, setError] = useState<string | null>(null)
  const deleted = object.deletedAtMs !== undefined
  const hypothesis = hypothesisOf(object)

  const loadLayers = useCallback(async () => {
    if (object.kind === ObjectKind.LAYER) {
      setLayers([])
      return
    }
    try {
      const res = await clients.objects.listObjects(
        create(ListObjectsRequestSchema, {
          spaceId: object.spaceId,
          kind: ObjectKind.LAYER,
          targetId: object.id,
          limit: 100,
        }),
      )
      setLayers(res.objects)
    } catch {
      setLayers([])
    }
  }, [clients, object.spaceId, object.id, object.kind])

  useEffect(() => {
    void loadLayers()
  }, [loadLayers])

  // Where a field write goes: the Object itself, an existing Layer over
  // it, or a fresh Layer. The Object is never changed by a Layer write.
  const targetFor = async (): Promise<string> => {
    if (destination === 'self') return object.id
    if (destination === 'new') {
      const res = await clients.objects.createObject(
        create(CreateObjectRequestSchema, {
          spaceId: object.spaceId,
          kind: ObjectKind.LAYER,
          targetId: object.id,
        }),
      )
      const id = res.object?.id ?? ''
      setDestination(id)
      await loadLayers()
      return id
    }
    return destination
  }

  const update = async (patch: {
    setFields?: Record<string, ReturnType<typeof parseValue>>
    removeFields?: string[]
    addTypes?: string[]
    removeTypes?: string[]
    setParticipants?: { slot: string; objectId: string; roleId: string }[]
    removeParticipants?: string[]
    toObject?: boolean
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
      const id =
        patch.toObject || Object.keys(setFields).length === 0 ? object.id : await targetFor()
      await clients.objects.updateObject(
        create(UpdateObjectRequestSchema, {
          spaceId: object.spaceId,
          id,
          setFields,
          removeFields: patch.removeFields ?? [],
          addTypes: patch.addTypes ?? [],
          removeTypes: patch.removeTypes ?? [],
          setParticipants: patch.setParticipants ?? [],
          removeParticipants: patch.removeParticipants ?? [],
        }),
      )
      setError(null)
      await onChanged()
      await loadLayers()
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
    await update({ addTypes: [typeId.trim()], toObject: true })
    setTypeId('')
  }

  const handleParticipant = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!slot.trim() || !participant.trim()) return
    await update({
      setParticipants: [{ slot: slot.trim(), objectId: participant.trim(), roleId: role.trim() }],
      toObject: true,
    })
    setSlot('')
    setParticipant('')
    setRole('')
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

  const review = async (status: HypothesisStatus) => {
    try {
      await clients.objects.reviewHypothesis(
        create(ReviewHypothesisRequestSchema, {
          spaceId: object.spaceId,
          id: object.id,
          status,
          note,
        }),
      )
      setNote('')
      setError(null)
      await onChanged()
    } catch (e) {
      setError(errorText(e))
    }
  }

  const open = async (id: string) => {
    if (!onSelect) return
    try {
      const res = await clients.objects.getObject(
        create(GetObjectRequestSchema, { spaceId: object.spaceId, id }),
      )
      if (res.object) onSelect(res.object)
    } catch (e) {
      setError(errorText(e))
    }
  }

  const editable = (k: string, v: Value) =>
    !k.startsWith('$') && (v.value.case === 'text' || v.value.case === 'rich')

  return (
    <div className="stack">
      <div>
        <h3>Object</h3>
        <code>{object.id}</code>
        <p className="muted">
          {object.kind === ObjectKind.RELATION
            ? 'relation'
            : object.kind === ObjectKind.LAYER
              ? 'layer over '
              : 'entity'}
          {object.kind === ObjectKind.LAYER && object.targetId && (
            <button
              type="button"
              className="linklike"
              onClick={() => void open(object.targetId ?? '')}
            >
              <code>{object.targetId}</code>
            </button>
          )}
          {deleted && ' · deleted'}
        </p>
      </div>

      {hypothesis && (
        <section className="section" aria-labelledby="hypothesis-heading">
          <h3 id="hypothesis-heading">AI hypothesis · {STATUS[hypothesis.status] ?? '?'}</h3>
          <p className="muted">
            proposed by <code>{hypothesis.agent}</code>
            {hypothesis.model && <> using {hypothesis.model}</>}
            {hypothesis.basis && (
              <>
                <br />
                {hypothesis.basis}
              </>
            )}
            {hypothesis.reviewedBy && (
              <>
                <br />
                reviewed by {hypothesis.reviewedBy}
                {hypothesis.note && `: ${hypothesis.note}`}
              </>
            )}
          </p>
          {hypothesis.status === HypothesisStatus.PROPOSED && (
            <div className="stack">
              <label htmlFor="review-note" className="visually-hidden">
                review note
              </label>
              <input
                id="review-note"
                value={note}
                onChange={(e) => setNote(e.target.value)}
                placeholder="note (optional)"
              />
              <div className="row">
                <button
                  type="button"
                  className="primary"
                  onClick={() => void review(HypothesisStatus.CONFIRMED)}
                >
                  Confirm
                </button>
                <button
                  type="button"
                  className="danger"
                  onClick={() => void review(HypothesisStatus.REJECTED)}
                >
                  Reject
                </button>
              </div>
            </div>
          )}
        </section>
      )}

      <div className="section">
        <h3>Types</h3>
        <div className="row" style={{ flexWrap: 'wrap' }}>
          {object.types.map((t) => (
            <span key={t} className="badge">
              {t}{' '}
              <button
                type="button"
                className="linklike"
                aria-label={`remove type ${t}`}
                onClick={() => update({ removeTypes: [t], toObject: true })}
              >
                ×
              </button>
            </span>
          ))}
        </div>
        <form onSubmit={handleAddType} className="row" style={{ marginTop: 8 }}>
          <label htmlFor="add-type" className="visually-hidden">
            type id
          </label>
          <input
            id="add-type"
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
                  <td>
                    {editing?.id === k ? (
                      <form
                        className="stack"
                        onSubmit={(e) => {
                          e.preventDefault()
                          void update({ setFields: { [k]: parseValue(kindOf(v), editing.text) } })
                          setEditing(null)
                        }}
                      >
                        <label htmlFor={`edit-${k}`} className="visually-hidden">
                          {k}
                        </label>
                        <textarea
                          id={`edit-${k}`}
                          rows={6}
                          value={editing.text}
                          onChange={(e) => setEditing({ id: k, text: e.target.value })}
                        />
                        <div className="row">
                          <button type="submit" className="primary">
                            Save
                          </button>
                          <button type="button" onClick={() => setEditing(null)}>
                            Cancel
                          </button>
                        </div>
                      </form>
                    ) : v.value.case === 'ref' ? (
                      <button
                        type="button"
                        className="linklike"
                        onClick={() => void open(textOf(v))}
                      >
                        <code>{textOf(v)}</code>
                      </button>
                    ) : (
                      <span style={{ whiteSpace: 'pre-wrap' }}>{textOf(v)}</span>
                    )}
                  </td>
                  <td>
                    {editable(k, v) && editing?.id !== k && (
                      <button
                        type="button"
                        aria-label={`edit ${k}`}
                        onClick={() => setEditing({ id: k, text: textOf(v) })}
                      >
                        Edit
                      </button>
                    )}
                    {!k.startsWith('$') && (
                      <button
                        type="button"
                        className="danger"
                        aria-label={`remove field ${k}`}
                        onClick={() => update({ removeFields: [k], toObject: true })}
                      >
                        ×
                      </button>
                    )}
                  </td>
                </tr>
              ))}
          </tbody>
        </table>
        {object.kind !== ObjectKind.LAYER && (
          <label className="row muted" style={{ marginTop: 8 }}>
            write to
            <select value={destination} onChange={(e) => setDestination(e.target.value)}>
              <option value="self">this object</option>
              {layers.map((l) => (
                <option key={l.id} value={l.id}>
                  layer {l.id.slice(0, 8)}
                </option>
              ))}
              <option value="new">a new layer over it</option>
            </select>
          </label>
        )}
        <form onSubmit={handleSetField} className="stack" style={{ marginTop: 8 }}>
          <div className="row">
            <label htmlFor="set-field-id" className="visually-hidden">
              field id
            </label>
            <input
              id="set-field-id"
              value={fieldId}
              onChange={(e) => setFieldId(e.target.value)}
              placeholder="field id"
              style={{ flex: 1, minWidth: 0 }}
            />
            <label htmlFor="set-field-kind" className="visually-hidden">
              value kind
            </label>
            <select
              id="set-field-kind"
              value={fieldKind}
              onChange={(e) => setFieldKind(e.target.value as ValueKind)}
            >
              {VALUE_KINDS.map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </div>
          <div className="row">
            <label htmlFor="set-field-value" className="visually-hidden">
              value
            </label>
            <input
              id="set-field-value"
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

      {layers.length > 0 && (
        <div className="section">
          <h3>Layers over it</h3>
          <ul className="list">
            {layers.map((l) => (
              <li key={l.id}>
                <button type="button" className="linklike" onClick={() => void open(l.id)}>
                  <code>{l.id}</code>
                  {hypothesisOf(l) && <span className="badge">AI hypothesis</span>}
                </button>
              </li>
            ))}
          </ul>
        </div>
      )}

      {object.kind === ObjectKind.RELATION && (
        <div className="section">
          <h3>Participants</h3>
          <table>
            <tbody>
              {object.participants.map((p) => (
                <tr key={p.slot}>
                  <td>{p.slot}</td>
                  <td>
                    <button
                      type="button"
                      className="linklike"
                      onClick={() => void open(p.objectId)}
                    >
                      <code>{p.objectId}</code>
                    </button>
                  </td>
                  <td className="muted">{p.roleId}</td>
                  <td>
                    <button
                      type="button"
                      className="danger"
                      aria-label={`remove participant ${p.slot}`}
                      onClick={() => update({ removeParticipants: [p.slot], toObject: true })}
                    >
                      ×
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          <form onSubmit={handleParticipant} className="stack" style={{ marginTop: 8 }}>
            <div className="row">
              <input
                aria-label="slot"
                value={slot}
                onChange={(e) => setSlot(e.target.value)}
                placeholder="slot"
                style={{ width: 90 }}
              />
              <input
                aria-label="participant object id"
                value={participant}
                onChange={(e) => setParticipant(e.target.value)}
                placeholder="object id"
                style={{ flex: 1, minWidth: 0 }}
              />
            </div>
            <div className="row">
              <input
                aria-label="role definition id"
                value={role}
                onChange={(e) => setRole(e.target.value)}
                placeholder="role (optional)"
                style={{ flex: 1, minWidth: 0 }}
              />
              <button type="submit" disabled={!slot.trim() || !participant.trim()}>
                Set
              </button>
            </div>
          </form>
        </div>
      )}

      {object.placements.length > 0 && (
        <div className="section">
          <h3>Placed blocks</h3>
          <ol className="list">
            {object.placements.map((p) => (
              <li key={p.id} style={{ paddingLeft: 10 + 12 * (p.parentId ? 1 : 0) }}>
                <button type="button" className="linklike" onClick={() => void open(p.blockId)}>
                  <code>{p.blockId}</code>
                </button>
              </li>
            ))}
          </ol>
        </div>
      )}

      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {!deleted && (
        <button type="button" className="danger" onClick={handleDelete}>
          Delete object
        </button>
      )}
    </div>
  )
}
