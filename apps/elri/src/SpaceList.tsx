import { create } from '@bufbuild/protobuf'
import { Authority, type Space } from '@elri/proto/elri/v1/space/domain_pb.ts'
import { CreateSpaceRequestSchema } from '@elri/proto/elri/v1/space/space_pb.ts'
import { type FormEvent, useState } from 'react'
import { type CoreClients, errorText } from './core.ts'

interface Props {
  clients: CoreClients
  spaces: Space[]
  current: Space | null
  onSelect: (s: Space) => void
  onChanged: () => Promise<unknown>
}

export function SpaceList({ clients, spaces, current, onSelect, onChanged }: Props) {
  const [name, setName] = useState('')
  const [error, setError] = useState<string | null>(null)

  const handleCreate = async (ev: FormEvent) => {
    ev.preventDefault()
    if (!name.trim()) return
    try {
      const res = await clients.spaces.createSpace(
        create(CreateSpaceRequestSchema, { name: name.trim() }),
      )
      setName('')
      setError(null)
      await onChanged()
      if (res.space) onSelect(res.space)
    } catch (e) {
      setError(errorText(e))
    }
  }

  return (
    <div className="section">
      <h3>Spaces</h3>
      {spaces.length === 0 && <p className="muted">No Spaces yet.</p>}
      <ul className="list">
        {spaces.map((s) => (
          <li key={s.id} className={current?.id === s.id ? 'active' : ''}>
            <button
              type="button"
              style={{ all: 'unset', cursor: 'pointer', flex: 1 }}
              onClick={() => onSelect(s)}
            >
              {s.name}
            </button>
            {s.authority === Authority.SERVER && <span className="badge">shared</span>}
          </li>
        ))}
      </ul>
      <form onSubmit={handleCreate} className="row" style={{ marginTop: 8 }}>
        <input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="New local Space"
          style={{ flex: 1, minWidth: 0 }}
        />
        <button type="submit" disabled={!name.trim()}>
          Add
        </button>
      </form>
      {error && <p className="error">{error}</p>}
    </div>
  )
}
