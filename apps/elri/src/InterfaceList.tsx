// Interfaces of the current Space: pick one to draw it in the main panel.

import { create } from '@bufbuild/protobuf'
import type { Interface } from '@elri/proto/elri/v1/ui/domain_pb.ts'
import { ListInterfacesRequestSchema } from '@elri/proto/elri/v1/ui/ui_pb.ts'
import { useCallback, useEffect, useState } from 'react'
import type { CoreClients } from './core.ts'
import { coalesce, watch } from './events.ts'

interface Props {
  clients: CoreClients
  spaceId: string
  current: string | null
  onSelect: (id: string | null) => void
}

export function InterfaceList({ clients, spaceId, current, onSelect }: Props) {
  const [items, setItems] = useState<Interface[]>([])

  const refresh = useCallback(async () => {
    try {
      const res = await clients.interfaces.listInterfaces(
        create(ListInterfacesRequestSchema, { spaceId }),
      )
      setItems(res.interfaces)
    } catch {
      setItems([])
    }
  }, [clients, spaceId])

  useEffect(() => {
    void refresh()
    const reload = coalesce(() => void refresh())
    return watch(clients, [spaceId], (ev) => {
      if (ev.event.case === 'object' && ev.event.value.kind === 10) reload()
    })
  }, [clients, spaceId, refresh])

  if (items.length === 0) return null
  return (
    <div className="section" style={{ marginTop: 16 }}>
      <h3>Interfaces</h3>
      <ul className="list">
        <li className={current === null ? 'active' : ''}>
          <button
            type="button"
            style={{ all: 'unset', cursor: 'pointer', flex: 1 }}
            onClick={() => onSelect(null)}
          >
            Objects
          </button>
        </li>
        {items.map((i) => (
          <li key={i.id} className={current === i.id ? 'active' : ''}>
            <button
              type="button"
              style={{ all: 'unset', cursor: 'pointer', flex: 1 }}
              onClick={() => onSelect(i.id)}
            >
              {i.name}
            </button>
          </li>
        ))}
      </ul>
    </div>
  )
}
