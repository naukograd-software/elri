// Connection to the local core. The shell hands over the loopback endpoint;
// outside Tauri (plain browser dev) it comes from Vite env variables.

import { ChangeServiceClient } from '@elri/proto/elri/v1/change/change_ws_pb.ts'
import { CoreServiceClient } from '@elri/proto/elri/v1/core/core_ws_pb.ts'
import { EventServiceClient } from '@elri/proto/elri/v1/events/event_ws_pb.ts'
import { ObjectServiceClient } from '@elri/proto/elri/v1/object/object_ws_pb.ts'
import { RemoteServiceClient } from '@elri/proto/elri/v1/remote/remote_ws_pb.ts'
import { SpaceServiceClient } from '@elri/proto/elri/v1/space/space_ws_pb.ts'
import { InterfaceServiceClient } from '@elri/proto/elri/v1/ui/ui_ws_pb.ts'
import { WsTransport } from '@gopherex/ws-proto-transport'
import { invoke } from '@tauri-apps/api/core'

export interface Endpoint {
  url: string
  token: string
  pid: number
}

export interface CoreClients {
  core: CoreServiceClient
  spaces: SpaceServiceClient
  objects: ObjectServiceClient
  changes: ChangeServiceClient
  remote: RemoteServiceClient
  events: EventServiceClient
  interfaces: InterfaceServiceClient
}

const inTauri = '__TAURI_INTERNALS__' in window

async function discover(): Promise<Endpoint> {
  if (inTauri) return invoke<Endpoint>('core_endpoint')
  const url = import.meta.env.VITE_ELRI_CORE_URL as string | undefined
  const token = import.meta.env.VITE_ELRI_CORE_TOKEN as string | undefined
  if (!url || !token)
    throw new Error('VITE_ELRI_CORE_URL and VITE_ELRI_CORE_TOKEN are required outside Tauri')
  return { url, token, pid: 0 }
}

export async function connect(): Promise<CoreClients> {
  const endpoint = await discover()
  const transport = new WsTransport(endpoint.url, {
    interceptors: [
      (next) => (req) => {
        req.header.authorization = `Bearer ${endpoint.token}`
        return next(req)
      },
    ],
  })
  return {
    core: new CoreServiceClient(transport),
    spaces: new SpaceServiceClient(transport),
    objects: new ObjectServiceClient(transport),
    changes: new ChangeServiceClient(transport),
    remote: new RemoteServiceClient(transport),
    events: new EventServiceClient(transport),
    interfaces: new InterfaceServiceClient(transport),
  }
}

export function errorText(e: unknown): string {
  return e instanceof Error ? e.message : String(e)
}
