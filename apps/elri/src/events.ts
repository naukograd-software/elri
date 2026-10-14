// Live events from the core. One subscription per consumer; the returned
// function ends it. The callback is also invoked on a lagged notice so the
// consumer reloads what it shows.

import { create } from '@bufbuild/protobuf'
import type { Event } from '@elri/proto/elri/v1/events/domain_pb.ts'
import { WatchRequestSchema } from '@elri/proto/elri/v1/events/event_pb.ts'
import type { CoreClients } from './core.ts'

export type EventKind = NonNullable<Event['event']['case']>

export function watch(
  clients: CoreClients,
  spaceIds: string[],
  onEvent: (ev: Event) => void,
): () => void {
  const abort = new AbortController()
  void (async () => {
    try {
      for await (const ev of clients.events.watch(create(WatchRequestSchema, { spaceIds }), {
        signal: abort.signal,
      })) {
        onEvent(ev)
      }
    } catch {
      // Ended by abort or by the core going away; the shell reconnects.
    }
  })()
  return () => abort.abort()
}

/** Coalesces bursts of events into one call, at most once per `delayMs`. */
export function coalesce(f: () => void, delayMs = 80): () => void {
  let timer: ReturnType<typeof setTimeout> | null = null
  return () => {
    if (timer !== null) return
    timer = setTimeout(() => {
      timer = null
      f()
    }, delayMs)
  }
}
