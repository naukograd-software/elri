-- The host API a script, a rule, a handler or a binding sees. Serve it to
-- an editor as the definitions file of the `elri` global; the core
-- enforces capabilities and limits at run time whatever the types say.

export type Mark = { start: number, ["end"]: number, key: string }

-- A field value as the host hands it over: scalars as themselves, time as
-- `{ time = ms }`, a reference as `{ ref = id }`, rich text as text with
-- marks, lists as arrays, maps as tables.
export type Value =
    boolean
    | number
    | string
    | { time: number }
    | { ref: string }
    | { text: string, marks: { Mark } }
    | { Value }
    | { [string]: Value }

export type Participant = { object: string, role: string }

export type Placement = { id: string, parent: string, index: number, block: string }

export type Object = {
    id: string,
    space: string,
    -- `entity`, `relation`, `layer`, ...
    kind: string,
    types: { string },
    fields: { [string]: Value },
    created_at: number,
    updated_at: number,
    deleted_at: number?,
    -- Layers: the Object enriched.
    target: string?,
    -- Relations: slot → participant.
    participants: { [string]: Participant }?,
    -- Documents: placed blocks in order.
    placements: { Placement }?,
}

export type Filter = {
    kind: string?,
    type: string?,
    limit: number?,
    include_deleted: boolean?,
    participant: string?,
    target: string?,
    placed_block: string?,
}

export type Hypothesis = { model: string?, sources: { string }?, basis: string? }

export type Spec = {
    kind: string?,
    types: { string }?,
    fields: { [string]: Value }?,
    -- Layers only.
    target: string?,
    participants: { [string]: { object: string, role: string? } }?,
    -- A layer proposed by the script, with its provenance.
    hypothesis: Hypothesis?,
}

export type Patch = {
    fields: { [string]: Value }?,
    remove_fields: { string }?,
    add_types: { string }?,
    remove_types: { string }?,
}

export type Hit = { object: Object, score: number, fields: { string } }

export type MappingMember = { space: string, field: string }

declare elri: {
    -- The Space of the run.
    space: string,
    -- This device.
    device: string,
    -- Milliseconds since the epoch when the run started.
    now: number,
    -- `RunScript` arguments, or the trigger's payload for a rule.
    args: any,
    -- Appends a line to the run's log.
    log: (...any) -> (),
    -- Tells the user; suppressed in a preview.
    notify: (text: string) -> (),
    get: (id: string, space: string?) -> Object,
    list: (filter: Filter?, space: string?) -> { Object },
    search: (query: string, space: string?) -> { Hit },
    create: (spec: Spec, space: string?) -> Object,
    update: (id: string, patch: Patch, space: string?) -> Object,
    delete: (id: string, space: string?) -> boolean,
    -- An installed, granted extension the manifest names.
    ext: (name: string, func: string, args: any?) -> any,
    -- In a workflow run: records the step's intent before `body` and its
    -- result after; a recorded result is returned without running again.
    step: <T>(name: string, body: (() -> T) | T) -> T,
    -- A mapping in the script's Space, waiting for a person's approval.
    propose_mapping: (spec: { name: string, members: { MappingMember } }) -> { id: string, approved: boolean },
}

-- Interface handlers and bindings: the interface's local state.
declare state: { [string]: any }
-- Interface handlers: what the renderer attached to the event.
declare event: any
-- Inside a component: its parameters.
declare params: { [string]: any }
