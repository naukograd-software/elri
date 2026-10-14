# Building interfaces

An interface is an Object holding a tree of nodes from a fixed catalog:
column, row, text, button, input, checkbox, list, table, card, component.
The renderer draws it; the core evaluates bindings and runs handlers.

1. `InterfaceService/CreateInterface` with a `name` and `state_json`
   (local state defaults, a JSON object string, e.g. `{"draft": ""}`).
2. Add nodes with `UpdateInterface`, `set_nodes`: each node has
   `parent_id` (empty at the top), `index`, `type`, and three JSON object
   strings:
   - `props_json`: literal properties (`{"label": "Add", "primary": true}`,
     `{"heading": true}`, `{"state": "draft"}` binds an input to state,
     `{"columns": ["title"]}` for tables).
   - `bindings_json`: property → Luau expression evaluated read-only,
     e.g. `{"items": "elri.list({ type = \"task\" })"}`,
     `{"text": "\"Tasks: \" .. #elri.list({ type = \"task\" })"}`,
     `{"disabled": "state.draft == \"\""}`.
   - `handlers_json`: event → Luau source run on the event, with `state`,
     `event` and `elri` in scope, e.g.
     `{"click": "elri.create({ types = {\"task\"}, fields = { title = state.draft } }); state.draft = \"\""}`.
   Create the parent first, read its id from the answer, then add children.
3. Check with `Evaluate` (`state_json` given): every binding's value and
   error per node.
4. Components: `CreateInterface` with `component: true` and `params_json`;
   place one with a node of type `component` and props
   `{"ref": "<component id>", "params": {...}}`; inside it `params` is in
   scope.
5. `PublishInterface` freezes a version; users choose versions themselves.

Lists and tables take Objects; `label` names the field shown.
