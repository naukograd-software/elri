# Modeling a domain

Fields are defined once per Space with a fixed kind
(`DefinitionService/CreateFieldDefinition`: string, text, rich text, int,
double, bool, time, ref, list, map). Types compose fields
(`CreateTypeDefinition` with `fields: [{field_id, required}]`). An Object
carries types by id and field values keyed by field definition id.

- Create Objects with `ObjectService/CreateObject`: `kind` (entity,
  relation, layer), `types`, `fields` as a map from field id to a Value
  (`{"string": "..."}`, `{"int": "5"}`, `{"bool": true}`,
  `{"ref": "<object id>"}`, `{"time_ms": "<ms>"}`, `{"text": "..."}`,
  `{"list": {"items": [...]}}`).
- Relations are Objects too: `kind: OBJECT_KIND_RELATION` with
  `participants: [{slot, object_id, role_id}]`.
- A layer (`kind: OBJECT_KIND_LAYER`, `target_id`) adds fields to another
  Object without changing it; use it for personal or derived values.
- Check existing definitions first (`ListFieldDefinitions`,
  `ListTypeDefinitions`) and reuse them; never create a second field
  with the same meaning.
- Unique values: `CreateConstraint` with `unique: true`; diagnostic by
  default, `ListViolations` shows conflicts.
- Several writes that belong together go in one `ObjectService/Commit`.
