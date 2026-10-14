//! Instructions shipped with the application: the base prompt every
//! agent starts from, and the built-in skills an agent loads on demand.

use elri_proto::elri::v1::ai::Skill;

/// What every agent is told first.
pub const BASE: &str = include_str!("skills/base.md");

/// Built-in skills: name, description, body.
const BUILTIN: &[(&str, &str, &str)] = &[
    (
        "domain",
        "Define fields and types, create entities, relations and layers, constraints.",
        include_str!("skills/domain.md"),
    ),
    (
        "interfaces",
        "Build interfaces as data: nodes, bindings, handlers, components, publishing.",
        include_str!("skills/interfaces.md"),
    ),
    (
        "scripts",
        "Write Luau scripts, rules and durable workflows; the host API declarations.",
        include_str!("skills/scripts.md"),
    ),
    (
        "memory",
        "Search by words and meaning, read history and contexts, propose hypotheses and mappings.",
        include_str!("skills/memory.md"),
    ),
];

/// Prefix of a built-in skill reference.
pub const BUILTIN_PREFIX: &str = "builtin:";

/// The built-in skills.
pub fn builtin() -> Vec<Skill> {
    BUILTIN
        .iter()
        .map(|(name, description, body)| Skill {
            id: format!("{BUILTIN_PREFIX}{name}"),
            space_id: String::new(),
            name: (*name).to_owned(),
            description: (*description).to_owned(),
            body: body.replace("{{SDK}}", crate::script::SDK.trim()),
            builtin: true,
            created_at_ms: 0,
            updated_at_ms: 0,
            deleted_at_ms: None,
        })
        .collect()
}
