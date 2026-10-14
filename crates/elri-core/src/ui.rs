//! Interfaces as data: an Object holding a tree of catalog nodes. The core
//! evaluates bindings and runs handlers; the renderer only draws.

use std::collections::BTreeMap;

use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::script::{Manifest, Script};
use elri_proto::elri::v1::ui::{Interface, Node, NodeValues};

use crate::model::{int_field, reserved, string_field, ObjectDoc, ObjectId, SpaceId};
use crate::script::{self, Host, RunIdentity};

/// Reserved field: declared local state as JSON.
pub const STATE: &str = "$state";
/// Reserved field: version number of a published copy.
pub const VERSION: &str = "$version";
/// Reserved field: the draft a published copy was taken from.
pub const DRAFT: &str = "$draft";
/// Device-setting prefix: the version selected for a draft.
pub const SELECTED: &str = "ui_version:";
/// Reserved field: a component's parameter defaults as JSON.
pub const PARAMS: &str = "$params";
/// Reserved field: the catalog version a published copy was made for.
pub const CATALOG_FIELD: &str = "$catalog";
/// The catalog this build draws. A version published for a newer one is
/// shown read-only.
pub const CATALOG_VERSION: i32 = 1;
/// Deepest nesting of components inside components.
pub const MAX_COMPONENT_DEPTH: usize = 4;
/// Most nodes an expanded tree may hold, placements included.
pub const MAX_EXPANDED_NODES: usize = 2000;

/// Node types the renderer draws.
pub const CATALOG: &[&str] = &[
    "column",
    "row",
    "text",
    "button",
    "input",
    "checkbox",
    "list",
    "table",
    "card",
    "component",
];

/// Whether a type is in the catalog.
pub fn in_catalog(t: &str) -> bool {
    CATALOG.contains(&t)
}

/// The protocol view of an interface Object with its nodes.
pub fn interface_of(o: &Object, doc: &ObjectDoc) -> Result<Interface, crate::model::ModelError> {
    Ok(Interface {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        nodes: doc.ui_nodes()?,
        state_json: string_field(o, STATE).unwrap_or("{}").to_owned(),
        version: i32::try_from(int_field(o, VERSION).unwrap_or(0)).unwrap_or(0),
        draft_id: string_field(o, DRAFT).unwrap_or_default().to_owned(),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
        component: o.kind() == ObjectKind::Component,
        params_json: string_field(o, PARAMS).unwrap_or("{}").to_owned(),
        catalog: i32::try_from(int_field(o, CATALOG_FIELD).unwrap_or(0)).unwrap_or(0),
    })
}

/// Whether this build can run a published copy's handlers.
pub fn compatible(iface: &Interface) -> bool {
    iface.catalog <= CATALOG_VERSION
}

/// The component a `component` node places, and its literal parameters.
pub fn component_ref(n: &Node) -> Option<(ObjectId, serde_json::Map<String, serde_json::Value>)> {
    if n.r#type.trim() != "component" {
        return None;
    }
    let props = json_object(&n.props_json).ok()?;
    let id = ObjectId::parse(props.get("ref")?.as_str()?).ok()?;
    let params = match props.get("params") {
        Some(serde_json::Value::Object(m)) => m.clone(),
        _ => serde_json::Map::new(),
    };
    Some((id, params))
}

/// The parameters an instance of a component runs with: the component's
/// defaults under the instance's literal and evaluated `params`.
pub fn merged_params(
    defaults: &str,
    literal: &serde_json::Map<String, serde_json::Value>,
    evaluated: Option<&serde_json::Value>,
) -> serde_json::Value {
    let mut out = json_object(defaults).unwrap_or_default();
    for (k, v) in literal {
        out.insert(k.clone(), v.clone());
    }
    if let Some(serde_json::Value::Object(m)) = evaluated {
        for (k, v) in m {
            out.insert(k.clone(), v.clone());
        }
    }
    serde_json::Value::Object(out)
}

/// The component's nodes placed under `instance`, ids prefixed with the
/// instance's so every expansion is distinct.
pub fn placed(instance: &str, nodes: &[Node]) -> Vec<Node> {
    nodes
        .iter()
        .map(|n| Node {
            id: format!("{instance}/{}", n.id),
            parent_id: if n.parent_id.is_empty() {
                instance.to_owned()
            } else {
                format!("{instance}/{}", n.parent_id)
            },
            ..n.clone()
        })
        .collect()
}

/// Hex SHA-256 over the nodes' handlers: what a grant for the interface's
/// executable part is bound to.
pub fn version_of(iface: &Interface) -> String {
    let mut all = String::new();
    for n in &iface.nodes {
        all.push_str(&n.id);
        all.push('\0');
        all.push_str(&n.handlers_json);
        all.push('\0');
    }
    script::version_of(&all, &Manifest::default())
}

/// The script-shaped identity an interface's handlers run under: no
/// manifest, so only the interface's own Space, hence always benign.
pub fn as_script(iface: &Interface) -> Script {
    Script {
        id: iface.id.clone(),
        space_id: iface.space_id.clone(),
        name: iface.name.clone(),
        source: String::new(),
        manifest: Some(Manifest::default()),
        version: version_of(iface),
        benign: true,
        created_at_ms: iface.created_at_ms,
        updated_at_ms: iface.updated_at_ms,
        deleted_at_ms: iface.deleted_at_ms,
    }
}

fn json_object(s: &str) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    if s.trim().is_empty() {
        return Ok(serde_json::Map::new());
    }
    match serde_json::from_str::<serde_json::Value>(s) {
        Ok(serde_json::Value::Object(m)) => Ok(m),
        Ok(_) => Err("not a JSON object".to_owned()),
        Err(e) => Err(e.to_string()),
    }
}

/// Checks a node's JSON attributes and type before it is written.
pub fn check_node(n: &Node) -> Result<(), String> {
    if !in_catalog(n.r#type.trim()) {
        return Err(format!("type {} is not in the catalog", n.r#type));
    }
    let props = json_object(&n.props_json).map_err(|e| format!("props: {e}"))?;
    if n.r#type.trim() == "component" {
        let id = props
            .get("ref")
            .and_then(serde_json::Value::as_str)
            .ok_or("component: props.ref must name a component")?;
        ObjectId::parse(id).map_err(|_| "component: props.ref is not an id".to_owned())?;
        if let Some(p) = props.get("params") {
            if !p.is_object() {
                return Err("component: props.params must be an object".to_owned());
            }
        }
    }
    let bindings = json_object(&n.bindings_json).map_err(|e| format!("bindings: {e}"))?;
    for (prop, expr) in &bindings {
        let Some(expr) = expr.as_str() else {
            return Err(format!("binding {prop}: not a string"));
        };
        let diagnostics = script::check(&format!("return {expr}"));
        if !diagnostics.is_empty() {
            return Err(format!("binding {prop}: {}", diagnostics.join("; ")));
        }
    }
    let handlers = json_object(&n.handlers_json).map_err(|e| format!("handlers: {e}"))?;
    for (event, src) in &handlers {
        let Some(src) = src.as_str() else {
            return Err(format!("handler {event}: not a string"));
        };
        let diagnostics = script::check(src);
        if !diagnostics.is_empty() {
            return Err(format!("handler {event}: {}", diagnostics.join("; ")));
        }
    }
    Ok(())
}

/// Evaluates every binding of an interface against the Space and `state`,
/// merging the results over the literal properties.
pub async fn evaluate(
    host: &Host,
    space: SpaceId,
    iface: &Interface,
    state: &serde_json::Value,
) -> Vec<NodeValues> {
    evaluate_nodes(host, space, &iface.nodes, state, &serde_json::Value::Null).await
}

/// Evaluates the bindings of `nodes` with `state` and `params` in scope.
pub async fn evaluate_nodes(
    host: &Host,
    space: SpaceId,
    nodes: &[Node],
    state: &serde_json::Value,
    params: &serde_json::Value,
) -> Vec<NodeValues> {
    let mut exprs: Vec<(String, String, String)> = Vec::new();
    let mut literal: BTreeMap<String, serde_json::Map<String, serde_json::Value>> = BTreeMap::new();
    for n in nodes {
        literal.insert(n.id.clone(), json_object(&n.props_json).unwrap_or_default());
        if let Ok(b) = json_object(&n.bindings_json) {
            for (prop, expr) in b {
                if let Some(e) = expr.as_str() {
                    exprs.push((n.id.clone(), prop, e.to_owned()));
                }
            }
        }
    }
    let results = host.evaluate(space, &exprs, state, params).await;
    let mut out = Vec::with_capacity(nodes.len());
    for n in nodes {
        let mut values = literal.remove(&n.id).unwrap_or_default();
        let mut errors = std::collections::HashMap::new();
        for ((node, prop, _), r) in exprs.iter().zip(&results) {
            if node != &n.id {
                continue;
            }
            match r {
                Ok(v) => {
                    values.insert(prop.clone(), v.clone());
                }
                Err(e) => {
                    errors.insert(prop.clone(), e.clone());
                }
            }
        }
        out.push(NodeValues {
            node_id: n.id.clone(),
            values_json: serde_json::Value::Object(values).to_string(),
            errors,
        });
    }
    out
}

/// One event aimed at a node.
pub struct UiEvent<'a> {
    /// The node.
    pub node_id: &'a str,
    /// `click`, `change`, `submit`, `select`, ...
    pub event: &'a str,
    /// Local state before the handler.
    pub state: serde_json::Value,
    /// What the renderer attached.
    pub payload: serde_json::Value,
    /// The component parameters in scope, or null outside a component.
    pub params: serde_json::Value,
}

/// Runs a node's handler for an event. Returns the state afterwards, the
/// logs, and the error if any.
pub async fn dispatch(
    host: &Host,
    space: SpaceId,
    owner: &Interface,
    node: &Node,
    grant_id: String,
    ev: UiEvent<'_>,
) -> (serde_json::Value, Vec<String>, String) {
    let UiEvent {
        node_id,
        event,
        state,
        payload,
        params,
    } = ev;
    if node.id != node_id {
        return (state, Vec::new(), format!("no node {node_id}"));
    }
    let handlers = match json_object(&node.handlers_json) {
        Ok(h) => h,
        Err(e) => return (state, Vec::new(), format!("handlers: {e}")),
    };
    let Some(src) = handlers.get(event).and_then(|v| v.as_str()) else {
        return (state, Vec::new(), String::new());
    };
    let identity = RunIdentity {
        script_id: owner.id.clone(),
        rule_id: None,
        grant_id,
    };
    host.handle(space, src, state, payload, params, identity)
        .await
}

/// Kinds an interface may be stored as: interfaces and components.
pub fn is_interface(o: &Object) -> bool {
    matches!(o.kind(), ObjectKind::Interface | ObjectKind::Component)
}

/// Device setting key for the selected version of a draft.
pub fn selected_key(draft: ObjectId) -> String {
    format!("{SELECTED}{draft}")
}
