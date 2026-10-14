//! Rules: a script that runs on one device when an Object of its Space
//! changes. The runner watches the event stream and fires rules whose
//! trigger matches; Changes made by scripts and rules never fire rules,
//! so a rule cannot chain into itself.

use std::sync::Arc;

use elri_proto::elri::v1::events::event;
use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::script::{Rule, Trigger};
use tokio::task::JoinHandle;

use crate::model::{
    int_value, list_value, reserved, string_field, string_value, ObjectDoc, ObjectId, SpaceId,
};
use crate::script::{self, Host, RunIdentity};
use crate::store::{ObjectFilter, Store, ACTOR_USER};

/// Reserved field: the script a rule runs.
pub const SCRIPT: &str = "$script";
/// Reserved field: the device that runs the rule.
pub const EXECUTOR: &str = "$executor";
/// Reserved field: whether the rule fires.
pub const ENABLED: &str = "$enabled";
/// Reserved field: kinds that fire the rule, as a list of codes.
pub const TRIGGER_KINDS: &str = "$trigger_kinds";
/// Reserved field: the type that fires the rule.
pub const TRIGGER_TYPE: &str = "$trigger_type";
/// Device setting: benign scripts are granted without asking.
pub const AUTO_GRANT_BENIGN: &str = "auto_grant_benign";

/// The protocol view of a rule Object.
pub fn rule_of(o: &Object) -> Rule {
    let kinds = match o.fields.get(TRIGGER_KINDS).and_then(|v| v.value.as_ref()) {
        Some(elri_proto::elri::v1::object::value::Value::List(l)) => l
            .items
            .iter()
            .filter_map(|v| match v.value {
                Some(elri_proto::elri::v1::object::value::Value::Int(i)) => i32::try_from(i).ok(),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    };
    Rule {
        id: o.id.clone(),
        space_id: o.space_id.clone(),
        name: string_field(o, reserved::NAME)
            .unwrap_or_default()
            .to_owned(),
        script_id: string_field(o, SCRIPT).unwrap_or_default().to_owned(),
        trigger: Some(Trigger {
            kinds,
            type_id: string_field(o, TRIGGER_TYPE).unwrap_or_default().to_owned(),
        }),
        executor_device_id: string_field(o, EXECUTOR).unwrap_or_default().to_owned(),
        enabled: matches!(
            o.fields.get(ENABLED).and_then(|v| v.value.as_ref()),
            Some(elri_proto::elri::v1::object::value::Value::Bool(true))
        ),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
    }
}

/// Writes a trigger onto a rule document.
pub fn write_trigger(doc: &ObjectDoc, t: &Trigger) -> Result<(), crate::model::ModelError> {
    doc.set_field(
        TRIGGER_KINDS,
        &list_value(t.kinds.iter().map(|k| int_value(i64::from(*k))).collect()),
    )?;
    if t.type_id.trim().is_empty() {
        doc.remove_field(TRIGGER_TYPE)?;
    } else {
        doc.set_field(TRIGGER_TYPE, &string_value(t.type_id.trim()))?;
    }
    Ok(())
}

/// Watches the event stream and runs the rules of this device.
pub fn spawn_runner(store: Arc<Store>, host: Host, device_id: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut events = store.watch();
        loop {
            let ev = match events.recv().await {
                Ok(ev) => ev,
                Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!(missed = n, "rule runner fell behind the event stream");
                    continue;
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            };
            let Some(event::Event::Object(o)) = ev.event else {
                continue;
            };
            // Only direct user writes and installed remote Changes fire rules.
            if o.actor != ACTOR_USER && o.actor != crate::store::ACTOR_REMOTE {
                continue;
            }
            if o.removed || o.kind == ObjectKind::Rule as i32 || o.kind == ObjectKind::Script as i32
            {
                continue;
            }
            if let Err(e) = fire(&store, &host, &device_id, &o).await {
                tracing::warn!(error = %e, "rule run failed");
            }
        }
    })
}

async fn fire(
    store: &Arc<Store>,
    host: &Host,
    device_id: &str,
    o: &elri_proto::elri::v1::events::ObjectEvent,
) -> Result<(), crate::script::ScriptError> {
    let space_id = ObjectId::parse(&o.space_id).map_err(crate::store::StoreError::from)?;
    let space = store.space(space_id).await?;
    let rules = space
        .list(&ObjectFilter {
            kind: Some(ObjectKind::Rule),
            limit: 1000,
            ..ObjectFilter::default()
        })
        .await?;
    let mut object: Option<Object> = None;
    for r in rules.iter().map(rule_of) {
        if !r.enabled || r.executor_device_id != device_id {
            continue;
        }
        let trigger = r.trigger.clone().unwrap_or_default();
        if !script::kind_matches(&trigger.kinds, o.kind) {
            continue;
        }
        if !trigger.type_id.is_empty() {
            if object.is_none() {
                object = Some(
                    space
                        .get(
                            ObjectId::parse(&o.object_id)
                                .map_err(crate::store::StoreError::from)?,
                        )
                        .await?,
                );
            }
            if !object
                .as_ref()
                .is_some_and(|obj| obj.types.contains(&trigger.type_id))
            {
                continue;
            }
        }
        let Ok(script_id) = ObjectId::parse(&r.script_id) else {
            continue;
        };
        let Ok(script_obj) = space.get(script_id).await else {
            tracing::warn!(rule = %r.id, "rule names a missing script");
            continue;
        };
        let script = script::script_of(&script_obj)?;
        let Some(grant) = grant_or_auto(store, space_id, &script).await? else {
            tracing::info!(rule = %r.id, script = %script.id, "rule skipped: no grant for this version");
            continue;
        };
        let args = serde_json::json!({
            "event": {
                "object_id": o.object_id,
                "space_id": o.space_id,
                "kind": ObjectKind::try_from(o.kind).map(|k| k.as_str_name().to_lowercase()).unwrap_or_default(),
                "actor": o.actor,
            },
            "rule": r.id,
        });
        let result = host
            .run(
                space_id,
                &script,
                args,
                RunIdentity {
                    script_id: script.id.clone(),
                    rule_id: Some(r.id.clone()),
                    grant_id: grant.id,
                },
            )
            .await;
        if result.error.is_empty() {
            tracing::debug!(rule = %r.id, ms = result.duration_ms, "rule ran");
        } else {
            tracing::warn!(rule = %r.id, error = %result.error, "rule run failed");
        }
    }
    Ok(())
}

/// The live grant for a script's version, created on the spot for a benign
/// script when the device policy allows.
pub async fn grant_or_auto(
    store: &Store,
    space_id: SpaceId,
    script: &elri_proto::elri::v1::script::Script,
) -> Result<Option<elri_proto::elri::v1::script::Grant>, crate::store::StoreError> {
    let script_id = ObjectId::parse(&script.id)?;
    if let Some(g) = store.grant_for(script_id, &script.version).await? {
        return Ok(Some(g));
    }
    if script.benign && store.setting(AUTO_GRANT_BENIGN, 1).await? != 0 {
        return Ok(Some(
            store
                .grant(space_id, script_id, &script.version, true)
                .await?,
        ));
    }
    Ok(None)
}
