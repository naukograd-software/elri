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
/// Reserved field: schedule interval in seconds; 0 = none.
pub const EVERY_SECONDS: &str = "$every_seconds";
/// Reserved field: whether events fire the rule.
pub const ON_EVENTS: &str = "$on_events";
/// How often the scheduler looks for due rules.
const SCHEDULER_TICK: std::time::Duration = std::time::Duration::from_secs(15);
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
            every_seconds: crate::model::int_field(o, EVERY_SECONDS).unwrap_or(0),
            on_events: matches!(
                o.fields.get(ON_EVENTS).and_then(|v| v.value.as_ref()),
                Some(elri_proto::elri::v1::object::value::Value::Bool(true))
            ),
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
    doc.set_field(EVERY_SECONDS, &int_value(t.every_seconds.max(0)))?;
    // Events fire unless the rule is schedule-only: a schedule with
    // `on_events` left unset means schedule only.
    let on_events = t.on_events || t.every_seconds <= 0;
    doc.set_field(ON_EVENTS, &crate::model::bool_value(on_events))?;
    Ok(())
}

/// Whether events fire the rule.
pub fn fires_on_events(t: &Trigger) -> bool {
    t.on_events || t.every_seconds <= 0
}

/// Watches the event stream and the clock and runs the rules of this
/// device: on matching changes, and on schedule.
pub fn spawn_runner(store: Arc<Store>, host: Host, device_id: String) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut events = store.watch();
        let mut tick = tokio::time::interval(SCHEDULER_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let ev = tokio::select! {
                ev = events.recv() => ev,
                _ = tick.tick() => {
                    if let Err(e) = run_due(&store, &host, &device_id).await {
                        tracing::warn!(error = %e, "scheduled rules failed");
                    }
                    continue;
                }
            };
            let ev = match ev {
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
            if o.kind == ObjectKind::Rule as i32 {
                // A new or changed rule with a schedule gets its first run
                // now rather than at the next tick.
                if let Err(e) = run_due(&store, &host, &device_id).await {
                    tracing::warn!(error = %e, "scheduled rules failed");
                }
                continue;
            }
            if o.removed || o.kind == ObjectKind::Script as i32 {
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
        if !fires_on_events(&trigger) || !script::kind_matches(&trigger.kinds, o.kind) {
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
        let args = serde_json::json!({
            "event": {
                "object_id": o.object_id,
                "space_id": o.space_id,
                "kind": ObjectKind::try_from(o.kind).map(|k| k.as_str_name().to_lowercase()).unwrap_or_default(),
                "actor": o.actor,
            },
            "rule": r.id,
        });
        run_rule(store, host, &space, space_id, &r, args, "event").await?;
    }
    Ok(())
}

/// Runs every scheduled rule of this device whose interval has elapsed.
/// A rule the device slept through runs once, not once per missed tick.
async fn run_due(
    store: &Arc<Store>,
    host: &Host,
    device_id: &str,
) -> Result<(), crate::script::ScriptError> {
    let now = crate::model::now_ms();
    for info in store.list_spaces().await? {
        let space_id = ObjectId::parse(&info.id).map_err(crate::store::StoreError::from)?;
        let space = store.space(space_id).await?;
        let rules = space
            .list(&ObjectFilter {
                kind: Some(ObjectKind::Rule),
                limit: 1000,
                ..ObjectFilter::default()
            })
            .await?;
        for r in rules.iter().map(rule_of) {
            let every = r.trigger.as_ref().map_or(0, |t| t.every_seconds);
            if !r.enabled || r.executor_device_id != device_id || every <= 0 {
                continue;
            }
            let Ok(rule_id) = ObjectId::parse(&r.id) else {
                continue;
            };
            let last = store.rule_last_scheduled(rule_id).await?;
            if now - last < every * 1000 {
                continue;
            }
            store.set_rule_last_scheduled(rule_id, now).await?;
            let args = serde_json::json!({ "schedule": { "at": now }, "rule": r.id });
            run_rule(store, host, &space, space_id, &r, args, "schedule").await?;
        }
    }
    Ok(())
}

/// Runs a rule's script under its grant and records the run.
async fn run_rule(
    store: &Arc<Store>,
    host: &Host,
    space: &crate::store::SpaceStore,
    space_id: SpaceId,
    r: &Rule,
    args: serde_json::Value,
    cause: &str,
) -> Result<(), crate::script::ScriptError> {
    let started = crate::model::now_ms();
    let Ok(script_id) = ObjectId::parse(&r.script_id) else {
        return Ok(());
    };
    let outcome = match space.get(script_id).await {
        Ok(script_obj) => {
            let script = script::script_of(&script_obj)?;
            match grant_or_auto(store, space_id, &script).await? {
                Some(grant) => {
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
                    result.error
                }
                None => "no grant for this version of the script on this device".to_owned(),
            }
        }
        Err(_) => "rule names a missing script".to_owned(),
    };
    if outcome.is_empty() {
        tracing::debug!(rule = %r.id, cause, "rule ran");
    } else {
        tracing::warn!(rule = %r.id, cause, error = %outcome, "rule run failed");
    }
    store
        .record_rule_run(&elri_proto::elri::v1::script::RuleRun {
            rule_id: r.id.clone(),
            started_at_ms: started,
            duration_ms: crate::model::now_ms() - started,
            cause: cause.to_owned(),
            error: outcome,
        })
        .await?;
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
