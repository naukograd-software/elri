//! Rules: a script that runs on one device when an Object of its Space
//! changes. The runner watches the event stream and fires rules whose
//! trigger matches; Changes made by scripts and rules never fire rules,
//! so a rule cannot chain into itself.

use std::sync::Arc;

use elri_proto::elri::v1::events::{event, NoticeEvent};
use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::script::{MissedRuns, NotifyPolicy, Rule, RuleRun, Trigger};
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
/// Reserved field: `MissedRuns` code.
pub const MISSED: &str = "$missed";
/// Reserved field: `NotifyPolicy` code.
pub const NOTIFY: &str = "$notify";
/// Device setting: script runs executed at once.
pub const MAX_CONCURRENT_SCRIPTS: &str = "max_concurrent_scripts";
/// Most slots a catch-up runs for.
const CATCH_UP_LIMIT: i64 = 10;
/// Longest schedule period honoured: a year.
const MAX_EVERY_SECONDS: i64 = 366 * 24 * 60 * 60;
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
            missed: crate::model::int_field(o, MISSED)
                .and_then(|m| i32::try_from(m).ok())
                .unwrap_or(0),
        }),
        executor_device_id: string_field(o, EXECUTOR).unwrap_or_default().to_owned(),
        enabled: matches!(
            o.fields.get(ENABLED).and_then(|v| v.value.as_ref()),
            Some(elri_proto::elri::v1::object::value::Value::Bool(true))
        ),
        created_at_ms: o.created_at_ms,
        updated_at_ms: o.updated_at_ms,
        deleted_at_ms: o.deleted_at_ms,
        notify: crate::model::int_field(o, NOTIFY)
            .and_then(|m| i32::try_from(m).ok())
            .unwrap_or(0),
    }
}

/// Writes the notification policy onto a rule document.
pub fn write_notify(doc: &ObjectDoc, n: NotifyPolicy) -> Result<(), crate::model::ModelError> {
    doc.set_field(NOTIFY, &int_value(i64::from(n as i32)))
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
    doc.set_field(MISSED, &int_value(i64::from(t.missed)))?;
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

/// Runs every scheduled rule of this device whose interval has elapsed,
/// treating slots the device slept through as the rule's missed-runs
/// policy says.
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
            let trigger = r.trigger.clone().unwrap_or_default();
            let every = trigger.every_seconds;
            if !r.enabled || r.executor_device_id != device_id || every <= 0 {
                continue;
            }
            let Ok(rule_id) = ObjectId::parse(&r.id) else {
                continue;
            };
            // A period is at least a second and at most a year, whatever
            // the rule says, so the arithmetic below cannot overflow.
            let period = every.clamp(1, MAX_EVERY_SECONDS).saturating_mul(1000);
            let last = store.rule_last_scheduled(rule_id).await?;
            if last > 0 && now.saturating_sub(last) < period {
                continue;
            }
            // Slots since the last run; the first is due, the rest were
            // missed while the device was away.
            let slots = if last > 0 {
                (now.saturating_sub(last) / period).max(1)
            } else {
                1
            };
            let missed = slots - 1;
            store.set_rule_last_scheduled(rule_id, now).await?;
            let at = |slot: i64| {
                serde_json::json!({
                    "schedule": { "at": last.saturating_add(slot.saturating_mul(period)) },
                    "rule": r.id,
                })
            };
            match trigger.missed() {
                MissedRuns::Skip if missed > 0 => {
                    tracing::info!(rule = %r.id, missed, "skipped missed scheduled runs");
                }
                MissedRuns::CatchUp if missed > 0 => {
                    let first = (slots - CATCH_UP_LIMIT).max(1);
                    for slot in first..=slots {
                        run_rule(store, host, &space, space_id, &r, at(slot), "catch-up").await?;
                    }
                }
                MissedRuns::Ask if missed > 0 => {
                    let text = format!(
                        "rule {} missed {missed} scheduled run(s) while this device was away; run it by hand or change its missed-runs policy",
                        r.name
                    );
                    store.emit(event::Event::Notice(NoticeEvent {
                        space_id: space_id.to_string(),
                        source: format!("rule:{}", r.id),
                        text: text.clone(),
                    }));
                    store
                        .record_rule_run(&RuleRun {
                            rule_id: r.id.clone(),
                            started_at_ms: now,
                            duration_ms: 0,
                            cause: "missed".to_owned(),
                            error: text,
                        })
                        .await?;
                }
                _ => {
                    let args = serde_json::json!({ "schedule": { "at": now }, "rule": r.id });
                    run_rule(store, host, &space, space_id, &r, args, "schedule").await?;
                }
            }
        }
    }
    Ok(())
}

/// Runs a rule now on this device, whatever its trigger, and records the
/// run with cause `manual`.
pub async fn run_now(
    store: &Arc<Store>,
    host: &Host,
    space_id: SpaceId,
    rule_id: ObjectId,
) -> Result<RuleRun, crate::script::ScriptError> {
    let space = store.space(space_id).await?;
    let rule = space.get(rule_id).await?;
    if rule.kind() != ObjectKind::Rule {
        return Err(crate::store::StoreError::ObjectNotFound(rule_id).into());
    }
    let r = rule_of(&rule);
    // Shaped like a scheduled run, so a script written for the schedule
    // works when run by hand; `manual` tells the two apart.
    let args = serde_json::json!({
        "schedule": { "at": crate::model::now_ms() },
        "manual": true,
        "rule": r.id,
    });
    run_rule(store, host, &space, space_id, &r, args, "manual").await
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
) -> Result<RuleRun, crate::script::ScriptError> {
    let started = crate::model::now_ms();
    let outcome = match ObjectId::parse(&r.script_id) {
        Err(_) => "rule names no script".to_owned(),
        Ok(script_id) => match space.get(script_id).await {
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
        },
    };
    if outcome.is_empty() {
        tracing::debug!(rule = %r.id, cause, "rule ran");
    } else {
        tracing::warn!(rule = %r.id, cause, error = %outcome, "rule run failed");
    }
    let run = RuleRun {
        rule_id: r.id.clone(),
        started_at_ms: started,
        duration_ms: crate::model::now_ms() - started,
        cause: cause.to_owned(),
        error: outcome,
    };
    store.record_rule_run(&run).await?;
    let tell = match r.notify() {
        NotifyPolicy::Never => false,
        NotifyPolicy::Always => true,
        NotifyPolicy::Unspecified | NotifyPolicy::OnError => !run.error.is_empty(),
    };
    if tell {
        store.emit(event::Event::Notice(NoticeEvent {
            space_id: space_id.to_string(),
            source: format!("rule:{}", r.id),
            text: if run.error.is_empty() {
                format!("rule {} ran ({cause})", r.name)
            } else {
                format!("rule {} failed ({cause}): {}", r.name, run.error)
            },
        }));
    }
    Ok(run)
}

fn agent_written_key(script: &elri_proto::elri::v1::script::Script) -> String {
    format!("agent_written:{}:{}", script.id, script.version)
}

/// Records that an agent wrote this version of a script: it is never
/// granted without asking, benign or not, so a rule cannot pick up code
/// an agent left behind.
pub async fn mark_agent_written(
    store: &Store,
    script: &elri_proto::elri::v1::script::Script,
) -> Result<(), crate::store::StoreError> {
    store.set_setting(&agent_written_key(script), 1).await
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
    if script.benign
        && store.setting(AUTO_GRANT_BENIGN, 1).await? != 0
        && store.setting(&agent_written_key(script), 0).await? == 0
    {
        return Ok(Some(
            store
                .grant(space_id, script_id, &script.version, true)
                .await?,
        ));
    }
    Ok(None)
}
