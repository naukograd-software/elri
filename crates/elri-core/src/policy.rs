//! Transfer policies: what may leave a Space, and where. The rules are an
//! Object of the Space, so every device enforces the same ones; the core
//! checks them on every operation it manages that carries data out of a
//! Space: a labelled write into another Space, an export, a move, a call
//! into an extension. Code outside the core that already holds plaintext
//! is not covered.

use std::collections::BTreeSet;

use elri_proto::elri::v1::object::{Object, ObjectKind};
use elri_proto::elri::v1::space::{TransferOp, TransferOutcome, TransferPolicy, TransferRule};

use crate::model::{reserved, string_field, ObjectId, SpaceId};
use crate::store::{ObjectFilter, Store, StoreError};

/// Why a transfer did not happen.
#[derive(Debug, Clone, thiserror::Error)]
pub enum PolicyError {
    /// A mandatory denial.
    #[error("transfer policy of space {from} denies {op} to {dest}")]
    Denied {
        /// The Space whose rule applied.
        from: String,
        /// The operation, lowercase.
        op: String,
        /// The destination, or `anywhere`.
        dest: String,
    },
    /// The rule asks the user; nothing confirmed it.
    #[error("transfer policy of space {from} asks before {op} to {dest}: confirm it")]
    Ask {
        /// The Space whose rule applied.
        from: String,
        /// The operation, lowercase.
        op: String,
        /// The destination, or `anywhere`.
        dest: String,
    },
}

fn op_name(op: TransferOp) -> String {
    op.as_str_name()
        .trim_start_matches("TRANSFER_OP_")
        .to_lowercase()
}

/// The rules as the JSON kept on the policy Object.
pub fn rules_json(rules: &[TransferRule]) -> String {
    serde_json::Value::Array(
        rules
            .iter()
            .map(|r| {
                serde_json::json!({
                    "op": r.op,
                    "destination": r.destination,
                    "outcome": r.outcome,
                })
            })
            .collect(),
    )
    .to_string()
}

/// The rules kept on a policy Object.
pub fn rules_of(o: &Object) -> Vec<TransferRule> {
    let v: serde_json::Value =
        serde_json::from_str(string_field(o, reserved::RULES).unwrap_or("[]")).unwrap_or_default();
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|r| TransferRule {
                    op: r
                        .get("op")
                        .and_then(serde_json::Value::as_i64)
                        .and_then(|i| i32::try_from(i).ok())
                        .unwrap_or(0),
                    destination: r
                        .get("destination")
                        .and_then(serde_json::Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    outcome: r
                        .get("outcome")
                        .and_then(serde_json::Value::as_i64)
                        .and_then(|i| i32::try_from(i).ok())
                        .unwrap_or(0),
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The protocol view of a policy Object.
pub fn policy_of(o: &Object) -> TransferPolicy {
    TransferPolicy {
        space_id: o.space_id.clone(),
        rules: rules_of(o),
        updated_at_ms: o.updated_at_ms,
    }
}

/// Checks a rule before it is stored.
pub fn check_rule(r: &TransferRule) -> Result<(), String> {
    if r.op() == TransferOp::Unspecified {
        return Err("rule: op is required".to_owned());
    }
    if r.outcome() == TransferOutcome::Unspecified {
        return Err("rule: outcome is required".to_owned());
    }
    let d = r.destination.trim();
    if d.is_empty() {
        return Ok(());
    }
    if let Some(name) = d.strip_prefix("extension:") {
        if name.is_empty() {
            return Err("rule: extension name is required".to_owned());
        }
        return Ok(());
    }
    ObjectId::parse(d)
        .map_err(|_| "rule: destination is a space id or extension:<name>".to_owned())?;
    Ok(())
}

/// What the rules say about `op` to `dest`: the rule naming `dest` wins
/// over the rule for any destination, among equals the later wins, and
/// without a rule the operation is allowed.
pub fn decide(rules: &[TransferRule], op: TransferOp, dest: &str) -> TransferOutcome {
    let mut any = None;
    let mut exact = None;
    for r in rules {
        if r.op() != op {
            continue;
        }
        if r.destination.trim().is_empty() {
            any = Some(r.outcome());
        } else if r.destination.trim() == dest {
            exact = Some(r.outcome());
        }
    }
    match exact.or(any) {
        Some(TransferOutcome::Unspecified) | None => TransferOutcome::Allow,
        Some(o) => o,
    }
}

/// The rules of a Space; empty without a policy Object or Space.
pub async fn rules(store: &Store, space: SpaceId) -> Result<Vec<TransferRule>, StoreError> {
    let Ok(s) = store.space(space).await else {
        return Ok(Vec::new());
    };
    let found = s
        .list(&ObjectFilter {
            kind: Some(ObjectKind::TransferPolicy),
            limit: 1,
            ..ObjectFilter::default()
        })
        .await?;
    Ok(found.first().map(rules_of).unwrap_or_default())
}

/// The policy Object of a Space, if any.
pub async fn policy_object(store: &Store, space: SpaceId) -> Result<Option<Object>, StoreError> {
    Ok(store
        .space(space)
        .await?
        .list(&ObjectFilter {
            kind: Some(ObjectKind::TransferPolicy),
            limit: 1,
            ..ObjectFilter::default()
        })
        .await?
        .into_iter()
        .next())
}

/// Checks one operation out of `from` to `dest`; `confirmed` satisfies a
/// rule that asks.
pub async fn check(
    store: &Store,
    from: SpaceId,
    op: TransferOp,
    dest: &str,
    confirmed: bool,
) -> Result<Result<(), PolicyError>, StoreError> {
    let rules = rules(store, from).await?;
    let shown = if dest.is_empty() {
        "anywhere".to_owned()
    } else {
        dest.to_owned()
    };
    Ok(match decide(&rules, op, dest) {
        TransferOutcome::Deny => Err(PolicyError::Denied {
            from: from.to_string(),
            op: op_name(op),
            dest: shown,
        }),
        TransferOutcome::Ask if !confirmed => Err(PolicyError::Ask {
            from: from.to_string(),
            op: op_name(op),
            dest: shown,
        }),
        _ => Ok(()),
    })
}

/// Checks a labelled operation: every Space read so far must allow `op`
/// to `dest`. Code cannot confirm, so a rule that asks refuses. A label
/// equal to the destination is not a transfer.
pub async fn check_labels(
    store: &Store,
    labels: &BTreeSet<String>,
    op: TransferOp,
    dest: &str,
) -> Result<(), String> {
    for label in labels {
        if label == dest {
            continue;
        }
        let Ok(from) = ObjectId::parse(label) else {
            continue;
        };
        match check(store, from, op, dest, false).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e.to_string()),
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rule(op: TransferOp, dest: &str, outcome: TransferOutcome) -> TransferRule {
        TransferRule {
            op: op.into(),
            destination: dest.into(),
            outcome: outcome.into(),
        }
    }

    #[test]
    fn the_named_destination_wins_and_the_later_rule_breaks_ties() {
        let rules = vec![
            rule(TransferOp::Copy, "", TransferOutcome::Deny),
            rule(TransferOp::Copy, "b", TransferOutcome::Ask),
            rule(TransferOp::Export, "", TransferOutcome::Allow),
            rule(TransferOp::Export, "", TransferOutcome::Ask),
        ];
        assert_eq!(decide(&rules, TransferOp::Copy, "b"), TransferOutcome::Ask);
        assert_eq!(decide(&rules, TransferOp::Copy, "c"), TransferOutcome::Deny);
        assert_eq!(decide(&rules, TransferOp::Export, ""), TransferOutcome::Ask);
        assert_eq!(
            decide(&rules, TransferOp::External, "x"),
            TransferOutcome::Allow
        );
        assert_eq!(decide(&[], TransferOp::Copy, "b"), TransferOutcome::Allow);
    }
}
