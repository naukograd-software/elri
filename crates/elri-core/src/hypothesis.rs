//! Hypotheses: Layers an agent proposed over an Object, with who proposed
//! them, what they rest on, and why. A person's review is recorded apart
//! from the author; only the proposing agent may revise one.

use elri_proto::elri::v1::object::{Hypothesis, HypothesisStatus, Object};

use crate::model::{
    bool_value, int_field, int_value, now_ms, reserved, string_field, string_value, ModelError,
    ObjectDoc, ObjectId,
};

/// What a script states when it proposes a hypothesis.
#[derive(Debug, Clone, Default)]
pub struct Proposal {
    /// Model identification, if any.
    pub model: String,
    /// Object ids the proposal rests on.
    pub sources: Vec<String>,
    /// The agent's reason.
    pub basis: String,
}

/// Longest basis or note kept.
pub const TEXT_LIMIT: usize = 4096;
/// Most sources recorded.
pub const SOURCES_LIMIT: usize = 200;

/// Checks a proposal before it is written.
pub fn check(p: &Proposal) -> Result<(), String> {
    if p.basis.len() > TEXT_LIMIT || p.model.len() > 256 {
        return Err("hypothesis: basis or model too long".to_owned());
    }
    if p.sources.len() > SOURCES_LIMIT {
        return Err("hypothesis: too many sources".to_owned());
    }
    for s in &p.sources {
        ObjectId::parse(s).map_err(|_| format!("hypothesis: source {s} is not an id"))?;
    }
    Ok(())
}

/// Marks a fresh Layer document as a hypothesis of `agent`.
pub fn write(doc: &ObjectDoc, agent: &str, p: &Proposal) -> Result<(), ModelError> {
    doc.set_field(reserved::HYPOTHESIS, &bool_value(true))?;
    doc.set_field(reserved::AGENT, &string_value(agent))?;
    if !p.model.is_empty() {
        doc.set_field(reserved::MODEL, &string_value(&p.model))?;
    }
    doc.set_field(
        reserved::SOURCES,
        &string_value(serde_json::Value::from(p.sources.clone()).to_string()),
    )?;
    if !p.basis.is_empty() {
        doc.set_field(reserved::BASIS, &string_value(&p.basis))?;
    }
    doc.set_field(
        reserved::STATUS,
        &int_value(i64::from(HypothesisStatus::Proposed as i32)),
    )?;
    Ok(())
}

/// Records a person's review.
pub fn review(doc: &ObjectDoc, status: HypothesisStatus, note: &str) -> Result<(), ModelError> {
    doc.set_field(reserved::STATUS, &int_value(i64::from(status as i32)))?;
    doc.set_field(reserved::REVIEWED_BY, &string_value("user"))?;
    doc.set_field(reserved::REVIEWED_AT, &int_value(now_ms()))?;
    if note.is_empty() {
        doc.remove_field(reserved::NOTE)?;
    } else {
        doc.set_field(reserved::NOTE, &string_value(note))?;
    }
    Ok(())
}

/// Whether the Object is a hypothesis.
pub fn is_hypothesis(o: &Object) -> bool {
    crate::model::bool_field(o, reserved::HYPOTHESIS) == Some(true)
}

/// The hypothesis an Object carries, if it is one.
pub fn hypothesis_of(o: &Object) -> Option<Hypothesis> {
    if !is_hypothesis(o) {
        return None;
    }
    let sources: Vec<String> =
        serde_json::from_str(string_field(o, reserved::SOURCES).unwrap_or("[]"))
            .unwrap_or_default();
    Some(Hypothesis {
        agent: string_field(o, reserved::AGENT)
            .unwrap_or_default()
            .to_owned(),
        model: string_field(o, reserved::MODEL)
            .unwrap_or_default()
            .to_owned(),
        sources,
        basis: string_field(o, reserved::BASIS)
            .unwrap_or_default()
            .to_owned(),
        status: int_field(o, reserved::STATUS)
            .and_then(|c| i32::try_from(c).ok())
            .unwrap_or(HypothesisStatus::Proposed as i32),
        reviewed_by: string_field(o, reserved::REVIEWED_BY)
            .unwrap_or_default()
            .to_owned(),
        reviewed_at_ms: int_field(o, reserved::REVIEWED_AT).unwrap_or(0),
        note: string_field(o, reserved::NOTE)
            .unwrap_or_default()
            .to_owned(),
    })
}

/// Whether `actor` may revise the Object: anything that is not a
/// hypothesis, or a hypothesis it proposed itself.
pub fn may_revise(o: &Object, actor: &str) -> Result<(), String> {
    match hypothesis_of(o) {
        Some(h) if h.agent != actor => Err(format!(
            "hypothesis {} belongs to {}; a person reviews it",
            o.id, h.agent
        )),
        _ => Ok(()),
    }
}
