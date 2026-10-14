//! Parts of documents: fragments of a block's text captured as cursors
//! plus a quote, and blocks split or merged as one Change.

use elri_proto::elri::v1::document::{Fragment, FragmentState, Range};
use elri_proto::elri::v1::object::ObjectKind;

use crate::model::{now_ms, reserved, string_field, ObjectDoc, ObjectId, SpaceId};
use crate::store::{Store, StoreError};

/// Characters of context kept on each side of a quote.
pub const CONTEXT: usize = 20;
/// Longest quote kept.
pub const QUOTE_LIMIT: usize = 4096;
/// How many merged-into redirects are followed.
const REDIRECT_LIMIT: usize = 5;

/// Where a fragment is now.
#[derive(Debug, Clone)]
pub struct Resolved {
    /// The state.
    pub state: FragmentState,
    /// Where the cursors point, when they apply.
    pub range: Option<Range>,
    /// Where the quote appears, when ambiguous.
    pub candidates: Vec<Range>,
    /// The block followed to through merges.
    pub redirected_to: String,
    /// Why, in words.
    pub reason: String,
}

fn slice(text: &str, start: usize, end: usize) -> String {
    text.chars()
        .skip(start)
        .take(end.saturating_sub(start))
        .collect()
}

fn char_len(text: &str) -> usize {
    text.chars().count()
}

/// Captures `start..end` of a text field of a loaded block.
pub fn capture(doc: &ObjectDoc, field: &str, start: usize, end: usize) -> Result<Fragment, String> {
    let text = doc.text_of(field).ok_or("the field is not text")?;
    let len = char_len(&text);
    if start > end || end > len {
        return Err("the range is outside the text".to_owned());
    }
    if end - start > QUOTE_LIMIT {
        return Err(format!(
            "a fragment quotes at most {QUOTE_LIMIT} characters"
        ));
    }
    let (a, b) = doc
        .text_cursors(field, start, end)
        .ok_or("the field has no cursors")?;
    Ok(Fragment {
        object_id: doc.id().to_string(),
        field_id: field.to_owned(),
        start: a.into(),
        end: b.into(),
        version: doc.version().into(),
        quote: slice(&text, start, end),
        before: slice(&text, start.saturating_sub(CONTEXT), start),
        after: slice(&text, end, (end + CONTEXT).min(len)),
        captured_at_ms: now_ms(),
    })
}

/// Every place `quote` appears in `text`, as ranges.
fn occurrences(text: &str, quote: &str) -> Vec<Range> {
    if quote.is_empty() {
        return Vec::new();
    }
    let chars: Vec<char> = text.chars().collect();
    let needle: Vec<char> = quote.chars().collect();
    let mut out = Vec::new();
    let mut i = 0;
    while i + needle.len() <= chars.len() {
        if chars[i..i + needle.len()] == needle[..] {
            out.push(Range {
                start: i32::try_from(i).unwrap_or(i32::MAX),
                end: i32::try_from(i + needle.len()).unwrap_or(i32::MAX),
                text: quote.to_owned(),
            });
            i += needle.len().max(1);
        } else {
            i += 1;
        }
        if out.len() >= 50 {
            break;
        }
    }
    out
}

fn resolved(state: FragmentState, reason: &str) -> Resolved {
    Resolved {
        state,
        range: None,
        candidates: Vec::new(),
        redirected_to: String::new(),
        reason: reason.to_owned(),
    }
}

/// Resolves a fragment against the Space, following merge redirects.
pub async fn resolve(store: &Store, space: SpaceId, f: &Fragment) -> Result<Resolved, StoreError> {
    let s = store.space(space).await?;
    let mut id = ObjectId::parse(&f.object_id)?;
    let mut redirected = String::new();
    let mut hops = 0;
    let doc = loop {
        let doc = s.load(id).await?;
        let o = doc.read()?;
        if let Some(target) = string_field(&o, reserved::MERGED_INTO) {
            if hops >= REDIRECT_LIMIT {
                let mut r = resolved(FragmentState::Detached, "too many merge redirects");
                r.redirected_to = redirected;
                return Ok(r);
            }
            id = ObjectId::parse(target)?;
            target.clone_into(&mut redirected);
            hops += 1;
            continue;
        }
        if o.deleted_at_ms.is_some() {
            let mut r = resolved(FragmentState::Deleted, "the block is deleted");
            r.redirected_to = redirected;
            return Ok(r);
        }
        break doc;
    };
    let Some(text) = doc.text_of(&f.field_id) else {
        let mut r = resolved(FragmentState::Detached, "the field is gone or is not text");
        r.redirected_to = redirected;
        return Ok(r);
    };
    let len = char_len(&text);
    let found = occurrences(&text, &f.quote);
    // Cursors taken on another block do not apply after a redirect: only
    // the quote can place the fragment, and a person confirms it.
    let cursors = if redirected.is_empty() {
        doc.cursor_position(&f.start)
            .zip(doc.cursor_position(&f.end))
    } else {
        None
    };
    let mut out = match cursors {
        Some((start, end)) => {
            let (start, end) = (start.min(len), end.min(len).max(start.min(len)));
            let here = slice(&text, start, end);
            let range = Range {
                start: i32::try_from(start).unwrap_or(i32::MAX),
                end: i32::try_from(end).unwrap_or(i32::MAX),
                text: here.clone(),
            };
            if here == f.quote {
                let mut r = resolved(FragmentState::Ok, "the cursors still enclose the quote");
                r.range = Some(range);
                r
            } else if found.is_empty() {
                let mut r = resolved(
                    FragmentState::Moved,
                    "the position survives but the quoted text is gone",
                );
                r.range = Some(range);
                r
            } else {
                let mut r = resolved(
                    FragmentState::Ambiguous,
                    "the quote is elsewhere; confirm where",
                );
                r.range = Some(range);
                r.candidates = found;
                r
            }
        }
        None if !found.is_empty() => {
            let mut r = resolved(
                FragmentState::Ambiguous,
                "the cursors no longer apply; the quote is found elsewhere",
            );
            r.candidates = found;
            r
        }
        None => resolved(
            FragmentState::Detached,
            "the cursors no longer apply and the quote is not found",
        ),
    };
    out.redirected_to = redirected;
    Ok(out)
}

/// The block kind a placement must point at.
pub fn is_block(kind: ObjectKind) -> bool {
    kind == ObjectKind::Entity
}
