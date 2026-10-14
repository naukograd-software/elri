//! Storage-level synchronization: a device's outbox is admitted by an
//! authority and delivered to another device.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use elri_core::change::{rejected, Outcome, Reason};
use elri_core::device::DeviceId;
use elri_core::model::{int_value, ref_value, string_value, ObjectDoc, ObjectId, SpaceId};
use elri_core::store::{ObjectFilter, Store};
use elri_proto::elri::v1::change::ChangeStatus;
use elri_proto::elri::v1::object::{value, ObjectKind, Value};
use elri_proto::elri::v1::space::Authority;

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-syncstore-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

async fn store(name: &str) -> Store {
    let dir = tmp_dir(name);
    let device = DeviceId::load_or_create(&dir).expect("device");
    Store::open(&dir, device).await.expect("store")
}

fn bool_value(b: bool) -> Value {
    Value {
        value: Some(value::Value::Bool(b)),
    }
}

fn seq_of(outcome: &Outcome) -> i64 {
    match outcome {
        Outcome::AcceptedSeq(seq) => *seq,
        Outcome::Rejected(r) => panic!("expected acceptance, got {r:?}"),
    }
}

#[tokio::test]
async fn outbox_is_admitted_and_delivered() {
    let space_id = SpaceId::new();
    let a = store("a").await;
    let server = store("server").await;
    let b = store("b").await;
    for s in [&a, &server, &b] {
        s.register_space(space_id, "shared", Authority::Server)
            .await
            .expect("register");
    }
    let a_space = a.space(space_id).await.expect("a space");
    let s_space = server.space(space_id).await.expect("server space");
    let b_space = b.space(space_id).await.expect("b space");

    // A creates and then edits an object: two local Changes.
    let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, 1).expect("doc");
    doc.set_field("title", &string_value("hello")).expect("set");
    doc.touch(2).expect("touch");
    let created = a_space.insert(&doc).await.expect("insert");
    let created_id = ObjectId::parse(&created.id).expect("id");
    a_space
        .update(created_id, |d| d.set_field("done", &bool_value(true)))
        .await
        .expect("update");
    let outbox = a_space.outbox(0).await.expect("outbox");
    assert_eq!(outbox.len(), 2);

    // The authority admits them in order.
    let mut seqs = Vec::new();
    for env in &outbox {
        let outcome = s_space.admit(env).await.expect("admit");
        seqs.push(seq_of(&outcome));
        a_space
            .record_outcome(&env.id, &outcome)
            .await
            .expect("record");
    }
    assert_eq!(seqs, vec![1, 2]);
    assert!(a_space.outbox(0).await.expect("outbox").is_empty());
    assert_eq!(s_space.last_seq().await.expect("seq"), 2);
    let on_server = s_space.get(created_id).await.expect("server get");
    assert_eq!(on_server.fields["title"], string_value("hello"));
    assert_eq!(on_server.fields["done"], bool_value(true));

    // Resubmitting returns the recorded outcome, no new log entry.
    let again = s_space.admit(&outbox[0]).await.expect("resubmit");
    assert_eq!(again, Outcome::AcceptedSeq(1));
    assert_eq!(s_space.last_seq().await.expect("seq"), 2);

    // B pulls the log and installs it.
    let entries = s_space.log_after(0, 0).await.expect("log");
    assert_eq!(entries.len(), 2);
    for e in &entries {
        b_space
            .apply_accepted(e.seq, e.change.as_ref().expect("change"))
            .await
            .expect("apply");
    }
    assert_eq!(b_space.cursor().await.expect("cursor"), 2);
    let on_b = b_space.get(created_id).await.expect("b get");
    assert_eq!(on_b.fields, on_server.fields);
    assert_eq!(on_b.kind(), ObjectKind::Entity);
    let listed = b_space.list(&ObjectFilter::default()).await.expect("list");
    assert_eq!(listed.len(), 1);

    // B edits; the authority admits; A installs — the circle closes.
    b_space
        .update(created_id, |d| {
            d.set_field("title", &string_value("hello from b"))
        })
        .await
        .expect("b update");
    let b_out = b_space.outbox(0).await.expect("b outbox");
    assert_eq!(b_out.len(), 1);
    let outcome = s_space.admit(&b_out[0]).await.expect("admit b");
    assert_eq!(outcome, Outcome::AcceptedSeq(3));
    b_space
        .record_outcome(&b_out[0].id, &outcome)
        .await
        .expect("record b");
    for e in s_space.log_after(2, 0).await.expect("log 2") {
        a_space
            .apply_accepted(e.seq, e.change.as_ref().expect("change"))
            .await
            .expect("apply on a");
    }
    let on_a = a_space.get(created_id).await.expect("a get");
    assert_eq!(on_a.fields["title"], string_value("hello from b"));
    assert_eq!(on_a.fields["done"], bool_value(true));
    assert_eq!(a_space.cursor().await.expect("cursor a"), 3);
}

#[tokio::test]
async fn missing_predecessors_are_rejected() {
    let space_id = SpaceId::new();
    let a = store("a2").await;
    let server = store("server2").await;
    for s in [&a, &server] {
        s.register_space(space_id, "shared", Authority::Server)
            .await
            .expect("register");
    }
    let a_space = a.space(space_id).await.expect("a space");
    let s_space = server.space(space_id).await.expect("server space");

    let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, 1).expect("doc");
    doc.touch(2).expect("touch");
    let created = a_space.insert(&doc).await.expect("insert");
    let created_id = ObjectId::parse(&created.id).expect("id");
    a_space
        .update(created_id, |d| d.set_field("n", &int_value(1)))
        .await
        .expect("update");
    let outbox = a_space.outbox(0).await.expect("outbox");

    // Submit the edit without the creation it builds on.
    let outcome = s_space.admit(&outbox[1]).await.expect("admit");
    match outcome {
        Outcome::Rejected(r) => assert_eq!(r.reason(), Reason::MissingPredecessors),
        Outcome::AcceptedSeq(_) => panic!("expected rejection"),
    }
    assert_eq!(s_space.last_seq().await.expect("seq"), 0);

    // A missing-predecessor rejection is transient: once the creation is
    // admitted, the very same submission goes through.
    assert_eq!(
        s_space.admit(&outbox[0]).await.expect("admit"),
        Outcome::AcceptedSeq(1)
    );
    assert_eq!(
        s_space.admit(&outbox[1]).await.expect("admit again"),
        Outcome::AcceptedSeq(2)
    );
    let on_server = s_space.get(created_id).await.expect("get");
    assert_eq!(on_server.fields["n"], int_value(1));
}

#[tokio::test]
async fn rejection_pauses_dependents_and_rebuilds_live_state() {
    let space_id = SpaceId::new();
    let a = store("a3").await;
    a.register_space(space_id, "shared", Authority::Server)
        .await
        .expect("register");
    let space = a.space(space_id).await.expect("space");

    // c1 creates X; c2 edits X (causal); c3 creates Y referring to X
    // (semantic); c4 creates Z, independent.
    let x = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, 1).expect("doc");
    x.set_field("title", &string_value("x")).expect("set");
    x.touch(2).expect("touch");
    let x_id = x.id();
    space.insert(&x).await.expect("c1");
    space
        .update(x_id, |d| d.set_field("n", &int_value(1)))
        .await
        .expect("c2");
    let y = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, 3).expect("doc");
    y.set_field("to", &ref_value(x_id)).expect("set");
    y.touch(4).expect("touch");
    let y_id = y.id();
    space.insert(&y).await.expect("c3");
    let z = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, 5).expect("doc");
    z.touch(6).expect("touch");
    let z_id = z.id();
    space.insert(&z).await.expect("c4");

    let outbox = space.outbox(0).await.expect("outbox");
    assert_eq!(outbox.len(), 4);
    let (c1, c2, c3, c4) = (
        outbox[0].id.clone(),
        outbox[1].id.clone(),
        outbox[2].id.clone(),
        outbox[3].id.clone(),
    );
    let changes = space.list_changes(0).await.expect("list");
    let deps = |id: &str| {
        changes
            .iter()
            .find(|c| c.id == id)
            .expect("change")
            .depends_on
            .clone()
    };
    assert_eq!(deps(&c1), Vec::<String>::new());
    assert_eq!(deps(&c2), vec![c1.clone()]);
    assert_eq!(deps(&c3), vec![c1.clone()]);
    assert_eq!(deps(&c4), Vec::<String>::new());

    // The authority rejects c1: c2 and c3 pause, c4 stays in the outbox,
    // X and Y leave the live state.
    let refused = rejected(Reason::PermissionDenied, "no");
    space.record_outcome(&c1, &refused).await.expect("record");
    let status = |changes: &[elri_proto::elri::v1::change::Change], id: &str| {
        changes
            .iter()
            .find(|c| c.id == id)
            .expect("change")
            .status()
    };
    let changes = space.list_changes(0).await.expect("list");
    assert_eq!(status(&changes, &c1), ChangeStatus::Rejected);
    assert_eq!(status(&changes, &c2), ChangeStatus::Paused);
    assert_eq!(status(&changes, &c3), ChangeStatus::Paused);
    assert_eq!(status(&changes, &c4), ChangeStatus::Local);
    let outbox = space.outbox(0).await.expect("outbox");
    assert_eq!(outbox.len(), 1);
    assert_eq!(outbox[0].id, c4);
    assert!(space.get(x_id).await.is_err(), "X was only created by c1");
    assert!(space.get(y_id).await.is_err(), "Y was only created by c3");
    space.get(z_id).await.expect("Z is untouched");
    let listed = space.list(&ObjectFilter::default()).await.expect("list");
    assert_eq!(listed.len(), 1);

    // Retry brings everything back, in the outbox and in the live state.
    let c1_again = space.retry_change(&c1).await.expect("retry");
    assert_eq!(c1_again.status(), ChangeStatus::Local);
    let changes = space.list_changes(0).await.expect("list");
    assert_eq!(status(&changes, &c2), ChangeStatus::Local);
    assert_eq!(status(&changes, &c3), ChangeStatus::Local);
    assert_eq!(space.outbox(0).await.expect("outbox").len(), 4);
    let x_live = space.get(x_id).await.expect("X is back");
    assert_eq!(x_live.fields["title"], string_value("x"));
    assert_eq!(x_live.fields["n"], int_value(1));
    space.get(y_id).await.expect("Y is back");

    // Reject again; only rejected Changes can be retried.
    space.record_outcome(&c1, &refused).await.expect("record");
    assert!(space.retry_change(&c4).await.is_err(), "c4 is not rejected");

    // Discard drops the rejected Change and its paused dependents; the
    // independent one is untouched.
    let dropped = space.discard_change(&c1).await.expect("discard");
    assert_eq!(dropped.len(), 3);
    assert!(dropped.contains(&c1) && dropped.contains(&c2) && dropped.contains(&c3));
    let changes = space.list_changes(0).await.expect("list");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].id, c4);
    assert!(space.discard_change(&c1).await.is_err());
}

#[tokio::test]
async fn rejected_edit_leaves_accepted_state_intact() {
    let space_id = SpaceId::new();
    let a = store("a4").await;
    let server = store("server4").await;
    for s in [&a, &server] {
        s.register_space(space_id, "shared", Authority::Server)
            .await
            .expect("register");
    }
    let a_space = a.space(space_id).await.expect("a space");
    let s_space = server.space(space_id).await.expect("server space");

    // Creation accepted; the edit rejected: the object stays as accepted.
    let doc = ObjectDoc::create(ObjectId::new(), space_id, ObjectKind::Entity, 1).expect("doc");
    doc.set_field("title", &string_value("accepted"))
        .expect("set");
    doc.touch(2).expect("touch");
    let id = doc.id();
    a_space.insert(&doc).await.expect("insert");
    let outbox = a_space.outbox(0).await.expect("outbox");
    let outcome = s_space.admit(&outbox[0]).await.expect("admit");
    a_space
        .record_outcome(&outbox[0].id, &outcome)
        .await
        .expect("record");
    a_space
        .update(id, |d| d.set_field("title", &string_value("rejected")))
        .await
        .expect("update");
    let outbox = a_space.outbox(0).await.expect("outbox");
    assert_eq!(outbox.len(), 1);
    let changes = a_space.list_changes(0).await.expect("list");
    assert!(
        changes[0].depends_on.is_empty(),
        "built on accepted state only"
    );
    a_space
        .record_outcome(&outbox[0].id, &rejected(Reason::PermissionDenied, "no"))
        .await
        .expect("record");
    let live = a_space.get(id).await.expect("get");
    assert_eq!(live.fields["title"], string_value("accepted"));

    // New work on the rebuilt document goes out on its own.
    a_space
        .update(id, |d| d.set_field("title", &string_value("repaired")))
        .await
        .expect("repair");
    let outbox = a_space.outbox(0).await.expect("outbox");
    assert_eq!(outbox.len(), 1);
    let outcome = s_space.admit(&outbox[0]).await.expect("admit repair");
    assert_eq!(outcome, Outcome::AcceptedSeq(2));
    let on_server = s_space.get(id).await.expect("server get");
    assert_eq!(on_server.fields["title"], string_value("repaired"));
}

#[tokio::test]
async fn full_text_search_finds_live_text() {
    let space_id = SpaceId::new();
    let dir = tmp_dir("search");
    let device = DeviceId::load_or_create(&dir).expect("device");
    let a = Store::open(&dir, device).await.expect("store");
    a.register_space(space_id, "s", Authority::Local)
        .await
        .expect("register");
    let space = a.space(space_id).await.expect("space");

    let make = |title: &str, body: &str, kind: ObjectKind| {
        let doc = ObjectDoc::create(ObjectId::new(), space_id, kind, 1).expect("doc");
        doc.set_field("title", &string_value(title)).expect("set");
        if !body.is_empty() {
            doc.set_field(
                "body",
                &Value {
                    value: Some(value::Value::Text(body.to_owned())),
                },
            )
            .expect("set");
        }
        doc.touch(2).expect("touch");
        doc
    };
    let apples = make(
        "Apple pie recipe",
        "peel the apples, add cinnamon",
        ObjectKind::Entity,
    );
    let pears = make("Pear tart", "slice pears thinly", ObjectKind::Entity);
    let layer = ObjectDoc::create_layer(ObjectId::new(), space_id, apples.id(), 1).expect("layer");
    layer
        .set_field("note", &string_value("my apples are green"))
        .expect("set");
    layer.touch(2).expect("touch");
    for d in [&apples, &pears, &layer] {
        space.insert(d).await.expect("insert");
    }

    let hits = space.search("apples", None, 0).await.expect("search");
    let ids: Vec<_> = hits.iter().map(|(o, _, _)| o.id.clone()).collect();
    assert_eq!(hits.len(), 2);
    assert!(ids.contains(&apples.id().to_string()));
    assert!(ids.contains(&layer.id().to_string()));
    let (_, _, fields) = hits
        .iter()
        .find(|(o, _, _)| o.id == apples.id().to_string())
        .expect("apples hit");
    // No stemming: "apples" is not "Apple", so only the body matched.
    assert_eq!(fields, &vec!["body".to_owned()]);
    let both = space
        .search("apple OR apples", None, 0)
        .await
        .expect("search");
    let (_, _, fields) = both
        .iter()
        .find(|(o, _, _)| o.id == apples.id().to_string())
        .expect("apples hit");
    assert_eq!(fields, &vec!["body".to_owned(), "title".to_owned()]);

    // Kind filter; a deleted object drops out; an edit re-indexes.
    let only_entities = space
        .search("apples", Some(ObjectKind::Entity), 0)
        .await
        .expect("search");
    assert_eq!(only_entities.len(), 1);
    space
        .update(pears.id(), |d| d.mark_deleted(3))
        .await
        .expect("delete");
    assert!(space
        .search("pears", None, 0)
        .await
        .expect("search")
        .is_empty());
    space
        .update(apples.id(), |d| {
            d.set_field("title", &string_value("Quince jam"))
        })
        .await
        .expect("edit");
    let quince = space.search("quince", None, 0).await.expect("search");
    assert_eq!(quince.len(), 1);
    assert_eq!(quince[0].2, vec!["title".to_owned()]);
    let apples_now = space.search("apples", None, 0).await.expect("search");
    assert_eq!(
        apples_now.len(),
        2,
        "body still mentions apples; so does the layer"
    );

    // The index survives reopening the store.
    drop(space);
    drop(a);
    let b = Store::open(&dir, device).await.expect("store again");
    let space = b.space(space_id).await.expect("space");
    assert_eq!(
        space
            .search("cinnamon", None, 0)
            .await
            .expect("search")
            .len(),
        1
    );
    assert!(space
        .search("nothing-here", None, 0)
        .await
        .expect("search")
        .is_empty());
}
