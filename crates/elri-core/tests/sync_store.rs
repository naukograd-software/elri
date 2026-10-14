//! Storage-level synchronization: a device's outbox is admitted by an
//! authority and delivered to another device.

#![allow(clippy::expect_used, clippy::panic)]

use elri_core::change::{Admission, RejectReason};
use elri_core::device::DeviceId;
use elri_core::model::{FieldValue, ObjectDoc, ObjectId, ObjectKind, SpaceId};
use elri_core::store::{Authority, Store};

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
    doc.set_field("title", &FieldValue::String("hello".into()))
        .expect("set");
    doc.touch(2).expect("touch");
    let created = a_space.insert(&doc).await.expect("insert");
    a_space
        .update(created.id, |d| d.set_field("done", &FieldValue::Bool(true)))
        .await
        .expect("update");
    let outbox = a_space.outbox(0).await.expect("outbox");
    assert_eq!(outbox.len(), 2);

    // The authority admits them in order.
    let mut seqs = Vec::new();
    for change in &outbox {
        let admission = s_space.admit(change).await.expect("admit");
        let Admission::Accepted(seq) = admission else {
            panic!("expected acceptance, got {admission:?}");
        };
        a_space
            .record_admission(change.id, &admission)
            .await
            .expect("record");
        seqs.push(seq);
    }
    assert_eq!(seqs, vec![1, 2]);
    assert!(a_space.outbox(0).await.expect("outbox").is_empty());
    assert_eq!(s_space.last_seq().await.expect("seq"), 2);
    let on_server = s_space.get(created.id).await.expect("server get");
    assert_eq!(
        on_server.fields["title"],
        FieldValue::String("hello".into())
    );
    assert_eq!(on_server.fields["done"], FieldValue::Bool(true));

    // Resubmitting returns the recorded outcome, no new log entry.
    let again = s_space.admit(&outbox[0]).await.expect("resubmit");
    assert_eq!(again, Admission::Accepted(1));
    assert_eq!(s_space.last_seq().await.expect("seq"), 2);

    // B pulls the log and installs it.
    let entries = s_space.log_after(0, 0).await.expect("log");
    assert_eq!(entries.len(), 2);
    for (seq, change) in &entries {
        b_space.apply_accepted(*seq, change).await.expect("apply");
    }
    assert_eq!(b_space.cursor().await.expect("cursor"), 2);
    let on_b = b_space.get(created.id).await.expect("b get");
    assert_eq!(on_b.fields, on_server.fields);
    assert_eq!(on_b.kind, ObjectKind::Entity);
    let listed = b_space
        .list(&elri_core::store::ObjectFilter::default())
        .await
        .expect("list");
    assert_eq!(listed.len(), 1);

    // B edits; the authority admits; A installs — the circle closes.
    b_space
        .update(created.id, |d| {
            d.set_field("title", &FieldValue::String("hello from b".into()))
        })
        .await
        .expect("b update");
    let b_out = b_space.outbox(0).await.expect("b outbox");
    assert_eq!(b_out.len(), 1);
    let admission = s_space.admit(&b_out[0]).await.expect("admit b");
    assert_eq!(admission, Admission::Accepted(3));
    b_space
        .record_admission(b_out[0].id, &admission)
        .await
        .expect("record b");
    for (seq, change) in s_space.log_after(2, 0).await.expect("log 2") {
        a_space
            .apply_accepted(seq, &change)
            .await
            .expect("apply on a");
    }
    let on_a = a_space.get(created.id).await.expect("a get");
    assert_eq!(
        on_a.fields["title"],
        FieldValue::String("hello from b".into())
    );
    assert_eq!(on_a.fields["done"], FieldValue::Bool(true));
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
    a_space
        .update(created.id, |d| d.set_field("n", &FieldValue::Int(1)))
        .await
        .expect("update");
    let outbox = a_space.outbox(0).await.expect("outbox");

    // Submit the edit without the creation it builds on.
    let admission = s_space.admit(&outbox[1]).await.expect("admit");
    assert_eq!(
        admission,
        Admission::Rejected(RejectReason::MissingPredecessors)
    );
    assert_eq!(s_space.last_seq().await.expect("seq"), 0);

    // A missing-predecessor rejection is transient: once the creation is
    // admitted, the very same submission goes through.
    assert_eq!(
        s_space.admit(&outbox[0]).await.expect("admit"),
        Admission::Accepted(1)
    );
    assert_eq!(
        s_space.admit(&outbox[1]).await.expect("admit again"),
        Admission::Accepted(2)
    );
    let on_server = s_space.get(created.id).await.expect("get");
    assert_eq!(on_server.fields["n"], FieldValue::Int(1));
}
