//! Two devices and one server: a Change made on one device reaches the other
//! through the server, in both directions, and a viewer's write is refused.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use elri_core::{BuildInfo, Config as CoreConfig, Core, Endpoint};
use elri_proto::elri::v1::attachment::attachment_service_ws::AttachmentServiceClient;
use elri_proto::elri::v1::attachment::{
    upload_request, DownloadRequest, GetAttachmentRequest, UploadHeader, UploadRequest,
};
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::{ChangeStatus, ListChangesRequest};
use elri_proto::elri::v1::definition::definition_service_ws::DefinitionServiceClient;
use elri_proto::elri::v1::definition::{
    CreateConstraintRequest, CreateFieldDefinitionRequest, FieldKind,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, write, CommitRequest, CreateObjectRequest, DeleteObjectRequest, GetObjectRequest,
    ListObjectsRequest, ObjectKind, PurgeObjectRequest, UpdateObjectRequest, Value, Write,
};
use elri_proto::elri::v1::remote::remote_service_ws::RemoteServiceClient;
use elri_proto::elri::v1::remote::{
    AddMemberRequest, ConnectRequest, CreateRemoteSpaceRequest, GetStatusRequest, JoinSpaceRequest,
    ListRemoteSpacesRequest,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::{
    Role, SetTransferPolicyRequest, TransferOp, TransferOutcome, TransferRule,
};
use elri_server::{Config as ServerConfig, Server};
use ws_proto::{Client, ClientConfig, Request};

fn tmp_dir_of(name: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("elri-repl-{name}-{}", std::process::id()))
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = tmp_dir_of(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

struct Device {
    _core: Core,
    endpoint: Endpoint,
    objects: ObjectServiceClient,
    remote: RemoteServiceClient,
    changes: ChangeServiceClient,
    attachments: AttachmentServiceClient,
    defs: DefinitionServiceClient,
    spaces: SpaceServiceClient,
}

impl Device {
    async fn start(name: &str) -> Self {
        let dir = tmp_dir(name);
        let core = Core::start(CoreConfig::new(&dir, build()))
            .await
            .expect("core");
        let endpoint = Endpoint::read(&dir).expect("endpoint");
        let conn = Client::dial(&endpoint.url, ClientConfig::default())
            .await
            .expect("dial core");
        Self {
            _core: core,
            endpoint,
            objects: ObjectServiceClient::new(conn.clone()),
            remote: RemoteServiceClient::new(conn.clone()),
            changes: ChangeServiceClient::new(conn.clone()),
            attachments: AttachmentServiceClient::new(conn.clone()),
            defs: DefinitionServiceClient::new(conn.clone()),
            spaces: SpaceServiceClient::new(conn),
        }
    }

    fn authed<T>(&self, msg: T) -> Request<T> {
        Request::new(msg).with_header("authorization", format!("Bearer {}", self.endpoint.token))
    }
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

async fn eventually<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    for _ in 0..100 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for {what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changes_flow_between_devices_through_the_server() {
    let server = Server::start(ServerConfig {
        data_dir: tmp_dir("server"),
        bind: "127.0.0.1:0".parse().expect("addr"),
        tls: None,
    })
    .await
    .expect("server");
    let (_, _, alice_token) = server
        .state()
        .registry
        .create_user("alice", false, "test")
        .await
        .expect("alice");
    let (_, _, bob_token) = server
        .state()
        .registry
        .create_user("bob", false, "test")
        .await
        .expect("bob");
    let (_, _, viewer_token) = server
        .state()
        .registry
        .create_user("viewer", false, "test")
        .await
        .expect("viewer");

    let a = Device::start("a").await;
    let b = Device::start("b").await;
    let v = Device::start("v").await;

    // Connect all three devices.
    let hello = a
        .remote
        .connect(a.authed(ConnectRequest {
            url: server.url(),
            token: alice_token.clone(),
        }))
        .await
        .expect("connect a")
        .msg;
    assert_eq!(hello.user_name, "alice");
    // The token went to the secret store (the file backend here), never
    // into the device database.
    let a_dir = tmp_dir_of("a");
    let secrets = std::fs::read_to_string(a_dir.join("secrets.json")).expect("secrets file");
    assert!(secrets.contains(&alice_token));
    let device_db = std::fs::read(a_dir.join("device.db")).expect("device db");
    assert!(
        !device_db
            .windows(alice_token.len())
            .any(|w| w == alice_token.as_bytes()),
        "token must not be stored in device.db"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt as _;
        let mode = std::fs::metadata(a_dir.join("secrets.json"))
            .expect("stat")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }
    b.remote
        .connect(b.authed(ConnectRequest {
            url: server.url(),
            token: bob_token,
        }))
        .await
        .expect("connect b");
    v.remote
        .connect(v.authed(ConnectRequest {
            url: server.url(),
            token: viewer_token,
        }))
        .await
        .expect("connect v");

    // Alice creates a shared Space and invites the others.
    let space = a
        .remote
        .create_remote_space(a.authed(CreateRemoteSpaceRequest {
            name: "team".into(),
        }))
        .await
        .expect("create remote space")
        .msg
        .space
        .expect("space");
    for (name, role) in [("bob", Role::Editor), ("viewer", Role::Viewer)] {
        a.remote
            .add_member(a.authed(AddMemberRequest {
                space_id: space.id.clone(),
                user_name: name.into(),
                role: role.into(),
            }))
            .await
            .expect("add member");
    }
    let visible = b
        .remote
        .list_remote_spaces(b.authed(ListRemoteSpacesRequest {}))
        .await
        .expect("list remote")
        .msg
        .spaces;
    assert_eq!(visible.len(), 1);
    assert!(!visible[0].joined);
    for d in [&b, &v] {
        d.remote
            .join_space(d.authed(JoinSpaceRequest {
                space_id: space.id.clone(),
            }))
            .await
            .expect("join");
    }

    // Alice writes; Bob sees it.
    let mut fields = HashMap::new();
    fields.insert("title".to_owned(), s("from alice"));
    let created = a
        .objects
        .create_object(a.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            types: vec!["note".into()],
            fields,
            ..Default::default()
        }))
        .await
        .expect("create")
        .msg
        .object
        .expect("object");
    eventually("bob to receive alice's object", || async {
        b.objects
            .get_object(b.authed(GetObjectRequest {
                space_id: space.id.clone(),
                id: created.id.clone(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg
                    .object
                    .is_some_and(|o| o.fields.get("title") == Some(&s("from alice")))
            })
    })
    .await;

    // Alice's change is marked accepted on her device.
    eventually("alice's change to be accepted", || async {
        a.changes
            .list_changes(a.authed(ListChangesRequest {
                space_id: space.id.clone(),
                limit: 10,
                object_id: String::new(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg
                    .changes
                    .iter()
                    .all(|c| c.status() == ChangeStatus::Accepted)
            })
    })
    .await;

    // Bob edits; Alice sees it.
    let mut set = HashMap::new();
    set.insert("title".to_owned(), s("edited by bob"));
    b.objects
        .update_object(b.authed(UpdateObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
            set_fields: set,
            ..Default::default()
        }))
        .await
        .expect("bob update");
    eventually("alice to receive bob's edit", || async {
        a.objects
            .get_object(a.authed(GetObjectRequest {
                space_id: space.id.clone(),
                id: created.id.clone(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg
                    .object
                    .is_some_and(|o| o.fields.get("title") == Some(&s("edited by bob")))
            })
    })
    .await;

    // The viewer sees everything but cannot write: the change stays local
    // on the viewer's device as rejected, and never reaches the others.
    eventually("viewer to receive the object", || async {
        v.objects
            .list_objects(v.authed(ListObjectsRequest {
                space_id: space.id.clone(),
                ..Default::default()
            }))
            .await
            .is_ok_and(|r| r.msg.objects.len() == 1)
    })
    .await;
    let mut set = HashMap::new();
    set.insert("title".to_owned(), s("viewer tries"));
    v.objects
        .update_object(v.authed(UpdateObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
            set_fields: set,
            ..Default::default()
        }))
        .await
        .expect("viewer local write succeeds");
    eventually("viewer's change to be rejected", || async {
        v.changes
            .list_changes(v.authed(ListChangesRequest {
                space_id: space.id.clone(),
                limit: 10,
                object_id: String::new(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg
                    .changes
                    .iter()
                    .any(|c| c.status() == ChangeStatus::Rejected)
            })
    })
    .await;
    let status = v
        .remote
        .get_status(v.authed(GetStatusRequest {}))
        .await
        .expect("status")
        .msg;
    assert!(status.configured && status.connected);
    assert_eq!(status.spaces.len(), 1);
    assert_eq!(status.spaces[0].rejected, 1);
    let on_v = v
        .objects
        .get_object(v.authed(GetObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("get")
        .msg
        .object
        .expect("object");
    assert_eq!(
        on_v.fields["title"],
        s("edited by bob"),
        "the rejected edit left the viewer's live state"
    );
    let on_a = a
        .objects
        .get_object(a.authed(GetObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("get")
        .msg
        .object
        .expect("object");
    assert_eq!(on_a.fields["title"], s("edited by bob"));

    // Alice deletes and purges; Bob's copy is erased through the log. Bob,
    // an editor, may not purge himself.
    a.objects
        .delete_object(a.authed(DeleteObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("delete");
    let denied = b
        .objects
        .purge_object(b.authed(PurgeObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect_err("editor cannot purge");
    assert_eq!(denied.code, ws_proto::Code::PermissionDenied);
    a.objects
        .purge_object(a.authed(PurgeObjectRequest {
            space_id: space.id.clone(),
            id: created.id.clone(),
        }))
        .await
        .expect("purge");
    eventually("bob's copy to be erased", || async {
        b.objects
            .get_object(b.authed(GetObjectRequest {
                space_id: space.id.clone(),
                id: created.id.clone(),
            }))
            .await
            .is_err_and(|e| e.code == ws_proto::Code::NotFound)
    })
    .await;

    // One Change across two server Spaces: Alice commits, Bob receives
    // both halves.
    let second = a
        .remote
        .create_remote_space(a.authed(CreateRemoteSpaceRequest {
            name: "second".into(),
        }))
        .await
        .expect("second space")
        .msg
        .space
        .expect("space");
    a.remote
        .add_member(a.authed(AddMemberRequest {
            space_id: second.id.clone(),
            user_name: "bob".into(),
            role: Role::Editor.into(),
        }))
        .await
        .expect("add bob");
    b.remote
        .join_space(b.authed(JoinSpaceRequest {
            space_id: second.id.clone(),
        }))
        .await
        .expect("join second");
    let mut f1 = HashMap::new();
    f1.insert("title".to_owned(), s("in team"));
    let mut f2 = HashMap::new();
    f2.insert("title".to_owned(), s("in second"));
    let committed = a
        .objects
        .commit(a.authed(CommitRequest {
            writes: vec![
                Write {
                    write: Some(write::Write::Create(CreateObjectRequest {
                        space_id: space.id.clone(),
                        kind: ObjectKind::Entity.into(),
                        fields: f1,
                        ..Default::default()
                    })),
                },
                Write {
                    write: Some(write::Write::Create(CreateObjectRequest {
                        space_id: second.id.clone(),
                        kind: ObjectKind::Entity.into(),
                        fields: f2,
                        ..Default::default()
                    })),
                },
            ],
        }))
        .await
        .expect("commit")
        .msg;
    assert_eq!(committed.objects.len(), 2);
    for (sp, obj) in [
        (&space, &committed.objects[0]),
        (&second, &committed.objects[1]),
    ] {
        eventually("bob to receive both halves", || async {
            b.objects
                .get_object(b.authed(GetObjectRequest {
                    space_id: sp.id.clone(),
                    id: obj.id.clone(),
                }))
                .await
                .is_ok()
        })
        .await;
    }
    // An attachment: Alice uploads, the bytes reach the server before the
    // Change does, Bob sees the attachment and fetches the bytes on demand.
    let payload = b"attachment bytes travelling through the server".to_vec();
    let stream = a
        .attachments
        .upload(
            ws_proto::CallOptions::new()
                .with_header("authorization", format!("Bearer {}", a.endpoint.token)),
        )
        .await
        .expect("upload");
    stream
        .send(&UploadRequest {
            request: Some(upload_request::Request::Header(UploadHeader {
                space_id: space.id.clone(),
                name: "notes.txt".into(),
                media_type: "text/plain".into(),
                size: i64::try_from(payload.len()).expect("len"),
            })),
        })
        .await
        .expect("header");
    stream
        .send(&UploadRequest {
            request: Some(upload_request::Request::Chunk(payload.clone().into())),
        })
        .await
        .expect("chunk");
    let uploaded = stream
        .close_and_recv()
        .await
        .expect("finish")
        .msg
        .attachment
        .expect("attachment");
    eventually("bob to see the attachment", || async {
        b.attachments
            .get_attachment(b.authed(GetAttachmentRequest {
                space_id: space.id.clone(),
                id: uploaded.id.clone(),
            }))
            .await
            .is_ok_and(|r| r.msg.attachment.is_some_and(|x| !x.available))
    })
    .await;
    let server_blob = server.state().blobs.exists(
        elri_core::model::ObjectId::parse(&space.id).expect("id"),
        &uploaded.sha256,
    );
    assert!(server_blob, "the server holds the bytes");
    let stream = b
        .attachments
        .download(b.authed(DownloadRequest {
            space_id: space.id.clone(),
            id: uploaded.id.clone(),
        }))
        .await
        .expect("download");
    let mut got = Vec::new();
    while let Some(c) = stream.recv().await.expect("chunk") {
        got.extend_from_slice(&c.data);
    }
    assert_eq!(got, payload);
    let now_local = b
        .attachments
        .get_attachment(b.authed(GetAttachmentRequest {
            space_id: space.id.clone(),
            id: uploaded.id.clone(),
        }))
        .await
        .expect("get")
        .msg
        .attachment
        .expect("attachment");
    assert!(now_local.available);

    // A strict constraint: Bob, an editor, may not set one; Alice, the
    // owner, may. Then two offline claims of one value: the server accepts
    // the first and rejects the second as a whole.
    let email = a
        .defs
        .create_field_definition(a.authed(CreateFieldDefinitionRequest {
            space_id: space.id.clone(),
            name: "Email".into(),
            description: String::new(),
            kind: FieldKind::String.into(),
        }))
        .await
        .expect("field")
        .msg
        .definition
        .expect("definition");
    eventually("bob to receive the field", || async {
        b.objects
            .get_object(b.authed(GetObjectRequest {
                space_id: space.id.clone(),
                id: email.id.clone(),
            }))
            .await
            .is_ok()
    })
    .await;
    let bobs = b
        .defs
        .create_constraint(b.authed(CreateConstraintRequest {
            space_id: space.id.clone(),
            name: "bob's strict".into(),
            field_id: email.id.clone(),
            unique: true,
            strict: true,
        }))
        .await
        .expect("local write succeeds")
        .msg
        .constraint
        .expect("constraint");
    eventually(
        "bob's strict constraint to be rejected by the server",
        || async {
            b.changes
                .list_changes(b.authed(ListChangesRequest {
                    space_id: space.id.clone(),
                    limit: 20,
                    object_id: bobs.id.clone(),
                }))
                .await
                .is_ok_and(|r| {
                    r.msg
                        .changes
                        .iter()
                        .any(|c| c.status() == ChangeStatus::Rejected)
                })
        },
    )
    .await;
    // A transfer policy is an owner's or admin's to set: Bob's is rejected,
    // Alice's lands on Bob's device.
    let bobs_policy = b
        .spaces
        .set_transfer_policy(b.authed(SetTransferPolicyRequest {
            space_id: space.id.clone(),
            rules: vec![TransferRule {
                op: TransferOp::Export.into(),
                destination: String::new(),
                outcome: TransferOutcome::Deny.into(),
            }],
        }))
        .await
        .expect("local write succeeds")
        .msg
        .policy
        .expect("policy");
    eventually("bob's policy to be rejected by the server", || async {
        b.objects
            .list_objects(b.authed(ListObjectsRequest {
                space_id: space.id.clone(),
                kind: ObjectKind::TransferPolicy.into(),
                ..Default::default()
            }))
            .await
            .is_ok_and(|r| r.msg.objects.is_empty())
    })
    .await;
    assert_eq!(bobs_policy.rules.len(), 1);
    let alices_policy = a
        .spaces
        .set_transfer_policy(a.authed(SetTransferPolicyRequest {
            space_id: space.id.clone(),
            rules: vec![TransferRule {
                op: TransferOp::Export.into(),
                destination: String::new(),
                outcome: TransferOutcome::Ask.into(),
            }],
        }))
        .await
        .expect("owner sets policy")
        .msg
        .policy
        .expect("policy");
    eventually("alice's policy to reach bob", || async {
        b.spaces
            .get_transfer_policy(
                b.authed(elri_proto::elri::v1::space::GetTransferPolicyRequest {
                    space_id: space.id.clone(),
                }),
            )
            .await
            .is_ok_and(|r| {
                r.msg.policy.is_some_and(|p| {
                    p.updated_at_ms == alices_policy.updated_at_ms && p.rules.len() == 1
                })
            })
    })
    .await;

    let strict = a
        .defs
        .create_constraint(a.authed(CreateConstraintRequest {
            space_id: space.id.clone(),
            name: "unique email".into(),
            field_id: email.id.clone(),
            unique: true,
            strict: true,
        }))
        .await
        .expect("owner sets strict")
        .msg
        .constraint
        .expect("constraint");
    eventually("bob to receive the constraint", || async {
        b.objects
            .get_object(b.authed(GetObjectRequest {
                space_id: space.id.clone(),
                id: strict.id.clone(),
            }))
            .await
            .is_ok()
    })
    .await;
    // Both devices claim the same value while the other's claim is not yet
    // there: each local write passes its provisional check.
    let claim = |d: &Device| {
        let mut fields = HashMap::new();
        fields.insert(email.id.clone(), s("same@x"));
        let req = d.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            fields,
            ..Default::default()
        });
        let objects = d.objects.clone();
        async move { objects.create_object(req).await }
    };
    let (ra, rb) = tokio::join!(claim(&a), claim(&b));
    let claim_a = ra.expect("alice's local write").msg.object.expect("object");
    let claim_b = rb.expect("bob's local write").msg.object.expect("object");
    let outcome = |d: &Device, id: String| {
        let req = d.authed(ListChangesRequest {
            space_id: space.id.clone(),
            limit: 20,
            object_id: id,
        });
        let changes = d.changes.clone();
        async move {
            changes.list_changes(req).await.ok().and_then(|r| {
                r.msg
                    .changes
                    .first()
                    .map(|c| (c.status(), c.rejection.clone()))
            })
        }
    };
    eventually("both claims to be decided", || async {
        let oa = outcome(&a, claim_a.id.clone()).await;
        let ob = outcome(&b, claim_b.id.clone()).await;
        matches!(
            (oa.map(|o| o.0), ob.map(|o| o.0)),
            (Some(ChangeStatus::Accepted), Some(ChangeStatus::Rejected))
                | (Some(ChangeStatus::Rejected), Some(ChangeStatus::Accepted))
        )
    })
    .await;
    let rejected_text = [
        outcome(&a, claim_a.id.clone()).await,
        outcome(&b, claim_b.id.clone()).await,
    ]
    .into_iter()
    .flatten()
    .find(|(st, _)| *st == ChangeStatus::Rejected)
    .map(|(_, text)| text)
    .expect("one rejection");
    assert!(rejected_text.contains("CONSTRAINT"), "{rejected_text}");

    // A Change too large for one message: a five-megabyte text goes to the
    // server in chunks and reaches Bob the same way.
    let big = "x".repeat(5 * 1024 * 1024);
    let mut fields = HashMap::new();
    fields.insert(
        "body".to_owned(),
        elri_proto::elri::v1::object::Value {
            value: Some(elri_proto::elri::v1::object::value::Value::Text(
                big.clone(),
            )),
        },
    );
    let large = a
        .objects
        .create_object(a.authed(CreateObjectRequest {
            space_id: space.id.clone(),
            kind: ObjectKind::Entity.into(),
            fields,
            ..Default::default()
        }))
        .await
        .expect("large object")
        .msg
        .object
        .expect("object");
    eventually("the large change to be accepted", || async {
        a.changes
            .list_changes(a.authed(ListChangesRequest {
                space_id: space.id.clone(),
                limit: 20,
                object_id: large.id.clone(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg
                    .changes
                    .iter()
                    .any(|c| c.status() == ChangeStatus::Accepted)
            })
    })
    .await;
    eventually("bob to receive the large object whole", || async {
        b.objects
            .get_object(b.authed(GetObjectRequest {
                space_id: space.id.clone(),
                id: large.id.clone(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg.object.is_some_and(|o| {
                    matches!(
                        o.fields.get("body").and_then(|v| v.value.as_ref()),
                        Some(elri_proto::elri::v1::object::value::Value::Text(t)) if t.len() == big.len()
                    )
                })
            })
    })
    .await;

    eventually("alice's coordinated change to be accepted", || async {
        a.changes
            .list_changes(a.authed(ListChangesRequest {
                space_id: second.id.clone(),
                limit: 10,
                object_id: String::new(),
            }))
            .await
            .is_ok_and(|r| {
                r.msg
                    .changes
                    .iter()
                    .any(|c| c.id == committed.change_id && c.status() == ChangeStatus::Accepted)
            })
    })
    .await;

    server.shutdown();
}
