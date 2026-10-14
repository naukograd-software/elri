//! Two devices and one server: a Change made on one device reaches the other
//! through the server, in both directions, and a viewer's write is refused.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;
use std::time::Duration;

use elri_core::{BuildInfo, Config as CoreConfig, Core, Endpoint};
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::{ChangeStatus, ListChangesRequest};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, DeleteObjectRequest, GetObjectRequest, ListObjectsRequest,
    ObjectKind, PurgeObjectRequest, UpdateObjectRequest, Value,
};
use elri_proto::elri::v1::remote::remote_service_ws::RemoteServiceClient;
use elri_proto::elri::v1::remote::{
    AddMemberRequest, ConnectRequest, CreateRemoteSpaceRequest, GetStatusRequest, JoinSpaceRequest,
    ListRemoteSpacesRequest,
};
use elri_proto::elri::v1::space::Role;
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
            changes: ChangeServiceClient::new(conn),
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

    server.shutdown();
}
