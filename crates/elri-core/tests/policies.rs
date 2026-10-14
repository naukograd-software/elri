//! Transfer policies: what leaves a Space is governed by its rules, for
//! scripts that carry what they read, for exports, and for moves.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{
    value, CreateObjectRequest, ListObjectsRequest, ObjectKind, Value,
};
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{CreateScriptRequest, Manifest, RunScriptRequest};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::{
    CreateSpaceRequest, GetTransferPolicyRequest, SetTransferPolicyRequest, TransferOp,
    TransferOutcome, TransferRule,
};
use elri_proto::elri::v1::transfer::transfer_service_ws::TransferServiceClient;
use elri_proto::elri::v1::transfer::{
    ExportObjectsRequest, ImportObjectsRequest, MoveObjectsRequest,
};
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-policies-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

fn s(v: &str) -> Value {
    Value {
        value: Some(value::Value::String(v.to_owned())),
    }
}

fn rule(op: TransferOp, dest: &str, outcome: TransferOutcome) -> TransferRule {
    TransferRule {
        op: op.into(),
        destination: dest.into(),
        outcome: outcome.into(),
    }
}

#[tokio::test]
async fn rules_govern_scripts_exports_and_moves() {
    let dir = tmp_dir("rules");
    let _core = Core::start(Config::new(
        &dir,
        BuildInfo {
            version: "test",
            commit: "",
        },
    ))
    .await
    .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let scripts = ScriptServiceClient::new(conn.clone());
    let transfer = TransferServiceClient::new(conn);
    let mk_space = |name: &str| {
        let req = authed(&endpoint, CreateSpaceRequest { name: name.into() });
        let spaces = spaces.clone();
        async move {
            spaces
                .create_space(req)
                .await
                .expect("space")
                .msg
                .space
                .expect("space")
                .id
        }
    };
    let work = mk_space("work").await;
    let home = mk_space("home").await;
    let partner = mk_space("partner").await;
    let mut fields = HashMap::new();
    fields.insert("secret".to_owned(), s("q3 numbers"));
    let note = objects
        .create_object(authed(
            &endpoint,
            CreateObjectRequest {
                space_id: work.clone(),
                kind: ObjectKind::Entity.into(),
                fields,
                ..Default::default()
            },
        ))
        .await
        .expect("note")
        .msg
        .object
        .expect("object");

    // Nothing set: everything is allowed; the policy reads as empty.
    let empty = spaces
        .get_transfer_policy(authed(
            &endpoint,
            GetTransferPolicyRequest {
                space_id: work.clone(),
            },
        ))
        .await
        .expect("get")
        .msg
        .policy
        .expect("policy");
    assert!(empty.rules.is_empty());

    // A bad rule is refused; a good policy is stored on the Space.
    let err = spaces
        .set_transfer_policy(authed(
            &endpoint,
            SetTransferPolicyRequest {
                space_id: work.clone(),
                rules: vec![rule(TransferOp::Copy, "nowhere", TransferOutcome::Deny)],
            },
        ))
        .await
        .expect_err("bad destination");
    assert_eq!(err.code, Code::InvalidArgument);
    let policy = spaces
        .set_transfer_policy(authed(
            &endpoint,
            SetTransferPolicyRequest {
                space_id: work.clone(),
                rules: vec![
                    rule(TransferOp::Copy, "", TransferOutcome::Ask),
                    rule(TransferOp::Copy, &partner, TransferOutcome::Deny),
                    rule(TransferOp::Export, "", TransferOutcome::Deny),
                ],
            },
        ))
        .await
        .expect("set")
        .msg
        .policy
        .expect("policy");
    assert_eq!(policy.rules.len(), 3);
    let listed = objects
        .list_objects(authed(
            &endpoint,
            ListObjectsRequest {
                space_id: work.clone(),
                kind: ObjectKind::TransferPolicy.into(),
                ..Default::default()
            },
        ))
        .await
        .expect("list")
        .msg
        .objects;
    assert_eq!(listed.len(), 1, "one policy object per space");

    // A script that read work may not write home without confirmation,
    // and never partner; a script that read nothing writes freely.
    let copier = |target: &str| {
        let req = authed(
            &endpoint,
            CreateScriptRequest {
                space_id: home.clone(),
                name: format!("copy to {target}"),
                source: format!(
                    r#"local n = elri.get("{}", "{work}"); elri.create({{ fields = {{ copied = n.fields.secret }} }}, "{target}"); return "copied""#,
                    note.id
                ),
                manifest: Some(Manifest {
                    reads: vec![work.clone()],
                    writes: vec![target.to_owned()],
                    ..Default::default()
                }),
            },
        );
        let scripts = scripts.clone();
        async move {
            scripts
                .create_script(req)
                .await
                .expect("script")
                .msg
                .script
                .expect("script")
        }
    };
    let run = |id: &str| {
        let req = authed(
            &endpoint,
            RunScriptRequest {
                space_id: home.clone(),
                id: id.to_owned(),
                args_json: String::new(),
            },
        );
        let scripts = scripts.clone();
        async move {
            scripts
                .run_script(req)
                .await
                .expect("run")
                .msg
                .result
                .expect("result")
        }
    };
    let to_home = copier(&home).await;
    let to_partner = copier(&partner).await;
    for script in [&to_home, &to_partner] {
        elri_proto::elri::v1::script::grant_service_ws::GrantServiceClient::new(
            Client::dial(&endpoint.url, ClientConfig::default())
                .await
                .expect("dial"),
        )
        .grant_script(authed(
            &endpoint,
            elri_proto::elri::v1::script::GrantScriptRequest {
                expected_version: String::new(),
                space_id: home.clone(),
                script_id: script.id.clone(),
            },
        ))
        .await
        .expect("grant");
    }
    let res = run(&to_home.id).await;
    assert!(res.error.contains("asks before copy"), "{}", res.error);
    let res = run(&to_partner.id).await;
    assert!(res.error.contains("denies copy"), "{}", res.error);
    let count = |space: &str| {
        let req = authed(
            &endpoint,
            ListObjectsRequest {
                space_id: space.to_owned(),
                kind: ObjectKind::Entity.into(),
                ..Default::default()
            },
        );
        let objects = objects.clone();
        async move {
            objects
                .list_objects(req)
                .await
                .expect("list")
                .msg
                .objects
                .len()
        }
    };
    assert_eq!(count(&home).await, 0);
    assert_eq!(count(&partner).await, 0);

    // Export from work is denied outright; confirmation does not help.
    let export = |confirm: bool| {
        let req = authed(
            &endpoint,
            ExportObjectsRequest {
                space_id: work.clone(),
                ids: vec![note.id.clone()],
                with_dependencies: false,
                confirm,
            },
        );
        let transfer = transfer.clone();
        async move { transfer.export_objects(req).await }
    };
    assert_eq!(
        export(false).await.expect_err("denied").code,
        Code::PermissionDenied
    );
    assert_eq!(
        export(true).await.expect_err("mandatory").code,
        Code::PermissionDenied
    );

    // A move to home asks: refused without confirmation, done with it.
    let mv = |to: &str, confirm: bool| {
        let req = authed(
            &endpoint,
            MoveObjectsRequest {
                source_space_id: work.clone(),
                dest_space_id: to.to_owned(),
                ids: vec![note.id.clone()],
                confirm,
            },
        );
        let transfer = transfer.clone();
        async move { transfer.move_objects(req).await }
    };
    assert_eq!(
        mv(&partner, true).await.expect_err("denied").code,
        Code::PermissionDenied
    );
    assert_eq!(
        mv(&home, false).await.expect_err("asks").code,
        Code::FailedPrecondition
    );
    mv(&home, true).await.expect("confirmed move");
    assert_eq!(count(&home).await, 1);

    // Export now allowed from home (no policy); importing that bundle into
    // partner is a copy from home, which home allows.
    let bundle = transfer
        .export_objects(authed(
            &endpoint,
            ExportObjectsRequest {
                space_id: home.clone(),
                ids: vec![note.id.clone()],
                with_dependencies: false,
                confirm: false,
            },
        ))
        .await
        .expect("export")
        .msg
        .bundle;
    transfer
        .import_objects(authed(
            &endpoint,
            ImportObjectsRequest {
                space_id: partner.clone(),
                bundle,
                keep_ids: false,
                confirm: false,
            },
        ))
        .await
        .expect("import");
    assert_eq!(count(&partner).await, 1);
}
