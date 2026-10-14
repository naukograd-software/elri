//! Extensions: a WASM module runs under fuel and a memory limit and talks
//! to the core through `elri.call`; a native library runs only after the
//! user accepts the risk; scripts reach extensions their manifest names.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::extension::extension_service_ws::ExtensionServiceClient;
use elri_proto::elri::v1::extension::{
    install_extension_request, CallExtensionRequest, ExtensionKind, GrantExtensionRequest,
    InstallExtensionRequest, InstallHeader, ListExtensionsRequest, RemoveExtensionRequest,
};
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{CreateObjectRequest, ListObjectsRequest, ObjectKind};
use elri_proto::elri::v1::script::grant_service_ws::GrantServiceClient;
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{
    CreateScriptRequest, GrantScriptRequest, Manifest, RunScriptRequest,
};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use ws_proto::{CallOptions, Client, ClientConfig, Code, Request};

fn build() -> BuildInfo {
    BuildInfo {
        version: "test",
        commit: "",
    }
}

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-ext-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(endpoint: &Endpoint, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {}", endpoint.token))
}

/// A module with a bump allocator. `count` lists the Space's entities
/// through the host and returns `{"count": n}`; `spin` never returns.
const MODULE: &str = r#"
(module
  (import "elri" "call" (func $call (param i32 i32) (result i64)))
  (memory (export "memory") 1)
  (global $next (mut i32) (i32.const 1024))
  (func (export "alloc") (param $len i32) (result i32)
    (local $p i32)
    (local.set $p (global.get $next))
    (global.set $next (i32.add (global.get $next) (i32.add (local.get $len) (i32.const 8))))
    (if (i32.gt_u (global.get $next) (i32.mul (memory.size) (i32.const 65536)))
      (then (drop (memory.grow (i32.const 1)))))
    (local.get $p))
  ;; {"op":"list","filter":{"kind":"entity"}}
  (data (i32.const 0) "{\"op\":\"list\",\"filter\":{\"kind\":\"entity\"}}")
  ;; {"count":
  (data (i32.const 64) "{\"count\":")
  (func (export "count") (param i32 i32) (result i64)
    (local $packed i64) (local $ptr i32) (local $len i32) (local $i i32) (local $n i32)
    (local $out i32) (local $w i32) (local $digits i32) (local $d i32)
    (local.set $packed (call $call (i32.const 0) (i32.const 40)))
    (local.set $ptr (i32.wrap_i64 (i64.shr_u (local.get $packed) (i64.const 32))))
    (local.set $len (i32.wrap_i64 (i64.and (local.get $packed) (i64.const 4294967295))))
    ;; count objects by counting "\"id\":" occurrences: scan for the 5-byte pattern
    (local.set $i (i32.const 0))
    (block $done (loop $scan
      (br_if $done (i32.ge_u (i32.add (local.get $i) (i32.const 5)) (local.get $len)))
      (if (i32.and
            (i32.and
              (i32.eq (i32.load8_u (i32.add (local.get $ptr) (local.get $i))) (i32.const 34))
              (i32.eq (i32.load8_u (i32.add (local.get $ptr) (i32.add (local.get $i) (i32.const 1)))) (i32.const 105)))
            (i32.and
              (i32.eq (i32.load8_u (i32.add (local.get $ptr) (i32.add (local.get $i) (i32.const 2)))) (i32.const 100))
              (i32.and
                (i32.eq (i32.load8_u (i32.add (local.get $ptr) (i32.add (local.get $i) (i32.const 3)))) (i32.const 34))
                (i32.eq (i32.load8_u (i32.add (local.get $ptr) (i32.add (local.get $i) (i32.const 4)))) (i32.const 58)))))
        (then (local.set $n (i32.add (local.get $n) (i32.const 1)))))
      (local.set $i (i32.add (local.get $i) (i32.const 1)))
      (br $scan)))
    ;; write {"count":N}
    (local.set $out (global.get $next))
    (global.set $next (i32.add (global.get $next) (i32.const 32)))
    (memory.copy (local.get $out) (i32.const 64) (i32.const 9))
    (local.set $w (i32.add (local.get $out) (i32.const 9)))
    ;; decimal digits of n (n < 100 is enough here)
    (if (i32.ge_u (local.get $n) (i32.const 10))
      (then
        (i32.store8 (local.get $w) (i32.add (i32.const 48) (i32.div_u (local.get $n) (i32.const 10))))
        (local.set $w (i32.add (local.get $w) (i32.const 1)))))
    (i32.store8 (local.get $w) (i32.add (i32.const 48) (i32.rem_u (local.get $n) (i32.const 10))))
    (local.set $w (i32.add (local.get $w) (i32.const 1)))
    (i32.store8 (local.get $w) (i32.const 125))
    (local.set $w (i32.add (local.get $w) (i32.const 1)))
    (i64.or
      (i64.shl (i64.extend_i32_u (local.get $out)) (i64.const 32))
      (i64.extend_i32_u (i32.sub (local.get $w) (local.get $out)))))
  (func (export "spin") (param i32 i32) (result i64)
    (loop $forever (br $forever))
    (i64.const 0)))
"#;

async fn install(
    client: &ExtensionServiceClient,
    endpoint: &Endpoint,
    header: InstallHeader,
    bytes: &[u8],
) -> Result<elri_proto::elri::v1::extension::Extension, ws_proto::Status> {
    let stream = client
        .install_extension(
            CallOptions::new().with_header("authorization", format!("Bearer {}", endpoint.token)),
        )
        .await?;
    stream
        .send(&InstallExtensionRequest {
            request: Some(install_extension_request::Request::Header(header)),
        })
        .await?;
    for chunk in bytes.chunks(4096) {
        stream
            .send(&InstallExtensionRequest {
                request: Some(install_extension_request::Request::Chunk(
                    chunk.to_vec().into(),
                )),
            })
            .await?;
    }
    Ok(stream
        .close_and_recv()
        .await?
        .msg
        .extension
        .expect("extension"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wasm_extensions_run_in_the_sandbox_and_scripts_call_them() {
    let dir = tmp_dir("wasm");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let scripts = ScriptServiceClient::new(conn.clone());
    let grants = GrantServiceClient::new(conn.clone());
    let ext = ExtensionServiceClient::new(conn);
    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    for _ in 0..3 {
        objects
            .create_object(authed(
                &endpoint,
                CreateObjectRequest {
                    space_id: space.clone(),
                    kind: ObjectKind::Entity.into(),
                    ..Default::default()
                },
            ))
            .await
            .expect("object");
    }
    let wasm = wat::parse_str(MODULE).expect("wat");

    // A module that does not export a declared function is refused.
    let bad = install(
        &ext,
        &endpoint,
        InstallHeader {
            name: "counter".into(),
            kind: ExtensionKind::Wasm.into(),
            manifest: None,
            functions: vec!["count".into(), "missing".into()],
        },
        &wasm,
    )
    .await
    .expect_err("missing export");
    assert_eq!(bad.code, Code::InvalidArgument);
    let installed = install(
        &ext,
        &endpoint,
        InstallHeader {
            name: "counter".into(),
            kind: ExtensionKind::Wasm.into(),
            manifest: None,
            functions: vec!["count".into(), "spin".into()],
        },
        &wasm,
    )
    .await
    .expect("install");
    assert_eq!(installed.version.len(), 64);
    assert!(installed.benign);
    assert!(!installed.granted, "not yet run");

    // Benign: granted on first call; the host call lists the Space.
    let res = ext
        .call_extension(authed(
            &endpoint,
            CallExtensionRequest {
                name: "counter".into(),
                function: "count".into(),
                space_id: space.clone(),
                args_json: "{}".into(),
            },
        ))
        .await
        .expect("call")
        .msg
        .result
        .expect("result");
    assert_eq!(res.error, "", "{res:?}");
    let out: serde_json::Value = serde_json::from_str(&res.output_json).expect("json");
    assert_eq!(out["count"], 3);
    assert_eq!(res.labels, vec![space.clone()]);
    let listed = ext
        .list_extensions(authed(&endpoint, ListExtensionsRequest {}))
        .await
        .expect("list")
        .msg
        .extensions;
    assert_eq!(listed.len(), 1);
    assert!(listed[0].granted);

    // A runaway module runs out of fuel instead of hanging the core.
    let res = ext
        .call_extension(authed(
            &endpoint,
            CallExtensionRequest {
                name: "counter".into(),
                function: "spin".into(),
                space_id: space.clone(),
                args_json: String::new(),
            },
        ))
        .await
        .expect("call")
        .msg
        .result
        .expect("result");
    assert!(res.error.contains("fuel"), "{}", res.error);

    // A script reaches the extension only when its manifest names it,
    // which makes the script non-benign: it needs a grant.
    let src = r#"return elri.ext("counter", "count", {}).count"#;
    let blind = scripts
        .create_script(authed(
            &endpoint,
            CreateScriptRequest {
                space_id: space.clone(),
                name: "blind".into(),
                source: src.into(),
                manifest: None,
            },
        ))
        .await
        .expect("script")
        .msg
        .script
        .expect("script");
    let res = scripts
        .run_script(authed(
            &endpoint,
            RunScriptRequest {
                space_id: space.clone(),
                id: blind.id.clone(),
                args_json: String::new(),
            },
        ))
        .await
        .expect("run")
        .msg
        .result
        .expect("result");
    assert!(res.error.contains("capability denied"), "{}", res.error);
    let sighted = scripts
        .create_script(authed(
            &endpoint,
            CreateScriptRequest {
                space_id: space.clone(),
                name: "sighted".into(),
                source: src.into(),
                manifest: Some(Manifest {
                    extensions: vec!["counter".into()],
                    ..Default::default()
                }),
            },
        ))
        .await
        .expect("script")
        .msg
        .script
        .expect("script");
    assert!(!sighted.benign);
    grants
        .grant_script(authed(
            &endpoint,
            GrantScriptRequest {
                space_id: space.clone(),
                script_id: sighted.id.clone(),
            },
        ))
        .await
        .expect("grant");
    let res = scripts
        .run_script(authed(
            &endpoint,
            RunScriptRequest {
                space_id: space.clone(),
                id: sighted.id.clone(),
                args_json: String::new(),
            },
        ))
        .await
        .expect("run")
        .msg
        .result
        .expect("result");
    assert_eq!(res.error, "", "{res:?}");
    assert_eq!(res.output_json, "3");

    ext.remove_extension(authed(
        &endpoint,
        RemoveExtensionRequest {
            name: "counter".into(),
        },
    ))
    .await
    .expect("remove");
    assert!(ext
        .list_extensions(authed(&endpoint, ListExtensionsRequest {}))
        .await
        .expect("list")
        .msg
        .extensions
        .is_empty());
    let _ = objects
        .list_objects(authed(
            &endpoint,
            ListObjectsRequest {
                space_id: space,
                ..Default::default()
            },
        ))
        .await;
}

#[cfg(not(any(target_os = "android", target_os = "ios")))]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_extensions_need_the_risk_accepted() {
    // Build the example library with the same toolchain.
    let status = std::process::Command::new(env!("CARGO"))
        .args(["build", "-p", "elri-ext-example"])
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .status()
        .expect("cargo build");
    assert!(status.success());
    let lib_name = if cfg!(target_os = "macos") {
        "libelri_ext_example.dylib"
    } else if cfg!(target_os = "windows") {
        "elri_ext_example.dll"
    } else {
        "libelri_ext_example.so"
    };
    let lib_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../target/debug")
        .join(lib_name);
    let bytes = std::fs::read(&lib_path).expect("example library built");

    let dir = tmp_dir("native");
    let _core = Core::start(Config::new(&dir, build()))
        .await
        .expect("start");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let spaces = SpaceServiceClient::new(conn.clone());
    let ext = ExtensionServiceClient::new(conn);
    let space = spaces
        .create_space(authed(&endpoint, CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    let installed = install(
        &ext,
        &endpoint,
        InstallHeader {
            name: "echo".into(),
            kind: ExtensionKind::Native.into(),
            manifest: None,
            functions: vec!["echo".into()],
        },
        &bytes,
    )
    .await
    .expect("install");
    assert!(!installed.benign, "native is never benign");

    let call = |args: &str| {
        let req = authed(
            &endpoint,
            CallExtensionRequest {
                name: "echo".into(),
                function: "echo".into(),
                space_id: space.clone(),
                args_json: args.to_owned(),
            },
        );
        let ext = ext.clone();
        async move { ext.call_extension(req).await }
    };
    let denied = call(r#"{"a": 1}"#).await.expect_err("no grant");
    assert_eq!(denied.code, Code::PermissionDenied);
    let refused = ext
        .grant_extension(authed(
            &endpoint,
            GrantExtensionRequest {
                name: "echo".into(),
                accept_native_risk: false,
            },
        ))
        .await
        .expect_err("risk not accepted");
    assert_eq!(refused.code, Code::FailedPrecondition);
    ext.grant_extension(authed(
        &endpoint,
        GrantExtensionRequest {
            name: "echo".into(),
            accept_native_risk: true,
        },
    ))
    .await
    .expect("grant");
    let res = call(r#"{"a": 1}"#)
        .await
        .expect("call")
        .msg
        .result
        .expect("result");
    assert_eq!(res.error, "", "{res:?}");
    let out: serde_json::Value = serde_json::from_str(&res.output_json).expect("json");
    assert_eq!(out["echo"]["a"], 1);
    assert_eq!(out["space"], space);
}
