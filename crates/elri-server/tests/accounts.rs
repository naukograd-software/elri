//! Users, tokens, and administration over the server's own protocol.

#![allow(clippy::expect_used, clippy::too_many_lines)]

use elri_proto::elri::v1::account::account_service_ws::AccountServiceClient;
use elri_proto::elri::v1::account::admin_service_ws::AdminServiceClient;
use elri_proto::elri::v1::account::{
    CreateUserRequest, IssueTokenRequest, ListTokensRequest, ListUserTokensRequest,
    ListUsersRequest, RevokeTokenRequest, RevokeUserTokenRequest, SetAdminRequest, WhoAmIRequest,
};
use elri_proto::elri::v1::sync::sync_service_ws::SyncServiceClient;
use elri_proto::elri::v1::sync::HelloRequest;
use elri_server::{Config, Server};
use ws_proto::{Client, ClientConfig, Code, Request};

fn tmp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("elri-accounts-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn authed<T>(token: &str, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {token}"))
}

#[tokio::test]
async fn admins_create_users_and_tokens_are_revocable() {
    let server = Server::start(Config {
        data_dir: tmp_dir("server"),
        bind: "127.0.0.1:0".parse().expect("addr"),
    })
    .await
    .expect("server");
    // Bootstrap: the first admin is created in the database directly.
    let (root, root_token, root_secret) = server
        .state()
        .registry
        .create_user("root", true, "laptop")
        .await
        .expect("root");
    assert!(root.admin);
    assert_eq!(root_token.label, "laptop");

    let conn = Client::dial(&server.url(), ClientConfig::default())
        .await
        .expect("dial");
    let account = AccountServiceClient::new(conn.clone());
    let admin = AdminServiceClient::new(conn.clone());
    let sync = SyncServiceClient::new(conn);

    let me = account
        .who_am_i(authed(&root_secret, WhoAmIRequest {}))
        .await
        .expect("whoami")
        .msg;
    assert_eq!(me.user.expect("user").name, "root");
    assert_eq!(me.token.expect("token").id, root_token.id.to_string());

    // The admin creates a plain user over the wire.
    let created = admin
        .create_user(authed(
            &root_secret,
            CreateUserRequest {
                name: "alice".into(),
                admin: false,
                label: "phone".into(),
            },
        ))
        .await
        .expect("create alice")
        .msg;
    let alice_secret = created.secret.clone();
    assert!(!alice_secret.is_empty());
    assert!(!created.user.as_ref().expect("user").admin);
    let dup = admin
        .create_user(authed(
            &root_secret,
            CreateUserRequest {
                name: "alice".into(),
                ..Default::default()
            },
        ))
        .await
        .expect_err("duplicate name");
    assert_eq!(dup.code, Code::AlreadyExists);

    // A plain user cannot administer.
    let denied = admin
        .list_users(authed(&alice_secret, ListUsersRequest {}))
        .await
        .expect_err("alice is not admin");
    assert_eq!(denied.code, Code::PermissionDenied);
    let users = admin
        .list_users(authed(&root_secret, ListUsersRequest {}))
        .await
        .expect("users")
        .msg
        .users;
    assert_eq!(users.len(), 2);

    // Alice issues a second token for another device, says hello with it,
    // and the server remembers which device used which token.
    let second = account
        .issue_token(authed(
            &alice_secret,
            IssueTokenRequest {
                label: "tablet".into(),
            },
        ))
        .await
        .expect("issue")
        .msg;
    let second_secret = second.secret.clone();
    let second_id = second.token.expect("token").id;
    sync.hello(authed(
        &second_secret,
        HelloRequest {
            device_id: "dev-tablet".into(),
            protocol_version: elri_server::service::PROTOCOL_VERSION,
        },
    ))
    .await
    .expect("hello");
    let tokens = account
        .list_tokens(authed(&alice_secret, ListTokensRequest::default()))
        .await
        .expect("tokens")
        .msg
        .tokens;
    assert_eq!(tokens.len(), 2);
    let tablet = tokens
        .iter()
        .find(|t| t.id == second_id)
        .expect("tablet token");
    assert_eq!(tablet.label, "tablet");
    assert_eq!(tablet.last_device_id, "dev-tablet");
    assert!(tablet.last_used_at_ms.is_some());

    // Alice revokes it; it stops working at once and is listed only on
    // request. Alice cannot revoke someone else's token.
    account
        .revoke_token(authed(
            &alice_secret,
            RevokeTokenRequest {
                id: second_id.clone(),
            },
        ))
        .await
        .expect("revoke");
    let gone = account
        .who_am_i(authed(&second_secret, WhoAmIRequest {}))
        .await
        .expect_err("revoked token");
    assert_eq!(gone.code, Code::Unauthenticated);
    let live = account
        .list_tokens(authed(&alice_secret, ListTokensRequest::default()))
        .await
        .expect("tokens")
        .msg
        .tokens;
    assert_eq!(live.len(), 1);
    let all = account
        .list_tokens(authed(
            &alice_secret,
            ListTokensRequest {
                include_revoked: true,
            },
        ))
        .await
        .expect("tokens")
        .msg
        .tokens;
    assert_eq!(all.len(), 2);
    let foreign = account
        .revoke_token(authed(
            &alice_secret,
            RevokeTokenRequest {
                id: root_token.id.to_string(),
            },
        ))
        .await
        .expect_err("not hers");
    assert_eq!(foreign.code, Code::NotFound);

    // The admin sees and revokes anyone's; cannot demote themself; can
    // promote alice, who then can.
    let alices = admin
        .list_user_tokens(authed(
            &root_secret,
            ListUserTokensRequest {
                user_name: "alice".into(),
                include_revoked: false,
            },
        ))
        .await
        .expect("alice tokens")
        .msg
        .tokens;
    assert_eq!(alices.len(), 1);
    let self_demote = admin
        .set_admin(authed(
            &root_secret,
            SetAdminRequest {
                user_name: "root".into(),
                admin: false,
            },
        ))
        .await
        .expect_err("self demote");
    assert_eq!(self_demote.code, Code::FailedPrecondition);
    admin
        .set_admin(authed(
            &root_secret,
            SetAdminRequest {
                user_name: "alice".into(),
                admin: true,
            },
        ))
        .await
        .expect("promote");
    admin
        .revoke_user_token(authed(
            &alice_secret,
            RevokeUserTokenRequest {
                id: root_token.id.to_string(),
            },
        ))
        .await
        .expect("alice, now admin, revokes root's token");
    let gone = account
        .who_am_i(authed(&root_secret, WhoAmIRequest {}))
        .await
        .expect_err("root's token is revoked");
    assert_eq!(gone.code, Code::Unauthenticated);

    server.shutdown();
}
