//! `elri-server`: serve; create the first user without serving; administer
//! a running server over its own protocol.

use std::io::{BufRead as _, IsTerminal as _};
use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use elri_proto::elri::v1::account::account_service_ws::AccountServiceClient;
use elri_proto::elri::v1::account::admin_service_ws::AdminServiceClient;
use elri_proto::elri::v1::account::{
    CreateUserRequest, IssueTokenRequest, IssueUserTokenRequest, ListTokensRequest,
    ListUserTokensRequest, ListUsersRequest, RevokeTokenRequest, RevokeUserTokenRequest,
    SetAdminRequest, Token, User, WhoAmIRequest,
};
use elri_server::store::ServerStore;
use elri_server::{Config, Server};
use serde_json::json;
use ws_proto::{Client, ClientConfig, Request};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Args {
    /// Directory holding the server database and Space files.
    #[arg(
        long,
        env = "ELRI_SERVER_DATA_DIR",
        default_value = "./elri-server-data"
    )]
    data_dir: PathBuf,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Run the server.
    Serve {
        /// Address to listen on.
        #[arg(long, default_value = "127.0.0.1:8710")]
        bind: SocketAddr,
    },
    /// Manage users directly in the database; the server must be stopped.
    /// This is how the first admin is created.
    User {
        #[command(subcommand)]
        cmd: UserCmd,
    },
    /// Administer a running server. The token is read from `ELRI_TOKEN`
    /// or, failing that, from standard input.
    Admin {
        /// Server URL.
        #[arg(long, env = "ELRI_SERVER_URL", default_value = "ws://127.0.0.1:8710/")]
        server: String,
        #[command(subcommand)]
        cmd: AdminCmd,
    },
}

#[derive(Subcommand, Debug)]
enum UserCmd {
    /// Create a user and print its token once.
    Add {
        /// Unique user name.
        name: String,
        /// Grant the admin flag.
        #[arg(long)]
        admin: bool,
        /// Label of the first token.
        #[arg(long, default_value = "first")]
        label: String,
    },
}

#[derive(Subcommand, Debug)]
enum AdminCmd {
    /// Who the token belongs to.
    Whoami,
    /// Users of the server (admin).
    Users,
    /// Create a user and print its first token once (admin).
    UserAdd {
        name: String,
        #[arg(long)]
        admin: bool,
        #[arg(long, default_value = "first")]
        label: String,
    },
    /// Grant or remove the admin flag (admin).
    SetAdmin {
        name: String,
        #[arg(long)]
        off: bool,
    },
    /// Tokens of a user (admin), or your own when no user is given.
    Tokens {
        user: Option<String>,
        #[arg(long)]
        include_revoked: bool,
    },
    /// Issue a token for a user (admin), or for yourself when no user is
    /// given; the secret is printed once.
    TokenIssue {
        #[arg(long)]
        user: Option<String>,
        #[arg(long, default_value = "")]
        label: String,
    },
    /// Revoke a token by id: your own, or anyone's as admin.
    TokenRevoke { id: String },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let args = Args::parse();
    match args.cmd {
        Cmd::Serve { bind } => {
            let server = Server::start(Config {
                data_dir: args.data_dir,
                bind,
            })
            .await?;
            println!("listening on {}", server.url());
            tokio::signal::ctrl_c().await?;
            server.shutdown();
            Ok(())
        }
        Cmd::User { cmd } => match cmd {
            UserCmd::Add { name, admin, label } => {
                let registry = ServerStore::open(&args.data_dir).await?;
                let (user, token, secret) = registry.create_user(&name, admin, &label).await?;
                println!(
                    "user {} ({}){}",
                    user.name,
                    user.id,
                    if admin { " admin" } else { "" }
                );
                println!("token {} ({})", token.id, token.label);
                println!("secret {secret}");
                Ok(())
            }
        },
        Cmd::Admin { server, cmd } => {
            let out = run_admin(&server, cmd).await?;
            println!("{}", serde_json::to_string_pretty(&out)?);
            Ok(())
        }
    }
}

fn read_token() -> Result<String, Box<dyn std::error::Error>> {
    if let Ok(t) = std::env::var("ELRI_TOKEN") {
        if !t.trim().is_empty() {
            return Ok(t.trim().to_owned());
        }
    }
    if std::io::stdin().is_terminal() {
        eprint!("token: ");
    }
    let mut line = String::new();
    std::io::stdin().lock().read_line(&mut line)?;
    let t = line.trim();
    if t.is_empty() {
        return Err("a token is required: set ELRI_TOKEN or pipe it on stdin".into());
    }
    Ok(t.to_owned())
}

fn user_json(u: &User) -> serde_json::Value {
    json!({ "id": u.id, "name": u.name, "admin": u.admin, "created_at_ms": u.created_at_ms })
}

fn token_json(t: &Token) -> serde_json::Value {
    json!({
        "id": t.id,
        "user_id": t.user_id,
        "label": t.label,
        "created_at_ms": t.created_at_ms,
        "last_used_at_ms": t.last_used_at_ms,
        "last_device_id": t.last_device_id,
        "revoked_at_ms": t.revoked_at_ms,
    })
}

fn authed<T>(token: &str, msg: T) -> Request<T> {
    Request::new(msg).with_header("authorization", format!("Bearer {token}"))
}

async fn run_admin(
    server: &str,
    cmd: AdminCmd,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    let token = read_token()?;
    let conn = Client::dial(server, ClientConfig::default()).await?;
    let account = AccountServiceClient::new(conn.clone());
    let admin = AdminServiceClient::new(conn);
    match cmd {
        AdminCmd::Whoami => {
            let res = account
                .who_am_i(authed(&token, WhoAmIRequest {}))
                .await?
                .msg;
            Ok(json!({
                "user": res.user.as_ref().map(user_json),
                "token": res.token.as_ref().map(token_json),
            }))
        }
        AdminCmd::Users => {
            let res = admin
                .list_users(authed(&token, ListUsersRequest {}))
                .await?
                .msg;
            Ok(json!(res.users.iter().map(user_json).collect::<Vec<_>>()))
        }
        AdminCmd::UserAdd {
            name,
            admin: flag,
            label,
        } => {
            let res = admin
                .create_user(authed(
                    &token,
                    CreateUserRequest {
                        name,
                        admin: flag,
                        label,
                    },
                ))
                .await?
                .msg;
            Ok(json!({
                "user": res.user.as_ref().map(user_json),
                "token": res.token.as_ref().map(token_json),
                "secret": res.secret,
            }))
        }
        AdminCmd::SetAdmin { name, off } => {
            let res = admin
                .set_admin(authed(
                    &token,
                    SetAdminRequest {
                        user_name: name,
                        admin: !off,
                    },
                ))
                .await?
                .msg;
            Ok(json!({ "user": res.user.as_ref().map(user_json) }))
        }
        other => run_admin_tokens(&token, &account, &admin, other).await,
    }
}

async fn run_admin_tokens(
    token: &str,
    account: &AccountServiceClient,
    admin: &AdminServiceClient,
    cmd: AdminCmd,
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    match cmd {
        AdminCmd::Tokens {
            user,
            include_revoked,
        } => {
            let tokens = if let Some(user_name) = user {
                admin
                    .list_user_tokens(authed(
                        token,
                        ListUserTokensRequest {
                            user_name,
                            include_revoked,
                        },
                    ))
                    .await?
                    .msg
                    .tokens
            } else {
                account
                    .list_tokens(authed(token, ListTokensRequest { include_revoked }))
                    .await?
                    .msg
                    .tokens
            };
            Ok(json!(tokens.iter().map(token_json).collect::<Vec<_>>()))
        }
        AdminCmd::TokenIssue { user, label } => {
            let (tok, secret) = if let Some(user_name) = user {
                let res = admin
                    .issue_user_token(authed(token, IssueUserTokenRequest { user_name, label }))
                    .await?
                    .msg;
                (res.token, res.secret)
            } else {
                let res = account
                    .issue_token(authed(token, IssueTokenRequest { label }))
                    .await?
                    .msg;
                (res.token, res.secret)
            };
            Ok(json!({ "token": tok.as_ref().map(token_json), "secret": secret }))
        }
        AdminCmd::TokenRevoke { id } => {
            // Own tokens go through the account service; an admin falls back
            // to the admin service for anyone else's.
            if account
                .revoke_token(authed(token, RevokeTokenRequest { id: id.clone() }))
                .await
                .is_err()
            {
                admin
                    .revoke_user_token(authed(token, RevokeUserTokenRequest { id: id.clone() }))
                    .await?;
            }
            Ok(json!({ "revoked": id }))
        }
        AdminCmd::Whoami
        | AdminCmd::Users
        | AdminCmd::UserAdd { .. }
        | AdminCmd::SetAdmin { .. } => Err("not a token command".into()),
    }
}
