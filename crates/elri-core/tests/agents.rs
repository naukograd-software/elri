//! Signing in with ChatGPT and running an agent against a local stand-in
//! for the provider: the OAuth flow with PKCE and a registered client, ID
//! token checks, refresh, models, a turn in which the agent describes the
//! API, creates an Object through it, and is refused a method it may not
//! call; its writes are attributed to it; consent and sign-out.

#![allow(clippy::expect_used, clippy::panic, clippy::too_many_lines)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use elri_core::ai::auth::b64url;
use elri_core::ai::Endpoints;
use elri_core::{BuildInfo, Config, Core, Endpoint};
use elri_proto::elri::v1::ai::agent_service_ws::AgentServiceClient;
use elri_proto::elri::v1::ai::ai_service_ws::AiServiceClient;
use elri_proto::elri::v1::ai::{
    turn_event, Agent, CreateAgentRequest, GetConversationRequest, GetSignInRequest,
    GrantAgentRequest, ListModelsRequest, MessageRole, SendRequest, SignInState, SignOutRequest,
    StartConversationRequest, StartSignInRequest, TurnEvent,
};
use elri_proto::elri::v1::change::change_service_ws::ChangeServiceClient;
use elri_proto::elri::v1::change::ListChangesRequest;
use elri_proto::elri::v1::context::context_service_ws::ContextServiceClient;
use elri_proto::elri::v1::context::ListMappingsRequest;
use elri_proto::elri::v1::object::object_service_ws::ObjectServiceClient;
use elri_proto::elri::v1::object::{ListObjectsRequest, ObjectKind};
use elri_proto::elri::v1::script::script_service_ws::ScriptServiceClient;
use elri_proto::elri::v1::script::{ListScriptsRequest, RunScriptRequest};
use elri_proto::elri::v1::settings::settings_service_ws::SettingsServiceClient;
use elri_proto::elri::v1::settings::{AiSettings, ProxyMode, ProxySettings, UpdateSettingsRequest};
use elri_proto::elri::v1::space::space_service_ws::SpaceServiceClient;
use elri_proto::elri::v1::space::CreateSpaceRequest;
use sha2::{Digest as _, Sha256};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use ws_proto::{Client, ClientConfig, Code, Request};

struct Authed(String);

impl Authed {
    fn req<T>(&self, msg: T) -> Request<T> {
        Request::new(msg).with_header("authorization", self.0.clone())
    }
}

#[derive(Default)]
struct MockState {
    base: String,
    nonce: String,
    challenge: String,
    client_id: String,
    refreshes: usize,
    revoked: usize,
    responses: Vec<serde_json::Value>,
}

struct Mock {
    state: Arc<Mutex<MockState>>,
    encoding: jsonwebtoken::EncodingKey,
    jwk: serde_json::Value,
}

fn id_token(m: &Mock, aud: &str, nonce: &str) -> String {
    let base = m.state.lock().expect("state").base.clone();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("time")
        .as_secs();
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::ES256);
    header.kid = Some("k1".to_owned());
    jsonwebtoken::encode(
        &header,
        &serde_json::json!({
            "iss": base, "aud": aud, "sub": "user-123", "email": "ada@example.com",
            "nonce": nonce, "iat": now, "exp": now + 3600,
        }),
        &m.encoding,
    )
    .expect("sign")
}

fn form(body: &str) -> HashMap<String, String> {
    reqwest::Url::parse(&format!("http://x/?{body}"))
        .expect("form")
        .query_pairs()
        .into_owned()
        .collect()
}

fn sse(events: &[serde_json::Value]) -> String {
    use std::fmt::Write as _;
    events.iter().fold(String::new(), |mut out, e| {
        let _ = write!(
            out,
            "event: {}\ndata: {e}\n\n",
            e["type"].as_str().unwrap_or("")
        );
        out
    })
}

fn function_call(name: &str, args: &serde_json::Value, n: usize) -> Vec<serde_json::Value> {
    let item = serde_json::json!({
        "type": "function_call", "id": format!("fc_{n}"), "call_id": format!("call_{n}"),
        "namespace": "elri", "name": name, "arguments": args.to_string(), "status": "completed",
    });
    vec![
        serde_json::json!({"type": "response.output_item.done", "item": item}),
        serde_json::json!({"type": "response.completed", "response": {"output": [item]}}),
    ]
}

/// What the stand-in model does next, by how many tool results it has.
fn respond(m: &Mock, body: &serde_json::Value) -> String {
    assert_eq!(body["store"], false);
    assert_eq!(body["stream"], true);
    assert_eq!(body["model"], "gpt-test");
    assert_eq!(body["reasoning"]["effort"], "low");
    assert_eq!(body["tools"][0]["type"], "namespace");
    let input = body["input"].as_array().expect("input");
    for item in input {
        assert!(
            item.get("id").is_none(),
            "item ids are not sent back: {item}"
        );
        assert!(
            item.get("at_ms").is_none(),
            "local fields are not sent: {item}"
        );
    }
    m.state.lock().expect("state").responses.push(body.clone());
    let instructions = body["instructions"].as_str().expect("instructions");
    assert!(instructions.contains("You are an agent inside elri"));
    assert!(instructions.contains("- interfaces:"), "skills are listed");
    let space = instructions
        .split("Your Space is ")
        .nth(1)
        .and_then(|r| r.split('.').next())
        .expect("space")
        .to_owned();
    let results = input
        .iter()
        .filter(|i| i["type"] == "function_call_output")
        .count();
    match results {
        0 => sse(&function_call(
            "describe",
            &serde_json::json!({"target": ""}),
            0,
        )),
        1 => {
            let last = input.last().expect("last");
            assert!(
                last["output"]
                    .as_str()
                    .expect("out")
                    .contains("elri.v1.ui.InterfaceService"),
                "describe lists the API"
            );
            let request = serde_json::json!({
                "space_id": space,
                "kind": "OBJECT_KIND_ENTITY",
                "fields": {"title": {"string": "Written by the agent"}},
            });
            sse(&function_call(
                "call",
                &serde_json::json!({
                    "method": "elri.v1.object.ObjectService/CreateObject",
                    "request": request.to_string(),
                }),
                1,
            ))
        }
        2 => sse(&function_call(
            "call",
            &serde_json::json!({
                "method": "elri.v1.script.ScriptService/CreateScript",
                "request": serde_json::json!({
                    "space_id": space, "name": "left behind", "source": "return 1",
                }).to_string(),
            }),
            2,
        )),
        3 => sse(&function_call(
            "call",
            &serde_json::json!({
                "method": "elri.v1.context.ContextService/ProposeMapping",
                "request": serde_json::json!({
                    "space_id": space, "name": "priority",
                    "members": [{"space_id": space, "field_id": space}],
                }).to_string(),
            }),
            3,
        )),
        4 => sse(&function_call(
            "call",
            &serde_json::json!({
                "method": "elri.v1.script.ScriptService/RunScript",
                "request": serde_json::json!({"space_id": space, "id": "x"}).to_string(),
            }),
            4,
        )),
        _ => {
            let item = serde_json::json!({
                "type": "message", "id": "msg_1", "role": "assistant", "status": "completed",
                "content": [{"type": "output_text", "text": "Done.", "annotations": []}],
            });
            sse(&[
                serde_json::json!({"type": "response.output_text.delta", "delta": "Do"}),
                serde_json::json!({"type": "response.output_text.delta", "delta": "ne."}),
                serde_json::json!({"type": "response.output_item.done", "item": item}),
                serde_json::json!({"type": "response.completed", "response": {"output": [item]}}),
            ])
        }
    }
}

async fn handle(m: Arc<Mock>, mut sock: tokio::net::TcpStream) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let (head_end, len) = loop {
        let n = sock.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            return;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..i]).to_lowercase();
            let len = head
                .lines()
                .find_map(|l| l.strip_prefix("content-length:"))
                .and_then(|v| v.trim().parse::<usize>().ok())
                .unwrap_or(0);
            break (i + 4, len);
        }
    };
    while buf.len() < head_end + len {
        let n = sock.read(&mut tmp).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
    }
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let body = String::from_utf8_lossy(&buf[head_end..]).into_owned();
    let mut first = head.lines().next().unwrap_or("").split_whitespace();
    let method = first.next().unwrap_or("").to_owned();
    let path = first.next().unwrap_or("").to_owned();
    let base = m.state.lock().expect("state").base.clone();
    let (ctype, out): (&str, String) = match (method.as_str(), path.as_str()) {
        ("GET", "/.well-known/openid-configuration") => (
            "application/json",
            serde_json::json!({
                "issuer": base,
                "authorization_endpoint": format!("{base}/api/accounts/authorize"),
                "token_endpoint": format!("{base}/api/accounts/oauth/token"),
                "jwks_uri": format!("{base}/.well-known/jwks.json"),
                "revocation_endpoint": format!("{base}/api/accounts/oauth/revoke"),
            })
            .to_string(),
        ),
        ("GET", "/.well-known/jwks.json") => (
            "application/json",
            serde_json::json!({ "keys": [m.jwk] }).to_string(),
        ),
        ("POST", "/api/accounts/oauth/token") => {
            let f = form(&body);
            let mut st = m.state.lock().expect("state");
            let cid = st.client_id.clone();
            assert_eq!(
                f.get("client_id"),
                Some(&cid),
                "the issued client id is used"
            );
            assert_eq!(
                f.get("resource").map(String::as_str),
                Some("https://api.openai.com/v1")
            );
            match f.get("grant_type").map(String::as_str) {
                Some("authorization_code") => {
                    let verifier = f.get("code_verifier").expect("verifier");
                    assert_eq!(b64url(&Sha256::digest(verifier.as_bytes())), st.challenge);
                    assert_eq!(f.get("code").map(String::as_str), Some("code-1"));
                    let nonce = st.nonce.clone();
                    drop(st);
                    let idt = id_token(&m, &cid, &nonce);
                    (
                        "application/json",
                        serde_json::json!({
                            "access_token": "at-0", "refresh_token": "rt-0", "id_token": idt,
                            "token_type": "Bearer", "expires_in": 60,
                            "scope": "openid profile email offline_access resource.invoke chatgpt.tokens.use.direct",
                        })
                        .to_string(),
                    )
                }
                Some("refresh_token") => {
                    st.refreshes += 1;
                    let n = st.refreshes;
                    assert_eq!(
                        f.get("refresh_token"),
                        Some(&format!("rt-{}", n - 1)),
                        "rotation"
                    );
                    (
                        "application/json",
                        serde_json::json!({
                            "access_token": format!("at-{n}"), "refresh_token": format!("rt-{n}"),
                            "token_type": "Bearer", "expires_in": 60,
                        })
                        .to_string(),
                    )
                }
                other => panic!("grant {other:?}"),
            }
        }
        ("POST", "/api/accounts/oauth/revoke") => {
            m.state.lock().expect("state").revoked += 1;
            ("application/json", String::new())
        }
        ("GET", "/v1/models") => (
            "application/json",
            serde_json::json!({"models": [
                {"slug": "gpt-test", "display_name": "GPT Test", "visibility": "list"},
                {"slug": "internal", "display_name": "Hidden", "visibility": "hide"},
            ]})
            .to_string(),
        ),
        ("POST", "/v1/responses") => {
            assert!(
                head.to_lowercase().contains("authorization: bearer at-"),
                "the plan token is sent"
            );
            let req: serde_json::Value = serde_json::from_str(&body).expect("json");
            ("text/event-stream", respond(&m, &req))
        }
        _ => ("text/plain", String::new()),
    };
    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{out}",
        out.len()
    );
    let _ = sock.write_all(resp.as_bytes()).await;
}

async fn start_mock() -> Arc<Mock> {
    let key = rcgen::KeyPair::generate().expect("key");
    let raw = key.public_key_raw();
    let jwk = serde_json::json!({
        "kty": "EC", "crv": "P-256", "kid": "k1", "alg": "ES256", "use": "sig",
        "x": b64url(&raw[1..33]), "y": b64url(&raw[33..65]),
    });
    let encoding =
        jsonwebtoken::EncodingKey::from_ec_pem(key.serialize_pem().as_bytes()).expect("pem");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let base = format!(
        "http://127.0.0.1:{}",
        listener.local_addr().expect("addr").port()
    );
    let m = Arc::new(Mock {
        state: Arc::new(Mutex::new(MockState {
            base,
            client_id: "oaiapp_test".to_owned(),
            ..MockState::default()
        })),
        encoding,
        jwk,
    });
    let server = Arc::clone(&m);
    tokio::spawn(async move {
        loop {
            let Ok((sock, _)) = listener.accept().await else {
                return;
            };
            tokio::spawn(handle(Arc::clone(&server), sock));
        }
    });
    m
}

async fn turn(
    agents: &AgentServiceClient,
    a: &Authed,
    space: &str,
    chat: &str,
    text: &str,
) -> Vec<TurnEvent> {
    let stream = agents
        .send(a.req(SendRequest {
            space_id: space.to_owned(),
            conversation_id: chat.to_owned(),
            text: text.to_owned(),
        }))
        .await
        .expect("send");
    let mut out = Vec::new();
    while let Some(ev) = tokio::time::timeout(Duration::from_secs(20), stream.recv())
        .await
        .expect("turn stalled")
        .expect("recv")
    {
        out.push(ev);
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sign_in_with_chatgpt_and_run_an_agent() {
    let mock = start_mock().await;
    let base = mock.state.lock().expect("state").base.clone();
    let dir = std::env::temp_dir().join(format!("elri-agents-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut config = Config::new(
        &dir,
        BuildInfo {
            version: "test",
            commit: "",
        },
    );
    config.semantic = elri_core::semantic::Backend::Hashing;
    config.openai = Endpoints {
        issuer: base.clone(),
        api_base: format!("{base}/v1"),
        callback_port: 0,
    };
    let _core = Core::start(config).await.expect("core");
    let endpoint = Endpoint::read(&dir).expect("endpoint");
    let conn = Client::dial(&endpoint.url, ClientConfig::default())
        .await
        .expect("dial");
    let a = Authed(format!("Bearer {}", endpoint.token));
    let spaces = SpaceServiceClient::new(conn.clone());
    let settings = SettingsServiceClient::new(conn.clone());
    let ai = AiServiceClient::new(conn.clone());
    let agents = AgentServiceClient::new(conn.clone());
    let scripts = ScriptServiceClient::new(conn.clone());
    let contexts = ContextServiceClient::new(conn.clone());
    let objects = ObjectServiceClient::new(conn.clone());
    let changes = ChangeServiceClient::new(conn);
    let space = spaces
        .create_space(a.req(CreateSpaceRequest { name: "s".into() }))
        .await
        .expect("space")
        .msg
        .space
        .expect("space")
        .id;
    let set_ai = |authorized: Vec<String>| {
        let req = a.req(UpdateSettingsRequest {
            proxy: Some(ProxySettings {
                mode: ProxyMode::None.into(),
                ..ProxySettings::default()
            }),
            semantic: None,
            ai: Some(AiSettings {
                default_account_id: String::new(),
                default_model: "gpt-test".into(),
                default_effort: "low".into(),
                authorized_space_ids: authorized,
            }),
        });
        let settings = settings.clone();
        async move { settings.update_settings(req).await.expect("settings") }
    };
    set_ai(vec![space.clone()]).await;

    // Sign in: the URL registers a new client for this host with PKCE.
    let started = ai
        .start_sign_in(a.req(StartSignInRequest {
            account_id: String::new(),
            label: "personal".into(),
        }))
        .await
        .expect("start")
        .msg;
    let url = reqwest::Url::parse(&started.authorize_url).expect("url");
    assert!(url
        .as_str()
        .starts_with(&format!("{base}/api/accounts/authorize")));
    let q: HashMap<String, String> = url.query_pairs().into_owned().collect();
    assert_eq!(q["client_id"], "dynamic_agent_client");
    assert_eq!(q["agent_name_hint"], "elri");
    assert!(q["ext_agent_host_id"].starts_with("urn:uuid:"));
    assert_eq!(q["code_challenge_method"], "S256");
    assert!(q["scope"].contains("chatgpt.tokens.use.direct"));
    assert_eq!(q["resource"], "https://api.openai.com/v1");
    assert!(q["redirect_uri"].starts_with("http://127.0.0.1:"));
    assert!(q["redirect_uri"].ends_with("/auth/callback"));
    {
        let mut st = mock.state.lock().expect("state");
        st.nonce = q["nonce"].clone();
        st.challenge = q["code_challenge"].clone();
    }
    // The browser comes back with the code and the issued client id.
    let callback = reqwest::Url::parse_with_params(
        &q["redirect_uri"],
        &[
            ("code", "code-1"),
            ("state", q["state"].as_str()),
            ("scope", q["scope"].as_str()),
            ("client_id", "oaiapp_test"),
        ],
    )
    .expect("callback");
    let page = reqwest::Client::builder()
        .no_proxy()
        .build()
        .expect("client")
        .get(callback)
        .send()
        .await
        .expect("callback")
        .text()
        .await
        .expect("page");
    assert!(page.contains("Signed in"), "{page}");
    let account = loop {
        let st = ai
            .get_sign_in(a.req(GetSignInRequest {
                attempt_id: started.attempt_id.clone(),
            }))
            .await
            .expect("state")
            .msg;
        match st.state() {
            SignInState::Done => break st.account.expect("account"),
            SignInState::Failed => panic!("sign-in failed: {}", st.error),
            _ => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    };
    assert_eq!(account.email, "ada@example.com");
    assert_eq!(account.client_id, "oaiapp_test");
    let secrets = std::fs::read_to_string(dir.join("secrets.json")).expect("secrets");
    assert!(
        secrets.contains("rt-0"),
        "tokens are in the credential store"
    );
    let db = std::fs::read(dir.join("device.db")).expect("db");
    assert!(
        !db.windows(4).any(|w| w == b"rt-0"),
        "and not in the database"
    );

    // Models: hidden ones are left out.
    let models = ai
        .list_models(a.req(ListModelsRequest {
            account_id: account.id.clone(),
        }))
        .await
        .expect("models")
        .msg
        .models;
    assert_eq!(models.len(), 1);
    assert_eq!(models[0].slug, "gpt-test");
    assert!(
        mock.state.lock().expect("state").refreshes >= 1,
        "short-lived tokens refresh"
    );

    // Even an agent of this Space only waits for the user's grant.
    let agent = agents
        .create_agent(a.req(CreateAgentRequest {
            space_id: space.clone(),
            agent: Some(Agent {
                name: "helper".into(),
                instructions: "Be brief.".into(),
                ..Agent::default()
            }),
        }))
        .await
        .expect("agent")
        .msg
        .agent
        .expect("agent");
    assert!(!agent.benign);
    let chat = agents
        .start_conversation(a.req(StartConversationRequest {
            space_id: space.clone(),
            agent_id: agent.id.clone(),
            title: String::new(),
        }))
        .await
        .expect("conversation")
        .msg
        .conversation
        .expect("conversation");
    let refused = turn(&agents, &a, &space, &chat.id, "hello").await;
    match refused.last().and_then(|e| e.event.as_ref()) {
        Some(turn_event::Event::Error(e)) => {
            assert_eq!(e.code, "consent");
            assert!(e.message.contains("not granted"), "{}", e.message);
        }
        other => panic!("expected a consent error, got {other:?}"),
    }
    agents
        .grant_agent(a.req(GrantAgentRequest {
            space_id: space.clone(),
            agent_id: agent.id.clone(),
            expected_version: agent.version.clone(),
        }))
        .await
        .expect("grant");

    let events = turn(&agents, &a, &space, &chat.id, "make a note").await;
    let kinds: Vec<&str> = events
        .iter()
        .map(|e| match &e.event {
            Some(turn_event::Event::TextDelta(_)) => "delta",
            Some(turn_event::Event::ToolCall(_)) => "call",
            Some(turn_event::Event::ToolResult(_)) => "result",
            Some(turn_event::Event::Message(_)) => "message",
            Some(turn_event::Event::Error(e)) => panic!("turn error {}: {}", e.code, e.message),
            None => "none",
        })
        .collect();
    assert_eq!(
        kinds.iter().filter(|k| **k == "call").count(),
        5,
        "{kinds:?}"
    );
    let results: Vec<_> = events
        .iter()
        .filter_map(|e| match &e.event {
            Some(turn_event::Event::ToolResult(r)) => Some(r.clone()),
            _ => None,
        })
        .collect();
    assert!(!results[0].failed);
    assert!(
        !results[1].failed,
        "create went through: {}",
        results[1].result
    );
    assert!(!results[2].failed, "script written: {}", results[2].result);
    assert!(
        !results[3].failed,
        "mapping proposed: {}",
        results[3].result
    );
    assert!(results[4].failed, "RunScript is refused");
    assert!(
        results[4].result.contains("not available"),
        "{}",
        results[4].result
    );
    assert!(kinds.contains(&"delta"));
    let last = events.last().expect("last");
    match &last.event {
        Some(turn_event::Event::Message(m)) => {
            assert_eq!(m.text, "Done.");
            assert_eq!(m.model, "gpt-test");
        }
        other => panic!("last event {other:?}"),
    }

    // The Object exists and its Change is the agent's.
    let made = objects
        .list_objects(a.req(ListObjectsRequest {
            space_id: space.clone(),
            kind: ObjectKind::Entity.into(),
            ..ListObjectsRequest::default()
        }))
        .await
        .expect("list")
        .msg
        .objects;
    assert_eq!(made.len(), 1);
    let history = changes
        .list_changes(a.req(ListChangesRequest {
            space_id: space.clone(),
            limit: 10,
            object_id: made[0].id.clone(),
        }))
        .await
        .expect("changes")
        .msg
        .changes;
    assert_eq!(history[0].actor, format!("agent:{}", agent.id));

    // The conversation keeps the whole turn.
    let saved = agents
        .get_conversation(a.req(GetConversationRequest {
            space_id: space.clone(),
            id: chat.id.clone(),
        }))
        .await
        .expect("get")
        .msg
        .conversation
        .expect("conversation");
    let roles: Vec<MessageRole> = saved
        .messages
        .iter()
        .map(elri_proto::elri::v1::ai::Message::role)
        .collect();
    assert_eq!(
        roles,
        vec![
            MessageRole::User,
            MessageRole::Tool,
            MessageRole::Tool,
            MessageRole::Tool,
            MessageRole::Tool,
            MessageRole::Tool,
            MessageRole::Assistant
        ]
    );
    assert!(saved.messages[5].failed);
    assert_eq!(saved.messages[6].text, "Done.");

    // A script an agent wrote is never granted without asking, even one
    // that only touches its own Space.
    let left = scripts
        .list_scripts(a.req(ListScriptsRequest {
            space_id: space.clone(),
            include_deleted: false,
        }))
        .await
        .expect("scripts")
        .msg
        .scripts;
    assert_eq!(left.len(), 1);
    assert!(left[0].benign);
    let err = scripts
        .run_script(a.req(RunScriptRequest {
            space_id: space.clone(),
            id: left[0].id.clone(),
            args_json: String::new(),
        }))
        .await
        .expect_err("needs a grant");
    assert_eq!(err.code, Code::PermissionDenied);

    // A mapping an agent proposed waits for a person.
    let mappings = contexts
        .list_mappings(a.req(ListMappingsRequest {
            space_id: space.clone(),
            approved_only: false,
            include_deleted: false,
        }))
        .await
        .expect("mappings")
        .msg
        .mappings;
    assert_eq!(mappings.len(), 1);
    assert!(!mappings[0].approved);
    assert_eq!(mappings[0].proposed_by, format!("agent:{}", agent.id));

    // Without consent for the Space nothing is sent.
    let before = mock.state.lock().expect("state").responses.len();
    set_ai(Vec::new()).await;
    let events = turn(&agents, &a, &space, &chat.id, "again").await;
    match &events.last().expect("event").event {
        Some(turn_event::Event::Error(e)) => assert_eq!(e.code, "consent"),
        other => panic!("expected a consent error, got {other:?}"),
    }
    assert_eq!(mock.state.lock().expect("state").responses.len(), before);

    // Sign out: revoked at the provider, the account needs a sign-in.
    let out = ai
        .sign_out(a.req(SignOutRequest {
            account_id: account.id.clone(),
        }))
        .await
        .expect("sign out")
        .msg;
    assert!(out.revoked);
    assert_eq!(mock.state.lock().expect("state").revoked, 1);
    let secrets = std::fs::read_to_string(dir.join("secrets.json")).expect("secrets");
    assert!(!secrets.contains("rt-"), "tokens are cleared");
}
