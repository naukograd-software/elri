//! The provider's HTTP API: the models an account may use, and streamed
//! Responses API turns. Requests never ask the provider to store them.

use elri_proto::elri::v1::ai::Model;

use super::{Ai, AiError};

/// What the stream reports as it goes.
#[derive(Debug, Clone)]
pub enum StreamEvent {
    /// A piece of the answer's text.
    TextDelta(String),
}

/// A finished response: the items it produced, in order.
#[derive(Debug, Clone, Default)]
pub struct Completed {
    /// Output items as the provider sent them.
    pub items: Vec<serde_json::Value>,
}

async fn provider_error(resp: reqwest::Response) -> AiError {
    let status = resp.status();
    let body: serde_json::Value = resp.json().await.unwrap_or_default();
    let err = body.get("error").cloned().unwrap_or_default();
    let code = err
        .get("code")
        .and_then(|c| c.as_str())
        .or_else(|| err.get("type").and_then(|c| c.as_str()))
        .map_or_else(|| status.as_u16().to_string(), str::to_owned);
    let message = err
        .get("message")
        .and_then(|m| m.as_str())
        .or_else(|| body.get("detail").and_then(|d| d.as_str()))
        .unwrap_or("")
        .to_owned();
    AiError::Provider { code, message }
}

fn event_error(v: &serde_json::Value) -> AiError {
    let err = v
        .pointer("/response/error")
        .or_else(|| v.get("error"))
        .cloned()
        .unwrap_or_default();
    AiError::Provider {
        code: err
            .get("code")
            .and_then(|c| c.as_str())
            .unwrap_or("response_failed")
            .to_owned(),
        message: err
            .get("message")
            .and_then(|m| m.as_str())
            .unwrap_or("")
            .to_owned(),
    }
}

impl Ai {
    fn api(&self, path: &str) -> String {
        format!("{}/{path}", self.endpoints.api_base.trim_end_matches('/'))
    }

    /// Models the account may use, in the provider's order.
    pub async fn list_models(&self, account: &str) -> Result<Vec<Model>, AiError> {
        let token = self.access_token(account).await?;
        let resp = self
            .net
            .client()
            .get(self.api("models"))
            .bearer_auth(&token)
            .send()
            .await
            .map_err(|e| AiError::Transport(e.without_url().to_string()))?;
        if !resp.status().is_success() {
            return Err(provider_error(resp).await);
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| AiError::Transport(e.without_url().to_string()))?;
        // Plan accounts get `models` with a visibility; API keys get `data`.
        if let Some(models) = body.get("models").and_then(|m| m.as_array()) {
            return Ok(models
                .iter()
                .filter(|m| {
                    m.get("visibility")
                        .and_then(|v| v.as_str())
                        .is_none_or(|v| v == "list")
                })
                .filter_map(|m| {
                    let slug = m.get("slug").and_then(|s| s.as_str())?.to_owned();
                    let display_name = m
                        .get("display_name")
                        .and_then(|s| s.as_str())
                        .unwrap_or(&slug)
                        .to_owned();
                    Some(Model { slug, display_name })
                })
                .collect());
        }
        Ok(body
            .get("data")
            .and_then(|d| d.as_array())
            .map(|d| {
                d.iter()
                    .filter_map(|m| m.get("id").and_then(|s| s.as_str()))
                    .map(|id| Model {
                        slug: id.to_owned(),
                        display_name: id.to_owned(),
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Runs one Responses API request and streams it. A rejected access
    /// token is refreshed once and the request repeated.
    pub async fn respond(
        &self,
        account: &str,
        body: &serde_json::Value,
        on_event: &mut (dyn FnMut(StreamEvent) + Send),
    ) -> Result<Completed, AiError> {
        let mut token = self.access_token(account).await?;
        for attempt in 0..2 {
            let resp = self
                .net
                .client()
                .post(self.api("responses"))
                .bearer_auth(&token)
                .header("accept", "text/event-stream")
                .json(body)
                .send()
                .await
                .map_err(|e| AiError::Transport(e.without_url().to_string()))?;
            if resp.status() == reqwest::StatusCode::UNAUTHORIZED && attempt == 0 {
                let is_plan = self.store.ai_account(account).await?.is_some_and(|(a, _)| {
                    a.kind() == elri_proto::elri::v1::ai::AccountKind::Chatgpt
                });
                if is_plan {
                    token = self.refresh(account, true).await?;
                    continue;
                }
            }
            if !resp.status().is_success() {
                return Err(provider_error(resp).await);
            }
            return read_stream(resp, on_event).await;
        }
        Err(AiError::SignIn(
            "the provider refused the refreshed token".to_owned(),
        ))
    }
}

async fn read_stream(
    resp: reqwest::Response,
    on_event: &mut (dyn FnMut(StreamEvent) + Send),
) -> Result<Completed, AiError> {
    use futures_util::StreamExt as _;
    let mut body = resp.bytes_stream();
    let mut buf: Vec<u8> = Vec::new();
    let mut done = Completed::default();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|e| AiError::Transport(e.without_url().to_string()))?;
        buf.extend_from_slice(&chunk);
        while let Some(end) = find_block_end(&buf) {
            let block: Vec<u8> = buf.drain(..end.0).collect();
            buf.drain(..end.1);
            let text = String::from_utf8_lossy(&block);
            let data: String = text
                .lines()
                .filter_map(|l| l.strip_prefix("data:"))
                .map(str::trim_start)
                .collect::<Vec<_>>()
                .join("\n");
            if data.is_empty() || data == "[DONE]" {
                continue;
            }
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) else {
                continue;
            };
            match v.get("type").and_then(|t| t.as_str()).unwrap_or("") {
                "response.output_text.delta" => {
                    if let Some(d) = v.get("delta").and_then(|d| d.as_str()) {
                        on_event(StreamEvent::TextDelta(d.to_owned()));
                    }
                }
                "response.output_item.done" => {
                    if let Some(item) = v.get("item") {
                        done.items.push(item.clone());
                    }
                }
                "response.completed" => return Ok(done),
                "response.incomplete" => {
                    let reason = v
                        .pointer("/response/incomplete_details/reason")
                        .and_then(|r| r.as_str())
                        .unwrap_or("unknown");
                    return Err(AiError::Provider {
                        code: "incomplete".to_owned(),
                        message: reason.to_owned(),
                    });
                }
                "response.failed" | "error" => return Err(event_error(&v)),
                _ => {}
            }
        }
    }
    Err(AiError::Transport(
        "the stream ended before the response completed".to_owned(),
    ))
}

/// The end of the first complete event block: its length and the
/// separator's.
fn find_block_end(buf: &[u8]) -> Option<(usize, usize)> {
    let lf = buf.windows(2).position(|w| w == b"\n\n").map(|i| (i, 2));
    let crlf = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|i| (i, 4));
    match (lf, crlf) {
        (Some(a), Some(b)) => Some(if a.0 <= b.0 { a } else { b }),
        (a, b) => a.or(b),
    }
}
