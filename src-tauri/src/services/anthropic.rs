use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

const API_URL: &str = "https://api.anthropic.com/v1/messages";
const MODEL: &str = "claude-haiku-4-5-20251001";
const API_VERSION: &str = "2023-06-01";
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_RETRIES: u32 = 3;

#[derive(Debug, Deserialize)]
struct MessagesResponse {
    content: Vec<ContentBlock>,
}

#[derive(Debug, Deserialize)]
struct ContentBlock {
    #[serde(rename = "type")]
    block_type: String,
    text: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: Option<ErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct ErrorDetail {
    message: Option<String>,
}

/// Test the Anthropic API connection by sending a simple message.
pub async fn test_connection(api_key: &str) -> Result<Value> {
    if api_key.is_empty() {
        anyhow::bail!("Anthropic API key is empty");
    }

    let response = chat(api_key, None, "Say hi in exactly 3 words.", false).await?;
    Ok(json!({
        "connected": true,
        "model": MODEL,
        "response": response,
    }))
}

/// Send a chat message to Anthropic's Messages API.
/// Matches the interface pattern used by ollama::generate — takes a prompt, returns a String.
pub async fn chat(
    api_key: &str,
    system: Option<&str>,
    prompt: &str,
    json_mode: bool,
) -> Result<String> {
    if api_key.is_empty() {
        anyhow::bail!("Anthropic API key not configured");
    }

    let client = reqwest::Client::new();

    let mut body = json!({
        "model": MODEL,
        "max_tokens": 4096,
        "messages": [
            { "role": "user", "content": prompt }
        ],
    });

    if let Some(sys) = system {
        body["system"] = json!(sys);
    }

    // Request JSON output hint — Anthropic doesn't have a formal json_mode,
    // but we can guide via system prompt or let the caller's prompt handle it.
    // The prompt already asks for JSON, so json_mode is informational only.
    let _ = json_mode;

    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s
            let delay = std::time::Duration::from_secs(1 << (attempt - 1));
            log::info!("[Anthropic] Retry attempt {} after {:?}", attempt + 1, delay);
            tokio::time::sleep(delay).await;
        }

        let resp = client
            .post(API_URL)
            .header("x-api-key", api_key)
            .header("anthropic-version", API_VERSION)
            .header("content-type", "application/json")
            .json(&body)
            .timeout(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                log::warn!("[Anthropic] Request failed (attempt {}): {}", attempt + 1, e);
                last_error = Some(e.into());
                continue;
            }
        };

        let status = resp.status();

        // Retry on 429 (rate limit) or 529 (overloaded)
        if status.as_u16() == 429 || status.as_u16() == 529 {
            let body_text = resp.text().await.unwrap_or_default();
            log::warn!(
                "[Anthropic] Rate limited/overloaded ({}), attempt {}: {}",
                status, attempt + 1, body_text
            );
            last_error = Some(anyhow::anyhow!("Anthropic {} error: {}", status, body_text));
            continue;
        }

        if !status.is_success() {
            let body_text = resp.text().await.unwrap_or_default();
            // Try to extract error message
            let err_msg = serde_json::from_str::<ErrorResponse>(&body_text)
                .ok()
                .and_then(|e| e.error)
                .and_then(|e| e.message)
                .unwrap_or_else(|| body_text.clone());
            anyhow::bail!("Anthropic API error {}: {}", status, err_msg);
        }

        let resp_body: MessagesResponse = resp.json().await?;

        // Extract text from the first text content block
        let text = resp_body
            .content
            .iter()
            .find(|b| b.block_type == "text")
            .and_then(|b| b.text.clone())
            .unwrap_or_default();

        log::info!(
            "[Anthropic] Response received ({} chars)",
            text.len()
        );

        // Clean the response — extract JSON object if present (same pattern as Ollama)
        return Ok(clean_json_response(&text));
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Anthropic request failed after {} retries", MAX_RETRIES)))
}

/// Clean the response — extract JSON object, matching Ollama's clean_json_response pattern.
fn clean_json_response(raw: &str) -> String {
    let s = raw.trim();

    // If it already starts with '{', return as-is
    if s.starts_with('{') {
        return s.to_string();
    }

    // Otherwise find the first '{' and last '}' and extract
    if let (Some(first), Some(last)) = (s.find('{'), s.rfind('}')) {
        if first < last {
            return s[first..=last].to_string();
        }
    }

    // Fallback: return trimmed string
    s.to_string()
}
