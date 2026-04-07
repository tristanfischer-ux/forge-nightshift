use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};

const API_URL: &str = "https://api.deepseek.com/v1/chat/completions";
const MODEL: &str = "deepseek-chat";
const DEFAULT_TIMEOUT_SECS: u64 = 30;
const MAX_RETRIES: u32 = 3;

#[derive(Debug, Deserialize)]
struct ChatResponse {
    choices: Vec<Choice>,
}

#[derive(Debug, Deserialize)]
struct Choice {
    message: Message,
}

#[derive(Debug, Deserialize)]
struct Message {
    content: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: Option<ErrorDetail>,
}

#[derive(Debug, Deserialize)]
struct ErrorDetail {
    message: Option<String>,
}

/// Test the DeepSeek API connection by sending a simple message.
pub async fn test_connection(api_key: &str) -> Result<Value> {
    if api_key.is_empty() {
        anyhow::bail!("DeepSeek API key is empty");
    }

    let response = chat(api_key, None, "Say hi in exactly 3 words.", false).await?;
    Ok(json!({
        "connected": true,
        "model": MODEL,
        "response": response,
    }))
}

/// Send a chat message to DeepSeek's OpenAI-compatible API.
/// Matches the interface pattern used by anthropic::chat — takes a prompt, returns a String.
pub async fn chat(
    api_key: &str,
    system: Option<&str>,
    prompt: &str,
    json_mode: bool,
) -> Result<String> {
    if api_key.is_empty() {
        anyhow::bail!("DeepSeek API key not configured");
    }

    let client = reqwest::Client::new();

    let mut messages = Vec::new();
    if let Some(sys) = system {
        messages.push(json!({ "role": "system", "content": sys }));
    }
    messages.push(json!({ "role": "user", "content": prompt }));

    let mut body = json!({
        "model": MODEL,
        "messages": messages,
        "max_tokens": 4096,
        "temperature": 0.1,
    });

    // DeepSeek supports OpenAI-style response_format for JSON mode
    if json_mode {
        body["response_format"] = json!({ "type": "json_object" });
    }

    let mut last_error: Option<anyhow::Error> = None;

    for attempt in 0..MAX_RETRIES {
        if attempt > 0 {
            // Exponential backoff: 1s, 2s, 4s
            let delay = std::time::Duration::from_secs(1 << (attempt - 1));
            log::info!("[DeepSeek] Retry attempt {} after {:?}", attempt + 1, delay);
            tokio::time::sleep(delay).await;
        }

        let resp = client
            .post(API_URL)
            .header("Authorization", format!("Bearer {}", api_key))
            .header("Content-Type", "application/json")
            .json(&body)
            .timeout(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
            .send()
            .await;

        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                log::warn!("[DeepSeek] Request failed (attempt {}): {}", attempt + 1, e);
                last_error = Some(e.into());
                continue;
            }
        };

        let status = resp.status();

        // Retry on 429 (rate limit) or 529 (overloaded)
        if status.as_u16() == 429 || status.as_u16() == 529 {
            let body_text = resp.text().await.unwrap_or_default();
            log::warn!(
                "[DeepSeek] Rate limited/overloaded ({}), attempt {}: {}",
                status, attempt + 1, body_text
            );
            last_error = Some(anyhow::anyhow!("DeepSeek {} error: {}", status, body_text));
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
            anyhow::bail!("DeepSeek API error {}: {}", status, err_msg);
        }

        let resp_body: ChatResponse = resp.json().await?;

        // Extract text from the first choice
        let text = resp_body
            .choices
            .first()
            .and_then(|c| c.message.content.clone())
            .unwrap_or_default();

        log::info!(
            "[DeepSeek] Response received ({} chars)",
            text.len()
        );

        // Clean the response — extract JSON object if present (same pattern as Anthropic)
        return Ok(clean_json_response(&text));
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("DeepSeek request failed after {} retries", MAX_RETRIES)))
}

/// Clean the response — extract JSON object, matching Anthropic's clean_json_response pattern.
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
