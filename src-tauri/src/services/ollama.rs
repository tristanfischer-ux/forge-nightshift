use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const DEFAULT_URL: &str = "http://localhost:11434";

#[derive(Debug, Serialize, Deserialize)]
struct GenerateResponse {
    model: String,
    response: String,
    done: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct TagsResponse {
    models: Vec<ModelInfo>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ModelInfo {
    name: String,
    size: u64,
}

pub async fn test_connection() -> Result<Value> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/api/tags", DEFAULT_URL))
        .timeout(std::time::Duration::from_secs(5))
        .send()
        .await?;

    let tags: TagsResponse = resp.json().await?;
    let model_names: Vec<String> = tags.models.iter().map(|m| m.name.clone()).collect();

    Ok(json!({
        "connected": true,
        "models": model_names,
    }))
}

pub async fn generate(
    base_url: &str,
    model: &str,
    prompt: &str,
    json_mode: bool,
) -> Result<String> {
    let client = reqwest::Client::new();
    let url = if base_url.is_empty() {
        DEFAULT_URL.to_string()
    } else {
        base_url.to_string()
    };

    let mut req = json!({
        "model": model,
        "prompt": prompt,
        "stream": false,
    });

    if json_mode {
        req["format"] = json!("json");
    }

    let resp = client
        .post(format!("{}/api/generate", url))
        .json(&req)
        .timeout(std::time::Duration::from_secs(300))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Ollama error {}: {}", status, body);
    }

    let gen_resp: GenerateResponse = resp.json().await?;

    if json_mode {
        Ok(clean_json_response(&gen_resp.response))
    } else {
        Ok(gen_resp.response)
    }
}

/// Strip `<think>...</think>` blocks and extract the JSON object from LLM
/// responses. qwen3 models emit reasoning tags even with `format: "json"`.
fn clean_json_response(raw: &str) -> String {
    let mut s = raw.to_string();

    // Strip all <think>...</think> blocks (may span multiple lines)
    while let Some(start) = s.find("<think>") {
        if let Some(end) = s.find("</think>") {
            let block_end = end + "</think>".len();
            s = format!("{}{}", &s[..start], &s[block_end..]);
        } else {
            // Unclosed <think> — strip from <think> to end
            s = s[..start].to_string();
            break;
        }
    }

    let s = s.trim();

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

    // Fallback: return trimmed string (will fail JSON parse downstream, which is fine)
    s.to_string()
}

