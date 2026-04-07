use anyhow::Result;
use serde::Deserialize;

const EMBEDDINGS_URL: &str = "https://api.openai.com/v1/embeddings";
const EMBEDDING_MODEL: &str = "text-embedding-3-small";
const TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Deserialize)]
struct EmbeddingsResponse {
    data: Vec<EmbeddingData>,
}

#[derive(Debug, Deserialize)]
struct EmbeddingData {
    embedding: Vec<f32>,
}

/// Call OpenAI embeddings API to embed a query string.
/// Returns a 1536-dim f32 vector.
pub async fn embed_query(api_key: &str, query: &str) -> Result<Vec<f32>> {
    if api_key.is_empty() {
        anyhow::bail!("OpenAI API key is empty");
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(TIMEOUT_SECS))
        .build()?;

    let response = client
        .post(EMBEDDINGS_URL)
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&serde_json::json!({
            "model": EMBEDDING_MODEL,
            "input": query,
        }))
        .send()
        .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("OpenAI API error ({}): {}", status, body);
    }

    let parsed: EmbeddingsResponse = response.json().await?;
    if parsed.data.is_empty() {
        anyhow::bail!("OpenAI returned empty embedding data");
    }

    Ok(parsed.data.into_iter().next().unwrap().embedding)
}
