use anyhow::Result;
use serde::{Deserialize, Serialize};

const RESEND_API_URL: &str = "https://api.resend.com";

#[derive(Debug, Serialize)]
struct SendEmailRequest {
    from: String,
    to: Vec<String>,
    subject: String,
    html: String,
}

#[derive(Debug, Deserialize)]
struct SendEmailResponse {
    id: String,
}

pub async fn test_connection(api_key: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/domains", RESEND_API_URL))
        .header("Authorization", format!("Bearer {}", api_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(resp.status().is_success())
}

pub async fn send_email(
    api_key: &str,
    from: &str,
    to: &str,
    subject: &str,
    html_body: &str,
) -> Result<String> {
    let client = reqwest::Client::new();
    let req = SendEmailRequest {
        from: from.to_string(),
        to: vec![to.to_string()],
        subject: subject.to_string(),
        html: html_body.to_string(),
    };

    let resp = client
        .post(format!("{}/emails", RESEND_API_URL))
        .header("Authorization", format!("Bearer {}", api_key))
        .header("Content-Type", "application/json")
        .json(&req)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Resend error {}: {}", status, body);
    }

    let send_resp: SendEmailResponse = resp.json().await?;
    Ok(send_resp.id)
}
