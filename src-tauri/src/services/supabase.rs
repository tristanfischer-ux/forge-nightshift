use anyhow::Result;
use serde_json::{json, Value};

pub async fn test_connection(url: &str, service_key: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/rest/v1/", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(resp.status().is_success())
}

pub async fn check_domain_exists(url: &str, service_key: &str, domain: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/rest/v1/marketplace_listings", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .query(&[
            ("select", "id"),
            ("attributes->>website_url", &format!("ilike.*{}*", domain)),
            ("limit", "1"),
        ])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(false);
    }

    let results: Vec<Value> = resp.json().await?;
    Ok(!results.is_empty())
}

pub async fn push_listing(
    url: &str,
    service_key: &str,
    foundry_id: &str,
    company: &Value,
) -> Result<String> {
    let client = reqwest::Client::new();

    let attributes = json!({
        "website_url": company.get("website_url").and_then(|v| v.as_str()).unwrap_or(""),
        "country": company.get("country").and_then(|v| v.as_str()).unwrap_or(""),
        "city": company.get("city").and_then(|v| v.as_str()).unwrap_or(""),
        "specialties": company.get("specialties").unwrap_or(&json!([])),
        "certifications": company.get("certifications").unwrap_or(&json!([])),
        "employees": company.get("company_size").and_then(|v| v.as_str()).unwrap_or(""),
        "year_founded": company.get("year_founded").and_then(|v| v.as_i64()),
        "nightshift_score": company.get("relevance_score").and_then(|v| v.as_i64()).unwrap_or(0),
        "discovered_at": chrono::Utc::now().to_rfc3339(),
    });

    let listing = json!({
        "foundry_id": foundry_id,
        "title": company.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "description": company.get("description").and_then(|v| v.as_str()).unwrap_or(""),
        "category": company.get("category").and_then(|v| v.as_str()).unwrap_or("Services"),
        "subcategory": company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("Manufacturing"),
        "attributes": attributes,
        "contact_name": company.get("contact_name").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_email": company.get("contact_email").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_title": company.get("contact_title").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_phone": company.get("contact_phone").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_source": "nightshift",
        "outreach_status": "not_started",
        "is_verified": false,
        "status": "active",
    });

    let resp = client
        .post(format!("{}/rest/v1/marketplace_listings", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .header("Content-Type", "application/json")
        .header("Prefer", "return=representation")
        .json(&listing)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase insert error {}: {}", status, body);
    }

    let results: Vec<Value> = resp.json().await?;
    let id = results
        .first()
        .and_then(|r| r.get("id"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    Ok(id)
}
