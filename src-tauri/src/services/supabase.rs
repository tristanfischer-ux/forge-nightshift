use anyhow::Result;
use serde_json::{json, Value};

pub async fn test_connection(url: &str, service_key: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/rest/v1/marketplace_listings?select=id&limit=1", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(resp.status().is_success())
}

pub async fn check_domain_exists(url: &str, service_key: &str, domain: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    // Search in attributes JSONB for website_url containing the domain
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

/// Push a company into ForgeOS marketplace_listings.
/// Schema has NO foundry_id — it's a global catalogue.
/// contact_source must be one of: 'manual', 'ai_enriched', 'self_reported', 'csv_import'
/// We use 'ai_enriched' for Nightshift-discovered companies.
pub async fn push_listing(
    url: &str,
    service_key: &str,
    _foundry_id: &str,
    company: &Value,
) -> Result<String> {
    let client = reqwest::Client::new();

    // Parse specialties/certifications — may be stored as JSON strings in SQLite
    let specialties = parse_json_field(company, "specialties");
    let certifications = parse_json_field(company, "certifications");

    let attributes = json!({
        "website_url": company.get("website_url").and_then(|v| v.as_str()).unwrap_or(""),
        "country": company.get("country").and_then(|v| v.as_str()).unwrap_or(""),
        "city": company.get("city").and_then(|v| v.as_str()).unwrap_or(""),
        "specialties": specialties,
        "certifications": certifications,
        "employees": company.get("company_size").and_then(|v| v.as_str()).unwrap_or(""),
        "year_founded": company.get("year_founded").and_then(|v| v.as_i64()),
        "nightshift_score": company.get("relevance_score").and_then(|v| v.as_str()).and_then(|v| v.parse::<i64>().ok()).unwrap_or(0),
        "discovered_at": chrono::Utc::now().to_rfc3339(),
        "source": "nightshift",
    });

    let mut listing = json!({
        "title": company.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "description": company.get("description").and_then(|v| v.as_str()).unwrap_or(""),
        "category": company.get("category").and_then(|v| v.as_str()).unwrap_or("Services"),
        "subcategory": company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("Manufacturing"),
        "attributes": attributes,
        "contact_source": "ai_enriched",
        "outreach_status": "not_started",
        "is_verified": false,
        "approval_status": "pending",
        "data_quality_score": company.get("enrichment_quality").and_then(|v| v.as_str()).and_then(|v| v.parse::<i64>().ok()).unwrap_or(30),
    });

    // Only include contact fields if they have values
    let contact_name = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("");
    let contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("");
    let contact_title = company.get("contact_title").and_then(|v| v.as_str()).unwrap_or("");
    let contact_phone = company.get("contact_phone").and_then(|v| v.as_str()).unwrap_or("");

    if !contact_name.is_empty() {
        listing["contact_name"] = json!(contact_name);
    }
    if !contact_email.is_empty() {
        listing["contact_email"] = json!(contact_email);
    }
    if !contact_title.is_empty() {
        listing["contact_title"] = json!(contact_title);
    }
    if !contact_phone.is_empty() {
        listing["contact_phone"] = json!(contact_phone);
    }

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

/// Parse a field that might be a JSON string (from SQLite) or already an array
fn parse_json_field(company: &Value, field: &str) -> Value {
    if let Some(val) = company.get(field) {
        if val.is_array() {
            return val.clone();
        }
        if let Some(s) = val.as_str() {
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                return parsed;
            }
        }
    }
    json!([])
}
