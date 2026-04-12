use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::Manager;

use crate::db::Database;
use crate::services;

const BATCH_LIMIT: i64 = 50;

/// Subpages likely to contain team/leadership information.
const TEAM_PATHS: &[&str] = &[
    "/team", "/about-us", "/about", "/leadership", "/management",
    "/our-people", "/our-team", "/people", "/staff", "/who-we-are",
    "/meet-the-team", "/directors", "/board",
];

#[derive(Debug, Deserialize)]
struct ExtractedContact {
    name: String,
    title: Option<String>,
    department: Option<String>,
    seniority: Option<String>,
    is_decision_maker: Option<bool>,
}

/// Extract contacts from company websites using LLM analysis.
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let deepseek_api_key = config
        .get("deepseek_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if deepseek_api_key.is_empty() {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "error", "No DeepSeek API key configured");
        anyhow::bail!("No DeepSeek API key configured for contact extraction");
    }

    let profile_id = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_active_profile_id()
    };

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies_needing_contacts(&profile_id, BATCH_LIMIT)?
    };

    let total = companies.len();
    if total == 0 {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "info", "No companies need contact extraction");
        return Ok(json!({ "processed": 0, "contacts_found": 0 }));
    }

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "info",
            &format!("Starting contact extraction for {} companies", total));
    }

    let extracted_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));

    super::emit_node(app, json!({
        "node_id": "contacts",
        "status": "running",
        "model": "deepseek-chat",
        "progress": { "current": 0, "total": total },
        "concurrency": 1,
    }));

    for (i, company) in companies.iter().enumerate() {
        if super::is_cancelled() {
            break;
        }

        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
        let website_url = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("");

        if company_id.is_empty() || website_url.is_empty() {
            continue;
        }

        log::info!("[Contacts] ({}/{}) Extracting contacts for: {}", i + 1, total, company_name);

        super::emit_node(app, json!({
            "node_id": "contacts",
            "status": "running",
            "model": "deepseek-chat",
            "progress": {
                "current": i + 1,
                "total": total,
                "current_item": company_name,
            },
            "concurrency": 1,
        }));

        match extract_contacts_for_company(
            &deepseek_api_key,
            company_id,
            company_name,
            website_url,
        ).await {
            Ok(contacts) => {
                let count = contacts.len();
                if count > 0 {
                    let db: tauri::State<'_, Database> = app.state();
                    for (ci, contact) in contacts.iter().enumerate() {
                        let is_dm = contact.is_decision_maker.unwrap_or(false);
                        let role = if is_dm { "decision_maker" } else { "influencer" };
                        let _ = db.save_contact(
                            company_id,
                            &contact.name,
                            contact.title.as_deref(),
                            None, // email — not reliably extracted from team pages
                            None, // phone
                            None, // linkedin_url
                            Some(role),
                            contact.department.as_deref(),
                            contact.seniority.as_deref(),
                            Some("company_website"),
                            None, // notes
                            ci == 0 && is_dm, // first decision maker is primary
                        );
                    }
                    extracted_count.fetch_add(count as i64, Ordering::Relaxed);
                    log::info!("[Contacts] Found {} contacts for {}", count, company_name);
                } else {
                    log::info!("[Contacts] No contacts found for {}", company_name);
                }
            }
            Err(e) => {
                log::warn!("[Contacts] Error extracting contacts for {}: {}", company_name, e);
                error_count.fetch_add(1, Ordering::Relaxed);
            }
        }

        // Rate limit between companies
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    let extracted = extracted_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    super::emit_node(app, json!({
        "node_id": "contacts",
        "status": "completed",
        "progress": { "current": total, "total": total },
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "info",
            &format!("Contact extraction complete: {} processed, {} contacts found, {} errors",
                total, extracted, errors));
    }

    Ok(json!({
        "processed": total,
        "contacts_found": extracted,
        "errors": errors,
    }))
}

/// Extract contacts for a single company by scraping team/about pages and sending to LLM.
async fn extract_contacts_for_company(
    deepseek_api_key: &str,
    _company_id: &str,
    company_name: &str,
    website_url: &str,
) -> Result<Vec<ExtractedContact>> {
    // Try to fetch team-related pages
    let base_url = normalize_base_url(website_url);
    let mut page_text = String::new();

    // First fetch the main about/team pages
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .build()?;

    let mut pages_fetched = 0;
    let mut circuit_breaker_failures = 0;

    for path in TEAM_PATHS {
        if circuit_breaker_failures >= 3 {
            log::info!("[Contacts] Circuit breaker: 3 consecutive failures for {}, stopping", company_name);
            break;
        }

        let url = format!("{}{}", base_url, path);
        match fetch_page_text(&client, &url).await {
            Ok(text) => {
                if !text.is_empty() && text.len() > 100 {
                    page_text.push_str(&format!("\n--- PAGE: {} ---\n{}\n", path, text));
                    pages_fetched += 1;
                    circuit_breaker_failures = 0;
                    if pages_fetched >= 3 {
                        break; // Enough pages
                    }
                }
            }
            Err(_) => {
                circuit_breaker_failures += 1;
            }
        }
    }

    // If no team pages found, try the root page
    if page_text.is_empty() {
        match services::scraper::fetch_website_text(website_url).await {
            Ok(text) => {
                page_text = text;
            }
            Err(e) => {
                anyhow::bail!("Failed to fetch website for {}: {}", company_name, e);
            }
        }
    }

    if page_text.is_empty() || page_text.len() < 50 {
        return Ok(vec![]);
    }

    // Truncate to avoid token limits
    let truncated = if page_text.len() > 8000 {
        &page_text[..8000]
    } else {
        &page_text
    };

    // Send to LLM for extraction
    let system_prompt = "You extract decision maker contacts from company web pages. \
        Return valid JSON only. No markdown, no explanation.";

    let prompt = format!(
        "This is a company called \"{}\" that might buy modular vertical farming systems \
        (container farms for growing salads, herbs, leafy greens). \
        Extract the most relevant decision makers from the following web page text. \
        Return a JSON array: [{{\"name\": \"...\", \"title\": \"...\", \"department\": \"...\", \
        \"seniority\": \"...\", \"is_decision_maker\": true/false}}]. \
        Focus on people in these roles: Head of Procurement/Fresh Produce, \
        Sustainability Director, Operations Director, Commercial Director, Innovation Lead, \
        CEO, COO, Managing Director. \
        For department, use one of: procurement, sustainability, operations, fresh_produce, \
        innovation, executive, other. \
        For seniority, use one of: c_suite, director, head_of, manager, other. \
        If no relevant people are found, return an empty array []. \
        Maximum 10 contacts.\n\n--- WEB PAGE TEXT ---\n{}",
        company_name, truncated,
    );

    let response = services::deepseek::chat(
        deepseek_api_key,
        Some(system_prompt),
        &prompt,
        true, // json mode
    ).await?;

    // Parse the response
    let contacts = parse_contacts_response(&response);
    Ok(contacts)
}

/// Parse LLM response into extracted contacts.
fn parse_contacts_response(response: &str) -> Vec<ExtractedContact> {
    let trimmed = response.trim();

    // Try parsing as array directly
    if let Ok(contacts) = serde_json::from_str::<Vec<ExtractedContact>>(trimmed) {
        return contacts;
    }

    // Try extracting array from within a JSON object (e.g. {"contacts": [...]})
    if let Ok(obj) = serde_json::from_str::<Value>(trimmed) {
        // Look for any array field
        if let Some(obj_map) = obj.as_object() {
            for (_key, val) in obj_map {
                if let Some(arr) = val.as_array() {
                    if let Ok(contacts) = serde_json::from_value::<Vec<ExtractedContact>>(Value::Array(arr.clone())) {
                        return contacts;
                    }
                }
            }
        }
    }

    // Try to find JSON array in the response text
    if let Some(start) = trimmed.find('[') {
        if let Some(end) = trimmed.rfind(']') {
            if start < end {
                let json_slice = &trimmed[start..=end];
                if let Ok(contacts) = serde_json::from_str::<Vec<ExtractedContact>>(json_slice) {
                    return contacts;
                }
            }
        }
    }

    log::warn!("[Contacts] Failed to parse LLM response: {}", &trimmed[..trimmed.len().min(200)]);
    vec![]
}

/// Normalize a URL to a base URL (scheme + host).
fn normalize_base_url(url: &str) -> String {
    if let Ok(parsed) = reqwest::Url::parse(url) {
        format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""))
    } else {
        // Fallback: strip trailing path
        let url = url.trim_end_matches('/');
        if let Some(idx) = url.find("://") {
            let after_scheme = &url[idx + 3..];
            if let Some(slash_idx) = after_scheme.find('/') {
                url[..idx + 3 + slash_idx].to_string()
            } else {
                url.to_string()
            }
        } else {
            format!("https://{}", url)
        }
    }
}

/// Fetch a page and extract text content.
async fn fetch_page_text(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }

    let html = resp.text().await?;
    Ok(html_to_text(&html))
}

/// Simple HTML to text conversion — strip tags, decode entities, collapse whitespace.
fn html_to_text(html: &str) -> String {
    // Remove script and style blocks
    let re_script = regex::Regex::new(r"(?is)<script[^>]*>.*?</script>").unwrap();
    let re_style = regex::Regex::new(r"(?is)<style[^>]*>.*?</style>").unwrap();
    let text = re_script.replace_all(html, " ");
    let text = re_style.replace_all(&text, " ");

    // Remove all HTML tags
    let re_tags = regex::Regex::new(r"<[^>]+>").unwrap();
    let text = re_tags.replace_all(&text, " ");

    // Decode common HTML entities
    let text = text
        .replace("&amp;", "&")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&#39;", "'")
        .replace("&nbsp;", " ");

    // Collapse whitespace
    let re_ws = regex::Regex::new(r"\s+").unwrap();
    re_ws.replace_all(&text, " ").trim().to_string()
}
