mod db;
mod services;
mod pipeline;

use db::Database;
use tauri::{Emitter, Manager};
use std::sync::Mutex;

/// Cached embeddings loaded from supplier_embeddings table.
/// Populated lazily on first semantic search to avoid re-reading 8,699 rows each time.
struct EmbeddingCache {
    embeddings: Vec<(String, Vec<f32>)>,
    loaded: bool,
}

impl EmbeddingCache {
    fn new() -> Self {
        Self {
            embeddings: Vec::new(),
            loaded: false,
        }
    }
}

/// Compute cosine similarity between two vectors.
fn cosine_similarity(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() || a.is_empty() {
        return 0.0;
    }
    let mut dot = 0.0f32;
    let mut norm_a = 0.0f32;
    let mut norm_b = 0.0f32;
    for i in 0..a.len() {
        dot += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }
    let denom = norm_a.sqrt() * norm_b.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

#[tauri::command]
fn get_stats(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_stats().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_extended_stats(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_extended_stats().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_companies(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    let limit = limit.unwrap_or(50).max(0).min(1000);
    let offset = offset.unwrap_or(0).max(0);
    db.get_companies(status.as_deref(), limit, offset)
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn get_company(db: tauri::State<'_, Database>, id: String) -> Result<serde_json::Value, String> {
    db.get_company(&id).map_err(|e| e.to_string())
}

const VALID_STATUSES: &[&str] = &[
    "discovered", "enriching", "enriched", "approved", "pushed", "rejected", "error",
];

#[tauri::command]
fn update_company_status(
    db: tauri::State<'_, Database>,
    id: String,
    status: String,
) -> Result<(), String> {
    if !VALID_STATUSES.contains(&status.as_str()) {
        return Err(format!("Invalid status: {}", status));
    }
    db.update_company_status(&id, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_emails(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    let limit = limit.unwrap_or(50).max(0).min(1000);
    db.get_emails(status.as_deref(), limit)
        .map_err(|e| e.to_string())
}

const VALID_EMAIL_STATUSES: &[&str] = &[
    "draft", "approved", "sending", "sent", "opened", "replied", "bounced", "failed",
];

#[tauri::command]
fn update_email_status(
    db: tauri::State<'_, Database>,
    id: String,
    status: String,
) -> Result<(), String> {
    if !VALID_EMAIL_STATUSES.contains(&status.as_str()) {
        return Err(format!("Invalid email status: {}", status));
    }
    db.update_email_status(&id, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_config(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_all_config().map_err(|e| e.to_string())
}

const VALID_CONFIG_KEYS: &[&str] = &[
    "relevance_threshold", "auto_approve_quality_threshold",
    "enrich_concurrency", "deep_enrich_concurrency",
    "categories_per_run", "daily_email_limit", "schedule_time",
    "target_countries", "research_model", "enrich_model", "outreach_model",
    "ollama_url", "from_email", "brave_api_key", "resend_api_key",
    "supabase_url", "supabase_service_key", "foundry_id",
    "companies_house_api_key", "anthropic_api_key", "deepseek_api_key", "openai_api_key", "llm_backend", "sound_enabled",
    "auto_outreach_enabled", "auto_outreach_template_id", "outreach_batch_size",
    "send_window_start", "send_window_end",
    "schedules",
    "active_profile_id",
];

#[tauri::command]
fn set_config(db: tauri::State<'_, Database>, key: String, value: String) -> Result<(), String> {
    if !VALID_CONFIG_KEYS.contains(&key.as_str()) {
        return Err(format!("Unknown config key: {}", key));
    }
    // Config validation
    match key.as_str() {
        "relevance_threshold" | "auto_approve_quality_threshold" => {
            let v: i64 = value.parse().map_err(|_| "Must be a number".to_string())?;
            if !(0..=100).contains(&v) {
                return Err("Must be between 0 and 100".to_string());
            }
        }
        "enrich_concurrency" => {
            let v: i64 = value.parse().map_err(|_| "Must be a number".to_string())?;
            if !(1..=10).contains(&v) {
                return Err("Must be between 1 and 10".to_string());
            }
        }
        "deep_enrich_concurrency" => {
            let v: i64 = value.parse().map_err(|_| "Must be a number".to_string())?;
            if !(1..=5).contains(&v) {
                return Err("Must be between 1 and 5".to_string());
            }
        }
        "categories_per_run" => {
            let v: i64 = value.parse().map_err(|_| "Must be a number".to_string())?;
            if !(1..=37).contains(&v) {
                return Err("Must be between 1 and 37".to_string());
            }
        }
        "daily_email_limit" => {
            let v: i64 = value.parse().map_err(|_| "Must be a number".to_string())?;
            if !(1..=500).contains(&v) {
                return Err("Must be between 1 and 500".to_string());
            }
        }
        "schedule_time" => {
            if !value.is_empty() {
                let parts: Vec<&str> = value.split(':').collect();
                if parts.len() != 2 {
                    return Err("Must be in HH:MM format".to_string());
                }
                let h: u32 = parts[0].parse().map_err(|_| "Invalid hour".to_string())?;
                let m: u32 = parts[1].parse().map_err(|_| "Invalid minute".to_string())?;
                if h > 23 || m > 59 {
                    return Err("Must be valid HH:MM (00:00-23:59)".to_string());
                }
            }
        }
        "outreach_batch_size" => {
            let v: i64 = value.parse().map_err(|_| "Must be a number".to_string())?;
            if !(1..=20).contains(&v) {
                return Err("Must be between 1 and 20".to_string());
            }
        }
        "auto_outreach_enabled" => {
            if value != "true" && value != "false" {
                return Err("Must be 'true' or 'false'".to_string());
            }
        }
        "llm_backend" => {
            if value != "haiku" && value != "ollama" && value != "deepseek" {
                return Err("Must be 'haiku', 'ollama', or 'deepseek'".to_string());
            }
        }
        _ => {}
    }

    db.set_config(&key, &value).map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_ollama_connection() -> Result<serde_json::Value, String> {
    services::ollama::test_connection()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_brave_connection(api_key: String) -> Result<bool, String> {
    services::brave::test_connection(&api_key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_supabase_connection(url: String, key: String) -> Result<bool, String> {
    services::supabase::test_connection(&url, &key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_resend_connection(api_key: String) -> Result<bool, String> {
    services::resend::test_connection(&api_key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_anthropic_connection(api_key: String) -> Result<serde_json::Value, String> {
    services::anthropic::test_connection(&api_key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_deepseek_connection(api_key: String) -> Result<serde_json::Value, String> {
    services::deepseek::test_connection(&api_key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn start_pipeline(app: tauri::AppHandle, stages: Vec<String>) -> Result<String, String> {
    pipeline::start_pipeline(app, stages)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn stop_pipeline(app: tauri::AppHandle) -> Result<(), String> {
    pipeline::stop_pipeline(app).await.map_err(|e| e.to_string())
}

#[tauri::command]
fn get_pipeline_status() -> Result<serde_json::Value, String> {
    pipeline::get_status().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_pipeline_nodes() -> Result<serde_json::Value, String> {
    pipeline::get_pipeline_nodes().map_err(|e| e.to_string())
}

#[tauri::command]
fn reset_error_companies(db: tauri::State<'_, Database>) -> Result<i64, String> {
    db.reset_error_companies().map_err(|e| e.to_string())
}

#[tauri::command]
fn approve_all_enriched(db: tauri::State<'_, Database>) -> Result<i64, String> {
    db.approve_all_enriched().map_err(|e| e.to_string())
}

#[tauri::command]
fn reenrich_all(db: tauri::State<'_, Database>) -> Result<i64, String> {
    db.reset_for_reenrichment().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_analytics(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_analytics().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_companies_filtered(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    subcategory: Option<String>,
    country: Option<String>,
    search: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    let limit = limit.unwrap_or(50).max(0).min(1000);
    let offset = offset.unwrap_or(0).max(0);
    db.get_companies_filtered(
        status.as_deref(),
        subcategory.as_deref(),
        country.as_deref(),
        search.as_deref(),
        limit,
        offset,
    )
    .map_err(|e| e.to_string())
}

#[tauri::command]
fn get_run_log(
    db: tauri::State<'_, Database>,
    job_id: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    let limit = limit.unwrap_or(100).max(0).min(1000);
    db.get_run_log(job_id.as_deref(), limit)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn refresh_email_statuses(
    db: tauri::State<'_, Database>,
) -> Result<i64, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let api_key = config
        .get("resend_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if api_key.is_empty() {
        return Err("Resend API key not configured".to_string());
    }

    let emails = db
        .get_sent_emails_for_tracking()
        .map_err(|e| e.to_string())?;

    let mut updated = 0i64;
    for email in &emails {
        let id = email.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let resend_id = email.get("resend_id").and_then(|v| v.as_str()).unwrap_or("");

        if resend_id.is_empty() {
            continue;
        }

        match services::resend::get_email_status(api_key, resend_id).await {
            Ok(status_data) => {
                let last_event = status_data
                    .get("last_event")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let bounced = last_event == "bounced";
                let opened_at = if last_event == "opened" {
                    status_data
                        .get("last_event_at")
                        .and_then(|v| v.as_str())
                        .or_else(|| {
                            // Fallback: check events array
                            None
                        })
                } else {
                    None
                };

                if bounced || opened_at.is_some() {
                    let _ = db.update_email_tracking(id, opened_at, bounced);
                    updated += 1;
                }
            }
            Err(_) => {
                // Skip failed lookups silently — rate limit may kick in
            }
        }

        // Rate limit between Resend API calls (600ms = ~1.6/sec, safe for Resend free tier 2/sec)
        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
    }

    // Update A/B experiment stats after tracking refresh
    let _ = db.update_experiment_stats();

    Ok(updated)
}

#[tauri::command]
fn backup_database(
    db: tauri::State<'_, Database>,
    app: tauri::AppHandle,
) -> Result<String, String> {
    let app_dir = app
        .path()
        .app_data_dir()
        .map_err(|e| e.to_string())?;

    let backup_dir = app_dir.join("backups");
    std::fs::create_dir_all(&backup_dir).map_err(|e| e.to_string())?;

    let timestamp = chrono::Local::now().format("%Y-%m-%d_%H%M%S");
    let backup_path = backup_dir.join(format!("nightshift_backup_{}.db", timestamp));

    db.backup(&backup_path).map_err(|e| e.to_string())?;

    Ok(backup_path.to_string_lossy().to_string())
}

#[tauri::command]
async fn import_for_audit(
    db: tauri::State<'_, Database>,
    threshold: Option<i32>,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        return Err("Supabase credentials not configured".to_string());
    }

    let quality_threshold = threshold.unwrap_or(50).max(1).min(100);

    // Use target_countries config to filter audit imports to relevant regions
    // Settings stores as JSON array (e.g. '["DE","FR"]'), fall back to comma-separated
    let target_countries: Vec<String> = {
        let raw = config
            .get("target_countries")
            .and_then(|v| v.as_str())
            .unwrap_or("DE");
        // Try JSON array first
        serde_json::from_str::<Vec<String>>(raw).unwrap_or_else(|_| {
            raw.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
    };

    let listings = services::supabase::fetch_low_quality_listings(
        supabase_url,
        supabase_key,
        quality_threshold,
        &target_countries,
    )
    .await
    .map_err(|e| e.to_string())?;

    let total_fetched = listings.len();
    let mut imported = 0i64;
    let mut skipped = 0i64;

    for listing in &listings {
        let listing_id = listing
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if listing_id.is_empty() {
            skipped += 1;
            continue;
        }

        // Extract website_url from promoted column or attributes fallback
        let website_url = listing
            .get("website_url")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                listing
                    .get("attributes")
                    .and_then(|a| a.get("website_url"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("");

        // Extract domain from website_url
        let domain = if !website_url.is_empty() {
            let d = website_url
                .replace("https://", "")
                .replace("http://", "")
                .replace("www.", "");
            d.split('/').next().unwrap_or("").to_lowercase()
        } else {
            String::new()
        };

        // Skip listings without a website — can't enrich without one
        if website_url.is_empty() {
            skipped += 1;
            continue;
        }

        // Dedup: skip if domain already in local SQLite
        if !domain.is_empty() {
            if db.domain_exists(&domain).unwrap_or(false) {
                skipped += 1;
                continue;
            }
        }

        let title = listing
            .get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        // Name-based dedup
        if !title.is_empty() && db.name_exists_normalized(title).unwrap_or(false) {
            skipped += 1;
            continue;
        }

        let country = listing
            .get("country")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                listing
                    .get("attributes")
                    .and_then(|a| a.get("country"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("");

        let city = listing
            .get("city")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                listing
                    .get("attributes")
                    .and_then(|a| a.get("city"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("");

        let company = serde_json::json!({
            "name": title,
            "website_url": website_url,
            "domain": domain,
            "country": country,
            "city": city,
            "description": listing.get("description").and_then(|v| v.as_str()).unwrap_or(""),
            "contact_name": listing.get("contact_name").and_then(|v| v.as_str()).unwrap_or(""),
            "contact_email": listing.get("contact_email").and_then(|v| v.as_str()).unwrap_or(""),
            "contact_title": listing.get("contact_title").and_then(|v| v.as_str()).unwrap_or(""),
            "contact_phone": listing.get("contact_phone").and_then(|v| v.as_str()).unwrap_or(""),
        });

        match db.insert_company_for_audit(&company, listing_id) {
            Ok(_) => imported += 1,
            Err(_) => skipped += 1,
        }
    }

    Ok(serde_json::json!({
        "fetched": total_fetched,
        "imported": imported,
        "skipped": skipped,
    }))
}

#[tauri::command]
async fn push_single_company(
    db: tauri::State<'_, Database>,
    id: String,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let supabase_url = config.get("supabase_url").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_key = config.get("supabase_service_key").and_then(|v| v.as_str()).unwrap_or("");
    let foundry_id = config.get("foundry_id").and_then(|v| v.as_str()).unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        return Err("Supabase credentials not configured".to_string());
    }
    if foundry_id.is_empty() {
        return Err("Foundry ID not configured".to_string());
    }

    let company = db.get_company(&id).map_err(|e| e.to_string())?;
    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let status = company.get("status").and_then(|v| v.as_str()).unwrap_or("");

    if status != "approved" {
        return Err(format!("Company '{}' is not approved (status: {})", name, status));
    }

    // Check domain dedup against Supabase
    let domain = company.get("domain").and_then(|v| v.as_str()).unwrap_or("");
    if !domain.is_empty() {
        match services::supabase::check_domain_exists(supabase_url, supabase_key, domain).await {
            Ok(true) => return Err(format!("'{}' already exists in ForgeOS (domain: {})", name, domain)),
            Ok(false) => {}
            Err(e) => log::warn!("Domain check failed for {}: {}", domain, e),
        }
    }

    services::supabase::push_listing(supabase_url, supabase_key, foundry_id, &company)
        .await
        .map_err(|e| format!("Failed to push '{}': {}", name, e))?;

    db.update_company_status(&id, "pushed").map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "pushed": true, "name": name }))
}

#[tauri::command]
async fn send_approved_emails(
    db: tauri::State<'_, Database>,
    app: tauri::AppHandle,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let api_key = config
        .get("resend_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if api_key.is_empty() {
        return Err("Resend API key not configured".to_string());
    }

    let emails = db.get_approved_emails().map_err(|e| e.to_string())?;
    let total = emails.len() as i64;

    if total == 0 {
        return Ok(serde_json::json!({ "sent": 0, "failed": 0, "total": 0 }));
    }

    let mut sent = 0i64;
    let mut failed = 0i64;

    for (i, email) in emails.iter().enumerate() {
        let id = email.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let to = email.get("to_email").and_then(|v| v.as_str()).unwrap_or("");
        let from = email.get("from_email").and_then(|v| v.as_str()).unwrap_or("");
        let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
        let body = email.get("body").and_then(|v| v.as_str()).unwrap_or("");

        if to.is_empty() || from.is_empty() {
            failed += 1;
            continue;
        }

        // Set status to sending for visual feedback
        let _ = db.update_email_status(id, "sending");
        let _ = app.emit("send_approved:progress", serde_json::json!({
            "current": i + 1,
            "total": total,
            "to": to,
        }));

        match services::resend::send_email(api_key, from, to, subject, body).await {
            Ok(resend_id) => {
                let _ = db.update_email_sent(id, &resend_id);
                sent += 1;
            }
            Err(e) => {
                let err_msg = e.to_string();
                log::error!("Failed to send email {}: {}", id, err_msg);
                let _ = db.update_email_status(id, "failed");
                let _ = db.set_email_error(id, &err_msg);
                failed += 1;
            }
        }

        // Rate limit between Resend API calls (600ms = ~1.6/sec, safe for Resend free tier 2/sec)
        if i + 1 < emails.len() {
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        }
    }

    Ok(serde_json::json!({
        "sent": sent,
        "failed": failed,
        "total": total,
    }))
}

/// Reset all failed emails back to "approved" so they can be re-sent.
#[tauri::command]
async fn retry_failed_emails(
    db: tauri::State<'_, Database>,
) -> Result<usize, String> {
    db.retry_failed_emails().map_err(|e| e.to_string())
}

/// Remove a company from the ForgeOS marketplace and delete it locally.
/// Requires the company to have a supabase_listing_id (i.e. it was imported via audit or pushed).
#[tauri::command]
async fn remove_from_marketplace(
    db: tauri::State<'_, Database>,
    id: String,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let supabase_url = config.get("supabase_url").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_key = config.get("supabase_service_key").and_then(|v| v.as_str()).unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        return Err("Supabase credentials not configured".to_string());
    }

    let company = db.get_company(&id).map_err(|e| e.to_string())?;
    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let listing_id = company
        .get("supabase_listing_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    match listing_id {
        Some(lid) => {
            services::supabase::delete_listing(supabase_url, supabase_key, lid)
                .await
                .map_err(|e| format!("Failed to delete '{}' from marketplace: {}", name, e))?;
        }
        None => {
            return Err(format!("'{}' has no Supabase listing ID — not in marketplace", name));
        }
    }

    // Remove from local DB too
    db.delete_company(&id).map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "removed": true, "name": name }))
}

#[tauri::command]
fn get_companies_for_map(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_companies_for_map().map_err(|e| e.to_string())
}

#[tauri::command]
async fn geocode_companies(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    let companies = db.get_companies_needing_geocoding().map_err(|e| e.to_string())?;
    let total = companies.len();
    let mut geocoded = 0i64;
    let mut failed = 0i64;

    // Separate UK companies (use fast postcodes.io bulk) from non-UK (use Nominatim)
    let mut uk_postcode_map: Vec<(String, String)> = Vec::new(); // (company_id, postcode)
    let mut uk_city_fallbacks: Vec<(String, String)> = Vec::new(); // (company_id, city)
    let mut non_uk_companies: Vec<(String, String, String, String)> = Vec::new(); // (id, address, city, country)

    for company in &companies {
        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let address = company.get("address").and_then(|v| v.as_str()).unwrap_or("");
        let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("");
        let country = company.get("country").and_then(|v| v.as_str()).unwrap_or("");

        if country == "GB" {
            if let Some(postcode) = services::postcodes::extract_uk_postcode(address) {
                uk_postcode_map.push((id.to_string(), postcode));
            } else if !city.is_empty() {
                uk_city_fallbacks.push((id.to_string(), city.to_string()));
            }
        } else {
            non_uk_companies.push((
                id.to_string(),
                address.to_string(),
                city.to_string(),
                country.to_string(),
            ));
        }
    }

    log::info!(
        "Geocoding: total={}, uk_postcodes={}, uk_cities={}, non_uk={}",
        total, uk_postcode_map.len(), uk_city_fallbacks.len(), non_uk_companies.len()
    );

    // UK: Bulk geocode postcodes in batches of 100 (fast, postcodes.io)
    for chunk in uk_postcode_map.chunks(100) {
        let postcodes: Vec<String> = chunk.iter().map(|(_, pc)| pc.clone()).collect();
        match services::postcodes::geocode_bulk(&postcodes).await {
            Ok(results) => {
                let lookup: std::collections::HashMap<String, (f64, f64)> = results
                    .into_iter()
                    .map(|(pc, lat, lng)| (pc, (lat, lng)))
                    .collect();

                for (id, postcode) in chunk {
                    if let Some((lat, lng)) = lookup.get(postcode) {
                        let _ = db.update_company_geocode(id, *lat, *lng);
                        geocoded += 1;
                    } else {
                        failed += 1;
                    }
                }
            }
            Err(_) => {
                failed += chunk.len() as i64;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    // UK city fallbacks via postcodes.io
    for (id, city) in &uk_city_fallbacks {
        match services::postcodes::geocode_place(city).await {
            Ok((lat, lng)) => {
                let _ = db.update_company_geocode(id, lat, lng);
                geocoded += 1;
            }
            Err(_) => {
                failed += 1;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    // Non-UK: Nominatim (1.1s rate limit per call built into the service)
    for (id, address, city, country) in &non_uk_companies {
        let mut success = false;

        // Try full address first
        if !address.is_empty() {
            if let Ok((lat, lng)) = services::nominatim::geocode_address(address).await {
                let _ = db.update_company_geocode(id, lat, lng);
                geocoded += 1;
                success = true;
            }
        }

        // Fallback: city + country
        if !success && !city.is_empty() && !country.is_empty() {
            if let Ok((lat, lng)) = services::nominatim::geocode_city_country(city, country).await {
                let _ = db.update_company_geocode(id, lat, lng);
                geocoded += 1;
                success = true;
            }
        }

        if !success {
            failed += 1;
        }
    }

    Ok(serde_json::json!({
        "total": total,
        "geocoded": geocoded,
        "failed": failed,
    }))
}

/// Bulk remove all audit-imported companies from the marketplace.
#[tauri::command]
async fn remove_all_from_marketplace(
    db: tauri::State<'_, Database>,
    company_ids: Vec<String>,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let supabase_url = config.get("supabase_url").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_key = config.get("supabase_service_key").and_then(|v| v.as_str()).unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        return Err("Supabase credentials not configured".to_string());
    }

    let mut removed = 0i64;
    let mut errors = 0i64;

    for id in &company_ids {
        let company = match db.get_company(id) {
            Ok(c) => c,
            Err(_) => { errors += 1; continue; }
        };

        let listing_id = company
            .get("supabase_listing_id")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty());

        if let Some(lid) = listing_id {
            match services::supabase::delete_listing(supabase_url, supabase_key, lid).await {
                Ok(()) => {
                    let _ = db.delete_company(id);
                    removed += 1;
                }
                Err(_) => { errors += 1; }
            }
        } else {
            // No listing ID — just delete locally
            let _ = db.delete_company(id);
            removed += 1;
        }
    }

    Ok(serde_json::json!({
        "removed": removed,
        "errors": errors,
    }))
}

#[tauri::command]
fn delete_emails(
    db: tauri::State<'_, Database>,
    ids: Vec<String>,
) -> Result<i64, String> {
    if ids.len() > 500 {
        return Err("Too many IDs (max 500)".to_string());
    }
    db.delete_emails(&ids).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_companies_count(
    db: tauri::State<'_, Database>,
    status: Option<String>,
) -> Result<i64, String> {
    db.get_companies_count(status.as_deref()).map_err(|e| e.to_string())
}

#[tauri::command]
fn batch_update_status(
    db: tauri::State<'_, Database>,
    ids: Vec<String>,
    status: String,
) -> Result<i64, String> {
    if !VALID_STATUSES.contains(&status.as_str()) {
        return Err(format!("Invalid status: {}", status));
    }
    if ids.len() > 500 {
        return Err("Too many IDs (max 500)".to_string());
    }
    db.batch_update_status(&ids, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_stats_history(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_stats_history().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_run_history(
    db: tauri::State<'_, Database>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    let limit = limit.unwrap_or(20).max(0).min(100);
    db.get_run_history(limit).map_err(|e| e.to_string())
}

// --- Email Templates ---

#[tauri::command]
fn get_email_templates(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_email_templates().map_err(|e| e.to_string())
}

#[tauri::command]
fn save_email_template(
    db: tauri::State<'_, Database>,
    id: Option<String>,
    name: String,
    subject: String,
    body: String,
) -> Result<serde_json::Value, String> {
    if name.trim().is_empty() || subject.trim().is_empty() || body.trim().is_empty() {
        return Err("Name, subject, and body are required".to_string());
    }
    match id {
        Some(existing_id) if !existing_id.is_empty() => {
            db.update_email_template(&existing_id, &name, &subject, &body)
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({ "id": existing_id, "updated": true }))
        }
        _ => {
            let new_id = db.insert_email_template(&name, &subject, &body)
                .map_err(|e| e.to_string())?;
            Ok(serde_json::json!({ "id": new_id, "created": true }))
        }
    }
}

#[tauri::command]
fn delete_email_template(db: tauri::State<'_, Database>, id: String) -> Result<(), String> {
    db.delete_email_template(&id).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_campaign_eligible_count(db: tauri::State<'_, Database>) -> Result<i64, String> {
    db.get_campaign_eligible_count().map_err(|e| e.to_string())
}

// --- Campaigns ---

#[tauri::command]
fn get_outreach_companies(
    db: tauri::State<'_, Database>,
    outreach_status: Option<String>,
    country: Option<String>,
    category: Option<String>,
    search: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<serde_json::Value, String> {
    let limit = limit.unwrap_or(50).max(1).min(200);
    let offset = offset.unwrap_or(0).max(0);
    let (rows, total) = db
        .get_outreach_companies(
            outreach_status.as_deref(),
            country.as_deref(),
            category.as_deref(),
            search.as_deref(),
            limit,
            offset,
        )
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "companies": rows, "total": total }))
}

#[tauri::command]
fn get_outreach_stats(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_outreach_stats().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_company_email_history(
    db: tauri::State<'_, Database>,
    company_id: String,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_company_email_history(&company_id)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn generate_drafts_for_companies(
    db: tauri::State<'_, Database>,
    app: tauri::AppHandle,
    company_ids: Vec<String>,
    template_id: String,
    ab_template_id: Option<String>,
) -> Result<serde_json::Value, String> {
    if company_ids.is_empty() {
        return Err("No companies selected".to_string());
    }
    if company_ids.len() > 100 {
        return Err("Maximum 100 companies per batch".to_string());
    }

    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let from_email = config.get("from_email").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_url = config.get("supabase_url").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_key = config.get("supabase_service_key").and_then(|v| v.as_str()).unwrap_or("");

    if from_email.is_empty() {
        return Err("from_email not configured".to_string());
    }
    if supabase_url.is_empty() || supabase_key.is_empty() {
        return Err("Supabase credentials not configured".to_string());
    }

    // Load learning context
    let insights = db.get_active_insights(10).unwrap_or_default();
    let insight_texts: Vec<String> = insights
        .iter()
        .filter_map(|i| i.get("insight").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();
    let active_experiment = db.get_active_experiment().unwrap_or(None);
    let experiment_id = active_experiment
        .as_ref()
        .and_then(|e| e.get("id").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let strategy_a = active_experiment
        .as_ref()
        .and_then(|e| e.get("variant_a_strategy").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let strategy_b = active_experiment
        .as_ref()
        .and_then(|e| e.get("variant_b_strategy").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let generation = active_experiment
        .as_ref()
        .and_then(|e| e.get("generation").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    let insights_json = if insight_texts.is_empty() {
        None
    } else {
        serde_json::to_string(&insight_texts).ok()
    };

    let has_ab = ab_template_id.is_some();
    let total = company_ids.len();
    let mut drafts_created = 0i64;
    let mut errors = 0i64;

    for (i, company_id) in company_ids.iter().enumerate() {
        // Determine which variant — use experiment strategy if available, else template A/B
        let variant = if !strategy_a.is_empty() && !strategy_b.is_empty() {
            if i % 2 == 0 { Some("A") } else { Some("B") }
        } else if has_ab {
            if i % 2 == 0 { Some("A") } else { Some("B") }
        } else {
            None
        };
        let current_template_id = if has_ab && i % 2 == 1 {
            ab_template_id.as_ref().unwrap().as_str()
        } else {
            template_id.as_str()
        };
        let strategy = match variant {
            Some("B") => &strategy_b,
            _ => &strategy_a,
        };

        // Fetch company data
        let company = match db.get_company(company_id) {
            Ok(c) => c,
            Err(_) => { errors += 1; continue; }
        };

        let contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("");
        let listing_id = company.get("supabase_listing_id").and_then(|v| v.as_str()).unwrap_or("");
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");

        if contact_email.is_empty() || listing_id.is_empty() {
            errors += 1;
            continue;
        }

        // Create claim token via Supabase
        let claim_token = match services::supabase::create_claim_token(
            supabase_url, supabase_key, listing_id, contact_email,
        ).await {
            Ok(token) => token,
            Err(e) => {
                log::error!("Claim token failed for {}: {}", company_name, e);
                errors += 1;
                continue;
            }
        };

        let claim_url = format!("https://fractionalforge.app/claim/{}", claim_token);

        // Build data teaser and assemble email — no LLM, pure template
        let contact_name_val = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("");
        let data_teaser = pipeline::template_outreach::build_data_teaser(&company);
        let (subject, body) = pipeline::template_outreach::assemble_email(&company, contact_name_val, company_name, &data_teaser, &claim_url);

        // Save draft with learning metadata
        match db.insert_template_email_with_learning(
            company_id, current_template_id, &subject, &body,
            contact_email, from_email, &claim_token, variant,
            if strategy.is_empty() { None } else { Some(strategy.as_str()) },
            generation,
            experiment_id.as_deref(),
            insights_json.as_deref(),
        ) {
            Ok(_) => drafts_created += 1,
            Err(_) => { errors += 1; continue; }
        }

        let _ = app.emit("drafts:progress", serde_json::json!({
            "current": i + 1,
            "total": total,
            "company": company_name,
            "variant": variant,
        }));

        // Small delay between Supabase claim token calls
        if i + 1 < total {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
    }

    Ok(serde_json::json!({
        "drafts_created": drafts_created,
        "errors": errors,
        "total": total,
    }))
}

#[tauri::command]
async fn sync_claim_statuses(
    db: tauri::State<'_, Database>,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;
    let supabase_url = config.get("supabase_url").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_key = config.get("supabase_service_key").and_then(|v| v.as_str()).unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        return Err("Supabase credentials not configured".to_string());
    }

    let token_pairs = db.get_emails_with_claim_tokens().map_err(|e| e.to_string())?;
    if token_pairs.is_empty() {
        return Ok(serde_json::json!({ "synced": 0 }));
    }

    let tokens: Vec<String> = token_pairs.iter().map(|(t, _)| t.clone()).collect();

    let statuses = services::supabase::get_claim_token_statuses(
        supabase_url, supabase_key, &tokens,
    ).await.map_err(|e| e.to_string())?;

    let updated = db.update_claim_statuses(&statuses).map_err(|e| e.to_string())?;

    Ok(serde_json::json!({ "synced": updated }))
}

// --- Self-Learning Outreach (v0.23.0) ---

#[tauri::command]
fn get_daily_outreach_stats(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_daily_outreach_stats().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_experiment_history(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_experiment_history().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_outreach_insights(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_active_insights(50).map_err(|e| e.to_string())
}

#[tauri::command]
fn seed_experiment(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    // Check if active experiment already exists
    if let Ok(Some(exp)) = db.get_active_experiment() {
        return Ok(exp);
    }
    let id = db
        .create_experiment(
            1,
            "Technical Depth: Focus on specific processes, materials, equipment, certifications. \
             Reference exact capabilities from the company data. Use technical language that \
             shows you understand their craft.",
            "Business Value: Focus on speed, cost savings, risk reduction, and revenue outcomes. \
             Frame everything in terms of business impact rather than technical detail. \
             Emphasise first-mover advantage and startup deal flow.",
        )
        .map_err(|e| e.to_string())?;
    Ok(serde_json::json!({ "id": id, "generation": 1, "created": true }))
}

#[tauri::command]
fn get_autopilot_status(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    let mut status = db.get_autopilot_status().map_err(|e| e.to_string())?;
    let config = db.get_all_config().map_err(|e| e.to_string())?;

    // Merge config into status
    let auto_enabled = config
        .get("auto_outreach_enabled")
        .and_then(|v| v.as_str())
        .unwrap_or("false") == "true";
    let schedule_time = config
        .get("schedule_time")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let daily_limit: i64 = config
        .get("daily_email_limit")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);
    let batch_size: i64 = config
        .get("outreach_batch_size")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);

    status["enabled"] = serde_json::json!(auto_enabled);
    status["schedule_time"] = serde_json::json!(schedule_time);
    status["daily_limit"] = serde_json::json!(daily_limit);
    status["batch_size"] = serde_json::json!(batch_size);

    Ok(status)
}

#[tauri::command]
async fn get_outreach_readiness(
    db: tauri::State<'_, Database>,
) -> Result<serde_json::Value, String> {
    let config = db.get_all_config().map_err(|e| e.to_string())?;

    // Check Resend API key
    let resend_key = config.get("resend_api_key").and_then(|v| v.as_str()).unwrap_or("");
    let has_resend_key = !resend_key.is_empty();

    // Check Resend domain verified (actually test the API)
    let resend_verified = if has_resend_key {
        services::resend::test_connection(resend_key).await.unwrap_or(false)
    } else {
        false
    };

    // Check Supabase
    let sb_url = config.get("supabase_url").and_then(|v| v.as_str()).unwrap_or("");
    let sb_key = config.get("supabase_service_key").and_then(|v| v.as_str()).unwrap_or("");
    let supabase_connected = if !sb_url.is_empty() && !sb_key.is_empty() {
        services::supabase::test_connection(sb_url, sb_key).await.unwrap_or(false)
    } else {
        false
    };

    // Check Ollama
    let ollama_result = services::ollama::test_connection().await;
    let ollama_running = ollama_result.is_ok();
    let ollama_has_model = if let Ok(ref val) = ollama_result {
        let outreach_model = config.get("outreach_model").and_then(|v| v.as_str()).unwrap_or("qwen3.5:27b-q4_K_M");
        val.get("models")
            .and_then(|m| m.as_array())
            .map(|models| models.iter().any(|m| {
                m.as_str().map(|s| s == outreach_model).unwrap_or(false)
            }))
            .unwrap_or(false)
    } else {
        false
    };

    // Check from email
    let from_email = config.get("from_email").and_then(|v| v.as_str()).unwrap_or("");
    let has_from_email = !from_email.is_empty();

    // Check templates
    let templates = db.get_email_templates().map(|t| t.len()).unwrap_or(0);
    let has_templates = templates > 0;

    // Check schedule time
    let schedule_time = config.get("schedule_time").and_then(|v| v.as_str()).unwrap_or("");
    let has_schedule = !schedule_time.is_empty();

    // Check autopilot enabled + template selected
    let autopilot_enabled = config.get("auto_outreach_enabled").and_then(|v| v.as_str()).unwrap_or("false") == "true";
    let autopilot_template = config.get("auto_outreach_template_id").and_then(|v| v.as_str()).unwrap_or("");
    let autopilot_configured = autopilot_enabled && !autopilot_template.is_empty();

    // Check eligible companies
    let eligible = db.get_campaign_eligible_count().unwrap_or(0);

    Ok(serde_json::json!({
        "resend_key": has_resend_key,
        "resend_verified": resend_verified,
        "supabase_connected": supabase_connected,
        "ollama_running": ollama_running,
        "ollama_has_model": ollama_has_model,
        "from_email": has_from_email,
        "has_templates": has_templates,
        "has_schedule": has_schedule,
        "autopilot_configured": autopilot_configured,
        "eligible_companies": eligible,
        "all_ready": has_resend_key && resend_verified && supabase_connected
            && ollama_running && has_from_email && has_templates
            && has_schedule && autopilot_configured && eligible > 0,
    }))
}

/// Resolve OpenAI API key from config DB, or fallback to ForgeOS .env.local / Forge-Capital .env
fn resolve_openai_key(db: &Database) -> Option<String> {
    // 1. Check config DB
    if let Ok(config) = db.get_all_config() {
        if let Some(key) = config.get("openai_api_key").and_then(|v| v.as_str()) {
            if !key.is_empty() {
                return Some(key.to_string());
            }
        }
    }

    // 2. Try ForgeOS .env.local
    let home = std::env::var("HOME").unwrap_or_default();
    let forgeos_env = format!("{}/Developer/CentaurOS created 260126 1435/.env.local", home);
    if let Ok(contents) = std::fs::read_to_string(&forgeos_env) {
        for line in contents.lines() {
            if let Some(val) = line.strip_prefix("OPENAI_API_KEY=") {
                let val = val.trim().trim_matches('"').trim_matches('\'');
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }

    // 3. Try Forge-Capital .env
    let fc_env = format!("{}/Developer/Forge-Capital/.env", home);
    if let Ok(contents) = std::fs::read_to_string(&fc_env) {
        for line in contents.lines() {
            if let Some(val) = line.strip_prefix("OPENAI_API_KEY=") {
                let val = val.trim().trim_matches('"').trim_matches('\'');
                if !val.is_empty() {
                    return Some(val.to_string());
                }
            }
        }
    }

    None
}

#[tauri::command]
fn get_company_activities(
    db: tauri::State<'_, Database>,
    company_id: String,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    let limit = limit.unwrap_or(10).max(1).min(50);
    db.get_company_activities(&company_id, limit)
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn get_company_intel(
    db: tauri::State<'_, Database>,
    company_id: String,
) -> Result<Option<serde_json::Value>, String> {
    db.get_intel(&company_id).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_company_verification(
    db: tauri::State<'_, Database>,
    company_id: String,
) -> Result<serde_json::Value, String> {
    db.get_company_verification(&company_id)
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn search_semantic(
    db: tauri::State<'_, Database>,
    cache: tauri::State<'_, Mutex<EmbeddingCache>>,
    query: String,
    limit: Option<usize>,
    status: Option<String>,
    subcategory: Option<String>,
    country: Option<String>,
) -> Result<serde_json::Value, String> {
    let query = query.trim().to_string();
    if query.is_empty() {
        return Err("Query cannot be empty".to_string());
    }

    let limit = limit.unwrap_or(50).min(200);

    // Resolve API key
    let api_key = resolve_openai_key(&db)
        .ok_or_else(|| "OpenAI API key not configured. Set it in Settings or in ForgeOS .env.local".to_string())?;

    // Embed the query via OpenAI
    let query_embedding = services::openai::embed_query(&api_key, &query)
        .await
        .map_err(|e| format!("Embedding failed: {}", e))?;

    // Load/use cached embeddings
    let embeddings = {
        let mut cache_guard = cache.lock().map_err(|e| e.to_string())?;
        if !cache_guard.loaded {
            let loaded = db.load_embeddings().map_err(|e| format!("Failed to load embeddings: {}", e))?;
            log::info!("Loaded {} embeddings into cache", loaded.len());
            cache_guard.embeddings = loaded;
            cache_guard.loaded = true;
        }
        cache_guard.embeddings.clone()
    };

    if embeddings.is_empty() {
        return Ok(serde_json::json!({
            "companies": [],
            "scores": [],
            "total": 0,
        }));
    }

    // Compute cosine similarity for all embeddings
    let threshold = 0.3f32;
    let mut scored: Vec<(String, f32)> = embeddings
        .iter()
        .filter_map(|(id, emb)| {
            let sim = cosine_similarity(&query_embedding, emb);
            if sim >= threshold {
                Some((id.clone(), sim))
            } else {
                None
            }
        })
        .collect();

    // Sort by score descending
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    // Fetch a larger batch first (we'll filter after)
    let fetch_limit = (limit * 3).min(scored.len());
    let top_ids: Vec<String> = scored.iter().take(fetch_limit).map(|(id, _)| id.clone()).collect();
    let score_map: std::collections::HashMap<String, f32> = scored.iter().cloned().collect();

    // Get full company data
    let all_companies = db.get_companies_by_ids(&top_ids).map_err(|e| format!("DB error: {}", e))?;

    // Apply filters (status, subcategory, country)
    let filtered: Vec<serde_json::Value> = all_companies
        .into_iter()
        .filter(|c| {
            if let Some(ref s) = status {
                if let Some(cs) = c.get("status").and_then(|v| v.as_str()) {
                    if cs != s.as_str() {
                        return false;
                    }
                }
            }
            if let Some(ref sc) = subcategory {
                if let Some(cs) = c.get("subcategory").and_then(|v| v.as_str()) {
                    if cs != sc.as_str() {
                        return false;
                    }
                }
            }
            if let Some(ref co) = country {
                if let Some(cc) = c.get("country").and_then(|v| v.as_str()) {
                    if cc != co.as_str() {
                        return false;
                    }
                }
            }
            true
        })
        .take(limit)
        .collect();

    let scores: Vec<f32> = filtered
        .iter()
        .map(|c| {
            let id = c.get("id").and_then(|v| v.as_str()).unwrap_or("");
            *score_map.get(id).unwrap_or(&0.0)
        })
        .collect();

    let total = filtered.len();

    Ok(serde_json::json!({
        "companies": filtered,
        "scores": scores,
        "total": total,
    }))
}

// ── Search Profile Commands ───────────────────────────────────────────

#[tauri::command]
fn get_search_profiles(db: tauri::State<'_, Database>) -> Result<Vec<serde_json::Value>, String> {
    db.get_search_profiles().map_err(|e| e.to_string())
}

#[tauri::command]
fn save_search_profile(
    db: tauri::State<'_, Database>,
    id: String,
    name: String,
    description: String,
    domain: String,
    categories_json: String,
    target_countries_json: String,
) -> Result<(), String> {
    db.save_search_profile(&id, &name, &description, &domain, &categories_json, &target_countries_json)
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn delete_search_profile(db: tauri::State<'_, Database>, id: String) -> Result<(), String> {
    db.delete_search_profile(&id).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_active_profile(db: tauri::State<'_, Database>) -> Result<String, String> {
    Ok(db.get_active_profile_id())
}

#[tauri::command]
fn set_active_profile(db: tauri::State<'_, Database>, id: String) -> Result<(), String> {
    db.set_config("active_profile_id", &id).map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    env_logger::init();

    tauri::Builder::default()
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .setup(|app| {
            // Initialize database in app data directory
            let app_dir = app
                .path()
                .app_data_dir()
                .expect("Failed to get app data directory");

            let database = Database::new(&app_dir).expect("Failed to initialize database");
            app.manage(database);
            app.manage(Mutex::new(EmbeddingCache::new()));

            // Auto-check Ollama connection on startup
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let result = services::ollama::test_connection().await;
                match result {
                    Ok(info) => {
                        log::info!("Ollama connected: {:?}", info);
                        let _ = handle.emit("ollama:status", serde_json::json!({
                            "connected": true,
                            "models": info.get("models"),
                        }));
                    }
                    Err(e) => {
                        log::warn!("Ollama not reachable: {}", e);
                        let _ = handle.emit("ollama:status", serde_json::json!({
                            "connected": false,
                            "error": e.to_string(),
                        }));
                    }
                }
            });

            // Start automated scheduler
            let scheduler_handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                pipeline::start_scheduler(scheduler_handle).await;
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_stats,
            get_extended_stats,
            get_companies,
            get_company,
            update_company_status,
            get_emails,
            update_email_status,
            get_config,
            set_config,
            test_ollama_connection,
            test_brave_connection,
            test_supabase_connection,
            test_resend_connection,
            test_anthropic_connection,
            test_deepseek_connection,
            start_pipeline,
            stop_pipeline,
            get_pipeline_status,
            get_run_log,
            reset_error_companies,
            approve_all_enriched,
            reenrich_all,
            get_analytics,
            get_companies_filtered,
            refresh_email_statuses,
            send_approved_emails,
            retry_failed_emails,
            backup_database,
            import_for_audit,
            push_single_company,
            remove_from_marketplace,
            remove_all_from_marketplace,
            get_companies_for_map,
            geocode_companies,
            get_companies_count,
            batch_update_status,
            get_stats_history,
            get_run_history,
            get_pipeline_nodes,
            get_email_templates,
            save_email_template,
            delete_email_template,
            get_campaign_eligible_count,
            delete_emails,
            get_outreach_companies,
            get_outreach_stats,
            get_company_email_history,
            generate_drafts_for_companies,
            sync_claim_statuses,
            get_daily_outreach_stats,
            get_experiment_history,
            get_outreach_insights,
            seed_experiment,
            get_autopilot_status,
            get_outreach_readiness,
            search_semantic,
            get_company_activities,
            get_company_intel,
            get_company_verification,
            get_search_profiles,
            save_search_profile,
            delete_search_profile,
            get_active_profile,
            set_active_profile,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
