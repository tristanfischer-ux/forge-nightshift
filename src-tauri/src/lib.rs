mod db;
mod services;
mod pipeline;

use db::Database;
use tauri::{Emitter, Manager};

#[tauri::command]
fn get_stats(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_stats().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_companies(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_companies(status.as_deref(), limit.unwrap_or(50), offset.unwrap_or(0))
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn get_company(db: tauri::State<'_, Database>, id: String) -> Result<serde_json::Value, String> {
    db.get_company(&id).map_err(|e| e.to_string())
}

#[tauri::command]
fn update_company_status(
    db: tauri::State<'_, Database>,
    id: String,
    status: String,
) -> Result<(), String> {
    db.update_company_status(&id, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_emails(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_emails(status.as_deref(), limit.unwrap_or(50))
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn update_email_status(
    db: tauri::State<'_, Database>,
    id: String,
    status: String,
) -> Result<(), String> {
    db.update_email_status(&id, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_config(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_all_config().map_err(|e| e.to_string())
}

#[tauri::command]
fn set_config(db: tauri::State<'_, Database>, key: String, value: String) -> Result<(), String> {
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
    db.get_companies_filtered(
        status.as_deref(),
        subcategory.as_deref(),
        country.as_deref(),
        search.as_deref(),
        limit.unwrap_or(50),
        offset.unwrap_or(0),
    )
    .map_err(|e| e.to_string())
}

#[tauri::command]
fn get_run_log(
    db: tauri::State<'_, Database>,
    job_id: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_run_log(job_id.as_deref(), limit.unwrap_or(100))
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

        // Rate limit between Resend API calls
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

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

    let quality_threshold = threshold.unwrap_or(50);

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
    db.get_run_history(limit.unwrap_or(20)).map_err(|e| e.to_string())
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
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
