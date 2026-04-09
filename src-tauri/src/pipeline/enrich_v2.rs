use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::db::Database;

/// Unified enrichment stage — replaces both enrich.rs and deep_enrich.rs.
/// For each company: deep scrape → two parallel LLM calls (metadata + processes) →
/// registry lookup → geocoding → validation → single combined DB write.
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434")
        .to_string();

    let enrich_model = config
        .get("enrich_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3.5:27b-q4_K_M")
        .to_string();

    let llm_backend = config
        .get("llm_backend")
        .and_then(|v| v.as_str())
        .unwrap_or("deepseek")
        .to_string();

    let display_model = match llm_backend.as_str() {
        "deepseek" => "deepseek-chat".to_string(),
        "haiku" => "claude-haiku-4.5".to_string(),
        _ => enrich_model.clone(),
    };

    let anthropic_api_key = config
        .get("anthropic_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let deepseek_api_key = config
        .get("deepseek_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let ch_api_key = config
        .get("companies_house_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let oc_api_key = config
        .get("opencorporates_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let relevance_threshold: i64 = config
        .get("relevance_threshold")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let quality_threshold: i64 = config
        .get("auto_approve_quality_threshold")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    let concurrency: usize = config
        .get("enrich_concurrency")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1)
        .min(10);

    // Load active profile domain for prompt customization
    let active_domain = {
        let db: tauri::State<'_, Database> = app.state();
        let profile_id = db.get_active_profile_id();
        match db.get_search_profile(&profile_id) {
            Ok(Some(profile)) => profile
                .get("domain")
                .and_then(|v| v.as_str())
                .unwrap_or("manufacturing")
                .to_string(),
            _ => "manufacturing".to_string(),
        }
    };

    // Reset stuck 'enriching' companies from a previous crashed run
    let stuck_count = {
        let db: tauri::State<'_, Database> = app.state();
        db.reset_stuck_enriching()?
    };
    if stuck_count > 0 {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "enrich",
            "info",
            &format!(
                "Reset {} stuck 'enriching' companies back to discovered",
                stuck_count
            ),
        );
    }

    // Cumulative counters — persist across all batches
    let enriched_count = Arc::new(AtomicI64::new(0));
    let approved_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));
    let no_website_total = Arc::new(AtomicI64::new(0));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "enrich",
            "info",
            &format!(
                "Starting enrich_v2 drain loop (concurrency={}, batch=50) using backend: {} / model: {} (auto-approve: relevance>={}, quality>={})",
                concurrency, llm_backend, enrich_model, relevance_threshold, quality_threshold
            ),
        );
    }

    let started_at = chrono::Utc::now();

    super::emit_node(
        app,
        json!({
            "node_id": "enrich",
            "status": "running",
            "model": &display_model,
            "progress": { "current": 0, "total": null, "rate": null, "current_item": null },
            "concurrency": concurrency,
            "started_at": started_at.to_rfc3339(),
            "elapsed_secs": 0
        }),
    );

    // === Drain loop: keep pulling batches until queue is empty and research is done ===
    loop {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "enrich", "warn", "Enrichment cancelled by user");
            break;
        }

        // Pre-filter: batch-mark all discovered companies without websites as errors
        let no_website_count = {
            let db: tauri::State<'_, Database> = app.state();
            db.batch_mark_no_website_errors()?
        };
        if no_website_count > 0 {
            no_website_total.fetch_add(no_website_count, Ordering::Relaxed);
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "enrich",
                "info",
                &format!("Pre-filtered {} companies with no website", no_website_count),
            );
        }

        // Load next batch (smaller batches = faster pickup of new discoveries)
        let companies = {
            let db: tauri::State<'_, Database> = app.state();
            db.get_enrichable_companies(50)?
        };

        if companies.is_empty() {
            if super::is_research_active() {
                // Research is still running — wait and retry
                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "enrich",
                        "info",
                        "Queue empty, waiting for research to discover more companies...",
                    );
                }
                let cur_enriched = enriched_count.load(Ordering::Relaxed);
                let cur_errors = error_count.load(Ordering::Relaxed);
                let _ = app.emit(
                    "pipeline:progress",
                    json!({
                        "stage": "enrich",
                        "phase": "waiting",
                        "enriched": cur_enriched,
                        "errors": cur_errors,
                        "model": display_model,
                    }),
                );
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            } else {
                // Research is done (or was never running) — exit
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "enrich",
                    "info",
                    "Queue empty, research complete — enrichment finished",
                );
                break;
            }
        }

        let batch_size = companies.len();
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "enrich",
                "info",
                &format!("Processing batch of {} companies", batch_size),
            );
        }

        stream::iter(companies.into_iter())
            .map(|company| {
                let app = app.clone();
                let job_id = job_id.to_string();
                let ollama_url = ollama_url.clone();
                let enrich_model = enrich_model.clone();
                let display_model = display_model.clone();
                let llm_backend = llm_backend.clone();
                let anthropic_api_key = anthropic_api_key.clone();
                let deepseek_api_key = deepseek_api_key.clone();
                let ch_api_key = ch_api_key.clone();
                let oc_api_key = oc_api_key.clone();
                let active_domain = active_domain.clone();
                let enriched_count = Arc::clone(&enriched_count);
                let approved_count = Arc::clone(&approved_count);
                let error_count = Arc::clone(&error_count);

                async move {
                    if super::is_cancelled() {
                        return;
                    }

                    let id = company
                        .get("id")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let name = company
                        .get("name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let website = company
                        .get("website_url")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let snippet = company
                        .get("raw_snippet")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let country = company
                        .get("country")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();
                    let city = company
                        .get("city")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_string();

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "enrich",
                            "info",
                            &format!("Enriching: {}", name),
                        );
                        let _ = db.update_company_status(&id, "enriching");
                    }

                    let cur_enriched = enriched_count.load(Ordering::Relaxed);
                    let cur_errors = error_count.load(Ordering::Relaxed);

                    let _ = app.emit(
                        "pipeline:progress",
                        json!({
                            "stage": "enrich",
                            "phase": "start",
                            "current_company": name,
                            "enriched": cur_enriched,
                            "errors": cur_errors,
                            "model": display_model,
                        }),
                    );

                    // ── Step A: Deep scrape (with basic fallback) ──────────────────
                    let mut enrichment_completeness = "full";
                    let website_text =
                        match crate::services::scraper::fetch_website_text_deep(&website).await {
                            Ok(text) => text,
                            Err(deep_err) => {
                                log::warn!(
                                    "[Enrich] Deep scrape failed for {}: {} — falling back to basic scrape",
                                    name,
                                    deep_err
                                );
                                match crate::services::scraper::fetch_website_text(&website).await {
                                    Ok(text) => {
                                        enrichment_completeness = "partial";
                                        text
                                    }
                                    Err(e) => {
                                        let error_msg =
                                            format!("Scrape failed (deep + basic): {}", e);
                                        log::warn!("[Enrich] {}: {}", name, error_msg);
                                        let db: tauri::State<'_, Database> = app.state();
                                        let _ = db.log_activity(
                                            &job_id,
                                            "enrich",
                                            "error",
                                            &format!(
                                                "Scrape failed for {}: {}",
                                                name, error_msg
                                            ),
                                        );
                                        let _ = db.set_company_error(&id, &error_msg);
                                        error_count.fetch_add(1, Ordering::Relaxed);
                                        return;
                                    }
                                }
                            }
                        };

                    if website_text.len() < 50 {
                        let error_msg = format!(
                            "Website text too short ({} chars)",
                            website_text.len()
                        );
                        log::warn!("[Enrich] {}: {}", name, error_msg);
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.set_company_error(&id, &error_msg);
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    }

                    // ── Step B: Two PARALLEL LLM calls ─────────────────────────────
                    let metadata_prompt = build_metadata_prompt(
                        &active_domain,
                        &name,
                        &website,
                        &website_text,
                        &snippet,
                    );
                    let processes_prompt =
                        build_processes_prompt(&name, &website, &website_text, &active_domain);

                    let (metadata_result, processes_result) = tokio::join!(
                        llm_call(
                            &llm_backend,
                            &anthropic_api_key,
                            &deepseek_api_key,
                            &ollama_url,
                            &enrich_model,
                            &metadata_prompt,
                        ),
                        llm_call(
                            &llm_backend,
                            &anthropic_api_key,
                            &deepseek_api_key,
                            &ollama_url,
                            &enrich_model,
                            &processes_prompt,
                        ),
                    );

                    // Handle LLM call failures gracefully
                    let metadata_ok = metadata_result.is_ok();
                    let processes_ok = processes_result.is_ok();

                    if !metadata_ok && !processes_ok {
                        let error_msg = format!(
                            "Both LLM calls failed — metadata: {}, processes: {}",
                            metadata_result.unwrap_err(),
                            processes_result.unwrap_err()
                        );
                        log::warn!("[Enrich] {}: {}", name, error_msg);
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "enrich",
                            "error",
                            &format!("LLM failed for {}: {}", name, error_msg),
                        );
                        let _ = db.set_company_error(&id, &error_msg);
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    }

                    if !metadata_ok || !processes_ok {
                        enrichment_completeness = "partial";
                    }

                    // Parse metadata response
                    let mut enriched: Value = if let Ok(ref response) = metadata_result {
                        match serde_json::from_str(response) {
                            Ok(v) => v,
                            Err(e) => {
                                let truncated: String = response.chars().take(300).collect();
                                log::warn!(
                                    "[Enrich] Metadata JSON parse failed for {}: {} — response: {}",
                                    name,
                                    e,
                                    truncated
                                );
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "warn",
                                    &format!("Metadata JSON parse failed for {}: {}", name, e),
                                );
                                enrichment_completeness = "partial";
                                json!({})
                            }
                        }
                    } else {
                        log::warn!(
                            "[Enrich] Metadata LLM failed for {}: {}",
                            name,
                            metadata_result.as_ref().unwrap_err()
                        );
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "enrich",
                            "warn",
                            &format!(
                                "Metadata LLM failed for {}: {}",
                                name,
                                metadata_result.unwrap_err()
                            ),
                        );
                        json!({})
                    };

                    // Parse processes response
                    let processes_json: String = if let Ok(ref response) = processes_result {
                        match serde_json::from_str::<Value>(response) {
                            Ok(parsed) => {
                                let processes = parsed
                                    .get("processes")
                                    .and_then(|v| v.as_array())
                                    .cloned()
                                    .unwrap_or_default();
                                serde_json::to_string(&processes)
                                    .unwrap_or_else(|_| "[]".to_string())
                            }
                            Err(e) => {
                                let truncated: String = response.chars().take(300).collect();
                                log::warn!(
                                    "[Enrich] Processes JSON parse failed for {}: {} — response: {}",
                                    name,
                                    e,
                                    truncated
                                );
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "warn",
                                    &format!("Processes JSON parse failed for {}: {}", name, e),
                                );
                                enrichment_completeness = "partial";
                                "[]".to_string()
                            }
                        }
                    } else {
                        log::warn!(
                            "[Enrich] Processes LLM failed for {}: {}",
                            name,
                            processes_result.as_ref().unwrap_err()
                        );
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "enrich",
                            "warn",
                            &format!(
                                "Processes LLM failed for {}: {}",
                                name,
                                processes_result.unwrap_err()
                            ),
                        );
                        "[]".to_string()
                    };

                    // If metadata was empty (both calls produced nothing useful), reject
                    let has_metadata = enriched.get("description").is_some()
                        || enriched.get("capabilities").is_some();
                    let has_processes = processes_json != "[]";
                    if !has_metadata && !has_processes {
                        let error_msg = "No usable data from LLM calls".to_string();
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.set_company_error(&id, &error_msg);
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    }

                    // ── Step E: Validation ──────────────────────────────────────────
                    if has_metadata {
                        if let Some(rejected) = validate_enrichment(&mut enriched) {
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "enrich",
                                "info",
                                &format!("Rejected {} — {}", name, rejected),
                            );
                            let _ = db.set_company_error(
                                &id,
                                &format!("Validation rejected: {}", rejected),
                            );
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    }

                    // ── Step C: Registry lookup ─────────────────────────────────────
                    let mut attributes = json!({
                        "website_url": website,
                        "country": country,
                        "city": city,
                        "specialties": enriched.get("capabilities").unwrap_or(&json!([])),
                        "certifications": enriched.get("certifications").unwrap_or(&json!([])),
                        "employees": enriched.get("company_size").and_then(|v| v.as_str()).unwrap_or(""),
                        "industries": enriched.get("industries").unwrap_or(&json!([])),
                        "materials": enriched.get("materials").unwrap_or(&json!([])),
                        "key_equipment": enriched.get("key_equipment").unwrap_or(&json!([])),
                        "production_capacity": enriched.get("production_capacity").and_then(|v| v.as_str()).unwrap_or(""),
                        "founded_year": enriched.get("founded_year"),
                        "employee_count_exact": enriched.get("employee_count_exact"),
                        "key_people": enriched.get("key_people").unwrap_or(&json!([])),
                        "nightshift_score": enriched.get("relevance_score").and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)).or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))).unwrap_or(0),
                        "products": enriched.get("products").unwrap_or(&json!([])),
                        "lead_time": enriched.get("lead_time"),
                        "minimum_order": enriched.get("minimum_order"),
                        "quality_systems": enriched.get("quality_systems"),
                        "export_controls": enriched.get("export_controls"),
                        "security_clearances": enriched.get("security_clearances").unwrap_or(&json!([])),
                    });

                    // Companies House enrichment for UK companies
                    if (country == "GB" || country == "UK") && !ch_api_key.is_empty() {
                        {
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "enrich",
                                "info",
                                &format!("Looking up {} on Companies House...", name),
                            );
                        }

                        match crate::services::companies_house::enrich_company(&ch_api_key, &name)
                            .await
                        {
                            Ok(Some(ch_data)) => {
                                if let Some(obj) = ch_data.as_object() {
                                    if let Some(attrs) = attributes.as_object_mut() {
                                        for (k, v) in obj {
                                            attrs.insert(k.clone(), v.clone());
                                        }
                                    }
                                }

                                let db: tauri::State<'_, Database> = app.state();
                                let ch_number = ch_data
                                    .get("ch_company_number")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("?");
                                let _ = db.mark_ch_verified(&id, ch_number);
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "info",
                                    &format!("CH match for {}: #{}", name, ch_number),
                                );
                            }
                            Ok(None) => {
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "info",
                                    &format!("No CH match found for {}", name),
                                );
                            }
                            Err(e) => {
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "warn",
                                    &format!("CH lookup failed for {}: {}", name, e),
                                );
                            }
                        }
                    }

                    // OpenCorporates enrichment for non-UK companies
                    if country != "GB" && country != "UK" && !country.is_empty() {
                        {
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "enrich",
                                "info",
                                &format!(
                                    "Looking up {} on OpenCorporates ({})...",
                                    name, country
                                ),
                            );
                        }

                        match crate::services::opencorporates::enrich_company(
                            &oc_api_key, &name, &country,
                        )
                        .await
                        {
                            Ok(Some(oc_data)) => {
                                if let Some(obj) = oc_data.as_object() {
                                    if let Some(attrs) = attributes.as_object_mut() {
                                        for (k, v) in obj {
                                            attrs.insert(k.clone(), v.clone());
                                        }
                                    }
                                }

                                let db: tauri::State<'_, Database> = app.state();
                                let oc_number = oc_data
                                    .get("oc_company_number")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("?");
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "info",
                                    &format!("OC match for {}: #{}", name, oc_number),
                                );
                            }
                            Ok(None) => {
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "info",
                                    &format!("No OC match found for {}", name),
                                );
                            }
                            Err(e) => {
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "warn",
                                    &format!("OC lookup failed for {}: {}", name, e),
                                );
                            }
                        }
                    }

                    // Extract financial health from whichever registry provided data
                    let financial_health = attributes
                        .get("financial_signals")
                        .and_then(|fs| fs.get("health"))
                        .and_then(|h| h.as_str())
                        .unwrap_or("")
                        .to_string();

                    // ── Step D: Geocoding ────────────────────────────────────────────
                    // Extract address: prefer LLM-extracted, fall back to registry
                    let address = enriched
                        .get("address")
                        .and_then(|v| v.as_str())
                        .filter(|s| !s.is_empty())
                        .or_else(|| {
                            attributes
                                .get("ch_registered_address")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                        })
                        .or_else(|| {
                            attributes
                                .get("oc_registered_address")
                                .and_then(|v| v.as_str())
                                .filter(|s| !s.is_empty())
                        })
                        .unwrap_or("")
                        .to_string();

                    let mut geo_lat: Option<f64> = None;
                    let mut geo_lng: Option<f64> = None;
                    if country == "GB" {
                        // UK: use postcodes.io (fast, no rate limit issues)
                        if let Some(postcode) =
                            crate::services::postcodes::extract_uk_postcode(&address)
                        {
                            match crate::services::postcodes::geocode_postcode(&postcode).await {
                                Ok((lat, lng)) => {
                                    geo_lat = Some(lat);
                                    geo_lng = Some(lng);
                                }
                                Err(_) => {} // fall through to city fallback
                            }
                        }
                        if geo_lat.is_none() && !city.is_empty() {
                            if let Ok((lat, lng)) =
                                crate::services::postcodes::geocode_place(&city).await
                            {
                                geo_lat = Some(lat);
                                geo_lng = Some(lng);
                            }
                        }
                    } else if !country.is_empty() {
                        // Non-UK: use Nominatim (1.1s rate limit built in)
                        if !address.is_empty() {
                            if let Ok((lat, lng)) =
                                crate::services::nominatim::geocode_address(&address).await
                            {
                                geo_lat = Some(lat);
                                geo_lng = Some(lng);
                            }
                        }
                        if geo_lat.is_none() && !city.is_empty() {
                            if let Ok((lat, lng)) =
                                crate::services::nominatim::geocode_city_country(&city, &country)
                                    .await
                            {
                                geo_lat = Some(lat);
                                geo_lng = Some(lng);
                            }
                        }
                    }

                    // ── Step F: Combined DB write ───────────────────────────────────
                    let mut enriched_with_attrs = enriched.clone();
                    enriched_with_attrs["attributes_json"] = attributes;
                    enriched_with_attrs["address"] = json!(address);
                    enriched_with_attrs["financial_health"] = json!(financial_health);

                    // Pass through translation fields from LLM response
                    if let Some(v) = enriched.get("description_original") {
                        enriched_with_attrs["description_original"] = v.clone();
                    }
                    if let Some(v) = enriched.get("snippet_english") {
                        enriched_with_attrs["snippet_english"] = v.clone();
                    }

                    // Map capabilities to specialties for backward compat with DB column
                    if enriched_with_attrs.get("specialties").is_none() {
                        enriched_with_attrs["specialties"] =
                            enriched.get("capabilities").cloned().unwrap_or(json!([]));
                    }

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        // Write metadata via update_company_enrichment
                        match db.update_company_enrichment(&id, &enriched_with_attrs) {
                            Ok(_) => {
                                enriched_count.fetch_add(1, Ordering::Relaxed);

                                // Save geocode data if available
                                if let (Some(lat), Some(lng)) = (geo_lat, geo_lng) {
                                    let _ = db.update_company_geocode(&id, lat, lng);
                                }

                                // Save process capabilities + deep scrape data
                                let _ = db.update_deep_enrichment(
                                    &id,
                                    &processes_json,
                                    &website_text,
                                );

                                // Auto-approve if scores meet thresholds
                                let rel = enriched_with_attrs
                                    .get("relevance_score")
                                    .and_then(|v| {
                                        v.as_i64()
                                            .or_else(|| v.as_f64().map(|f| f as i64))
                                            .or_else(|| {
                                                v.as_str().and_then(|s| s.parse().ok())
                                            })
                                    })
                                    .unwrap_or(0);
                                let qual = enriched_with_attrs
                                    .get("enrichment_quality")
                                    .and_then(|v| {
                                        v.as_i64()
                                            .or_else(|| v.as_f64().map(|f| f as i64))
                                            .or_else(|| {
                                                v.as_str().and_then(|s| s.parse().ok())
                                            })
                                    })
                                    .unwrap_or(0);
                                if rel >= relevance_threshold && qual >= quality_threshold {
                                    let _ = db.update_company_status(&id, "approved");
                                    approved_count.fetch_add(1, Ordering::Relaxed);
                                }
                            }
                            Err(e) => {
                                let error_msg = format!("DB save failed: {}", e);
                                let _ = db.log_activity(
                                    &job_id,
                                    "enrich",
                                    "error",
                                    &format!(
                                        "Failed to save enrichment for {}: {}",
                                        name, error_msg
                                    ),
                                );
                                let _ = db.set_company_error(&id, &error_msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    }

                    let cur_enriched = enriched_count.load(Ordering::Relaxed);
                    let cur_errors = error_count.load(Ordering::Relaxed);

                    let _ = app.emit(
                        "pipeline:progress",
                        json!({
                            "stage": "enrich",
                            "phase": "done",
                            "current_company": name,
                            "enriched": cur_enriched,
                            "errors": cur_errors,
                            "model": display_model,
                            "completeness": enrichment_completeness,
                        }),
                    );

                    if cur_enriched % 5 == 0 || cur_enriched == 1 {
                        let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                        let rate = if elapsed > 0 {
                            cur_enriched as f64 / elapsed as f64 * 3600.0
                        } else {
                            0.0
                        };
                        super::emit_node(
                            &app,
                            json!({
                                "node_id": "enrich",
                                "status": "running",
                                "model": &display_model,
                                "progress": { "current": cur_enriched, "total": null, "rate": rate, "current_item": &name },
                                "concurrency": concurrency,
                                "started_at": started_at.to_rfc3339(),
                                "elapsed_secs": elapsed
                            }),
                        );
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<()>>()
            .await;
    }

    let final_enriched = enriched_count.load(Ordering::Relaxed);
    let final_approved = approved_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);
    let final_no_website = no_website_total.load(Ordering::Relaxed);

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    super::emit_node(
        app,
        json!({
            "node_id": "enrich",
            "status": "completed",
            "model": &display_model,
            "progress": { "current": final_enriched, "total": final_enriched, "rate": null, "current_item": null },
            "concurrency": concurrency,
            "started_at": started_at.to_rfc3339(),
            "elapsed_secs": elapsed
        }),
    );

    Ok(json!({
        "companies_enriched": final_enriched,
        "auto_approved": final_approved,
        "errors": final_errors,
        "no_website_filtered": final_no_website,
    }))
}

/// Dispatch an LLM call to the configured backend.
/// Returns the raw response string on success.
async fn llm_call(
    llm_backend: &str,
    anthropic_api_key: &str,
    deepseek_api_key: &str,
    ollama_url: &str,
    model: &str,
    prompt: &str,
) -> Result<String> {
    if llm_backend == "haiku" {
        crate::services::anthropic::chat(anthropic_api_key, None, prompt, true).await
    } else if llm_backend == "deepseek" {
        crate::services::deepseek::chat(deepseek_api_key, None, prompt, true).await
    } else {
        // Ollama — use generate (not generate_with_ctx) with think: false for qwen models
        crate::services::ollama::generate(ollama_url, model, prompt, false).await
    }
}

/// Build the metadata extraction prompt (same fields as enrich.rs).
fn build_metadata_prompt(
    active_domain: &str,
    name: &str,
    website: &str,
    website_text: &str,
    snippet: &str,
) -> String {
    format!(
        r#"Analyze this {} company for a B2B marketplace. Return JSON with these fields:
description (2-3 sentences, English), description_original (original language if not English, else null), snippet_english (English translation of snippet, null if already English), category ("Products"/"Services"), subcategory, capabilities (array), industries (array), materials (array of specific materials with grades/alloys, e.g. ["Aluminium 6061-T6", "Stainless Steel 316L", "Titanium Ti-6Al-4V", "ABS", "Carbon Fibre", "PA12 Nylon", "Brass CZ121", "Mild Steel S275"]), key_equipment (array with brand+model), production_capacity (string or null), certifications (array), company_size ("1-9"/"10-49"/"50-99"/"100-249"/"250-499"/"500+"), employee_count_exact (int or null), key_people (array of name+title, max 5), founded_year (int or null), contact_name, contact_email (extract from mailto: links, contact/about pages, footer — prefer sales@, info@, contact@ — if listed in CONTACT EMAILS FOUND above, USE IT), contact_title, address (full with postcode or null), products (array), lead_time (string or null), minimum_order (string or null), quality_systems (string or null), export_controls (string or null), security_clearances (array), relevance_score (0-100, 80+=clearly manufacturing), enrichment_quality (0-100).

CRITICAL: Return null if no evidence. Do NOT guess. All text in English.

Company: {}
Website: {}
Data:
{}

Snippet: {}

Return ONLY valid JSON. /no_think"#,
        active_domain, name, website, website_text, snippet
    )
}

/// Build the process capabilities extraction prompt (same as deep_enrich.rs).
fn build_processes_prompt(
    name: &str,
    website: &str,
    website_text: &str,
    _active_domain: &str,
) -> String {
    format!(
        r#"You are a manufacturing process analyst. Extract EVERY specific manufacturing process, capability, and technical specification from this company's website text.

Return JSON with a single key "processes" containing an array. Each element represents ONE distinct manufacturing process or capability:

{{
  "processes": [
    {{
      "process_category": "<one of: cnc_machining, injection_moulding, additive_manufacturing, sheet_metal, casting, forging, welding, surface_treatment, heat_treatment, assembly, metrology, other>",
      "process_name": "<specific name, e.g. 'CNC 5-Axis Milling', 'SLS Nylon Printing', 'MIG Welding'>",
      "materials_worked": ["<with grades where mentioned, e.g. 'Aluminium 6061-T6', 'Stainless Steel 316L'>"],
      "tolerance_claimed": "<exact text from website, e.g. '±0.01mm', 'within 0.005\"'>",
      "tolerance_value_mm": <numeric in mm, or null if not stated>,
      "surface_finish_claimed": "<exact text, e.g. 'Ra 0.8μm', '32 microinch'>",
      "surface_finish_ra_um": <numeric in μm, or null if not stated>,
      "max_part_dimensions": "<if mentioned, e.g. '2000mm x 1500mm x 800mm'>",
      "batch_size_range": "<if mentioned, e.g. '1-10000', 'prototype to production'>",
      "equipment_mentioned": ["<brand+model ONLY if specifically named, e.g. 'DMG Mori DMU 50', 'EOS M 290'>"],
      "surface_treatments": ["<post-processing offered, e.g. 'anodising', 'powder coating', 'electroless nickel'>"],
      "confidence": <0.0-1.0, how confident you are this process is actually offered>,
      "source_excerpt": "<copy the EXACT sentence(s) from the website text that support this claim, max 200 chars>"
    }}
  ]
}}

RULES:
- Only extract processes actually offered by this company. Do NOT invent capabilities.
- If the website says "CNC machining" with no other detail, still include it but with null tolerances/finishes and confidence 0.5.
- If specific tolerances, finishes, or equipment are mentioned, confidence should be 0.8-1.0.
- surface_treatments is a separate category but also list treatments as part of the process that offers them.
- Return an empty array if no manufacturing processes are evident.
- source_excerpt MUST be actual text from the website, not paraphrased.

Company: {name}
Website: {website}

--- WEBSITE TEXT ---
{website_text}
--- END ---

Return ONLY valid JSON."#,
        name = name,
        website = website,
        website_text = website_text,
    )
}

/// Validate and adjust enrichment data. Returns Some(reason) if the enrichment should be rejected.
fn validate_enrichment(enriched: &mut Value) -> Option<String> {
    // Clamp relevance_score to 0-100
    if let Some(score) = enriched.get("relevance_score").and_then(|v| v.as_i64()) {
        enriched["relevance_score"] = json!(score.clamp(0, 100));
    }

    // Clamp enrichment_quality to 0-100
    if let Some(quality) = enriched.get("enrichment_quality").and_then(|v| v.as_i64()) {
        enriched["enrichment_quality"] = json!(quality.clamp(0, 100));
    }

    let description = enriched
        .get("description")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    // If description is empty or too short, cap quality
    if description.is_empty() || description.len() < 20 {
        enriched["enrichment_quality"] = json!(enriched
            .get("enrichment_quality")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .min(20));
    }

    // If no capabilities AND no key_equipment AND no materials, cap quality
    let has_capabilities = enriched
        .get("capabilities")
        .and_then(|v| v.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    let has_equipment = enriched
        .get("key_equipment")
        .and_then(|v| v.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false);
    let has_materials = enriched
        .get("materials")
        .and_then(|v| v.as_array())
        .map(|a| !a.is_empty())
        .unwrap_or(false);

    if !has_capabilities && !has_equipment && !has_materials {
        enriched["enrichment_quality"] = json!(enriched
            .get("enrichment_quality")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
            .min(30));
    }

    // Filter out generic equipment entries (no brand name)
    if let Some(equipment) = enriched
        .get("key_equipment")
        .and_then(|v| v.as_array())
        .cloned()
    {
        let generic_terms = ["CNC machine", "lathe", "milling machine", "press", "saw"];
        let filtered: Vec<Value> = equipment
            .into_iter()
            .filter(|item| {
                if let Some(s) = item.as_str() {
                    !generic_terms.iter().any(|g| s.eq_ignore_ascii_case(g))
                } else {
                    true
                }
            })
            .collect();
        enriched["key_equipment"] = json!(filtered);
    }

    // Validate certification formats — keep only known patterns
    if let Some(certs) = enriched
        .get("certifications")
        .and_then(|v| v.as_array())
        .cloned()
    {
        let valid_prefixes = [
            "ISO", "AS9100", "AS/EN", "IATF", "NADCAP", "CE", "UL", "CSA", "ATEX", "PED", "EN",
            "BS", "DIN", "JOSCAR", "Cyber Essentials", "SC21", "Fit4Nuclear", "OHSAS", "ASME",
            "API", "DNV", "Lloyd", "TUV", "TÜV",
        ];
        let filtered: Vec<Value> = certs
            .into_iter()
            .filter(|item| {
                if let Some(s) = item.as_str() {
                    valid_prefixes.iter().any(|p| s.contains(p))
                } else {
                    false
                }
            })
            .collect();
        enriched["certifications"] = json!(filtered);
    }

    // Validate security_clearances — keep only known patterns
    if let Some(clearances) = enriched
        .get("security_clearances")
        .and_then(|v| v.as_array())
        .cloned()
    {
        let valid_clearances = [
            "SC",
            "DV",
            "CTC",
            "BPSS",
            "NATO",
            "ITAR",
            "EAR",
            "Cyber Essentials",
            "Cyber Essentials Plus",
            "List X",
            "List N",
        ];
        let filtered: Vec<Value> = clearances
            .into_iter()
            .filter(|item| {
                if let Some(s) = item.as_str() {
                    valid_clearances.iter().any(|vc| s.contains(vc))
                } else {
                    false
                }
            })
            .collect();
        enriched["security_clearances"] = json!(filtered);
    }

    // Validate contact_email
    if let Some(email) = enriched.get("contact_email").and_then(|v| v.as_str()) {
        if !email.contains('@') {
            enriched["contact_email"] = json!(null);
        }
    }

    // Reject if relevance_score < 20 (handle both numeric and string-typed scores)
    let relevance = enriched
        .get("relevance_score")
        .and_then(|v| {
            v.as_i64()
                .or_else(|| v.as_f64().map(|f| f as i64))
                .or_else(|| v.as_str().and_then(|s| s.parse::<i64>().ok()))
        })
        .unwrap_or(0);
    if relevance < 20 {
        return Some(format!("relevance_score {} < 20", relevance));
    }

    None
}
