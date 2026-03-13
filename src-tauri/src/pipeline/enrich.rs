use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::db::Database;

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
            &format!("Reset {} stuck 'enriching' companies back to discovered", stuck_count),
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
                "Starting enrichment drain loop (concurrency={}, batch=50) using model: {} (auto-approve: relevance>={}, quality>={})",
                concurrency, enrich_model, relevance_threshold, quality_threshold
            ),
        );
    }

    let started_at = chrono::Utc::now();

    super::emit_node(app, json!({
        "node_id": "enrich",
        "status": "running",
        "model": &enrich_model,
        "progress": { "current": 0, "total": null, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

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
                    let _ = db.log_activity(job_id, "enrich", "info", "Queue empty, waiting for research to discover more companies...");
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
                        "model": enrich_model,
                    }),
                );
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
                continue;
            } else {
                // Research is done (or was never running) — exit
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "enrich", "info", "Queue empty, research complete — enrichment finished");
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
                let ch_api_key = ch_api_key.clone();
                let oc_api_key = oc_api_key.clone();
                let enriched_count = Arc::clone(&enriched_count);
                let approved_count = Arc::clone(&approved_count);
                let error_count = Arc::clone(&error_count);

                async move {
                    if super::is_cancelled() {
                        return;
                    }

                    let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let website = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let snippet = company.get("raw_snippet").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let country = company.get("country").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("").to_string();

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(&job_id, "enrich", "info", &format!("Enriching: {}", name));
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
                            "model": enrich_model,
                        }),
                    );

                    // Fetch website content
                    let website_text = crate::services::scraper::fetch_website_text(&website).await.ok();

                    let enrich_prompt = format!(
                        r#"Analyze this manufacturing company for a B2B marketplace. Return JSON with these fields:
description (2-3 sentences, English), description_original (original language if not English, else null), snippet_english (English translation of snippet, null if already English), category ("Products"/"Services"), subcategory, capabilities (array), industries (array), materials (array of specific materials with grades/alloys, e.g. ["Aluminium 6061-T6", "Stainless Steel 316L", "Titanium Ti-6Al-4V", "ABS", "Carbon Fibre", "PA12 Nylon", "Brass CZ121", "Mild Steel S275"]), key_equipment (array with brand+model), production_capacity (string or null), certifications (array), company_size ("1-9"/"10-49"/"50-99"/"100-249"/"250-499"/"500+"), employee_count_exact (int or null), key_people (array of name+title, max 5), founded_year (int or null), contact_name, contact_email (extract from mailto: links, contact/about pages, footer — prefer sales@, info@, contact@ — if listed in CONTACT EMAILS FOUND above, USE IT), contact_title, address (full with postcode or null), products (array), lead_time (string or null), minimum_order (string or null), quality_systems (string or null), export_controls (string or null), security_clearances (array), relevance_score (0-100, 80+=clearly manufacturing), enrichment_quality (0-100).

CRITICAL: Return null if no evidence. Do NOT guess. All text in English.

Company: {}
Website: {}
Data:
{}

Snippet: {}

Return ONLY valid JSON. /no_think"#,
                        name, website,
                        website_text.as_deref().unwrap_or(""),
                        snippet
                    );

                    let response = match crate::services::ollama::generate(
                        &ollama_url,
                        &enrich_model,
                        &enrich_prompt,
                        false,
                    )
                    .await
                    {
                        Ok(r) => r,
                        Err(e) => {
                            let error_msg = format!("Ollama request failed: {}", e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "enrich",
                                "error",
                                &format!("Enrichment failed for {}: {}", name, error_msg),
                            );
                            let _ = db.set_company_error(&id, &error_msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    let mut enriched: Value = match serde_json::from_str(&response) {
                        Ok(v) => v,
                        Err(e) => {
                            let truncated: String = response.chars().take(300).collect();
                            let error_msg = format!("JSON parse error: {} (len={}). Response start: {}", e, response.len(), truncated);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "enrich",
                                "error",
                                &format!("Parse failed for {}: {}", name, error_msg),
                            );
                            let _ = db.set_company_error(&id, &error_msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // Score validation & hallucination guard
                    if let Some(rejected) = validate_enrichment(&mut enriched) {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "enrich",
                            "info",
                            &format!("Rejected {} — {}", name, rejected),
                        );
                        let _ = db.set_company_error(&id, &format!("Validation rejected: {}", rejected));
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    }

                    // Build attributes_json matching ForgeOS marketplace_listings.attributes
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
                        // New v2 fields
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

                        match crate::services::companies_house::enrich_company(&ch_api_key, &name).await {
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
                                &format!("Looking up {} on OpenCorporates ({})...", name, country),
                            );
                        }

                        match crate::services::opencorporates::enrich_company(&oc_api_key, &name, &country).await {
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

                    // Extract address: prefer LLM-extracted address, fall back to registry address
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

                    // Geocode all companies
                    let mut geo_lat: Option<f64> = None;
                    let mut geo_lng: Option<f64> = None;
                    if country == "GB" {
                        // UK: use postcodes.io (fast, no rate limit issues)
                        if let Some(postcode) = crate::services::postcodes::extract_uk_postcode(&address) {
                            match crate::services::postcodes::geocode_postcode(&postcode).await {
                                Ok((lat, lng)) => {
                                    geo_lat = Some(lat);
                                    geo_lng = Some(lng);
                                }
                                Err(_) => {} // fall through to city fallback
                            }
                        }
                        if geo_lat.is_none() && !city.is_empty() {
                            if let Ok((lat, lng)) = crate::services::postcodes::geocode_place(&city).await {
                                geo_lat = Some(lat);
                                geo_lng = Some(lng);
                            }
                        }
                    } else if !country.is_empty() {
                        // Non-UK: use Nominatim (1.1s rate limit built in)
                        if !address.is_empty() {
                            if let Ok((lat, lng)) = crate::services::nominatim::geocode_address(&address).await {
                                geo_lat = Some(lat);
                                geo_lng = Some(lng);
                            }
                        }
                        if geo_lat.is_none() && !city.is_empty() {
                            if let Ok((lat, lng)) = crate::services::nominatim::geocode_city_country(&city, &country).await {
                                geo_lat = Some(lat);
                                geo_lng = Some(lng);
                            }
                        }
                    }

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
                        enriched_with_attrs["specialties"] = enriched
                            .get("capabilities")
                            .cloned()
                            .unwrap_or(json!([]));
                    }

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        match db.update_company_enrichment(&id, &enriched_with_attrs) {
                            Ok(_) => {
                                enriched_count.fetch_add(1, Ordering::Relaxed);
                                // Save geocode data if available
                                if let (Some(lat), Some(lng)) = (geo_lat, geo_lng) {
                                    let _ = db.update_company_geocode(&id, lat, lng);
                                }
                                // Auto-approve if scores meet thresholds
                                let rel = enriched_with_attrs
                                    .get("relevance_score")
                                    .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)).or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                    .unwrap_or(0);
                                let qual = enriched_with_attrs
                                    .get("enrichment_quality")
                                    .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)).or_else(|| v.as_str().and_then(|s| s.parse().ok())))
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
                                    &format!("Failed to save enrichment for {}: {}", name, error_msg),
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
                            "model": enrich_model,
                        }),
                    );

                    if cur_enriched % 5 == 0 || cur_enriched == 1 {
                        let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                        let rate = if elapsed > 0 { cur_enriched as f64 / elapsed as f64 * 3600.0 } else { 0.0 };
                        super::emit_node(&app, json!({
                            "node_id": "enrich",
                            "status": "running",
                            "model": &enrich_model,
                            "progress": { "current": cur_enriched, "total": null, "rate": rate, "current_item": &name },
                            "concurrency": concurrency,
                            "started_at": started_at.to_rfc3339(),
                            "elapsed_secs": elapsed
                        }));
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
    super::emit_node(app, json!({
        "node_id": "enrich",
        "status": "completed",
        "model": &enrich_model,
        "progress": { "current": final_enriched, "total": final_enriched, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    Ok(json!({
        "companies_enriched": final_enriched,
        "auto_approved": final_approved,
        "errors": final_errors,
        "no_website_filtered": final_no_website,
    }))
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
        enriched["enrichment_quality"] = json!(
            enriched.get("enrichment_quality").and_then(|v| v.as_i64()).unwrap_or(0).min(20)
        );
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
        enriched["enrichment_quality"] = json!(
            enriched.get("enrichment_quality").and_then(|v| v.as_i64()).unwrap_or(0).min(30)
        );
    }

    // Filter out generic equipment entries (no brand name)
    if let Some(equipment) = enriched.get("key_equipment").and_then(|v| v.as_array()).cloned() {
        let generic_terms = ["CNC machine", "lathe", "milling machine", "press", "saw"];
        let filtered: Vec<Value> = equipment
            .into_iter()
            .filter(|item| {
                if let Some(s) = item.as_str() {
                    // Keep if it has a digit (model number) or is longer than generic
                    !generic_terms.iter().any(|g| s.eq_ignore_ascii_case(g))
                } else {
                    true
                }
            })
            .collect();
        enriched["key_equipment"] = json!(filtered);
    }

    // Validate certification formats — keep only known patterns
    if let Some(certs) = enriched.get("certifications").and_then(|v| v.as_array()).cloned() {
        let valid_prefixes = [
            "ISO", "AS9100", "AS/EN", "IATF", "NADCAP", "CE", "UL", "CSA", "ATEX", "PED",
            "EN", "BS", "DIN", "JOSCAR", "Cyber Essentials", "SC21", "Fit4Nuclear",
            "OHSAS", "ASME", "API", "DNV", "Lloyd", "TUV", "TÜV",
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
    if let Some(clearances) = enriched.get("security_clearances").and_then(|v| v.as_array()).cloned() {
        let valid_clearances = [
            "SC", "DV", "CTC", "BPSS", "NATO", "ITAR", "EAR",
            "Cyber Essentials", "Cyber Essentials Plus",
            "List X", "List N",
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
