use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use tauri::{Emitter, Manager};

use crate::db::Database;

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434");

    let enrich_model = config
        .get("enrich_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3:30b-a3b");

    let ch_api_key = config
        .get("companies_house_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies(Some("discovered"), 500, 0)?
    };

    let total = companies.len();
    let mut enriched_count = 0;
    let mut error_count = 0;

    // Parallel website fetching (5 concurrent) before sequential LLM enrichment
    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "enrich",
            "info",
            &format!("Pre-fetching websites for {} companies (5 concurrent)...", total),
        );
    }

    let website_texts: HashMap<String, String> = {
        let fetch_tasks: Vec<(String, String)> = companies
            .iter()
            .filter_map(|c| {
                let id = c.get("id").and_then(|v| v.as_str())?.to_string();
                let url = c.get("website_url").and_then(|v| v.as_str())?.to_string();
                if url.is_empty() { None } else { Some((id, url)) }
            })
            .collect();

        let results: Vec<(String, Option<String>)> = stream::iter(fetch_tasks)
            .map(|(id, url)| async move {
                let text = crate::services::scraper::fetch_website_text(&url).await.ok();
                (id, text)
            })
            .buffer_unordered(5)
            .collect()
            .await;

        results
            .into_iter()
            .filter_map(|(id, text)| text.map(|t| (id, t)))
            .collect()
    };

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "enrich",
            "info",
            &format!("Fetched website content for {}/{} companies", website_texts.len(), total),
        );
    }

    for (idx, company) in companies.iter().enumerate() {
        if super::is_cancelled() {
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let website = company
            .get("website_url")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let snippet = company
            .get("raw_snippet")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let country = company
            .get("country")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("");

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "enrich", "info", &format!("Enriching: {}", name));
            let _ = db.update_company_status(id, "enriching");
        }

        let _ = app.emit(
            "pipeline:progress",
            json!({
                "stage": "enrich",
                "phase": "start",
                "current_company": name,
                "current_index": idx,
                "total": total,
                "enriched": enriched_count,
                "errors": error_count,
            }),
        );

        // Build data source section — prefer website content over snippet
        let website_text = website_texts.get(id);
        let data_source = if let Some(text) = website_text {
            format!(
                "Website content (primary source):\n{}\n\nSearch snippet (secondary): {}",
                text, snippet
            )
        } else {
            format!("Known info: {}", snippet)
        };

        let enrich_prompt = format!(
            r#"You are analyzing a manufacturing/engineering company for a B2B marketplace.
Based on the information below, provide a detailed analysis. Return JSON with ALL of these fields:

IMPORTANT: All text fields (description, subcategory, capabilities, etc.) MUST be in English. If the source material is in another language, translate it to English.

- description: 2-3 sentence description IN ENGLISH of the company and what they manufacture/provide
- description_original: if the source text was NOT in English, put the original-language description here; otherwise set to null
- snippet_english: English translation of the raw snippet/known info below; null if already in English
- category: "Products" or "Services"
- subcategory: specific type (e.g., "CNC Machining", "Sheet Metal Fabrication", "Electronics")
- capabilities: array of specific services/processes (e.g., ["5-axis CNC milling", "wire EDM", "surface grinding"])
- industries: array of sectors served (e.g., ["Automotive", "Aerospace", "Defence", "Medical", "Oil & Gas"])
- materials: array of materials worked with (e.g., ["aluminium", "titanium", "stainless steel", "Inconel"])
- key_equipment: array of SPECIFIC machinery with brand and model where possible (e.g., ["DMG Mori DMU 50 5-axis", "Trumpf TruLaser 3030", "Zeiss Contura CMM", "FANUC R-2000iC robot"]). Include axis count, tonnage, or power rating where relevant. Include metrology (CMMs), robots, welding equipment. NEVER use generic terms like "CNC machine" without brand context.
- production_capacity: facility/volume info string (e.g., "30 CNC machines, 5,000 sqm facility, 24/7 operation") or null if unknown
- certifications: array (e.g., ["ISO 9001", "AS9100", "ISO 14001", "JOSCAR", "Cyber Essentials"])
- company_size: estimated size ("1-9", "10-49", "50-99", "100-249", "250-499", "500+")
- employee_count_exact: exact headcount if stated on site (integer or null)
- key_people: array of {{"name": "...", "title": "..."}} — directors, founders, MD, key leadership (max 5). Only include people whose names are explicitly mentioned on the website. Return empty array if none found.
- founded_year: year established if mentioned (integer or null)
- contact_name: best contact person name if found
- contact_email: contact email if found
- contact_title: contact person's title if found
- relevance_score: 0-100 how relevant for a manufacturing marketplace (be strict: 80+ = clearly manufacturing)
- enrichment_quality: 0-100 confidence in this data (be honest: only high if real data extracted)

Company: {}
Website: {}
{}

Return ONLY valid JSON. Do not include any thinking or explanation."#,
            name, website, data_source
        );

        let response = match crate::services::ollama::generate(
            ollama_url,
            enrich_model,
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
                    job_id,
                    "enrich",
                    "error",
                    &format!("Enrichment failed for {}: {}", name, error_msg),
                );
                let _ = db.set_company_error(id, &error_msg);
                error_count += 1;
                continue;
            }
        };

        let mut enriched: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(e) => {
                let truncated: String = response.chars().take(200).collect();
                let error_msg = format!("JSON parse error: {}. Response start: {}", e, truncated);
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "enrich",
                    "error",
                    &format!("Parse failed for {}: {}", name, error_msg),
                );
                let _ = db.set_company_error(id, &error_msg);
                error_count += 1;
                continue;
            }
        };

        // Score validation & hallucination guard
        if let Some(rejected) = validate_enrichment(&mut enriched) {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "enrich",
                "info",
                &format!("Rejected {} — {}", name, rejected),
            );
            let _ = db.set_company_error(id, &format!("Validation rejected: {}", rejected));
            error_count += 1;
            continue;
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
            "nightshift_score": enriched.get("relevance_score").and_then(|v| v.as_i64()).unwrap_or(0),
        });

        // Companies House enrichment for UK companies
        if (country == "GB" || country == "UK") && !ch_api_key.is_empty() {
            {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "enrich",
                    "info",
                    &format!("Looking up {} on Companies House...", name),
                );
            }

            match crate::services::companies_house::enrich_company(ch_api_key, name).await {
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
                    let _ = db.log_activity(
                        job_id,
                        "enrich",
                        "info",
                        &format!("CH match for {}: #{}", name, ch_number),
                    );
                }
                Ok(None) => {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "enrich",
                        "info",
                        &format!("No CH match found for {}", name),
                    );
                }
                Err(e) => {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "enrich",
                        "warn",
                        &format!("CH lookup failed for {}: {}", name, e),
                    );
                }
            }
        }

        let mut enriched_with_attrs = enriched.clone();
        enriched_with_attrs["attributes_json"] = attributes;

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
            match db.update_company_enrichment(id, &enriched_with_attrs) {
                Ok(_) => enriched_count += 1,
                Err(e) => {
                    let error_msg = format!("DB save failed: {}", e);
                    let _ = db.log_activity(
                        job_id,
                        "enrich",
                        "error",
                        &format!("Failed to save enrichment for {}: {}", name, error_msg),
                    );
                    let _ = db.set_company_error(id, &error_msg);
                    error_count += 1;
                    continue;
                }
            }
        }

        let _ = app.emit(
            "pipeline:progress",
            json!({
                "stage": "enrich",
                "phase": "done",
                "current_company": name,
                "current_index": idx,
                "enriched": enriched_count,
                "errors": error_count,
                "total": total,
            }),
        );
    }

    Ok(json!({
        "companies_enriched": enriched_count,
        "errors": error_count,
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

    // Validate contact_email
    if let Some(email) = enriched.get("contact_email").and_then(|v| v.as_str()) {
        if !email.contains('@') {
            enriched["contact_email"] = json!(null);
        }
    }

    // Reject if relevance_score < 20
    let relevance = enriched
        .get("relevance_score")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    if relevance < 20 {
        return Some(format!("relevance_score {} < 20", relevance));
    }

    None
}
