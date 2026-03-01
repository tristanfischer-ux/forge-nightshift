use anyhow::Result;
use serde_json::{json, Value};
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
        db.get_companies(Some("discovered"), 100, 0)?
    };

    let total = companies.len();
    let mut enriched_count = 0;
    let mut error_count = 0;

    for company in &companies {
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

        // LLM enrichment with richer prompt
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
- key_equipment: array of specific machinery/technology (e.g., ["DMG Mori DMU 50", "Trumpf TruLaser"])
- certifications: array (e.g., ["ISO 9001", "AS9100", "ISO 14001", "JOSCAR", "Cyber Essentials"])
- company_size: estimated size ("1-9", "10-49", "50-99", "100-249", "250-499", "500+")
- founded_year: year established if mentioned (integer or null)
- contact_name: best contact person name if found
- contact_email: contact email if found
- contact_title: contact person's title if found
- relevance_score: 0-100 how relevant for a manufacturing marketplace (be strict: 80+ = clearly manufacturing)
- enrichment_quality: 0-100 confidence in this data (be honest: only high if real data extracted)

Company: {}
Website: {}
Known info: {}

Return ONLY valid JSON. Do not include any thinking or explanation."#,
            name, website, snippet
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

        let enriched: Value = match serde_json::from_str(&response) {
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
            "founded_year": enriched.get("founded_year"),
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
                    // Merge CH fields into attributes
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
                "enriched": enriched_count,
                "total": total,
            }),
        );
    }

    Ok(json!({
        "companies_enriched": enriched_count,
        "errors": error_count,
    }))
}
