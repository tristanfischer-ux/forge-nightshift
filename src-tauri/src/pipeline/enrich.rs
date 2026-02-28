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

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "enrich", "info", &format!("Enriching: {}", name));
            let _ = db.update_company_status(id, "enriching");
        }

        let enrich_prompt = format!(
            r#"You are analyzing a manufacturing/engineering company for a B2B marketplace.
Based on the information below, provide a detailed analysis. Return JSON with these fields:

- description: 2-3 sentence description of the company
- category: "Products" or "Services"
- subcategory: specific type (e.g., "CNC Machining", "Metal Fabrication")
- specialties: array of capabilities (e.g., ["5-axis CNC", "aluminum machining"])
- certifications: array of certifications (e.g., ["ISO 9001", "AS9100"])
- company_size: estimated size ("1-10", "11-50", "51-200", "201-500", "500+")
- industries: array of industries served (e.g., ["automotive", "aerospace"])
- contact_name: best contact person name if found
- contact_email: contact email if found
- contact_title: contact person's title if found
- relevance_score: 0-100 how relevant for a manufacturing marketplace
- enrichment_quality: 0-100 confidence in this data

Company: {}
Website: {}
Known info: {}

Return ONLY valid JSON."#,
            name, website, snippet
        );

        let response = match crate::services::ollama::generate(
            ollama_url,
            enrich_model,
            &enrich_prompt,
            true,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "enrich",
                    "error",
                    &format!("Enrichment failed for {}: {}", name, e),
                );
                let _ = db.update_company_status(id, "error");
                error_count += 1;
                continue;
            }
        };

        let enriched: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(_) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.update_company_status(id, "error");
                error_count += 1;
                continue;
            }
        };

        let country = company
            .get("country")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("");

        let attributes = json!({
            "website_url": website,
            "country": country,
            "city": city,
            "specialties": enriched.get("specialties").unwrap_or(&json!([])),
            "certifications": enriched.get("certifications").unwrap_or(&json!([])),
            "employees": enriched.get("company_size").and_then(|v| v.as_str()).unwrap_or(""),
            "industries": enriched.get("industries").unwrap_or(&json!([])),
            "nightshift_score": enriched.get("relevance_score").and_then(|v| v.as_i64()).unwrap_or(0),
        });

        let mut enriched_with_attrs = enriched.clone();
        enriched_with_attrs["attributes_json"] = attributes;

        {
            let db: tauri::State<'_, Database> = app.state();
            match db.update_company_enrichment(id, &enriched_with_attrs) {
                Ok(_) => enriched_count += 1,
                Err(e) => {
                    let _ = db.log_activity(
                        job_id,
                        "enrich",
                        "error",
                        &format!("Failed to save enrichment for {}: {}", name, e),
                    );
                    let _ = db.update_company_status(id, "error");
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
