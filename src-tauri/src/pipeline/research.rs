use anyhow::Result;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let brave_key = config
        .get("brave_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if brave_key.is_empty() {
        anyhow::bail!("Brave API key not configured");
    }

    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434");

    let research_model = config
        .get("research_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3:8b");

    let countries_str = config
        .get("target_countries")
        .and_then(|v| v.as_str())
        .unwrap_or("[\"DE\"]");

    let countries: Vec<String> =
        serde_json::from_str(countries_str).unwrap_or_else(|_| vec!["DE".to_string()]);

    let specialties = vec![
        "CNC machining",
        "precision engineering",
        "metal fabrication",
        "injection molding",
        "sheet metal",
    ];

    let mut total_discovered = 0;
    let mut total_queries = 0;

    for country in &countries {
        if super::is_cancelled() {
            break;
        }

        let queries = crate::services::brave::generate_queries(country, &specialties);

        for query in queries {
            if super::is_cancelled() {
                break;
            }

            // Skip already-executed queries
            {
                let db: tauri::State<'_, Database> = app.state();
                if db.search_already_done(&query).unwrap_or(false) {
                    continue;
                }
            }

            {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "research", "info", &format!("Searching: {}", query));
            }

            let results =
                match crate::services::brave::search(brave_key, &query, country, 10).await {
                    Ok(r) => r,
                    Err(e) => {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            job_id,
                            "research",
                            "warn",
                            &format!("Search failed: {}", e),
                        );
                        continue;
                    }
                };

            {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.record_search(&query, country, results.len() as i64);
            }
            total_queries += 1;

            for result in &results {
                if super::is_cancelled() {
                    break;
                }

                let parse_prompt = format!(
                    r#"Extract company information from this search result. Return JSON with these fields:
- name: company name (required)
- website_url: company website URL
- domain: just the domain (e.g. "example.com")
- country: "{}"
- city: city if mentioned
- description: brief description of what they do
- category: one of "Products" or "Services"
- subcategory: manufacturing specialty

Search result:
Title: {}
URL: {}
Description: {}

If this is NOT a manufacturing/engineering company, return {{"skip": true}}.
Return ONLY valid JSON."#,
                    country, result.title, result.url, result.description
                );

                let llm_response = match crate::services::ollama::generate(
                    ollama_url,
                    research_model,
                    &parse_prompt,
                    true,
                )
                .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            job_id,
                            "research",
                            "warn",
                            &format!("LLM parse failed: {}", e),
                        );
                        continue;
                    }
                };

                let parsed: Value = match serde_json::from_str(&llm_response) {
                    Ok(v) => v,
                    Err(_) => continue,
                };

                if parsed
                    .get("skip")
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
                {
                    continue;
                }

                let name = parsed.get("name").and_then(|v| v.as_str()).unwrap_or("");
                if name.is_empty() {
                    continue;
                }

                let domain = parsed
                    .get("domain")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Dedup by domain
                if !domain.is_empty() {
                    let db: tauri::State<'_, Database> = app.state();
                    if db.domain_exists(domain).unwrap_or(true) {
                        continue;
                    }
                }

                let mut company = parsed.clone();
                company["source"] = json!("brave");
                company["source_url"] = json!(result.url);
                company["source_query"] = json!(query);
                company["raw_snippet"] = json!(result.description);

                let db: tauri::State<'_, Database> = app.state();
                match db.insert_company(&company) {
                    Ok(_) => {
                        total_discovered += 1;
                        let _ = app.emit(
                            "pipeline:progress",
                            json!({
                                "stage": "research",
                                "discovered": total_discovered,
                            }),
                        );
                    }
                    Err(e) => {
                        let _ = db.log_activity(
                            job_id,
                            "research",
                            "warn",
                            &format!("Failed to store company {}: {}", name, e),
                        );
                    }
                }
            }

            // Rate limit between searches
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    }

    Ok(json!({
        "queries_run": total_queries,
        "companies_discovered": total_discovered,
    }))
}
