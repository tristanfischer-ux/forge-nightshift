use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashSet;
use tauri::{Emitter, Manager};

use crate::db::Database;
use crate::services::brave::CATEGORIES;

/// Run the research stage using category rotation and Supabase dedup.
/// Returns discovered company IDs for immediate enrichment.
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
        .unwrap_or("qwen3.5:9b");

    let countries_str = config
        .get("target_countries")
        .and_then(|v| v.as_str())
        .unwrap_or("[\"DE\"]");

    let countries: Vec<String> =
        serde_json::from_str(countries_str).unwrap_or_else(|_| vec!["DE".to_string()]);

    let categories_per_run: usize = config
        .get("categories_per_run")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(8);

    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "research",
            "info",
            &format!(
                "Starting research using Brave Search API + LLM parser (model: {})",
                research_model
            ),
        );
    }

    // Step 1: Fetch known domains + names from Supabase (one-time)
    let (supabase_domains, supabase_names) = if !supabase_url.is_empty() && !supabase_key.is_empty() {
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "research", "info", "Fetching known domains & names from ForgeOS...");
        }
        match crate::services::supabase::fetch_all_known_domains_and_names(supabase_url, supabase_key).await {
            Ok((domains, names)) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "research",
                    "info",
                    &format!("Loaded {} known domains + {} names from ForgeOS", domains.len(), names.len()),
                );
                (domains, names)
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "research",
                    "warn",
                    &format!("Could not fetch ForgeOS data: {}. Continuing without dedup.", e),
                );
                (HashSet::new(), HashSet::new())
            }
        }
    } else {
        (HashSet::new(), HashSet::new())
    };

    let mut total_discovered = 0;
    let mut total_queries = 0;

    // Step 2: For each country, pick least-covered categories
    for country in &countries {
        if super::is_cancelled() {
            break;
        }

        let selected_categories = pick_categories(app, country, categories_per_run);

        {
            let db: tauri::State<'_, Database> = app.state();
            let names: Vec<&str> = selected_categories.iter().map(|c| c.name).collect();
            let _ = db.log_activity(
                job_id,
                "research",
                "info",
                &format!("Country {}: searching {} categories: {}", country, names.len(), names.join(", ")),
            );
        }

        // Step 3: For each selected category, run queries
        for category in &selected_categories {
            if super::is_cancelled() {
                break;
            }

            let queries = crate::services::brave::generate_queries_for_category(country, category);
            let mut batch_discovered = 0;

            for (query, _cat_id) in &queries {
                if super::is_cancelled() {
                    break;
                }

                // Skip already-executed queries
                {
                    let db: tauri::State<'_, Database> = app.state();
                    if db.search_already_done(query).unwrap_or(false) {
                        continue;
                    }
                }

                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "research",
                        "info",
                        &format!("[{}] Searching: {}", category.name, query),
                    );
                }

                let results =
                    match crate::services::brave::search(brave_key, query, country, 20, 0).await {
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

                let page1_full = results.len() >= 20;

                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.record_search(query, country, results.len() as i64);
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
- subcategory: "{}"

Search result:
Title: {}
URL: {}
Description: {}

If this is NOT a manufacturing/engineering company, return {{"skip": true}}.
Return ONLY valid JSON."#,
                        country, category.name, result.title, result.url, result.description
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

                    // Dedup: check Supabase domains first (www-stripped), then local DB domain, then name
                    if !domain.is_empty() {
                        let normalized_domain = domain.to_lowercase();
                        let stripped_domain = normalized_domain.strip_prefix("www.").unwrap_or(&normalized_domain);
                        if supabase_domains.contains(stripped_domain) {
                            continue;
                        }
                        let db: tauri::State<'_, Database> = app.state();
                        match db.domain_exists(domain) {
                            Ok(true) => continue,
                            Ok(false) => {} // new domain, proceed
                            Err(e) => {
                                let _ = db.log_activity(
                                    job_id,
                                    "research",
                                    "warn",
                                    &format!("domain_exists check failed for {}: {} — keeping company", domain, e),
                                );
                            }
                        }
                    }

                    // Name-based dedup: check Supabase names, then local DB
                    if !name.is_empty() {
                        let name_lower = name.to_lowercase().trim().to_string();
                        let mut n = name_lower.clone();
                        for suffix in &[
                            " ltd", " limited", " gmbh", " sas", " bv", " ag", " sa", " srl", " nv", " inc",
                            " llc", " co.", " corp", " corporation", " plc", " s.r.l.", " s.a.", " e.k.",
                            " ohg", " kg", " ug",
                        ] {
                            if n.ends_with(suffix) {
                                n = n[..n.len() - suffix.len()].to_string();
                            }
                        }
                        let normalized_name = n.trim().to_string();
                        if supabase_names.contains(&normalized_name) {
                            continue;
                        }
                        let db: tauri::State<'_, Database> = app.state();
                        if db.name_exists_normalized(name).unwrap_or(false) {
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
                            batch_discovered += 1;
                            let _ = app.emit(
                                "pipeline:progress",
                                json!({
                                    "stage": "research",
                                    "discovered": total_discovered,
                                    "category": category.name,
                                    "country": country,
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

                // Brave pagination: if page 1 returned a full page, fetch page 2
                if page1_full && !super::is_cancelled() {
                    let page2_key = format!("{} [page 2]", query);
                    let already_done = {
                        let db: tauri::State<'_, Database> = app.state();
                        db.search_already_done(&page2_key).unwrap_or(false)
                    };
                    if !already_done {
                        if let Ok(page2_results) = crate::services::brave::search(brave_key, query, country, 20, 1).await {
                            {
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.record_search(&page2_key, country, page2_results.len() as i64);
                            }
                            total_queries += 1;

                            for result in &page2_results {
                                if super::is_cancelled() { break; }

                                let parse_prompt = format!(
                                    r#"Extract company information from this search result. Return JSON with these fields:
- name: company name (required)
- website_url: company website URL
- domain: just the domain (e.g. "example.com")
- country: "{}"
- city: city if mentioned
- description: brief description of what they do
- category: one of "Products" or "Services"
- subcategory: "{}"

Search result:
Title: {}
URL: {}
Description: {}

If this is NOT a manufacturing/engineering company, return {{"skip": true}}.
Return ONLY valid JSON."#,
                                    country, category.name, result.title, result.url, result.description
                                );

                                let llm_response = match crate::services::ollama::generate(
                                    ollama_url, research_model, &parse_prompt, true,
                                ).await {
                                    Ok(r) => r,
                                    Err(_) => continue,
                                };

                                let parsed: Value = match serde_json::from_str(&llm_response) {
                                    Ok(v) => v,
                                    Err(_) => continue,
                                };

                                if parsed.get("skip").and_then(|v| v.as_bool()).unwrap_or(false) { continue; }

                                let name = parsed.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                if name.is_empty() { continue; }

                                let domain = parsed.get("domain").and_then(|v| v.as_str()).unwrap_or("");

                                if !domain.is_empty() {
                                    let normalized_domain = domain.to_lowercase();
                                    let stripped_domain = normalized_domain.strip_prefix("www.").unwrap_or(&normalized_domain);
                                    if supabase_domains.contains(stripped_domain) { continue; }
                                    let db: tauri::State<'_, Database> = app.state();
                                    if db.domain_exists(domain).unwrap_or(false) { continue; }
                                }

                                if !name.is_empty() {
                                    if supabase_names.contains(&name.to_lowercase().trim().to_string()) { continue; }
                                    let db: tauri::State<'_, Database> = app.state();
                                    if db.name_exists_normalized(name).unwrap_or(false) { continue; }
                                }

                                let mut company = parsed.clone();
                                company["source"] = json!("brave");
                                company["source_url"] = json!(result.url);
                                company["source_query"] = json!(page2_key);
                                company["raw_snippet"] = json!(result.description);

                                let db: tauri::State<'_, Database> = app.state();
                                if let Ok(_) = db.insert_company(&company) {
                                    total_discovered += 1;
                                    batch_discovered += 1;
                                    let _ = app.emit(
                                        "pipeline:progress",
                                        json!({
                                            "stage": "research",
                                            "discovered": total_discovered,
                                            "category": category.name,
                                            "country": country,
                                        }),
                                    );
                                }
                            }

                            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                        }
                    }
                }
            }

            // Update category coverage
            {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.increment_category_coverage(category.id, country, batch_discovered);
                let _ = db.log_activity(
                    job_id,
                    "research",
                    "info",
                    &format!("[{}] {} — found {} new companies", country, category.name, batch_discovered),
                );
            }
        }
    }

    Ok(json!({
        "queries_run": total_queries,
        "companies_discovered": total_discovered,
    }))
}

/// Pick the least-covered categories for a country.
/// Returns `count` categories sorted by fewest companies found.
fn pick_categories(
    app: &tauri::AppHandle,
    country: &str,
    count: usize,
) -> Vec<&'static crate::services::brave::SearchCategory> {
    let db: tauri::State<'_, Database> = app.state();
    let coverage = db.get_category_coverage(country).unwrap_or_default();

    // Build a map of category_id -> (companies_found, searches_run)
    let coverage_map: std::collections::HashMap<String, (i64, i64)> = coverage
        .into_iter()
        .map(|(cat_id, searches, companies)| (cat_id, (companies, searches)))
        .collect();

    // Sort by (companies_found, searches_run) tuple — guarantees rotation
    // when all have 0 found, those with fewer searches get picked first
    let mut categories: Vec<&crate::services::brave::SearchCategory> = CATEGORIES.iter().collect();
    categories.sort_by_key(|cat| coverage_map.get(cat.id).copied().unwrap_or((0, 0)));
    categories.truncate(count);
    categories
}
