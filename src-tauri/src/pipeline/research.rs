use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashSet;
use tauri::{Emitter, Manager};

use crate::db::Database;
use crate::services::brave::{CATEGORIES, DynamicSearchCategory, country_names};

/// Run the research stage using category rotation and Supabase dedup.
/// Returns discovered company IDs for immediate enrichment.
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    // Flag that research is active so that the parallel enrich stage will wait
    // for new discoveries instead of exiting when its queue is briefly empty.
    // Resets automatically on drop (including panic or early return).
    let _research_guard = super::research_active_guard();

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

    let llm_backend = config
        .get("llm_backend")
        .and_then(|v| v.as_str())
        .unwrap_or("deepseek");

    // Display the actual model being used, not the Ollama model name
    let display_model = match llm_backend {
        "deepseek" => "deepseek-chat",
        "haiku" => "claude-haiku-4.5",
        _ => research_model,
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

    // Load active search profile from DB
    let (active_profile_id, active_domain, dynamic_categories) = {
        let db: tauri::State<'_, Database> = app.state();
        let profile_id = db.get_active_profile_id();
        match db.get_search_profile(&profile_id) {
            Ok(Some(profile)) => {
                let domain = profile.get("domain").and_then(|v| v.as_str()).unwrap_or("manufacturing").to_string();
                let cats_str = profile.get("categories_json").and_then(|v| v.as_str()).unwrap_or("[]");
                let cats: Vec<DynamicSearchCategory> = serde_json::from_str(cats_str).unwrap_or_default();
                (profile_id, domain, cats)
            }
            _ => {
                // Fallback: use hardcoded CATEGORIES
                let cats: Vec<DynamicSearchCategory> = CATEGORIES.iter().map(|c| DynamicSearchCategory {
                    id: c.id.to_string(),
                    name: c.name.to_string(),
                    keywords: c.keywords.iter().map(|k| k.to_string()).collect(),
                }).collect();
                (profile_id, "manufacturing".to_string(), cats)
            }
        }
    };

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "research",
            "info",
            &format!(
                "Starting research using Brave Search API + LLM parser (model: {}, profile: {}, domain: {})",
                research_model, active_profile_id, active_domain
            ),
        );
    }

    super::emit_node(app, json!({
        "node_id": "research",
        "status": "running",
        "model": display_model,
        "progress": { "current": 0, "total": null, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": chrono::Utc::now().to_rfc3339(),
        "elapsed_secs": null
    }));

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

    let batch_limit: i64 = config
        .get("pipeline_batch_size")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0); // 0 = no limit

    let mut total_discovered = 0;
    let mut total_queries = 0;

    // Step 1.5: Directory search phase (first pass — before individual search)
    let directory_search_enabled = config
        .get("directory_search_enabled")
        .and_then(|v| v.as_str())
        .unwrap_or("true")
        == "true";

    if directory_search_enabled {
        let dir_discovered = run_directory_search(
            app,
            job_id,
            brave_key,
            &countries,
            &dynamic_categories,
            &active_domain,
            &active_profile_id,
            &supabase_domains,
            &supabase_names,
            llm_backend,
            &deepseek_api_key,
            &anthropic_api_key,
            ollama_url,
            research_model,
        )
        .await;
        total_discovered += dir_discovered;
    }

    // Check batch limit after directory search
    if batch_limit > 0 && total_discovered >= batch_limit {
        log::info!("[Research] Reached batch limit of {} after directory search", batch_limit);
        super::emit_node(app, json!({
            "node_id": "research",
            "status": "completed",
            "model": display_model,
            "progress": { "current": total_discovered, "total": total_discovered, "rate": null, "current_item": null },
            "concurrency": 1,
            "started_at": null,
            "elapsed_secs": null
        }));
        return Ok(json!({
            "queries_run": total_queries,
            "companies_discovered": total_discovered,
            "batch_limited": true,
        }));
    }

    // Step 2: For each country, pick least-covered categories
    for country in &countries {
        if super::is_cancelled() {
            break;
        }

        let selected_categories = pick_dynamic_categories(app, country, categories_per_run, &dynamic_categories);

        {
            let db: tauri::State<'_, Database> = app.state();
            let names: Vec<&str> = selected_categories.iter().map(|c| c.name.as_str()).collect();
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

            let queries = crate::services::brave::generate_queries_for_dynamic_category(country, category, &active_domain);
            let mut batch_discovered = 0;
            let mut batch_results = 0u64;
            let mut skipped_llm_error = 0u64;
            let mut skipped_llm_skip = 0u64;
            let mut skipped_domain_supabase = 0u64;
            let mut skipped_domain_local = 0u64;
            let mut skipped_name_supabase = 0u64;
            let mut skipped_name_local = 0u64;
            let mut skipped_parse = 0u64;
            let mut skipped_no_name = 0u64;

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
                    batch_results += 1;

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

If this is NOT a {} company, return {{"skip": true}}.
Return ONLY valid JSON."#,
                        country, category.name, result.title, result.url, result.description, active_domain
                    );

                    let llm_response = if llm_backend == "haiku" {
                        match crate::services::anthropic::chat(
                            &anthropic_api_key,
                            None,
                            &parse_prompt,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                skipped_llm_error += 1;
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    job_id,
                                    "research",
                                    "warn",
                                    &format!("[Anthropic] LLM parse failed: {}", e),
                                );
                                continue;
                            }
                        }
                    } else if llm_backend == "deepseek" {
                        match crate::services::deepseek::chat(
                            &deepseek_api_key,
                            None,
                            &parse_prompt,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                skipped_llm_error += 1;
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    job_id,
                                    "research",
                                    "warn",
                                    &format!("[DeepSeek] LLM parse failed: {}", e),
                                );
                                continue;
                            }
                        }
                    } else {
                        match crate::services::ollama::generate(
                            ollama_url,
                            research_model,
                            &parse_prompt,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                skipped_llm_error += 1;
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(
                                    job_id,
                                    "research",
                                    "warn",
                                    &format!("LLM parse failed: {}", e),
                                );
                                continue;
                            }
                        }
                    };

                    let parsed: Value = match serde_json::from_str(&llm_response) {
                        Ok(v) => v,
                        Err(_) => { skipped_parse += 1; continue; }
                    };

                    if parsed
                        .get("skip")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false)
                    {
                        skipped_llm_skip += 1;
                        continue;
                    }

                    let name = parsed.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    if name.is_empty() {
                        skipped_no_name += 1;
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
                            skipped_domain_supabase += 1;
                            continue;
                        }
                        let db: tauri::State<'_, Database> = app.state();
                        match db.domain_exists(domain) {
                            Ok(true) => { skipped_domain_local += 1; continue; }
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
                            skipped_name_supabase += 1;
                            continue;
                        }
                        let db: tauri::State<'_, Database> = app.state();
                        if db.name_exists_normalized(name).unwrap_or(false) {
                            skipped_name_local += 1;
                            continue;
                        }
                    }

                    let mut company = parsed.clone();
                    company["source"] = json!("brave");
                    company["source_url"] = json!(result.url);
                    company["source_query"] = json!(query);
                    company["raw_snippet"] = json!(result.description);
                    company["search_profile_id"] = json!(&active_profile_id);

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
                            if total_discovered % 5 == 0 || total_discovered == 1 {
                                super::emit_node(app, json!({
                                    "node_id": "research",
                                    "status": "running",
                                    "model": display_model,
                                    "progress": { "current": total_discovered, "total": null, "rate": null, "current_item": category.name },
                                    "concurrency": 1,
                                    "started_at": null,
                                    "elapsed_secs": null
                                }));
                            }
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
                // Skip page 2 if batch limit already reached
                if page1_full && !super::is_cancelled() && !(batch_limit > 0 && total_discovered >= batch_limit) {
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
                                if batch_limit > 0 && total_discovered >= batch_limit { break; }
                                batch_results += 1;

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

If this is NOT a {} company, return {{"skip": true}}.
Return ONLY valid JSON."#,
                                    country, category.name, result.title, result.url, result.description, active_domain
                                );

                                let llm_response = match crate::services::ollama::generate(
                                    ollama_url, research_model, &parse_prompt, true,
                                ).await {
                                    Ok(r) => r,
                                    Err(_) => { skipped_llm_error += 1; continue; }
                                };

                                let parsed: Value = match serde_json::from_str(&llm_response) {
                                    Ok(v) => v,
                                    Err(_) => { skipped_parse += 1; continue; }
                                };

                                if parsed.get("skip").and_then(|v| v.as_bool()).unwrap_or(false) { skipped_llm_skip += 1; continue; }

                                let name = parsed.get("name").and_then(|v| v.as_str()).unwrap_or("");
                                if name.is_empty() { skipped_no_name += 1; continue; }

                                let domain = parsed.get("domain").and_then(|v| v.as_str()).unwrap_or("");

                                if !domain.is_empty() {
                                    let normalized_domain = domain.to_lowercase();
                                    let stripped_domain = normalized_domain.strip_prefix("www.").unwrap_or(&normalized_domain);
                                    if supabase_domains.contains(stripped_domain) { skipped_domain_supabase += 1; continue; }
                                    let db: tauri::State<'_, Database> = app.state();
                                    if db.domain_exists(domain).unwrap_or(false) { skipped_domain_local += 1; continue; }
                                }

                                if !name.is_empty() {
                                    if supabase_names.contains(&name.to_lowercase().trim().to_string()) { skipped_name_supabase += 1; continue; }
                                    let db: tauri::State<'_, Database> = app.state();
                                    if db.name_exists_normalized(name).unwrap_or(false) { skipped_name_local += 1; continue; }
                                }

                                let mut company = parsed.clone();
                                company["source"] = json!("brave");
                                company["source_url"] = json!(result.url);
                                company["source_query"] = json!(page2_key);
                                company["raw_snippet"] = json!(result.description);
                                company["search_profile_id"] = json!(&active_profile_id);

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
                                    if total_discovered % 5 == 0 || total_discovered == 1 {
                                        super::emit_node(app, json!({
                                            "node_id": "research",
                                            "status": "running",
                                            "model": display_model,
                                            "progress": { "current": total_discovered, "total": null, "rate": null, "current_item": category.name },
                                            "concurrency": 1,
                                            "started_at": null,
                                            "elapsed_secs": null
                                        }));
                                    }
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
                let _ = db.increment_category_coverage(&category.id, country, batch_discovered);
                let _ = db.log_activity(
                    job_id,
                    "research",
                    "info",
                    &format!(
                        "[{}] {} — {} results: {} new, {} skip, {} parse-err, {} no-name, {} domain-supabase, {} domain-local, {} name-supabase, {} name-local, {} llm-error{}",
                        country, category.name, batch_results, batch_discovered,
                        skipped_llm_skip, skipped_parse, skipped_no_name,
                        skipped_domain_supabase, skipped_domain_local,
                        skipped_name_supabase, skipped_name_local,
                        skipped_llm_error,
                        if batch_limit > 0 { format!(", batch {}/{}", total_discovered, batch_limit) } else { String::new() },
                    ),
                );
            }

            // Check batch limit after each category
            if batch_limit > 0 && total_discovered >= batch_limit {
                log::info!("[Research] Reached batch limit of {} during category search", batch_limit);
                break;
            }
        }

        // Check batch limit after each country
        if batch_limit > 0 && total_discovered >= batch_limit {
            break;
        }
    }

    super::emit_node(app, json!({
        "node_id": "research",
        "status": "completed",
        "model": display_model,
        "progress": { "current": total_discovered, "total": total_discovered, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": null,
        "elapsed_secs": null
    }));

    Ok(json!({
        "queries_run": total_queries,
        "companies_discovered": total_discovered,
    }))
}

/// Pick the least-covered dynamic categories for a country.
/// Returns `count` categories sorted by fewest companies found.
fn pick_dynamic_categories(
    app: &tauri::AppHandle,
    country: &str,
    count: usize,
    all_categories: &[DynamicSearchCategory],
) -> Vec<DynamicSearchCategory> {
    let db: tauri::State<'_, Database> = app.state();
    let coverage = db.get_category_coverage(country).unwrap_or_default();

    let coverage_map: std::collections::HashMap<String, (i64, i64)> = coverage
        .into_iter()
        .map(|(cat_id, searches, companies)| (cat_id, (companies, searches)))
        .collect();

    let mut categories: Vec<DynamicSearchCategory> = all_categories.to_vec();
    categories.sort_by_key(|cat| coverage_map.get(&cat.id).copied().unwrap_or((0, 0)));
    categories.truncate(count);
    categories
}

/// Domains that are search engines / aggregators — skip these as directory sources.
const SKIP_DOMAINS: &[&str] = &[
    "google.com", "bing.com", "yahoo.com", "duckduckgo.com", "baidu.com",
    "yandex.com", "ask.com", "aol.com", "wikipedia.org", "amazon.com",
    "ebay.com", "alibaba.com", "linkedin.com", "facebook.com", "twitter.com",
    "youtube.com", "reddit.com",
];

/// Patterns in page text that indicate a paywalled directory.
const PAYWALL_INDICATORS: &[&str] = &[
    "sign up to view", "register to see", "create an account to access",
    "subscribe to view", "login to view", "members only", "premium access",
    "unlock full list", "sign in to continue",
];

/// Generate directory-specific search queries for a category and country.
fn generate_directory_queries(
    category_name: &str,
    country: &str,
    domain: &str,
) -> Vec<String> {
    let names = country_names(country);
    if names.is_empty() {
        return vec![];
    }
    let country_name = names[0];

    let mut queries = vec![
        format!("{} directory {}", category_name, country_name),
        format!("{} companies list {}", category_name, country_name),
        format!("{} supplier directory {}", domain, country_name),
        format!("list of {} companies {}", category_name, country_name),
        format!("{} trade association members {}", category_name, country_name),
    ];

    // Add domain-specific directory keywords
    let extra = get_domain_directory_keywords(domain);
    for kw in extra.iter().take(2) {
        queries.push(format!("{} {}", kw, country_name));
    }

    // Add cleantech-specific directory queries (real directories and certification schemes)
    if domain == "cleantech" {
        let cleantech_queries = get_cleantech_directory_queries(country_name);
        queries.extend(cleantech_queries);
    }

    queries
}

/// Cleantech-specific directory search queries targeting real industry directories,
/// certification bodies, and membership organisations.
fn get_cleantech_directory_queries(country_name: &str) -> Vec<String> {
    vec![
        format!("cleantech directory {} companies", country_name),
        format!("renewable energy company directory {}", country_name),
        format!("clean energy companies list {}", country_name),
        format!("green business directory {}", country_name),
        format!("REA members directory {}", country_name),
        "Solar Energy UK member directory".to_string(),
        "Energy UK members".to_string(),
        "Clean Growth UK companies".to_string(),
        "Innovate UK clean energy funded companies".to_string(),
        "Carbon Trust certified companies".to_string(),
        "MCS certified installer directory".to_string(),
        "NAPIT registered installer directory".to_string(),
        "NICEIC registered contractor directory".to_string(),
        format!("heat pump installer directory {}", country_name),
        format!("EV charging installer directory {}", country_name),
    ]
}

/// Domain-aware directory search terms.
fn get_domain_directory_keywords(domain: &str) -> Vec<&'static str> {
    match domain {
        "manufacturing" => vec![
            "engineering directory", "manufacturing suppliers", "precision engineering companies",
            "made in", "industrial directory",
        ],
        "cleantech" => vec![
            "renewable energy directory", "cleantech companies", "green business directory",
            "sustainable companies", "clean energy firms",
        ],
        "biotech" => vec![
            "biotech directory", "life sciences companies", "pharmaceutical directory",
            "biotech firms list",
        ],
        _ => vec![
            "company directory", "business directory", "industry association members",
        ],
    }
}

/// Check if a URL is a search engine or aggregator that should be skipped.
fn is_skip_domain(url: &str) -> bool {
    let lower = url.to_lowercase();
    SKIP_DOMAINS.iter().any(|d| lower.contains(d))
}

/// Check if page content indicates a paywalled directory.
fn is_paywalled(text: &str) -> bool {
    let lower = text.to_lowercase();
    PAYWALL_INDICATORS.iter().any(|p| lower.contains(p))
}

/// Run the directory search phase: search for directories, scrape them, extract companies.
/// Returns the number of new companies discovered.
#[allow(clippy::too_many_arguments)]
async fn run_directory_search(
    app: &tauri::AppHandle,
    job_id: &str,
    brave_key: &str,
    countries: &[String],
    categories: &[DynamicSearchCategory],
    domain: &str,
    profile_id: &str,
    supabase_domains: &HashSet<String>,
    supabase_names: &HashSet<String>,
    llm_backend: &str,
    deepseek_api_key: &str,
    anthropic_api_key: &str,
    ollama_url: &str,
    research_model: &str,
) -> i64 {
    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "research",
            "info",
            "[Research:Directory] Starting directory discovery phase",
        );
    }

    let mut total_discovered: i64 = 0;
    let mut directories_scraped: i64 = 0;
    let max_directories: i64 = 10;

    for country in countries {
        if super::is_cancelled() || directories_scraped >= max_directories {
            break;
        }

        let names = country_names(country);
        let country_name = if names.is_empty() { country.as_str() } else { names[0] };

        for category in categories {
            if super::is_cancelled() || directories_scraped >= max_directories {
                break;
            }

            let queries = generate_directory_queries(&category.name, country, domain);

            // Take up to 5 queries per category, search each, collect top 3 URLs
            let mut directory_urls: Vec<String> = Vec::new();

            for query in queries.iter().take(5) {
                if super::is_cancelled() || directories_scraped >= max_directories {
                    break;
                }

                // Skip already-executed directory queries
                let dir_query_key = format!("[directory] {}", query);
                {
                    let db: tauri::State<'_, Database> = app.state();
                    if db.search_already_done(&dir_query_key).unwrap_or(false) {
                        continue;
                    }
                }

                let results = match crate::services::brave::search(brave_key, query, country, 5, 0).await {
                    Ok(r) => r,
                    Err(e) => {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            job_id,
                            "research",
                            "warn",
                            &format!("[Research:Directory] Search failed for '{}': {}", query, e),
                        );
                        continue;
                    }
                };

                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.record_search(&dir_query_key, country, results.len() as i64);
                }

                // Take top 3 results that aren't search engines
                for result in results.iter().take(3) {
                    if !is_skip_domain(&result.url) && !directory_urls.contains(&result.url) {
                        directory_urls.push(result.url.clone());
                    }
                }

                // Rate limit between searches
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }

            // Scrape each directory URL and extract companies
            for dir_url in &directory_urls {
                if super::is_cancelled() || directories_scraped >= max_directories {
                    break;
                }

                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "research",
                        "info",
                        &format!("[Research:Directory] Scraping directory: {}", dir_url),
                    );
                }

                // Fetch directory page text
                let page_text = match crate::services::scraper::fetch_website_text(dir_url).await {
                    Ok(text) => text,
                    Err(e) => {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            job_id,
                            "research",
                            "warn",
                            &format!("[Research:Directory] Failed to scrape {}: {}", dir_url, e),
                        );
                        continue;
                    }
                };

                // Skip paywalled directories
                if is_paywalled(&page_text) {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "research",
                        "info",
                        &format!("[Research:Directory] Skipping paywalled directory: {}", dir_url),
                    );
                    continue;
                }

                directories_scraped += 1;

                // Use LLM to extract company listings from the directory page
                let extract_prompt = format!(
                    r#"This is a directory/listing page. Extract all company names and their website URLs from the page content below.

Return a JSON array of objects: [{{"name": "Company Name", "website_url": "https://example.com", "city": "City Name", "description": "Brief description"}}]

Rules:
- Only include companies that are clearly {} businesses in {}
- Include the company website URL if visible (not the directory page link)
- Include city/location if mentioned
- Include a brief description if available
- Maximum 50 companies
- If no companies are found, return an empty array: []
- Return ONLY valid JSON, no other text

Page content:
{}

Return ONLY the JSON array."#,
                    domain, country_name, &page_text[..page_text.len().min(6000)]
                );

                let llm_response = if llm_backend == "deepseek" {
                    match crate::services::deepseek::chat(
                        deepseek_api_key,
                        None,
                        &extract_prompt,
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
                                &format!("[Research:Directory] LLM extraction failed for {}: {}", dir_url, e),
                            );
                            continue;
                        }
                    }
                } else if llm_backend == "haiku" {
                    match crate::services::anthropic::chat(
                        anthropic_api_key,
                        None,
                        &extract_prompt,
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
                                &format!("[Research:Directory] LLM extraction failed for {}: {}", dir_url, e),
                            );
                            continue;
                        }
                    }
                } else {
                    match crate::services::ollama::generate(
                        ollama_url,
                        research_model,
                        &extract_prompt,
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
                                &format!("[Research:Directory] LLM extraction failed for {}: {}", dir_url, e),
                            );
                            continue;
                        }
                    }
                };

                // Parse the LLM response as a JSON array of companies
                let companies: Vec<Value> = match serde_json::from_str(&llm_response) {
                    Ok(Value::Array(arr)) => arr,
                    _ => {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            job_id,
                            "research",
                            "warn",
                            &format!("[Research:Directory] Failed to parse LLM response as array for {}", dir_url),
                        );
                        continue;
                    }
                };

                let mut dir_new = 0i64;

                for entry in companies.iter().take(50) {
                    if super::is_cancelled() {
                        break;
                    }

                    let name = entry.get("name").and_then(|v| v.as_str()).unwrap_or("");
                    if name.is_empty() {
                        continue;
                    }

                    // Extract domain from website_url if present
                    let website_url = entry.get("website_url").and_then(|v| v.as_str()).unwrap_or("");
                    let extracted_domain = extract_domain_from_url(website_url);

                    // Dedup: check domain against Supabase + local DB
                    if !extracted_domain.is_empty() {
                        let normalized_domain = extracted_domain.to_lowercase();
                        let stripped = normalized_domain.strip_prefix("www.").unwrap_or(&normalized_domain);
                        if supabase_domains.contains(stripped) {
                            continue;
                        }
                        let db: tauri::State<'_, Database> = app.state();
                        if db.domain_exists(&extracted_domain).unwrap_or(false) {
                            continue;
                        }
                    }

                    // Dedup: check name against Supabase + local DB
                    {
                        let name_lower = name.to_lowercase().trim().to_string();
                        let mut n = name_lower.clone();
                        for suffix in &[
                            " ltd", " limited", " gmbh", " sas", " bv", " ag", " sa", " srl",
                            " nv", " inc", " llc", " co.", " corp", " corporation", " plc",
                            " s.r.l.", " s.a.", " e.k.", " ohg", " kg", " ug",
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

                    // Build company record
                    let discovery_source_val = format!("directory:{}", dir_url);
                    let company = json!({
                        "name": name,
                        "website_url": website_url,
                        "domain": extracted_domain,
                        "country": country,
                        "city": entry.get("city").and_then(|v| v.as_str()).unwrap_or(""),
                        "source": "directory",
                        "source_url": dir_url,
                        "source_query": format!("[directory] {}", category.name),
                        "raw_snippet": entry.get("description").and_then(|v| v.as_str()).unwrap_or(""),
                        "search_profile_id": profile_id,
                        "discovery_source": discovery_source_val,
                    });

                    let db: tauri::State<'_, Database> = app.state();
                    match db.insert_company(&company) {
                        Ok(_) => {
                            dir_new += 1;
                            total_discovered += 1;
                        }
                        Err(e) => {
                            let _ = db.log_activity(
                                job_id,
                                "research",
                                "warn",
                                &format!("[Research:Directory] Failed to store {}: {}", name, e),
                            );
                        }
                    }
                }

                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "research",
                        "info",
                        &format!(
                            "[Research:Directory] Directory {} yielded {} new companies",
                            dir_url, dir_new
                        ),
                    );
                }

                // Rate limit between directory scrapes (be polite)
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            }
        }
    }

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "research",
            "info",
            &format!(
                "[Research:Directory] Directory phase complete: {} directories scraped, {} new companies",
                directories_scraped, total_discovered
            ),
        );
    }

    total_discovered
}

/// Extract the domain from a URL (e.g., "https://www.example.com/page" -> "example.com").
fn extract_domain_from_url(url: &str) -> String {
    if url.is_empty() {
        return String::new();
    }
    // Strip scheme
    let without_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    // Take host part (before first /)
    let host = without_scheme.split('/').next().unwrap_or("");
    // Strip port
    let host = host.split(':').next().unwrap_or(host);
    // Strip www. prefix
    let host = host.strip_prefix("www.").unwrap_or(host);
    host.to_lowercase()
}
