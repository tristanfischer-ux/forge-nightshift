use anyhow::Result;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::db::Database;

const BATCH_LIMIT: i64 = 100;
const BRAVE_RESULTS_PER_COMPANY: u32 = 5;
const RATE_LIMIT_MS: u64 = 200;

/// Classify an activity type from title + snippet using keyword matching.
fn classify_activity(title: &str, snippet: &str) -> &'static str {
    let text = format!("{} {}", title, snippet).to_lowercase();

    if text.contains("funding") || text.contains("investment") || text.contains("raised") || text.contains("series ") {
        return "funding_round";
    }
    if text.contains("contract") || text.contains("awarded") || text.contains("won ") {
        return "contract_win";
    }
    if text.contains("expansion") || text.contains("new facility") || text.contains("opening") || text.contains("new site") {
        return "expansion";
    }
    if text.contains("hire") || text.contains("appointed") || text.contains(" joins") || text.contains("new ceo") || text.contains("new cto") {
        return "key_hire";
    }
    if text.contains("acquisition") || text.contains("acquired") || text.contains("merger") || text.contains("takeover") {
        return "acquisition";
    }

    "news"
}

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let brave_api_key = config
        .get("brave_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if brave_api_key.is_empty() {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "activity", "error", "No Brave API key configured");
        anyhow::bail!("No Brave API key configured for activity feed");
    }

    let fetched_count = Arc::new(AtomicI64::new(0));
    let saved_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_activity_eligible_companies(BATCH_LIMIT)?
    };

    let total = companies.len();

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "activity",
            "info",
            &format!("Starting activity feed for {} companies", total),
        );
    }

    let started_at = chrono::Utc::now();

    super::emit_node(app, json!({
        "node_id": "activity",
        "status": "running",
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    for company in &companies {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "activity", "warn", "Activity feed cancelled by user");
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("");

        // Build search query
        let query = if city.is_empty() {
            format!("\"{}\" news", name)
        } else {
            format!("\"{}\" {} news", name, city)
        };

        // Search Brave with freshness=pm (past month)
        let results = match search_news(&brave_api_key, &query, BRAVE_RESULTS_PER_COMPANY).await {
            Ok(r) => r,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "activity",
                    "warn",
                    &format!("[Activity] Search failed for {}: {}", name, e),
                );
                error_count.fetch_add(1, Ordering::Relaxed);
                // Rate limit even on errors
                tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS)).await;
                continue;
            }
        };

        // Save each result
        for result in &results {
            let title = result.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let url = result.get("url").and_then(|v| v.as_str()).unwrap_or("");
            let snippet = result.get("description").and_then(|v| v.as_str()).unwrap_or("");

            if url.is_empty() {
                continue;
            }

            let activity_type = classify_activity(title, snippet);

            let db: tauri::State<'_, Database> = app.state();
            match db.save_activity(id, title, url, Some(snippet), activity_type, None) {
                Ok(inserted) => {
                    if inserted {
                        saved_count.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Err(e) => {
                    let _ = db.log_activity(
                        job_id,
                        "activity",
                        "warn",
                        &format!("[Activity] DB save failed for {} ({}): {}", name, url, e),
                    );
                }
            }
        }

        fetched_count.fetch_add(1, Ordering::Relaxed);

        let cur_fetched = fetched_count.load(Ordering::Relaxed);
        if cur_fetched % 10 == 0 || cur_fetched == 1 {
            let elapsed = (chrono::Utc::now() - started_at).num_seconds();
            let rate = if elapsed > 0 {
                cur_fetched as f64 / elapsed as f64 * 3600.0
            } else {
                0.0
            };
            super::emit_node(app, json!({
                "node_id": "activity",
                "status": "running",
                "progress": { "current": cur_fetched, "total": total, "rate": rate, "current_item": name },
                "started_at": started_at.to_rfc3339(),
                "elapsed_secs": elapsed
            }));
        }

        let _ = app.emit(
            "pipeline:progress",
            json!({
                "stage": "activity",
                "current_company": name,
                "fetched": cur_fetched,
                "saved": saved_count.load(Ordering::Relaxed),
                "errors": error_count.load(Ordering::Relaxed),
            }),
        );

        // Rate limit between Brave API calls
        tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS)).await;
    }

    let final_fetched = fetched_count.load(Ordering::Relaxed);
    let final_saved = saved_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);
    let elapsed = (chrono::Utc::now() - started_at).num_seconds();

    super::emit_node(app, json!({
        "node_id": "activity",
        "status": "completed",
        "progress": { "current": final_fetched, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "activity",
            "info",
            &format!(
                "[Activity] Complete: {} companies searched, {} items saved, {} errors in {}s",
                final_fetched, final_saved, final_errors, elapsed
            ),
        );
    }

    Ok(json!({
        "companies_searched": final_fetched,
        "items_saved": final_saved,
        "errors": final_errors,
        "elapsed_secs": elapsed,
    }))
}

/// Search Brave Web Search with freshness=pm (past month) for news.
async fn search_news(api_key: &str, query: &str, count: u32) -> Result<Vec<Value>> {
    let client = reqwest::Client::new();
    let resp = client
        .get("https://api.search.brave.com/res/v1/web/search")
        .header("X-Subscription-Token", api_key)
        .query(&[
            ("q", query),
            ("count", &count.to_string()),
            ("freshness", "pm"),
        ])
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Brave Search error {}: {}", status, body);
    }

    let body: Value = resp.json().await?;
    let results = body
        .get("web")
        .and_then(|w| w.get("results"))
        .and_then(|r| r.as_array())
        .cloned()
        .unwrap_or_default();

    Ok(results)
}
