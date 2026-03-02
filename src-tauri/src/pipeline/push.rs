use anyhow::Result;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let foundry_id = config
        .get("foundry_id")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        anyhow::bail!("Supabase credentials not configured");
    }
    if foundry_id.is_empty() {
        anyhow::bail!("Foundry ID not configured");
    }

    let threshold: i64 = config
        .get("relevance_threshold")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let enriched = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies(Some("approved"), 200, 0)?
    };

    let mut pushed_count = 0;
    let mut skipped_count = 0;
    let mut error_count = 0;

    for company in &enriched {
        if super::is_cancelled() {
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let score = company
            .get("relevance_score")
            .and_then(|v| v.as_str())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0);

        if score < threshold {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "push",
                "info",
                &format!("Skipping {} (score {} < threshold {})", name, score, threshold),
            );
            skipped_count += 1;
            continue;
        }

        let domain = company
            .get("domain")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        if !domain.is_empty() {
            match crate::services::supabase::check_domain_exists(supabase_url, supabase_key, domain)
                .await
            {
                Ok(true) => {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "push",
                        "info",
                        &format!("Skipping {} — already in ForgeOS", name),
                    );
                    skipped_count += 1;
                    continue;
                }
                Ok(false) => {}
                Err(e) => {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        job_id,
                        "push",
                        "warn",
                        &format!("Domain check failed for {}: {}", domain, e),
                    );
                }
            }
        }

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "push", "info", &format!("Pushing {} to ForgeOS", name));
        }

        match crate::services::supabase::push_listing(supabase_url, supabase_key, foundry_id, company)
            .await
        {
            Ok(_listing_id) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.update_company_status(id, "pushed");
                pushed_count += 1;

                let _ = app.emit(
                    "pipeline:progress",
                    json!({
                        "stage": "push",
                        "pushed": pushed_count,
                    }),
                );

                // Rate limit between Supabase inserts
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "push",
                    "error",
                    &format!("Failed to push {}: {}", name, e),
                );
                let _ = db.update_company_status(id, "error");
                error_count += 1;
            }
        }
    }

    Ok(json!({
        "companies_pushed": pushed_count,
        "skipped_below_threshold": skipped_count,
        "errors": error_count,
    }))
}
