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

    let started_at = chrono::Utc::now();
    super::emit_node(app, json!({
        "node_id": "push",
        "status": "running",
        "model": null,
        "progress": { "current": 0, "total": null, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    let mut pushed_count = 0;
    let mut skipped_threshold = 0;
    let mut skipped_domain = 0;
    let mut error_count = 0;

    let batch_size: i64 = 200;
    let mut offset: i64 = 0;

    loop {
        let batch = {
            let db: tauri::State<'_, Database> = app.state();
            db.get_companies(Some("approved"), batch_size, offset, None)?
        };

        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len() as i64;

    let batch_total = batch.len();

    for (batch_idx, company) in batch.iter().enumerate() {
        if super::is_cancelled() {
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let score = company
            .get("relevance_score")
            .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
            .unwrap_or(0);

        if score < threshold {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "push",
                "info",
                &format!("Skipping {} (score {} < threshold {})", name, score, threshold),
            );
            skipped_threshold += 1;
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
                    skipped_domain += 1;
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
            Ok(listing_id) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.update_company_status(id, "pushed");
                if !listing_id.is_empty() {
                    let _ = db.set_supabase_listing_id(id, &listing_id);
                }
                pushed_count += 1;

                let _ = app.emit(
                    "pipeline:progress",
                    json!({
                        "stage": "push",
                        "phase": "done",
                        "current_company": name,
                        "current_index": offset as usize + batch_idx,
                        "pushed": pushed_count,
                        "skipped": skipped_threshold + skipped_domain,
                        "errors": error_count,
                        "total": batch_total,
                    }),
                );

                if pushed_count % 5 == 0 || pushed_count == 1 {
                    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                    let rate = if elapsed > 0 { pushed_count as f64 / elapsed as f64 * 3600.0 } else { 0.0 };
                    super::emit_node(app, json!({
                        "node_id": "push",
                        "status": "running",
                        "model": null,
                        "progress": { "current": pushed_count + skipped_threshold + skipped_domain + error_count, "total": null, "rate": rate, "current_item": name },
                        "concurrency": 1,
                        "started_at": started_at.to_rfc3339(),
                        "elapsed_secs": elapsed
                    }));
                }

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

        offset += batch_len;

        // If we got fewer than batch_size, we've reached the end
        if batch_len < batch_size {
            break;
        }
    }

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    super::emit_node(app, json!({
        "node_id": "push",
        "status": "completed",
        "model": null,
        "progress": { "current": pushed_count + skipped_threshold + skipped_domain + error_count, "total": null, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    Ok(json!({
        "companies_pushed": pushed_count,
        "skipped_below_threshold": skipped_threshold,
        "skipped_duplicate_domain": skipped_domain,
        "errors": error_count,
    }))
}

/// Backfill process_capabilities onto existing marketplace_listings.
/// Finds pushed companies with process_capabilities_json and a supabase_listing_id,
/// then PATCHes each listing with the parsed capabilities.
pub async fn push_capabilities(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        anyhow::bail!("Supabase credentials not configured");
    }

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_pushed_companies_with_capabilities()?
    };

    if companies.is_empty() {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "push_capabilities", "info", "No pushed companies with capabilities to backfill");
        return Ok(json!({ "patched": 0, "message": "No companies to backfill" }));
    }

    let total = companies.len();
    let started_at = chrono::Utc::now();

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "push_capabilities",
            "info",
            &format!("Backfilling process_capabilities for {} listings", total),
        );
    }

    super::emit_node(app, json!({
        "node_id": "push_capabilities",
        "status": "running",
        "model": null,
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    let mut patched = 0u32;
    let mut errors = 0u32;

    for company in &companies {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "push_capabilities", "warn", "Cancelled by user");
            break;
        }

        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
        let listing_id = company.get("supabase_listing_id").and_then(|v| v.as_str()).unwrap_or("");
        let caps_str = company.get("process_capabilities_json").and_then(|v| v.as_str()).unwrap_or("[]");

        if listing_id.is_empty() {
            errors += 1;
            continue;
        }

        let capabilities: Value = match serde_json::from_str(caps_str) {
            Ok(v) => v,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "push_capabilities",
                    "warn",
                    &format!("JSON parse failed for {}: {}", name, e),
                );
                errors += 1;
                continue;
            }
        };

        match crate::services::supabase::patch_listing_capabilities(
            supabase_url,
            supabase_key,
            listing_id,
            capabilities,
        )
        .await
        {
            Ok(()) => {
                patched += 1;
                log::info!("  Patched capabilities for: {} (listing {})", name, listing_id);

                let processed = patched + errors;
                let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                if patched % 5 == 0 || patched == 1 {
                    super::emit_node(app, json!({
                        "node_id": "push_capabilities",
                        "status": "running",
                        "model": null,
                        "progress": { "current": processed, "total": total, "rate": null, "current_item": name },
                        "concurrency": 1,
                        "started_at": started_at.to_rfc3339(),
                        "elapsed_secs": elapsed
                    }));
                }

                // Rate limit between Supabase patches
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "push_capabilities",
                    "error",
                    &format!("Failed to patch {}: {}", name, e),
                );
                errors += 1;
            }
        }
    }

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    let summary = json!({
        "total": total,
        "patched": patched,
        "errors": errors,
    });

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "push_capabilities",
            "info",
            &format!("Complete: {}/{} patched, {} errors", patched, total, errors),
        );
    }

    super::emit_node(app, json!({
        "node_id": "push_capabilities",
        "status": "completed",
        "model": null,
        "progress": { "current": total, "total": total, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    log::info!("push_capabilities complete: {}/{} patched", patched, total);
    Ok(summary)
}
