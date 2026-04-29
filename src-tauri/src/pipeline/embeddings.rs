use anyhow::Result;
use serde_json::{json, Value};
use tauri::Manager;

use crate::db::Database;
use crate::services::openai;

/// Generate embeddings for enriched companies that don't have them yet.
/// Uses OpenAI text-embedding-3-small (1536 dimensions).
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let openai_key = {
        let db: tauri::State<'_, Database> = app.state();
        // Try config first, then resolve from env files
        let key = config
            .get("openai_api_key")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if key.is_empty() {
            crate::resolve_openai_key(&db).unwrap_or_default()
        } else {
            key
        }
    };

    if openai_key.is_empty() {
        log::warn!("[Embeddings] OpenAI API key not configured, skipping embeddings");
        return Ok(json!({ "skipped": true, "reason": "no_api_key" }));
    }

    let profile_id = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_active_profile_id()
    };

    let started_at = chrono::Utc::now();
    super::emit_node(app, json!({
        "node_id": "embeddings",
        "status": "running",
        "model": "text-embedding-3-small",
        "progress": { "current": 0, "total": null, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    // Get companies needing embeddings (enriched/approved/pushed, no embedding yet)
    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies_needing_embeddings(&profile_id, 500)?
    };

    let total = companies.len();
    log::info!("[Embeddings] {} companies need embeddings (profile: {})", total, profile_id);

    if total == 0 {
        super::emit_node(app, json!({
            "node_id": "embeddings",
            "status": "completed",
            "model": "text-embedding-3-small",
            "progress": { "current": 0, "total": 0, "rate": null, "current_item": null },
            "concurrency": 1,
            "started_at": started_at.to_rfc3339(),
            "elapsed_secs": 0
        }));
        return Ok(json!({ "embedded": 0, "total": 0 }));
    }

    let mut embedded = 0;
    let mut errors = 0;
    let mut first_error_logged = false;

    for (i, company) in companies.iter().enumerate() {
        if super::is_cancelled() {
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");

        // Build embedding text from available fields
        let mut text_parts = Vec::new();
        if let Some(n) = company.get("name").and_then(|v| v.as_str()) {
            text_parts.push(n.to_string());
        }
        if let Some(d) = company.get("description").and_then(|v| v.as_str()) {
            text_parts.push(d.to_string());
        }
        if let Some(cat) = company.get("category").and_then(|v| v.as_str()) {
            text_parts.push(cat.to_string());
        }
        if let Some(sub) = company.get("subcategory").and_then(|v| v.as_str()) {
            text_parts.push(sub.to_string());
        }
        if let Some(country) = company.get("country").and_then(|v| v.as_str()) {
            text_parts.push(country.to_string());
        }
        if let Some(city) = company.get("city").and_then(|v| v.as_str()) {
            text_parts.push(city.to_string());
        }
        // Add specialties, certifications, industries from JSON arrays
        // NOTE: `materials` removed — column does not exist on companies table
        for field in &["specialties", "certifications", "industries"] {
            if let Some(val) = company.get(*field).and_then(|v| v.as_str()) {
                if let Ok(arr) = serde_json::from_str::<Vec<String>>(val) {
                    text_parts.push(arr.join(", "));
                }
            }
        }
        // Add synthesis if available
        if let Some(synth) = company.get("synthesis_public_json").and_then(|v| v.as_str()) {
            if let Ok(s) = serde_json::from_str::<Value>(synth) {
                if let Some(summary) = s.get("capability_summary").and_then(|v| v.as_str()) {
                    text_parts.push(summary.to_string());
                }
            }
        }

        // Add recent news/activity if available
        {
            let db: tauri::State<'_, Database> = app.state();
            if let Ok(activities) = db.get_company_activities(&id, 5) {
                for act in activities {
                    if let Some(title) = act.get("title").and_then(|v| v.as_str()) {
                        text_parts.push(title.to_string());
                    }
                    if let Some(snippet) = act.get("snippet").and_then(|v| v.as_str()) {
                        text_parts.push(snippet.to_string());
                    }
                }
            }
        }

        let embed_text = text_parts.join(". ");
        if embed_text.len() < 10 {
            continue;
        }

        // Truncate to 8000 chars (OpenAI limit)
        let embed_text = if embed_text.len() > 8000 {
            embed_text[..8000].to_string()
        } else {
            embed_text
        };

        match openai::embed_query(&openai_key, &embed_text).await {
            Ok(embedding) => {
                let embedding_json = serde_json::to_string(&embedding).unwrap_or_default();
                let db: tauri::State<'_, Database> = app.state();
                if let Err(e) = db.save_embedding(id, &embedding_json) {
                    log::warn!("[Embeddings] Failed to save embedding for {}: {}", name, e);
                    // FIX 2026-04-16: surface first save error to db so it's visible
                    // in the dashboard. Previously this only logged to stderr, hiding
                    // a NOT NULL constraint failure that affected every single insert.
                    if !first_error_logged {
                        let _ = db.log_activity(job_id, "embeddings", "error",
                            &format!("First save error this run: {} (this error blocks all subsequent saves)", e));
                        first_error_logged = true;
                    }
                    errors += 1;
                } else {
                    embedded += 1;
                }
            }
            Err(e) => {
                log::warn!("[Embeddings] Failed to embed {}: {}", name, e);
                if !first_error_logged {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(job_id, "embeddings", "error",
                        &format!("First embed error this run: {}", e));
                    first_error_logged = true;
                }
                errors += 1;
            }
        }

        // Progress update every 10 companies
        if i % 10 == 0 || i == total - 1 {
            let elapsed = (chrono::Utc::now() - started_at).num_seconds();
            let rate = if elapsed > 0 { (embedded as f64 / elapsed as f64 * 60.0) as i64 } else { 0 };
            super::emit_node(app, json!({
                "node_id": "embeddings",
                "status": "running",
                "model": "text-embedding-3-small",
                "progress": { "current": embedded, "total": total, "rate": format!("{}/min", rate), "current_item": name },
                "concurrency": 1,
                "started_at": started_at.to_rfc3339(),
                "elapsed_secs": elapsed
            }));
        }

        // Rate limit: 200ms between calls
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    log::info!("[Embeddings] Done: {}/{} embedded, {} errors, {}s", embedded, total, errors, elapsed);

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "embeddings",
            "info",
            &format!("Embedded {}/{} companies ({} errors) in {}s", embedded, total, errors, elapsed),
        );
    }

    super::emit_node(app, json!({
        "node_id": "embeddings",
        "status": "completed",
        "model": "text-embedding-3-small",
        "progress": { "current": embedded, "total": total, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    Ok(json!({
        "embedded": embedded,
        "errors": errors,
        "total": total,
        "elapsed_secs": elapsed
    }))
}
