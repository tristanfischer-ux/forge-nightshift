use anyhow::Result;
use serde_json::{json, Value};
use tauri::Manager;

use crate::db::Database;

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let ch_api_key = config
        .get("companies_house_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if ch_api_key.is_empty() {
        anyhow::bail!("companies_house_api_key not configured");
    }

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_gb_companies_needing_ch_check()?
    };

    let total = companies.len() as i64;

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "companies_house",
            "info",
            &format!("Starting CH verification for {} GB companies", total),
        );
    }

    let started_at = chrono::Utc::now();

    super::emit_node(app, json!({
        "node_id": "companies_house",
        "status": "running",
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    let mut verified: i64 = 0;
    let mut no_match: i64 = 0;
    let mut errors: i64 = 0;
    let mut rechecked: i64 = 0;

    for company in &companies {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "companies_house", "warn", "CH verification cancelled");
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let existing_attrs_str = company.get("attributes_json").and_then(|v| v.as_str()).unwrap_or("{}");
        let is_recheck = company.get("ch_verified_at").is_some()
            && company.get("ch_verified_at").and_then(|v| v.as_str()).is_some();

        // Parse existing attributes
        let mut attributes: Value = serde_json::from_str(existing_attrs_str).unwrap_or(json!({}));

        match crate::services::companies_house::enrich_company(&ch_api_key, name).await {
            Ok(Some(ch_data)) => {
                // Merge CH data into attributes
                if let Some(obj) = ch_data.as_object() {
                    if let Some(attrs) = attributes.as_object_mut() {
                        for (k, v) in obj {
                            attrs.insert(k.clone(), v.clone());
                        }
                    }
                }

                let ch_number = ch_data
                    .get("ch_company_number")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let financial_health = attributes
                    .get("financial_signals")
                    .and_then(|fs| fs.get("health"))
                    .and_then(|h| h.as_str())
                    .unwrap_or("");

                let db: tauri::State<'_, Database> = app.state();
                db.update_ch_verification(
                    id,
                    ch_number,
                    &attributes.to_string(),
                    financial_health,
                )?;

                verified += 1;
                if is_recheck {
                    rechecked += 1;
                }

                let _ = db.log_activity(
                    job_id,
                    "companies_house",
                    "info",
                    &format!("CH verified: {} → #{}", name, ch_number),
                );
            }
            Ok(None) => {
                // No match — still mark as checked so we don't retry immediately
                let db: tauri::State<'_, Database> = app.state();
                db.mark_ch_verified(id, "")?;
                no_match += 1;

                let _ = db.log_activity(
                    job_id,
                    "companies_house",
                    "info",
                    &format!("No CH match for: {}", name),
                );
            }
            Err(e) => {
                errors += 1;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "companies_house",
                    "warn",
                    &format!("CH lookup failed for {}: {}", name, e),
                );
            }
        }

        let current = verified + no_match + errors;
        let elapsed = (chrono::Utc::now() - started_at).num_seconds();
        let rate = if elapsed > 0 {
            current as f64 / elapsed as f64 * 3600.0
        } else {
            0.0
        };

        super::emit_node(app, json!({
            "node_id": "companies_house",
            "status": "running",
            "progress": { "current": current, "total": total, "rate": rate, "current_item": name },
            "started_at": started_at.to_rfc3339(),
            "elapsed_secs": elapsed
        }));
    }

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();

    super::emit_node(app, json!({
        "node_id": "companies_house",
        "status": "completed",
        "progress": { "current": verified + no_match + errors, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "companies_house",
            "info",
            &format!(
                "CH verification complete: {} verified, {} no match, {} errors, {} rechecked",
                verified, no_match, errors, rechecked
            ),
        );
    }

    Ok(json!({
        "total": total,
        "verified": verified,
        "no_match": no_match,
        "errors": errors,
        "rechecked": rechecked,
    }))
}
