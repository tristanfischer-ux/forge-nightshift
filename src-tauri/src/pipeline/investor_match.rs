use anyhow::Result;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::db::Database;

const BATCH_LIMIT: i64 = 100;
const TOP_MATCHES_PER_COMPANY: usize = 5;

/// Fetch Finance-category investors from Supabase marketplace_listings.
/// Returns a Vec of investor objects with id, name, sector_focus, stage_focus, geo_focus.
async fn fetch_investors_from_supabase(url: &str, key: &str) -> Result<Vec<Value>> {
    let client = reqwest::Client::new();
    let mut all_investors: Vec<Value> = Vec::new();
    let page_size = 1000;
    let mut offset = 0;

    loop {
        let resp = client
            .get(format!("{}/rest/v1/marketplace_listings", url))
            .header("apikey", key)
            .header("Authorization", format!("Bearer {}", key))
            .header("Range", format!("{}-{}", offset, offset + page_size - 1))
            .query(&[
                ("select", "id,title,subcategory,attributes"),
                ("category", "eq.Finance"),
                ("approval_status", "eq.approved"),
                ("limit", "1000"),
            ])
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Supabase fetch investors error {}: {}", status, body);
        }

        let page: Vec<Value> = resp.json().await?;
        let count = page.len();
        all_investors.extend(page);

        if count < page_size as usize {
            break;
        }
        offset += page_size;
    }

    Ok(all_investors)
}

/// Helper: extract a JSON array of strings from attributes, lowercased.
fn extract_string_array(attrs: &Value, key: &str) -> Vec<String> {
    attrs
        .get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str())
                .map(|s| s.to_lowercase())
                .collect()
        })
        .unwrap_or_default()
}

/// Compute a keyword overlap match score (0-100) between a company and an investor.
/// Uses sector, stage, and geo overlap as signals.
/// Investor fields (sectors, stage_focus, geo_focus) live inside the `attributes` JSONB column.
fn compute_match_score(company: &Value, investor: &Value) -> (i32, Vec<String>) {
    let mut score = 0i32;
    let mut reasons: Vec<String> = Vec::new();

    // Extract company fields
    let company_category = company.get("category").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let company_subcategory = company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let company_specialties = company.get("specialties").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let company_industries = company.get("industries").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let company_size = company.get("company_size").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let company_country = company.get("country").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();

    // Extract investor fields from attributes JSONB
    let attrs = investor.get("attributes").cloned().unwrap_or(json!({}));
    let investor_sectors = extract_string_array(&attrs, "sectors");
    let investor_stages = extract_string_array(&attrs, "stage_focus");
    let investor_geos = extract_string_array(&attrs, "geo_focus");

    // Build combined text for matching (used as fallback for keyword search)
    let investor_sector_text = investor_sectors.join(" ");
    let investor_stage_text = investor_stages.join(" ");
    let _investor_geo_text = investor_geos.join(" ");

    // Combine all company sector text for matching
    let company_sector_text = format!("{} {} {} {}", company_category, company_subcategory, company_specialties, company_industries);

    // 1. Sector overlap (max 40 points)
    // Each investor sector keyword is checked against the company's combined sector text
    let mut sector_matches = 0;
    let mut sector_total = 0;
    for sector in &investor_sectors {
        for keyword in extract_keywords(sector) {
            if keyword.len() >= 3 {
                sector_total += 1;
                if company_sector_text.contains(keyword) {
                    sector_matches += 1;
                }
            }
        }
    }
    if sector_total > 0 {
        let sector_score = (sector_matches as f64 / sector_total as f64 * 40.0).min(40.0) as i32;
        if sector_score > 0 {
            score += sector_score;
            reasons.push(format!("Sector overlap: {}/{} keywords match", sector_matches, sector_total));
        }
    }

    // Bonus for cleantech/manufacturing/engineering keywords in investor focus
    let cleantech_keywords = ["cleantech", "clean tech", "green", "sustainability", "renewable", "energy", "climate", "environment", "manufacturing", "industrial", "engineering", "hardware"];
    for kw in &cleantech_keywords {
        if investor_sector_text.contains(kw) && company_sector_text.contains(kw) {
            score += 5;
            reasons.push(format!("Shared focus: {}", kw));
            break; // Only count once
        }
    }

    // 2. Stage overlap (max 25 points)
    let stage_match = match company_size.as_str() {
        "small" | "micro" | "startup" => {
            investor_stages.iter().any(|s| {
                s.contains("seed") || s.contains("early") || s.contains("pre-seed")
                    || s.contains("series a") || s.contains("angel")
            })
        }
        "medium" | "sme" => {
            investor_stages.iter().any(|s| {
                s.contains("series b") || s.contains("growth")
                    || s.contains("series a") || s.contains("expansion")
            })
        }
        "large" | "enterprise" => {
            investor_stages.iter().any(|s| {
                s.contains("late") || s.contains("growth")
                    || s.contains("buyout") || s.contains("pe")
            })
        }
        _ => {
            // Fallback: check combined stage text
            !investor_stage_text.is_empty()
        },
    };
    if stage_match {
        score += 25;
        reasons.push(format!("Stage fit: {} company matches investor focus", company_size));
    }

    // 3. Geo overlap (max 25 points)
    let geo_match = if !company_country.is_empty() {
        investor_geos.iter().any(|g| {
            g.contains(&company_country)
                || (g.contains("uk") && (company_country == "gb" || company_country == "uk"))
                || (g.contains("europe") && ["gb", "uk", "de", "fr", "nl", "it", "es", "se", "no", "dk", "fi", "be", "at", "ch", "ie", "pl", "cz", "pt"].contains(&company_country.as_str()))
                || g.contains("global")
        }) || investor_geos.is_empty() // No geo restriction = open to all
    } else {
        false
    };
    if geo_match {
        score += 25;
        reasons.push("Geographic match".to_string());
    }

    // 4. Name/brand recognition bonus (max 5 points)
    let investor_name = investor.get("title").and_then(|v| v.as_str()).unwrap_or("").to_lowercase();
    let known_brands = ["innovate uk", "british business bank", "eic", "horizon", "clean growth fund", "green angel", "carbon trust"];
    for brand in &known_brands {
        if investor_name.contains(brand) {
            score += 5;
            reasons.push(format!("Known cleantech funder: {}", brand));
            break;
        }
    }

    (score.min(100), reasons)
}

/// Extract meaningful keywords from a comma/semicolon separated string.
fn extract_keywords(text: &str) -> Vec<&str> {
    text.split(|c: char| c == ',' || c == ';' || c == '|')
        .map(|s| s.trim())
        .filter(|s| s.len() >= 3)
        .collect()
}

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if supabase_url.is_empty() || supabase_key.is_empty() {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "investor_match", "error", "No Supabase URL/key configured");
        anyhow::bail!("No Supabase URL/key configured for investor matching");
    }

    let matched_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));

    let started_at = chrono::Utc::now();

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "investor_match", "info", "Fetching investors from Supabase...");
    }

    super::emit_node(app, json!({
        "node_id": "investor_match",
        "status": "running",
        "progress": { "current": 0, "total": 0, "rate": null, "current_item": "Fetching investors..." },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    // Fetch all Finance investors from Supabase (cached for the entire run)
    let investors = match fetch_investors_from_supabase(&supabase_url, &supabase_key).await {
        Ok(inv) => inv,
        Err(e) => {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "investor_match", "error", &format!("Failed to fetch investors: {}", e));
            anyhow::bail!("Failed to fetch investors from Supabase: {}", e);
        }
    };

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "investor_match",
            "info",
            &format!("Fetched {} investors from Supabase", investors.len()),
        );
    }

    // Get companies needing matching
    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_investor_match_eligible_companies(BATCH_LIMIT)?
    };

    let total = companies.len();

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "investor_match",
            "info",
            &format!("Matching {} companies against {} investors", total, investors.len()),
        );
    }

    super::emit_node(app, json!({
        "node_id": "investor_match",
        "status": "running",
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    for company in &companies {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "investor_match", "warn", "Investor matching cancelled by user");
            break;
        }

        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");

        // Score all investors against this company
        let mut scored: Vec<(i32, Vec<String>, &Value)> = investors
            .iter()
            .map(|inv| {
                let (score, reasons) = compute_match_score(company, inv);
                (score, reasons, inv)
            })
            .filter(|(score, _, _)| *score > 0)
            .collect();

        // Sort by score descending, take top N
        scored.sort_by(|a, b| b.0.cmp(&a.0));
        scored.truncate(TOP_MATCHES_PER_COMPANY);

        // Save matches
        let db: tauri::State<'_, Database> = app.state();
        for (score, reasons, investor) in &scored {
            let inv_id = investor.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let inv_name = investor.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let inv_attrs = investor.get("attributes").cloned().unwrap_or(json!({}));
            let inv_sector = inv_attrs.get("sectors")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            let inv_stage = inv_attrs.get("stage_focus")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            let inv_geo = inv_attrs.get("geo_focus")
                .and_then(|v| v.as_array())
                .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect::<Vec<_>>().join(", "))
                .unwrap_or_default();
            let reasons_text = reasons.join("; ");

            match db.save_investor_match(
                company_id, inv_id, inv_name, &inv_sector, &inv_stage, &inv_geo, *score, &reasons_text,
            ) {
                Ok(_) => {}
                Err(e) => {
                    let _ = db.log_activity(
                        job_id,
                        "investor_match",
                        "warn",
                        &format!("[InvestorMatch] DB save failed for {} → {}: {}", company_name, inv_name, e),
                    );
                    error_count.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        matched_count.fetch_add(1, Ordering::Relaxed);
        let cur = matched_count.load(Ordering::Relaxed);

        if cur % 10 == 0 || cur == 1 {
            let elapsed = (chrono::Utc::now() - started_at).num_seconds();
            let rate = if elapsed > 0 {
                cur as f64 / elapsed as f64 * 3600.0
            } else {
                0.0
            };
            super::emit_node(app, json!({
                "node_id": "investor_match",
                "status": "running",
                "progress": { "current": cur, "total": total, "rate": rate, "current_item": company_name },
                "started_at": started_at.to_rfc3339(),
                "elapsed_secs": elapsed
            }));
        }

        let _ = app.emit(
            "pipeline:progress",
            json!({
                "stage": "investor_match",
                "current_company": company_name,
                "matched": cur,
                "total": total,
                "errors": error_count.load(Ordering::Relaxed),
            }),
        );
    }

    let final_matched = matched_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);
    let elapsed = (chrono::Utc::now() - started_at).num_seconds();

    super::emit_node(app, json!({
        "node_id": "investor_match",
        "status": "completed",
        "progress": { "current": final_matched, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "investor_match",
            "info",
            &format!(
                "[InvestorMatch] Complete: {} companies matched against {} investors, {} errors in {}s",
                final_matched, investors.len(), final_errors, elapsed
            ),
        );
    }

    Ok(json!({
        "companies_matched": final_matched,
        "investors_available": investors.len(),
        "errors": final_errors,
        "elapsed_secs": elapsed,
    }))
}
