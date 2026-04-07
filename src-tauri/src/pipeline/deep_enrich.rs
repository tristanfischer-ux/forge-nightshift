use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::Manager;

use crate::db::Database;
use crate::services::{ollama, scraper};

/// Run deep enrichment, optionally filtered by sector.
/// If sector is provided (e.g., "sheet_metal"), only processes companies matching that sector.
/// Otherwise falls back to the original 3-tier diverse sampling.
pub async fn run_trial(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    run_with_sector(app, job_id, config, None).await
}

/// Run deep enrichment for a specific sector.
pub async fn run_sector(app: &tauri::AppHandle, job_id: &str, config: &Value, sector: &str) -> Result<Value> {
    run_with_sector(app, job_id, config, Some(sector)).await
}

/// Run deep enrichment in drain mode — concurrent with enrich.
/// Polls for newly enriched companies in a loop, sleeping when empty + enrich still active.
pub async fn run_drain(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(job_id, "deep_enrich", "info", "Starting deep enrichment (drain mode — concurrent with enrich)");
    log::info!("[deep_enrich] Drain mode started — will pick up companies as enrich completes them");

    let succeeded = Arc::new(AtomicI64::new(0));
    let failed = Arc::new(AtomicI64::new(0));
    let mut batch_num = 0u32;

    loop {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "deep_enrich", "warn", "Deep enrichment (drain) cancelled by user");
            break;
        }

        let candidates = {
            let db: tauri::State<'_, Database> = app.state();
            db.get_deep_enrich_batch(20)?
        };

        if candidates.is_empty() {
            if super::is_enrich_active() {
                // Enrich still running — wait for more companies
                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(job_id, "deep_enrich", "info", "Queue empty, waiting for enrich to produce more companies...");
                }
                log::info!("[deep_enrich] Drain: queue empty, enrich still active — sleeping 15s");
                tokio::time::sleep(std::time::Duration::from_secs(15)).await;
                continue;
            } else {
                // Enrich done — one final fetch to catch stragglers
                let stragglers = {
                    let db: tauri::State<'_, Database> = app.state();
                    db.get_deep_enrich_batch(20)?
                };
                if !stragglers.is_empty() {
                    batch_num += 1;
                    log::info!("[deep_enrich] Drain: final sweep — {} stragglers", stragglers.len());
                    let _ = run_candidates(app, job_id, config, stragglers, &format!("deep enrichment (drain batch {})", batch_num)).await;
                }
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "deep_enrich", "info", "Enrich complete, drain loop finished");
                log::info!("[deep_enrich] Drain mode complete");
                break;
            }
        }

        batch_num += 1;
        let batch_size = candidates.len();
        log::info!("[deep_enrich] Drain batch {}: {} candidates", batch_num, batch_size);
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "deep_enrich",
                "info",
                &format!("Processing drain batch {} ({} companies)", batch_num, batch_size),
            );
        }

        match run_candidates(app, job_id, config, candidates, &format!("deep enrichment (drain batch {})", batch_num)).await {
            Ok(result) => {
                let batch_succeeded = result.get("succeeded").and_then(|v| v.as_i64()).unwrap_or(0);
                let batch_failed = result.get("failed").and_then(|v| v.as_i64()).unwrap_or(0);
                succeeded.fetch_add(batch_succeeded, Ordering::Relaxed);
                failed.fetch_add(batch_failed, Ordering::Relaxed);
            }
            Err(e) => {
                log::warn!("[deep_enrich] Drain batch {} failed: {}", batch_num, e);
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "deep_enrich", "warn", &format!("Drain batch {} failed: {}", batch_num, e));
            }
        }
    }

    let total_succeeded = succeeded.load(Ordering::Relaxed);
    let total_failed = failed.load(Ordering::Relaxed);

    Ok(json!({
        "mode": "drain",
        "batches": batch_num,
        "succeeded": total_succeeded,
        "failed": total_failed,
    }))
}

/// Run deep enrichment on ALL remaining unenriched companies (no sector filter, no limit).
pub async fn run_all(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(job_id, "deep_enrich", "info", "Starting deep enrichment (ALL remaining companies)");

    let candidates = db.get_all_deep_enrich_candidates()?;
    run_candidates(app, job_id, config, candidates, "deep enrichment (all)").await
}

async fn run_with_sector(app: &tauri::AppHandle, job_id: &str, config: &Value, sector: Option<&str>) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let label = match sector {
        Some(s) => format!("deep enrichment (sector: {})", s),
        None => "deep enrichment trial".to_string(),
    };
    let _ = db.log_activity(job_id, "deep_enrich", "info", &format!("Starting {}", label));

    let candidates = match sector {
        Some(s) => db.get_deep_enrich_candidates_by_sector(s, 50)?,
        None => db.get_deep_enrich_candidates(30)?,
    };

    run_candidates(app, job_id, config, candidates, &label).await
}

/// Result from a single parallel deep-enrich task, used for post-collection aggregation.
struct DeepEnrichItemResult {
    name: String,
    process_count: usize,
    categories: HashMap<String, u32>,
    tolerance_count: u32,
    equipment_count: u32,
}

/// Shared processing loop for deep enrichment — used by trial, sector, and all modes.
/// Uses buffer_unordered for parallel processing (concurrency configurable, default 2).
async fn run_candidates(app: &tauri::AppHandle, job_id: &str, config: &Value, candidates: Vec<Value>, label: &str) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let total = candidates.len();

    if total == 0 {
        let msg = "No candidates found for deep enrichment (need enriched/approved/pushed companies with websites)";
        log::warn!("{}", msg);
        let _ = db.log_activity(job_id, "deep_enrich", "warn", msg);
        return Ok(json!({ "error": msg }));
    }

    let concurrency: usize = config
        .get("deep_enrich_concurrency")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(2)
        .max(1)
        .min(5);

    log::info!("{}: {} candidates selected (concurrency={})", label, total, concurrency);
    let _ = db.log_activity(
        job_id,
        "deep_enrich",
        "info",
        &format!("Selected {} candidates for {} (concurrency={})", total, label, concurrency),
    );

    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let model = "qwen3.5:27b-q4_K_M";

    let llm_backend = config
        .get("llm_backend")
        .and_then(|v| v.as_str())
        .unwrap_or("deepseek")
        .to_string();

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

    let started_at = chrono::Utc::now();

    let display_model = if llm_backend == "haiku" { "claude-haiku-4-5" } else if llm_backend == "deepseek" { "deepseek-chat" } else { model };

    super::emit_node(app, json!({
        "node_id": "deep_enrich",
        "status": "running",
        "model": display_model,
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    let succeeded = Arc::new(AtomicI64::new(0));
    let failed = Arc::new(AtomicI64::new(0));

    // Process companies in parallel, each returning Option<DeepEnrichItemResult> for aggregation
    let results: Vec<Option<DeepEnrichItemResult>> = stream::iter(candidates.into_iter())
        .map(|company| {
            let app = app.clone();
            let job_id = job_id.to_string();
            let ollama_url = ollama_url.clone();
            let model_str = model.to_string();
            let llm_backend = llm_backend.clone();
            let anthropic_api_key = anthropic_api_key.clone();
            let deepseek_api_key = deepseek_api_key.clone();
            let succeeded = Arc::clone(&succeeded);
            let failed = Arc::clone(&failed);

            async move {
                if super::is_cancelled() {
                    return None;
                }

                let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
                let website = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("").to_string();

                if website.is_empty() || id.is_empty() {
                    failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }

                let cur_succeeded = succeeded.load(Ordering::Relaxed);
                let cur_failed = failed.load(Ordering::Relaxed);
                let processed = cur_succeeded + cur_failed;

                log::info!("[{}/{}] Deep enriching: {} ({})", processed + 1, total, name, website);
                {
                    let db: tauri::State<'_, Database> = app.state();
                    let _ = db.log_activity(
                        &job_id,
                        "deep_enrich",
                        "info",
                        &format!("[{}/{}] Processing: {}", processed + 1, total, name),
                    );
                }

                // Step 1: Deep scrape
                let website_text = match scraper::fetch_website_text_deep(&website).await {
                    Ok(text) => text,
                    Err(e) => {
                        log::warn!("Deep scrape failed for {}: {}", name, e);
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "deep_enrich",
                            "warn",
                            &format!("Scrape failed for {}: {}", name, e),
                        );
                        failed.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                };

                if website_text.len() < 100 {
                    log::warn!("Deep scrape too short for {} ({} chars)", name, website_text.len());
                    failed.fetch_add(1, Ordering::Relaxed);
                    return None;
                }

                // Step 2: LLM extraction
                let prompt = build_extraction_prompt(&name, &website, &website_text);
                let response = if llm_backend == "haiku" {
                    match crate::services::anthropic::chat(
                        &anthropic_api_key,
                        None,
                        &prompt,
                        true,
                    ).await {
                        Ok(r) => r,
                        Err(e) => {
                            log::warn!("[Anthropic] LLM extraction failed for {}: {}", name, e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "deep_enrich",
                                "warn",
                                &format!("[Anthropic] LLM failed for {}: {}", name, e),
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                            return None;
                        }
                    }
                } else if llm_backend == "deepseek" {
                    match crate::services::deepseek::chat(
                        &deepseek_api_key,
                        None,
                        &prompt,
                        true,
                    ).await {
                        Ok(r) => r,
                        Err(e) => {
                            log::warn!("[DeepSeek] LLM extraction failed for {}: {}", name, e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "deep_enrich",
                                "warn",
                                &format!("[DeepSeek] LLM failed for {}: {}", name, e),
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                            return None;
                        }
                    }
                } else {
                    match ollama::generate_with_ctx(&ollama_url, &model_str, &prompt, true, 16384).await {
                        Ok(r) => r,
                        Err(e) => {
                            log::warn!("LLM extraction failed for {}: {}", name, e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "deep_enrich",
                                "warn",
                                &format!("LLM failed for {}: {}", name, e),
                            );
                            failed.fetch_add(1, Ordering::Relaxed);
                            return None;
                        }
                    }
                };

                // Step 3: Parse response
                let parsed: Value = match serde_json::from_str(&response) {
                    Ok(v) => v,
                    Err(e) => {
                        let truncated: String = response.chars().take(300).collect();
                        log::warn!("JSON parse failed for {}: {} — response: {}", name, e, truncated);
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(
                            &job_id,
                            "deep_enrich",
                            "warn",
                            &format!("JSON parse failed for {}: {}", name, e),
                        );
                        failed.fetch_add(1, Ordering::Relaxed);
                        return None;
                    }
                };

                // Extract the processes array
                let processes = parsed
                    .get("processes")
                    .and_then(|v| v.as_array())
                    .cloned()
                    .unwrap_or_default();

                let process_count = processes.len();

                // Track per-item stats for post-aggregation
                let mut categories: HashMap<String, u32> = HashMap::new();
                let mut tolerance_count = 0u32;
                let mut equipment_count = 0u32;

                for process in &processes {
                    if let Some(cat) = process.get("process_category").and_then(|v| v.as_str()) {
                        *categories.entry(cat.to_string()).or_insert(0) += 1;
                    }
                    if process.get("tolerance_value_mm").and_then(|v| v.as_f64()).is_some() {
                        tolerance_count += 1;
                    }
                    let equip = process
                        .get("equipment_mentioned")
                        .and_then(|v| v.as_array())
                        .map(|a| a.len())
                        .unwrap_or(0);
                    if equip > 0 {
                        equipment_count += 1;
                    }
                }

                // Step 4: Save to DB
                let capabilities_json = serde_json::to_string(&processes).unwrap_or_else(|_| "[]".to_string());
                {
                    let db: tauri::State<'_, Database> = app.state();
                    match db.update_deep_enrichment(&id, &capabilities_json, &website_text) {
                        Ok(()) => {
                            succeeded.fetch_add(1, Ordering::Relaxed);
                            log::info!("  {} — {} processes extracted", name, process_count);
                        }
                        Err(e) => {
                            log::warn!("DB save failed for {}: {}", name, e);
                            failed.fetch_add(1, Ordering::Relaxed);
                            return None;
                        }
                    }
                }

                // Emit progress event (throttled)
                let cur_succeeded = succeeded.load(Ordering::Relaxed);
                let cur_failed = failed.load(Ordering::Relaxed);
                let processed = (cur_succeeded + cur_failed) as usize;
                if processed % 5 == 0 || processed == 1 {
                    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                    let rate = if elapsed > 0 { cur_succeeded as f64 / elapsed as f64 * 3600.0 } else { 0.0 };
                    super::emit_node(&app, json!({
                        "node_id": "deep_enrich",
                        "status": "running",
                        "model": &model_str,
                        "progress": { "current": processed, "total": total, "rate": rate, "current_item": &name },
                        "concurrency": concurrency,
                        "started_at": started_at.to_rfc3339(),
                        "elapsed_secs": elapsed
                    }));
                }

                Some(DeepEnrichItemResult {
                    name,
                    process_count,
                    categories,
                    tolerance_count,
                    equipment_count,
                })
            }
        })
        .buffer_unordered(concurrency)
        .collect()
        .await;

    // Aggregate results from parallel tasks
    let final_succeeded = succeeded.load(Ordering::Relaxed) as u32;
    let final_failed = failed.load(Ordering::Relaxed) as u32;
    let mut total_processes = 0u32;
    let mut category_counts: HashMap<String, u32> = HashMap::new();
    let mut with_tolerance = 0u32;
    let mut with_equipment = 0u32;
    let mut best_extraction: Option<(String, usize)> = None;
    let mut worst_extraction: Option<(String, usize)> = None;

    for result in results.into_iter().flatten() {
        total_processes += result.process_count as u32;

        for (cat, count) in &result.categories {
            *category_counts.entry(cat.clone()).or_insert(0) += count;
        }
        with_tolerance += result.tolerance_count;
        with_equipment += result.equipment_count;

        match &best_extraction {
            None => best_extraction = Some((result.name.clone(), result.process_count)),
            Some((_, best_count)) if result.process_count > *best_count => {
                best_extraction = Some((result.name.clone(), result.process_count));
            }
            _ => {}
        }
        match &worst_extraction {
            None if result.process_count > 0 => worst_extraction = Some((result.name.clone(), result.process_count)),
            Some((_, worst_count)) if result.process_count < *worst_count && result.process_count > 0 => {
                worst_extraction = Some((result.name.clone(), result.process_count));
            }
            None => worst_extraction = Some((result.name, result.process_count)),
            _ => {}
        }
    }

    // Print summary report
    let avg_processes = if final_succeeded > 0 {
        total_processes as f64 / final_succeeded as f64
    } else {
        0.0
    };
    let tolerance_pct = if total_processes > 0 {
        (with_tolerance as f64 / total_processes as f64 * 100.0) as u32
    } else {
        0
    };
    let equipment_pct = if total_processes > 0 {
        (with_equipment as f64 / total_processes as f64 * 100.0) as u32
    } else {
        0
    };

    let mut report = String::new();
    report.push_str("\n╔══════════════════════════════════════════════╗\n");
    report.push_str("║     DEEP ENRICHMENT — SUMMARY REPORT         ║\n");
    report.push_str("╠══════════════════════════════════════════════╣\n");
    report.push_str(&format!("║ Companies processed: {:<24}║\n", format!("{}/{}", final_succeeded + final_failed, total)));
    report.push_str(&format!("║ Succeeded:           {:<24}║\n", final_succeeded));
    report.push_str(&format!("║ Failed:              {:<24}║\n", final_failed));
    report.push_str(&format!("║ Concurrency:         {:<24}║\n", concurrency));
    report.push_str(&format!("║ Total processes:     {:<24}║\n", total_processes));
    report.push_str(&format!("║ Avg processes/co:    {:<24}║\n", format!("{:.1}", avg_processes)));
    report.push_str("╠══════════════════════════════════════════════╣\n");
    report.push_str("║ PROCESS CATEGORY DISTRIBUTION                ║\n");
    report.push_str("╠══════════════════════════════════════════════╣\n");

    let mut sorted_cats: Vec<_> = category_counts.iter().collect();
    sorted_cats.sort_by(|a, b| b.1.cmp(a.1));
    for (cat, count) in &sorted_cats {
        report.push_str(&format!("║   {:<30} {:>10} ║\n", cat, count));
    }
    if sorted_cats.is_empty() {
        report.push_str("║   (none)                                     ║\n");
    }

    report.push_str("╠══════════════════════════════════════════════╣\n");
    report.push_str("║ DATA COVERAGE                                ║\n");
    report.push_str("╠══════════════════════════════════════════════╣\n");
    report.push_str(&format!("║ With numeric tolerance: {:<22}║\n", format!("{}%", tolerance_pct)));
    report.push_str(&format!("║ With brand+model equip: {:<21}║\n", format!("{}%", equipment_pct)));

    if let Some((name, count)) = &best_extraction {
        report.push_str("╠══════════════════════════════════════════════╣\n");
        report.push_str(&format!("║ Best:  {} ({} processes)\n", name, count));
    }
    if let Some((name, count)) = &worst_extraction {
        report.push_str(&format!("║ Worst: {} ({} processes)\n", name, count));
    }

    report.push_str("╚══════════════════════════════════════════════╝\n");

    log::info!("{}", report);
    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "deep_enrich", "info", &report);
    }

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    super::emit_node(app, json!({
        "node_id": "deep_enrich",
        "status": "completed",
        "model": model,
        "progress": { "current": final_succeeded + final_failed, "total": total, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    Ok(json!({
        "total": total,
        "succeeded": final_succeeded,
        "failed": final_failed,
        "concurrency": concurrency,
        "total_processes": total_processes,
        "avg_processes_per_company": avg_processes,
        "tolerance_coverage_pct": tolerance_pct,
        "equipment_coverage_pct": equipment_pct,
        "category_distribution": category_counts,
    }))
}

fn build_extraction_prompt(name: &str, website: &str, website_text: &str) -> String {
    format!(
        r#"You are a manufacturing process analyst. Extract EVERY specific manufacturing process, capability, and technical specification from this company's website text.

Return JSON with a single key "processes" containing an array. Each element represents ONE distinct manufacturing process or capability:

{{
  "processes": [
    {{
      "process_category": "<one of: cnc_machining, injection_moulding, additive_manufacturing, sheet_metal, casting, forging, welding, surface_treatment, heat_treatment, assembly, metrology, other>",
      "process_name": "<specific name, e.g. 'CNC 5-Axis Milling', 'SLS Nylon Printing', 'MIG Welding'>",
      "materials_worked": ["<with grades where mentioned, e.g. 'Aluminium 6061-T6', 'Stainless Steel 316L'>"],
      "tolerance_claimed": "<exact text from website, e.g. '±0.01mm', 'within 0.005\"'>",
      "tolerance_value_mm": <numeric in mm, or null if not stated>,
      "surface_finish_claimed": "<exact text, e.g. 'Ra 0.8μm', '32 microinch'>",
      "surface_finish_ra_um": <numeric in μm, or null if not stated>,
      "max_part_dimensions": "<if mentioned, e.g. '2000mm x 1500mm x 800mm'>",
      "batch_size_range": "<if mentioned, e.g. '1-10000', 'prototype to production'>",
      "equipment_mentioned": ["<brand+model ONLY if specifically named, e.g. 'DMG Mori DMU 50', 'EOS M 290'>"],
      "surface_treatments": ["<post-processing offered, e.g. 'anodising', 'powder coating', 'electroless nickel'>"],
      "confidence": <0.0-1.0, how confident you are this process is actually offered>,
      "source_excerpt": "<copy the EXACT sentence(s) from the website text that support this claim, max 200 chars>"
    }}
  ]
}}

RULES:
- Only extract processes actually offered by this company. Do NOT invent capabilities.
- If the website says "CNC machining" with no other detail, still include it but with null tolerances/finishes and confidence 0.5.
- If specific tolerances, finishes, or equipment are mentioned, confidence should be 0.8-1.0.
- surface_treatments is a separate category but also list treatments as part of the process that offers them.
- Return an empty array if no manufacturing processes are evident.
- source_excerpt MUST be actual text from the website, not paraphrased.

Company: {name}
Website: {website}

--- WEBSITE TEXT ---
{website_text}
--- END ---

Return ONLY valid JSON."#,
        name = name,
        website = website,
        website_text = website_text,
    )
}
