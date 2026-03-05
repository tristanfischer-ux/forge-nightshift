use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashMap;
use tauri::Manager;

use crate::db::Database;
use crate::services::{ollama, scraper};

/// Run the deep enrichment trial on ~30 companies.
/// Selects a diverse mix, re-scrapes with 16k char limit,
/// extracts structured manufacturing process data via LLM,
/// saves to DB, and prints a summary report.
pub async fn run_trial(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(job_id, "deep_enrich", "info", "Starting deep enrichment trial");

    let candidates = db.get_deep_enrich_candidates(30)?;
    let total = candidates.len();

    if total == 0 {
        let msg = "No candidates found for deep enrichment (need enriched/approved/pushed companies with websites)";
        log::warn!("{}", msg);
        let _ = db.log_activity(job_id, "deep_enrich", "warn", msg);
        return Ok(json!({ "error": msg }));
    }

    log::info!("Deep enrichment trial: {} candidates selected", total);
    let _ = db.log_activity(
        job_id,
        "deep_enrich",
        "info",
        &format!("Selected {} candidates for deep enrichment", total),
    );

    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let model = "qwen3.5:27b-q4_K_M";

    let mut succeeded = 0u32;
    let mut failed = 0u32;
    let mut total_processes = 0u32;
    let mut category_counts: HashMap<String, u32> = HashMap::new();
    let mut with_tolerance = 0u32;
    let mut with_equipment = 0u32;
    let mut best_extraction: Option<(String, usize)> = None; // (company name, process count)
    let mut worst_extraction: Option<(String, usize)> = None;

    for (i, company) in candidates.iter().enumerate() {
        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
        let website = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("");

        if website.is_empty() || id.is_empty() {
            failed += 1;
            continue;
        }

        log::info!("[{}/{}] Deep enriching: {} ({})", i + 1, total, name, website);
        let _ = db.log_activity(
            job_id,
            "deep_enrich",
            "info",
            &format!("[{}/{}] Processing: {}", i + 1, total, name),
        );

        // Check cancellation
        if super::is_cancelled() {
            let _ = db.log_activity(job_id, "deep_enrich", "warn", "Cancelled by user");
            break;
        }

        // Step 1: Deep scrape
        let website_text = match scraper::fetch_website_text_deep(website).await {
            Ok(text) => text,
            Err(e) => {
                log::warn!("Deep scrape failed for {}: {}", name, e);
                let _ = db.log_activity(
                    job_id,
                    "deep_enrich",
                    "warn",
                    &format!("Scrape failed for {}: {}", name, e),
                );
                failed += 1;
                continue;
            }
        };

        if website_text.len() < 100 {
            log::warn!("Deep scrape too short for {} ({} chars)", name, website_text.len());
            failed += 1;
            continue;
        }

        // Step 2: LLM extraction
        let prompt = build_extraction_prompt(name, website, &website_text);
        let response = match ollama::generate_with_ctx(ollama_url, model, &prompt, true, 16384).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("LLM extraction failed for {}: {}", name, e);
                let _ = db.log_activity(
                    job_id,
                    "deep_enrich",
                    "warn",
                    &format!("LLM failed for {}: {}", name, e),
                );
                failed += 1;
                continue;
            }
        };

        // Step 3: Parse response
        let parsed: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(e) => {
                let truncated: String = response.chars().take(300).collect();
                log::warn!("JSON parse failed for {}: {} — response: {}", name, e, truncated);
                let _ = db.log_activity(
                    job_id,
                    "deep_enrich",
                    "warn",
                    &format!("JSON parse failed for {}: {}", name, e),
                );
                failed += 1;
                continue;
            }
        };

        // Extract the processes array
        let processes = parsed
            .get("processes")
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();

        let process_count = processes.len();
        total_processes += process_count as u32;

        // Track category distribution
        for process in &processes {
            if let Some(cat) = process.get("process_category").and_then(|v| v.as_str()) {
                *category_counts.entry(cat.to_string()).or_insert(0) += 1;
            }
            if process.get("tolerance_value_mm").and_then(|v| v.as_f64()).is_some() {
                with_tolerance += 1;
            }
            let equipment = process
                .get("equipment_mentioned")
                .and_then(|v| v.as_array())
                .map(|a| a.len())
                .unwrap_or(0);
            if equipment > 0 {
                with_equipment += 1;
            }
        }

        // Track best/worst
        match &best_extraction {
            None => best_extraction = Some((name.to_string(), process_count)),
            Some((_, best_count)) if process_count > *best_count => {
                best_extraction = Some((name.to_string(), process_count));
            }
            _ => {}
        }
        match &worst_extraction {
            None if process_count > 0 => worst_extraction = Some((name.to_string(), process_count)),
            Some((_, worst_count)) if process_count < *worst_count && process_count > 0 => {
                worst_extraction = Some((name.to_string(), process_count));
            }
            None => worst_extraction = Some((name.to_string(), process_count)),
            _ => {}
        }

        // Step 4: Save to DB
        let capabilities_json = serde_json::to_string(&processes).unwrap_or_else(|_| "[]".to_string());
        match db.update_deep_enrichment(id, &capabilities_json, &website_text) {
            Ok(()) => {
                succeeded += 1;
                log::info!(
                    "  {} — {} processes extracted",
                    name,
                    process_count
                );
            }
            Err(e) => {
                log::warn!("DB save failed for {}: {}", name, e);
                failed += 1;
            }
        }
    }

    // Print summary report
    let _companies_with_processes = if total_processes > 0 { succeeded } else { 0 };
    let avg_processes = if succeeded > 0 {
        total_processes as f64 / succeeded as f64
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
    report.push_str("║     DEEP ENRICHMENT TRIAL — SUMMARY REPORT   ║\n");
    report.push_str("╠══════════════════════════════════════════════╣\n");
    report.push_str(&format!("║ Companies processed: {:<24}║\n", format!("{}/{}", succeeded + failed, total)));
    report.push_str(&format!("║ Succeeded:           {:<24}║\n", succeeded));
    report.push_str(&format!("║ Failed:              {:<24}║\n", failed));
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
    let _ = db.log_activity(job_id, "deep_enrich", "info", &report);

    Ok(json!({
        "total": total,
        "succeeded": succeeded,
        "failed": failed,
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
