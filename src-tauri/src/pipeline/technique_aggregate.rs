use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashMap;
use tauri::Manager;

use crate::db::Database;
use crate::services::{ollama, supabase};

/// Map process_category values from deep_enrich to ForgeOS technique slugs.
/// Returns a vec of possible technique slugs for a given process category.
fn category_to_technique_slugs(category: &str, process_name: &str) -> Vec<String> {
    let name_lower = process_name.to_lowercase();
    match category {
        "cnc_machining" => {
            if name_lower.contains("5-axis") || name_lower.contains("5 axis") || name_lower.contains("five axis") {
                vec!["cnc-milling-5-axis".into()]
            } else if name_lower.contains("turn") || name_lower.contains("lathe") {
                vec!["cnc-turning".into()]
            } else if name_lower.contains("edm") || name_lower.contains("electrical discharge") {
                vec!["wire-edm".into()]
            } else if name_lower.contains("grind") {
                vec!["precision-grinding".into()]
            } else {
                vec!["cnc-milling-3-axis".into()]
            }
        }
        "sheet_metal" => {
            if name_lower.contains("laser") {
                vec!["laser-cutting".into()]
            } else if name_lower.contains("stamp") || name_lower.contains("press") {
                vec!["sheet-metal-stamping".into()]
            } else if name_lower.contains("bend") || name_lower.contains("fold") {
                vec!["sheet-metal-bending".into()]
            } else if name_lower.contains("water") || name_lower.contains("waterjet") {
                vec!["waterjet-cutting".into()]
            } else {
                vec!["sheet-metal-bending".into(), "laser-cutting".into()]
            }
        }
        "injection_moulding" => vec!["injection-molding".into()],
        "additive_manufacturing" => {
            if name_lower.contains("fdm") || name_lower.contains("fff") || name_lower.contains("fused") {
                vec!["fdm".into()]
            } else if name_lower.contains("sla") || name_lower.contains("stereolith") {
                vec!["sla".into()]
            } else if name_lower.contains("sls") || name_lower.contains("selective laser sinter") {
                vec!["sls".into()]
            } else if name_lower.contains("slm") || name_lower.contains("dmls") || name_lower.contains("metal") {
                vec!["slm".into()]
            } else if name_lower.contains("mjf") || name_lower.contains("multi jet") {
                vec!["multi-jet-fusion".into()]
            } else if name_lower.contains("binder") {
                vec!["metal-binder-jetting".into()]
            } else if name_lower.contains("ebm") || name_lower.contains("electron beam") {
                vec!["ebm".into()]
            } else {
                vec!["fdm".into()]
            }
        }
        "casting" => {
            if name_lower.contains("investment") || name_lower.contains("lost wax") {
                vec!["investment-casting".into()]
            } else if name_lower.contains("die") {
                vec!["die-casting".into()]
            } else {
                vec!["sand-casting".into()]
            }
        }
        "forging" => vec!["metal-forging".into()],
        "welding" => {
            if name_lower.contains("tig") {
                vec!["tig-welding".into()]
            } else if name_lower.contains("mig") {
                vec!["mig-welding".into()]
            } else if name_lower.contains("laser") {
                vec!["laser-welding".into()]
            } else {
                vec!["mig-welding".into(), "tig-welding".into()]
            }
        }
        "surface_treatment" => {
            if name_lower.contains("anod") {
                vec!["anodizing".into()]
            } else if name_lower.contains("powder coat") {
                vec!["powder-coating".into()]
            } else if name_lower.contains("plat") || name_lower.contains("electroplat") {
                vec!["electroplating".into()]
            } else if name_lower.contains("polish") {
                vec!["electropolishing".into()]
            } else {
                vec!["powder-coating".into()]
            }
        }
        "heat_treatment" => vec![],
        "assembly" => vec![],
        "metrology" => vec![],
        _ => vec![],
    }
}

/// Aggregate technique knowledge from deep-enriched company data.
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(job_id, "aggregate_techniques", "info", "Starting technique aggregation");

    // Get all deep-enriched companies
    let companies = db.get_deep_enriched_processes(None)?;
    if companies.is_empty() {
        let msg = "No deep-enriched companies found for aggregation";
        log::warn!("{}", msg);
        let _ = db.log_activity(job_id, "aggregate_techniques", "warn", msg);
        return Ok(json!({ "error": msg }));
    }

    log::info!("Aggregating techniques from {} companies", companies.len());

    // Group processes by technique slug
    let mut technique_data: HashMap<String, Vec<Value>> = HashMap::new();
    let mut technique_companies: HashMap<String, Vec<String>> = HashMap::new();

    for company in &companies {
        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let capabilities_str = company.get("process_capabilities_json").and_then(|v| v.as_str()).unwrap_or("[]");
        let processes: Vec<Value> = serde_json::from_str(capabilities_str).unwrap_or_default();

        for process in processes {
            let category = process.get("process_category").and_then(|v| v.as_str()).unwrap_or("");
            let process_name = process.get("process_name").and_then(|v| v.as_str()).unwrap_or("");
            let slugs = category_to_technique_slugs(category, process_name);

            for slug in slugs {
                technique_data.entry(slug.clone()).or_default().push(process.clone());
                let companies_list = technique_companies.entry(slug).or_default();
                if !companies_list.contains(&company_id) {
                    companies_list.push(company_id.clone());
                }
            }
        }
    }

    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let model = "qwen3.5:27b-q4_K_M";

    let mut generated = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;
    let techniques_to_process: Vec<_> = technique_data.keys().cloned().collect();
    let total = techniques_to_process.len();

    let started_at = chrono::Utc::now();
    super::emit_node(app, json!({
        "node_id": "aggregate_techniques",
        "status": "running",
        "model": model,
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    for (i, slug) in techniques_to_process.iter().enumerate() {
        if super::is_cancelled() {
            let _ = db.log_activity(job_id, "aggregate_techniques", "warn", "Cancelled by user");
            break;
        }

        let processes = &technique_data[slug];
        let company_ids = &technique_companies[slug];

        // Need at least 2 data points for meaningful aggregation
        if company_ids.len() < 2 {
            log::info!("[{}/{}] Skipping {} — only {} supplier(s)", i + 1, total, slug, company_ids.len());
            skipped += 1;
            continue;
        }

        log::info!("[{}/{}] Aggregating: {} ({} processes from {} suppliers)", i + 1, total, slug, processes.len(), company_ids.len());
        let _ = db.log_activity(
            job_id,
            "aggregate_techniques",
            "info",
            &format!("[{}/{}] Aggregating: {} ({} suppliers)", i + 1, total, slug, company_ids.len()),
        );

        // Build aggregation prompt
        let prompt = build_aggregation_prompt(slug, processes);
        let response = match ollama::generate_with_ctx(ollama_url, model, &prompt, true, 16384).await {
            Ok(r) => r,
            Err(e) => {
                log::warn!("LLM aggregation failed for {}: {}", slug, e);
                let _ = db.log_activity(
                    job_id,
                    "aggregate_techniques",
                    "warn",
                    &format!("LLM failed for {}: {}", slug, e),
                );
                failed += 1;
                continue;
            }
        };

        let parsed: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(e) => {
                let truncated: String = response.chars().take(300).collect();
                log::warn!("JSON parse failed for {}: {} — response: {}", slug, e, truncated);
                failed += 1;
                continue;
            }
        };

        // Build the record
        let record_id = format!("{}_{}", slug, "all");
        let record = json!({
            "id": record_id,
            "technique_slug": slug,
            "sector": "all",
            "article_markdown": parsed.get("article_markdown").and_then(|v| v.as_str()),
            "real_world_tolerances": parsed.get("real_world_tolerances"),
            "real_world_materials": parsed.get("real_world_materials"),
            "real_world_equipment": parsed.get("real_world_equipment"),
            "real_world_surface_finishes": parsed.get("real_world_surface_finishes"),
            "typical_batch_sizes": parsed.get("typical_batch_sizes"),
            "tips_and_insights": parsed.get("tips_and_insights"),
            "common_applications": parsed.get("common_applications"),
            "supplier_count": company_ids.len(),
            "source_company_ids": company_ids,
        });

        match db.upsert_technique_knowledge(&record) {
            Ok(()) => {
                generated += 1;
                log::info!("  {} — article generated ({} suppliers)", slug, company_ids.len());

                let processed = (generated + skipped + failed) as usize;
                let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                super::emit_node(app, json!({
                    "node_id": "aggregate_techniques",
                    "status": "running",
                    "model": model,
                    "progress": { "current": processed, "total": total, "rate": null, "current_item": slug },
                    "concurrency": 1,
                    "started_at": started_at.to_rfc3339(),
                    "elapsed_secs": elapsed
                }));
            }
            Err(e) => {
                log::warn!("DB save failed for {}: {}", slug, e);
                failed += 1;
            }
        }
    }

    // Summary
    let summary = json!({
        "total_techniques": total,
        "generated": generated,
        "skipped_insufficient_data": skipped,
        "failed": failed,
        "companies_analysed": companies.len(),
    });

    log::info!("Technique aggregation complete: {} generated, {} skipped, {} failed", generated, skipped, failed);
    let _ = db.log_activity(
        job_id,
        "aggregate_techniques",
        "info",
        &format!("Complete: {} generated, {} skipped, {} failed from {} companies", generated, skipped, failed, companies.len()),
    );

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    super::emit_node(app, json!({
        "node_id": "aggregate_techniques",
        "status": "completed",
        "model": model,
        "progress": { "current": total, "total": total, "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    Ok(summary)
}

fn build_aggregation_prompt(technique_slug: &str, processes: &[Value]) -> String {
    // Gather all the raw data points
    let mut materials: Vec<String> = Vec::new();
    let mut tolerances: Vec<String> = Vec::new();
    let mut tolerance_values: Vec<f64> = Vec::new();
    let mut equipment: Vec<String> = Vec::new();
    let mut finishes: Vec<String> = Vec::new();
    let mut finish_values: Vec<f64> = Vec::new();
    let mut batch_sizes: Vec<String> = Vec::new();
    let mut excerpts: Vec<String> = Vec::new();
    let mut treatments: Vec<String> = Vec::new();

    for p in processes {
        if let Some(mats) = p.get("materials_worked").and_then(|v| v.as_array()) {
            for m in mats {
                if let Some(s) = m.as_str() {
                    materials.push(s.to_string());
                }
            }
        }
        if let Some(t) = p.get("tolerance_claimed").and_then(|v| v.as_str()) {
            tolerances.push(t.to_string());
        }
        if let Some(tv) = p.get("tolerance_value_mm").and_then(|v| v.as_f64()) {
            tolerance_values.push(tv);
        }
        if let Some(equips) = p.get("equipment_mentioned").and_then(|v| v.as_array()) {
            for e in equips {
                if let Some(s) = e.as_str() {
                    equipment.push(s.to_string());
                }
            }
        }
        if let Some(f) = p.get("surface_finish_claimed").and_then(|v| v.as_str()) {
            finishes.push(f.to_string());
        }
        if let Some(fv) = p.get("surface_finish_ra_um").and_then(|v| v.as_f64()) {
            finish_values.push(fv);
        }
        if let Some(b) = p.get("batch_size_range").and_then(|v| v.as_str()) {
            batch_sizes.push(b.to_string());
        }
        if let Some(e) = p.get("source_excerpt").and_then(|v| v.as_str()) {
            excerpts.push(e.to_string());
        }
        if let Some(treats) = p.get("surface_treatments").and_then(|v| v.as_array()) {
            for t in treats {
                if let Some(s) = t.as_str() {
                    treatments.push(s.to_string());
                }
            }
        }
    }

    let data_summary = format!(
        r#"Technique: {slug}
Number of supplier data points: {count}

Materials mentioned across suppliers: {materials}
Tolerances claimed: {tolerances}
Tolerance values (mm): {tolerance_values:?}
Equipment mentioned: {equipment}
Surface finishes claimed: {finishes}
Surface finish values (Ra μm): {finish_values:?}
Batch sizes offered: {batch_sizes}
Surface treatments offered: {treatments}

Real excerpts from supplier websites:
{excerpts}"#,
        slug = technique_slug,
        count = processes.len(),
        materials = if materials.is_empty() { "none".to_string() } else { materials.join(", ") },
        tolerances = if tolerances.is_empty() { "none".to_string() } else { tolerances.join(", ") },
        tolerance_values = tolerance_values,
        equipment = if equipment.is_empty() { "none".to_string() } else { equipment.join(", ") },
        finishes = if finishes.is_empty() { "none".to_string() } else { finishes.join(", ") },
        finish_values = finish_values,
        batch_sizes = if batch_sizes.is_empty() { "none".to_string() } else { batch_sizes.join(", ") },
        treatments = if treatments.is_empty() { "none".to_string() } else { treatments.join(", ") },
        excerpts = excerpts.iter().take(20).enumerate().map(|(i, e)| format!("  {}. {}", i + 1, e)).collect::<Vec<_>>().join("\n"),
    );

    format!(
        r#"You are a manufacturing technology writer. Using the real-world data below from {count} UK manufacturing suppliers, write a comprehensive knowledge article about this manufacturing technique.

Return JSON with these keys:

{{
  "article_markdown": "<500-1500 word article in markdown. Cover: what this technique is, how it works in practice, typical applications, materials commonly processed, achievable tolerances and surface finishes, key considerations for designers. Write for an engineer or product designer audience. Use the real supplier data to ground your claims — cite specific numbers and materials. Do NOT name individual companies.>",
  "real_world_tolerances": {{
    "min_mm": <smallest tolerance value seen>,
    "max_mm": <largest tolerance value seen>,
    "typical_mm": <most common/median value>,
    "notes": "<brief note on what affects tolerance>"
  }},
  "real_world_materials": [
    {{ "material": "<name with grade>", "frequency": <how many suppliers mention it> }}
  ],
  "real_world_equipment": [
    {{ "brand_model": "<brand and model>", "frequency": <how many suppliers mention it> }}
  ],
  "real_world_surface_finishes": {{
    "min_ra_um": <best finish seen>,
    "max_ra_um": <roughest finish seen>,
    "typical_ra_um": <most common>,
    "notes": "<what affects finish quality>"
  }},
  "typical_batch_sizes": {{
    "prototype": <true/false — any supplier offers prototypes>,
    "low_volume": "<typical low volume range>",
    "production": "<typical production range>",
    "notes": "<what affects batch economics>"
  }},
  "tips_and_insights": ["<practical tip 1>", "<practical tip 2>", ...],
  "common_applications": ["<application 1>", "<application 2>", ...]
}}

RULES:
- Base EVERYTHING on the real data provided. Do not invent data.
- If a field has no data, use null or empty arrays.
- Materials should be ranked by frequency (most common first).
- Equipment should only include specific brand+model mentions, not generic terms.
- Tips should be practical advice for someone designing parts for this process.
- Article should reference specific numbers from the data (e.g., "tolerances of ±0.05mm are achievable").

--- SUPPLIER DATA ---
{data}
--- END ---

Return ONLY valid JSON."#,
        count = processes.len(),
        data = data_summary,
    )
}

/// Push all unpushed technique knowledge records to Supabase.
pub async fn push_techniques(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(job_id, "push_techniques", "info", "Starting technique knowledge push to Supabase");

    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if supabase_url.is_empty() || supabase_key.is_empty() {
        let msg = "Supabase URL or service key not configured";
        log::warn!("{}", msg);
        return Ok(json!({ "error": msg }));
    }

    let records = db.get_unpushed_technique_knowledge()?;
    if records.is_empty() {
        let msg = "No unpushed technique knowledge records found";
        log::info!("{}", msg);
        let _ = db.log_activity(job_id, "push_techniques", "info", msg);
        return Ok(json!({ "pushed": 0, "message": msg }));
    }

    log::info!("Pushing {} technique knowledge records to Supabase", records.len());

    let started_at = chrono::Utc::now();
    super::emit_node(app, json!({
        "node_id": "push_techniques",
        "status": "running",
        "model": null,
        "progress": { "current": 0, "total": records.len(), "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    let mut pushed = 0u32;
    let mut errors = 0u32;

    for record in &records {
        if super::is_cancelled() {
            let _ = db.log_activity(job_id, "push_techniques", "warn", "Cancelled by user");
            break;
        }

        let slug = record.get("technique_slug").and_then(|v| v.as_str()).unwrap_or("unknown");
        let id = record.get("id").and_then(|v| v.as_str()).unwrap_or("");

        match supabase::push_technique_enrichment(supabase_url, supabase_key, record).await {
            Ok(()) => {
                let _ = db.mark_technique_pushed(id);
                pushed += 1;
                log::info!("  Pushed technique: {}", slug);

                super::emit_node(app, json!({
                    "node_id": "push_techniques",
                    "status": "running",
                    "model": null,
                    "progress": { "current": pushed + errors, "total": records.len(), "rate": null, "current_item": slug },
                    "concurrency": 1,
                    "started_at": started_at.to_rfc3339(),
                    "elapsed_secs": (chrono::Utc::now() - started_at).num_seconds()
                }));
            }
            Err(e) => {
                log::warn!("Failed to push {}: {}", slug, e);
                let _ = db.log_activity(
                    job_id,
                    "push_techniques",
                    "warn",
                    &format!("Failed to push {}: {}", slug, e),
                );
                errors += 1;
            }
        }

        // Rate limit
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    let summary = json!({
        "total": records.len(),
        "pushed": pushed,
        "errors": errors,
    });

    log::info!("Technique push complete: {}/{} pushed", pushed, records.len());
    let _ = db.log_activity(
        job_id,
        "push_techniques",
        "info",
        &format!("Complete: {}/{} pushed, {} errors", pushed, records.len(), errors),
    );

    super::emit_node(app, json!({
        "node_id": "push_techniques",
        "status": "completed",
        "model": null,
        "progress": { "current": records.len(), "total": records.len(), "rate": null, "current_item": null },
        "concurrency": 1,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": (chrono::Utc::now() - started_at).num_seconds()
    }));

    Ok(summary)
}
