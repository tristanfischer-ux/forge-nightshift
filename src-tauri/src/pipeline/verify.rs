use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::db::Database;

const BATCH_SIZE: i64 = 20;

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let llm_backend = config
        .get("llm_backend")
        .and_then(|v| v.as_str())
        .unwrap_or("haiku")
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

    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434")
        .to_string();

    let enrich_model = config
        .get("enrich_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3.5:27b-q4_K_M")
        .to_string();

    let concurrency: usize = config
        .get("verify_concurrency")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1)
        .min(10);

    let verified_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));
    let corrections_count = Arc::new(AtomicI64::new(0));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "verify",
            "info",
            &format!(
                "Starting verification (concurrency={}, batch={}) using backend: {}",
                concurrency, BATCH_SIZE, llm_backend
            ),
        );
    }

    let started_at = chrono::Utc::now();

    super::emit_node(app, json!({
        "node_id": "verify",
        "status": "running",
        "progress": { "current": 0, "total": null, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    // Drain loop: keep pulling batches until queue is empty
    loop {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "verify", "warn", "Verification cancelled by user");
            break;
        }

        let companies = {
            let db: tauri::State<'_, Database> = app.state();
            db.get_verifiable_companies(BATCH_SIZE)?
        };

        if companies.is_empty() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "verify", "info", "Queue empty — verification finished");
            break;
        }

        let batch_size = companies.len();
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "verify",
                "info",
                &format!("Processing batch of {} companies", batch_size),
            );
        }

        stream::iter(companies.into_iter())
            .map(|company| {
                let app = app.clone();
                let job_id = job_id.to_string();
                let llm_backend = llm_backend.clone();
                let anthropic_api_key = anthropic_api_key.clone();
                let deepseek_api_key = deepseek_api_key.clone();
                let ollama_url = ollama_url.clone();
                let enrich_model = enrich_model.clone();
                let verified_count = Arc::clone(&verified_count);
                let error_count = Arc::clone(&error_count);
                let corrections_count = Arc::clone(&corrections_count);

                async move {
                    if super::is_cancelled() {
                        return;
                    }

                    let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let website = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("").to_string();

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(&job_id, "verify", "info", &format!("[Verify] Verifying: {}", name));
                    }

                    let cur_verified = verified_count.load(Ordering::Relaxed);
                    let cur_errors = error_count.load(Ordering::Relaxed);

                    let _ = app.emit(
                        "pipeline:progress",
                        json!({
                            "stage": "verify",
                            "phase": "start",
                            "current_company": name,
                            "verified": cur_verified,
                            "errors": cur_errors,
                        }),
                    );

                    // Step 1: Re-scrape the website for fresh content
                    let website_text = match crate::services::scraper::fetch_website_text(&website).await {
                        Ok(text) => text,
                        Err(e) => {
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "verify",
                                "warn",
                                &format!("[Verify] Scrape failed for {}: {} — marking verified with no corrections", name, e),
                            );
                            // Still mark as verified even if scrape fails (confirms we tried)
                            let changes = json!({
                                "verified_at": chrono::Utc::now().to_rfc3339(),
                                "scrape_failed": true,
                                "error": e.to_string(),
                            });
                            let _ = db.mark_verified(&id, &changes.to_string());
                            verified_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // Step 2: Build the verification prompt
                    let description = company.get("description").and_then(|v| v.as_str()).unwrap_or("MISSING");
                    let category = company.get("category").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let subcategory = company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let certifications = company.get("certifications").and_then(|v| v.as_str()).unwrap_or("NONE RECORDED");
                    let company_size = company.get("company_size").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let contact_name = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let contact_title = company.get("contact_title").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let address = company.get("address").and_then(|v| v.as_str()).unwrap_or("MISSING");
                    let country = company.get("country").and_then(|v| v.as_str()).unwrap_or("UNKNOWN");
                    let relevance = company.get("relevance_score").and_then(|v| v.as_i64()).unwrap_or(0);
                    let quality = company.get("enrichment_quality").and_then(|v| v.as_i64()).unwrap_or(0);

                    let system_prompt = "You are verifying a manufacturing company's data against their own website. \
                        The website is the SINGLE SOURCE OF TRUTH. Only report what is evidenced on the website. \
                        Do NOT guess or infer information that isn't explicitly stated. \
                        Use the company's own words for descriptions — do not paraphrase into formal third-person. \
                        Return valid JSON only. /no_think";

                    let user_prompt = format!(
                        r#"EXISTING DATABASE RECORD:
Company: {name}
Website: {website}
Country: {country}
Description: {description}
Category: {category} / {subcategory}
Certifications: {certifications}
Company Size: {company_size}
Contact: {contact_name} - {contact_title} - {contact_email}
Address: {address}
Current Relevance Score: {relevance}
Current Quality Score: {quality}

FRESH WEBSITE CONTENT:
{website_text}

Compare the database record against the website. Return JSON with these sections:

1. "corrections" — fields that need updating (only include fields that are WRONG or significantly incomplete):
   {{
     "description": "corrected description using the company's own words",
     "certifications": ["ISO 9001:2015", ...],
     "company_size": "50-99",
     "address": "full address with postcode",
     "contact_name": "...",
     "contact_email": "...",
     "contact_title": "..."
   }}

2. "people" — ALL people found on the website:
   [
     {{
       "name": "John Smith",
       "title": "Managing Director",
       "email": "john@example.com or null",
       "is_decision_maker": true,
       "seniority": "c-suite|director|manager|staff",
       "functional_area": "operations|sales|finance|engineering|quality|general"
     }}
   ]

3. "case_studies" — specific projects/examples found:
   [
     {{
       "title": "Aerospace bracket for Airbus A350",
       "materials": ["Titanium Ti-6Al-4V"],
       "industry": "aerospace",
       "client": "Airbus",
       "description": "5-axis machined from solid..."
     }}
   ]

4. "equipment" — specific machines/equipment found:
   [
     {{"name": "DMG Mori DMU 50", "type": "5-axis CNC", "source_page": "/equipment"}}
   ]

5. "clients_and_industries" — who they serve:
   {{
     "named_clients": ["Rolls-Royce", "BAE Systems"],
     "industries": ["aerospace", "defence", "automotive"],
     "export_markets": ["USA", "Germany"]
   }}

6. "fractional_forge_signals" — assess the company's likely need for fractional executive help:
   {{
     "visible_leadership_roles": ["Managing Director", "Operations Manager"],
     "likely_missing_roles": ["CFO", "Sales Director", "CTO/Technical Director"],
     "company_maturity": "established_sme|growing|startup|declining",
     "needs_fractional_help": true,
     "reasoning": "60-person company with only 2 visible senior roles..."
   }}

7. "fractional_executive_candidates" — could anyone here become a Forge executive?
   [
     {{
       "name": "John Smith",
       "potential_role": "Fractional CTO",
       "reasoning": "35 years in precision engineering...",
       "confidence": 0.6
     }}
   ]

8. "confidence" — per-field confidence (0.0-1.0):
   {{
     "description": 0.9,
     "certifications": 0.8,
     "people": 0.85,
     "contact_email": 0.7
   }}

9. "quality_scores" — re-assessed scores (0-100):
   {{
     "relevance_score": 85,
     "enrichment_quality": 72,
     "data_completeness": 0.8
   }}

10. "verification_notes": "Summary of what was found and any concerns"

Return ONLY valid JSON."#,
                        name = name,
                        website = website,
                        country = country,
                        description = description,
                        category = category,
                        subcategory = subcategory,
                        certifications = certifications,
                        company_size = company_size,
                        contact_name = contact_name,
                        contact_title = contact_title,
                        contact_email = contact_email,
                        address = address,
                        relevance = relevance,
                        quality = quality,
                        website_text = website_text,
                    );

                    // Step 3: Call LLM
                    let response = if llm_backend == "haiku" {
                        match crate::services::anthropic::chat(
                            &anthropic_api_key,
                            Some(system_prompt),
                            &user_prompt,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                let error_msg = format!("[Verify] Anthropic request failed: {}", e);
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "verify", "error", &format!("[Verify] Failed for {}: {}", name, error_msg));
                                let _ = db.set_company_error(&id, &error_msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    } else if llm_backend == "deepseek" {
                        match crate::services::deepseek::chat(
                            &deepseek_api_key,
                            Some(system_prompt),
                            &user_prompt,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                let error_msg = format!("[Verify] DeepSeek request failed: {}", e);
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "verify", "error", &format!("[Verify] Failed for {}: {}", name, error_msg));
                                let _ = db.set_company_error(&id, &error_msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    } else {
                        match crate::services::ollama::generate(
                            &ollama_url,
                            &enrich_model,
                            &format!("{}\n\n{}", system_prompt, user_prompt),
                            false,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                let error_msg = format!("[Verify] Ollama request failed: {}", e);
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "verify", "error", &format!("[Verify] Failed for {}: {}", name, error_msg));
                                let _ = db.set_company_error(&id, &error_msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    };

                    // Step 4: Parse JSON response
                    let result: Value = match serde_json::from_str(&response) {
                        Ok(v) => v,
                        Err(e) => {
                            let truncated: String = response.chars().take(300).collect();
                            let error_msg = format!("[Verify] JSON parse error: {} (len={}). Start: {}", e, response.len(), truncated);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "verify", "error", &format!("[Verify] Parse failed for {}: {}", name, error_msg));
                            // Still mark verified so we don't retry endlessly — store error in changes
                            let changes = json!({
                                "verified_at": chrono::Utc::now().to_rfc3339(),
                                "parse_error": true,
                                "error": format!("{}", e),
                            });
                            let _ = db.mark_verified(&id, &changes.to_string());
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // Step 5: Extract corrections and apply
                    let corrections = result.get("corrections").cloned().unwrap_or(json!({}));
                    let correction_count = corrections.as_object().map(|o| o.len()).unwrap_or(0);

                    // Build quality scores
                    let empty_obj = json!({});
                    let quality_scores = result.get("quality_scores").unwrap_or(&empty_obj);
                    let new_relevance = quality_scores
                        .get("relevance_score")
                        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
                        .map(|v| v.clamp(0, 100));
                    let new_quality = quality_scores
                        .get("enrichment_quality")
                        .and_then(|v| v.as_i64().or_else(|| v.as_f64().map(|f| f as i64)))
                        .map(|v| v.clamp(0, 100));

                    // Build audit trail
                    let changes_json = json!({
                        "verified_at": chrono::Utc::now().to_rfc3339(),
                        "corrections": &corrections,
                        "correction_count": correction_count,
                        "extracted": {
                            "people": result.get("people").unwrap_or(&json!([])),
                            "case_studies": result.get("case_studies").unwrap_or(&json!([])),
                            "equipment": result.get("equipment").unwrap_or(&json!([])),
                            "clients_and_industries": result.get("clients_and_industries").unwrap_or(&json!({})),
                        },
                        "confidence": result.get("confidence").unwrap_or(&json!({})),
                        "quality_scores": quality_scores,
                        "verification_notes": result.get("verification_notes").unwrap_or(&json!("")),
                    });

                    let fractional_signals = json!({
                        "fractional_forge_signals": result.get("fractional_forge_signals").unwrap_or(&json!({})),
                        "fractional_executive_candidates": result.get("fractional_executive_candidates").unwrap_or(&json!([])),
                    });

                    // Step 6: Write to DB
                    {
                        let db: tauri::State<'_, Database> = app.state();
                        match db.apply_verification(
                            &id,
                            &corrections,
                            &changes_json.to_string(),
                            &fractional_signals.to_string(),
                            new_relevance,
                            new_quality,
                        ) {
                            Ok(_) => {
                                verified_count.fetch_add(1, Ordering::Relaxed);
                                if correction_count > 0 {
                                    corrections_count.fetch_add(correction_count as i64, Ordering::Relaxed);
                                    let _ = db.log_activity(
                                        &job_id,
                                        "verify",
                                        "info",
                                        &format!("[Verify] {} — {} corrections applied", name, correction_count),
                                    );
                                } else {
                                    let _ = db.log_activity(
                                        &job_id,
                                        "verify",
                                        "info",
                                        &format!("[Verify] {} — verified, no corrections needed", name),
                                    );
                                }
                            }
                            Err(e) => {
                                let error_msg = format!("[Verify] DB save failed: {}", e);
                                let _ = db.log_activity(&job_id, "verify", "error", &format!("[Verify] Failed to save for {}: {}", name, error_msg));
                                let _ = db.set_company_error(&id, &error_msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    }

                    // Emit progress
                    let cur_verified = verified_count.load(Ordering::Relaxed);
                    let cur_errors = error_count.load(Ordering::Relaxed);

                    let _ = app.emit(
                        "pipeline:progress",
                        json!({
                            "stage": "verify",
                            "phase": "done",
                            "current_company": name,
                            "verified": cur_verified,
                            "errors": cur_errors,
                            "corrections": corrections_count.load(Ordering::Relaxed),
                        }),
                    );

                    if cur_verified % 5 == 0 || cur_verified == 1 {
                        let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                        let rate = if elapsed > 0 { cur_verified as f64 / elapsed as f64 * 3600.0 } else { 0.0 };
                        super::emit_node(&app, json!({
                            "node_id": "verify",
                            "status": "running",
                            "progress": { "current": cur_verified, "total": null, "rate": rate, "current_item": &name },
                            "concurrency": concurrency,
                            "started_at": started_at.to_rfc3339(),
                            "elapsed_secs": elapsed
                        }));
                    }
                }
            })
            .buffer_unordered(concurrency)
            .collect::<Vec<()>>()
            .await;
    }

    let final_verified = verified_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);
    let final_corrections = corrections_count.load(Ordering::Relaxed);

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    super::emit_node(app, json!({
        "node_id": "verify",
        "status": "completed",
        "progress": { "current": final_verified, "total": final_verified, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "verify",
            "info",
            &format!(
                "[Verify] Complete: {} verified, {} errors, {} corrections applied in {}s",
                final_verified, final_errors, final_corrections, elapsed
            ),
        );
    }

    Ok(json!({
        "companies_verified": final_verified,
        "errors": final_errors,
        "corrections_applied": final_corrections,
        "elapsed_secs": elapsed,
    }))
}
