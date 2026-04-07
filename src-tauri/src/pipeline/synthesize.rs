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
        .get("synthesize_concurrency")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1)
        .min(10);

    let synthesized_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "synthesize",
            "info",
            &format!(
                "[Synthesize] Starting synthesis (concurrency={}, batch={}) using backend: {}",
                concurrency, BATCH_SIZE, llm_backend
            ),
        );
    }

    let started_at = chrono::Utc::now();

    super::emit_node(app, json!({
        "node_id": "synthesize",
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
            let _ = db.log_activity(job_id, "synthesize", "warn", "[Synthesize] Cancelled by user");
            break;
        }

        let companies = {
            let db: tauri::State<'_, Database> = app.state();
            db.get_synthesizable_companies(BATCH_SIZE)?
        };

        if companies.is_empty() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "synthesize", "info", "[Synthesize] Queue empty — synthesis finished");
            break;
        }

        let batch_size = companies.len();
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "synthesize",
                "info",
                &format!("[Synthesize] Processing batch of {} companies", batch_size),
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
                let synthesized_count = Arc::clone(&synthesized_count);
                let error_count = Arc::clone(&error_count);

                async move {
                    if super::is_cancelled() {
                        return;
                    }

                    let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
                    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(&job_id, "synthesize", "info", &format!("[Synthesize] Synthesizing: {}", name));
                    }

                    let _ = app.emit(
                        "pipeline:progress",
                        json!({
                            "stage": "synthesize",
                            "phase": "start",
                            "current_company": name,
                            "synthesized": synthesized_count.load(Ordering::Relaxed),
                            "errors": error_count.load(Ordering::Relaxed),
                        }),
                    );

                    // ── Build company data context ──────────────────────────────
                    let description = company.get("description").and_then(|v| v.as_str()).unwrap_or("No description");
                    let country = company.get("country").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let category = company.get("category").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let subcategory = company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let certifications = company.get("certifications").and_then(|v| v.as_str()).unwrap_or("None listed");
                    let company_size = company.get("company_size").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let year_founded = company.get("year_founded").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let website_url = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("Not provided");
                    let contact_name = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let _contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("Unknown");
                    let contact_title = company.get("contact_title").and_then(|v| v.as_str()).unwrap_or("");
                    let status = company.get("status").and_then(|v| v.as_str()).unwrap_or("Unknown");

                    // Parse verification data for equipment, case studies, clients, people
                    let verification_changes: Value = company
                        .get("verification_changes_json")
                        .and_then(|v| v.as_str())
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(json!({}));

                    let fractional_signals: Value = company
                        .get("fractional_signals_json")
                        .and_then(|v| v.as_str())
                        .and_then(|s| serde_json::from_str(s).ok())
                        .unwrap_or(json!({}));

                    // Extract equipment list
                    let equipment_str = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("equipment"))
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|e| {
                                    e.get("name")
                                        .and_then(|n| n.as_str())
                                        .or_else(|| e.as_str())
                                        .map(|s| s.to_string())
                                })
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_else(|| "Not specified".to_string());

                    // Extract case studies
                    let case_studies_str = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("case_studies"))
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|cs| {
                                    cs.get("title")
                                        .and_then(|t| t.as_str())
                                        .or_else(|| cs.as_str())
                                        .map(|s| s.to_string())
                                })
                                .collect::<Vec<_>>()
                                .join("; ")
                        })
                        .unwrap_or_else(|| "Not specified".to_string());

                    // Extract clients/industries
                    let clients_str = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("clients_and_industries"))
                        .and_then(|v| v.get("named_clients"))
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|c| c.as_str().map(|s| s.to_string()))
                                .collect::<Vec<_>>()
                                .join(", ")
                        })
                        .unwrap_or_else(|| "Not specified".to_string());

                    // Extract people count
                    let people_count = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("people"))
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.len())
                        .unwrap_or(0);

                    // Extract people details (for private synthesis)
                    let people_str = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("people"))
                        .and_then(|v| v.as_array())
                        .map(|arr| {
                            arr.iter()
                                .take(10)
                                .filter_map(|p| {
                                    let pname = p.get("name").and_then(|n| n.as_str()).unwrap_or("Unknown");
                                    let ptitle = p.get("title").and_then(|t| t.as_str()).unwrap_or("");
                                    if ptitle.is_empty() {
                                        Some(pname.to_string())
                                    } else {
                                        Some(format!("{} ({})", pname, ptitle))
                                    }
                                })
                                .collect::<Vec<_>>()
                                .join("; ")
                        })
                        .unwrap_or_else(|| "Not identified".to_string());

                    // Extract fractional signals
                    let frac_hiring = fractional_signals
                        .get("fractional_forge_signals")
                        .or_else(|| Some(&fractional_signals))
                        .and_then(|v| v.get("hiring_signals"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("Not detected");
                    let frac_activity = fractional_signals
                        .get("fractional_forge_signals")
                        .or_else(|| Some(&fractional_signals))
                        .and_then(|v| v.get("activity_signals"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("Not detected");
                    let frac_business = fractional_signals
                        .get("fractional_forge_signals")
                        .or_else(|| Some(&fractional_signals))
                        .and_then(|v| v.get("business_signals"))
                        .and_then(|v| v.as_str())
                        .unwrap_or("Not detected");

                    // ── PUBLIC SYNTHESIS ────────────────────────────────────────
                    let public_system = "You are a manufacturing intelligence analyst writing marketplace listings for Fractional Forge, a B2B marketplace connecting buyers with precision manufacturers. Write in a professional but approachable tone. Be specific — mention actual certifications, equipment, and capabilities.\n\nCRITICAL RULES:\n- Use only information provided. Do not infer or speculate.\n- Be specific: \"AS9100-certified 5-axis CNC machining of titanium\" not \"precision work\".\n- Write for buyers looking for manufacturing partners.\n- Highlight competitive differentiation.\n- NEVER include: director ages, acquisition scores, ownership structure, financial data, founder bios, board composition.\n- Return ONLY valid JSON. No markdown, no explanations.";

                    let public_user = format!(
                        r#"COMPANY DATA:
Name: {name}
Country: {country}, City: {city}
Description: {description}
Category: {category} / {subcategory}
Certifications: {certifications}
Company Size: {company_size}
Founded: {year_founded}
Website: {website_url}
Equipment: {equipment_str}
Case Studies: {case_studies_str}
Clients/Industries: {clients_str}
People: {people_count} team members identified

Generate a marketplace synthesis. Return JSON:
{{
  "capability_summary": "2-3 sentence pitch of what makes this company distinctive. Be specific about their strengths — mention specific certifications, equipment, sectors served. Use active, confident language.",

  "ideal_buyer_profile": "What kind of buyer should choose this company? Be specific: 'Aerospace OEMs needing AS9100-certified 5-axis machining of titanium components in batches of 10-500' not 'companies needing machining'.",

  "competitive_positioning": {{
    "market_segment": "specialty|mid-market|commodity",
    "production_type": "prototype|small_batch|medium_batch|high_volume|mixed",
    "technical_level": "basic|intermediate|advanced|cutting_edge",
    "pricing_tier": "premium|mid|value|unknown",
    "key_differentiator": "What one thing sets them apart?"
  }},

  "marketplace_tags": {{
    "primary_capabilities": ["5-axis CNC machining", "EDM wire cutting"],
    "materials_expertise": ["titanium", "inconel", "aluminium"],
    "industry_focus": ["aerospace", "defence", "medical"],
    "certifications": ["AS9100D", "ISO 13485"],
    "batch_size": "prototype to 500",
    "lead_time": "2-6 weeks"
  }},

  "search_keywords": ["precision machining", "aerospace components", "titanium machining", "5-axis"],

  "data_quality_assessment": {{
    "overall_grade": "A|B|C|D",
    "confidence": 0.85,
    "missing_data": ["no pricing info", "no lead time mentioned"],
    "needs_human_review": false,
    "review_reason": null
  }}
}}

Return ONLY valid JSON."#,
                        name = name,
                        country = country,
                        city = city,
                        description = description,
                        category = category,
                        subcategory = subcategory,
                        certifications = certifications,
                        company_size = company_size,
                        year_founded = year_founded,
                        website_url = website_url,
                        equipment_str = equipment_str,
                        case_studies_str = case_studies_str,
                        clients_str = clients_str,
                        people_count = people_count,
                    );

                    let public_response = if llm_backend == "haiku" {
                        crate::services::anthropic::chat(
                            &anthropic_api_key,
                            Some(public_system),
                            &public_user,
                            true,
                        )
                        .await
                    } else if llm_backend == "deepseek" {
                        crate::services::deepseek::chat(
                            &deepseek_api_key,
                            Some(public_system),
                            &public_user,
                            true,
                        )
                        .await
                    } else {
                        crate::services::ollama::generate(
                            &ollama_url,
                            &enrich_model,
                            &format!("{}\n\n{}", public_system, public_user),
                            false,
                        )
                        .await
                    };

                    let public_json_str = match public_response {
                        Ok(r) => r,
                        Err(e) => {
                            let error_msg = format!("[Synthesize] Public synthesis LLM failed: {}", e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Failed for {}: {}", name, error_msg));
                            let _ = db.set_company_error(&id, &error_msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // Validate public JSON parses
                    let public_value: Value = match serde_json::from_str(&public_json_str) {
                        Ok(v) => v,
                        Err(e) => {
                            let truncated: String = public_json_str.chars().take(300).collect();
                            let error_msg = format!("[Synthesize] Public JSON parse error: {} (len={}). Start: {}", e, public_json_str.len(), truncated);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Parse failed for {}: {}", name, error_msg));
                            let _ = db.set_company_error(&id, &error_msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // ── PRIVATE SYNTHESIS ───────────────────────────────────────
                    let private_system = "You are a business analyst evaluating manufacturing SMEs for a private equity investor interested in acquiring and improving manufacturing companies. Be analytical and direct. Focus on actionable intelligence for acquisition diligence and fractional executive recruitment.\n\nCRITICAL RULES:\n- Use only information provided. Do not infer.\n- Focus on growth trajectory, team depth, and acquisition readiness.\n- Identify gaps where fractional executives could add value.\n- Assess M&A attractiveness based on signals in the data.\n- Return ONLY valid JSON. No markdown, no explanations.";

                    let private_user = format!(
                        r#"COMPANY DATA:
Name: {name}
Country: {country}, City: {city}
Status: {status}
Description: {description}
Category: {category} / {subcategory}
Certifications: {certifications}
Company Size: {company_size}
Founded: {year_founded}
Website: {website_url}
Contact: {contact_name} ({contact_title})

ENRICHMENT DATA:
Team: {people_str}
Equipment: {equipment_str}
Clients: {clients_str}
Hiring Signals: {frac_hiring}
Activity Signals: {frac_activity}
Business Signals: {frac_business}

Generate private intelligence. Return JSON:
{{
  "company_health_narrative": "One paragraph: trading history, investment signals, hiring activity, website freshness, client calibre, overall trajectory. Is this company growing, stable, or declining?",

  "fractional_needs_analysis": {{
    "visible_leadership": ["Managing Director", "Operations Manager"],
    "likely_missing_roles": ["CFO", "Sales Director", "CTO"],
    "most_impactful_hire": "Fractional CFO — company appears to have grown beyond founder-managed finances",
    "urgency": "high|medium|low",
    "reasoning": "..."
  }},

  "approach_strategy": {{
    "best_contact": {{"name": "...", "title": "...", "method": "email|linkedin|phone", "email": "..."}},
    "angle": "Lead with how Fractional Forge helps SME manufacturers like them scale operations without full-time C-suite hires. Emphasise peer companies in their sector.",
    "timing": "good|neutral|bad",
    "timing_reason": "Company appears to be investing and growing — good time to offer operational support"
  }},

  "fractional_executive_candidates": [
    {{
      "name": "...",
      "current_title": "...",
      "potential_forge_role": "Fractional CTO — Manufacturing",
      "reasoning": "35 years in aerospace machining, career timeline suggests approaching 60s",
      "approach_method": "LinkedIn — mention their specific expertise in 5-axis titanium work",
      "confidence": 0.6
    }}
  ],

  "acquisition_fit": {{
    "verdict": "strong_target|watch_list|not_suitable",
    "reasoning": "Well-run 50-person precision engineering company with AS9100 cert and blue-chip clients. Founder aged 67 with no visible succession. Clean accounts. Classic acquisition profile.",
    "estimated_revenue_bracket": "unknown",
    "key_assets": ["AS9100 certification", "trained workforce"],
    "key_risks": ["key-man dependency on founder", "limited digital presence"]
  }}
}}

Return ONLY valid JSON."#,
                        name = name,
                        country = country,
                        city = city,
                        status = status,
                        description = description,
                        category = category,
                        subcategory = subcategory,
                        certifications = certifications,
                        company_size = company_size,
                        year_founded = year_founded,
                        website_url = website_url,
                        contact_name = contact_name,
                        contact_title = contact_title,
                        people_str = people_str,
                        equipment_str = equipment_str,
                        clients_str = clients_str,
                        frac_hiring = frac_hiring,
                        frac_activity = frac_activity,
                        frac_business = frac_business,
                    );

                    let private_response = if llm_backend == "haiku" {
                        crate::services::anthropic::chat(
                            &anthropic_api_key,
                            Some(private_system),
                            &private_user,
                            true,
                        )
                        .await
                    } else if llm_backend == "deepseek" {
                        crate::services::deepseek::chat(
                            &deepseek_api_key,
                            Some(private_system),
                            &private_user,
                            true,
                        )
                        .await
                    } else {
                        crate::services::ollama::generate(
                            &ollama_url,
                            &enrich_model,
                            &format!("{}\n\n{}", private_system, private_user),
                            false,
                        )
                        .await
                    };

                    let private_json_str = match private_response {
                        Ok(r) => r,
                        Err(e) => {
                            let error_msg = format!("[Synthesize] Private synthesis LLM failed: {}", e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Failed for {}: {}", name, error_msg));
                            let _ = db.set_company_error(&id, &error_msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // Validate private JSON parses
                    let private_value: Value = match serde_json::from_str(&private_json_str) {
                        Ok(v) => v,
                        Err(e) => {
                            let truncated: String = private_json_str.chars().take(300).collect();
                            let error_msg = format!("[Synthesize] Private JSON parse error: {} (len={}). Start: {}", e, private_json_str.len(), truncated);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Parse failed for {}: {}", name, error_msg));
                            let _ = db.set_company_error(&id, &error_msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };

                    // ── Save both to DB ────────────────────────────────────────
                    {
                        let db: tauri::State<'_, Database> = app.state();
                        match db.save_synthesis(
                            &id,
                            &public_value.to_string(),
                            &private_value.to_string(),
                        ) {
                            Ok(_) => {
                                synthesized_count.fetch_add(1, Ordering::Relaxed);
                                let _ = db.log_activity(
                                    &job_id,
                                    "synthesize",
                                    "info",
                                    &format!("[Synthesize] {} — public + private synthesis saved", name),
                                );
                            }
                            Err(e) => {
                                let error_msg = format!("[Synthesize] DB save failed: {}", e);
                                let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Failed to save for {}: {}", name, error_msg));
                                let _ = db.set_company_error(&id, &error_msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    }

                    // Emit progress
                    let cur = synthesized_count.load(Ordering::Relaxed);
                    let cur_errors = error_count.load(Ordering::Relaxed);

                    let _ = app.emit(
                        "pipeline:progress",
                        json!({
                            "stage": "synthesize",
                            "phase": "done",
                            "current_company": name,
                            "synthesized": cur,
                            "errors": cur_errors,
                        }),
                    );

                    if cur % 5 == 0 || cur == 1 {
                        let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                        let rate = if elapsed > 0 { cur as f64 / elapsed as f64 * 3600.0 } else { 0.0 };
                        super::emit_node(&app, json!({
                            "node_id": "synthesize",
                            "status": "running",
                            "progress": { "current": cur, "total": null, "rate": rate, "current_item": &name },
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

    let final_synthesized = synthesized_count.load(Ordering::Relaxed);
    let final_errors = error_count.load(Ordering::Relaxed);

    let elapsed = (chrono::Utc::now() - started_at).num_seconds();
    super::emit_node(app, json!({
        "node_id": "synthesize",
        "status": "completed",
        "progress": { "current": final_synthesized, "total": final_synthesized, "rate": null, "current_item": null },
        "concurrency": concurrency,
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "synthesize",
            "info",
            &format!(
                "[Synthesize] Complete: {} synthesized, {} errors in {}s",
                final_synthesized, final_errors, elapsed
            ),
        );
    }

    Ok(json!({
        "companies_synthesized": final_synthesized,
        "errors": final_errors,
        "elapsed_secs": elapsed,
    }))
}
