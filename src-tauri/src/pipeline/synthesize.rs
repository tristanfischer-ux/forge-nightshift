//! Synthesis pipeline stage.
//!
//! Runs a two-pass **VERIFY → SYNTHESIS** chain (Claude Haiku 4.5) over every company
//! that has been verified but not yet synthesised. Ported from Forge Capital's
//! `research/17-unified-pipeline.py` (lines ~411–479), with sections adapted for
//! customer vs supplier campaigns. The anti-speculation block in SYNTHESIS is kept
//! verbatim (noun swapped `investor` → `company`).
//!
//! Closes the Phase 0 quality audit gap: `synthesis_public_json`,
//! `synthesis_private_json`, `fractional_signals_json`, `structured_signals_json`,
//! and `ff_suitability_reason` are all NULL in production. This stage populates
//! them via `Database::save_synthesis_v2`.
//!
//! For DeepSeek and Ollama backends the original single-pass marketplace listing
//! prompt is preserved so other operators' pipelines keep working; only the Haiku
//! backend runs the new two-pass chain (Haiku is the only backend whose output
//! has been validated against the Forge Capital 40.2/50 quality bar).

use anyhow::Result;
use futures::stream::{self, StreamExt};
use serde_json::{json, Value};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::{Emitter, Manager};

use crate::db::Database;

const BATCH_SIZE: i64 = 20;
const WEBSITE_TEXT_CAP: usize = 60_000;

// ───────────────────────────────────────────────────────────────────────────
// Prompt building (pure — no I/O, tested below)
// ───────────────────────────────────────────────────────────────────────────

/// Intent of the campaign driving this synthesis pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CampaignIntent {
    /// We are looking for companies to sell to (buyers / customers).
    Customer,
    /// We are looking for companies to buy from (suppliers / contract manufacturers).
    Supplier,
}

impl CampaignIntent {
    pub fn from_config(config: &Value) -> Self {
        match config
            .get("campaign_intent")
            .and_then(|v| v.as_str())
            .unwrap_or("supplier")
            .to_ascii_lowercase()
            .as_str()
        {
            "customer" | "buyer" => CampaignIntent::Customer,
            _ => CampaignIntent::Supplier,
        }
    }
}

/// VERIFY system prompt — customer campaigns.
/// Pass 1 of the chain: reconcile DB against website. "Website wins" anchoring.
pub fn verify_system_prompt_customer() -> &'static str {
    r#"You are a company intelligence analyst. You are given scraped website content from a company's website, along with what our database currently says about them.

Your job is to VERIFY and CORRECT our database records against the company's own website content.

CRITICAL: The company's website is the SINGLE SOURCE OF TRUTH. Use names, titles, language, and capability claims EXACTLY as they appear on the website. Do NOT paraphrase or rewrite in third person.

Return ONLY valid JSON:
{
  "verification_status": "verified|needs_correction|not_a_buyer|dead_website|insufficient_content",
  "confidence": 0.0-1.0,
  "description_accuracy": "accurate|needs_update|missing",
  "description_from_website": "What the company does IN THEIR OWN WORDS from the website (2-4 sentences). Copy their language.",
  "decision_makers": [
    {"name": "Full Name", "title": "Their Title", "bio": "Brief bio from website", "linkedin": "URL if found", "email": "email if found"}
  ],
  "existing_stack_signals": [
    "Names of tools, equipment, suppliers, or systems mentioned on the site"
  ],
  "pain_signals": [
    "Phrases or pages that suggest a pain or need relevant to our offering — quote verbatim, do not paraphrase"
  ],
  "field_corrections": {
    "description": "...", "certifications": "...", "company_size": "...",
    "contact_email": "...", "contact_name": "...", "contact_title": "...",
    "address": "...", "year_founded": "..."
  },
  "notes": "Any other observations — especially recent hires, expansions, RFPs, grants"
}

If the website clearly shows this is NOT a potential buyer in our space (e.g., it's a consultancy, news site, or a competitor rather than a customer), set verification_status to "not_a_buyer".
If the website is dead or has no meaningful content, set it accordingly."#
}

/// VERIFY system prompt — supplier campaigns.
pub fn verify_system_prompt_supplier() -> &'static str {
    r#"You are a company intelligence analyst. You are given scraped website content from a company's website, along with what our database currently says about them.

Your job is to VERIFY and CORRECT our database records against the company's own website content.

CRITICAL: The company's website is the SINGLE SOURCE OF TRUTH. Use names, titles, capability language, and certifications EXACTLY as they appear on the website. Do NOT paraphrase or rewrite in third person.

Return ONLY valid JSON:
{
  "verification_status": "verified|needs_correction|not_a_supplier|dead_website|insufficient_content",
  "confidence": 0.0-1.0,
  "description_accuracy": "accurate|needs_update|missing",
  "description_from_website": "What the company makes or services IN THEIR OWN WORDS from the website (2-4 sentences). Copy their language.",
  "decision_makers": [
    {"name": "Full Name", "title": "Their Title", "bio": "Brief bio from website", "linkedin": "URL if found", "email": "email if found"}
  ],
  "capability_claims": [
    "Processes, materials, tolerances, or equipment they explicitly list — verbatim where possible"
  ],
  "capacity_signals": [
    "Site square footage, machine count, shift pattern, headcount, or batch-size ranges quoted on the site"
  ],
  "prior_customer_signals": [
    "Named customers, logos, case studies, or sectors the site claims to serve"
  ],
  "field_corrections": {
    "description": "...", "certifications": "...", "company_size": "...",
    "contact_email": "...", "contact_name": "...", "contact_title": "...",
    "address": "...", "year_founded": "..."
  },
  "notes": "Any other observations — especially accreditations, recent hires, new capacity"
}

If the website clearly shows this is NOT a supplier / contract manufacturer in our space (e.g., it's a trade body, distributor-only, or consultancy), set verification_status to "not_a_supplier".
If the website is dead or has no meaningful content, set it accordingly."#
}

/// SYNTHESIS system prompt — customer campaigns. Keeps the anti-speculation WRONG/RIGHT
/// block from Forge Capital 17-unified-pipeline.py verbatim (noun swapped to "company").
pub fn synthesis_system_prompt_customer() -> &'static str {
    r#"You are an intelligence analyst preparing buyer briefings for a hardware sales team. You receive verified data about a company: what they say they do, their people (with bios where available), their existing stack, and any pain or expansion signals visible on their site.

Your job is to SYNTHESIZE this data into actionable intelligence a salesperson can walk into a call with.

CRITICAL RULES:
- ONLY reference information present in the provided data. Do not infer, assume, or add any information not explicitly stated.
- Every factual claim must be traceable to a specific person, product, page, or database field.
- The signals you received are those visible on the company's website and may not represent the full picture. Frame analysis as "based on the visible site content" rather than absolute claims.
- Write in plain language. This should read like a colleague briefing you before a sales call.

Return ONLY valid JSON with this schema:
{
  "structured_signals": {
    "buyer_fit_score": 0-100,
    "pain_signals": ["Specific phrases or facts that suggest they have a pain our offer addresses"],
    "existing_stack_mentions": ["Named tools, equipment, or suppliers they reference"],
    "scale_hints": "What size of operation the site suggests — if discernible",
    "buying_cycle_hints": "Any evidence of procurement timing (RFPs, grant announcements, new site construction, expansions)",
    "decision_maker_depth": "none|some|strong — based on whether named decision-makers with titles are visible",
    "technical_depth": "none|some|strong — based on whether the site shows in-house technical capability vs outsourced"
  },
  "pain_and_fit": "2-4 sentences describing what their visible situation is, and why (or why not) our offering fits — based ONLY on what's on the site.",
  "decision_maker_read": "2-4 sentences describing who the decision-maker appears to be and what is visible about their background.",
  "connection_brief": "3-5 sentences of FACTUAL visibility and reachability information ONLY.",
  "ff_suitability_reason": "One sentence stating whether this company is a plausible customer for our offer, with the single strongest supporting fact."
}

CRITICAL RULES FOR connection_brief:
- State ONLY verifiable facts: what platforms they are active on, what topics they publish about, what events they attend, what trade bodies they belong to, what partners they name.
- Do NOT speculate about personality, communication preferences, or what they "respond well to."
- Do NOT give approach advice such as "leverage their passion for X" or "lead with Y." That is dangerous — you do not know this person.
- Do NOT claim someone is "approachable" or "open to cold outreach" unless they have explicitly stated this publicly.
- WRONG: "To approach them effectively, leverage their interest in sustainability — they respond well to ROI-driven pitches."
- RIGHT: "Active on LinkedIn. Publishes quarterly in Aquaculture Today. Speaks at RAS-N Annual Conference. Member of the Canadian Aquaculture Industry Alliance. Cites Innovasea and Pentair as current equipment partners on their plant page."
- If you do not have enough data to write factual visibility information, say: "Limited public visibility data available."

If the data is too thin to produce meaningful analysis, say so honestly in the relevant fields rather than speculating."#
}

/// SYNTHESIS system prompt — supplier campaigns.
pub fn synthesis_system_prompt_supplier() -> &'static str {
    r#"You are an intelligence analyst preparing supplier briefings for a hardware procurement team. You receive verified data about a company: what they claim to make or offer, their people (with bios where available), their capacity and certifications, and any prior-customer signals visible on their site.

Your job is to SYNTHESIZE this data into actionable intelligence a buyer or marketplace operator can walk into a sourcing decision with.

CRITICAL RULES:
- ONLY reference information present in the provided data. Do not infer, assume, or add any information not explicitly stated.
- Every factual claim must be traceable to a specific person, certification, case study, or database field.
- The signals you received are those visible on the company's website and may not represent the full picture. Frame analysis as "based on the visible site content" rather than absolute claims.
- Write in plain language. This should read like a colleague briefing a procurement lead before a call.

Return ONLY valid JSON with this schema:
{
  "structured_signals": {
    "capability_fit_score": 0-100,
    "primary_capabilities": ["Named processes / materials the site foregrounds"],
    "certifications": ["ISO 9001", "AS9100D"],
    "lead_time_hints": "What the site says about lead times",
    "capacity_hints": "What the site reveals about throughput — machine count, square footage, headcount, shift pattern",
    "prior_customer_signals": ["Named logos or case-study clients — verbatim"],
    "technical_depth": "none|some|strong — based on whether bios/case studies reveal engineering depth vs sales-first posture",
    "operational_depth": "none|some|strong — based on evidence of QMS, traceability, in-house inspection"
  },
  "capability_and_fit": "2-4 sentences describing what they can actually do based on site evidence, and where they sit on specialty vs commodity — based ONLY on what's on the site.",
  "decision_maker_read": "2-4 sentences describing who the decision-maker appears to be and what is visible about their background.",
  "connection_brief": "3-5 sentences of FACTUAL visibility and reachability information ONLY.",
  "ff_suitability_reason": "One sentence stating whether this supplier is a plausible fit for the Fractional Forge marketplace, with the single strongest supporting fact."
}

CRITICAL RULES FOR connection_brief:
- State ONLY verifiable facts: what platforms they are active on, what topics they publish about, what trade fairs they attend, what certifications bodies list them, what named partners they reference.
- Do NOT speculate about personality, communication preferences, or what they "respond well to."
- Do NOT give approach advice such as "leverage their passion for X" or "lead with Y." That is dangerous — you do not know this person.
- Do NOT claim someone is "approachable" or "open to cold outreach" unless they have explicitly stated this publicly.
- WRONG: "To approach them effectively, leverage their interest in aerospace — they respond well to technical pitches."
- RIGHT: "Active on LinkedIn. Case studies name Rolls-Royce, BAE Systems, and Meggitt. Listed on the ADS Group members directory. AS9100D certified by NQA, expires 2027-03. Exhibits at Farnborough biennially."
- If you do not have enough data to write factual visibility information, say: "Limited public visibility data available."

If the data is too thin to produce meaningful analysis, say so honestly in the relevant fields rather than speculating."#
}

/// Build the user prompt for pass 1 (VERIFY). Interpolates DB record + scraped text.
pub fn build_verify_user_prompt(company: &Value, website_text: &str, intent: CampaignIntent) -> String {
    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");
    let website = company
        .get("website_url")
        .and_then(|v| v.as_str())
        .unwrap_or("(no website)");

    // Mirror 17-unified-pipeline.py: send the DB record the LLM should reconcile against.
    let db_data = json!({
        "description": company.get("description").and_then(|v| v.as_str()).unwrap_or(""),
        "certifications": company.get("certifications").and_then(|v| v.as_str()).unwrap_or(""),
        "company_size": company.get("company_size").and_then(|v| v.as_str()).unwrap_or(""),
        "country": company.get("country").and_then(|v| v.as_str()).unwrap_or(""),
        "city": company.get("city").and_then(|v| v.as_str()).unwrap_or(""),
        "category": company.get("category").and_then(|v| v.as_str()).unwrap_or(""),
        "subcategory": company.get("subcategory").and_then(|v| v.as_str()).unwrap_or(""),
        "year_founded": company.get("year_founded").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_email": company.get("contact_email").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_name": company.get("contact_name").and_then(|v| v.as_str()).unwrap_or(""),
        "contact_title": company.get("contact_title").and_then(|v| v.as_str()).unwrap_or(""),
    });

    // Cap website text at 60k chars — same as 17-unified-pipeline.py to keep within Haiku context.
    let capped_text: String = website_text.chars().take(WEBSITE_TEXT_CAP).collect();

    let instruction = match intent {
        CampaignIntent::Customer =>
            "Verify our database against this website content. Extract decision-makers, existing stack, and pain signals.",
        CampaignIntent::Supplier =>
            "Verify our database against this website content. Extract decision-makers, capability claims, capacity signals, and prior customers.",
    };

    format!(
        "COMPANY: {name}\nWEBSITE: {website}\n\nDATABASE RECORDS:\n{db}\n\nWEBSITE CONTENT:\n{content}\n\n{instr}",
        name = name,
        website = website,
        db = serde_json::to_string_pretty(&db_data).unwrap_or_default(),
        content = capped_text,
        instr = instruction,
    )
}

/// Build the user prompt for pass 2 (SYNTHESIS). Takes verified output from pass 1.
pub fn build_synthesis_user_prompt(company: &Value, verify_output: &Value, intent: CampaignIntent) -> String {
    let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown");

    let instruction = match intent {
        CampaignIntent::Customer => "Synthesize actionable buyer intelligence.",
        CampaignIntent::Supplier => "Synthesize actionable supplier intelligence.",
    };

    format!(
        "COMPANY: {name}\n\nVERIFIED DATA FROM WEBSITE:\n{verified}\n\n{instr}",
        name = name,
        verified = serde_json::to_string_pretty(verify_output).unwrap_or_default(),
        instr = instruction,
    )
}

// ───────────────────────────────────────────────────────────────────────────
// Output parsing (pure — tested below)
// ───────────────────────────────────────────────────────────────────────────

/// Parsed result of a VERIFY + SYNTHESIS chain, mapped onto the 5 currently-NULL
/// companies-table columns the Phase 0 audit called out.
#[derive(Debug, Clone, PartialEq)]
pub struct SynthesisColumns {
    pub synthesis_public_json: String,
    pub synthesis_private_json: String,
    pub structured_signals_json: String,
    pub fractional_signals_json: Option<String>,
    pub ff_suitability_reason: Option<String>,
    /// Verify-pass `verification_status` — lets callers route `not_a_buyer` /
    /// `not_a_supplier` / `dead_website` records differently instead of storing
    /// speculative synthesis against them.
    pub verification_status: Option<String>,
}

/// Take the raw stringified outputs of the two Haiku passes and project them
/// onto the 5 JSON columns. Pure function — no I/O, no panics on malformed input.
pub fn parse_synthesis_output(
    verify_raw: &str,
    synthesis_raw: &str,
) -> Result<SynthesisColumns> {
    let verify: Value = serde_json::from_str(verify_raw)
        .map_err(|e| anyhow::anyhow!("Verify JSON parse error: {} (start: {})", e, preview(verify_raw)))?;

    let synthesis: Value = serde_json::from_str(synthesis_raw)
        .map_err(|e| anyhow::anyhow!("Synthesis JSON parse error: {} (start: {})", e, preview(synthesis_raw)))?;

    // Build PUBLIC synthesis — the buyer/marketplace-facing view.
    // Mirrors the original public synthesis shape but is sourced from the
    // verified-then-synthesized LLM output, not a single-shot listing prompt.
    let public = json!({
        "description_from_website": verify.get("description_from_website").cloned().unwrap_or(Value::Null),
        "capability_and_fit": synthesis.get("capability_and_fit").cloned().unwrap_or(Value::Null),
        "pain_and_fit": synthesis.get("pain_and_fit").cloned().unwrap_or(Value::Null),
        "connection_brief": synthesis.get("connection_brief").cloned().unwrap_or(Value::Null),
        "primary_capabilities": synthesis
            .pointer("/structured_signals/primary_capabilities")
            .cloned()
            .unwrap_or(Value::Null),
        "certifications": synthesis
            .pointer("/structured_signals/certifications")
            .cloned()
            .unwrap_or(Value::Null),
        "prior_customer_signals": synthesis
            .pointer("/structured_signals/prior_customer_signals")
            .cloned()
            .unwrap_or(Value::Null),
        "existing_stack_mentions": synthesis
            .pointer("/structured_signals/existing_stack_mentions")
            .cloned()
            .unwrap_or(Value::Null),
        "source": "synthesize_v2",
    });

    // Build PRIVATE synthesis — sales/acquisition-facing view. Keeps decision
    // maker read + notes out of the public listing.
    let private = json!({
        "decision_maker_read": synthesis.get("decision_maker_read").cloned().unwrap_or(Value::Null),
        "decision_makers": verify.get("decision_makers").cloned().unwrap_or(Value::Null),
        "pain_signals": verify.get("pain_signals").cloned().unwrap_or(Value::Null),
        "capability_claims": verify.get("capability_claims").cloned().unwrap_or(Value::Null),
        "capacity_signals": verify.get("capacity_signals").cloned().unwrap_or(Value::Null),
        "notes": verify.get("notes").cloned().unwrap_or(Value::Null),
        "confidence": verify.get("confidence").cloned().unwrap_or(Value::Null),
        "source": "synthesize_v2",
    });

    // structured_signals_json is the model's own typed signal block — store verbatim.
    let structured_signals = synthesis
        .get("structured_signals")
        .cloned()
        .unwrap_or_else(|| json!({}));

    // fractional_signals_json: we keep the verify stage's existing value (handled by
    // COALESCE in save_synthesis_v2) unless the new output has promoted signals.
    // For customer campaigns we promote buyer-cycle / pain signals; for supplier
    // campaigns we promote capacity / certification signals. Only set if non-empty.
    let promoted_signals = build_promoted_fractional_signals(&verify, &synthesis);
    let fractional_signals_json = if promoted_signals.as_object().map(|o| !o.is_empty()).unwrap_or(false) {
        Some(promoted_signals.to_string())
    } else {
        None
    };

    let ff_suitability_reason = synthesis
        .get("ff_suitability_reason")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let verification_status = verify
        .get("verification_status")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    Ok(SynthesisColumns {
        synthesis_public_json: public.to_string(),
        synthesis_private_json: private.to_string(),
        structured_signals_json: structured_signals.to_string(),
        fractional_signals_json,
        ff_suitability_reason,
        verification_status,
    })
}

fn build_promoted_fractional_signals(verify: &Value, synthesis: &Value) -> Value {
    let mut obj = serde_json::Map::new();

    // Customer-campaign signals
    if let Some(v) = verify.get("pain_signals").filter(|v| !v.is_null()) {
        obj.insert("pain_signals".to_string(), v.clone());
    }
    if let Some(v) = synthesis.pointer("/structured_signals/buying_cycle_hints").filter(|v| !v.is_null()) {
        obj.insert("buying_cycle_hints".to_string(), v.clone());
    }
    if let Some(v) = synthesis.pointer("/structured_signals/scale_hints").filter(|v| !v.is_null()) {
        obj.insert("scale_hints".to_string(), v.clone());
    }

    // Supplier-campaign signals
    if let Some(v) = verify.get("capacity_signals").filter(|v| !v.is_null()) {
        obj.insert("capacity_signals".to_string(), v.clone());
    }
    if let Some(v) = synthesis.pointer("/structured_signals/lead_time_hints").filter(|v| !v.is_null()) {
        obj.insert("lead_time_hints".to_string(), v.clone());
    }
    if let Some(v) = synthesis.pointer("/structured_signals/capacity_hints").filter(|v| !v.is_null()) {
        obj.insert("capacity_hints".to_string(), v.clone());
    }

    Value::Object(obj)
}

fn preview(s: &str) -> String {
    s.chars().take(200).collect()
}

// ───────────────────────────────────────────────────────────────────────────
// Pipeline stage entry point (async, I/O heavy)
// ───────────────────────────────────────────────────────────────────────────

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

    let relevance_threshold: i64 = config
        .get("relevance_threshold")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let quality_threshold: i64 = config
        .get("auto_approve_quality_threshold")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(50);

    let intent = CampaignIntent::from_config(config);

    // Load active profile domain for the legacy (DeepSeek/Ollama) prompt paths.
    let active_domain = {
        let db: tauri::State<'_, Database> = app.state();
        let profile_id = db.get_active_profile_id();
        match db.get_search_profile(&profile_id) {
            Ok(Some(profile)) => profile.get("domain").and_then(|v| v.as_str()).unwrap_or("manufacturing").to_string(),
            _ => "manufacturing".to_string(),
        }
    };

    let synthesized_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "synthesize",
            "info",
            &format!(
                "[Synthesize] Starting (concurrency={}, batch={}, backend={}, intent={:?})",
                concurrency, BATCH_SIZE, llm_backend, intent
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
                let active_domain = active_domain.clone();
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

                    // ── HAIKU: two-pass VERIFY → SYNTHESIS ────────────────────────
                    if llm_backend == "haiku" {
                        let website_text = company
                            .get("deep_website_text")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");

                        if website_text.trim().is_empty() {
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(
                                &job_id,
                                "synthesize",
                                "warn",
                                &format!("[Synthesize] {} — skipped: no deep_website_text", name),
                            );
                            return;
                        }

                        let verify_system = match intent {
                            CampaignIntent::Customer => verify_system_prompt_customer(),
                            CampaignIntent::Supplier => verify_system_prompt_supplier(),
                        };
                        let synthesis_system = match intent {
                            CampaignIntent::Customer => synthesis_system_prompt_customer(),
                            CampaignIntent::Supplier => synthesis_system_prompt_supplier(),
                        };

                        let verify_user = build_verify_user_prompt(&company, website_text, intent);

                        // Pass 1: VERIFY
                        let verify_raw = match crate::services::anthropic::chat(
                            &anthropic_api_key,
                            Some(verify_system),
                            &verify_user,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                let msg = format!("[Synthesize] VERIFY LLM failed for {}: {}", name, e);
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "synthesize", "error", &msg);
                                let _ = db.set_company_error(&id, &msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        };

                        let verify_value: Value = match serde_json::from_str(&verify_raw) {
                            Ok(v) => v,
                            Err(e) => {
                                let msg = format!(
                                    "[Synthesize] VERIFY JSON parse error for {}: {} (len={})",
                                    name,
                                    e,
                                    verify_raw.len()
                                );
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "synthesize", "error", &msg);
                                let _ = db.set_company_error(&id, &msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        };

                        let synthesis_user = build_synthesis_user_prompt(&company, &verify_value, intent);

                        // Pass 2: SYNTHESIS
                        let synthesis_raw = match crate::services::anthropic::chat(
                            &anthropic_api_key,
                            Some(synthesis_system),
                            &synthesis_user,
                            true,
                        )
                        .await
                        {
                            Ok(r) => r,
                            Err(e) => {
                                let msg = format!("[Synthesize] SYNTHESIS LLM failed for {}: {}", name, e);
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "synthesize", "error", &msg);
                                let _ = db.set_company_error(&id, &msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        };

                        let parsed = match parse_synthesis_output(&verify_raw, &synthesis_raw) {
                            Ok(p) => p,
                            Err(e) => {
                                let msg = format!("[Synthesize] parse_synthesis_output failed for {}: {}", name, e);
                                let db: tauri::State<'_, Database> = app.state();
                                let _ = db.log_activity(&job_id, "synthesize", "error", &msg);
                                let _ = db.set_company_error(&id, &msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        };

                        {
                            let db: tauri::State<'_, Database> = app.state();
                            if let Err(e) = db.save_synthesis_v2(
                                &id,
                                &parsed.synthesis_public_json,
                                &parsed.synthesis_private_json,
                                &parsed.structured_signals_json,
                                parsed.fractional_signals_json.as_deref(),
                                parsed.ff_suitability_reason.as_deref(),
                            ) {
                                let msg = format!("[Synthesize] DB save failed for {}: {}", name, e);
                                let _ = db.log_activity(&job_id, "synthesize", "error", &msg);
                                let _ = db.set_company_error(&id, &msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                            synthesized_count.fetch_add(1, Ordering::Relaxed);
                            let _ = db.log_activity(
                                &job_id,
                                "synthesize",
                                "info",
                                &format!(
                                    "[Synthesize] {} — v2 chain ok (status={:?})",
                                    name, parsed.verification_status
                                ),
                            );

                            // Auto-qualify if thresholds met.
                            let rel: i64 = company.get("relevance_score")
                                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                .unwrap_or(0);
                            let qual: i64 = company.get("enrichment_quality")
                                .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                .unwrap_or(0);
                            if rel >= relevance_threshold && qual >= quality_threshold {
                                let _ = db.update_company_status(&id, "approved");
                                log::info!(
                                    "[Synthesize] {} auto-qualified (rel={}, qual={})",
                                    name, rel, qual
                                );
                            }
                        }

                        let _ = app.emit(
                            "pipeline:progress",
                            json!({
                                "stage": "synthesize",
                                "phase": "done",
                                "current_company": name,
                                "synthesized": synthesized_count.load(Ordering::Relaxed),
                                "errors": error_count.load(Ordering::Relaxed),
                            }),
                        );

                        let cur = synthesized_count.load(Ordering::Relaxed);
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
                        return;
                    }

                    // ── LEGACY PATH (DeepSeek / Ollama) ────────────────────────
                    // Kept so other operators' pipelines continue to populate the
                    // two original JSON columns. The marketplace-listing prompt
                    // produces lower-fidelity output than the two-pass Haiku chain
                    // per the Phase 0 quality audit, so operators targeting parity
                    // with Forge Capital should run with llm_backend="haiku".
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
                    let contact_title = company.get("contact_title").and_then(|v| v.as_str()).unwrap_or("");
                    let status = company.get("status").and_then(|v| v.as_str()).unwrap_or("Unknown");

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

                    let equipment_str = extracted_list(&verification_changes, "equipment", "name").unwrap_or_else(|| "Not specified".into());
                    let case_studies_str = extracted_list(&verification_changes, "case_studies", "title").unwrap_or_else(|| "Not specified".into());
                    let clients_str = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("clients_and_industries"))
                        .and_then(|v| v.get("named_clients"))
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().filter_map(|c| c.as_str().map(|s| s.to_string())).collect::<Vec<_>>().join(", "))
                        .unwrap_or_else(|| "Not specified".into());
                    let people_count = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("people"))
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.len())
                        .unwrap_or(0);
                    let people_str = verification_changes
                        .get("extracted")
                        .or_else(|| Some(&verification_changes))
                        .and_then(|v| v.get("people"))
                        .and_then(|v| v.as_array())
                        .map(|arr| arr.iter().take(10).filter_map(|p| {
                            let pname = p.get("name").and_then(|n| n.as_str()).unwrap_or("Unknown");
                            let ptitle = p.get("title").and_then(|t| t.as_str()).unwrap_or("");
                            if ptitle.is_empty() { Some(pname.to_string()) } else { Some(format!("{} ({})", pname, ptitle)) }
                        }).collect::<Vec<_>>().join("; "))
                        .unwrap_or_else(|| "Not identified".into());
                    let frac_hiring = fractional_signals.get("fractional_forge_signals").or_else(|| Some(&fractional_signals))
                        .and_then(|v| v.get("hiring_signals")).and_then(|v| v.as_str()).unwrap_or("Not detected");
                    let frac_activity = fractional_signals.get("fractional_forge_signals").or_else(|| Some(&fractional_signals))
                        .and_then(|v| v.get("activity_signals")).and_then(|v| v.as_str()).unwrap_or("Not detected");
                    let frac_business = fractional_signals.get("fractional_forge_signals").or_else(|| Some(&fractional_signals))
                        .and_then(|v| v.get("business_signals")).and_then(|v| v.as_str()).unwrap_or("Not detected");

                    let public_system = format!("You are a {} intelligence analyst writing marketplace listings for Fractional Forge, a B2B marketplace connecting buyers with {} companies. Write in a professional but approachable tone. Be specific — mention actual certifications, equipment, and capabilities.\n\nCRITICAL RULES:\n- Use only information provided. Do not infer or speculate.\n- Be specific about capabilities and differentiators.\n- Write for buyers looking for {} partners.\n- Highlight competitive differentiation.\n- NEVER include: director ages, acquisition scores, ownership structure, financial data, founder bios, board composition.\n- Return ONLY valid JSON. No markdown, no explanations.", active_domain, active_domain, active_domain);

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
  "capability_summary": "2-3 sentence pitch.",
  "ideal_buyer_profile": "Specific buyer persona.",
  "competitive_positioning": {{
    "market_segment": "specialty|mid-market|commodity",
    "production_type": "prototype|small_batch|medium_batch|high_volume|mixed",
    "technical_level": "basic|intermediate|advanced|cutting_edge",
    "pricing_tier": "premium|mid|value|unknown",
    "key_differentiator": "What one thing sets them apart?"
  }},
  "marketplace_tags": {{
    "primary_capabilities": [], "materials_expertise": [], "industry_focus": [],
    "certifications": [], "batch_size": "", "lead_time": ""
  }},
  "search_keywords": [],
  "data_quality_assessment": {{
    "overall_grade": "A|B|C|D", "confidence": 0.85,
    "missing_data": [], "needs_human_review": false, "review_reason": null
  }}
}}

Return ONLY valid JSON."#,
                        name = name, country = country, city = city, description = description,
                        category = category, subcategory = subcategory, certifications = certifications,
                        company_size = company_size, year_founded = year_founded, website_url = website_url,
                        equipment_str = equipment_str, case_studies_str = case_studies_str,
                        clients_str = clients_str, people_count = people_count,
                    );

                    let public_response = if llm_backend == "deepseek" {
                        crate::services::deepseek::chat(&deepseek_api_key, Some(&public_system), &public_user, true).await
                    } else {
                        crate::services::ollama::generate(&ollama_url, &enrich_model, &format!("{}\n\n{}", public_system, public_user), false).await
                    };

                    let public_json_str = match public_response {
                        Ok(r) => r,
                        Err(e) => {
                            let msg = format!("[Synthesize] Public synthesis LLM failed: {}", e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Failed for {}: {}", name, msg));
                            let _ = db.set_company_error(&id, &msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };
                    if serde_json::from_str::<Value>(&public_json_str).is_err() {
                        let msg = format!("[Synthesize] Public JSON parse error (len={})", public_json_str.len());
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Parse failed for {}: {}", name, msg));
                        let _ = db.set_company_error(&id, &msg);
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    }

                    let private_system = format!("You are a business analyst evaluating {} SMEs for a private equity investor interested in acquiring and improving {} companies. Be analytical and direct. Focus on actionable intelligence for acquisition diligence and fractional executive recruitment.\n\nCRITICAL RULES:\n- Use only information provided. Do not infer.\n- Focus on growth trajectory, team depth, and acquisition readiness.\n- Identify gaps where fractional executives could add value.\n- Assess M&A attractiveness based on signals in the data.\n- Return ONLY valid JSON. No markdown, no explanations.", active_domain, active_domain);

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

Return JSON with company_health_narrative, fractional_needs_analysis, approach_strategy,
fractional_executive_candidates, acquisition_fit. Return ONLY valid JSON."#,
                        name = name, country = country, city = city, status = status,
                        description = description, category = category, subcategory = subcategory,
                        certifications = certifications, company_size = company_size,
                        year_founded = year_founded, website_url = website_url,
                        contact_name = contact_name, contact_title = contact_title,
                        people_str = people_str, equipment_str = equipment_str, clients_str = clients_str,
                        frac_hiring = frac_hiring, frac_activity = frac_activity, frac_business = frac_business,
                    );

                    let private_response = if llm_backend == "deepseek" {
                        crate::services::deepseek::chat(&deepseek_api_key, Some(&private_system), &private_user, true).await
                    } else {
                        crate::services::ollama::generate(&ollama_url, &enrich_model, &format!("{}\n\n{}", private_system, private_user), false).await
                    };

                    let private_json_str = match private_response {
                        Ok(r) => r,
                        Err(e) => {
                            let msg = format!("[Synthesize] Private synthesis LLM failed: {}", e);
                            let db: tauri::State<'_, Database> = app.state();
                            let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Failed for {}: {}", name, msg));
                            let _ = db.set_company_error(&id, &msg);
                            error_count.fetch_add(1, Ordering::Relaxed);
                            return;
                        }
                    };
                    if serde_json::from_str::<Value>(&private_json_str).is_err() {
                        let msg = format!("[Synthesize] Private JSON parse error (len={})", private_json_str.len());
                        let db: tauri::State<'_, Database> = app.state();
                        let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Parse failed for {}: {}", name, msg));
                        let _ = db.set_company_error(&id, &msg);
                        error_count.fetch_add(1, Ordering::Relaxed);
                        return;
                    }

                    {
                        let db: tauri::State<'_, Database> = app.state();
                        match db.save_synthesis(&id, &public_json_str, &private_json_str) {
                            Ok(_) => {
                                synthesized_count.fetch_add(1, Ordering::Relaxed);
                                let _ = db.log_activity(&job_id, "synthesize", "info",
                                    &format!("[Synthesize] {} — legacy public+private saved", name));

                                let rel: i64 = company.get("relevance_score")
                                    .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                    .unwrap_or(0);
                                let qual: i64 = company.get("enrichment_quality")
                                    .and_then(|v| v.as_i64().or_else(|| v.as_str().and_then(|s| s.parse().ok())))
                                    .unwrap_or(0);
                                if rel >= relevance_threshold && qual >= quality_threshold {
                                    let _ = db.update_company_status(&id, "approved");
                                }
                            }
                            Err(e) => {
                                let msg = format!("[Synthesize] DB save failed: {}", e);
                                let _ = db.log_activity(&job_id, "synthesize", "error", &format!("[Synthesize] Failed to save for {}: {}", name, msg));
                                let _ = db.set_company_error(&id, &msg);
                                error_count.fetch_add(1, Ordering::Relaxed);
                                return;
                            }
                        }
                    }

                    let cur = synthesized_count.load(Ordering::Relaxed);
                    let cur_errors = error_count.load(Ordering::Relaxed);
                    let _ = app.emit("pipeline:progress", json!({
                        "stage": "synthesize", "phase": "done",
                        "current_company": name,
                        "synthesized": cur, "errors": cur_errors,
                    }));
                    if cur % 5 == 0 || cur == 1 {
                        let elapsed = (chrono::Utc::now() - started_at).num_seconds();
                        let rate = if elapsed > 0 { cur as f64 / elapsed as f64 * 3600.0 } else { 0.0 };
                        super::emit_node(&app, json!({
                            "node_id": "synthesize", "status": "running",
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
        "node_id": "synthesize", "status": "completed",
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

// Helper for legacy-path extraction — kept private.
fn extracted_list(val: &Value, key: &str, name_field: &str) -> Option<String> {
    val.get("extracted")
        .or(Some(val))
        .and_then(|v| v.get(key))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|e| {
                    e.get(name_field)
                        .and_then(|n| n.as_str())
                        .or_else(|| e.as_str())
                        .map(|s| s.to_string())
                })
                .collect::<Vec<_>>()
                .join(", ")
        })
}

// ───────────────────────────────────────────────────────────────────────────
// Tests (TDD — RED → GREEN for the pure helpers)
// ───────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use tempfile::TempDir;

    /// Canned Haiku VERIFY output for a realistic Ontario native plant nursery,
    /// one of the 5 companies called out by name in the Phase 0 quality audit as
    /// currently producing null synthesis columns.
    fn canned_verify_output() -> String {
        json!({
            "verification_status": "verified",
            "confidence": 0.82,
            "description_accuracy": "accurate",
            "description_from_website": "Not So Hollow Farm is a native plant nursery in Dufferin County, Ontario, founded in 2002 by Ian Payne and Viki Reynolds.",
            "decision_makers": [
                {"name": "Ian Payne", "title": "Owner", "bio": "Founder, Dufferin County", "linkedin": null, "email": null},
                {"name": "Viki Reynolds", "title": "Owner", "bio": "Founder", "linkedin": null, "email": null}
            ],
            "capability_claims": [
                "Ontario native species",
                "wholesale and retail supply",
                "container-grown native plants"
            ],
            "capacity_signals": [
                "Single site, Dufferin County"
            ],
            "prior_customer_signals": [
                "Conservation authorities",
                "Landscape architects"
            ],
            "field_corrections": {},
            "notes": "Founded 2002, founder-run."
        })
        .to_string()
    }

    /// Canned Haiku SYNTHESIS output.
    fn canned_synthesis_output() -> String {
        json!({
            "structured_signals": {
                "capability_fit_score": 62,
                "primary_capabilities": ["Ontario native plants", "container-grown propagation"],
                "certifications": [],
                "lead_time_hints": "Not stated",
                "capacity_hints": "Single site, Dufferin County, no stated machine count",
                "prior_customer_signals": ["Conservation authorities", "Landscape architects"],
                "technical_depth": "some",
                "operational_depth": "some"
            },
            "capability_and_fit": "Based on the visible site content, Not So Hollow Farm supplies container-grown Ontario native plants from a single Dufferin County site. Specialty segment, founder-run. No visible certifications.",
            "decision_maker_read": "Ian Payne and Viki Reynolds are named as owners on the site with no further bio. No named operations or sales lead.",
            "connection_brief": "Website active. Lists wholesale and retail contact routes. Not visibly active on LinkedIn. No trade-body memberships named on the site. Limited public visibility data available.",
            "ff_suitability_reason": "Plausible niche supplier for Ontario restoration projects; single strongest supporting fact is their stated 2002 founding and Ontario-native species specialism."
        })
        .to_string()
    }

    #[test]
    fn intent_defaults_to_supplier() {
        assert_eq!(CampaignIntent::from_config(&json!({})), CampaignIntent::Supplier);
    }

    #[test]
    fn intent_reads_customer_from_config() {
        let cfg = json!({"campaign_intent": "customer"});
        assert_eq!(CampaignIntent::from_config(&cfg), CampaignIntent::Customer);
    }

    #[test]
    fn intent_reads_buyer_alias() {
        let cfg = json!({"campaign_intent": "BUYER"});
        assert_eq!(CampaignIntent::from_config(&cfg), CampaignIntent::Customer);
    }

    #[test]
    fn verify_prompts_carry_website_wins_anchor() {
        assert!(verify_system_prompt_customer().contains("SINGLE SOURCE OF TRUTH"));
        assert!(verify_system_prompt_supplier().contains("SINGLE SOURCE OF TRUTH"));
    }

    #[test]
    fn synthesis_prompts_carry_anti_speculation_block_verbatim() {
        // The WRONG/RIGHT pair is the highest-leverage copy from 17-unified-pipeline.py.
        // If this test fails, a future edit has sanded off the anti-speculation rule.
        for p in [synthesis_system_prompt_customer(), synthesis_system_prompt_supplier()] {
            assert!(p.contains("WRONG:"), "anti-speculation WRONG/ example missing");
            assert!(p.contains("RIGHT:"), "anti-speculation RIGHT/ example missing");
            assert!(p.contains("Do NOT speculate"), "Do NOT speculate rule missing");
            assert!(p.contains("you do not know this person"), "'you do not know this person' missing");
            assert!(p.contains("Limited public visibility data available"),
                "thin-data fallback line missing");
        }
    }

    #[test]
    fn build_verify_user_prompt_includes_website_and_db() {
        let company = json!({
            "name": "Not So Hollow Farm",
            "website_url": "https://notsohollowfarm.ca",
            "description": "Native plant nursery.",
            "country": "Canada",
        });
        let prompt = build_verify_user_prompt(&company, "About us: Ontario native plants.", CampaignIntent::Supplier);
        assert!(prompt.contains("Not So Hollow Farm"));
        assert!(prompt.contains("notsohollowfarm.ca"));
        assert!(prompt.contains("Ontario native plants"));
        assert!(prompt.contains("DATABASE RECORDS:"));
        assert!(prompt.contains("capability claims"));
    }

    #[test]
    fn parse_synthesis_output_populates_all_five_columns() {
        // Integration-style: given known VERIFY + SYNTHESIS strings, the parser
        // produces non-null values in every one of the 5 JSON columns flagged by
        // the Phase 0 quality audit. This is the claim the task asks us to prove.
        let parsed = parse_synthesis_output(&canned_verify_output(), &canned_synthesis_output())
            .expect("parse should succeed");

        assert!(!parsed.synthesis_public_json.is_empty(), "synthesis_public_json empty");
        assert!(!parsed.synthesis_private_json.is_empty(), "synthesis_private_json empty");
        assert!(!parsed.structured_signals_json.is_empty(), "structured_signals_json empty");
        assert!(parsed.fractional_signals_json.is_some(), "fractional_signals_json None");
        assert!(parsed.ff_suitability_reason.is_some(), "ff_suitability_reason None");

        // All three stringified JSONs must parse back.
        let public: Value = serde_json::from_str(&parsed.synthesis_public_json).unwrap();
        let private: Value = serde_json::from_str(&parsed.synthesis_private_json).unwrap();
        let signals: Value = serde_json::from_str(&parsed.structured_signals_json).unwrap();

        // Key content checks — the anti-hallucination gate.
        assert_eq!(
            public.get("connection_brief").and_then(|v| v.as_str()).unwrap_or(""),
            "Website active. Lists wholesale and retail contact routes. Not visibly active on LinkedIn. No trade-body memberships named on the site. Limited public visibility data available."
        );
        assert!(private.get("decision_makers").and_then(|v| v.as_array()).is_some_and(|a| a.len() == 2));
        assert_eq!(signals.get("capability_fit_score").and_then(|v| v.as_i64()), Some(62));

        // ff_suitability_reason is a one-sentence analyst verdict, not hallucinated prose.
        let reason = parsed.ff_suitability_reason.as_deref().unwrap_or("");
        assert!(reason.contains("Plausible"), "reason = {}", reason);
        assert_eq!(parsed.verification_status.as_deref(), Some("verified"));
    }

    #[test]
    fn parse_synthesis_output_errors_cleanly_on_bad_json() {
        let err = parse_synthesis_output("not json", "not json").unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Verify JSON parse error"), "got: {}", msg);
    }

    #[test]
    fn parse_synthesis_output_thin_data_still_returns_three_json_strings() {
        // When SYNTHESIS has almost nothing, we still write valid JSON strings to
        // the three mandatory columns, but the two optional ones are None so
        // save_synthesis_v2's COALESCE leaves the old values untouched.
        let thin_verify = json!({"verification_status": "insufficient_content"}).to_string();
        let thin_synthesis = json!({"structured_signals": {}}).to_string();
        let parsed = parse_synthesis_output(&thin_verify, &thin_synthesis).unwrap();

        assert!(!parsed.synthesis_public_json.is_empty());
        assert!(!parsed.synthesis_private_json.is_empty());
        assert!(!parsed.structured_signals_json.is_empty());
        assert!(parsed.fractional_signals_json.is_none());
        assert!(parsed.ff_suitability_reason.is_none());
        assert_eq!(parsed.verification_status.as_deref(), Some("insufficient_content"));
    }

    /// End-to-end test of the DB write half of the new stage. Opens an in-memory
    /// Database (via TempDir), seeds a minimal verified company row, runs the
    /// canned LLM output through parse_synthesis_output, and writes via
    /// save_synthesis_v2. The test then asserts every one of the 5 target
    /// columns is non-NULL in the companies row — the exact condition the Phase
    /// 0 quality audit identified as failing in production.
    #[test]
    fn save_synthesis_v2_writes_all_five_columns() {
        let tmp = TempDir::new().expect("tempdir");
        let db = Database::new(tmp.path()).expect("db init");

        // Seed one company row. We bypass any helper and talk to the inner
        // connection directly — tests only, no production path uses this.
        {
            let conn = db.raw_conn_for_test();
            conn.execute(
                "INSERT INTO companies (id, name, domain, status, search_profile_id, verified_v2_at, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, datetime('now'), datetime('now'), datetime('now'))",
                rusqlite::params![
                    "test-co-1",
                    "Not So Hollow Farm",
                    "notsohollowfarm.ca",
                    "enriched",
                    "manufacturing",
                ],
            ).unwrap();
        }

        let parsed = parse_synthesis_output(&canned_verify_output(), &canned_synthesis_output()).unwrap();

        db.save_synthesis_v2(
            "test-co-1",
            &parsed.synthesis_public_json,
            &parsed.synthesis_private_json,
            &parsed.structured_signals_json,
            parsed.fractional_signals_json.as_deref(),
            parsed.ff_suitability_reason.as_deref(),
        )
        .expect("save_synthesis_v2 should succeed");

        // Read back and prove every one of the 5 columns is non-null.
        let conn = db.raw_conn_for_test();
        let (pub_j, priv_j, struct_j, frac_j, suit): (
            Option<String>, Option<String>, Option<String>, Option<String>, Option<String>
        ) = conn.query_row(
            "SELECT synthesis_public_json, synthesis_private_json, structured_signals_json, \
                    fractional_signals_json, ff_suitability_reason \
             FROM companies WHERE id = ?1",
            rusqlite::params!["test-co-1"],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?)),
        ).unwrap();

        assert!(pub_j.is_some(),    "synthesis_public_json still NULL after save");
        assert!(priv_j.is_some(),   "synthesis_private_json still NULL after save");
        assert!(struct_j.is_some(), "structured_signals_json still NULL after save");
        assert!(frac_j.is_some(),   "fractional_signals_json still NULL after save");
        assert!(suit.is_some(),     "ff_suitability_reason still NULL after save");

        // And the public JSON parses back into something recognisable.
        let v: Value = serde_json::from_str(&pub_j.unwrap()).unwrap();
        assert!(v.get("capability_and_fit").is_some());
        assert_eq!(v.get("source").and_then(|v| v.as_str()), Some("synthesize_v2"));
    }
}
