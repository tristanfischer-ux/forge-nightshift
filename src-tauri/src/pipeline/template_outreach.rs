use anyhow::Result;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

/// LLM-personalised template outreach: loads a template, fetches eligible companies
/// with full enrichment data, creates claim tokens, sends company data to Ollama
/// for personalisation, and saves as DRAFT only (no auto-send).
pub async fn run(
    app: &tauri::AppHandle,
    job_id: &str,
    config: &Value,
    template_id: &str,
) -> Result<Value> {
    let from_email = config
        .get("from_email")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let supabase_url = config
        .get("supabase_url")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let supabase_key = config
        .get("supabase_service_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434");
    let outreach_model = config
        .get("outreach_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3.5:27b-q4_K_M");

    if from_email.is_empty() {
        anyhow::bail!("from_email not configured");
    }
    if supabase_url.is_empty() || supabase_key.is_empty() {
        anyhow::bail!("Supabase credentials not configured");
    }

    let daily_limit: i64 = config
        .get("daily_email_limit")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    // Load template (used for reference in logging, template_id links to DB)
    {
        let db: tauri::State<'_, Database> = app.state();
        let template = db.get_email_template(template_id)?;
        let name = template.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
        db.log_activity(job_id, "template_outreach", "info",
            &format!("Using template '{}' with Ollama model '{}'", name, outreach_model))?;
    }

    // Fetch eligible companies (full enrichment data)
    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_campaign_eligible_companies(daily_limit)?
    };

    let total = companies.len();
    let mut drafts_created = 0i64;
    let mut error_count = 0i64;

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "template_outreach",
            "info",
            &format!("Generating personalised drafts for {} eligible companies (limit {})", total, daily_limit),
        );
    }

    for company in &companies {
        if super::is_cancelled() || drafts_created >= daily_limit {
            break;
        }

        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("");
        let listing_id = company.get("supabase_listing_id").and_then(|v| v.as_str()).unwrap_or("");

        if contact_email.is_empty() || listing_id.is_empty() {
            continue;
        }

        // Create claim token via Supabase
        let claim_token = match crate::services::supabase::create_claim_token(
            supabase_url, supabase_key, listing_id, contact_email,
        ).await {
            Ok(token) => token,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "template_outreach", "error",
                    &format!("Claim token failed for {}: {}", company_name, e));
                error_count += 1;
                continue;
            }
        };

        let claim_url = format!("https://fractionalforge.app/claim/{}", claim_token);

        // Build Ollama prompt with company data
        let prompt = build_personalisation_prompt(company, &claim_url);

        // Call Ollama for personalisation
        let llm_output = match crate::services::ollama::generate(
            ollama_url, outreach_model, &prompt, false,
        ).await {
            Ok(text) => text,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "template_outreach", "error",
                    &format!("Ollama generation failed for {}: {}", company_name, e));
                error_count += 1;
                continue;
            }
        };

        // Parse subject and body from LLM output
        let (subject, body) = parse_email_output(&llm_output, company_name);

        if body.is_empty() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "template_outreach", "error",
                &format!("Empty email body from Ollama for {}", company_name));
            error_count += 1;
            continue;
        }

        // Save as DRAFT — do NOT send
        {
            let db: tauri::State<'_, Database> = app.state();
            db.insert_template_email(
                company_id, template_id, &subject, &body,
                contact_email, from_email, &claim_token,
            )?;
        }

        drafts_created += 1;

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "template_outreach", "info",
                &format!("Draft created for {} ({}/{})", company_name, drafts_created, total));
        }

        let _ = app.emit("pipeline:progress", json!({
            "stage": "template_outreach",
            "drafts": drafts_created,
            "total": total,
            "limit": daily_limit,
        }));

        // Small delay between Ollama calls
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }

    Ok(json!({
        "drafts_created": drafts_created,
        "errors": error_count,
        "eligible": total,
    }))
}

/// Extract a string field from a company JSON value, falling back to default.
fn str_or<'a>(company: &'a Value, field: &str, default: &'a str) -> &'a str {
    company
        .get(field)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .unwrap_or(default)
}

/// Parse a JSON array field (may be stored as JSON string in SQLite) into comma-separated text.
fn json_array_to_text(company: &Value, field: &str) -> String {
    let val = company.get(field).cloned().unwrap_or(Value::Null);
    let arr = if let Some(s) = val.as_str() {
        serde_json::from_str::<Value>(s).ok().unwrap_or(Value::Null)
    } else {
        val
    };
    if let Some(items) = arr.as_array() {
        items.iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        String::new()
    }
}

/// Extract array fields from attributes_json (materials, key_equipment, etc.)
fn attrs_array_to_text(company: &Value, field: &str) -> String {
    let attrs_str = company.get("attributes_json").and_then(|v| v.as_str()).unwrap_or("{}");
    let attrs: Value = serde_json::from_str(attrs_str).unwrap_or(json!({}));
    if let Some(arr) = attrs.get(field).and_then(|v| v.as_array()) {
        arr.iter()
            .filter_map(|v| v.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    } else {
        String::new()
    }
}

/// Build the full Ollama prompt with company data filled in.
fn build_personalisation_prompt(company: &Value, claim_url: &str) -> String {
    let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let contact_name = str_or(company, "contact_name", "");
    let contact_title = str_or(company, "contact_title", "");
    let subcategory = str_or(company, "subcategory", "");
    let city = str_or(company, "city", "");
    let description = str_or(company, "description", "");
    let company_size = str_or(company, "company_size", "");
    let year_founded = company.get("year_founded").and_then(|v| v.as_i64());

    let specialties = json_array_to_text(company, "specialties");
    let certifications = json_array_to_text(company, "certifications");
    let industries = json_array_to_text(company, "industries");
    let materials = attrs_array_to_text(company, "materials");
    let key_equipment = attrs_array_to_text(company, "key_equipment");

    let founded_str = year_founded
        .map(|y| y.to_string())
        .unwrap_or_default();

    format!(
r#"You are writing a personalised outreach email for Fractional Forge, a UK manufacturing marketplace. You will receive company data and an approved email structure. Your job is to write a natural, personalised version of the email using the company's real data.

COMPANY DATA:
- Company name: {company_name}
- Contact name: {contact_name}
- Contact title: {contact_title}
- Subcategory: {subcategory}
- City: {city}
- Description: {description}
- Specialties: {specialties}
- Certifications: {certifications}
- Materials: {materials}
- Key equipment: {key_equipment}
- Industries: {industries}
- Company size: {company_size}
- Founded: {founded}
- Claim URL: {claim_url}

Write an email following this exact structure. Do not add, remove, or reorder any sections.

SECTION 1 — SUBJECT LINE
Write: "[Company name] is already on Fractional Forge — claim your listing"

SECTION 2 — GREETING AND INTRO
Write: "Hi [first name]," then a fixed line: "I'm Tristan, and I'm building Fractional Forge — a marketplace that makes it easier for UK startups to find and work with British manufacturers."

SECTION 3 — HOW WE FOUND THEM (personalise this)
Write 2 sentences. First sentence: say you came across them while researching [their subcategory] companies in [their city/region]. Second sentence: mention that you've already created a listing covering their specific capabilities — pick 2-3 of their most notable specialties or certifications from the data. End with: "You can claim it and check everything's accurate — it takes about two minutes."

SECTION 4 — WHY STARTUPS NEED THEM (personalise this)
Write 2-3 sentences. Describe a concrete, realistic scenario where a hardware startup would need this company's services. Be specific — reference the actual type of work based on their specialties and materials. For example, if they do injection moulding, mention a startup needing a first batch of moulded enclosures. If they do heat treatment, mention a startup needing stress relieving on titanium parts. Do not be generic. Do not say "various industries."

SECTION 5 — FRACTIONAL EXECUTIVE ANGLE (personalise this)
Write 2 sentences. Suggest what kind of specialist advisory work someone on their team could offer. Base this on their subcategory and specialties — for example, "mould design and material selection" or "DFM for sheet metal" or "alloy selection for casting applications." Frame it as monetising expertise they already have, generating additional revenue.

SECTION 6 — FIRST MOVER ADVANTAGE (fixed)
Write exactly: "We're in early launch, so there's a genuine first-mover advantage: companies that join now will be the first recommendation in their category for the next six months. After that, ranking shifts to user reviews and quality."

SECTION 7 — FREE TO CLAIM (fixed)
Write exactly: "Claiming your listing is completely free. No cost unless a transaction happens through the platform."

SECTION 8 — CLAIM LINK (fixed)
Write exactly: "Claim your listing here: {claim_url}"

SECTION 9 — SIGN OFF (fixed)
Write exactly:
"Any questions, just reply — it comes straight to me.

Best,
Tristan Fischer
Founder, Fractional Forge
fractionalforge.app"

RULES:
1. Use natural, conversational British English. Not salesy, not corporate.
2. Only reference capabilities, certifications, materials, and equipment that appear in the company data. Never invent or assume.
3. If a data field is empty, skip it — do not mention it or draw attention to its absence.
4. Keep the total email under 250 words (excluding subject and sign-off).
5. No emojis, no bullet points, no bold text, no HTML formatting.
6. Output the complete email as plain text. First line must be the subject prefixed with "Subject: ".
7. If the contact name is empty or null, use "Hi there," as the greeting.
8. Do not include any preamble, explanation, or commentary — just the email."#,
        company_name = company_name,
        contact_name = contact_name,
        contact_title = contact_title,
        subcategory = subcategory,
        city = city,
        description = description,
        specialties = specialties,
        certifications = certifications,
        materials = materials,
        key_equipment = key_equipment,
        industries = industries,
        company_size = company_size,
        founded = founded_str,
        claim_url = claim_url,
    )
}

/// Parse the LLM output into (subject, body).
/// Expects first line to be "Subject: ..." followed by the email body.
fn parse_email_output(output: &str, company_name: &str) -> (String, String) {
    let trimmed = output.trim();

    // Try to extract "Subject: ..." from first line
    let (subject, body) = if let Some(rest) = trimmed.strip_prefix("Subject:") {
        if let Some(newline_pos) = rest.find('\n') {
            let subj = rest[..newline_pos].trim().to_string();
            let body = rest[newline_pos..].trim().to_string();
            (subj, body)
        } else {
            // Only subject line, no body
            (rest.trim().to_string(), String::new())
        }
    } else {
        // No "Subject:" prefix — use default subject, treat entire output as body
        let default_subject = format!(
            "{} is already on Fractional Forge — claim your listing",
            company_name
        );
        (default_subject, trimmed.to_string())
    };

    // Convert plain text body to simple HTML (wrap paragraphs in <p> tags)
    let html_body = body
        .split("\n\n")
        .filter(|p| !p.trim().is_empty())
        .map(|p| format!("<p>{}</p>", p.trim().replace('\n', "<br/>")))
        .collect::<Vec<_>>()
        .join("\n");

    (subject, html_body)
}
