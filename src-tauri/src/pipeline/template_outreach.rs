use anyhow::Result;
use rand::Rng;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

/// Template outreach: fetches eligible companies with enrichment data,
/// creates claim tokens, assembles fixed-template emails (no LLM),
/// and saves as DRAFT only (no auto-send).
///
/// The email's only job is to get the recipient to click the claim link.
/// All explanation of the platform, advisory model, etc. lives on the claim page.
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
            &format!("Using template '{}' (no LLM — pure template)", name))?;
    }

    // Load learning metadata for DB tracking (insights/experiment still recorded per draft)
    let (insights, active_experiment) = {
        let db: tauri::State<'_, Database> = app.state();
        let insights = db.get_active_insights(10).unwrap_or_default();
        let experiment = db.get_active_experiment().unwrap_or(None);
        (insights, experiment)
    };

    let insight_texts: Vec<String> = insights
        .iter()
        .filter_map(|i| i.get("insight").and_then(|v| v.as_str()).map(|s| s.to_string()))
        .collect();

    let experiment_id = active_experiment
        .as_ref()
        .and_then(|e| e.get("id").and_then(|v| v.as_str()))
        .map(|s| s.to_string());
    let strategy_a = active_experiment
        .as_ref()
        .and_then(|e| e.get("variant_a_strategy").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let strategy_b = active_experiment
        .as_ref()
        .and_then(|e| e.get("variant_b_strategy").and_then(|v| v.as_str()))
        .unwrap_or("")
        .to_string();
    let generation = active_experiment
        .as_ref()
        .and_then(|e| e.get("generation").and_then(|v| v.as_i64()))
        .unwrap_or(0);

    let insights_json = if insight_texts.is_empty() {
        None
    } else {
        serde_json::to_string(&insight_texts).ok()
    };

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
            &format!("Generating drafts for {} eligible companies (limit {})", total, daily_limit),
        );
    }

    for company in &companies {
        if super::is_cancelled() || drafts_created >= daily_limit {
            break;
        }

        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("");
        let contact_name = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("");
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

        // Determine A/B variant (tracked for future analysis, even without LLM)
        let variant = if !strategy_a.is_empty() && !strategy_b.is_empty() {
            if drafts_created % 2 == 0 { "A" } else { "B" }
        } else {
            "A"
        };
        let strategy = if variant == "A" { &strategy_a } else { &strategy_b };

        // Build a teaser line from enrichment data (what data fields we have on them)
        let data_teaser = build_data_teaser(company);

        // Assemble the full email — no LLM, pure template, varied subject line
        let (subject, body) = assemble_email(company, contact_name, company_name, &data_teaser, &claim_url);

        // Save as DRAFT with learning metadata
        {
            let db: tauri::State<'_, Database> = app.state();
            db.insert_template_email_with_learning(
                company_id, template_id, &subject, &body,
                contact_email, from_email, &claim_token,
                Some(variant),
                if strategy.is_empty() { None } else { Some(strategy) },
                generation,
                experiment_id.as_deref(),
                insights_json.as_deref(),
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
    }

    Ok(json!({
        "drafts_created": drafts_created,
        "errors": error_count,
        "eligible": total,
    }))
}

/// Extract the first name from a full contact name.
fn first_name(contact_name: &str) -> &str {
    contact_name.split_whitespace().next().unwrap_or(contact_name)
}

/// HTML-escape text to prevent malformed HTML from company names with & or <.
fn html_escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Parse a JSON array field (may be stored as JSON string in SQLite) into a Vec of strings.
fn json_array_to_vec(company: &Value, field: &str) -> Vec<String> {
    let val = company.get(field).cloned().unwrap_or(Value::Null);
    let arr = if let Some(s) = val.as_str() {
        serde_json::from_str::<Value>(s).ok().unwrap_or(Value::Null)
    } else {
        val
    };
    if let Some(items) = arr.as_array() {
        items.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    } else {
        Vec::new()
    }
}

/// Extract array fields from attributes_json (materials, key_equipment, etc.)
fn attrs_array_to_vec(company: &Value, field: &str) -> Vec<String> {
    let attrs_str = company.get("attributes_json").and_then(|v| v.as_str()).unwrap_or("{}");
    let attrs: Value = serde_json::from_str(attrs_str).unwrap_or(json!({}));
    if let Some(arr) = attrs.get(field).and_then(|v| v.as_array()) {
        arr.iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect()
    } else {
        Vec::new()
    }
}

/// Build a short teaser describing what data we have on the company.
/// This does NOT reveal the details — it tells them what categories of info
/// we've captured, creating curiosity to click through and check.
///
/// Examples:
/// - "I've put together a listing based on your capabilities, certifications, and the materials you work with."
/// - "I've put together a listing based on your capabilities and equipment."
/// - "I've put together a listing based on what I could find online."
pub fn build_data_teaser(company: &Value) -> String {
    let has_specialties = !json_array_to_vec(company, "specialties").is_empty();
    let has_certs = !json_array_to_vec(company, "certifications").is_empty();
    let has_materials = !attrs_array_to_vec(company, "materials").is_empty();
    let has_equipment = !attrs_array_to_vec(company, "key_equipment").is_empty();

    let mut parts: Vec<&str> = Vec::new();
    if has_specialties { parts.push("your capabilities"); }
    if has_certs { parts.push("certifications"); }
    if has_materials { parts.push("the materials you work with"); }
    if has_equipment { parts.push("equipment"); }

    if parts.is_empty() {
        "I've put together a listing based on what I could find online.".to_string()
    } else if parts.len() == 1 {
        format!("I've put together a listing based on {}.", parts[0])
    } else {
        let last = parts.pop().unwrap();
        format!("I've put together a listing based on {}, and {}.", parts.join(", "), last)
    }
}

/// Pick a varied subject line based on available company data.
/// Tier 1 subjects require specific enrichment fields; Tier 2 only need company name.
/// One is chosen at random from all applicable options.
fn pick_subject_line(company: &Value, company_name: &str) -> String {
    let mut pool: Vec<String> = Vec::new();

    // Tier 1 — data-rich (only when relevant data exists)
    let specialties = json_array_to_vec(company, "specialties");
    if let Some(first) = specialties.first() {
        pool.push(format!("Your {} capabilities caught my eye", first));
    }

    let certs = json_array_to_vec(company, "certifications");
    if let Some(first) = certs.first() {
        pool.push(format!("{} certified — have I got the details right?", first));
    }

    let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("");
    if !city.is_empty() {
        pool.push(format!("I found {} while researching {} manufacturers", company_name, city));
    }

    let materials = attrs_array_to_vec(company, "materials");
    if let Some(first) = materials.first() {
        pool.push(format!("Your {} work — quick question", first));
    }

    // Tier 2 — always available (company name only)
    pool.push(format!("{} — is this listing accurate?", company_name));
    pool.push(format!("Quick question about {}", company_name));
    pool.push(format!("Listing for {}", company_name));
    pool.push(format!("{} — worth a quick look?", company_name));

    let idx = rand::thread_rng().gen_range(0..pool.len());
    pool.swap_remove(idx)
}

/// Assemble the full email from fixed template text.
/// No LLM involved — the only personalisation is company name, contact name,
/// claim URL, and a teaser of what data categories we have.
/// Returns (subject, html_body).
pub fn assemble_email(
    company: &Value,
    contact_name: &str,
    company_name: &str,
    data_teaser: &str,
    claim_url: &str,
) -> (String, String) {
    let greeting = if contact_name.is_empty() {
        "Hi,".to_string()
    } else {
        format!("Hi {},", first_name(contact_name))
    };

    let safe_company = html_escape(company_name);

    let subject = pick_subject_line(company, company_name);

    let body = format!(
        "{greeting}\n\n\
        I've added {safe_company} to Fractional Forge \u{2014} a platform I'm building to help \
        companies find UK manufacturers. {data_teaser}\n\n\
        Could you take two minutes to check I got it right?\n\
        <a href=\"{claim_url}\">Check your listing</a>\n\n\
        Bit about me \u{2014} I've set up and equipped factories myself with products shipping \
        from them, so this comes from experience. I'm trying to make UK manufacturing easier to find.\n\n\
        No cost to be listed. Any thoughts on how to make this more useful, I'm all ears.\n\n\
        Tristan\n\
        <a href=\"https://fractionalforge.app\">fractionalforge.app</a> | \
        <a href=\"https://www.linkedin.com/in/tristanfischer/\">LinkedIn</a>"
    );

    let html_body = html_wrap(&body);
    (subject, html_body)
}

/// Convert plain text email body to simple HTML (paragraphs wrapped in <p> tags).
fn html_wrap(text: &str) -> String {
    text.split("\n\n")
        .filter(|p| !p.trim().is_empty())
        .map(|p| format!("<p style=\"margin:0 0 12px 0;\">{}</p>", p.trim().replace('\n', "<br/>")))
        .collect::<Vec<_>>()
        .join("\n")
}
