use anyhow::Result;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

/// Template-based outreach: loads a template, fetches eligible companies,
/// creates claim tokens via Supabase, renders template with placeholders,
/// sends via Resend, and records everything.
pub async fn run(
    app: &tauri::AppHandle,
    job_id: &str,
    config: &Value,
    template_id: &str,
) -> Result<Value> {
    let resend_key = config
        .get("resend_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");
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

    if resend_key.is_empty() || from_email.is_empty() {
        anyhow::bail!("Resend API key or from_email not configured");
    }
    if supabase_url.is_empty() || supabase_key.is_empty() {
        anyhow::bail!("Supabase credentials not configured");
    }

    let daily_limit: i64 = config
        .get("daily_email_limit")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    // Load template
    let template = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_email_template(template_id)?
    };

    let template_subject = template
        .get("subject")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let template_body = template
        .get("body")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if template_body.is_empty() {
        anyhow::bail!("Template body is empty");
    }

    // Fetch eligible companies
    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_campaign_eligible_companies(daily_limit)?
    };

    let total = companies.len();
    let mut emails_sent = 0i64;
    let mut error_count = 0i64;

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "template_outreach",
            "info",
            &format!("Starting template campaign with {} eligible companies (limit {})", total, daily_limit),
        );
    }

    for company in &companies {
        if super::is_cancelled() || emails_sent >= daily_limit {
            break;
        }

        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let contact_name = company
            .get("contact_name")
            .and_then(|v| v.as_str())
            .filter(|s| !s.is_empty())
            .unwrap_or("there");
        let contact_email = company
            .get("contact_email")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let listing_id = company
            .get("supabase_listing_id")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if contact_email.is_empty() || listing_id.is_empty() {
            continue;
        }

        // Create claim token via Supabase
        let claim_token = match crate::services::supabase::create_claim_token(
            supabase_url,
            supabase_key,
            listing_id,
            contact_email,
        )
        .await
        {
            Ok(token) => token,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "template_outreach",
                    "error",
                    &format!("Claim token creation failed for {}: {}", company_name, e),
                );
                error_count += 1;
                continue;
            }
        };

        let claim_url = format!("https://fractionalforge.app/claim/{}", claim_token);

        // Render template with placeholders
        let rendered_subject = template_subject
            .replace("{company_name}", company_name)
            .replace("{contact_name}", contact_name)
            .replace("{claim_url}", &claim_url);

        let rendered_body = template_body
            .replace("{company_name}", company_name)
            .replace("{contact_name}", contact_name)
            .replace("{claim_url}", &claim_url);

        // Store email draft with template reference
        let email_id = {
            let db: tauri::State<'_, Database> = app.state();
            db.insert_template_email(
                company_id,
                template_id,
                &rendered_subject,
                &rendered_body,
                contact_email,
                from_email,
                &claim_token,
            )?
        };

        // Send via Resend
        match crate::services::resend::send_email(
            resend_key,
            from_email,
            contact_email,
            &rendered_subject,
            &rendered_body,
        )
        .await
        {
            Ok(resend_id) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.update_email_sent(&email_id, &resend_id);
                let _ = db.log_activity(
                    job_id,
                    "template_outreach",
                    "info",
                    &format!("Email sent to {} at {} (claim: {})", company_name, contact_email, &claim_token[..8]),
                );
                emails_sent += 1;

                let _ = app.emit(
                    "pipeline:progress",
                    json!({
                        "stage": "template_outreach",
                        "sent": emails_sent,
                        "total": total,
                        "limit": daily_limit,
                    }),
                );
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "template_outreach",
                    "error",
                    &format!("Failed to send email to {}: {}", contact_email, e),
                );
                let _ = db.update_email_status(&email_id, "failed");
                error_count += 1;
            }
        }

        // Small delay between sends to avoid rate limiting
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Ok(json!({
        "emails_sent": emails_sent,
        "errors": error_count,
        "eligible": total,
    }))
}
