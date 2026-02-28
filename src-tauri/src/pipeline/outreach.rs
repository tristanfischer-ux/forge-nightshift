use anyhow::Result;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let resend_key = config
        .get("resend_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    let from_email = config
        .get("from_email")
        .and_then(|v| v.as_str())
        .unwrap_or("");

    if resend_key.is_empty() || from_email.is_empty() {
        anyhow::bail!("Resend API key or from_email not configured");
    }

    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434");
    let outreach_model = config
        .get("outreach_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3:32b");

    let daily_limit: i64 = config
        .get("daily_email_limit")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies(Some("pushed"), daily_limit, 0)?
    };

    let mut emails_generated = 0;
    let mut emails_sent = 0;
    let mut error_count = 0;

    for company in &companies {
        if super::is_cancelled() || emails_sent >= daily_limit {
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let contact_email = company
            .get("contact_email")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let contact_name = company
            .get("contact_name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let country = company
            .get("country")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let description = company
            .get("description")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if contact_email.is_empty() {
            continue;
        }

        let language_instruction = match country {
            "DE" => "Write the email in German.",
            "FR" => "Write the email in French.",
            "NL" => "Write the email in Dutch.",
            "IT" => "Write the email in Italian.",
            _ => "Write the email in English.",
        };

        let language = match country {
            "DE" => "de",
            "FR" => "fr",
            "NL" => "nl",
            "IT" => "it",
            _ => "en",
        };

        let email_prompt = format!(
            r#"Generate a personalized B2B outreach email for a manufacturing marketplace called ForgeOS.

The email should:
1. Be professional and personalized to the company
2. Pitch three value propositions: marketplace sales visibility, fractional executive income, and facility bookings
3. Be concise (under 200 words)
4. Include a clear call-to-action
5. {}
6. Include an unsubscribe line at the bottom

Company: {}
Contact: {} ({})
Description: {}
Country: {}

Return JSON with:
- subject: email subject line
- body: email body in HTML format (use <p> tags, no <html>/<body> wrappers)

Return ONLY valid JSON."#,
            language_instruction, name, contact_name, contact_email, description, country
        );

        let response = match crate::services::ollama::generate(
            ollama_url,
            outreach_model,
            &email_prompt,
            true,
        )
        .await
        {
            Ok(r) => r,
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "outreach",
                    "error",
                    &format!("Email generation failed for {}: {}", name, e),
                );
                error_count += 1;
                continue;
            }
        };

        let email_data: Value = match serde_json::from_str(&response) {
            Ok(v) => v,
            Err(_) => {
                error_count += 1;
                continue;
            }
        };

        let subject = email_data
            .get("subject")
            .and_then(|v| v.as_str())
            .unwrap_or("ForgeOS Marketplace Opportunity");
        let body = email_data
            .get("body")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if body.is_empty() {
            continue;
        }

        // Store email draft
        let email_id = {
            let db: tauri::State<'_, Database> = app.state();
            db.insert_email(id, subject, body, contact_email, from_email, language)?
        };
        emails_generated += 1;

        // Send via Resend
        match crate::services::resend::send_email(resend_key, from_email, contact_email, subject, body)
            .await
        {
            Ok(resend_id) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.update_email_sent(&email_id, &resend_id);
                let _ = db.log_activity(
                    job_id,
                    "outreach",
                    "info",
                    &format!(
                        "Email sent to {} at {} (resend_id: {})",
                        name, contact_email, resend_id
                    ),
                );
                emails_sent += 1;

                let _ = app.emit(
                    "pipeline:progress",
                    json!({
                        "stage": "outreach",
                        "sent": emails_sent,
                        "limit": daily_limit,
                    }),
                );
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "outreach",
                    "error",
                    &format!("Failed to send email to {}: {}", contact_email, e),
                );
                let _ = db.update_email_status(&email_id, "failed");
                error_count += 1;
            }
        }
    }

    Ok(json!({
        "emails_generated": emails_generated,
        "emails_sent": emails_sent,
        "errors": error_count,
    }))
}
