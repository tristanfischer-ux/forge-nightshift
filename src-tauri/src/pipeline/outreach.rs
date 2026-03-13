use anyhow::Result;
use serde_json::Value;
use tauri::Manager;

use crate::db::Database;

/// DEPRECATED: This stage auto-sends emails without review.
/// Use template_outreach (draft-only with Ollama personalisation) instead.
/// This function now refuses to run and returns an error to prevent accidental sends.
pub async fn run(app: &tauri::AppHandle, job_id: &str, _config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(
        job_id,
        "outreach",
        "warn",
        "Old outreach stage is disabled — use template_outreach (draft-only) instead",
    );
    anyhow::bail!("Old outreach stage is disabled. Use template_outreach via the Outreach UI (generates drafts for review, does not auto-send).");
}
