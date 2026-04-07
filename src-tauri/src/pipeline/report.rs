use anyhow::Result;
use serde_json::{json, Value};
use tauri::{Emitter, Manager};

use crate::db::Database;

pub async fn run(app: &tauri::AppHandle, job_id: &str, _config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();

    let stats = db.get_stats(None)?;
    let logs = db.get_run_log(Some(job_id), 50)?;

    let error_count = logs
        .iter()
        .filter(|l| l.get("level").and_then(|v| v.as_str()) == Some("error"))
        .count();

    let summary = json!({
        "job_id": job_id,
        "stats": stats,
        "errors": error_count,
        "log_entries": logs.len(),
        "completed_at": chrono::Utc::now().to_rfc3339(),
    });

    let _ = app.emit("pipeline:report", &summary);

    let _ = db.log_activity(
        job_id,
        "report",
        "info",
        &format!("Pipeline completed. {} errors.", error_count),
    );

    Ok(summary)
}
