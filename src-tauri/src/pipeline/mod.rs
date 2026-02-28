mod research;
mod enrich;
mod push;
mod outreach;
mod report;

use anyhow::Result;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicBool, Ordering};
use tauri::{Emitter, Manager};

use crate::db::Database;

static RUNNING: AtomicBool = AtomicBool::new(false);
static CANCEL: AtomicBool = AtomicBool::new(false);

pub async fn start_pipeline(app: tauri::AppHandle, stages: Vec<String>) -> Result<String> {
    if RUNNING.load(Ordering::SeqCst) {
        anyhow::bail!("Pipeline is already running");
    }

    RUNNING.store(true, Ordering::SeqCst);
    CANCEL.store(false, Ordering::SeqCst);

    let db: tauri::State<'_, Database> = app.state();
    let job_id = db.insert_job(&stages)?;
    let job_id_clone = job_id.clone();

    let _ = app.emit("pipeline:status", json!({
        "status": "running",
        "job_id": &job_id,
        "stages": &stages,
    }));

    tauri::async_runtime::spawn(async move {
        let result = run_stages(&app, &job_id_clone, &stages).await;

        let db: tauri::State<'_, Database> = app.state();
        let (status, summary) = match result {
            Ok(summary) => ("completed", summary),
            Err(e) => ("failed", json!({"error": e.to_string()})),
        };

        let _ = db.update_job(&job_id_clone, status, &summary);

        let _ = app.emit("pipeline:status", json!({
            "status": status,
            "job_id": &job_id_clone,
            "summary": &summary,
        }));

        RUNNING.store(false, Ordering::SeqCst);
    });

    Ok(job_id)
}

pub async fn stop_pipeline(app: tauri::AppHandle) -> Result<()> {
    CANCEL.store(true, Ordering::SeqCst);
    let _ = app.emit("pipeline:status", json!({"status": "cancelling"}));
    Ok(())
}

pub fn get_status() -> Result<Value> {
    Ok(json!({
        "running": RUNNING.load(Ordering::SeqCst),
        "cancelling": CANCEL.load(Ordering::SeqCst),
    }))
}

pub fn is_cancelled() -> bool {
    CANCEL.load(Ordering::SeqCst)
}

async fn run_stages(app: &tauri::AppHandle, job_id: &str, stages: &[String]) -> Result<Value> {
    let mut summary = json!({});

    let db: tauri::State<'_, Database> = app.state();
    let config = db.get_all_config()?;

    for stage in stages {
        if is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, stage, "warn", "Pipeline cancelled by user");
            break;
        }

        let _ = app.emit("pipeline:stage", json!({
            "stage": stage,
            "status": "running",
        }));

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, stage, "info", &format!("Starting {} stage", stage));
        }

        let stage_result = match stage.as_str() {
            "research" => research::run(app, job_id, &config).await,
            "enrich" => enrich::run(app, job_id, &config).await,
            "push" => push::run(app, job_id, &config).await,
            "outreach" => outreach::run(app, job_id, &config).await,
            "report" => report::run(app, job_id, &config).await,
            unknown => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, unknown, "error", "Unknown stage");
                Err(anyhow::anyhow!("Unknown stage: {}", unknown))
            }
        };

        match stage_result {
            Ok(result) => {
                summary[stage] = result;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, stage, "info", &format!("{} stage completed", stage));
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, stage, "error", &format!("{} stage failed: {}", stage, e));
                summary[stage] = json!({"error": e.to_string()});
            }
        }

        let _ = app.emit("pipeline:stage", json!({
            "stage": stage,
            "status": "completed",
        }));
    }

    Ok(summary)
}
