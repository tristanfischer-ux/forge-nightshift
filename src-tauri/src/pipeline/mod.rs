mod research;
mod enrich;
mod push;
mod outreach;
mod report;
mod deep_enrich;
mod technique_aggregate;

use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use tauri::{Emitter, Manager};

use crate::db::Database;

static RUNNING: AtomicBool = AtomicBool::new(false);
static CANCEL: AtomicBool = AtomicBool::new(false);
static RESEARCH_ACTIVE: AtomicBool = AtomicBool::new(false);

fn node_states() -> &'static Mutex<HashMap<String, Value>> {
    static STATES: OnceLock<Mutex<HashMap<String, Value>>> = OnceLock::new();
    STATES.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn emit_node(app: &tauri::AppHandle, payload: Value) {
    if let Some(node_id) = payload.get("node_id").and_then(|v| v.as_str()) {
        // Store state without holding lock during emit (prevents IPC backpressure blocking pipeline)
        if let Ok(mut states) = node_states().try_lock() {
            states.insert(node_id.to_string(), payload.clone());
        }
    }
    let _ = app.emit("pipeline:node", &payload);
}

pub fn get_all_node_states() -> HashMap<String, Value> {
    node_states().lock().map(|s| s.clone()).unwrap_or_default()
}

pub fn reset_node_states() {
    if let Ok(mut states) = node_states().lock() {
        states.clear();
    }
}

pub fn is_research_active() -> bool {
    RESEARCH_ACTIVE.load(Ordering::SeqCst)
}

pub async fn start_pipeline(app: tauri::AppHandle, stages: Vec<String>) -> Result<String> {
    if RUNNING.load(Ordering::SeqCst) {
        anyhow::bail!("Pipeline is already running");
    }

    RUNNING.store(true, Ordering::SeqCst);
    CANCEL.store(false, Ordering::SeqCst);
    reset_node_states();

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

    // Auto-backup before pipeline run
    {
        let db: tauri::State<'_, Database> = app.state();
        let app_dir = app
            .path()
            .app_data_dir()
            .unwrap_or_default();
        let backup_dir = app_dir.join("backups");
        if std::fs::create_dir_all(&backup_dir).is_ok() {
            let timestamp = chrono::Local::now().format("%Y-%m-%d_%H%M%S");
            let backup_path = backup_dir.join(format!("nightshift_backup_{}.db", timestamp));
            match db.backup(&backup_path) {
                Ok(_) => {
                    let _ = db.log_activity(
                        job_id,
                        "backup",
                        "info",
                        &format!("Auto-backup created: {}", backup_path.display()),
                    );
                }
                Err(e) => {
                    let _ = db.log_activity(
                        job_id,
                        "backup",
                        "warn",
                        &format!("Auto-backup failed: {}", e),
                    );
                }
            }
        }
    }

    let db: tauri::State<'_, Database> = app.state();
    let config = db.get_all_config()?;

    // Check if both research and enrich are in the stage list for parallel execution
    let has_research = stages.iter().any(|s| s == "research");
    let has_enrich = stages.iter().any(|s| s == "enrich");
    let run_parallel = has_research && has_enrich;

    if run_parallel {
        // Run research + enrich concurrently, then remaining stages sequentially
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "pipeline", "info", "Running research + enrich in parallel");
        }

        let _ = app.emit("pipeline:stage", json!({"stage": "research", "status": "running"}));
        let _ = app.emit("pipeline:stage", json!({"stage": "enrich", "status": "running"}));

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "research", "info", "Starting research stage");
            let _ = db.log_activity(job_id, "enrich", "info", "Starting enrich stage");
        }

        RESEARCH_ACTIVE.store(true, Ordering::SeqCst);
        let (research_result, enrich_result) = tokio::join!(
            async {
                let r = research::run(app, job_id, &config).await;
                RESEARCH_ACTIVE.store(false, Ordering::SeqCst);
                r
            },
            enrich::run(app, job_id, &config)
        );

        // Process research result
        match research_result {
            Ok(result) => {
                summary["research"] = result;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "research", "info", "research stage completed");
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "research", "error", &format!("research stage failed: {}", e));
                summary["research"] = json!({"error": e.to_string()});
            }
        }
        let _ = app.emit("pipeline:stage", json!({"stage": "research", "status": "completed"}));

        // Process enrich result
        match enrich_result {
            Ok(result) => {
                summary["enrich"] = result;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "enrich", "info", "enrich stage completed");
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "enrich", "error", &format!("enrich stage failed: {}", e));
                summary["enrich"] = json!({"error": e.to_string()});
            }
        }
        let _ = app.emit("pipeline:stage", json!({"stage": "enrich", "status": "completed"}));

        // Run remaining stages sequentially (skip research and enrich)
        for stage in stages {
            if stage == "research" || stage == "enrich" {
                continue;
            }
            if is_cancelled() {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, stage, "warn", "Pipeline cancelled by user");
                break;
            }

            let _ = app.emit("pipeline:stage", json!({"stage": stage, "status": "running"}));
            {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, stage, "info", &format!("Starting {} stage", stage));
            }

            let stage_result = match stage.as_str() {
                "push" => push::run(app, job_id, &config).await,
                "push_capabilities" => push::push_capabilities(app, job_id, &config).await,
                "outreach" => outreach::run(app, job_id, &config).await,
                "report" => report::run(app, job_id, &config).await,
                "deep_enrich_trial" => deep_enrich::run_trial(app, job_id, &config).await,
                "deep_enrich_all" => deep_enrich::run_all(app, job_id, &config).await,
                "aggregate_techniques" => technique_aggregate::run(app, job_id, &config).await,
                "push_techniques" => technique_aggregate::push_techniques(app, job_id, &config).await,
                "enrich_all" => run_enrich_all(app, job_id, &config).await,
                s if s.starts_with("deep_enrich:") => {
                    let sector = &s["deep_enrich:".len()..];
                    deep_enrich::run_sector(app, job_id, &config, sector).await
                }
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

            let _ = app.emit("pipeline:stage", json!({"stage": stage, "status": "completed"}));
        }
    } else {
        // Sequential execution (original behavior)
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
                "push_capabilities" => push::push_capabilities(app, job_id, &config).await,
                "outreach" => outreach::run(app, job_id, &config).await,
                "report" => report::run(app, job_id, &config).await,
                "deep_enrich_trial" => deep_enrich::run_trial(app, job_id, &config).await,
                "deep_enrich_all" => deep_enrich::run_all(app, job_id, &config).await,
                "aggregate_techniques" => technique_aggregate::run(app, job_id, &config).await,
                "push_techniques" => technique_aggregate::push_techniques(app, job_id, &config).await,
                "enrich_all" => run_enrich_all(app, job_id, &config).await,
                s if s.starts_with("deep_enrich:") => {
                    let sector = &s["deep_enrich:".len()..];
                    deep_enrich::run_sector(app, job_id, &config, sector).await
                }
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
    }

    Ok(summary)
}

/// Composite stage: deep_enrich_trial → aggregate_techniques → push_techniques
async fn run_enrich_all(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let db: tauri::State<'_, Database> = app.state();
    let _ = db.log_activity(job_id, "enrich_all", "info", "Starting enrich_all: deep_enrich → aggregate → push");

    // 1. Deep enrich (30-company trial sample)
    log::info!("[enrich_all] Phase 1/3: deep_enrich_trial");
    let _ = app.emit("pipeline:stage", json!({"stage": "deep_enrich_trial", "status": "running"}));
    let deep_result = deep_enrich::run_trial(app, job_id, config).await?;
    let _ = app.emit("pipeline:stage", json!({"stage": "deep_enrich_trial", "status": "completed"}));

    if is_cancelled() {
        return Ok(json!({"deep_enrich": deep_result, "cancelled": true}));
    }

    // 2. Aggregate techniques from all deep-enriched data
    log::info!("[enrich_all] Phase 2/3: aggregate_techniques");
    let _ = app.emit("pipeline:stage", json!({"stage": "aggregate_techniques", "status": "running"}));
    let agg_result = technique_aggregate::run(app, job_id, config).await?;
    let _ = app.emit("pipeline:stage", json!({"stage": "aggregate_techniques", "status": "completed"}));

    if is_cancelled() {
        return Ok(json!({"deep_enrich": deep_result, "aggregate": agg_result, "cancelled": true}));
    }

    // 3. Push to Supabase
    log::info!("[enrich_all] Phase 3/3: push_techniques");
    let _ = app.emit("pipeline:stage", json!({"stage": "push_techniques", "status": "running"}));
    let push_result = technique_aggregate::push_techniques(app, job_id, config).await?;
    let _ = app.emit("pipeline:stage", json!({"stage": "push_techniques", "status": "completed"}));

    let summary = json!({
        "deep_enrich": deep_result,
        "aggregate": agg_result,
        "push": push_result,
    });

    log::info!("[enrich_all] Complete");
    Ok(summary)
}

pub fn get_pipeline_nodes() -> Result<Value> {
    Ok(json!(get_all_node_states()))
}

/// Automated scheduler — checks trigger file every 5s, schedule every 60s.
pub async fn start_scheduler(app: tauri::AppHandle) {
    let mut last_run_date = String::new();
    let mut tick: u32 = 0;

    let trigger_path = std::path::PathBuf::from(
        std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string()),
    )
    .join(".nightshift-trigger");

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        tick += 1;

        // Don't interfere if pipeline is already running
        if RUNNING.load(Ordering::SeqCst) {
            continue;
        }

        // Check for CLI trigger file (~/.nightshift-trigger)
        if trigger_path.exists() {
            if let Ok(contents) = std::fs::read_to_string(&trigger_path) {
                let _ = std::fs::remove_file(&trigger_path);
                let stages: Vec<String> = contents
                    .trim()
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                if !stages.is_empty() {
                    log::info!("CLI trigger: starting pipeline with stages {:?}", stages);
                    let app_clone = app.clone();
                    match start_pipeline(app_clone, stages).await {
                        Ok(job_id) => log::info!("CLI-triggered pipeline started: {}", job_id),
                        Err(e) => log::error!("CLI-triggered pipeline failed: {}", e),
                    }
                }
            } else {
                let _ = std::fs::remove_file(&trigger_path);
            }
            continue;
        }

        // Only check schedule every ~60s (12 ticks × 5s)
        if tick % 12 != 0 {
            continue;
        }

        let db: tauri::State<'_, Database> = app.state();
        let config = match db.get_all_config() {
            Ok(c) => c,
            Err(_) => continue,
        };

        let schedule_time = config
            .get("schedule_time")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if schedule_time.is_empty() {
            continue;
        }

        let now = chrono::Local::now();
        let current_time = now.format("%H:%M").to_string();
        let current_date = now.format("%Y-%m-%d").to_string();

        // Match HH:MM and haven't run today
        if current_time == schedule_time && current_date != last_run_date {
            last_run_date = current_date;
            log::info!("Scheduler triggered at {}", current_time);

            let stages = vec![
                "research".to_string(),
                "enrich".to_string(),
                "push".to_string(),
                "outreach".to_string(),
                "report".to_string(),
            ];

            let app_clone = app.clone();
            match start_pipeline(app_clone, stages).await {
                Ok(job_id) => {
                    log::info!("Scheduled pipeline started: {}", job_id);
                }
                Err(e) => {
                    log::error!("Scheduled pipeline failed to start: {}", e);
                }
            }
        }
    }
}
