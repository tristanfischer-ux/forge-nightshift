mod research;
mod enrich;
mod push;
mod outreach;
mod report;
mod deep_enrich;
mod technique_aggregate;
mod template_outreach;
mod companies_house;

use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use tauri::{Emitter, Manager};

use crate::db::Database;

/// Drop guard that resets an AtomicBool to false when dropped (even on panic).
struct AtomicGuard(&'static AtomicBool);
impl Drop for AtomicGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

static RUNNING: AtomicBool = AtomicBool::new(false);
static CANCEL: AtomicBool = AtomicBool::new(false);
static RESEARCH_ACTIVE: AtomicBool = AtomicBool::new(false);
static ENRICH_ACTIVE: AtomicBool = AtomicBool::new(false);

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

pub fn is_enrich_active() -> bool {
    ENRICH_ACTIVE.load(Ordering::SeqCst)
}

pub async fn start_pipeline(app: tauri::AppHandle, stages: Vec<String>) -> Result<String> {
    if RUNNING.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        anyhow::bail!("Pipeline is already running");
    }
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
        let _running_guard = AtomicGuard(&RUNNING); // Ensures RUNNING=false even on panic
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
        // _running_guard drops here, setting RUNNING=false
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

    // Auto-inject deep_enrich_drain if enrich is requested but no deep enrich variant is present.
    // This ensures deep enrichment always runs concurrently with enrich for new companies.
    let stages: Vec<String> = {
        let mut s = stages.to_vec();
        let has_enrich = s.iter().any(|st| st == "enrich");
        let has_any_deep = s.iter().any(|st| st == "deep_enrich_drain" || st == "deep_enrich_all" || st.starts_with("deep_enrich:"));
        if has_enrich && !has_any_deep {
            if let Some(pos) = s.iter().position(|st| st == "enrich") {
                s.insert(pos + 1, "deep_enrich_drain".to_string());
            }
            log::info!("[pipeline] Auto-injected deep_enrich_drain alongside enrich");
        }
        s
    };

    // Determine which stages can run concurrently
    let has_research = stages.iter().any(|s| s == "research");
    let has_enrich = stages.iter().any(|s| s == "enrich");
    let has_deep_enrich_drain = stages.iter().any(|s| s == "deep_enrich_drain");

    // Stages that are handled in the parallel block (skip in sequential remainder)
    let parallel_stages: Vec<&str> = {
        let mut ps = Vec::new();
        if has_research && has_enrich { ps.push("research"); ps.push("enrich"); }
        if has_deep_enrich_drain && has_enrich { ps.push("deep_enrich_drain"); }
        ps
    };
    let run_parallel = !parallel_stages.is_empty();

    if run_parallel {
        // Log what's running in parallel
        let parallel_label = parallel_stages.join(" + ");
        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "pipeline", "info", &format!("Running {} in parallel", parallel_label));
        }

        for &s in &parallel_stages {
            let _ = app.emit("pipeline:stage", json!({"stage": s, "status": "running"}));
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, s, "info", &format!("Starting {} stage", s));
        }

        // 4 cases based on which stages are present
        if has_research && has_enrich && has_deep_enrich_drain {
            // Case 1: research + enrich + deep_enrich_drain — all 3 concurrent
            RESEARCH_ACTIVE.store(true, Ordering::SeqCst);
            ENRICH_ACTIVE.store(true, Ordering::SeqCst);

            let (research_result, enrich_result, deep_enrich_result) = tokio::join!(
                async {
                    let _guard = AtomicGuard(&RESEARCH_ACTIVE);
                    research::run(app, job_id, &config).await
                },
                async {
                    let _guard = AtomicGuard(&ENRICH_ACTIVE);
                    enrich::run(app, job_id, &config).await
                },
                deep_enrich::run_drain(app, job_id, &config)
            );

            process_parallel_result(&mut summary, app, job_id, "research", research_result);
            process_parallel_result(&mut summary, app, job_id, "enrich", enrich_result);
            process_parallel_result(&mut summary, app, job_id, "deep_enrich_drain", deep_enrich_result);

        } else if has_research && has_enrich {
            // Case 2: research + enrich — existing behavior
            RESEARCH_ACTIVE.store(true, Ordering::SeqCst);

            let (research_result, enrich_result) = tokio::join!(
                async {
                    let _guard = AtomicGuard(&RESEARCH_ACTIVE);
                    research::run(app, job_id, &config).await
                },
                enrich::run(app, job_id, &config)
            );

            process_parallel_result(&mut summary, app, job_id, "research", research_result);
            process_parallel_result(&mut summary, app, job_id, "enrich", enrich_result);

        } else if has_enrich && has_deep_enrich_drain {
            // Case 3: enrich + deep_enrich_drain — no research
            ENRICH_ACTIVE.store(true, Ordering::SeqCst);

            let (enrich_result, deep_enrich_result) = tokio::join!(
                async {
                    let _guard = AtomicGuard(&ENRICH_ACTIVE);
                    enrich::run(app, job_id, &config).await
                },
                deep_enrich::run_drain(app, job_id, &config)
            );

            process_parallel_result(&mut summary, app, job_id, "enrich", enrich_result);
            process_parallel_result(&mut summary, app, job_id, "deep_enrich_drain", deep_enrich_result);
        }

        // Run remaining stages sequentially (skip those handled in parallel)
        for stage in &stages {
            if parallel_stages.contains(&stage.as_str()) {
                continue;
            }
            if is_cancelled() {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, stage, "warn", "Pipeline cancelled by user");
                break;
            }

            let result = run_single_stage(app, job_id, &config, stage).await;
            match result {
                Ok(r) => summary[stage.as_str()] = r,
                Err(e) => summary[stage.as_str()] = json!({"error": e.to_string()}),
            }
        }
    } else {
        // Sequential execution (original behavior)
        for stage in &stages {
            if is_cancelled() {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, stage, "warn", "Pipeline cancelled by user");
                break;
            }

            let result = run_single_stage(app, job_id, &config, stage).await;
            match result {
                Ok(r) => summary[stage.as_str()] = r,
                Err(e) => summary[stage.as_str()] = json!({"error": e.to_string()}),
            }
        }
    }

    Ok(summary)
}

/// Process the result of a parallel stage and update summary.
fn process_parallel_result(
    summary: &mut Value,
    app: &tauri::AppHandle,
    job_id: &str,
    stage: &str,
    result: Result<Value>,
) {
    match result {
        Ok(r) => {
            summary[stage] = r;
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

/// Run a single stage with logging and event emission.
async fn run_single_stage(
    app: &tauri::AppHandle,
    job_id: &str,
    config: &Value,
    stage: &str,
) -> Result<Value> {
    let _ = app.emit("pipeline:stage", json!({"stage": stage, "status": "running"}));
    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, stage, "info", &format!("Starting {} stage", stage));
    }

    let stage_result = match stage {
        "research" => research::run(app, job_id, config).await,
        "enrich" => enrich::run(app, job_id, config).await,
        "push" => push::run(app, job_id, config).await,
        "push_capabilities" => push::push_capabilities(app, job_id, config).await,
        "outreach" => outreach::run(app, job_id, config).await,
        "report" => report::run(app, job_id, config).await,
        "deep_enrich_trial" => deep_enrich::run_trial(app, job_id, config).await,
        "deep_enrich_all" => deep_enrich::run_all(app, job_id, config).await,
        "deep_enrich_drain" => deep_enrich::run_drain(app, job_id, config).await,
        "aggregate_techniques" => technique_aggregate::run(app, job_id, config).await,
        "push_techniques" => technique_aggregate::push_techniques(app, job_id, config).await,
        "enrich_all" => run_enrich_all(app, job_id, config).await,
        s if s.starts_with("deep_enrich:") => {
            let sector = &s["deep_enrich:".len()..];
            deep_enrich::run_sector(app, job_id, config, sector).await
        }
        "companies_house" => companies_house::run(app, job_id, config).await,
        s if s.starts_with("template_outreach:") => {
            let template_id = &s["template_outreach:".len()..];
            template_outreach::run(app, job_id, config, template_id).await
        }
        unknown => {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, unknown, "error", "Unknown stage");
            Err(anyhow::anyhow!("Unknown stage: {}", unknown))
        }
    };

    match &stage_result {
        Ok(_) => {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, stage, "info", &format!("{} stage completed", stage));
        }
        Err(e) => {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, stage, "error", &format!("{} stage failed: {}", stage, e));
        }
    }

    let _ = app.emit("pipeline:stage", json!({"stage": stage, "status": "completed"}));
    stage_result
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
    let mut tick: u64 = 0;

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

            // DECISION: old outreach stage removed — it auto-sends without review.
            // Use template_outreach (draft-only) via the Outreach UI instead.
            let stages = vec![
                "research".to_string(),
                "enrich".to_string(),
                "push".to_string(),
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
