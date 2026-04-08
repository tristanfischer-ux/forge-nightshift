mod research;
mod enrich;
mod push;
mod outreach;
mod report;
mod deep_enrich;
mod technique_aggregate;
pub mod template_outreach;
mod companies_house;
pub mod outreach_learner;
mod verify;
mod synthesize;
mod director_intel;
mod activity;
mod embeddings;

use anyhow::Result;
use chrono::{Datelike, Timelike};
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
static SENDING: AtomicBool = AtomicBool::new(false);

#[derive(serde::Deserialize, serde::Serialize, Clone, Debug)]
struct Schedule {
    id: String,
    name: String,
    enabled: bool,
    #[serde(rename = "type")]
    schedule_type: String,
    interval_hours: Option<u32>,
    time: Option<String>,
    days: Option<Vec<u8>>,
    stages: Vec<String>,
    last_run_at: Option<String>,
}

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

/// Batch pipeline mode: processes companies in waves of N.
/// Each wave runs research (capped) → enrich → deep_enrich → verify → synthesize → director_intel sequentially.
/// Stops when research finds nothing new or pipeline is cancelled.
async fn batch_pipeline(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let batch_size: usize = config
        .get("pipeline_batch_size")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(100);

    let mut wave = 1u32;
    let mut total_processed: usize = 0;

    loop {
        if is_cancelled() {
            break;
        }

        log::info!("[Pipeline] Wave {} — batch size {}", wave, batch_size);
        emit_node(app, json!({
            "node_id": "batch",
            "status": "running",
            "wave": wave,
            "batch_size": batch_size,
            "total_processed": total_processed
        }));

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "batch", "info",
                &format!("Starting wave {} (batch size {})", wave, batch_size));
        }

        // Inject batch_size into config so research respects it
        let mut wave_config = config.clone();
        wave_config["pipeline_batch_size"] = json!(batch_size.to_string());

        // Step 1: Research (capped at batch_size)
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "research", "status": "running"}));
            let res = research::run(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "research", "status": "completed"}));
            if let Err(e) = &res {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "batch", "warn", &format!("Research error in wave {}: {}", wave, e));
            }
        }

        // Step 2: Enrich discovered companies
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "enrich", "status": "running"}));
            let _ = enrich::run(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "enrich", "status": "completed"}));
        }

        // Step 3: Deep enrich (all mode since enrich is done for this batch)
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "deep_enrich_all", "status": "running"}));
            let _ = deep_enrich::run_all(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "deep_enrich_all", "status": "completed"}));
        }

        // Step 4: Verify
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "verify", "status": "running"}));
            let _ = verify::run(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "verify", "status": "completed"}));
        }

        // Step 5: Synthesize
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "synthesize", "status": "running"}));
            let _ = synthesize::run(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "synthesize", "status": "completed"}));
        }

        // Step 6: Director intel
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "director_intel", "status": "running"}));
            let _ = director_intel::run(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "director_intel", "status": "completed"}));
        }

        // Step 7: Generate embeddings for semantic search
        if !is_cancelled() {
            let _ = app.emit("pipeline:stage", json!({"stage": "embeddings", "status": "running"}));
            let _ = embeddings::run(app, job_id, &wave_config).await;
            let _ = app.emit("pipeline:stage", json!({"stage": "embeddings", "status": "completed"}));
        }

        // Count remaining discovered (un-enriched) companies for this profile
        let discovered_remaining = {
            let db: tauri::State<'_, Database> = app.state();
            let profile_id = db.get_active_profile_id();
            db.get_companies_count(Some("discovered"), Some(&profile_id))
                .unwrap_or(0)
        };

        total_processed += batch_size;

        {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "batch", "info",
                &format!("Wave {} complete. Discovered remaining: {}, total processed: ~{}", wave, discovered_remaining, total_processed));
        }

        emit_node(app, json!({
            "node_id": "batch",
            "status": "running",
            "wave": wave,
            "batch_size": batch_size,
            "total_processed": total_processed,
            "discovered_remaining": discovered_remaining
        }));

        wave += 1;

        // If research found nothing new (no discovered companies left), stop
        if discovered_remaining == 0 || is_cancelled() {
            break;
        }
    }

    emit_node(app, json!({
        "node_id": "batch",
        "status": "completed",
        "waves_completed": wave - 1,
        "total_processed": total_processed
    }));

    Ok(json!({
        "waves_completed": wave - 1,
        "total_processed": total_processed,
        "cancelled": is_cancelled()
    }))
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

    // Ollama preflight check — skip outreach stages if Ollama is unreachable
    let ollama_available = {
        match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            crate::services::ollama::test_connection(),
        ).await {
            Ok(Ok(_)) => true,
            _ => {
                log::warn!("[pipeline] Ollama unreachable — skipping learn_outreach and template_outreach stages");
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(job_id, "pipeline", "warn", "Ollama unreachable, skipping outreach stages");
                false
            }
        }
    };

    let db: tauri::State<'_, Database> = app.state();
    let config = db.get_all_config()?;

    // Batch mode: if stages contains "batch", run batch_pipeline instead of normal flow
    if stages.iter().any(|s| s == "batch") {
        log::info!("[pipeline] Batch mode activated");
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "pipeline", "info", "Running in batch mode");
        return batch_pipeline(app, job_id, &config).await;
    }

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

    // Filter out Ollama-dependent outreach stages if Ollama is down
    let stages: Vec<String> = if !ollama_available {
        stages.into_iter().filter(|s| {
            s != "learn_outreach" && !s.starts_with("template_outreach:")
        }).collect()
    } else {
        stages
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
            // Case 2: research + enrich — no deep_enrich_drain
            RESEARCH_ACTIVE.store(true, Ordering::SeqCst);
            ENRICH_ACTIVE.store(true, Ordering::SeqCst);

            let (research_result, enrich_result) = tokio::join!(
                async {
                    let _guard = AtomicGuard(&RESEARCH_ACTIVE);
                    research::run(app, job_id, &config).await
                },
                async {
                    let _guard = AtomicGuard(&ENRICH_ACTIVE);
                    enrich::run(app, job_id, &config).await
                }
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
        "verify" => verify::run(app, job_id, config).await,
        "synthesize" => synthesize::run(app, job_id, config).await,
        "activity" => activity::run(app, job_id, config).await,
        "companies_house" => companies_house::run(app, job_id, config).await,
        "director_intel" => director_intel::run(app, job_id, config).await,
        "embeddings" => embeddings::run(app, job_id, config).await,
        "learn_outreach" => outreach_learner::run_learning_cycle(app, job_id, config).await,
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

            // Auto-approve drafts after template_outreach if autopilot is enabled
            if stage.starts_with("template_outreach:") {
                let auto_enabled = config
                    .get("auto_outreach_enabled")
                    .and_then(|v| v.as_str())
                    .unwrap_or("false") == "true";
                if auto_enabled {
                    match db.approve_all_drafts() {
                        Ok(count) => {
                            log::info!("[autopilot] Auto-approved {} drafts", count);
                            let _ = db.log_activity(job_id, "autopilot", "info",
                                &format!("Auto-approved {} drafts", count));
                        }
                        Err(e) => {
                            log::error!("[autopilot] Failed to auto-approve drafts: {}", e);
                        }
                    }
                }
            }
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

/// Send exactly one approved email (drip sender for anti-spam).
/// Returns true if an email was sent successfully.
async fn send_one_email(app: &tauri::AppHandle) -> bool {
    if SENDING.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return false; // Already sending
    }
    let _guard = AtomicGuard(&SENDING);

    let db: tauri::State<'_, Database> = app.state();
    let config = match db.get_all_config() {
        Ok(c) => c,
        Err(_) => return false,
    };

    let api_key = config.get("resend_api_key").and_then(|v| v.as_str()).unwrap_or("");
    if api_key.is_empty() {
        return false;
    }

    let daily_limit: i64 = config
        .get("daily_email_limit")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let sent_today = match db.get_emails_sent_today() {
        Ok(n) => n,
        Err(e) => {
            log::error!("[drip] Failed to get sent count: {}", e);
            return false;
        }
    };

    if sent_today >= daily_limit {
        log::info!("[drip] Daily limit reached ({}/{}), skipping", sent_today, daily_limit);
        return false;
    }

    let emails = match db.get_approved_emails_batch(1) {
        Ok(e) => e,
        Err(e) => {
            log::error!("[drip] Failed to get approved email: {}", e);
            return false;
        }
    };

    let email = match emails.first() {
        Some(e) => e,
        None => return false,
    };

    let id = email.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let to = email.get("to_email").and_then(|v| v.as_str()).unwrap_or("");
    let from = email.get("from_email").and_then(|v| v.as_str()).unwrap_or("");
    let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
    let body = email.get("body").and_then(|v| v.as_str()).unwrap_or("");

    if to.is_empty() || from.is_empty() {
        return false;
    }

    let _ = db.update_email_status(id, "sending");

    match crate::services::resend::send_email(api_key, from, to, subject, body).await {
        Ok(resend_id) => {
            let _ = db.update_email_sent(id, &resend_id);
            log::info!("[drip] Sent email to {} ({}/{})", to, sent_today + 1, daily_limit);
            let _ = db.log_activity("autopilot", "auto_send:drip", "info",
                &format!("Drip sent to {} (total today: {})", to, sent_today + 1));
            let _ = app.emit("auto_send:drip", json!({
                "sent_today": sent_today + 1,
                "daily_limit": daily_limit,
            }));
            true
        }
        Err(e) => {
            let err_msg = e.to_string();
            log::error!("[drip] Failed to send email {}: {}", id, err_msg);
            let _ = db.update_email_status(id, "failed");
            let _ = db.set_email_error(id, &err_msg);
            false
        }
    }
}

/// Auto-retry stale failed emails (>1 hour old). Run hourly.
async fn retry_stale_emails(app: &tauri::AppHandle) {
    let db: tauri::State<'_, Database> = app.state();
    match db.retry_stale_failed_emails() {
        Ok(count) if count > 0 => {
            log::info!("[autopilot] Retried {} stale failed emails", count);
            let _ = db.log_activity("autopilot", "auto_retry", "info",
                &format!("Reset {} stale failed emails for retry", count));
        }
        Ok(_) => {} // no stale emails
        Err(e) => log::warn!("[autopilot] Failed to retry stale emails: {}", e),
    }
}

/// Send a batch of approved emails (manual/bulk sender — used by UI "Send Now" and retry commands).
pub async fn send_batch(app: &tauri::AppHandle) {
    if SENDING.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
        return; // Already sending
    }
    let _guard = AtomicGuard(&SENDING);

    let db: tauri::State<'_, Database> = app.state();
    let config = match db.get_all_config() {
        Ok(c) => c,
        Err(_) => return,
    };

    let api_key = config.get("resend_api_key").and_then(|v| v.as_str()).unwrap_or("");
    if api_key.is_empty() {
        log::warn!("[autopilot] No resend_api_key configured, skipping batch send");
        return;
    }

    let batch_size: i64 = config
        .get("outreach_batch_size")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(5);
    let daily_limit: i64 = config
        .get("daily_email_limit")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse().ok())
        .unwrap_or(30);

    let sent_today = match db.get_emails_sent_today() {
        Ok(n) => n,
        Err(e) => {
            log::error!("[autopilot] Failed to get sent count: {}", e);
            return;
        }
    };

    if sent_today >= daily_limit {
        log::info!("[autopilot] Daily limit reached ({}/{}), skipping", sent_today, daily_limit);
        return;
    }

    let remaining = (daily_limit - sent_today).min(batch_size);
    let emails = match db.get_approved_emails_batch(remaining) {
        Ok(e) => e,
        Err(e) => {
            log::error!("[autopilot] Failed to get approved emails: {}", e);
            return;
        }
    };

    if emails.is_empty() {
        log::info!("[autopilot] No approved emails to send");
        return;
    }

    log::info!("[autopilot] Sending batch of {} emails ({}/{} today)", emails.len(), sent_today, daily_limit);
    let mut sent = 0i64;
    let mut failed = 0i64;

    for (i, email) in emails.iter().enumerate() {
        let id = email.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let to = email.get("to_email").and_then(|v| v.as_str()).unwrap_or("");
        let from = email.get("from_email").and_then(|v| v.as_str()).unwrap_or("");
        let subject = email.get("subject").and_then(|v| v.as_str()).unwrap_or("");
        let body = email.get("body").and_then(|v| v.as_str()).unwrap_or("");

        if to.is_empty() || from.is_empty() {
            failed += 1;
            continue;
        }

        let _ = db.update_email_status(id, "sending");

        match crate::services::resend::send_email(api_key, from, to, subject, body).await {
            Ok(resend_id) => {
                let _ = db.update_email_sent(id, &resend_id);
                sent += 1;
            }
            Err(e) => {
                let err_msg = e.to_string();
                log::error!("[autopilot] Failed to send email {}: {}", id, err_msg);
                let _ = db.update_email_status(id, "failed");
                let _ = db.set_email_error(id, &err_msg);
                failed += 1;
            }
        }

        // Rate limit (600ms for Resend free tier)
        if i + 1 < emails.len() {
            tokio::time::sleep(std::time::Duration::from_millis(600)).await;
        }
    }

    log::info!("[autopilot] Batch complete: {} sent, {} failed", sent, failed);
    let _ = db.log_activity("autopilot", "auto_send", "info",
        &format!("Batch sent: {} sent, {} failed (total today: {})", sent, failed, sent_today + sent));
    let _ = app.emit("auto_send:batch", json!({
        "sent": sent,
        "failed": failed,
        "sent_today": sent_today + sent,
        "daily_limit": daily_limit,
    }));
}

/// Automated scheduler — checks trigger file every 5s, drip-sends emails every ~20min during business hours.
pub async fn start_scheduler(app: tauri::AppHandle) {
    let mut last_retry_hour: i32 = -1;
    let mut last_drip_send: i64 = 0;
    let mut tick: u64 = 0;
    let mut last_run_failed = false;
    let mut retry_after_tick: u64 = 0;

    let trigger_path = std::path::PathBuf::from(
        std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string()),
    )
    .join(".nightshift-trigger");

    // Migrate legacy schedule_time to new schedules format
    {
        let db: tauri::State<'_, Database> = app.state();
        if let Ok(config) = db.get_all_config() {
            let has_schedules = config.get("schedules")
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty() && s != "[]")
                .unwrap_or(false);
            let schedule_time = config.get("schedule_time")
                .and_then(|v| v.as_str())
                .unwrap_or("");

            if !has_schedules && !schedule_time.is_empty() {
                let mut stages = vec![
                    "research".to_string(),
                    "enrich".to_string(),
                    "push".to_string(),
                    "report".to_string(),
                ];
                let auto_enabled = config.get("auto_outreach_enabled")
                    .and_then(|v| v.as_str())
                    .unwrap_or("false") == "true";
                let auto_template = config.get("auto_outreach_template_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                if auto_enabled && !auto_template.is_empty() {
                    stages.push("learn_outreach".to_string());
                    stages.push(format!("template_outreach:{}", auto_template));
                }

                let schedule = Schedule {
                    id: format!("{:016x}", chrono::Utc::now().timestamp_millis() as u64),
                    name: format!("Daily at {}", schedule_time),
                    enabled: true,
                    schedule_type: "daily".to_string(),
                    interval_hours: None,
                    time: Some(schedule_time.to_string()),
                    days: None,
                    stages,
                    last_run_at: None,
                };

                if let Ok(json) = serde_json::to_string(&vec![schedule]) {
                    let _ = db.set_config("schedules", &json);
                    log::info!("[scheduler] Migrated schedule_time={} to new schedules format", schedule_time);
                }
            }
        }
    }

    loop {
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        tick += 1;

        // Drip sender — every 60s tick, send one email if within business hours and 20min since last
        if tick % 12 == 0 && !SENDING.load(Ordering::SeqCst) {
            let now = chrono::Local::now();
            let current_hour = now.hour() as i32;
            let now_ts = now.timestamp();

            // Check if autopilot is enabled
            let db: tauri::State<'_, Database> = app.state();
            if let Ok(config) = db.get_all_config() {
                let auto_enabled = config
                    .get("auto_outreach_enabled")
                    .and_then(|v| v.as_str())
                    .unwrap_or("false") == "true";

                if auto_enabled {
                    // Business hours window (configurable, default 7am–7pm)
                    let window_start: i32 = config
                        .get("send_window_start")
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(7);
                    let window_end: i32 = config
                        .get("send_window_end")
                        .and_then(|v| v.as_str())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(19);

                    let in_window = current_hour >= window_start && current_hour < window_end;
                    let enough_gap = (now_ts - last_drip_send) >= 1200; // 20 minutes

                    if in_window && enough_gap {
                        if send_one_email(&app).await {
                            last_drip_send = now_ts;
                        }
                    }

                    // Hourly stale-email retry (independent of drip sends)
                    if current_hour != last_retry_hour {
                        last_retry_hour = current_hour;
                        retry_stale_emails(&app).await;
                    }
                }
            }
        }

        // 6-hourly email tracking refresh (tick % 4320 == 0 → 4320 × 5s = 6h)
        if tick % 4320 == 0 && tick > 0 {
            log::info!("[scheduler] Running 6-hourly email tracking refresh");
            let db: tauri::State<'_, Database> = app.state();
            if let Ok(config) = db.get_all_config() {
                let api_key = config.get("resend_api_key").and_then(|v| v.as_str()).unwrap_or("");
                if !api_key.is_empty() {
                    let emails = db.get_sent_emails_for_tracking().unwrap_or_default();
                    let mut updated = 0i64;
                    for email in &emails {
                        let id = email.get("id").and_then(|v| v.as_str()).unwrap_or("");
                        let resend_id = email.get("resend_id").and_then(|v| v.as_str()).unwrap_or("");
                        if resend_id.is_empty() { continue; }

                        match crate::services::resend::get_email_status(api_key, resend_id).await {
                            Ok(status_data) => {
                                let last_event = status_data
                                    .get("last_event")
                                    .and_then(|v| v.as_str())
                                    .unwrap_or("");
                                let bounced = last_event == "bounced";
                                let opened_at = if last_event == "opened" {
                                    status_data.get("last_event_at").and_then(|v| v.as_str())
                                } else {
                                    None
                                };
                                if bounced || opened_at.is_some() {
                                    let _ = db.update_email_tracking(id, opened_at, bounced);
                                    updated += 1;
                                }
                            }
                            Err(_) => {} // skip failed lookups
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(600)).await;
                    }
                    if updated > 0 {
                        log::info!("[scheduler] Tracking refresh: {} emails updated", updated);
                    }
                    let _ = db.update_experiment_stats();
                }
            }
        }

        // Don't interfere if pipeline is already running
        if RUNNING.load(Ordering::SeqCst) {
            continue;
        }

        // Pipeline retry after 4 hours if last scheduled run failed
        if last_run_failed && tick >= retry_after_tick && !RUNNING.load(Ordering::SeqCst) {
            log::info!("[scheduler] Retrying pipeline after previous failure");
            last_run_failed = false;

            let db: tauri::State<'_, Database> = app.state();
            if let Ok(config) = db.get_all_config() {
                // Use stages from the first enabled schedule, or default
                let schedules_json = config.get("schedules")
                    .and_then(|v| v.as_str())
                    .unwrap_or("[]");
                let schedules: Vec<Schedule> = serde_json::from_str(schedules_json).unwrap_or_default();
                let stages = schedules.iter()
                    .find(|s| s.enabled)
                    .map(|s| s.stages.clone())
                    .unwrap_or_else(|| {
                        let mut s = vec![
                            "research".to_string(),
                            "enrich".to_string(),
                            "push".to_string(),
                            "report".to_string(),
                        ];
                        let auto_enabled = config.get("auto_outreach_enabled")
                            .and_then(|v| v.as_str())
                            .unwrap_or("false") == "true";
                        let auto_template = config.get("auto_outreach_template_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or("");
                        if auto_enabled && !auto_template.is_empty() {
                            s.push("learn_outreach".to_string());
                            s.push(format!("template_outreach:{}", auto_template));
                        }
                        s
                    });
                let app_clone = app.clone();
                match start_pipeline(app_clone, stages).await {
                    Ok(job_id) => log::info!("[scheduler] Retry pipeline started: {}", job_id),
                    Err(e) => log::error!("[scheduler] Retry pipeline also failed: {}", e),
                }
            }
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

        let schedules_json = config
            .get("schedules")
            .and_then(|v| v.as_str())
            .unwrap_or("[]");

        let mut schedules: Vec<Schedule> = match serde_json::from_str(schedules_json) {
            Ok(s) => s,
            Err(_) => continue,
        };

        let now = chrono::Local::now();
        let current_time = now.format("%H:%M").to_string();
        let mut triggered_idx: Option<usize> = None;

        for (idx, schedule) in schedules.iter().enumerate() {
            if !schedule.enabled {
                continue;
            }

            let should_trigger = match schedule.schedule_type.as_str() {
                "daily" => {
                    if let Some(ref time) = schedule.time {
                        let today = now.format("%Y-%m-%d").to_string();
                        let last_run_date = schedule.last_run_at.as_ref()
                            .and_then(|ts| ts.get(..10).map(|s| s.to_string()))
                            .unwrap_or_default();
                        &current_time == time && last_run_date != today
                    } else {
                        false
                    }
                }
                "weekly" => {
                    if let (Some(ref time), Some(ref days)) = (&schedule.time, &schedule.days) {
                        let today = now.format("%Y-%m-%d").to_string();
                        let last_run_date = schedule.last_run_at.as_ref()
                            .and_then(|ts| ts.get(..10).map(|s| s.to_string()))
                            .unwrap_or_default();
                        // chrono: Mon=0..Sun=6, our format: Sun=0..Sat=6
                        let current_dow = match now.weekday() {
                            chrono::Weekday::Sun => 0u8,
                            chrono::Weekday::Mon => 1,
                            chrono::Weekday::Tue => 2,
                            chrono::Weekday::Wed => 3,
                            chrono::Weekday::Thu => 4,
                            chrono::Weekday::Fri => 5,
                            chrono::Weekday::Sat => 6,
                        };
                        &current_time == time && days.contains(&current_dow) && last_run_date != today
                    } else {
                        false
                    }
                }
                "interval" => {
                    if let Some(hours) = schedule.interval_hours {
                        let hours = hours.max(1);
                        let elapsed = schedule.last_run_at.as_ref()
                            .and_then(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                            .map(|last| (now - last.with_timezone(&chrono::Local)).num_seconds())
                            .unwrap_or(i64::MAX); // Never run before = trigger immediately
                        elapsed >= (hours as i64) * 3600
                    } else {
                        false
                    }
                }
                _ => false,
            };

            if should_trigger {
                triggered_idx = Some(idx);
                break;
            }
        }

        if let Some(idx) = triggered_idx {
            let schedule = &schedules[idx];
            log::info!("[scheduler] Schedule '{}' triggered (type={}, stages={:?})",
                schedule.name, schedule.schedule_type, schedule.stages);

            let stages = schedule.stages.clone();
            let app_clone = app.clone();
            match start_pipeline(app_clone, stages).await {
                Ok(job_id) => {
                    log::info!("[scheduler] Schedule '{}' pipeline started: {}", schedule.name, job_id);
                    // Update last_run_at
                    schedules[idx].last_run_at = Some(chrono::Utc::now().to_rfc3339());
                    if let Ok(json) = serde_json::to_string(&schedules) {
                        let _ = db.set_config("schedules", &json);
                    }
                    last_run_failed = false;
                }
                Err(e) => {
                    log::error!("[scheduler] Schedule '{}' pipeline failed: {}", schedule.name, e);
                    last_run_failed = true;
                    retry_after_tick = tick + 2880;
                }
            }
        }
    }
}
