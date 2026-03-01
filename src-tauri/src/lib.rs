mod db;
mod services;
mod pipeline;

use db::Database;
use tauri::{Emitter, Manager};

#[tauri::command]
fn get_stats(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_stats().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_companies(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_companies(status.as_deref(), limit.unwrap_or(50), offset.unwrap_or(0))
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn get_company(db: tauri::State<'_, Database>, id: String) -> Result<serde_json::Value, String> {
    db.get_company(&id).map_err(|e| e.to_string())
}

#[tauri::command]
fn update_company_status(
    db: tauri::State<'_, Database>,
    id: String,
    status: String,
) -> Result<(), String> {
    db.update_company_status(&id, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_emails(
    db: tauri::State<'_, Database>,
    status: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_emails(status.as_deref(), limit.unwrap_or(50))
        .map_err(|e| e.to_string())
}

#[tauri::command]
fn update_email_status(
    db: tauri::State<'_, Database>,
    id: String,
    status: String,
) -> Result<(), String> {
    db.update_email_status(&id, &status).map_err(|e| e.to_string())
}

#[tauri::command]
fn get_config(db: tauri::State<'_, Database>) -> Result<serde_json::Value, String> {
    db.get_all_config().map_err(|e| e.to_string())
}

#[tauri::command]
fn set_config(db: tauri::State<'_, Database>, key: String, value: String) -> Result<(), String> {
    db.set_config(&key, &value).map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_ollama_connection() -> Result<serde_json::Value, String> {
    services::ollama::test_connection()
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_brave_connection(api_key: String) -> Result<bool, String> {
    services::brave::test_connection(&api_key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_supabase_connection(url: String, key: String) -> Result<bool, String> {
    services::supabase::test_connection(&url, &key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn test_resend_connection(api_key: String) -> Result<bool, String> {
    services::resend::test_connection(&api_key)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn start_pipeline(app: tauri::AppHandle, stages: Vec<String>) -> Result<String, String> {
    pipeline::start_pipeline(app, stages)
        .await
        .map_err(|e| e.to_string())
}

#[tauri::command]
async fn stop_pipeline(app: tauri::AppHandle) -> Result<(), String> {
    pipeline::stop_pipeline(app).await.map_err(|e| e.to_string())
}

#[tauri::command]
fn get_pipeline_status() -> Result<serde_json::Value, String> {
    pipeline::get_status().map_err(|e| e.to_string())
}

#[tauri::command]
fn reset_error_companies(db: tauri::State<'_, Database>) -> Result<i64, String> {
    db.reset_error_companies().map_err(|e| e.to_string())
}

#[tauri::command]
fn get_run_log(
    db: tauri::State<'_, Database>,
    job_id: Option<String>,
    limit: Option<i64>,
) -> Result<Vec<serde_json::Value>, String> {
    db.get_run_log(job_id.as_deref(), limit.unwrap_or(100))
        .map_err(|e| e.to_string())
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    env_logger::init();

    tauri::Builder::default()
        .plugin(tauri_plugin_notification::init())
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .setup(|app| {
            // Initialize database in app data directory
            let app_dir = app
                .path()
                .app_data_dir()
                .expect("Failed to get app data directory");

            let database = Database::new(&app_dir).expect("Failed to initialize database");
            app.manage(database);

            // Auto-check Ollama connection on startup
            let handle = app.handle().clone();
            tauri::async_runtime::spawn(async move {
                let result = services::ollama::test_connection().await;
                match result {
                    Ok(info) => {
                        log::info!("Ollama connected: {:?}", info);
                        let _ = handle.emit("ollama:status", serde_json::json!({
                            "connected": true,
                            "models": info.get("models"),
                        }));
                    }
                    Err(e) => {
                        log::warn!("Ollama not reachable: {}", e);
                        let _ = handle.emit("ollama:status", serde_json::json!({
                            "connected": false,
                            "error": e.to_string(),
                        }));
                    }
                }
            });

            Ok(())
        })
        .invoke_handler(tauri::generate_handler![
            get_stats,
            get_companies,
            get_company,
            update_company_status,
            get_emails,
            update_email_status,
            get_config,
            set_config,
            test_ollama_connection,
            test_brave_connection,
            test_supabase_connection,
            test_resend_connection,
            start_pipeline,
            stop_pipeline,
            get_pipeline_status,
            get_run_log,
            reset_error_companies,
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
