//! Self-audit system for the Nightshift pipeline.
//!
//! Runs after every wave to detect bad states and auto-fix safe issues.
//! Design constraints:
//! - FAST: no LLM calls, just DB queries (<1 second)
//! - SAFE: only auto-fix obvious things (orphaned enriching, no-website errors, permanent errors)
//! - NEVER delete real companies
//! - NEVER switch profiles or LLM backends
//! - Log findings prominently so user can see them

use serde_json::{json, Value};
use tauri::Manager;

use crate::db::Database;

/// Run a self-audit on the pipeline state for the given profile.
/// Returns a JSON object with findings (warnings) and fixes (auto-applied).
pub fn run_audit(app: &tauri::AppHandle, profile_id: &str) -> Value {
    let db: tauri::State<'_, Database> = app.state();
    let mut findings: Vec<String> = Vec::new();
    let mut fixes: Vec<String> = Vec::new();

    // CHECK 1: Orphaned "enriching" companies (stuck > 30 min)
    // Safe to fix: reset to "discovered" so they get re-processed
    match db.reset_orphaned_enriching(profile_id) {
        Ok(count) if count > 0 => {
            fixes.push(format!("Reset {} orphaned enriching companies (stuck >30min)", count));
        }
        Err(e) => {
            log::warn!("[Audit] Failed to check orphaned enriching: {}", e);
        }
        _ => {}
    }

    // CHECK 2: Error rate in recent companies
    let recent_total = db.count_recent_total(profile_id, 200).unwrap_or(0);
    let recent_errors = db.count_recent_errors(profile_id, 200).unwrap_or(0);
    let error_rate = if recent_total > 0 {
        recent_errors as f64 / recent_total as f64
    } else {
        0.0
    };
    if error_rate > 0.4 {
        findings.push(format!(
            "HIGH ERROR RATE: {:.0}% ({}/{}) — check LLM/scraping health",
            error_rate * 100.0,
            recent_errors,
            recent_total
        ));
    }

    // CHECK 3: No-website errors — archive permanently
    // Companies with no website can never be enriched, so remove them from the pipeline
    match db.archive_no_website_errors(profile_id) {
        Ok(count) if count > 0 => {
            fixes.push(format!("Archived {} no-website errors", count));
        }
        Err(e) => {
            log::warn!("[Audit] Failed to archive no-website errors: {}", e);
        }
        _ => {}
    }

    // CHECK 4: Average enrichment quality dropping
    let avg_quality = db.avg_recent_quality(profile_id, 100).unwrap_or(0.0);
    if avg_quality > 0.0 && avg_quality < 25.0 {
        findings.push(format!(
            "LOW QUALITY: avg {:.0} — enrichment may be producing junk",
            avg_quality
        ));
    }

    // CHECK 5: Duplicate domains
    let duplicate_count = db.count_duplicate_domains(profile_id).unwrap_or(0);
    if duplicate_count > 0 {
        findings.push(format!("{} duplicate domains detected", duplicate_count));
    }

    // CHECK 6: Companies with error_count >= 3 still in error state (should be archived)
    match db.archive_permanent_errors(profile_id) {
        Ok(count) if count > 0 => {
            fixes.push(format!("Archived {} permanent errors (3+ failures)", count));
        }
        Err(e) => { log::warn!("[Audit] Failed to archive permanent errors: {}", e); }
        _ => {}
    }

    // CHECK 7: Companies in liquidation/dissolved/administration — remove from active pipeline
    match db.archive_dead_companies(profile_id) {
        Ok(count) if count > 0 => {
            fixes.push(format!("Removed {} dead companies (liquidation/dissolved/administration)", count));
        }
        Err(e) => { log::warn!("[Audit] Failed to archive dead companies: {}", e); }
        _ => {}
    }

    // Log summary
    if !findings.is_empty() {
        for f in &findings {
            log::warn!("[Audit] FINDING: {}", f);
        }
    }
    if !fixes.is_empty() {
        for f in &fixes {
            log::info!("[Audit] FIX: {}", f);
        }
    }

    json!({
        "findings": findings,
        "fixes": fixes,
        "error_rate": format!("{:.1}", error_rate * 100.0),
        "avg_quality": format!("{:.0}", avg_quality),
        "duplicates": duplicate_count
    })
}
