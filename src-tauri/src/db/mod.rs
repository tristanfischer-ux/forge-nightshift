use anyhow::Result;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;

/// Convert a rusqlite Value to a serde_json Value, handling all SQLite types.
fn sqlite_to_json(val: rusqlite::types::Value) -> Value {
    match val {
        rusqlite::types::Value::Null => Value::Null,
        rusqlite::types::Value::Integer(i) => json!(i),
        rusqlite::types::Value::Real(f) => json!(f),
        rusqlite::types::Value::Text(s) => json!(s),
        rusqlite::types::Value::Blob(b) => json!(base64_encode(&b)),
    }
}

fn base64_encode(data: &[u8]) -> String {
    use std::fmt::Write;
    let mut s = String::with_capacity(data.len() * 4 / 3 + 4);
    for byte in data {
        write!(s, "{:02x}", byte).unwrap();
    }
    s
}

pub struct Database {
    conn: Mutex<Connection>,
}

impl Database {
    pub fn new(app_dir: &std::path::Path) -> Result<Self> {
        std::fs::create_dir_all(app_dir)?;
        let db_path = app_dir.join("nightshift.db");
        let conn = Connection::open(db_path)?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")?;
        let db = Self {
            conn: Mutex::new(conn),
        };
        db.run_migrations()?;
        Ok(db)
    }

    fn run_migrations(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(include_str!("migrations/001_initial.sql"))?;
        conn.execute_batch(include_str!("migrations/002_category_coverage.sql"))?;
        // 003+: additive ALTER TABLE — ignore "duplicate column" errors on re-run
        for migration in &[
            include_str!("migrations/003_translation_fields.sql"),
            include_str!("migrations/004_name_normalized.sql"),
            include_str!("migrations/005_enrichment_v2.sql"),
            include_str!("migrations/006_qwen35_models.sql"),
            include_str!("migrations/007_qwen35_research.sql"),
            include_str!("migrations/008_research_fixes.sql"),
            include_str!("migrations/009_auto_approve_threshold.sql"),
            include_str!("migrations/010_enrich_concurrency.sql"),
            include_str!("migrations/011_geocoding.sql"),
            include_str!("migrations/012_normalize_country.sql"),
            include_str!("migrations/013_deep_enrichment.sql"),
            include_str!("migrations/014_technique_knowledge.sql"),
            include_str!("migrations/015_indexes.sql"),
            include_str!("migrations/016_email_templates.sql"),
            include_str!("migrations/017_companies_house_verified.sql"),
            include_str!("migrations/018_email_last_error.sql"),
            include_str!("migrations/019_campaigns.sql"),
            include_str!("migrations/020_self_learning.sql"),
            include_str!("migrations/021_verification.sql"),
            include_str!("migrations/022_synthesis.sql"),
            include_str!("migrations/023_activity_feed.sql"),
            include_str!("migrations/024_nightshift_intel.sql"),
        ] {
            for stmt in migration.split(';') {
                let stmt = stmt.trim();
                if !stmt.is_empty() {
                    let _ = conn.execute_batch(stmt);
                }
            }
        }
        Ok(())
    }

    pub fn get_stats(&self) -> Result<Value> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare("SELECT status, COUNT(*) as count FROM companies GROUP BY status")?;
        let companies: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "status": row.get::<_, String>(0)?,
                    "count": row.get::<_, i64>(1)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut stmt = conn.prepare("SELECT status, COUNT(*) as count FROM emails GROUP BY status")?;
        let emails: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "status": row.get::<_, String>(0)?,
                    "count": row.get::<_, i64>(1)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut stmt = conn.prepare("SELECT id, stages, status, summary, started_at, completed_at FROM jobs ORDER BY created_at DESC LIMIT 1")?;
        let latest_job: Option<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "stages": row.get::<_, String>(1)?,
                    "status": row.get::<_, String>(2)?,
                    "summary": row.get::<_, Option<String>>(3)?,
                    "started_at": row.get::<_, Option<String>>(4)?,
                    "completed_at": row.get::<_, Option<String>>(5)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .next();

        Ok(json!({
            "companies": companies,
            "emails": emails,
            "latest_job": latest_job,
        }))
    }

    pub fn get_extended_stats(&self) -> Result<Value> {
        let conn = self.conn.lock().unwrap();

        let verified: i64 = conn.query_row(
            "SELECT COUNT(*) FROM companies WHERE verified_v2_at IS NOT NULL",
            [],
            |row| row.get(0),
        ).unwrap_or(0);

        let synthesized: i64 = conn.query_row(
            "SELECT COUNT(*) FROM companies WHERE synthesis_public_json IS NOT NULL AND synthesis_public_json != ''",
            [],
            |row| row.get(0),
        ).unwrap_or(0);

        let intel_records: i64 = conn.query_row(
            "SELECT COUNT(*) FROM nightshift_intel",
            [],
            |row| row.get(0),
        ).unwrap_or(0);

        let activities: i64 = conn.query_row(
            "SELECT COUNT(*) FROM activity_feed",
            [],
            |row| row.get(0),
        ).unwrap_or(0);

        Ok(json!({
            "verified": verified,
            "synthesized": synthesized,
            "intel_records": intel_records,
            "activities": activities,
        }))
    }

    pub fn get_companies(&self, status: Option<&str>, limit: i64, offset: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let (query, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(s) = status {
            (
                "SELECT * FROM companies WHERE status = ?1 ORDER BY created_at DESC LIMIT ?2 OFFSET ?3",
                vec![Box::new(s.to_string()), Box::new(limit), Box::new(offset)],
            )
        } else {
            (
                "SELECT * FROM companies ORDER BY created_at DESC LIMIT ?1 OFFSET ?2",
                vec![Box::new(limit), Box::new(offset)],
            )
        };

        let mut stmt = conn.prepare(query)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let rows: Vec<Value> = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Batch-mark all discovered companies without a website as errors in one UPDATE.
    pub fn batch_mark_no_website_errors(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = 'error', last_error = 'No website — cannot enrich', updated_at = datetime('now') WHERE status = 'discovered' AND (website_url IS NULL OR website_url = '')",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    /// Reset stuck 'enriching' companies back to 'discovered' (e.g. from a crashed previous run).
    pub fn reset_stuck_enriching(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = 'discovered', updated_at = datetime('now') WHERE status = 'enriching'",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    /// Get discovered companies that have a website (enrichable).
    pub fn get_enrichable_companies(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM companies WHERE status = 'discovered' AND website_url IS NOT NULL AND website_url != '' ORDER BY created_at ASC LIMIT ?1"
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();

        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    pub fn get_company(&self, id: &str) -> Result<Value> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT * FROM companies WHERE id = ?1")?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();

        let row = stmt.query_row([id], |row| {
            let mut obj = serde_json::Map::new();
            for (i, col) in columns.iter().enumerate() {
                let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                obj.insert(col.clone(), sqlite_to_json(val));
            }
            Ok(Value::Object(obj))
        })?;

        Ok(row)
    }

    pub fn delete_company(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM companies WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn update_company_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = ?1, updated_at = datetime('now') WHERE id = ?2",
            [status, id],
        )?;
        Ok(())
    }

    pub fn update_company_enrichment(&self, id: &str, enriched: &Value) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             description = ?1, category = ?2, subcategory = ?3, \
             specialties = ?4, certifications = ?5, company_size = ?6, \
             relevance_score = ?7, enrichment_quality = ?8, \
             contact_name = ?9, contact_email = ?10, contact_title = ?11, \
             attributes_json = ?12, \
             description_original = ?13, snippet_english = ?14, \
             address = ?15, financial_health = ?16, \
             last_error = NULL, \
             status = 'enriched', updated_at = datetime('now') \
             WHERE id = ?17",
            rusqlite::params![
                enriched.get("description").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("category").and_then(|v| v.as_str()).unwrap_or("Services"),
                enriched.get("subcategory").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("specialties").unwrap_or(&json!([])).to_string(),
                enriched.get("certifications").unwrap_or(&json!([])).to_string(),
                enriched.get("company_size").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("relevance_score").and_then(|v| v.as_i64()).unwrap_or(0),
                enriched.get("enrichment_quality").and_then(|v| v.as_i64()).unwrap_or(0),
                enriched.get("contact_name").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("contact_email").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("contact_title").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("attributes_json").unwrap_or(&json!({})).to_string(),
                enriched.get("description_original").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("snippet_english").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("address").and_then(|v| v.as_str()).unwrap_or(""),
                enriched.get("financial_health").and_then(|v| v.as_str()).unwrap_or(""),
                id,
            ],
        )?;
        Ok(())
    }

    pub fn set_company_error(&self, id: &str, error: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = 'error', last_error = ?1, updated_at = datetime('now') WHERE id = ?2",
            [error, id],
        )?;
        Ok(())
    }

    /// Get GB companies needing Companies House verification (never checked or stale >90 days).
    /// Excludes 'discovered' and 'error' statuses.
    pub fn get_gb_companies_needing_ch_check(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, country, attributes_json, financial_health \
             FROM companies \
             WHERE (country = 'GB' OR country = 'UK') \
               AND status NOT IN ('discovered', 'error') \
               AND (ch_verified_at IS NULL OR ch_verified_at < datetime('now', '-90 days')) \
             ORDER BY ch_verified_at ASC NULLS FIRST"
        )?;

        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "country": row.get::<_, String>(2)?,
                    "attributes_json": row.get::<_, Option<String>>(3)?,
                    "financial_health": row.get::<_, Option<String>>(4)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Full CH verification update: merges attributes, sets ch_verified_at + ch_company_number + financial_health.
    pub fn update_ch_verification(
        &self,
        id: &str,
        company_number: &str,
        attributes_json: &str,
        financial_health: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             attributes_json = ?1, \
             financial_health = ?2, \
             ch_verified_at = datetime('now'), \
             ch_company_number = ?3, \
             updated_at = datetime('now') \
             WHERE id = ?4",
            rusqlite::params![attributes_json, financial_health, company_number, id],
        )?;
        Ok(())
    }

    /// Lightweight: just mark ch_verified_at + ch_company_number (used by enrich stage).
    pub fn mark_ch_verified(&self, id: &str, company_number: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             ch_verified_at = datetime('now'), \
             ch_company_number = ?1, \
             updated_at = datetime('now') \
             WHERE id = ?2",
            rusqlite::params![company_number, id],
        )?;
        Ok(())
    }

    pub fn reset_error_companies(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = 'discovered', last_error = NULL, updated_at = datetime('now') WHERE status = 'error'",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    /// Reset enriched, enriching, and error companies back to discovered for re-enrichment.
    /// Clears all enrichment fields so they go through the full pipeline again.
    pub fn reset_for_reenrichment(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             status = 'discovered', \
             description = NULL, description_original = NULL, snippet_english = NULL, \
             category = NULL, subcategory = NULL, \
             specialties = NULL, certifications = NULL, company_size = NULL, \
             relevance_score = NULL, enrichment_quality = NULL, \
             contact_name = NULL, contact_email = NULL, contact_title = NULL, \
             attributes_json = NULL, address = NULL, financial_health = NULL, \
             last_error = NULL, \
             updated_at = datetime('now') \
             WHERE status IN ('enriched', 'enriching', 'error')",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    pub fn get_emails(&self, status: Option<&str>, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let (query, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(s) = status {
            (
                "SELECT e.*, c.name as company_name FROM emails e JOIN companies c ON e.company_id = c.id WHERE e.status = ?1 ORDER BY e.created_at DESC LIMIT ?2",
                vec![Box::new(s.to_string()), Box::new(limit)],
            )
        } else {
            (
                "SELECT e.*, c.name as company_name FROM emails e JOIN companies c ON e.company_id = c.id ORDER BY e.created_at DESC LIMIT ?1",
                vec![Box::new(limit)],
            )
        };

        let mut stmt = conn.prepare(query)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let rows: Vec<Value> = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    obj.insert(col.clone(), val.map(|v| json!(v)).unwrap_or(json!(null)));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    pub fn update_email_status(&self, id: &str, status: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE emails SET status = ?1, updated_at = datetime('now') WHERE id = ?2",
            [status, id],
        )?;
        Ok(())
    }

    /// Store the error message on a failed email for debugging.
    pub fn set_email_error(&self, id: &str, error: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE emails SET last_error = ?1, updated_at = datetime('now') WHERE id = ?2",
            [error, id],
        )?;
        Ok(())
    }

    pub fn retry_failed_emails(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE emails SET status = 'approved', last_error = NULL, updated_at = datetime('now') WHERE status = 'failed'",
            [],
        )?;
        Ok(count)
    }

    /// Reset emails that have been "failed" for >1 hour back to "approved" for retry.
    /// This avoids tight retry loops while allowing transient errors to self-heal.
    pub fn retry_stale_failed_emails(&self) -> Result<usize> {
        let conn = self.conn.lock().unwrap();
        let count = conn.execute(
            "UPDATE emails SET status = 'approved', updated_at = datetime('now') WHERE status = 'failed' AND updated_at < datetime('now', '-1 hour')",
            [],
        )?;
        Ok(count)
    }

    pub fn get_all_config(&self) -> Result<Value> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare("SELECT key, value FROM config")?;
        let mut config = serde_json::Map::new();
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        for row in rows {
            if let Ok((k, v)) = row {
                config.insert(k, json!(v));
            }
        }
        Ok(Value::Object(config))
    }

    pub fn set_config(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO config (key, value) VALUES (?1, ?2) ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
            [key, value],
        )?;
        Ok(())
    }

    pub fn get_run_log(&self, job_id: Option<&str>, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let (query, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = if let Some(jid) = job_id {
            (
                "SELECT * FROM run_log WHERE job_id = ?1 ORDER BY created_at DESC LIMIT ?2",
                vec![Box::new(jid.to_string()), Box::new(limit)],
            )
        } else {
            (
                "SELECT * FROM run_log ORDER BY created_at DESC LIMIT ?1",
                vec![Box::new(limit)],
            )
        };

        let mut stmt = conn.prepare(query)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();

        let rows: Vec<Value> = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    obj.insert(col.clone(), val.map(|v| json!(v)).unwrap_or(json!(null)));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    pub fn insert_company(&self, company: &Value) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let name_normalized = normalize_company_name(name);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO companies (id, name, website_url, domain, country, city, source, source_url, source_query, raw_snippet, name_normalized, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, 'discovered')",
            rusqlite::params![
                id,
                name,
                company.get("website_url").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("domain").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("country").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("city").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("source").and_then(|v| v.as_str()).unwrap_or("brave"),
                company.get("source_url").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("source_query").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("raw_snippet").and_then(|v| v.as_str()).unwrap_or(""),
                name_normalized,
            ],
        )?;
        Ok(id)
    }

    /// Insert a company imported from Supabase for audit re-enrichment.
    /// Sets supabase_listing_id so push stage knows to UPDATE, not INSERT.
    pub fn insert_company_for_audit(
        &self,
        company: &Value,
        supabase_listing_id: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let name = company
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let name_normalized = normalize_company_name(name);
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO companies (id, name, website_url, domain, country, city, source, description, contact_name, contact_email, contact_title, contact_phone, supabase_listing_id, name_normalized, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'audit', ?7, ?8, ?9, ?10, ?11, ?12, ?13, 'discovered')",
            rusqlite::params![
                id,
                name,
                company.get("website_url").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("domain").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("country").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("city").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("description").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("contact_name").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("contact_email").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("contact_title").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("contact_phone").and_then(|v| v.as_str()).unwrap_or(""),
                supabase_listing_id,
                name_normalized,
            ],
        )?;
        Ok(id)
    }

    pub fn domain_exists(&self, domain: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM companies WHERE domain = ?1",
            [domain],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Check if a company name already exists (normalized matching).
    pub fn name_exists_normalized(&self, name: &str) -> Result<bool> {
        let normalized = normalize_company_name(name);
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM companies WHERE name_normalized = ?1",
            [&normalized],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn insert_job(&self, stages: &[String]) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO jobs (id, stages, status) VALUES (?1, ?2, 'running')",
            [&id, &stages.join(",")],
        )?;
        Ok(id)
    }

    pub fn update_job(&self, id: &str, status: &str, summary: &Value) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET status = ?1, summary = ?2, completed_at = datetime('now'), updated_at = datetime('now') WHERE id = ?3",
            rusqlite::params![status, summary.to_string(), id],
        )?;
        Ok(())
    }

    pub fn log_activity(&self, job_id: &str, stage: &str, level: &str, message: &str) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO run_log (id, job_id, stage, level, message) VALUES (?1, ?2, ?3, ?4, ?5)",
            [&id, job_id, stage, level, message],
        )?;
        Ok(())
    }

    pub fn record_search(&self, query: &str, country: &str, result_count: i64) -> Result<()> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO search_history (id, query, country, result_count) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, query, country, result_count],
        )?;
        Ok(())
    }

    pub fn search_already_done(&self, query: &str) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM search_history WHERE query = ?1 AND created_at > datetime('now', '-7 days')",
            [query],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    pub fn insert_email(
        &self,
        company_id: &str,
        subject: &str,
        body: &str,
        to_email: &str,
        from_email: &str,
        language: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO emails (id, company_id, subject, body, to_email, from_email, language, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'draft')",
            rusqlite::params![id, company_id, subject, body, to_email, from_email, language],
        )?;
        Ok(id)
    }

    pub fn update_email_sent(&self, id: &str, resend_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE emails SET status = 'sent', resend_id = ?1, sent_at = datetime('now'), updated_at = datetime('now') WHERE id = ?2",
            [resend_id, id],
        )?;
        Ok(())
    }

    /// Get all approved emails ready for sending.
    pub fn get_approved_emails(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, to_email, from_email, subject, body FROM emails WHERE status = 'approved' ORDER BY created_at ASC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "to_email": row.get::<_, String>(1)?,
                    "from_email": row.get::<_, String>(2)?,
                    "subject": row.get::<_, String>(3)?,
                    "body": row.get::<_, String>(4)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get a batch of approved emails ready for sending (FIFO, limited).
    pub fn get_approved_emails_batch(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, to_email, from_email, subject, body FROM emails WHERE status = 'approved' ORDER BY created_at ASC LIMIT ?1"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "to_email": row.get::<_, String>(1)?,
                    "from_email": row.get::<_, String>(2)?,
                    "subject": row.get::<_, String>(3)?,
                    "body": row.get::<_, String>(4)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Count emails sent today (sent, opened, replied, bounced).
    pub fn get_emails_sent_today(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status IN ('sent','opened','replied','bounced') AND sent_at >= date('now', 'localtime', 'start of day')",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Approve all draft emails, returning count updated.
    pub fn approve_all_drafts(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE emails SET status = 'approved', updated_at = datetime('now') WHERE status = 'draft'",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    /// Update email tracking fields from Resend polling.
    pub fn update_email_tracking(&self, id: &str, opened_at: Option<&str>, bounced: bool) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        if bounced {
            conn.execute(
                "UPDATE emails SET status = 'bounced', bounced_at = datetime('now'), updated_at = datetime('now') WHERE id = ?1",
                [id],
            )?;
        } else if let Some(opened) = opened_at {
            conn.execute(
                "UPDATE emails SET status = 'opened', opened_at = ?1, updated_at = datetime('now') WHERE id = ?2",
                [opened, id],
            )?;
        }
        Ok(())
    }

    /// Get sent emails with resend_ids for status polling.
    pub fn get_sent_emails_for_tracking(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, resend_id FROM emails WHERE resend_id IS NOT NULL AND resend_id != '' AND status IN ('sent', 'opened')"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "resend_id": row.get::<_, String>(1)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get category coverage for a country, sorted by companies_found ASC (least covered first)
    pub fn get_category_coverage(&self, country: &str) -> Result<Vec<(String, i64, i64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT category_id, searches_run, companies_found FROM category_coverage WHERE country = ?1 ORDER BY companies_found ASC"
        )?;
        let rows = stmt
            .query_map([country], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Increment category coverage counters after a batch search
    pub fn increment_category_coverage(&self, category_id: &str, country: &str, new_companies: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let id = uuid::Uuid::new_v4().to_string();
        conn.execute(
            "INSERT INTO category_coverage (id, category_id, country, searches_run, companies_found, last_searched_at) \
             VALUES (?1, ?2, ?3, 1, ?4, datetime('now')) \
             ON CONFLICT(category_id, country) DO UPDATE SET \
             searches_run = searches_run + 1, \
             companies_found = companies_found + excluded.companies_found, \
             last_searched_at = datetime('now')",
            rusqlite::params![id, category_id, country, new_companies],
        )?;
        Ok(())
    }

    /// Get analytics data for dashboard charts
    pub fn get_analytics(&self) -> Result<Value> {
        let conn = self.conn.lock().unwrap();

        let mut stmt = conn.prepare(
            "SELECT subcategory, COUNT(*) as count FROM companies WHERE subcategory IS NOT NULL AND subcategory != '' GROUP BY subcategory ORDER BY count DESC LIMIT 20"
        )?;
        let by_subcategory: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({ "name": row.get::<_, String>(0)?, "count": row.get::<_, i64>(1)? }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut stmt = conn.prepare(
            "SELECT country, COUNT(*) as count FROM companies WHERE country IS NOT NULL AND country != '' GROUP BY country ORDER BY count DESC"
        )?;
        let by_country: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({ "name": row.get::<_, String>(0)?, "count": row.get::<_, i64>(1)? }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut stmt = conn.prepare(
            "SELECT status, COUNT(*) as count FROM companies GROUP BY status ORDER BY count DESC"
        )?;
        let pipeline_funnel: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({ "name": row.get::<_, String>(0)?, "count": row.get::<_, i64>(1)? }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let mut stmt = conn.prepare(
            "SELECT attributes_json FROM companies WHERE attributes_json IS NOT NULL AND attributes_json != ''"
        )?;
        let attr_rows: Vec<String> = stmt
            .query_map([], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();

        let mut equipment_counts: HashMap<String, i64> = HashMap::new();
        let mut material_counts: HashMap<String, i64> = HashMap::new();
        let mut cert_counts: HashMap<String, i64> = HashMap::new();
        let mut industry_counts: HashMap<String, i64> = HashMap::new();

        for raw in &attr_rows {
            if let Ok(attrs) = serde_json::from_str::<Value>(raw) {
                if let Some(arr) = attrs.get("key_equipment").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            if !s.is_empty() {
                                *equipment_counts.entry(s.to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                }
                if let Some(arr) = attrs.get("materials").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            if !s.is_empty() {
                                *material_counts.entry(s.to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                }
                if let Some(arr) = attrs.get("certifications").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            if !s.is_empty() {
                                *cert_counts.entry(s.to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                }
                if let Some(arr) = attrs.get("industries").and_then(|v| v.as_array()) {
                    for item in arr {
                        if let Some(s) = item.as_str() {
                            if !s.is_empty() {
                                *industry_counts.entry(s.to_string()).or_insert(0) += 1;
                            }
                        }
                    }
                }
            }
        }

        fn top_n(map: &HashMap<String, i64>, n: usize) -> Vec<Value> {
            let mut entries: Vec<_> = map.iter().collect();
            entries.sort_by(|a, b| b.1.cmp(a.1));
            entries.into_iter().take(n).map(|(name, count)| json!({ "name": name, "count": count })).collect()
        }

        Ok(json!({
            "by_subcategory": by_subcategory,
            "by_country": by_country,
            "pipeline_funnel": pipeline_funnel,
            "by_equipment": top_n(&equipment_counts, 20),
            "by_material": top_n(&material_counts, 20),
            "by_certification": top_n(&cert_counts, 20),
            "by_industry": top_n(&industry_counts, 20),
        }))
    }

    /// Get companies with optional filters for drill-down
    pub fn get_companies_filtered(
        &self,
        status: Option<&str>,
        subcategory: Option<&str>,
        country: Option<&str>,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();

        let mut conditions = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut idx = 1;

        if let Some(s) = status {
            conditions.push(format!("status = ?{}", idx));
            params.push(Box::new(s.to_string()));
            idx += 1;
        }
        if let Some(sc) = subcategory {
            conditions.push(format!("subcategory = ?{}", idx));
            params.push(Box::new(sc.to_string()));
            idx += 1;
        }
        if let Some(c) = country {
            conditions.push(format!("country = ?{}", idx));
            params.push(Box::new(c.to_string()));
            idx += 1;
        }
        if let Some(q) = search {
            conditions.push(format!(
                "(name LIKE ?{0} OR description LIKE ?{0} OR attributes_json LIKE ?{0})",
                idx
            ));
            params.push(Box::new(format!("%{}%", q)));
            idx += 1;
        }

        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };

        let query = format!(
            "SELECT * FROM companies {} ORDER BY created_at DESC LIMIT ?{} OFFSET ?{}",
            where_clause, idx, idx + 1
        );
        params.push(Box::new(limit));
        params.push(Box::new(offset));

        let mut stmt = conn.prepare(&query)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let params_refs: Vec<&dyn rusqlite::types::ToSql> =
            params.iter().map(|p| p.as_ref()).collect();

        let rows: Vec<Value> = stmt
            .query_map(params_refs.as_slice(), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Load all embeddings from supplier_embeddings table.
    /// Returns Vec<(company_id, embedding)>.
    pub fn load_embeddings(&self) -> Result<Vec<(String, Vec<f32>)>> {
        let conn = self.conn.lock().unwrap();

        // Check if table exists
        let table_exists: bool = conn
            .prepare("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='supplier_embeddings'")?
            .query_row([], |row| row.get::<_, i64>(0))
            .map(|c| c > 0)?;

        if !table_exists {
            return Ok(Vec::new());
        }

        let mut stmt = conn.prepare("SELECT company_id, embedding FROM supplier_embeddings")?;
        let rows: Vec<(String, Vec<f32>)> = stmt
            .query_map([], |row| {
                let id: String = row.get(0)?;
                let emb_json: String = row.get(1)?;
                Ok((id, emb_json))
            })?
            .filter_map(|r| r.ok())
            .filter_map(|(id, emb_json)| {
                let emb: Vec<f32> = serde_json::from_str(&emb_json).ok()?;
                Some((id, emb))
            })
            .collect();

        Ok(rows)
    }

    /// Get companies by a list of IDs, preserving the order of the input.
    pub fn get_companies_by_ids(&self, ids: &[String]) -> Result<Vec<Value>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let conn = self.conn.lock().unwrap();

        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i)).collect();
        let query = format!(
            "SELECT * FROM companies WHERE id IN ({})",
            placeholders.join(", ")
        );

        let mut stmt = conn.prepare(&query)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let params: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|s| s as &dyn rusqlite::types::ToSql).collect();

        let rows: Vec<Value> = stmt
            .query_map(params.as_slice(), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        // Re-order to match input ids order
        let mut map: HashMap<String, Value> = HashMap::new();
        for row in rows {
            if let Some(id) = row.get("id").and_then(|v| v.as_str()) {
                map.insert(id.to_string(), row);
            }
        }
        let ordered: Vec<Value> = ids.iter().filter_map(|id| map.remove(id)).collect();
        Ok(ordered)
    }

    /// Approve all enriched companies (set status from 'enriched' to 'approved')
    pub fn approve_all_enriched(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = 'approved', updated_at = datetime('now') WHERE status = 'enriched'",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    pub fn get_companies_count(&self, status: Option<&str>) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = if let Some(s) = status {
            conn.query_row(
                "SELECT COUNT(*) FROM companies WHERE status = ?1",
                [s],
                |row| row.get(0),
            )?
        } else {
            conn.query_row("SELECT COUNT(*) FROM companies", [], |row| row.get(0))?
        };
        Ok(count)
    }

    pub fn batch_update_status(&self, ids: &[String], status: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        if ids.is_empty() {
            return Ok(0);
        }
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i + 1)).collect();
        let query = format!(
            "UPDATE companies SET status = ?1, updated_at = datetime('now') WHERE id IN ({})",
            placeholders.join(",")
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        params.push(Box::new(status.to_string()));
        for id in ids {
            params.push(Box::new(id.clone()));
        }
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        conn.execute(&query, params_refs.as_slice())?;
        Ok(conn.changes() as i64)
    }

    pub fn delete_emails(&self, ids: &[String]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        if ids.is_empty() {
            return Ok(0);
        }
        let placeholders: Vec<String> = (1..=ids.len()).map(|i| format!("?{}", i)).collect();
        let query = format!(
            "DELETE FROM emails WHERE id IN ({})",
            placeholders.join(",")
        );
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        for id in ids {
            params.push(Box::new(id.clone()));
        }
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        conn.execute(&query, params_refs.as_slice())?;
        Ok(conn.changes() as i64)
    }

    pub fn get_stats_history(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT date(created_at) as day, \
             COUNT(*) as total, \
             SUM(CASE WHEN status IN ('enriched','approved','pushed') THEN 1 ELSE 0 END) as enriched, \
             SUM(CASE WHEN status = 'pushed' THEN 1 ELSE 0 END) as pushed \
             FROM companies \
             WHERE created_at >= date('now', '-7 days') \
             GROUP BY date(created_at) \
             ORDER BY day ASC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "date": row.get::<_, String>(0)?,
                    "companies": row.get::<_, i64>(1)?,
                    "enriched": row.get::<_, i64>(2)?,
                    "pushed": row.get::<_, i64>(3)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn get_run_history(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, stages, status, summary, started_at, completed_at, created_at FROM jobs ORDER BY created_at DESC LIMIT ?1"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "stages": row.get::<_, String>(1)?,
                    "status": row.get::<_, String>(2)?,
                    "summary": row.get::<_, Option<String>>(3)?,
                    "started_at": row.get::<_, Option<String>>(4)?,
                    "completed_at": row.get::<_, Option<String>>(5)?,
                    "created_at": row.get::<_, Option<String>>(6)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Backup the database to a file using VACUUM INTO.
    pub fn backup(&self, backup_path: &std::path::Path) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let path_str = backup_path.to_str().ok_or_else(|| anyhow::anyhow!("Invalid backup path"))?;
        conn.execute_batch(&format!("VACUUM INTO '{}'", path_str.replace('\'', "''")))?;
        Ok(())
    }

    /// Get companies with lat/lng for the map view. Lightweight payload.
    pub fn get_companies_for_map(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, latitude, longitude, subcategory, city, country, relevance_score, website_url \
             FROM companies \
             WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "latitude": row.get::<_, f64>(2)?,
                    "longitude": row.get::<_, f64>(3)?,
                    "subcategory": row.get::<_, Option<String>>(4)?,
                    "city": row.get::<_, Option<String>>(5)?,
                    "country": row.get::<_, Option<String>>(6)?,
                    "relevance_score": row.get::<_, Option<i64>>(7)?,
                    "website_url": row.get::<_, Option<String>>(8)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Update latitude and longitude for a company.
    pub fn update_company_geocode(&self, id: &str, lat: f64, lng: f64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET latitude = ?1, longitude = ?2, updated_at = datetime('now') WHERE id = ?3",
            rusqlite::params![lat, lng, id],
        )?;
        Ok(())
    }

    /// Get candidates for deep enrichment trial: mix of top/mid/lower quality enriched companies.
    pub fn get_deep_enrich_candidates(&self, count: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let per_tier = count / 3;
        let remainder = count - per_tier * 3;

        // Top tier
        let mut stmt = conn.prepare(
            "SELECT * FROM companies WHERE status IN ('enriched','approved','pushed') AND website_url IS NOT NULL AND website_url != '' AND deep_enriched_at IS NULL ORDER BY enrichment_quality DESC LIMIT ?1"
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let mut rows: Vec<Value> = stmt
            .query_map([per_tier + remainder], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        let top_ids: Vec<String> = rows.iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        // Mid tier
        let mut stmt2 = conn.prepare(
            "SELECT * FROM companies WHERE status IN ('enriched','approved','pushed') AND website_url IS NOT NULL AND website_url != '' AND deep_enriched_at IS NULL ORDER BY enrichment_quality DESC LIMIT ?1 OFFSET 100"
        )?;
        let columns2: Vec<String> = stmt2.column_names().iter().map(|c| c.to_string()).collect();
        let mid_rows: Vec<Value> = stmt2
            .query_map([per_tier], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns2.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .filter(|r| {
                let id = r.get("id").and_then(|v| v.as_str()).unwrap_or("");
                !top_ids.contains(&id.to_string())
            })
            .collect();
        rows.extend(mid_rows);

        let all_ids: Vec<String> = rows.iter()
            .filter_map(|r| r.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
            .collect();

        // Lower tier
        let mut stmt3 = conn.prepare(
            "SELECT * FROM companies WHERE status IN ('enriched','approved','pushed') AND website_url IS NOT NULL AND website_url != '' AND deep_enriched_at IS NULL ORDER BY enrichment_quality DESC LIMIT ?1 OFFSET 500"
        )?;
        let columns3: Vec<String> = stmt3.column_names().iter().map(|c| c.to_string()).collect();
        let lower_rows: Vec<Value> = stmt3
            .query_map([per_tier], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns3.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .filter(|r| {
                let id = r.get("id").and_then(|v| v.as_str()).unwrap_or("");
                !all_ids.contains(&id.to_string())
            })
            .collect();
        rows.extend(lower_rows);

        Ok(rows)
    }

    /// Save deep enrichment results for a company.
    pub fn update_deep_enrichment(
        &self,
        id: &str,
        process_capabilities_json: &str,
        deep_website_text: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET process_capabilities_json = ?1, deep_website_text = ?2, deep_enriched_at = datetime('now'), updated_at = datetime('now') WHERE id = ?3",
            rusqlite::params![process_capabilities_json, deep_website_text, id],
        )?;
        Ok(())
    }

    /// Get a batch of unenriched candidates for the deep enrichment drain-loop.
    /// Simple query with LIMIT — no stratified sampling (that's only for trial mode).
    pub fn get_deep_enrich_batch(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM companies WHERE status IN ('enriched','approved','pushed') AND website_url IS NOT NULL AND website_url != '' AND deep_enriched_at IS NULL ORDER BY enrichment_quality DESC LIMIT ?1"
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get ALL unenriched candidates for deep enrichment (no sector filter, no limit).
    pub fn get_all_deep_enrich_candidates(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM companies WHERE status IN ('enriched','approved','pushed') AND website_url IS NOT NULL AND website_url != '' AND deep_enriched_at IS NULL ORDER BY enrichment_quality DESC"
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get deep enrich candidates filtered by sector (category, subcategory, or specialties).
    pub fn get_deep_enrich_candidates_by_sector(&self, sector: &str, count: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let pattern = format!("%{}%", sector);
        let mut stmt = conn.prepare(
            "SELECT * FROM companies WHERE status IN ('enriched','approved','pushed') AND website_url IS NOT NULL AND website_url != '' AND deep_enriched_at IS NULL AND (category LIKE ?1 OR subcategory LIKE ?1 OR specialties LIKE ?1) ORDER BY enrichment_quality DESC LIMIT ?2"
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows: Vec<Value> = stmt
            .query_map(rusqlite::params![pattern, count], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get all deep-enriched companies (optionally by sector) with their process capabilities.
    pub fn get_deep_enriched_processes(&self, sector: Option<&str>) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = match sector {
            Some(s) => {
                let pattern = format!("%{}%", s);
                (
                    "SELECT id, name, process_capabilities_json, subcategory, category FROM companies WHERE deep_enriched_at IS NOT NULL AND process_capabilities_json IS NOT NULL AND process_capabilities_json != '[]' AND (category LIKE ?1 OR subcategory LIKE ?1 OR specialties LIKE ?1)",
                    vec![Box::new(pattern) as Box<dyn rusqlite::types::ToSql>],
                )
            }
            None => (
                "SELECT id, name, process_capabilities_json, subcategory, category FROM companies WHERE deep_enriched_at IS NOT NULL AND process_capabilities_json IS NOT NULL AND process_capabilities_json != '[]'",
                vec![],
            ),
        };
        let mut stmt = conn.prepare(sql)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows: Vec<Value> = stmt
            .query_map(rusqlite::params_from_iter(&params), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Insert or replace a technique knowledge record.
    pub fn upsert_technique_knowledge(&self, record: &Value) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR REPLACE INTO technique_knowledge (id, technique_slug, sector, article_markdown, real_world_tolerances, real_world_materials, real_world_equipment, real_world_surface_finishes, typical_batch_sizes, tips_and_insights, common_applications, supplier_count, source_company_ids, generated_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, datetime('now'))",
            rusqlite::params![
                record.get("id").and_then(|v| v.as_str()).unwrap_or(""),
                record.get("technique_slug").and_then(|v| v.as_str()).unwrap_or(""),
                record.get("sector").and_then(|v| v.as_str()).unwrap_or(""),
                record.get("article_markdown").and_then(|v| v.as_str()),
                record.get("real_world_tolerances").map(|v| v.to_string()),
                record.get("real_world_materials").map(|v| v.to_string()),
                record.get("real_world_equipment").map(|v| v.to_string()),
                record.get("real_world_surface_finishes").map(|v| v.to_string()),
                record.get("typical_batch_sizes").map(|v| v.to_string()),
                record.get("tips_and_insights").map(|v| v.to_string()),
                record.get("common_applications").map(|v| v.to_string()),
                record.get("supplier_count").and_then(|v| v.as_i64()).unwrap_or(0),
                record.get("source_company_ids").map(|v| v.to_string()),
            ],
        )?;
        Ok(())
    }

    /// Get all technique knowledge records, optionally filtered by sector.
    pub fn get_technique_knowledge(&self, sector: Option<&str>) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let (sql, params): (&str, Vec<Box<dyn rusqlite::types::ToSql>>) = match sector {
            Some(s) => (
                "SELECT * FROM technique_knowledge WHERE sector = ?1 ORDER BY supplier_count DESC",
                vec![Box::new(s.to_string()) as Box<dyn rusqlite::types::ToSql>],
            ),
            None => (
                "SELECT * FROM technique_knowledge ORDER BY supplier_count DESC",
                vec![],
            ),
        };
        let mut stmt = conn.prepare(sql)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows: Vec<Value> = stmt
            .query_map(rusqlite::params_from_iter(&params), |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Mark a technique knowledge record as pushed to Supabase.
    pub fn mark_technique_pushed(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE technique_knowledge SET pushed_at = datetime('now') WHERE id = ?1",
            rusqlite::params![id],
        )?;
        Ok(())
    }

    /// Get technique knowledge records that haven't been pushed yet.
    pub fn get_unpushed_technique_knowledge(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM technique_knowledge WHERE pushed_at IS NULL ORDER BY supplier_count DESC"
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Store the Supabase listing ID back to the company after pushing.
    pub fn set_supabase_listing_id(&self, company_id: &str, listing_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET supabase_listing_id = ?1, updated_at = datetime('now') WHERE id = ?2",
            [listing_id, company_id],
        )?;
        Ok(())
    }

    /// Get pushed companies that have process_capabilities_json and a supabase_listing_id.
    pub fn get_pushed_companies_with_capabilities(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, supabase_listing_id, process_capabilities_json FROM companies \
             WHERE status = 'pushed' \
             AND supabase_listing_id IS NOT NULL AND supabase_listing_id != '' \
             AND process_capabilities_json IS NOT NULL AND process_capabilities_json != '' AND process_capabilities_json != '[]'"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, Option<String>>(1)?,
                    "supabase_listing_id": row.get::<_, String>(2)?,
                    "process_capabilities_json": row.get::<_, String>(3)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get companies that have an address or city but no lat/lng (for backfill geocoding).
    pub fn get_companies_needing_geocoding(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, address, city, country FROM companies \
             WHERE latitude IS NULL \
             AND ((address IS NOT NULL AND address != '') OR (city IS NOT NULL AND city != ''))"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "address": row.get::<_, Option<String>>(1)?,
                    "city": row.get::<_, Option<String>>(2)?,
                    "country": row.get::<_, Option<String>>(3)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    // --- Email Templates ---

    pub fn insert_email_template(&self, name: &str, subject: &str, body: &str) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO email_templates (id, name, subject, body) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, name, subject, body],
        )?;
        Ok(id)
    }

    pub fn update_email_template(&self, id: &str, name: &str, subject: &str, body: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE email_templates SET name = ?1, subject = ?2, body = ?3, updated_at = datetime('now') WHERE id = ?4",
            rusqlite::params![name, subject, body, id],
        )?;
        Ok(())
    }

    pub fn delete_email_template(&self, id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute("DELETE FROM email_templates WHERE id = ?1", [id])?;
        Ok(())
    }

    pub fn get_email_templates(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, subject, body, is_active, created_at, updated_at FROM email_templates ORDER BY updated_at DESC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "subject": row.get::<_, String>(2)?,
                    "body": row.get::<_, String>(3)?,
                    "is_active": row.get::<_, i64>(4)?,
                    "created_at": row.get::<_, String>(5)?,
                    "updated_at": row.get::<_, String>(6)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn get_email_template(&self, id: &str) -> Result<Value> {
        let conn = self.conn.lock().unwrap();
        let row = conn.query_row(
            "SELECT id, name, subject, body, is_active, created_at, updated_at FROM email_templates WHERE id = ?1",
            [id],
            |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "subject": row.get::<_, String>(2)?,
                    "body": row.get::<_, String>(3)?,
                    "is_active": row.get::<_, i64>(4)?,
                    "created_at": row.get::<_, String>(5)?,
                    "updated_at": row.get::<_, String>(6)?,
                }))
            },
        )?;
        Ok(row)
    }

    /// Get companies eligible for template-based outreach campaigns.
    /// Must be pushed, have contact_email + supabase_listing_id, and not already emailed via template.
    /// Returns all fields needed for LLM personalisation prompt.
    pub fn get_campaign_eligible_companies(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT c.id, c.name, c.contact_name, c.contact_email, c.contact_title, \
                    c.country, c.city, c.subcategory, c.description, \
                    c.specialties, c.certifications, c.industries, \
                    c.company_size, c.year_founded, \
                    c.attributes_json, c.process_capabilities_json, \
                    c.supabase_listing_id \
             FROM companies c \
             WHERE c.status = 'pushed' \
             AND c.contact_email IS NOT NULL AND c.contact_email != '' \
             AND c.supabase_listing_id IS NOT NULL AND c.supabase_listing_id != '' \
             AND c.id NOT IN ( \
                 SELECT e.company_id FROM emails e \
                 WHERE e.template_id IS NOT NULL \
                 AND e.status NOT IN ('failed', 'bounced') \
             ) \
             ORDER BY c.updated_at DESC \
             LIMIT ?1"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "contact_name": row.get::<_, Option<String>>(2)?,
                    "contact_email": row.get::<_, String>(3)?,
                    "contact_title": row.get::<_, Option<String>>(4)?,
                    "country": row.get::<_, Option<String>>(5)?,
                    "city": row.get::<_, Option<String>>(6)?,
                    "subcategory": row.get::<_, Option<String>>(7)?,
                    "description": row.get::<_, Option<String>>(8)?,
                    "specialties": row.get::<_, Option<String>>(9)?,
                    "certifications": row.get::<_, Option<String>>(10)?,
                    "industries": row.get::<_, Option<String>>(11)?,
                    "company_size": row.get::<_, Option<String>>(12)?,
                    "year_founded": row.get::<_, Option<i64>>(13)?,
                    "attributes_json": row.get::<_, Option<String>>(14)?,
                    "process_capabilities_json": row.get::<_, Option<String>>(15)?,
                    "supabase_listing_id": row.get::<_, String>(16)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    pub fn get_campaign_eligible_count(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM companies c \
             WHERE c.status = 'pushed' \
             AND c.contact_email IS NOT NULL AND c.contact_email != '' \
             AND c.supabase_listing_id IS NOT NULL AND c.supabase_listing_id != '' \
             AND c.id NOT IN ( \
                 SELECT e.company_id FROM emails e \
                 WHERE e.template_id IS NOT NULL \
                 AND e.status NOT IN ('failed', 'bounced') \
             )",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Insert an email created from a template, with claim token for audit trail.
    pub fn insert_template_email(
        &self,
        company_id: &str,
        template_id: &str,
        subject: &str,
        body: &str,
        to_email: &str,
        from_email: &str,
        claim_token: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO emails (id, company_id, subject, body, to_email, from_email, language, status, template_id, claim_token) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'en', 'draft', ?7, ?8)",
            rusqlite::params![id, company_id, subject, body, to_email, from_email, template_id, claim_token],
        )?;
        Ok(id)
    }

    /// Insert an email with optional A/B variant tracking.
    pub fn insert_template_email_with_variant(
        &self,
        company_id: &str,
        template_id: &str,
        subject: &str,
        body: &str,
        to_email: &str,
        from_email: &str,
        claim_token: &str,
        ab_variant: Option<&str>,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO emails (id, company_id, subject, body, to_email, from_email, language, status, template_id, claim_token, ab_variant) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'en', 'draft', ?7, ?8, ?9)",
            rusqlite::params![id, company_id, subject, body, to_email, from_email, template_id, claim_token, ab_variant],
        )?;
        Ok(id)
    }

    /// Get companies with outreach status for the campaigns view.
    /// LEFT JOINs with the latest email per company to derive outreach status.
    pub fn get_outreach_companies(
        &self,
        outreach_status: Option<&str>,
        country: Option<&str>,
        category: Option<&str>,
        search: Option<&str>,
        limit: i64,
        offset: i64,
    ) -> Result<(Vec<Value>, i64)> {
        let conn = self.conn.lock().unwrap();

        // Build the base query with LEFT JOIN to latest email per company
        let base = "\
            FROM companies c \
            LEFT JOIN ( \
                SELECT company_id, status as email_status, created_at as last_email_at, \
                       claim_token, ab_variant, claim_status, \
                       ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY created_at DESC) as rn \
                FROM emails \
            ) le ON le.company_id = c.id AND le.rn = 1 \
            WHERE c.status = 'pushed' \
            AND c.contact_email IS NOT NULL AND c.contact_email != '' \
            AND c.supabase_listing_id IS NOT NULL AND c.supabase_listing_id != ''";

        let mut conditions = Vec::new();
        let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
        let mut param_idx = 1;

        if let Some(os) = outreach_status {
            if os == "not_contacted" {
                conditions.push("le.email_status IS NULL".to_string());
            } else {
                conditions.push(format!("le.email_status = ?{}", param_idx));
                params.push(Box::new(os.to_string()));
                param_idx += 1;
            }
        }

        if let Some(c) = country {
            conditions.push(format!("c.country = ?{}", param_idx));
            params.push(Box::new(c.to_string()));
            param_idx += 1;
        }

        if let Some(cat) = category {
            conditions.push(format!("c.subcategory = ?{}", param_idx));
            params.push(Box::new(cat.to_string()));
            param_idx += 1;
        }

        if let Some(s) = search {
            if !s.is_empty() {
                conditions.push(format!(
                    "(c.name LIKE ?{p} OR c.contact_email LIKE ?{p} OR c.contact_name LIKE ?{p})",
                    p = param_idx
                ));
                params.push(Box::new(format!("%{}%", s)));
                param_idx += 1;
            }
        }

        let where_extra = if conditions.is_empty() {
            String::new()
        } else {
            format!(" AND {}", conditions.join(" AND "))
        };

        // Count query
        let count_sql = format!("SELECT COUNT(*) {}{}", base, where_extra);
        let params_refs: Vec<&dyn rusqlite::types::ToSql> = params.iter().map(|p| p.as_ref()).collect();
        let total: i64 = conn.query_row(&count_sql, params_refs.as_slice(), |row| row.get(0))?;

        // Data query
        let data_sql = format!(
            "SELECT c.id, c.name, c.subcategory, c.country, c.city, c.contact_email, c.contact_name, \
                    c.contact_title, c.description, c.website_url, c.supabase_listing_id, \
                    COALESCE(le.email_status, 'not_contacted') as outreach_status, \
                    le.last_email_at, le.claim_status \
             {} {} \
             ORDER BY le.last_email_at DESC NULLS LAST, c.name ASC \
             LIMIT ?{} OFFSET ?{}",
            base, where_extra, param_idx, param_idx + 1
        );

        let mut data_params = params;
        data_params.push(Box::new(limit));
        data_params.push(Box::new(offset));
        let data_refs: Vec<&dyn rusqlite::types::ToSql> = data_params.iter().map(|p| p.as_ref()).collect();

        let mut stmt = conn.prepare(&data_sql)?;
        let rows: Vec<Value> = stmt
            .query_map(data_refs.as_slice(), |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "subcategory": row.get::<_, Option<String>>(2)?,
                    "country": row.get::<_, Option<String>>(3)?,
                    "city": row.get::<_, Option<String>>(4)?,
                    "contact_email": row.get::<_, Option<String>>(5)?,
                    "contact_name": row.get::<_, Option<String>>(6)?,
                    "contact_title": row.get::<_, Option<String>>(7)?,
                    "description": row.get::<_, Option<String>>(8)?,
                    "website_url": row.get::<_, Option<String>>(9)?,
                    "supabase_listing_id": row.get::<_, Option<String>>(10)?,
                    "outreach_status": row.get::<_, String>(11)?,
                    "last_email_at": row.get::<_, Option<String>>(12)?,
                    "claim_status": row.get::<_, Option<String>>(13)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok((rows, total))
    }

    /// Get aggregate outreach stats including A/B breakdown.
    pub fn get_outreach_stats(&self) -> Result<Value> {
        let conn = self.conn.lock().unwrap();

        let total_sent: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status IN ('sent', 'opened', 'replied', 'bounced')",
            [], |row| row.get(0),
        )?;
        let total_opened: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status IN ('opened', 'replied')",
            [], |row| row.get(0),
        )?;
        let total_bounced: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status = 'bounced'",
            [], |row| row.get(0),
        )?;
        let total_claimed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE claim_status = 'claimed'",
            [], |row| row.get(0),
        )?;
        let total_drafted: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status = 'draft'",
            [], |row| row.get(0),
        )?;
        let total_approved: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status = 'approved'",
            [], |row| row.get(0),
        )?;
        let total_failed: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status = 'failed'",
            [], |row| row.get(0),
        )?;

        let open_rate = if total_sent > 0 {
            (total_opened as f64 / total_sent as f64) * 100.0
        } else {
            0.0
        };
        let bounce_rate = if total_sent > 0 {
            (total_bounced as f64 / total_sent as f64) * 100.0
        } else {
            0.0
        };
        let claim_rate = if total_sent > 0 {
            (total_claimed as f64 / total_sent as f64) * 100.0
        } else {
            0.0
        };

        // A/B breakdown (SQLite doesn't support FILTER — use CASE)
        let mut ab_stmt = conn.prepare(
            "SELECT ab_variant, \
                    SUM(CASE WHEN status IN ('sent','opened','replied','bounced') THEN 1 ELSE 0 END) as sent, \
                    SUM(CASE WHEN status IN ('opened','replied') THEN 1 ELSE 0 END) as opened \
             FROM emails \
             WHERE ab_variant IS NOT NULL \
             GROUP BY ab_variant"
        )?;
        let ab_rows: Vec<Value> = ab_stmt
            .query_map([], |row| {
                let variant: String = row.get(0)?;
                let sent: i64 = row.get(1)?;
                let opened: i64 = row.get(2)?;
                let rate = if sent > 0 {
                    (opened as f64 / sent as f64) * 100.0
                } else {
                    0.0
                };
                Ok(json!({
                    "variant": variant,
                    "sent": sent,
                    "opened": opened,
                    "open_rate": (rate * 10.0).round() / 10.0,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(json!({
            "total_sent": total_sent,
            "total_opened": total_opened,
            "total_bounced": total_bounced,
            "total_claimed": total_claimed,
            "total_drafted": total_drafted,
            "total_approved": total_approved,
            "total_failed": total_failed,
            "open_rate": (open_rate * 10.0).round() / 10.0,
            "bounce_rate": (bounce_rate * 10.0).round() / 10.0,
            "claim_rate": (claim_rate * 10.0).round() / 10.0,
            "ab_variants": ab_rows,
        }))
    }

    /// Get all emails for a specific company, ordered by most recent first.
    pub fn get_company_email_history(&self, company_id: &str) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.subject, e.body, e.to_email, e.status, e.template_id, \
                    e.claim_token, e.ab_variant, e.claim_status, e.last_error, \
                    e.sent_at, e.opened_at, e.bounced_at, e.created_at, e.resend_id \
             FROM emails e \
             WHERE e.company_id = ?1 \
             ORDER BY e.created_at DESC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([company_id], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "subject": row.get::<_, String>(1)?,
                    "body": row.get::<_, String>(2)?,
                    "to_email": row.get::<_, String>(3)?,
                    "status": row.get::<_, String>(4)?,
                    "template_id": row.get::<_, Option<String>>(5)?,
                    "claim_token": row.get::<_, Option<String>>(6)?,
                    "ab_variant": row.get::<_, Option<String>>(7)?,
                    "claim_status": row.get::<_, Option<String>>(8)?,
                    "last_error": row.get::<_, Option<String>>(9)?,
                    "sent_at": row.get::<_, Option<String>>(10)?,
                    "opened_at": row.get::<_, Option<String>>(11)?,
                    "bounced_at": row.get::<_, Option<String>>(12)?,
                    "created_at": row.get::<_, Option<String>>(13)?,
                    "resend_id": row.get::<_, Option<String>>(14)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Batch update claim_status on emails matching by claim_token.
    pub fn update_claim_statuses(&self, updates: &[(String, String)]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let mut count = 0i64;
        for (token, status) in updates {
            let changed = conn.execute(
                "UPDATE emails SET claim_status = ?1, claim_status_synced_at = datetime('now') \
                 WHERE claim_token = ?2",
                rusqlite::params![status, token],
            )?;
            count += changed as i64;
        }
        Ok(count)
    }

    /// Get all emails that have claim tokens (for syncing claim status from Supabase).
    pub fn get_emails_with_claim_tokens(&self) -> Result<Vec<(String, String)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT DISTINCT claim_token, id FROM emails \
             WHERE claim_token IS NOT NULL AND claim_token != '' \
             AND status IN ('sent', 'opened', 'replied', 'draft', 'approved')"
        )?;
        let rows: Vec<(String, String)> = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    // --- Self-Learning Outreach (v0.23.0) ---

    /// Get all sent emails with outcomes for the learning cycle.
    pub fn get_email_outcomes_for_learning(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.id, e.status, e.ab_variant, e.strategy_text, e.generation, \
                    e.sent_at, e.opened_at, e.bounced_at, e.claim_status, \
                    c.subcategory, c.company_size, c.certifications, c.country \
             FROM emails e \
             LEFT JOIN companies c ON c.id = e.company_id \
             WHERE e.status IN ('sent', 'opened', 'replied', 'bounced') \
             ORDER BY e.sent_at DESC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "status": row.get::<_, String>(1)?,
                    "ab_variant": row.get::<_, Option<String>>(2)?,
                    "strategy_text": row.get::<_, Option<String>>(3)?,
                    "generation": row.get::<_, Option<i64>>(4)?,
                    "sent_at": row.get::<_, Option<String>>(5)?,
                    "opened_at": row.get::<_, Option<String>>(6)?,
                    "bounced_at": row.get::<_, Option<String>>(7)?,
                    "claim_status": row.get::<_, Option<String>>(8)?,
                    "subcategory": row.get::<_, Option<String>>(9)?,
                    "company_size": row.get::<_, Option<String>>(10)?,
                    "certifications": row.get::<_, Option<String>>(11)?,
                    "country": row.get::<_, Option<String>>(12)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Upsert an insight — replace if same type+text exists, otherwise insert.
    pub fn upsert_insight(
        &self,
        insight_type: &str,
        insight: &str,
        confidence: f64,
        source_count: i64,
        generation: i64,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // Check if a similar insight already exists
        let existing: Option<String> = conn
            .query_row(
                "SELECT id FROM outreach_insights WHERE insight_type = ?1 AND insight = ?2",
                rusqlite::params![insight_type, insight],
                |row| row.get(0),
            )
            .ok();

        if let Some(id) = existing {
            conn.execute(
                "UPDATE outreach_insights SET confidence = ?1, source_email_count = ?2, \
                 generation = ?3 WHERE id = ?4",
                rusqlite::params![confidence, source_count, generation, id],
            )?;
        } else {
            let id = uuid::Uuid::new_v4().to_string();
            conn.execute(
                "INSERT INTO outreach_insights (id, generation, insight_type, insight, confidence, source_email_count) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                rusqlite::params![id, generation, insight_type, insight, confidence, source_count],
            )?;
        }
        Ok(())
    }

    /// Get top N active insights ordered by confidence.
    pub fn get_active_insights(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, generation, insight_type, insight, confidence, source_email_count, created_at \
             FROM outreach_insights \
             ORDER BY confidence DESC \
             LIMIT ?1"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "generation": row.get::<_, i64>(1)?,
                    "insight_type": row.get::<_, String>(2)?,
                    "insight": row.get::<_, String>(3)?,
                    "confidence": row.get::<_, f64>(4)?,
                    "source_email_count": row.get::<_, i64>(5)?,
                    "created_at": row.get::<_, String>(6)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get the currently active A/B experiment (if any).
    pub fn get_active_experiment(&self) -> Result<Option<Value>> {
        let conn = self.conn.lock().unwrap();
        let result = conn.query_row(
            "SELECT id, generation, variant_a_strategy, variant_b_strategy, \
                    variant_a_sent, variant_b_sent, variant_a_opened, variant_b_opened, \
                    variant_a_claimed, variant_b_claimed, winner, status, created_at, completed_at \
             FROM ab_experiments \
             WHERE status = 'active' \
             LIMIT 1",
            [],
            |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "generation": row.get::<_, i64>(1)?,
                    "variant_a_strategy": row.get::<_, String>(2)?,
                    "variant_b_strategy": row.get::<_, String>(3)?,
                    "variant_a_sent": row.get::<_, i64>(4)?,
                    "variant_b_sent": row.get::<_, i64>(5)?,
                    "variant_a_opened": row.get::<_, i64>(6)?,
                    "variant_b_opened": row.get::<_, i64>(7)?,
                    "variant_a_claimed": row.get::<_, i64>(8)?,
                    "variant_b_claimed": row.get::<_, i64>(9)?,
                    "winner": row.get::<_, Option<String>>(10)?,
                    "status": row.get::<_, String>(11)?,
                    "created_at": row.get::<_, String>(12)?,
                    "completed_at": row.get::<_, Option<String>>(13)?,
                }))
            },
        );
        match result {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Create a new A/B experiment, returning its ID.
    pub fn create_experiment(
        &self,
        generation: i64,
        strategy_a: &str,
        strategy_b: &str,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO ab_experiments (id, generation, variant_a_strategy, variant_b_strategy) \
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![id, generation, strategy_a, strategy_b],
        )?;
        Ok(id)
    }

    /// Recalculate experiment stats from the emails table.
    pub fn update_experiment_stats(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        // Get active experiment
        let exp_id: Option<String> = conn
            .query_row(
                "SELECT id FROM ab_experiments WHERE status = 'active' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok();

        let exp_id = match exp_id {
            Some(id) => id,
            None => return Ok(()),
        };

        // Count A/B stats from emails linked to this experiment
        let (a_sent, a_opened, a_claimed): (i64, i64, i64) = conn.query_row(
            "SELECT \
                SUM(CASE WHEN status IN ('sent','opened','replied','bounced') THEN 1 ELSE 0 END), \
                SUM(CASE WHEN status IN ('opened','replied') THEN 1 ELSE 0 END), \
                SUM(CASE WHEN claim_status = 'claimed' THEN 1 ELSE 0 END) \
             FROM emails WHERE experiment_id = ?1 AND ab_variant = 'A'",
            [&exp_id],
            |row| Ok((
                row.get::<_, Option<i64>>(0)?.unwrap_or(0),
                row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                row.get::<_, Option<i64>>(2)?.unwrap_or(0),
            )),
        )?;

        let (b_sent, b_opened, b_claimed): (i64, i64, i64) = conn.query_row(
            "SELECT \
                SUM(CASE WHEN status IN ('sent','opened','replied','bounced') THEN 1 ELSE 0 END), \
                SUM(CASE WHEN status IN ('opened','replied') THEN 1 ELSE 0 END), \
                SUM(CASE WHEN claim_status = 'claimed' THEN 1 ELSE 0 END) \
             FROM emails WHERE experiment_id = ?1 AND ab_variant = 'B'",
            [&exp_id],
            |row| Ok((
                row.get::<_, Option<i64>>(0)?.unwrap_or(0),
                row.get::<_, Option<i64>>(1)?.unwrap_or(0),
                row.get::<_, Option<i64>>(2)?.unwrap_or(0),
            )),
        )?;

        conn.execute(
            "UPDATE ab_experiments SET \
             variant_a_sent = ?1, variant_a_opened = ?2, variant_a_claimed = ?3, \
             variant_b_sent = ?4, variant_b_opened = ?5, variant_b_claimed = ?6 \
             WHERE id = ?7",
            rusqlite::params![a_sent, a_opened, a_claimed, b_sent, b_opened, b_claimed, exp_id],
        )?;

        Ok(())
    }

    /// Complete an experiment by declaring a winner.
    pub fn complete_experiment(&self, id: &str, winner: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE ab_experiments SET winner = ?1, status = 'completed', \
             completed_at = datetime('now') WHERE id = ?2",
            rusqlite::params![winner, id],
        )?;
        Ok(())
    }

    /// Get daily outreach stats grouped by sent date.
    pub fn get_daily_outreach_stats(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT date(sent_at) as send_date, \
                    COUNT(*) as sent, \
                    SUM(CASE WHEN status IN ('opened','replied') THEN 1 ELSE 0 END) as opened, \
                    SUM(CASE WHEN status = 'bounced' THEN 1 ELSE 0 END) as bounced, \
                    SUM(CASE WHEN claim_status = 'claimed' THEN 1 ELSE 0 END) as claimed, \
                    MAX(generation) as generation \
             FROM emails \
             WHERE sent_at IS NOT NULL \
             GROUP BY date(sent_at) \
             ORDER BY send_date ASC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                let sent: i64 = row.get(1)?;
                let opened: i64 = row.get(2)?;
                let open_rate = if sent > 0 {
                    (opened as f64 / sent as f64 * 100.0 * 10.0).round() / 10.0
                } else {
                    0.0
                };
                Ok(json!({
                    "date": row.get::<_, String>(0)?,
                    "sent": sent,
                    "opened": opened,
                    "bounced": row.get::<_, i64>(3)?,
                    "claimed": row.get::<_, i64>(4)?,
                    "open_rate": open_rate,
                    "generation": row.get::<_, Option<i64>>(5)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get all A/B experiment history.
    pub fn get_experiment_history(&self) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, generation, variant_a_strategy, variant_b_strategy, \
                    variant_a_sent, variant_b_sent, variant_a_opened, variant_b_opened, \
                    variant_a_claimed, variant_b_claimed, winner, status, created_at, completed_at \
             FROM ab_experiments \
             ORDER BY generation ASC"
        )?;
        let rows: Vec<Value> = stmt
            .query_map([], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "generation": row.get::<_, i64>(1)?,
                    "variant_a_strategy": row.get::<_, String>(2)?,
                    "variant_b_strategy": row.get::<_, String>(3)?,
                    "variant_a_sent": row.get::<_, i64>(4)?,
                    "variant_b_sent": row.get::<_, i64>(5)?,
                    "variant_a_opened": row.get::<_, i64>(6)?,
                    "variant_b_opened": row.get::<_, i64>(7)?,
                    "variant_a_claimed": row.get::<_, i64>(8)?,
                    "variant_b_claimed": row.get::<_, i64>(9)?,
                    "winner": row.get::<_, Option<String>>(10)?,
                    "status": row.get::<_, String>(11)?,
                    "created_at": row.get::<_, String>(12)?,
                    "completed_at": row.get::<_, Option<String>>(13)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Insert a template email with full learning metadata.
    pub fn insert_template_email_with_learning(
        &self,
        company_id: &str,
        template_id: &str,
        subject: &str,
        body: &str,
        to_email: &str,
        from_email: &str,
        claim_token: &str,
        ab_variant: Option<&str>,
        strategy_text: Option<&str>,
        generation: i64,
        experiment_id: Option<&str>,
        insights_used: Option<&str>,
    ) -> Result<String> {
        let id = uuid::Uuid::new_v4().to_string();
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO emails (id, company_id, subject, body, to_email, from_email, language, status, \
             template_id, claim_token, ab_variant, strategy_text, generation, experiment_id, insights_used) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'en', 'draft', ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                id, company_id, subject, body, to_email, from_email,
                template_id, claim_token, ab_variant, strategy_text,
                generation, experiment_id, insights_used
            ],
        )?;
        Ok(id)
    }

    /// Get autopilot status: sent today, queued count, active generation info.
    pub fn get_autopilot_status(&self) -> Result<Value> {
        let conn = self.conn.lock().unwrap();

        let sent_today: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails \
             WHERE status IN ('sent','opened','replied','bounced') \
             AND date(sent_at) = date('now', 'localtime')",
            [],
            |row| row.get(0),
        )?;

        let approved_queued: i64 = conn.query_row(
            "SELECT COUNT(*) FROM emails WHERE status IN ('draft', 'approved')",
            [],
            |row| row.get(0),
        )?;

        let active_generation: Option<i64> = conn
            .query_row(
                "SELECT generation FROM ab_experiments WHERE status = 'active' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok();

        let active_experiment_id: Option<String> = conn
            .query_row(
                "SELECT id FROM ab_experiments WHERE status = 'active' LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok();

        let last_learning: Option<String> = conn
            .query_row(
                "SELECT created_at FROM outreach_insights ORDER BY created_at DESC LIMIT 1",
                [],
                |row| row.get(0),
            )
            .ok();

        let insight_count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM outreach_insights",
            [],
            |row| row.get(0),
        )?;

        Ok(json!({
            "sent_today": sent_today,
            "approved_queued": approved_queued,
            "active_generation": active_generation,
            "active_experiment_id": active_experiment_id,
            "last_learning_at": last_learning,
            "insight_count": insight_count,
        }))
    }

    // ── Verification stage helpers ──────────────────────────────────────

    /// Get companies that need verification: enriched/approved/pushed with no verified_v2_at.
    pub fn get_verifiable_companies(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, website_url, country, city, status, \
                    description, category, subcategory, certifications, company_size, \
                    contact_email, contact_name, contact_title, address, \
                    relevance_score, enrichment_quality, attributes_json \
             FROM companies \
             WHERE verified_v2_at IS NULL \
               AND status IN ('enriched', 'approved', 'pushed') \
               AND website_url IS NOT NULL AND website_url != '' \
             ORDER BY created_at ASC \
             LIMIT ?1"
        )?;

        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "website_url": row.get::<_, String>(2)?,
                    "country": row.get::<_, Option<String>>(3)?,
                    "city": row.get::<_, Option<String>>(4)?,
                    "status": row.get::<_, String>(5)?,
                    "description": row.get::<_, Option<String>>(6)?,
                    "category": row.get::<_, Option<String>>(7)?,
                    "subcategory": row.get::<_, Option<String>>(8)?,
                    "certifications": row.get::<_, Option<String>>(9)?,
                    "company_size": row.get::<_, Option<String>>(10)?,
                    "contact_email": row.get::<_, Option<String>>(11)?,
                    "contact_name": row.get::<_, Option<String>>(12)?,
                    "contact_title": row.get::<_, Option<String>>(13)?,
                    "address": row.get::<_, Option<String>>(14)?,
                    "relevance_score": row.get::<_, Option<i64>>(15)?,
                    "enrichment_quality": row.get::<_, Option<i64>>(16)?,
                    "attributes_json": row.get::<_, Option<String>>(17)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Apply verification results to a company.
    /// Uses COALESCE so corrections only overwrite when a value is provided (non-destructive).
    pub fn apply_verification(
        &self,
        id: &str,
        corrections: &Value,
        verification_changes_json: &str,
        fractional_signals_json: &str,
        relevance_score: Option<i64>,
        enrichment_quality: Option<i64>,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             description = COALESCE(?1, description), \
             certifications = COALESCE(?2, certifications), \
             company_size = COALESCE(?3, company_size), \
             contact_email = COALESCE(?4, contact_email), \
             contact_name = COALESCE(?5, contact_name), \
             contact_title = COALESCE(?6, contact_title), \
             address = COALESCE(?7, address), \
             relevance_score = COALESCE(?8, relevance_score), \
             enrichment_quality = COALESCE(?9, enrichment_quality), \
             verification_changes_json = ?10, \
             fractional_signals_json = ?11, \
             verified_v2_at = datetime('now'), \
             updated_at = datetime('now') \
             WHERE id = ?12",
            rusqlite::params![
                corrections.get("description").and_then(|v| v.as_str()),
                corrections.get("certifications").map(|v| v.to_string()),
                corrections.get("company_size").and_then(|v| v.as_str()),
                corrections.get("contact_email").and_then(|v| v.as_str()),
                corrections.get("contact_name").and_then(|v| v.as_str()),
                corrections.get("contact_title").and_then(|v| v.as_str()),
                corrections.get("address").and_then(|v| v.as_str()),
                relevance_score,
                enrichment_quality,
                verification_changes_json,
                fractional_signals_json,
                id,
            ],
        )?;
        Ok(())
    }

    /// Mark a company as verified with no corrections (just sets timestamp).
    pub fn mark_verified(&self, id: &str, verification_changes_json: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             verification_changes_json = ?1, \
             verified_v2_at = datetime('now'), \
             updated_at = datetime('now') \
             WHERE id = ?2",
            rusqlite::params![verification_changes_json, id],
        )?;
        Ok(())
    }

    /// Get companies that need synthesis (verified but not yet synthesized).
    pub fn get_synthesizable_companies(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, domain, website_url, country, city, status, \
                    description, category, subcategory, certifications, company_size, \
                    contact_email, contact_name, contact_title, address, year_founded, \
                    relevance_score, enrichment_quality, \
                    verification_changes_json, fractional_signals_json \
             FROM companies \
             WHERE verified_v2_at IS NOT NULL \
               AND (synthesis_public_json IS NULL OR synthesis_public_json = '') \
               AND status IN ('enriched', 'approved', 'pushed') \
             ORDER BY CASE WHEN status = 'pushed' THEN 0 ELSE 1 END, created_at ASC \
             LIMIT ?1"
        )?;

        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, Option<String>>(1)?,
                    "domain": row.get::<_, Option<String>>(2)?,
                    "website_url": row.get::<_, Option<String>>(3)?,
                    "country": row.get::<_, Option<String>>(4)?,
                    "city": row.get::<_, Option<String>>(5)?,
                    "status": row.get::<_, String>(6)?,
                    "description": row.get::<_, Option<String>>(7)?,
                    "category": row.get::<_, Option<String>>(8)?,
                    "subcategory": row.get::<_, Option<String>>(9)?,
                    "certifications": row.get::<_, Option<String>>(10)?,
                    "company_size": row.get::<_, Option<String>>(11)?,
                    "contact_email": row.get::<_, Option<String>>(12)?,
                    "contact_name": row.get::<_, Option<String>>(13)?,
                    "contact_title": row.get::<_, Option<String>>(14)?,
                    "address": row.get::<_, Option<String>>(15)?,
                    "year_founded": row.get::<_, Option<String>>(16)?,
                    "relevance_score": row.get::<_, Option<i64>>(17)?,
                    "enrichment_quality": row.get::<_, Option<i64>>(18)?,
                    "verification_changes_json": row.get::<_, Option<String>>(19)?,
                    "fractional_signals_json": row.get::<_, Option<String>>(20)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }

    /// Save synthesis results (public + private JSON) for a company.
    pub fn save_synthesis(&self, id: &str, public_json: &str, private_json: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET \
             synthesis_public_json = ?1, \
             synthesis_private_json = ?2, \
             synthesized_v2_at = datetime('now'), \
             updated_at = datetime('now') \
             WHERE id = ?3",
            rusqlite::params![public_json, private_json, id],
        )?;
        Ok(())
    }

    // ── Activity Feed helpers ──────────────────────────────────────────

    /// Save an activity feed item. Deduplicates by URL (INSERT OR IGNORE).
    pub fn save_activity(
        &self,
        company_id: &str,
        title: &str,
        url: &str,
        snippet: Option<&str>,
        activity_type: &str,
        published_at: Option<&str>,
    ) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let rows = conn.execute(
            "INSERT OR IGNORE INTO activity_feed (company_id, title, url, snippet, activity_type, published_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![company_id, title, url, snippet, activity_type, published_at],
        )?;
        Ok(rows > 0)
    }

    /// Get recent activity feed items for a company.
    pub fn get_company_activities(&self, company_id: &str, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, company_id, title, url, snippet, activity_type, published_at, fetched_at \
             FROM activity_feed WHERE company_id = ?1 \
             ORDER BY fetched_at DESC LIMIT ?2",
        )?;
        let rows: Vec<Value> = stmt
            .query_map(rusqlite::params![company_id, limit], |row| {
                Ok(json!({
                    "id": row.get::<_, i64>(0)?,
                    "company_id": row.get::<_, String>(1)?,
                    "title": row.get::<_, String>(2)?,
                    "url": row.get::<_, String>(3)?,
                    "snippet": row.get::<_, Option<String>>(4)?,
                    "activity_type": row.get::<_, String>(5)?,
                    "published_at": row.get::<_, Option<String>>(6)?,
                    "fetched_at": row.get::<_, String>(7)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get companies eligible for activity feed fetch (pushed/approved, limit N).
    pub fn get_activity_eligible_companies(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, name, city, country FROM companies \
             WHERE status IN ('pushed', 'approved') \
             ORDER BY RANDOM() LIMIT ?1",
        )?;
        let rows: Vec<Value> = stmt
            .query_map(rusqlite::params![limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "city": row.get::<_, Option<String>>(2)?,
                    "country": row.get::<_, Option<String>>(3)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    // ── Nightshift Intel (PRIVATE — never push to ForgeOS) ────────────

    /// Upsert director intel for a company. ON CONFLICT(company_id) updates.
    pub fn save_intel(&self, company_id: &str, intel: &Value) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO nightshift_intel (
                company_id, directors_json, director_count,
                avg_director_age, oldest_director_age, youngest_director_age,
                founder_director_name, founder_director_age, founder_director_tenure_years,
                psc_json, psc_count, single_owner, owner_is_director, majority_control_nature,
                no_young_directors, recent_director_changes, years_trading, has_company_secretary,
                accounts_type, last_accounts_date, accounts_overdue,
                has_charges, has_insolvency_history, company_status, sic_codes,
                acquisition_readiness_score, acquisition_signals_json,
                ownership_structure, age_source, ch_fetched_at, estimated_at,
                updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9,
                ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18,
                ?19, ?20, ?21, ?22, ?23, ?24, ?25,
                ?26, ?27, ?28, ?29, ?30, ?31,
                datetime('now')
             )
             ON CONFLICT(company_id) DO UPDATE SET
                directors_json = excluded.directors_json,
                director_count = excluded.director_count,
                avg_director_age = excluded.avg_director_age,
                oldest_director_age = excluded.oldest_director_age,
                youngest_director_age = excluded.youngest_director_age,
                founder_director_name = excluded.founder_director_name,
                founder_director_age = excluded.founder_director_age,
                founder_director_tenure_years = excluded.founder_director_tenure_years,
                psc_json = excluded.psc_json,
                psc_count = excluded.psc_count,
                single_owner = excluded.single_owner,
                owner_is_director = excluded.owner_is_director,
                majority_control_nature = excluded.majority_control_nature,
                no_young_directors = excluded.no_young_directors,
                recent_director_changes = excluded.recent_director_changes,
                years_trading = excluded.years_trading,
                has_company_secretary = excluded.has_company_secretary,
                accounts_type = excluded.accounts_type,
                last_accounts_date = excluded.last_accounts_date,
                accounts_overdue = excluded.accounts_overdue,
                has_charges = excluded.has_charges,
                has_insolvency_history = excluded.has_insolvency_history,
                company_status = excluded.company_status,
                sic_codes = excluded.sic_codes,
                acquisition_readiness_score = excluded.acquisition_readiness_score,
                acquisition_signals_json = excluded.acquisition_signals_json,
                ownership_structure = excluded.ownership_structure,
                age_source = excluded.age_source,
                ch_fetched_at = excluded.ch_fetched_at,
                estimated_at = excluded.estimated_at,
                updated_at = datetime('now')",
            rusqlite::params![
                company_id,
                intel.get("directors_json").and_then(|v| v.as_str()),
                intel.get("director_count").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("avg_director_age").and_then(|v| v.as_f64()),
                intel.get("oldest_director_age").and_then(|v| v.as_i64()),
                intel.get("youngest_director_age").and_then(|v| v.as_i64()),
                intel.get("founder_director_name").and_then(|v| v.as_str()),
                intel.get("founder_director_age").and_then(|v| v.as_i64()),
                intel.get("founder_director_tenure_years").and_then(|v| v.as_i64()),
                intel.get("psc_json").and_then(|v| v.as_str()),
                intel.get("psc_count").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("single_owner").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("owner_is_director").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("majority_control_nature").and_then(|v| v.as_str()),
                intel.get("no_young_directors").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("recent_director_changes").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("years_trading").and_then(|v| v.as_i64()),
                intel.get("has_company_secretary").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("accounts_type").and_then(|v| v.as_str()),
                intel.get("last_accounts_date").and_then(|v| v.as_str()),
                intel.get("accounts_overdue").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("has_charges").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("has_insolvency_history").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("company_status").and_then(|v| v.as_str()),
                intel.get("sic_codes").and_then(|v| v.as_str()),
                intel.get("acquisition_readiness_score").and_then(|v| v.as_i64()).unwrap_or(0),
                intel.get("acquisition_signals_json").and_then(|v| v.as_str()),
                intel.get("ownership_structure").and_then(|v| v.as_str()),
                intel.get("age_source").and_then(|v| v.as_str()).unwrap_or("unknown"),
                intel.get("ch_fetched_at").and_then(|v| v.as_str()),
                intel.get("estimated_at").and_then(|v| v.as_str()),
            ],
        )?;
        Ok(())
    }

    /// Get GB companies with ch_company_number that have not been intel-analysed yet.
    pub fn get_companies_for_intel(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT c.id, c.name, c.country, c.ch_company_number,
                    c.attributes_json, c.description, c.subcategory, c.company_size
             FROM companies c
             LEFT JOIN nightshift_intel ni ON c.id = ni.company_id
             WHERE c.status NOT IN ('discovered', 'error')
               AND c.ch_company_number IS NOT NULL
               AND c.ch_company_number != ''
               AND ni.company_id IS NULL
             ORDER BY c.updated_at DESC
             LIMIT ?1",
        )?;
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "country": row.get::<_, Option<String>>(2)?,
                    "ch_company_number": row.get::<_, Option<String>>(3)?,
                    "attributes_json": row.get::<_, Option<String>>(4)?,
                    "description": row.get::<_, Option<String>>(5)?,
                    "subcategory": row.get::<_, Option<String>>(6)?,
                    "company_size": row.get::<_, Option<String>>(7)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get non-UK companies without intel that have enrichment data (for Haiku estimation).
    pub fn get_non_uk_companies_for_intel(&self, limit: i64) -> Result<Vec<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT c.id, c.name, c.country, c.description, c.subcategory,
                    c.company_size, c.attributes_json, c.contact_name, c.contact_title
             FROM companies c
             LEFT JOIN nightshift_intel ni ON c.id = ni.company_id
             WHERE c.status NOT IN ('discovered', 'error')
               AND (c.country != 'GB' AND c.country != 'UK')
               AND c.description IS NOT NULL AND c.description != ''
               AND ni.company_id IS NULL
             ORDER BY c.updated_at DESC
             LIMIT ?1",
        )?;
        let rows: Vec<Value> = stmt
            .query_map([limit], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "name": row.get::<_, String>(1)?,
                    "country": row.get::<_, Option<String>>(2)?,
                    "description": row.get::<_, Option<String>>(3)?,
                    "subcategory": row.get::<_, Option<String>>(4)?,
                    "company_size": row.get::<_, Option<String>>(5)?,
                    "attributes_json": row.get::<_, Option<String>>(6)?,
                    "contact_name": row.get::<_, Option<String>>(7)?,
                    "contact_title": row.get::<_, Option<String>>(8)?,
                }))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(rows)
    }

    /// Get intel for a specific company.
    pub fn get_intel(&self, company_id: &str) -> Result<Option<Value>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT * FROM nightshift_intel WHERE company_id = ?1",
        )?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let row = stmt
            .query_map([company_id], |row| {
                let mut obj = serde_json::Map::new();
                for (i, col) in columns.iter().enumerate() {
                    let val: rusqlite::types::Value = row.get(i).unwrap_or(rusqlite::types::Value::Null);
                    obj.insert(col.clone(), sqlite_to_json(val));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .next();
        Ok(row)
    }

    /// Get verification data for a specific company.
    pub fn get_company_verification(&self, company_id: &str) -> Result<Value> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT verified_v2_at, verification_changes_json, fractional_signals_json \
             FROM companies WHERE id = ?1",
        )?;

        let row = stmt.query_row([company_id], |row| {
            Ok(json!({
                "verified_v2_at": row.get::<_, Option<String>>(0)?,
                "verification_changes_json": row.get::<_, Option<String>>(1)?,
                "fractional_signals_json": row.get::<_, Option<String>>(2)?,
            }))
        })?;

        Ok(row)
    }
}

/// Normalize a company name for dedup: lowercase, strip common legal suffixes, trim.
fn normalize_company_name(name: &str) -> String {
    let mut n = name.to_lowercase();
    for suffix in &[
        " ltd", " limited", " gmbh", " sas", " bv", " ag", " sa", " srl", " nv", " inc",
        " llc", " co.", " corp", " corporation", " plc", " s.r.l.", " s.a.", " e.k.",
        " ohg", " kg", " ug",
    ] {
        if n.ends_with(suffix) {
            n = n[..n.len() - suffix.len()].to_string();
        }
    }
    n.trim().to_string()
}
