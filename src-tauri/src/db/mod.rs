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

    /// Approve all enriched companies (set status from 'enriched' to 'approved')
    pub fn approve_all_enriched(&self) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE companies SET status = 'approved', updated_at = datetime('now') WHERE status = 'enriched'",
            [],
        )?;
        Ok(conn.changes() as i64)
    }

    /// Get companies by list of IDs
    #[allow(dead_code)]
    pub fn get_companies_by_ids(&self, ids: &[String]) -> Result<Vec<Value>> {
        if ids.is_empty() {
            return Ok(vec![]);
        }
        let conn = self.conn.lock().unwrap();
        let placeholders: Vec<String> = ids.iter().enumerate().map(|(i, _)| format!("?{}", i + 1)).collect();
        let query = format!(
            "SELECT * FROM companies WHERE id IN ({}) ORDER BY created_at DESC",
            placeholders.join(",")
        );
        let mut stmt = conn.prepare(&query)?;
        let columns: Vec<String> = stmt.column_names().iter().map(|c| c.to_string()).collect();
        let params: Vec<&dyn rusqlite::types::ToSql> = ids.iter().map(|id| id as &dyn rusqlite::types::ToSql).collect();

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
