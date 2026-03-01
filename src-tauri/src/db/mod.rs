use anyhow::Result;
use rusqlite::Connection;
use serde_json::{json, Value};
use std::sync::Mutex;

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
        // 003: additive ALTER TABLE — ignore "duplicate column" errors on re-run
        for stmt in include_str!("migrations/003_translation_fields.sql").split(';') {
            let stmt = stmt.trim();
            if !stmt.is_empty() {
                let _ = conn.execute_batch(stmt);
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
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    obj.insert(col.clone(), val.map(|v| json!(v)).unwrap_or(json!(null)));
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
                let val: Option<String> = row.get(i).unwrap_or(None);
                obj.insert(col.clone(), val.map(|v| json!(v)).unwrap_or(json!(null)));
            }
            Ok(Value::Object(obj))
        })?;

        Ok(row)
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
             description_original = ?13, snippet_english = ?14, last_error = NULL, \
             status = 'enriched', updated_at = datetime('now') \
             WHERE id = ?15",
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
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO companies (id, name, website_url, domain, country, city, source, source_url, source_query, raw_snippet, status) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, 'discovered')",
            rusqlite::params![
                id,
                company.get("name").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("website_url").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("domain").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("country").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("city").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("source").and_then(|v| v.as_str()).unwrap_or("brave"),
                company.get("source_url").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("source_query").and_then(|v| v.as_str()).unwrap_or(""),
                company.get("raw_snippet").and_then(|v| v.as_str()).unwrap_or(""),
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
            "SELECT COUNT(*) FROM search_history WHERE query = ?1",
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

    /// Get companies by list of IDs
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
                    let val: Option<String> = row.get(i).unwrap_or(None);
                    obj.insert(col.clone(), val.map(|v| json!(v)).unwrap_or(json!(null)));
                }
                Ok(Value::Object(obj))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(rows)
    }
}
