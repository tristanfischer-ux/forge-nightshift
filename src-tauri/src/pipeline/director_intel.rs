//! Phase 5: Director Analysis & Acquisition Scoring
//!
//! PRIVACY: nightshift_intel data is NEVER pushed to ForgeOS.
//! Director ages, acquisition scores, ownership structure are PRIVATE M&A intelligence.

use anyhow::Result;
use chrono::{Datelike, NaiveDate, Utc};
use serde_json::{json, Value};
use tauri::Manager;

use crate::db::Database;
use crate::services::companies_house::{self, Officer, PSC};

/// Run director intel analysis for UK companies (via CH API) and non-UK companies (via Haiku).
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let ch_api_key = config
        .get("companies_house_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let anthropic_api_key = config
        .get("anthropic_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Phase 1: UK companies with CH data
    let uk_companies = {
        let db: tauri::State<'_, Database> = app.state();
        if ch_api_key.is_empty() {
            let _ = db.log_activity(job_id, "director_intel", "warn", "No CH API key — skipping UK director analysis");
            vec![]
        } else {
            db.get_companies_for_intel(500)?
        }
    };

    // Phase 2: Non-UK companies for Haiku estimation
    let non_uk_companies = {
        let db: tauri::State<'_, Database> = app.state();
        if anthropic_api_key.is_empty() {
            let _ = db.log_activity(job_id, "director_intel", "warn", "No Anthropic API key — skipping non-UK director estimation");
            vec![]
        } else {
            db.get_non_uk_companies_for_intel(200)?
        }
    };

    let total = (uk_companies.len() + non_uk_companies.len()) as i64;

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "director_intel",
            "info",
            &format!("Starting director intel: {} UK + {} non-UK companies", uk_companies.len(), non_uk_companies.len()),
        );
    }

    let started_at = Utc::now();

    super::emit_node(app, json!({
        "node_id": "director_intel",
        "status": "running",
        "progress": { "current": 0, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": 0
    }));

    let mut analysed: i64 = 0;
    let mut estimated: i64 = 0;
    let mut errors: i64 = 0;
    let mut high_score_count: i64 = 0;

    // ── UK Companies: Full CH director analysis ──

    for company in &uk_companies {
        if super::is_cancelled() {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(job_id, "director_intel", "warn", "Director intel cancelled");
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let ch_number = company.get("ch_company_number").and_then(|v| v.as_str()).unwrap_or("");
        let attrs_str = company.get("attributes_json").and_then(|v| v.as_str()).unwrap_or("{}");
        let subcategory = company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("");
        let company_size = company.get("company_size").and_then(|v| v.as_str()).unwrap_or("");

        if ch_number.is_empty() {
            continue;
        }

        match analyse_uk_company(&ch_api_key, ch_number, name, attrs_str, subcategory, company_size).await {
            Ok(intel) => {
                let score = intel.get("acquisition_readiness_score")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0);

                let db: tauri::State<'_, Database> = app.state();
                db.save_intel(id, &intel)?;
                analysed += 1;

                if score >= 60 {
                    high_score_count += 1;
                    let _ = db.log_activity(
                        job_id,
                        "director_intel",
                        "info",
                        &format!("HIGH SCORE {}: {} (score: {})", ch_number, name, score),
                    );
                }
            }
            Err(e) => {
                errors += 1;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "director_intel",
                    "warn",
                    &format!("Intel failed for {} ({}): {}", name, ch_number, e),
                );
            }
        }

        let current = analysed + errors;
        let elapsed = (Utc::now() - started_at).num_seconds();
        let rate = if elapsed > 0 {
            current as f64 / elapsed as f64 * 3600.0
        } else {
            0.0
        };

        super::emit_node(app, json!({
            "node_id": "director_intel",
            "status": "running",
            "progress": { "current": current, "total": total, "rate": rate, "current_item": name },
            "started_at": started_at.to_rfc3339(),
            "elapsed_secs": elapsed
        }));
    }

    // ── Non-UK Companies: Haiku estimation ──

    for company in &non_uk_companies {
        if super::is_cancelled() {
            break;
        }

        let id = company.get("id").and_then(|v| v.as_str()).unwrap_or("");
        let name = company.get("name").and_then(|v| v.as_str()).unwrap_or("");
        let description = company.get("description").and_then(|v| v.as_str()).unwrap_or("");
        let subcategory = company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("");
        let company_size = company.get("company_size").and_then(|v| v.as_str()).unwrap_or("");
        let attrs_str = company.get("attributes_json").and_then(|v| v.as_str()).unwrap_or("{}");
        let contact_name = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("");
        let contact_title = company.get("contact_title").and_then(|v| v.as_str()).unwrap_or("");

        match estimate_non_uk_intel(
            &anthropic_api_key, name, description, subcategory,
            company_size, attrs_str, contact_name, contact_title,
        ).await {
            Ok(intel) => {
                let db: tauri::State<'_, Database> = app.state();
                db.save_intel(id, &intel)?;
                estimated += 1;
            }
            Err(e) => {
                errors += 1;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "director_intel",
                    "warn",
                    &format!("Haiku estimation failed for {}: {}", name, e),
                );
            }
        }

        let current = analysed + estimated + errors;
        let elapsed = (Utc::now() - started_at).num_seconds();
        super::emit_node(app, json!({
            "node_id": "director_intel",
            "status": "running",
            "progress": { "current": current, "total": total, "rate": null, "current_item": name },
            "started_at": started_at.to_rfc3339(),
            "elapsed_secs": elapsed
        }));
    }

    let elapsed = (Utc::now() - started_at).num_seconds();

    super::emit_node(app, json!({
        "node_id": "director_intel",
        "status": "completed",
        "progress": { "current": analysed + estimated + errors, "total": total, "rate": null, "current_item": null },
        "started_at": started_at.to_rfc3339(),
        "elapsed_secs": elapsed
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "director_intel",
            "info",
            &format!(
                "Director intel complete: {} UK analysed, {} non-UK estimated, {} errors, {} high-score targets",
                analysed, estimated, errors, high_score_count
            ),
        );
    }

    Ok(json!({
        "total": total,
        "analysed": analysed,
        "estimated": estimated,
        "errors": errors,
        "high_score_targets": high_score_count,
    }))
}

/// Analyse a UK company using Companies House officers + PSC API.
async fn analyse_uk_company(
    ch_api_key: &str,
    company_number: &str,
    _company_name: &str,
    _attrs_str: &str,
    subcategory: &str,
    company_size: &str,
) -> Result<Value> {
    // Fetch officers (includes resigned + active, with DOB)
    let all_officers = companies_house::get_officers(ch_api_key, company_number).await?;

    // Fetch PSC
    let pscs = companies_house::get_psc(ch_api_key, company_number).await?;

    // Get company profile for financial data
    let profile = companies_house::get_company(ch_api_key, company_number).await?;

    let creation_date = profile.date_of_creation.as_deref().unwrap_or("");

    // Split into active and resigned
    let active_officers: Vec<&Officer> = all_officers.iter()
        .filter(|o| o.resigned_on.is_none())
        .collect();

    let active_directors: Vec<&Officer> = active_officers.iter()
        .filter(|o| {
            let role = o.officer_role.as_deref().unwrap_or("").to_lowercase();
            role == "director" || role == "member"
        })
        .copied()
        .collect();

    if active_directors.is_empty() {
        anyhow::bail!("No active directors found for {}", company_number);
    }

    let now = Utc::now();
    let current_year = now.year();
    let current_month = now.month();

    // Calculate ages from DOB
    let ages: Vec<i32> = active_directors.iter()
        .filter_map(|d| {
            let dob = d.date_of_birth.as_ref()?;
            let year = dob.year?;
            let month = dob.month.unwrap_or(1);
            let mut age = current_year - year;
            if current_month < month {
                age -= 1;
            }
            Some(age)
        })
        .collect();

    let oldest_age = ages.iter().max().copied();
    let youngest_age = ages.iter().min().copied();
    let avg_age: Option<f64> = if !ages.is_empty() {
        Some(ages.iter().sum::<i32>() as f64 / ages.len() as f64)
    } else {
        None
    };

    // Calculate tenures
    let _tenures: Vec<(String, Option<i32>, i64)> = active_directors.iter()
        .map(|d| {
            let tenure = d.appointed_on.as_deref()
                .and_then(|a| NaiveDate::parse_from_str(a, "%Y-%m-%d").ok())
                .map(|appointed| {
                    let today = now.date_naive();
                    ((today - appointed).num_days() / 365) as i64
                })
                .unwrap_or(0);

            let age = d.date_of_birth.as_ref().and_then(|dob| {
                let year = dob.year?;
                let month = dob.month.unwrap_or(1);
                let mut a = current_year - year;
                if current_month < month { a -= 1; }
                Some(a)
            });

            (d.name.clone(), age, tenure)
        })
        .collect();

    // Find founder-director (appointed within 2 years of incorporation)
    let founder = if !creation_date.is_empty() {
        let created = NaiveDate::parse_from_str(creation_date, "%Y-%m-%d").ok();
        created.and_then(|created_dt| {
            active_directors.iter().find_map(|d| {
                let appointed = d.appointed_on.as_deref()
                    .and_then(|a| NaiveDate::parse_from_str(a, "%Y-%m-%d").ok())?;
                let years_between = (appointed - created_dt).num_days() as f64 / 365.25;
                if (0.0..=2.0).contains(&years_between) {
                    let age = d.date_of_birth.as_ref().and_then(|dob| {
                        let year = dob.year?;
                        let month = dob.month.unwrap_or(1);
                        let mut a = current_year - year;
                        if current_month < month { a -= 1; }
                        Some(a)
                    });
                    let tenure = ((now.date_naive() - appointed).num_days() / 365) as i64;
                    Some((d.name.clone(), age, tenure))
                } else {
                    None
                }
            })
        })
    } else {
        None
    };

    // Check for secretary
    let has_secretary = active_officers.iter()
        .any(|o| o.officer_role.as_deref().unwrap_or("").to_lowercase() == "secretary");

    // No young directors
    let no_young_directors = if ages.is_empty() { true } else { !ages.iter().any(|&a| a < 45) };

    // Recent director changes (appointments or resignations in last 2 years)
    let two_years_ago = (now - chrono::Duration::days(730)).format("%Y-%m-%d").to_string();
    let recent_changes = all_officers.iter().any(|o| {
        let role = o.officer_role.as_deref().unwrap_or("").to_lowercase();
        if role != "director" && role != "member" { return false; }
        // Recently appointed
        if let Some(a) = o.appointed_on.as_deref() {
            if a > two_years_ago.as_str() { return true; }
        }
        // Recently resigned
        if let Some(r) = o.resigned_on.as_deref() {
            if r > two_years_ago.as_str() { return true; }
        }
        false
    });

    // Years trading
    let years_trading = if !creation_date.is_empty() {
        NaiveDate::parse_from_str(creation_date, "%Y-%m-%d").ok().map(|created| {
            ((now.date_naive() - created).num_days() / 365) as i64
        })
    } else {
        None
    };

    // Financial signals from profile
    let accounts_type = profile.accounts_type.as_deref().unwrap_or("");
    let last_accounts_date = profile.last_accounts_date.as_deref().unwrap_or("");
    let has_charges = profile.has_charges.unwrap_or(false);
    let has_insolvency = profile.has_insolvency_history.unwrap_or(false);
    let company_status = profile.company_status.as_deref().unwrap_or("");
    let sic_codes = serde_json::to_string(&profile.sic_codes.unwrap_or_default()).unwrap_or_default();

    // Accounts overdue check
    let accounts_overdue = if !last_accounts_date.is_empty() {
        NaiveDate::parse_from_str(last_accounts_date, "%Y-%m-%d").ok()
            .map(|d| (now.date_naive() - d).num_days() > 365)
            .unwrap_or(false)
    } else {
        false
    };

    // Ownership analysis
    let single_owner = pscs.len() == 1;
    let owner_is_director = if single_owner {
        let psc_name = pscs[0].name.as_deref().unwrap_or("").to_uppercase();
        active_directors.iter().any(|d| {
            name_similarity(&d.name.to_uppercase(), &psc_name) > 0.8
        })
    } else {
        false
    };

    let majority_control = pscs.first().and_then(|psc| {
        let natures = psc.natures_of_control.as_deref().unwrap_or(&[]);
        if natures.iter().any(|n| n.contains("75-to-100-percent")) {
            Some("75-100% ownership")
        } else if natures.iter().any(|n| n.contains("50-to-75-percent")) {
            Some("50-75% ownership")
        } else if natures.iter().any(|n| n.contains("voting-rights-75-to-100")) {
            Some("75-100% voting rights")
        } else {
            None
        }
    });

    let ownership_structure = determine_ownership_structure(&pscs, &active_directors);

    // ── Calculate acquisition readiness score ──
    let (score, signals) = calculate_acquisition_score(
        &ages,
        oldest_age,
        youngest_age,
        &founder,
        single_owner,
        owner_is_director,
        recent_changes,
        years_trading,
        has_charges,
        has_insolvency,
        accounts_overdue,
        accounts_type,
        company_size,
        subcategory,
        &active_directors,
    );

    // Build directors JSON
    let directors_json: Vec<Value> = active_directors.iter().map(|d| {
        let age = d.date_of_birth.as_ref().and_then(|dob| {
            let year = dob.year?;
            let month = dob.month.unwrap_or(1);
            let mut a = current_year - year;
            if current_month < month { a -= 1; }
            Some(a)
        });
        let tenure = d.appointed_on.as_deref()
            .and_then(|a| NaiveDate::parse_from_str(a, "%Y-%m-%d").ok())
            .map(|appointed| ((now.date_naive() - appointed).num_days() / 365) as i64);

        json!({
            "name": d.name,
            "role": d.officer_role,
            "appointed_on": d.appointed_on,
            "age": age,
            "tenure_years": tenure,
            "nationality": d.nationality,
            "occupation": d.occupation,
            "is_active": true,
        })
    }).collect();

    let psc_json: Vec<Value> = pscs.iter().map(|p| {
        json!({
            "name": p.name,
            "natures_of_control": p.natures_of_control,
            "notified_on": p.notified_on,
            "kind": p.kind,
        })
    }).collect();

    Ok(json!({
        "directors_json": serde_json::to_string(&directors_json).unwrap_or_default(),
        "director_count": active_directors.len(),
        "avg_director_age": avg_age,
        "oldest_director_age": oldest_age,
        "youngest_director_age": youngest_age,
        "founder_director_name": founder.as_ref().map(|f| f.0.as_str()),
        "founder_director_age": founder.as_ref().and_then(|f| f.1),
        "founder_director_tenure_years": founder.as_ref().map(|f| f.2),
        "psc_json": serde_json::to_string(&psc_json).unwrap_or_default(),
        "psc_count": pscs.len(),
        "single_owner": if single_owner { 1 } else { 0 },
        "owner_is_director": if owner_is_director { 1 } else { 0 },
        "majority_control_nature": majority_control,
        "no_young_directors": if no_young_directors { 1 } else { 0 },
        "recent_director_changes": if recent_changes { 1 } else { 0 },
        "years_trading": years_trading,
        "has_company_secretary": if has_secretary { 1 } else { 0 },
        "accounts_type": accounts_type,
        "last_accounts_date": last_accounts_date,
        "accounts_overdue": if accounts_overdue { 1 } else { 0 },
        "has_charges": if has_charges { 1 } else { 0 },
        "has_insolvency_history": if has_insolvency { 1 } else { 0 },
        "company_status": company_status,
        "sic_codes": sic_codes,
        "acquisition_readiness_score": score,
        "acquisition_signals_json": serde_json::to_string(&signals).unwrap_or_default(),
        "ownership_structure": ownership_structure,
        "age_source": "companies_house_api",
        "ch_fetched_at": Utc::now().to_rfc3339(),
    }))
}

/// Calculate acquisition readiness score (0-100).
fn calculate_acquisition_score(
    _ages: &[i32],
    _oldest_age: Option<i32>,
    youngest_age: Option<i32>,
    founder: &Option<(String, Option<i32>, i64)>,
    single_owner: bool,
    owner_is_director: bool,
    recent_changes: bool,
    years_trading: Option<i64>,
    has_charges: bool,
    has_insolvency: bool,
    accounts_overdue: bool,
    accounts_type: &str,
    company_size: &str,
    subcategory: &str,
    active_directors: &[&Officer],
) -> (i64, Vec<String>) {
    let mut score: i64 = 0;
    let mut signals: Vec<String> = Vec::new();

    // Founder age >55 → +20 points
    if let Some((ref name, Some(age), tenure)) = founder {
        if *age > 55 {
            score += 20;
            signals.push(format!("Founder-director {} aged {}, tenure {}y", name, age, tenure));
        }
        if *age > 65 {
            score += 5;
            signals.push("Founder-director over 65".to_string());
        }
    }

    // Single director/shareholder → +15 points
    if single_owner && active_directors.len() <= 2 {
        score += 15;
        signals.push("Single owner with few directors (concentrated control)".to_string());
    }

    // Long tenure (>15 years) → +10 points
    if let Some((_, _, tenure)) = founder {
        if *tenure > 15 {
            score += 10;
            signals.push(format!("Founder tenure {}+ years", tenure));
        }
    }

    // No succession plan → +15 points
    if active_directors.len() == 1 && youngest_age.map_or(true, |a| a > 45) {
        score += 15;
        signals.push("Single director, no young directors (no succession plan)".to_string());
    } else if youngest_age.map_or(false, |a| a > 50) {
        score += 10;
        signals.push(format!("No director under 50 (youngest: {})", youngest_age.unwrap_or(0)));
    } else if youngest_age.map_or(false, |a| a > 45) {
        score += 5;
        signals.push(format!("No director under 45 (youngest: {})", youngest_age.unwrap_or(0)));
    }

    // Recent director changes → +10 points (succession in progress)
    if !recent_changes {
        score += 10;
        signals.push("No director changes in 2+ years (stable but static)".to_string());
    }

    // Small company → +10 points
    let is_small = matches!(company_size, "1-10" | "11-50" | "Small" | "Micro" | "");
    if is_small {
        score += 10;
        signals.push("Small company (easier acquisition target)".to_string());
    }

    // Niche/specialist subcategory → +10 points
    if !subcategory.is_empty() {
        score += 10;
        signals.push(format!("Specialist: {}", subcategory));
    }

    // Profitable signals → +10 points
    if !has_insolvency && !has_charges && !accounts_overdue {
        score += 10;
        signals.push("Clean financials (no insolvency, no charges, accounts current)".to_string());
    } else {
        if has_insolvency {
            score -= 10;
            signals.push("Has insolvency history (RISK)".to_string());
        }
        if accounts_overdue {
            score -= 5;
            signals.push("Accounts overdue".to_string());
        }
    }

    // Company age signals
    if let Some(years) = years_trading {
        if years > 25 {
            score += 5;
            signals.push(format!("Trading {}+ years (established)", years));
        }
    }

    // Owner-operator model bonus
    if owner_is_director {
        score += 5;
        signals.push("Owner is also a director (owner-operator model)".to_string());
    }

    // Micro-entity accounts
    if matches!(accounts_type, "micro-entity" | "small" | "dormant") {
        score += 5;
        signals.push(format!("Files {} accounts (lower complexity)", accounts_type));
    }

    // Cap at 0-100
    score = score.clamp(0, 100);

    (score, signals)
}

/// Determine ownership structure label from PSC and director data.
fn determine_ownership_structure(pscs: &[PSC], directors: &[&Officer]) -> String {
    if pscs.is_empty() {
        return "unknown".to_string();
    }

    // Check for corporate PSC (not an individual)
    let has_corporate = pscs.iter().any(|p| {
        p.kind.as_deref().map_or(false, |k| k.contains("corporate"))
    });

    if has_corporate {
        return "corporate_owned".to_string();
    }

    if pscs.len() == 1 {
        let psc_name = pscs[0].name.as_deref().unwrap_or("").to_uppercase();
        let owner_is_director = directors.iter().any(|d| {
            name_similarity(&d.name.to_uppercase(), &psc_name) > 0.8
        });

        if owner_is_director {
            return "owner_operator".to_string();
        }
        return "single_owner".to_string();
    }

    // Check for family business (multiple PSCs with same surname)
    let surnames: Vec<String> = pscs.iter()
        .filter_map(|p| {
            p.name.as_deref().and_then(|n| n.split(',').next()).map(|s| s.trim().to_uppercase())
        })
        .collect();

    if surnames.len() >= 2 {
        let first = &surnames[0];
        let is_family = surnames.iter().skip(1).any(|s| s == first);
        if is_family {
            return "family_business".to_string();
        }
    }

    if pscs.len() <= 3 {
        "partnership".to_string()
    } else {
        "distributed".to_string()
    }
}

/// Simple name similarity (sequence matcher approximation).
fn name_similarity(a: &str, b: &str) -> f64 {
    if a == b {
        return 1.0;
    }
    let a_chars: Vec<char> = a.chars().collect();
    let b_chars: Vec<char> = b.chars().collect();
    let total = a_chars.len() + b_chars.len();
    if total == 0 {
        return 0.0;
    }

    // Simple LCS-based similarity
    let mut matches = 0;
    let mut b_used = vec![false; b_chars.len()];
    for ac in &a_chars {
        for (j, bc) in b_chars.iter().enumerate() {
            if !b_used[j] && ac == bc {
                matches += 1;
                b_used[j] = true;
                break;
            }
        }
    }

    (2.0 * matches as f64) / total as f64
}

/// Estimate director info for non-UK companies using Haiku.
async fn estimate_non_uk_intel(
    api_key: &str,
    company_name: &str,
    description: &str,
    subcategory: &str,
    company_size: &str,
    attrs_str: &str,
    contact_name: &str,
    contact_title: &str,
) -> Result<Value> {
    // Parse attributes for team/people data
    let attrs: Value = serde_json::from_str(attrs_str).unwrap_or(json!({}));
    let team_info = attrs.get("team").or(attrs.get("people")).cloned().unwrap_or(json!(null));

    let prompt = format!(
        r#"Analyse this manufacturing company for acquisition readiness. Estimate director/founder ages from the available data.

Company: {company_name}
Description: {description}
Subcategory: {subcategory}
Company size: {company_size}
Contact: {contact_name} ({contact_title})
Team data: {team_data}

Based on available signals, provide a JSON response:
{{
  "estimated_founder_age": <number or null>,
  "estimated_director_count": <number>,
  "company_age_years": <number or null>,
  "ownership_structure": "<owner_operator|family_business|partnership|corporate_owned|unknown>",
  "acquisition_score": <0-100>,
  "signals": ["<signal 1>", "<signal 2>", ...],
  "confidence": "<low|medium|high>"
}}

Scoring guide (0-100):
- Founder age >55: +20
- Small company: +10
- Specialist/niche: +10
- Owner-operator: +15
- Long-established (>15y): +10
- No obvious succession: +15
- Clean description (no distress signals): +10

Return ONLY valid JSON."#,
        team_data = team_info.to_string(),
    );

    let response = crate::services::anthropic::chat(api_key, None, &prompt, true).await?;

    // Parse the Haiku response
    let parsed: Value = serde_json::from_str(&response)
        .map_err(|e| anyhow::anyhow!("Failed to parse Haiku response: {} — raw: {}", e, &response[..200.min(response.len())]))?;

    let score = parsed.get("acquisition_score").and_then(|v| v.as_i64()).unwrap_or(0);
    let signals = parsed.get("signals").cloned().unwrap_or(json!([]));
    let founder_age = parsed.get("estimated_founder_age").and_then(|v| v.as_i64());
    let director_count = parsed.get("estimated_director_count").and_then(|v| v.as_i64()).unwrap_or(1);
    let company_age = parsed.get("company_age_years").and_then(|v| v.as_i64());
    let ownership = parsed.get("ownership_structure").and_then(|v| v.as_str()).unwrap_or("unknown");

    Ok(json!({
        "directors_json": json!([{
            "name": contact_name,
            "role": contact_title,
            "estimated_age": founder_age,
            "source": "haiku_estimation",
        }]).to_string(),
        "director_count": director_count,
        "avg_director_age": founder_age,
        "oldest_director_age": founder_age,
        "youngest_director_age": founder_age,
        "founder_director_name": if !contact_name.is_empty() { Some(contact_name) } else { None },
        "founder_director_age": founder_age,
        "founder_director_tenure_years": company_age,
        "psc_json": null,
        "psc_count": 0,
        "single_owner": 0,
        "owner_is_director": 0,
        "majority_control_nature": null,
        "no_young_directors": 0,
        "recent_director_changes": 0,
        "years_trading": company_age,
        "has_company_secretary": 0,
        "accounts_type": null,
        "last_accounts_date": null,
        "accounts_overdue": 0,
        "has_charges": 0,
        "has_insolvency_history": 0,
        "company_status": null,
        "sic_codes": null,
        "acquisition_readiness_score": score,
        "acquisition_signals_json": signals.to_string(),
        "ownership_structure": ownership,
        "age_source": "haiku_estimation",
        "ch_fetched_at": null,
        "estimated_at": Utc::now().to_rfc3339(),
    }))
}
