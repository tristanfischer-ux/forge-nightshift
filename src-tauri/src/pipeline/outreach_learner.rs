use anyhow::Result;
use serde_json::{json, Value};
use tauri::Manager;

use crate::db::Database;

/// Run the learning cycle: analyse email outcomes, generate insights, evaluate A/B experiment.
pub async fn run_learning_cycle(
    app: &tauri::AppHandle,
    job_id: &str,
    config: &Value,
) -> Result<Value> {
    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434");
    let outreach_model = config
        .get("outreach_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3.5:27b-q4_K_M");

    // 1. Get email outcomes
    let outcomes = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_email_outcomes_for_learning()?
    };

    if outcomes.len() < 10 {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "learn_outreach",
            "info",
            &format!("Skipping learning — only {} sent emails (need 10+)", outcomes.len()),
        );
        // Still ensure experiment exists
        ensure_experiment_exists(app, job_id)?;
        return Ok(json!({ "skipped": true, "reason": "insufficient_data", "sent_count": outcomes.len() }));
    }

    // 2. Load existing insights
    let existing_insights = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_active_insights(20)?
    };

    // 3. Build analysis prompt
    let prompt = build_analysis_prompt(&outcomes, &existing_insights);

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "learn_outreach",
            "info",
            &format!("Analysing {} email outcomes with {} existing insights", outcomes.len(), existing_insights.len()),
        );
    }

    // 4. Call Ollama for analysis
    let analysis = match crate::services::ollama::generate(
        ollama_url, outreach_model, &prompt, true,
    )
    .await
    {
        Ok(text) => text,
        Err(e) => {
            let db: tauri::State<'_, Database> = app.state();
            let _ = db.log_activity(
                job_id,
                "learn_outreach",
                "error",
                &format!("Ollama analysis failed: {}", e),
            );
            return Ok(json!({ "error": e.to_string() }));
        }
    };

    // 5. Parse insights from JSON response
    let parsed: Value = serde_json::from_str(&analysis).unwrap_or(json!({}));
    let insights = parsed
        .get("insights")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();

    // Get current generation
    let generation = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_active_experiment()?
            .and_then(|e| e.get("generation").and_then(|g| g.as_i64()))
            .unwrap_or(1)
    };

    let mut upserted = 0;
    {
        let db: tauri::State<'_, Database> = app.state();
        for insight in &insights {
            let itype = insight
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("pattern");
            let text = insight.get("text").and_then(|v| v.as_str()).unwrap_or("");
            let confidence = insight
                .get("confidence")
                .and_then(|v| v.as_f64())
                .unwrap_or(0.5);

            if text.is_empty() {
                continue;
            }

            let _ = db.upsert_insight(itype, text, confidence, outcomes.len() as i64, generation);
            upserted += 1;
        }

        let _ = db.log_activity(
            job_id,
            "learn_outreach",
            "info",
            &format!("Upserted {} insights from {} email outcomes", upserted, outcomes.len()),
        );
    }

    // 6. Evaluate A/B experiment
    evaluate_ab_experiment(app, job_id, config).await?;

    Ok(json!({
        "insights_generated": upserted,
        "emails_analysed": outcomes.len(),
        "generation": generation,
    }))
}

/// Ensure an A/B experiment exists. If none, seed the initial one.
fn ensure_experiment_exists(app: &tauri::AppHandle, job_id: &str) -> Result<()> {
    let db: tauri::State<'_, Database> = app.state();
    if db.get_active_experiment()?.is_none() {
        let id = db.create_experiment(
            1,
            "Technical Depth: Focus on specific processes, materials, equipment, certifications. \
             Reference exact capabilities from the company data. Use technical language that \
             shows you understand their craft.",
            "Business Value: Focus on speed, cost savings, risk reduction, and revenue outcomes. \
             Frame everything in terms of business impact rather than technical detail. \
             Emphasise first-mover advantage and startup deal flow.",
        )?;
        let _ = db.log_activity(
            job_id,
            "learn_outreach",
            "info",
            &format!("Seeded initial A/B experiment: {}", id),
        );
    }
    Ok(())
}

/// Evaluate the active A/B experiment and potentially evolve to next generation.
async fn evaluate_ab_experiment(
    app: &tauri::AppHandle,
    job_id: &str,
    config: &Value,
) -> Result<()> {
    let db: tauri::State<'_, Database> = app.state();

    // Update stats first
    db.update_experiment_stats()?;

    let experiment = match db.get_active_experiment()? {
        Some(e) => e,
        None => {
            // Seed initial experiment
            ensure_experiment_exists(app, job_id)?;
            return Ok(());
        }
    };

    let a_sent = experiment.get("variant_a_sent").and_then(|v| v.as_i64()).unwrap_or(0);
    let b_sent = experiment.get("variant_b_sent").and_then(|v| v.as_i64()).unwrap_or(0);

    // Need at least 20 sends per variant before evaluating
    if a_sent < 20 || b_sent < 20 {
        let _ = db.log_activity(
            job_id,
            "learn_outreach",
            "info",
            &format!("A/B experiment needs more data: A={}/{}, B={}/{} (need 20 each)",
                a_sent, 20, b_sent, 20),
        );
        return Ok(());
    }

    let a_opened = experiment.get("variant_a_opened").and_then(|v| v.as_i64()).unwrap_or(0);
    let b_opened = experiment.get("variant_b_opened").and_then(|v| v.as_i64()).unwrap_or(0);

    let a_rate = if a_sent > 0 { a_opened as f64 / a_sent as f64 * 100.0 } else { 0.0 };
    let b_rate = if b_sent > 0 { b_opened as f64 / b_sent as f64 * 100.0 } else { 0.0 };
    let diff = (a_rate - b_rate).abs();

    // Need >5pp difference to declare winner
    if diff <= 5.0 {
        let _ = db.log_activity(
            job_id,
            "learn_outreach",
            "info",
            &format!("A/B experiment too close to call: A={:.1}% vs B={:.1}% (diff {:.1}pp, need >5pp)",
                a_rate, b_rate, diff),
        );
        return Ok(());
    }

    let (winner, winner_strategy) = if a_rate > b_rate {
        ("A", experiment.get("variant_a_strategy").and_then(|v| v.as_str()).unwrap_or(""))
    } else {
        ("B", experiment.get("variant_b_strategy").and_then(|v| v.as_str()).unwrap_or(""))
    };

    let exp_id = experiment.get("id").and_then(|v| v.as_str()).unwrap_or("");
    let generation = experiment.get("generation").and_then(|v| v.as_i64()).unwrap_or(1);

    // Complete current experiment
    db.complete_experiment(exp_id, winner)?;

    let _ = db.log_activity(
        job_id,
        "learn_outreach",
        "info",
        &format!("Generation {} winner: Variant {} ({:.1}% vs {:.1}%)",
            generation, winner, a_rate, b_rate),
    );

    // Generate new challenger strategy via Ollama
    let ollama_url = config
        .get("ollama_url")
        .and_then(|v| v.as_str())
        .unwrap_or("http://localhost:11434");
    let outreach_model = config
        .get("outreach_model")
        .and_then(|v| v.as_str())
        .unwrap_or("qwen3.5:27b-q4_K_M");

    let challenger_prompt = format!(
        r#"You are an email strategy analyst. The winning outreach strategy had a {:.1}% open rate:

WINNING STRATEGY: "{}"

The losing strategy had a {:.1}% open rate.

Generate a NEW challenger strategy that builds on the winner's strengths but tries a different angle. The strategy should be a 2-3 sentence description of tone, focus areas, and approach.

Return JSON: {{"strategy": "your new strategy description"}}"#,
        if winner == "A" { a_rate } else { b_rate },
        winner_strategy,
        if winner == "A" { b_rate } else { a_rate },
    );

    let challenger = match crate::services::ollama::generate(
        ollama_url, outreach_model, &challenger_prompt, true,
    ).await {
        Ok(text) => {
            let parsed: Value = serde_json::from_str(&text).unwrap_or(json!({}));
            parsed
                .get("strategy")
                .and_then(|v| v.as_str())
                .unwrap_or("Balanced approach: mix technical specifics with clear business outcomes")
                .to_string()
        }
        Err(_) => {
            "Balanced approach: combine technical credibility with clear business outcomes. \
             Lead with a specific capability, then immediately connect it to a startup use case."
                .to_string()
        }
    };

    // Create new experiment
    let new_gen = generation + 1;
    let new_id = db.create_experiment(new_gen, winner_strategy, &challenger)?;

    let _ = db.log_activity(
        job_id,
        "learn_outreach",
        "info",
        &format!("Started Generation {} experiment: {} — winner vs new challenger", new_gen, new_id),
    );

    Ok(())
}

/// Build the analysis prompt for Ollama.
fn build_analysis_prompt(outcomes: &[Value], existing_insights: &[Value]) -> String {
    // Aggregate outcomes by strategy variant
    let mut a_count = 0i64;
    let mut a_opened = 0i64;
    let mut b_count = 0i64;
    let mut b_opened = 0i64;
    let mut by_subcategory: std::collections::HashMap<String, (i64, i64)> = std::collections::HashMap::new();
    let mut by_size: std::collections::HashMap<String, (i64, i64)> = std::collections::HashMap::new();

    for outcome in outcomes {
        let variant = outcome.get("ab_variant").and_then(|v| v.as_str()).unwrap_or("none");
        let opened = matches!(
            outcome.get("status").and_then(|v| v.as_str()),
            Some("opened") | Some("replied")
        );

        match variant {
            "A" => { a_count += 1; if opened { a_opened += 1; } }
            "B" => { b_count += 1; if opened { b_opened += 1; } }
            _ => {}
        }

        let sub = outcome
            .get("subcategory")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let entry = by_subcategory.entry(sub).or_insert((0, 0));
        entry.0 += 1;
        if opened { entry.1 += 1; }

        let size = outcome
            .get("company_size")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown")
            .to_string();
        let entry = by_size.entry(size).or_insert((0, 0));
        entry.0 += 1;
        if opened { entry.1 += 1; }
    }

    let mut subcategory_lines = String::new();
    for (sub, (sent, opened)) in &by_subcategory {
        let rate = if *sent > 0 { *opened as f64 / *sent as f64 * 100.0 } else { 0.0 };
        subcategory_lines.push_str(&format!("  - {}: {}/{} sent, {:.0}% open rate\n", sub, opened, sent, rate));
    }

    let mut size_lines = String::new();
    for (size, (sent, opened)) in &by_size {
        let rate = if *sent > 0 { *opened as f64 / *sent as f64 * 100.0 } else { 0.0 };
        size_lines.push_str(&format!("  - {}: {}/{} sent, {:.0}% open rate\n", size, opened, sent, rate));
    }

    let existing_text = if existing_insights.is_empty() {
        "None yet — this is the first analysis.".to_string()
    } else {
        existing_insights
            .iter()
            .enumerate()
            .map(|(i, insight)| {
                format!(
                    "{}. [{}] {} (confidence: {:.0}%)",
                    i + 1,
                    insight.get("insight_type").and_then(|v| v.as_str()).unwrap_or("?"),
                    insight.get("insight").and_then(|v| v.as_str()).unwrap_or(""),
                    insight.get("confidence").and_then(|v| v.as_f64()).unwrap_or(0.0) * 100.0,
                )
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    format!(
        r#"You are analysing outreach email performance for a UK manufacturing marketplace. Your job is to identify what works and what doesn't, so future emails can be improved.

OUTCOME DATA ({total} emails sent):

Strategy A: {a_count} sent, {a_opened} opened ({a_rate:.1}%)
Strategy B: {b_count} sent, {b_opened} opened ({b_rate:.1}%)

By company subcategory:
{subcategory_lines}
By company size:
{size_lines}

EXISTING INSIGHTS:
{existing_text}

Analyse the data and return JSON with insights. Each insight should be actionable — something that can directly improve the next batch of emails.

Types:
- "pattern": something that works (e.g. "emails to CNC machining companies have 2x higher open rate")
- "anti_pattern": something that doesn't work (e.g. "emails to large companies rarely get opened")
- "style_rule": a writing guideline (e.g. "shorter subject lines correlate with higher opens")

Return JSON:
{{
  "insights": [
    {{ "type": "pattern|anti_pattern|style_rule", "text": "clear actionable insight", "confidence": 0.0-1.0 }}
  ]
}}"#,
        total = outcomes.len(),
        a_count = a_count,
        a_opened = a_opened,
        a_rate = if a_count > 0 { a_opened as f64 / a_count as f64 * 100.0 } else { 0.0 },
        b_count = b_count,
        b_opened = b_opened,
        b_rate = if b_count > 0 { b_opened as f64 / b_count as f64 * 100.0 } else { 0.0 },
        subcategory_lines = subcategory_lines,
        size_lines = size_lines,
        existing_text = existing_text,
    )
}
