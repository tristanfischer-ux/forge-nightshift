use anyhow::Result;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use tauri::Manager;
use tokio::task::JoinSet;

use crate::db::Database;
use crate::services;

const BATCH_LIMIT: i64 = 300;
/// How many companies to process concurrently per contacts run.
/// Each worker does up to 6 HTTP fetches + 1 LLM call. Anthropic Haiku Tier 1
/// gives 50 RPM headroom, so 8 parallel × ~3 LLM calls/min ≈ 24 RPM — well under.
const PARALLEL_COMPANIES: usize = 8;

/// Subpages likely to contain team / leadership info.
const TEAM_PATHS: &[&str] = &[
    "/team", "/about-us", "/about", "/leadership", "/management",
    "/our-people", "/our-team", "/people", "/staff", "/who-we-are",
    "/meet-the-team", "/directors", "/board",
    // English additions
    "/executives", "/our-leadership", "/the-team", "/founders", "/our-story",
    // French (Canadian)
    "/equipe", "/notre-equipe", "/a-propos", "/direction",
    // Spanish (LatAm)
    "/equipo", "/nuestro-equipo", "/nosotros",
    // Arabic-transliterated common slugs
    "/about-company", "/our-company",
    // German
    "/uber-uns",
];

/// Subpages likely to contain direct contact details (email, phone, sales contacts).
/// FIX 2026-04-16: Tristan flagged that the previous crawler never visited contact pages,
/// so even though the LLM extracted names + titles from /team pages, no email or phone
/// data was ever captured. Visiting these pages explicitly + a regex pass on every fetched
/// page is what turns the pipeline from "discovery only" into "actionable outreach list".
const CONTACT_PATHS: &[&str] = &[
    "/contact", "/contact-us", "/contactus", "/get-in-touch",
    "/reach-us", "/find-us", "/sales", "/enquiries", "/enquiry",
    "/wholesale", "/trade", "/business", "/become-a-customer",
    "/work-with-us", "/partnerships",
    // English additions
    "/connect", "/customer-service", "/help", "/support", "/locations",
    "/offices", "/dealers", "/distributors", "/where-to-buy", "/stockists",
    "/agents", "/representatives",
    // French
    "/contactez-nous", "/nous-contacter", "/distributeurs", "/revendeurs",
    // Spanish
    "/contacto", "/contactenos", "/distribuidores", "/donde-comprar",
    // Sales-oriented
    "/quote", "/request-a-quote", "/rfq", "/bulk-orders", "/commercial",
];

/// Common impersonal mailbox prefixes — saved as the company's general contact rather
/// than attributed to a named decision maker.
const GENERIC_LOCAL_PARTS: &[&str] = &[
    "info", "hello", "contact", "enquiries", "sales", "team", "support",
    "admin", "office", "reception", "press", "media", "marketing",
    "trade", "wholesale", "orders", "customer", "customerservice",
    "noreply", "no-reply", "donotreply", "webmaster", "postmaster",
    "mail", "mailbox",
];

#[derive(Debug, Deserialize)]
struct ExtractedContact {
    name: String,
    title: Option<String>,
    department: Option<String>,
    seniority: Option<String>,
    is_decision_maker: Option<bool>,
    /// FIX 2026-04-16: ask the LLM for these explicitly. The previous prompt didn't
    /// ask for them, so even when emails were on the team page we missed them.
    email: Option<String>,
    phone: Option<String>,
    linkedin_url: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct ScrapedSignals {
    /// All emails found across all fetched pages, deduplicated, lowercased.
    emails: HashSet<String>,
    /// All UK-shaped phone numbers found, deduplicated, normalized to digits/+.
    phones: HashSet<String>,
    /// All linkedin.com/in/ profile URLs found, deduplicated.
    linkedin_profiles: HashSet<String>,
    /// linkedin.com/company/ URL (just the first one — usually one per site).
    linkedin_company: Option<String>,
}

/// Extract contacts from company websites using LLM analysis + regex passes.
///
/// SPEEDUP 2026-04-20: Switched LLM from DeepSeek (~5-15s/call) to Anthropic
/// Claude Haiku 4.5 (~2-3s/call). Added 8-way concurrency on the company loop
/// using tokio::JoinSet — per-task work (HTTP fetches + LLM) runs in parallel
/// while DB writes serialise on the existing Mutex<Connection>. Falls back to
/// DeepSeek if no Anthropic key is configured.
pub async fn run(app: &tauri::AppHandle, job_id: &str, config: &Value) -> Result<Value> {
    let anthropic_key = config
        .get("anthropic_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    let deepseek_key = config
        .get("deepseek_api_key")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();
    // FIX 2026-04-22 (v0.55.0): Apollo API as fallback when website scraping
    // yields no email. ~70% Gulf coverage, ~85% Canada/UK. Optional — if key
    // is empty, the fallback is silently skipped.
    let apollo_key = Arc::new(
        config
            .get("apollo_api_key")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string(),
    );

    let provider = if !anthropic_key.is_empty() {
        LlmProvider::Anthropic(anthropic_key)
    } else if !deepseek_key.is_empty() {
        LlmProvider::Deepseek(deepseek_key)
    } else {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "error",
            "No LLM API key configured (need anthropic_api_key or deepseek_api_key)");
        anyhow::bail!("No LLM API key configured for contact extraction");
    };

    let provider_label = match &provider {
        LlmProvider::Anthropic(_) => "claude-haiku-4-5",
        LlmProvider::Deepseek(_) => "deepseek-chat",
    };

    let profile_id = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_active_profile_id()
    };

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies_needing_contacts(&profile_id, BATCH_LIMIT)?
    };

    let total = companies.len();
    if total == 0 {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "info", "No companies need contact extraction");
        return Ok(json!({ "processed": 0, "contacts_found": 0 }));
    }

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "info",
            &format!("Starting contact extraction for {} companies (provider={}, parallel={})",
                total, provider_label, PARALLEL_COMPANIES));
    }

    let extracted_count = Arc::new(AtomicI64::new(0));
    let general_count = Arc::new(AtomicI64::new(0));
    let error_count = Arc::new(AtomicI64::new(0));
    let processed_count = Arc::new(AtomicI64::new(0));
    let provider = Arc::new(provider);

    super::emit_node(app, json!({
        "node_id": "contacts",
        "status": "running",
        "model": format!("{} + regex", provider_label),
        "progress": { "current": 0, "total": total },
        "concurrency": PARALLEL_COMPANIES,
    }));

    let mut tasks: JoinSet<()> = JoinSet::new();

    for company in companies.into_iter() {
        if super::is_cancelled() {
            break;
        }

        let company_id = company.get("id").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let company_name = company.get("name").and_then(|v| v.as_str()).unwrap_or("Unknown").to_string();
        let website_url = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("").to_string();

        if company_id.is_empty() || website_url.is_empty() {
            continue;
        }

        // Throttle: wait until a worker frees up before spawning next
        while tasks.len() >= PARALLEL_COMPANIES {
            let _ = tasks.join_next().await;
        }

        let app_clone = app.clone();
        let provider_clone = provider.clone();
        let apollo_clone = apollo_key.clone();
        let extracted_clone = extracted_count.clone();
        let general_clone = general_count.clone();
        let error_clone = error_count.clone();
        let processed_clone = processed_count.clone();
        let total_for_log = total;

        tasks.spawn(async move {
            let idx = processed_clone.fetch_add(1, Ordering::Relaxed) + 1;
            log::info!("[Contacts] ({}/{}) Extracting for: {}", idx, total_for_log, company_name);

            // Track whether ANY email got saved for this company. Used to decide
            // whether to fall back to Apollo at the end.
            let mut any_email_saved = false;
            // Cap email-permutation+verify probes per company. Each probe is
            // 5-10s of MX+SMTP work; 3 is enough to reach the most senior DM.
            let mut verify_budget: u8 = 3;
            let extracted_domain;

            match extract_contacts_for_company(&provider_clone, &company_name, &website_url).await {
                Ok((contacts, signals, company_domain)) => {
                    extracted_domain = company_domain.clone();
                    let db: tauri::State<'_, Database> = app_clone.state();

                    for (ci, contact) in contacts.iter().enumerate() {
                        let is_dm = contact.is_decision_maker.unwrap_or(false);
                        let role = if is_dm { "decision_maker" } else { "influencer" };

                        // Track WHERE the email came from. Set as we go so we can
                        // stamp `email_source` on the saved row after-the-fact.
                        let mut email_source: Option<&'static str> = None;
                        let matched_email = match_email_to_name(&contact.name, &signals.emails);
                        let mut email = if let Some(e) = contact.email.clone() {
                            email_source = Some("page_html");
                            Some(e)
                        } else if let Some(e) = matched_email.clone() {
                            email_source = Some("page_regex");
                            Some(e)
                        } else {
                            None
                        };

                        // FIX 2026-04-22 (v0.55.1): if we know the person's name but
                        // didn't find an email on the page, try two fallbacks in order:
                        //   (a) Apollo /people/match — paid (1 credit), most reliable
                        //   (b) SMTP permutation+verify — free, slower
                        // Caps at 3 attempts per company to keep batch time bounded.
                        // Apollo runs first because it's faster (~1s vs 5-10s SMTP per name)
                        // and returns more metadata (department, seniority, linkedin).
                        if email.is_none() && verify_budget > 0 && !company_domain.is_empty() {
                            if let Some((first, last)) = split_name(&contact.name) {
                                verify_budget -= 1;
                                // (a) Apollo
                                if !apollo_clone.is_empty() {
                                    match services::apollo::match_person(
                                        &apollo_clone, &first, &last, &company_name,
                                    ).await {
                                        Ok(Some(ac)) if !ac.email.is_empty() => {
                                            log::info!("[Contacts] Apollo-matched {} for {}: {}",
                                                contact.name, company_name, ac.email);
                                            email = Some(ac.email.clone());
                                            email_source = Some("apollo_match");
                                        }
                                        Ok(_) => {}
                                        Err(e) => {
                                            log::warn!("[Contacts] Apollo match err for {}: {}",
                                                contact.name, e);
                                        }
                                    }
                                }
                                // (b) SMTP fallback if Apollo had nothing
                                if email.is_none() {
                                    if let Some(found) = services::email_verify::find_working_email(
                                        &first, &last, &company_domain,
                                    ).await {
                                        log::info!("[Contacts] SMTP-verified {} for {}: {}",
                                            contact.name, company_name, found);
                                        email = Some(found);
                                        email_source = Some("smtp_verified");
                                    }
                                }
                            }
                        }

                        let linkedin = contact.linkedin_url.as_deref()
                            .or_else(|| signals.linkedin_profiles.iter().next().map(|s| s.as_str()));

                        let saved = db.save_contact(
                            &company_id,
                            &contact.name,
                            contact.title.as_deref(),
                            email.as_deref(),
                            contact.phone.as_deref(),
                            linkedin,
                            Some(role),
                            contact.department.as_deref(),
                            contact.seniority.as_deref(),
                            Some("company_website"),
                            None,
                            ci == 0 && is_dm,
                        );
                        if saved.is_ok() && email.is_some() {
                            any_email_saved = true;
                            // Stamp per-email provenance so we can audit yield by tool.
                            if let Some(src) = email_source {
                                let _ = db.set_email_source(&company_id, &contact.name, src);
                            }
                        }
                    }
                    if !contacts.is_empty() {
                        extracted_clone.fetch_add(contacts.len() as i64, Ordering::Relaxed);
                    }

                    if let Some(general_email) = pick_general_email(&signals.emails, &company_domain) {
                        let general_phone = signals.phones.iter().next().map(|s| s.as_str());
                        let general_linkedin = signals.linkedin_company.as_deref();
                        let saved = db.save_contact(
                            &company_id,
                            "General",
                            Some("Company contact"),
                            Some(&general_email),
                            general_phone,
                            general_linkedin,
                            Some("general"),
                            None,
                            None,
                            Some("company_website"),
                            Some("Auto-extracted from contact pages — not attributed to a named person"),
                            false,
                        );
                        if saved.is_ok() {
                            general_clone.fetch_add(1, Ordering::Relaxed);
                            any_email_saved = true;
                            let _ = db.set_email_source(&company_id, "General", "page_general");
                        }
                    } else if let Some(phone) = signals.phones.iter().next() {
                        let _ = db.save_contact(
                            &company_id,
                            "General",
                            Some("Company contact"),
                            None,
                            Some(phone.as_str()),
                            signals.linkedin_company.as_deref(),
                            Some("general"),
                            None,
                            None,
                            Some("company_website"),
                            Some("Phone only — no email found on website"),
                            false,
                        );
                        general_clone.fetch_add(1, Ordering::Relaxed);
                    }

                    if contacts.is_empty() && signals.emails.is_empty() && signals.phones.is_empty() {
                        log::info!("[Contacts] No contacts or signals for {}", company_name);
                    }
                }
                Err(e) => {
                    log::warn!("[Contacts] Error extracting for {}: {}", company_name, e);
                    error_clone.fetch_add(1, Ordering::Relaxed);
                    extracted_domain = extract_domain(&normalize_base_url(&website_url));
                }
            }

            // 2026-04-22: company-level Apollo discovery removed. Apollo's
            // search endpoints on the free/lower tiers return obfuscated last
            // names so we can't act on the results without a paid reveal step.
            // Apollo enrichment now runs PER NAME inside the contact loop above
            // (see "Apollo /people/match" branch). For companies with zero
            // names extracted at all, Apollo can't help on this tier — they
            // need either a paid Apollo upgrade or LinkedIn scraping.
            let _ = any_email_saved;
            let _ = extracted_domain;

            // FIX 2026-04-22: count this attempt regardless of outcome so the
            // queue stops re-serving companies that consistently yield nothing.
            // Pairs with `contact_attempts < 2` filter in
            // db::get_companies_needing_contacts.
            let db: tauri::State<'_, Database> = app_clone.state();
            let _ = db.mark_contact_attempt(&company_id);
        });
    }

    // Drain remaining workers
    while let Some(_) = tasks.join_next().await {}

    let extracted = extracted_count.load(Ordering::Relaxed);
    let general = general_count.load(Ordering::Relaxed);
    let errors = error_count.load(Ordering::Relaxed);

    super::emit_node(app, json!({
        "node_id": "contacts",
        "status": "completed",
        "progress": { "current": total, "total": total },
    }));

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "contacts", "info",
            &format!("Contacts complete: {} processed, {} named contacts, {} general contacts, {} errors",
                total, extracted, general, errors));
    }

    Ok(json!({
        "processed": total,
        "contacts_found": extracted,
        "general_contacts": general,
        "errors": errors,
    }))
}

/// LLM provider for contact extraction. Anthropic Haiku 4.5 preferred for speed
/// (~2-3s/call vs DeepSeek ~5-15s); DeepSeek used as fallback if no Anthropic key.
pub(crate) enum LlmProvider {
    Anthropic(String),
    Deepseek(String),
}

/// Visit team + contact pages, run regex pass, then send to LLM for named-contact extraction.
async fn extract_contacts_for_company(
    provider: &LlmProvider,
    company_name: &str,
    website_url: &str,
) -> Result<(Vec<ExtractedContact>, ScrapedSignals, String)> {
    let base_url = normalize_base_url(website_url);
    let company_domain = extract_domain(&base_url);
    let mut page_text = String::new();
    let mut signals = ScrapedSignals::default();

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        .build()?;

    // FIX 2026-04-16: Visit BOTH team pages (for named contacts) AND contact pages
    // (for emails, phones, sales addresses). Try team paths first because the LLM
    // is most useful when the page is a team listing.
    let mut all_paths: Vec<&str> = Vec::with_capacity(TEAM_PATHS.len() + CONTACT_PATHS.len());
    all_paths.extend(TEAM_PATHS.iter());
    all_paths.extend(CONTACT_PATHS.iter());

    let mut pages_fetched = 0;
    let mut consecutive_failures = 0;
    let max_pages = 6;
    let circuit_breaker_threshold = 5;

    for path in &all_paths {
        if pages_fetched >= max_pages {
            break;
        }
        if consecutive_failures >= circuit_breaker_threshold {
            log::info!("[Contacts] Circuit breaker tripped for {} after {} consecutive failures",
                company_name, consecutive_failures);
            break;
        }

        let url = format!("{}{}", base_url, path);
        match fetch_page_text(&client, &url).await {
            Ok(text) => {
                if !text.is_empty() && text.len() > 100 {
                    extract_signals_into(&text, &company_domain, &mut signals);
                    page_text.push_str(&format!("\n--- PAGE: {} ---\n{}\n", path, text));
                    pages_fetched += 1;
                    consecutive_failures = 0;
                }
            }
            Err(_) => {
                consecutive_failures += 1;
            }
        }
    }

    // Fallback: if no subpages worked, fetch the root page.
    if page_text.is_empty() {
        match services::scraper::fetch_website_text(website_url).await {
            Ok(text) => {
                extract_signals_into(&text, &company_domain, &mut signals);
                page_text = text;
            }
            Err(e) => {
                anyhow::bail!("Failed to fetch website for {}: {}", company_name, e);
            }
        }
    }

    if page_text.is_empty() || page_text.len() < 50 {
        // No content but we may have signals from the root attempt
        return Ok((vec![], signals, company_domain));
    }

    // Truncate to avoid token limits.
    // FIX 2026-04-20: bumped from 8,000 → 25,000 chars. UAE/Saudi flower sites
    // typically return 300-800 KB of SSR HTML where the contact email lives in
    // the footer or contact page beyond char 8,000. The 3x token cost is trivial
    // (Haiku at $0.80/M in × ~25K input ≈ $0.02 per company) and the yield
    // uplift on long pages is meaningful.
    let truncated = if page_text.len() > 25000 {
        &page_text[..25000]
    } else {
        page_text.as_str()
    };

    // FIX 2026-04-16: ask LLM for email / phone / linkedin alongside name + title.
    // The previous prompt only asked for name/title/dept/seniority, so even when the
    // page literally said "Jane Smith — VP Sales — jane.smith@acme.com" we lost the email.
    let system_prompt = "You extract decision maker contacts from company web pages. \
        Return valid JSON only. No markdown, no explanation.";

    let prompt = format!(
        "This is a company called \"{}\" that might buy modular vertical farming systems \
        (container farms for growing salads, herbs, leafy greens, ornamental plants). \
        Extract the most relevant decision makers from the following web page text. \
        Return a JSON array: [{{\"name\": \"...\", \"title\": \"...\", \"department\": \"...\", \
        \"seniority\": \"...\", \"is_decision_maker\": true/false, \"email\": \"...\", \
        \"phone\": \"...\", \"linkedin_url\": \"...\"}}]. \
        Include email / phone / linkedin_url ONLY when they appear next to or clearly belong to \
        that named person on the page. Use null if the field is not present. \
        Focus on people in these roles: Head of Procurement / Buying Director / Fresh Produce, \
        Sustainability Director, Operations Director, Commercial Director, Innovation Lead, \
        CEO, COO, Managing Director, Founder, Owner. \
        For department, use one of: procurement, sustainability, operations, fresh_produce, \
        innovation, executive, sales, other. \
        For seniority, use one of: c_suite, director, head_of, manager, other. \
        If no relevant people are found, return an empty array []. \
        Maximum 10 contacts.\n\n--- WEB PAGE TEXT ---\n{}",
        company_name, truncated,
    );

    let response = match provider {
        LlmProvider::Anthropic(key) => {
            // chat_raw skips the {...}-only cleaner so JSON arrays survive intact
            services::anthropic::chat_raw(key, Some(system_prompt), &prompt).await?
        }
        LlmProvider::Deepseek(key) => {
            services::deepseek::chat(key, Some(system_prompt), &prompt, true).await?
        }
    };

    let contacts = parse_contacts_response(&response);
    Ok((contacts, signals, company_domain))
}

/// Parse LLM response into extracted contacts.
fn parse_contacts_response(response: &str) -> Vec<ExtractedContact> {
    let trimmed = response.trim();

    if let Ok(contacts) = serde_json::from_str::<Vec<ExtractedContact>>(trimmed) {
        return contacts;
    }

    if let Ok(obj) = serde_json::from_str::<Value>(trimmed) {
        if let Some(obj_map) = obj.as_object() {
            for (_key, val) in obj_map {
                if let Some(arr) = val.as_array() {
                    if let Ok(contacts) = serde_json::from_value::<Vec<ExtractedContact>>(Value::Array(arr.clone())) {
                        return contacts;
                    }
                }
            }
        }
    }

    if let Some(start) = trimmed.find('[') {
        if let Some(end) = trimmed.rfind(']') {
            if start < end {
                let json_slice = &trimmed[start..=end];
                if let Ok(contacts) = serde_json::from_str::<Vec<ExtractedContact>>(json_slice) {
                    return contacts;
                }
            }
        }
    }

    log::warn!("[Contacts] Failed to parse LLM response: {}", &trimmed[..trimmed.len().min(200)]);
    vec![]
}

/// FIX 2026-04-16: Regex pass to extract emails, phones, and LinkedIn URLs from page HTML.
/// FIX 2026-04-20: relaxed strict same-domain filter — UAE / SA / CA companies
/// often expose info@<related-tld> or use Gmail, all of which were silently dropped.
/// Now we accept all email-shaped strings except those on a noise blocklist (analytics,
/// CDN, social widgets). pick_general_email() still prefers same-domain when ranking.
fn extract_signals_into(text: &str, company_domain: &str, signals: &mut ScrapedSignals) {
    // Domains we never want to capture as a "company contact" — these are
    // tracking pixels, JS analytics, social embeds, dev / hosting providers.
    const NOISE_DOMAINS: &[&str] = &[
        "sentry.io", "sentry.com", "googletagmanager.com", "google-analytics.com",
        "doubleclick.net", "facebook.com", "fbcdn.net", "instagram.com",
        "twitter.com", "x.com", "tiktok.com", "youtube.com", "pinterest.com",
        "linkedin.com", "intercom.com", "intercom.io", "zendesk.com",
        "hubspot.com", "salesforce.com", "mailchimp.com", "mailerlite.com",
        "constantcontact.com", "amazonaws.com", "cloudfront.net", "wordpress.com",
        "wix.com", "squarespace.com", "shopify.com", "github.com", "gitlab.com",
        "bitbucket.org", "stripe.com", "paypal.com", "klaviyo.com", "segment.com",
        "mixpanel.com", "fullstory.com", "hotjar.com", "drift.com",
        "calendly.com", "typeform.com", "jotform.com", "wufoo.com",
        "sentry.com", "datadog.com", "newrelic.com", "logrocket.com",
        "example.com", "example.org", "domain.com", "yourdomain.com",
        "test.com",
    ];
    let _ = company_domain;  // kept for backward compat — no longer required for filter

    // Emails — basic email regex, lowercased, deduplicated
    if let Ok(re) = Regex::new(r"(?i)\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b") {
        for m in re.find_iter(text) {
            let email = m.as_str().to_lowercase();
            // Skip image-tracking URLs and common false positives
            if email.contains("@2x") || email.contains("@3x") || email.starts_with("@") {
                continue;
            }
            // Skip noise domains
            if let Some(domain) = email.split('@').nth(1) {
                if NOISE_DOMAINS.iter().any(|nd| domain == *nd || domain.ends_with(&format!(".{}", nd))) {
                    continue;
                }
                // Skip domains that look like image filenames or hashes
                if domain.len() < 4 || !domain.contains('.') {
                    continue;
                }
            } else {
                continue;
            }
            signals.emails.insert(email);
        }
    }

    // FIX 2026-04-20: explicit mailto: pass on the raw text. Even after HTML
    // stripping, mailto: prefixes sometimes survive when emitted from JS
    // bundles or JSON-LD blocks, and capturing them explicitly ensures we don't
    // miss link-encoded addresses that the generic email regex above already
    // handles (this is a belt-and-braces second pass).
    if let Ok(re) = Regex::new(r#"(?i)mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"#) {
        for cap in re.captures_iter(text) {
            if let Some(m) = cap.get(1) {
                let email = m.as_str().to_lowercase();
                if let Some(domain) = email.split('@').nth(1) {
                    if NOISE_DOMAINS.iter().any(|nd| domain == *nd || domain.ends_with(&format!(".{}", nd))) {
                        continue;
                    }
                }
                signals.emails.insert(email);
            }
        }
    }

    // UK phone numbers — quite forgiving, then normalised
    if let Ok(re) = Regex::new(r"(?i)(\+44\s?[\d\s().-]{8,15}|\b0[127]\d{2,3}\s?[\d\s().-]{6,10})") {
        for m in re.find_iter(text) {
            let raw = m.as_str();
            // Normalise: keep digits, leading +, drop everything else
            let mut normalised = String::with_capacity(raw.len());
            for ch in raw.chars() {
                if ch.is_ascii_digit() || ch == '+' {
                    normalised.push(ch);
                }
            }
            // Sanity check on length
            if normalised.len() >= 10 && normalised.len() <= 15 {
                signals.phones.insert(normalised);
            }
        }
    }

    // LinkedIn personal profile URLs
    if let Ok(re) = Regex::new(r"(?i)https?://(?:www\.|uk\.)?linkedin\.com/in/[A-Za-z0-9_-]+") {
        for m in re.find_iter(text) {
            signals.linkedin_profiles.insert(m.as_str().to_string());
        }
    }

    // LinkedIn company URL
    if signals.linkedin_company.is_none() {
        if let Ok(re) = Regex::new(r"(?i)https?://(?:www\.|uk\.)?linkedin\.com/company/[A-Za-z0-9_-]+") {
            if let Some(m) = re.find(text) {
                signals.linkedin_company = Some(m.as_str().to_string());
            }
        }
    }
}

/// Pick the best company-general email from the scraped set.
/// Preference: matches a known generic prefix on the company domain → any email on company domain.
fn pick_general_email(emails: &HashSet<String>, company_domain: &str) -> Option<String> {
    if emails.is_empty() {
        return None;
    }
    let domain_match: Vec<&String> = emails.iter()
        .filter(|e| company_domain.is_empty() || e.ends_with(&format!("@{}", company_domain))
            || e.ends_with(&strip_subdomain(company_domain)))
        .collect();

    // First pass: a known generic prefix on the company domain
    for e in &domain_match {
        let local = e.split('@').next().unwrap_or("").to_lowercase();
        if GENERIC_LOCAL_PARTS.contains(&local.as_str()) {
            return Some((*e).clone());
        }
    }
    // Second pass: any email on the company domain
    if let Some(e) = domain_match.first() {
        return Some((*e).clone());
    }
    None
}

/// Loose name → email match: looks for emails whose local part contains parts of the name.
/// Returns the first plausible match, or None.
fn match_email_to_name(name: &str, emails: &HashSet<String>) -> Option<String> {
    let parts: Vec<String> = name.to_lowercase()
        .split_whitespace()
        .filter(|p| p.len() >= 2)
        .map(|p| p.replace('.', ""))
        .collect();
    if parts.is_empty() {
        return None;
    }
    let first = parts.first().cloned().unwrap_or_default();
    let last = parts.last().cloned().unwrap_or_default();

    for e in emails {
        let local = e.split('@').next().unwrap_or("").to_lowercase();
        // Match patterns: first.last, firstlast, flast, first.l, firstl
        let candidates = [
            format!("{}.{}", first, last),
            format!("{}{}", first, last),
            format!("{}{}", first.chars().next().unwrap_or(' '), last),
            format!("{}.{}", first, last.chars().next().unwrap_or(' ')),
            first.clone(),
        ];
        for c in &candidates {
            if !c.trim().is_empty() && local == *c {
                return Some(e.clone());
            }
        }
    }
    None
}

fn strip_subdomain(domain: &str) -> String {
    // Returns "@domain.com" form when stripping "www." etc.
    let cleaned = domain.trim_start_matches("www.");
    format!("@{}", cleaned)
}

/// Split a person's full name into (first, last) suitable for email permutation.
/// Strips honorifics ("Dr", "Mr", "Mrs", "Ms", "Prof") and suffixes ("Jr", "Sr",
/// "II", "III", "PhD", "MBE", "OBE"). Returns None if no usable parts remain.
fn split_name(full_name: &str) -> Option<(String, String)> {
    let cleaned = full_name.trim();
    if cleaned.is_empty() {
        return None;
    }
    let honorifics = ["dr", "dr.", "mr", "mr.", "mrs", "mrs.", "ms", "ms.",
        "prof", "prof.", "professor", "sir", "dame", "rev", "rev."];
    let suffixes = ["jr", "jr.", "sr", "sr.", "ii", "iii", "iv",
        "phd", "ph.d", "ph.d.", "mba", "mbe", "obe", "cbe", "esq", "esq."];

    let parts: Vec<String> = cleaned
        .split_whitespace()
        .map(|p| p.trim_end_matches(',').to_string())
        .filter(|p| {
            let lower = p.to_lowercase();
            !honorifics.contains(&lower.as_str()) && !suffixes.contains(&lower.as_str())
        })
        .collect();

    if parts.len() < 2 {
        return None;
    }
    Some((parts[0].clone(), parts[parts.len() - 1].clone()))
}

fn extract_domain(url: &str) -> String {
    if let Ok(parsed) = reqwest::Url::parse(url) {
        if let Some(host) = parsed.host_str() {
            return host.trim_start_matches("www.").to_lowercase();
        }
    }
    String::new()
}

fn normalize_base_url(url: &str) -> String {
    if let Ok(parsed) = reqwest::Url::parse(url) {
        format!("{}://{}", parsed.scheme(), parsed.host_str().unwrap_or(""))
    } else {
        let url = url.trim_end_matches('/');
        if let Some(idx) = url.find("://") {
            let after_scheme = &url[idx + 3..];
            if let Some(slash_idx) = after_scheme.find('/') {
                url[..idx + 3 + slash_idx].to_string()
            } else {
                url.to_string()
            }
        } else {
            format!("https://{}", url)
        }
    }
}

async fn fetch_page_text(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client.get(url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }
    let html = resp.text().await?;
    // FIX 2026-04-20: extract mailto: links from raw HTML BEFORE strip_html
    // destroys the <a href="mailto:..."> tags. Prepend the discovered emails as
    // plain text so the downstream regex pass in extract_signals_into picks
    // them up. Without this, link-encoded emails on /contact pages get lost.
    let mut prefix = String::new();
    if let Ok(re) = Regex::new(r#"(?i)mailto:([A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,})"#) {
        for cap in re.captures_iter(&html) {
            if let Some(m) = cap.get(1) {
                prefix.push_str("mailto:");
                prefix.push_str(m.as_str());
                prefix.push('\n');
            }
        }
    }
    let cleaned = strip_html(&html);
    Ok(format!("{}{}", prefix, cleaned))
}

fn strip_html(html: &str) -> String {
    // Remove script + style blocks
    let mut out = String::with_capacity(html.len());
    let mut in_tag = false;
    let mut in_script = false;
    let mut in_style = false;
    let mut tag_buf = String::new();

    for ch in html.chars() {
        if ch == '<' {
            in_tag = true;
            tag_buf.clear();
            continue;
        }
        if ch == '>' {
            in_tag = false;
            let lower = tag_buf.to_lowercase();
            if lower.starts_with("script") { in_script = true; }
            if lower.starts_with("/script") { in_script = false; }
            if lower.starts_with("style") { in_style = true; }
            if lower.starts_with("/style") { in_style = false; }
            tag_buf.clear();
            out.push(' ');
            continue;
        }
        if in_tag {
            tag_buf.push(ch);
            continue;
        }
        if in_script || in_style {
            continue;
        }
        out.push(ch);
    }
    // Collapse whitespace
    let collapsed: String = out
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");
    collapsed
}
