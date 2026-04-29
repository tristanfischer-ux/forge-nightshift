//! v0.58.0 scrape-once-mine-many.
//!
//! Today's pipeline does fetch → strip text → LLM extracts → discard text.
//! Every new attribute requires re-scraping. This module instead does a
//! BFS crawl of every company website, captures the raw HTML (gzipped),
//! visible text, image metadata, PDF link metadata, and internal link
//! graph — all into `company_raw_pages`. Future extraction passes can
//! re-mine the corpus without touching the network.
//!
//! Behaviour highlights:
//!   * Same registrable-domain BFS only (no external link follow).
//!   * 2 MB body cap, 10s per-page timeout, max 50 pages per company,
//!     max 4-way parallel fetches per company.
//!   * Records every attempt — even fetch failures land a row with
//!     `error` populated so we don't retry the same dead URL forever.
//!   * Honours robots.txt (User-agent: * Disallow:) when reachable;
//!     no-op when robots.txt is missing.

use anyhow::Result;
use flate2::write::GzEncoder;
use flate2::Compression;
use scraper::{Html, Selector};
use serde_json::{json, Value};
use std::collections::{HashSet, VecDeque};
use std::io::Write;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tauri::Manager;
use tokio::sync::Mutex;
use tokio::task::JoinSet;
use url::Url;

use crate::db::Database;

/// Default ceilings for scrape_company_corpus. Tuned for "deep enough to
/// be useful for re-mining" without becoming an overnight crawl per site.
pub const DEFAULT_MAX_PAGES: usize = 50;
pub const DEFAULT_MAX_BYTES: usize = 2_000_000;
pub const DEFAULT_TIMEOUT_PER_PAGE: Duration = Duration::from_secs(10);
/// 4 in-flight fetches per company is polite and ~3-4x throughput vs serial
/// for typical 30-50 page sites.
const FETCHES_IN_FLIGHT: usize = 4;
/// Cap on companies processed in a single pipeline batch.
const BATCH_LIMIT: i64 = 50;

const USER_AGENT: &str =
    "Mozilla/5.0 (compatible; ForgeNightshift/0.58.0; +https://fractionalforge.com)";

/// Aggregated outcome for a single company crawl. Returned to the caller
/// (the pipeline stage runner) for logging.
#[derive(Debug, Clone, Default)]
pub struct ScrapeStats {
    pub pages_fetched: usize,
    pub pages_failed: usize,
    pub total_bytes: usize,
    pub elapsed: Duration,
    pub images_seen: usize,
    pub pdfs_seen: usize,
}

/// Pipeline entry point. Wired from pipeline::run_single_stage when stage
/// == "raw_scrape". Reads the active profile, pulls a batch of companies
/// that have a website_url but no rows in company_raw_pages, and crawls
/// each one sequentially (with internal 4-way fetch parallelism).
pub async fn run(app: &tauri::AppHandle, job_id: &str) -> Result<Value> {
    let profile_id = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_active_profile_id()
    };

    let companies = {
        let db: tauri::State<'_, Database> = app.state();
        db.get_companies_needing_raw_scrape(&profile_id, BATCH_LIMIT)?
    };

    let total = companies.len();
    if total == 0 {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "raw_scrape",
            "info",
            "No companies need raw scraping",
        );
        return Ok(json!({ "processed": 0, "pages_total": 0 }));
    }

    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(
            job_id,
            "raw_scrape",
            "info",
            &format!("Raw-scraping batch of {} companies", total),
        );
    }

    super::emit_node(
        app,
        json!({
            "node_id": "raw_scrape",
            "status": "running",
            "progress": { "current": 0, "total": total },
        }),
    );

    let mut pages_total: usize = 0;
    let mut bytes_total: usize = 0;

    for (idx, company) in companies.iter().enumerate() {
        if super::is_cancelled() {
            break;
        }

        let company_id = company
            .get("id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let company_name = company
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        let website_url = company
            .get("website_url")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        if company_id.is_empty() || website_url.is_empty() {
            continue;
        }

        let stats_result = {
            let db: tauri::State<'_, Database> = app.state();
            scrape_company_corpus(
                &db,
                &company_id,
                &website_url,
                DEFAULT_MAX_PAGES,
                DEFAULT_MAX_BYTES,
                DEFAULT_TIMEOUT_PER_PAGE,
            )
            .await
        };

        match stats_result {
            Ok(stats) => {
                pages_total += stats.pages_fetched;
                bytes_total += stats.total_bytes;
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "raw_scrape",
                    "info",
                    &format!(
                        "[{}/{}] {} → {} pages, {} bytes, {:.1}s",
                        idx + 1,
                        total,
                        company_name,
                        stats.pages_fetched,
                        stats.total_bytes,
                        stats.elapsed.as_secs_f64(),
                    ),
                );
                super::emit_node(
                    app,
                    json!({
                        "node_id": "raw_scrape",
                        "status": "running",
                        "progress": { "current": idx + 1, "total": total },
                        "last_company": company_name,
                        "last_pages": stats.pages_fetched,
                    }),
                );
            }
            Err(e) => {
                let db: tauri::State<'_, Database> = app.state();
                let _ = db.log_activity(
                    job_id,
                    "raw_scrape",
                    "warn",
                    &format!(
                        "[{}/{}] {} → crawl failed: {}",
                        idx + 1,
                        total,
                        company_name,
                        e
                    ),
                );
            }
        }
    }

    let summary = format!(
        "Raw scrape batch complete: {} companies, {} pages total, {} bytes",
        total, pages_total, bytes_total
    );
    {
        let db: tauri::State<'_, Database> = app.state();
        let _ = db.log_activity(job_id, "raw_scrape", "info", &summary);
    }

    super::emit_node(
        app,
        json!({
            "node_id": "raw_scrape",
            "status": "completed",
            "progress": { "current": total, "total": total },
            "pages_total": pages_total,
            "bytes_total": bytes_total,
        }),
    );

    Ok(json!({
        "processed": total,
        "pages_total": pages_total,
        "bytes_total": bytes_total,
    }))
}

/// Recursively crawl `start_url` within the same registrable domain,
/// persisting one row per fetched URL into `company_raw_pages`. Returns
/// stats describing what was collected.
pub async fn scrape_company_corpus(
    db: &Database,
    company_id: &str,
    start_url: &str,
    max_pages: usize,
    max_bytes_per_page: usize,
    timeout_per_page: Duration,
) -> Result<ScrapeStats> {
    let started = Instant::now();
    let mut stats = ScrapeStats::default();

    let parsed_start = match Url::parse(start_url) {
        Ok(u) => u,
        Err(_) => {
            // Try prefixing https:// for bare-host inputs like "acme.com".
            match Url::parse(&format!("https://{}", start_url)) {
                Ok(u) => u,
                Err(_) => {
                    db.save_raw_page(
                        company_id,
                        start_url,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        None,
                        Some("invalid start URL"),
                    )?;
                    stats.pages_failed += 1;
                    stats.elapsed = started.elapsed();
                    return Ok(stats);
                }
            }
        }
    };

    let host = match parsed_start.host_str() {
        Some(h) => h.to_string(),
        None => {
            db.save_raw_page(
                company_id,
                start_url,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                Some("no host in start URL"),
            )?;
            stats.pages_failed += 1;
            stats.elapsed = started.elapsed();
            return Ok(stats);
        }
    };

    let registrable = registrable_domain(&host);

    let client = reqwest::Client::builder()
        .timeout(timeout_per_page)
        .redirect(reqwest::redirect::Policy::limited(5))
        .user_agent(USER_AGENT)
        .build()?;

    let robots_disallows = fetch_robots_disallows(&client, &parsed_start).await;

    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<String> = VecDeque::new();

    let normalized_start = normalize_url(&parsed_start);
    queue.push_back(normalized_start.clone());
    visited.insert(normalized_start);

    // Process the BFS queue in waves of FETCHES_IN_FLIGHT. Within each wave
    // we fetch + parse in parallel; new links found are pushed to the queue
    // for the next wave.
    let visited_arc = Arc::new(Mutex::new(visited));
    let queue_arc = Arc::new(Mutex::new(queue));

    while stats.pages_fetched < max_pages {
        // Drain up to FETCHES_IN_FLIGHT URLs to process this wave.
        let wave_urls: Vec<String> = {
            let mut q = queue_arc.lock().await;
            let mut wave = Vec::new();
            while wave.len() < FETCHES_IN_FLIGHT
                && stats.pages_fetched + stats.pages_failed + wave.len() < max_pages
            {
                match q.pop_front() {
                    Some(u) => wave.push(u),
                    None => break,
                }
            }
            wave
        };

        if wave_urls.is_empty() {
            break;
        }

        let mut tasks: JoinSet<FetchOutcome> = JoinSet::new();
        for u in wave_urls {
            let client = client.clone();
            let registrable = registrable.clone();
            let robots = robots_disallows.clone();
            tasks.spawn(async move {
                fetch_one(&client, &u, &registrable, &robots, max_bytes_per_page).await
            });
        }

        while let Some(joined) = tasks.join_next().await {
            let outcome = match joined {
                Ok(o) => o,
                Err(_) => continue,
            };

            // Persist whatever we got — success OR failure — keyed on URL.
            let save_res = db.save_raw_page(
                company_id,
                &outcome.url,
                outcome.status_code,
                outcome.content_type.as_deref(),
                outcome.content_text.as_deref(),
                outcome.content_html_gz.as_deref(),
                outcome.image_metadata_json.as_deref(),
                outcome.pdf_links_json.as_deref(),
                outcome.internal_links_json.as_deref(),
                outcome.bytes_fetched,
                outcome.error.as_deref(),
            );
            if let Err(e) = save_res {
                log::warn!(
                    "[raw_scraper] DB save_raw_page failed for {}: {}",
                    outcome.url,
                    e
                );
            }

            if outcome.error.is_some() {
                stats.pages_failed += 1;
            } else {
                stats.pages_fetched += 1;
                stats.total_bytes += outcome.bytes_fetched.unwrap_or(0) as usize;
                stats.images_seen += outcome.images_count;
                stats.pdfs_seen += outcome.pdfs_count;

                // Push newly-discovered same-domain links onto the queue.
                let mut visited = visited_arc.lock().await;
                let mut q = queue_arc.lock().await;
                for link in outcome.internal_links {
                    if visited.contains(&link) {
                        continue;
                    }
                    visited.insert(link.clone());
                    q.push_back(link);
                }
            }
        }
    }

    stats.elapsed = started.elapsed();
    Ok(stats)
}

#[derive(Default)]
struct FetchOutcome {
    url: String,
    status_code: Option<i64>,
    content_type: Option<String>,
    content_text: Option<String>,
    content_html_gz: Option<Vec<u8>>,
    image_metadata_json: Option<String>,
    pdf_links_json: Option<String>,
    internal_links_json: Option<String>,
    bytes_fetched: Option<i64>,
    error: Option<String>,
    /// Same-domain links extracted from this page — used to extend the
    /// BFS queue. Not persisted directly (we persist the JSON snapshot).
    internal_links: Vec<String>,
    images_count: usize,
    pdfs_count: usize,
}

async fn fetch_one(
    client: &reqwest::Client,
    url: &str,
    registrable: &str,
    robots_disallows: &[String],
    max_bytes: usize,
) -> FetchOutcome {
    let mut out = FetchOutcome {
        url: url.to_string(),
        ..Default::default()
    };

    if let Ok(parsed) = Url::parse(url) {
        if path_blocked_by_robots(parsed.path(), robots_disallows) {
            out.error = Some("blocked by robots.txt".into());
            return out;
        }
    }

    let resp = match client.get(url).send().await {
        Ok(r) => r,
        Err(e) => {
            out.error = Some(format!("fetch error: {}", e));
            return out;
        }
    };

    out.status_code = Some(resp.status().as_u16() as i64);
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());
    out.content_type = content_type.clone();

    if !resp.status().is_success() {
        out.error = Some(format!("HTTP {}", resp.status().as_u16()));
        return out;
    }

    // Stream body so we can enforce the byte cap without holding the full
    // multi-MB body in RAM if it overshoots.
    let body_bytes = match read_body_capped(resp, max_bytes).await {
        Ok(b) => b,
        Err(e) => {
            out.error = Some(e);
            return out;
        }
    };
    out.bytes_fetched = Some(body_bytes.len() as i64);

    let is_html = content_type
        .as_deref()
        .map(|ct| ct.to_ascii_lowercase().contains("text/html"))
        .unwrap_or(true); // assume HTML if header missing — common for static hosts

    if !is_html {
        // Non-HTML 200s: store the byte count + content_type but skip parsing.
        return out;
    }

    let html_str = String::from_utf8_lossy(&body_bytes).into_owned();

    // Compress full HTML for archival.
    out.content_html_gz = gzip_bytes(html_str.as_bytes()).ok();

    // Parse + extract.
    let doc = Html::parse_document(&html_str);

    let visible_text = extract_visible_text(&doc);
    out.content_text = Some(visible_text);

    let base = Url::parse(url).ok();

    let images = extract_image_metadata(&doc, base.as_ref());
    out.images_count = images.len();
    out.image_metadata_json = serde_json::to_string(&images).ok();

    let pdfs = extract_pdf_links(&doc, base.as_ref());
    out.pdfs_count = pdfs.len();
    out.pdf_links_json = serde_json::to_string(&pdfs).ok();

    let internal = extract_internal_links(&doc, base.as_ref(), registrable);
    out.internal_links_json = serde_json::to_string(&internal).ok();
    out.internal_links = internal;

    out
}

async fn read_body_capped(
    mut resp: reqwest::Response,
    max_bytes: usize,
) -> std::result::Result<Vec<u8>, String> {
    let mut buf: Vec<u8> = Vec::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                if buf.len() + chunk.len() > max_bytes {
                    return Err(format!(
                        "body exceeded {} byte cap",
                        max_bytes
                    ));
                }
                buf.extend_from_slice(&chunk);
            }
            Ok(None) => break,
            Err(e) => return Err(format!("body read error: {}", e)),
        }
    }
    Ok(buf)
}

fn gzip_bytes(input: &[u8]) -> std::io::Result<Vec<u8>> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(input)?;
    encoder.finish()
}

// ---------------------------------------------------------------------------
// URL helpers
// ---------------------------------------------------------------------------

/// Strip fragment, drop default ports, lowercase host, normalise trailing
/// slash on the root path. Query strings are preserved (they're often
/// navigational on listing pages).
pub fn normalize_url(u: &Url) -> String {
    let mut copy = u.clone();
    copy.set_fragment(None);
    if let Some(host) = copy.host_str() {
        let lower = host.to_ascii_lowercase();
        let _ = copy.set_host(Some(&lower));
    }
    // Collapse "/path/" → "/path" except for the bare root which stays "/".
    let path = copy.path().to_string();
    if path.len() > 1 && path.ends_with('/') {
        copy.set_path(path.trim_end_matches('/'));
    }
    copy.to_string()
}

/// Drop `www.` and return the rest. Anything with the same suffix is
/// treated as same-site (so `foo.acme.com` matches `acme.com`).
pub fn registrable_domain(host: &str) -> String {
    let lower = host.to_ascii_lowercase();
    lower.strip_prefix("www.").unwrap_or(&lower).to_string()
}

/// True when `candidate` belongs to `registrable` (or any subdomain of it).
pub fn is_same_site(candidate_host: &str, registrable: &str) -> bool {
    let cand = candidate_host.to_ascii_lowercase();
    let cand = cand.strip_prefix("www.").unwrap_or(&cand);
    cand == registrable || cand.ends_with(&format!(".{}", registrable))
}

/// Extension-based filter — rules out static assets we don't want to crawl.
pub fn is_skippable_ext(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    let path = lower
        .split('?')
        .next()
        .unwrap_or("")
        .split('#')
        .next()
        .unwrap_or("");
    const SKIP: &[&str] = &[
        ".css", ".js", ".png", ".jpg", ".jpeg", ".gif", ".svg", ".webp",
        ".ico", ".woff", ".woff2", ".ttf", ".eot", ".zip", ".tar.gz",
        ".dmg", ".exe",
    ];
    SKIP.iter().any(|ext| path.ends_with(ext))
}

/// Path-based filter — noise pages we don't want in the corpus.
pub fn is_skippable_path(path: &str) -> bool {
    const SKIP: &[&str] = &[
        "/wp-admin/", "/cart", "/checkout", "/login", "/signup", "/account",
    ];
    let lower = path.to_ascii_lowercase();
    SKIP.iter().any(|p| lower.contains(p))
}

// ---------------------------------------------------------------------------
// HTML extraction
// ---------------------------------------------------------------------------

/// Collect all visible text from an HTML document, dropping script/style/svg
/// nodes, then collapsing whitespace.
pub fn extract_visible_text(doc: &Html) -> String {
    // Recursive walk from the document root. Skip subtrees rooted at
    // script/style/svg/noscript so CSS/JS source, embedded SVG, and the
    // noscript fallback (usually a duplicate of the main content) don't
    // leak into the visible-text dump.
    let mut out = String::new();
    walk_text_into(doc.tree.root(), &mut out);
    normalize_whitespace(&out)
}

fn walk_text_into(
    node: ego_tree::NodeRef<'_, scraper::Node>,
    out: &mut String,
) {
    for child in node.children() {
        match child.value() {
            scraper::Node::Text(text) => {
                out.push_str(text);
                out.push(' ');
            }
            scraper::Node::Element(elem) => {
                let name = elem.name();
                if matches!(name, "script" | "style" | "svg" | "noscript") {
                    continue;
                }
                walk_text_into(child, out);
            }
            _ => {
                walk_text_into(child, out);
            }
        }
    }
}

fn normalize_whitespace(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut last_was_ws = true; // skip leading ws
    for ch in s.chars() {
        if ch.is_whitespace() {
            if !last_was_ws {
                out.push(' ');
                last_was_ws = true;
            }
        } else {
            out.push(ch);
            last_was_ws = false;
        }
    }
    out.trim().to_string()
}

/// Extract `<img>` metadata. `nearby_text` falls back through previous
/// sibling text → parent text → empty.
pub fn extract_image_metadata(doc: &Html, base: Option<&Url>) -> Vec<Value> {
    let sel = Selector::parse("img").unwrap();
    let mut out = Vec::new();
    for img in doc.select(&sel) {
        let src_raw = img.value().attr("src").unwrap_or("");
        if src_raw.is_empty() {
            continue;
        }
        let src_abs = resolve_url(base, src_raw).unwrap_or_else(|| src_raw.to_string());
        let filename = url_basename(&src_abs);
        let alt = img.value().attr("alt").unwrap_or("").to_string();
        let title = img.value().attr("title").unwrap_or("").to_string();
        let nearby = nearby_text_for(&img);
        out.push(json!({
            "src_url": src_abs,
            "filename": filename,
            "alt": alt,
            "title": title,
            "nearby_text": nearby,
        }));
    }
    out
}

fn nearby_text_for(img: &scraper::ElementRef<'_>) -> String {
    // Walk previous siblings, gathering text from text nodes AND from
    // element subtrees (so a previous <p>caption</p> counts). Stop as
    // soon as we have something non-empty.
    let mut sib = img.prev_sibling();
    while let Some(node) = sib {
        let collected = collect_text_in_subtree(node);
        let t = normalize_whitespace(&collected);
        if !t.is_empty() {
            return truncate_chars(&t, 120);
        }
        sib = node.prev_sibling();
    }
    // Fall back to parent's combined text (excluding the img itself).
    if let Some(parent) = img.parent() {
        let mut combined = String::new();
        for child in parent.children() {
            // Skip the img element itself.
            if let Some(elem) = child.value().as_element() {
                if elem.name() == "img" {
                    continue;
                }
            }
            combined.push_str(&collect_text_in_subtree(child));
            combined.push(' ');
        }
        let t = normalize_whitespace(&combined);
        if !t.is_empty() {
            return truncate_chars(&t, 120);
        }
    }
    String::new()
}

fn collect_text_in_subtree(node: ego_tree::NodeRef<'_, scraper::Node>) -> String {
    let mut s = String::new();
    if let scraper::Node::Text(t) = node.value() {
        s.push_str(t);
        s.push(' ');
    }
    for child in node.descendants() {
        if let scraper::Node::Text(t) = child.value() {
            s.push_str(t);
            s.push(' ');
        }
    }
    s
}

fn truncate_chars(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        return s.to_string();
    }
    s.chars().take(max).collect()
}

/// Extract `<a href>` PDF links (URL ends in .pdf).
pub fn extract_pdf_links(doc: &Html, base: Option<&Url>) -> Vec<Value> {
    let sel = Selector::parse("a[href]").unwrap();
    let mut out = Vec::new();
    for a in doc.select(&sel) {
        let href = a.value().attr("href").unwrap_or("");
        if href.is_empty() {
            continue;
        }
        let abs = match resolve_url(base, href) {
            Some(u) => u,
            None => continue,
        };
        let lower = abs.to_ascii_lowercase();
        let path_part = lower.split('?').next().unwrap_or("");
        if !path_part.ends_with(".pdf") {
            continue;
        }
        let anchor: String = a.text().collect::<Vec<_>>().join(" ");
        let anchor = normalize_whitespace(&anchor);
        let filename = url_basename(&abs);
        out.push(json!({
            "url": abs,
            "filename": filename,
            "anchor_text": anchor,
        }));
    }
    out
}

/// Extract `<a href>` links that resolve to the same registrable domain,
/// deduped, normalised, and filtered against asset/extension/path skiplists.
pub fn extract_internal_links(
    doc: &Html,
    base: Option<&Url>,
    registrable: &str,
) -> Vec<String> {
    let sel = Selector::parse("a[href]").unwrap();
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for a in doc.select(&sel) {
        let href = a.value().attr("href").unwrap_or("");
        if href.is_empty()
            || href.starts_with('#')
            || href.starts_with("mailto:")
            || href.starts_with("tel:")
            || href.starts_with("javascript:")
        {
            continue;
        }
        let abs = match resolve_url(base, href) {
            Some(u) => u,
            None => continue,
        };
        if is_skippable_ext(&abs) {
            continue;
        }
        let parsed = match Url::parse(&abs) {
            Ok(u) => u,
            Err(_) => continue,
        };
        if !matches!(parsed.scheme(), "http" | "https") {
            continue;
        }
        let host = match parsed.host_str() {
            Some(h) => h,
            None => continue,
        };
        if !is_same_site(host, registrable) {
            continue;
        }
        if is_skippable_path(parsed.path()) {
            continue;
        }
        let normalised = normalize_url(&parsed);
        if seen.insert(normalised.clone()) {
            out.push(normalised);
        }
    }
    out
}

fn resolve_url(base: Option<&Url>, href: &str) -> Option<String> {
    if let Ok(abs) = Url::parse(href) {
        return Some(abs.to_string());
    }
    let base = base?;
    base.join(href).ok().map(|u| u.to_string())
}

fn url_basename(url_str: &str) -> String {
    let no_query = url_str.split('?').next().unwrap_or("");
    let no_frag = no_query.split('#').next().unwrap_or("");
    no_frag
        .rsplit('/')
        .next()
        .unwrap_or("")
        .to_string()
}

// ---------------------------------------------------------------------------
// robots.txt
// ---------------------------------------------------------------------------

/// Fetch and parse robots.txt for the site, returning a list of Disallow
/// path prefixes for `User-agent: *`. If robots.txt is unreachable or
/// missing, returns empty (don't block on missing robots).
async fn fetch_robots_disallows(client: &reqwest::Client, start: &Url) -> Vec<String> {
    let mut robots_url = start.clone();
    robots_url.set_path("/robots.txt");
    robots_url.set_query(None);
    robots_url.set_fragment(None);

    let resp = match client.get(robots_url.as_str()).send().await {
        Ok(r) => r,
        Err(_) => return Vec::new(),
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    let body = match resp.text().await {
        Ok(b) => b,
        Err(_) => return Vec::new(),
    };
    parse_robots_disallows(&body)
}

/// Parse robots.txt body, returning Disallow paths under `User-agent: *`.
/// Other UA sections are ignored (we follow the User-Agent: * rules only).
fn parse_robots_disallows(body: &str) -> Vec<String> {
    let mut disallows: Vec<String> = Vec::new();
    let mut in_star_block = false;
    let mut seen_any_ua_in_block = false;

    for raw in body.lines() {
        // Strip comments, trim.
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, ':');
        let key = parts.next().unwrap_or("").trim().to_ascii_lowercase();
        let val = parts.next().unwrap_or("").trim();

        if key == "user-agent" {
            // New UA block. If we hit another UA before any rule, treat
            // both as part of the same block.
            if !seen_any_ua_in_block {
                in_star_block = in_star_block || val == "*";
            } else {
                in_star_block = val == "*";
                seen_any_ua_in_block = false;
            }
            continue;
        }

        if key == "disallow" {
            seen_any_ua_in_block = true;
            if in_star_block && !val.is_empty() {
                disallows.push(val.to_string());
            }
        } else if key == "allow" {
            seen_any_ua_in_block = true;
            // We don't model Allow precedence — Disallow wins, which is
            // safe (we crawl less, never more).
        }
    }
    disallows
}

fn path_blocked_by_robots(path: &str, disallows: &[String]) -> bool {
    disallows.iter().any(|prefix| path.starts_with(prefix))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_fragment_and_trailing_slash() {
        let u = Url::parse("https://Acme.com/about/#team").unwrap();
        assert_eq!(normalize_url(&u), "https://acme.com/about");
    }

    #[test]
    fn normalize_keeps_root_slash() {
        let u = Url::parse("https://acme.com/").unwrap();
        assert_eq!(normalize_url(&u), "https://acme.com/");
    }

    #[test]
    fn normalize_preserves_query() {
        let u = Url::parse("https://acme.com/products?cat=1#top").unwrap();
        assert_eq!(normalize_url(&u), "https://acme.com/products?cat=1");
    }

    #[test]
    fn same_site_matches_subdomains_and_www() {
        let reg = registrable_domain("www.acme.com");
        assert_eq!(reg, "acme.com");
        assert!(is_same_site("acme.com", &reg));
        assert!(is_same_site("www.acme.com", &reg));
        assert!(is_same_site("foo.acme.com", &reg));
        assert!(is_same_site("blog.foo.acme.com", &reg));
        assert!(!is_same_site("evil.com", &reg));
        assert!(!is_same_site("acme.com.evil.com", &reg));
        assert!(!is_same_site("notacme.com", &reg));
    }

    #[test]
    fn resolve_relative_against_base() {
        let base = Url::parse("https://acme.com/about/").unwrap();
        assert_eq!(
            resolve_url(Some(&base), "team.html"),
            Some("https://acme.com/about/team.html".to_string())
        );
        assert_eq!(
            resolve_url(Some(&base), "/contact"),
            Some("https://acme.com/contact".to_string())
        );
        assert_eq!(
            resolve_url(Some(&base), "https://other.com/x"),
            Some("https://other.com/x".to_string())
        );
    }

    #[test]
    fn skippable_ext_filters_assets() {
        assert!(is_skippable_ext("https://x.com/a.css"));
        assert!(is_skippable_ext("https://x.com/img.PNG?v=2"));
        assert!(is_skippable_ext("https://x.com/font.woff2"));
        assert!(!is_skippable_ext("https://x.com/about"));
        assert!(!is_skippable_ext("https://x.com/whitepaper.pdf")); // PDFs are tracked separately, not skipped here
    }

    #[test]
    fn skippable_path_filters_noise() {
        assert!(is_skippable_path("/wp-admin/users.php"));
        assert!(is_skippable_path("/cart"));
        assert!(is_skippable_path("/checkout/step1"));
        assert!(is_skippable_path("/login"));
        assert!(!is_skippable_path("/about"));
        assert!(!is_skippable_path("/products/widgets"));
    }

    #[test]
    fn url_basename_extracts_filename() {
        assert_eq!(url_basename("https://x.com/team-john-smith.jpg"), "team-john-smith.jpg");
        assert_eq!(url_basename("https://x.com/files/brochure.pdf?v=2"), "brochure.pdf");
        assert_eq!(url_basename("https://x.com/path/"), "");
    }

    #[test]
    fn visible_text_strips_script_and_style() {
        let html = r#"
            <html><head><style>body{color:red}</style></head>
            <body>
              <script>alert('hi')</script>
              <h1>Hello   World</h1>
              <p>We make   widgets.</p>
              <noscript>fallback</noscript>
            </body></html>
        "#;
        let doc = Html::parse_document(html);
        let text = extract_visible_text(&doc);
        assert!(text.contains("Hello World"), "got: {}", text);
        assert!(text.contains("We make widgets."), "got: {}", text);
        assert!(!text.contains("alert"), "should drop script: {}", text);
        assert!(!text.contains("color:red"), "should drop style: {}", text);
        assert!(!text.contains("fallback"), "should drop noscript: {}", text);
    }

    #[test]
    fn image_metadata_resolves_and_captures_alt() {
        let html = r#"
            <html><body>
              <p>Meet our team:</p>
              <img src="/img/team-john.jpg" alt="John Smith, CEO" title="Founder">
              <img src="https://cdn.acme.com/logo.png" alt="">
            </body></html>
        "#;
        let doc = Html::parse_document(html);
        let base = Url::parse("https://acme.com/about").ok();
        let imgs = extract_image_metadata(&doc, base.as_ref());
        assert_eq!(imgs.len(), 2);
        let first = &imgs[0];
        assert_eq!(first["src_url"], "https://acme.com/img/team-john.jpg");
        assert_eq!(first["filename"], "team-john.jpg");
        assert_eq!(first["alt"], "John Smith, CEO");
        assert_eq!(first["title"], "Founder");
        let near = first["nearby_text"].as_str().unwrap_or("");
        assert!(near.contains("Meet our team"), "nearby_text: {}", near);

        let second = &imgs[1];
        assert_eq!(second["src_url"], "https://cdn.acme.com/logo.png");
        assert_eq!(second["filename"], "logo.png");
        assert_eq!(second["alt"], "");
    }

    #[test]
    fn pdf_links_extracted_with_anchor_text() {
        let html = r#"
            <html><body>
              <a href="/files/whitepaper-2025.pdf">Download our 2025 white paper</a>
              <a href="https://acme.com/brochure.PDF?v=2">Brochure</a>
              <a href="/about">About</a>
            </body></html>
        "#;
        let doc = Html::parse_document(html);
        let base = Url::parse("https://acme.com/").ok();
        let pdfs = extract_pdf_links(&doc, base.as_ref());
        assert_eq!(pdfs.len(), 2);
        assert_eq!(pdfs[0]["url"], "https://acme.com/files/whitepaper-2025.pdf");
        assert_eq!(pdfs[0]["filename"], "whitepaper-2025.pdf");
        assert_eq!(pdfs[0]["anchor_text"], "Download our 2025 white paper");
        assert_eq!(pdfs[1]["url"], "https://acme.com/brochure.PDF?v=2");
        assert_eq!(pdfs[1]["filename"], "brochure.PDF");
    }

    #[test]
    fn internal_links_filtered_to_same_domain() {
        let html = r##"
            <html><body>
              <a href="/about">About</a>
              <a href="https://acme.com/team">Team</a>
              <a href="https://blog.acme.com/post">Blog</a>
              <a href="https://evil.com/x">External</a>
              <a href="mailto:hi@acme.com">Email</a>
              <a href="#top">Anchor</a>
              <a href="/style.css">Stylesheet</a>
              <a href="/cart">Cart</a>
              <a href="/about">About duplicate</a>
            </body></html>
        "##;
        let doc = Html::parse_document(html);
        let base = Url::parse("https://acme.com/").ok();
        let links = extract_internal_links(&doc, base.as_ref(), "acme.com");
        assert!(links.contains(&"https://acme.com/about".to_string()), "links: {:?}", links);
        assert!(links.contains(&"https://acme.com/team".to_string()));
        assert!(links.contains(&"https://blog.acme.com/post".to_string()));
        assert!(!links.iter().any(|l| l.contains("evil.com")));
        assert!(!links.iter().any(|l| l.contains("mailto")));
        assert!(!links.iter().any(|l| l.ends_with(".css")));
        assert!(!links.iter().any(|l| l.contains("/cart")));
        // dedup
        let about_count = links.iter().filter(|l| *l == "https://acme.com/about").count();
        assert_eq!(about_count, 1);
    }

    #[test]
    fn robots_disallow_only_applies_to_star_ua() {
        let body = r#"
            User-agent: GPTBot
            Disallow: /

            User-agent: *
            Disallow: /admin
            Disallow: /private/

            User-agent: Bingbot
            Disallow: /no-bing
        "#;
        let dis = parse_robots_disallows(body);
        assert!(dis.iter().any(|d| d == "/admin"));
        assert!(dis.iter().any(|d| d == "/private/"));
        assert!(!dis.iter().any(|d| d == "/"));
        assert!(!dis.iter().any(|d| d == "/no-bing"));
        assert!(path_blocked_by_robots("/admin/users", &dis));
        assert!(path_blocked_by_robots("/private/secret", &dis));
        assert!(!path_blocked_by_robots("/about", &dis));
    }

    #[test]
    fn robots_handles_grouped_user_agents() {
        // Multiple consecutive UAs apply to the same rule block.
        let body = r#"
            User-agent: GPTBot
            User-agent: *
            Disallow: /noai
        "#;
        let dis = parse_robots_disallows(body);
        assert!(dis.iter().any(|d| d == "/noai"), "dis: {:?}", dis);
    }

    #[test]
    fn whitespace_normalisation_collapses_runs() {
        assert_eq!(normalize_whitespace("  hello   world\n\nfoo\tbar  "), "hello world foo bar");
        assert_eq!(normalize_whitespace(""), "");
        assert_eq!(normalize_whitespace("   "), "");
    }
}
