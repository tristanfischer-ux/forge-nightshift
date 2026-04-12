use anyhow::Result;
use futures::stream::{self, StreamExt};
use log;
use regex::Regex;
use std::collections::HashSet;

/// Keywords that indicate high-value subpages for manufacturing company enrichment.
const SUBPAGE_KEYWORDS: &[&str] = &[
    "about", "services", "capabilities", "team", "contact", "products",
    "what-we-do", "quality", "certifications", "manufacturing", "facilities",
    "equipment", "processes", "materials", "industries", "sectors",
];

/// Fetch a website's text content including relevant subpages.
/// Discovers internal links, scores by URL path keywords, fetches top 5 subpages
/// in parallel, concatenates with section headers, and truncates to 24k chars.
pub async fn fetch_website_text(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    // Step 1: Fetch root page HTML
    let root_html = fetch_html(&client, url).await?;

    // Step 1.5: Extract emails from root HTML before stripping tags
    let mut all_emails: Vec<String> = extract_emails(&root_html);

    // Step 2: Extract root page text
    let root_text = html_to_text(&root_html);

    // Step 3: Discover and score internal subpage links
    let base_url = extract_base_url(url);
    let subpage_urls = discover_subpages(&root_html, &base_url, url);

    if subpage_urls.is_empty() {
        log::info!("No scored subpages found for {}, using root only", url);
        let mut content = format!("--- PAGE: / ---\n{}", root_text);
        if !all_emails.is_empty() {
            let unique: Vec<String> = {
                let mut seen = HashSet::new();
                all_emails.into_iter().filter(|e| seen.insert(e.clone())).collect()
            };
            log::info!("Found {} contact emails for {}: {}", unique.len(), url, unique.join(", "));
            content = format!("CONTACT EMAILS FOUND: {}\n\n{}", unique.join(", "), content);
        }
        return Ok(truncate_text(&content, 3000, url));
    }

    log::info!(
        "Found {} scored subpages for {}, fetching top {}",
        subpage_urls.len(),
        url,
        subpage_urls.len().min(5)
    );

    // Step 4: Fetch top 5 subpages in parallel
    let top_urls: Vec<String> = subpage_urls.into_iter().take(5).collect();
    let subpage_results: Vec<(String, Option<String>, Vec<String>)> = stream::iter(top_urls)
        .map(|sub_url| {
            let client = client.clone();
            async move {
                match fetch_html(&client, &sub_url).await {
                    Ok(html) => {
                        let emails = extract_emails(&html);
                        let text = html_to_text(&html);
                        (sub_url, Some(text), emails)
                    }
                    Err(_) => (sub_url, None, vec![]),
                }
            }
        })
        .buffer_unordered(5)
        .collect()
        .await;

    // Step 5: Collect subpage emails and concatenate with section headers
    let mut combined = format!("--- PAGE: / ---\n{}", root_text);
    for (sub_url, text, emails) in &subpage_results {
        all_emails.extend(emails.iter().cloned());
        if let Some(t) = text {
            if !t.is_empty() {
                let path = sub_url.strip_prefix(&base_url).unwrap_or(sub_url);
                combined.push_str(&format!("\n\n--- PAGE: {} ---\n{}", path, t));
            }
        }
    }

    // Deduplicate and prepend email header if any found
    let unique_emails: Vec<String> = {
        let mut seen = HashSet::new();
        all_emails.into_iter().filter(|e| seen.insert(e.clone())).collect()
    };

    if !unique_emails.is_empty() {
        log::info!("Found {} contact emails for {}: {}", unique_emails.len(), url, unique_emails.join(", "));
        combined = format!("CONTACT EMAILS FOUND: {}\n\n{}", unique_emails.join(", "), combined);
    }

    Ok(truncate_text(&combined, 3000, url))
}

/// Fetch raw HTML from a URL.
async fn fetch_html(client: &reqwest::Client, url: &str) -> Result<String> {
    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
        .header("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
        .header("Accept-Language", "en-GB,en;q=0.9")
        .header("Accept-Encoding", "gzip, deflate")
        .header("Connection", "keep-alive")
        .header("Upgrade-Insecure-Requests", "1")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }

    Ok(resp.text().await?)
}

/// Extract email addresses from raw HTML before it gets stripped.
/// Finds mailto: hrefs and inline email patterns, filters false positives.
fn extract_emails(html: &str) -> Vec<String> {
    let mut emails = HashSet::new();

    // 1. Extract mailto: hrefs
    let mailto_re = Regex::new(r#"mailto:([a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,})"#).unwrap();
    for cap in mailto_re.captures_iter(html) {
        emails.insert(cap[1].to_lowercase());
    }

    // 2. Extract inline email patterns
    let email_re = Regex::new(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}").unwrap();
    for m in email_re.find_iter(html) {
        emails.insert(m.as_str().to_lowercase());
    }

    // 3. Filter false positives
    let blacklist_prefixes = ["noreply@", "no-reply@", "example@", "test@", "user@", "admin@"];
    let blacklist_domains = ["sentry.io", "w3.org", "example.com", "schema.org", "gravatar.com", "wordpress.org", "googleapis.com"];

    emails
        .into_iter()
        .filter(|e| {
            !blacklist_prefixes.iter().any(|p| e.starts_with(p))
                && !blacklist_domains.iter().any(|d| e.ends_with(d))
                && !e.ends_with(".png")
                && !e.ends_with(".jpg")
                && !e.ends_with(".js")
                && !e.ends_with(".css")
        })
        .collect()
}

/// Strip HTML to plain text: remove script/style blocks, tags, collapse whitespace.
fn html_to_text(html: &str) -> String {
    // No backreferences — Rust regex crate doesn't support \1
    let script_re = Regex::new(r"(?si)<script[^>]*>.*?</script>|<style[^>]*>.*?</style>|<noscript[^>]*>.*?</noscript>|<svg[^>]*>.*?</svg>").unwrap();
    let cleaned = script_re.replace_all(html, " ");

    let tag_re = Regex::new(r"<[^>]+>").unwrap();
    let text = tag_re.replace_all(&cleaned, " ");

    let ws_re = Regex::new(r"\s+").unwrap();
    let text = ws_re.replace_all(&text, " ");

    text.trim().to_string()
}

/// Extract base URL (scheme + host) from a full URL.
fn extract_base_url(url: &str) -> String {
    // Find scheme://host
    if let Some(pos) = url.find("://") {
        let after_scheme = &url[pos + 3..];
        if let Some(slash) = after_scheme.find('/') {
            return url[..pos + 3 + slash].to_string();
        }
    }
    url.trim_end_matches('/').to_string()
}

/// Discover internal links from HTML and return scored subpage URLs (best first).
fn discover_subpages(html: &str, base_url: &str, page_url: &str) -> Vec<String> {
    let href_re = Regex::new(r#"<a[^>]+href\s*=\s*["']([^"'#]+)["']"#).unwrap();
    let mut seen = HashSet::new();
    let mut scored: Vec<(String, usize)> = Vec::new();

    // Normalize the root URL path to avoid re-fetching it
    let root_path = page_url
        .strip_prefix(base_url)
        .unwrap_or("/")
        .trim_end_matches('/');
    let root_path = if root_path.is_empty() { "/" } else { root_path };

    for cap in href_re.captures_iter(html) {
        let href = cap[1].trim();

        // Resolve relative/absolute URLs
        let full_url = if href.starts_with("http://") || href.starts_with("https://") {
            // Must be same host
            if !href.starts_with(base_url) {
                continue;
            }
            href.to_string()
        } else if href.starts_with('/') {
            format!("{}{}", base_url, href)
        } else {
            // Relative path — skip fragments, mailto, tel, javascript
            if href.starts_with("mailto:")
                || href.starts_with("tel:")
                || href.starts_with("javascript:")
            {
                continue;
            }
            format!("{}/{}", base_url, href)
        };

        // Skip non-HTML resources
        let lower = full_url.to_lowercase();
        if lower.ends_with(".pdf")
            || lower.ends_with(".jpg")
            || lower.ends_with(".png")
            || lower.ends_with(".gif")
            || lower.ends_with(".svg")
            || lower.ends_with(".css")
            || lower.ends_with(".js")
            || lower.ends_with(".zip")
            || lower.ends_with(".mp4")
        {
            continue;
        }

        // Normalize: strip trailing slash and query params for dedup
        let normalized = full_url.split('?').next().unwrap_or(&full_url);
        let normalized = normalized.trim_end_matches('/').to_string();

        // Skip root page
        let path = normalized
            .strip_prefix(base_url)
            .unwrap_or("/")
            .trim_end_matches('/');
        let path = if path.is_empty() { "/" } else { path };
        if path == root_path || path == "/" {
            continue;
        }

        // Skip already seen
        if !seen.insert(normalized.clone()) {
            continue;
        }

        // Score by keyword presence in path
        let path_lower = path.to_lowercase();
        let score: usize = SUBPAGE_KEYWORDS
            .iter()
            .filter(|kw| path_lower.contains(*kw))
            .count();

        if score > 0 {
            scored.push((normalized, score));
        }
    }

    // Sort by score descending
    scored.sort_by(|a, b| b.1.cmp(&a.1));
    scored.into_iter().map(|(url, _)| url).collect()
}

/// Priority tiers for deep scraping page discovery.
/// Higher score = higher priority. Modeled after Python script 31's 10-tier system.
struct PriorityTier {
    keywords: &'static [&'static str],
    score: usize,
}

const PRIORITY_TIERS: &[PriorityTier] = &[
    PriorityTier { keywords: &["team", "people", "leadership", "about-us", "about", "management", "directors", "our-team", "meet-the-team"], score: 100 },
    PriorityTier { keywords: &["capabilities", "services", "what-we-do", "solutions", "offerings"], score: 90 },
    PriorityTier { keywords: &["equipment", "facilities", "machinery", "machines", "machine-list", "technology", "plant"], score: 85 },
    PriorityTier { keywords: &["contact", "contact-us", "get-in-touch", "enquiry", "enquiries"], score: 80 },
    PriorityTier { keywords: &["certifications", "quality", "accreditations", "compliance", "iso", "nadcap", "approvals"], score: 75 },
    PriorityTier { keywords: &["case-studies", "portfolio", "projects", "work", "gallery"], score: 70 },
    PriorityTier { keywords: &["careers", "jobs", "hiring", "vacancies", "join-us", "recruitment"], score: 65 },
    PriorityTier { keywords: &["news", "blog", "updates", "latest", "press"], score: 60 },
    PriorityTier { keywords: &["industries", "sectors", "markets", "applications"], score: 55 },
    PriorityTier { keywords: &["partners", "clients", "customers", "testimonials", "suppliers"], score: 50 },
];

/// Maximum consecutive page fetch failures before circuit breaker trips.
const CIRCUIT_BREAKER_THRESHOLD: usize = 3;

/// Deep-scrape a website for manufacturing technique extraction.
/// Uses 10 priority tiers for page scoring, fetches up to 8 subpages
/// with 10 concurrent workers, circuit breaker after 3 consecutive failures,
/// structured signal extraction, and returns up to 16,000 chars of content.
pub async fn fetch_website_text_deep(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    let root_html = fetch_html(&client, url).await?;
    let mut all_emails: Vec<String> = extract_emails(&root_html);
    let root_text = html_to_text(&root_html);

    let base_url = extract_base_url(url);
    let subpage_urls = discover_subpages_deep(&root_html, &base_url, url);

    if subpage_urls.is_empty() {
        log::info!("Deep scrape: no scored subpages for {}, using root only", url);
        let signals = extract_signals(&root_text, &all_emails, &root_html);
        let content = format!("{}\n--- PAGE: / ---\n{}", signals, root_text);
        return Ok(truncate_text(&content, 16000, url));
    }

    log::info!(
        "Deep scrape: found {} scored subpages for {}, fetching top {}",
        subpage_urls.len(), url, subpage_urls.len().min(8)
    );

    // Fetch top 8 subpages with 10 concurrent workers and circuit breaker
    let top_urls: Vec<String> = subpage_urls.into_iter().take(8).collect();
    let consecutive_failures = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let circuit_broken = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

    let subpage_results: Vec<(String, Option<String>, Vec<String>, Option<String>)> = stream::iter(top_urls)
        .map(|sub_url| {
            let client = client.clone();
            let consecutive_failures = consecutive_failures.clone();
            let circuit_broken = circuit_broken.clone();
            async move {
                // Check circuit breaker before fetching
                if circuit_broken.load(std::sync::atomic::Ordering::Relaxed) {
                    log::warn!("Deep scrape: circuit breaker tripped, skipping {}", sub_url);
                    return (sub_url, None, vec![], None);
                }

                match fetch_html(&client, &sub_url).await {
                    Ok(html) => {
                        // Reset consecutive failures on success
                        consecutive_failures.store(0, std::sync::atomic::Ordering::Relaxed);
                        let emails = extract_emails(&html);
                        let text = html_to_text(&html);
                        (sub_url, Some(text), emails, Some(html))
                    }
                    Err(e) => {
                        let count = consecutive_failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                        log::warn!("Deep scrape: failed to fetch {} ({}) — consecutive failures: {}", sub_url, e, count);
                        if count >= CIRCUIT_BREAKER_THRESHOLD {
                            circuit_broken.store(true, std::sync::atomic::Ordering::Relaxed);
                            log::warn!("Deep scrape: circuit breaker tripped after {} consecutive failures on {}", count, extract_base_url(&sub_url));
                        }
                        (sub_url, None, vec![], None)
                    }
                }
            }
        })
        .buffer_unordered(10)
        .collect()
        .await;

    // Collect all page text and HTML for signal extraction
    let mut combined = format!("--- PAGE: / ---\n{}", root_text);
    let mut all_text = root_text.clone();
    let mut all_html = root_html;
    for (sub_url, text, emails, html) in &subpage_results {
        all_emails.extend(emails.iter().cloned());
        if let Some(t) = text {
            if !t.is_empty() {
                let path = sub_url.strip_prefix(&base_url).unwrap_or(sub_url);
                combined.push_str(&format!("\n\n--- PAGE: {} ---\n{}", path, t));
                all_text.push(' ');
                all_text.push_str(t);
            }
        }
        if let Some(h) = html {
            all_html.push(' ');
            all_html.push_str(h);
        }
    }

    // Deduplicate emails
    let unique_emails: Vec<String> = {
        let mut seen = HashSet::new();
        all_emails.into_iter().filter(|e| seen.insert(e.clone())).collect()
    };

    // Extract structured signals from all collected text and HTML
    let signals = extract_signals(&all_text, &unique_emails, &all_html);

    // Prepend signals header
    combined = format!("{}\n{}", signals, combined);

    Ok(truncate_text(&combined, 16000, url))
}

/// Discover subpages using 10 priority tiers for scoring.
/// Each URL gets the highest matching tier score (not additive).
fn discover_subpages_deep(html: &str, base_url: &str, page_url: &str) -> Vec<String> {
    let href_re = Regex::new(r#"<a[^>]+href\s*=\s*["']([^"'#]+)["']"#).unwrap();
    let mut seen = HashSet::new();
    let mut scored: Vec<(String, usize)> = Vec::new();

    let root_path = page_url
        .strip_prefix(base_url)
        .unwrap_or("/")
        .trim_end_matches('/');
    let root_path = if root_path.is_empty() { "/" } else { root_path };

    for cap in href_re.captures_iter(html) {
        let href = cap[1].trim();

        let full_url = if href.starts_with("http://") || href.starts_with("https://") {
            if !href.starts_with(base_url) {
                continue;
            }
            href.to_string()
        } else if href.starts_with('/') {
            format!("{}{}", base_url, href)
        } else {
            if href.starts_with("mailto:") || href.starts_with("tel:") || href.starts_with("javascript:") {
                continue;
            }
            format!("{}/{}", base_url, href)
        };

        let lower = full_url.to_lowercase();
        if lower.ends_with(".pdf") || lower.ends_with(".jpg") || lower.ends_with(".png")
            || lower.ends_with(".gif") || lower.ends_with(".svg") || lower.ends_with(".css")
            || lower.ends_with(".js") || lower.ends_with(".zip") || lower.ends_with(".mp4")
        {
            continue;
        }

        let normalized = full_url.split('?').next().unwrap_or(&full_url);
        let normalized = normalized.trim_end_matches('/').to_string();

        let path = normalized
            .strip_prefix(base_url)
            .unwrap_or("/")
            .trim_end_matches('/');
        let path = if path.is_empty() { "/" } else { path };
        if path == root_path || path == "/" {
            continue;
        }

        if !seen.insert(normalized.clone()) {
            continue;
        }

        // Score using priority tiers — take highest matching tier score
        let path_lower = path.to_lowercase();
        let score = score_url_by_priority(&path_lower);

        if score > 0 {
            scored.push((normalized, score));
        }
    }

    // Sort by score descending (highest priority first)
    scored.sort_by(|a, b| b.1.cmp(&a.1));
    scored.into_iter().map(|(url, _)| url).collect()
}

/// Score a URL path against the 10 priority tiers.
/// Returns the highest matching tier score, or 0 if no match.
fn score_url_by_priority(path_lower: &str) -> usize {
    let mut best_score = 0;
    for tier in PRIORITY_TIERS {
        for keyword in tier.keywords {
            if path_lower.contains(keyword) {
                if tier.score > best_score {
                    best_score = tier.score;
                }
                break; // Found a match in this tier, no need to check more keywords in it
            }
        }
    }
    best_score
}

/// Extract structured signals from page text and raw HTML.
/// Finds people names+titles, email addresses, LinkedIn URLs, social media URLs, and hiring indicators.
/// `html` parameter is the raw HTML (for extracting href-based social links); pass "" if unavailable.
fn extract_signals(text: &str, emails: &[String], html: &str) -> String {
    let mut sections: Vec<String> = Vec::new();

    // 1. People names and titles — "Name, Title" or "Name - Title" patterns
    let people = extract_people(text);
    if !people.is_empty() {
        sections.push(format!("PEOPLE FOUND: {}", people.join("; ")));
    }

    // 2. Email addresses
    if !emails.is_empty() {
        sections.push(format!("CONTACT EMAILS FOUND: {}", emails.join(", ")));
    }

    // 3. LinkedIn profile URLs
    let linkedin_urls = extract_linkedin_urls(text);
    if !linkedin_urls.is_empty() {
        sections.push(format!("LINKEDIN PROFILES: {}", linkedin_urls.join(", ")));
    }

    // 4. Hiring indicators
    let hiring_signals = extract_hiring_indicators(text);
    if !hiring_signals.is_empty() {
        sections.push(format!("HIRING SIGNALS: {}", hiring_signals.join("; ")));
    }

    // 5. Social media URLs (extracted from HTML hrefs + text)
    let social = extract_social_media(html, text);
    if !social.is_empty() {
        sections.push(format!("SOCIAL MEDIA: {}", social.join(" | ")));
    }

    if sections.is_empty() {
        String::new()
    } else {
        format!("--- EXTRACTED SIGNALS ---\n{}\n--- END SIGNALS ---\n", sections.join("\n"))
    }
}

/// Extract social media URLs from raw HTML (href attributes) and page text.
/// Returns formatted entries like "LinkedIn Company: https://linkedin.com/company/xxx".
fn extract_social_media(html: &str, text: &str) -> Vec<String> {
    let mut results: Vec<String> = Vec::new();

    // Combine HTML + text for searching (HTML has hrefs, text has visible URLs)
    let combined = format!("{} {}", html, text);

    // LinkedIn company pages (NOT personal /in/ profiles — those are captured separately)
    let linkedin_co_re = Regex::new(
        r#"https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9\-_]+/?"#
    ).unwrap();
    if let Some(m) = linkedin_co_re.find(&combined) {
        let url = m.as_str().trim_end_matches('/').to_string();
        results.push(format!("LinkedIn Company: {}", url));
    }

    // Twitter/X
    let twitter_re = Regex::new(
        r#"https?://(?:www\.)?(?:twitter\.com|x\.com)/[a-zA-Z0-9_]+/?"#
    ).unwrap();
    if let Some(m) = twitter_re.find(&combined) {
        let url = m.as_str().trim_end_matches('/').to_string();
        // Filter out generic paths like twitter.com/share, twitter.com/intent
        let path = url.split('/').last().unwrap_or("");
        if !["share", "intent", "home", "search", "login", "signup", "i", "hashtag"].contains(&path) {
            results.push(format!("Twitter: {}", url));
        }
    }

    // Facebook
    let facebook_re = Regex::new(
        r#"https?://(?:www\.)?facebook\.com/[a-zA-Z0-9.\-_]+/?"#
    ).unwrap();
    if let Some(m) = facebook_re.find(&combined) {
        let url = m.as_str().trim_end_matches('/').to_string();
        let path = url.split('/').last().unwrap_or("");
        if !["sharer", "sharer.php", "share", "dialog", "login", "tr"].contains(&path) {
            results.push(format!("Facebook: {}", url));
        }
    }

    // Instagram
    let instagram_re = Regex::new(
        r#"https?://(?:www\.)?instagram\.com/[a-zA-Z0-9._]+/?"#
    ).unwrap();
    if let Some(m) = instagram_re.find(&combined) {
        let url = m.as_str().trim_end_matches('/').to_string();
        let path = url.split('/').last().unwrap_or("");
        if !["accounts", "explore", "p"].contains(&path) {
            results.push(format!("Instagram: {}", url));
        }
    }

    // YouTube — channel, @handle, or user pages
    let youtube_re = Regex::new(
        r#"https?://(?:www\.)?youtube\.com/(?:channel/[a-zA-Z0-9_\-]+|@[a-zA-Z0-9_\-]+|c/[a-zA-Z0-9_\-]+|user/[a-zA-Z0-9_\-]+)/?"#
    ).unwrap();
    if let Some(m) = youtube_re.find(&combined) {
        let url = m.as_str().trim_end_matches('/').to_string();
        results.push(format!("YouTube: {}", url));
    }

    results
}

/// Extract "Name, Title" patterns from text.
/// Looks for patterns like "John Smith, Managing Director" or "Jane Doe - CEO".
fn extract_people(text: &str) -> Vec<String> {
    let mut people = Vec::new();
    let mut seen = HashSet::new();

    // Title keywords that indicate a person's role
    let title_keywords = [
        "CEO", "CTO", "CFO", "COO", "CMO", "CIO",
        "Director", "Manager", "President", "Vice President",
        "Founder", "Co-Founder", "Owner", "Partner",
        "Head of", "Lead", "Chief", "Managing",
        "Engineer", "Supervisor", "Foreman",
        "Sales", "Operations", "Production", "Quality",
    ];

    // Pattern: "Capitalized Name, Title" or "Capitalized Name - Title"
    // Match: 2-4 capitalized words followed by comma/dash and a title keyword
    let people_re = Regex::new(
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})\s*(?:,|–|—|-)\s*([A-Z][A-Za-z\s&]{2,50})"
    ).unwrap();

    for cap in people_re.captures_iter(text) {
        let name = cap[1].trim().to_string();
        let title = cap[2].trim().to_string();

        // Verify the title contains a known title keyword
        let title_lower = title.to_lowercase();
        let has_title_keyword = title_keywords.iter().any(|kw| title_lower.contains(&kw.to_lowercase()));
        if !has_title_keyword {
            continue;
        }

        let entry = format!("{} ({})", name, title);
        if seen.insert(entry.clone()) {
            people.push(entry);
        }
    }

    // Cap at 20 to avoid noise
    people.truncate(20);
    people
}

/// Extract LinkedIn profile URLs from text.
fn extract_linkedin_urls(text: &str) -> Vec<String> {
    let linkedin_re = Regex::new(
        r"https?://(?:www\.)?linkedin\.com/in/[a-zA-Z0-9\-_]+/?"
    ).unwrap();

    let mut urls: Vec<String> = Vec::new();
    let mut seen = HashSet::new();

    for m in linkedin_re.find_iter(text) {
        let url = m.as_str().trim_end_matches('/').to_string();
        if seen.insert(url.clone()) {
            urls.push(url);
        }
    }

    urls.truncate(30);
    urls
}

/// Detect hiring indicators in text.
fn extract_hiring_indicators(text: &str) -> Vec<String> {
    let text_lower = text.to_lowercase();
    let indicators = [
        ("we're hiring", "Active hiring mentioned"),
        ("we are hiring", "Active hiring mentioned"),
        ("join our team", "Team recruitment active"),
        ("join the team", "Team recruitment active"),
        ("current vacancies", "Vacancies listed"),
        ("current openings", "Job openings listed"),
        ("job openings", "Job openings listed"),
        ("career opportunities", "Career opportunities listed"),
        ("apply now", "Active job applications"),
        ("send your cv", "Accepting CVs"),
        ("send your resume", "Accepting resumes"),
        ("positions available", "Positions available"),
        ("we're looking for", "Actively seeking candidates"),
        ("we are looking for", "Actively seeking candidates"),
        ("apprenticeship", "Apprenticeship programme"),
        ("work with us", "Recruitment page present"),
    ];

    let mut found = Vec::new();
    let mut seen_descriptions = HashSet::new();

    for (phrase, description) in &indicators {
        if text_lower.contains(phrase) && seen_descriptions.insert(description.to_string()) {
            found.push(description.to_string());
        }
    }

    found
}

/// Truncate text to max_chars with a log message if needed.
fn truncate_text(text: &str, max_chars: usize, url: &str) -> String {
    if text.len() > max_chars {
        log::info!(
            "Truncating website text from {} to {} chars for {}",
            text.len(),
            max_chars,
            url
        );
        // Find nearest char boundary at or before max_chars
        let mut end = max_chars;
        while !text.is_char_boundary(end) && end > 0 {
            end -= 1;
        }
        text[..end].to_string()
    } else {
        text.to_string()
    }
}
