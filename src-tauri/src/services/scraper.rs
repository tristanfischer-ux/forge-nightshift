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
        .header("User-Agent", "Mozilla/5.0 (compatible; ForgeOS-Nightshift/0.7)")
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

/// Additional manufacturing-specific subpage keywords for deep enrichment.
const DEEP_SUBPAGE_KEYWORDS: &[&str] = &[
    "about", "services", "capabilities", "products", "what-we-do", "quality",
    "certifications", "manufacturing", "facilities", "equipment", "processes",
    "materials", "industries", "sectors", "tolerances", "specifications",
    "machining", "finishing", "treatments", "machine-list", "technology",
    "precision", "cnc", "additive", "casting", "sheet-metal", "injection",
    "surface-finish", "iso", "nadcap", "capacity",
];

/// Deep-scrape a website for manufacturing technique extraction.
/// Uses manufacturing-specific subpage keywords, fetches up to 8 subpages,
/// and returns up to 16,000 chars of content.
pub async fn fetch_website_text_deep(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    let root_html = fetch_html(&client, url).await?;
    let root_text = html_to_text(&root_html);

    let base_url = extract_base_url(url);
    let subpage_urls = discover_subpages_deep(&root_html, &base_url, url);

    if subpage_urls.is_empty() {
        log::info!("Deep scrape: no scored subpages for {}, using root only", url);
        let content = format!("--- PAGE: / ---\n{}", root_text);
        return Ok(truncate_text(&content, 16000, url));
    }

    log::info!(
        "Deep scrape: found {} scored subpages for {}, fetching top {}",
        subpage_urls.len(), url, subpage_urls.len().min(8)
    );

    let top_urls: Vec<String> = subpage_urls.into_iter().take(8).collect();
    let subpage_results: Vec<(String, Option<String>)> = stream::iter(top_urls)
        .map(|sub_url| {
            let client = client.clone();
            async move {
                match fetch_html(&client, &sub_url).await {
                    Ok(html) => {
                        let text = html_to_text(&html);
                        (sub_url, Some(text))
                    }
                    Err(_) => (sub_url, None),
                }
            }
        })
        .buffer_unordered(4)
        .collect()
        .await;

    let mut combined = format!("--- PAGE: / ---\n{}", root_text);
    for (sub_url, text) in &subpage_results {
        if let Some(t) = text {
            if !t.is_empty() {
                let path = sub_url.strip_prefix(&base_url).unwrap_or(sub_url);
                combined.push_str(&format!("\n\n--- PAGE: {} ---\n{}", path, t));
            }
        }
    }

    Ok(truncate_text(&combined, 16000, url))
}

/// Discover subpages using manufacturing-specific deep keywords.
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

        let path_lower = path.to_lowercase();
        let score: usize = DEEP_SUBPAGE_KEYWORDS
            .iter()
            .filter(|kw| path_lower.contains(*kw))
            .count();

        if score > 0 {
            scored.push((normalized, score));
        }
    }

    scored.sort_by(|a, b| b.1.cmp(&a.1));
    scored.into_iter().map(|(url, _)| url).collect()
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
