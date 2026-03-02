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

    // Step 2: Extract root page text
    let root_text = html_to_text(&root_html);

    // Step 3: Discover and score internal subpage links
    let base_url = extract_base_url(url);
    let subpage_urls = discover_subpages(&root_html, &base_url, url);

    if subpage_urls.is_empty() {
        log::info!("No scored subpages found for {}, using root only", url);
        return Ok(truncate_text(
            &format!("--- PAGE: / ---\n{}", root_text),
            24000,
            url,
        ));
    }

    log::info!(
        "Found {} scored subpages for {}, fetching top {}",
        subpage_urls.len(),
        url,
        subpage_urls.len().min(5)
    );

    // Step 4: Fetch top 5 subpages in parallel
    let top_urls: Vec<String> = subpage_urls.into_iter().take(5).collect();
    let subpage_results: Vec<(String, Option<String>)> = stream::iter(top_urls)
        .map(|sub_url| {
            let client = client.clone();
            async move {
                let text = fetch_html(&client, &sub_url)
                    .await
                    .ok()
                    .map(|html| html_to_text(&html));
                (sub_url, text)
            }
        })
        .buffer_unordered(5)
        .collect()
        .await;

    // Step 5: Concatenate with section headers
    let mut combined = format!("--- PAGE: / ---\n{}", root_text);
    for (sub_url, text) in &subpage_results {
        if let Some(t) = text {
            if !t.is_empty() {
                let path = sub_url.strip_prefix(&base_url).unwrap_or(sub_url);
                combined.push_str(&format!("\n\n--- PAGE: {} ---\n{}", path, t));
            }
        }
    }

    Ok(truncate_text(&combined, 24000, url))
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

/// Strip HTML to plain text: remove script/style blocks, tags, collapse whitespace.
fn html_to_text(html: &str) -> String {
    let script_re = Regex::new(r"(?si)<(script|style|noscript|svg)[^>]*>.*?</\1>").unwrap();
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

/// Truncate text to max_chars with a log message if needed.
fn truncate_text(text: &str, max_chars: usize, url: &str) -> String {
    if text.len() > max_chars {
        log::info!(
            "Truncating website text from {} to {} chars for {}",
            text.len(),
            max_chars,
            url
        );
        text[..max_chars].to_string()
    } else {
        text.to_string()
    }
}
