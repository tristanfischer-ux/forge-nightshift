use anyhow::Result;
use log;
use regex::Regex;

/// Fetch a website's text content, stripping HTML tags and truncating to fit LLM context.
pub async fn fetch_website_text(url: &str) -> Result<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::limited(5))
        .build()?;

    let resp = client
        .get(url)
        .header("User-Agent", "Mozilla/5.0 (compatible; ForgeOS-Nightshift/0.6)")
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("HTTP {}", resp.status());
    }

    let html = resp.text().await?;

    // Strip script and style blocks entirely
    let script_re = Regex::new(r"(?si)<(script|style)[^>]*>.*?</\1>")?;
    let cleaned = script_re.replace_all(&html, " ");

    // Strip all remaining HTML tags
    let tag_re = Regex::new(r"<[^>]+>")?;
    let text = tag_re.replace_all(&cleaned, " ");

    // Collapse whitespace
    let ws_re = Regex::new(r"\s+")?;
    let text = ws_re.replace_all(&text, " ");

    let text = text.trim().to_string();

    // Truncate to 12000 chars (enough to capture multi-page content for LLM)
    if text.len() > 12000 {
        log::info!("Truncating website text from {} to 12000 chars for {}", text.len(), url);
        Ok(text[..12000].to_string())
    } else {
        Ok(text)
    }
}
