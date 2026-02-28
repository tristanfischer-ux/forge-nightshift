use anyhow::Result;
use serde::{Deserialize, Serialize};

const BRAVE_SEARCH_URL: &str = "https://api.search.brave.com/res/v1/web/search";

#[derive(Debug, Serialize, Deserialize)]
pub struct SearchResult {
    pub title: String,
    pub url: String,
    pub description: String,
}

#[derive(Debug, Deserialize)]
struct BraveResponse {
    web: Option<WebResults>,
}

#[derive(Debug, Deserialize)]
struct WebResults {
    results: Vec<BraveResult>,
}

#[derive(Debug, Deserialize)]
struct BraveResult {
    title: String,
    url: String,
    description: String,
}

pub async fn test_connection(api_key: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(BRAVE_SEARCH_URL)
        .header("X-Subscription-Token", api_key)
        .query(&[("q", "test"), ("count", "1")])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(resp.status().is_success())
}

pub async fn search(api_key: &str, query: &str, country: &str, count: u32) -> Result<Vec<SearchResult>> {
    let client = reqwest::Client::new();
    let resp = client
        .get(BRAVE_SEARCH_URL)
        .header("X-Subscription-Token", api_key)
        .query(&[
            ("q", query),
            ("count", &count.to_string()),
            ("country", country),
            ("search_lang", "en"),
        ])
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Brave Search error {}: {}", status, body);
    }

    let brave_resp: BraveResponse = resp.json().await?;
    let results = brave_resp
        .web
        .map(|w| {
            w.results
                .into_iter()
                .map(|r| SearchResult {
                    title: r.title,
                    url: r.url,
                    description: r.description,
                })
                .collect()
        })
        .unwrap_or_default();

    Ok(results)
}

/// Generate search queries for a given country and manufacturing specialty
pub fn generate_queries(country: &str, specialties: &[&str]) -> Vec<String> {
    let country_names = match country {
        "DE" => vec!["Germany", "Deutschland"],
        "FR" => vec!["France"],
        "NL" => vec!["Netherlands", "Nederland"],
        "BE" => vec!["Belgium", "België"],
        "IT" => vec!["Italy", "Italia"],
        _ => vec![country],
    };

    let mut queries = Vec::new();
    for country_name in &country_names {
        for specialty in specialties {
            queries.push(format!(
                "{} manufacturer {} precision engineering",
                specialty, country_name
            ));
            queries.push(format!(
                "{} CNC machining company {}",
                country_name, specialty
            ));
        }
    }
    queries
}
