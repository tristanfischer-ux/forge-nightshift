use anyhow::Result;
use serde::Deserialize;

const NOMINATIM_API: &str = "https://nominatim.openstreetmap.org";
const USER_AGENT: &str = "ForgeNightshift/0.11.1 (tristan@fractionalforge.com)";
const RATE_LIMIT_MS: u64 = 1100; // Nominatim TOS: max 1 req/sec

#[derive(Debug, Deserialize)]
struct NominatimResult {
    lat: String,
    lon: String,
}

/// Geocode a full address string to (latitude, longitude).
/// Works worldwide via OpenStreetMap Nominatim (free, no API key).
pub async fn geocode_address(address: &str) -> Result<(f64, f64)> {
    // Rate limit: 1.1s between calls
    tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/search", NOMINATIM_API))
        .query(&[("q", address), ("format", "json"), ("limit", "1")])
        .header("User-Agent", USER_AGENT)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Nominatim error: {}", resp.status());
    }

    let results: Vec<NominatimResult> = resp.json().await?;
    match results.first() {
        Some(r) => {
            let lat: f64 = r.lat.parse().map_err(|_| anyhow::anyhow!("Invalid lat"))?;
            let lon: f64 = r.lon.parse().map_err(|_| anyhow::anyhow!("Invalid lon"))?;
            Ok((lat, lon))
        }
        None => anyhow::bail!("No Nominatim result for '{}'", address),
    }
}

/// Geocode a city + country code to (latitude, longitude).
/// Fallback when full address lookup fails.
pub async fn geocode_city_country(city: &str, country_code: &str) -> Result<(f64, f64)> {
    // Rate limit: 1.1s between calls
    tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS)).await;

    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/search", NOMINATIM_API))
        .query(&[
            ("city", city),
            ("countrycodes", country_code),
            ("format", "json"),
            ("limit", "1"),
        ])
        .header("User-Agent", USER_AGENT)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Nominatim error: {}", resp.status());
    }

    let results: Vec<NominatimResult> = resp.json().await?;
    match results.first() {
        Some(r) => {
            let lat: f64 = r.lat.parse().map_err(|_| anyhow::anyhow!("Invalid lat"))?;
            let lon: f64 = r.lon.parse().map_err(|_| anyhow::anyhow!("Invalid lon"))?;
            Ok((lat, lon))
        }
        None => anyhow::bail!("No Nominatim result for '{}, {}'", city, country_code),
    }
}
