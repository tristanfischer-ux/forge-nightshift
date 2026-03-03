use anyhow::Result;
use serde::Deserialize;

const POSTCODES_API: &str = "https://api.postcodes.io";

#[derive(Debug, Deserialize)]
struct PostcodeResponse {
    status: i32,
    result: Option<PostcodeResult>,
}

#[derive(Debug, Deserialize)]
struct PostcodeResult {
    latitude: f64,
    longitude: f64,
}

#[derive(Debug, Deserialize)]
struct BulkPostcodeResponse {
    #[allow(dead_code)]
    status: i32,
    result: Option<Vec<BulkPostcodeResult>>,
}

#[derive(Debug, Deserialize)]
struct BulkPostcodeResult {
    query: String,
    result: Option<PostcodeResult>,
}

#[derive(Debug, Deserialize)]
struct PlaceResponse {
    #[allow(dead_code)]
    status: i32,
    result: Option<Vec<PlaceResult>>,
}

#[derive(Debug, Deserialize)]
struct PlaceResult {
    latitude: Option<f64>,
    longitude: Option<f64>,
}

/// Geocode a single UK postcode to (latitude, longitude).
/// Free, no API key needed.
pub async fn geocode_postcode(postcode: &str) -> Result<(f64, f64)> {
    let client = reqwest::Client::new();
    let encoded = postcode.replace(' ', "%20");
    let resp = client
        .get(format!("{}/postcodes/{}", POSTCODES_API, encoded))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Postcodes.io error: {}", resp.status());
    }

    let data: PostcodeResponse = resp.json().await?;
    if data.status != 200 {
        anyhow::bail!("Postcodes.io returned status {}", data.status);
    }

    match data.result {
        Some(r) => Ok((r.latitude, r.longitude)),
        None => anyhow::bail!("No result for postcode '{}'", postcode),
    }
}

/// Geocode multiple UK postcodes in bulk (up to 100 per call).
/// Returns Vec of (postcode, latitude, longitude) for successful lookups.
pub async fn geocode_bulk(postcodes: &[String]) -> Result<Vec<(String, f64, f64)>> {
    if postcodes.is_empty() {
        return Ok(vec![]);
    }

    let client = reqwest::Client::new();
    let body = serde_json::json!({ "postcodes": postcodes });

    let resp = client
        .post(format!("{}/postcodes", POSTCODES_API))
        .json(&body)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Postcodes.io bulk error: {}", resp.status());
    }

    let data: BulkPostcodeResponse = resp.json().await?;
    let mut results = Vec::new();

    if let Some(items) = data.result {
        for item in items {
            if let Some(r) = item.result {
                results.push((item.query, r.latitude, r.longitude));
            }
        }
    }

    Ok(results)
}

/// Geocode a UK city/place name to (latitude, longitude).
/// Fallback when no postcode is available.
pub async fn geocode_place(place: &str) -> Result<(f64, f64)> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/places", POSTCODES_API))
        .query(&[("q", place), ("limit", "1")])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        anyhow::bail!("Postcodes.io place search error: {}", resp.status());
    }

    let data: PlaceResponse = resp.json().await?;

    if let Some(results) = data.result {
        if let Some(first) = results.first() {
            if let (Some(lat), Some(lng)) = (first.latitude, first.longitude) {
                return Ok((lat, lng));
            }
        }
    }

    anyhow::bail!("No place result for '{}'", place)
}

/// UK postcode regex pattern
pub fn extract_uk_postcode(text: &str) -> Option<String> {
    let re = regex::Regex::new(r"[A-Z]{1,2}[0-9][A-Z0-9]?\s?[0-9][A-Z]{2}").ok()?;
    re.find(&text.to_uppercase()).map(|m| m.as_str().to_string())
}
