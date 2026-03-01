use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

const CH_BASE_URL: &str = "https://api.company-information.service.gov.uk";

/// Rate limit: 600 req/min — 120ms between calls
const RATE_LIMIT_MS: u64 = 120;

#[derive(Debug, Serialize, Deserialize)]
pub struct CHCompany {
    pub company_number: String,
    pub company_name: String,
    pub company_status: Option<String>,
    pub company_type: Option<String>,
    pub date_of_creation: Option<String>,
    pub registered_office_address: Option<Value>,
    pub sic_codes: Option<Vec<String>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Officer {
    pub name: String,
    pub officer_role: Option<String>,
    pub appointed_on: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PSC {
    pub name: Option<String>,
    pub natures_of_control: Option<Vec<String>>,
    pub notified_on: Option<String>,
}

#[derive(Debug, Deserialize)]
struct SearchResponse {
    items: Option<Vec<SearchItem>>,
}

#[derive(Debug, Deserialize)]
struct SearchItem {
    company_number: String,
    title: String,
    company_status: Option<String>,
    company_type: Option<String>,
    date_of_creation: Option<String>,
    address: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct CompanyProfile {
    company_number: String,
    company_name: String,
    company_status: Option<String>,
    company_type: Option<String>,
    date_of_creation: Option<String>,
    registered_office_address: Option<Value>,
    sic_codes: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct OfficersResponse {
    items: Option<Vec<OfficerItem>>,
}

#[derive(Debug, Deserialize)]
struct OfficerItem {
    name: String,
    officer_role: Option<String>,
    appointed_on: Option<String>,
    resigned_on: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PSCResponse {
    items: Option<Vec<PSCItem>>,
}

#[derive(Debug, Deserialize)]
struct PSCItem {
    name: Option<String>,
    natures_of_control: Option<Vec<String>>,
    notified_on: Option<String>,
    ceased_on: Option<String>,
}

fn build_client(api_key: &str) -> Result<reqwest::Client> {
    use reqwest::header;
    use base64::Engine;

    let credentials = base64::engine::general_purpose::STANDARD
        .encode(format!("{}:", api_key));

    let mut headers = header::HeaderMap::new();
    headers.insert(
        header::AUTHORIZATION,
        header::HeaderValue::from_str(&format!("Basic {}", credentials))?,
    );

    Ok(reqwest::Client::builder()
        .default_headers(headers)
        .timeout(std::time::Duration::from_secs(15))
        .build()?)
}

/// Search Companies House by company name, return best match
pub async fn search_company(api_key: &str, name: &str) -> Result<Option<CHCompany>> {
    let client = build_client(api_key)?;
    let resp = client
        .get(format!("{}/search/companies", CH_BASE_URL))
        .query(&[("q", name), ("items_per_page", "5")])
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        log::warn!("CH search failed {}: {}", status, body);
        return Ok(None);
    }

    let search: SearchResponse = resp.json().await?;
    let items = search.items.unwrap_or_default();

    // Find best match: prefer active companies whose name closely matches
    let query_lower = name.to_lowercase();
    let best = items.into_iter().find(|item| {
        let title_lower = item.title.to_lowercase();
        let status = item.company_status.as_deref().unwrap_or("");
        (title_lower.contains(&query_lower) || query_lower.contains(&title_lower))
            && status != "dissolved"
    });

    match best {
        Some(item) => Ok(Some(CHCompany {
            company_number: item.company_number,
            company_name: item.title,
            company_status: item.company_status,
            company_type: item.company_type,
            date_of_creation: item.date_of_creation,
            registered_office_address: item.address,
            sic_codes: None, // Need full profile for SIC codes
        })),
        None => Ok(None),
    }
}

/// Get full company profile by company number
pub async fn get_company(api_key: &str, company_number: &str) -> Result<CHCompany> {
    rate_limit_pause().await;
    let client = build_client(api_key)?;
    let resp = client
        .get(format!("{}/company/{}", CH_BASE_URL, company_number))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("CH get_company error {}: {}", status, body);
    }

    let profile: CompanyProfile = resp.json().await?;
    Ok(CHCompany {
        company_number: profile.company_number,
        company_name: profile.company_name,
        company_status: profile.company_status,
        company_type: profile.company_type,
        date_of_creation: profile.date_of_creation,
        registered_office_address: profile.registered_office_address,
        sic_codes: profile.sic_codes,
    })
}

/// Get active officers (directors) for a company
pub async fn get_officers(api_key: &str, company_number: &str) -> Result<Vec<Officer>> {
    rate_limit_pause().await;
    let client = build_client(api_key)?;
    let resp = client
        .get(format!("{}/company/{}/officers", CH_BASE_URL, company_number))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(vec![]);
    }

    let officers_resp: OfficersResponse = resp.json().await?;
    let officers = officers_resp
        .items
        .unwrap_or_default()
        .into_iter()
        .filter(|o| o.resigned_on.is_none()) // Only active officers
        .map(|o| Officer {
            name: o.name,
            officer_role: o.officer_role,
            appointed_on: o.appointed_on,
        })
        .collect();

    Ok(officers)
}

/// Get persons with significant control
pub async fn get_psc(api_key: &str, company_number: &str) -> Result<Vec<PSC>> {
    rate_limit_pause().await;
    let client = build_client(api_key)?;
    let resp = client
        .get(format!("{}/company/{}/persons-with-significant-control", CH_BASE_URL, company_number))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(vec![]);
    }

    let psc_resp: PSCResponse = resp.json().await?;
    let pscs = psc_resp
        .items
        .unwrap_or_default()
        .into_iter()
        .filter(|p| p.ceased_on.is_none()) // Only active PSCs
        .map(|p| PSC {
            name: p.name,
            natures_of_control: p.natures_of_control,
            notified_on: p.notified_on,
        })
        .collect();

    Ok(pscs)
}

/// Enrich a company with full CH data. Returns JSON to merge into attributes_json.
pub async fn enrich_company(api_key: &str, company_name: &str) -> Result<Option<Value>> {
    // Step 1: Search for the company
    let ch_match = match search_company(api_key, company_name).await? {
        Some(c) => c,
        None => return Ok(None),
    };

    let number = &ch_match.company_number;

    // Step 2: Get full profile (for SIC codes, address, etc.)
    let profile = get_company(api_key, number).await?;

    // Step 3: Get officers
    let officers = get_officers(api_key, number).await?;

    // Step 4: Get PSC
    let pscs = get_psc(api_key, number).await?;

    // Format registered address as string
    let address_str = profile
        .registered_office_address
        .as_ref()
        .map(|addr| {
            let parts: Vec<&str> = [
                addr.get("address_line_1").and_then(|v| v.as_str()),
                addr.get("address_line_2").and_then(|v| v.as_str()),
                addr.get("locality").and_then(|v| v.as_str()),
                addr.get("region").and_then(|v| v.as_str()),
                addr.get("postal_code").and_then(|v| v.as_str()),
            ]
            .into_iter()
            .flatten()
            .collect();
            parts.join(", ")
        })
        .unwrap_or_default();

    // Determine company size hint from type
    let company_type = profile.company_type.as_deref().unwrap_or("");
    let size_hint = match company_type {
        "ltd" | "private-limited-guarant-nsc" => "SME",
        "plc" | "private-limited-shares-section-30-exemption" => "Large",
        _ => "",
    };

    let directors_json: Vec<Value> = officers
        .iter()
        .map(|o| {
            json!({
                "name": o.name,
                "role": o.officer_role,
                "appointed_on": o.appointed_on,
            })
        })
        .collect();

    let psc_json: Vec<Value> = pscs
        .iter()
        .map(|p| {
            json!({
                "name": p.name,
                "natures_of_control": p.natures_of_control,
                "notified_on": p.notified_on,
            })
        })
        .collect();

    Ok(Some(json!({
        "ch_company_number": profile.company_number,
        "ch_company_status": profile.company_status,
        "ch_registered_address": address_str,
        "ch_incorporation_date": profile.date_of_creation,
        "ch_company_type": company_type,
        "ch_company_size": size_hint,
        "ch_directors": directors_json,
        "ch_psc": psc_json,
        "ch_sic_codes": profile.sic_codes.unwrap_or_default(),
    })))
}

async fn rate_limit_pause() {
    tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS)).await;
}
