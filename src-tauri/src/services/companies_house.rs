use anyhow::Result;
use chrono::Datelike;
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
    pub accounts_type: Option<String>,
    pub last_accounts_date: Option<String>,
    pub has_charges: Option<bool>,
    pub has_insolvency_history: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Officer {
    pub name: String,
    pub officer_role: Option<String>,
    pub appointed_on: Option<String>,
    pub resigned_on: Option<String>,
    pub date_of_birth: Option<DateOfBirth>,
    pub nationality: Option<String>,
    pub occupation: Option<String>,
    pub country_of_residence: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DateOfBirth {
    pub month: Option<u32>,
    pub year: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PSC {
    pub name: Option<String>,
    pub natures_of_control: Option<Vec<String>>,
    pub notified_on: Option<String>,
    pub kind: Option<String>,
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
    accounts: Option<AccountsInfo>,
    has_charges: Option<bool>,
    has_insolvency_history: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct AccountsInfo {
    last_accounts: Option<LastAccounts>,
}

#[derive(Debug, Deserialize)]
struct LastAccounts {
    #[serde(rename = "type")]
    account_type: Option<String>,
    made_up_to: Option<String>,
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
    date_of_birth: Option<DateOfBirthRaw>,
    nationality: Option<String>,
    occupation: Option<String>,
    country_of_residence: Option<String>,
}

#[derive(Debug, Deserialize)]
struct DateOfBirthRaw {
    month: Option<u32>,
    year: Option<i32>,
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
    kind: Option<String>,
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
            accounts_type: None,
            last_accounts_date: None,
            has_charges: None,
            has_insolvency_history: None,
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

    let accounts_type = profile
        .accounts
        .as_ref()
        .and_then(|a| a.last_accounts.as_ref())
        .and_then(|la| la.account_type.clone());

    let last_accounts_date = profile
        .accounts
        .as_ref()
        .and_then(|a| a.last_accounts.as_ref())
        .and_then(|la| la.made_up_to.clone());

    Ok(CHCompany {
        company_number: profile.company_number,
        company_name: profile.company_name,
        company_status: profile.company_status,
        company_type: profile.company_type,
        date_of_creation: profile.date_of_creation,
        registered_office_address: profile.registered_office_address,
        sic_codes: profile.sic_codes,
        accounts_type,
        last_accounts_date,
        has_charges: profile.has_charges,
        has_insolvency_history: profile.has_insolvency_history,
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
        .map(|o| Officer {
            name: o.name,
            officer_role: o.officer_role,
            appointed_on: o.appointed_on,
            resigned_on: o.resigned_on,
            date_of_birth: o.date_of_birth.map(|d| DateOfBirth {
                month: d.month,
                year: d.year,
            }),
            nationality: o.nationality,
            occupation: o.occupation,
            country_of_residence: o.country_of_residence,
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
            kind: p.kind,
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

    // Filter to active officers for the enrichment JSON (backward compat)
    let active_officers: Vec<&Officer> = officers.iter().filter(|o| o.resigned_on.is_none()).collect();

    let directors_json: Vec<Value> = active_officers
        .iter()
        .map(|o| {
            let age = o.date_of_birth.as_ref().and_then(|dob| {
                dob.year.map(|y| {
                    let now = chrono::Utc::now();
                    let current_year = now.year();
                    let current_month = now.month();
                    let birth_month = dob.month.unwrap_or(6);
                    let mut a = current_year - y;
                    if current_month < birth_month { a -= 1; }
                    a
                })
            });
            json!({
                "name": o.name,
                "role": o.officer_role,
                "appointed_on": o.appointed_on,
                "age": age,
                "nationality": o.nationality,
                "occupation": o.occupation,
                "country_of_residence": o.country_of_residence,
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

    // Extract financial signals
    let accounts_type = profile.accounts_type.as_deref().unwrap_or("").to_string();
    let last_accounts_date = profile.last_accounts_date.as_deref().unwrap_or("").to_string();
    let has_charges = profile.has_charges.unwrap_or(false);
    let has_insolvency = profile.has_insolvency_history.unwrap_or(false);

    let company_status_str = profile.company_status.as_deref().unwrap_or("");
    let creation_date = profile.date_of_creation.as_deref().unwrap_or("");

    // Calculate years trading
    let years_trading = if !creation_date.is_empty() {
        let now = chrono::Utc::now().format("%Y").to_string();
        let creation_year: i32 = creation_date
            .split('-')
            .next()
            .and_then(|y| y.parse().ok())
            .unwrap_or(0);
        let current_year: i32 = now.parse().unwrap_or(2026);
        if creation_year > 0 {
            Some(current_year - creation_year)
        } else {
            None
        }
    } else {
        None
    };

    // Derive health score
    let health = derive_financial_health(
        company_status_str,
        &last_accounts_date,
        has_insolvency,
        years_trading,
    );

    let financial_signals = json!({
        "company_status": company_status_str,
        "incorporation_date": creation_date,
        "years_trading": years_trading,
        "accounts_type": accounts_type,
        "last_accounts_date": last_accounts_date,
        "has_insolvency_history": has_insolvency,
        "has_charges": has_charges,
        "health": health,
    });

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
        "financial_signals": financial_signals,
    })))
}

/// Derive a financial health label from Companies House data.
/// "good" = active + recent accounts + no insolvency + >5 years trading
/// "caution" = missing data or <5 years
/// "risk" = dissolved/insolvency/stale accounts (>2 years old)
fn derive_financial_health(
    status: &str,
    last_accounts_date: &str,
    has_insolvency: bool,
    years_trading: Option<i32>,
) -> String {
    // Immediate risk signals
    if status == "dissolved" || status == "liquidation" || has_insolvency {
        return "risk".to_string();
    }

    // Check accounts staleness (>2 years = risk)
    if !last_accounts_date.is_empty() {
        if let Ok(filed) = chrono::NaiveDate::parse_from_str(last_accounts_date, "%Y-%m-%d") {
            let now = chrono::Utc::now().date_naive();
            let months_old = (now.year() - filed.year()) * 12 + (now.month() as i32 - filed.month() as i32);
            if months_old > 24 {
                return "risk".to_string();
            }
        }
    }

    // Active + recent accounts + no insolvency + >5 years
    let mature = years_trading.map(|y| y >= 5).unwrap_or(false);
    let has_accounts = !last_accounts_date.is_empty();

    if status == "active" && has_accounts && mature {
        "good".to_string()
    } else {
        "caution".to_string()
    }
}

async fn rate_limit_pause() {
    tokio::time::sleep(std::time::Duration::from_millis(RATE_LIMIT_MS)).await;
}
