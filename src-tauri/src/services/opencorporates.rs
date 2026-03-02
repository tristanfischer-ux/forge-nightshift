use anyhow::Result;
use chrono::Datelike;
use serde::Deserialize;
use serde_json::{json, Value};

const OC_BASE_URL: &str = "https://api.opencorporates.com/v0.4";

/// Map our 2-letter country codes to OpenCorporates jurisdiction codes.
fn country_to_jurisdiction(country: &str) -> &str {
    match country.to_uppercase().as_str() {
        "DE" => "de",
        "FR" => "fr",
        "NL" => "nl",
        "BE" => "be",
        "IT" => "it",
        "GB" | "UK" => "gb",
        "ES" => "es",
        "AT" => "at",
        "CH" => "ch",
        "SE" => "se",
        "DK" => "dk",
        "PL" => "pl",
        "CZ" => "cz",
        _ => country.to_lowercase().leak(),
    }
}

#[derive(Debug, Deserialize)]
struct OCSearchResponse {
    results: Option<OCResults>,
}

#[derive(Debug, Deserialize)]
struct OCResults {
    companies: Option<Vec<OCCompanyWrapper>>,
}

#[derive(Debug, Deserialize)]
struct OCCompanyWrapper {
    company: OCCompany,
}

#[derive(Debug, Deserialize)]
struct OCCompany {
    name: Option<String>,
    company_number: Option<String>,
    jurisdiction_code: Option<String>,
    incorporation_date: Option<String>,
    current_status: Option<String>,
    company_type: Option<String>,
    registered_address_in_full: Option<String>,
    industry_codes: Option<Vec<OCIndustryCode>>,
}

#[derive(Debug, Deserialize)]
struct OCIndustryCode {
    code: Option<String>,
    description: Option<String>,
}

/// Search OpenCorporates for a company by name and country code.
/// Returns financial_signals JSON to merge into attributes_json, or None if no match.
pub async fn enrich_company(
    api_key: &str,
    company_name: &str,
    country: &str,
) -> Result<Option<Value>> {
    let jurisdiction = country_to_jurisdiction(country);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let mut query_params = vec![
        ("q", company_name.to_string()),
        ("jurisdiction_code", jurisdiction.to_string()),
        ("per_page", "5".to_string()),
    ];

    if !api_key.is_empty() {
        query_params.push(("api_token", api_key.to_string()));
    }

    let resp = client
        .get(format!("{}/companies/search", OC_BASE_URL))
        .query(&query_params)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        log::warn!("OpenCorporates search failed {}: {}", status, company_name);
        return Ok(None);
    }

    let search: OCSearchResponse = resp.json().await?;
    let companies = search
        .results
        .and_then(|r| r.companies)
        .unwrap_or_default();

    if companies.is_empty() {
        return Ok(None);
    }

    // Find best match: prefer active companies whose name closely matches
    let query_lower = company_name.to_lowercase();
    let best = companies
        .into_iter()
        .find(|w| {
            let name_lower = w.company.name.as_deref().unwrap_or("").to_lowercase();
            let status = w.company.current_status.as_deref().unwrap_or("");
            (name_lower.contains(&query_lower) || query_lower.contains(&name_lower))
                && status != "Dissolved"
                && status != "Inactive"
        })
        .or_else(|| None);

    let oc = match best {
        Some(w) => w.company,
        None => return Ok(None),
    };

    // Calculate years trading
    let years_trading = oc.incorporation_date.as_deref().and_then(|d| {
        let year: i32 = d.split('-').next()?.parse().ok()?;
        let current_year = chrono::Utc::now().year();
        Some(current_year - year)
    });

    let status_str = oc.current_status.as_deref().unwrap_or("unknown");

    // Derive health
    let health = if status_str == "Dissolved" || status_str == "Inactive" || status_str == "Liquidation" {
        "risk"
    } else if years_trading.map(|y| y >= 5).unwrap_or(false) && status_str == "Active" {
        "good"
    } else {
        "caution"
    };

    let industry_codes: Vec<Value> = oc
        .industry_codes
        .unwrap_or_default()
        .into_iter()
        .map(|ic| {
            json!({
                "code": ic.code,
                "description": ic.description,
            })
        })
        .collect();

    Ok(Some(json!({
        "oc_company_number": oc.company_number,
        "oc_jurisdiction": oc.jurisdiction_code,
        "oc_registered_address": oc.registered_address_in_full,
        "oc_company_type": oc.company_type,
        "oc_industry_codes": industry_codes,
        "financial_signals": {
            "company_status": status_str,
            "incorporation_date": oc.incorporation_date,
            "years_trading": years_trading,
            "accounts_type": null,
            "last_accounts_date": null,
            "has_insolvency_history": false,
            "has_charges": false,
            "health": health,
        }
    })))
}
