use anyhow::Result;
use serde_json::{json, Value};
use std::collections::HashSet;

/// Validate that a listing ID is a valid UUID to prevent PostgREST query injection.
fn validate_listing_id(listing_id: &str) -> Result<()> {
    uuid::Uuid::parse_str(listing_id)
        .map_err(|_| anyhow::anyhow!("Invalid listing ID format: {}", listing_id))?;
    Ok(())
}

pub async fn test_connection(url: &str, service_key: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/rest/v1/marketplace_listings?select=id&limit=1", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    Ok(resp.status().is_success())
}

pub async fn check_domain_exists(url: &str, service_key: &str, domain: &str) -> Result<bool> {
    let client = reqwest::Client::new();
    // Query the promoted website_url column
    let resp = client
        .get(format!("{}/rest/v1/marketplace_listings", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .query(&[
            ("select", "id"),
            ("website_url", &format!("ilike.*{}*", domain.replace('\\', "\\\\").replace('%', "").replace('*', "").replace('_', "\\_"))),
            ("limit", "1"),
        ])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        return Ok(false);
    }

    let results: Vec<Value> = resp.json().await?;
    Ok(!results.is_empty())
}

/// Push a company into ForgeOS marketplace_listings.
/// Schema has NO foundry_id — it's a global catalogue.
/// contact_source must be one of: 'manual', 'ai_enriched', 'self_reported', 'csv_import'
/// We use 'ai_enriched' for Nightshift-discovered companies.
/// If company has supabase_listing_id, updates the existing listing (PATCH).
pub async fn push_listing(
    url: &str,
    service_key: &str,
    _foundry_id: &str,
    company: &Value,
) -> Result<String> {
    let client = reqwest::Client::new();

    // Start with attributes_json as base — it contains CH fields, industries,
    // materials, equipment, etc. from enrichment. Then overlay standard fields.
    let mut attributes = company
        .get("attributes_json")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| json!({}));

    // Parse specialties/certifications — may be stored as JSON strings in SQLite
    let specialties = parse_json_field(company, "specialties");
    let certifications = parse_json_field(company, "certifications");
    let industries = parse_json_field(company, "industries");

    // Overlay standard fields onto the base attributes
    let city = company.get("city").and_then(|v| v.as_str()).unwrap_or("");
    let country = company.get("country").and_then(|v| v.as_str()).unwrap_or("");
    let subcategory = company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("");
    let website = company.get("website_url").and_then(|v| v.as_str()).unwrap_or("");
    let company_size = company.get("company_size").and_then(|v| v.as_str()).unwrap_or("");

    attributes["website_url"] = json!(website);
    attributes["country"] = json!(country);
    attributes["city"] = json!(city);
    attributes["specialties"] = specialties.clone();
    attributes["certifications"] = certifications.clone();
    attributes["employees"] = json!(company_size);
    attributes["year_founded"] = json!(company.get("year_founded").and_then(|v| v.as_i64()));
    attributes["nightshift_score"] = json!(company.get("relevance_score").and_then(|v| v.as_str()).and_then(|v| v.parse::<i64>().ok()).unwrap_or(0));
    attributes["discovered_at"] = json!(chrono::Utc::now().to_rfc3339());
    attributes["source"] = json!("nightshift");

    // Construct location from city + country
    if !city.is_empty() && !country.is_empty() {
        attributes["location"] = json!(format!("{}, {}", city, country));
    } else if !city.is_empty() {
        attributes["location"] = json!(city);
    } else if !country.is_empty() {
        attributes["location"] = json!(country);
    }

    // Set company_type from subcategory
    if !subcategory.is_empty() {
        attributes["company_type"] = json!(subcategory);
    }

    // Parse attributes_json for array fields not stored as direct SQLite columns
    let attrs_parsed = company
        .get("attributes_json")
        .and_then(|v| v.as_str())
        .and_then(|s| serde_json::from_str::<Value>(s).ok())
        .unwrap_or_else(|| json!({}));

    let materials = parse_json_field_from_attrs(&attrs_parsed, "materials");
    let key_equipment = parse_json_field_from_attrs(&attrs_parsed, "key_equipment");
    let products = parse_json_field_from_attrs(&attrs_parsed, "products");
    let key_people = parse_json_field_from_attrs(&attrs_parsed, "key_people");
    let security_clearances = parse_json_field_from_attrs(&attrs_parsed, "security_clearances");

    let relevance_score = company.get("relevance_score")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<i64>().ok())
        .or_else(|| company.get("relevance_score").and_then(|v| v.as_i64()))
        .unwrap_or(0);
    let enrichment_quality = company.get("enrichment_quality")
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<i64>().ok())
        .or_else(|| company.get("enrichment_quality").and_then(|v| v.as_i64()))
        .unwrap_or(30);
    let founded_year = company.get("year_founded")
        .and_then(|v| v.as_i64())
        .or_else(|| attrs_parsed.get("founded_year").and_then(|v| v.as_i64()));
    let employee_count_exact = attrs_parsed.get("employee_count_exact").and_then(|v| v.as_i64());
    let production_capacity = attrs_parsed.get("production_capacity").and_then(|v| v.as_str()).unwrap_or("");
    let address = company.get("address").and_then(|v| v.as_str()).unwrap_or("");
    let financial_health = company.get("financial_health").and_then(|v| v.as_str()).unwrap_or("");
    let lead_time = attrs_parsed.get("lead_time").and_then(|v| v.as_str()).unwrap_or("");
    let minimum_order = attrs_parsed.get("minimum_order").and_then(|v| v.as_str()).unwrap_or("");
    let quality_systems = attrs_parsed.get("quality_systems").and_then(|v| v.as_str()).unwrap_or("");
    let export_controls = attrs_parsed.get("export_controls").and_then(|v| v.as_str()).unwrap_or("");

    let mut listing = json!({
        "title": company.get("name").and_then(|v| v.as_str()).unwrap_or(""),
        "description": company.get("description").and_then(|v| v.as_str()).unwrap_or(""),
        "category": company.get("category").and_then(|v| v.as_str()).unwrap_or("Services"),
        "subcategory": company.get("subcategory").and_then(|v| v.as_str()).unwrap_or("Manufacturing"),
        "attributes": attributes,
        "contact_source": "ai_enriched",
        "outreach_status": "not_started",
        "is_verified": false,
        "approval_status": "pending",
        "data_quality_score": enrichment_quality,
        // Promoted columns
        "website_url": website,
        "country": country,
        "city": city,
        "specialties": specialties,
        "certifications": certifications,
        "industries": industries,
        "materials": materials,
        "key_equipment": key_equipment,
        "products": products,
        "key_people": key_people,
        "security_clearances": security_clearances,
        "company_size": company_size,
        "relevance_score": relevance_score,
        "enrichment_quality": enrichment_quality,
    });

    // Optional fields — only set if non-empty to avoid overwriting with empty
    if founded_year.is_some() {
        listing["founded_year"] = json!(founded_year);
    }
    if employee_count_exact.is_some() {
        listing["employee_count_exact"] = json!(employee_count_exact);
    }
    if !production_capacity.is_empty() {
        listing["production_capacity"] = json!(production_capacity);
    }
    if !address.is_empty() {
        listing["address"] = json!(address);
    }
    if !financial_health.is_empty() {
        listing["financial_health"] = json!(financial_health);
    }
    if !lead_time.is_empty() {
        listing["lead_time"] = json!(lead_time);
    }
    if !minimum_order.is_empty() {
        listing["minimum_order"] = json!(minimum_order);
    }
    if !quality_systems.is_empty() {
        listing["quality_systems"] = json!(quality_systems);
    }
    if !export_controls.is_empty() {
        listing["export_controls"] = json!(export_controls);
    }

    // Process capabilities from deep enrichment
    if let Some(caps_str) = company.get("process_capabilities_json").and_then(|v| v.as_str()) {
        if !caps_str.is_empty() && caps_str != "[]" {
            if let Ok(caps) = serde_json::from_str::<serde_json::Value>(caps_str) {
                listing["process_capabilities"] = caps;
            }
        }
    }

    // Only include contact fields if they have values
    let contact_name = company.get("contact_name").and_then(|v| v.as_str()).unwrap_or("");
    let contact_email = company.get("contact_email").and_then(|v| v.as_str()).unwrap_or("");
    let contact_title = company.get("contact_title").and_then(|v| v.as_str()).unwrap_or("");
    let contact_phone = company.get("contact_phone").and_then(|v| v.as_str()).unwrap_or("");

    if !contact_name.is_empty() {
        listing["contact_name"] = json!(contact_name);
    }
    if !contact_email.is_empty() {
        listing["contact_email"] = json!(contact_email);
    }
    if !contact_title.is_empty() {
        listing["contact_title"] = json!(contact_title);
    }
    if !contact_phone.is_empty() {
        listing["contact_phone"] = json!(contact_phone);
    }

    // Check if this is an audit re-enrichment (has existing supabase_listing_id)
    let existing_listing_id = company
        .get("supabase_listing_id")
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty());

    if let Some(listing_id) = existing_listing_id {
        validate_listing_id(listing_id)?;
        // UPDATE existing listing via PATCH
        let resp = client
            .patch(format!(
                "{}/rest/v1/marketplace_listings?id=eq.{}",
                url, listing_id
            ))
            .header("apikey", service_key)
            .header("Authorization", format!("Bearer {}", service_key))
            .header("Content-Type", "application/json")
            .header("Prefer", "return=representation")
            .json(&listing)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Supabase update error {}: {}", status, body);
        }

        Ok(listing_id.to_string())
    } else {
        // INSERT new listing via POST
        let resp = client
            .post(format!("{}/rest/v1/marketplace_listings", url))
            .header("apikey", service_key)
            .header("Authorization", format!("Bearer {}", service_key))
            .header("Content-Type", "application/json")
            .header("Prefer", "return=representation")
            .json(&listing)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Supabase insert error {}: {}", status, body);
        }

        let results: Vec<Value> = resp.json().await?;
        let id = results
            .first()
            .and_then(|r| r.get("id"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        Ok(id)
    }
}

/// PATCH an existing marketplace_listing to add process_capabilities.
pub async fn patch_listing_capabilities(
    url: &str,
    service_key: &str,
    listing_id: &str,
    capabilities: Value,
) -> Result<()> {
    validate_listing_id(listing_id)?;
    let client = reqwest::Client::new();
    let body = json!({
        "process_capabilities": capabilities,
    });

    let resp = client
        .patch(format!(
            "{}/rest/v1/marketplace_listings?id=eq.{}",
            url, listing_id
        ))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .header("Content-Type", "application/json")
        .header("Prefer", "return=minimal")
        .json(&body)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase PATCH capabilities error {}: {}", status, resp_body);
    }

    Ok(())
}

/// Fetch all known domains and company names from ForgeOS marketplace_listings.
/// Paginates through the table extracting website_url domains and title (name).
/// Returns (domains, names) HashSets for O(1) dedup lookups.
pub async fn fetch_all_known_domains_and_names(url: &str, service_key: &str) -> Result<(HashSet<String>, HashSet<String>)> {
    let client = reqwest::Client::new();
    let mut domains = HashSet::new();
    let mut names = HashSet::new();
    let page_size = 1000;
    let mut offset = 0;

    loop {
        let resp = client
            .get(format!("{}/rest/v1/marketplace_listings", url))
            .header("apikey", service_key)
            .header("Authorization", format!("Bearer {}", service_key))
            .header("Range", format!("{}-{}", offset, offset + page_size - 1))
            .query(&[("select", "website_url,title")])
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await?;

        if !resp.status().is_success() && resp.status().as_u16() != 206 {
            // 206 = partial content (pagination)
            break;
        }

        let rows: Vec<Value> = resp.json().await?;
        if rows.is_empty() {
            break;
        }

        let count = rows.len();
        for row in rows {
            if let Some(website) = row.get("website_url").and_then(|v| v.as_str()) {
                if let Some(domain) = extract_domain(website) {
                    // Strip www. prefix for normalized matching
                    let normalized = domain.strip_prefix("www.").unwrap_or(&domain).to_string();
                    domains.insert(normalized);
                }
            }
            if let Some(title) = row.get("title").and_then(|v| v.as_str()) {
                if !title.is_empty() {
                    names.insert(normalize_name_for_dedup(title));
                }
            }
        }

        if count < page_size as usize {
            break;
        }
        offset += page_size;
    }

    Ok((domains, names))
}

/// Normalize a company name for dedup: lowercase, strip common legal suffixes, trim.
fn normalize_name_for_dedup(name: &str) -> String {
    let mut n = name.to_lowercase();
    for suffix in &[
        " ltd", " limited", " gmbh", " sas", " bv", " ag", " sa", " srl", " nv", " inc",
        " llc", " co.", " corp", " corporation", " plc", " s.r.l.", " s.a.", " e.k.",
        " ohg", " kg", " ug",
    ] {
        if n.ends_with(suffix) {
            n = n[..n.len() - suffix.len()].to_string();
        }
    }
    n.trim().to_string()
}

/// Fetch low-quality listings from ForgeOS for audit re-enrichment.
/// Filters by quality threshold, requires website_url, and optionally filters by country.
pub async fn fetch_low_quality_listings(
    url: &str,
    service_key: &str,
    threshold: i32,
    countries: &[String],
) -> Result<Vec<Value>> {
    let client = reqwest::Client::new();
    let mut query_params: Vec<(&str, String)> = vec![
        ("select", "*".to_string()),
        ("data_quality_score", format!("lt.{}", threshold)),
        ("order", "data_quality_score.asc".to_string()),
        ("limit", "200".to_string()),
    ];

    if !countries.is_empty() {
        // Validate country codes are 2-letter alpha to prevent PostgREST filter injection
        for c in countries {
            if c.len() != 2 || !c.chars().all(|ch| ch.is_ascii_alphabetic()) {
                anyhow::bail!("Invalid country code: {}", c);
            }
        }
        let country_list = countries.join(",");
        query_params.push(("country", format!("in.({})", country_list)));
    }

    let resp = client
        .get(format!("{}/rest/v1/marketplace_listings", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .query(&query_params)
        .timeout(std::time::Duration::from_secs(30))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase fetch error {}: {}", status, body);
    }

    let listings: Vec<Value> = resp.json().await?;
    Ok(listings)
}

/// Extract domain from a URL string
fn extract_domain(url_str: &str) -> Option<String> {
    let url_str = url_str.trim();
    if url_str.is_empty() {
        return None;
    }

    // Add scheme if missing for URL parsing
    let full = if url_str.starts_with("http://") || url_str.starts_with("https://") {
        url_str.to_string()
    } else {
        format!("https://{}", url_str)
    };

    // Simple domain extraction
    full.split("//")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .map(|s| s.split(':').next().unwrap_or(s))
        .map(|s| s.to_lowercase())
        .filter(|s| s.contains('.'))
}

/// Parse a field that might be a JSON string (from SQLite) or already an array
fn parse_json_field(company: &Value, field: &str) -> Value {
    if let Some(val) = company.get(field) {
        if val.is_array() {
            return val.clone();
        }
        if let Some(s) = val.as_str() {
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                return parsed;
            }
        }
    }
    json!([])
}

/// Delete a listing from ForgeOS marketplace_listings by ID.
pub async fn delete_listing(url: &str, service_key: &str, listing_id: &str) -> Result<()> {
    validate_listing_id(listing_id)?;
    let client = reqwest::Client::new();
    let resp = client
        .delete(format!(
            "{}/rest/v1/marketplace_listings?id=eq.{}",
            url, listing_id
        ))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase delete error {}: {}", status, body);
    }

    Ok(())
}

/// Parse a JSONB array field directly from an already-parsed attributes object
fn parse_json_field_from_attrs(attrs: &Value, field: &str) -> Value {
    attrs
        .get(field)
        .filter(|v| v.is_array())
        .cloned()
        .unwrap_or_else(|| json!([]))
}

/// Create a claim token for a marketplace listing via ForgeOS Supabase.
/// Checks for existing valid (pending/clicked, non-expired) token first to avoid duplicates.
/// Token is auto-generated by Postgres default (two UUIDs concatenated, hyphens stripped).
pub async fn create_claim_token(
    url: &str,
    service_key: &str,
    listing_id: &str,
    email: &str,
) -> Result<String> {
    validate_listing_id(listing_id)?;
    let client = reqwest::Client::new();

    // Check for existing valid token (dedup)
    let check_resp = client
        .get(format!("{}/rest/v1/listing_claim_tokens", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .query(&[
            ("select", "token"),
            ("listing_id", &format!("eq.{}", listing_id)),
            ("email", &format!("eq.{}", email)),
            ("status", "in.(pending,clicked)"),
            ("expires_at", "gt.now()"),
            ("limit", "1"),
        ])
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if check_resp.status().is_success() {
        let existing: Vec<Value> = check_resp.json().await?;
        if let Some(token) = existing.first().and_then(|r| r.get("token")).and_then(|v| v.as_str()) {
            return Ok(token.to_string());
        }
    }

    // Insert new token — Postgres generates the token via DEFAULT
    let body = json!({
        "listing_id": listing_id,
        "email": email,
    });

    let resp = client
        .post(format!("{}/rest/v1/listing_claim_tokens", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .header("Content-Type", "application/json")
        .header("Prefer", "return=representation")
        .json(&body)
        .timeout(std::time::Duration::from_secs(10))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase create claim token error {}: {}", status, resp_body);
    }

    let results: Vec<Value> = resp.json().await?;
    let token = results
        .first()
        .and_then(|r| r.get("token"))
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("No token returned from Supabase"))?
        .to_string();

    Ok(token)
}

/// Query claim token statuses from ForgeOS Supabase listing_claim_tokens table.
/// Returns Vec of (token, status) pairs.
pub async fn get_claim_token_statuses(
    url: &str,
    service_key: &str,
    tokens: &[String],
) -> Result<Vec<(String, String)>> {
    if tokens.is_empty() {
        return Ok(Vec::new());
    }

    let client = reqwest::Client::new();
    let mut results = Vec::new();

    // Batch in chunks of 50 to avoid URL length limits
    for chunk in tokens.chunks(50) {
        let token_list = chunk.join(",");
        let resp = client
            .get(format!("{}/rest/v1/listing_claim_tokens", url))
            .header("apikey", service_key)
            .header("Authorization", format!("Bearer {}", service_key))
            .query(&[
                ("select", "token,status"),
                ("token", &format!("in.({})", token_list)),
            ])
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await?;

        if resp.status().is_success() {
            let rows: Vec<Value> = resp.json().await?;
            for row in rows {
                let token = row.get("token").and_then(|v| v.as_str()).unwrap_or("").to_string();
                let status = row.get("status").and_then(|v| v.as_str()).unwrap_or("pending").to_string();
                if !token.is_empty() {
                    results.push((token, status));
                }
            }
        }
    }

    Ok(results)
}

/// Push a technique enrichment record to ForgeOS manufacturing_technique_enrichments table.
/// Uses UPSERT (on technique_slug conflict, update).
pub async fn push_technique_enrichment(
    url: &str,
    service_key: &str,
    record: &Value,
) -> Result<()> {
    let client = reqwest::Client::new();

    let technique_slug = record.get("technique_slug").and_then(|v| v.as_str()).unwrap_or("");
    if technique_slug.is_empty() {
        anyhow::bail!("technique_slug is required");
    }

    // Parse JSON string fields back into objects for JSONB columns
    let parse_json_field = |field: &str| -> Value {
        record
            .get(field)
            .and_then(|v| {
                if v.is_string() {
                    serde_json::from_str(v.as_str().unwrap_or("{}")).ok()
                } else if v.is_null() {
                    None
                } else {
                    Some(v.clone())
                }
            })
            .unwrap_or(json!({}))
    };

    let parse_json_array_field = |field: &str| -> Value {
        record
            .get(field)
            .and_then(|v| {
                if v.is_string() {
                    serde_json::from_str(v.as_str().unwrap_or("[]")).ok()
                } else if v.is_null() {
                    None
                } else {
                    Some(v.clone())
                }
            })
            .unwrap_or(json!([]))
    };

    let body = json!({
        "technique_slug": technique_slug,
        "article_markdown": record.get("article_markdown").and_then(|v| v.as_str()),
        "real_world_tolerances": parse_json_field("real_world_tolerances"),
        "real_world_materials": parse_json_array_field("real_world_materials"),
        "real_world_equipment": parse_json_array_field("real_world_equipment"),
        "real_world_surface_finishes": parse_json_field("real_world_surface_finishes"),
        "typical_batch_sizes": parse_json_field("typical_batch_sizes"),
        "tips_and_insights": parse_json_array_field("tips_and_insights"),
        "common_applications": parse_json_array_field("common_applications"),
        "supplier_count": record.get("supplier_count").and_then(|v| v.as_i64()).unwrap_or(0),
        "source_company_ids": parse_json_array_field("source_company_ids"),
        "source": "nightshift",
        "updated_at": chrono::Utc::now().to_rfc3339(),
    });

    let resp = client
        .post(format!("{}/rest/v1/manufacturing_technique_enrichments", url))
        .header("apikey", service_key)
        .header("Authorization", format!("Bearer {}", service_key))
        .header("Content-Type", "application/json")
        .header("Prefer", "resolution=merge-duplicates")
        .json(&body)
        .timeout(std::time::Duration::from_secs(15))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let resp_body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Supabase push technique enrichment error {}: {}", status, resp_body);
    }

    Ok(())
}
