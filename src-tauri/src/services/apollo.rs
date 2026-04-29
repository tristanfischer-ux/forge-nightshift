use anyhow::Result;
use serde::Deserialize;

// FIX 2026-04-22: switched from /v1/mixed_people/search (deprecated, returns
// HTTP 422 with a redirect to /api/v1/mixed_people/api_search) to per-name
// /api/v1/people/match. Search-style endpoints on the free tier return
// obfuscated last names ("Ar***s") and `email: null`, which is useless for
// outreach. /people/match takes (first, last, organization_name) → returns
// the verified email + linkedin. We pass LLM-extracted names from the website
// as input, so Apollo becomes an enrichment step, not a discovery step.
const MATCH_URL: &str = "https://api.apollo.io/api/v1/people/match";
const DEFAULT_TIMEOUT_SECS: u64 = 15;
const RETRY_BACKOFF_MS: u64 = 500;

/// Decision-maker contact returned by Apollo for outreach.
#[derive(Debug, Clone)]
pub struct ApolloContact {
    pub name: String,
    pub title: String,
    pub email: String,
    pub linkedin_url: String,
    pub seniority: String,
    pub department: String,
}

#[derive(Debug, Deserialize)]
struct MatchResponse {
    #[serde(default)]
    person: Option<ApolloPerson>,
}

#[derive(Debug, Deserialize)]
struct ApolloPerson {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    first_name: Option<String>,
    #[serde(default)]
    last_name: Option<String>,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    email_status: Option<String>,
    #[serde(default)]
    linkedin_url: Option<String>,
    #[serde(default)]
    seniority: Option<String>,
    #[serde(default)]
    departments: Option<Vec<String>>,
    #[serde(default)]
    subdepartments: Option<Vec<String>>,
}

/// Look up a known person at a known company via Apollo's /people/match.
/// Returns Some(contact) if Apollo had a verified email, None if no match.
///
/// Returns Err on 401 (bad key) and 402 (credit exhausted) — those are
/// unrecoverable per-batch failures that should bubble up. Other failures
/// (404, 429, 5xx, network) return Ok(None) so a single bad lookup does not
/// halt the wider batch.
pub async fn match_person(
    api_key: &str,
    first_name: &str,
    last_name: &str,
    organization_name: &str,
) -> Result<Option<ApolloContact>> {
    if api_key.is_empty() {
        anyhow::bail!("Apollo API key not configured");
    }
    if first_name.trim().is_empty() || last_name.trim().is_empty() {
        return Ok(None);
    }

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(DEFAULT_TIMEOUT_SECS))
        .build()?;

    let body = serde_json::json!({
        "first_name": first_name,
        "last_name": last_name,
        "organization_name": organization_name,
    });

    let send = || async {
        client
            .post(MATCH_URL)
            .header("X-Api-Key", api_key)
            .header("Content-Type", "application/json")
            .header("Cache-Control", "no-cache")
            .json(&body)
            .send()
            .await
    };

    let resp = match send().await {
        Ok(r) => r,
        Err(e) => {
            log::warn!(
                "[Apollo] Match network error for {} {} @ {} (retrying): {}",
                first_name, last_name, organization_name, e
            );
            tokio::time::sleep(std::time::Duration::from_millis(RETRY_BACKOFF_MS)).await;
            send().await?
        }
    };

    let status = resp.status();
    if status.as_u16() == 401 {
        anyhow::bail!("Apollo: invalid Apollo key (401)");
    }
    if status.as_u16() == 402 {
        anyhow::bail!("Apollo: credit exhausted (402)");
    }
    if status.as_u16() == 429 {
        log::warn!("[Apollo] Rate limited matching {} {} @ {}", first_name, last_name, organization_name);
        return Ok(None);
    }
    if !status.is_success() {
        let body_text = resp.text().await.unwrap_or_default();
        log::warn!(
            "[Apollo] Match non-2xx ({}) for {} {} @ {}: {}",
            status, first_name, last_name, organization_name,
            body_text.chars().take(200).collect::<String>(),
        );
        return Ok(None);
    }

    let parsed: MatchResponse = match resp.json().await {
        Ok(p) => p,
        Err(e) => {
            log::warn!("[Apollo] Match JSON parse error: {}", e);
            return Ok(None);
        }
    };

    let person = match parsed.person {
        Some(p) => p,
        None => return Ok(None),
    };

    Ok(person_to_contact(person))
}

fn person_to_contact(p: ApolloPerson) -> Option<ApolloContact> {
    // Only return contacts with a real email — outreach is the whole point.
    let email = p.email.unwrap_or_default();
    if email.is_empty() || email.eq_ignore_ascii_case("email_not_unlocked@domain.com") {
        return None;
    }
    // Apollo's email_status: "verified", "guessed", "unverified", "bounced",
    // "unknown". Skip anything we know is bad.
    if let Some(status) = p.email_status.as_deref() {
        if status.eq_ignore_ascii_case("bounced") || status.eq_ignore_ascii_case("invalid") {
            return None;
        }
    }

    let name = p.name.unwrap_or_else(|| {
        let first = p.first_name.unwrap_or_default();
        let last = p.last_name.unwrap_or_default();
        format!("{} {}", first, last).trim().to_string()
    });
    if name.is_empty() {
        return None;
    }

    let department = p
        .departments
        .as_ref()
        .and_then(|d| d.first().cloned())
        .or_else(|| p.subdepartments.as_ref().and_then(|d| d.first().cloned()))
        .unwrap_or_default();

    Some(ApolloContact {
        name,
        title: p.title.unwrap_or_default(),
        email,
        linkedin_url: p.linkedin_url.unwrap_or_default(),
        seniority: p.seniority.unwrap_or_default(),
        department,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_match_response_with_full_person() {
        let raw = r#"{
            "person": {
                "name": "Tim Cook",
                "first_name": "Tim",
                "last_name": "Cook",
                "title": "CEO",
                "email": "tcook@apple.com",
                "email_status": "verified",
                "linkedin_url": "https://www.linkedin.com/in/tim-cook",
                "seniority": "c_suite",
                "departments": ["c_suite"],
                "subdepartments": ["executive"]
            }
        }"#;

        let parsed: MatchResponse = serde_json::from_str(raw).expect("should parse");
        let contact = parsed.person.and_then(person_to_contact).expect("should yield a contact");
        assert_eq!(contact.name, "Tim Cook");
        assert_eq!(contact.email, "tcook@apple.com");
        assert_eq!(contact.title, "CEO");
        assert_eq!(contact.department, "c_suite");
    }

    #[test]
    fn rejects_match_with_unlocked_email_sentinel() {
        let raw = r#"{
            "person": {
                "name": "Locked Person",
                "title": "CEO",
                "email": "email_not_unlocked@domain.com"
            }
        }"#;
        let parsed: MatchResponse = serde_json::from_str(raw).expect("should parse");
        assert!(parsed.person.and_then(person_to_contact).is_none());
    }

    #[test]
    fn rejects_match_with_bounced_email_status() {
        let raw = r#"{
            "person": {
                "name": "Bounced Person",
                "email": "bp@example.com",
                "email_status": "bounced"
            }
        }"#;
        let parsed: MatchResponse = serde_json::from_str(raw).expect("should parse");
        assert!(parsed.person.and_then(person_to_contact).is_none());
    }

    #[test]
    fn handles_match_response_with_no_person() {
        let raw = r#"{}"#;
        let parsed: MatchResponse = serde_json::from_str(raw).expect("should parse");
        assert!(parsed.person.is_none());
    }
}
