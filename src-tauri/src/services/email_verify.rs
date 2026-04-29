//! Email permutation + SMTP verification.
//!
//! Used by the contact extraction pipeline when it has a NAME but no EMAIL.
//! We generate likely permutations, then probe each via SMTP RCPT TO to find
//! which one the company actually accepts.
//!
//! Public API:
//! - `permute_emails(first, last, domain)` — pure, deterministic, no I/O
//! - `verify_email(email)` — async, MX lookup + SMTP probe
//! - `is_catch_all(domain)` — async, probes a bogus address (cached per-domain)
//! - `find_working_email(first, last, domain)` — convenience: try in order

use hickory_resolver::config::{ResolverConfig, ResolverOpts};
use hickory_resolver::TokioAsyncResolver;
use rand::Rng;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::OnceCell;
use tokio::time::{sleep, timeout, Instant};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
const READ_TIMEOUT: Duration = Duration::from_secs(5);
const PER_DOMAIN_PROBE_INTERVAL: Duration = Duration::from_secs(1);
const HELO_HOST: &str = "nightshift.local";
const MAIL_FROM: &str = "verify@nightshift.local";

#[derive(Debug, Clone, PartialEq)]
pub enum EmailStatus {
    /// SMTP RCPT TO returned 250 — mailbox exists.
    Verified,
    /// SMTP RCPT TO returned 5xx — mailbox doesn't exist.
    Invalid,
    /// Domain accepts everything — can't verify specific mailbox.
    CatchAll,
    /// SMTP probe blocked / timed out / greylisted.
    Unverifiable,
    /// No MX record for the domain.
    InvalidDomain,
}

// ---------------------------------------------------------------------------
// Permutation
// ---------------------------------------------------------------------------

/// Strip accents and non-alphanumerics, lowercase.
/// Keeps only [a-z0-9] (caller adds dot/underscore in the right places).
fn normalize_token(input: &str) -> String {
    // Take the FIRST whitespace-separated word only.
    let first_word = input.split_whitespace().next().unwrap_or("");

    let mut out = String::with_capacity(first_word.len());
    for ch in first_word.chars() {
        let folded = fold_accent(ch);
        for fc in folded.chars() {
            let lc = fc.to_ascii_lowercase();
            if lc.is_ascii_alphanumeric() {
                out.push(lc);
            }
        }
    }
    out
}

/// Simple ASCII accent fold for the most common Latin diacritics.
/// Returns a small string (usually 1 char, sometimes 2 for ligatures).
fn fold_accent(ch: char) -> String {
    match ch {
        'á' | 'à' | 'â' | 'ã' | 'ä' | 'å' | 'ā' | 'ă' | 'ą' => "a".into(),
        'Á' | 'À' | 'Â' | 'Ã' | 'Ä' | 'Å' | 'Ā' | 'Ă' | 'Ą' => "A".into(),
        'ç' | 'ć' | 'č' | 'ĉ' | 'ċ' => "c".into(),
        'Ç' | 'Ć' | 'Č' | 'Ĉ' | 'Ċ' => "C".into(),
        'é' | 'è' | 'ê' | 'ë' | 'ē' | 'ĕ' | 'ė' | 'ę' | 'ě' => "e".into(),
        'É' | 'È' | 'Ê' | 'Ë' | 'Ē' | 'Ĕ' | 'Ė' | 'Ę' | 'Ě' => "E".into(),
        'í' | 'ì' | 'î' | 'ï' | 'ī' | 'ĭ' | 'į' => "i".into(),
        'Í' | 'Ì' | 'Î' | 'Ï' | 'Ī' | 'Ĭ' | 'Į' => "I".into(),
        'ñ' | 'ń' | 'ň' | 'ņ' => "n".into(),
        'Ñ' | 'Ń' | 'Ň' | 'Ņ' => "N".into(),
        'ó' | 'ò' | 'ô' | 'õ' | 'ö' | 'ø' | 'ō' | 'ŏ' | 'ő' => "o".into(),
        'Ó' | 'Ò' | 'Ô' | 'Õ' | 'Ö' | 'Ø' | 'Ō' | 'Ŏ' | 'Ő' => "O".into(),
        'ś' | 'š' | 'ş' | 'ŝ' => "s".into(),
        'Ś' | 'Š' | 'Ş' | 'Ŝ' => "S".into(),
        'ú' | 'ù' | 'û' | 'ü' | 'ū' | 'ŭ' | 'ů' | 'ű' | 'ų' => "u".into(),
        'Ú' | 'Ù' | 'Û' | 'Ü' | 'Ū' | 'Ŭ' | 'Ů' | 'Ű' | 'Ų' => "U".into(),
        'ý' | 'ÿ' | 'ŷ' => "y".into(),
        'Ý' | 'Ÿ' | 'Ŷ' => "Y".into(),
        'ź' | 'ž' | 'ż' => "z".into(),
        'Ź' | 'Ž' | 'Ż' => "Z".into(),
        'ß' => "ss".into(),
        'æ' => "ae".into(),
        'Æ' => "AE".into(),
        'œ' => "oe".into(),
        'Œ' => "OE".into(),
        'ð' => "d".into(),
        'Ð' => "D".into(),
        'þ' => "th".into(),
        'Þ' => "Th".into(),
        'ł' => "l".into(),
        'Ł' => "L".into(),
        other => other.to_string(),
    }
}

fn normalize_domain(domain: &str) -> String {
    domain.trim().to_ascii_lowercase()
}

/// Generate permutations in lowest-noise-first order.
///
/// Patterns (in order):
/// 1. firstname.lastname
/// 2. firstname
/// 3. firstinitial.lastname (j.smith)
/// 4. firstinitiallastname (jsmith)
/// 5. firstname_lastname
/// 6. lastname.firstname
/// 7. lastname
/// 8. firstnamelastname (johnsmith)
pub fn permute_emails(first_name: &str, last_name: &str, domain: &str) -> Vec<String> {
    let first = normalize_token(first_name);
    let last = normalize_token(last_name);
    let dom = normalize_domain(domain);

    if dom.is_empty() {
        return Vec::new();
    }

    let mut out = Vec::with_capacity(8);

    // Need at least one name component to do anything.
    let have_first = !first.is_empty();
    let have_last = !last.is_empty();

    if have_first && have_last {
        out.push(format!("{}.{}@{}", first, last, dom));
    }
    if have_first {
        out.push(format!("{}@{}", first, dom));
    }
    if have_first && have_last {
        let fi = first.chars().next().unwrap();
        out.push(format!("{}.{}@{}", fi, last, dom));
        out.push(format!("{}{}@{}", fi, last, dom));
        out.push(format!("{}_{}@{}", first, last, dom));
        out.push(format!("{}.{}@{}", last, first, dom));
    }
    if have_last {
        out.push(format!("{}@{}", last, dom));
    }
    if have_first && have_last {
        out.push(format!("{}{}@{}", first, last, dom));
    }

    out
}

// ---------------------------------------------------------------------------
// DNS resolver (shared, lazily initialised)
// ---------------------------------------------------------------------------

static RESOLVER: OnceCell<TokioAsyncResolver> = OnceCell::const_new();

async fn resolver() -> &'static TokioAsyncResolver {
    RESOLVER
        .get_or_init(|| async {
            // Cloudflare + Google fallback. system_conf() can be flaky on
            // sandboxed macOS builds.
            TokioAsyncResolver::tokio(ResolverConfig::cloudflare(), ResolverOpts::default())
        })
        .await
}

/// Look up MX records, sorted by preference (lowest = highest priority).
/// Returns the host names of MX targets. Empty vec if none.
async fn lookup_mx(domain: &str) -> Vec<String> {
    let resolver = resolver().await;
    match resolver.mx_lookup(domain).await {
        Ok(resp) => {
            let mut hosts: Vec<(u16, String)> = resp
                .iter()
                .map(|mx| (mx.preference(), mx.exchange().to_ascii()))
                .collect();
            hosts.sort_by_key(|(pref, _)| *pref);
            hosts
                .into_iter()
                .map(|(_, host)| host.trim_end_matches('.').to_string())
                .collect()
        }
        Err(_) => Vec::new(),
    }
}

// ---------------------------------------------------------------------------
// Per-domain rate limiter (1 probe per domain per second)
// ---------------------------------------------------------------------------

static LAST_PROBE: OnceCell<Mutex<HashMap<String, Instant>>> = OnceCell::const_new();

async fn rate_limit(domain: &str) {
    let map = LAST_PROBE
        .get_or_init(|| async { Mutex::new(HashMap::new()) })
        .await;

    let wait = {
        let mut guard = map.lock().unwrap();
        let now = Instant::now();
        let wait = match guard.get(domain) {
            Some(last) => {
                let elapsed = now.saturating_duration_since(*last);
                if elapsed < PER_DOMAIN_PROBE_INTERVAL {
                    PER_DOMAIN_PROBE_INTERVAL - elapsed
                } else {
                    Duration::ZERO
                }
            }
            None => Duration::ZERO,
        };
        // Reserve the slot now (set the timestamp to now+wait) so concurrent
        // callers space themselves out instead of all sleeping the same amount.
        guard.insert(domain.to_string(), now + wait);
        wait
    };

    if !wait.is_zero() {
        sleep(wait).await;
    }
}

// ---------------------------------------------------------------------------
// SMTP probe
// ---------------------------------------------------------------------------

/// Outcome of a single RCPT TO probe.
#[derive(Debug, Clone, PartialEq)]
enum RcptResult {
    Accepted,     // 250
    Rejected,     // 5xx — mailbox doesn't exist
    Unverifiable, // 4xx, timeout, network error, anything else
}

/// Connect to one MX host and probe one address.
async fn probe_mx(mx_host: &str, email: &str) -> RcptResult {
    // Skip non-ASCII emails — rare in B2B and SMTPUTF8 negotiation is messy.
    if !email.is_ascii() {
        return RcptResult::Unverifiable;
    }

    let connect = TcpStream::connect(format!("{}:25", mx_host));
    let stream = match timeout(CONNECT_TIMEOUT, connect).await {
        Ok(Ok(s)) => s,
        _ => return RcptResult::Unverifiable,
    };

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);

    // Read the server greeting. Expect 2xx.
    if !read_smtp_response(&mut reader, '2').await {
        return RcptResult::Unverifiable;
    }

    // EHLO
    if write_half
        .write_all(format!("EHLO {}\r\n", HELO_HOST).as_bytes())
        .await
        .is_err()
    {
        return RcptResult::Unverifiable;
    }
    if !read_smtp_response(&mut reader, '2').await {
        return RcptResult::Unverifiable;
    }

    // MAIL FROM
    if write_half
        .write_all(format!("MAIL FROM:<{}>\r\n", MAIL_FROM).as_bytes())
        .await
        .is_err()
    {
        return RcptResult::Unverifiable;
    }
    if !read_smtp_response(&mut reader, '2').await {
        return RcptResult::Unverifiable;
    }

    // RCPT TO — this is the actual probe.
    if write_half
        .write_all(format!("RCPT TO:<{}>\r\n", email).as_bytes())
        .await
        .is_err()
    {
        return RcptResult::Unverifiable;
    }
    let rcpt = match read_smtp_status(&mut reader).await {
        Some(code) => code,
        None => return RcptResult::Unverifiable,
    };

    // Politely close.
    let _ = write_half.write_all(b"QUIT\r\n").await;

    match rcpt {
        250..=259 => RcptResult::Accepted,
        // 550 user unknown, 551 user not local, 553 invalid mailbox name,
        // 552/554 also treat as rejection (over quota / generic 5xx).
        500..=599 => RcptResult::Rejected,
        // 4xx — greylist, temp fail, mailbox busy.
        _ => RcptResult::Unverifiable,
    }
}

/// Read SMTP multi-line response, return the numeric status code.
/// SMTP responses look like:
///   250-First line
///   250-Second line
///   250 Last line
async fn read_smtp_status(reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>) -> Option<u16> {
    let mut last_code: Option<u16> = None;
    loop {
        let mut line = String::new();
        let read = timeout(READ_TIMEOUT, reader.read_line(&mut line)).await;
        match read {
            Ok(Ok(0)) => return last_code, // EOF
            Ok(Ok(_)) => {}
            _ => return None,
        }
        if line.len() < 4 {
            return None;
        }
        let code: u16 = line[..3].parse().ok()?;
        last_code = Some(code);
        // 4th char is '-' for continuation, ' ' (or '\r'/'\n') for last line.
        let sep = line.as_bytes().get(3).copied().unwrap_or(b' ');
        if sep != b'-' {
            return Some(code);
        }
    }
}

/// Convenience: read a response, check it starts with the expected leading char.
async fn read_smtp_response(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
    expect_leading: char,
) -> bool {
    match read_smtp_status(reader).await {
        Some(code) => code.to_string().starts_with(expect_leading),
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Public verification API
// ---------------------------------------------------------------------------

/// Extract the domain part of an email (after the last `@`).
fn domain_of(email: &str) -> Option<&str> {
    email.rsplit_once('@').map(|(_, d)| d)
}

/// Verify a single email address via MX lookup + SMTP RCPT TO probe.
pub async fn verify_email(email: &str) -> EmailStatus {
    let domain = match domain_of(email) {
        Some(d) if !d.is_empty() => d.to_ascii_lowercase(),
        _ => return EmailStatus::InvalidDomain,
    };

    let mx_hosts = lookup_mx(&domain).await;
    if mx_hosts.is_empty() {
        return EmailStatus::InvalidDomain;
    }

    // Catch-all check first — if the domain accepts anything, the answer
    // is meaningless and we shouldn't waste a probe slot on it.
    if is_catch_all(&domain).await {
        return EmailStatus::CatchAll;
    }

    rate_limit(&domain).await;

    // Try MX hosts in priority order. Stop on the first definitive answer.
    let mut last = RcptResult::Unverifiable;
    for mx in &mx_hosts {
        let res = probe_mx(mx, email).await;
        match res {
            RcptResult::Accepted => return EmailStatus::Verified,
            RcptResult::Rejected => return EmailStatus::Invalid,
            RcptResult::Unverifiable => last = res,
        }
    }
    let _ = last;
    EmailStatus::Unverifiable
}

// Catch-all cache — same domain shouldn't be probed twice in one pipeline run.
static CATCH_ALL_CACHE: OnceCell<Mutex<HashMap<String, bool>>> = OnceCell::const_new();

/// Probe a known-bogus address to detect catch-all domains.
/// Returns true if the domain accepts ANY email.
pub async fn is_catch_all(domain: &str) -> bool {
    let domain = domain.trim().to_ascii_lowercase();

    // Cache check.
    let cache = CATCH_ALL_CACHE
        .get_or_init(|| async { Mutex::new(HashMap::new()) })
        .await;
    if let Some(cached) = cache.lock().unwrap().get(&domain).copied() {
        return cached;
    }

    let mx_hosts = lookup_mx(&domain).await;
    if mx_hosts.is_empty() {
        // No MX → can't be catch-all (and verify_email will return InvalidDomain).
        cache.lock().unwrap().insert(domain.clone(), false);
        return false;
    }

    // Random unguessable local-part.
    let suffix: u64 = rand::thread_rng().gen();
    let bogus = format!("xyzabc123notreal{:x}@{}", suffix, domain);

    rate_limit(&domain).await;

    let mut catch_all = false;
    for mx in &mx_hosts {
        match probe_mx(mx, &bogus).await {
            RcptResult::Accepted => {
                catch_all = true;
                break;
            }
            RcptResult::Rejected => {
                catch_all = false;
                break;
            }
            RcptResult::Unverifiable => continue,
        }
    }

    cache.lock().unwrap().insert(domain, catch_all);
    catch_all
}

/// Try every permutation in order; return the first Verified address.
/// Returns None if none verify, or if the domain is catch-all.
pub async fn find_working_email(
    first_name: &str,
    last_name: &str,
    domain: &str,
) -> Option<String> {
    let dom = normalize_domain(domain);
    if dom.is_empty() {
        return None;
    }

    // No MX → nothing to try.
    if lookup_mx(&dom).await.is_empty() {
        return None;
    }

    // Catch-all → can't trust any answer.
    if is_catch_all(&dom).await {
        return None;
    }

    for candidate in permute_emails(first_name, last_name, &dom) {
        match verify_email(&candidate).await {
            EmailStatus::Verified => return Some(candidate),
            EmailStatus::Invalid => continue,
            // Domain-level results — no point trying further permutations.
            EmailStatus::CatchAll | EmailStatus::InvalidDomain => return None,
            // Transient — skip this one but keep trying others.
            EmailStatus::Unverifiable => continue,
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Tests (no network)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn permute_john_smith_acme() {
        let got = permute_emails("John", "Smith", "acme.com");
        let want = vec![
            "john.smith@acme.com",
            "john@acme.com",
            "j.smith@acme.com",
            "jsmith@acme.com",
            "john_smith@acme.com",
            "smith.john@acme.com",
            "smith@acme.com",
            "johnsmith@acme.com",
        ];
        assert_eq!(got, want);
    }

    #[test]
    fn permute_strips_accents() {
        let got = permute_emails("José", "Müller", "x.com");
        // All output must be ASCII.
        for addr in &got {
            assert!(addr.is_ascii(), "non-ASCII in: {addr}");
        }
        // First permutation should be the dotted form, accents folded.
        assert_eq!(got[0], "jose.muller@x.com");
        // ß handling on a separate name.
        let beta = permute_emails("Hans", "Straße", "x.com");
        assert_eq!(beta[0], "hans.strasse@x.com");
    }

    #[test]
    fn permute_takes_first_word_only() {
        let got = permute_emails("Mary Jane", "Smith Jones", "x.com");
        assert_eq!(got[0], "mary.smith@x.com");
        assert_eq!(got[1], "mary@x.com");
        assert_eq!(got[2], "m.smith@x.com");
        assert_eq!(got[3], "msmith@x.com");
        assert_eq!(got[4], "mary_smith@x.com");
        assert_eq!(got[5], "smith.mary@x.com");
        assert_eq!(got[6], "smith@x.com");
        assert_eq!(got[7], "marysmith@x.com");
    }

    #[test]
    fn permute_empty_domain_returns_empty() {
        assert!(permute_emails("John", "Smith", "").is_empty());
    }

    #[test]
    fn permute_lowercases_and_strips_punct() {
        let got = permute_emails("Jean-Luc", "O'Brien", "X.COM");
        // "Jean-Luc" -> normalized to "jeanluc" (hyphen stripped, single word
        // because there's no whitespace), "O'Brien" -> "obrien".
        assert_eq!(got[0], "jeanluc.obrien@x.com");
    }
}
