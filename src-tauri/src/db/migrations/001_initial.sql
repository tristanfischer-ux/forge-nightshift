-- Companies: core tracking table for discovered manufacturers
CREATE TABLE IF NOT EXISTS companies (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    website_url TEXT,
    domain TEXT,
    country TEXT,
    city TEXT,

    -- Source tracking
    source TEXT DEFAULT 'brave',
    source_url TEXT,
    source_query TEXT,
    raw_snippet TEXT,

    -- Enrichment data
    description TEXT,
    category TEXT,
    subcategory TEXT,
    specialties TEXT, -- JSON array
    certifications TEXT, -- JSON array
    company_size TEXT,
    industries TEXT, -- JSON array
    year_founded INTEGER,

    -- Contact info
    contact_name TEXT,
    contact_email TEXT,
    contact_title TEXT,
    contact_phone TEXT,

    -- Scoring
    relevance_score INTEGER DEFAULT 0,
    enrichment_quality INTEGER DEFAULT 0,

    -- Pipeline status: discovered → enriching → enriched → approved → pushed → rejected → error
    status TEXT DEFAULT 'discovered',

    -- ForgeOS link
    supabase_listing_id TEXT,
    attributes_json TEXT, -- JSON matching ForgeOS marketplace_listings.attributes

    created_at DATETIME DEFAULT (datetime('now')),
    updated_at DATETIME DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_companies_domain ON companies(domain) WHERE domain IS NOT NULL AND domain != '';
CREATE INDEX IF NOT EXISTS idx_companies_status ON companies(status);
CREATE INDEX IF NOT EXISTS idx_companies_country ON companies(country);

-- Emails: outreach tracking
CREATE TABLE IF NOT EXISTS emails (
    id TEXT PRIMARY KEY,
    company_id TEXT NOT NULL REFERENCES companies(id),
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    to_email TEXT NOT NULL,
    from_email TEXT,
    language TEXT DEFAULT 'en',

    -- Status: draft → approved → sending → sent → opened → replied → bounced → failed
    status TEXT DEFAULT 'draft',

    -- Resend tracking
    resend_id TEXT,
    sent_at DATETIME,
    opened_at DATETIME,
    replied_at DATETIME,
    bounced_at DATETIME,

    -- Metadata
    personalization_notes TEXT,

    created_at DATETIME DEFAULT (datetime('now')),
    updated_at DATETIME DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_emails_status ON emails(status);
CREATE INDEX IF NOT EXISTS idx_emails_company ON emails(company_id);

-- Jobs: pipeline run tracking
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    stages TEXT NOT NULL, -- comma-separated: research,enrich,push,outreach,report
    status TEXT DEFAULT 'pending', -- pending, running, completed, failed, cancelled
    summary TEXT, -- JSON summary of results

    started_at DATETIME DEFAULT (datetime('now')),
    completed_at DATETIME,
    created_at DATETIME DEFAULT (datetime('now')),
    updated_at DATETIME DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);

-- Run log: detailed activity log
CREATE TABLE IF NOT EXISTS run_log (
    id TEXT PRIMARY KEY,
    job_id TEXT REFERENCES jobs(id),
    stage TEXT, -- research, enrich, push, outreach, report
    level TEXT DEFAULT 'info', -- info, warn, error
    message TEXT NOT NULL,

    created_at DATETIME DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_run_log_job ON run_log(job_id);

-- Search history: avoid duplicate queries
CREATE TABLE IF NOT EXISTS search_history (
    id TEXT PRIMARY KEY,
    query TEXT NOT NULL,
    country TEXT,
    result_count INTEGER DEFAULT 0,

    created_at DATETIME DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_search_query ON search_history(query);

-- Config: key-value settings store
CREATE TABLE IF NOT EXISTS config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at DATETIME DEFAULT (datetime('now'))
);

-- Default config values (pre-populated from ForgeOS)
INSERT OR IGNORE INTO config (key, value) VALUES
    ('target_countries', '["DE","FR","NL","BE","IT","GB"]'),
    ('schedule_time', '23:00'),
    ('daily_email_limit', '30'),
    ('relevance_threshold', '60'),
    ('categories_per_run', '12'),
    ('research_model', 'qwen3.5:9b'),
    ('enrich_model', 'qwen3.5:27b-q4_K_M'),
    ('outreach_model', 'qwen3.5:27b-q4_K_M'),
    ('ollama_url', 'http://localhost:11434'),
    ('from_email', 'ForgeOS <noreply@fractionalforge.app>'),
    ('brave_api_key', ''),
    ('resend_api_key', ''),
    ('supabase_url', ''),
    ('supabase_service_key', ''),
    ('foundry_id', ''),
    ('companies_house_api_key', ''),
    ('directory_search_enabled', 'true');
