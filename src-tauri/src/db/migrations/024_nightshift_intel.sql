-- Migration 023: nightshift_intel table for private director analysis & acquisition scoring
-- PRIVACY: This data is NEVER pushed to ForgeOS. Private M&A intelligence only.

CREATE TABLE IF NOT EXISTS nightshift_intel (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT UNIQUE REFERENCES companies(id),

    -- Director data
    directors_json TEXT,
    director_count INTEGER,
    avg_director_age REAL,
    oldest_director_age INTEGER,
    youngest_director_age INTEGER,
    founder_director_name TEXT,
    founder_director_age INTEGER,
    founder_director_tenure_years INTEGER,

    -- Ownership structure
    psc_json TEXT,
    psc_count INTEGER,
    single_owner INTEGER DEFAULT 0,
    owner_is_director INTEGER DEFAULT 0,
    majority_control_nature TEXT,

    -- Succession signals
    no_young_directors INTEGER DEFAULT 0,
    recent_director_changes INTEGER DEFAULT 0,
    years_trading INTEGER,
    has_company_secretary INTEGER DEFAULT 0,

    -- Financial signals
    accounts_type TEXT,
    last_accounts_date TEXT,
    accounts_overdue INTEGER DEFAULT 0,
    has_charges INTEGER DEFAULT 0,
    has_insolvency_history INTEGER DEFAULT 0,
    company_status TEXT,
    sic_codes TEXT,

    -- Composite scores
    acquisition_readiness_score INTEGER DEFAULT 0,
    acquisition_signals_json TEXT,

    -- Ownership structure label
    ownership_structure TEXT,

    -- Source tracking
    age_source TEXT DEFAULT 'unknown',
    ch_fetched_at TEXT,
    estimated_at TEXT,

    -- Meta
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_intel_company_id ON nightshift_intel(company_id);
CREATE INDEX IF NOT EXISTS idx_intel_acquisition_score ON nightshift_intel(acquisition_readiness_score);
CREATE INDEX IF NOT EXISTS idx_intel_oldest_director ON nightshift_intel(oldest_director_age);
