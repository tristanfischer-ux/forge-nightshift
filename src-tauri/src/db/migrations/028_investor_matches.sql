CREATE TABLE IF NOT EXISTS investor_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT NOT NULL,
    investor_listing_id TEXT NOT NULL,
    investor_name TEXT,
    investor_sector_focus TEXT,
    investor_stage_focus TEXT,
    investor_geo_focus TEXT,
    match_score INTEGER DEFAULT 0,
    match_reasons TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    UNIQUE(company_id, investor_listing_id),
    FOREIGN KEY (company_id) REFERENCES companies(id)
);
CREATE INDEX IF NOT EXISTS idx_investor_matches_company ON investor_matches(company_id);
CREATE INDEX IF NOT EXISTS idx_investor_matches_score ON investor_matches(match_score DESC);
