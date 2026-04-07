CREATE TABLE IF NOT EXISTS search_profiles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    domain TEXT NOT NULL,
    categories_json TEXT NOT NULL,
    target_countries_json TEXT DEFAULT '["GB"]',
    is_active INTEGER DEFAULT 1,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now'))
);

ALTER TABLE companies ADD COLUMN search_profile_id TEXT DEFAULT 'manufacturing';
