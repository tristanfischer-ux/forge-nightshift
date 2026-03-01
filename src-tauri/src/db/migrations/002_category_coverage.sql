-- Category coverage tracking: ensures diverse searches across categories and countries
CREATE TABLE IF NOT EXISTS category_coverage (
    id TEXT PRIMARY KEY,
    category_id TEXT NOT NULL,
    country TEXT NOT NULL,
    searches_run INTEGER DEFAULT 0,
    companies_found INTEGER DEFAULT 0,
    last_searched_at DATETIME,
    created_at DATETIME DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_cat_cov_unique ON category_coverage(category_id, country);
