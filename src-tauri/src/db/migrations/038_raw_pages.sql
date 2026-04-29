-- v0.58.0: scrape-once-mine-many architecture
-- Stores every page we visit per company, so future extraction passes (new
-- attribute schemas, synthesis, semantic search) can re-mine the corpus
-- without re-fetching the network. Image + PDF metadata captured but the
-- binary content NOT downloaded — filenames are often descriptive and
-- worth keeping for "search later" use cases (e.g. team-john-smith.jpg,
-- company-brochure-2025.pdf).
CREATE TABLE IF NOT EXISTS company_raw_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT NOT NULL,
    url TEXT NOT NULL,
    fetched_at TEXT DEFAULT (datetime('now')),
    status_code INTEGER,
    content_type TEXT,
    content_text TEXT,
    content_html_gz BLOB,
    image_metadata_json TEXT,
    pdf_links_json TEXT,
    internal_links_json TEXT,
    bytes_fetched INTEGER,
    error TEXT,
    UNIQUE(company_id, url)
);
CREATE INDEX IF NOT EXISTS idx_raw_pages_company ON company_raw_pages(company_id);
CREATE INDEX IF NOT EXISTS idx_raw_pages_url ON company_raw_pages(url);
