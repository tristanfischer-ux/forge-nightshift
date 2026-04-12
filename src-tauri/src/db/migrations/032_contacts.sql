CREATE TABLE IF NOT EXISTS contacts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT NOT NULL,
    name TEXT NOT NULL,
    title TEXT,
    email TEXT,
    phone TEXT,
    linkedin_url TEXT,
    role_type TEXT,
    department TEXT,
    seniority TEXT,
    source TEXT,
    notes TEXT,
    is_primary INTEGER DEFAULT 0,
    outreach_status TEXT DEFAULT 'not_contacted',
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(company_id, name)
);
CREATE INDEX IF NOT EXISTS idx_contacts_company ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_role ON contacts(role_type);
