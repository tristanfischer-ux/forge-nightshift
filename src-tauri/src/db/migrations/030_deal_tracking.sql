CREATE TABLE IF NOT EXISTS deal_tracking (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    company_id TEXT NOT NULL,
    deal_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'identified',
    priority TEXT DEFAULT 'medium',
    notes TEXT,
    assigned_to TEXT,
    estimated_value TEXT,
    next_action TEXT,
    next_action_date TEXT,
    created_at TEXT DEFAULT (datetime('now')),
    updated_at TEXT DEFAULT (datetime('now')),
    UNIQUE(company_id, deal_type)
);
CREATE INDEX IF NOT EXISTS idx_deal_tracking_company ON deal_tracking(company_id);
CREATE INDEX IF NOT EXISTS idx_deal_tracking_status ON deal_tracking(status);
CREATE INDEX IF NOT EXISTS idx_deal_tracking_type ON deal_tracking(deal_type);
