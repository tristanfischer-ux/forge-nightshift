CREATE TABLE IF NOT EXISTS outreach_insights (
    id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 1,
    insight_type TEXT NOT NULL,
    insight TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    source_email_count INTEGER DEFAULT 0,
    created_at DATETIME DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS ab_experiments (
    id TEXT PRIMARY KEY,
    generation INTEGER NOT NULL DEFAULT 1,
    variant_a_strategy TEXT NOT NULL,
    variant_b_strategy TEXT NOT NULL,
    variant_a_sent INTEGER DEFAULT 0,
    variant_b_sent INTEGER DEFAULT 0,
    variant_a_opened INTEGER DEFAULT 0,
    variant_b_opened INTEGER DEFAULT 0,
    variant_a_claimed INTEGER DEFAULT 0,
    variant_b_claimed INTEGER DEFAULT 0,
    winner TEXT,
    status TEXT DEFAULT 'active',
    created_at DATETIME DEFAULT (datetime('now')),
    completed_at DATETIME
);

ALTER TABLE emails ADD COLUMN strategy_text TEXT;
ALTER TABLE emails ADD COLUMN generation INTEGER DEFAULT 0;
ALTER TABLE emails ADD COLUMN insights_used TEXT;
ALTER TABLE emails ADD COLUMN experiment_id TEXT;
