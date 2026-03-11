-- Email templates for template-based outreach campaigns
CREATE TABLE IF NOT EXISTS email_templates (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    subject TEXT NOT NULL,
    body TEXT NOT NULL,
    is_active INTEGER DEFAULT 1,
    created_at DATETIME DEFAULT (datetime('now')),
    updated_at DATETIME DEFAULT (datetime('now'))
);

-- Add template_id and claim_token to emails table
ALTER TABLE emails ADD COLUMN template_id TEXT REFERENCES email_templates(id);
ALTER TABLE emails ADD COLUMN claim_token TEXT;
