ALTER TABLE emails ADD COLUMN ab_variant TEXT;
ALTER TABLE emails ADD COLUMN claim_status TEXT;
ALTER TABLE emails ADD COLUMN claim_status_synced_at DATETIME;
CREATE INDEX IF NOT EXISTS idx_emails_company_template ON emails(company_id, template_id);
