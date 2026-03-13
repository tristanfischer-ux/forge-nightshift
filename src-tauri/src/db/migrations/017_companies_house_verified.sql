ALTER TABLE companies ADD COLUMN ch_verified_at DATETIME;
ALTER TABLE companies ADD COLUMN ch_company_number TEXT;
CREATE INDEX IF NOT EXISTS idx_companies_ch_verified ON companies(country, ch_verified_at);
