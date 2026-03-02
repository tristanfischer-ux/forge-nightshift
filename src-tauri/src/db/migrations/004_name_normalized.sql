ALTER TABLE companies ADD COLUMN name_normalized TEXT;

CREATE INDEX IF NOT EXISTS idx_companies_name_normalized ON companies(name_normalized);

-- Backfill: lowercase + strip common suffixes
UPDATE companies SET name_normalized = LOWER(TRIM(
    REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
        name,
        ' Ltd', ''), ' Limited', ''), ' GmbH', ''), ' SAS', ''), ' BV', ''),
        ' AG', ''), ' SA', ''), ' SRL', ''), ' NV', ''), ' Inc', ''), ' LLC', '')
)) WHERE name_normalized IS NULL;