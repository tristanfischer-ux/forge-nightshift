-- Add discovery_source to companies for tracking where companies were found
ALTER TABLE companies ADD COLUMN discovery_source TEXT DEFAULT 'brave_search';

-- Add directory_search_enabled config for existing DBs
INSERT OR IGNORE INTO config (key, value) VALUES ('directory_search_enabled', 'true')
