-- Add push_to_forgeos flag to search profiles
-- Only manufacturing suppliers should be pushed to ForgeOS marketplace
-- Other profiles (cleantech, biotech, customers) stay local

ALTER TABLE search_profiles ADD COLUMN push_to_forgeos INTEGER DEFAULT 0;

-- Manufacturing profile pushes to ForgeOS
UPDATE search_profiles SET push_to_forgeos = 1 WHERE id = 'manufacturing';
