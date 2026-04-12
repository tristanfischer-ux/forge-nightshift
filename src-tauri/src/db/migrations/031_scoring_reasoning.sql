ALTER TABLE companies ADD COLUMN ff_suitability_score INTEGER DEFAULT 0;
ALTER TABLE companies ADD COLUMN ff_suitability_reason TEXT;
ALTER TABLE companies ADD COLUMN ma_reason TEXT;
ALTER TABLE companies ADD COLUMN fundraise_reason TEXT;
