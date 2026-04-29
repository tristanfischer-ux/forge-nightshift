-- Migration 034: structured_signals_json column for the new two-pass synthesis stage.
-- Port of Forge Capital's VERIFY + SYNTHESIS chain from research/17-unified-pipeline.py.
-- The three already-existing companion columns are:
--   synthesis_public_json  (022_synthesis.sql)
--   synthesis_private_json (022_synthesis.sql)
--   fractional_signals_json (021_verification.sql, re-used)
--   ff_suitability_reason  (031_scoring_reasoning.sql, re-used)
-- Adding the fifth:
ALTER TABLE companies ADD COLUMN structured_signals_json TEXT;
