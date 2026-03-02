-- Upgrade all models to qwen3.5 variants
UPDATE config SET value = 'qwen3.5:9b', updated_at = datetime('now')
    WHERE key = 'research_model' AND value = 'qwen3:8b';

UPDATE config SET value = 'qwen3.5:27b-q4_K_M', updated_at = datetime('now')
    WHERE key = 'enrich_model' AND value LIKE 'qwen3:%';

UPDATE config SET value = 'qwen3.5:27b-q4_K_M', updated_at = datetime('now')
    WHERE key = 'outreach_model' AND value LIKE 'qwen3:%';
