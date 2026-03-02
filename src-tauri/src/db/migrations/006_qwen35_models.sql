-- Revert to qwen3 models (qwen3.5 not yet available locally)
UPDATE config SET value = 'qwen3:8b', updated_at = datetime('now') WHERE key = 'research_model' AND value = 'qwen3.5:9b';
UPDATE config SET value = 'qwen3:30b-a3b-instruct-2507-q4_K_M', updated_at = datetime('now') WHERE key = 'enrich_model' AND value = 'qwen3.5:27b-q4_K_M';
UPDATE config SET value = 'qwen3:32b', updated_at = datetime('now') WHERE key = 'outreach_model' AND value = 'qwen3.5:27b-q4_K_M';
