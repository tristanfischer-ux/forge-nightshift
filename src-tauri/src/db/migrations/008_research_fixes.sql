-- Bump categories_per_run from 8 to 12
UPDATE config SET value = '12', updated_at = datetime('now') WHERE key = 'categories_per_run' AND value = '8';

-- One-time cleanup: delete search_history older than 7 days to unblock stale queries
DELETE FROM search_history WHERE created_at < datetime('now', '-7 days');
