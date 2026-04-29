# Outstanding Tasks Checklist

## All items — status

### 1. Scoring fix — dead companies getting high scores
- [x] Dashboard scoring: M&A + Fundraise forced to 0 for liquidation/dissolved/administration
- [x] 43 dead companies archived from active datasets
- [x] Self-audit CHECK 7 added: auto-archives dead companies every wave

### 2. Self-audit every wave (built in v0.46.0)
- [x] audit.rs runs after every wave with 7 checks
- [x] CHECK 7 added: dead company detection (liquidation/dissolved)
- [x] Verified producing log entries

### 3. Overhaul Fischer Farms search categories
- [x] Replaced 25 community-farm categories with 14 commercially-focused ones
- [x] Cleared old search history for fresh start
- [x] Pipeline restarted with new categories

### 4. Score Fischer Farms companies for suitability
- [x] ff_suitability_score + ff_suitability_reason columns (migration 031)
- [x] 399/400 companies scored via DeepSeek
- [x] Dashboard shows suitability scores + reasoning

### 5. M&A/Fundraise reasoning text
- [x] ma_reason + fundraise_reason columns (migration 031)
- [x] Reasoning generated via DeepSeek
- [x] Dashboard: reasoning in modal + "Why" column in top 10 tables
- [x] Top 10 Fischer Farms Prospects table on Overview

### 6. Fischer Farms dashboard
- [x] Generated with suitability scores, reasoning, dead company fix

### 7. Cleantech dashboard
- [x] Regenerated with dead company scoring fix

### 8. Commit and push
- [x] Nightshift v0.47.0 pushed
- [x] Dashboards committed

### 9. Rebuild app and restart pipeline
- [x] v0.47.0 built and running
- [x] Fischer Farms profile active with new categories

### 10. Pipeline running
- [x] Batch mode, Fischer Farms Customers, new commercial categories

### 11. LinkedIn Sales Navigator integration
- [x] Login + 2FA verified via agent-browser
- [x] Found decision makers at 4 top companies (Brakes, Bidfood, Sodexo, Fresca) — 18 contacts
- [x] Created LinkedIn Navigator skill file (~/.claude/skills/linkedin-navigator.md)

### 12. LinkedIn backfill — Fischer Farms batch 1 (score 85-100)
- [x] 66 contacts found across 18 companies via Sonnet subagent
- [x] Top prospects identified: Oliver Kay, Daylesford, Hunt's Food Group, Reynolds, Nationwide Produce
- [x] Warm leads identified: Scott Hollins (20 mutual), Beth Emmens (18), Ed Rowlands (16)

### 13. Pipeline flow improvements (v0.50.0)
- [x] Auto-pause research when verify backlog > 200 companies
- [x] Funnel redesigned: losses shown inline (no-website, errors, awaiting fact-check)
- [x] Contacts node added to FlowChart (9 nodes, 5+4 layout)
- [x] Contacts row added to funnel table
- [x] Contacts stage moved earlier (after Enrich, before Verify)
- [x] Verify concurrency increased 3→6
- [x] Activity Live Feed fixed (subscribes to pipeline:progress events)
- [x] Migration 033: work_history_json column on contacts table

### 14. LinkedIn backfill — Fischer Farms batch 2 (score 70-80)
- [ ] 19 companies queued (Sonnet subagent running)

### 15. Forge Capital — LinkedIn investor enrichment
- [ ] Phase 1: 12 zero-partner UK investors (Sonnet subagent running)
- [ ] Phase 2: LinkedIn URLs for 4,110 partners at 368 UK investors
- [ ] Phase 3: Career history enrichment for 3,877 partners
