# Outstanding Tasks Checklist

## From this conversation (in order asked)

### 1. Scoring is broken — company in liquidation has Fundraise 70/100
- [ ] The fundraise score uses simple keyword matching that doesn't check Companies House status
- [ ] Fix: if `ch_company_status` is "liquidation", "dissolved", "administration" → set M&A and Fundraise scores to 0
- [ ] Re-score all Fischer Farms companies with this fix
- [ ] Also check: are there other obviously wrong scores?

### 2. Self-audit every 200 companies (ALREADY BUILT in v0.46.0)
- [x] audit.rs runs after every wave
- [x] Checks: orphaned enriching, error rate, no-website errors, quality drops, duplicates, permanent errors
- [ ] Verify it's actually running and producing log entries

### 3. Overhaul Fischer Farms search categories (DONE)
- [x] Replaced 25 community-farm categories with 14 commercially-focused ones
- [x] Cleared old search history for fresh start

### 4. Score existing Fischer Farms companies for suitability (DONE)
- [x] ff_suitability_score + ff_suitability_reason added to DB (migration 031)
- [x] 399/400 companies scored via DeepSeek
- [x] Dashboard shows suitability scores

### 5. Add M&A/Fundraise reasoning text (DONE)
- [x] ma_reason + fundraise_reason columns added (migration 031)
- [x] Reasoning generated via DeepSeek for all companies
- [x] Dashboard shows reasoning in modal + "Why" column in top 10 tables
- [x] Top 10 Fischer Farms Prospects table added to Overview

### 6. Fischer Farms dashboard (DONE)
- [x] Generated: 1.6MB, 440 companies, orange theme

### 7. Fix fundraise scoring to check company status
- [ ] Companies in liquidation/dissolved/administration should score 0
- [ ] Update the scoring in: dashboard script (39-cleantech-dashboard.py) + Review page (Review.tsx)
- [ ] Re-run scoring script for Fischer Farms

### 8. Commit and push all changes
- [ ] Nightshift repo
- [ ] Forge-Capital repo (dashboards)

### 9. Rebuild app and restart pipeline on Fischer Farms with new categories
- [ ] Rebuild v0.47.0
- [ ] Restart pipeline
- [ ] Verify new categories are being searched

### 10. Regenerate dashboards after fixes
- [ ] Fischer Farms dashboard
- [ ] Cleantech dashboard
