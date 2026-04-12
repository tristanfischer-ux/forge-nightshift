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
