# Overnight Work Tracker — 2026-04-11

## Mission
Make the cleantech database and dashboard as useful as possible. 5-round red team + implementation.

## Status at start
- Cleantech UK: 2,885 researched, 2,885 fact-checked, 2,885 analysed
- Director Intel: 0 (BROKEN — save_intel DB schema mismatch)
- Embeddings: ~608 generating (background process running)
- Investor Match: 0 (Supabase query was fixed but never completed a run)
- Pipeline funnel order needs updating
- Dashboard: static HTML, no auto-update, no semantic search
- App version: v0.40.1

## Red Team Rounds
- [ ] Round 1: Data quality & completeness
- [ ] Round 2: UX & dashboard usefulness
- [ ] Round 3: New functionality — what's missing?
- [ ] Round 4: New functionality — what would make this 10x more useful?
- [ ] Round 5: Integration & automation

## Implementation Tasks (Priority Order)

### Must Do (broken/blocking)
- [x] Round 1-5: Fix embeddings for all cleantech (running in background)
- [ ] Fix director intel DB schema → merge into enrichment stage
- [ ] Fix pipeline funnel order in app
- [ ] Fix investor match to run to completion
- [ ] Archive permanent errors (don't show in active funnel)

### High Impact (new functionality)
- [ ] Add lead scoring (weighted: quality + relevance + company size + growth signals)
- [ ] Add CSV export from dashboard
- [ ] Add "Save with note" / prospect tagging in Review page
- [ ] Add semantic search to HTML dashboard
- [ ] Auto-regenerate HTML dashboard after pipeline completes

### Nice to Have
- [ ] Faceted search in dashboard (location + capability + size)
- [ ] Last updated timestamps on company cards
- [ ] Cross-profile recommendations

## Completed
(To be filled as work progresses)
