# Overnight Work Tracker — 2026-04-11

## Mission
Make the cleantech database and dashboard as useful as possible for M&A targets and fundraising candidates.

## Red Team Rounds
- [x] Round 1: Data quality & completeness
- [x] Round 2: UX & dashboard usefulness
- [x] Round 3: New functionality — what's missing?
- [x] Round 4: New functionality — 10x multiplier
- [x] Round 5: Integration & automation

## Implementation Tasks

### Done
- [x] Fix director intel → merged into enrichment (CH already captures officers)
- [x] Fix pipeline funnel order in app (12 rows, correct sequence)
- [x] All 2,885 cleantech embeddings generated
- [x] Semantic search in HTML dashboard (500 concept vocab, quantized embeddings)
- [x] Lead scoring (0-100) in app + dashboard
- [x] M&A Score (0-100) + Fundraise Score (0-100) in app + dashboard
- [x] Deal tracking system (M&A targets + Fundraise candidates with status pipeline)
- [x] Deals page with tabs, filters, assigned_to
- [x] CSV export from HTML dashboard
- [x] Dashboard regenerated with all scoring + semantic search (13.6MB)
- [x] Removed broken director_intel stage from batch pipeline

### Remaining
- [ ] Fix investor match to run to completion (Supabase query fixed but never completed)
- [ ] Archive permanent errors (error_count >= 3, don't pollute funnel)
- [ ] Auto-regenerate HTML dashboard after pipeline completes
- [ ] Restart pipeline
- [ ] Rebuild app with latest changes

## Final Status
- v0.42.0 committed and pushed
- Dashboard: 13.6MB with semantic search, M&A/Fundraise scoring, CSV export
- Pipeline: needs restart
- App: needs rebuild
