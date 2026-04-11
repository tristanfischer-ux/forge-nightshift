# Pipeline UI Consistency Fix — Tracker

## Issues from screenshots

### 1. FlowChart — "Research" appears TWICE, overlapping boxes, weird connector lines
- Node 1: "Research" (blue border, showing 320 + Insulation & Retrofit)  
- Node 2: "Research" (green border, showing deepseek-chat + 5m 16s)
- These are research + enrich running in parallel, but both display as "Research"
- The enrich node is showing as "Research" instead of "Research" (it should be "Enrich")
- Connectors go to wrong places, lines overlap

### 2. Stage order inconsistent across 3 places:
**FlowChart (row 1→2):** Research, Research, Fact-Check, Analyse, News, Search Index, Investor Fit, Publish
**Pipeline Funnel:** Found, Researched, Capabilities, Fact-Checked, Analysed, Qualified, Search Index, News, Investor Fit, Published, Errors
**Review Queue tabs:** All, Found, Researched, Fact-Checked, Analysed, Qualified, Published, Error

ALL THREE must use the SAME order.

### 3. Sidebar heartbeat — no companies/min rate showing
- Shows "Research 320 Insulation & Retrofit" 
- Should show rate like "Research 320 (8/min) Insulation & Retrofit"

### 4. Activity Live Feed — shows 0 events despite pipeline running
- Pipeline IS emitting node events (the FlowChart updates)
- But the Activity section shows "No activity yet"
- The event listener might not be capturing events

### 5. Auto-switch to Fischer Farms after 500 cleantech companies

## Canonical stage order (SINGLE SOURCE OF TRUTH)
1. Research (find companies)
2. Enrich (deep scrape + LLM + CH lookup)
3. Fact-Check (verify against website)
4. Analyse (synthesis)
5. News & Updates (Brave Search activity)
6. Search Index (embeddings)
7. Investor Fit (match against ForgeOS investors)
8. Publish (push to ForgeOS)

## Fixes needed
- [ ] Fix FlowChart: correct node IDs (research vs enrich), fix connectors
- [ ] Standardise stage order across FlowChart, Funnel, Review tabs
- [ ] Fix Activity Live Feed listener
- [ ] Add companies/min to sidebar heartbeat
- [ ] Add auto-switch to Fischer Farms after 500 new cleantech
- [ ] Rebuild and restart
