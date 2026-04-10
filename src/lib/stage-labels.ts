/**
 * Display labels and tooltips for pipeline stages.
 *
 * Backend status values (DB columns, config keys) are UNCHANGED —
 * only the UI-facing text lives here.
 */

/** Map from backend status / stage key → human-friendly label */
export const STAGE_LABELS: Record<string, string> = {
  // Company statuses (DB `status` column values)
  discovered: "Found",
  enriched: "Researched",
  verified: "Fact-Checked",
  synthesized: "Analysed",
  approved: "Qualified",
  pushed: "Published",
  enriching: "Researching",

  // Pipeline node / flow-chart IDs
  research: "Research",
  enrich: "Research",
  verify: "Fact-Check",
  synthesize: "Analyse",
  director_intel: "Leadership",
  embeddings: "Search Index",
  push: "Publish",
  outreach: "Outreach",
  activity: "News & Updates",

  // Funnel-only keys
  with_process_capabilities: "Capabilities",
  synthesized_public: "Analysed (Public)",
  synthesized_private: "Analysed (Private)",
  investor_matches: "Investor Fit",
};

/** Tooltip descriptions shown on hover */
export const STAGE_TOOLTIPS: Record<string, string> = {
  // Company statuses
  discovered:
    "Companies discovered via Brave Search and industry directories. Not yet researched.",
  enriched:
    "Company data extracted from their website including description, certifications, capabilities, contact info, and quality score.",
  verified:
    "Each company\u2019s data has been compared against their live website to confirm accuracy. Corrections are applied automatically.",
  synthesized:
    "AI-generated summary of each company including marketplace positioning, competitive analysis, and acquisition intelligence.",
  approved:
    "Companies that passed quality and relevance thresholds \u2014 ready for deeper analysis and outreach.",
  pushed:
    "Pushed to the ForgeOS marketplace where buyers can discover them.",

  // Pipeline node IDs
  research: "Discover new companies via Brave Search and industry directories.",
  enrich:
    "Extract company data from their website including description, certifications, capabilities, contact info, and quality score.",
  verify:
    "Compare each company\u2019s data against their live website to confirm accuracy. Corrections are applied automatically.",
  synthesize:
    "Generate an AI summary of each company including marketplace positioning, competitive analysis, and acquisition intelligence.",
  director_intel:
    "Directors, ownership structure, succession signals, and acquisition readiness score from Companies House data.",
  embeddings:
    "Vector embeddings generated for semantic search \u2014 enables finding companies by describing what you need, not just keywords.",
  push: "Push qualified companies to the ForgeOS marketplace where buyers can discover them.",
  outreach: "Send personalised outreach emails to qualified companies via Resend.",
  activity:
    "Recent news articles, funding announcements, contract wins, and hiring activity fetched from Brave Search.",

  // Funnel keys
  with_process_capabilities:
    "Structured data about what each company can do \u2014 processes, equipment, materials, and technical specifications extracted from their website.",
  synthesized_public:
    "AI-generated public summary including marketplace positioning and competitive analysis.",
  synthesized_private:
    "AI-generated private summary including acquisition intelligence and readiness signals.",
  investor_matches:
    "Cross-referenced against the ForgeOS investor database to find which investors are most likely to fund this company.",
};

/**
 * Get the display label for a backend status or stage key.
 * Falls back to title-casing the key if no mapping exists.
 */
export function stageLabel(key: string): string {
  return (
    STAGE_LABELS[key] ??
    key
      .replace(/_/g, " ")
      .replace(/\b\w/g, (c) => c.toUpperCase())
  );
}

/**
 * Get the tooltip for a backend status or stage key.
 * Returns undefined if no tooltip is defined.
 */
export function stageTooltip(key: string): string | undefined {
  return STAGE_TOOLTIPS[key];
}
