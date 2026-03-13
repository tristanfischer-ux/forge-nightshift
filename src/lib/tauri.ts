import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";

// Stats
export async function getStats() {
  return invoke<Record<string, unknown>>("get_stats");
}

// Companies
export async function getCompanies(
  status?: string,
  limit?: number,
  offset?: number
) {
  return invoke<Record<string, unknown>[]>("get_companies", {
    status,
    limit,
    offset,
  });
}

export async function getCompany(id: string) {
  return invoke<Record<string, unknown>>("get_company", { id });
}

export async function updateCompanyStatus(id: string, status: string) {
  return invoke("update_company_status", { id, status });
}

// Emails
export async function getEmails(status?: string, limit?: number) {
  return invoke<Record<string, unknown>[]>("get_emails", { status, limit });
}

export async function updateEmailStatus(id: string, status: string) {
  return invoke("update_email_status", { id, status });
}

// Config
export async function getConfig() {
  return invoke<Record<string, string>>("get_config");
}

export async function setConfig(key: string, value: string) {
  return invoke("set_config", { key, value });
}

// Connection tests
export async function testOllamaConnection() {
  return invoke<{ connected: boolean; models: string[] }>(
    "test_ollama_connection"
  );
}

export async function testBraveConnection(apiKey: string) {
  return invoke<boolean>("test_brave_connection", { apiKey });
}

export async function testSupabaseConnection(url: string, key: string) {
  return invoke<boolean>("test_supabase_connection", { url, key });
}

export async function testResendConnection(apiKey: string) {
  return invoke<boolean>("test_resend_connection", { apiKey });
}

// Pipeline
export async function startPipeline(stages: string[]) {
  return invoke<string>("start_pipeline", { stages });
}

export async function stopPipeline() {
  return invoke("stop_pipeline");
}

export async function getPipelineStatus() {
  return invoke<{ running: boolean; cancelling: boolean }>(
    "get_pipeline_status"
  );
}

// Reset error companies
export async function resetErrorCompanies() {
  return invoke<number>("reset_error_companies");
}

// Approve all enriched companies
export async function approveAllEnriched() {
  return invoke<number>("approve_all_enriched");
}

// Re-enrich all companies (reset enriched/enriching/error → discovered)
export async function reenrichAll() {
  return invoke<number>("reenrich_all");
}

// Analytics
export interface ChartDataPoint {
  name: string;
  count: number;
}

export interface AnalyticsData {
  by_subcategory: ChartDataPoint[];
  by_country: ChartDataPoint[];
  pipeline_funnel: ChartDataPoint[];
  by_equipment: ChartDataPoint[];
  by_material: ChartDataPoint[];
  by_certification: ChartDataPoint[];
  by_industry: ChartDataPoint[];
}

export async function getAnalytics() {
  return invoke<AnalyticsData>("get_analytics");
}

// Filtered companies (drill-down)
export async function getCompaniesFiltered(filters: {
  status?: string;
  subcategory?: string;
  country?: string;
  search?: string;
  limit?: number;
  offset?: number;
}) {
  return invoke<Record<string, unknown>[]>("get_companies_filtered", filters);
}

// Run log
export async function getRunLog(jobId?: string, limit?: number) {
  return invoke<Record<string, unknown>[]>("get_run_log", { jobId, limit });
}

// Refresh email delivery statuses from Resend
export async function refreshEmailStatuses() {
  return invoke<number>("refresh_email_statuses");
}

// Backup database
export async function backupDatabase() {
  return invoke<string>("backup_database");
}

// Import low-quality listings from Supabase for audit re-enrichment
export async function importForAudit(threshold?: number) {
  return invoke<{ fetched: number; imported: number; skipped: number }>(
    "import_for_audit",
    { threshold }
  );
}

// Remove a single company from ForgeOS marketplace and local DB
export async function removeFromMarketplace(id: string) {
  return invoke<{ removed: boolean; name: string }>(
    "remove_from_marketplace",
    { id }
  );
}

// Bulk remove companies from ForgeOS marketplace
export async function removeAllFromMarketplace(companyIds: string[]) {
  return invoke<{ removed: number; errors: number }>(
    "remove_all_from_marketplace",
    { companyIds }
  );
}

// Push a single company to ForgeOS
export async function pushSingleCompany(id: string) {
  return invoke<{ pushed: boolean; name: string }>("push_single_company", {
    id,
  });
}

// Map data
export interface MapCompany {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  subcategory: string | null;
  city: string | null;
  country: string | null;
  relevance_score: number | null;
  website_url: string | null;
}

export async function getCompaniesForMap() {
  return invoke<MapCompany[]>("get_companies_for_map");
}

export async function geocodeCompanies() {
  return invoke<{ total: number; geocoded: number; failed: number }>(
    "geocode_companies"
  );
}

// Event listeners
export function onPipelineStatus(
  callback: (payload: Record<string, unknown>) => void
) {
  return listen<Record<string, unknown>>("pipeline:status", (event) =>
    callback(event.payload)
  );
}

export function onPipelineStage(
  callback: (payload: { stage: string; status: string }) => void
) {
  return listen<{ stage: string; status: string }>("pipeline:stage", (event) =>
    callback(event.payload)
  );
}

export function onPipelineProgress(
  callback: (payload: Record<string, unknown>) => void
) {
  return listen<Record<string, unknown>>("pipeline:progress", (event) =>
    callback(event.payload)
  );
}

// Pipeline node events (flow chart monitor)
export interface PipelineNodeEvent {
  node_id: string;
  status: "idle" | "running" | "completed" | "failed" | "waiting";
  model: string | null;
  progress: {
    current: number;
    total: number | null;
    rate: number | null;
    current_item: string | null;
  };
  concurrency: number;
  started_at: string | null;
  elapsed_secs: number | null;
}

export function onPipelineNode(
  callback: (payload: PipelineNodeEvent) => void
) {
  return listen<PipelineNodeEvent>("pipeline:node", (event) =>
    callback(event.payload)
  );
}

export async function getPipelineNodes(): Promise<Record<string, PipelineNodeEvent>> {
  return invoke<Record<string, PipelineNodeEvent>>("get_pipeline_nodes");
}

// New v0.17.0 commands
export async function getCompaniesCount(status?: string) {
  return invoke<number>("get_companies_count", { status });
}

export async function batchUpdateStatus(ids: string[], status: string) {
  return invoke<number>("batch_update_status", { ids, status });
}

export interface StatsHistoryEntry {
  date: string;
  companies: number;
  enriched: number;
  pushed: number;
}

export async function getStatsHistory() {
  return invoke<StatsHistoryEntry[]>("get_stats_history");
}

export interface RunHistoryEntry {
  id: string;
  stages: string;
  status: string;
  summary: string | null;
  started_at: string | null;
  completed_at: string | null;
  created_at: string | null;
}

export async function getRunHistory(limit?: number) {
  return invoke<RunHistoryEntry[]>("get_run_history", { limit });
}

// Email Templates
export interface EmailTemplate {
  id: string;
  name: string;
  subject: string;
  body: string;
  is_active: number;
  created_at: string;
  updated_at: string;
}

export async function getEmailTemplates() {
  return invoke<EmailTemplate[]>("get_email_templates");
}

export async function saveEmailTemplate(params: {
  id?: string;
  name: string;
  subject: string;
  body: string;
}) {
  return invoke<{ id: string; created?: boolean; updated?: boolean }>(
    "save_email_template",
    params
  );
}

export async function deleteEmailTemplate(id: string) {
  return invoke("delete_email_template", { id });
}

export async function getCampaignEligibleCount() {
  return invoke<number>("get_campaign_eligible_count");
}

export async function deleteEmails(ids: string[]) {
  return invoke<number>("delete_emails", { ids });
}

// Send all approved emails via Resend
export async function sendApprovedEmails() {
  return invoke<{ sent: number; failed: number; total: number }>(
    "send_approved_emails"
  );
}

// Reset failed emails back to "approved" for retry
export async function retryFailedEmails() {
  return invoke<number>("retry_failed_emails");
}
