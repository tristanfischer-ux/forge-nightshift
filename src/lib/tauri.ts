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

// Run log
export async function getRunLog(jobId?: string, limit?: number) {
  return invoke<Record<string, unknown>[]>("get_run_log", { jobId, limit });
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
