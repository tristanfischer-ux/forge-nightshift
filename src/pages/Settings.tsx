import { useEffect, useState } from "react";
import {
  Save,
  TestTube,
  CheckCircle,
  XCircle,
  Loader2,
  HardDrive,
  RefreshCw,
  Download,
} from "lucide-react";
import {
  getConfig,
  setConfig,
  testOllamaConnection,
  testBraveConnection,
  testSupabaseConnection,
  testResendConnection,
  backupDatabase,
  reenrichAll,
  getEmailTemplates,
  EmailTemplate,
} from "../lib/tauri";
import { useError } from "../contexts/ErrorContext";

type TestStatus = "idle" | "testing" | "success" | "error";

export default function Settings() {
  const [config, setConfigState] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const { showError } = useError();
  const [ollamaStatus, setOllamaStatus] = useState<TestStatus>("idle");
  const [ollamaError, setOllamaError] = useState("");
  const [ollamaModels, setOllamaModels] = useState<string[]>([]);
  const [braveStatus, setBraveStatus] = useState<TestStatus>("idle");
  const [braveError, setBraveError] = useState("");
  const [supabaseStatus, setSupabaseStatus] = useState<TestStatus>("idle");
  const [supabaseError, setSupabaseError] = useState("");
  const [resendStatus, setResendStatus] = useState<TestStatus>("idle");
  const [resendError, setResendError] = useState("");
  const [backingUp, setBackingUp] = useState(false);
  const [backupPath, setBackupPath] = useState<string | null>(null);
  const [reenrichStage, setReenrichStage] = useState<"idle" | "confirm" | "running" | "done" | "error">("idle");
  const [reenrichCount, setReenrichCount] = useState(0);
  const [validationErrors, setValidationErrors] = useState<Record<string, string>>({});
  const [importPreview, setImportPreview] = useState<Record<string, string> | null>(null);
  const [excludeSecrets, setExcludeSecrets] = useState(true);
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);

  useEffect(() => {
    loadConfig();
    getEmailTemplates().then(setTemplates).catch(() => {});
  }, []);

  async function loadConfig() {
    try {
      const data = await getConfig();
      setConfigState(data);
    } catch (e) {
      console.error("Failed to load config:", e);
    }
  }

  function validateField(key: string, value: string): string {
    if (key === "ollama_url" && value) {
      try { new URL(value); } catch { return "Invalid URL format"; }
    }
    if (key === "supabase_url" && value) {
      try { new URL(value); } catch { return "Invalid URL format"; }
    }
    if ((key === "brave_api_key" || key === "resend_api_key" || key === "supabase_service_key") && value && value.length < 8) {
      return "Key seems too short";
    }
    if (key === "schedule_time" && value) {
      const m = value.match(/^(\d{2}):(\d{2})$/);
      if (!m) return "Must be HH:MM format";
      const h = parseInt(m[1], 10), min = parseInt(m[2], 10);
      if (h > 23 || min > 59) return "Must be valid time (00:00-23:59)";
    }
    return "";
  }

  function updateField(key: string, value: string) {
    setConfigState((prev) => ({ ...prev, [key]: value }));
    setSaved(false);
    const error = validateField(key, value);
    setValidationErrors((prev) => {
      const next = { ...prev };
      if (error) next[key] = error; else delete next[key];
      return next;
    });
  }

  async function saveAll() {
    setSaving(true);
    const failed: string[] = [];
    for (const [key, value] of Object.entries(config)) {
      try {
        await setConfig(key, value);
      } catch {
        failed.push(key);
      }
    }
    if (failed.length > 0) {
      showError(`Failed to save: ${failed.join(", ")}`);
    } else {
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    }
    setSaving(false);
  }

  async function handleTestOllama() {
    setOllamaStatus("testing");
    setOllamaError("");
    try {
      const result = await testOllamaConnection();
      setOllamaModels(result.models);
      setOllamaStatus("success");
    } catch (e) {
      setOllamaError(String(e));
      setOllamaStatus("error");
    }
  }

  async function handleTestBrave() {
    setBraveStatus("testing");
    setBraveError("");
    try {
      const ok = await testBraveConnection(config.brave_api_key || "");
      setBraveStatus(ok ? "success" : "error");
      if (!ok) setBraveError("Connection test returned false");
    } catch (e) {
      setBraveError(String(e));
      setBraveStatus("error");
    }
  }

  async function handleTestSupabase() {
    setSupabaseStatus("testing");
    setSupabaseError("");
    try {
      const ok = await testSupabaseConnection(
        config.supabase_url || "",
        config.supabase_service_key || ""
      );
      setSupabaseStatus(ok ? "success" : "error");
      if (!ok) setSupabaseError("Connection test returned false");
    } catch (e) {
      setSupabaseError(String(e));
      setSupabaseStatus("error");
    }
  }

  async function handleTestResend() {
    setResendStatus("testing");
    setResendError("");
    try {
      const ok = await testResendConnection(config.resend_api_key || "");
      setResendStatus(ok ? "success" : "error");
      if (!ok) setResendError("Connection test returned false");
    } catch (e) {
      setResendError(String(e));
      setResendStatus("error");
    }
  }

  async function handleBackup() {
    setBackingUp(true);
    setBackupPath(null);
    try {
      const path = await backupDatabase();
      setBackupPath(path);
    } catch (e) {
      showError(`Backup failed: ${e}`);
      setBackupPath("error");
    }
    setBackingUp(false);
  }

  async function handleReenrichAll() {
    if (reenrichStage === "idle" || reenrichStage === "done" || reenrichStage === "error") {
      setReenrichStage("confirm");
      return;
    }
    if (reenrichStage === "confirm") {
      setReenrichStage("running");
      try {
        const count = await reenrichAll();
        setReenrichCount(count);
        setReenrichStage("done");
      } catch (e) {
        showError(`Re-enrich failed: ${e}`);
        setReenrichStage("error");
      }
    }
  }

  const SENSITIVE_KEYS = ["brave_api_key", "supabase_service_key", "resend_api_key", "companies_house_api_key"];

  function handleExportConfig() {
    const exportData = { ...config };
    if (excludeSecrets) {
      for (const key of SENSITIVE_KEYS) delete exportData[key];
    }
    const blob = new Blob([JSON.stringify(exportData, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `nightshift-config-${new Date().toISOString().slice(0, 10)}.json`;
    a.click();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function handleImportFile(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => {
      try {
        const data = JSON.parse(reader.result as string);
        if (typeof data !== "object" || data === null || Array.isArray(data)) {
          showError("Config must be a JSON object, not an array");
          return;
        }
        setImportPreview(data as Record<string, string>);
      } catch {
        showError("Invalid JSON file");
      }
    };
    reader.readAsText(file);
    e.target.value = "";
  }

  async function handleApplyImport() {
    if (!importPreview) return;
    setSaving(true);
    try {
      for (const [key, value] of Object.entries(importPreview)) {
        // Skip null/undefined values — don't store literal "null" in config
        if (value == null) continue;
        // Stringify non-string values (numbers, booleans, arrays) for the config store
        const strValue = typeof value === "string" ? value : JSON.stringify(value);
        await setConfig(key, strValue);
      }
      // Normalize all values to strings for local state
      const normalized: Record<string, string> = {};
      for (const [key, value] of Object.entries(importPreview)) {
        normalized[key] = typeof value === "string" ? value : JSON.stringify(value);
      }
      setConfigState((prev) => ({ ...prev, ...normalized }));
      setImportPreview(null);
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch (e) {
      showError(`Import failed: ${e}`);
    }
    setSaving(false);
  }

  function StatusIcon({ status }: { status: TestStatus }) {
    switch (status) {
      case "testing":
        return <Loader2 className="w-4 h-4 animate-spin text-gray-400" />;
      case "success":
        return <CheckCircle className="w-4 h-4 text-green-500" />;
      case "error":
        return <XCircle className="w-4 h-4 text-red-500" />;
      default:
        return null;
    }
  }

  return (
    <div className="space-y-6 max-w-2xl">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Settings</h1>
          <p className="text-sm text-gray-500 mt-1">
            Configure API keys, models, and pipeline behavior
          </p>
        </div>

        <button
          onClick={saveAll}
          disabled={saving || Object.keys(validationErrors).length > 0}
          className="flex items-center gap-2 px-4 py-2 bg-forge-600 hover:bg-forge-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
        >
          {saved ? (
            <CheckCircle className="w-4 h-4 text-green-300" />
          ) : (
            <Save className="w-4 h-4" />
          )}
          {saved ? "Saved" : "Save All"}
        </button>
      </div>

      {/* Ollama */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Ollama (Local LLM)</h2>
          <button
            onClick={handleTestOllama}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs text-gray-700 transition-colors"
          >
            <TestTube className="w-3 h-3" />
            Test
            <StatusIcon status={ollamaStatus} />
          </button>
        </div>
        <Input
          label="Ollama URL"
          value={config.ollama_url || ""}
          onChange={(v) => updateField("ollama_url", v)}
          placeholder="http://localhost:11434"
          error={validationErrors["ollama_url"]}
        />
        <ModelSelect label="Research Model" value={config.research_model || ""} onChange={(v) => updateField("research_model", v)} models={ollamaModels} placeholder="qwen3:8b" />
        <ModelSelect label="Enrichment Model" value={config.enrich_model || ""} onChange={(v) => updateField("enrich_model", v)} models={ollamaModels} placeholder="qwen3:30b-a3b-instruct-2507-q4_K_M" />
        <ModelSelect label="Outreach Model" value={config.outreach_model || ""} onChange={(v) => updateField("outreach_model", v)} models={ollamaModels} placeholder="qwen3:32b" />
        {ollamaStatus === "error" && ollamaError && (
          <p className="text-xs text-red-600">{ollamaError}</p>
        )}
      </section>

      {/* Brave Search */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Brave Search</h2>
          <button
            onClick={handleTestBrave}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs text-gray-700 transition-colors"
          >
            <TestTube className="w-3 h-3" />
            Test
            <StatusIcon status={braveStatus} />
          </button>
        </div>
        <Input
          label="API Key"
          value={config.brave_api_key || ""}
          onChange={(v) => updateField("brave_api_key", v)}
          placeholder="BSA..."
          type="password"
          error={validationErrors["brave_api_key"]}
        />
        {braveStatus === "error" && braveError && (
          <p className="text-xs text-red-600">{braveError}</p>
        )}
      </section>

      {/* Supabase */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Supabase (ForgeOS)</h2>
          <button
            onClick={handleTestSupabase}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs text-gray-700 transition-colors"
          >
            <TestTube className="w-3 h-3" />
            Test
            <StatusIcon status={supabaseStatus} />
          </button>
        </div>
        <Input
          label="Project URL"
          value={config.supabase_url || ""}
          onChange={(v) => updateField("supabase_url", v)}
          placeholder="https://xxx.supabase.co"
          error={validationErrors["supabase_url"]}
        />
        <Input
          label="Service Role Key"
          value={config.supabase_service_key || ""}
          onChange={(v) => updateField("supabase_service_key", v)}
          placeholder="eyJ..."
          type="password"
          error={validationErrors["supabase_service_key"]}
        />
        <Input
          label="Foundry ID"
          value={config.foundry_id || ""}
          onChange={(v) => updateField("foundry_id", v)}
          placeholder="UUID of your foundry"
        />
        {supabaseStatus === "error" && supabaseError && (
          <p className="text-xs text-red-600">{supabaseError}</p>
        )}
      </section>

      {/* Resend */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Resend (Email)</h2>
          <button
            onClick={handleTestResend}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs text-gray-700 transition-colors"
          >
            <TestTube className="w-3 h-3" />
            Test
            <StatusIcon status={resendStatus} />
          </button>
        </div>
        <Input
          label="API Key"
          value={config.resend_api_key || ""}
          onChange={(v) => updateField("resend_api_key", v)}
          placeholder="re_..."
          type="password"
          error={validationErrors["resend_api_key"]}
        />
        <Input
          label="From Email"
          value={config.from_email || ""}
          onChange={(v) => updateField("from_email", v)}
          placeholder="outreach@fractionalforge.com"
        />
        {resendStatus === "error" && resendError && (
          <p className="text-xs text-red-600">{resendError}</p>
        )}
      </section>

      {/* Companies House */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">
          Companies House (UK Enrichment)
        </h2>
        <Input
          label="API Key"
          value={config.companies_house_api_key || ""}
          onChange={(v) => updateField("companies_house_api_key", v)}
          placeholder="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
          type="password"
        />
        <p className="text-xs text-gray-400">
          Used to enrich UK companies with directors, SIC codes, and company
          status from Companies House.
        </p>
      </section>

      {/* Pipeline */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">Pipeline</h2>
        <Input
          label="Schedule Time"
          value={config.schedule_time || ""}
          onChange={(v) => updateField("schedule_time", v)}
          placeholder="23:00"
          error={validationErrors["schedule_time"]}
        />
        <Input
          label="Daily Email Limit"
          value={config.daily_email_limit || ""}
          onChange={(v) => updateField("daily_email_limit", v)}
          placeholder="30"
          type="number"
          min={1}
          max={500}
        />
        <Input
          label="Relevance Threshold (0-100)"
          value={config.relevance_threshold || ""}
          onChange={(v) => updateField("relevance_threshold", v)}
          placeholder="60"
          type="number"
          min={0}
          max={100}
        />
        <Input
          label="Categories per Run"
          value={config.categories_per_run || ""}
          onChange={(v) => updateField("categories_per_run", v)}
          placeholder="8"
          type="number"
          min={1}
          max={37}
        />
        <Input
          label="Enrich Concurrency (1-10)"
          value={config.enrich_concurrency || ""}
          onChange={(v) => updateField("enrich_concurrency", v)}
          placeholder="3"
          type="number"
          min={1}
          max={10}
        />
        <Input
          label="Deep Enrich Concurrency (1-5)"
          value={config.deep_enrich_concurrency || ""}
          onChange={(v) => updateField("deep_enrich_concurrency", v)}
          placeholder="2"
          type="number"
          min={1}
          max={5}
        />
        <Input
          label="Target Countries (JSON)"
          value={config.target_countries || ""}
          onChange={(v) => updateField("target_countries", v)}
          placeholder='["DE","FR","NL","BE","IT","GB"]'
        />
        <div className="border-t border-gray-100 pt-3">
          {reenrichStage === "confirm" && (
            <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 mb-2">
              <p className="text-sm font-medium text-amber-800">
                This will reset all enriched, enriching, and error companies back to discovered.
              </p>
              <p className="text-xs text-amber-600 mt-1">
                They will need to go through the enrichment pipeline again.
              </p>
              <div className="flex gap-2 mt-2.5">
                <button
                  onClick={handleReenrichAll}
                  className="px-3 py-1.5 bg-amber-600 hover:bg-amber-700 rounded-lg text-xs font-medium text-white transition-colors"
                >
                  Yes, Reset All
                </button>
                <button
                  onClick={() => setReenrichStage("idle")}
                  className="px-3 py-1.5 bg-white hover:bg-gray-50 border border-gray-200 rounded-lg text-xs font-medium text-gray-600 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}
          {reenrichStage !== "confirm" && (
            <button
              onClick={handleReenrichAll}
              disabled={reenrichStage === "running"}
              className="flex items-center gap-2 px-4 py-2 bg-amber-50 hover:bg-amber-100 border border-amber-200 disabled:opacity-50 rounded-lg text-sm font-medium text-amber-800 transition-colors"
            >
              {reenrichStage === "running" ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <RefreshCw className="w-4 h-4" />
              )}
              {reenrichStage === "running" ? "Resetting..." : "Re-enrich All Companies"}
            </button>
          )}
          {reenrichStage === "done" && (
            <p className="text-xs text-green-600 mt-1.5 flex items-center gap-1">
              <CheckCircle className="w-3.5 h-3.5" />
              {reenrichCount} companies reset to discovered — run the Enrich pipeline to process them.
            </p>
          )}
          {reenrichStage === "error" && (
            <p className="text-xs text-red-600 mt-1.5 flex items-center gap-1">
              <XCircle className="w-3.5 h-3.5" />
              Re-enrich reset failed
            </p>
          )}
          {reenrichStage === "idle" && (
            <p className="text-xs text-gray-400 mt-1.5">
              Resets enriched, enriching, and error companies back to discovered so they go through the new website-scraping enrichment pipeline.
            </p>
          )}
        </div>
      </section>

      {/* Autopilot Outreach */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">Autopilot Outreach</h2>
        <div className="flex items-center gap-3">
          <label className="flex items-center gap-2 cursor-pointer">
            <input
              type="checkbox"
              checked={config.auto_outreach_enabled === "true"}
              onChange={(e) =>
                updateField("auto_outreach_enabled", e.target.checked ? "true" : "false")
              }
              className="accent-forge-600 w-4 h-4"
            />
            <span className="text-sm text-gray-700">Enable autopilot</span>
          </label>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Template</label>
          <select
            value={config.auto_outreach_template_id || ""}
            onChange={(e) => updateField("auto_outreach_template_id", e.target.value)}
            className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500"
          >
            <option value="">Select a template...</option>
            {templates.map((t) => (
              <option key={t.id} value={t.id}>
                {t.name}
              </option>
            ))}
          </select>
        </div>
        <Input
          label="Batch Size (emails per hour, 1-20)"
          value={config.outreach_batch_size || ""}
          onChange={(v) => updateField("outreach_batch_size", v)}
          placeholder="5"
          type="number"
          min={1}
          max={20}
        />
        <div className="bg-gray-50 border border-gray-100 rounded-lg p-3 space-y-1.5">
          <p className="text-xs font-medium text-gray-600">How it works across multiple days:</p>
          <ol className="text-xs text-gray-400 list-decimal list-inside space-y-1">
            <li>
              At your scheduled time each day, the pipeline runs (research → enrich → push),
              then auto-generates personalised drafts for all eligible companies and auto-approves them.
            </li>
            <li>
              Starting the next hour, the batch sender drip-sends {config.outreach_batch_size || "5"} emails/hour
              until the daily limit ({config.daily_email_limit || "30"}) is reached, then stops for the day.
            </li>
            <li>
              Any unsent approved emails carry over to the next day — they get sent first (FIFO),
              before new drafts from that day's pipeline run.
            </li>
            <li>
              The daily sent count resets at midnight, so the cycle repeats automatically each day.
            </li>
          </ol>
        </div>
      </section>

      {/* Database */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">Database</h2>
        <button
          onClick={handleBackup}
          disabled={backingUp}
          className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 rounded-lg text-sm font-medium text-gray-700 transition-colors"
        >
          {backingUp ? (
            <Loader2 className="w-4 h-4 animate-spin" />
          ) : (
            <HardDrive className="w-4 h-4" />
          )}
          Backup Database
        </button>
        {backupPath && backupPath !== "error" && (
          <p className="text-xs text-green-600">
            Backup saved: {backupPath}
          </p>
        )}
        {backupPath === "error" && (
          <p className="text-xs text-red-600">Backup failed</p>
        )}
        <p className="text-xs text-gray-400">
          Creates a copy of the database. Backups also run automatically before each pipeline run.
        </p>
      </section>

      {/* Import / Export */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">Import / Export</h2>
        <div className="flex items-center gap-3">
          <button onClick={handleExportConfig} className="flex items-center gap-2 px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg text-sm font-medium text-gray-700 transition-colors">
            <Download className="w-4 h-4" />
            Export Config
          </button>
          <label className="flex items-center gap-2">
            <input type="checkbox" checked={excludeSecrets} onChange={(e) => setExcludeSecrets(e.target.checked)} className="accent-forge-600" />
            <span className="text-xs text-gray-500">Exclude API keys</span>
          </label>
        </div>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Import Config</label>
          <input type="file" accept=".json" onChange={handleImportFile} className="text-sm text-gray-500 file:mr-2 file:py-1 file:px-3 file:rounded-lg file:border-0 file:text-xs file:bg-gray-100 file:text-gray-700 hover:file:bg-gray-200" />
        </div>
        {importPreview && (
          <div className="bg-gray-50 border border-gray-200 rounded-lg p-3 space-y-2">
            <p className="text-xs font-medium text-gray-700">Preview: {Object.keys(importPreview).length} keys</p>
            <div className="max-h-32 overflow-y-auto text-xs text-gray-500 font-mono">
              {Object.keys(importPreview).map((k) => <div key={k}>{k}</div>)}
            </div>
            <div className="flex gap-2">
              <button onClick={handleApplyImport} className="px-3 py-1.5 bg-forge-600 hover:bg-forge-700 rounded-lg text-xs font-medium text-white">Apply</button>
              <button onClick={() => setImportPreview(null)} className="px-3 py-1.5 bg-gray-200 hover:bg-gray-300 rounded-lg text-xs font-medium text-gray-700">Cancel</button>
            </div>
          </div>
        )}
      </section>
    </div>
  );
}

function Input({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
  min,
  max,
  error,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
  min?: number;
  max?: number;
  error?: string;
}) {
  return (
    <div>
      <label className="block text-xs text-gray-500 mb-1">{label}</label>
      <input
        type={type}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        min={min}
        max={max}
        className={`w-full px-3 py-2 bg-white border rounded-lg text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-forge-500 focus:border-forge-500 transition-colors ${
          error ? "border-red-300" : "border-gray-300"
        }`}
      />
      {error && <p className="text-xs text-red-500 mt-0.5">{error}</p>}
    </div>
  );
}

function ModelSelect({ label, value, onChange, models, placeholder }: {
  label: string; value: string; onChange: (v: string) => void; models: string[]; placeholder: string;
}) {
  if (models.length > 0) {
    // Include current value in options even if not in Ollama list (e.g. model was pulled after last test)
    const options = value && !models.includes(value) ? [value, ...models] : models;
    return (
      <div>
        <label className="block text-xs text-gray-500 mb-1">{label}</label>
        <select
          value={value}
          onChange={(e) => onChange(e.target.value)}
          className="w-full px-3 py-2 bg-white border border-gray-300 rounded-lg text-sm text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500 focus:border-forge-500 transition-colors"
        >
          <option value="">Select a model...</option>
          {options.map((m) => <option key={m} value={m}>{m}{!models.includes(m) ? " (not found)" : ""}</option>)}
        </select>
      </div>
    );
  }
  return <Input label={label} value={value} onChange={onChange} placeholder={placeholder} />;
}
