import { useEffect, useState } from "react";
import {
  Save,
  TestTube,
  CheckCircle,
  XCircle,
  Loader2,
  HardDrive,
} from "lucide-react";
import {
  getConfig,
  setConfig,
  testOllamaConnection,
  testBraveConnection,
  testSupabaseConnection,
  testResendConnection,
  backupDatabase,
} from "../lib/tauri";

type TestStatus = "idle" | "testing" | "success" | "error";

export default function Settings() {
  const [config, setConfigState] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [saved, setSaved] = useState(false);
  const [ollamaStatus, setOllamaStatus] = useState<TestStatus>("idle");
  const [ollamaModels, setOllamaModels] = useState<string[]>([]);
  const [braveStatus, setBraveStatus] = useState<TestStatus>("idle");
  const [supabaseStatus, setSupabaseStatus] = useState<TestStatus>("idle");
  const [resendStatus, setResendStatus] = useState<TestStatus>("idle");
  const [backingUp, setBackingUp] = useState(false);
  const [backupPath, setBackupPath] = useState<string | null>(null);

  useEffect(() => {
    loadConfig();
  }, []);

  async function loadConfig() {
    try {
      const data = await getConfig();
      setConfigState(data);
    } catch {
      // DB may not be ready
    }
  }

  function updateField(key: string, value: string) {
    setConfigState((prev) => ({ ...prev, [key]: value }));
    setSaved(false);
  }

  async function saveAll() {
    setSaving(true);
    try {
      for (const [key, value] of Object.entries(config)) {
        await setConfig(key, value);
      }
      setSaved(true);
      setTimeout(() => setSaved(false), 2000);
    } catch {
      // handle error
    }
    setSaving(false);
  }

  async function handleTestOllama() {
    setOllamaStatus("testing");
    try {
      const result = await testOllamaConnection();
      setOllamaModels(result.models);
      setOllamaStatus("success");
    } catch {
      setOllamaStatus("error");
    }
  }

  async function handleTestBrave() {
    setBraveStatus("testing");
    try {
      const ok = await testBraveConnection(config.brave_api_key || "");
      setBraveStatus(ok ? "success" : "error");
    } catch {
      setBraveStatus("error");
    }
  }

  async function handleTestSupabase() {
    setSupabaseStatus("testing");
    try {
      const ok = await testSupabaseConnection(
        config.supabase_url || "",
        config.supabase_service_key || ""
      );
      setSupabaseStatus(ok ? "success" : "error");
    } catch {
      setSupabaseStatus("error");
    }
  }

  async function handleTestResend() {
    setResendStatus("testing");
    try {
      const ok = await testResendConnection(config.resend_api_key || "");
      setResendStatus(ok ? "success" : "error");
    } catch {
      setResendStatus("error");
    }
  }

  async function handleBackup() {
    setBackingUp(true);
    setBackupPath(null);
    try {
      const path = await backupDatabase();
      setBackupPath(path);
    } catch {
      setBackupPath("error");
    }
    setBackingUp(false);
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
          disabled={saving}
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
        />
        <Input
          label="Research Model"
          value={config.research_model || ""}
          onChange={(v) => updateField("research_model", v)}
          placeholder="qwen3:8b"
        />
        <Input
          label="Enrichment Model"
          value={config.enrich_model || ""}
          onChange={(v) => updateField("enrich_model", v)}
          placeholder="qwen3:30b-a3b"
        />
        <Input
          label="Outreach Model"
          value={config.outreach_model || ""}
          onChange={(v) => updateField("outreach_model", v)}
          placeholder="qwen3:32b"
        />
        {ollamaModels.length > 0 && (
          <div className="text-xs text-gray-500">
            Available models: {ollamaModels.join(", ")}
          </div>
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
        />
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
        />
        <Input
          label="Service Role Key"
          value={config.supabase_service_key || ""}
          onChange={(v) => updateField("supabase_service_key", v)}
          placeholder="eyJ..."
          type="password"
        />
        <Input
          label="Foundry ID"
          value={config.foundry_id || ""}
          onChange={(v) => updateField("foundry_id", v)}
          placeholder="UUID of your foundry"
        />
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
        />
        <Input
          label="From Email"
          value={config.from_email || ""}
          onChange={(v) => updateField("from_email", v)}
          placeholder="outreach@fractionalforge.com"
        />
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
          label="Target Countries (JSON)"
          value={config.target_countries || ""}
          onChange={(v) => updateField("target_countries", v)}
          placeholder='["DE","FR","NL","BE","IT","GB"]'
        />
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
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
  min?: number;
  max?: number;
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
        className="w-full px-3 py-2 bg-white border border-gray-300 rounded-lg text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-forge-500 focus:border-forge-500 transition-colors"
      />
    </div>
  );
}
