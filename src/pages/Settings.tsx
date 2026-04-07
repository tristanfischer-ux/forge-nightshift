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
  Plus,
  Pencil,
  Trash2,
} from "lucide-react";
import {
  getConfig,
  setConfig,
  testOllamaConnection,
  testAnthropicConnection,
  testDeepSeekConnection,
  testBraveConnection,
  testSupabaseConnection,
  testResendConnection,
  backupDatabase,
  reenrichAll,
  getEmailTemplates,
  EmailTemplate,
  getSearchProfiles,
  saveSearchProfile,
  deleteSearchProfile,
  getActiveProfile,
  setActiveProfile,
} from "../lib/tauri";
import type { SearchProfile } from "../lib/tauri";
import ScheduleCalendar from "../components/ScheduleCalendar";
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
  const [anthropicStatus, setAnthropicStatus] = useState<TestStatus>("idle");
  const [anthropicError, setAnthropicError] = useState("");
  const [deepseekStatus, setDeepseekStatus] = useState<TestStatus>("idle");
  const [deepseekError, setDeepseekError] = useState("");
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

  // Search Profiles state
  const [searchProfiles, setSearchProfiles] = useState<SearchProfile[]>([]);
  const [activeProfileId, setActiveProfileIdState] = useState<string>("");
  const [editingProfile, setEditingProfile] = useState<Partial<SearchProfile> | null>(null);
  const [profileSaving, setProfileSaving] = useState(false);

  useEffect(() => {
    loadConfig();
    getEmailTemplates().then(setTemplates).catch(() => {});
    loadProfiles();
  }, []);

  async function loadProfiles() {
    try {
      const [profiles, activeId] = await Promise.all([
        getSearchProfiles(),
        getActiveProfile(),
      ]);
      setSearchProfiles(profiles);
      setActiveProfileIdState(activeId);
    } catch (e) {
      console.error("Failed to load profiles:", e);
    }
  }

  async function handleSaveProfile() {
    if (!editingProfile?.name || !editingProfile?.domain) return;
    setProfileSaving(true);
    try {
      const id = editingProfile.id || crypto.randomUUID();
      await saveSearchProfile({
        id,
        name: editingProfile.name,
        description: editingProfile.description || "",
        domain: editingProfile.domain,
        categories_json: editingProfile.categories_json || "[]",
        target_countries_json: editingProfile.target_countries_json || "[]",
      });
      setEditingProfile(null);
      await loadProfiles();
    } catch (e) {
      showError(`Failed to save profile: ${e}`);
    }
    setProfileSaving(false);
  }

  async function handleDeleteProfile(id: string) {
    try {
      await deleteSearchProfile(id);
      await loadProfiles();
    } catch (e) {
      showError(`Failed to delete profile: ${e}`);
    }
  }

  async function handleSetActiveProfile(id: string) {
    try {
      await setActiveProfile(id);
      setActiveProfileIdState(id);
    } catch (e) {
      showError(`Failed to set active profile: ${e}`);
    }
  }

  function getCategoryCount(json: string): number {
    try {
      const parsed = JSON.parse(json);
      return Array.isArray(parsed) ? parsed.length : 0;
    } catch {
      return 0;
    }
  }

  function getCountryList(json: string): string {
    try {
      const parsed = JSON.parse(json);
      if (Array.isArray(parsed)) return parsed.join(", ");
      return String(json);
    } catch {
      return json;
    }
  }

  const DOMAIN_BADGE_COLORS: Record<string, string> = {
    manufacturing: "bg-blue-100 text-blue-700",
    cleantech: "bg-green-100 text-green-700",
    biotech: "bg-purple-100 text-purple-700",
  };

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

  async function handleTestAnthropic() {
    setAnthropicStatus("testing");
    setAnthropicError("");
    try {
      const result = await testAnthropicConnection(config.anthropic_api_key || "");
      setAnthropicStatus(result.connected ? "success" : "error");
      if (!result.connected) setAnthropicError("Connection test returned false");
    } catch (e) {
      setAnthropicError(String(e));
      setAnthropicStatus("error");
    }
  }

  async function handleTestDeepSeek() {
    setDeepseekStatus("testing");
    setDeepseekError("");
    try {
      const result = await testDeepSeekConnection(config.deepseek_api_key || "");
      setDeepseekStatus(result.connected ? "success" : "error");
      if (!result.connected) setDeepseekError("Connection test returned false");
    } catch (e) {
      setDeepseekError(String(e));
      setDeepseekStatus("error");
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

  const SENSITIVE_KEYS = ["brave_api_key", "supabase_service_key", "resend_api_key", "companies_house_api_key", "anthropic_api_key", "deepseek_api_key", "openai_api_key"];

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

      {/* Search Profiles */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-4 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Search Profiles</h2>
          <button
            onClick={() => setEditingProfile({ id: "", name: "", domain: "", description: "", categories_json: "[]", target_countries_json: "[]" })}
            className="flex items-center gap-1.5 px-3 py-1.5 bg-forge-600 hover:bg-forge-700 rounded-lg text-xs font-medium text-white transition-colors"
          >
            <Plus className="w-3 h-3" />
            Create Profile
          </button>
        </div>

        <div className="space-y-3">
          {searchProfiles.map((profile) => (
            <div
              key={profile.id}
              className={`border rounded-lg p-3 ${
                profile.id === activeProfileId
                  ? "border-forge-300 bg-forge-50"
                  : "border-gray-200"
              }`}
            >
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  <span className="font-medium text-sm text-gray-900">{profile.name}</span>
                  <span className={`px-2 py-0.5 rounded-full text-xs ${DOMAIN_BADGE_COLORS[profile.domain] || "bg-gray-100 text-gray-600"}`}>
                    {profile.domain}
                  </span>
                  {profile.id === activeProfileId && (
                    <span className="px-2 py-0.5 rounded-full text-xs bg-forge-100 text-forge-700 font-medium">
                      Active
                    </span>
                  )}
                </div>
                <div className="flex items-center gap-1">
                  {profile.id !== activeProfileId && (
                    <button
                      onClick={() => handleSetActiveProfile(profile.id)}
                      className="px-2 py-1 text-xs text-forge-600 hover:bg-forge-50 rounded transition-colors"
                    >
                      Set Active
                    </button>
                  )}
                  <button
                    onClick={() => setEditingProfile({ ...profile })}
                    className="p-1 text-gray-400 hover:text-gray-600 rounded transition-colors"
                  >
                    <Pencil className="w-3.5 h-3.5" />
                  </button>
                  <button
                    onClick={() => handleDeleteProfile(profile.id)}
                    disabled={profile.id === "manufacturing"}
                    className="p-1 text-gray-400 hover:text-red-500 rounded transition-colors disabled:opacity-30 disabled:cursor-not-allowed"
                  >
                    <Trash2 className="w-3.5 h-3.5" />
                  </button>
                </div>
              </div>
              <div className="flex items-center gap-3 mt-1.5 text-xs text-gray-500">
                <span>{getCategoryCount(profile.categories_json)} categories</span>
                <span>{getCountryList(profile.target_countries_json) || "All countries"}</span>
              </div>
              {profile.description && (
                <p className="text-xs text-gray-400 mt-1">{profile.description}</p>
              )}
            </div>
          ))}
          {searchProfiles.length === 0 && (
            <p className="text-sm text-gray-400 text-center py-4">No search profiles configured</p>
          )}
        </div>

        {/* Edit/Create Profile Form */}
        {editingProfile && (
          <div className="border border-gray-200 rounded-lg p-4 space-y-3 bg-gray-50">
            <h3 className="text-sm font-semibold text-gray-900">
              {editingProfile.id ? "Edit Profile" : "New Profile"}
            </h3>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Name</label>
              <input
                type="text"
                value={editingProfile.name || ""}
                onChange={(e) => setEditingProfile({ ...editingProfile, name: e.target.value })}
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500"
                placeholder="e.g. Clean Tech UK"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Domain</label>
              <input
                type="text"
                value={editingProfile.domain || ""}
                onChange={(e) => setEditingProfile({ ...editingProfile, domain: e.target.value })}
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500"
                placeholder="e.g. cleantech, biotech, manufacturing"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Description</label>
              <textarea
                value={editingProfile.description || ""}
                onChange={(e) => setEditingProfile({ ...editingProfile, description: e.target.value })}
                rows={2}
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500 resize-none"
                placeholder="Optional description"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Categories (JSON array)</label>
              <textarea
                value={editingProfile.categories_json || "[]"}
                onChange={(e) => setEditingProfile({ ...editingProfile, categories_json: e.target.value })}
                rows={4}
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-xs font-mono bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500 resize-none"
                placeholder='["CNC Machining", "Sheet Metal", ...]'
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Target Countries (comma-separated)</label>
              <input
                type="text"
                value={(() => {
                  try {
                    const parsed = JSON.parse(editingProfile.target_countries_json || "[]");
                    return Array.isArray(parsed) ? parsed.join(", ") : editingProfile.target_countries_json || "";
                  } catch {
                    return editingProfile.target_countries_json || "";
                  }
                })()}
                onChange={(e) => {
                  const countries = e.target.value.split(",").map((s) => s.trim()).filter(Boolean);
                  setEditingProfile({ ...editingProfile, target_countries_json: JSON.stringify(countries) });
                }}
                className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500"
                placeholder="DE, FR, GB, NL"
              />
            </div>
            <div className="flex items-center gap-2 pt-1">
              <button
                onClick={handleSaveProfile}
                disabled={profileSaving || !editingProfile.name || !editingProfile.domain}
                className="flex items-center gap-1.5 px-4 py-2 bg-forge-600 hover:bg-forge-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
              >
                {profileSaving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Save className="w-3 h-3" />}
                Save
              </button>
              <button
                onClick={() => setEditingProfile(null)}
                className="px-4 py-2 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs font-medium text-gray-600 transition-colors"
              >
                Cancel
              </button>
            </div>
          </div>
        )}
      </section>

      {/* LLM Backend */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">LLM Backend</h2>
        <div>
          <label className="block text-xs text-gray-500 mb-1">Active Backend</label>
          <select
            value={config.llm_backend || "haiku"}
            onChange={(e) => updateField("llm_backend", e.target.value)}
            className="w-full border border-gray-200 rounded-lg px-3 py-2 text-sm bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-500"
          >
            <option value="haiku">Haiku (Cloud)</option>
            <option value="deepseek">DeepSeek (Cloud)</option>
            <option value="ollama">Ollama (Local)</option>
          </select>
        </div>
        <p className="text-xs text-gray-400">
          Controls which LLM is used for research, enrichment, and deep enrichment.
          Haiku is faster and more reliable. Ollama runs locally but may hang on some hardware.
        </p>
      </section>

      {/* Anthropic */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Anthropic (Cloud LLM)</h2>
          <button
            onClick={handleTestAnthropic}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs text-gray-700 transition-colors"
          >
            <TestTube className="w-3 h-3" />
            Test
            <StatusIcon status={anthropicStatus} />
          </button>
        </div>
        <Input
          label="API Key"
          value={config.anthropic_api_key || ""}
          onChange={(v) => updateField("anthropic_api_key", v)}
          placeholder="sk-ant-..."
          type="password"
          error={validationErrors["anthropic_api_key"]}
        />
        {anthropicStatus === "error" && anthropicError && (
          <p className="text-xs text-red-600">{anthropicError}</p>
        )}
        {anthropicStatus === "success" && (
          <p className="text-xs text-green-600">Connected to Claude Haiku</p>
        )}
      </section>

      {/* DeepSeek */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <div className="flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">DeepSeek (Cloud LLM)</h2>
          <button
            onClick={handleTestDeepSeek}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 rounded-lg text-xs text-gray-700 transition-colors"
          >
            <TestTube className="w-3 h-3" />
            Test
            <StatusIcon status={deepseekStatus} />
          </button>
        </div>
        <Input
          label="API Key"
          value={config.deepseek_api_key || ""}
          onChange={(v) => updateField("deepseek_api_key", v)}
          placeholder="sk-..."
          type="password"
          error={validationErrors["deepseek_api_key"]}
        />
        {deepseekStatus === "error" && deepseekError && (
          <p className="text-xs text-red-600">{deepseekError}</p>
        )}
        {deepseekStatus === "success" && (
          <p className="text-xs text-green-600">Connected to DeepSeek V4</p>
        )}
        <p className="text-xs text-gray-400">
          DeepSeek V4 is significantly cheaper than Haiku ($0.30/$0.50 per MTok vs $0.80/$4.00).
        </p>
      </section>

      {/* OpenAI (Embeddings) */}
      <section className="bg-white rounded-xl border border-gray-200 p-4 space-y-3 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900">OpenAI (Semantic Search)</h2>
        <p className="text-xs text-gray-500">Used for embedding queries in semantic search. Falls back to ForgeOS .env.local if not set here.</p>
        <Input
          label="API Key"
          value={config.openai_api_key || ""}
          onChange={(v) => updateField("openai_api_key", v)}
          placeholder="sk-..."
          type="password"
        />
      </section>

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
        <div>
          <label className="block text-xs text-gray-500 mb-1">Schedules</label>
          <ScheduleCalendar
            schedules={(() => { try { return JSON.parse(config.schedules || "[]"); } catch { return []; } })()}
            templateId={config.auto_outreach_template_id}
            onChange={async (schedules) => {
              const json = JSON.stringify(schedules);
              updateField("schedules", json);
              try { await setConfig("schedules", json); } catch {}
            }}
          />
        </div>
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
  const [showPassword, setShowPassword] = useState(false);
  const isPassword = type === "password";
  return (
    <div>
      <label className="block text-xs text-gray-500 mb-1">{label}</label>
      <div className="relative">
        <input
          type={isPassword && showPassword ? "text" : type}
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder={placeholder}
          min={min}
          max={max}
          className={`w-full px-3 py-2 bg-white border rounded-lg text-sm text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-forge-500 focus:border-forge-500 transition-colors ${
            isPassword ? "pr-16" : ""
          } ${error ? "border-red-300" : "border-gray-300"}`}
        />
        {isPassword && value && (
          <button
            type="button"
            onClick={() => setShowPassword((p) => !p)}
            className="absolute right-2 top-1/2 -translate-y-1/2 text-xs text-gray-400 hover:text-gray-600 transition-colors px-1.5 py-0.5"
          >
            {showPassword ? "Hide" : "Show"}
          </button>
        )}
      </div>
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
