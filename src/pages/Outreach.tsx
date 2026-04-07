import { useEffect, useState, useCallback, useMemo, useRef } from "react";
import {
  Mail,
  Send,
  Eye,
  RefreshCw,
  Loader2,
  FileText,
  Plus,
  Trash2,
  Play,
  Save,
  Square,
  ChevronDown,
  ChevronRight,
  ChevronLeft,
  CheckSquare,
  X,
  Search,
  ExternalLink,
  BarChart3,
  Zap,
  TrendingUp,
  TrendingDown,
  Brain,
  Clock,
  AlertCircle,
  CheckCircle2,
  XCircle,
  Info,
} from "lucide-react";
import DOMPurify from "dompurify";
import {
  getEmails,
  updateEmailStatus,
  refreshEmailStatuses,
  getEmailTemplates,
  saveEmailTemplate,
  deleteEmailTemplate,
  getCampaignEligibleCount,
  startPipeline,
  stopPipeline,
  deleteEmails,
  sendApprovedEmails,
  retryFailedEmails,
  getOutreachCompanies,
  getOutreachStats,
  getCompanyEmailHistory,
  generateDraftsForCompanies,
  syncClaimStatuses,
  getConfig,
  getDailyOutreachStats,
  getExperimentHistory,
  getOutreachInsights,
  getAutopilotStatus,
  EmailTemplate,
  OutreachCompany,
  OutreachStats,
  CompanyEmail,
  DailyOutreachStat,
  ABExperiment,
  OutreachInsight,
  AutopilotStatus,
  getOutreachReadiness,
  OutreachReadiness,
} from "../lib/tauri";
import { useError } from "../contexts/ErrorContext";

const STATUS_COLORS: Record<string, string> = {
  not_contacted: "bg-gray-50 text-gray-400",
  draft: "bg-gray-100 text-gray-600",
  approved: "bg-blue-100 text-blue-700",
  sending: "bg-yellow-100 text-yellow-700",
  sent: "bg-green-100 text-green-700",
  opened: "bg-purple-100 text-purple-700",
  clicked: "bg-indigo-100 text-indigo-700",
  claimed: "bg-teal-100 text-teal-700",
  replied: "bg-emerald-100 text-emerald-700",
  bounced: "bg-red-100 text-red-700",
  failed: "bg-red-100 text-red-700",
};

type Tab = "campaigns" | "emails" | "templates" | "performance";

export default function Outreach() {
  const { showError } = useError();
  const [tab, setTab] = useState<Tab>("campaigns");

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Outreach</h1>
          <p className="text-sm text-gray-500 mt-1">
            Email drafts, templates, and send tracking
          </p>
        </div>
        <div className="flex items-center gap-1 bg-gray-100 rounded-lg p-0.5">
          <button
            onClick={() => setTab("campaigns")}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === "campaigns"
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            <Send className="w-3 h-3 inline mr-1" />
            Campaigns
          </button>
          <button
            onClick={() => setTab("emails")}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === "emails"
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            <Mail className="w-3 h-3 inline mr-1" />
            Email Queue
          </button>
          <button
            onClick={() => setTab("templates")}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === "templates"
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            <FileText className="w-3 h-3 inline mr-1" />
            Templates
          </button>
          <button
            onClick={() => setTab("performance")}
            className={`px-3 py-1.5 rounded-md text-xs font-medium transition-colors ${
              tab === "performance"
                ? "bg-white text-gray-900 shadow-sm"
                : "text-gray-500 hover:text-gray-700"
            }`}
          >
            <BarChart3 className="w-3 h-3 inline mr-1" />
            Performance
          </button>
        </div>
      </div>

      {tab === "campaigns" ? (
        <CampaignsTab showError={showError} />
      ) : tab === "emails" ? (
        <EmailQueueTab showError={showError} />
      ) : tab === "performance" ? (
        <PerformanceTab showError={showError} />
      ) : (
        <TemplatesTab showError={showError} />
      )}
    </div>
  );
}

// --- Email History Item (expandable) ---

function EmailHistoryItem({ email }: { email: CompanyEmail }) {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="border border-gray-100 rounded-md overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full text-left p-2 hover:bg-gray-50 transition-colors"
      >
        <div className="flex items-center gap-2">
          <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${STATUS_COLORS[email.status] || ""}`}>
            {email.status}
          </span>
          {email.ab_variant && (
            <span className="text-[10px] px-1 py-0.5 bg-gray-100 rounded text-gray-500">
              Variant {email.ab_variant}
            </span>
          )}
          {email.claim_status && email.claim_status !== "pending" && (
            <span className="text-[10px] px-1 py-0.5 bg-teal-50 text-teal-700 rounded">
              {email.claim_status}
            </span>
          )}
          <span className="text-[10px] text-gray-400 ml-auto">
            {email.created_at ? new Date(email.created_at).toLocaleDateString() : ""}
          </span>
          {expanded ? (
            <ChevronDown className="w-3 h-3 text-gray-400" />
          ) : (
            <ChevronRight className="w-3 h-3 text-gray-400" />
          )}
        </div>
        <div className="text-xs font-medium text-gray-800 mt-1 truncate">{email.subject}</div>
        {email.sent_at && (
          <div className="text-[10px] text-gray-400">
            Sent: {new Date(email.sent_at).toLocaleString()}
          </div>
        )}
      </button>
      {expanded && (
        <div className="border-t border-gray-100 p-3 bg-gray-50 space-y-2">
          <div className="text-[10px] text-gray-500">
            To: {email.to_email}
          </div>
          <div className="text-xs font-semibold text-gray-800">{email.subject}</div>
          <div
            className="text-xs text-gray-700 whitespace-pre-wrap leading-relaxed"
            dangerouslySetInnerHTML={{ __html: DOMPurify.sanitize(email.body) }}
          />
          {email.opened_at && (
            <div className="text-[10px] text-purple-600">
              Opened: {new Date(email.opened_at).toLocaleString()}
            </div>
          )}
          {email.bounced_at && (
            <div className="text-[10px] text-red-600">
              Bounced: {new Date(email.bounced_at).toLocaleString()}
            </div>
          )}
          {email.last_error && (
            <div className="text-[10px] text-red-500">
              Error: {email.last_error}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

// --- Campaigns Tab ---

function SetupChecklist({ onAllReady }: { onAllReady?: () => void }) {
  const [readiness, setReadiness] = useState<OutreachReadiness | null>(null);
  const [loading, setLoading] = useState(true);
  const [collapsed, setCollapsed] = useState(false);

  useEffect(() => {
    const check = async () => {
      try {
        const r = await getOutreachReadiness();
        setReadiness(r);
        if (r.all_ready && onAllReady) onAllReady();
      } catch {
        // non-critical
      } finally {
        setLoading(false);
      }
    };
    check();
    const interval = setInterval(check, 30_000);
    return () => clearInterval(interval);
  }, [onAllReady]);

  if (loading || !readiness) return null;
  if (readiness.all_ready) return null;

  const checks = [
    { key: "resend_key", label: "Resend API key configured", ok: readiness.resend_key, action: "Settings → Resend API Key" },
    { key: "resend_verified", label: "Resend domain verified", ok: readiness.resend_verified, action: "Verify domain in Resend dashboard" },
    { key: "supabase_connected", label: "Supabase connected", ok: readiness.supabase_connected, action: "Settings → Supabase URL & Key" },
    { key: "ollama_running", label: "Ollama running", ok: readiness.ollama_running, action: "Start Ollama (ollama serve)" },
    { key: "ollama_has_model", label: "Outreach model available", ok: readiness.ollama_has_model, action: "Pull model (ollama pull qwen3.5:27b-q4_K_M)" },
    { key: "from_email", label: "From email configured", ok: readiness.from_email, action: "Settings → From Email" },
    { key: "has_templates", label: "At least 1 email template created", ok: readiness.has_templates, action: "Templates tab → Create" },
    { key: "has_schedule", label: "Schedule time set", ok: readiness.has_schedule, action: "Settings → Schedule Time" },
    { key: "autopilot_configured", label: "Autopilot enabled with template", ok: readiness.autopilot_configured, action: "Settings → Enable Autopilot" },
    { key: "eligible", label: `Eligible companies available (${readiness.eligible_companies})`, ok: readiness.eligible_companies > 0, action: "Run pipeline to discover companies" },
  ];

  const passCount = checks.filter(c => c.ok).length;

  return (
    <div className="bg-amber-50 border border-amber-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setCollapsed(!collapsed)}
        className="w-full flex items-center justify-between p-4 text-left"
      >
        <div className="flex items-center gap-2">
          <AlertCircle className="w-4 h-4 text-amber-600" />
          <span className="text-sm font-medium text-amber-800">
            Setup Checklist — {passCount}/{checks.length} ready
          </span>
        </div>
        {collapsed ? (
          <ChevronRight className="w-4 h-4 text-amber-600" />
        ) : (
          <ChevronDown className="w-4 h-4 text-amber-600" />
        )}
      </button>
      {!collapsed && (
        <div className="px-4 pb-4 space-y-2">
          {checks.map((c) => (
            <div key={c.key} className="flex items-center gap-2 text-sm">
              {c.ok ? (
                <CheckCircle2 className="w-4 h-4 text-green-500 flex-shrink-0" />
              ) : (
                <XCircle className="w-4 h-4 text-red-400 flex-shrink-0" />
              )}
              <span className={c.ok ? "text-gray-600" : "text-gray-900 font-medium"}>
                {c.label}
              </span>
              {!c.ok && (
                <span className="text-xs text-amber-600 ml-auto">{c.action}</span>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function HowAutopilotWorks() {
  const [expanded, setExpanded] = useState(false);

  return (
    <div className="bg-blue-50 border border-blue-200 rounded-lg overflow-hidden">
      <button
        onClick={() => setExpanded(!expanded)}
        className="w-full flex items-center justify-between p-3 text-left"
      >
        <div className="flex items-center gap-2">
          <Info className="w-4 h-4 text-blue-600" />
          <span className="text-sm font-medium text-blue-800">How Autopilot Works</span>
        </div>
        {expanded ? (
          <ChevronDown className="w-4 h-4 text-blue-600" />
        ) : (
          <ChevronRight className="w-4 h-4 text-blue-600" />
        )}
      </button>
      {expanded && (
        <div className="px-4 pb-4 text-sm text-blue-800 space-y-2">
          <ol className="list-decimal list-inside space-y-1.5">
            <li><strong>Set schedule time</strong> (e.g., 02:00) — pipeline runs daily at this time</li>
            <li><strong>Pipeline runs:</strong> discovers companies → enriches → pushes to ForgeOS → learns from yesterday → generates 30 personalised emails</li>
            <li><strong>All 30 drafts auto-approved</strong> immediately after generation</li>
            <li><strong>Hourly sender</strong> drip-sends 5 at a time (5/hr × 6hrs = 30/day)</li>
            <li><strong>Tracking</strong> — system checks opens/bounces every 6 hours (runs in background, no UI needed)</li>
            <li><strong>Next day:</strong> learning cycle analyses what worked, evolves A/B strategy</li>
            <li><strong>By day 5–7:</strong> insights accumulate, emails get more targeted</li>
          </ol>
          <p className="text-xs text-blue-600 mt-2">
            Failed emails auto-retry after 1 hour. If Ollama is down, outreach stages are skipped but research/enrich/push still run.
            If the pipeline fails, it retries once 4 hours later.
          </p>
        </div>
      )}
    </div>
  );
}

function CampaignsTab({ showError }: { showError: (msg: string) => void }) {
  const [companies, setCompanies] = useState<OutreachCompany[]>([]);
  const [total, setTotal] = useState(0);
  const [stats, setStats] = useState<OutreachStats | null>(null);
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);
  const [loading, setLoading] = useState(true);
  const [page, setPage] = useState(0);
  const pageSize = 50;

  // Filters
  const [statusFilter, setStatusFilter] = useState<string>("");
  const [countryFilter, setCountryFilter] = useState<string>("");
  const [categoryFilter, _setCategoryFilter] = useState<string>("");
  const [searchInput, setSearchInput] = useState("");
  const [search, setSearch] = useState("");
  const searchTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Selection
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());
  const [selectedCompany, setSelectedCompany] = useState<OutreachCompany | null>(null);
  const [companyEmails, setCompanyEmails] = useState<CompanyEmail[]>([]);
  const [loadingEmails, setLoadingEmails] = useState(false);

  // Autopilot banner
  const [autopilotConfig, setAutopilotConfig] = useState<Record<string, string>>({});
  const [autopilotStatus, setAutopilotStatus] = useState<AutopilotStatus | null>(null);

  // Generate drafts modal
  const [showDraftModal, setShowDraftModal] = useState(false);
  const [draftTemplateId, setDraftTemplateId] = useState<string>("");
  const [draftAbTemplateId, setDraftAbTemplateId] = useState<string>("");
  const [generating, setGenerating] = useState(false);

  // Keyboard nav
  const [focusIdx, setFocusIdx] = useState(-1);
  const tableRef = useRef<HTMLDivElement>(null);

  const loadCompanies = useCallback(async () => {
    try {
      const result = await getOutreachCompanies({
        outreachStatus: statusFilter || undefined,
        country: countryFilter || undefined,
        category: categoryFilter || undefined,
        search: search || undefined,
        limit: pageSize,
        offset: page * pageSize,
      });
      setCompanies(result.companies);
      setTotal(result.total);
    } catch (e) {
      showError(String(e));
    } finally {
      setLoading(false);
    }
  }, [statusFilter, countryFilter, categoryFilter, search, page, showError]);

  const loadStats = useCallback(async () => {
    try {
      const s = await getOutreachStats();
      setStats(s);
    } catch {
      // non-critical
    }
  }, []);

  const loadTemplates = useCallback(async () => {
    try {
      const t = await getEmailTemplates();
      setTemplates(t);
    } catch {
      // non-critical
    }
  }, []);

  useEffect(() => {
    getConfig().then(setAutopilotConfig).catch(() => {});
    getAutopilotStatus().then(setAutopilotStatus).catch(() => {});
  }, []);

  useEffect(() => {
    loadCompanies();
    loadStats();
    loadTemplates();

    // Auto-refresh stats + claim sync every 60s
    const interval = setInterval(async () => {
      try {
        await syncClaimStatuses();
      } catch {
        // silent
      }
      loadStats();
      loadCompanies();
      getAutopilotStatus().then(setAutopilotStatus).catch(() => {});
    }, 60_000);
    return () => clearInterval(interval);
  }, [loadCompanies, loadStats, loadTemplates]);

  // Debounced search
  useEffect(() => {
    if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    searchTimerRef.current = setTimeout(() => {
      setSearch(searchInput);
      setPage(0);
    }, 300);
    return () => {
      if (searchTimerRef.current) clearTimeout(searchTimerRef.current);
    };
  }, [searchInput]);

  // Sync selectedIds from fresh companies
  useEffect(() => {
    const freshIds = new Set(companies.map((c) => c.id));
    setSelectedIds((prev) => {
      const next = new Set<string>();
      prev.forEach((id) => {
        if (freshIds.has(id)) next.add(id);
      });
      return next;
    });
  }, [companies]);

  // Load email history when company selected
  useEffect(() => {
    if (!selectedCompany) {
      setCompanyEmails([]);
      return;
    }
    let cancelled = false;
    setLoadingEmails(true);
    getCompanyEmailHistory(selectedCompany.id)
      .then((emails) => {
        if (!cancelled) setCompanyEmails(emails);
      })
      .catch(() => {
        if (!cancelled) setCompanyEmails([]);
      })
      .finally(() => {
        if (!cancelled) setLoadingEmails(false);
      });
    return () => { cancelled = true; };
  }, [selectedCompany]);

  // Keyboard navigation
  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      if (e.key === "j") {
        e.preventDefault();
        setFocusIdx((prev) => Math.min(prev + 1, companies.length - 1));
      } else if (e.key === "k") {
        e.preventDefault();
        setFocusIdx((prev) => Math.max(prev - 1, 0));
      } else if (e.key === " " && focusIdx >= 0) {
        e.preventDefault();
        const c = companies[focusIdx];
        if (c) toggleSelect(c.id);
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [companies, focusIdx]);

  useEffect(() => {
    if (focusIdx >= 0 && focusIdx < companies.length) {
      setSelectedCompany(companies[focusIdx]);
    }
  }, [focusIdx, companies]);

  function toggleSelect(id: string) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleSelectAll() {
    if (selectedIds.size === companies.length) {
      setSelectedIds(new Set());
    } else {
      setSelectedIds(new Set(companies.map((c) => c.id)));
    }
  }

  async function handleGenerateDrafts() {
    if (!draftTemplateId || selectedIds.size === 0) return;
    setGenerating(true);
    try {
      const result = await generateDraftsForCompanies({
        companyIds: Array.from(selectedIds),
        templateId: draftTemplateId,
        abTemplateId: draftAbTemplateId || undefined,
      });
      setShowDraftModal(false);
      setSelectedIds(new Set());
      loadCompanies();
      loadStats();
      alert(`Created ${result.drafts_created} drafts${result.errors > 0 ? ` (${result.errors} errors)` : ""}`);
    } catch (e) {
      showError(String(e));
    } finally {
      setGenerating(false);
    }
  }

  const totalPages = Math.ceil(total / pageSize);

  const autopilotEnabled = autopilotConfig.auto_outreach_enabled === "true";
  const autopilotTemplateName = autopilotEnabled
    ? templates.find((t) => t.id === autopilotConfig.auto_outreach_template_id)?.name
    : null;

  return (
    <div className="space-y-4">
      {/* Setup Checklist — shown when not fully configured */}
      <SetupChecklist />

      {/* How Autopilot Works guide */}
      {!autopilotEnabled && <HowAutopilotWorks />}

      {/* Enhanced Autopilot Banner */}
      {autopilotEnabled && autopilotStatus != null && (
        <div className="bg-green-50 border border-green-200 rounded-lg p-4">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full bg-green-500 animate-pulse" />
              <span className="text-sm font-medium text-green-800">
                Autopilot Active
                {autopilotTemplateName ? ` — "${autopilotTemplateName}"` : ""}
              </span>
              {autopilotStatus.active_generation != null && (
                <span className="text-xs bg-green-100 text-green-700 px-2 py-0.5 rounded-full">
                  Gen {autopilotStatus.active_generation}
                </span>
              )}
            </div>
            {autopilotStatus.schedule_time && (
              <span className="text-xs text-green-600 flex items-center gap-1">
                <Clock className="w-3 h-3" />
                Pipeline at {autopilotStatus.schedule_time}
              </span>
            )}
          </div>

          {/* Progress bar */}
          <div className="mt-2 ml-4">
            <div className="flex items-center gap-2">
              <div className="flex-1 bg-green-200 rounded-full h-2">
                <div
                  className="bg-green-600 h-2 rounded-full transition-all"
                  style={{ width: `${Math.min(100, (autopilotStatus.sent_today / autopilotStatus.daily_limit) * 100)}%` }}
                />
              </div>
              <span className="text-xs font-mono text-green-700 whitespace-nowrap">
                {autopilotStatus.sent_today}/{autopilotStatus.daily_limit} today
              </span>
            </div>
            <div className="flex items-center gap-4 mt-1.5 text-xs text-green-600">
              <span>{autopilotStatus.approved_queued} queued</span>
              <span>{autopilotStatus.batch_size}/hr send rate</span>
              {autopilotStatus.insight_count > 0 && (
                <span className="flex items-center gap-1">
                  <Brain className="w-3 h-3" />
                  {autopilotStatus.insight_count} insights learned
                </span>
              )}
              {autopilotStatus.last_learning_at && (
                <span>Last learning: {new Date(autopilotStatus.last_learning_at + "Z").toLocaleDateString()}</span>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Stats Bar */}
      {stats != null && (
        <div className="grid grid-cols-4 gap-3">
          <div className="bg-white border border-gray-200 rounded-lg p-3">
            <div className="text-xs text-gray-500 font-medium">Contacted</div>
            <div className="text-xl font-bold text-gray-900 mt-0.5">{stats.total_sent}</div>
            <div className="text-xs text-gray-400">{stats.total_drafted} drafts, {stats.total_approved} approved</div>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-3">
            <div className="text-xs text-gray-500 font-medium">Open Rate</div>
            <div className="text-xl font-bold text-gray-900 mt-0.5">{stats.open_rate}%</div>
            <div className="text-xs text-gray-400">{stats.total_opened} opened of {stats.total_sent} sent</div>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-3">
            <div className="text-xs text-gray-500 font-medium">Bounce Rate</div>
            <div className="text-xl font-bold text-gray-900 mt-0.5">{stats.bounce_rate}%</div>
            <div className="text-xs text-gray-400">{stats.total_bounced} bounced</div>
          </div>
          <div className="bg-white border border-gray-200 rounded-lg p-3">
            <div className="text-xs text-gray-500 font-medium">Claim Rate</div>
            <div className="text-xl font-bold text-gray-900 mt-0.5">{stats.claim_rate}%</div>
            <div className="text-xs text-gray-400">{stats.total_claimed} claimed</div>
          </div>
        </div>
      )}

      {/* A/B Comparison */}
      {stats != null && stats.ab_variants.length >= 2 && (
        <div className="bg-white border border-gray-200 rounded-lg p-3 text-sm">
          <span className="font-medium text-gray-700">A/B Test: </span>
          {stats.ab_variants.map((v, i) => (
            <span key={v.variant}>
              {i > 0 && " vs "}
              <span className={`font-semibold ${
                stats.ab_variants.length === 2 &&
                v.open_rate === Math.max(...stats.ab_variants.map((x) => x.open_rate)) &&
                v.sent >= 20
                  ? "text-green-700"
                  : "text-gray-900"
              }`}>
                Template {v.variant}: {v.open_rate}% opens
              </span>
              <span className="text-gray-400"> ({v.sent} sent)</span>
            </span>
          ))}
        </div>
      )}

      {/* Filters + Bulk Actions */}
      <div className="flex items-center gap-2 flex-wrap">
        <div className="relative flex-1 min-w-[200px] max-w-xs">
          <Search className="w-3.5 h-3.5 absolute left-2.5 top-1/2 -translate-y-1/2 text-gray-400" />
          <input
            type="text"
            placeholder="Search companies..."
            value={searchInput}
            onChange={(e) => setSearchInput(e.target.value)}
            className="w-full pl-8 pr-3 py-1.5 text-xs border border-gray-200 rounded-md focus:outline-none focus:ring-1 focus:ring-blue-500"
          />
        </div>
        <select
          value={statusFilter}
          onChange={(e) => { setStatusFilter(e.target.value); setPage(0); }}
          className="text-xs border border-gray-200 rounded-md px-2 py-1.5 bg-white"
        >
          <option value="">All statuses</option>
          <option value="not_contacted">Not contacted</option>
          <option value="draft">Draft</option>
          <option value="approved">Approved</option>
          <option value="sent">Sent</option>
          <option value="opened">Opened</option>
          <option value="bounced">Bounced</option>
          <option value="failed">Failed</option>
        </select>
        <select
          value={countryFilter}
          onChange={(e) => { setCountryFilter(e.target.value); setPage(0); }}
          className="text-xs border border-gray-200 rounded-md px-2 py-1.5 bg-white"
        >
          <option value="">All countries</option>
          <option value="GB">GB</option>
          <option value="DE">DE</option>
          <option value="FR">FR</option>
          <option value="NL">NL</option>
          <option value="US">US</option>
        </select>

        {selectedIds.size > 0 && (
          <button
            onClick={() => { setShowDraftModal(true); setDraftTemplateId(""); setDraftAbTemplateId(""); }}
            className="ml-auto px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700"
          >
            <FileText className="w-3 h-3 inline mr-1" />
            Generate Drafts ({selectedIds.size})
          </button>
        )}

        <div className="ml-auto text-xs text-gray-500">
          {total.toLocaleString()} companies
        </div>
      </div>

      {/* Main content: table + detail */}
      <div className="flex gap-4" style={{ minHeight: 500 }}>
        {/* Company Table (left) */}
        <div ref={tableRef} className="flex-[3] bg-white border border-gray-200 rounded-lg overflow-hidden flex flex-col">
          {/* Table header */}
          <div className="grid grid-cols-[32px_1fr_120px_60px_180px_100px_100px] gap-2 px-3 py-2 bg-gray-50 border-b border-gray-200 text-xs font-medium text-gray-500">
            <div>
              <input
                type="checkbox"
                checked={companies.length > 0 && selectedIds.size === companies.length}
                onChange={toggleSelectAll}
                className="rounded"
              />
            </div>
            <div>Company</div>
            <div>Category</div>
            <div>Country</div>
            <div>Contact</div>
            <div>Status</div>
            <div>Last Action</div>
          </div>

          {/* Table body */}
          <div className="flex-1 overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center py-12 text-gray-400">
                <Loader2 className="w-4 h-4 animate-spin mr-2" />
                Loading...
              </div>
            ) : companies.length === 0 ? (
              <div className="flex items-center justify-center py-12 text-gray-400 text-sm">
                No companies found
              </div>
            ) : (
              companies.map((c, idx) => (
                <div
                  key={c.id}
                  onClick={() => { setSelectedCompany(c); setFocusIdx(idx); }}
                  className={`grid grid-cols-[32px_1fr_120px_60px_180px_100px_100px] gap-2 px-3 py-2 text-xs border-b border-gray-100 cursor-pointer transition-colors ${
                    selectedCompany?.id === c.id
                      ? "bg-blue-50"
                      : focusIdx === idx
                        ? "bg-gray-50"
                        : "hover:bg-gray-50"
                  }`}
                >
                  <div onClick={(e) => e.stopPropagation()}>
                    <input
                      type="checkbox"
                      checked={selectedIds.has(c.id)}
                      onChange={() => toggleSelect(c.id)}
                      className="rounded"
                    />
                  </div>
                  <div className="truncate font-medium text-gray-900">{c.name}</div>
                  <div className="truncate text-gray-500">{c.subcategory || "—"}</div>
                  <div className="text-gray-500">{c.country || "—"}</div>
                  <div className="truncate text-gray-500">{c.contact_email || "—"}</div>
                  <div>
                    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${STATUS_COLORS[c.outreach_status] || STATUS_COLORS.not_contacted}`}>
                      {c.outreach_status}
                    </span>
                  </div>
                  <div className="text-gray-400">
                    {c.last_email_at ? new Date(c.last_email_at).toLocaleDateString() : "—"}
                  </div>
                </div>
              ))
            )}
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between px-3 py-2 border-t border-gray-200 bg-gray-50">
              <button
                onClick={() => setPage((p) => Math.max(0, p - 1))}
                disabled={page === 0}
                className="text-xs text-gray-600 hover:text-gray-900 disabled:opacity-30"
              >
                <ChevronLeft className="w-3.5 h-3.5 inline" /> Previous
              </button>
              <span className="text-xs text-gray-500">
                Page {page + 1} of {totalPages}
              </span>
              <button
                onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                disabled={page >= totalPages - 1}
                className="text-xs text-gray-600 hover:text-gray-900 disabled:opacity-30"
              >
                Next <ChevronRight className="w-3.5 h-3.5 inline" />
              </button>
            </div>
          )}
        </div>

        {/* Company Detail (right) */}
        <div className="flex-[2] bg-white border border-gray-200 rounded-lg overflow-hidden flex flex-col">
          {selectedCompany ? (
            <div className="flex-1 overflow-y-auto p-4 space-y-4">
              {/* Company Info */}
              <div>
                <h3 className="text-sm font-bold text-gray-900">{selectedCompany.name}</h3>
                {selectedCompany.website_url && (
                  <a
                    href={selectedCompany.website_url.startsWith("http") ? selectedCompany.website_url : `https://${selectedCompany.website_url}`}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-xs text-blue-600 hover:underline inline-flex items-center gap-0.5"
                  >
                    {selectedCompany.website_url} <ExternalLink className="w-2.5 h-2.5" />
                  </a>
                )}
                <div className="mt-2 grid grid-cols-2 gap-1 text-xs">
                  <div className="text-gray-500">Category</div>
                  <div className="text-gray-900">{selectedCompany.subcategory || "—"}</div>
                  <div className="text-gray-500">Location</div>
                  <div className="text-gray-900">
                    {[selectedCompany.city, selectedCompany.country].filter(Boolean).join(", ") || "—"}
                  </div>
                  <div className="text-gray-500">Contact</div>
                  <div className="text-gray-900">
                    {selectedCompany.contact_name || "—"}
                    {selectedCompany.contact_title ? ` (${selectedCompany.contact_title})` : ""}
                  </div>
                  <div className="text-gray-500">Email</div>
                  <div className="text-gray-900 truncate">{selectedCompany.contact_email || "—"}</div>
                  <div className="text-gray-500">Status</div>
                  <div>
                    <span className={`inline-block px-1.5 py-0.5 rounded text-[10px] font-medium ${STATUS_COLORS[selectedCompany.outreach_status] || ""}`}>
                      {selectedCompany.outreach_status}
                    </span>
                  </div>
                </div>
                {selectedCompany.description && (
                  <p className="mt-2 text-xs text-gray-600 line-clamp-3">{selectedCompany.description}</p>
                )}
              </div>

              {/* Single company draft button */}
              {selectedCompany.outreach_status === "not_contacted" && (
                <button
                  onClick={() => {
                    setSelectedIds(new Set([selectedCompany.id]));
                    setShowDraftModal(true);
                    setDraftTemplateId("");
                    setDraftAbTemplateId("");
                  }}
                  className="w-full px-3 py-1.5 text-xs font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700"
                >
                  <FileText className="w-3 h-3 inline mr-1" />
                  Generate Draft
                </button>
              )}

              {/* Email History */}
              <div>
                <h4 className="text-xs font-semibold text-gray-700 mb-2">Email History</h4>
                {loadingEmails ? (
                  <div className="text-xs text-gray-400 flex items-center gap-1">
                    <Loader2 className="w-3 h-3 animate-spin" /> Loading...
                  </div>
                ) : companyEmails.length === 0 ? (
                  <div className="text-xs text-gray-400">No emails yet</div>
                ) : (
                  <div className="space-y-2">
                    {companyEmails.map((email) => (
                      <EmailHistoryItem key={email.id} email={email} />
                    ))}
                  </div>
                )}
              </div>
            </div>
          ) : (
            <div className="flex-1 flex items-center justify-center text-xs text-gray-400">
              Select a company to view details
            </div>
          )}
        </div>
      </div>

      {/* Generate Drafts Modal */}
      {showDraftModal && (
        <div className="fixed inset-0 bg-black/30 flex items-center justify-center z-50" onClick={() => !generating && setShowDraftModal(false)}>
          <div className="bg-white rounded-xl shadow-xl p-6 w-full max-w-md" onClick={(e) => e.stopPropagation()}>
            <h3 className="text-sm font-bold text-gray-900 mb-4">
              Generate Drafts for {selectedIds.size} {selectedIds.size === 1 ? "company" : "companies"}
            </h3>

            <div className="space-y-3">
              <div>
                <label className="text-xs font-medium text-gray-700 block mb-1">Template</label>
                <select
                  value={draftTemplateId}
                  onChange={(e) => setDraftTemplateId(e.target.value)}
                  className="w-full text-xs border border-gray-200 rounded-md px-2 py-1.5 bg-white"
                >
                  <option value="">Select a template...</option>
                  {templates.map((t) => (
                    <option key={t.id} value={t.id}>{t.name}</option>
                  ))}
                </select>
              </div>

              <div>
                <label className="text-xs font-medium text-gray-700 block mb-1">
                  A/B Template (optional)
                </label>
                <select
                  value={draftAbTemplateId}
                  onChange={(e) => setDraftAbTemplateId(e.target.value)}
                  className="w-full text-xs border border-gray-200 rounded-md px-2 py-1.5 bg-white"
                >
                  <option value="">No A/B test</option>
                  {templates.filter((t) => t.id !== draftTemplateId).map((t) => (
                    <option key={t.id} value={t.id}>{t.name}</option>
                  ))}
                </select>
                {draftAbTemplateId && (
                  <p className="text-[10px] text-gray-400 mt-1">
                    Companies will alternate A/B (50/50 split)
                  </p>
                )}
              </div>
            </div>

            <div className="flex justify-end gap-2 mt-5">
              <button
                onClick={() => setShowDraftModal(false)}
                disabled={generating}
                className="px-3 py-1.5 text-xs text-gray-600 hover:text-gray-900"
              >
                Cancel
              </button>
              <button
                onClick={handleGenerateDrafts}
                disabled={!draftTemplateId || generating}
                className="px-4 py-1.5 text-xs font-medium bg-blue-600 text-white rounded-md hover:bg-blue-700 disabled:opacity-50 flex items-center gap-1"
              >
                {generating ? (
                  <>
                    <Loader2 className="w-3 h-3 animate-spin" />
                    Generating...
                  </>
                ) : (
                  "Generate"
                )}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// --- Email Queue Tab (existing functionality) ---

function EmailQueueTab({ showError }: { showError: (msg: string) => void }) {
  const [emails, setEmails] = useState<Record<string, unknown>[]>([]);
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);
  const [selectedEmail, setSelectedEmail] = useState<Record<
    string,
    unknown
  > | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [refreshCount, setRefreshCount] = useState<number | null>(null);
  const [approving, setApproving] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [sending, setSending] = useState(false);
  const [retrying, setRetrying] = useState(false);
  const [sendResult, setSendResult] = useState<{ sent: number; failed: number } | null>(null);
  const [collapsedGroups, setCollapsedGroups] = useState<Set<string>>(new Set());
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  useEffect(() => {
    loadEmails();
    loadTemplates();

    // Auto-refresh statuses every 60s
    const interval = setInterval(async () => {
      try {
        await refreshEmailStatuses();
        const data = await getEmails(undefined, 100);
        setEmails(data);
      } catch {
        // silent — don't disrupt UI on background poll failure
      }
    }, 60_000);
    return () => clearInterval(interval);
  }, []);

  async function loadTemplates() {
    try {
      const data = await getEmailTemplates();
      setTemplates(data);
    } catch {
      // templates are optional context — don't block the queue
    }
  }

  // Map template_id → template name
  const templateNames = useMemo(() => {
    const map = new Map<string, string>();
    for (const t of templates) map.set(t.id, t.name);
    return map;
  }, [templates]);

  // Group emails by template_id (preserving order within groups)
  const emailGroups = useMemo(() => {
    const groups: { key: string; label: string; emails: Record<string, unknown>[] }[] = [];
    const groupMap = new Map<string, Record<string, unknown>[]>();
    const order: string[] = [];

    for (const email of emails) {
      const tid = String(email.template_id || "");
      const key = tid || "__none__";
      if (!groupMap.has(key)) {
        groupMap.set(key, []);
        order.push(key);
      }
      groupMap.get(key)!.push(email);
    }

    for (const key of order) {
      const label = key === "__none__"
        ? "Manual / Legacy"
        : templateNames.get(key) || `Template ${key.slice(0, 8)}...`;
      groups.push({ key, label, emails: groupMap.get(key)! });
    }

    return groups;
  }, [emails, templateNames]);

  function toggleGroup(key: string) {
    setCollapsedGroups((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }

  // Flat list of visible (non-collapsed) emails for keyboard nav
  const visibleEmails = useMemo(() => {
    const flat: Record<string, unknown>[] = [];
    for (const group of emailGroups) {
      if (!collapsedGroups.has(group.key)) {
        flat.push(...group.emails);
      }
    }
    return flat;
  }, [emailGroups, collapsedGroups]);

  // Ref for scrolling selected row into view
  const listRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Don't intercept if user is typing in an input/textarea
      const tag = (e.target as HTMLElement)?.tagName;
      if (tag === "INPUT" || tag === "TEXTAREA" || tag === "SELECT") return;

      if (e.key === "ArrowDown" || e.key === "j") {
        e.preventDefault();
        setSelectedEmail((prev) => {
          if (!prev) return visibleEmails[0] ?? null;
          const idx = visibleEmails.findIndex((em) => em.id === prev.id);
          const next = visibleEmails[idx + 1];
          return next ?? prev;
        });
      } else if (e.key === "ArrowUp" || e.key === "k") {
        e.preventDefault();
        setSelectedEmail((prev) => {
          if (!prev) return visibleEmails[visibleEmails.length - 1] ?? null;
          const idx = visibleEmails.findIndex((em) => em.id === prev.id);
          const next = visibleEmails[idx - 1];
          return next ?? prev;
        });
      } else if (e.key === "Delete" || e.key === "Backspace") {
        e.preventDefault();
        handleDeleteSelected();
      }
    }

    window.addEventListener("keydown", handleKeyDown);
    return () => window.removeEventListener("keydown", handleKeyDown);
  }, [visibleEmails]);

  // Scroll selected email row into view
  useEffect(() => {
    if (!selectedEmail || !listRef.current) return;
    const row = listRef.current.querySelector(`[data-email-id="${String(selectedEmail.id)}"]`);
    if (row) row.scrollIntoView({ block: "nearest" });
  }, [selectedEmail]);

  async function loadEmails() {
    setLoading(true);
    try {
      const data = await getEmails(undefined, 100);
      setEmails(data);
      setSelectedEmail((prev) => {
        if (!prev) return null;
        return data.find((e) => e.id === prev.id) ?? null;
      });
    } catch (e) {
      showError(`Failed to load emails: ${e}`);
    }
    setLoading(false);
  }

  async function handleApproveEmail(id: string) {
    if (approving) return;
    setApproving(true);
    try {
      await updateEmailStatus(id, "approved");
      await loadEmails();
    } catch (e) {
      showError(`Failed to approve email: ${e}`);
    }
    setApproving(false);
  }

  async function handleRefreshStatuses() {
    setRefreshing(true);
    setRefreshCount(null);
    try {
      const count = await refreshEmailStatuses();
      setRefreshCount(count);
      await loadEmails();
    } catch (e) {
      showError(`Failed to refresh statuses: ${e}`);
      setRefreshCount(-1);
    }
    setRefreshing(false);
  }

  function toggleSelect(id: string) {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  }

  function toggleSelectAll(groupEmails: Record<string, unknown>[]) {
    const groupIds = groupEmails.map((e) => String(e.id));
    const allSelected = groupIds.every((id) => selectedIds.has(id));
    setSelectedIds((prev) => {
      const next = new Set(prev);
      for (const id of groupIds) {
        if (allSelected) next.delete(id);
        else next.add(id);
      }
      return next;
    });
  }

  async function handleDeleteSelected() {
    if (deleting || selectedIds.size === 0) return;
    setDeleting(true);
    try {
      await deleteEmails(Array.from(selectedIds));
      setSelectedIds(new Set());
      setSelectedEmail((prev) => {
        if (prev && selectedIds.has(String(prev.id))) return null;
        return prev;
      });
      await loadEmails();
    } catch (e) {
      showError(`Failed to delete emails: ${e}`);
    }
    setDeleting(false);
  }

  async function handleDeleteSingle(id: string) {
    if (deleting) return;
    setDeleting(true);
    try {
      await deleteEmails([id]);
      setSelectedEmail((prev) => {
        if (prev && String(prev.id) === id) return null;
        return prev;
      });
      setSelectedIds((prev) => {
        const next = new Set(prev);
        next.delete(id);
        return next;
      });
      await loadEmails();
    } catch (e) {
      showError(`Failed to delete email: ${e}`);
    }
    setDeleting(false);
  }

  async function handleApproveSelected() {
    if (approving || selectedIds.size === 0) return;
    setApproving(true);
    try {
      for (const id of selectedIds) {
        await updateEmailStatus(id, "approved");
      }
      setSelectedIds(new Set());
      await loadEmails();
    } catch (e) {
      showError(`Failed to approve emails: ${e}`);
    }
    setApproving(false);
  }

  const approvedCount = useMemo(
    () => emails.filter((e) => e.status === "approved").length,
    [emails]
  );

  const failedCount = useMemo(
    () => emails.filter((e) => e.status === "failed").length,
    [emails]
  );

  async function handleRetryFailed() {
    if (retrying) return;
    setRetrying(true);
    try {
      await retryFailedEmails();
      await loadEmails();
    } catch (e) {
      showError(`Failed to retry emails: ${e}`);
    }
    setRetrying(false);
  }

  async function handleSendApproved() {
    if (sending) return;
    setSending(true);
    setSendResult(null);
    try {
      const result = await sendApprovedEmails();
      setSendResult({ sent: result.sent, failed: result.failed });
      await loadEmails();
    } catch (e) {
      showError(`Failed to send emails: ${e}`);
    }
    setSending(false);
  }

  // Sync selectedIds after loadEmails — drop stale IDs
  useEffect(() => {
    const currentIds = new Set(emails.map((e) => String(e.id)));
    setSelectedIds((prev) => {
      const next = new Set<string>();
      for (const id of prev) {
        if (currentIds.has(id)) next.add(id);
      }
      if (next.size === prev.size) return prev;
      return next;
    });
  }, [emails]);

  return (
    <>
      <div className="flex justify-end">
        <div className="flex items-center gap-2">
          {approvedCount > 0 && (
            <button
              onClick={handleSendApproved}
              disabled={sending}
              className="flex items-center gap-2 px-3 py-1.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {sending ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : (
                <Send className="w-3 h-3" />
              )}
              {sending ? "Sending..." : `Send ${approvedCount} Approved`}
            </button>
          )}
          {failedCount > 0 && (
            <button
              onClick={handleRetryFailed}
              disabled={retrying}
              className="flex items-center gap-2 px-3 py-1.5 bg-amber-500 hover:bg-amber-600 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {retrying ? (
                <Loader2 className="w-3 h-3 animate-spin" />
              ) : (
                <RefreshCw className="w-3 h-3" />
              )}
              {retrying ? "Retrying..." : `Retry ${failedCount} Failed`}
            </button>
          )}
          {sendResult != null && (
            <span className="text-xs text-green-600">
              {sendResult.sent} sent{sendResult.failed > 0 ? `, ${sendResult.failed} failed` : ""}
            </span>
          )}
          <button
            onClick={handleRefreshStatuses}
            disabled={refreshing}
            className="flex items-center gap-2 px-3 py-1.5 bg-gray-100 hover:bg-gray-200 disabled:opacity-50 rounded-lg text-xs text-gray-700 transition-colors"
          >
            {refreshing ? (
              <Loader2 className="w-3 h-3 animate-spin" />
            ) : (
              <RefreshCw className="w-3 h-3" />
            )}
            Refresh Status
          </button>
          {refreshCount !== null && refreshCount >= 0 && (
            <span className="text-xs text-green-600">
              {refreshCount} updated
            </span>
          )}
          {refreshCount === -1 && (
            <span className="text-xs text-red-600">Failed</span>
          )}
        </div>
      </div>

      {/* Bulk action bar */}
      {selectedIds.size > 0 && (
        <div className="flex items-center gap-3 px-4 py-2.5 bg-blue-50 border border-blue-200 rounded-lg">
          <CheckSquare className="w-4 h-4 text-blue-600 shrink-0" />
          <span className="text-sm font-medium text-blue-800">
            {selectedIds.size} selected
          </span>
          <div className="flex items-center gap-2 ml-auto">
            <button
              onClick={handleApproveSelected}
              disabled={approving}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {approving ? <Loader2 className="w-3 h-3 animate-spin" /> : <Send className="w-3 h-3" />}
              Approve Selected
            </button>
            <button
              onClick={handleDeleteSelected}
              disabled={deleting}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
            >
              {deleting ? <Loader2 className="w-3 h-3 animate-spin" /> : <Trash2 className="w-3 h-3" />}
              Delete Selected
            </button>
            <button
              onClick={() => setSelectedIds(new Set())}
              className="flex items-center gap-1.5 px-3 py-1.5 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg text-xs font-medium text-gray-700 transition-colors"
            >
              <X className="w-3 h-3" />
              Clear
            </button>
          </div>
        </div>
      )}

      <div className="flex gap-4">
        {/* Email list */}
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-sm font-semibold text-gray-900">
              Email Queue ({emails.length})
            </h2>
          </div>

          <div ref={listRef} className="max-h-[calc(100vh-280px)] overflow-y-auto">
            {loading ? (
              <div className="flex items-center justify-center p-8">
                <Loader2 className="w-5 h-5 text-gray-400 animate-spin" />
              </div>
            ) : emails.length === 0 ? (
              <div className="p-8 text-center text-gray-400 text-sm">
                No emails yet. Run the outreach pipeline stage to generate
                emails.
              </div>
            ) : (
              emailGroups.map((group) => {
                const isCollapsed = collapsedGroups.has(group.key);
                const statusCounts = group.emails.reduce<Record<string, number>>((acc, e) => {
                  const s = String(e.status || "draft");
                  acc[s] = (acc[s] || 0) + 1;
                  return acc;
                }, {});

                return (
                  <div key={group.key}>
                    {/* Campaign group header */}
                    <div
                      className="flex items-center gap-2 px-4 py-2.5 bg-gray-50 border-b border-gray-200 cursor-pointer hover:bg-gray-100 transition-colors sticky top-0 z-10"
                      onClick={() => toggleGroup(group.key)}
                    >
                      <input
                        type="checkbox"
                        className="w-3.5 h-3.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0"
                        checked={group.emails.length > 0 && group.emails.every((e) => selectedIds.has(String(e.id)))}
                        onChange={(e) => {
                          e.stopPropagation();
                          toggleSelectAll(group.emails);
                        }}
                        onClick={(e) => e.stopPropagation()}
                      />
                      {isCollapsed ? (
                        <ChevronRight className="w-3.5 h-3.5 text-gray-400 shrink-0" />
                      ) : (
                        <ChevronDown className="w-3.5 h-3.5 text-gray-400 shrink-0" />
                      )}
                      <FileText className="w-3.5 h-3.5 text-blue-500 shrink-0" />
                      <span className="text-xs font-semibold text-gray-700 truncate">
                        {group.label}
                      </span>
                      <span className="text-xs text-gray-400 ml-auto shrink-0">
                        {group.emails.length} email{group.emails.length !== 1 ? "s" : ""}
                      </span>
                      <div className="flex gap-1 shrink-0">
                        {Object.entries(statusCounts).map(([status, count]) => (
                          <span
                            key={status}
                            className={`px-1.5 py-0 rounded-full text-[10px] font-medium ${STATUS_COLORS[status] || STATUS_COLORS.draft}`}
                          >
                            {count} {status}
                          </span>
                        ))}
                      </div>
                    </div>

                    {/* Email rows */}
                    {!isCollapsed && (
                      <div className="divide-y divide-gray-100">
                        {group.emails.map((email) => (
                          <div
                            key={String(email.id)}
                            data-email-id={String(email.id)}
                            className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                              selectedEmail?.id === email.id
                                ? "bg-blue-50"
                                : "hover:bg-gray-50"
                            }`}
                            onClick={() => setSelectedEmail(email)}
                          >
                            <input
                              type="checkbox"
                              className="w-3.5 h-3.5 rounded border-gray-300 text-blue-600 focus:ring-blue-500 shrink-0"
                              checked={selectedIds.has(String(email.id))}
                              onChange={() => toggleSelect(String(email.id))}
                              onClick={(e) => e.stopPropagation()}
                            />
                            <Mail className="w-4 h-4 text-gray-400 shrink-0" />
                            <div className="flex-1 min-w-0">
                              <p className="text-sm font-medium text-gray-900 truncate">
                                {String(email.company_name || email.to_email || "")}
                              </p>
                              <p className="text-xs text-gray-500 truncate">
                                {String(email.subject || "")}
                              </p>
                            </div>
                            <span
                              className={`px-2 py-0.5 rounded-full text-xs ${STATUS_COLORS[String(email.status)] || STATUS_COLORS.draft}`}
                            >
                              {String(email.status || "")}
                            </span>
                          </div>
                        ))}
                      </div>
                    )}
                  </div>
                );
              })
            )}
          </div>
        </div>

        {/* Email preview — realistic inbox rendering */}
        {selectedEmail && (
          <div className="w-[520px] flex flex-col gap-3">
            {/* Status bar + actions */}
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-2">
                <span
                  className={`px-2 py-0.5 rounded-full text-xs font-medium ${STATUS_COLORS[String(selectedEmail.status)] || STATUS_COLORS.draft}`}
                >
                  {String(selectedEmail.status || "")}
                </span>
                {typeof selectedEmail.template_id === "string" && selectedEmail.template_id !== "" && (
                  <span className="flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium bg-blue-50 text-blue-700">
                    <FileText className="w-3 h-3" />
                    {templateNames.get(String(selectedEmail.template_id)) || "Template"}
                  </span>
                )}
                {typeof selectedEmail.claim_token === "string" && selectedEmail.claim_token !== "" && (
                  <span className="text-xs text-blue-500">
                    Token: {selectedEmail.claim_token.slice(0, 12)}...
                  </span>
                )}
              </div>
              {selectedEmail.sent_at ? (
                <div className="flex items-center gap-2 text-xs text-gray-500">
                  <Eye className="w-3 h-3" />
                  {"Sent: " + String(selectedEmail.sent_at)}
                  {selectedEmail.opened_at
                    ? " | Opened: " + String(selectedEmail.opened_at)
                    : null}
                </div>
              ) : null}
              {selectedEmail.status === "failed" && typeof selectedEmail.last_error === "string" && selectedEmail.last_error !== "" ? (
                <div className="text-xs text-red-600 bg-red-50 px-2 py-1 rounded max-w-xs truncate" title={selectedEmail.last_error}>
                  {selectedEmail.last_error.slice(0, 80)}
                </div>
              ) : null}
            </div>

            {/* Email client mockup */}
            <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden flex flex-col max-h-[calc(100vh-300px)]">
              {/* Email header — mimics inbox style */}
              <div className="px-5 py-4 border-b border-gray-100">
                <h3 className="text-base font-semibold text-gray-900 leading-snug">
                  {String(selectedEmail.subject || "")}
                </h3>
                <div className="mt-3 flex items-start gap-3">
                  <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center shrink-0 mt-0.5">
                    <span className="text-white text-xs font-bold">TF</span>
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="flex items-baseline gap-2">
                      <span className="text-sm font-medium text-gray-900">Tristan Fischer</span>
                      <span className="text-xs text-gray-400">&lt;tristan@fractionalforge.app&gt;</span>
                    </div>
                    <p className="text-xs text-gray-500 mt-0.5">
                      to {String(selectedEmail.to_email || "")}
                    </p>
                  </div>
                </div>
              </div>

              {/* Email body — white background like a real email */}
              <div className="flex-1 overflow-y-auto px-5 py-5">
                <div
                  className="text-sm text-gray-800 leading-relaxed [&_p]:mb-3 [&_ul]:mb-3 [&_ul]:pl-5 [&_ul]:list-disc [&_li]:mb-1 [&_a]:text-blue-600 [&_a]:underline [&_strong]:font-semibold"
                  dangerouslySetInnerHTML={{
                    __html: DOMPurify.sanitize(
                      String(selectedEmail.body || "")
                    ),
                  }}
                />
              </div>
            </div>

            {/* Action buttons below the email */}
            {selectedEmail.status === "draft" && (
              <div className="flex gap-2">
                <button
                  onClick={() =>
                    handleApproveEmail(String(selectedEmail.id))
                  }
                  disabled={approving}
                  className="flex-1 flex items-center justify-center gap-2 px-4 py-2.5 bg-green-600 hover:bg-green-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
                >
                  <Send className="w-4 h-4" />
                  Approve & Send
                </button>
                <button
                  onClick={() =>
                    handleDeleteSingle(String(selectedEmail.id))
                  }
                  disabled={deleting}
                  className="flex items-center justify-center gap-2 px-4 py-2.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
                >
                  <Trash2 className="w-4 h-4" />
                  Delete
                </button>
              </div>
            )}
          </div>
        )}
      </div>
    </>
  );
}

// --- Templates Tab ---

const DEFAULT_TEMPLATE_BODY = `<p>Hi {contact_name},</p>

<p>I came across <strong>{company_name}</strong> and was impressed by your capabilities.</p>

<p>We're building <strong>ForgeOS</strong> — a marketplace that connects manufacturers with engineers who need production partners. Companies on our platform get:</p>

<ul>
  <li><strong>Marketplace visibility</strong> — engineers search by capability, location, and certification</li>
  <li><strong>Fractional executive income</strong> — monetise spare capacity and expertise</li>
  <li><strong>Facility bookings</strong> — fill downtime with on-demand production runs</li>
</ul>

<p>We've already created a listing for {company_name}. You can <strong>claim and customise it</strong> in under 2 minutes:</p>

<p><a href="{claim_url}" style="display:inline-block;padding:10px 24px;background:#2563eb;color:#ffffff;text-decoration:none;border-radius:6px;font-weight:600;">Claim Your Listing</a></p>

<p>No cost, no commitment — just verify your details and you're live.</p>

<p>Best regards,<br/>The ForgeOS Team</p>

<p style="font-size:11px;color:#9ca3af;">If you'd prefer not to hear from us, simply ignore this email.</p>`;

// --- Performance Tab ---

function PerformanceTab({ showError }: { showError: (msg: string) => void }) {
  const [dailyStats, setDailyStats] = useState<DailyOutreachStat[]>([]);
  const [experiments, setExperiments] = useState<ABExperiment[]>([]);
  const [insights, setInsights] = useState<OutreachInsight[]>([]);
  const [loading, setLoading] = useState(true);

  const loadData = useCallback(async () => {
    try {
      const [stats, exps, ins] = await Promise.all([
        getDailyOutreachStats(),
        getExperimentHistory(),
        getOutreachInsights(),
      ]);
      setDailyStats(stats);
      setExperiments(exps);
      setInsights(ins);
    } catch (e) {
      showError(String(e));
    } finally {
      setLoading(false);
    }
  }, [showError]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-12">
        <Loader2 className="w-6 h-6 animate-spin text-gray-400" />
      </div>
    );
  }

  // Compute headline
  const firstDay = dailyStats.length > 0 ? dailyStats[0] : null;
  const lastDay = dailyStats.length > 1 ? dailyStats[dailyStats.length - 1] : null;
  const totalSent = dailyStats.reduce((sum, d) => sum + d.sent, 0);
  const totalOpened = dailyStats.reduce((sum, d) => sum + d.opened, 0);
  const overallRate = totalSent > 0 ? ((totalOpened / totalSent) * 100).toFixed(1) : "0";

  return (
    <div className="space-y-6">
      {/* Headline */}
      <div className="bg-white border border-gray-200 rounded-lg p-4">
        <div className="flex items-center justify-between">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">
              Outreach Performance
            </h2>
            <p className="text-sm text-gray-500 mt-0.5">
              {totalSent} emails sent across {dailyStats.length} days — {overallRate}% overall open rate
            </p>
          </div>
          {firstDay && lastDay && (
            <div className="flex items-center gap-2">
              <span className="text-sm text-gray-500">
                {firstDay.open_rate}%
              </span>
              {lastDay.open_rate > firstDay.open_rate ? (
                <TrendingUp className="w-4 h-4 text-green-600" />
              ) : lastDay.open_rate < firstDay.open_rate ? (
                <TrendingDown className="w-4 h-4 text-red-500" />
              ) : null}
              <span className="text-sm font-medium text-gray-900">
                {lastDay.open_rate}%
              </span>
            </div>
          )}
        </div>
      </div>

      {/* Daily Stats Table */}
      <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100">
          <h3 className="text-sm font-semibold text-gray-900">Daily Breakdown</h3>
        </div>
        {dailyStats.length === 0 ? (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No emails sent yet. Start a campaign to see daily stats.
          </div>
        ) : (
          <table className="w-full text-sm">
            <thead>
              <tr className="bg-gray-50 text-gray-500 text-xs font-medium">
                <th className="px-4 py-2 text-left">Date</th>
                <th className="px-4 py-2 text-right">Sent</th>
                <th className="px-4 py-2 text-right">Opened</th>
                <th className="px-4 py-2 text-right">Bounced</th>
                <th className="px-4 py-2 text-right">Claimed</th>
                <th className="px-4 py-2 text-right">Open Rate</th>
                <th className="px-4 py-2 text-right">Gen</th>
              </tr>
            </thead>
            <tbody>
              {[...dailyStats].reverse().map((day, i) => {
                const prev = [...dailyStats].reverse()[i + 1];
                const improving = prev ? day.open_rate > prev.open_rate : false;
                const declining = prev ? day.open_rate < prev.open_rate : false;
                return (
                  <tr key={day.date} className="border-t border-gray-50 hover:bg-gray-50">
                    <td className="px-4 py-2 text-gray-900 font-medium">{day.date}</td>
                    <td className="px-4 py-2 text-right text-gray-700">{day.sent}</td>
                    <td className="px-4 py-2 text-right text-gray-700">{day.opened}</td>
                    <td className="px-4 py-2 text-right text-gray-700">{day.bounced}</td>
                    <td className="px-4 py-2 text-right text-gray-700">{day.claimed}</td>
                    <td className={`px-4 py-2 text-right font-medium ${
                      improving ? "text-green-600" : declining ? "text-red-500" : "text-gray-700"
                    }`}>
                      {day.open_rate}%
                      {improving && <TrendingUp className="w-3 h-3 inline ml-1" />}
                      {declining && <TrendingDown className="w-3 h-3 inline ml-1" />}
                    </td>
                    <td className="px-4 py-2 text-right text-gray-400">{day.generation ?? "—"}</td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        )}
      </div>

      {/* A/B Experiment History */}
      <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100">
          <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2">
            <Zap className="w-4 h-4 text-amber-500" />
            A/B Experiment History
          </h3>
        </div>
        {experiments.length === 0 ? (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No experiments yet. The learning loop will create the first one automatically.
          </div>
        ) : (
          <div className="divide-y divide-gray-100">
            {experiments.map((exp) => {
              const aRate = exp.variant_a_sent > 0
                ? ((exp.variant_a_opened / exp.variant_a_sent) * 100).toFixed(1)
                : "0";
              const bRate = exp.variant_b_sent > 0
                ? ((exp.variant_b_opened / exp.variant_b_sent) * 100).toFixed(1)
                : "0";
              return (
                <div key={exp.id} className="p-4">
                  <div className="flex items-center justify-between mb-2">
                    <span className="text-sm font-medium text-gray-900">
                      Generation {exp.generation}
                    </span>
                    <span className={`text-xs px-2 py-0.5 rounded-full ${
                      exp.status === "active"
                        ? "bg-blue-100 text-blue-700"
                        : "bg-gray-100 text-gray-600"
                    }`}>
                      {exp.status === "active" ? "Running" : `Winner: ${exp.winner}`}
                    </span>
                  </div>
                  <div className="grid grid-cols-2 gap-3 text-xs">
                    <div className={`p-2 rounded border ${
                      exp.winner === "A" ? "border-green-300 bg-green-50" : "border-gray-200"
                    }`}>
                      <div className="font-medium text-gray-700 mb-1">Variant A</div>
                      <div className="text-gray-500 line-clamp-2">{exp.variant_a_strategy}</div>
                      <div className="mt-1 font-mono text-gray-900">
                        {exp.variant_a_sent} sent / {aRate}% open
                      </div>
                    </div>
                    <div className={`p-2 rounded border ${
                      exp.winner === "B" ? "border-green-300 bg-green-50" : "border-gray-200"
                    }`}>
                      <div className="font-medium text-gray-700 mb-1">Variant B</div>
                      <div className="text-gray-500 line-clamp-2">{exp.variant_b_strategy}</div>
                      <div className="mt-1 font-mono text-gray-900">
                        {exp.variant_b_sent} sent / {bRate}% open
                      </div>
                    </div>
                  </div>
                </div>
              );
            })}
          </div>
        )}
      </div>

      {/* Active Insights */}
      <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100">
          <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2">
            <Brain className="w-4 h-4 text-purple-500" />
            Learned Insights ({insights.length})
          </h3>
        </div>
        {insights.length === 0 ? (
          <div className="px-4 py-8 text-center text-sm text-gray-400">
            No insights yet. The system learns after 10+ emails are sent and tracked.
          </div>
        ) : (
          <div className="divide-y divide-gray-50">
            {insights.map((insight) => (
              <div key={insight.id} className="px-4 py-3 flex items-start gap-3">
                <span className={`text-xs px-2 py-0.5 rounded-full whitespace-nowrap mt-0.5 ${
                  insight.insight_type === "pattern"
                    ? "bg-green-100 text-green-700"
                    : insight.insight_type === "anti_pattern"
                    ? "bg-red-100 text-red-700"
                    : "bg-blue-100 text-blue-700"
                }`}>
                  {insight.insight_type === "anti_pattern" ? "anti-pattern" : insight.insight_type}
                </span>
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-gray-800">{insight.insight}</p>
                  <div className="flex items-center gap-3 mt-1 text-xs text-gray-400">
                    <span>Gen {insight.generation}</span>
                    <span>From {insight.source_email_count} emails</span>
                    <div className="flex items-center gap-1">
                      <div className="w-16 bg-gray-200 rounded-full h-1.5">
                        <div
                          className="bg-purple-500 h-1.5 rounded-full"
                          style={{ width: `${insight.confidence * 100}%` }}
                        />
                      </div>
                      <span>{(insight.confidence * 100).toFixed(0)}%</span>
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

function TemplatesTab({ showError }: { showError: (msg: string) => void }) {
  const [templates, setTemplates] = useState<EmailTemplate[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [eligibleCount, setEligibleCount] = useState<number | null>(null);
  const [sending, setSending] = useState(false);
  const [stopping, setStopping] = useState(false);
  const [showConfirm, setShowConfirm] = useState(false);

  // Editor state
  const [editName, setEditName] = useState("");
  const [editSubject, setEditSubject] = useState("");
  const [editBody, setEditBody] = useState("");
  const [isNew, setIsNew] = useState(false);
  const [showPreview, setShowPreview] = useState(false);

  const loadTemplates = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getEmailTemplates();
      setTemplates(data);
    } catch (e) {
      showError(`Failed to load templates: ${e}`);
    }
    setLoading(false);
  }, [showError]);

  const loadEligibleCount = useCallback(async () => {
    try {
      const count = await getCampaignEligibleCount();
      setEligibleCount(count);
    } catch {
      setEligibleCount(null);
    }
  }, []);

  useEffect(() => {
    loadTemplates();
    loadEligibleCount();
  }, [loadTemplates, loadEligibleCount]);

  function handleSelectTemplate(t: EmailTemplate) {
    setSelectedId(t.id);
    setEditName(t.name);
    setEditSubject(t.subject);
    setEditBody(t.body);
    setIsNew(false);
    setShowPreview(false);
  }

  function handleNewTemplate() {
    setSelectedId(null);
    setEditName("Default Outreach");
    setEditSubject("Claim your {company_name} listing on ForgeOS");
    setEditBody(DEFAULT_TEMPLATE_BODY);
    setIsNew(true);
    setShowPreview(false);
  }

  async function handleSave() {
    if (saving) return;
    setSaving(true);
    try {
      const result = await saveEmailTemplate({
        id: isNew ? undefined : (selectedId ?? undefined),
        name: editName,
        subject: editSubject,
        body: editBody,
      });
      if (result.created) {
        setSelectedId(result.id);
        setIsNew(false);
      }
      await loadTemplates();
    } catch (e) {
      showError(`Failed to save template: ${e}`);
    }
    setSaving(false);
  }

  async function handleDelete(id: string) {
    try {
      await deleteEmailTemplate(id);
      if (selectedId === id) {
        setSelectedId(null);
        setEditName("");
        setEditSubject("");
        setEditBody("");
      }
      await loadTemplates();
    } catch (e) {
      showError(`Failed to delete template: ${e}`);
    }
  }

  async function handleSendCampaign() {
    if (!selectedId || sending) return;
    setSending(true);
    setShowConfirm(false);
    try {
      await startPipeline([`template_outreach:${selectedId}`]);
    } catch (e) {
      showError(`Failed to start campaign: ${e}`);
    }
    setSending(false);
  }

  async function handleStop() {
    setStopping(true);
    try {
      await stopPipeline();
    } catch (e) {
      showError(`Failed to stop: ${e}`);
    }
    setStopping(false);
    setSending(false);
  }

  // Preview with sample data
  const previewSubject = editSubject
    .replace(/{company_name}/g, "Acme Manufacturing GmbH")
    .replace(/{contact_name}/g, "Hans Mueller")
    .replace(/{claim_url}/g, "https://fractionalforge.app/claim/abc123def456...");
  const previewBody = editBody
    .replace(/{company_name}/g, "Acme Manufacturing GmbH")
    .replace(/{contact_name}/g, "Hans Mueller")
    .replace(/{claim_url}/g, "https://fractionalforge.app/claim/abc123def456...");

  const hasEditor = selectedId !== null || isNew;

  return (
    <div className="flex gap-4">
      {/* Template list */}
      <div className="w-64 bg-white rounded-xl border border-gray-200 shadow-sm shrink-0">
        <div className="p-4 border-b border-gray-200 flex items-center justify-between">
          <h2 className="text-sm font-semibold text-gray-900">Templates</h2>
          <button
            onClick={handleNewTemplate}
            className="p-1 rounded hover:bg-gray-100 text-gray-500 transition-colors"
            title="New template"
          >
            <Plus className="w-4 h-4" />
          </button>
        </div>

        <div className="divide-y divide-gray-100 max-h-[calc(100vh-280px)] overflow-y-auto">
          {loading ? (
            <div className="flex items-center justify-center p-8">
              <Loader2 className="w-5 h-5 text-gray-400 animate-spin" />
            </div>
          ) : templates.length === 0 && !isNew ? (
            <div className="p-6 text-center text-gray-400 text-sm">
              No templates yet.
              <button
                onClick={handleNewTemplate}
                className="block mx-auto mt-2 text-blue-600 hover:text-blue-700 text-xs"
              >
                Create one
              </button>
            </div>
          ) : (
            templates.map((t) => (
              <div
                key={t.id}
                className={`flex items-center gap-2 px-4 py-3 cursor-pointer transition-colors ${
                  selectedId === t.id && !isNew
                    ? "bg-blue-50"
                    : "hover:bg-gray-50"
                }`}
                onClick={() => handleSelectTemplate(t)}
              >
                <FileText className="w-4 h-4 text-gray-400 shrink-0" />
                <div className="flex-1 min-w-0">
                  <p className="text-sm font-medium text-gray-900 truncate">
                    {t.name}
                  </p>
                  <p className="text-xs text-gray-500 truncate">{t.subject}</p>
                </div>
                <button
                  onClick={(e) => {
                    e.stopPropagation();
                    handleDelete(t.id);
                  }}
                  className="p-1 rounded hover:bg-red-50 text-gray-300 hover:text-red-500 transition-colors"
                >
                  <Trash2 className="w-3 h-3" />
                </button>
              </div>
            ))
          )}
        </div>

        {/* Eligible count */}
        {eligibleCount !== null && (
          <div className="p-4 border-t border-gray-200">
            <p className="text-xs text-gray-500">
              <span className="font-semibold text-gray-700">{eligibleCount}</span>{" "}
              companies eligible
            </p>
          </div>
        )}
      </div>

      {/* Editor / Preview */}
      {hasEditor ? (
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          {/* Editor header */}
          <div className="p-4 border-b border-gray-200 flex items-center justify-between">
            <div className="flex items-center gap-2">
              <h2 className="text-sm font-semibold text-gray-900">
                {isNew ? "New Template" : "Edit Template"}
              </h2>
              <div className="flex items-center gap-1 bg-gray-100 rounded-md p-0.5 ml-2">
                <button
                  onClick={() => setShowPreview(false)}
                  className={`px-2 py-0.5 rounded text-xs transition-colors ${
                    !showPreview
                      ? "bg-white text-gray-900 shadow-sm"
                      : "text-gray-500"
                  }`}
                >
                  Edit
                </button>
                <button
                  onClick={() => setShowPreview(true)}
                  className={`px-2 py-0.5 rounded text-xs transition-colors ${
                    showPreview
                      ? "bg-white text-gray-900 shadow-sm"
                      : "text-gray-500"
                  }`}
                >
                  Preview
                </button>
              </div>
            </div>
            <div className="flex items-center gap-2">
              <button
                onClick={handleSave}
                disabled={saving}
                className="flex items-center gap-1.5 px-3 py-1.5 bg-gray-900 hover:bg-gray-800 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
              >
                {saving ? (
                  <Loader2 className="w-3 h-3 animate-spin" />
                ) : (
                  <Save className="w-3 h-3" />
                )}
                Save
              </button>
              {!isNew && selectedId && (
                sending ? (
                  <button
                    onClick={handleStop}
                    disabled={stopping}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
                  >
                    {stopping ? (
                      <Loader2 className="w-3 h-3 animate-spin" />
                    ) : (
                      <Square className="w-3 h-3" />
                    )}
                    Stop Generating
                  </button>
                ) : (
                  <button
                    onClick={() => setShowConfirm(true)}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-blue-600 hover:bg-blue-700 rounded-lg text-xs font-medium text-white transition-colors"
                  >
                    <Play className="w-3 h-3" />
                    Generate Drafts
                  </button>
                )
              )}
            </div>
          </div>

          {/* Confirm dialog */}
          {showConfirm && (
            <div className="mx-4 mt-4 p-4 bg-amber-50 border border-amber-200 rounded-lg">
              <p className="text-sm text-amber-800 font-medium">
                Generate personalised drafts for {eligibleCount ?? "?"} companies?
              </p>
              <p className="text-xs text-amber-600 mt-1">
                Ollama will personalise each email using company data. You'll
                review each draft before sending.
              </p>
              <div className="flex gap-2 mt-3">
                <button
                  onClick={handleSendCampaign}
                  className="px-3 py-1.5 bg-blue-600 hover:bg-blue-700 rounded-lg text-xs font-medium text-white transition-colors"
                >
                  Generate Drafts
                </button>
                <button
                  onClick={() => setShowConfirm(false)}
                  className="px-3 py-1.5 bg-white border border-gray-200 hover:bg-gray-50 rounded-lg text-xs font-medium text-gray-700 transition-colors"
                >
                  Cancel
                </button>
              </div>
            </div>
          )}

          {showPreview ? (
            /* Preview mode — realistic email client mockup */
            <div className="p-6 space-y-3">
              <p className="text-xs text-gray-400">
                Preview with sample data — this is how the email will appear in the recipient's inbox.
              </p>

              {/* Email client mockup */}
              <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
                {/* Email header */}
                <div className="px-5 py-4 border-b border-gray-100">
                  <h3 className="text-base font-semibold text-gray-900 leading-snug">
                    {previewSubject}
                  </h3>
                  <div className="mt-3 flex items-start gap-3">
                    <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center shrink-0 mt-0.5">
                      <span className="text-white text-xs font-bold">TF</span>
                    </div>
                    <div className="flex-1 min-w-0">
                      <div className="flex items-baseline gap-2">
                        <span className="text-sm font-medium text-gray-900">Tristan Fischer</span>
                        <span className="text-xs text-gray-400">&lt;tristan@fractionalforge.app&gt;</span>
                      </div>
                      <p className="text-xs text-gray-500 mt-0.5">
                        to hans.mueller@acme-company.de
                      </p>
                    </div>
                  </div>
                </div>

                {/* Email body */}
                <div className="px-5 py-5 max-h-[calc(100vh-480px)] overflow-y-auto">
                  <div
                    className="text-sm text-gray-800 leading-relaxed [&_p]:mb-3 [&_ul]:mb-3 [&_ul]:pl-5 [&_ul]:list-disc [&_li]:mb-1 [&_a]:text-blue-600 [&_a]:underline [&_strong]:font-semibold"
                    dangerouslySetInnerHTML={{
                      __html: DOMPurify.sanitize(previewBody),
                    }}
                  />
                </div>
              </div>
            </div>
          ) : (
            /* Edit mode */
            <div className="p-6 space-y-4">
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Template Name
                </label>
                <input
                  type="text"
                  value={editName}
                  onChange={(e) => setEditName(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="e.g. Default Outreach"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Subject Line
                </label>
                <input
                  type="text"
                  value={editSubject}
                  onChange={(e) => setEditSubject(e.target.value)}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-blue-500"
                  placeholder="e.g. Claim your {company_name} listing on ForgeOS"
                />
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-700 mb-1">
                  Body (HTML)
                </label>
                <textarea
                  value={editBody}
                  onChange={(e) => setEditBody(e.target.value)}
                  rows={16}
                  className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm font-mono focus:outline-none focus:ring-2 focus:ring-blue-500 resize-y"
                  placeholder="HTML email body..."
                />
              </div>
              <div className="bg-gray-50 rounded-lg p-3">
                <p className="text-xs font-medium text-gray-600 mb-1">
                  Available placeholders:
                </p>
                <div className="flex flex-wrap gap-2">
                  {["{company_name}", "{contact_name}", "{claim_url}"].map(
                    (p) => (
                      <code
                        key={p}
                        className="px-2 py-0.5 bg-white border border-gray-200 rounded text-xs text-gray-700"
                      >
                        {p}
                      </code>
                    )
                  )}
                </div>
              </div>
            </div>
          )}
        </div>
      ) : (
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm flex items-center justify-center">
          <div className="text-center text-gray-400 p-8">
            <FileText className="w-8 h-8 mx-auto mb-2 opacity-50" />
            <p className="text-sm">Select a template or create a new one</p>
          </div>
        </div>
      )}
    </div>
  );
}
