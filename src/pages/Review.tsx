import { useEffect, useState, useRef } from "react";
import { useSearchParams } from "react-router-dom";
import {
  CheckCircle,
  XCircle,
  X,
  ChevronRight,
  Star,
  Building2,
  RefreshCw,
  Globe,
  Mail,
  User,
  Tag,
  Award,
  Factory,
  ExternalLink,
  AlertTriangle,
  Play,
  RotateCcw,
  Languages,
  Loader2,
  MapPin,
  ShieldCheck,
  Clock,
  Package,
  HeartPulse,
  Upload,
  ArrowUpCircle,
  Search,
  Undo2,
  StopCircle,
  Trash2,
  Wrench,
  ChevronDown,
} from "lucide-react";
import { listen } from "@tauri-apps/api/event";
import {
  getCompanies,
  getCompaniesFiltered,
  getStats,
  updateCompanyStatus,
  startPipeline,
  stopPipeline,
  resetErrorCompanies,
  getPipelineStatus,
  approveAllEnriched,
  importForAudit,
  pushSingleCompany,
  removeFromMarketplace,
  removeAllFromMarketplace,
  getCompaniesCount,
  batchUpdateStatus,
} from "../lib/tauri";
import { useError } from "../contexts/ErrorContext";
import ConfirmDialog from "../components/ConfirmDialog";

const COUNTRIES: Record<string, string> = {
  DE: "Germany",
  FR: "France",
  NL: "Netherlands",
  BE: "Belgium",
  IT: "Italy",
  GB: "United Kingdom",
};

type StatusFilter = "all" | "discovered" | "enriched" | "approved" | "pushed" | "error";

const STATUS_BADGE: Record<string, string> = {
  discovered: "bg-blue-100 text-blue-700",
  enriching: "bg-forge-100 text-forge-700 animate-pulse",
  enriched: "bg-green-100 text-green-700",
  approved: "bg-yellow-100 text-yellow-700",
  rejected: "bg-gray-100 text-gray-500",
  error: "bg-red-100 text-red-700",
  pushed: "bg-purple-100 text-purple-700",
};

function parseJsonField(value: unknown): string[] {
  if (!value) return [];
  try {
    const parsed = JSON.parse(String(value));
    return Array.isArray(parsed) ? parsed.map(String) : [];
  } catch {
    return [];
  }
}

function parseAttributesJson(value: unknown): Record<string, unknown> {
  if (!value) return {};
  try {
    const parsed = JSON.parse(String(value));
    return typeof parsed === "object" && parsed !== null
      ? (parsed as Record<string, unknown>)
      : {};
  } catch {
    return {};
  }
}

function TagPills({ items, color }: { items: string[]; color: string }) {
  if (items.length === 0) return null;
  return (
    <div className="flex flex-wrap gap-1.5">
      {items.map((item, i) => (
        <span
          key={i}
          className={`px-2 py-0.5 rounded-full text-xs ${color}`}
        >
          {item}
        </span>
      ))}
    </div>
  );
}

function DetailField({
  label,
  value,
}: {
  label: string;
  value: string | number | null | undefined;
}) {
  if (!value && value !== 0) return null;
  return (
    <div>
      <h4 className="text-xs text-gray-400 uppercase mb-0.5">{label}</h4>
      <p className="text-sm text-gray-700">{String(value)}</p>
    </div>
  );
}

export default function Review() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [companies, setCompanies] = useState<Record<string, unknown>[]>([]);
  const [selected, setSelected] = useState<Record<string, unknown> | null>(
    null
  );
  const [filter, setFilter] = useState<StatusFilter>("all");
  const [counts, setCounts] = useState<Record<StatusFilter, number>>({
    all: 0,
    discovered: 0,
    enriched: 0,
    approved: 0,
    pushed: 0,
    error: 0,
  });
  const [enriching, setEnriching] = useState(false);
  const [enrichProgress, setEnrichProgress] = useState<{
    currentCompany?: string;
    enriched: number;
    errors: number;
    total?: number;
    model: string;
    phase?: string;
  } | null>(null);
  const [pushProgress, setPushProgress] = useState<{
    currentCompany: string;
    currentIndex: number;
    pushed: number;
    skipped: number;
    errors: number;
    total: number;
  } | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [pushing, setPushing] = useState<string | null>(null);
  const [removing, setRemoving] = useState<string | null>(null);
  const [removingAll, setRemovingAll] = useState(false);
  const [searchQuery, setSearchQuery] = useState("");
  const [auditing, setAuditing] = useState(false);
  const [auditResult, setAuditResult] = useState<{
    fetched: number;
    imported: number;
    skipped: number;
  } | null>(null);
  const [confirmDialog, setConfirmDialog] = useState<{
    title: string;
    message: string;
    confirmLabel: string;
    onConfirm: () => void;
  } | null>(null);

  // Enhancement 8: Pagination
  const [page, setPage] = useState(0);
  const [totalCount, setTotalCount] = useState(0);

  // Enhancement 9: Keyboard shortcuts
  const [selectedIndex, setSelectedIndex] = useState(-1);

  // Enhancement 10: Bulk selection
  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set());

  // Enhancement 11: Detail tabs
  const [detailTab, setDetailTab] = useState<"overview" | "capabilities" | "contact" | "raw">("overview");

  // Enhancement 12: Side-by-side comparison
  const [compareMode, setCompareMode] = useState(false);
  const [compareList, setCompareList] = useState<string[]>([]);

  const { showError, showInfo } = useError();
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  // Drill-down filters from URL params
  const drillSubcategory = searchParams.get("subcategory");
  const drillCountry = searchParams.get("country");
  const drillSearch = searchParams.get("search");
  const hasDrillDown = !!(drillSubcategory || drillCountry || drillSearch);

  function clearDrillDown() {
    setSearchParams({});
  }

  useEffect(() => {
    loadCompanies(filter);
    loadCounts();
  }, [filter, drillSubcategory, drillCountry, drillSearch, page]);

  // Reset page and selection when filters change
  useEffect(() => {
    setPage(0);
    setSelectedIds(new Set());
  }, [filter, drillSubcategory, drillCountry, drillSearch, searchQuery]);

  // Debounced search
  useEffect(() => {
    const timer = setTimeout(() => {
      loadCompanies(filter);
    }, 300);
    return () => clearTimeout(timer);
  }, [searchQuery]);

  // Check if pipeline is already running on mount
  useEffect(() => {
    getPipelineStatus().then((s) => setEnriching(s.running)).catch(() => {});
  }, []);

  // Listen for pipeline status events to update enriching state + auto-refresh
  useEffect(() => {
    const unlisten = listen<{ status: string }>("pipeline:status", (event) => {
      const running = event.payload.status === "running";
      setEnriching(running);
      if (!running) {
        setEnrichProgress(null);
        setPushProgress(null);
        setCancelling(false);
        loadCompanies(filter).catch(() => {});
        loadCounts().catch(() => {});
      }
    });
    return () => { unlisten.then((fn) => fn()); };
  }, [filter]);

  // Listen for per-company progress events instead of polling
  useEffect(() => {
    const unlisten = listen<{
      stage: string;
      phase: string;
      current_company?: string;
      current_index?: number;
      enriched?: number;
      pushed?: number;
      skipped?: number;
      errors: number;
      total?: number;
      model?: string;
    }>("pipeline:progress", (event) => {
      const p = event.payload;
      if (p.stage === "enrich") {
        setEnrichProgress({
          currentCompany: p.current_company,
          enriched: p.enriched || 0,
          errors: p.errors,
          total: p.total,
          model: p.model || "",
          phase: p.phase,
        });
      } else if (p.stage === "push") {
        setPushProgress({
          currentCompany: p.current_company || "",
          currentIndex: p.current_index || 0,
          pushed: p.pushed || 0,
          skipped: p.skipped || 0,
          errors: p.errors,
          total: p.total || 0,
        });
      } else {
        return;
      }
      // Debounce DB refresh — at most once every 3s to avoid flooding IPC during parallel pipeline
      if (!refreshTimer.current) {
        refreshTimer.current = setTimeout(() => {
          refreshTimer.current = null;
          loadCompanies(filter).catch(() => {});
          loadCounts().catch(() => {});
        }, 3000);
      }
    });
    return () => { unlisten.then((fn) => fn()); };
  }, [filter]);

  // Enhancement 9: Keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Don't intercept when typing in input fields
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;

      if (e.key === "ArrowDown" || e.key === "j") {
        e.preventDefault();
        const newIndex = Math.min(selectedIndex + 1, companies.length - 1);
        setSelectedIndex(newIndex);
        if (companies[newIndex]) setSelected(companies[newIndex]);
      } else if (e.key === "ArrowUp" || e.key === "k") {
        e.preventDefault();
        const newIndex = Math.max(selectedIndex - 1, 0);
        setSelectedIndex(newIndex);
        if (companies[newIndex]) setSelected(companies[newIndex]);
      } else if (e.key === "Enter" || e.key === "a") {
        if (selected) {
          e.preventDefault();
          handleApprove(String(selected.id));
        }
      } else if (e.key === "Backspace" || e.key === "x") {
        if (selected) {
          e.preventDefault();
          const rejectId = String(selected.id);
          setConfirmDialog({
            title: "Reject Company",
            message: `Reject "${String(selected.name || "")}"? This will set its status to rejected.`,
            confirmLabel: "Reject",
            onConfirm: () => {
              setConfirmDialog(null);
              handleReject(rejectId);
            },
          });
        }
      } else if (e.key === "Escape") {
        setSelected(null);
        setSelectedIndex(-1);
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [selectedIndex, companies, selected]);

  async function loadCounts() {
    try {
      const stats = await getStats();
      const rows = (stats.companies as { status: string; count: number }[]) || [];
      const c: Record<StatusFilter, number> = { all: 0, discovered: 0, enriched: 0, approved: 0, pushed: 0, error: 0 };
      for (const row of rows) {
        c.all += Number(row.count) || 0;
        if (row.status === "discovered") c.discovered += Number(row.count) || 0;
        else if (row.status === "enriched") c.enriched += Number(row.count) || 0;
        else if (row.status === "approved") c.approved += Number(row.count) || 0;
        else if (row.status === "pushed") c.pushed += Number(row.count) || 0;
        else if (row.status === "error") c.error += Number(row.count) || 0;
      }
      setCounts(c);
      // Update totalCount based on current filter
      if (filter === "all") {
        setTotalCount(c.all);
      } else {
        setTotalCount(c[filter] || 0);
      }
    } catch (e) {
      showError(`Failed to load counts: ${e}`);
    }
  }

  async function loadCompanies(status: StatusFilter) {
    try {
      const limit = 50;
      const offset = page * 50;
      if (hasDrillDown || searchQuery.trim()) {
        const data = await getCompaniesFiltered({
          status: status === "all" ? undefined : status,
          subcategory: drillSubcategory || undefined,
          country: drillCountry || undefined,
          search: searchQuery.trim() || drillSearch || undefined,
          limit,
          offset,
        });
        setCompanies(data);
      } else {
        const s = status === "all" ? undefined : status;
        const data = await getCompanies(s, limit, offset);
        setCompanies(data);
      }
      // Fetch total count for pagination
      try {
        const count = await getCompaniesCount(status === "all" ? undefined : status);
        setTotalCount(count);
      } catch {
        // getCompaniesCount may not be available yet
      }
    } catch (e) {
      showError(`Failed to load companies: ${e}`);
    }
  }

  async function handleApprove(id: string) {
    await updateCompanyStatus(id, "approved");
    loadCompanies(filter);
    loadCounts();
    if (selected && String(selected.id) === id) {
      setSelected({ ...selected, status: "approved" });
    }
  }

  async function handleUnapprove(id: string) {
    await updateCompanyStatus(id, "enriched");
    loadCompanies(filter);
    loadCounts();
    if (selected && String(selected.id) === id) {
      setSelected({ ...selected, status: "enriched" });
    }
  }

  async function handlePushSingle(id: string) {
    try {
      setPushing(id);
      await pushSingleCompany(id);
      loadCompanies(filter);
      loadCounts();
      if (selected && String(selected.id) === id) {
        setSelected({ ...selected, status: "pushed" });
      }
    } catch (e) {
      showError(String(e));
    } finally {
      setPushing(null);
    }
  }

  function handleRemoveFromMarketplace(id: string) {
    setConfirmDialog({
      title: "Remove from Marketplace",
      message: "Remove this company from the ForgeOS marketplace? This cannot be undone.",
      confirmLabel: "Remove",
      onConfirm: async () => {
        setConfirmDialog(null);
        try {
          setRemoving(id);
          await removeFromMarketplace(id);
          if (selected && String(selected.id) === id) setSelected(null);
          loadCompanies(filter);
          loadCounts();
        } catch (e) {
          showError(String(e));
        } finally {
          setRemoving(null);
        }
      },
    });
  }

  function handleRemoveAllFromMarketplace() {
    const ids = companies
      .filter((c) => Boolean(c.supabase_listing_id))
      .map((c) => String(c.id));
    if (ids.length === 0) {
      showInfo("No companies with marketplace listings to remove.");
      return;
    }
    setConfirmDialog({
      title: "Remove All from Marketplace",
      message: `Remove ${ids.length} companies from the ForgeOS marketplace? This cannot be undone.`,
      confirmLabel: "Remove All",
      onConfirm: async () => {
        setConfirmDialog(null);
        try {
          setRemovingAll(true);
          const result = await removeAllFromMarketplace(ids);
          showInfo(`Removed ${result.removed} listings from marketplace${result.errors > 0 ? ` (${result.errors} errors)` : ""}`);
          setSelected(null);
          loadCompanies(filter);
          loadCounts();
        } catch (e) {
          showError(String(e));
        } finally {
          setRemovingAll(false);
        }
      },
    });
  }

  async function handleReject(id: string) {
    await updateCompanyStatus(id, "rejected");
    loadCompanies(filter);
    loadCounts();
    if (selected && String(selected.id) === id) setSelected(null);
  }

  async function handleRunEnrich() {
    try {
      setEnriching(true);
      await startPipeline(["enrich"]);
    } catch (e) {
      showError(`Failed to start enrich: ${e}`);
      setEnriching(false);
    }
  }

  async function handleStop() {
    try {
      setCancelling(true);
      await stopPipeline();
    } catch (e) {
      showError(`Failed to stop pipeline: ${e}`);
      setCancelling(false);
    }
  }

  async function handlePushToForgeOS() {
    setEnriching(true);
    try {
      // Wait for any previous pipeline to fully stop (up to 30s)
      for (let i = 0; i < 30; i++) {
        const status = await getPipelineStatus();
        if (!status.running) break;
        if (i === 29) throw new Error("Previous pipeline did not stop in time");
        await new Promise((r) => setTimeout(r, 1000));
      }
      await startPipeline(["push"]);
    } catch (e) {
      showError(`Failed to start push: ${e}`);
      setEnriching(false);
    }
  }

  async function handleResetErrors() {
    try {
      await resetErrorCompanies();
      loadCompanies(filter);
      loadCounts();
      setSelected(null);
    } catch (e) {
      showError(`Failed to reset errors: ${e}`);
    }
  }

  async function handleBulkApprove() {
    try {
      await approveAllEnriched();
      loadCompanies(filter);
      loadCounts();
    } catch (e) {
      showError(`Failed to bulk approve: ${e}`);
    }
  }

  // Enhancement 10: Bulk selection handlers
  async function handleBulkApproveSelected() {
    try {
      await batchUpdateStatus(Array.from(selectedIds), "approved");
      setSelectedIds(new Set());
      loadCompanies(filter);
      loadCounts();
    } catch (e) {
      showError(`Bulk approve failed: ${e}`);
    }
  }

  async function handleBulkRejectSelected() {
    try {
      await batchUpdateStatus(Array.from(selectedIds), "rejected");
      setSelectedIds(new Set());
      loadCompanies(filter);
      loadCounts();
    } catch (e) {
      showError(`Bulk reject failed: ${e}`);
    }
  }

  async function handleAuditMarketplace() {
    try {
      setAuditing(true);
      setAuditResult(null);
      const result = await importForAudit(50);
      setAuditResult(result);
      loadCompanies(filter);
      loadCounts();
    } catch (e) {
      showError(`Marketplace audit failed: ${e}`);
    } finally {
      setAuditing(false);
    }
  }

  const tabs: { key: StatusFilter; label: string }[] = [
    { key: "all", label: "All" },
    { key: "discovered", label: "Discovered" },
    { key: "enriched", label: "Enriched" },
    { key: "approved", label: "Approved" },
    { key: "pushed", label: "Pushed" },
    { key: "error", label: "Error" },
  ];

  const status = selected ? String(selected.status || "") : "";
  const specialties = selected ? parseJsonField(selected.specialties) : [];
  const certifications = selected
    ? parseJsonField(selected.certifications)
    : [];
  const industries = selected ? parseJsonField(selected.industries) : [];
  const attrs = selected ? parseAttributesJson(selected.attributes_json) : {};
  const attrMaterials = Array.isArray(attrs.materials)
    ? (attrs.materials as string[])
    : [];
  const attrEquipment = Array.isArray(attrs.key_equipment)
    ? (attrs.key_equipment as string[])
    : [];
  const attrIndustries = Array.isArray(attrs.industries)
    ? (attrs.industries as string[])
    : [];
  const attrSicCodes = Array.isArray(attrs.sic_codes)
    ? (attrs.sic_codes as string[])
    : [];
  const attrKeyPeople = Array.isArray(attrs.key_people)
    ? (attrs.key_people as { name: string; title: string }[])
    : [];
  const attrChDirectors = Array.isArray(attrs.ch_directors)
    ? (attrs.ch_directors as string[])
    : [];
  const attrProducts = Array.isArray(attrs.products)
    ? (attrs.products as string[])
    : [];
  const attrSecurityClearances = Array.isArray(attrs.security_clearances)
    ? (attrs.security_clearances as string[])
    : [];
  const attrFinancialSignals = attrs.financial_signals as
    | Record<string, unknown>
    | undefined;
  const financialHealth = selected
    ? String(selected.financial_health || "")
    : "";
  const companyAddress = selected ? String(selected.address || "") : "";

  // Deep enrichment — process capabilities
  const processCapabilities: {
    process_category?: string;
    process_name?: string;
    materials_worked?: string[];
    tolerance_claimed?: string;
    tolerance_value_mm?: number;
    surface_finish_claimed?: string;
    surface_finish_ra_um?: number;
    max_part_dimensions?: string;
    batch_size_range?: string;
    equipment_mentioned?: string[];
    surface_treatments?: string[];
    confidence?: number;
    source_excerpt?: string;
  }[] = selected?.process_capabilities_json
    ? (() => {
        try {
          const parsed = JSON.parse(String(selected.process_capabilities_json));
          return Array.isArray(parsed) ? parsed : [];
        } catch {
          return [];
        }
      })()
    : [];

  const [expandedExcerpts, setExpandedExcerpts] = useState<Set<number>>(new Set());
  const [processCapOpen, setProcessCapOpen] = useState(true);

  // Reset detail state when selected company changes
  useEffect(() => {
    setExpandedExcerpts(new Set());
    setDetailTab("overview");
  }, [selected ? String(selected.id) : null]);

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Review Queue</h1>
          <p className="text-sm text-gray-500 mt-1">
            Review all companies across every pipeline stage
          </p>
        </div>

        <div className="flex gap-2">
          {/* Audit Marketplace button — always visible */}
          <button
            onClick={handleAuditMarketplace}
            disabled={auditing || enriching}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              auditing
                ? "bg-indigo-400 text-white cursor-not-allowed"
                : "bg-indigo-600 hover:bg-indigo-700 text-white"
            }`}
          >
            {auditing ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Search className="w-4 h-4" />
            )}
            {auditing ? "Importing..." : "Audit Marketplace"}
          </button>

          {enriching && (
            <button
              onClick={handleStop}
              disabled={cancelling}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
                cancelling
                  ? "bg-red-300 text-white cursor-not-allowed"
                  : "bg-red-600 hover:bg-red-700 text-white"
              }`}
            >
              <StopCircle className="w-4 h-4" />
              {cancelling ? "Stopping..." : "Stop Pipeline"}
            </button>
          )}
          {filter === "discovered" && companies.length > 0 && (
            <>
              <button
                onClick={handleRunEnrich}
                disabled={enriching}
                className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors ${
                  enriching
                    ? "bg-forge-400 cursor-not-allowed"
                    : "bg-forge-600 hover:bg-forge-700"
                }`}
              >
                {enriching ? (
                  <RefreshCw className="w-4 h-4 animate-spin" />
                ) : (
                  <Play className="w-4 h-4" />
                )}
                {enriching ? "Enriching..." : "Enrich All"}
              </button>
              {companies.some((c) => Boolean(c.supabase_listing_id)) && (
                <button
                  onClick={handleRemoveAllFromMarketplace}
                  disabled={removingAll}
                  className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors ${
                    removingAll
                      ? "bg-red-400 cursor-not-allowed"
                      : "bg-red-600 hover:bg-red-700"
                  }`}
                >
                  {removingAll ? (
                    <Loader2 className="w-4 h-4 animate-spin" />
                  ) : (
                    <Trash2 className="w-4 h-4" />
                  )}
                  {removingAll ? "Removing..." : "Remove All from Marketplace"}
                </button>
              )}
            </>
          )}
          {filter === "error" && companies.length > 0 && (
            <button
              onClick={handleResetErrors}
              className="flex items-center gap-2 px-4 py-2 bg-amber-600 hover:bg-amber-700 rounded-lg text-sm font-medium text-white transition-colors"
            >
              <RotateCcw className="w-4 h-4" />
              Reset All Errors
            </button>
          )}
          {filter === "enriched" && companies.length > 0 && (
            <button
              onClick={handleBulkApprove}
              className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg text-sm font-medium text-white transition-colors"
            >
              <CheckCircle className="w-4 h-4" />
              Approve All
            </button>
          )}
          {filter === "approved" && companies.length > 0 && (
            <button
              onClick={handlePushToForgeOS}
              disabled={enriching}
              className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors ${
                enriching
                  ? "bg-purple-400 cursor-not-allowed"
                  : "bg-purple-600 hover:bg-purple-700"
              }`}
            >
              {enriching ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <ArrowUpCircle className="w-4 h-4" />
              )}
              {enriching ? "Pipeline Running..." : "Push to ForgeOS"}
            </button>
          )}
        </div>
      </div>

      {/* Audit result banner */}
      {auditResult && (
        <div className="flex items-center gap-3 p-3 bg-indigo-50 border border-indigo-200 rounded-lg">
          <Upload className="w-4 h-4 text-indigo-600 shrink-0" />
          <span className="text-sm text-indigo-700">
            Audit complete: fetched {auditResult.fetched} low-quality listings,
            imported {auditResult.imported}, skipped {auditResult.skipped} (already in Nightshift)
          </span>
          <button
            onClick={() => setAuditResult(null)}
            className="ml-auto text-indigo-400 hover:text-indigo-600"
          >
            <X className="w-4 h-4" />
          </button>
        </div>
      )}

      {/* Status filter tabs */}
      <div className="flex gap-1 bg-white rounded-xl border border-gray-200 p-1 shadow-sm">
        {tabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => {
              setFilter(tab.key);
              setSelected(null);
            }}
            className={`flex items-center gap-2 px-4 py-2 rounded-lg text-sm font-medium transition-colors ${
              filter === tab.key
                ? "bg-forge-600 text-white"
                : "text-gray-600 hover:bg-gray-100"
            }`}
          >
            {tab.label}
            <span
              className={`px-1.5 py-0.5 rounded-full text-xs ${
                filter === tab.key
                  ? "bg-white/20 text-white"
                  : "bg-gray-100 text-gray-500"
              }`}
            >
              {counts[tab.key]}
            </span>
          </button>
        ))}
      </div>

      {/* Drill-down filter banner */}
      {hasDrillDown && (
        <div className="flex items-center gap-2 p-3 bg-forge-50 border border-forge-200 rounded-lg">
          <span className="text-sm text-gray-700">Filtered by:</span>
          {drillSubcategory && (
            <span className="px-2 py-0.5 bg-forge-100 text-forge-700 rounded-full text-xs font-medium">
              {drillSubcategory}
            </span>
          )}
          {drillCountry && (
            <span className="px-2 py-0.5 bg-forge-100 text-forge-700 rounded-full text-xs font-medium">
              {COUNTRIES[drillCountry] || drillCountry}
            </span>
          )}
          {drillSearch && (
            <span className="px-2 py-0.5 bg-forge-100 text-forge-700 rounded-full text-xs font-medium">
              &ldquo;{drillSearch}&rdquo;
            </span>
          )}
          <button
            onClick={clearDrillDown}
            className="ml-auto flex items-center gap-1 text-xs text-gray-500 hover:text-gray-700 transition-colors"
          >
            <X className="w-3 h-3" />
            Clear
          </button>
        </div>
      )}

      {/* Enrichment progress banner */}
      {enriching && enrichProgress && (
        <div className="bg-white rounded-xl border border-forge-200 shadow-sm p-4 space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 min-w-0">
              <Loader2 className="w-4 h-4 text-forge-600 animate-spin shrink-0" />
              <span className="text-sm font-medium text-gray-900 truncate">
                {enrichProgress.phase === "waiting"
                  ? "Waiting for new companies..."
                  : <>Enriching: {enrichProgress.currentCompany}</>}
                {enrichProgress.model && (
                  <span className="text-gray-400 font-normal ml-1">
                    ({enrichProgress.model})
                  </span>
                )}
              </span>
            </div>
            <div className="flex items-center gap-3 shrink-0 text-xs text-gray-500">
              <span>
                Enriched: {enrichProgress.enriched}
              </span>
              {enrichProgress.errors > 0 && (
                <span className="text-red-600">
                  {enrichProgress.errors} error{enrichProgress.errors !== 1 ? "s" : ""}
                </span>
              )}
              {enrichProgress.total != null && (
                <span className="font-medium text-gray-700">
                  {Math.round(((enrichProgress.enriched + enrichProgress.errors) / enrichProgress.total) * 100)}%
                </span>
              )}
              <button
                onClick={handleStop}
                disabled={cancelling}
                className={`flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${
                  cancelling
                    ? "bg-gray-200 text-gray-400 cursor-not-allowed"
                    : "bg-red-100 text-red-700 hover:bg-red-200"
                }`}
              >
                <StopCircle className="w-3.5 h-3.5" />
                {cancelling ? "Stopping..." : "Stop"}
              </button>
            </div>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
            {enrichProgress.phase === "waiting" ? (
              <div className="bg-forge-400 h-2 rounded-full w-full animate-pulse" />
            ) : enrichProgress.total != null ? (
              <div
                className="bg-forge-500 h-2 rounded-full transition-all duration-500 ease-out"
                style={{
                  width: `${((enrichProgress.enriched + enrichProgress.errors) / enrichProgress.total) * 100}%`,
                }}
              />
            ) : (
              <div className="bg-forge-500 h-2 rounded-full animate-pulse" style={{ width: "100%" }} />
            )}
          </div>
        </div>
      )}

      {/* Push progress banner */}
      {enriching && pushProgress && (
        <div className="bg-white rounded-xl border border-purple-200 shadow-sm p-4 space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 min-w-0">
              <Loader2 className="w-4 h-4 text-purple-600 animate-spin shrink-0" />
              <span className="text-sm font-medium text-gray-900 truncate">
                Pushing: {pushProgress.currentCompany}
              </span>
            </div>
            <div className="flex items-center gap-3 shrink-0 text-xs text-gray-500">
              <span>
                {pushProgress.pushed} pushed
              </span>
              {pushProgress.skipped > 0 && (
                <span className="text-amber-600">
                  {pushProgress.skipped} skipped
                </span>
              )}
              {pushProgress.errors > 0 && (
                <span className="text-red-600">
                  {pushProgress.errors} error{pushProgress.errors !== 1 ? "s" : ""}
                </span>
              )}
              <button
                onClick={handleStop}
                disabled={cancelling}
                className={`flex items-center gap-1 px-2.5 py-1 rounded-md text-xs font-medium transition-colors ${
                  cancelling
                    ? "bg-gray-200 text-gray-400 cursor-not-allowed"
                    : "bg-red-100 text-red-700 hover:bg-red-200"
                }`}
              >
                <StopCircle className="w-3.5 h-3.5" />
                {cancelling ? "Stopping..." : "Stop"}
              </button>
            </div>
          </div>
          <div className="w-full bg-gray-100 rounded-full h-2 overflow-hidden">
            <div
              className="bg-purple-500 h-2 rounded-full transition-all duration-500 ease-out"
              style={{
                width: `${pushProgress.total > 0 ? ((pushProgress.pushed + pushProgress.skipped + pushProgress.errors) / pushProgress.total) * 100 : 0}%`,
              }}
            />
          </div>
        </div>
      )}

      {/* Enhancement 10: Bulk selection action bar */}
      {selectedIds.size > 0 && (
        <div className="flex items-center gap-3 p-3 bg-forge-50 border border-forge-200 rounded-lg">
          <span className="text-sm font-medium text-forge-700">{selectedIds.size} selected</span>
          <button onClick={handleBulkApproveSelected} className="px-3 py-1 bg-green-600 text-white text-xs rounded-lg hover:bg-green-700">Approve Selected</button>
          <button onClick={handleBulkRejectSelected} className="px-3 py-1 bg-red-600 text-white text-xs rounded-lg hover:bg-red-700">Reject Selected</button>
          <button onClick={() => setSelectedIds(new Set())} className="px-3 py-1 bg-gray-200 text-gray-700 text-xs rounded-lg hover:bg-gray-300">Clear</button>
        </div>
      )}

      {/* Enhancement 12: Side-by-side comparison */}
      {compareMode && compareList.length >= 2 && (
        <div className="flex gap-3">
          {compareList.map((id) => {
            const c = companies.find((co) => String(co.id) === id);
            if (!c) return null;
            return (
              <div key={id} className="flex-1 bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
                <h4 className="text-sm font-semibold text-gray-900 truncate">{String(c.name)}</h4>
                <div className="mt-2 space-y-1 text-xs text-gray-600">
                  <p>Relevance: {String(c.relevance_score || "\u2014")}</p>
                  <p>Quality: {String(c.enrichment_quality || "\u2014")}</p>
                  <p>Category: {String(c.subcategory || "\u2014")}</p>
                  <p>Country: {String(c.country || "\u2014")}</p>
                  <p>Status: {String(c.status || "\u2014")}</p>
                </div>
                <button onClick={() => setCompareList((prev) => prev.filter((x) => x !== id))} className="mt-2 text-[10px] text-red-500 hover:underline">Remove</button>
              </div>
            );
          })}
          <button onClick={() => { setCompareMode(false); setCompareList([]); }} className="self-start px-2 py-1 text-xs text-gray-500 hover:text-gray-700">Exit Compare</button>
        </div>
      )}

      <div className="flex gap-4">
        {/* Company list */}
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm flex flex-col">
          {/* Search bar */}
          <div className="px-3 py-2 border-b border-gray-100">
            <div className="flex items-center gap-2">
              {/* Enhancement 10: Select All checkbox */}
              <input
                type="checkbox"
                checked={companies.length > 0 && companies.every((c) => selectedIds.has(String(c.id)))}
                onChange={(e) => {
                  if (e.target.checked) {
                    setSelectedIds(new Set(companies.map((c) => String(c.id))));
                  } else {
                    setSelectedIds(new Set());
                  }
                }}
                className="w-3.5 h-3.5 rounded border-gray-300 text-forge-600 focus:ring-forge-500 shrink-0"
              />
              <div className="relative flex-1">
                <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-gray-400" />
                <input
                  type="text"
                  value={searchQuery}
                  onChange={(e) => setSearchQuery(e.target.value)}
                  placeholder="Search companies by name, description, materials..."
                  className="w-full pl-8 pr-8 py-1.5 text-sm border border-gray-200 rounded-lg bg-gray-50 focus:bg-white focus:border-forge-400 focus:outline-none focus:ring-1 focus:ring-forge-400 transition-colors"
                />
                {searchQuery && (
                  <button
                    onClick={() => setSearchQuery("")}
                    className="absolute right-2.5 top-1/2 -translate-y-1/2 text-gray-400 hover:text-gray-600"
                  >
                    <X className="w-3.5 h-3.5" />
                  </button>
                )}
              </div>
              {/* Enhancement 12: Compare toggle */}
              <button
                onClick={() => {
                  setCompareMode(!compareMode);
                  if (compareMode) setCompareList([]);
                }}
                className={`px-2.5 py-1.5 text-xs font-medium rounded-lg border transition-colors shrink-0 ${
                  compareMode
                    ? "bg-forge-100 text-forge-700 border-forge-300"
                    : "bg-white text-gray-600 border-gray-200 hover:bg-gray-50"
                }`}
              >
                Compare
              </button>
            </div>
          </div>
          <div className="divide-y divide-gray-100 max-h-[calc(100vh-310px)] overflow-y-auto flex-1">
            {companies.length === 0 ? (
              <div className="p-8 text-center text-gray-400 text-sm">
                No companies found with this status.
              </div>
            ) : (
              companies.map((company, companyIndex) => {
                const cStatus = String(company.status || "");
                const companyId = String(company.id);
                const isKeyboardSelected = companyIndex === selectedIndex;
                return (
                  <div
                    key={companyId}
                    className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                      selected && String(selected.id) === companyId
                        ? "bg-blue-50"
                        : "hover:bg-gray-50"
                    } ${isKeyboardSelected ? "ring-2 ring-forge-400 ring-inset" : ""}`}
                    onClick={() => {
                      if (compareMode) {
                        setCompareList((prev) =>
                          prev.includes(companyId)
                            ? prev.filter((x) => x !== companyId)
                            : prev.length < 3
                              ? [...prev, companyId]
                              : prev
                        );
                      } else {
                        setSelected(company);
                        setSelectedIndex(companyIndex);
                      }
                    }}
                  >
                    {/* Enhancement 10: Checkbox */}
                    <input
                      type="checkbox"
                      checked={selectedIds.has(companyId)}
                      onChange={(e) => {
                        e.stopPropagation();
                        setSelectedIds((prev) => {
                          const next = new Set(prev);
                          if (next.has(companyId)) next.delete(companyId);
                          else next.add(companyId);
                          return next;
                        });
                      }}
                      onClick={(e) => e.stopPropagation()}
                      className="w-3.5 h-3.5 rounded border-gray-300 text-forge-600 focus:ring-forge-500 shrink-0"
                    />
                    <Building2 className="w-4 h-4 text-gray-400 shrink-0" />
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-medium text-gray-900 truncate">
                        {String(company.name || "")}
                      </p>
                      <p className="text-xs text-gray-500">
                        {COUNTRIES[String(company.country || "")] ||
                          String(company.country || "")}{" "}
                        &middot; {String(company.domain || "")}
                      </p>
                    </div>
                    <div className="flex items-center gap-2 shrink-0">
                      <span
                        className={`px-2 py-0.5 rounded-full text-xs ${
                          STATUS_BADGE[cStatus] || "bg-gray-100 text-gray-500"
                        }`}
                      >
                        {cStatus}
                      </span>
                      {company.relevance_score != null &&
                        Number(company.relevance_score) > 0 && (
                          <div className="flex items-center gap-1">
                            <Star className="w-3 h-3 text-yellow-500" />
                            <span className="text-xs font-medium text-gray-700">
                              {String(company.relevance_score)}
                            </span>
                          </div>
                        )}
                      {cStatus === "approved" && (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handlePushSingle(String(company.id));
                          }}
                          disabled={pushing === String(company.id)}
                          className="p-1 rounded hover:bg-purple-100 text-purple-600 transition-colors"
                          title="Push to ForgeOS"
                        >
                          {pushing === String(company.id) ? (
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          ) : (
                            <ArrowUpCircle className="w-3.5 h-3.5" />
                          )}
                        </button>
                      )}
                      {Boolean(company.supabase_listing_id) && (
                        <button
                          onClick={(e) => {
                            e.stopPropagation();
                            handleRemoveFromMarketplace(String(company.id));
                          }}
                          disabled={removing === String(company.id)}
                          className="p-1 rounded hover:bg-red-100 text-red-500 transition-colors"
                          title="Remove from Marketplace"
                        >
                          {removing === String(company.id) ? (
                            <Loader2 className="w-3.5 h-3.5 animate-spin" />
                          ) : (
                            <Trash2 className="w-3.5 h-3.5" />
                          )}
                        </button>
                      )}
                      <ChevronRight className="w-4 h-4 text-gray-300" />
                    </div>
                  </div>
                );
              })
            )}
          </div>
          {/* Enhancement 8: Pagination */}
          <div className="flex items-center justify-between px-4 py-2 border-t border-gray-100">
            <span className="text-xs text-gray-500">
              {companies.length > 0 ? `${page * 50 + 1}\u2013${Math.min((page + 1) * 50, totalCount)} of ${totalCount}` : "0 results"}
            </span>
            <div className="flex items-center gap-2">
              <button
                onClick={() => setPage(Math.max(0, page - 1))}
                disabled={page === 0}
                className="px-2 py-1 text-xs text-gray-600 border border-gray-200 rounded disabled:opacity-30 hover:bg-gray-50"
              >
                Prev
              </button>
              <span className="text-xs text-gray-500">
                Page {page + 1} of {Math.max(1, Math.ceil(totalCount / 50))}
              </span>
              <button
                onClick={() => setPage(page + 1)}
                disabled={(page + 1) * 50 >= totalCount}
                className="px-2 py-1 text-xs text-gray-600 border border-gray-200 rounded disabled:opacity-30 hover:bg-gray-50"
              >
                Next
              </button>
            </div>
          </div>
        </div>

        {/* Detail panel */}
        {selected && (
          <div className="w-[420px] shrink-0 bg-white rounded-xl border border-gray-200 shadow-sm max-h-[calc(100vh-260px)] overflow-y-auto">
            <div className="p-4 space-y-5">
              {/* Header */}
              <div>
                <div className="flex items-start justify-between gap-2">
                  <h3 className="text-lg font-semibold text-gray-900">
                    {String(selected.name || "")}
                  </h3>
                  <span
                    className={`px-2 py-0.5 rounded-full text-xs shrink-0 ${
                      STATUS_BADGE[status] || "bg-gray-100 text-gray-500"
                    }`}
                  >
                    {status}
                  </span>
                </div>
                {!!selected.domain && (
                  <p className="text-sm text-gray-500 flex items-center gap-1 mt-1">
                    <Globe className="w-3 h-3" />
                    {String(selected.domain)}
                  </p>
                )}
                {status === "pushed" && !!selected.supabase_listing_id && (
                  <p className="text-xs text-purple-600 mt-1 font-mono">
                    Listing ID: {String(selected.supabase_listing_id)}
                  </p>
                )}
              </div>

              {/* Enhancement 11: Detail tabs */}
              <div className="flex gap-1 border-b border-gray-100 pb-2">
                {(["overview", "capabilities", "contact", "raw"] as const).map((tab) => (
                  <button
                    key={tab}
                    onClick={() => setDetailTab(tab)}
                    className={`px-3 py-1 text-xs rounded-md transition-colors ${
                      detailTab === tab ? "bg-forge-100 text-forge-700 font-medium" : "text-gray-500 hover:text-gray-700"
                    }`}
                  >
                    {tab.charAt(0).toUpperCase() + tab.slice(1)}
                  </button>
                ))}
              </div>

              {/* === OVERVIEW TAB === */}
              {detailTab === "overview" && (
                <div className="space-y-5">
                  {/* Scores */}
                  {(Number(selected.relevance_score) > 0 ||
                    Number(selected.enrichment_quality) > 0) && (
                    <div className="flex gap-4">
                      <div className="text-center">
                        <div className="text-2xl font-bold text-yellow-600">
                          {String(selected.relevance_score || 0)}
                        </div>
                        <div className="text-xs text-gray-400">Relevance</div>
                      </div>
                      <div className="text-center">
                        <div className="text-2xl font-bold text-purple-600">
                          {String(selected.enrichment_quality || 0)}
                        </div>
                        <div className="text-xs text-gray-400">Quality</div>
                      </div>
                    </div>
                  )}

                  {/* Error detail */}
                  {status === "error" && !!selected.last_error && (
                    <div className="rounded-lg bg-red-50 border border-red-200 p-3">
                      <h4 className="text-xs font-medium text-red-700 uppercase mb-1 flex items-center gap-1">
                        <AlertTriangle className="w-3 h-3" /> Last Error
                      </h4>
                      <p className="text-xs text-red-600 font-mono whitespace-pre-wrap break-all">
                        {String(selected.last_error)}
                      </p>
                    </div>
                  )}

                  {/* Description */}
                  {!!selected.description && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-1">
                        Description
                      </h4>
                      <p className="text-sm text-gray-700">
                        {String(selected.description)}
                      </p>
                    </div>
                  )}

                  {/* Original-language description */}
                  {!!selected.description_original &&
                    String(selected.description_original) !== "" && (
                      <div className="rounded-lg bg-blue-50 border border-blue-200 p-3">
                        <h4 className="text-xs text-blue-600 uppercase mb-1 flex items-center gap-1">
                          <Languages className="w-3 h-3" /> Original (source
                          language)
                        </h4>
                        <p className="text-sm text-blue-700 italic">
                          {String(selected.description_original)}
                        </p>
                      </div>
                    )}

                  {/* Address */}
                  {companyAddress && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-1 flex items-center gap-1">
                        <MapPin className="w-3 h-3" /> Address
                      </h4>
                      <p className="text-sm text-gray-700">{companyAddress}</p>
                    </div>
                  )}

                  {/* Financial Health */}
                  {financialHealth && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <HeartPulse className="w-3 h-3" /> Financial Health
                      </h4>
                      <div className="flex items-center gap-2 mb-2">
                        <span
                          className={`px-2.5 py-1 rounded-full text-xs font-semibold ${
                            financialHealth === "good"
                              ? "bg-green-100 text-green-700"
                              : financialHealth === "caution"
                                ? "bg-yellow-100 text-yellow-700"
                                : financialHealth === "risk"
                                  ? "bg-red-100 text-red-700"
                                  : "bg-gray-100 text-gray-500"
                          }`}
                        >
                          {financialHealth.charAt(0).toUpperCase() +
                            financialHealth.slice(1)}
                        </span>
                      </div>
                      {attrFinancialSignals && (
                        <div className="grid grid-cols-2 gap-2 text-xs">
                          <DetailField
                            label="Status"
                            value={attrFinancialSignals.company_status as string}
                          />
                          <DetailField
                            label="Years Trading"
                            value={attrFinancialSignals.years_trading as number}
                          />
                          <DetailField
                            label="Accounts Type"
                            value={attrFinancialSignals.accounts_type as string}
                          />
                          <DetailField
                            label="Last Accounts"
                            value={
                              attrFinancialSignals.last_accounts_date as string
                            }
                          />
                          {attrFinancialSignals.has_insolvency_history === true && (
                            <div className="col-span-2 flex items-center gap-1 text-red-600">
                              <AlertTriangle className="w-3 h-3" />
                              <span>Insolvency history</span>
                            </div>
                          )}
                          {attrFinancialSignals.has_charges === true && (
                            <div className="col-span-2 flex items-center gap-1 text-amber-600">
                              <AlertTriangle className="w-3 h-3" />
                              <span>Has secured charges</span>
                            </div>
                          )}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Category */}
                  {!!(selected.category || selected.subcategory) && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <Tag className="w-3 h-3" /> Classification
                      </h4>
                      <div className="grid grid-cols-2 gap-3">
                        <DetailField
                          label="Category"
                          value={selected.category as string}
                        />
                        <DetailField
                          label="Subcategory"
                          value={selected.subcategory as string}
                        />
                      </div>
                    </div>
                  )}

                  {/* Basic Info (country/city/domain) */}
                  <div>
                    <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                      <Building2 className="w-3 h-3" /> Basic Info
                    </h4>
                    <div className="grid grid-cols-2 gap-3">
                      <DetailField
                        label="Country"
                        value={
                          COUNTRIES[String(selected.country || "")] ||
                          (selected.country as string)
                        }
                      />
                      <DetailField label="City" value={selected.city as string} />
                      <DetailField
                        label="Founded"
                        value={
                          (attrs.founded_year as string) ||
                          (selected.year_founded as string)
                        }
                      />
                      <DetailField
                        label="Company Size"
                        value={
                          (attrs.company_size as string) ||
                          (attrs.employees as string) ||
                          (selected.company_size as string)
                        }
                      />
                    </div>
                  </div>
                </div>
              )}

              {/* === CAPABILITIES TAB === */}
              {detailTab === "capabilities" && (
                <div className="space-y-5">
                  {/* Process Capabilities (from deep enrichment) */}
                  {processCapabilities.length > 0 && (
                    <div>
                      <button
                        onClick={() => setProcessCapOpen(!processCapOpen)}
                        className="flex items-center justify-between w-full mb-2"
                      >
                        <h4 className="text-xs text-gray-400 uppercase flex items-center gap-1">
                          <Wrench className="w-3 h-3" /> Process Capabilities
                          <span className="ml-1 px-1.5 py-0.5 rounded-full bg-violet-100 text-violet-700 text-[10px] font-medium">
                            {processCapabilities.length}
                          </span>
                        </h4>
                        <ChevronDown
                          className={`w-3.5 h-3.5 text-gray-400 transition-transform ${
                            processCapOpen ? "rotate-180" : ""
                          }`}
                        />
                      </button>
                      {processCapOpen && (
                        <div className="space-y-3">
                          {processCapabilities.map((proc, idx) => (
                            <div
                              key={idx}
                              className="rounded-lg border border-gray-100 bg-gray-50 p-3 space-y-2"
                            >
                              {/* Process heading + category badge + confidence */}
                              <div className="flex items-start justify-between gap-2">
                                <div className="flex items-center gap-2 flex-wrap">
                                  <span className="text-sm font-medium text-gray-900">
                                    {proc.process_name || "Unknown Process"}
                                  </span>
                                  {proc.process_category && (
                                    <span className="px-1.5 py-0.5 rounded text-[10px] font-medium bg-violet-50 text-violet-600">
                                      {proc.process_category.replace(/_/g, " ")}
                                    </span>
                                  )}
                                </div>
                                {proc.confidence != null && (
                                  <span
                                    className={`w-2 h-2 rounded-full shrink-0 mt-1.5 ${
                                      proc.confidence >= 0.8
                                        ? "bg-green-500"
                                        : proc.confidence >= 0.5
                                          ? "bg-amber-500"
                                          : "bg-red-500"
                                    }`}
                                    title={`Confidence: ${(proc.confidence * 100).toFixed(0)}%`}
                                  />
                                )}
                              </div>

                              {/* Materials */}
                              {proc.materials_worked && proc.materials_worked.length > 0 && (
                                <TagPills
                                  items={proc.materials_worked}
                                  color="bg-amber-50 text-amber-700"
                                />
                              )}

                              {/* Tolerance + Surface finish */}
                              {(proc.tolerance_claimed || proc.surface_finish_claimed) && (
                                <div className="flex gap-3 text-xs">
                                  {proc.tolerance_claimed && (
                                    <span className="text-gray-600">
                                      <span className="text-gray-400">Tol: </span>
                                      {proc.tolerance_claimed}
                                    </span>
                                  )}
                                  {proc.surface_finish_claimed && (
                                    <span className="text-gray-600">
                                      <span className="text-gray-400">Finish: </span>
                                      {proc.surface_finish_claimed}
                                    </span>
                                  )}
                                </div>
                              )}

                              {/* Equipment */}
                              {proc.equipment_mentioned && proc.equipment_mentioned.length > 0 && (
                                <TagPills
                                  items={proc.equipment_mentioned}
                                  color="bg-teal-50 text-teal-700"
                                />
                              )}

                              {/* Batch size + Max dimensions */}
                              {(proc.batch_size_range || proc.max_part_dimensions) && (
                                <div className="flex gap-3 text-xs">
                                  {proc.batch_size_range && (
                                    <span className="text-gray-600">
                                      <span className="text-gray-400">Batch: </span>
                                      {proc.batch_size_range}
                                    </span>
                                  )}
                                  {proc.max_part_dimensions && (
                                    <span className="text-gray-600">
                                      <span className="text-gray-400">Max: </span>
                                      {proc.max_part_dimensions}
                                    </span>
                                  )}
                                </div>
                              )}

                              {/* Surface treatments */}
                              {proc.surface_treatments && proc.surface_treatments.length > 0 && (
                                <TagPills
                                  items={proc.surface_treatments}
                                  color="bg-green-50 text-green-700"
                                />
                              )}

                              {/* Source excerpt (collapsed by default) */}
                              {proc.source_excerpt && (
                                <div>
                                  <button
                                    onClick={() => {
                                      setExpandedExcerpts((prev) => {
                                        const next = new Set(prev);
                                        if (next.has(idx)) next.delete(idx);
                                        else next.add(idx);
                                        return next;
                                      });
                                    }}
                                    className="text-[10px] text-gray-400 hover:text-gray-600"
                                  >
                                    {expandedExcerpts.has(idx) ? "Hide source" : "Show source"}
                                  </button>
                                  {expandedExcerpts.has(idx) && (
                                    <p className="text-[11px] text-gray-400 italic mt-1 leading-relaxed">
                                      &ldquo;{proc.source_excerpt}&rdquo;
                                    </p>
                                  )}
                                </div>
                              )}
                            </div>
                          ))}
                        </div>
                      )}
                    </div>
                  )}

                  {/* Specialties & Certifications */}
                  {(specialties.length > 0 || certifications.length > 0) && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <Award className="w-3 h-3" /> Capabilities &amp;
                        Certifications
                      </h4>
                      {specialties.length > 0 && (
                        <div className="mb-2">
                          <p className="text-xs text-gray-400 mb-1">Specialties</p>
                          <TagPills
                            items={specialties}
                            color="bg-blue-50 text-blue-700"
                          />
                        </div>
                      )}
                      {certifications.length > 0 && (
                        <div>
                          <p className="text-xs text-gray-400 mb-1">
                            Certifications
                          </p>
                          <TagPills
                            items={certifications}
                            color="bg-green-50 text-green-700"
                          />
                        </div>
                      )}
                    </div>
                  )}

                  {/* Materials */}
                  {attrMaterials.length > 0 && (
                    <div>
                      <p className="text-xs text-gray-400 mb-1">Materials</p>
                      <TagPills
                        items={attrMaterials}
                        color="bg-amber-50 text-amber-700"
                      />
                    </div>
                  )}

                  {/* Key Equipment */}
                  {attrEquipment.length > 0 && (
                    <div>
                      <p className="text-xs text-gray-400 mb-1">
                        Key Equipment
                      </p>
                      <TagPills
                        items={attrEquipment}
                        color="bg-teal-50 text-teal-700"
                      />
                    </div>
                  )}

                  {/* Lead Time & MOQ */}
                  {!!(attrs.lead_time || attrs.minimum_order) && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <Clock className="w-3 h-3" /> Lead Time &amp; MOQ
                      </h4>
                      <div className="grid grid-cols-2 gap-3">
                        <DetailField
                          label="Lead Time"
                          value={attrs.lead_time as string}
                        />
                        <DetailField
                          label="Minimum Order"
                          value={attrs.minimum_order as string}
                        />
                      </div>
                    </div>
                  )}

                  {/* Quality & Compliance */}
                  {!!(attrs.quality_systems ||
                    attrs.export_controls ||
                    attrSecurityClearances.length > 0) && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <ShieldCheck className="w-3 h-3" /> Quality &amp; Compliance
                      </h4>
                      {!!attrs.quality_systems && (
                        <div className="mb-2">
                          <p className="text-xs text-gray-400 mb-0.5">
                            Quality Systems
                          </p>
                          <p className="text-sm text-gray-700">
                            {String(attrs.quality_systems)}
                          </p>
                        </div>
                      )}
                      {!!attrs.export_controls && (
                        <div className="mb-2">
                          <p className="text-xs text-gray-400 mb-0.5">
                            Export Controls
                          </p>
                          <p className="text-sm text-gray-700">
                            {String(attrs.export_controls)}
                          </p>
                        </div>
                      )}
                      {attrSecurityClearances.length > 0 && (
                        <div>
                          <p className="text-xs text-gray-400 mb-1">
                            Security Clearances
                          </p>
                          <TagPills
                            items={attrSecurityClearances}
                            color="bg-red-50 text-red-700"
                          />
                        </div>
                      )}
                    </div>
                  )}
                </div>
              )}

              {/* === CONTACT TAB === */}
              {detailTab === "contact" && (
                <div className="space-y-5">
                  {/* Contact */}
                  {!!(selected.contact_name ||
                    selected.contact_email ||
                    selected.contact_title) && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <User className="w-3 h-3" /> Contact
                      </h4>
                      <div className="grid grid-cols-2 gap-3">
                        <DetailField
                          label="Name"
                          value={selected.contact_name as string}
                        />
                        <DetailField
                          label="Title"
                          value={selected.contact_title as string}
                        />
                      </div>
                      {!!selected.contact_email && (
                        <a
                          href={`mailto:${String(selected.contact_email)}`}
                          className="inline-flex items-center gap-1 text-xs text-forge-600 hover:text-forge-700 mt-2"
                        >
                          <Mail className="w-3 h-3" />
                          {String(selected.contact_email)}
                        </a>
                      )}
                    </div>
                  )}

                  {/* Key People (from LLM enrichment) */}
                  {attrKeyPeople.length > 0 && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <User className="w-3 h-3" /> Key People
                      </h4>
                      <div className="space-y-1.5">
                        {attrKeyPeople.map((person, i) => (
                          <div key={i} className="flex items-center gap-2">
                            <User className="w-3 h-3 text-gray-300 shrink-0" />
                            <span className="text-sm text-gray-900 font-medium">
                              {person.name}
                            </span>
                            {person.title && (
                              <span className="text-xs text-gray-500">
                                — {person.title}
                              </span>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Directors from Companies House */}
                  {attrChDirectors.length > 0 && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <Building2 className="w-3 h-3" /> Directors (Companies House)
                      </h4>
                      <div className="space-y-1">
                        {attrChDirectors.map((director, i) => (
                          <div key={i} className="flex items-center gap-2">
                            <User className="w-3 h-3 text-gray-300 shrink-0" />
                            <span className="text-sm text-gray-700">{director}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* No contact info message */}
                  {!(selected.contact_name || selected.contact_email || selected.contact_title) &&
                    attrKeyPeople.length === 0 &&
                    attrChDirectors.length === 0 && (
                    <div className="p-4 text-center text-gray-400 text-sm">
                      No contact information available.
                    </div>
                  )}
                </div>
              )}

              {/* === RAW TAB === */}
              {detailTab === "raw" && (
                <div className="space-y-5">
                  {/* Attributes from attributes_json */}
                  {(attrIndustries.length > 0 ||
                    industries.length > 0 ||
                    attrs.founded_year ||
                    attrs.company_size ||
                    attrs.employees ||
                    attrs.employee_count_exact ||
                    attrs.production_capacity ||
                    selected.company_size ||
                    selected.year_founded ||
                    attrs.company_number ||
                    attrSicCodes.length > 0) && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <Factory className="w-3 h-3" /> Attributes
                      </h4>

                      <div className="space-y-2">
                        {(attrIndustries.length > 0 || industries.length > 0) && (
                          <div>
                            <p className="text-xs text-gray-400 mb-1">
                              Industries
                            </p>
                            <TagPills
                              items={[
                                ...new Set([...industries, ...attrIndustries]),
                              ]}
                              color="bg-indigo-50 text-indigo-700"
                            />
                          </div>
                        )}

                        <div className="grid grid-cols-2 gap-3">
                          <DetailField
                            label="Founded"
                            value={
                              (attrs.founded_year as string) ||
                              (selected.year_founded as string)
                            }
                          />
                          <DetailField
                            label="Company Size"
                            value={
                              (attrs.company_size as string) ||
                              (attrs.employees as string) ||
                              (selected.company_size as string)
                            }
                          />
                          <DetailField
                            label="Employees (exact)"
                            value={attrs.employee_count_exact as string}
                          />
                          <DetailField
                            label="Production Capacity"
                            value={attrs.production_capacity as string}
                          />
                          <DetailField
                            label="Company Number"
                            value={attrs.company_number as string}
                          />
                        </div>

                        {attrSicCodes.length > 0 && (
                          <div>
                            <p className="text-xs text-gray-400 mb-1">SIC Codes</p>
                            <TagPills
                              items={attrSicCodes}
                              color="bg-gray-100 text-gray-600"
                            />
                          </div>
                        )}
                      </div>
                    </div>
                  )}

                  {/* Source Info */}
                  <div>
                    <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                      <Building2 className="w-3 h-3" /> Source Info
                    </h4>
                    <div className="grid grid-cols-2 gap-3">
                      <DetailField
                        label="Source"
                        value={selected.source as string}
                      />
                      <DetailField
                        label="Source Query"
                        value={selected.source_query as string}
                      />
                    </div>
                    {!!selected.source_url && (
                      <a
                        href={String(selected.source_url)}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="inline-flex items-center gap-1 text-xs text-forge-600 hover:text-forge-700 mt-2"
                      >
                        <ExternalLink className="w-3 h-3" />
                        Source URL
                      </a>
                    )}
                    {!!selected.raw_snippet && (
                      <div className="mt-2">
                        <h4 className="text-xs text-gray-400 uppercase mb-0.5">
                          Raw Snippet
                        </h4>
                        <p className="text-xs text-gray-500 italic">
                          {String(selected.raw_snippet)}
                        </p>
                      </div>
                    )}
                    {!!selected.snippet_english &&
                      String(selected.snippet_english) !== "" && (
                        <div className="mt-2">
                          <h4 className="text-xs text-blue-500 uppercase mb-0.5 flex items-center gap-1">
                            <Languages className="w-3 h-3" /> Snippet (English)
                          </h4>
                          <p className="text-xs text-gray-600 italic">
                            {String(selected.snippet_english)}
                          </p>
                        </div>
                      )}
                  </div>

                  {/* Products */}
                  {attrProducts.length > 0 && (
                    <div>
                      <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                        <Package className="w-3 h-3" /> Products
                      </h4>
                      <TagPills
                        items={attrProducts}
                        color="bg-rose-50 text-rose-700"
                      />
                    </div>
                  )}
                </div>
              )}

              {/* Actions */}
              <div className="flex gap-2 pt-2 border-t border-gray-100">
                {(status === "discovered" || status === "error") && (
                  <button
                    onClick={handleRunEnrich}
                    disabled={enriching}
                    className={`flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors ${
                      enriching
                        ? "bg-forge-400 cursor-not-allowed"
                        : "bg-forge-600 hover:bg-forge-700"
                    }`}
                  >
                    <RefreshCw className={`w-4 h-4 ${enriching ? "animate-spin" : ""}`} />
                    {enriching
                      ? "Enriching..."
                      : status === "error"
                        ? "Retry Enrichment"
                        : "Run Enrichment"}
                  </button>
                )}
                {status === "enriched" && (
                  <>
                    <button
                      onClick={() => handleApprove(String(selected.id))}
                      className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg text-sm font-medium text-white transition-colors"
                    >
                      <CheckCircle className="w-4 h-4" />
                      Approve
                      <kbd className="ml-1 px-1 py-0.5 bg-green-700 rounded text-[10px] font-mono">a</kbd>
                    </button>
                    <button
                      onClick={() => handleReject(String(selected.id))}
                      className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm font-medium text-white transition-colors"
                    >
                      <XCircle className="w-4 h-4" />
                      Reject
                      <kbd className="ml-1 px-1 py-0.5 bg-red-700 rounded text-[10px] font-mono">x</kbd>
                    </button>
                  </>
                )}
                {status === "approved" && (
                  <>
                    <button
                      onClick={() => handlePushSingle(String(selected.id))}
                      disabled={pushing === String(selected.id)}
                      className={`flex-1 flex items-center justify-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors ${
                        pushing === String(selected.id)
                          ? "bg-purple-400 cursor-not-allowed"
                          : "bg-purple-600 hover:bg-purple-700"
                      }`}
                    >
                      {pushing === String(selected.id) ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <ArrowUpCircle className="w-4 h-4" />
                      )}
                      {pushing === String(selected.id) ? "Pushing..." : "Push to ForgeOS"}
                    </button>
                    <button
                      onClick={() => handleUnapprove(String(selected.id))}
                      className="flex items-center justify-center gap-2 px-4 py-2 bg-gray-600 hover:bg-gray-700 rounded-lg text-sm font-medium text-white transition-colors"
                    >
                      <Undo2 className="w-4 h-4" />
                      Un-approve
                    </button>
                  </>
                )}
                {Boolean(selected.supabase_listing_id) && (
                  <button
                    onClick={() => handleRemoveFromMarketplace(String(selected.id))}
                    disabled={removing === String(selected.id)}
                    className={`flex items-center justify-center gap-2 px-4 py-2 rounded-lg text-sm font-medium text-white transition-colors ${
                      removing === String(selected.id)
                        ? "bg-red-400 cursor-not-allowed"
                        : "bg-red-600 hover:bg-red-700"
                    }`}
                  >
                    {removing === String(selected.id) ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <Trash2 className="w-4 h-4" />
                    )}
                    {removing === String(selected.id) ? "Removing..." : "Remove from Marketplace"}
                  </button>
                )}
              </div>
            </div>
          </div>
        )}
      </div>
      <ConfirmDialog
        open={!!confirmDialog}
        title={confirmDialog?.title ?? ""}
        message={confirmDialog?.message ?? ""}
        confirmLabel={confirmDialog?.confirmLabel ?? "Confirm"}
        variant="danger"
        onConfirm={() => confirmDialog?.onConfirm()}
        onCancel={() => setConfirmDialog(null)}
      />
    </div>
  );
}
