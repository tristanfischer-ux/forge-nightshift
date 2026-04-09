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
  Sparkles,
  Newspaper,
  Brain,
  FileSearch,
  Lock,
  Eye,
  Users,
  Crown,
  TrendingUp,
  Target,
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
  searchSemantic,
  getCompanyActivities,
  type ActivityItem,
  getCompanyIntel,
  getCompanyVerification,
  getSearchProfiles,
  getActiveProfile,
  getExtendedStats,
} from "../lib/tauri";
import type { SearchProfile } from "../lib/tauri";
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

type StatusFilter = "all" | "discovered" | "enriched" | "verified" | "synthesized" | "approved" | "pushed" | "error";

const STATUS_BADGE: Record<string, string> = {
  discovered: "bg-blue-100 text-blue-700",
  enriching: "bg-forge-100 text-forge-700 animate-pulse",
  enriched: "bg-green-100 text-green-700",
  verified: "bg-teal-100 text-teal-700",
  synthesized: "bg-indigo-100 text-indigo-700",
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
    verified: 0,
    synthesized: 0,
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
  const [semanticMode, setSemanticMode] = useState(false);
  const [semanticScores, setSemanticScores] = useState<Record<string, number>>({});
  const [semanticLoading, setSemanticLoading] = useState(false);
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
  const [detailTab, setDetailTab] = useState<"overview" | "capabilities" | "intelligence" | "synthesis" | "verification" | "contact" | "raw">("overview");

  // Activity feed
  const [activities, setActivities] = useState<ActivityItem[]>([]);
  const [activitiesLoading, setActivitiesLoading] = useState(false);

  // Intel tab data
  const [intelData, setIntelData] = useState<Record<string, unknown> | null>(null);
  const [intelLoading, setIntelLoading] = useState(false);

  // Verification tab data
  const [verificationData, setVerificationData] = useState<Record<string, unknown> | null>(null);
  const [verificationLoading, setVerificationLoading] = useState(false);

  // Enhancement 12: Side-by-side comparison
  const [compareMode, setCompareMode] = useState(false);
  const [compareList, setCompareList] = useState<string[]>([]);

  const [activeProfileName, setActiveProfileName] = useState<string>("");

  const { showError, showInfo } = useError();
  const refreshTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const pageRef = useRef(page);
  pageRef.current = page;
  const filterRef = useRef(filter);
  filterRef.current = filter;

  // Drill-down filters from URL params
  const drillSubcategory = searchParams.get("subcategory");
  const drillCountry = searchParams.get("country");
  const drillSearch = searchParams.get("search");
  const hasDrillDown = !!(drillSubcategory || drillCountry || drillSearch);

  function clearDrillDown() {
    setSearchParams({});
  }

  // Load active profile name on mount
  useEffect(() => {
    Promise.all([getSearchProfiles(), getActiveProfile()])
      .then(([profiles, activeId]) => {
        const active = profiles.find((p: SearchProfile) => p.id === activeId);
        if (active) setActiveProfileName(active.name);
      })
      .catch(() => {});
  }, []);

  useEffect(() => {
    loadCompanies(filter);
    loadCounts();
  }, [filter, drillSubcategory, drillCountry, drillSearch, page]);

  // Reset page and selection when filters change
  useEffect(() => {
    setPage(0);
    setSelectedIndex(-1);
    setSelectedIds(new Set());
    setCompareList([]);
  }, [filter, drillSubcategory, drillCountry, drillSearch]);

  // Clear selection and compare list when page changes
  useEffect(() => {
    setSelectedIds(new Set());
    setCompareList([]);
  }, [page]);

  // Debounced search — resets page and fetches (uses filterRef to avoid stale closure)
  // Semantic mode uses 500ms debounce (calls OpenAI API), LIKE search uses 300ms
  const searchQueryPrev = useRef(searchQuery);
  useEffect(() => {
    if (searchQueryPrev.current === searchQuery) return;
    searchQueryPrev.current = searchQuery;
    const delay = semanticMode ? 500 : 300;
    const timer = setTimeout(() => {
      setPage(0);
      setSelectedIds(new Set());
      setCompareList([]);
      loadCompanies(filterRef.current);
    }, delay);
    return () => clearTimeout(timer);
  }, [searchQuery, semanticMode]);

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
        loadCompanies(filter).catch((e) => console.warn("pipeline:status refresh failed:", e));
        loadCounts().catch((e) => console.warn("pipeline:status counts refresh failed:", e));
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
          loadCompanies(filter).catch((e) => console.warn("progress refresh failed:", e));
          loadCounts().catch((e) => console.warn("progress counts refresh failed:", e));
        }, 3000);
      }
    });
    return () => {
      unlisten.then((fn) => fn());
      if (refreshTimer.current) {
        clearTimeout(refreshTimer.current);
        refreshTimer.current = null;
      }
    };
  }, [filter]);

  // Enhancement 9: Keyboard shortcuts
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      // Don't intercept when typing in input fields
      if (e.target instanceof HTMLInputElement || e.target instanceof HTMLTextAreaElement) return;
      // Don't intercept when CommandPalette or other overlays are open
      if (document.querySelector("[data-command-palette]")) return;
      // Don't intercept when confirm dialog is open
      if (confirmDialog) return;

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
        if (selected && String(selected.status) === "enriched") {
          e.preventDefault();
          handleApprove(String(selected.id));
        }
      } else if (e.key === "Backspace" || e.key === "x") {
        if (selected && String(selected.status) === "enriched") {
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
  }, [selectedIndex, companies, selected, filter, confirmDialog]);

  // Load activities when a company is selected
  useEffect(() => {
    if (!selected) {
      setActivities([]);
      return;
    }
    const companyId = String(selected.id || "");
    if (!companyId) return;
    setActivitiesLoading(true);
    getCompanyActivities(companyId, 10)
      .then(setActivities)
      .catch(() => setActivities([]))
      .finally(() => setActivitiesLoading(false));
  }, [selected?.id]); // eslint-disable-line react-hooks/exhaustive-deps

  async function loadCounts() {
    try {
      const [stats, extStats] = await Promise.all([getStats(), getExtendedStats()]);
      const rows = (stats.companies as { status: string; count: number }[]) || [];
      const c: Record<StatusFilter, number> = { all: 0, discovered: 0, enriched: 0, verified: 0, synthesized: 0, approved: 0, pushed: 0, error: 0 };
      for (const row of rows) {
        c.all += Number(row.count) || 0;
        if (row.status === "discovered") c.discovered += Number(row.count) || 0;
        else if (row.status === "enriched") c.enriched += Number(row.count) || 0;
        else if (row.status === "approved") c.approved += Number(row.count) || 0;
        else if (row.status === "pushed") c.pushed += Number(row.count) || 0;
        else if (row.status === "error") c.error += Number(row.count) || 0;
      }
      // Verified = enriched + verified_v2_at but no synthesis
      // Synthesized = has synthesis_public_json but not yet approved/pushed
      c.verified = Number(extStats.verified) || 0;
      c.synthesized = Number(extStats.synthesized) || 0;
      setCounts(c);
      // Only update totalCount from stats when no drill-down/search is active
      // (drill-down totalCount is set by loadCompanies from getCompaniesCount)
      if (!hasDrillDown && !searchQuery.trim()) {
        if (filter === "all") {
          setTotalCount(c.all);
        } else {
          setTotalCount(c[filter] || 0);
        }
      }
    } catch (e) {
      showError(`Failed to load counts: ${e}`);
    }
  }

  function filterByVirtualStage(data: Record<string, unknown>[], stage: StatusFilter): Record<string, unknown>[] {
    if (stage === "verified") {
      // Verified: has verified_v2_at but no synthesis_public_json
      return data.filter(c =>
        c.verified_v2_at != null && c.verified_v2_at !== "" &&
        (c.synthesis_public_json == null || c.synthesis_public_json === "")
      );
    }
    if (stage === "synthesized") {
      // Synthesized: has synthesis_public_json
      return data.filter(c =>
        c.synthesis_public_json != null && c.synthesis_public_json !== ""
      );
    }
    return data;
  }

  async function loadCompanies(status: StatusFilter) {
    try {
      const limit = 50;
      const offset = pageRef.current * 50;

      // Semantic search path
      if (semanticMode && searchQuery.trim()) {
        setSemanticLoading(true);
        try {
          const result = await searchSemantic(
            searchQuery.trim(),
            limit,
            status === "all" ? undefined : status,
            drillSubcategory || undefined,
            drillCountry || undefined,
          );
          setCompanies(result.companies);
          setTotalCount(result.total);
          // Build score map by company id
          const scoreMap: Record<string, number> = {};
          result.companies.forEach((c, i) => {
            scoreMap[String(c.id)] = result.scores[i] ?? 0;
          });
          setSemanticScores(scoreMap);
        } catch (e) {
          showError(`Semantic search failed: ${e}`);
          // Fall back to regular search
          setSemanticScores({});
        } finally {
          setSemanticLoading(false);
        }
        return;
      }

      // Clear semantic scores when not in semantic mode
      if (Object.keys(semanticScores).length > 0) {
        setSemanticScores({});
      }

      // Verified/Synthesized are sub-stages of "enriched" — fetch enriched and filter client-side
      const isVirtualStage = status === "verified" || status === "synthesized";
      const dbStatus = isVirtualStage ? "enriched" : status;

      if (hasDrillDown || searchQuery.trim()) {
        const data = await getCompaniesFiltered({
          status: dbStatus === "all" ? undefined : dbStatus,
          subcategory: drillSubcategory || undefined,
          country: drillCountry || undefined,
          search: searchQuery.trim() || drillSearch || undefined,
          limit: isVirtualStage ? 1000 : limit,
          offset: isVirtualStage ? 0 : offset,
        });
        if (isVirtualStage) {
          const filtered = filterByVirtualStage(data, status);
          setCompanies(filtered.slice(offset, offset + limit));
          setTotalCount(filtered.length);
        } else {
          setCompanies(data);
        }
      } else {
        const s = dbStatus === "all" ? undefined : dbStatus;
        const data = await getCompanies(s, isVirtualStage ? 1000 : limit, isVirtualStage ? 0 : offset);
        if (isVirtualStage) {
          const filtered = filterByVirtualStage(data, status);
          setCompanies(filtered.slice(offset, offset + limit));
          setTotalCount(filtered.length);
        } else {
          setCompanies(data);
        }
      }
      // Fetch total count for pagination (skip for virtual stages — already set above)
      if (!isVirtualStage) {
        try {
          const count = await getCompaniesCount(dbStatus === "all" ? undefined : dbStatus);
          setTotalCount(count);
        } catch {
          // getCompaniesCount may not be available yet
        }
      }
    } catch (e) {
      showError(`Failed to load companies: ${e}`);
    }
  }

  async function handleApprove(id: string) {
    try {
      await updateCompanyStatus(id, "approved");
      loadCompanies(filter);
      loadCounts();
      setSelected((prev) => prev && String(prev.id) === id ? { ...prev, status: "approved" } : prev);
    } catch (e) {
      showError(`Failed to approve: ${e}`);
    }
  }

  async function handleUnapprove(id: string) {
    try {
      await updateCompanyStatus(id, "enriched");
      loadCompanies(filter);
      loadCounts();
      setSelected((prev) => prev && String(prev.id) === id ? { ...prev, status: "enriched" } : prev);
    } catch (e) {
      showError(`Failed to unapprove: ${e}`);
    }
  }

  async function handlePushSingle(id: string) {
    try {
      setPushing(id);
      await pushSingleCompany(id);
      loadCompanies(filter);
      loadCounts();
      setSelected((prev) => prev && String(prev.id) === id ? { ...prev, status: "pushed" } : prev);
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
    try {
      await updateCompanyStatus(id, "rejected");
      loadCompanies(filter);
      loadCounts();
      if (selected && String(selected.id) === id) setSelected(null);
    } catch (e) {
      showError(`Failed to reject: ${e}`);
    }
  }

  // Intentionally enrich-only: runs enrichment on Discovered tab companies.
  // For the full pipeline (research + enrich + deep_enrich + verify + synthesize + ...),
  // use the Pipeline page or Command Palette "Start Full Pipeline".
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
      // Stop any running pipeline first, then wait for it to finish
      const status = await getPipelineStatus();
      if (status.running) {
        await stopPipeline();
        for (let i = 0; i < 30; i++) {
          const s = await getPipelineStatus();
          if (!s.running) break;
          if (i === 29) throw new Error("Previous pipeline did not stop in time");
          await new Promise((r) => setTimeout(r, 1000));
        }
      }
      await startPipeline(["push"]);
    } catch (e) {
      showError(`Failed to start push: ${e}`);
      setEnriching(false);
    }
  }

  function handleResetErrors() {
    setConfirmDialog({
      title: "Retry All Errors",
      message: `Reset ${counts.error} error companies back to "discovered" so they can be re-processed by the pipeline?`,
      confirmLabel: "Retry All",
      onConfirm: async () => {
        setConfirmDialog(null);
        try {
          await resetErrorCompanies();
          loadCompanies(filter);
          loadCounts();
          setSelected(null);
        } catch (e) {
          showError(`Failed to reset errors: ${e}`);
        }
      },
    });
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
    { key: "verified", label: "Verified" },
    { key: "synthesized", label: "Synthesized" },
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

  // Clamp selectedIndex when companies list shrinks
  useEffect(() => {
    if (selectedIndex >= companies.length && companies.length > 0) {
      setSelectedIndex(companies.length - 1);
    }
  }, [companies.length]);

  // Sync selected with fresh data when companies array updates
  useEffect(() => {
    if (!selected) return;
    const fresh = companies.find((c) => String(c.id) === String(selected.id));
    if (fresh) {
      setSelected(fresh);
    }
  }, [companies]);

  // Reset detail state when selected company changes
  const selectedId = selected ? String(selected.id) : null;
  useEffect(() => {
    setExpandedExcerpts(new Set());
    setDetailTab("overview");
    setProcessCapOpen(true);
    setIntelData(null);
    setVerificationData(null);
  }, [selectedId]);

  // Load intel data on tab click
  useEffect(() => {
    if (detailTab !== "intelligence" || !selectedId) return;
    if (intelData) return; // already loaded
    setIntelLoading(true);
    getCompanyIntel(selectedId)
      .then((data) => setIntelData(data))
      .catch(() => setIntelData(null))
      .finally(() => setIntelLoading(false));
  }, [detailTab, selectedId]); // eslint-disable-line react-hooks/exhaustive-deps

  // Load verification data on tab click
  useEffect(() => {
    if (detailTab !== "verification" || !selectedId) return;
    if (verificationData) return; // already loaded
    setVerificationLoading(true);
    getCompanyVerification(selectedId)
      .then((data) => setVerificationData(data))
      .catch(() => setVerificationData(null))
      .finally(() => setVerificationLoading(false));
  }, [detailTab, selectedId]); // eslint-disable-line react-hooks/exhaustive-deps

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Review Queue</h1>
          <p className="text-sm text-gray-500 mt-1">
            Review all companies across every pipeline stage
            {activeProfileName && (
              <span className="ml-2 text-forge-600 font-medium">(Showing: {activeProfileName})</span>
            )}
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
                title="Enrichment only — processes discovered companies. Use Pipeline page for the full pipeline."
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
              Retry All Errors
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
              {enrichProgress.total != null && enrichProgress.total > 0 && (
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
            ) : enrichProgress.total != null && enrichProgress.total > 0 ? (
              <div
                className="bg-forge-500 h-2 rounded-full transition-all duration-500 ease-out"
                style={{
                  width: `${Math.min(100, ((enrichProgress.enriched + enrichProgress.errors) / enrichProgress.total) * 100)}%`,
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
              {/* Semantic search toggle */}
              <button
                onClick={() => {
                  setSemanticMode(!semanticMode);
                  if (!semanticMode && searchQuery.trim()) {
                    // Switching to semantic — trigger search
                    setPage(0);
                    setTimeout(() => loadCompanies(filter), 0);
                  } else if (semanticMode) {
                    // Switching off semantic — revert to LIKE search
                    setSemanticScores({});
                    setPage(0);
                    setTimeout(() => loadCompanies(filter), 0);
                  }
                }}
                title={semanticMode ? "Semantic search ON (AI-powered)" : "Enable semantic search"}
                className={`flex items-center gap-1 px-2.5 py-1.5 text-xs font-medium rounded-lg border transition-colors shrink-0 ${
                  semanticMode
                    ? "bg-purple-100 text-purple-700 border-purple-300"
                    : "bg-white text-gray-600 border-gray-200 hover:bg-gray-50"
                }`}
              >
                <Sparkles className="w-3 h-3" />
                {semanticLoading ? "Searching..." : "Semantic"}
              </button>
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
                      {/* Semantic match score */}
                      {semanticScores[companyId] != null && (
                        <span
                          className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                            semanticScores[companyId] >= 0.8
                              ? "bg-green-100 text-green-700"
                              : semanticScores[companyId] >= 0.6
                                ? "bg-yellow-100 text-yellow-700"
                                : "bg-gray-100 text-gray-500"
                          }`}
                          title={`Semantic similarity: ${(semanticScores[companyId] * 100).toFixed(1)}%`}
                        >
                          {Math.round(semanticScores[companyId] * 100)}%
                        </span>
                      )}
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
              <div className="flex gap-1 border-b border-gray-100 pb-2 flex-wrap">
                {(["overview", "capabilities", "intelligence", "synthesis", "verification", "contact", "raw"] as const).map((tab) => (
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
                  {/* Recent Activity */}
                  <div>
                    <div className="flex items-center gap-2 mb-2">
                      <Newspaper className="w-4 h-4 text-blue-500" />
                      <h4 className="font-medium text-sm text-gray-700">Recent Activity</h4>
                    </div>
                    {activitiesLoading ? (
                      <div className="flex items-center gap-2 text-xs text-gray-400">
                        <Loader2 className="w-3 h-3 animate-spin" />
                        Loading...
                      </div>
                    ) : activities.length === 0 ? (
                      <p className="text-xs text-gray-400">No recent activity found</p>
                    ) : (
                      <div className="space-y-2">
                        {activities.map((item) => (
                          <div key={item.id} className="border border-gray-100 rounded-lg p-2">
                            <div className="flex items-start gap-2">
                              <span className={`inline-block px-1.5 py-0.5 text-[10px] font-medium rounded-full shrink-0 mt-0.5 ${
                                item.activity_type === "funding_round" ? "bg-green-100 text-green-700" :
                                item.activity_type === "contract_win" ? "bg-blue-100 text-blue-700" :
                                item.activity_type === "expansion" ? "bg-purple-100 text-purple-700" :
                                item.activity_type === "key_hire" ? "bg-yellow-100 text-yellow-700" :
                                item.activity_type === "acquisition" ? "bg-red-100 text-red-700" :
                                "bg-gray-100 text-gray-600"
                              }`}>
                                {item.activity_type.replace("_", " ")}
                              </span>
                              <div className="min-w-0 flex-1">
                                <a
                                  href={item.url}
                                  target="_blank"
                                  rel="noopener noreferrer"
                                  className="text-xs font-medium text-blue-600 hover:underline line-clamp-2"
                                >
                                  {item.title}
                                </a>
                                {item.snippet && (
                                  <p className="text-[11px] text-gray-500 mt-0.5 line-clamp-2">{item.snippet}</p>
                                )}
                                <p className="text-[10px] text-gray-400 mt-0.5">
                                  {item.published_at
                                    ? new Date(item.published_at).toLocaleDateString()
                                    : new Date(item.fetched_at).toLocaleDateString()}
                                </p>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                    )}
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

              {/* === INTELLIGENCE TAB === */}
              {detailTab === "intelligence" && (
                <div className="space-y-5">
                  {intelLoading ? (
                    <div className="flex items-center gap-2 text-xs text-gray-400 py-4">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Loading intelligence...
                    </div>
                  ) : !intelData ? (
                    <div className="p-4 text-center text-gray-400 text-sm">
                      No intelligence data available. Run the Intel pipeline stage first.
                    </div>
                  ) : (
                    <>
                      {/* Acquisition Readiness Score */}
                      {intelData.acquisition_readiness_score != null && (
                        <div className="text-center">
                          <div
                            className={`text-4xl font-bold ${
                              Number(intelData.acquisition_readiness_score) > 70
                                ? "text-green-600"
                                : Number(intelData.acquisition_readiness_score) >= 40
                                  ? "text-yellow-600"
                                  : "text-red-600"
                            }`}
                          >
                            {String(intelData.acquisition_readiness_score)}
                          </div>
                          <div className="text-xs text-gray-400 mt-1">Acquisition Readiness Score</div>
                        </div>
                      )}

                      {/* Directors */}
                      {intelData.directors_json && (() => {
                        try {
                          const directors = JSON.parse(String(intelData.directors_json));
                          if (!Array.isArray(directors) || directors.length === 0) return null;
                          return (
                            <div>
                              <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                                <Users className="w-3 h-3" /> Directors
                                <span className="ml-1 px-1.5 py-0.5 rounded-full bg-blue-100 text-blue-700 text-[10px] font-medium">
                                  {directors.length}
                                </span>
                              </h4>
                              <div className="space-y-1.5">
                                {directors.map((d: Record<string, unknown>, i: number) => (
                                  <div key={i} className="flex items-center gap-2 text-sm">
                                    {String(d.name || "") === String(intelData.founder_director_name || "") ? (
                                      <span title="Founder Director"><Crown className="w-3 h-3 text-yellow-500 shrink-0" /></span>
                                    ) : (
                                      <User className="w-3 h-3 text-gray-300 shrink-0" />
                                    )}
                                    <span className="font-medium text-gray-900">{String(d.name || "")}</span>
                                    {!!d.role && <span className="text-xs text-gray-500">{String(d.role)}</span>}
                                    {d.age != null && (
                                      <span className="text-xs text-gray-400">Age {String(d.age)}</span>
                                    )}
                                    {!!d.nationality && (
                                      <span className="text-[10px] text-gray-400">{String(d.nationality)}</span>
                                    )}
                                  </div>
                                ))}
                              </div>
                            </div>
                          );
                        } catch { return null; }
                      })()}

                      {/* Ownership Structure */}
                      {intelData.ownership_structure && (
                        <div>
                          <h4 className="text-xs text-gray-400 uppercase mb-1 flex items-center gap-1">
                            <Building2 className="w-3 h-3" /> Ownership Structure
                          </h4>
                          <span className="inline-block px-2.5 py-1 rounded-full text-xs font-semibold bg-indigo-100 text-indigo-700">
                            {String(intelData.ownership_structure).replace(/_/g, " ")}
                          </span>
                          {intelData.single_owner === 1 && (
                            <p className="text-xs text-gray-500 mt-1">Single owner controls the company</p>
                          )}
                          {intelData.owner_is_director === 1 && (
                            <p className="text-xs text-gray-500">Owner is also a director</p>
                          )}
                        </div>
                      )}

                      {/* PSC (Persons of Significant Control) */}
                      {intelData.psc_json && (() => {
                        try {
                          const pscs = JSON.parse(String(intelData.psc_json));
                          if (!Array.isArray(pscs) || pscs.length === 0) return null;
                          return (
                            <div>
                              <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                                <Lock className="w-3 h-3" /> Persons of Significant Control
                                <span className="ml-1 px-1.5 py-0.5 rounded-full bg-purple-100 text-purple-700 text-[10px] font-medium">
                                  {pscs.length}
                                </span>
                              </h4>
                              <div className="space-y-1.5">
                                {pscs.map((psc: Record<string, unknown>, i: number) => (
                                  <div key={i} className="rounded-lg border border-gray-100 bg-gray-50 p-2">
                                    <p className="text-sm font-medium text-gray-900">{String(psc.name || "")}</p>
                                    {!!psc.natures_of_control && Array.isArray(psc.natures_of_control) && (
                                      <div className="flex flex-wrap gap-1 mt-1">
                                        {(psc.natures_of_control as string[]).map((nature, j) => (
                                          <span key={j} className="px-1.5 py-0.5 rounded text-[10px] bg-purple-50 text-purple-600">
                                            {nature}
                                          </span>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                ))}
                              </div>
                            </div>
                          );
                        } catch { return null; }
                      })()}

                      {/* Succession Signals */}
                      {(intelData.no_young_directors === 1 ||
                        intelData.recent_director_changes === 1 ||
                        intelData.has_company_secretary === 1 ||
                        intelData.years_trading != null) && (
                        <div>
                          <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                            <TrendingUp className="w-3 h-3" /> Succession Signals
                          </h4>
                          <div className="space-y-1 text-xs">
                            {intelData.no_young_directors === 1 && (
                              <div className="flex items-center gap-1.5 text-amber-700">
                                <AlertTriangle className="w-3 h-3" />
                                No young directors in pipeline
                              </div>
                            )}
                            {intelData.recent_director_changes === 1 && (
                              <div className="flex items-center gap-1.5 text-blue-700">
                                <RefreshCw className="w-3 h-3" />
                                Recent director changes detected
                              </div>
                            )}
                            {intelData.has_company_secretary === 1 && (
                              <div className="flex items-center gap-1.5 text-gray-600">
                                <ShieldCheck className="w-3 h-3" />
                                Has company secretary
                              </div>
                            )}
                            {intelData.years_trading != null && (
                              <div className="flex items-center gap-1.5 text-gray-600">
                                <Clock className="w-3 h-3" />
                                {String(intelData.years_trading)} years trading
                              </div>
                            )}
                          </div>
                        </div>
                      )}

                      {/* Financial Health */}
                      {(intelData.company_status || intelData.accounts_type || intelData.has_insolvency_history === 1) && (
                        <div>
                          <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                            <HeartPulse className="w-3 h-3" /> Financial Health
                          </h4>
                          <div className="grid grid-cols-2 gap-3 text-xs">
                            <DetailField label="Company Status" value={intelData.company_status as string} />
                            <DetailField label="Accounts Type" value={intelData.accounts_type as string} />
                            <DetailField label="Last Accounts" value={intelData.last_accounts_date as string} />
                          </div>
                          <div className="mt-2 space-y-1 text-xs">
                            {intelData.has_insolvency_history === 1 && (
                              <div className="flex items-center gap-1 text-red-600">
                                <AlertTriangle className="w-3 h-3" />
                                Insolvency history
                              </div>
                            )}
                            {intelData.has_charges === 1 && (
                              <div className="flex items-center gap-1 text-amber-600">
                                <AlertTriangle className="w-3 h-3" />
                                Has secured charges
                              </div>
                            )}
                            {intelData.accounts_overdue === 1 && (
                              <div className="flex items-center gap-1 text-red-600">
                                <AlertTriangle className="w-3 h-3" />
                                Accounts overdue
                              </div>
                            )}
                          </div>
                        </div>
                      )}

                      {/* Acquisition Signals */}
                      {intelData.acquisition_signals_json && (() => {
                        try {
                          const signals = JSON.parse(String(intelData.acquisition_signals_json));
                          if (!Array.isArray(signals) || signals.length === 0) return null;
                          return (
                            <div>
                              <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                                <Target className="w-3 h-3" /> Acquisition Signals
                              </h4>
                              <div className="space-y-1">
                                {signals.map((signal: string, i: number) => (
                                  <div key={i} className="flex items-start gap-1.5 text-xs text-gray-700">
                                    <span className="text-forge-500 mt-0.5 shrink-0">&bull;</span>
                                    {signal}
                                  </div>
                                ))}
                              </div>
                            </div>
                          );
                        } catch { return null; }
                      })()}

                      {/* Source info */}
                      {!!(intelData.age_source || intelData.ch_fetched_at) && (
                        <div className="text-[10px] text-gray-400 pt-2 border-t border-gray-100">
                          {!!intelData.age_source && <span>Age source: {String(intelData.age_source)}</span>}
                          {!!intelData.ch_fetched_at && <span className="ml-3">CH fetched: {String(intelData.ch_fetched_at)}</span>}
                        </div>
                      )}
                    </>
                  )}
                </div>
              )}

              {/* === SYNTHESIS TAB === */}
              {detailTab === "synthesis" && (
                <div className="space-y-5">
                  {/* Public Synthesis */}
                  {(() => {
                    const publicRaw = selected.synthesis_public_json;
                    if (!publicRaw) return (
                      <div className="p-4 text-center text-gray-400 text-sm">
                        No synthesis data available. Run the Synthesis pipeline stage first.
                      </div>
                    );
                    try {
                      const pub_ = JSON.parse(String(publicRaw)) as Record<string, unknown>;
                      return (
                        <>
                          {/* Public section */}
                          <div className="rounded-lg border-2 border-blue-200 bg-blue-50/50 p-4 space-y-4">
                            <h4 className="text-xs font-semibold text-blue-700 uppercase flex items-center gap-1">
                              <Eye className="w-3 h-3" /> Public Synthesis
                            </h4>

                            {/* Capability summary */}
                            {!!pub_.capability_summary && (
                              <div>
                                <p className="text-xs text-gray-400 mb-0.5">Capability Summary</p>
                                <p className="text-sm text-gray-700">{String(pub_.capability_summary)}</p>
                              </div>
                            )}

                            {/* Marketplace tags */}
                            {Array.isArray(pub_.marketplace_tags) && (pub_.marketplace_tags as unknown[]).length > 0 && (
                              <div>
                                <p className="text-xs text-gray-400 mb-1">Marketplace Tags</p>
                                <TagPills items={pub_.marketplace_tags as string[]} color="bg-blue-100 text-blue-700" />
                              </div>
                            )}

                            {/* Competitive positioning */}
                            {!!pub_.competitive_positioning && typeof pub_.competitive_positioning === "object" && (
                              <div>
                                <p className="text-xs text-gray-400 mb-1">Competitive Positioning</p>
                                <div className="grid grid-cols-2 gap-2 text-xs">
                                  {Object.entries(pub_.competitive_positioning as Record<string, unknown>).map(([k, v]) => (
                                    <DetailField key={k} label={k.replace(/_/g, " ")} value={String(v)} />
                                  ))}
                                </div>
                              </div>
                            )}

                            {/* Search keywords */}
                            {Array.isArray(pub_.search_keywords) && (pub_.search_keywords as unknown[]).length > 0 && (
                              <div>
                                <p className="text-xs text-gray-400 mb-1">Search Keywords</p>
                                <TagPills items={pub_.search_keywords as string[]} color="bg-gray-100 text-gray-600" />
                              </div>
                            )}

                            {/* Ideal buyer profile */}
                            {!!pub_.ideal_buyer_profile && (
                              <div>
                                <p className="text-xs text-gray-400 mb-0.5">Ideal Buyer Profile</p>
                                <p className="text-sm text-gray-700">{String(pub_.ideal_buyer_profile)}</p>
                              </div>
                            )}

                            {/* Data quality assessment */}
                            {!!pub_.data_quality_grade && (
                              <div>
                                <p className="text-xs text-gray-400 mb-0.5">Data Quality</p>
                                <span className={`inline-block px-2 py-0.5 rounded-full text-xs font-semibold ${
                                  String(pub_.data_quality_grade).toUpperCase().startsWith("A") ? "bg-green-100 text-green-700" :
                                  String(pub_.data_quality_grade).toUpperCase().startsWith("B") ? "bg-yellow-100 text-yellow-700" :
                                  "bg-red-100 text-red-700"
                                }`}>
                                  {String(pub_.data_quality_grade)}
                                </span>
                              </div>
                            )}
                          </div>

                          {/* Private Intelligence */}
                          {!!selected.synthesis_private_json && (() => {
                            try {
                              const priv = JSON.parse(String(selected.synthesis_private_json)) as Record<string, unknown>;
                              return (
                                <div className="rounded-lg border-2 border-red-200 bg-red-50/50 p-4 space-y-4">
                                  <div className="flex items-center gap-2">
                                    <h4 className="text-xs font-semibold text-red-700 uppercase flex items-center gap-1">
                                      <Lock className="w-3 h-3" /> Private Intelligence
                                    </h4>
                                    <span className="px-1.5 py-0.5 rounded text-[9px] font-bold bg-red-200 text-red-800 tracking-wider">
                                      CONFIDENTIAL
                                    </span>
                                  </div>

                                  {!!priv.growth_trajectory && (
                                    <div>
                                      <p className="text-xs text-gray-400 mb-0.5">Growth Trajectory</p>
                                      <p className="text-sm text-gray-700">{String(priv.growth_trajectory)}</p>
                                    </div>
                                  )}

                                  {!!priv.fractional_executive_needs && (
                                    <div>
                                      <p className="text-xs text-gray-400 mb-0.5">Fractional Executive Needs</p>
                                      <p className="text-sm text-gray-700">{String(priv.fractional_executive_needs)}</p>
                                    </div>
                                  )}

                                  {!!priv.acquisition_fit_analysis && (
                                    <div>
                                      <p className="text-xs text-gray-400 mb-0.5">Acquisition Fit Analysis</p>
                                      <p className="text-sm text-gray-700">{String(priv.acquisition_fit_analysis)}</p>
                                    </div>
                                  )}

                                  {!!priv.approach_strategy && (
                                    <div>
                                      <p className="text-xs text-gray-400 mb-0.5">Approach Strategy</p>
                                      <p className="text-sm text-gray-700">{String(priv.approach_strategy)}</p>
                                    </div>
                                  )}

                                  {/* Show any other string fields not covered above */}
                                  {Object.entries(priv)
                                    .filter(([k]) => !["growth_trajectory", "fractional_executive_needs", "acquisition_fit_analysis", "approach_strategy"].includes(k))
                                    .filter(([, v]) => typeof v === "string" && v)
                                    .map(([k, v]) => (
                                      <div key={k}>
                                        <p className="text-xs text-gray-400 mb-0.5">{k.replace(/_/g, " ")}</p>
                                        <p className="text-sm text-gray-700">{String(v)}</p>
                                      </div>
                                    ))}
                                </div>
                              );
                            } catch { return null; }
                          })()}
                        </>
                      );
                    } catch {
                      return (
                        <div className="p-4 text-center text-red-400 text-sm">
                          Failed to parse synthesis data.
                        </div>
                      );
                    }
                  })()}
                </div>
              )}

              {/* === VERIFICATION TAB === */}
              {detailTab === "verification" && (
                <div className="space-y-5">
                  {verificationLoading ? (
                    <div className="flex items-center gap-2 text-xs text-gray-400 py-4">
                      <Loader2 className="w-4 h-4 animate-spin" />
                      Loading verification data...
                    </div>
                  ) : !verificationData ? (
                    <div className="p-4 text-center text-gray-400 text-sm">
                      No verification data available.
                    </div>
                  ) : (
                    <>
                      {/* Verified at */}
                      {verificationData.verified_v2_at ? (
                        <div className="flex items-center gap-2">
                          <ShieldCheck className="w-4 h-4 text-green-600" />
                          <span className="text-sm font-medium text-green-700">Verified</span>
                          <span className="text-xs text-gray-400">
                            {new Date(String(verificationData.verified_v2_at)).toLocaleString()}
                          </span>
                        </div>
                      ) : (
                        <div className="flex items-center gap-2">
                          <AlertTriangle className="w-4 h-4 text-gray-400" />
                          <span className="text-sm text-gray-500">Not yet verified</span>
                        </div>
                      )}

                      {/* Corrections Applied */}
                      {verificationData.verification_changes_json && (() => {
                        try {
                          const changes = JSON.parse(String(verificationData.verification_changes_json));
                          if (!changes || (Array.isArray(changes) && changes.length === 0)) return null;
                          const changeList = Array.isArray(changes) ? changes : typeof changes === "object" ? Object.entries(changes) : [];
                          if (changeList.length === 0) return null;
                          return (
                            <div>
                              <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                                <FileSearch className="w-3 h-3" /> Corrections Applied
                              </h4>
                              <div className="space-y-1.5">
                                {Array.isArray(changes) ? (
                                  changes.map((change: Record<string, unknown>, i: number) => (
                                    <div key={i} className="rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                                      <span className="font-medium text-gray-700">{String(change.field || change.key || "")}: </span>
                                      {change.old != null && (
                                        <span className="text-red-500 line-through mr-1">{String(change.old)}</span>
                                      )}
                                      {change.new != null && (
                                        <span className="text-green-600">{String(change.new)}</span>
                                      )}
                                      {!!change.reason && (
                                        <p className="text-[10px] text-gray-400 mt-0.5">{String(change.reason)}</p>
                                      )}
                                    </div>
                                  ))
                                ) : (
                                  Object.entries(changes as Record<string, unknown>).map(([field, val]) => (
                                    <div key={field} className="rounded-lg border border-gray-100 bg-gray-50 p-2 text-xs">
                                      <span className="font-medium text-gray-700">{field}: </span>
                                      <span className="text-green-600">{String(val)}</span>
                                    </div>
                                  ))
                                )}
                              </div>
                            </div>
                          );
                        } catch { return null; }
                      })()}

                      {/* Fractional Signals */}
                      {!!verificationData.fractional_signals_json && (() => {
                        try {
                          const signals = JSON.parse(String(verificationData.fractional_signals_json));
                          if (!signals || typeof signals !== "object") return null;
                          return (
                            <div>
                              <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                                <Brain className="w-3 h-3" /> Fractional Signals
                              </h4>
                              <div className="space-y-3">
                                {/* Ownership analysis */}
                                {!!signals.ownership_analysis && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-0.5">Ownership Analysis</p>
                                    <p className="text-sm text-gray-700">{String(signals.ownership_analysis)}</p>
                                  </div>
                                )}

                                {/* Missing roles */}
                                {Array.isArray(signals.missing_roles) && (signals.missing_roles as unknown[]).length > 0 && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-1">Missing Roles</p>
                                    <TagPills items={signals.missing_roles as string[]} color="bg-amber-50 text-amber-700" />
                                  </div>
                                )}

                                {/* Needs assessment */}
                                {!!signals.needs_assessment && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-0.5">Needs Assessment</p>
                                    <p className="text-sm text-gray-700">{String(signals.needs_assessment)}</p>
                                  </div>
                                )}

                                {/* Extracted people */}
                                {Array.isArray(signals.extracted_people) && (signals.extracted_people as unknown[]).length > 0 && (
                                  <div>
                                    <h4 className="text-xs text-gray-400 uppercase mb-2 flex items-center gap-1">
                                      <Users className="w-3 h-3" /> Extracted People
                                    </h4>
                                    <div className="space-y-1">
                                      {(signals.extracted_people as { name: string; title?: string }[]).map((person, i) => (
                                        <div key={i} className="flex items-center gap-2 text-sm">
                                          <User className="w-3 h-3 text-gray-300 shrink-0" />
                                          <span className="font-medium text-gray-900">{person.name}</span>
                                          {person.title && <span className="text-xs text-gray-500">{person.title}</span>}
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                )}

                                {/* Equipment mentioned */}
                                {Array.isArray(signals.equipment_mentioned) && (signals.equipment_mentioned as unknown[]).length > 0 && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-1">Equipment Mentioned</p>
                                    <TagPills items={signals.equipment_mentioned as string[]} color="bg-teal-50 text-teal-700" />
                                  </div>
                                )}

                                {/* Case studies */}
                                {Array.isArray(signals.case_studies) && (signals.case_studies as unknown[]).length > 0 && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-1">Case Studies</p>
                                    <div className="space-y-1.5">
                                      {(signals.case_studies as Record<string, unknown>[]).map((cs, i) => (
                                        <div key={i} className="rounded-lg border border-gray-100 bg-gray-50 p-2">
                                          <p className="text-xs font-medium text-gray-900">{String(cs.title || cs.name || "")}</p>
                                          {!!cs.description && (
                                            <p className="text-[11px] text-gray-500 mt-0.5">{String(cs.description)}</p>
                                          )}
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                )}

                                {/* Named clients */}
                                {Array.isArray(signals.named_clients) && (signals.named_clients as unknown[]).length > 0 && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-1">Named Clients</p>
                                    <TagPills items={signals.named_clients as string[]} color="bg-purple-50 text-purple-700" />
                                  </div>
                                )}

                                {/* Confidence scores */}
                                {!!signals.confidence_scores && typeof signals.confidence_scores === "object" && (
                                  <div>
                                    <p className="text-xs text-gray-400 mb-2">Confidence Scores</p>
                                    <div className="space-y-2">
                                      {Object.entries(signals.confidence_scores as Record<string, number>).map(([field, score]) => (
                                        <div key={field}>
                                          <div className="flex justify-between text-[10px] text-gray-500 mb-0.5">
                                            <span>{field.replace(/_/g, " ")}</span>
                                            <span>{typeof score === "number" ? Math.round(score * 100) : score}%</span>
                                          </div>
                                          <div className="w-full bg-gray-100 rounded-full h-1.5">
                                            <div
                                              className={`h-1.5 rounded-full ${
                                                (typeof score === "number" ? score : 0) >= 0.8 ? "bg-green-500" :
                                                (typeof score === "number" ? score : 0) >= 0.5 ? "bg-yellow-500" :
                                                "bg-red-500"
                                              }`}
                                              style={{ width: `${typeof score === "number" ? Math.min(100, score * 100) : 0}%` }}
                                            />
                                          </div>
                                        </div>
                                      ))}
                                    </div>
                                  </div>
                                )}

                                {/* Show any other string fields */}
                                {Object.entries(signals as Record<string, unknown>)
                                  .filter(([k]) => !["ownership_analysis", "missing_roles", "needs_assessment", "extracted_people", "equipment_mentioned", "case_studies", "named_clients", "confidence_scores"].includes(k))
                                  .filter(([, v]) => typeof v === "string" && v)
                                  .map(([k, v]) => (
                                    <div key={k}>
                                      <p className="text-xs text-gray-400 mb-0.5">{k.replace(/_/g, " ")}</p>
                                      <p className="text-sm text-gray-700">{String(v)}</p>
                                    </div>
                                  ))}
                              </div>
                            </div>
                          );
                        } catch { return null; }
                      })()}

                      {/* If no data at all */}
                      {!verificationData.verified_v2_at &&
                        !verificationData.verification_changes_json &&
                        !verificationData.fractional_signals_json && (
                        <div className="p-4 text-center text-gray-400 text-sm">
                          No verification data available. Run the Verification pipeline stage first.
                        </div>
                      )}
                    </>
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
                    title="Enrichment only — processes discovered companies. Use Pipeline page for the full pipeline."
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
                      onClick={() => {
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
                      }}
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
