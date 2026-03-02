import { useEffect, useState } from "react";
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
} from "../lib/tauri";

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
    currentCompany: string;
    currentIndex: number;
    enriched: number;
    errors: number;
    total: number;
    model: string;
  } | null>(null);
  const [cancelling, setCancelling] = useState(false);
  const [pushing, setPushing] = useState<string | null>(null);
  const [removing, setRemoving] = useState<string | null>(null);
  const [removingAll, setRemovingAll] = useState(false);
  const [auditing, setAuditing] = useState(false);
  const [auditResult, setAuditResult] = useState<{
    fetched: number;
    imported: number;
    skipped: number;
  } | null>(null);

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
  }, [filter, drillSubcategory, drillCountry, drillSearch]);

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
        setCancelling(false);
        loadCompanies(filter);
        loadCounts();
      }
    });
    return () => { unlisten.then((fn) => fn()); };
  }, [filter]);

  // Listen for per-company progress events instead of polling
  useEffect(() => {
    const unlisten = listen<{
      stage: string;
      phase: string;
      current_company: string;
      current_index: number;
      enriched: number;
      errors: number;
      total: number;
      model?: string;
    }>("pipeline:progress", (event) => {
      const p = event.payload;
      if (p.stage !== "enrich") return;
      setEnrichProgress({
        currentCompany: p.current_company,
        currentIndex: p.current_index,
        enriched: p.enriched,
        errors: p.errors,
        total: p.total,
        model: p.model || "",
      });
      // Refresh list on each company event so badges update live
      loadCompanies(filter);
      loadCounts();
    });
    return () => { unlisten.then((fn) => fn()); };
  }, [filter]);

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
    } catch {
      // ignore
    }
  }

  async function loadCompanies(status: StatusFilter) {
    try {
      if (hasDrillDown) {
        const data = await getCompaniesFiltered({
          status: status === "all" ? undefined : status,
          subcategory: drillSubcategory || undefined,
          country: drillCountry || undefined,
          search: drillSearch || undefined,
          limit: 2000,
          offset: 0,
        });
        setCompanies(data);
      } else {
        const s = status === "all" ? undefined : status;
        const data = await getCompanies(s, 2000, 0);
        setCompanies(data);
      }
    } catch {
      // DB may not be ready
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
      alert(String(e));
    } finally {
      setPushing(null);
    }
  }

  async function handleRemoveFromMarketplace(id: string) {
    if (!confirm("Remove this company from the ForgeOS marketplace? This cannot be undone.")) return;
    try {
      setRemoving(id);
      await removeFromMarketplace(id);
      if (selected && String(selected.id) === id) setSelected(null);
      loadCompanies(filter);
      loadCounts();
    } catch (e) {
      alert(String(e));
    } finally {
      setRemoving(null);
    }
  }

  async function handleRemoveAllFromMarketplace() {
    const ids = companies
      .filter((c) => Boolean(c.supabase_listing_id))
      .map((c) => String(c.id));
    if (ids.length === 0) {
      alert("No companies with marketplace listings to remove.");
      return;
    }
    if (!confirm(`Remove ${ids.length} companies from the ForgeOS marketplace? This cannot be undone.`)) return;
    try {
      setRemovingAll(true);
      const result = await removeAllFromMarketplace(ids);
      alert(`Removed ${result.removed} listings from marketplace${result.errors > 0 ? ` (${result.errors} errors)` : ""}`);
      setSelected(null);
      loadCompanies(filter);
      loadCounts();
    } catch (e) {
      alert(String(e));
    } finally {
      setRemovingAll(false);
    }
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
    } catch {
      setEnriching(false);
    }
  }

  async function handleStop() {
    try {
      setCancelling(true);
      await stopPipeline();
    } catch {
      setCancelling(false);
    }
  }

  async function handlePushToForgeOS() {
    try {
      setEnriching(true);
      await startPipeline(["push"]);
    } catch {
      setEnriching(false);
    }
  }

  async function handleResetErrors() {
    try {
      await resetErrorCompanies();
      loadCompanies(filter);
      loadCounts();
      setSelected(null);
    } catch {
      // handled elsewhere
    }
  }

  async function handleBulkApprove() {
    try {
      await approveAllEnriched();
      loadCompanies(filter);
      loadCounts();
    } catch {
      // handled elsewhere
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
    } catch {
      // handled elsewhere
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
              {enriching ? "Pushing..." : "Push to ForgeOS"}
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
                Enriching: {enrichProgress.currentCompany}
                {enrichProgress.model && (
                  <span className="text-gray-400 font-normal ml-1">
                    ({enrichProgress.model})
                  </span>
                )}
              </span>
            </div>
            <div className="flex items-center gap-3 shrink-0 text-xs text-gray-500">
              <span>
                {enrichProgress.currentIndex + 1} of {enrichProgress.total}
              </span>
              {enrichProgress.errors > 0 && (
                <span className="text-red-600">
                  {enrichProgress.errors} error{enrichProgress.errors !== 1 ? "s" : ""}
                </span>
              )}
              <span className="font-medium text-gray-700">
                {Math.round(((enrichProgress.currentIndex + 1) / enrichProgress.total) * 100)}%
              </span>
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
              className="bg-forge-500 h-2 rounded-full transition-all duration-500 ease-out"
              style={{
                width: `${((enrichProgress.currentIndex + 1) / enrichProgress.total) * 100}%`,
              }}
            />
          </div>
        </div>
      )}

      <div className="flex gap-4">
        {/* Company list */}
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          <div className="divide-y divide-gray-100 max-h-[calc(100vh-260px)] overflow-y-auto">
            {companies.length === 0 ? (
              <div className="p-8 text-center text-gray-400 text-sm">
                No companies found with this status.
              </div>
            ) : (
              companies.map((company) => {
                const cStatus = String(company.status || "");
                return (
                  <div
                    key={String(company.id)}
                    className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                      selected && String(selected.id) === String(company.id)
                        ? "bg-blue-50"
                        : "hover:bg-gray-50"
                    }`}
                    onClick={() => setSelected(company)}
                  >
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

              {/* Basic Info */}
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

              {/* Capabilities & Certifications */}
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

              {/* Attributes from attributes_json */}
              {(attrIndustries.length > 0 ||
                industries.length > 0 ||
                attrMaterials.length > 0 ||
                attrEquipment.length > 0 ||
                attrs.founded_year ||
                attrs.company_size ||
                attrs.employees ||
                attrs.employee_count_exact ||
                attrs.production_capacity ||
                selected.company_size ||
                selected.year_founded ||
                attrs.company_number ||
                attrKeyPeople.length > 0 ||
                attrChDirectors.length > 0 ||
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

                    {attrMaterials.length > 0 && (
                      <div>
                        <p className="text-xs text-gray-400 mb-1">Materials</p>
                        <TagPills
                          items={attrMaterials}
                          color="bg-amber-50 text-amber-700"
                        />
                      </div>
                    )}

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
                    </button>
                    <button
                      onClick={() => handleReject(String(selected.id))}
                      className="flex-1 flex items-center justify-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm font-medium text-white transition-colors"
                    >
                      <XCircle className="w-4 h-4" />
                      Reject
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
    </div>
  );
}
