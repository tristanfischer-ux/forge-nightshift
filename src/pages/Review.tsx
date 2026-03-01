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
} from "lucide-react";
import { listen } from "@tauri-apps/api/event";
import {
  getCompanies,
  getCompaniesFiltered,
  updateCompanyStatus,
  startPipeline,
  resetErrorCompanies,
  getPipelineStatus,
  approveAllEnriched,
} from "../lib/tauri";

const COUNTRIES: Record<string, string> = {
  DE: "Germany",
  FR: "France",
  NL: "Netherlands",
  BE: "Belgium",
  IT: "Italy",
  GB: "United Kingdom",
};

type StatusFilter = "all" | "discovered" | "enriched" | "error";

const STATUS_BADGE: Record<string, string> = {
  discovered: "bg-blue-100 text-blue-700",
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
    error: 0,
  });
  const [enriching, setEnriching] = useState(false);

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
        loadCompanies(filter);
        loadCounts();
      }
    });
    return () => { unlisten.then((fn) => fn()); };
  }, [filter]);

  // Poll for updates while enriching
  useEffect(() => {
    if (!enriching) return;
    const interval = setInterval(() => {
      loadCompanies(filter);
      loadCounts();
    }, 3000);
    return () => clearInterval(interval);
  }, [enriching, filter]);

  async function loadCounts() {
    try {
      const all = await getCompanies(undefined, 1000, 0);
      const c = { all: all.length, discovered: 0, enriched: 0, error: 0 };
      for (const co of all) {
        const s = String(co.status || "");
        if (s === "discovered") c.discovered++;
        else if (s === "enriched" || s === "approved") c.enriched++;
        else if (s === "error") c.error++;
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
          limit: 500,
          offset: 0,
        });
        setCompanies(data);
      } else {
        const s = status === "all" ? undefined : status;
        const data = await getCompanies(s, 500, 0);
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

  const tabs: { key: StatusFilter; label: string }[] = [
    { key: "all", label: "All" },
    { key: "discovered", label: "Discovered" },
    { key: "enriched", label: "Enriched" },
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
          {filter === "discovered" && companies.length > 0 && (
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
        </div>
      </div>

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
                attrs.nightshift_score ||
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
                        label="Nightshift Score"
                        value={attrs.nightshift_score as string}
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
              </div>
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
