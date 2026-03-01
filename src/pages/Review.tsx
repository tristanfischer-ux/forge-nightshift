import { useEffect, useState } from "react";
import {
  CheckCircle,
  XCircle,
  ChevronRight,
  Star,
  Building2,
} from "lucide-react";
import { getCompanies, updateCompanyStatus } from "../lib/tauri";

export default function Review() {
  const [companies, setCompanies] = useState<Record<string, unknown>[]>([]);
  const [selected, setSelected] = useState<Record<string, unknown> | null>(
    null
  );

  useEffect(() => {
    loadCompanies();
  }, []);

  async function loadCompanies() {
    try {
      const data = await getCompanies("enriched", 100, 0);
      setCompanies(data);
    } catch {
      // DB may not be ready
    }
  }

  async function handleApprove(id: string) {
    await updateCompanyStatus(id, "approved");
    setCompanies((prev) => prev.filter((c) => c.id !== id));
    if (selected?.id === id) setSelected(null);
  }

  async function handleReject(id: string) {
    await updateCompanyStatus(id, "rejected");
    setCompanies((prev) => prev.filter((c) => c.id !== id));
    if (selected?.id === id) setSelected(null);
  }

  async function handleBulkApprove() {
    for (const company of companies) {
      const score = Number(company.relevance_score) || 0;
      if (score >= 60) {
        await updateCompanyStatus(String(company.id), "approved");
      }
    }
    loadCompanies();
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Review Queue</h1>
          <p className="text-sm text-gray-500 mt-1">
            Approve or reject enriched companies before pushing to ForgeOS
          </p>
        </div>

        {companies.length > 0 && (
          <button
            onClick={handleBulkApprove}
            className="flex items-center gap-2 px-4 py-2 bg-green-600 hover:bg-green-700 rounded-lg text-sm font-medium text-white transition-colors"
          >
            <CheckCircle className="w-4 h-4" />
            Approve All 60+
          </button>
        )}
      </div>

      <div className="flex gap-4">
        {/* Company list */}
        <div className="flex-1 bg-white rounded-xl border border-gray-200 shadow-sm">
          <div className="p-4 border-b border-gray-200">
            <h2 className="text-sm font-semibold text-gray-900">
              Pending Review ({companies.length})
            </h2>
          </div>
          <div className="divide-y divide-gray-100 max-h-[calc(100vh-220px)] overflow-y-auto">
            {companies.length === 0 ? (
              <div className="p-8 text-center text-gray-400 text-sm">
                No companies awaiting review. Run enrichment first.
              </div>
            ) : (
              companies.map((company) => (
                <div
                  key={String(company.id)}
                  className={`flex items-center gap-3 px-4 py-3 cursor-pointer transition-colors ${
                    selected?.id === company.id
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
                      {String(company.country || "")} &middot;{" "}
                      {String(company.subcategory || "")}
                    </p>
                  </div>
                  <div className="flex items-center gap-2">
                    <div className="flex items-center gap-1">
                      <Star className="w-3 h-3 text-yellow-500" />
                      <span className="text-xs font-medium text-gray-700">
                        {String(company.relevance_score || 0)}
                      </span>
                    </div>
                    <ChevronRight className="w-4 h-4 text-gray-300" />
                  </div>
                </div>
              ))
            )}
          </div>
        </div>

        {/* Detail panel */}
        {selected && (
          <div className="w-96 bg-white rounded-xl border border-gray-200 p-4 space-y-4 shadow-sm">
            <div>
              <h3 className="text-lg font-semibold text-gray-900">
                {String(selected.name || "")}
              </h3>
              <p className="text-sm text-gray-500">
                {String(selected.domain || "")}
              </p>
            </div>

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

            <div>
              <h4 className="text-xs text-gray-400 uppercase mb-1">
                Description
              </h4>
              <p className="text-sm text-gray-700">
                {String(selected.description || "No description available")}
              </p>
            </div>

            <div className="grid grid-cols-2 gap-3">
              <div>
                <h4 className="text-xs text-gray-400 uppercase mb-1">
                  Category
                </h4>
                <p className="text-sm text-gray-700">
                  {String(selected.category || "—")}
                </p>
              </div>
              <div>
                <h4 className="text-xs text-gray-400 uppercase mb-1">
                  Subcategory
                </h4>
                <p className="text-sm text-gray-700">
                  {String(selected.subcategory || "—")}
                </p>
              </div>
              <div>
                <h4 className="text-xs text-gray-400 uppercase mb-1">
                  Contact
                </h4>
                <p className="text-sm text-gray-700">
                  {String(selected.contact_name || "—")}
                </p>
              </div>
              <div>
                <h4 className="text-xs text-gray-400 uppercase mb-1">
                  Email
                </h4>
                <p className="text-sm text-gray-700 truncate">
                  {String(selected.contact_email || "—")}
                </p>
              </div>
            </div>

            <div className="flex gap-2 pt-2">
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
            </div>
          </div>
        )}
      </div>
    </div>
  );
}
