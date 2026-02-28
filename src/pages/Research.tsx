import { useEffect, useState } from "react";
import { Search, Globe, RefreshCw } from "lucide-react";
import { getCompanies, startPipeline } from "../lib/tauri";

const COUNTRIES: Record<string, string> = {
  DE: "Germany",
  FR: "France",
  NL: "Netherlands",
  BE: "Belgium",
  IT: "Italy",
};

export default function Research() {
  const [companies, setCompanies] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadCompanies();
  }, []);

  async function loadCompanies() {
    try {
      const data = await getCompanies("discovered", 100, 0);
      setCompanies(data);
    } catch {
      // DB may not be ready yet
    }
  }

  async function runResearchOnly() {
    setLoading(true);
    try {
      await startPipeline(["research"]);
    } catch {
      // handled by dashboard
    }
    setLoading(false);
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Research</h1>
          <p className="text-sm text-forge-400 mt-1">
            Company discovery across European markets
          </p>
        </div>

        <button
          onClick={runResearchOnly}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 bg-forge-600 hover:bg-forge-700 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
        >
          {loading ? (
            <RefreshCw className="w-4 h-4 animate-spin" />
          ) : (
            <Search className="w-4 h-4" />
          )}
          Run Research
        </button>
      </div>

      {/* Target countries */}
      <div className="bg-forge-900/50 rounded-xl border border-forge-800/50 p-4">
        <h2 className="text-sm font-semibold mb-3">Target Countries</h2>
        <div className="flex gap-2">
          {Object.entries(COUNTRIES).map(([code, name]) => (
            <div
              key={code}
              className="flex items-center gap-2 px-3 py-1.5 bg-forge-800/50 rounded-lg text-sm"
            >
              <Globe className="w-3 h-3 text-forge-400" />
              {name}
            </div>
          ))}
        </div>
      </div>

      {/* Discovered companies table */}
      <div className="bg-forge-900/50 rounded-xl border border-forge-800/50">
        <div className="p-4 border-b border-forge-800/50">
          <h2 className="text-sm font-semibold">
            Discovered Companies ({companies.length})
          </h2>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-forge-800/30">
                <th className="text-left px-4 py-3 text-forge-400 font-medium">
                  Company
                </th>
                <th className="text-left px-4 py-3 text-forge-400 font-medium">
                  Country
                </th>
                <th className="text-left px-4 py-3 text-forge-400 font-medium">
                  Website
                </th>
                <th className="text-left px-4 py-3 text-forge-400 font-medium">
                  Source
                </th>
                <th className="text-left px-4 py-3 text-forge-400 font-medium">
                  Status
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-forge-800/30">
              {companies.length === 0 ? (
                <tr>
                  <td
                    colSpan={5}
                    className="px-4 py-8 text-center text-forge-500"
                  >
                    No companies discovered yet. Run a research pipeline to get
                    started.
                  </td>
                </tr>
              ) : (
                companies.map((company) => (
                  <tr
                    key={String(company.id)}
                    className="hover:bg-forge-800/20"
                  >
                    <td className="px-4 py-3 font-medium">
                      {String(company.name || "")}
                    </td>
                    <td className="px-4 py-3 text-forge-300">
                      {COUNTRIES[String(company.country || "")] ||
                        String(company.country || "")}
                    </td>
                    <td className="px-4 py-3 text-forge-400 truncate max-w-xs">
                      {String(company.domain || company.website_url || "")}
                    </td>
                    <td className="px-4 py-3 text-forge-400">
                      {String(company.source || "")}
                    </td>
                    <td className="px-4 py-3">
                      <span className="px-2 py-0.5 rounded-full text-xs bg-blue-900/50 text-blue-300">
                        {String(company.status || "")}
                      </span>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
