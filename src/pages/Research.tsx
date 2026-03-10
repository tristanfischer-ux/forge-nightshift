import { useEffect, useState } from "react";
import { Search, Globe, RefreshCw } from "lucide-react";
import { getCompanies, getConfig, setConfig, startPipeline } from "../lib/tauri";
import { useError } from "../contexts/ErrorContext";

const STATUS_BADGE: Record<string, string> = {
  discovered: "bg-blue-100 text-blue-700",
  enriching: "bg-amber-100 text-amber-700 animate-pulse",
  enriched: "bg-green-100 text-green-700",
  approved: "bg-yellow-100 text-yellow-700",
  pushed: "bg-purple-100 text-purple-700",
  error: "bg-red-100 text-red-700",
};

const COUNTRIES: Record<string, string> = {
  DE: "Germany",
  FR: "France",
  NL: "Netherlands",
  BE: "Belgium",
  IT: "Italy",
  GB: "United Kingdom",
};

export default function Research() {
  const { showError } = useError();
  const [companies, setCompanies] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);
  const [enabledCountries, setEnabledCountries] = useState<Set<string>>(
    new Set(Object.keys(COUNTRIES))
  );

  useEffect(() => {
    loadCompanies();
    loadCountryConfig();
  }, []);

  async function loadCountryConfig() {
    try {
      const config = await getConfig();
      if (config.target_countries) {
        const parsed = JSON.parse(config.target_countries) as string[];
        setEnabledCountries(new Set(parsed));
      }
    } catch (e) {
      showError(`Failed to load country config: ${e}`);
    }
  }

  async function toggleCountry(code: string) {
    setEnabledCountries((prev) => {
      const next = new Set(prev);
      if (next.has(code)) {
        next.delete(code);
      } else {
        next.add(code);
      }
      setConfig("target_countries", JSON.stringify([...next]));
      return next;
    });
  }

  async function loadCompanies() {
    try {
      const data = await getCompanies(undefined, 2000, 0);
      setCompanies(data);
    } catch (e) {
      showError(`Failed to load companies: ${e}`);
    }
  }

  async function runResearchOnly() {
    setLoading(true);
    try {
      await startPipeline(["research"]);
    } catch (e) {
      showError(`Failed to start research: ${e}`);
    }
    setLoading(false);
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Research</h1>
          <p className="text-sm text-gray-500 mt-1">
            Company discovery across European markets
          </p>
        </div>

        <button
          onClick={runResearchOnly}
          disabled={loading}
          className="flex items-center gap-2 px-4 py-2 bg-forge-600 hover:bg-forge-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
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
      <div className="bg-white rounded-xl border border-gray-200 p-4 shadow-sm">
        <h2 className="text-sm font-semibold text-gray-900 mb-3">
          Target Countries
        </h2>
        <div className="flex gap-2">
          {Object.entries(COUNTRIES).map(([code, name]) => {
            const enabled = enabledCountries.has(code);
            return (
              <button
                key={code}
                onClick={() => toggleCountry(code)}
                className={`flex items-center gap-2 px-3 py-1.5 rounded-lg text-sm transition-colors ${
                  enabled
                    ? "bg-forge-600 text-white"
                    : "bg-gray-100 text-gray-400 opacity-60"
                }`}
              >
                <Globe className="w-3 h-3" />
                {name}
              </button>
            );
          })}
        </div>
      </div>

      {/* Discovered companies table */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="p-4 border-b border-gray-200">
          <h2 className="text-sm font-semibold text-gray-900">
            All Companies ({companies.length})
          </h2>
        </div>

        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-100">
                <th className="text-left px-4 py-3 text-gray-500 font-medium">
                  Company
                </th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">
                  Country
                </th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">
                  Website
                </th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">
                  Source
                </th>
                <th className="text-left px-4 py-3 text-gray-500 font-medium">
                  Status
                </th>
              </tr>
            </thead>
            <tbody className="divide-y divide-gray-100">
              {companies.length === 0 ? (
                <tr>
                  <td
                    colSpan={5}
                    className="px-4 py-8 text-center text-gray-400"
                  >
                    No companies discovered yet. Run a research pipeline to get
                    started.
                  </td>
                </tr>
              ) : (
                companies.map((company) => (
                  <tr
                    key={String(company.id)}
                    className="hover:bg-gray-50"
                  >
                    <td className="px-4 py-3 font-medium text-gray-900">
                      {String(company.name || "")}
                    </td>
                    <td className="px-4 py-3 text-gray-600">
                      {COUNTRIES[String(company.country || "")] ||
                        String(company.country || "")}
                    </td>
                    <td className="px-4 py-3 text-gray-500 truncate max-w-xs">
                      {String(company.domain || company.website_url || "")}
                    </td>
                    <td className="px-4 py-3 text-gray-500">
                      {String(company.source || "")}
                    </td>
                    <td className="px-4 py-3">
                      <span className={`px-2 py-0.5 rounded-full text-xs ${STATUS_BADGE[String(company.status || "")] || "bg-gray-100 text-gray-700"}`}>
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
