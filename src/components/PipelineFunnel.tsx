import { useNavigate } from "react-router-dom";
import type { PipelineFunnelData } from "../lib/tauri";

interface FunnelRow {
  label: string;
  key: keyof PipelineFunnelData;
  color: string;
  /** URL search params for drill-down navigation to /review */
  filter?: string;
}

const FUNNEL_ROWS: FunnelRow[] = [
  { label: "Total", key: "total", color: "bg-gray-400" },
  { label: "Enriched", key: "enriched", color: "bg-purple-500", filter: "status=enriched" },
  { label: "Process Caps", key: "with_process_capabilities", color: "bg-blue-500" },
  { label: "Verified", key: "verified", color: "bg-teal-500" },
  { label: "Synth (Public)", key: "synthesized_public", color: "bg-indigo-500" },
  { label: "Synth (Private)", key: "synthesized_private", color: "bg-indigo-400" },
  { label: "Director Intel", key: "director_intel", color: "bg-yellow-500" },
  { label: "Embeddings", key: "embeddings", color: "bg-cyan-500" },
  { label: "Approved", key: "approved", color: "bg-green-500", filter: "status=approved" },
  { label: "Pushed", key: "pushed", color: "bg-green-600", filter: "status=pushed" },
  { label: "Errors", key: "error", color: "bg-red-500", filter: "status=error" },
];

interface Props {
  data: PipelineFunnelData | null;
  profileName?: string;
  /** Render in compact mode (smaller text, no profile header) */
  compact?: boolean;
}

export default function PipelineFunnel({ data, profileName, compact = false }: Props) {
  const navigate = useNavigate();

  if (!data) {
    return (
      <div className={`bg-white rounded-xl border border-gray-200 shadow-sm ${compact ? "p-3" : "p-6"}`}>
        <div className="animate-pulse space-y-3">
          <div className="h-4 bg-gray-200 rounded w-48" />
          {Array.from({ length: 6 }).map((_, i) => (
            <div key={i} className="h-3 bg-gray-100 rounded w-full" />
          ))}
        </div>
      </div>
    );
  }

  const maxCount = Math.max(data.total, 1);

  function handleRowClick(row: FunnelRow) {
    if (row.filter) {
      navigate(`/review?${row.filter}`);
    }
  }

  return (
    <div className={`bg-white rounded-xl border border-gray-200 shadow-sm ${compact ? "p-3" : "p-6"}`}>
      {!compact && (
        <div className="mb-4">
          <h2 className="text-lg font-semibold text-gray-900">Pipeline Funnel</h2>
          {profileName && (
            <p className="text-sm text-forge-600 font-medium mt-0.5">{profileName}</p>
          )}
        </div>
      )}
      {compact && (
        <h3 className="text-sm font-semibold text-gray-900 mb-2">Pipeline Funnel</h3>
      )}

      <table className="w-full">
        <thead>
          <tr className={`text-left ${compact ? "text-[10px]" : "text-xs"} text-gray-400 uppercase tracking-wide`}>
            <th className="pb-2 font-medium w-36">Stage</th>
            <th className={`pb-2 font-medium ${compact ? "w-16" : "w-20"} text-right pr-3`}>Count</th>
            <th className="pb-2 font-medium">Distribution</th>
          </tr>
        </thead>
        <tbody>
          {FUNNEL_ROWS.map((row) => {
            const count = data[row.key];
            const pct = maxCount > 0 ? (count / maxCount) * 100 : 0;
            const isClickable = !!row.filter;
            const isError = row.key === "error";

            return (
              <tr
                key={row.key}
                onClick={() => handleRowClick(row)}
                className={`group ${
                  isClickable
                    ? "cursor-pointer hover:bg-gray-50 transition-colors"
                    : ""
                } ${compact ? "h-7" : "h-9"}`}
              >
                <td className={`${compact ? "text-xs" : "text-sm"} font-medium ${
                  isError && count > 0 ? "text-red-600" : "text-gray-700"
                } ${isClickable ? "group-hover:text-forge-600" : ""}`}>
                  {row.label}
                </td>
                <td className={`${compact ? "text-xs" : "text-sm"} font-bold text-right pr-3 tabular-nums ${
                  isError && count > 0 ? "text-red-600" : "text-gray-900"
                }`}>
                  {count.toLocaleString()}
                </td>
                <td className="py-1">
                  <div className={`w-full ${compact ? "h-3" : "h-4"} bg-gray-100 rounded-full overflow-hidden`}>
                    {pct > 0 && (
                      <div
                        className={`h-full ${row.color} rounded-full transition-all duration-500`}
                        style={{ width: `${Math.max(pct, 1)}%` }}
                      />
                    )}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
