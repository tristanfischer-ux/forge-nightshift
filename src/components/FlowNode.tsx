import { Loader2, CheckCircle, XCircle, Clock, Circle } from "lucide-react";
import type { PipelineNodeEvent } from "../lib/tauri";

interface FlowNodeProps {
  nodeId: string;
  label: string;
  tooltip?: string;
  state: PipelineNodeEvent | null;
  x?: number;
  y?: number;
}

function StatusIcon({ status }: { status: string }) {
  switch (status) {
    case "running":
      return <Loader2 className="w-4 h-4 text-forge-600 animate-spin" />;
    case "completed":
      return <CheckCircle className="w-4 h-4 text-green-600" />;
    case "failed":
      return <XCircle className="w-4 h-4 text-red-600" />;
    case "waiting":
      return <Clock className="w-4 h-4 text-amber-600" />;
    default:
      return <Circle className="w-4 h-4 text-gray-300" />;
  }
}

function formatRate(rate: number | null): string {
  if (!rate || rate === 0) return "";
  if (rate >= 60) return `${Math.round(rate / 60)}/min`;
  if (rate >= 1) return `${Math.round(rate)}/hr`;
  return `1 per ${Math.round(60 / rate)}min`;
}

function formatElapsed(secs: number | null): string {
  if (!secs || secs <= 0) return "";
  if (secs < 60) return `${secs}s`;
  if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
  return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

export default function FlowNode({ nodeId, label, tooltip, state, x, y }: FlowNodeProps) {
  const status = state?.status ?? "idle";

  const borderClass = {
    idle: "border-gray-200",
    running: "border-forge-500 ring-2 ring-forge-200",
    completed: "border-green-300 bg-green-50",
    failed: "border-red-300 bg-red-50",
    waiting: "border-amber-300 bg-amber-50",
  }[status] || "border-gray-200";

  const bgClass = status === "idle" || status === "running" ? "bg-white" : "";

  const progress = state?.progress;
  const pct = progress?.total && progress.total > 0
    ? Math.min(100, Math.round((progress.current / progress.total) * 100))
    : null;

  return (
    <div
      className={`rounded-xl border shadow-sm p-3 transition-all duration-300 ${borderClass} ${bgClass} ${x != null ? 'absolute w-48' : 'w-full'}`}
      style={x != null ? { left: x, top: y } : undefined}
      data-node-id={nodeId}
      title={tooltip}
    >
      {status === "running" && (
        <div className="absolute left-0 top-0 bottom-0 w-1 bg-forge-500 rounded-l-xl animate-pulse" />
      )}

      <div className="flex items-center justify-between mb-1">
        <span className="text-sm font-semibold text-gray-900">{label}</span>
        <StatusIcon status={status} />
      </div>

      {state?.model && (
        <p className="text-[10px] text-gray-400 mb-1 truncate">{state.model}</p>
      )}
      {!state?.model && nodeId === "push" && (
        <p className="text-[10px] text-gray-400 mb-1">Supabase</p>
      )}
      {!state?.model && nodeId === "verify" && (
        <p className="text-[10px] text-gray-400 mb-1">Website scrape</p>
      )}
      {!state?.model && nodeId === "outreach" && (
        <p className="text-[10px] text-gray-400 mb-1">Resend</p>
      )}
      {!state?.model && nodeId === "activity" && (
        <p className="text-[10px] text-gray-400 mb-1">Brave Search</p>
      )}

      {status === "running" && progress && (
        <>
          {pct !== null && (
            <div className="w-full bg-gray-100 rounded-full h-1.5 mb-1">
              <div
                className="bg-forge-500 h-1.5 rounded-full transition-all duration-500"
                style={{ width: `${pct}%` }}
              />
            </div>
          )}
          <div className="flex items-center justify-between text-[10px] text-gray-500">
            <span>
              {progress.current}
              {progress.total ? `/${progress.total}` : ""}
              {pct !== null ? ` (${pct}%)` : ""}
            </span>
            {progress.rate != null && progress.rate > 0 && (
              <span>{formatRate(progress.rate)}</span>
            )}
          </div>
          {progress.current_item && (
            <p className="text-[10px] text-gray-400 truncate mt-0.5" title={progress.current_item}>
              {progress.current_item}
            </p>
          )}
        </>
      )}

      {status === "completed" && state?.elapsed_secs != null && (
        <p className="text-[10px] text-green-600 mt-1">{formatElapsed(state.elapsed_secs)}</p>
      )}

      {status === "running" && progress && progress.total != null && progress.total > 0 && progress.current < progress.total && progress.rate != null && progress.rate > 0.001 && (() => {
        const eta = Math.round((progress.total - progress.current) / progress.rate * 3600);
        return !isNaN(eta) && isFinite(eta) && eta > 0 && eta < 864000 ? (
          <p className="text-[10px] text-gray-400 mt-0.5">
            ~{formatElapsed(eta)} remaining
          </p>
        ) : null;
      })()}

      {status === "running" && state?.concurrency && state.concurrency > 1 && (
        <p className="text-[10px] text-forge-500 mt-0.5">x{state.concurrency} parallel</p>
      )}
    </div>
  );
}
