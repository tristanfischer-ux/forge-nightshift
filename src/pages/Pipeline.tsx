import { useEffect, useState, useRef } from "react";
import { Play, Square, RefreshCw, Search, Activity, Zap, Clock, ChevronDown, ChevronRight, Building2 } from "lucide-react";
import FlowChart from "../components/FlowChart";
import {
  startPipeline,
  stopPipeline,
  getPipelineStatus,
  getPipelineNodes,
  onPipelineNode,
  onPipelineStatus,
  getStats,
  getRunHistory,
  getConfig,
} from "../lib/tauri";
import type { PipelineNodeEvent, RunHistoryEntry } from "../lib/tauri";

// activityCounter moved to useRef inside component

interface ActivityEntry {
  id: number;
  time: string;
  nodeId: string;
  status: string;
  item: string | null;
}

const PRESETS: { label: string; stages: string[]; icon: React.ReactNode; description: string }[] = [
  {
    label: "Full Pipeline",
    stages: ["research", "enrich", "deep_enrich_all", "aggregate_techniques", "push_techniques"],
    icon: <Play className="w-3.5 h-3.5" />,
    description: "Find new + backfill all",
  },
  {
    label: "Full + Deep",
    stages: ["research", "enrich", "deep_enrich_drain"],
    icon: <Zap className="w-3.5 h-3.5" />,
    description: "Find + enrich + deep enrich concurrently",
  },
  {
    label: "Backfill Only",
    stages: ["deep_enrich_all", "aggregate_techniques", "push_techniques"],
    icon: <RefreshCw className="w-3.5 h-3.5" />,
    description: "Process existing companies",
  },
  {
    label: "Discovery Only",
    stages: ["research", "enrich", "push"],
    icon: <Search className="w-3.5 h-3.5" />,
    description: "Find + enrich + deep enrich + push",
  },
  {
    label: "CH Verify",
    stages: ["companies_house"],
    icon: <Building2 className="w-3.5 h-3.5" />,
    description: "Cross-check all GB companies against Companies House",
  },
];

export default function Pipeline() {
  const [nodes, setNodes] = useState<Record<string, PipelineNodeEvent | null>>({});
  const [running, setRunning] = useState(false);
  const [starting, setStarting] = useState(false);
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const activityCounterRef = useRef(0);
  const [stats, setStats] = useState<Record<string, unknown>>({});
  const activityRef = useRef<HTMLDivElement>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [expandedJob, setExpandedJob] = useState<string | null>(null);
  const [scheduleTime, setScheduleTime] = useState<string | null>(null);
  const [nextRunText, setNextRunText] = useState("");

  useEffect(() => {
    // Load initial state
    getPipelineStatus().then((s) => setRunning(s.running)).catch(() => {});
    getPipelineNodes().then((n) => setNodes(n as Record<string, PipelineNodeEvent | null>)).catch(() => {});
    getStats().then(setStats).catch(() => {});
    getRunHistory(10).then(setRunHistory).catch(() => {});
    getConfig().then((c) => {
      const t = c.schedule_time;
      if (t) setScheduleTime(t);
    }).catch(() => {});

    // Subscribe to events
    const unNode = onPipelineNode((payload) => {
      setNodes((prev) => ({ ...prev, [payload.node_id]: payload }));
      setActivity((prev) => {
        const entry: ActivityEntry = {
          id: ++activityCounterRef.current,
          time: new Date().toLocaleTimeString(),
          nodeId: payload.node_id,
          status: payload.status,
          item: payload.progress?.current_item ?? null,
        };
        return [entry, ...prev].slice(0, 50);
      });
    });

    const unStatus = onPipelineStatus((payload) => {
      const status = payload.status as string;
      setRunning(status === "running");
      if (status === "completed" || status === "failed") {
        getStats().then(setStats).catch(() => {});
        getRunHistory(10).then(setRunHistory).catch(() => {});
      }
    });

    return () => {
      unNode.then((fn) => fn());
      unStatus.then((fn) => fn());
    };
  }, []);

  // Schedule next-run countdown
  useEffect(() => {
    if (!scheduleTime) return;
    function updateNextRun() {
      const [hh, mm] = scheduleTime!.split(":").map(Number);
      if (isNaN(hh) || isNaN(mm)) return;
      const now = new Date();
      const next = new Date(now);
      next.setHours(hh, mm, 0, 0);
      if (next <= now) next.setDate(next.getDate() + 1);
      const diffMs = next.getTime() - now.getTime();
      const diffH = Math.floor(diffMs / 3600000);
      const diffM = Math.floor((diffMs % 3600000) / 60000);
      setNextRunText(`Next run at ${scheduleTime} (in ${diffH}h ${diffM}m)`);
    }
    updateNextRun();
    const interval = setInterval(updateNextRun, 60000);
    return () => clearInterval(interval);
  }, [scheduleTime]);

  const handleStart = async (stages: string[]) => {
    if (starting) return;
    setStarting(true);
    // Clear node states BEFORE starting so early events aren't wiped
    setNodes({});
    setActivity([]);
    try {
      await startPipeline(stages);
      // Don't set running=true here — let the event handler be the source of truth
    } catch (e) {
      console.error("Failed to start pipeline:", e);
    } finally {
      setStarting(false);
    }
  };

  const handleStop = async () => {
    try {
      await stopPipeline();
    } catch (e) {
      console.error("Failed to stop pipeline:", e);
    }
  };

  // stats.companies is an array of {status, count} — sum all counts for total
  const companyCounts = (stats.companies as { status: string; count: number }[]) ?? [];
  const totalCompanies = companyCounts.reduce((sum, c) => sum + (c.count ?? 0), 0);
  // Deep enriched = companies with process_capabilities_json set (not directly in stats, approximate from pipeline node)
  const deepEnrichNode = nodes.deep_enrich;
  const deepEnriched = deepEnrichNode?.status === "completed"
    ? (deepEnrichNode.progress?.total ?? deepEnrichNode.progress?.current ?? 0)
    : (deepEnrichNode?.progress?.current ?? 0);
  // Technique articles from aggregate node
  const aggregateNode = nodes.aggregate_techniques;
  const techniques = aggregateNode?.status === "completed"
    ? (aggregateNode.progress?.total ?? aggregateNode.progress?.current ?? 0)
    : (aggregateNode?.progress?.current ?? 0);

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipeline Monitor</h1>
          <p className="text-sm text-gray-500 mt-1">
            Real-time view of the Nightshift enrichment pipeline
          </p>
        </div>
        <div className="flex gap-2">
          {running ? (
            <button
              onClick={handleStop}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm font-medium text-white transition-colors"
            >
              <Square className="w-3.5 h-3.5" />
              Stop
            </button>
          ) : (
            PRESETS.map((preset) => (
              <button
                key={preset.label}
                onClick={() => handleStart(preset.stages)}
                disabled={starting}
                className="flex items-center gap-2 px-3 py-2 bg-forge-600 hover:bg-forge-700 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors"
                title={preset.description}
              >
                {preset.icon}
                {preset.label}
              </button>
            ))
          )}
        </div>
      </div>

      {/* Stats bar */}
      <div className="grid grid-cols-3 gap-4">
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-xs text-gray-500 uppercase tracking-wide">Companies</p>
          <p className="text-xl font-bold text-gray-900">{totalCompanies.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-xs text-gray-500 uppercase tracking-wide">Deep Enriched</p>
          <p className="text-xl font-bold text-gray-900">{deepEnriched.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-xs text-gray-500 uppercase tracking-wide">Technique Articles</p>
          <p className="text-xl font-bold text-gray-900">{techniques.toLocaleString()}</p>
        </div>
      </div>

      {/* Schedule indicator */}
      {nextRunText && !running && (
        <div className="flex items-center gap-2 text-xs text-gray-500">
          <Clock className="w-3.5 h-3.5" />
          {nextRunText}
        </div>
      )}

      {/* Flow chart */}
      <FlowChart nodes={nodes} />

      {/* Run history */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <button
          onClick={() => setHistoryOpen(!historyOpen)}
          className="flex items-center gap-2 p-4 w-full text-left"
        >
          {historyOpen ? <ChevronDown className="w-4 h-4 text-gray-400" /> : <ChevronRight className="w-4 h-4 text-gray-400" />}
          <h2 className="text-sm font-semibold text-gray-900">Run History</h2>
          <span className="text-xs text-gray-400 ml-auto">{runHistory.length} runs</span>
        </button>
        {historyOpen && (
          <div className="divide-y divide-gray-100 max-h-64 overflow-y-auto">
            {runHistory.length === 0 ? (
              <div className="p-4 text-sm text-gray-400 text-center">No pipeline runs recorded yet.</div>
            ) : (
              runHistory.map((job) => (
                <div key={job.id} className="px-4 py-2">
                  <div
                    className="flex items-center gap-3 text-xs cursor-pointer"
                    onClick={() => setExpandedJob(expandedJob === job.id ? null : job.id)}
                  >
                    <span className={`w-2 h-2 rounded-full shrink-0 ${
                      job.status === "completed" ? "bg-green-500" : job.status === "failed" ? "bg-red-500" : "bg-gray-300"
                    }`} />
                    <span className="font-medium text-gray-700 w-32 truncate">{job.stages}</span>
                    <span className="text-gray-500 capitalize w-20">{job.status}</span>
                    <span className="text-gray-400 ml-auto">{job.started_at?.slice(0, 16) || job.created_at?.slice(0, 16)}</span>
                    {job.started_at && job.completed_at && (
                      <span className="text-gray-400">
                        {Math.round((new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()) / 60000)}m
                      </span>
                    )}
                  </div>
                  {expandedJob === job.id && job.summary && (
                    <pre className="mt-1 text-[10px] text-gray-500 bg-gray-50 rounded p-2 whitespace-pre-wrap">{job.summary}</pre>
                  )}
                </div>
              ))
            )}
          </div>
        )}
      </div>

      {/* Activity feed */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="flex items-center gap-2 p-4 border-b border-gray-200">
          <Activity className="w-4 h-4 text-gray-400" />
          <h2 className="text-sm font-semibold text-gray-900">Activity Feed</h2>
          <span className="text-xs text-gray-400 ml-auto">{activity.length} events</span>
        </div>
        <div ref={activityRef} className="divide-y divide-gray-100 max-h-64 overflow-y-auto">
          {activity.length === 0 ? (
            <div className="p-4 text-sm text-gray-400 text-center">
              No activity yet. Start a pipeline to see events.
            </div>
          ) : (
            activity.map((entry) => (
              <div key={entry.id} className="flex items-center gap-3 px-4 py-2 text-xs">
                <span className="text-gray-400 font-mono w-16 shrink-0">{entry.time}</span>
                <span className={`w-2 h-2 rounded-full shrink-0 ${
                  entry.status === "running" ? "bg-forge-500" :
                  entry.status === "completed" ? "bg-green-500" :
                  entry.status === "failed" ? "bg-red-500" :
                  "bg-gray-300"
                }`} />
                <span className="font-medium text-gray-700 w-32 shrink-0">{entry.nodeId.replace(/_/g, " ")}</span>
                <span className="text-gray-500 capitalize w-20 shrink-0">{entry.status}</span>
                {entry.item && (
                  <span className="text-gray-400 truncate">{entry.item}</span>
                )}
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
