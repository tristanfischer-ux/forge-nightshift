import { useEffect, useState, useRef } from "react";
import { Play, Square, Search, Activity, Zap, Clock, ChevronDown, ChevronRight, Shield, Send, Rss } from "lucide-react";
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
  getExtendedStats,
} from "../lib/tauri";
import type { PipelineNodeEvent, RunHistoryEntry, ExtendedStats } from "../lib/tauri";

// activityCounter moved to useRef inside component

interface ActivityEntry {
  id: number;
  time: string;
  nodeId: string;
  status: string;
  item: string | null;
}

const PRESETS: { label: string; stages: string[]; icon: React.ReactNode; description: string; primary?: boolean }[] = [
  {
    label: "Full Pipeline",
    stages: ["research", "enrich", "deep_enrich_drain", "verify", "synthesize", "director_intel", "push"],
    icon: <Play className="w-3.5 h-3.5" />,
    description: "End-to-end: discover, enrich, verify, synthesize, push",
    primary: true,
  },
  {
    label: "Intelligence",
    stages: ["verify", "synthesize", "director_intel"],
    icon: <Shield className="w-3.5 h-3.5" />,
    description: "Verify + synthesize + director analysis",
  },
  {
    label: "Enrich + Verify",
    stages: ["enrich", "deep_enrich_drain", "verify", "synthesize"],
    icon: <Zap className="w-3.5 h-3.5" />,
    description: "Enrich, deep enrich, verify, and synthesize",
  },
  {
    label: "Discovery Only",
    stages: ["research", "enrich"],
    icon: <Search className="w-3.5 h-3.5" />,
    description: "Find new companies and basic enrichment",
  },
  {
    label: "Push + Outreach",
    stages: ["push", "outreach", "activity"],
    icon: <Send className="w-3.5 h-3.5" />,
    description: "Push to ForgeOS, send outreach, fetch activity",
  },
  {
    label: "Activity Feed",
    stages: ["activity"],
    icon: <Rss className="w-3.5 h-3.5" />,
    description: "Fetch latest activity for tracked companies",
  },
];

export default function Pipeline() {
  const [nodes, setNodes] = useState<Record<string, PipelineNodeEvent | null>>({});
  const [running, setRunning] = useState(false);
  const [starting, setStarting] = useState(false);
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const activityCounterRef = useRef(0);
  const [stats, setStats] = useState<Record<string, unknown>>({});
  const [extStats, setExtStats] = useState<ExtendedStats>({ verified: 0, synthesized: 0, intel_records: 0, activities: 0 });
  const activityRef = useRef<HTMLDivElement>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [historyOpen, setHistoryOpen] = useState(false);
  const [expandedJob, setExpandedJob] = useState<string | null>(null);
  const [nextRunText, setNextRunText] = useState("");

  useEffect(() => {
    // Load initial state
    getPipelineStatus().then((s) => setRunning(s.running)).catch(() => {});
    getPipelineNodes().then((n) => setNodes(n as Record<string, PipelineNodeEvent | null>)).catch(() => {});
    getStats().then(setStats).catch(() => {});
    getExtendedStats().then(setExtStats).catch(() => {});
    getRunHistory(10).then(setRunHistory).catch(() => {});
    getConfig().then((c) => {
      try {
        const schedules = JSON.parse(c.schedules || "[]") as { enabled: boolean; name: string; type: string; interval_hours?: number; time?: string; last_run_at?: string }[];
        updateNextRunFromSchedules(schedules);
      } catch {}
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
        getExtendedStats().then(setExtStats).catch(() => {});
        getRunHistory(10).then(setRunHistory).catch(() => {});
      }
    });

    return () => {
      unNode.then((fn) => fn());
      unStatus.then((fn) => fn());
    };
  }, []);

  function updateNextRunFromSchedules(schedules: { enabled: boolean; name: string; type: string; interval_hours?: number; time?: string; days?: number[]; last_run_at?: string }[]) {
    const now = Date.now();
    let soonestMs = Infinity;
    let soonestName = "";

    for (const s of schedules) {
      if (!s.enabled) continue;
      let nextMs = Infinity;
      if (s.type === "daily" && s.time) {
        const [hh, mm] = s.time.split(":").map(Number);
        if (isNaN(hh) || isNaN(mm)) continue;
        const next = new Date();
        next.setHours(hh, mm, 0, 0);
        if (next.getTime() <= now) next.setDate(next.getDate() + 1);
        nextMs = next.getTime();
      } else if (s.type === "weekly" && s.time && s.days && s.days.length > 0) {
        const [hh, mm] = s.time.split(":").map(Number);
        if (isNaN(hh) || isNaN(mm)) continue;
        // Find next matching day-of-week (0=Sun..6=Sat)
        const today = new Date();
        for (let offset = 0; offset < 8; offset++) {
          const candidate = new Date(today);
          candidate.setDate(candidate.getDate() + offset);
          candidate.setHours(hh, mm, 0, 0);
          const dow = candidate.getDay();
          if (s.days.includes(dow) && candidate.getTime() > now) {
            nextMs = candidate.getTime();
            break;
          }
        }
      } else if (s.type === "interval" && s.interval_hours) {
        const hours = Math.max(1, s.interval_hours);
        const lastRun = s.last_run_at ? new Date(s.last_run_at).getTime() : 0;
        nextMs = lastRun + hours * 3600000;
        if (nextMs <= now) nextMs = now; // overdue, will fire next tick
      }
      if (nextMs < soonestMs) {
        soonestMs = nextMs;
        soonestName = s.name;
      }
    }

    if (soonestMs === Infinity) {
      setNextRunText("");
      return;
    }

    const diffMs = Math.max(0, soonestMs - now);
    const diffH = Math.floor(diffMs / 3600000);
    const diffM = Math.floor((diffMs % 3600000) / 60000);
    const timeStr = diffMs <= 0 ? "imminent" : `in ${diffH}h ${diffM}m`;
    setNextRunText(`Next: "${soonestName}" ${timeStr}`);
  }

  // Refresh countdown every 60s
  useEffect(() => {
    if (!nextRunText) return;
    const interval = setInterval(() => {
      getConfig().then((c) => {
        try {
          const schedules = JSON.parse(c.schedules || "[]");
          updateNextRunFromSchedules(schedules);
        } catch {}
      }).catch(() => {});
    }, 60000);
    return () => clearInterval(interval);
  }, [nextRunText]);

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
        <div className="flex gap-2 flex-wrap justify-end">
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
                className={`flex items-center gap-2 px-3 py-2 disabled:opacity-50 rounded-lg text-xs font-medium text-white transition-colors ${
                  preset.primary
                    ? "bg-forge-600 hover:bg-forge-700"
                    : "bg-gray-600 hover:bg-gray-700"
                }`}
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
      <div className="grid grid-cols-7 gap-3">
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Companies</p>
          <p className="text-lg font-bold text-gray-900">{totalCompanies.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Deep Enriched</p>
          <p className="text-lg font-bold text-gray-900">{deepEnriched.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Verified</p>
          <p className="text-lg font-bold text-gray-900">{extStats.verified.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Synthesized</p>
          <p className="text-lg font-bold text-gray-900">{extStats.synthesized.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Intel Records</p>
          <p className="text-lg font-bold text-gray-900">{extStats.intel_records.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Activities</p>
          <p className="text-lg font-bold text-gray-900">{extStats.activities.toLocaleString()}</p>
        </div>
        <div className="bg-white rounded-xl border border-gray-200 p-3 shadow-sm">
          <p className="text-[10px] text-gray-500 uppercase tracking-wide">Pushed</p>
          <p className="text-lg font-bold text-gray-900">{companyCounts.find(c => c.status === "pushed")?.count?.toLocaleString() ?? "0"}</p>
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
