import { useEffect, useState, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { Play, Square, Search, Activity, Zap, Clock, ChevronDown, ChevronRight, Shield, Send, AlertTriangle, Settings2, History } from "lucide-react";
import FlowChart from "../components/FlowChart";
import PipelineFunnel from "../components/PipelineFunnel";
import {
  startPipeline,
  stopPipeline,
  getPipelineStatus,
  getPipelineNodes,
  onPipelineNode,
  onPipelineStatus,
  onPipelineStage,
  getStats,
  getRunHistory,
  getConfig,
  getExtendedStats,
  getActiveProfile,
  getSearchProfiles,
  getPipelineFunnel,
} from "../lib/tauri";
import type { PipelineNodeEvent, RunHistoryEntry, ExtendedStats, SearchProfile, PipelineFunnelData } from "../lib/tauri";
import { stageLabel, stageTooltip } from "../lib/stage-labels";

interface ActivityEntry {
  id: number;
  time: string;
  nodeId: string;
  status: string;
  item: string | null;
}

interface ConfirmState {
  open: boolean;
  label: string;
  stages: string[];
  stageDescription: string;
}

const ADVANCED_PRESETS: { label: string; stages: string[]; icon: React.ReactNode; description: string }[] = [
  {
    label: "Intelligence",
    stages: ["verify", "synthesize", "director_intel"],
    icon: <Shield className="w-3.5 h-3.5" />,
    description: "Fact-check + analyse + leadership intel",
  },
  {
    label: "Enrich + Verify",
    stages: ["enrich", "verify", "synthesize"],
    icon: <Zap className="w-3.5 h-3.5" />,
    description: "Research, fact-check, and analyse",
  },
  {
    label: "Discovery Only",
    stages: ["research", "enrich"],
    icon: <Search className="w-3.5 h-3.5" />,
    description: "Find new companies and basic research",
  },
  {
    label: "Push + Outreach",
    stages: ["push", "outreach", "activity"],
    icon: <Send className="w-3.5 h-3.5" />,
    description: "Publish to ForgeOS, send outreach, fetch news",
  },
];

function stagesToDescription(stages: string[]): string {
  if (stages.length === 1 && stages[0] === "batch") {
    return `${stageLabel("research")} \u2192 ${stageLabel("enrich")} \u2192 ${stageLabel("verify")} \u2192 ${stageLabel("synthesize")} \u2192 ${stageLabel("activities")} \u2192 ${stageLabel("embeddings")} \u2192 ${stageLabel("investor_matches")} \u2192 ${stageLabel("push")}`;
  }
  return stages.map((s) => stageLabel(s)).join(" \u2192 ");
}

function stagesToMode(stages: string[]): string {
  if (stages.length === 1 && stages[0] === "batch") return "Batch (waves of 100)";
  return "Custom";
}

type ActivityTab = "feed" | "history";

export default function Pipeline() {
  const navigate = useNavigate();
  const [nodes, setNodes] = useState<Record<string, PipelineNodeEvent | null>>({});
  const [running, setRunning] = useState(false);
  const [starting, setStarting] = useState(false);
  const [activity, setActivity] = useState<ActivityEntry[]>([]);
  const activityCounterRef = useRef(0);
  const [stats, setStats] = useState<Record<string, unknown>>({});
  const [extStats, setExtStats] = useState<ExtendedStats>({ verified: 0, synthesized: 0, intel_records: 0, activities: 0 });
  const activityRef = useRef<HTMLDivElement>(null);
  const [runHistory, setRunHistory] = useState<RunHistoryEntry[]>([]);
  const [expandedJob, setExpandedJob] = useState<string | null>(null);
  const [nextRunText, setNextRunText] = useState("");
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [confirm, setConfirm] = useState<ConfirmState>({ open: false, label: "", stages: [], stageDescription: "" });
  const [activityTab, setActivityTab] = useState<ActivityTab>("feed");
  const [activityOpen, setActivityOpen] = useState(true);
  const [profileName, setProfileName] = useState("");
  const [funnel, setFunnel] = useState<PipelineFunnelData | null>(null);

  useEffect(() => {
    // Load initial state
    getPipelineStatus().then((s) => setRunning(s.running)).catch(() => {});
    getPipelineNodes().then((n) => setNodes(n as Record<string, PipelineNodeEvent | null>)).catch(() => {});
    getStats().then(setStats).catch(() => {});
    getExtendedStats().then(setExtStats).catch(() => {});
    getRunHistory(10).then(setRunHistory).catch(() => {});
    getPipelineFunnel().then(setFunnel).catch(() => {});
    getConfig().then((c) => {
      try {
        const schedules = JSON.parse(c.schedules || "[]") as { enabled: boolean; name: string; type: string; interval_hours?: number; time?: string; last_run_at?: string }[];
        updateNextRunFromSchedules(schedules);
      } catch {}
    }).catch(() => {});

    // Load active profile name
    getActiveProfile().then((activeId) => {
      if (!activeId) return;
      getSearchProfiles().then((profiles) => {
        const match = profiles.find((p: SearchProfile) => p.id === activeId);
        if (match) setProfileName(match.name);
      }).catch(() => {});
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

    // Subscribe to stage transitions (batch_pipeline emits these for each stage start/complete)
    const unStage = onPipelineStage((payload) => {
      setActivity((prev) => {
        const entry: ActivityEntry = {
          id: ++activityCounterRef.current,
          time: new Date().toLocaleTimeString(),
          nodeId: payload.stage,
          status: payload.status,
          item: null,
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
        getPipelineFunnel().then(setFunnel).catch(() => {});
      }
    });

    return () => {
      unNode.then((fn) => fn());
      unStage.then((fn) => fn());
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
        if (nextMs <= now) nextMs = now;
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

  const requestStart = (label: string, stages: string[]) => {
    setConfirm({
      open: true,
      label,
      stages,
      stageDescription: stagesToDescription(stages),
    });
  };

  const handleConfirmedStart = async () => {
    const stages = confirm.stages;
    setConfirm({ open: false, label: "", stages: [], stageDescription: "" });
    if (starting) return;
    setStarting(true);
    setNodes({});
    setActivity([]);
    setActivityOpen(true);
    setActivityTab("feed");
    try {
      await startPipeline(stages);
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

  const companyCounts = (stats.companies as { status: string; count: number }[]) ?? [];
  const totalCompanies = companyCounts.reduce((sum, c) => sum + (c.count ?? 0), 0);
  const errorCount = companyCounts.find((c) => c.status === "error")?.count ?? 0;
  const pushedCount = companyCounts.find((c) => c.status === "pushed")?.count ?? 0;

  // Batch wave progress
  const batchNode = nodes.batch;
  const batchWave = (batchNode as Record<string, unknown> | null)?.wave as number | undefined;
  const batchTotalProcessed = (batchNode as Record<string, unknown> | null)?.total_processed as number | undefined;
  const batchCurrentStage = batchNode?.progress?.current_item ?? (batchNode?.status === "running" ? "Processing" : null);

  return (
    <div className="space-y-4">
      {/* Confirmation dialog */}
      {confirm.open && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40">
          <div className="bg-white rounded-xl shadow-xl border border-gray-200 p-6 max-w-md w-full mx-4">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">Start Pipeline?</h3>
            <div className="space-y-2 text-sm text-gray-600 mb-6">
              <p><span className="font-medium text-gray-700">Mode:</span> {stagesToMode(confirm.stages)}</p>
              <p><span className="font-medium text-gray-700">Stages:</span> {confirm.stageDescription}</p>
            </div>
            <div className="flex justify-end gap-3">
              <button
                onClick={() => setConfirm({ open: false, label: "", stages: [], stageDescription: "" })}
                className="px-4 py-2 text-sm font-medium text-gray-700 bg-gray-100 hover:bg-gray-200 rounded-lg transition-colors"
              >
                Cancel
              </button>
              <button
                onClick={handleConfirmedStart}
                className="px-4 py-2 text-sm font-medium text-white bg-forge-600 hover:bg-forge-700 rounded-lg transition-colors"
              >
                Start
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Pipeline</h1>
          <div className="flex items-center gap-2 mt-0.5">
            {profileName && <span className="text-sm text-gray-500">{profileName}</span>}
            {profileName && <span className="text-gray-300">|</span>}
            <span className={`inline-flex items-center gap-1.5 text-sm font-medium ${running ? "text-forge-600" : "text-gray-500"}`}>
              <span className={`w-1.5 h-1.5 rounded-full ${running ? "bg-forge-500 animate-pulse" : "bg-gray-400"}`} />
              {running ? "Running" : "Ready"}
            </span>
            {nextRunText && !running && (
              <>
                <span className="text-gray-300">|</span>
                <span className="flex items-center gap-1 text-xs text-gray-400">
                  <Clock className="w-3 h-3" />
                  {nextRunText}
                </span>
              </>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {running ? (
            <button
              onClick={handleStop}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 rounded-lg text-sm font-medium text-white transition-colors"
            >
              <Square className="w-3.5 h-3.5" />
              Stop
            </button>
          ) : (
            <>
              <button
                onClick={() => setAdvancedOpen(!advancedOpen)}
                className="flex items-center gap-1 px-2.5 py-2 text-xs text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-colors"
              >
                <Settings2 className="w-3.5 h-3.5" />
                {advancedOpen ? <ChevronDown className="w-3 h-3" /> : <ChevronRight className="w-3 h-3" />}
              </button>
              <button
                onClick={() => requestStart("Batch Pipeline", ["batch"])}
                disabled={starting}
                className="flex items-center gap-2 px-4 py-2 bg-forge-600 hover:bg-forge-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
              >
                <Play className="w-3.5 h-3.5" />
                Run Pipeline
              </button>
            </>
          )}
        </div>
      </div>

      {/* Advanced presets */}
      {advancedOpen && !running && (
        <div className="flex gap-2 flex-wrap">
          {ADVANCED_PRESETS.map((preset) => (
            <button
              key={preset.label}
              onClick={() => requestStart(preset.label, preset.stages)}
              disabled={starting}
              className="flex items-center gap-1.5 px-3 py-1.5 disabled:opacity-50 rounded-lg text-xs font-medium text-white bg-gray-600 hover:bg-gray-700 transition-colors"
              title={preset.description}
            >
              {preset.icon}
              {preset.label}
            </button>
          ))}
        </div>
      )}

      {/* Batch progress banner — prominent when running */}
      {batchNode && running && (
        <div className="flex items-center gap-3 px-4 py-3 bg-forge-600 rounded-xl text-sm text-white">
          <div className="flex items-center gap-2 font-semibold">
            <span className="w-2 h-2 rounded-full bg-white animate-pulse" />
            {batchWave != null ? `Wave ${batchWave}` : "Batch"}
          </div>
          {batchCurrentStage && (
            <>
              <span className="text-forge-200">|</span>
              <span>Stage: {batchCurrentStage}</span>
            </>
          )}
          {batchTotalProcessed != null && (
            <>
              <span className="text-forge-200">|</span>
              <span>{batchTotalProcessed.toLocaleString()} processed</span>
            </>
          )}
        </div>
      )}

      {/* Stats row + Flow chart */}
      <div className="space-y-3">
        {/* Compact stats */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
          <div className="flex items-center gap-3 bg-white rounded-lg border border-gray-200 px-3 py-2">
            <div>
              <p className="text-[10px] text-gray-400 uppercase tracking-wide leading-none">Companies</p>
              <p className="text-base font-bold text-gray-900 mt-0.5">{totalCompanies.toLocaleString()}</p>
            </div>
          </div>
          <div className="flex items-center gap-3 bg-white rounded-lg border border-gray-200 px-3 py-2">
            <div>
              <p className="text-[10px] text-gray-400 uppercase tracking-wide leading-none" title={stageTooltip("verified")}>Fact-Checked</p>
              <p className="text-base font-bold text-gray-900 mt-0.5">{extStats.verified.toLocaleString()}</p>
            </div>
          </div>
          <button
            onClick={() => navigate("/review?status=error")}
            className={`flex items-center gap-3 bg-white rounded-lg border px-3 py-2 text-left transition-colors hover:bg-red-50 ${
              errorCount > 0 ? "border-red-300" : "border-gray-200"
            }`}
          >
            <div>
              <p className={`text-[10px] uppercase tracking-wide leading-none flex items-center gap-1 ${errorCount > 0 ? "text-red-500" : "text-gray-400"}`}>
                <AlertTriangle className="w-2.5 h-2.5" />
                Errors
              </p>
              <p className={`text-base font-bold mt-0.5 ${errorCount > 0 ? "text-red-600" : "text-gray-900"}`}>{errorCount.toLocaleString()}</p>
            </div>
          </button>
          <div className="flex items-center gap-3 bg-white rounded-lg border border-gray-200 px-3 py-2">
            <div>
              <p className="text-[10px] text-gray-400 uppercase tracking-wide leading-none" title={stageTooltip("pushed")}>Published</p>
              <p className="text-base font-bold text-gray-900 mt-0.5">{pushedCount.toLocaleString()}</p>
            </div>
          </div>
        </div>

        {/* Flow chart — fixed height */}
        <div className="h-[280px]">
          <FlowChart nodes={nodes} />
        </div>

        {/* Compact pipeline funnel */}
        <PipelineFunnel data={funnel} compact />
      </div>

      {/* Activity — merged feed + history */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="flex items-center gap-2 px-4 py-2.5">
          <button
            onClick={() => setActivityOpen(!activityOpen)}
            className="text-gray-400 hover:text-gray-600 transition-colors"
          >
            {activityOpen ? <ChevronDown className="w-4 h-4" /> : <ChevronRight className="w-4 h-4" />}
          </button>
          <h2 className="text-sm font-semibold text-gray-900">Activity</h2>
          {/* Tab toggle */}
          <div className="flex items-center gap-0.5 ml-3 bg-gray-100 rounded-md p-0.5">
            <button
              onClick={() => setActivityTab("feed")}
              className={`flex items-center gap-1 px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                activityTab === "feed" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"
              }`}
            >
              <Activity className="w-3 h-3" />
              Live Feed
            </button>
            <button
              onClick={() => setActivityTab("history")}
              className={`flex items-center gap-1 px-2.5 py-1 rounded text-xs font-medium transition-colors ${
                activityTab === "history" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"
              }`}
            >
              <History className="w-3 h-3" />
              Run History
            </button>
          </div>
          <span className="text-xs text-gray-400 ml-auto">
            {activityTab === "feed" ? `${activity.length} events` : `${runHistory.length} runs`}
          </span>
        </div>

        {activityOpen && (
          <div className="border-t border-gray-100">
            {activityTab === "feed" ? (
              /* Live Feed */
              <div ref={activityRef} className="divide-y divide-gray-50 max-h-56 overflow-y-auto">
                {activity.length === 0 ? (
                  <div className="p-4 text-sm text-gray-400 text-center">
                    {running
                      ? "Pipeline running — events will appear as stages complete..."
                      : "No activity yet. Start a pipeline to see events."}
                  </div>
                ) : (
                  activity.map((entry) => (
                    <div key={entry.id} className="flex items-center gap-3 px-4 py-1.5 text-xs">
                      <span className="text-gray-400 font-mono w-16 shrink-0">{entry.time}</span>
                      <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                        entry.status === "running" ? "bg-forge-500" :
                        entry.status === "completed" ? "bg-green-500" :
                        entry.status === "failed" ? "bg-red-500" :
                        "bg-gray-300"
                      }`} />
                      <span className="font-medium text-gray-700 w-28 shrink-0 truncate">{stageLabel(entry.nodeId)}</span>
                      <span className="text-gray-500 capitalize w-16 shrink-0">{entry.status}</span>
                      {entry.item && (
                        <span className="text-gray-400 truncate">{entry.item}</span>
                      )}
                    </div>
                  ))
                )}
              </div>
            ) : (
              /* Run History */
              <div className="divide-y divide-gray-50 max-h-56 overflow-y-auto">
                {runHistory.length === 0 ? (
                  <div className="p-4 text-sm text-gray-400 text-center">No pipeline runs recorded yet.</div>
                ) : (
                  runHistory.map((job) => (
                    <div key={job.id} className="px-4 py-1.5">
                      <div
                        className="flex items-center gap-3 text-xs cursor-pointer"
                        onClick={() => setExpandedJob(expandedJob === job.id ? null : job.id)}
                      >
                        <span className={`w-1.5 h-1.5 rounded-full shrink-0 ${
                          job.status === "completed" ? "bg-green-500" : job.status === "failed" ? "bg-red-500" : "bg-gray-300"
                        }`} />
                        <span className="font-medium text-gray-700 w-28 truncate">{job.stages}</span>
                        <span className="text-gray-500 capitalize w-16">{job.status}</span>
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
        )}
      </div>
    </div>
  );
}
