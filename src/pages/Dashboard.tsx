import { useEffect, useState } from "react";
import {
  Building2,
  Mail,
  Upload,
  Search,
  Play,
  Square,
  Activity,
  AlertCircle,
} from "lucide-react";
import StatCard from "../components/StatCard";
import {
  getStats,
  getPipelineStatus,
  startPipeline,
  stopPipeline,
  getRunLog,
  onPipelineStatus,
  onPipelineProgress,
} from "../lib/tauri";

interface PipelineState {
  running: boolean;
  cancelling: boolean;
}

export default function Dashboard() {
  const [stats, setStats] = useState<Record<string, unknown> | null>(null);
  const [pipeline, setPipeline] = useState<PipelineState>({
    running: false,
    cancelling: false,
  });
  const [logs, setLogs] = useState<Record<string, unknown>[]>([]);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    loadData();

    const unlistenStatus = onPipelineStatus((payload) => {
      setPipeline({
        running: payload.status === "running",
        cancelling: payload.status === "cancelling",
      });
      loadData();
    });

    const unlistenProgress = onPipelineProgress(() => {
      loadData();
    });

    return () => {
      unlistenStatus.then((fn) => fn());
      unlistenProgress.then((fn) => fn());
    };
  }, []);

  async function loadData() {
    try {
      const [s, p, l] = await Promise.all([
        getStats(),
        getPipelineStatus(),
        getRunLog(undefined, 20),
      ]);
      setStats(s);
      setPipeline(p);
      setLogs(l);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleStartPipeline() {
    try {
      await startPipeline([
        "research",
        "enrich",
        "push",
        "outreach",
        "report",
      ]);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleStopPipeline() {
    try {
      await stopPipeline();
    } catch (e) {
      setError(String(e));
    }
  }

  function getStatCount(
    data: unknown[] | undefined,
    status?: string
  ): number {
    if (!Array.isArray(data)) return 0;
    if (!status) return data.reduce((sum: number, item) => {
      const row = item as Record<string, unknown>;
      return sum + (Number(row.count) || 0);
    }, 0 as number);
    const row = data.find(
      (item) => (item as Record<string, unknown>).status === status
    ) as Record<string, unknown> | undefined;
    return Number(row?.count) || 0;
  }

  const companiesData = stats?.companies as unknown[] | undefined;
  const emailsData = stats?.emails as unknown[] | undefined;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold">Dashboard</h1>
          <p className="text-sm text-forge-400 mt-1">
            Overnight pipeline status & controls
          </p>
        </div>

        <div className="flex gap-2">
          {pipeline.running ? (
            <button
              onClick={handleStopPipeline}
              disabled={pipeline.cancelling}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-sm font-medium transition-colors"
            >
              <Square className="w-4 h-4" />
              {pipeline.cancelling ? "Stopping..." : "Stop Pipeline"}
            </button>
          ) : (
            <button
              onClick={handleStartPipeline}
              className="flex items-center gap-2 px-4 py-2 bg-forge-600 hover:bg-forge-700 rounded-lg text-sm font-medium transition-colors"
            >
              <Play className="w-4 h-4" />
              Start Pipeline
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="flex items-center gap-2 p-3 bg-red-900/30 border border-red-800/50 rounded-lg text-sm text-red-300">
          <AlertCircle className="w-4 h-4 shrink-0" />
          {error}
        </div>
      )}

      {/* Pipeline status banner */}
      {pipeline.running && (
        <div className="flex items-center gap-3 p-4 bg-forge-700/30 border border-forge-600/50 rounded-xl">
          <div className="w-3 h-3 rounded-full bg-orange-500 animate-pulse" />
          <span className="text-sm font-medium">Pipeline running...</span>
        </div>
      )}

      {/* Stats cards */}
      <div className="grid grid-cols-4 gap-4">
        <StatCard
          label="Companies Found"
          value={getStatCount(companiesData)}
          icon={Building2}
          color="text-blue-400"
        />
        <StatCard
          label="Enriched"
          value={getStatCount(companiesData, "enriched")}
          icon={Search}
          color="text-purple-400"
        />
        <StatCard
          label="Pushed to ForgeOS"
          value={getStatCount(companiesData, "pushed")}
          icon={Upload}
          color="text-green-400"
        />
        <StatCard
          label="Emails Sent"
          value={getStatCount(emailsData, "sent")}
          icon={Mail}
          color="text-orange-400"
        />
      </div>

      {/* Recent activity */}
      <div className="bg-forge-900/50 rounded-xl border border-forge-800/50">
        <div className="flex items-center gap-2 p-4 border-b border-forge-800/50">
          <Activity className="w-4 h-4 text-forge-400" />
          <h2 className="text-sm font-semibold">Recent Activity</h2>
        </div>
        <div className="divide-y divide-forge-800/30 max-h-80 overflow-y-auto">
          {logs.length === 0 ? (
            <div className="p-8 text-center text-forge-500 text-sm">
              No activity yet. Start a pipeline run to begin discovering
              companies.
            </div>
          ) : (
            logs.map((log, i) => (
              <div key={i} className="flex items-start gap-3 px-4 py-3">
                <div
                  className={`mt-1 w-2 h-2 rounded-full shrink-0 ${
                    log.level === "error"
                      ? "bg-red-500"
                      : log.level === "warn"
                        ? "bg-yellow-500"
                        : "bg-forge-500"
                  }`}
                />
                <div className="flex-1 min-w-0">
                  <p className="text-sm text-forge-200 truncate">
                    {String(log.message || "")}
                  </p>
                  <p className="text-xs text-forge-500 mt-0.5">
                    {String(log.stage || "")} &middot;{" "}
                    {String(log.created_at || "")}
                  </p>
                </div>
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}
