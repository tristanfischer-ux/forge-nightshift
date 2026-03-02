import { useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  Building2,
  Mail,
  Upload,
  Search,
  Play,
  Square,
  Activity,
  AlertCircle,
  CheckCircle,
} from "lucide-react";
import StatCard from "../components/StatCard";
import ChartCard from "../components/ChartCard";
import {
  getStats,
  getPipelineStatus,
  startPipeline,
  stopPipeline,
  getRunLog,
  getAnalytics,
  onPipelineStatus,
  onPipelineProgress,
} from "../lib/tauri";
import type { AnalyticsData } from "../lib/tauri";

interface PipelineState {
  running: boolean;
  cancelling: boolean;
}

export default function Dashboard() {
  const navigate = useNavigate();
  const [stats, setStats] = useState<Record<string, unknown> | null>(null);
  const [analytics, setAnalytics] = useState<AnalyticsData | null>(null);
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
      const [s, p, l, a] = await Promise.all([
        getStats(),
        getPipelineStatus(),
        getRunLog(undefined, 20),
        getAnalytics(),
      ]);
      setStats(s);
      setPipeline(p);
      setLogs(l);
      setAnalytics(a);
      setError(null);
    } catch (e) {
      setError(String(e));
    }
  }

  async function handleStartPipeline() {
    try {
      await startPipeline(["research", "enrich", "push"]);
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

  function getStatCount(data: unknown[] | undefined, status?: string): number {
    if (!Array.isArray(data)) return 0;
    if (!status)
      return data.reduce((sum: number, item) => {
        const row = item as Record<string, unknown>;
        return sum + (Number(row.count) || 0);
      }, 0 as number);
    const row = data.find(
      (item) => (item as Record<string, unknown>).status === status
    ) as Record<string, unknown> | undefined;
    return Number(row?.count) || 0;
  }

  function drillDown(param: string, value: string) {
    navigate(`/review?${param}=${encodeURIComponent(value)}`);
  }

  const companiesData = stats?.companies as unknown[] | undefined;
  const emailsData = stats?.emails as unknown[] | undefined;

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Dashboard</h1>
          <p className="text-sm text-gray-500 mt-1">
            Overnight pipeline status & controls
          </p>
        </div>

        <div className="flex gap-2">
          {pipeline.running ? (
            <button
              onClick={handleStopPipeline}
              disabled={pipeline.cancelling}
              className="flex items-center gap-2 px-4 py-2 bg-red-600 hover:bg-red-700 disabled:opacity-50 rounded-lg text-sm font-medium text-white transition-colors"
            >
              <Square className="w-4 h-4" />
              {pipeline.cancelling ? "Stopping..." : "Stop Pipeline"}
            </button>
          ) : (
            <button
              onClick={handleStartPipeline}
              className="flex items-center gap-2 px-4 py-2 bg-forge-600 hover:bg-forge-700 rounded-lg text-sm font-medium text-white transition-colors"
            >
              <Play className="w-4 h-4" />
              Start Pipeline
            </button>
          )}
        </div>
      </div>

      {error && (
        <div className="flex items-center gap-2 p-3 bg-red-50 border border-red-200 rounded-lg text-sm text-red-700">
          <AlertCircle className="w-4 h-4 shrink-0" />
          {error}
        </div>
      )}

      {pipeline.running && (
        <div className="flex items-center gap-3 p-4 bg-blue-50 border border-blue-200 rounded-xl">
          <div className="w-3 h-3 rounded-full bg-orange-500 animate-pulse" />
          <span className="text-sm font-medium text-gray-700">
            Pipeline running...
          </span>
        </div>
      )}

      {/* Stats cards */}
      <div className="grid grid-cols-5 gap-4">
        <StatCard
          label="Companies Found"
          value={getStatCount(companiesData)}
          icon={Building2}
          color="text-blue-600"
        />
        <StatCard
          label="Enriched"
          value={
            getStatCount(companiesData, "enriched") +
            getStatCount(companiesData, "approved") +
            getStatCount(companiesData, "pushed")
          }
          icon={Search}
          color="text-purple-600"
        />
        <StatCard
          label="Approved"
          value={
            getStatCount(companiesData, "approved") +
            getStatCount(companiesData, "pushed")
          }
          icon={CheckCircle}
          color="text-yellow-600"
        />
        <StatCard
          label="Pushed to ForgeOS"
          value={getStatCount(companiesData, "pushed")}
          icon={Upload}
          color="text-green-600"
        />
        <StatCard
          label="Emails Sent"
          value={getStatCount(emailsData, "sent")}
          icon={Mail}
          color="text-orange-600"
        />
      </div>

      {/* Charts row 1: Pipeline Funnel + Country Distribution */}
      <div className="grid grid-cols-2 gap-4">
        <ChartCard
          title="Pipeline Funnel"
          data={analytics?.pipeline_funnel ?? []}
        />
        <ChartCard
          title="Country Distribution"
          data={analytics?.by_country ?? []}
          type="pie"
          onSegmentClick={(name) => drillDown("country", name)}
        />
      </div>

      {/* Charts row 2: Manufacturing Techniques + Certifications */}
      <div className="grid grid-cols-2 gap-4">
        <ChartCard
          title="Manufacturing Techniques"
          data={analytics?.by_subcategory ?? []}
          onSegmentClick={(name) => drillDown("subcategory", name)}
        />
        <ChartCard
          title="Certifications"
          data={analytics?.by_certification ?? []}
          onSegmentClick={(name) => drillDown("search", name)}
        />
      </div>

      {/* Charts row 3: Equipment + Materials */}
      <div className="grid grid-cols-2 gap-4">
        <ChartCard
          title="Top Equipment"
          data={analytics?.by_equipment ?? []}
          onSegmentClick={(name) => drillDown("search", name)}
        />
        <ChartCard
          title="Materials"
          data={analytics?.by_material ?? []}
          onSegmentClick={(name) => drillDown("search", name)}
        />
      </div>

      {/* Charts row 4: Industry Sectors */}
      <div className="grid grid-cols-2 gap-4">
        <ChartCard
          title="Industry Sectors"
          data={analytics?.by_industry ?? []}
          onSegmentClick={(name) => drillDown("search", name)}
        />
        <div />
      </div>

      {/* Recent activity */}
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm">
        <div className="flex items-center gap-2 p-4 border-b border-gray-200">
          <Activity className="w-4 h-4 text-gray-400" />
          <h2 className="text-sm font-semibold text-gray-900">
            Recent Activity
          </h2>
        </div>
        <div className="divide-y divide-gray-100 max-h-80 overflow-y-auto">
          {logs.length === 0 ? (
            <div className="p-8 text-center text-gray-400 text-sm">
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
                  <p className="text-sm text-gray-700 truncate">
                    {String(log.message || "")}
                  </p>
                  <p className="text-xs text-gray-400 mt-0.5">
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
