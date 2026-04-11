import { useEffect, useState } from "react";
import { NavLink } from "react-router-dom";
import { listen } from "@tauri-apps/api/event";
import { getVersion } from "@tauri-apps/api/app";
import {
  LayoutDashboard,
  Search,
  MapPin,
  CheckSquare,
  Mail,
  Settings,
  Hammer,
  Workflow,
  Rows3,
  Crosshair,
} from "lucide-react";
import { getPipelineStatus, getStats, getSearchProfiles, getActiveProfile, setActiveProfile } from "../lib/tauri";
import type { SearchProfile } from "../lib/tauri";

const navItems = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/pipeline", label: "Pipeline", icon: Workflow },
  { path: "/research", label: "Research", icon: Search },
  { path: "/map", label: "Map", icon: MapPin },
  { path: "/review", label: "Review", icon: CheckSquare },
  { path: "/deals", label: "Deals", icon: Crosshair },
  { path: "/outreach", label: "Outreach", icon: Mail },
  { path: "/settings", label: "Settings", icon: Settings },
];

function domainDot(domain: string): string {
  if (domain === "manufacturing") return "\u{1F535}";
  if (domain === "cleantech") return "\u{1F7E2}";
  if (domain === "biotech") return "\u{1F7E3}";
  return "\u{26AA}";
}

export default function Sidebar() {
  const [ollamaConnected, setOllamaConnected] = useState<boolean | null>(null);
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [version, setVersion] = useState("");
  const [badges, setBadges] = useState<Record<string, number>>({});
  const [dense, setDense] = useState(() => localStorage.getItem("nightshift-density") === "dense");
  const [profiles, setProfiles] = useState<SearchProfile[]>([]);
  const [activeProfileId, setActiveProfileId] = useState<string>("");

  useEffect(() => {
    getVersion().then(setVersion);

    // Load search profiles
    getSearchProfiles().then(setProfiles).catch(() => {});
    getActiveProfile().then(setActiveProfileId).catch(() => {});

    // Listen for Ollama status from startup check
    const unlistenOllama = listen<{ connected: boolean }>(
      "ollama:status",
      (event) => {
        setOllamaConnected(event.payload.connected);
      }
    );

    // Check initial pipeline state
    getPipelineStatus()
      .then((s) => setPipelineRunning(s.running))
      .catch(() => {});

    // Load badge counts
    function loadBadges() {
      getStats()
        .then((s) => {
          const rows = (s.companies as { status: string; count: number }[]) || [];
          const enriched = rows.find((r) => r.status === "enriched")?.count || 0;
          const errors = rows.find((r) => r.status === "error")?.count || 0;
          const emails = (s.emails as { status: string; count: number }[]) || [];
          const drafts = emails.find((r) => r.status === "draft")?.count || 0;
          setBadges({ "/review": enriched, "/review:errors": errors, "/outreach": drafts });
        })
        .catch(() => {});
    }
    loadBadges();

    // Single pipeline:status listener for both running state and badge refresh
    const unlistenPipeline = listen<{ status: string }>(
      "pipeline:status",
      (event) => {
        setPipelineRunning(event.payload.status === "running");
        loadBadges();
      }
    );

    return () => {
      unlistenOllama.then((fn) => fn()).catch(() => {});
      unlistenPipeline.then((fn) => fn()).catch(() => {});
    };
  }, []);

  const statusColor = pipelineRunning
    ? "bg-orange-500 animate-pulse"
    : ollamaConnected === false
      ? "bg-red-500"
      : "bg-green-500";

  const statusText = pipelineRunning
    ? "Running"
    : ollamaConnected === null
      ? "Connecting..."
      : ollamaConnected
        ? "Ready"
        : "Ollama offline";

  return (
    <aside className="w-56 bg-white border-r border-gray-200 flex flex-col">
      <div className="p-4 border-b border-gray-200">
        <div className="flex items-center gap-2">
          <Hammer className="w-5 h-5 text-forge-600" />
          <div>
            <h1 className="text-sm font-semibold text-gray-900">
              Forge Nightshift
            </h1>
            <p className="text-[10px] text-gray-400">v{version}</p>
          </div>
        </div>
      </div>

      {profiles.length > 0 && (
        <div className="px-3 py-2 border-b border-gray-200">
          <select
            value={activeProfileId}
            onChange={async (e) => {
              const id = e.target.value;
              try {
                await setActiveProfile(id);
                setActiveProfileId(id);
                window.location.reload();
              } catch (err) {
                console.error("Failed to set active profile:", err);
              }
            }}
            className="w-full text-xs border border-gray-200 rounded-md px-2 py-1.5 bg-white text-gray-700 focus:outline-none focus:ring-1 focus:ring-forge-500"
          >
            {profiles.map((p) => (
              <option key={p.id} value={p.id}>
                {domainDot(p.domain)} {p.name}
              </option>
            ))}
          </select>
        </div>
      )}

      <nav className="flex-1 p-2">
        {navItems.map((item) => (
          <NavLink
            key={item.path}
            to={item.path}
            end={item.path === "/"}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2 rounded-lg text-sm transition-colors ${
                isActive
                  ? "bg-forge-50 text-forge-700 font-medium"
                  : "text-gray-600 hover:bg-gray-100 hover:text-gray-900"
              }`
            }
          >
            <item.icon className="w-4 h-4" />
            <span className="flex-1">{item.label}</span>
            {item.path === "/review" ? (
              <span className="flex items-center gap-1">
                {badges["/review"] > 0 && (
                  <span className="bg-forge-100 text-forge-700 text-xs px-1.5 rounded-full">
                    {badges["/review"]}
                  </span>
                )}
                {badges["/review:errors"] > 0 && (
                  <span className="bg-red-100 text-red-700 text-xs px-1.5 rounded-full">
                    {badges["/review:errors"]}
                  </span>
                )}
              </span>
            ) : (
              badges[item.path] != null && badges[item.path] > 0 && (
                <span className="bg-forge-100 text-forge-700 text-xs px-1.5 rounded-full">
                  {badges[item.path]}
                </span>
              )
            )}
          </NavLink>
        ))}
      </nav>

      <div className="p-3 border-t border-gray-200 space-y-2">
        <button
          onClick={() => {
            const next = !dense;
            setDense(next);
            localStorage.setItem("nightshift-density", next ? "dense" : "normal");
            window.dispatchEvent(new CustomEvent("nightshift-density", { detail: next }));
          }}
          className="flex items-center gap-2 w-full px-2 py-1 text-xs text-gray-500 hover:text-gray-700 hover:bg-gray-50 rounded transition-colors"
        >
          <Rows3 className="w-3.5 h-3.5" />
          {dense ? "Comfortable" : "Compact"}
        </button>
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${statusColor}`} />
          <span className="text-xs text-gray-500">{statusText}</span>
        </div>
      </div>
    </aside>
  );
}
