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
} from "lucide-react";
import { getPipelineStatus } from "../lib/tauri";

const navItems = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/pipeline", label: "Pipeline", icon: Workflow },
  { path: "/research", label: "Research", icon: Search },
  { path: "/map", label: "Map", icon: MapPin },
  { path: "/review", label: "Review", icon: CheckSquare },
  { path: "/outreach", label: "Outreach", icon: Mail },
  { path: "/settings", label: "Settings", icon: Settings },
];

export default function Sidebar() {
  const [ollamaConnected, setOllamaConnected] = useState<boolean | null>(null);
  const [pipelineRunning, setPipelineRunning] = useState(false);
  const [version, setVersion] = useState("");

  useEffect(() => {
    getVersion().then(setVersion);

    // Listen for Ollama status from startup check
    const unlistenOllama = listen<{ connected: boolean }>(
      "ollama:status",
      (event) => {
        setOllamaConnected(event.payload.connected);
      }
    );

    // Listen for pipeline status changes
    const unlistenPipeline = listen<{ status: string }>(
      "pipeline:status",
      (event) => {
        setPipelineRunning(event.payload.status === "running");
      }
    );

    // Check initial pipeline state
    getPipelineStatus()
      .then((s) => setPipelineRunning(s.running))
      .catch(() => {});

    return () => {
      unlistenOllama.then((fn) => fn());
      unlistenPipeline.then((fn) => fn());
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
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="p-3 border-t border-gray-200">
        <div className="flex items-center gap-2">
          <div className={`w-2 h-2 rounded-full ${statusColor}`} />
          <span className="text-xs text-gray-500">{statusText}</span>
        </div>
      </div>
    </aside>
  );
}
