import { useState, useEffect, useRef } from "react";
import { useNavigate } from "react-router-dom";
import { useError } from "../contexts/ErrorContext";
import {
  LayoutDashboard,
  Workflow,
  Search,
  MapPin,
  CheckSquare,
  Mail,
  Settings,
  Play,
  Square,
  HardDrive,
  RefreshCw,
  CheckCircle,
  Upload,
} from "lucide-react";
import {
  startPipeline,
  stopPipeline,
  backupDatabase,
  approveAllEnriched,
  getPipelineStatus,
} from "../lib/tauri";

interface Action {
  id: string;
  label: string;
  category: string;
  icon: React.ReactNode;
  action: () => void;
}

export default function CommandPalette({
  open,
  onClose,
}: {
  open: boolean;
  onClose: () => void;
}) {
  const navigate = useNavigate();
  const { showError } = useError();
  const [query, setQuery] = useState("");
  const [selectedIndex, setSelectedIndex] = useState(0);
  const inputRef = useRef<HTMLInputElement>(null);

  const actions: Action[] = [
    { id: "nav-dashboard", label: "Go to Dashboard", category: "Navigate", icon: <LayoutDashboard className="w-4 h-4" />, action: () => { navigate("/"); onClose(); } },
    { id: "nav-pipeline", label: "Go to Pipeline", category: "Navigate", icon: <Workflow className="w-4 h-4" />, action: () => { navigate("/pipeline"); onClose(); } },
    { id: "nav-research", label: "Go to Research", category: "Navigate", icon: <Search className="w-4 h-4" />, action: () => { navigate("/research"); onClose(); } },
    { id: "nav-map", label: "Go to Map", category: "Navigate", icon: <MapPin className="w-4 h-4" />, action: () => { navigate("/map"); onClose(); } },
    { id: "nav-review", label: "Go to Review", category: "Navigate", icon: <CheckSquare className="w-4 h-4" />, action: () => { navigate("/review"); onClose(); } },
    { id: "nav-outreach", label: "Go to Outreach", category: "Navigate", icon: <Mail className="w-4 h-4" />, action: () => { navigate("/outreach"); onClose(); } },
    { id: "nav-settings", label: "Go to Settings", category: "Navigate", icon: <Settings className="w-4 h-4" />, action: () => { navigate("/settings"); onClose(); } },
    {
      id: "pipeline-start", label: "Start Full Pipeline", category: "Pipeline", icon: <Play className="w-4 h-4" />,
      action: async () => {
        try {
          const status = await getPipelineStatus();
          if (!status.running) await startPipeline(["research", "enrich", "deep_enrich_all", "aggregate_techniques", "push_techniques"]);
        } catch (e) { showError(`Failed to start pipeline: ${e}`); }
        onClose();
      },
    },
    {
      id: "pipeline-stop", label: "Stop Pipeline", category: "Pipeline", icon: <Square className="w-4 h-4" />,
      action: async () => {
        try { await stopPipeline(); } catch (e) { showError(`Failed to stop pipeline: ${e}`); }
        onClose();
      },
    },
    {
      id: "data-backup", label: "Backup Database", category: "Data", icon: <HardDrive className="w-4 h-4" />,
      action: async () => {
        try { await backupDatabase(); } catch (e) { showError(`Backup failed: ${e}`); }
        onClose();
      },
    },
    {
      id: "data-refresh", label: "Refresh Stats", category: "Data", icon: <RefreshCw className="w-4 h-4" />,
      action: () => { window.location.reload(); },
    },
    {
      id: "quick-approve", label: "Approve All Enriched", category: "Quick", icon: <CheckCircle className="w-4 h-4" />,
      action: async () => {
        try { await approveAllEnriched(); } catch (e) { showError(`Failed to approve all: ${e}`); }
        onClose();
      },
    },
    {
      id: "quick-push", label: "Push All Approved", category: "Quick", icon: <Upload className="w-4 h-4" />,
      action: async () => {
        try {
          const status = await getPipelineStatus();
          if (!status.running) await startPipeline(["push"]);
        } catch (e) { showError(`Failed to push: ${e}`); }
        onClose();
      },
    },
  ];

  const filtered = query.trim()
    ? actions.filter((a) =>
        a.label.toLowerCase().includes(query.toLowerCase()) ||
        a.category.toLowerCase().includes(query.toLowerCase())
      )
    : actions;

  useEffect(() => {
    if (open) {
      setQuery("");
      setSelectedIndex(0);
      const timer = setTimeout(() => inputRef.current?.focus(), 50);
      return () => clearTimeout(timer);
    }
  }, [open]);

  useEffect(() => {
    setSelectedIndex(0);
  }, [query]);

  const safeIndex = Math.max(0, Math.min(selectedIndex, filtered.length - 1));

  function handleKeyDown(e: React.KeyboardEvent) {
    if (e.key === "ArrowDown") {
      e.preventDefault();
      setSelectedIndex((i) => Math.min(i + 1, filtered.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSelectedIndex((i) => Math.max(i - 1, 0));
    } else if (e.key === "Enter" && filtered.length > 0 && filtered[safeIndex]) {
      e.preventDefault();
      filtered[safeIndex].action();
    } else if (e.key === "Escape") {
      onClose();
    }
  }

  if (!open) return null;

  return (
    <div data-command-palette className="fixed inset-0 z-50 flex items-start justify-center pt-[20vh]">
      <div className="fixed inset-0 bg-black/30" onClick={onClose} />
      <div className="relative w-[520px] bg-white rounded-xl shadow-2xl border border-gray-200 overflow-hidden">
        <div className="px-4 py-3 border-b border-gray-100">
          <input
            ref={inputRef}
            type="text"
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={handleKeyDown}
            placeholder="Type a command..."
            className="w-full text-sm text-gray-900 placeholder-gray-400 focus:outline-none bg-transparent"
          />
        </div>
        <div className="max-h-[320px] overflow-y-auto py-1">
          {filtered.length === 0 ? (
            <div className="px-4 py-6 text-center text-sm text-gray-400">
              No matching commands
            </div>
          ) : (
            filtered.map((action, i) => (
              <button
                key={action.id}
                onClick={action.action}
                onMouseEnter={() => setSelectedIndex(i)}
                className={`w-full flex items-center gap-3 px-4 py-2.5 text-left text-sm transition-colors ${
                  i === safeIndex
                    ? "bg-forge-50 text-forge-700"
                    : "text-gray-700 hover:bg-gray-50"
                }`}
              >
                <span className="text-gray-400">{action.icon}</span>
                <span className="flex-1">{action.label}</span>
                <span className="text-[10px] text-gray-400 uppercase">
                  {action.category}
                </span>
              </button>
            ))
          )}
        </div>
        <div className="px-4 py-2 border-t border-gray-100 flex items-center gap-4 text-[10px] text-gray-400">
          <span><kbd className="px-1 py-0.5 bg-gray-100 rounded text-[10px]">↑↓</kbd> Navigate</span>
          <span><kbd className="px-1 py-0.5 bg-gray-100 rounded text-[10px]">↵</kbd> Execute</span>
          <span><kbd className="px-1 py-0.5 bg-gray-100 rounded text-[10px]">esc</kbd> Close</span>
        </div>
      </div>
    </div>
  );
}
