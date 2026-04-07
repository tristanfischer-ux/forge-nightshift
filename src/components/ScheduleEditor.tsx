import { useState } from "react";
import { Clock, Plus, Trash2, X } from "lucide-react";

interface Schedule {
  id: string;
  name: string;
  enabled: boolean;
  type: "interval" | "daily";
  interval_hours?: number;
  time?: string;
  stages: string[];
  last_run_at?: string;
}

interface ScheduleEditorProps {
  schedules: Schedule[];
  templateId?: string;
  onChange: (schedules: Schedule[]) => void;
}

function relativeTime(iso: string | undefined | null): string {
  if (!iso) return "Never run";
  const diff = Date.now() - new Date(iso).getTime();
  if (diff < 0) return "Just now";
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) {
    const remMins = mins % 60;
    return remMins > 0 ? `${hours}h ${remMins}m ago` : `${hours}h ago`;
  }
  if (hours < 48) return "Yesterday";
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function typeDescription(s: Schedule): string {
  if (s.type === "interval") return `Every ${s.interval_hours ?? 1}h`;
  return `Daily at ${s.time ?? "09:00"}`;
}

interface Preset {
  label: string;
  stages: string[];
  needsTemplate?: boolean;
}

function getPresets(templateId?: string): Preset[] {
  return [
    {
      label: "Full Pipeline",
      stages: ["research", "enrich", "push", "learn_outreach", `template_outreach:${templateId}`],
      needsTemplate: true,
    },
    {
      label: "Discovery Only",
      stages: ["research", "enrich", "push"],
    },
    {
      label: "Full + Deep",
      stages: ["research", "enrich", "deep_enrich_drain"],
    },
    {
      label: "Backfill Only",
      stages: ["deep_enrich_all", "aggregate_techniques", "push_techniques"],
    },
    {
      label: "Outreach Only",
      stages: ["learn_outreach", `template_outreach:${templateId}`],
      needsTemplate: true,
    },
  ];
}

export default function ScheduleEditor({
  schedules,
  templateId,
  onChange,
}: ScheduleEditorProps) {
  const [showForm, setShowForm] = useState(false);
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null);

  // Form state
  const [name, setName] = useState("");
  const [stages, setStages] = useState<string[]>([]);
  const [freqType, setFreqType] = useState<"interval" | "daily">("interval");
  const [intervalHours, setIntervalHours] = useState(6);
  const [dailyTime, setDailyTime] = useState("09:00");

  const presets = getPresets(templateId);

  function resetForm() {
    setName("");
    setStages([]);
    setFreqType("interval");
    setIntervalHours(6);
    setDailyTime("09:00");
    setShowForm(false);
  }

  function handlePreset(preset: Preset) {
    setName(preset.label);
    setStages(preset.stages);
  }

  function handleAdd() {
    if (!name.trim() || stages.length === 0) return;
    const newSchedule: Schedule = {
      id: crypto.randomUUID(),
      name: name.trim(),
      enabled: true,
      type: freqType,
      ...(freqType === "interval" ? { interval_hours: intervalHours } : { time: dailyTime }),
      stages,
    };
    onChange([...schedules, newSchedule]);
    resetForm();
  }

  function handleToggle(id: string) {
    onChange(
      schedules.map((s) => (s.id === id ? { ...s, enabled: !s.enabled } : s))
    );
  }

  function handleDelete(id: string) {
    onChange(schedules.filter((s) => s.id !== id));
    setDeleteConfirmId(null);
  }

  return (
    <div className="space-y-3">
      {/* Existing schedules */}
      {schedules.map((s) => (
        <div
          key={s.id}
          className={`bg-white rounded-lg border border-gray-200 shadow-sm p-3 ${
            !s.enabled ? "opacity-60" : ""
          }`}
        >
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-2 min-w-0">
              <label className="relative inline-flex items-center cursor-pointer shrink-0">
                <input
                  type="checkbox"
                  checked={s.enabled}
                  onChange={() => handleToggle(s.id)}
                  className="sr-only peer"
                />
                <div className="w-8 h-4 bg-gray-200 peer-checked:bg-forge-600 rounded-full transition-colors after:content-[''] after:absolute after:top-0.5 after:left-[2px] after:bg-white after:rounded-full after:h-3 after:w-3 after:transition-all peer-checked:after:translate-x-4" />
              </label>
              <span className="text-sm font-medium text-gray-900 truncate">
                {s.name}
              </span>
            </div>
            <div className="flex items-center gap-3 shrink-0">
              <div className="flex items-center gap-1 text-xs text-gray-500">
                <Clock className="w-3 h-3" />
                <span>{typeDescription(s)}</span>
              </div>
              {deleteConfirmId === s.id ? (
                <div className="flex items-center gap-1">
                  <button
                    onClick={() => handleDelete(s.id)}
                    className="px-2 py-0.5 text-xs bg-red-600 text-white rounded hover:bg-red-700 transition-colors"
                  >
                    Delete
                  </button>
                  <button
                    onClick={() => setDeleteConfirmId(null)}
                    className="p-0.5 text-gray-400 hover:text-gray-600"
                  >
                    <X className="w-3 h-3" />
                  </button>
                </div>
              ) : (
                <button
                  onClick={() => setDeleteConfirmId(s.id)}
                  className="p-1 text-gray-400 hover:text-red-500 transition-colors"
                >
                  <Trash2 className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
          </div>
          <div className="mt-1.5 flex items-center gap-2">
            <span className="text-xs text-gray-400">
              {relativeTime(s.last_run_at)}
            </span>
            <span className="text-xs text-gray-300">|</span>
            <span className="text-xs text-gray-400 truncate">
              {s.stages.join(" → ")}
            </span>
          </div>
        </div>
      ))}

      {/* Add button / form */}
      {!showForm ? (
        <button
          onClick={() => setShowForm(true)}
          className="flex items-center gap-1.5 text-sm text-forge-600 hover:text-forge-700 font-medium transition-colors"
        >
          <Plus className="w-4 h-4" />
          Add Schedule
        </button>
      ) : (
        <div className="bg-white rounded-lg border border-gray-200 shadow-sm p-4 space-y-4">
          {/* Name */}
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">
              Name
            </label>
            <input
              type="text"
              value={name}
              onChange={(e) => setName(e.target.value)}
              placeholder="Schedule name"
              className="w-full px-3 py-1.5 text-sm border border-gray-200 rounded-lg bg-white text-gray-900 placeholder-gray-400 focus:outline-none focus:ring-2 focus:ring-forge-600/20 focus:border-forge-600"
            />
          </div>

          {/* Presets */}
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              Preset
            </label>
            <div className="flex flex-wrap gap-1.5">
              {presets.map((p) => {
                const disabled = p.needsTemplate && !templateId;
                return (
                  <button
                    key={p.label}
                    onClick={() => !disabled && handlePreset(p)}
                    disabled={disabled}
                    className={`px-2.5 py-1 text-xs rounded-md border transition-colors ${
                      disabled
                        ? "border-gray-100 text-gray-300 bg-gray-50 cursor-not-allowed"
                        : stages === p.stages || (name === p.label && stages.length > 0)
                          ? "border-forge-600 bg-forge-50 text-forge-700 font-medium"
                          : "border-gray-200 text-gray-600 bg-white hover:border-forge-300 hover:text-forge-600"
                    }`}
                  >
                    {p.label}
                  </button>
                );
              })}
            </div>
          </div>

          {/* Frequency */}
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1.5">
              Frequency
            </label>
            <div className="space-y-2">
              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="radio"
                  name="freq"
                  checked={freqType === "interval"}
                  onChange={() => setFreqType("interval")}
                  className="accent-forge-600"
                />
                <span className="text-sm text-gray-700">Every</span>
                <select
                  value={intervalHours}
                  onChange={(e) => setIntervalHours(Number(e.target.value))}
                  disabled={freqType !== "interval"}
                  className="px-2 py-1 text-sm border border-gray-200 rounded-md bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-600/20 focus:border-forge-600 disabled:opacity-40"
                >
                  {Array.from({ length: 24 }, (_, i) => i + 1).map((h) => (
                    <option key={h} value={h}>
                      {h}
                    </option>
                  ))}
                </select>
                <span className="text-sm text-gray-700">hours</span>
              </label>

              <label className="flex items-center gap-2 cursor-pointer">
                <input
                  type="radio"
                  name="freq"
                  checked={freqType === "daily"}
                  onChange={() => setFreqType("daily")}
                  className="accent-forge-600"
                />
                <span className="text-sm text-gray-700">Daily at</span>
                <input
                  type="time"
                  value={dailyTime}
                  onChange={(e) => setDailyTime(e.target.value)}
                  disabled={freqType !== "daily"}
                  className="px-2 py-1 text-sm border border-gray-200 rounded-md bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-600/20 focus:border-forge-600 disabled:opacity-40"
                />
              </label>
            </div>
          </div>

          {/* Actions */}
          <div className="flex justify-end gap-2 pt-1">
            <button
              onClick={resetForm}
              className="px-3 py-1.5 text-sm font-medium text-gray-700 bg-white border border-gray-200 rounded-lg hover:bg-gray-50 transition-colors"
            >
              Cancel
            </button>
            <button
              onClick={handleAdd}
              disabled={!name.trim() || stages.length === 0}
              className="px-3 py-1.5 text-sm font-medium text-white bg-forge-600 rounded-lg hover:bg-forge-700 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              Add Schedule
            </button>
          </div>
        </div>
      )}
    </div>
  );
}
