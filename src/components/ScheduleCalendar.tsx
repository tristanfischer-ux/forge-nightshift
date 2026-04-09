import { useState, useEffect, useRef } from "react";
import { Clock, Plus, Trash2, X, Edit2 } from "lucide-react";

interface Schedule {
  id: string;
  name: string;
  enabled: boolean;
  type: "interval" | "daily" | "weekly";
  interval_hours?: number;
  time?: string;
  days?: number[];
  stages: string[];
  last_run_at?: string;
}

interface ScheduleCalendarProps {
  schedules: Schedule[];
  templateId?: string;
  onChange: (schedules: Schedule[]) => void;
}

const DAY_LABELS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const DAY_LETTERS = ["S", "M", "T", "W", "T", "F", "S"];
const HOUR_PX = 40;
const HOURS = Array.from({ length: 24 }, (_, i) => i);

const STAGE_COLORS: Record<string, { bg: string; border: string; text: string }> = {
  research: { bg: "bg-blue-100", border: "border-blue-300", text: "text-blue-800" },
  enrich: { bg: "bg-purple-100", border: "border-purple-300", text: "text-purple-800" },
  push: { bg: "bg-green-100", border: "border-green-300", text: "text-green-800" },
  outreach: { bg: "bg-amber-100", border: "border-amber-300", text: "text-amber-800" },
  embeddings: { bg: "bg-indigo-100", border: "border-indigo-300", text: "text-indigo-800" },
};

function getColor(stages: string[]) {
  const first = stages[0] ?? "";
  for (const [key, val] of Object.entries(STAGE_COLORS)) {
    if (first.startsWith(key)) return val;
  }
  return { bg: "bg-sky-100", border: "border-sky-300", text: "text-sky-800" };
}

function relativeTime(iso: string | undefined | null): string {
  if (!iso) return "Never run";
  const diff = Date.now() - new Date(iso).getTime();
  if (diff < 0) return "Just now";
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return "Just now";
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  if (hours < 48) return "Yesterday";
  return `${Math.floor(hours / 24)}d ago`;
}

function typeDescription(s: Schedule): string {
  if (s.type === "interval") return `Every ${s.interval_hours ?? 1}h`;
  if (s.type === "weekly") {
    const dayStr = (s.days ?? []).map((d) => DAY_LABELS[d]).join(", ");
    return `${dayStr} at ${s.time ?? "09:00"}`;
  }
  return `Daily at ${s.time ?? "09:00"}`;
}

interface Preset {
  label: string;
  stages: string[];
  needsTemplate?: boolean;
}

function getPresets(templateId?: string): Preset[] {
  return [
    { label: "Full Pipeline", stages: ["research", "enrich", "push", "learn_outreach", `template_outreach:${templateId}`], needsTemplate: true },
    { label: "Discovery Only", stages: ["research", "enrich", "push"] },
    { label: "Full Pipeline", stages: ["research", "enrich", "verify", "synthesize"] },
    { label: "Techniques", stages: ["aggregate_techniques", "push_techniques"] },
    { label: "Outreach Only", stages: ["learn_outreach", `template_outreach:${templateId}`], needsTemplate: true },
  ];
}

/** Get the hour (0-23) for a schedule, or null if interval */
function getScheduleHour(s: Schedule): number | null {
  if ((s.type === "daily" || s.type === "weekly") && s.time) {
    const h = parseInt(s.time.split(":")[0], 10);
    return isNaN(h) ? null : h;
  }
  return null;
}

/** Get minute offset (0-59) */
function getScheduleMinute(s: Schedule): number {
  if (s.time) {
    const m = parseInt(s.time.split(":")[1], 10);
    return isNaN(m) ? 0 : m;
  }
  return 0;
}

/** For interval schedules, get all fire hours in a day */
function getIntervalHours(s: Schedule): number[] {
  if (s.type !== "interval" || !s.interval_hours) return [];
  const hours: number[] = [];
  const interval = Math.max(1, s.interval_hours);
  for (let h = 0; h < 24; h += interval) {
    hours.push(h);
  }
  return hours;
}

/** Check if a schedule runs on a given day (0=Sun..6=Sat) */
function runsOnDay(s: Schedule, day: number): boolean {
  if (s.type === "daily" || s.type === "interval") return true;
  if (s.type === "weekly") return (s.days ?? []).includes(day);
  return false;
}

export default function ScheduleCalendar({ schedules, templateId, onChange }: ScheduleCalendarProps) {
  const [view, setView] = useState<"day" | "week">("day");
  const [showForm, setShowForm] = useState(false);
  const [editId, setEditId] = useState<string | null>(null);
  const [deleteConfirmId, setDeleteConfirmId] = useState<string | null>(null);
  const [currentMinute, setCurrentMinute] = useState(() => {
    const n = new Date();
    return n.getHours() * 60 + n.getMinutes();
  });
  const scrollRef = useRef<HTMLDivElement>(null);

  // Form state
  const [name, setName] = useState("");
  const [stages, setStages] = useState<string[]>([]);
  const [freqType, setFreqType] = useState<"interval" | "daily" | "weekly">("daily");
  const [intervalHours, setIntervalHours] = useState(6);
  const [dailyTime, setDailyTime] = useState("09:00");
  const [weeklyDays, setWeeklyDays] = useState<number[]>([1, 2, 3, 4, 5]); // Mon-Fri default

  const presets = getPresets(templateId);

  // Update current time line every 60s
  useEffect(() => {
    const interval = setInterval(() => {
      const n = new Date();
      setCurrentMinute(n.getHours() * 60 + n.getMinutes());
    }, 60000);
    return () => clearInterval(interval);
  }, []);

  // Auto-scroll to business hours on mount
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = 6 * HOUR_PX; // Scroll to 06:00
    }
  }, [view]);

  function resetForm() {
    setName("");
    setStages([]);
    setFreqType("daily");
    setIntervalHours(6);
    setDailyTime("09:00");
    setWeeklyDays([1, 2, 3, 4, 5]);
    setShowForm(false);
    setEditId(null);
  }

  function openAddForm(time?: string) {
    resetForm();
    if (time) {
      setDailyTime(time);
    }
    setShowForm(true);
  }

  function openEditForm(s: Schedule) {
    setName(s.name);
    setStages(s.stages);
    setFreqType(s.type);
    setIntervalHours(s.interval_hours ?? 6);
    setDailyTime(s.time ?? "09:00");
    setWeeklyDays(s.days ?? [1, 2, 3, 4, 5]);
    setEditId(s.id);
    setShowForm(true);
  }

  function handlePreset(preset: Preset) {
    setName(preset.label);
    setStages(preset.stages);
  }

  function handleSave() {
    if (!name.trim() || stages.length === 0) return;

    const schedule: Schedule = {
      id: editId ?? crypto.randomUUID(),
      name: name.trim(),
      enabled: editId ? (schedules.find((s) => s.id === editId)?.enabled ?? true) : true,
      type: freqType,
      ...(freqType === "interval" ? { interval_hours: intervalHours } : { time: dailyTime }),
      ...(freqType === "weekly" ? { days: weeklyDays } : {}),
      stages,
      last_run_at: editId ? schedules.find((s) => s.id === editId)?.last_run_at : undefined,
    };

    if (editId) {
      onChange(schedules.map((s) => (s.id === editId ? schedule : s)));
    } else {
      onChange([...schedules, schedule]);
    }
    resetForm();
  }

  function handleToggle(id: string) {
    onChange(schedules.map((s) => (s.id === id ? { ...s, enabled: !s.enabled } : s)));
  }

  function handleDelete(id: string) {
    onChange(schedules.filter((s) => s.id !== id));
    setDeleteConfirmId(null);
  }

  function toggleDay(day: number) {
    setWeeklyDays((prev) =>
      prev.includes(day) ? prev.filter((d) => d !== day) : [...prev, day].sort()
    );
  }

  function handleTimeSlotClick(hour: number) {
    const timeStr = `${hour.toString().padStart(2, "0")}:00`;
    openAddForm(timeStr);
  }

  // Render a schedule block at a given hour position
  function renderBlock(s: Schedule, hour: number, minute: number, isInterval: boolean, compact?: boolean) {
    const color = getColor(s.stages);
    const top = hour * HOUR_PX + (minute / 60) * HOUR_PX;
    const height = Math.max(28, HOUR_PX * 0.7);

    return (
      <div
        key={`${s.id}-${hour}`}
        className={`absolute left-1 right-1 rounded-md border px-1.5 py-0.5 cursor-pointer overflow-hidden transition-opacity ${color.bg} ${color.border} ${color.text} ${
          !s.enabled ? "opacity-40" : ""
        } ${isInterval ? "border-dashed" : ""}`}
        style={{ top: `${top}px`, height: `${height}px` }}
        onClick={(e) => {
          e.stopPropagation();
          openEditForm(s);
        }}
        title={`${s.name}\n${s.stages.join(" → ")}`}
      >
        {!compact && (
          <>
            <div className="text-[10px] font-semibold truncate leading-tight">{s.name}</div>
            <div className="text-[9px] truncate opacity-75 leading-tight">{s.stages.join(" → ")}</div>
          </>
        )}
        {compact && (
          <div className="text-[9px] font-semibold truncate leading-tight mt-0.5">{s.name.slice(0, 3)}</div>
        )}
      </div>
    );
  }

  // Day view
  function renderDayView() {
    const today = new Date().getDay(); // 0=Sun

    return (
      <div className="relative" style={{ height: `${24 * HOUR_PX}px` }}>
        {/* Hour grid lines */}
        {HOURS.map((h) => (
          <div
            key={h}
            className={`absolute left-0 right-0 border-t border-gray-100 ${
              h >= 7 && h < 19 ? "bg-gray-50/50" : ""
            }`}
            style={{ top: `${h * HOUR_PX}px`, height: `${HOUR_PX}px` }}
            onClick={() => handleTimeSlotClick(h)}
          >
            <span className="absolute -top-2.5 left-0 text-[10px] text-gray-400 w-10 text-right pr-2">
              {h.toString().padStart(2, "0")}:00
            </span>
          </div>
        ))}

        {/* Current time line */}
        <div
          className="absolute left-10 right-0 h-px bg-red-400 z-20 pointer-events-none"
          style={{ top: `${(currentMinute / 60) * HOUR_PX}px` }}
        >
          <div className="absolute -left-1.5 -top-1.5 w-3 h-3 rounded-full bg-red-400" />
        </div>

        {/* Schedule blocks */}
        <div className="absolute left-12 right-0">
          {schedules.map((s) => {
            if (s.type === "daily" || (s.type === "weekly" && runsOnDay(s, today))) {
              const hour = getScheduleHour(s);
              if (hour == null) return null;
              return renderBlock(s, hour, getScheduleMinute(s), false);
            }
            if (s.type === "interval") {
              return getIntervalHours(s).map((h) =>
                renderBlock(s, h, 0, true)
              );
            }
            return null;
          })}
        </div>
      </div>
    );
  }

  // Week view
  function renderWeekView() {
    return (
      <div className="relative">
        {/* Day headers */}
        <div className="flex border-b border-gray-200 sticky top-0 bg-white z-10">
          <div className="w-12 shrink-0" />
          {DAY_LABELS.map((label, i) => (
            <div
              key={i}
              className={`flex-1 text-center text-[10px] font-medium py-1 ${
                i === new Date().getDay() ? "text-forge-700 bg-forge-50" : "text-gray-500"
              }`}
            >
              {label}
            </div>
          ))}
        </div>

        <div className="relative" style={{ height: `${24 * HOUR_PX}px` }}>
          {/* Hour grid */}
          {HOURS.map((h) => (
            <div
              key={h}
              className={`absolute left-0 right-0 border-t border-gray-100 ${
                h >= 7 && h < 19 ? "bg-gray-50/30" : ""
              }`}
              style={{ top: `${h * HOUR_PX}px`, height: `${HOUR_PX}px` }}
            >
              <span className="absolute -top-2.5 left-0 text-[10px] text-gray-400 w-10 text-right pr-2">
                {h.toString().padStart(2, "0")}:00
              </span>
            </div>
          ))}

          {/* Current time line */}
          <div
            className="absolute left-12 right-0 h-px bg-red-400 z-20 pointer-events-none"
            style={{ top: `${(currentMinute / 60) * HOUR_PX}px` }}
          />

          {/* Day columns with schedule blocks */}
          <div className="absolute left-12 right-0 top-0 bottom-0 flex">
            {Array.from({ length: 7 }, (_, dayIdx) => (
              <div key={dayIdx} className="flex-1 relative border-l border-gray-100 first:border-l-0">
                {schedules.map((s) => {
                  if (!runsOnDay(s, dayIdx)) return null;

                  if (s.type === "daily" || s.type === "weekly") {
                    const hour = getScheduleHour(s);
                    if (hour == null) return null;
                    return renderBlock(s, hour, getScheduleMinute(s), false, true);
                  }
                  if (s.type === "interval") {
                    return getIntervalHours(s).map((h) =>
                      renderBlock(s, h, 0, true, true)
                    );
                  }
                  return null;
                })}
              </div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {/* View toggle + Add button */}
      <div className="flex items-center justify-between">
        <div className="flex bg-gray-100 rounded-lg p-0.5">
          <button
            onClick={() => setView("day")}
            className={`px-3 py-1 text-xs font-medium rounded-md transition-colors ${
              view === "day" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"
            }`}
          >
            Day
          </button>
          <button
            onClick={() => setView("week")}
            className={`px-3 py-1 text-xs font-medium rounded-md transition-colors ${
              view === "week" ? "bg-white text-gray-900 shadow-sm" : "text-gray-500 hover:text-gray-700"
            }`}
          >
            Week
          </button>
        </div>
        <button
          onClick={() => openAddForm()}
          className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-medium text-white bg-forge-600 rounded-lg hover:bg-forge-700 transition-colors"
        >
          <Plus className="w-3.5 h-3.5" />
          Add
        </button>
      </div>

      {/* Calendar timeline */}
      <div
        ref={scrollRef}
        className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-y-auto"
        style={{ maxHeight: "420px" }}
      >
        <div className="p-2">
          {view === "day" ? renderDayView() : renderWeekView()}
        </div>
      </div>

      {/* Add / Edit form */}
      {showForm && (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4 space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-sm font-semibold text-gray-900">
              {editId ? "Edit Schedule" : "Add Schedule"}
            </h3>
            <button onClick={resetForm} className="p-1 text-gray-400 hover:text-gray-600">
              <X className="w-4 h-4" />
            </button>
          </div>

          {/* Name */}
          <div>
            <label className="block text-xs font-medium text-gray-700 mb-1">Name</label>
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
            <label className="block text-xs font-medium text-gray-700 mb-1.5">Preset</label>
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
                        : name === p.label
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
            <label className="block text-xs font-medium text-gray-700 mb-1.5">Frequency</label>
            <div className="space-y-2.5">
              {/* Interval */}
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
                    <option key={h} value={h}>{h}</option>
                  ))}
                </select>
                <span className="text-sm text-gray-700">hours</span>
              </label>

              {/* Daily */}
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
                  disabled={freqType !== "daily" && freqType !== "weekly"}
                  className="px-2 py-1 text-sm border border-gray-200 rounded-md bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-600/20 focus:border-forge-600 disabled:opacity-40"
                />
              </label>

              {/* Weekly */}
              <div>
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="radio"
                    name="freq"
                    checked={freqType === "weekly"}
                    onChange={() => setFreqType("weekly")}
                    className="accent-forge-600"
                  />
                  <span className="text-sm text-gray-700">Weekly at</span>
                  <input
                    type="time"
                    value={dailyTime}
                    onChange={(e) => setDailyTime(e.target.value)}
                    disabled={freqType !== "weekly"}
                    className="px-2 py-1 text-sm border border-gray-200 rounded-md bg-white text-gray-900 focus:outline-none focus:ring-2 focus:ring-forge-600/20 focus:border-forge-600 disabled:opacity-40"
                  />
                  <span className="text-sm text-gray-700">on</span>
                </label>
                <div className="flex gap-1 mt-1.5 ml-6">
                  {DAY_LETTERS.map((letter, i) => (
                    <button
                      key={i}
                      onClick={() => freqType === "weekly" && toggleDay(i)}
                      disabled={freqType !== "weekly"}
                      className={`w-7 h-7 rounded-full text-xs font-medium transition-colors ${
                        freqType !== "weekly"
                          ? "bg-gray-100 text-gray-300 cursor-not-allowed"
                          : weeklyDays.includes(i)
                            ? "bg-forge-600 text-white"
                            : "bg-gray-100 text-gray-500 hover:bg-gray-200"
                      }`}
                    >
                      {letter}
                    </button>
                  ))}
                </div>
              </div>
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
              onClick={handleSave}
              disabled={!name.trim() || stages.length === 0 || (freqType === "weekly" && weeklyDays.length === 0)}
              className="px-3 py-1.5 text-sm font-medium text-white bg-forge-600 rounded-lg hover:bg-forge-700 transition-colors disabled:opacity-40 disabled:cursor-not-allowed"
            >
              {editId ? "Save Changes" : "Add Schedule"}
            </button>
          </div>
        </div>
      )}

      {/* Schedule list */}
      {schedules.length > 0 && (
        <div className="space-y-2">
          <h3 className="text-xs font-semibold text-gray-500 uppercase tracking-wide">Schedules</h3>
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
                  <span className="text-sm font-medium text-gray-900 truncate">{s.name}</span>
                </div>
                <div className="flex items-center gap-2 shrink-0">
                  <div className="flex items-center gap-1 text-xs text-gray-500">
                    <Clock className="w-3 h-3" />
                    <span>{typeDescription(s)}</span>
                  </div>
                  <button
                    onClick={() => openEditForm(s)}
                    className="p-1 text-gray-400 hover:text-forge-600 transition-colors"
                  >
                    <Edit2 className="w-3.5 h-3.5" />
                  </button>
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
                <span className="text-xs text-gray-400">{relativeTime(s.last_run_at)}</span>
                <span className="text-xs text-gray-300">|</span>
                <span className="text-xs text-gray-400 truncate">{s.stages.join(" → ")}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
