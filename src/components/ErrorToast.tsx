import { AlertCircle, AlertTriangle, Info, X } from "lucide-react";
import { useError } from "../contexts/ErrorContext";

const STYLES = {
  error: "bg-red-50 border-red-200 text-red-700",
  warn: "bg-amber-50 border-amber-200 text-amber-700",
  info: "bg-blue-50 border-blue-200 text-blue-700",
} as const;

const ICONS = {
  error: AlertCircle,
  warn: AlertTriangle,
  info: Info,
} as const;

export default function ErrorToast() {
  const { toasts, dismiss } = useError();

  if (toasts.length === 0) return null;

  return (
    <div className="fixed bottom-4 right-4 z-50 flex flex-col gap-2 max-w-sm">
      {toasts.map((toast) => {
        const Icon = ICONS[toast.level];
        return (
          <div
            key={toast.id}
            className={`flex items-start gap-2 p-3 border rounded-lg shadow-lg text-sm animate-in slide-in-from-right ${STYLES[toast.level]}`}
          >
            <Icon className="w-4 h-4 mt-0.5 shrink-0" />
            <span className="flex-1">{toast.message}</span>
            <button
              onClick={() => dismiss(toast.id)}
              className="shrink-0 hover:opacity-70"
            >
              <X className="w-3.5 h-3.5" />
            </button>
          </div>
        );
      })}
    </div>
  );
}
