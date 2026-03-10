import { createContext, useContext, useState, useCallback, type ReactNode } from "react";

interface Toast {
  id: number;
  message: string;
  level: "error" | "warn" | "info";
}

interface ErrorContextValue {
  showError: (message: string) => void;
  showWarning: (message: string) => void;
  showInfo: (message: string) => void;
  toasts: Toast[];
  dismiss: (id: number) => void;
}

const ErrorContext = createContext<ErrorContextValue | null>(null);

let nextId = 0;

export function ErrorProvider({ children }: { children: ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);

  const addToast = useCallback((message: string, level: Toast["level"]) => {
    const id = ++nextId;
    setToasts((prev) => [...prev, { id, message, level }]);
    setTimeout(() => {
      setToasts((prev) => prev.filter((t) => t.id !== id));
    }, 6000);
  }, []);

  const showError = useCallback((msg: string) => addToast(msg, "error"), [addToast]);
  const showWarning = useCallback((msg: string) => addToast(msg, "warn"), [addToast]);
  const showInfo = useCallback((msg: string) => addToast(msg, "info"), [addToast]);

  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  return (
    <ErrorContext.Provider value={{ showError, showWarning, showInfo, toasts, dismiss }}>
      {children}
    </ErrorContext.Provider>
  );
}

export function useError() {
  const ctx = useContext(ErrorContext);
  if (!ctx) throw new Error("useError must be used within ErrorProvider");
  return ctx;
}
