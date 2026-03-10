import { useState, useEffect } from "react";
import { Routes, Route } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import UpdateBanner from "./components/UpdateBanner";
import ErrorBoundary from "./components/ErrorBoundary";
import ErrorToast from "./components/ErrorToast";
import { ErrorProvider } from "./contexts/ErrorContext";
import Dashboard from "./pages/Dashboard";
import Research from "./pages/Research";
import MapPage from "./pages/Map";
import Review from "./pages/Review";
import Outreach from "./pages/Outreach";
import Settings from "./pages/Settings";
import Pipeline from "./pages/Pipeline";
import CommandPalette from "./components/CommandPalette";

function NotFound() {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center">
      <h1 className="text-4xl font-bold text-gray-300 mb-2">404</h1>
      <p className="text-sm text-gray-500">Page not found</p>
    </div>
  );
}

function App() {
  const [cmdOpen, setCmdOpen] = useState(false);
  const [isDense, setIsDense] = useState(() => localStorage.getItem("nightshift-density") === "dense");

  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setCmdOpen((o) => !o);
      }
    }
    function handleDensity(e: Event) {
      setIsDense((e as CustomEvent).detail);
    }
    document.addEventListener("keydown", handleKeyDown);
    window.addEventListener("nightshift-density", handleDensity);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      window.removeEventListener("nightshift-density", handleDensity);
    };
  }, []);

  return (
    <ErrorBoundary>
      <ErrorProvider>
        <div className="flex h-screen bg-gray-50 text-gray-900">
          <Sidebar />
          <main className={`flex-1 overflow-y-auto p-6${isDense ? " dense" : ""}`}>
            <UpdateBanner />
            <Routes>
              <Route path="/" element={<Dashboard />} />
              <Route path="/pipeline" element={<Pipeline />} />
              <Route path="/research" element={<Research />} />
              <Route path="/map" element={<MapPage />} />
              <Route path="/review" element={<Review />} />
              <Route path="/outreach" element={<Outreach />} />
              <Route path="/settings" element={<Settings />} />
              <Route path="*" element={<NotFound />} />
            </Routes>
          </main>
        </div>
        <CommandPalette open={cmdOpen} onClose={() => setCmdOpen(false)} />
        <ErrorToast />
      </ErrorProvider>
    </ErrorBoundary>
  );
}

export default App;
