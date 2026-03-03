import { Routes, Route } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import UpdateBanner from "./components/UpdateBanner";
import Dashboard from "./pages/Dashboard";
import Research from "./pages/Research";
import MapPage from "./pages/Map";
import Review from "./pages/Review";
import Outreach from "./pages/Outreach";
import Settings from "./pages/Settings";

function App() {
  return (
    <div className="flex h-screen bg-gray-50 text-gray-900">
      <Sidebar />
      <main className="flex-1 overflow-y-auto p-6">
        <UpdateBanner />
        <Routes>
          <Route path="/" element={<Dashboard />} />
          <Route path="/research" element={<Research />} />
          <Route path="/map" element={<MapPage />} />
          <Route path="/review" element={<Review />} />
          <Route path="/outreach" element={<Outreach />} />
          <Route path="/settings" element={<Settings />} />
        </Routes>
      </main>
    </div>
  );
}

export default App;
