import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  Search,
  CheckSquare,
  Mail,
  Settings,
  Moon,
} from "lucide-react";

const navItems = [
  { path: "/", label: "Dashboard", icon: LayoutDashboard },
  { path: "/research", label: "Research", icon: Search },
  { path: "/review", label: "Review", icon: CheckSquare },
  { path: "/outreach", label: "Outreach", icon: Mail },
  { path: "/settings", label: "Settings", icon: Settings },
];

export default function Sidebar() {
  return (
    <aside className="w-56 bg-forge-900/50 border-r border-forge-800/50 flex flex-col">
      <div className="p-4 border-b border-forge-800/50">
        <div className="flex items-center gap-2">
          <Moon className="w-5 h-5 text-forge-400" />
          <div>
            <h1 className="text-sm font-semibold text-white">
              Forge Nightshift
            </h1>
            <p className="text-[10px] text-forge-400">v0.1.0</p>
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
                  ? "bg-forge-700/50 text-white"
                  : "text-forge-300 hover:bg-forge-800/50 hover:text-white"
              }`
            }
          >
            <item.icon className="w-4 h-4" />
            {item.label}
          </NavLink>
        ))}
      </nav>

      <div className="p-3 border-t border-forge-800/50">
        <div className="flex items-center gap-2">
          <div className="w-2 h-2 rounded-full bg-green-500" />
          <span className="text-xs text-forge-400">Idle</span>
        </div>
      </div>
    </aside>
  );
}
