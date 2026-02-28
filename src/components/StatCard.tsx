import { type LucideIcon } from "lucide-react";

interface StatCardProps {
  label: string;
  value: string | number;
  icon: LucideIcon;
  color?: string;
}

export default function StatCard({
  label,
  value,
  icon: Icon,
  color = "text-forge-400",
}: StatCardProps) {
  return (
    <div className="bg-forge-900/50 rounded-xl border border-forge-800/50 p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs text-forge-400 uppercase tracking-wide">
          {label}
        </span>
        <Icon className={`w-4 h-4 ${color}`} />
      </div>
      <div className="text-2xl font-bold">{value}</div>
    </div>
  );
}
