import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
  PieChart,
  Pie,
  Cell,
} from "recharts";
import type { ChartDataPoint } from "../lib/tauri";

const COLORS = [
  "#4c6ef5", "#5c7cfa", "#748ffc", "#91a7ff", "#bac8ff",
  "#364fc7", "#3b5bdb", "#4263eb", "#5c7cfa", "#748ffc",
  "#1e2a5e", "#2b3d8e", "#3651bf", "#4c6ef5", "#6b8aff",
  "#8da4ff", "#aebfff", "#c5d1ff", "#dbe3ff", "#eef1ff",
];

interface ChartCardProps {
  title: string;
  data: ChartDataPoint[];
  type?: "bar" | "pie";
  onSegmentClick?: (name: string) => void;
}

export default function ChartCard({
  title,
  data,
  type = "bar",
  onSegmentClick,
}: ChartCardProps) {
  if (data.length === 0) {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
        <h3 className="text-sm font-semibold text-gray-900 mb-3">{title}</h3>
        <div className="flex items-center justify-center h-48 text-sm text-gray-400">
          No data yet
        </div>
      </div>
    );
  }

  if (type === "pie") {
    return (
      <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
        <h3 className="text-sm font-semibold text-gray-900 mb-3">{title}</h3>
        <ResponsiveContainer width="100%" height={260}>
          <PieChart>
            <Pie
              data={data}
              dataKey="count"
              nameKey="name"
              cx="50%"
              cy="50%"
              outerRadius={90}
              label={({ name, percent }: { name?: string; percent?: number }) =>
                `${name ?? ""} ${((percent ?? 0) * 100).toFixed(0)}%`
              }
              labelLine={false}
              style={{ cursor: onSegmentClick ? "pointer" : "default" }}
              onClick={
                onSegmentClick
                  ? (entry) => onSegmentClick(entry.name)
                  : undefined
              }
            >
              {data.map((_, i) => (
                <Cell key={i} fill={COLORS[i % COLORS.length]} />
              ))}
            </Pie>
            <Tooltip
              contentStyle={{
                borderRadius: "8px",
                border: "1px solid #e5e7eb",
                fontSize: "12px",
              }}
            />
          </PieChart>
        </ResponsiveContainer>
      </div>
    );
  }

  // Horizontal bar chart
  const chartHeight = Math.max(200, data.length * 32);

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
      <h3 className="text-sm font-semibold text-gray-900 mb-3">{title}</h3>
      <ResponsiveContainer width="100%" height={chartHeight}>
        <BarChart
          data={data}
          layout="vertical"
          margin={{ top: 0, right: 20, bottom: 0, left: 0 }}
        >
          <XAxis type="number" tick={{ fontSize: 11, fill: "#6b7280" }} />
          <YAxis
            type="category"
            dataKey="name"
            width={140}
            tick={{ fontSize: 11, fill: "#374151" }}
          />
          <Tooltip
            contentStyle={{
              borderRadius: "8px",
              border: "1px solid #e5e7eb",
              fontSize: "12px",
            }}
          />
          <Bar
            dataKey="count"
            fill="#4c6ef5"
            radius={[0, 4, 4, 0]}
            style={{ cursor: onSegmentClick ? "pointer" : "default" }}
            onClick={
              onSegmentClick
                ? (entry: { name?: string }) =>
                    onSegmentClick(entry.name ?? "")
                : undefined
            }
          />
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
