import FlowNode from "./FlowNode";
import FlowConnector from "./FlowConnector";
import type { PipelineNodeEvent } from "../lib/tauri";

interface FlowChartProps {
  nodes: Record<string, PipelineNodeEvent | null>;
}

// Layout: positioned nodes with fixed coordinates
// research(left) + enrich(right) -> deep_enrich -> auto_approve -> aggregate -> push_techniques
const NODE_DEFS = [
  { id: "research", label: "Research", x: 30, y: 20 },
  { id: "enrich", label: "Enrich", x: 230, y: 20 },
  { id: "deep_enrich", label: "Deep Enrich", x: 130, y: 140 },
  { id: "auto_approve", label: "Auto-Approve", x: 130, y: 260 },
  { id: "aggregate_techniques", label: "Aggregate Tech.", x: 130, y: 380 },
  { id: "push_techniques", label: "Push Techniques", x: 130, y: 500 },
];

// Node dimensions for connector math
const NODE_W = 192; // w-48 = 12rem = 192px
const NODE_H = 80;  // approximate height

const CONNECTORS = [
  { from: "research", to: "deep_enrich" },
  { from: "enrich", to: "deep_enrich" },
  { from: "deep_enrich", to: "auto_approve" },
  { from: "auto_approve", to: "aggregate_techniques" },
  { from: "aggregate_techniques", to: "push_techniques" },
];

function getNodePos(id: string) {
  return NODE_DEFS.find((n) => n.id === id) ?? { x: 0, y: 0 };
}

export default function FlowChart({ nodes }: FlowChartProps) {
  return (
    <div className="relative bg-white rounded-xl border border-gray-200 shadow-sm min-h-[400px]" style={{ height: "calc(100vh - 280px)" }}>
      <svg className="absolute inset-0 w-full h-full pointer-events-none" style={{ zIndex: 0 }}>
        {CONNECTORS.map((conn) => {
          const from = getNodePos(conn.from);
          const to = getNodePos(conn.to);
          const fromStatus = nodes[conn.from]?.status ?? "idle";
          const toStatus = nodes[conn.to]?.status ?? "idle";
          const active = fromStatus === "completed" && toStatus === "running";
          return (
            <FlowConnector
              key={`${conn.from}-${conn.to}`}
              from={{ x: from.x + NODE_W / 2, y: from.y + NODE_H }}
              to={{ x: to.x + NODE_W / 2, y: to.y }}
              active={active}
            />
          );
        })}
      </svg>
      <div className="relative" style={{ zIndex: 1 }}>
        {NODE_DEFS.map((def) => (
          <FlowNode
            key={def.id}
            nodeId={def.id}
            label={def.label}
            state={nodes[def.id] ?? null}
            x={def.x}
            y={def.y}
          />
        ))}
      </div>
    </div>
  );
}
