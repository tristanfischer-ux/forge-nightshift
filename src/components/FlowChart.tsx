import FlowNode from "./FlowNode";
import FlowConnector from "./FlowConnector";
import type { PipelineNodeEvent } from "../lib/tauri";

interface FlowChartProps {
  nodes: Record<string, PipelineNodeEvent | null>;
}

// Layout: 9-node pipeline flow
// research → enrich → deep_enrich → verify → synthesize → director_intel → push → outreach → activity
//
// Row 1 (y=20):  research, enrich, deep_enrich
// Row 2 (y=140): verify, synthesize, director_intel
// Row 3 (y=260): push, outreach, activity
const NODE_DEFS = [
  { id: "research",       label: "Research",       x: 30,  y: 20 },
  { id: "enrich",         label: "Enrich",         x: 230, y: 20 },
  { id: "deep_enrich",    label: "Deep Enrich",    x: 430, y: 20 },
  { id: "verify",         label: "Verify",         x: 30,  y: 140 },
  { id: "synthesize",     label: "Synthesize",     x: 230, y: 140 },
  { id: "director_intel", label: "Director Intel",  x: 430, y: 140 },
  { id: "push",           label: "Push",           x: 30,  y: 260 },
  { id: "outreach",       label: "Outreach",       x: 230, y: 260 },
  { id: "activity",       label: "Activity",       x: 430, y: 260 },
];

// Node dimensions for connector math
const NODE_W = 192; // w-48 = 12rem = 192px
const NODE_H = 80;  // approximate height

const CONNECTORS = [
  { from: "research", to: "enrich" },
  { from: "enrich", to: "deep_enrich" },
  { from: "deep_enrich", to: "verify" },
  { from: "verify", to: "synthesize" },
  { from: "synthesize", to: "director_intel" },
  { from: "director_intel", to: "push" },
  { from: "push", to: "outreach" },
  { from: "outreach", to: "activity" },
];

function getNodePos(id: string) {
  return NODE_DEFS.find((n) => n.id === id) ?? { x: 0, y: 0 };
}

export default function FlowChart({ nodes }: FlowChartProps) {
  return (
    <div className="relative bg-white rounded-xl border border-gray-200 shadow-sm max-h-[420px]" style={{ minHeight: "380px" }}>
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
