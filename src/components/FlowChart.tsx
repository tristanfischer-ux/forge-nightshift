import FlowNode from "./FlowNode";
import FlowConnector from "./FlowConnector";
import type { PipelineNodeEvent } from "../lib/tauri";
import { stageLabel, stageTooltip } from "../lib/stage-labels";

interface FlowChartProps {
  nodes: Record<string, PipelineNodeEvent | null>;
}

// Layout: 8-node pipeline flow (2 rows of 4)
// research → enrich → fact-check → analyse → news → search index → investor fit → publish
//
// Row 1 (y=20):  research, enrich, verify, synthesize
// Row 2 (y=140): activity, embeddings, investor_match, push
const NODE_DEFS = [
  { id: "research",       label: stageLabel("research"),         tooltip: stageTooltip("research"),         x: 15,  y: 20 },
  { id: "enrich",         label: stageLabel("enrich"),           tooltip: stageTooltip("enrich"),           x: 175, y: 20 },
  { id: "verify",         label: stageLabel("verify"),           tooltip: stageTooltip("verify"),           x: 335, y: 20 },
  { id: "synthesize",     label: stageLabel("synthesize"),       tooltip: stageTooltip("synthesize"),       x: 495, y: 20 },
  { id: "activity",       label: stageLabel("activities"),       tooltip: stageTooltip("activities"),       x: 15,  y: 140 },
  { id: "embeddings",     label: stageLabel("embeddings"),       tooltip: stageTooltip("embeddings"),       x: 175, y: 140 },
  { id: "investor_match", label: stageLabel("investor_matches"), tooltip: stageTooltip("investor_matches"), x: 335, y: 140 },
  { id: "push",           label: stageLabel("push"),             tooltip: stageTooltip("push"),             x: 495, y: 140 },
];

// Node dimensions for connector math
const NODE_W = 192; // w-48 = 12rem = 192px
const NODE_H = 80;  // approximate height

const CONNECTORS = [
  { from: "research", to: "enrich" },
  { from: "enrich", to: "verify" },
  { from: "verify", to: "synthesize" },
  { from: "synthesize", to: "director_intel" },
  { from: "director_intel", to: "embeddings" },
  { from: "embeddings", to: "push" },
  { from: "push", to: "outreach" },
];

function getNodePos(id: string) {
  return NODE_DEFS.find((n) => n.id === id) ?? { x: 0, y: 0 };
}

export default function FlowChart({ nodes }: FlowChartProps) {
  return (
    <div className="relative bg-white rounded-xl border border-gray-200 shadow-sm max-h-[320px]" style={{ minHeight: "280px" }}>
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
            tooltip={def.tooltip}
            state={nodes[def.id] ?? null}
            x={def.x}
            y={def.y}
          />
        ))}
      </div>
    </div>
  );
}
