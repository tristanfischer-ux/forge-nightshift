import FlowNode from "./FlowNode";
import type { PipelineNodeEvent } from "../lib/tauri";
import { stageLabel, stageTooltip } from "../lib/stage-labels";

interface FlowChartProps {
  nodes: Record<string, PipelineNodeEvent | null>;
}

// Pipeline stages in order (2 rows of 4)
const ROW1 = [
  { id: "research",   label: stageLabel("research"),   tooltip: stageTooltip("research") },
  { id: "enrich",     label: stageLabel("enrich"),     tooltip: stageTooltip("enrich") },
  { id: "verify",     label: stageLabel("verify"),     tooltip: stageTooltip("verify") },
  { id: "synthesize", label: stageLabel("synthesize"), tooltip: stageTooltip("synthesize") },
];

const ROW2 = [
  { id: "activity",       label: stageLabel("activities"),       tooltip: stageTooltip("activities") },
  { id: "embeddings",     label: stageLabel("embeddings"),       tooltip: stageTooltip("embeddings") },
  { id: "investor_match", label: stageLabel("investor_matches"), tooltip: stageTooltip("investor_matches") },
  { id: "push",           label: stageLabel("push"),             tooltip: stageTooltip("push") },
];

// Arrow connector between nodes
function Arrow({ active }: { active: boolean }) {
  return (
    <div className="flex items-center px-1">
      <svg width="20" height="16" viewBox="0 0 20 16" className="shrink-0">
        <path
          d="M0 8 L14 8 M10 3 L16 8 L10 13"
          fill="none"
          stroke={active ? "#4f46e5" : "#d1d5db"}
          strokeWidth={active ? 2 : 1.5}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </div>
  );
}

// Curved down arrow between rows
function DownArrow({ active }: { active: boolean }) {
  return (
    <div className="flex justify-end pr-12 py-2">
      <svg width="40" height="24" viewBox="0 0 40 24" className="shrink-0">
        <path
          d="M20 0 L20 16 M14 12 L20 18 L26 12"
          fill="none"
          stroke={active ? "#4f46e5" : "#d1d5db"}
          strokeWidth={active ? 2 : 1.5}
          strokeLinecap="round"
          strokeLinejoin="round"
        />
      </svg>
    </div>
  );
}

export default function FlowChart({ nodes }: FlowChartProps) {
  // Check if the connection between two stages is active
  function isConnectionActive(fromId: string, toId: string): boolean {
    const fromStatus = nodes[fromId]?.status ?? "idle";
    const toStatus = nodes[toId]?.status ?? "idle";
    return fromStatus === "completed" && toStatus === "running";
  }

  return (
    <div className="bg-white rounded-xl border border-gray-200 shadow-sm p-4">
      {/* Row 1 */}
      <div className="flex items-stretch gap-0">
        {ROW1.map((stage, i) => (
          <div key={stage.id} className="flex items-stretch">
            <div className="w-[150px] shrink-0">
              <FlowNode
                nodeId={stage.id}
                label={stage.label}
                tooltip={stage.tooltip}
                state={nodes[stage.id] ?? null}
              />
            </div>
            {i < ROW1.length - 1 && (
              <Arrow active={isConnectionActive(ROW1[i].id, ROW1[i + 1].id)} />
            )}
          </div>
        ))}
      </div>

      {/* Down arrow from row 1 to row 2 */}
      <DownArrow active={isConnectionActive("synthesize", "activity")} />

      {/* Row 2 */}
      <div className="flex items-stretch gap-0">
        {ROW2.map((stage, i) => (
          <div key={stage.id} className="flex items-stretch">
            <div className="w-[150px] shrink-0">
              <FlowNode
                nodeId={stage.id}
                label={stage.label}
                tooltip={stage.tooltip}
                state={nodes[stage.id] ?? null}
              />
            </div>
            {i < ROW2.length - 1 && (
              <Arrow active={isConnectionActive(ROW2[i].id, ROW2[i + 1].id)} />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}
