interface FlowConnectorProps {
  from: { x: number; y: number }; // center-bottom of source
  to: { x: number; y: number };   // center-top of target
  active: boolean; // source completed + target running
}

export default function FlowConnector({ from, to, active }: FlowConnectorProps) {
  const midY = (from.y + to.y) / 2;
  const d = `M ${from.x} ${from.y} C ${from.x} ${midY}, ${to.x} ${midY}, ${to.x} ${to.y}`;

  return (
    <path
      d={d}
      fill="none"
      stroke={active ? "#5c7cfa" : "#d1d5db"}
      strokeWidth={active ? 2 : 1.5}
      strokeDasharray={active ? "6 4" : "none"}
      className={active ? "animate-dash" : ""}
    />
  );
}
