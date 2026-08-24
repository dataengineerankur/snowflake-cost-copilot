"use client";

import { useMemo, useState } from "react";

export default function FixSimulatorCockpit() {
  const [scan, setScan] = useState(20);
  const [spill, setSpill] = useState(30);
  const [resize, setResize] = useState(true);
  const projection = useMemo(() => {
    const creditDelta = -(scan * 0.4 + spill * 0.2 + (resize ? 8 : 2));
    const runtimeDelta = -(resize ? 15 : 5) - scan * 0.2;
    return { creditDelta: creditDelta.toFixed(2), runtimeDelta: runtimeDelta.toFixed(2) };
  }, [scan, spill, resize]);

  return (
    <div className="panel">
      <h3 className="text-lg font-semibold mb-3">Fix Simulator Cockpit</h3>
      <div className="grid grid-cols-2 gap-3 text-sm">
        <label>Scan reduction %</label>
        <input type="range" min={0} max={60} value={scan} onChange={(e) => setScan(Number(e.target.value))} />
        <label>Spill reduction %</label>
        <input type="range" min={0} max={60} value={spill} onChange={(e) => setSpill(Number(e.target.value))} />
        <label>Warehouse resize</label>
        <input type="checkbox" checked={resize} onChange={(e) => setResize(e.target.checked)} />
      </div>
      <div className="mt-3 text-sm text-cyan-200">
        Expected credit delta: {projection.creditDelta}% | Runtime delta: {projection.runtimeDelta}%
      </div>
    </div>
  );
}
