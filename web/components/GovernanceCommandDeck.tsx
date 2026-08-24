"use client";

export default function GovernanceCommandDeck() {
  return (
    <div className="panel">
      <h3 className="text-lg font-semibold mb-2">Governance Command Deck</h3>
      <div className="grid grid-cols-2 gap-3 text-sm">
        <div className="bg-slate-900 rounded p-3">
          <div className="text-cyan-300 mb-1">Before State</div>
          <div>AUTO_SUSPEND: 300</div>
          <div>AUTO_RESUME: TRUE</div>
        </div>
        <div className="bg-slate-900 rounded p-3">
          <div className="text-cyan-300 mb-1">After State</div>
          <div>AUTO_SUSPEND: 60</div>
          <div>AUTO_RESUME: TRUE</div>
        </div>
      </div>
      <button className="mt-3 bg-cyan-500 text-black px-3 py-1 rounded font-semibold">Arm Apply</button>
    </div>
  );
}
