"use client";

import { useEffect, useState } from "react";
import { motion } from "framer-motion";

export default function IncidentReplayTheater() {
  const [frame, setFrame] = useState(0);
  useEffect(() => {
    const id = setInterval(() => setFrame((f) => (f + 1) % 100), 120);
    return () => clearInterval(id);
  }, []);
  return (
    <div className="panel">
      <h3 className="text-lg font-semibold mb-2">Incident Replay Theater</h3>
      <div className="h-24 bg-slate-900 rounded-lg relative overflow-hidden">
        <motion.div className="absolute top-0 left-0 h-full bg-cyan-500/30" animate={{ width: `${frame}%` }} />
        <div className="absolute inset-0 flex items-center justify-center text-sm text-slate-200">
          Timeline replay frame: {frame}
        </div>
      </div>
    </div>
  );
}
