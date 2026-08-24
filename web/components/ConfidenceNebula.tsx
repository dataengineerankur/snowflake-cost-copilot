"use client";

import { motion } from "framer-motion";

export default function ConfidenceNebula() {
  return (
    <div className="panel h-[220px] relative overflow-hidden">
      <h3 className="text-lg font-semibold">Confidence Nebula</h3>
      <motion.div
        className="absolute w-44 h-44 rounded-full bg-cyan-400/30 blur-2xl top-12 left-16"
        animate={{ scale: [0.9, 1.2, 0.95], opacity: [0.4, 0.8, 0.5] }}
        transition={{ duration: 6, repeat: Infinity }}
      />
      <motion.div
        className="absolute w-32 h-32 rounded-full bg-violet-400/25 blur-2xl top-20 left-40"
        animate={{ scale: [1.1, 0.9, 1.15], opacity: [0.6, 0.4, 0.7] }}
        transition={{ duration: 5, repeat: Infinity }}
      />
      <div className="absolute bottom-4 left-4 text-sm text-slate-300">Dense glow = high evidence quality</div>
    </div>
  );
}
