"use client";

import { motion } from "framer-motion";

export default function QueryDNAHelix() {
  const points = Array.from({ length: 32 }, (_, i) => i);
  return (
    <div className="panel h-[260px] overflow-hidden">
      <h3 className="text-lg font-semibold mb-2">Query DNA Helix</h3>
      <div className="relative h-[200px]">
        {points.map((i) => (
          <motion.div
            key={`a-${i}`}
            className="absolute w-2 h-2 rounded-full bg-cyan-300"
            animate={{ x: 80 + Math.sin(i / 2) * 60, y: i * 6 }}
            transition={{ repeat: Infinity, duration: 4 + i * 0.03, repeatType: "reverse" }}
          />
        ))}
        {points.map((i) => (
          <motion.div
            key={`b-${i}`}
            className="absolute w-2 h-2 rounded-full bg-violet-300"
            animate={{ x: 220 + Math.cos(i / 2) * 60, y: i * 6 }}
            transition={{ repeat: Infinity, duration: 4 + i * 0.03, repeatType: "reverse" }}
          />
        ))}
      </div>
    </div>
  );
}
