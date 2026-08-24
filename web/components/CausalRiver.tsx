"use client";

import { motion } from "framer-motion";

const lanes = [
  { label: "dbt models", width: "80%" },
  { label: "airflow dags", width: "55%" },
  { label: "snowpark jobs", width: "68%" },
  { label: "pipe ingest", width: "42%" }
];

export default function CausalRiver() {
  return (
    <div className="panel">
      <h3 className="text-lg font-semibold mb-3">Causal River</h3>
      <div className="space-y-3">
        {lanes.map((lane) => (
          <div key={lane.label}>
            <div className="text-xs text-slate-400 mb-1">{lane.label}</div>
            <motion.div
              className="h-4 rounded-full bg-gradient-to-r from-cyan-500 to-violet-500"
              animate={{ width: [lane.width, "95%", lane.width] }}
              transition={{ duration: 5, repeat: Infinity }}
            />
          </div>
        ))}
      </div>
    </div>
  );
}
