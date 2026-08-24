"use client";

import { Canvas } from "@react-three/fiber";
import { OrbitControls, Stars } from "@react-three/drei";
import { motion } from "framer-motion";

function Node({ position, size }: { position: [number, number, number]; size: number }) {
  return (
    <mesh position={position}>
      <sphereGeometry args={[size, 24, 24]} />
      <meshStandardMaterial color="#38bdf8" emissive="#0ea5e9" emissiveIntensity={0.8} />
    </mesh>
  );
}

export default function CostConstellation() {
  return (
    <motion.div className="panel h-[360px]" initial={{ opacity: 0 }} animate={{ opacity: 1 }}>
      <h3 className="text-lg font-semibold mb-2">Cost Constellation (3D)</h3>
      <Canvas camera={{ position: [0, 0, 8] }}>
        <ambientLight intensity={0.35} />
        <pointLight position={[4, 5, 5]} intensity={1.2} />
        <Stars radius={80} depth={35} count={1800} factor={3} saturation={0} fade speed={1} />
        <Node position={[-2, 1.2, 0]} size={0.24} />
        <Node position={[2.2, 1.1, -0.5]} size={0.34} />
        <Node position={[0.6, -1.6, 0.3]} size={0.28} />
        <Node position={[-1.6, -1.2, -0.6]} size={0.21} />
        <OrbitControls enablePan={false} />
      </Canvas>
    </motion.div>
  );
}
