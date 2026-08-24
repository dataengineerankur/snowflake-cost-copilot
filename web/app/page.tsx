"use client";

import CostConstellation from "../components/CostConstellation";
import QueryDNAHelix from "../components/QueryDNAHelix";
import IncidentReplayTheater from "../components/IncidentReplayTheater";
import CausalRiver from "../components/CausalRiver";
import FixSimulatorCockpit from "../components/FixSimulatorCockpit";
import ConfidenceNebula from "../components/ConfidenceNebula";
import GovernanceCommandDeck from "../components/GovernanceCommandDeck";
import TeamScorecardsArena from "../components/TeamScorecardsArena";

export default function HomePage() {
  return (
    <main className="p-6 space-y-4">
      <header className="panel">
        <h1 className="text-2xl font-bold">Snowflake Cost Copilot: Cinematic Command Center</h1>
        <p className="text-slate-300">Cinematic mode with anomaly, replay, simulation, governance, and team impact intelligence.</p>
      </header>

      <section className="grid grid-cols-1 xl:grid-cols-2 gap-4">
        <CostConstellation />
        <QueryDNAHelix />
        <IncidentReplayTheater />
        <CausalRiver />
        <FixSimulatorCockpit />
        <ConfidenceNebula />
        <GovernanceCommandDeck />
        <TeamScorecardsArena />
      </section>
    </main>
  );
}
