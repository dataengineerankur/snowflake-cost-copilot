"use client";

const rows = [
  { team: "DBT", savings: 1240, regressions: 1, response: "2h" },
  { team: "AIRFLOW", savings: 980, regressions: 2, response: "3h" },
  { team: "SNOWPARK", savings: 730, regressions: 0, response: "1h" },
  { team: "UNOWNED", savings: 0, regressions: 8, response: "N/A" }
];

export default function TeamScorecardsArena() {
  return (
    <div className="panel">
      <h3 className="text-lg font-semibold mb-3">Team Scorecards Arena</h3>
      <table className="w-full text-sm">
        <thead className="text-slate-400">
          <tr>
            <th className="text-left">Team</th>
            <th className="text-right">Savings</th>
            <th className="text-right">Regressions</th>
            <th className="text-right">Response</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((r) => (
            <tr key={r.team} className="border-t border-slate-700">
              <td>{r.team}</td>
              <td className="text-right">${r.savings}</td>
              <td className="text-right">{r.regressions}</td>
              <td className="text-right">{r.response}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
