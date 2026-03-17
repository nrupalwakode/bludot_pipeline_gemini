import { useState, useEffect } from "react";
import { Link } from "react-router-dom";
import { API } from "../hooks/useApi";

const STATUS_ORDER = {
  paused: 0, running: 1, failed: 2, pending: 3, not_started: 4, completed: 5
};

export default function CitiesPage() {
  const [cities, setCities] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    API.get("/cities/").then(setCities).finally(() => setLoading(false));
    const t = setInterval(() => API.get("/cities/").then(setCities), 5000);
    return () => clearInterval(t);
  }, []);

  const sorted = [...cities].sort(
    (a, b) => (STATUS_ORDER[a.pipeline_status] ?? 9) - (STATUS_ORDER[b.pipeline_status] ?? 9)
  );

  const counts = {
    total:     cities.length,
    running:   cities.filter(c => c.pipeline_status === "running").length,
    paused:    cities.filter(c => c.pipeline_status === "paused").length,
    completed: cities.filter(c => c.pipeline_status === "completed").length,
    failed:    cities.filter(c => c.pipeline_status === "failed").length,
  };

  return (
    <div>
      <div className="page-header">
        <div className="page-header-left">
          <h1>Cities</h1>
          <span className="text-muted">{cities.length} total</span>
        </div>
        <Link to="/new" className="btn btn-primary">＋ New City</Link>
      </div>

      <div className="stat-grid" style={{ marginBottom: 28 }}>
        <div className="stat-card">
          <div className="stat-label">Total</div>
          <div className="stat-value">{counts.total}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Running</div>
          <div className="stat-value accent">{counts.running}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Needs Review</div>
          <div className="stat-value warn">{counts.paused}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Completed</div>
          <div className="stat-value success">{counts.completed}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Failed</div>
          <div className="stat-value danger">{counts.failed}</div>
        </div>
      </div>

      {loading ? (
        <div style={{ textAlign: "center", padding: 40 }}>
          <div className="spinner" style={{ width: 28, height: 28 }} />
        </div>
      ) : sorted.length === 0 ? (
        <div className="empty-state">
          <div className="empty-icon">🏙</div>
          <h2>No cities yet</h2>
          <p style={{ marginTop: 8 }}>Upload a city dataset to get started</p>
          <Link to="/new" className="btn btn-primary" style={{ marginTop: 20 }}>
            ＋ New City
          </Link>
        </div>
      ) : (
        <div className="card" style={{ padding: 0 }}>
          <div className="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>City / County</th>
                  <th>Type</th>
                  <th>Status</th>
                  <th>Current Step</th>
                  <th>Created</th>
                  <th></th>
                </tr>
              </thead>
              <tbody>
                {sorted.map(city => (
                  <tr key={city.id}>
                    <td>
                      <Link to={`/city/${city.id}`} style={{ color: "var(--accent)", textDecoration: "none", fontWeight: 500 }}>
                        {city.name}
                      </Link>
                    </td>
                    <td className="text-muted">{city.city_or_county}</td>
                    <td><StatusBadge status={city.pipeline_status} /></td>
                    <td>
                      <span className="mono" style={{ fontSize: 12, color: "var(--text-muted)" }}>
                        {city.current_step?.replace(/_/g, " ") || "—"}
                      </span>
                    </td>
                    <td className="text-muted">
                      {new Date(city.created_at).toLocaleDateString()}
                    </td>
                    <td>
                      {city.pipeline_status === "paused" && (
                        <Link to={`/city/${city.id}/review`} className="btn btn-sm" style={{ background: "rgba(245,158,11,0.15)", color: "var(--warn)", border: "1px solid rgba(245,158,11,0.3)" }}>
                          Review Queue ↗
                        </Link>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

export function StatusBadge({ status }) {
  const map = {
    running:     "badge-running",
    paused:      "badge-paused",
    completed:   "badge-completed",
    failed:      "badge-failed",
    pending:     "badge-pending",
    not_started: "badge-pending",
  };
  return (
    <span className={`badge ${map[status] || "badge-pending"}`}>
      {status?.replace(/_/g, " ") || "not started"}
    </span>
  );
}
