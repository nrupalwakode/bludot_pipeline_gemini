import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import { API } from "../hooks/useApi";

export default function MatchesPage() {
  const { cityId } = useParams();
  const [matches,   setMatches]   = useState([]);
  const [cityName,  setCityName]  = useState("");
  const [loading,   setLoading]   = useState(true);
  const [filter,    setFilter]    = useState("all");
  const [search,    setSearch]    = useState("");

  useEffect(() => {
    async function load() {
      const [cityData, matchData] = await Promise.all([
        API.get(`/cities/${cityId}`),
        API.get(`/cities/${cityId}/matches`),
      ]);
      setCityName(cityData.name);
      setMatches(matchData.matches || []);
      setLoading(false);
    }
    load();
  }, [cityId]);

  const filtered = matches.filter(m => {
    const matchesFilter =
      filter === "all" ||
      (filter === "auto"   && m.final_decision === "AUTO_MATCH") ||
      (filter === "human"  && m.final_decision === "HUMAN_ACCEPT");
    const q = search.toLowerCase();
    const matchesSearch = !q ||
      m.city_name?.toLowerCase().includes(q) ||
      m.bludot_name?.toLowerCase().includes(q) ||
      m.city_address?.toLowerCase().includes(q);
    return matchesFilter && matchesSearch;
  });

  const counts = {
    all:   matches.length,
    auto:  matches.filter(m => m.final_decision === "AUTO_MATCH").length,
    human: matches.filter(m => m.final_decision === "HUMAN_ACCEPT").length,
  };

  if (loading) return (
    <div style={{ display: "flex", justifyContent: "center", padding: 80 }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  return (
    <div>
      {/* Breadcrumb */}
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 20, fontSize: 13 }}>
        <Link to="/" style={{ color: "var(--text-muted)", textDecoration: "none" }}>Cities</Link>
        <span style={{ color: "var(--border2)" }}>/</span>
        <Link to={`/city/${cityId}`} style={{ color: "var(--text-muted)", textDecoration: "none" }}>{cityName}</Link>
        <span style={{ color: "var(--border2)" }}>/</span>
        <span style={{ color: "var(--text)" }}>Matches</span>
      </div>

      <div className="page-header" style={{ marginBottom: 20 }}>
        <div className="page-header-left">
          <h1>Matched Records</h1>
          <span className="text-muted">{matches.length} total matches</span>
        </div>
        <Link to={`/city/${cityId}`} className="btn">← Back to Pipeline</Link>
      </div>

      {/* Stats */}
      <div className="stat-grid" style={{ marginBottom: 20 }}>
        <div className="stat-card">
          <div className="stat-label">Total Matches</div>
          <div className="stat-value">{counts.all}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Auto Matched (LLM)</div>
          <div className="stat-value accent">{counts.auto}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Human Accepted</div>
          <div className="stat-value success">{counts.human}</div>
        </div>
      </div>

      {/* Filter + Search */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, alignItems: "center" }}>
        <div style={{ display: "flex", gap: 4, background: "var(--surface2)", padding: 3, borderRadius: 7, border: "1px solid var(--border)" }}>
          {[
            { key: "all",   label: `All (${counts.all})` },
            { key: "auto",  label: `Auto (${counts.auto})` },
            { key: "human", label: `Human (${counts.human})` },
          ].map(f => (
            <button key={f.key} onClick={() => setFilter(f.key)} style={{
              padding: "5px 12px", borderRadius: 5, border: "none",
              background: filter === f.key ? "var(--surface)" : "transparent",
              color: filter === f.key ? "var(--text)" : "var(--text-muted)",
              fontSize: 12, cursor: "pointer",
              boxShadow: filter === f.key ? "0 1px 3px rgba(0,0,0,0.3)" : "none",
            }}>{f.label}</button>
          ))}
        </div>

        <input
          type="text"
          placeholder="Search by name or address…"
          value={search}
          onChange={e => setSearch(e.target.value)}
          style={{
            flex: 1, padding: "7px 12px", fontSize: 13,
            background: "var(--surface2)", border: "1px solid var(--border)",
            borderRadius: 7, color: "var(--text)", outline: "none",
          }}
        />

        <span style={{ fontSize: 12, color: "var(--text-muted)", whiteSpace: "nowrap" }}>
          {filtered.length} shown
        </span>
      </div>

      {/* Table */}
      {filtered.length === 0 ? (
        <div className="card" style={{ textAlign: "center", padding: 48 }}>
          <div style={{ fontSize: 32, marginBottom: 12 }}>🔍</div>
          <p className="text-muted">No matches found</p>
        </div>
      ) : (
        <div className="card" style={{ padding: 0, overflow: "hidden" }}>
          <div className="table-wrap">
            <table style={{ width: "100%", tableLayout: "fixed" }}>
              <thead>
                <tr>
                  <th style={{ width: "28%" }}>City Business Name</th>
                  <th style={{ width: "20%" }}>City Address</th>
                  <th style={{ width: "28%" }}>Bludot Name</th>
                  <th style={{ width: "20%" }}>Bludot Address</th>
                  <th style={{ width: "4%" }}></th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((m, i) => (
                  <tr key={i}>
                    <td style={{ fontWeight: 500, color: "var(--text)" }}>
                      {m.city_name || "—"}
                    </td>
                    <td style={{ fontSize: 12, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
                      {m.city_address || "—"}
                    </td>
                    <td style={{ color: "var(--accent)" }}>
                      {m.bludot_name || "—"}
                    </td>
                    <td style={{ fontSize: 12, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
                      {m.bludot_address || "—"}
                    </td>
                    <td>
                      <span style={{
                        fontSize: 10, padding: "2px 6px", borderRadius: 4,
                        background: m.final_decision === "AUTO_MATCH"
                          ? "rgba(79,142,247,0.15)" : "rgba(52,211,153,0.15)",
                        color: m.final_decision === "AUTO_MATCH"
                          ? "var(--accent)" : "var(--success)",
                        fontWeight: 600,
                      }}>
                        {m.final_decision === "AUTO_MATCH" ? "AUTO" : "HUMAN"}
                      </span>
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
