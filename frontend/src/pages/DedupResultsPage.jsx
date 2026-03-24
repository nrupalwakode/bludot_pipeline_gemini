import { useState, useEffect } from "react";
import { useParams, Link } from "react-router-dom";
import { API } from "../hooks/useApi";

const DECISION_LABELS = {
  DUPLICATE:     { label: "Duplicate",     color: "var(--success)",  bg: "rgba(52,211,153,0.12)" },
  NOT_DUPLICATE: { label: "Not Duplicate", color: "var(--text-muted)",bg: "var(--surface2)" },
  UNCERTAIN:     { label: "Needs Review",  color: "var(--warn)",     bg: "rgba(245,158,11,0.12)" },
};

export default function DedupResultsPage() {
  const { cityId } = useParams();
  const [pairs,    setPairs]    = useState([]);
  const [cityName, setCityName] = useState("");
  const [loading,  setLoading]  = useState(true);
  const [filter,   setFilter]   = useState("all");
  const [search,   setSearch]   = useState("");

  useEffect(() => {
    async function load() {
      const [cityData, dedupData] = await Promise.all([
        API.get(`/cities/${cityId}`),
        API.get(`/cities/${cityId}/dedup-results`),
      ]);
      setCityName(cityData.name);
      setPairs(dedupData.pairs || []);
      setLoading(false);
    }
    load();
  }, [cityId]);

  const counts = {
    all:          pairs.length,
    DUPLICATE:    pairs.filter(p => p.decision === "DUPLICATE").length,
    NOT_DUPLICATE:pairs.filter(p => p.decision === "NOT_DUPLICATE").length,
    UNCERTAIN:    pairs.filter(p => p.decision === "UNCERTAIN").length,
  };

  const filtered = pairs.filter(p => {
    const matchesFilter = filter === "all" || p.decision === filter;
    const q = search.toLowerCase();
    const matchesSearch = !q ||
      p.name_a?.toLowerCase().includes(q) ||
      p.name_b?.toLowerCase().includes(q) ||
      p.address_a?.toLowerCase().includes(q) ||
      p.address_b?.toLowerCase().includes(q);
    return matchesFilter && matchesSearch;
  });

  if (loading) return (
    <div style={{ display: "flex", justifyContent: "center", padding: 80 }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  return (
    <div>
      <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 20, fontSize: 13 }}>
        <Link to="/" style={{ color: "var(--text-muted)", textDecoration: "none" }}>Cities</Link>
        <span style={{ color: "var(--border2)" }}>/</span>
        <Link to={`/city/${cityId}`} style={{ color: "var(--text-muted)", textDecoration: "none" }}>{cityName}</Link>
        <span style={{ color: "var(--border2)" }}>/</span>
        <span style={{ color: "var(--text)" }}>Dedup Results</span>
      </div>

      <div className="page-header" style={{ marginBottom: 20 }}>
        <div className="page-header-left">
          <h1>Dedup Results</h1>
          <span className="text-muted">All intra-cluster pairs verified by LLM</span>
        </div>
        <Link to={`/city/${cityId}`} className="btn">← Back</Link>
      </div>

      {/* Stats */}
      <div className="stat-grid" style={{ marginBottom: 20 }}>
        <div className="stat-card">
          <div className="stat-label">Total Pairs</div>
          <div className="stat-value">{counts.all}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Duplicates (merged)</div>
          <div className="stat-value success">{counts.DUPLICATE}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Not Duplicates (split)</div>
          <div className="stat-value">{counts.NOT_DUPLICATE}</div>
        </div>
        <div className="stat-card">
          <div className="stat-label">Needs Review</div>
          <div className="stat-value warn">{counts.UNCERTAIN}</div>
        </div>
      </div>

      {/* Filter + Search */}
      <div style={{ display: "flex", gap: 12, marginBottom: 16, alignItems: "center" }}>
        <div style={{ display: "flex", gap: 4, background: "var(--surface2)", padding: 3, borderRadius: 7, border: "1px solid var(--border)" }}>
          {[
            { key: "all",           label: `All (${counts.all})` },
            { key: "DUPLICATE",     label: `Duplicate (${counts.DUPLICATE})` },
            { key: "NOT_DUPLICATE", label: `Not Dup (${counts.NOT_DUPLICATE})` },
            { key: "UNCERTAIN",     label: `Review (${counts.UNCERTAIN})` },
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

      {/* Pairs list */}
      {filtered.length === 0 ? (
        <div className="card" style={{ textAlign: "center", padding: 48 }}>
          <p className="text-muted">No pairs found</p>
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
          {filtered.map(p => {
            const d = DECISION_LABELS[p.decision] || DECISION_LABELS.UNCERTAIN;
            return (
              <div key={p.id} className="card" style={{
                padding: 0, overflow: "hidden",
                borderLeft: `3px solid ${d.color}`,
              }}>
                <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 160px" }}>
                  {/* Record A */}
                  <div style={{ padding: "12px 16px", borderRight: "1px solid var(--border)" }}>
                    <div style={{ fontSize: 10, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 4 }}>
                      Record A
                    </div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", marginBottom: 2 }}>
                      {p.name_a || "—"}
                    </div>
                    <div style={{ fontSize: 12, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
                      {p.address_a || <span style={{ fontStyle: "italic" }}>no address</span>}
                    </div>
                  </div>

                  {/* Record B */}
                  <div style={{ padding: "12px 16px", borderRight: "1px solid var(--border)" }}>
                    <div style={{ fontSize: 10, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 4 }}>
                      Record B
                    </div>
                    <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", marginBottom: 2 }}>
                      {p.name_b || "—"}
                    </div>
                    <div style={{ fontSize: 12, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
                      {p.address_b || <span style={{ fontStyle: "italic" }}>no address</span>}
                    </div>
                  </div>

                  {/* Decision */}
                  <div style={{
                    padding: "12px 16px", background: d.bg,
                    display: "flex", flexDirection: "column",
                    justifyContent: "center", gap: 6,
                  }}>
                    <span style={{
                      fontSize: 11, fontWeight: 700, color: d.color,
                      textTransform: "uppercase", letterSpacing: "0.5px",
                    }}>
                      {d.label}
                    </span>
                    <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                      {Math.round(p.similarity * 100)}% similar
                    </span>
                    {p.llm_reason && (
                      <span style={{ fontSize: 11, color: "var(--text-muted)", fontStyle: "italic", lineHeight: 1.3 }}>
                        {p.llm_reason}
                      </span>
                    )}
                    <span style={{
                      fontSize: 10, color: "var(--text-muted)",
                      padding: "1px 5px", borderRadius: 3,
                      background: "var(--surface2)",
                      display: "inline-block", width: "fit-content",
                    }}>
                      {p.intra_cluster ? "intra-cluster" : "cross-cluster"}
                    </span>
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
