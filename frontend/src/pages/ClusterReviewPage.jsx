import { useState, useEffect } from "react";
import { useParams, useNavigate, Link } from "react-router-dom";
import { API } from "../hooks/useApi";

export default function ClusterReviewPage() {
  const { cityId } = useParams();
  const navigate   = useNavigate();

  const [groups,   setGroups]   = useState([]);
  const [cityName, setCityName] = useState("");
  const [loading,  setLoading]  = useState(true);
  const [saving,   setSaving]   = useState({});
  const [done,     setDone]     = useState(false);

  async function loadData() {
    try {
      const [cityData, clusterData] = await Promise.all([
        API.get(`/cities/${cityId}`),
        API.get(`/cities/${cityId}/cluster-review`),
      ]);
      setCityName(cityData.name);
      setGroups(clusterData.groups || []);
    } finally {
      setLoading(false);
    }
  }

  useEffect(() => { loadData(); }, [cityId]);

  async function handleMerge(group) {
    const clusterIds = group.clusters.map(c => c.cluster_id);
    setSaving(s => ({ ...s, [group.group_id]: "merging" }));
    try {
      await API.post(`/cities/${cityId}/cluster-review/merge`, { cluster_ids: clusterIds });
      await refresh();
    } finally {
      setSaving(s => ({ ...s, [group.group_id]: null }));
    }
  }

  async function handleKeepSeparate(group) {
    const pairIds = group.linking_pairs.map(p => p.pair_id);
    setSaving(s => ({ ...s, [group.group_id]: "keeping" }));
    try {
      await API.post(`/cities/${cityId}/cluster-review/keep-separate`, { pair_ids: pairIds });
      await refresh();
    } finally {
      setSaving(s => ({ ...s, [group.group_id]: null }));
    }
  }

  async function refresh() {
    const data = await API.get(`/cities/${cityId}/cluster-review`);
    const remaining = data.groups || [];
    setGroups(remaining);
    if (remaining.length === 0) setDone(true);
  }

  if (loading) return (
    <div style={{ display: "flex", justifyContent: "center", padding: 80 }}>
      <div className="spinner" style={{ width: 32, height: 32 }} />
    </div>
  );

  if (done) return (
    <div>
      <Breadcrumb cityId={cityId} cityName={cityName} />
      <div className="card" style={{ textAlign: "center", padding: 64 }}>
        <div style={{ fontSize: 48, marginBottom: 16 }}>✅</div>
        <h2>Cluster Review Complete</h2>
        <p className="text-muted" style={{ marginTop: 8, marginBottom: 28 }}>
          All clusters resolved. Resume the pipeline to continue.
        </p>
        <div style={{ display: "flex", gap: 12, justifyContent: "center" }}>
          <button className="btn btn-primary" onClick={async () => {
            await API.post(`/cities/${cityId}/resume`);
            navigate(`/city/${cityId}`);
          }}>▶ Resume Pipeline</button>
          <Link to={`/city/${cityId}`} className="btn">View Dashboard</Link>
        </div>
      </div>
    </div>
  );

  return (
    <div>
      <Breadcrumb cityId={cityId} cityName={cityName} />

      <div className="page-header" style={{ marginBottom: 24 }}>
        <div className="page-header-left">
          <h1>Cluster Review</h1>
          <span className="text-muted">{groups.length} groups need review</span>
        </div>
      </div>

      <div className="card" style={{ padding: "10px 16px", marginBottom: 20, display: "flex", gap: 24 }}>
        <span className="text-muted" style={{ fontSize: 12 }}>💡 Groups show clusters that may be the same business.</span>
        <span style={{ fontSize: 12 }}><strong style={{ color: "var(--success)" }}>Merge</strong> <span className="text-muted">= same business</span></span>
        <span style={{ fontSize: 12 }}><strong className="text-muted">Keep Separate</strong> <span className="text-muted">= different businesses</span></span>
      </div>

      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>
        {groups.map((group, gi) => (
          <ClusterGroup
            key={group.group_id}
            group={group}
            index={gi + 1}
            total={groups.length}
            savingState={saving[group.group_id]}
            onMerge={() => handleMerge(group)}
            onKeepSeparate={() => handleKeepSeparate(group)}
          />
        ))}
      </div>
    </div>
  );
}

function ClusterGroup({ group, index, total, savingState, onMerge, onKeepSeparate }) {
  const linkedIndexes = new Set(group.linking_pairs.flatMap(p => [p.index_a, p.index_b]));

  return (
    <div className="card" style={{ padding: 0, overflow: "hidden" }}>
      <div style={{
        padding: "10px 16px", background: "var(--surface2)",
        borderBottom: "1px solid var(--border)",
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <span style={{ fontSize: 13, color: "var(--text-muted)" }}>
          Group {index} of {total} · <strong style={{ color: "var(--text)" }}>{group.clusters.length}</strong> clusters · <strong style={{ color: "var(--text)" }}>{group.linking_pairs.length}</strong> suspicious pair{group.linking_pairs.length > 1 ? "s" : ""}
        </span>
        <div style={{ display: "flex", gap: 8 }}>
          <button className="btn" disabled={!!savingState} onClick={onKeepSeparate} style={{ fontSize: 12 }}>
            {savingState === "keeping" ? <><span className="spinner" /> Saving…</> : "✂ Keep Separate"}
          </button>
          <button className="btn btn-primary" disabled={!!savingState} onClick={onMerge} style={{ fontSize: 12 }}>
            {savingState === "merging" ? <><span className="spinner" /> Merging…</> : "🔀 Merge All"}
          </button>
        </div>
      </div>

      <div style={{ display: "grid", gridTemplateColumns: `repeat(${Math.min(group.clusters.length, 4)}, 1fr)` }}>
        {group.clusters.map((cluster, ci) => (
          <div key={cluster.cluster_id} style={{ borderRight: ci < group.clusters.length - 1 ? "1px solid var(--border)" : "none" }}>
            <div style={{
              padding: "6px 14px", fontSize: 11, fontWeight: 600,
              color: "var(--accent)", textTransform: "uppercase", letterSpacing: "0.5px",
              borderBottom: "1px solid var(--border)", background: "rgba(79,142,247,0.05)",
            }}>
              Cluster {cluster.cluster_id} · {cluster.records.length} record{cluster.records.length > 1 ? "s" : ""}
            </div>
            {cluster.records.map(rec => {
              const isLinked = linkedIndexes.has(rec.city_index);
              return (
                <div key={rec.id} style={{
                  padding: "10px 14px",
                  borderLeft: isLinked ? "3px solid var(--warn)" : "3px solid transparent",
                  borderBottom: "1px solid var(--border)",
                  background: isLinked ? "rgba(245,158,11,0.04)" : "transparent",
                }}>
                  <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text)", marginBottom: 2 }}>
                    {rec.business_name || <span className="text-muted" style={{ fontStyle: "italic" }}>No name</span>}
                  </div>
                  <div style={{ fontSize: 12, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
                    {rec.address1 || <span style={{ fontStyle: "italic" }}>No address</span>}
                  </div>
                  {isLinked && <span className="badge badge-paused" style={{ marginTop: 4, fontSize: 10 }}>flagged</span>}
                </div>
              );
            })}
          </div>
        ))}
      </div>

      {group.linking_pairs.length > 0 && (
        <div style={{ padding: "10px 16px", borderTop: "1px solid var(--border)", background: "var(--surface2)" }}>
          <div style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginBottom: 6 }}>
            Why flagged
          </div>
          {group.linking_pairs.map(pair => (
            <div key={pair.pair_id} style={{ display: "flex", gap: 10, fontSize: 12, alignItems: "center", flexWrap: "wrap", marginBottom: 4 }}>
              <span className="badge badge-paused" style={{ fontSize: 11, fontWeight: 700 }}>
                {Math.round((pair.similarity || 0) * 100)}% name similarity
              </span>
              <span style={{ color: "var(--text)" }}>
                <strong>{pair.name_a}</strong>
                {pair.address_a && <span className="text-muted"> · {pair.address_a}</span>}
              </span>
              <span className="text-muted">vs</span>
              <span style={{ color: "var(--text)" }}>
                <strong>{pair.name_b}</strong>
                {pair.address_b && <span className="text-muted"> · {pair.address_b}</span>}
              </span>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

function Breadcrumb({ cityId, cityName }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 20, fontSize: 13 }}>
      <Link to="/" style={{ color: "var(--text-muted)", textDecoration: "none" }}>Cities</Link>
      <span style={{ color: "var(--border2)" }}>/</span>
      <Link to={`/city/${cityId}`} style={{ color: "var(--text-muted)", textDecoration: "none" }}>{cityName}</Link>
      <span style={{ color: "var(--border2)" }}>/</span>
      <span style={{ color: "var(--text)" }}>Cluster Review</span>
    </div>
  );
}
