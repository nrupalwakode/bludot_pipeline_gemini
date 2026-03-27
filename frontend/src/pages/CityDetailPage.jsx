import { useState, useEffect, useCallback } from "react";
import { useParams, Link, useNavigate } from "react-router-dom";
import { API } from "../hooks/useApi";
import { StatusBadge } from "./CitiesPage";

const STEPS = [
  { key: "step0_dedup",              label: "Deduplication (LSH + LLM)" },
  { key: "gate0_cluster_review",     label: "Cluster Review" },
  { key: "step1_format",             label: "Reformat + Merge Columns" },
  { key: "gate1_verify_step1",       label: "Verify Step 1 Output" },
  { key: "step2_match",              label: "Match Candidates + LLM (Pass 1)" },
  { key: "gate2_match_review_pass1", label: "Human Review (Pass 1)" },
  { key: "step3_split",              label: "Split Records" },
  { key: "gate3_verify_split",       label: "Verify Split Files" },
  { key: "step4_match_pass2",        label: "Second-Pass Match + LLM (Pass 2)" },
  { key: "gate4_match_review_pass2", label: "Human Review (Pass 2)" },
  { key: "step5_output",             label: "Generate Output Sheets (Step 5)" },
  { key: "gate5_verify_step5",       label: "Verify Step 5 Output" },
  { key: "step6_contacts",           label: "Contacts Dedup (Step 6)" },
  { key: "gate6_verify_contacts",    label: "Verify Contacts" },
  { key: "done",                     label: "Complete" },
  { key: "done",                label: "Complete" },
];

export default function CityDetailPage() {
  const { cityId } = useParams();
  const navigate   = useNavigate();

  const [city,    setCity]    = useState(null);
  const [status,  setStatus]  = useState(null);
  const [stats,   setStats]   = useState(null);
  const [loading, setLoading] = useState(true);
  const [resuming, setResuming] = useState(false);
  const [error,   setError]   = useState("");

  const fetchAll = useCallback(async () => {
    try {
      const [cityData, statusData, statsData] = await Promise.all([
        API.get(`/cities/${cityId}`),
        API.get(`/cities/${cityId}/status`),
        API.get(`/cities/${cityId}/stats`).catch(() => null),
      ]);
      setCity(cityData);
      setStatus(statusData);
      setStats(statsData);
    } catch (e) {
      setError(e.message);
    } finally {
      setLoading(false);
    }
  }, [cityId]);

  useEffect(() => {
    fetchAll();
    // Poll every 4s while running
    const t = setInterval(() => {
      if (status?.status === "running") fetchAll();
    }, 4000);
    return () => clearInterval(t);
  }, [fetchAll, status?.status]);

  async function handleResume() {
    setResuming(true);
    setError("");
    try {
      await API.post(`/cities/${cityId}/resume`);
      await fetchAll();
    } catch (e) {
      setError(e.message);
    } finally {
      setResuming(false);
    }
  }

  async function handleStart() {
    setError("");
    try {
      await API.post(`/cities/${cityId}/start`);
      await fetchAll();
    } catch (e) {
      setError(e.message);
    }
  }

  if (loading) return <div style={{ padding: 40, textAlign: "center" }}><div className="spinner" style={{ width: 32, height: 32 }} /></div>;
  if (!city)   return <div className="alert alert-error">City not found</div>;

  const pipelineStatus = status?.status || "not_started";
  const currentStep    = status?.current_step;
  const stepLogs       = status?.step_logs || [];
  const logMap         = {};
  stepLogs.forEach(l => { logMap[l.step] = l; });

  const pendingReview = status?.step_logs?.find(
    l => l.status === "paused" && l.stats?.pending_review > 0
  )?.stats?.pending_review || 0;

  // Map each paused gate to its UI action
  const GATE_CONFIG = {
    "gate0_cluster_review":     { label: "Cluster Review",        path: `/city/${cityId}/cluster-review`, needsReview: true },
    "gate1_verify_step1":       { label: "Verify Step 1 File",    path: null,                             needsReview: false },
    "gate2_match_review_pass1": { label: "Review Queue (Pass 1)", path: `/city/${cityId}/review`,         needsReview: true },
    "gate3_verify_split":       { label: "Verify Split Files",    path: null,                             needsReview: false },
    "gate4_match_review_pass2": { label: "Review Queue (Pass 2)", path: `/city/${cityId}/review?pass=2`,  needsReview: true },
    "gate5_verify_step5":       { label: "Verify Step 5 Output",  path: null,                             needsReview: false },
    "gate6_verify_contacts":    { label: "Verify Contacts",       path: null,                             needsReview: false },
  };

  const currentGate   = GATE_CONFIG[currentStep] || null;
  const gateLabel     = currentGate?.label || "Review Queue";
  const gatePath      = currentGate?.path || null;
  const gateNeedsUI   = currentGate?.needsReview && pendingReview > 0;

  // Paused message per gate
  const GATE_MESSAGES = {
    "gate0_cluster_review":     `${pendingReview} uncertain dedup pairs need cluster review.`,
    "gate1_verify_step1":       "Verify de_duplication_merged.xlsx — check Business Name, phones, addresses.",
    "gate2_match_review_pass1": `${pendingReview} pairs need human review (pass 1).`,
    "gate3_verify_split":       "Verify split files. Check final_matched_records and additional files. Add manual matches to city_bludot_index.xlsx if needed.",
    "gate4_match_review_pass2": `${pendingReview} pairs need human review (pass 2).`,
    "gate5_verify_step5":       "Verify Business_Matched_Records.xlsx — check Business, Custom, Contact sheets.",
    "gate6_verify_contacts":    "Verify Contact_Matched_Records sheet in the final Excel.",
  };
  const pauseMessage = GATE_MESSAGES[currentStep] || "Review required before continuing.";

  return (
    <div>
      {/* Header */}
      <div className="page-header">
        <div className="page-header-left">
          <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
            <Link to="/" style={{ color: "var(--text-muted)", textDecoration: "none", fontSize: 13 }}>
              Cities
            </Link>
            <span style={{ color: "var(--border2)" }}>/</span>
            <h1 style={{ margin: 0 }}>{city.name}</h1>
            <StatusBadge status={pipelineStatus} />
          </div>
          <span className="text-muted" style={{ fontSize: 13 }}>
            {city.city_or_county} · Created {new Date(city.created_at).toLocaleDateString()}
          </span>
        </div>

        <div style={{ display: "flex", gap: 10 }}>
          {pipelineStatus === "not_started" && (
            <button className="btn btn-primary" onClick={handleStart}>▶ Start Pipeline</button>
          )}
          {pipelineStatus === "paused" && (
            <>
              {gatePath && gateNeedsUI && (
                <Link to={gatePath} className="btn" style={{
                  background: "rgba(245,158,11,0.15)", color: "var(--warn)",
                  border: "1px solid rgba(245,158,11,0.3)"
                }}>
                  ⚑ {gateLabel} ({pendingReview})
                </Link>
              )}
              {gatePath && !gateNeedsUI && (
                <Link to={gatePath} className="btn" style={{
                  background: "rgba(245,158,11,0.15)", color: "var(--warn)",
                  border: "1px solid rgba(245,158,11,0.3)"
                }}>
                  ⚑ {gateLabel}
                </Link>
              )}
              <button className="btn btn-primary" onClick={handleResume}
                disabled={resuming || (gateNeedsUI && pendingReview > 0)}>
                {resuming ? <><span className="spinner" /> Resuming…</> : "▶ Resume"}
              </button>
            </>
          )}
          {pipelineStatus === "failed" && (
            <button className="btn btn-primary" onClick={handleStart}>↺ Retry</button>
          )}
          {pipelineStatus === "completed" && (
            <Link to={`/city/${cityId}/matches`} className="btn btn-primary">
              ✓ View Matches
            </Link>
          )}
          {pipelineStatus !== "not_started" && (
            <Link to={`/city/${cityId}/dedup-results`} className="btn" style={{ fontSize: 13 }}>
              ⊞ Dedup Results
            </Link>
          )}
        </div>
      </div>

      {error && <div className="alert alert-error" style={{ marginBottom: 20 }}>{error}</div>}

      {pipelineStatus === "paused" && gateNeedsUI && (
        <div className="alert alert-warn" style={{ marginBottom: 20 }}>
          ⏸ Pipeline paused — <strong>{pendingReview} pairs</strong> need review.{" "}
          {gatePath && (
            <Link to={gatePath} style={{ color: "var(--warn)", fontWeight: 600 }}>
              Open {gateLabel} →
            </Link>
          )}
        </div>
      )}

      {pipelineStatus === "paused" && !gateNeedsUI && (
        <div className="alert alert-warn" style={{ marginBottom: 20 }}>
          ⏸ Pipeline paused at <strong>{gateLabel}</strong> — {pauseMessage}
          {gatePath && (
            <>{" "}<Link to={gatePath} style={{ color: "var(--warn)", fontWeight: 600 }}>Open →</Link></>
          )}
          {" "}<strong>Click Resume to continue after verifying.</strong>
        </div>
      )}

      {/* Stats */}
      {stats && (
        <div className="stat-grid">
          <div className="stat-card">
            <div className="stat-label">City Records</div>
            <div className="stat-value">{stats.total_city_records ?? "—"}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Bludot Records</div>
            <div className="stat-value">{stats.total_bludot_records ?? "—"}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Auto Matched</div>
            <div className="stat-value accent">{stats.auto_matched ?? "—"}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Human Reviewed</div>
            <div className="stat-value success">{(stats.human_accepted ?? 0) + (stats.human_rejected ?? 0)}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Pending Review</div>
            <div className="stat-value warn">{stats.pending_review ?? "—"}</div>
          </div>
          <div className="stat-card">
            <div className="stat-label">Total Matches</div>
            <div className="stat-value success">{stats.total_confirmed_matches ?? "—"}</div>
          </div>
        </div>
      )}

      {/* Pipeline Steps */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 20 }}>
        <div className="card">
          <h2 style={{ marginBottom: 20 }}>Pipeline Steps</h2>
          <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
            {STEPS.map((step, i) => {
              const log    = logMap[step.key];
              const stepStatus = log?.status;
              const isCurrent  = currentStep === step.key;

              return (
                <div key={step.key} style={{
                  display: "flex", alignItems: "flex-start", gap: 12,
                  padding: "10px 12px", borderRadius: 8,
                  background: isCurrent ? "rgba(79,142,247,0.07)" : "transparent",
                  border: isCurrent ? "1px solid rgba(79,142,247,0.2)" : "1px solid transparent",
                }}>
                  {/* Step indicator */}
                  <div style={{ display: "flex", flexDirection: "column", alignItems: "center", paddingTop: 1 }}>
                    <StepDot status={stepStatus} active={isCurrent} />
                    {i < STEPS.length - 1 && (
                      <div style={{
                        width: 1, height: 16, marginTop: 3,
                        background: stepStatus === "completed" ? "var(--success)" : "var(--border)",
                      }} />
                    )}
                  </div>

                  <div style={{ flex: 1 }}>
                    <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between" }}>
                      <span style={{
                        fontSize: 13, fontWeight: isCurrent ? 600 : 400,
                        color: isCurrent ? "var(--text)" :
                               stepStatus === "completed" ? "var(--text)" : "var(--text-muted)"
                      }}>
                        {step.label}
                      </span>
                      {stepStatus && <StatusBadge status={stepStatus} />}
                    </div>

                    {log?.message && (
                      <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 2 }}>
                        {log.message}
                      </div>
                    )}

                    {log?.stats && Object.keys(log.stats).length > 0 && (
                      <div style={{ display: "flex", gap: 12, marginTop: 4, flexWrap: "wrap" }}>
                        {Object.entries(log.stats).map(([k, v]) => (
                          <span key={k} style={{ fontSize: 11, fontFamily: "IBM Plex Mono, monospace", color: "var(--text-muted)" }}>
                            {k.replace(/_/g, " ")}: <strong style={{ color: "var(--text)" }}>{v}</strong>
                          </span>
                        ))}
                      </div>
                    )}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Match Breakdown */}
        <div style={{ display: "flex", flexDirection: "column", gap: 20 }}>
          {stats && <MatchBreakdown stats={stats} />}
          <div className="card">
            <h2 style={{ marginBottom: 12 }}>Files</h2>
            <div style={{ display: "flex", flexDirection: "column", gap: 8 }}>
              <FileRow label="City Sheet" path={city.raw_data_path} />
              <FileRow label="Bludot Export" path={city.bludot_export_path} />
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

function StepDot({ status, active }) {
  const color =
    status === "completed" ? "var(--success)" :
    status === "running"   ? "var(--accent)" :
    status === "paused"    ? "var(--warn)" :
    status === "failed"    ? "var(--danger)" :
    "var(--border2)";

  return (
    <div style={{
      width: 10, height: 10, borderRadius: "50%", flexShrink: 0,
      background: status ? color : "transparent",
      border: `2px solid ${color}`,
      boxShadow: active ? `0 0 0 3px rgba(79,142,247,0.2)` : "none",
    }} />
  );
}

function MatchBreakdown({ stats }) {
  const total = (stats.auto_matched || 0) + (stats.auto_rejected || 0) +
                (stats.pending_review || 0) + (stats.human_accepted || 0) + (stats.human_rejected || 0);

  if (!total) return null;

  const rows = [
    { label: "Auto Matched (LLM)",  value: stats.auto_matched,   color: "var(--accent)" },
    { label: "Human Accepted",      value: stats.human_accepted,  color: "var(--success)" },
    { label: "Pending Review",      value: stats.pending_review,  color: "var(--warn)" },
    { label: "Human Rejected",      value: stats.human_rejected,  color: "var(--danger)" },
    { label: "Auto Rejected (LLM)", value: stats.auto_rejected,   color: "var(--border2)" },
  ];

  return (
    <div className="card">
      <h2 style={{ marginBottom: 16 }}>Match Breakdown</h2>
      {rows.map(row => (
        <div key={row.label} style={{ marginBottom: 10 }}>
          <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 4 }}>
            <span style={{ fontSize: 12, color: "var(--text-muted)" }}>{row.label}</span>
            <span style={{ fontSize: 12, fontFamily: "IBM Plex Mono, monospace", color: row.color }}>
              {row.value ?? 0}
            </span>
          </div>
          <div className="progress-bar">
            <div className="progress-fill" style={{
              width: `${total > 0 ? ((row.value || 0) / total) * 100 : 0}%`,
              background: row.color,
            }} />
          </div>
        </div>
      ))}
    </div>
  );
}

function FileRow({ label, path }) {
  const filename = path?.split(/[\\/]/).pop() || "—";
  return (
    <div style={{
      display: "flex", justifyContent: "space-between", alignItems: "center",
      padding: "8px 12px", background: "var(--surface2)",
      borderRadius: 6, border: "1px solid var(--border)"
    }}>
      <span style={{ fontSize: 12, color: "var(--text-muted)" }}>{label}</span>
      <span style={{ fontSize: 12, fontFamily: "IBM Plex Mono, monospace", color: "var(--text)" }}>
        {filename}
      </span>
    </div>
  );
}
