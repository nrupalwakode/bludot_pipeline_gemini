import { useState, useEffect, useCallback, useRef } from "react";
import { useParams, useNavigate, useSearchParams, Link } from "react-router-dom";
import { API } from "../hooks/useApi";

/**
 * ReviewPage handles TWO review modes:
 *
 * 1. DEDUP mode  — pipeline paused at step0_dedup_review
 *    Endpoint: GET/POST /cities/{id}/dedup-review
 *    Decision: "DUPLICATE" | "NOT_DUPLICATE"
 *    Items have: pair_id, name_a, address_a, name_b, address_b, similarity, llm_reason
 *
 * 2. MATCH mode  — pipeline paused at step2_review or step4_1_review
 *    Endpoint: GET /cities/{id}/review?match_pass=N
 *    Decision: accepted (true/false)
 *    Items have: candidate_id, city_record, bludot_record, name_score, address_score, llm_reason
 */
export default function ReviewPage() {
  const { cityId }      = useParams();
  const navigate        = useNavigate();
  const [searchParams]  = useSearchParams();
  const matchPass       = parseInt(searchParams.get("pass") || "1");

  const [mode,          setMode]          = useState(null);
  const [activePass,    setActivePass]    = useState(matchPass);  // persists for reloads
  const [items,         setItems]         = useState([]);
  const [index,      setIndex]      = useState(0);
  const [loading,    setLoading]    = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [cityName,   setCityName]   = useState("");
  const [decisions,  setDecisions]  = useState({});
  const [note,       setNote]       = useState("");
  const [filter,     setFilter]     = useState("all");
  const [done,       setDone]       = useState(false);
  const noteRef = useRef(null);

  // Detect mode from pipeline status, then load items
  useEffect(() => {
    async function load() {
      try {
        const [cityData, statusData] = await Promise.all([
          API.get(`/cities/${cityId}`),
          API.get(`/cities/${cityId}/status`),
        ]);
        setCityName(cityData.name);

        const currentStep = statusData.current_step || "";
        const isDedup = currentStep === "step0_dedup_review";
        // Detect pass from pipeline step if not in URL
        const passFromStep = currentStep === "step4_1_review" ? 2 : 1;
        const effectivePass = parseInt(searchParams.get("pass") || String(passFromStep));
        setActivePass(effectivePass);

        if (isDedup) {
          setMode("dedup");
          const dedupData = await API.get(`/cities/${cityId}/dedup-review`);
          setItems(dedupData.items || []);
        } else {
          setMode("match");
          const reviewData = await API.get(`/cities/${cityId}/review?match_pass=${effectivePass}`);
          setItems(reviewData.items || []);
        }
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [cityId, matchPass]);

  const itemId = (item) => mode === "dedup" ? item.pair_id : item.candidate_id;

  const filteredItems = filter === "all"
    ? items
    : items.filter(it => decisions[itemId(it)] === undefined);

  const current   = filteredItems[index];
  const total     = filteredItems.length;
  const reviewed  = Object.keys(decisions).length;
  const remaining = items.filter(it => decisions[itemId(it)] === undefined).length;

  // Keyboard shortcuts
  useEffect(() => {
    function onKey(e) {
      if (e.target.tagName === "TEXTAREA" || e.target.tagName === "INPUT") return;
      if (e.key === "a" || e.key === "ArrowRight") handleDecide(mode === "dedup" ? "DUPLICATE"     : true);
      if (e.key === "r" || e.key === "ArrowLeft")  handleDecide(mode === "dedup" ? "NOT_DUPLICATE" : false);
      if (e.key === "ArrowDown" && index < total - 1) { setIndex(i => i + 1); setNote(""); }
      if (e.key === "ArrowUp"   && index > 0)          { setIndex(i => i - 1); setNote(""); }
      if (e.key === "n") noteRef.current?.focus();
    }
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [index, total, current, mode]);

  function handleDecide(value) {
    if (!current) return;
    setDecisions(d => ({ ...d, [itemId(current)]: value }));
    if (index < total - 1) { setIndex(i => i + 1); setNote(""); }
  }

  async function submitBatch() {
    if (Object.keys(decisions).length === 0) return;
    setSubmitting(true);
    try {
      if (mode === "dedup") {
        await API.post(`/cities/${cityId}/dedup-review/bulk`, {
          decisions,   // {pair_id: "DUPLICATE"|"NOT_DUPLICATE"}
          reviewer: "human",
        });
      } else {
        await API.post(`/cities/${cityId}/review/bulk`, {
          decisions,   // {candidate_id: true|false}
          reviewer: "human",
        });
      }

      const stillRemaining = items.filter(it => decisions[itemId(it)] === undefined).length;
      if (stillRemaining === 0) {
        setDone(true);
      } else {
        // Reload
        if (mode === "dedup") {
          const data = await API.get(`/cities/${cityId}/dedup-review`);
          setItems(data.items || []);
        } else {
          const data = await API.get(`/cities/${cityId}/review?match_pass=${activePass}`);
          setItems(data.items || []);
        }
        setDecisions({});
        setIndex(0);
      }
    } finally {
      setSubmitting(false);
    }
  }

  if (loading) return (
    <div style={{ display: "flex", justifyContent: "center", padding: 80 }}>
      <div className="spinner" style={{ width: 36, height: 36 }} />
    </div>
  );

  if (done) return <DoneScreen cityId={cityId} cityName={cityName} navigate={navigate} />;

  if (items.length === 0) return (
    <div>
      <Breadcrumb cityId={cityId} cityName={cityName} />
      <div className="card" style={{ textAlign: "center", padding: 64, marginTop: 20 }}>
        <div style={{ fontSize: 48, marginBottom: 16 }}>🎉</div>
        <h2>No items to review</h2>
        <p className="text-muted" style={{ marginTop: 8 }}>
          All {mode === "dedup" ? "deduplication pairs" : "match candidates"} were resolved automatically.
        </p>
        <Link to={`/city/${cityId}`} className="btn btn-primary" style={{ marginTop: 24 }}>
          ← Back to Pipeline
        </Link>
      </div>
    </div>
  );

  const modeLabel   = mode === "dedup" ? "Dedup Review" : `Match Review (Pass ${matchPass})`;
  const acceptLabel = mode === "dedup" ? "Duplicate (A)" : "Accept (A)";
  const rejectLabel = mode === "dedup" ? "Not Duplicate (R)" : "Reject (R)";

  const accepted = Object.values(decisions).filter(v => v === true || v === "DUPLICATE").length;
  const rejected = Object.values(decisions).filter(v => v === false || v === "NOT_DUPLICATE").length;

  return (
    <div>
      <Breadcrumb cityId={cityId} cityName={cityName} />

      {/* Top bar */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 20 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 16 }}>
          <h1>Review Queue</h1>
          <span className="badge badge-paused">{modeLabel}</span>
        </div>

        <div style={{ display: "flex", gap: 12 }}>
          <div style={{ display: "flex", gap: 4, background: "var(--surface2)", padding: 3, borderRadius: 7, border: "1px solid var(--border)" }}>
            {["all", "unreviewed"].map(f => (
              <button key={f} onClick={() => { setFilter(f); setIndex(0); }}
                style={{
                  padding: "5px 12px", borderRadius: 5, border: "none",
                  background: filter === f ? "var(--surface)" : "transparent",
                  color: filter === f ? "var(--text)" : "var(--text-muted)",
                  fontSize: 12, cursor: "pointer",
                  boxShadow: filter === f ? "0 1px 3px rgba(0,0,0,0.3)" : "none",
                }}>
                {f === "all" ? `All (${items.length})` : `Unreviewed (${remaining})`}
              </button>
            ))}
          </div>

          <button
            className="btn btn-primary"
            disabled={Object.keys(decisions).length === 0 || submitting}
            onClick={submitBatch}
          >
            {submitting
              ? <><span className="spinner" /> Saving…</>
              : `Submit ${Object.keys(decisions).length} Decisions`}
          </button>
        </div>
      </div>

      {/* Progress */}
      <div style={{ marginBottom: 20 }}>
        <div style={{ display: "flex", justifyContent: "space-between", marginBottom: 6 }}>
          <span style={{ fontSize: 12, color: "var(--text-muted)" }}>
            {index + 1} of {total} · {accepted} {mode === "dedup" ? "duplicates" : "accepted"} · {rejected} {mode === "dedup" ? "not duplicates" : "rejected"} · {remaining} remaining
          </span>
          <span style={{ fontSize: 12, fontFamily: "IBM Plex Mono, monospace", color: "var(--text-muted)" }}>
            A/→ {mode === "dedup" ? "duplicate" : "accept"} · R/← {mode === "dedup" ? "not duplicate" : "reject"} · N note · ↑↓ navigate
          </span>
        </div>
        <div className="progress-bar">
          <div className="progress-fill" style={{ width: `${total > 0 ? (reviewed / total) * 100 : 0}%` }} />
        </div>
      </div>

      {/* Navigator */}
      <div style={{ display: "flex", gap: 8, marginBottom: 16 }}>
        <button className="btn btn-ghost btn-sm" disabled={index === 0}
          onClick={() => { setIndex(i => i - 1); setNote(""); }}>← Prev</button>
        <button className="btn btn-ghost btn-sm" disabled={index >= total - 1}
          onClick={() => { setIndex(i => i + 1); setNote(""); }}>Next →</button>
        <input
          type="number" min={1} max={total} value={index + 1}
          onChange={e => { const v = parseInt(e.target.value) - 1; if (v >= 0 && v < total) setIndex(v); }}
          style={{ width: 70, padding: "4px 8px", fontSize: 12 }}
        />
        <span style={{ lineHeight: "30px", fontSize: 12, color: "var(--text-muted)" }}>/ {total}</span>
      </div>

      {current && (
        mode === "dedup"
          ? <DedupCard
              item={current}
              decision={decisions[itemId(current)]}
              note={note}
              noteRef={noteRef}
              onNote={setNote}
              onDuplicate={() => handleDecide("DUPLICATE")}
              onNotDuplicate={() => handleDecide("NOT_DUPLICATE")}
              onUndo={() => setDecisions(d => { const c = {...d}; delete c[itemId(current)]; return c; })}
            />
          : <MatchCard
              item={current}
              decision={decisions[itemId(current)]}
              note={note}
              noteRef={noteRef}
              onNote={setNote}
              onAccept={() => handleDecide(true)}
              onReject={() => handleDecide(false)}
              onUndo={() => setDecisions(d => { const c = {...d}; delete c[itemId(current)]; return c; })}
            />
      )}

      <QueueStrip
        items={filteredItems}
        current={index}
        decisions={decisions}
        mode={mode}
        getId={itemId}
        onSelect={i => { setIndex(i); setNote(""); }}
      />
    </div>
  );
}

// ── Dedup Review Card ────────────────────────────────────────────────────────

function DedupCard({ item, decision, note, noteRef, onNote, onDuplicate, onNotDuplicate, onUndo }) {
  const borderColor =
    decision === "DUPLICATE"     ? "var(--success)" :
    decision === "NOT_DUPLICATE" ? "var(--danger)"  : "var(--border)";

  return (
    <div style={{
      border: `1.5px solid ${borderColor}`, borderRadius: 12,
      overflow: "hidden", transition: "border-color 0.2s", marginBottom: 16,
    }}>
      {/* Decision banner */}
      {decision !== undefined && (
        <div style={{
          padding: "8px 20px",
          background: decision === "DUPLICATE" ? "rgba(52,211,153,0.12)" : "rgba(248,113,113,0.12)",
          borderBottom: `1px solid ${borderColor}`,
          display: "flex", alignItems: "center", justifyContent: "space-between",
        }}>
          <span style={{ fontWeight: 600, color: borderColor, fontSize: 13 }}>
            {decision === "DUPLICATE" ? "✓ Duplicate — will merge clusters" : "✗ Not Duplicate — different businesses"}
          </span>
          <button className="btn btn-ghost btn-sm" onClick={onUndo}>↩ Undo</button>
        </div>
      )}

      {/* Side-by-side */}
      <div style={{ display: "grid", gridTemplateColumns: "1fr 80px 1fr" }}>
        <RecordPanel label="Record A" record={{ name: item.name_a, address: item.address_a, raw: {} }} color="var(--accent)" />

        {/* Middle — similarity score */}
        <div style={{
          background: "var(--surface2)", borderLeft: "1px solid var(--border)",
          borderRight: "1px solid var(--border)",
          display: "flex", flexDirection: "column", alignItems: "center",
          justifyContent: "center", gap: 8, padding: "12px 4px",
        }}>
          <ScorePip label="Sim" value={Math.round((item.similarity || 0) * 100)} />
          <div style={{ fontSize: 9, color: "var(--text-muted)", textAlign: "center", lineHeight: 1.4 }}>
            {item.intra_cluster ? "Same\nCluster" : "Diff\nCluster"}
          </div>
        </div>

        <RecordPanel label="Record B" record={{ name: item.name_b, address: item.address_b, raw: {} }} color="var(--accent2)" />
      </div>

      {/* Reason */}
      <div style={{
        padding: "12px 20px", background: "var(--surface2)",
        borderTop: "1px solid var(--border)",
      }}>
        <span style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", marginRight: 10 }}>
          Reason
        </span>
        <span style={{ fontSize: 13, color: "var(--text)", fontStyle: "italic" }}>
          {item.llm_reason || "—"}
        </span>
      </div>

      {/* Action buttons */}
      <div style={{
        padding: "14px 20px", background: "var(--surface)",
        borderTop: "1px solid var(--border)",
        display: "flex", gap: 12, alignItems: "center",
      }}>
        <textarea
          ref={noteRef}
          placeholder="Optional note (press N to focus)"
          value={note}
          onChange={e => onNote(e.target.value)}
          rows={1}
          style={{ flex: 1, resize: "none", fontSize: 12, padding: "7px 10px" }}
        />
        <button className="btn btn-danger" style={{ minWidth: 140 }} onClick={onNotDuplicate}>
          ✗ Not Duplicate (R)
        </button>
        <button className="btn btn-success" style={{ minWidth: 130 }} onClick={onDuplicate}>
          ✓ Duplicate (A)
        </button>
      </div>
    </div>
  );
}

// ── Match Review Card ────────────────────────────────────────────────────────

function MatchCard({ item, decision, note, noteRef, onNote, onAccept, onReject, onUndo }) {
  const cr = item.city_record;
  const br = item.bludot_record;

  const borderColor =
    decision === true  ? "var(--success)" :
    decision === false ? "var(--danger)"  : "var(--border)";

  return (
    <div style={{
      border: `1.5px solid ${borderColor}`, borderRadius: 12,
      overflow: "hidden", transition: "border-color 0.2s", marginBottom: 16,
    }}>
      {decision !== undefined && (
        <div style={{
          padding: "8px 20px",
          background: decision ? "rgba(52,211,153,0.12)" : "rgba(248,113,113,0.12)",
          borderBottom: `1px solid ${borderColor}`,
          display: "flex", alignItems: "center", justifyContent: "space-between",
        }}>
          <span style={{ fontWeight: 600, color: borderColor, fontSize: 13 }}>
            {decision ? "✓ Accepted" : "✗ Rejected"}
          </span>
          <button className="btn btn-ghost btn-sm" onClick={onUndo}>↩ Undo</button>
        </div>
      )}

      <div style={{ display: "grid", gridTemplateColumns: "1fr 60px 1fr" }}>
        <RecordPanel label="City Record" record={{ name: cr.business_name, address: cr.address1, raw: cr.raw_data }} color="var(--accent)" />

        <div style={{
          background: "var(--surface2)", borderLeft: "1px solid var(--border)",
          borderRight: "1px solid var(--border)",
          display: "flex", flexDirection: "column", alignItems: "center",
          justifyContent: "center", gap: 12, padding: "12px 0",
        }}>
          <ScorePip label="Name" value={item.name_score} />
          <ScorePip label="Addr" value={item.address_score} />
          <div style={{ width: 1, height: 10, background: "var(--border)" }} />
          <StreetNumBadge match={item.street_num_match} />
        </div>

        <RecordPanel label="Bludot Record" record={{ name: br.name, address: br.address1, raw: br.raw_data }} color="var(--accent2)" />
      </div>

      <div style={{
        padding: "12px 20px", background: "var(--surface2)",
        borderTop: "1px solid var(--border)",
        display: "flex", flexDirection: "column", gap: 8,
      }}>
        <div style={{ display: "flex", alignItems: "flex-start", gap: 10 }}>
          <span style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", minWidth: 80, paddingTop: 1 }}>
            LLM Reason
          </span>
          <span style={{ fontSize: 13, color: "var(--text)", fontStyle: "italic" }}>
            {item.llm_reason || "—"}
          </span>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <span style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.5px", minWidth: 80 }}>
            Rule
          </span>
          <span style={{ fontSize: 12, color: "var(--text-muted)", fontFamily: "IBM Plex Mono, monospace" }}>
            {item.rule_verdict}
          </span>
        </div>
      </div>

      <div style={{
        padding: "14px 20px", background: "var(--surface)",
        borderTop: "1px solid var(--border)",
        display: "flex", gap: 12, alignItems: "center",
      }}>
        <textarea
          ref={noteRef}
          placeholder="Optional note (press N to focus)"
          value={note}
          onChange={e => onNote(e.target.value)}
          rows={1}
          style={{ flex: 1, resize: "none", fontSize: 12, padding: "7px 10px" }}
        />
        <button className="btn btn-danger"  style={{ minWidth: 100 }} onClick={onReject}>✗ Reject (R)</button>
        <button className="btn btn-success" style={{ minWidth: 100 }} onClick={onAccept}>✓ Accept (A)</button>
      </div>
    </div>
  );
}

// ── Shared components ────────────────────────────────────────────────────────

function RecordPanel({ label, record, color }) {
  const [expanded, setExpanded] = useState(false);
  const raw = record.raw || {};

  const SKIP_KEYS = ["city_index", "bludot_index", "cluster id", "norm_name",
                     "norm_address", "is_po_box", "po_box_num", "street_num",
                     "street_name", "business_address", "lsh_bucket"];

  const extraFields = Object.entries(raw)
    .filter(([k]) => !SKIP_KEYS.includes(k) && k !== "Business Name" && k !== "Address1"
                  && k !== "Name" && k !== "name");

  return (
    <div style={{ padding: 20 }}>
      <div style={{ fontSize: 11, fontWeight: 600, color, textTransform: "uppercase", letterSpacing: "0.6px", marginBottom: 12 }}>
        {label}
      </div>
      <div style={{ marginBottom: 6 }}>
        <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 2 }}>Business Name</div>
        <div style={{ fontSize: 15, fontWeight: 600, color: "var(--text)" }}>{record.name || "—"}</div>
      </div>
      <div style={{ marginBottom: 10 }}>
        <div style={{ fontSize: 11, color: "var(--text-muted)", marginBottom: 2 }}>Address</div>
        <div style={{ fontSize: 13, color: "var(--text)" }}>
          {record.address || <span style={{ color: "var(--text-muted)", fontStyle: "italic" }}>blank</span>}
        </div>
      </div>
      {extraFields.length > 0 && (
        <>
          <button onClick={() => setExpanded(e => !e)} style={{
            background: "none", border: "none", cursor: "pointer",
            color: "var(--text-muted)", fontSize: 11, padding: 0,
            display: "flex", alignItems: "center", gap: 4,
          }}>
            {expanded ? "▾" : "▸"} {extraFields.length} more fields
          </button>
          {expanded && (
            <div style={{ marginTop: 10, display: "flex", flexDirection: "column", gap: 5 }}>
              {extraFields.map(([k, v]) => (
                <div key={k} style={{ display: "flex", gap: 8, fontSize: 12 }}>
                  <span style={{ color: "var(--text-muted)", minWidth: 100, flexShrink: 0 }}>{k}</span>
                  <span style={{ color: "var(--text)", fontFamily: "IBM Plex Mono, monospace", wordBreak: "break-all" }}>
                    {String(v || "—")}
                  </span>
                </div>
              ))}
            </div>
          )}
        </>
      )}
    </div>
  );
}

function ScorePip({ label, value }) {
  const pct = value ?? 0;
  const color = pct >= 80 ? "var(--success)" : pct >= 55 ? "var(--warn)" : "var(--danger)";
  return (
    <div style={{ display: "flex", flexDirection: "column", alignItems: "center", gap: 2 }}>
      <div style={{
        width: 32, height: 32, borderRadius: "50%",
        background: `conic-gradient(${color} ${pct * 3.6}deg, var(--border) 0deg)`,
        display: "flex", alignItems: "center", justifyContent: "center",
      }}>
        <div style={{
          width: 22, height: 22, borderRadius: "50%",
          background: "var(--surface2)",
          display: "flex", alignItems: "center", justifyContent: "center",
          fontSize: 9, fontWeight: 700, fontFamily: "IBM Plex Mono, monospace", color,
        }}>
          {Math.round(pct)}
        </div>
      </div>
      <span style={{ fontSize: 9, color: "var(--text-muted)", textTransform: "uppercase", letterSpacing: "0.4px" }}>{label}</span>
    </div>
  );
}

function StreetNumBadge({ match }) {
  if (match === null || match === undefined) {
    return <div style={{ fontSize: 9, color: "var(--text-muted)", textAlign: "center", lineHeight: 1.3 }}>ST#<br/>blank</div>;
  }
  return (
    <div style={{ fontSize: 9, fontWeight: 700, color: match ? "var(--success)" : "var(--danger)", textAlign: "center", lineHeight: 1.3 }}>
      ST#<br/>{match ? "✓" : "✗"}
    </div>
  );
}

function QueueStrip({ items, current, decisions, mode, getId, onSelect }) {
  return (
    <div style={{ display: "flex", gap: 4, overflowX: "auto", padding: "12px 0", marginTop: 8 }}>
      {items.map((item, i) => {
        const dec = decisions[getId(item)];
        const isAccepted = dec === true || dec === "DUPLICATE";
        const isRejected = dec === false || dec === "NOT_DUPLICATE";
        const bg = isAccepted ? "rgba(52,211,153,0.25)" :
                   isRejected ? "rgba(248,113,113,0.25)" :
                   i === current ? "var(--surface2)" : "transparent";
        const border = i === current ? "var(--accent)" :
                       isAccepted   ? "var(--success)" :
                       isRejected   ? "var(--danger)"  : "var(--border)";
        return (
          <button key={getId(item)} onClick={() => onSelect(i)} style={{
            flexShrink: 0, width: 32, height: 32, borderRadius: 6,
            background: bg, border: `1.5px solid ${border}`,
            cursor: "pointer", fontSize: 11,
            fontFamily: "IBM Plex Mono, monospace",
            color: i === current ? "var(--accent)" : "var(--text-muted)",
            fontWeight: i === current ? 700 : 400,
          }}>
            {i + 1}
          </button>
        );
      })}
    </div>
  );
}

function DoneScreen({ cityId, cityName, navigate }) {
  return (
    <div>
      <Breadcrumb cityId={cityId} cityName={cityName} />
      <div className="card" style={{ textAlign: "center", padding: 64, marginTop: 20 }}>
        <div style={{ fontSize: 48, marginBottom: 16 }}>✅</div>
        <h2>Review Complete</h2>
        <p className="text-muted" style={{ marginTop: 8, marginBottom: 28 }}>
          All pairs reviewed. You can now resume the pipeline.
        </p>
        <div style={{ display: "flex", gap: 12, justifyContent: "center" }}>
          <button className="btn btn-primary" style={{ padding: "10px 28px" }}
            onClick={async () => {
              await API.post(`/cities/${cityId}/resume`);
              navigate(`/city/${cityId}`);
            }}>
            ▶ Resume Pipeline
          </button>
          <Link to={`/city/${cityId}`} className="btn btn-ghost">View Dashboard</Link>
        </div>
      </div>
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
      <span style={{ color: "var(--text)" }}>Review Queue</span>
    </div>
  );
}
