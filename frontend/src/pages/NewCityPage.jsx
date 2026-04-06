import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { API } from "../hooks/useApi";

// ── Fixed Business schema ─────────────────────────────────────────────────────
const BUSINESS_FIELDS = [
  { field: "Business Name",             required: true },
  { field: "Address1",                  required: true },
  { field: "Address2",                  required: false },
  { field: "City",                      required: false },
  { field: "State",                     required: false },
  { field: "Country",                   required: false },
  { field: "Zipcode",                   required: false },
  { field: "Phonenumber",               required: false },
  { field: "Website",                   required: false },
  { field: "Lat",                       required: false },
  { field: "Long",                      required: false },
  { field: "DBA Name",                  required: false },
  { field: "Business Operational Status", required: false },
];

const CONTACT_TYPES = ["email", "phone_number", "address"];
const TYPE_OPTIONS  = ["office", "home", "mobile", "direct", "fax", "others"];
const CONTACT_ROLES = ["Owner", "Manager", "Agent", "Contact", "Other"];

export default function NewCityPage() {
  const navigate = useNavigate();
  const [step, setStep]             = useState(1);
  const [cityName, setCityName]     = useState("");
  const [cityType, setCityType]     = useState("City");
  const [rawFile, setRawFile]       = useState(null);
  const [bludotFile, setBludotFile] = useState(null);
  const [uploading, setUploading]   = useState(false);
  const [error, setError]           = useState("");
  const [saving, setSaving]         = useState(false);

  const [cityId, setCityId]             = useState(null);
  const [detectedCols, setDetectedCols] = useState([]);
  const [activeTab, setActiveTab]       = useState("business");
  const [llmSuggesting, setLlmSuggesting] = useState(false);
  const [llmDone, setLlmDone]           = useState(false);
  const [bludotCustomCols, setBludotCustomCols] = useState([]);

  const [businessMap, setBusinessMap] = useState({});
  const [contactRows, setContactRows] = useState([]);
  const [customRows, setCustomRows]   = useState([]);

  // ── LLM suggest mapping ─────────────────────────────────────────────────────
  async function handleSuggestMapping() {
    setLlmSuggesting(true);
    setError("");
    try {
      const data = await API.post(`/cities/${cityId}/suggest-mapping`);
      setBludotCustomCols(data.bludot_custom_cols || []);
      const suggestions = data.suggestions || [];

      const newBizMap = { ...businessMap };
      const newContactRows = [];
      const newCustomRows  = [];

      for (const s of suggestions) {
        if (s.mapping_type === "business") {
          newBizMap[s.source_col] = s.target_col || "SKIP";
        } else if (s.mapping_type === "contact") {
          newContactRows.push({
            sourceCol:      s.source_col,
            contactType:    s.meta?.contact_type || "email",
            typeVal:        s.meta?.type || "office",
            role:           s.meta?.role || "Contact",
            personCol:      s.meta?.person_col || "",
            personColParts: s.meta?.person_col_parts || [],
          });
        } else if (s.mapping_type === "custom") {
          newCustomRows.push({
            sourceCol:       s.source_col,
            targetLabel:     s.target_col || s.source_col,
            bludotCustomCol: s.meta?.bludot_custom_col || "",
          });
        }
      }

      setBusinessMap(newBizMap);
      setContactRows(newContactRows);
      setCustomRows(newCustomRows);
      setLlmDone(true);
    } catch (err) {
      setError(`LLM suggest failed: ${err.message}`);
    } finally {
      setLlmSuggesting(false);
    }
  }

  // ── Step 1: Upload ──────────────────────────────────────────────────────────
  async function handleUpload(e) {
    e.preventDefault();
    if (!cityName || !rawFile || !bludotFile) { setError("All fields are required"); return; }
    setError("");
    setUploading(true);
    try {
      const form = new FormData();
      form.append("name", cityName);
      form.append("city_or_county", cityType);
      form.append("raw_data_file", rawFile);
      form.append("bludot_export_file", bludotFile);

      const res = await fetch("http://localhost:8000/cities/", { method: "POST", body: form });
      if (!res.ok) throw new Error(await res.text());
      const data = await res.json();

      const cols = data.detected_columns || [];
      setCityId(data.city_id);
      setDetectedCols(cols);

      // Auto-guess business fields
      const bMap = {};
      cols.forEach(col => { bMap[col] = guessBusinessField(col); });
      setBusinessMap(bMap);

      // Auto-guess contact rows
      const cRows = [];
      cols.forEach(col => {
        const guess = guessContactField(col);
        if (guess) cRows.push({ sourceCol: col, ...guess, typeVal: "office", personCol: "", personColParts: [] });
      });
      setContactRows(cRows);

      // Remaining → custom
      const bAssigned = new Set(Object.entries(bMap).filter(([, v]) => v !== "SKIP").map(([k]) => k));
      const cAssigned = new Set(cRows.map(r => r.sourceCol));
      setCustomRows(
        cols.filter(c => !bAssigned.has(c) && !cAssigned.has(c))
            .map(col => ({ sourceCol: col, targetLabel: col, bludotCustomCol: "" }))
      );

      setStep(2);
    } catch (err) {
      setError(err.message);
    } finally {
      setUploading(false);
    }
  }

  // ── Step 2: Save mapping ────────────────────────────────────────────────────
  async function handleSaveMapping(e) {
    e.preventDefault();
    const mappedFields = Object.values(businessMap).filter(v => v !== "SKIP");
    if (!mappedFields.includes("Business Name")) {
      setError("Business Name is required — map it in the Business Fields tab");
      setActiveTab("business"); return;
    }
    if (!mappedFields.includes("Address1")) {
      setError("Address1 is required — map it in the Business Fields tab");
      setActiveTab("business"); return;
    }
    setError("");
    setSaving(true);
    try {
      const mappings = [];

      Object.entries(businessMap).forEach(([source_col, target_col]) => {
        if (target_col && target_col !== "SKIP")
          mappings.push({ source_col, target_col, mapping_type: "business" });
      });

      contactRows.forEach(row => {
        mappings.push({
          source_col  : row.sourceCol,
          target_col  : `[${row.contactType}]`,
          mapping_type: "contact",
          meta: {
            role:             row.role,
            contact_type:     row.contactType,
            type:             row.typeVal || "office",
            person_col:       (row.personColParts?.length > 0) ? "" : (row.personCol || ""),
            person_col_parts: (row.personColParts?.length > 0) ? row.personColParts : [],
          },
        });
      });

      customRows.forEach(row => {
        if (row.sourceCol)
          mappings.push({
            source_col  : row.sourceCol,
            target_col  : row.targetLabel || row.sourceCol,
            mapping_type: "custom",
            meta        : { bludot_custom_col: row.bludotCustomCol },
          });
      });

      await API.post(`/cities/${cityId}/column-mapping`, { mappings });
      setStep(3);
    } catch (err) {
      setError(err.message);
    } finally {
      setSaving(false);
    }
  }

  async function handleStartPipeline() {
    setError("");
    try {
      await API.post(`/cities/${cityId}/start`);
      navigate(`/city/${cityId}`);
    } catch (err) {
      setError(err.message);
    }
  }

  const businessMapped = Object.values(businessMap).filter(v => v !== "SKIP").length;

  return (
    <div style={{ maxWidth: 860 }}>
      <div className="page-header">
        <h1>New City</h1>
        <StepIndicator current={step} />
      </div>

      {error && <div className="alert alert-error">{error}</div>}

      {/* ── STEP 1: Upload ── */}
      {step === 1 && (
        <form onSubmit={handleUpload}>
          <div className="card">
            <h2 style={{ marginBottom: 20 }}>Upload Files</h2>
            <div className="form-group">
              <label>City / County Name *</label>
              <input value={cityName} onChange={e => setCityName(e.target.value)}
                placeholder="e.g. Medford_OR" />
            </div>
            <div className="form-group">
              <label>Type</label>
              <select value={cityType} onChange={e => setCityType(e.target.value)}>
                <option value="City">City</option>
                <option value="County">County</option>
              </select>
            </div>
            <div className="form-group">
              <label>City Data Sheet (CSV / Excel) *</label>
              <input type="file" accept=".csv,.xlsx,.xls"
                onChange={e => setRawFile(e.target.files[0])} />
            </div>
            <div className="form-group">
              <label>Bludot Export (Excel, multi-sheet) *</label>
              <input type="file" accept=".xlsx,.xls"
                onChange={e => setBludotFile(e.target.files[0])} />
            </div>
            <button type="submit" className="btn btn-primary" disabled={uploading}>
              {uploading ? <><span className="spinner" /> Uploading…</> : "Upload & Detect Columns →"}
            </button>
          </div>
        </form>
      )}

      {/* ── STEP 2: Column Mapping ── */}
      {step === 2 && (
        <form onSubmit={handleSaveMapping}>
          <div className="card" style={{ padding: 0 }}>
            {/* Tab bar */}
            <div style={{ display: "flex", borderBottom: "1px solid var(--border)", padding: "0 20px" }}>
              {[
                { key: "business", label: "Business Fields", count: businessMapped,       color: "var(--accent)" },
                { key: "contact",  label: "Contact Fields",  count: contactRows.length,   color: "var(--accent2)" },
                { key: "custom",   label: "Custom Fields",   count: customRows.length,    color: "var(--warn)" },
              ].map(tab => (
                <button key={tab.key} type="button" onClick={() => setActiveTab(tab.key)}
                  style={{
                    padding: "14px 18px", border: "none", background: "none",
                    cursor: "pointer", fontSize: 13,
                    fontWeight: activeTab === tab.key ? 600 : 400,
                    color: activeTab === tab.key ? tab.color : "var(--text-muted)",
                    borderBottom: activeTab === tab.key ? `2px solid ${tab.color}` : "2px solid transparent",
                    marginBottom: -1,
                    display: "flex", alignItems: "center", gap: 8,
                  }}>
                  {tab.label}
                  <span style={{
                    background: activeTab === tab.key ? tab.color : "var(--surface2)",
                    color: activeTab === tab.key ? "#fff" : "var(--text-muted)",
                    borderRadius: 10, padding: "1px 7px", fontSize: 11, fontWeight: 600,
                  }}>{tab.count}</span>
                </button>
              ))}
            </div>

            <div style={{ padding: 20 }}>
              {activeTab === "business" && (
                <BusinessTab detectedCols={detectedCols} businessMap={businessMap} setBusinessMap={setBusinessMap} />
              )}
              {activeTab === "contact" && (
                <ContactTab detectedCols={detectedCols} contactRows={contactRows} setContactRows={setContactRows} />
              )}
              {activeTab === "custom" && (
                <CustomTab detectedCols={detectedCols} customRows={customRows} setCustomRows={setCustomRows} businessMap={businessMap} contactRows={contactRows} />
              )}
            </div>

            <div style={{ padding: "16px 20px", borderTop: "1px solid var(--border)", display: "flex", gap: 12 }}>
              <button type="button" className="btn btn-ghost" onClick={() => setStep(1)}>← Back</button>
              <button type="submit" className="btn btn-primary" disabled={saving}>
                {saving ? <><span className="spinner" /> Saving…</> : "Save Mapping →"}
              </button>
            </div>
          </div>
        </form>
      )}

      {/* ── STEP 3: Start ── */}
      {step === 3 && (
        <div className="card" style={{ textAlign: "center", padding: 48 }}>
          <div style={{ fontSize: 48, marginBottom: 16 }}>✅</div>
          <h2>Ready to run</h2>
          <p className="text-muted" style={{ marginTop: 8, marginBottom: 28 }}>
            Files uploaded and columns mapped. Start the pipeline to begin processing.
          </p>
          <button className="btn btn-primary" onClick={handleStartPipeline}
            style={{ padding: "10px 28px", fontSize: 14 }}>
            ▶ Start Pipeline
          </button>
        </div>
      )}
    </div>
  );
}

// ── Business Fields Tab ───────────────────────────────────────────────────────
function BusinessTab({ detectedCols, businessMap, setBusinessMap }) {
  return (
    <div>
      <p className="text-muted" style={{ fontSize: 13, marginBottom: 14 }}>
        Map source columns to the fixed output schema.
      </p>
      {/* Schema pills */}
      <div style={{
        display: "flex", flexWrap: "wrap", gap: 6, marginBottom: 18,
        padding: "10px 14px", background: "var(--surface2)", borderRadius: 8, border: "1px solid var(--border)",
      }}>
        {["ID (auto)", ...BUSINESS_FIELDS.map(f => f.field + (f.required ? " *" : "")), "is_business (auto)", "business_source (auto)"].map(f => {
          const raw = f.replace(" *", "").replace(" (auto)", "");
          const mapped = Object.values(businessMap).includes(raw);
          const isAuto = f.includes("(auto)");
          return (
            <span key={f} style={{
              padding: "3px 9px", borderRadius: 5, fontSize: 11,
              fontFamily: "IBM Plex Mono, monospace",
              background: isAuto ? "transparent" : mapped ? "rgba(79,142,247,0.2)" : "var(--surface)",
              color: isAuto ? "var(--border2)" : mapped ? "var(--accent)" : "var(--text-muted)",
              border: `1px solid ${isAuto ? "var(--border)" : mapped ? "rgba(79,142,247,0.4)" : "var(--border)"}`,
              opacity: isAuto ? 0.6 : 1,
            }}>{f}</span>
          );
        })}
      </div>

      <div style={{ display: "grid", gap: 8 }}>
        <div style={{ display: "grid", gridTemplateColumns: "1fr 28px 1fr", gap: 10, marginBottom: 2 }}>
          <span style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase" }}>Source Column</span>
          <span />
          <span style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase" }}>Maps To</span>
        </div>
        {detectedCols.map(col => (
          <div key={col} style={{ display: "grid", gridTemplateColumns: "1fr 28px 1fr", alignItems: "center", gap: 10 }}>
            <div style={{
              background: "var(--surface2)", border: "1px solid var(--border2)",
              padding: "8px 12px", borderRadius: 6, fontSize: 13,
              fontFamily: "IBM Plex Mono, monospace", color: "var(--text)",
              overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap",
            }}>{col}</div>
            <span style={{ textAlign: "center", color: "var(--text-muted)", fontSize: 12 }}>→</span>
            <select value={businessMap[col] || "SKIP"}
              onChange={e => setBusinessMap({ ...businessMap, [col]: e.target.value })}>
              <option value="SKIP">— Skip / Not a business field —</option>
              {BUSINESS_FIELDS.map(f => (
                <option key={f.field} value={f.field}>{f.field}{f.required ? " *" : ""}</option>
              ))}
            </select>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── Contact Fields Tab ────────────────────────────────────────────────────────
function ContactTab({ detectedCols, contactRows, setContactRows }) {
  function addRow() {
    setContactRows([...contactRows, { sourceCol: detectedCols[0] || "", role: "Owner", contactType: "email", typeVal: "office", personCol: "", personColParts: [] }]);
  }
  
  function updateMulti(i, updates) {
    const u = [...contactRows];
    u[i] = { ...u[i], ...updates };
    setContactRows(u);
  }

  function update(i, field, value) {
    updateMulti(i, { [field]: value });
  }

  function remove(i) { setContactRows(contactRows.filter((_, idx) => idx !== i)); }

  return (
    <div>
      <p className="text-muted" style={{ fontSize: 13, marginBottom: 16 }}>
        Map columns containing contact info (emails, phones). Link the person's name using the checkboxes.
      </p>

      {contactRows.length === 0 ? (
        <div style={{
          textAlign: "center", padding: "28px 20px", border: "1px dashed var(--border2)", 
          borderRadius: 8, color: "var(--text-muted)", marginBottom: 16, fontSize: 13,
        }}>
          Click <strong>+ Add Row</strong> to begin.
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 12, marginBottom: 16 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1.3fr 1fr 1fr 1fr 1.5fr 28px", gap: 8 }}>
            {["Value Column", "Contact Type", "Type", "Role", "Person Name Column(s)", ""].map(h => (
              <span key={h} style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase" }}>{h}</span>
            ))}
          </div>
          {contactRows.map((row, i) => (
            <div key={i} style={{ display: "flex", flexDirection: "column", gap: 6 }}>
              <div style={{ display: "grid", gridTemplateColumns: "1.3fr 1fr 1fr 1fr 1.5fr 28px", gap: 8, alignItems: "start" }}>
                
                <select value={row.sourceCol} onChange={e => update(i, "sourceCol", e.target.value)}>
                  {detectedCols.map(c => <option key={c} value={c}>{c}</option>)}
                </select>
                
                <select value={row.contactType} onChange={e => update(i, "contactType", e.target.value)}>
                  {CONTACT_TYPES.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
                
                <select value={row.typeVal || "office"} onChange={e => update(i, "typeVal", e.target.value)}>
                  {TYPE_OPTIONS.map(t => <option key={t} value={t}>{t}</option>)}
                </select>
                
                <select value={row.role} onChange={e => update(i, "role", e.target.value)}>
                  {CONTACT_ROLES.map(r => <option key={r} value={r}>{r}</option>)}
                </select>
                
                {/* Person name — single col or multi-select checkboxes */}
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  <select
                    value={row.personCol || ""}
                    onChange={e => {
                      updateMulti(i, {
                        personCol: e.target.value,
                        ...(e.target.value ? { personColParts: [] } : {})
                      });
                    }}
                  >
                    <option value="">— Single column —</option>
                    {detectedCols.map(c => <option key={c} value={c}>{c}</option>)}
                  </select>
                  
                  <div style={{ fontSize: 11, color: "var(--text-muted)" }}>
                    Or check multiple to concatenate:
                  </div>
                  
                  {/* UX FIX: Checkboxes with word-wrapping */}
                  <div style={{ 
                    maxHeight: 140, overflowY: "auto", border: "1px solid var(--border2)", 
                    borderRadius: 4, padding: "6px 8px", background: "var(--surface)", 
                    display: "flex", flexDirection: "column", gap: 8 
                  }}>
                    {detectedCols.map(c => {
                      const isSelected = (row.personColParts || []).includes(c);
                      return (
                        <label 
                          key={c} title={c}
                          style={{ display: "flex", alignItems: "flex-start", gap: 6, fontSize: 12, cursor: "pointer", margin: 0 }}
                        >
                          <input
                            type="checkbox"
                            style={{ marginTop: 2 }}
                            checked={isSelected}
                            onChange={(e) => {
                              let currentParts = [...(row.personColParts || [])];
                              if (e.target.checked) currentParts.push(c);
                              else currentParts = currentParts.filter(p => p !== c);
                              
                              updateMulti(i, {
                                personColParts: currentParts,
                                ...(currentParts.length > 0 ? { personCol: "" } : {})
                              });
                            }}
                          />
                          <span style={{ wordBreak: "break-word", lineHeight: 1.3 }}>{c}</span>
                        </label>
                      );
                    })}
                  </div>

                  {(row.personColParts || []).length > 0 && (
                    <span style={{ fontSize: 11, color: "var(--accent)", wordBreak: "break-all" }}>
                      ✓ {row.personColParts.join(" + ")}
                    </span>
                  )}
                </div>
                
                <button type="button" onClick={() => remove(i)}
                  style={{ background: "none", border: "none", cursor: "pointer", color: "var(--danger)", fontSize: 18, padding: 0, marginTop: 4 }}>×</button>
              </div>
            </div>
          ))}
        </div>
      )}
      <button type="button" className="btn btn-ghost btn-sm" onClick={addRow}>+ Add Row</button>
    </div>
  );
}

// ── Custom Fields Tab ─────────────────────────────────────────────────────────
function CustomTab({ detectedCols, customRows, setCustomRows, businessMap, contactRows }) {
  // UX FIX: Now checks personCol and personColParts so names don't show as "unmapped"
  const taken = new Set([
    ...Object.entries(businessMap).filter(([,v]) => v !== "SKIP").map(([k]) => k),
    ...contactRows.map(r => r.sourceCol),
    ...contactRows.map(r => r.personCol).filter(Boolean),
    ...contactRows.flatMap(r => r.personColParts || []),
  ]);
  
  const unmapped = detectedCols.filter(c => !taken.has(c) && !customRows.find(r => r.sourceCol === c));

  function addRow(col = "") {
    setCustomRows([...customRows, { sourceCol: col, targetLabel: col, bludotCustomCol: "" }]);
  }
  function update(i, field, value) {
    const u = [...customRows];
    u[i] = { ...u[i], [field]: value };
    if (field === "sourceCol" && !u[i].targetLabel) u[i].targetLabel = value;
    setCustomRows(u);
  }
  function remove(i) { setCustomRows(customRows.filter((_, idx) => idx !== i)); }

  return (
    <div>
      <p className="text-muted" style={{ fontSize: 13, marginBottom: 16 }}>
        Any field not mapped to Business or Contact goes here.
      </p>

      {customRows.length === 0 ? (
        <div style={{
          textAlign: "center", padding: "28px 20px", border: "1px dashed var(--border2)", 
          borderRadius: 8, color: "var(--text-muted)", marginBottom: 16, fontSize: 13,
        }}>
          No custom fields mapped yet.
        </div>
      ) : (
        <div style={{ display: "flex", flexDirection: "column", gap: 8, marginBottom: 16 }}>
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 28px", gap: 8 }}>
            {["Source Column", "Output Label", "Bludot Custom Field (opt.)", ""].map(h => (
              <span key={h} style={{ fontSize: 11, fontWeight: 600, color: "var(--text-muted)", textTransform: "uppercase" }}>{h}</span>
            ))}
          </div>
          {customRows.map((row, i) => (
            <div key={i} style={{ display: "grid", gridTemplateColumns: "1fr 1fr 1fr 28px", gap: 8, alignItems: "center" }}>
              <select value={row.sourceCol} onChange={e => update(i, "sourceCol", e.target.value)}>
                <option value="">— Select column —</option>
                {detectedCols.map(c => <option key={c} value={c}>{c}</option>)}
              </select>
              <input value={row.targetLabel} onChange={e => update(i, "targetLabel", e.target.value)} placeholder="Output column name" />
              <input value={row.bludotCustomCol} onChange={e => update(i, "bludotCustomCol", e.target.value)} placeholder="e.g. Employee Count" />
              <button type="button" onClick={() => remove(i)}
                style={{ background: "none", border: "none", cursor: "pointer", color: "var(--danger)", fontSize: 18, padding: 0 }}>×</button>
            </div>
          ))}
        </div>
      )}

      <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
        <button type="button" className="btn btn-ghost btn-sm" onClick={() => addRow()}>+ Add Custom Field</button>
        {unmapped.length > 0 && (
          <button type="button" className="btn btn-ghost btn-sm" onClick={() => unmapped.forEach(c => addRow(c))} style={{ color: "var(--text-muted)" }}>
            + Add All Unmapped ({unmapped.length})
          </button>
        )}
      </div>

      {unmapped.length > 0 && (
        <div style={{ marginTop: 14 }}>
          <p style={{ fontSize: 12, color: "var(--text-muted)", marginBottom: 8 }}>Columns not yet mapped anywhere:</p>
          <div style={{ display: "flex", flexWrap: "wrap", gap: 6 }}>
            {unmapped.map(c => (
              <button key={c} type="button" onClick={() => addRow(c)} style={{
                padding: "3px 10px", borderRadius: 5, fontSize: 12, fontFamily: "IBM Plex Mono, monospace",
                background: "var(--surface2)", border: "1px solid var(--border2)", color: "var(--text-muted)", cursor: "pointer",
              }}>+ {c}</button>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

// ── Step indicator ────────────────────────────────────────────────────────────
function StepIndicator({ current }) {
  return (
    <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
      {["Upload", "Mapping", "Run"].map((label, i) => {
        const n = i + 1;
        const done = n < current; const active = n === current;
        return (
          <div key={n} style={{ display: "flex", alignItems: "center", gap: 8 }}>
            {i > 0 && <div style={{ width: 20, height: 1, background: "var(--border2)" }} />}
            <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
              <div style={{
                width: 22, height: 22, borderRadius: "50%",
                display: "flex", alignItems: "center", justifyContent: "center",
                fontSize: 11, fontWeight: 600,
                background: done ? "var(--success)" : active ? "var(--accent)" : "var(--surface2)",
                color: done || active ? "#fff" : "var(--text-muted)",
                border: `1px solid ${done ? "var(--success)" : active ? "var(--accent)" : "var(--border2)"}`,
              }}>{done ? "✓" : n}</div>
              <span style={{ fontSize: 12, color: active ? "var(--text)" : "var(--text-muted)" }}>{label}</span>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// ── Auto-guess helpers ────────────────────────────────────────────────────────
function guessBusinessField(col) {
  const c = col.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (/businessname|dbaname|companyname|bizname/.test(c) && !/owner|contact/.test(c)) return "Business Name";
  if (/^name$/.test(c)) return "Business Name";
  if ((/address1|streetaddress|addr1|^address$/).test(c) && !/2|two|owner/.test(c)) return "Address1";
  if (/address2|addr2|suite|unit/.test(c)) return "Address2";
  if (/^city$/.test(c)) return "City";
  if (/^state$|statecode/.test(c)) return "State";
  if (/^country$/.test(c)) return "Country";
  if (/^zip|postal|zipcode/.test(c)) return "Zipcode";
  if (/^phone$|businessphone/.test(c) && !/owner/.test(c)) return "Phonenumber";
  if (/website|^web$|^url$/.test(c)) return "Website";
  if (/^lat$|latitude/.test(c)) return "Lat";
  if (/^lon$|^lng$|^long$|longitude/.test(c)) return "Long";
  return "SKIP";
}

function guessContactField(col) {
  const c = col.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (/owneremail|owner.*email/.test(c))     return { contactType: "email",        role: "Owner" };
  if (/ownerphone|owner.*phone/.test(c))     return { contactType: "phone_number", role: "Owner" };
  if (/manageremail|manager.*email/.test(c)) return { contactType: "email",        role: "Manager" };
  if (/managerphone|manager.*phone/.test(c)) return { contactType: "phone_number", role: "Manager" };
  if (/contactemail|contact.*email/.test(c)) return { contactType: "email",        role: "Contact" };
  if (/contactphone|contact.*phone/.test(c)) return { contactType: "phone_number", role: "Contact" };
  return null;
}