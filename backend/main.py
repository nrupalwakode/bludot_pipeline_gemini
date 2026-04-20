"""
FastAPI Application
===================
All REST endpoints consumed by the React frontend.

Endpoints:
  POST /cities/                    → Create new city + upload files
  GET  /cities/                    → List all cities
  GET  /cities/{city_id}           → City detail + pipeline status
  POST /cities/{city_id}/start     → Start pipeline
  POST /cities/{city_id}/resume    → Resume paused pipeline
  GET  /cities/{city_id}/status    → Live pipeline status (polling)

  GET  /cities/{city_id}/column-mapping         → Get detected columns
  POST /cities/{city_id}/column-mapping         → Save column mapping

  GET  /cities/{city_id}/review                 → Get review queue
  GET  /cities/{city_id}/review?pass=2          → Second-pass review queue
  POST /cities/{city_id}/review/{candidate_id}  → Submit human decision

  GET  /cities/{city_id}/stats                  → Match statistics
  GET  /cities/{city_id}/logs                   → Pipeline step logs
"""

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Form, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from .db.session import get_db, init_db
from .db.models import (
    City, PipelineRun, PipelineStatus, PipelineStep,
    ColumnMapping, MatchCandidate, MatchDecision,
    CityRecord, BludotRecord
)
from .core.matching_orchestrator import (
    get_review_queue, apply_human_decision, get_match_stats
)
from .pipeline.pipeline import run_city_pipeline, resume_city_pipeline

# ── App setup ─────────────────────────────────────────────────────────────────

app = FastAPI(
    title="Bludot Pipeline API",
    description="Automated business record matching pipeline",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)


@app.on_event("startup")
def startup():
    init_db()


# ── Pydantic schemas ──────────────────────────────────────────────────────────

class ColumnMappingItem(BaseModel):
    source_col:   str
    target_col:   str
    mapping_type: str = "business"   # business | contact | custom
    meta:         dict = {}


class ColumnMappingRequest(BaseModel):
    mappings: list[ColumnMappingItem]


class ReviewDecision(BaseModel):
    accepted: bool
    reviewer: str = "human"
    note: str = ""


class BulkReviewDecision(BaseModel):
    decisions: dict[str, bool]   # {candidate_id: accepted}
    reviewer: str = "human"


# ── City endpoints ────────────────────────────────────────────────────────────

@app.post("/cities/")
async def create_city(
    name: str = Form(...),
    city_or_county: str = Form("City"),
    raw_data_file: UploadFile = File(...),
    bludot_export_file: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    """Upload city files and create a new city record."""
    city_dir = UPLOAD_DIR / name.replace(" ", "_")
    city_dir.mkdir(exist_ok=True)

    raw_path    = city_dir / raw_data_file.filename
    bludot_path = city_dir / bludot_export_file.filename

    with open(raw_path, "wb") as f:
        shutil.copyfileobj(raw_data_file.file, f)
    with open(bludot_path, "wb") as f:
        shutil.copyfileobj(bludot_export_file.file, f)

    city = City(
        name               = name,
        city_or_county     = city_or_county,
        raw_data_path      = str(raw_path),
        bludot_export_path = str(bludot_path),
    )
    db.add(city)
    db.commit()
    db.refresh(city)

    # Auto-detect columns from the uploaded file
    columns = _detect_columns(str(raw_path))

    return {
        "city_id"          : city.id,
        "name"             : city.name,
        "raw_data_path"    : str(raw_path),
        "bludot_export_path": str(bludot_path),
        "detected_columns" : columns,
    }


@app.get("/cities/")
def list_cities(db: Session = Depends(get_db)):
    cities = db.query(City).order_by(City.created_at.desc()).all()
    result = []
    for city in cities:
        run = db.query(PipelineRun).filter_by(city_id=city.id).order_by(
            PipelineRun.id.desc()
        ).first()
        result.append({
            "id"            : city.id,
            "name"          : city.name,
            "city_or_county": city.city_or_county,
            "created_at"    : city.created_at.isoformat(),
            "pipeline_status": run.status if run else "not_started",
            "current_step"  : run.current_step if run else None,
        })
    return result


@app.get("/cities/{city_id}")
def get_city(city_id: int, db: Session = Depends(get_db)):
    city = db.get(City, city_id)
    if not city:
        raise HTTPException(404, f"City {city_id} not found")

    run = db.query(PipelineRun).filter_by(city_id=city_id).order_by(
        PipelineRun.id.desc()
    ).first()

    return {
        "id"                : city.id,
        "name"              : city.name,
        "city_or_county"    : city.city_or_county,
        "created_at"        : city.created_at.isoformat(),
        "raw_data_path"     : city.raw_data_path,
        "bludot_export_path": city.bludot_export_path,
        "pipeline_run"      : {
            "id"           : run.id if run else None,
            "status"       : run.status if run else "not_started",
            "current_step" : run.current_step if run else None,
            "started_at"   : run.started_at.isoformat() if run else None,
            "updated_at"   : run.updated_at.isoformat() if run else None,
            "error_log"    : run.error_log if run else None,
        } if run else None,
    }


@app.post("/cities/{city_id}/start")
def start_pipeline(city_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Start the pipeline for a city (runs in background)."""
    city = db.get(City, city_id)
    if not city:
        raise HTTPException(404, f"City {city_id} not found")

    # Check column mapping is complete
    mappings = db.query(ColumnMapping).filter_by(city_id=city_id).count()
    if mappings == 0:
        raise HTTPException(400, "Column mapping must be completed before starting the pipeline")

    background_tasks.add_task(run_city_pipeline, city_id=city_id)
    return {"message": f"Pipeline started for city_id={city_id}"}


@app.post("/cities/{city_id}/resume")
def resume_pipeline(city_id: int, background_tasks: BackgroundTasks, db: Session = Depends(get_db)):
    """Resume a paused pipeline after human review or verification is complete."""
    run = db.query(PipelineRun).filter_by(
        city_id=city_id, status=PipelineStatus.PAUSED
    ).order_by(PipelineRun.id.desc()).first()

    if not run:
        raise HTTPException(400, f"No paused pipeline found for city_id={city_id}")

    # Only block resume if the current gate is a MATCH review gate with pending items.
    # Verification gates (step1, split, step5, contacts) should always resume freely.
    MATCH_REVIEW_GATES = {"gate2_match_review_pass1", "gate4_match_review_pass2"}
    if run.current_step in MATCH_REVIEW_GATES:
        pending = db.query(MatchCandidate).filter_by(
            city_id=city_id, final_decision=MatchDecision.NEEDS_REVIEW
        ).count()
        if pending > 0:
            raise HTTPException(400, f"There are still {pending} unresolved review items")

    background_tasks.add_task(resume_city_pipeline, city_id=city_id)
    return {"message": f"Pipeline resumed for city_id={city_id}"}


@app.get("/cities/{city_id}/status")
def get_pipeline_status(city_id: int, db: Session = Depends(get_db)):
    """Polling endpoint for live status updates."""
    run = db.query(PipelineRun).filter_by(city_id=city_id).order_by(
        PipelineRun.id.desc()
    ).first()

    if not run:
        return {"status": "not_started"}

    logs = [
        {
            "step"      : log.step,
            "status"    : log.status,
            "message"   : log.message,
            "stats"     : log.stats,
            "started_at": log.started_at.isoformat(),
            "ended_at"  : log.ended_at.isoformat() if log.ended_at else None,
        }
        for log in run.step_logs
    ]

    return {
        "run_id"       : run.id,
        "status"       : run.status,
        "current_step" : run.current_step,
        "updated_at"   : run.updated_at.isoformat(),
        "error_log"    : run.error_log,
        "step_logs"    : logs,
    }


# ── Column Mapping endpoints ──────────────────────────────────────────────────

@app.get("/cities/{city_id}/column-mapping")
def get_column_mapping(city_id: int, db: Session = Depends(get_db)):
    """Return detected source columns + any existing mappings."""
    city = db.get(City, city_id)
    if not city:
        raise HTTPException(404)

    detected = _detect_columns(city.raw_data_path)
    existing = db.query(ColumnMapping).filter_by(city_id=city_id).all()
    existing_map = {m.source_col: m.target_col for m in existing}

    return {
        "detected_columns": detected,
        "existing_mappings": existing_map,
        "target_schema": _get_target_schema(),
    }


@app.post("/cities/{city_id}/column-mapping")
def save_column_mapping(
    city_id: int,
    payload: ColumnMappingRequest,
    db: Session = Depends(get_db),
):
    """Save column mapping from the UI mapper."""
    db.query(ColumnMapping).filter_by(city_id=city_id).delete()

    for item in payload.mappings:
        mapping = ColumnMapping(
            city_id      = city_id,
            source_col   = item.source_col,
            target_col   = item.target_col,
            mapping_type = item.mapping_type,
            meta         = item.meta or {},
        )
        db.add(mapping)

    db.commit()
    return {"message": f"Saved {len(payload.mappings)} column mappings"}


@app.post("/cities/{city_id}/suggest-mapping")
def suggest_mapping(city_id: int, db: Session = Depends(get_db)):
    """
    Use Gemini to suggest column mappings for the city sheet.
    Returns suggested mappings pre-filled in the same format as /column-mapping.
    """
    from .core.llm_judge import suggest_column_mapping
    city = db.get(City, city_id)
    if not city:
        raise HTTPException(404)

    # Read city sheet columns + a few sample rows
    try:
        import pandas as pd
        ext = Path(city.raw_data_path).suffix.lower()
        df  = pd.read_excel(city.raw_data_path, nrows=5) if ext in ('.xlsx', '.xls') \
              else pd.read_csv(city.raw_data_path, nrows=5)
        df  = df.fillna('')
        city_columns = list(df.columns)
        sample_rows  = df.head(3).to_dict('records')
    except Exception as e:
        raise HTTPException(400, f"Could not read city sheet: {e}")

    # Get bludot custom column names from the export
    bludot_custom_cols = []
    try:
        import pandas as pd
        xl = pd.ExcelFile(city.bludot_export_path)
        cust_sheet = next((s for s in xl.sheet_names if 'custom' in s.lower()), None)
        if cust_sheet:
            cust_df = pd.read_excel(city.bludot_export_path, sheet_name=cust_sheet, nrows=0)
            bludot_custom_cols = [c for c in cust_df.columns
                                  if c not in ('UUID', 'Custom Data Name', 'ID')]
    except Exception:
        pass

    suggestions = suggest_column_mapping(city_columns, sample_rows, bludot_custom_cols)
    return {"suggestions": suggestions, "bludot_custom_cols": bludot_custom_cols}


# ── Review endpoints ──────────────────────────────────────────────────────────

@app.get("/cities/{city_id}/review")
def get_review_items(
    city_id: int,
    match_pass: int = 1,
    db: Session = Depends(get_db),
):
    """Return all UNCERTAIN pairs needing human review."""
    queue = get_review_queue(db, city_id, match_pass=match_pass)
    return {
        "city_id"   : city_id,
        "match_pass": match_pass,
        "total"     : len(queue),
        "items"     : queue,
    }


@app.post("/cities/{city_id}/review/bulk")
def bulk_review(
    city_id: int,
    payload: BulkReviewDecision,
    db: Session = Depends(get_db),
):
    """Submit multiple review decisions at once."""
    results = []
    for cand_id_str, accepted in payload.decisions.items():
        try:
            cand_id = int(cand_id_str)
        except (ValueError, TypeError):
            continue
        mc = apply_human_decision(db, cand_id, accepted, payload.reviewer)
        results.append({"candidate_id": cand_id, "decision": mc.final_decision})

    pending = db.query(MatchCandidate).filter_by(
        city_id=city_id, final_decision=MatchDecision.NEEDS_REVIEW
    ).count()

    return {"processed": len(results), "remaining": pending, "results": results}


@app.post("/cities/{city_id}/review/{candidate_id}")
def submit_review(
    city_id: int,
    candidate_id: int,
    decision: ReviewDecision,
    db: Session = Depends(get_db),
):
    """Submit a single Accept/Reject decision."""
    mc = apply_human_decision(
        db           = db,
        candidate_id = candidate_id,
        accepted     = decision.accepted,
        reviewer     = decision.reviewer,
        note         = decision.note,
    )
    pending = db.query(MatchCandidate).filter_by(
        city_id=city_id, final_decision=MatchDecision.NEEDS_REVIEW
    ).count()

    return {
        "candidate_id": candidate_id,
        "decision"    : mc.final_decision,
        "remaining"   : pending,
    }


# ── Stats endpoint ────────────────────────────────────────────────────────────

@app.get("/cities/{city_id}/stats")
def get_stats(city_id: int, db: Session = Depends(get_db)):
    return get_match_stats(db, city_id)


# ── Dedup review endpoints ────────────────────────────────────────────────────

@app.get("/cities/{city_id}/dedup-review")
def get_dedup_review(city_id: int, db: Session = Depends(get_db)):
    """Return near-miss dedup pairs flagged by LLM for human review."""
    from .db.models import DedupReviewPair, DedupDecision
    pairs = db.query(DedupReviewPair).filter_by(
        city_id=city_id,
        decision=DedupDecision.UNCERTAIN,
    ).all()
    return {
        "total": len(pairs),
        "items": [
            {
                "pair_id":    p.id,
                "index_a":    p.index_a,
                "index_b":    p.index_b,
                "name_a":     p.name_a,
                "address_a":  p.address_a,
                "name_b":     p.name_b,
                "address_b":  p.address_b,
                "similarity": p.similarity,
                "llm_reason": p.llm_reason,
                "decision":   p.decision,
            }
            for p in pairs
        ],
    }


class DedupDecisionPayload(BaseModel):
    decisions: dict[str, str]   # {pair_id: "DUPLICATE" | "NOT_DUPLICATE"}
    reviewer: str = "human"


@app.post("/cities/{city_id}/dedup-review/bulk")
def submit_dedup_review(
    city_id: int,
    payload: DedupDecisionPayload,
    db: Session = Depends(get_db),
):
    """Submit human dedup decisions. DUPLICATE pairs get merged into same cluster."""
    from .db.models import DedupReviewPair, DedupDecision, CityRecord
    results = []
    for pair_id, decision in payload.decisions.items():
        pair = db.get(DedupReviewPair, int(pair_id))
        if not pair:
            continue
        pair.decision    = decision
        pair.reviewed_by = payload.reviewer
        pair.reviewed_at = datetime.utcnow()

        if decision == "DUPLICATE":
            # Merge cluster — set both records to same cluster_id (the lower one)
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=pair.index_a).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=pair.index_b).first()
            if rec_a and rec_b:
                merged_cluster = min(
                    rec_a.cluster_id or rec_a.id,
                    rec_b.cluster_id or rec_b.id
                )
                rec_a.cluster_id = merged_cluster
                rec_b.cluster_id = merged_cluster

        results.append({"pair_id": pair_id, "decision": decision})

    db.commit()
    pending = db.query(DedupReviewPair).filter_by(
        city_id=city_id, decision=DedupDecision.UNCERTAIN
    ).count()
    return {"processed": len(results), "remaining": pending}


# ── Dedup results viewer ──────────────────────────────────────────────────────

@app.get("/cities/{city_id}/dedup-results")
def get_dedup_results(city_id: int, db: Session = Depends(get_db)):
    """Return ALL dedup pairs with their decisions for the results viewer."""
    from .db.models import DedupReviewPair
    pairs = db.query(DedupReviewPair).filter_by(city_id=city_id).order_by(
        DedupReviewPair.decision, DedupReviewPair.similarity.desc()
    ).all()
    return {
        "city_id": city_id,
        "total": len(pairs),
        "pairs": [
            {
                "id"           : p.id,
                "index_a"      : p.index_a,
                "index_b"      : p.index_b,
                "name_a"       : p.name_a,
                "address_a"    : p.address_a,
                "name_b"       : p.name_b,
                "address_b"    : p.address_b,
                "similarity"   : p.similarity,
                "decision"     : p.decision,
                "llm_reason"   : p.llm_reason,
                "intra_cluster": p.intra_cluster,
                "reviewed_by"  : p.reviewed_by,
            }
            for p in pairs
        ],
    }


# ── Matches viewer endpoint ───────────────────────────────────────────────────

@app.get("/cities/{city_id}/matches")
def get_matches(city_id: int, db: Session = Depends(get_db)):
    """Return all confirmed matched pairs for display in the matches viewer."""
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id == city_id,
        MatchCandidate.final_decision.in_([
            MatchDecision.AUTO_MATCH,
            MatchDecision.HUMAN_ACCEPTED,
        ])
    ).order_by(MatchCandidate.id).all()

    matches = []
    for mc in candidates:
        cr = db.get(CityRecord, mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue
        matches.append({
            "candidate_id"  : mc.id,
            "city_name"     : cr.business_name,
            "city_address"  : cr.address1,
            "bludot_name"   : br.name,
            "bludot_address": br.address1,
            "bludot_uuid"   : br.uuid,
            "final_decision": mc.final_decision,
            "llm_reason"    : mc.llm_reason,
            "name_score"    : mc.name_score,
            "address_score" : mc.address_score,
        })

    return {"city_id": city_id, "total": len(matches), "matches": matches}


# ── Cluster Review endpoints ──────────────────────────────────────────────────

@app.get("/cities/{city_id}/cluster-review")
def get_cluster_review(city_id: int, db: Session = Depends(get_db)):
    """Return cluster groups with uncertain pairs for the cluster review UI."""
    from .db.models import DedupReviewPair, CityRecord
    from collections import defaultdict

    pairs = db.query(DedupReviewPair).filter_by(
        city_id=city_id, decision="UNCERTAIN"
    ).all()

    if not pairs:
        return {"total_groups": 0, "groups": []}

    # Collect all cluster IDs involved
    involved = set()
    for p in pairs:
        ra = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_a).first()
        rb = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_b).first()
        if ra and ra.cluster_id is not None: involved.add(ra.cluster_id)
        if rb and rb.cluster_id is not None: involved.add(rb.cluster_id)

    # Union-find to group linked clusters
    parent = {c: c for c in involved}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(x, y):
        parent[find(x)] = find(y)

    for p in pairs:
        ra = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_a).first()
        rb = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_b).first()
        if ra and rb and ra.cluster_id is not None and rb.cluster_id is not None:
            if ra.cluster_id != rb.cluster_id:
                union(ra.cluster_id, rb.cluster_id)

    root_to_clusters = defaultdict(set)
    for cid in involved:
        root_to_clusters[find(cid)].add(cid)

    def cluster_records(cluster_id):
        recs = db.query(CityRecord).filter_by(city_id=city_id, cluster_id=cluster_id).all()
        return [{"id": r.id, "city_index": r.city_index,
                 "business_name": r.business_name, "address1": r.address1}
                for r in recs]

    groups = []
    for root, cluster_ids in root_to_clusters.items():
        clusters = [{"cluster_id": cid, "records": cluster_records(cid)}
                    for cid in sorted(cluster_ids) if cluster_records(cid)]

        linking_pairs = []
        for p in pairs:
            ra = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_a).first()
            rb = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_b).first()
            if not ra or not rb: continue
            if ra.cluster_id in cluster_ids or rb.cluster_id in cluster_ids:
                linking_pairs.append({
                    "pair_id": p.id, "index_a": p.index_a, "index_b": p.index_b,
                    "name_a": p.name_a, "address_a": p.address_a,
                    "name_b": p.name_b, "address_b": p.address_b,
                    "similarity": p.similarity, "reason": p.llm_reason,
                    "cluster_a": ra.cluster_id, "cluster_b": rb.cluster_id,
                })

        if clusters:
            groups.append({"group_id": root, "clusters": clusters, "linking_pairs": linking_pairs})

    return {"total_groups": len(groups), "groups": groups}


class MergeClusterPayload(BaseModel):
    cluster_ids: list[int]


@app.post("/cities/{city_id}/cluster-review/merge")
def merge_clusters(city_id: int, payload: MergeClusterPayload, db: Session = Depends(get_db)):
    """Merge multiple clusters into one."""
    from .db.models import CityRecord, DedupReviewPair

    if len(payload.cluster_ids) < 2:
        raise HTTPException(400, "Need at least 2 cluster IDs to merge")

    target = min(payload.cluster_ids)
    updated = 0
    for cid in payload.cluster_ids:
        if cid == target: continue
        rows = db.query(CityRecord).filter_by(city_id=city_id, cluster_id=cid).all()
        for r in rows:
            r.cluster_id = target
            updated += 1

    # Mark related pairs as resolved
    for p in db.query(DedupReviewPair).filter_by(city_id=city_id, decision="UNCERTAIN").all():
        ra = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_a).first()
        rb = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_b).first()
        if ra and rb:
            if ra.cluster_id in payload.cluster_ids or rb.cluster_id in payload.cluster_ids:
                p.decision = "DUPLICATE"

    db.commit()
    return {"merged_into": target, "records_updated": updated}


class KeepSeparatePayload(BaseModel):
    pair_ids: list[int]


@app.post("/cities/{city_id}/cluster-review/keep-separate")
def keep_separate(city_id: int, payload: KeepSeparatePayload, db: Session = Depends(get_db)):
    """Mark pairs as NOT_DUPLICATE."""
    from .db.models import DedupReviewPair

    updated = 0
    for pid in payload.pair_ids:
        pair = db.get(DedupReviewPair, pid)
        if pair and pair.city_id == city_id:
            pair.decision = "NOT_DUPLICATE"
            updated += 1

    db.commit()
    remaining = db.query(DedupReviewPair).filter_by(
        city_id=city_id, decision="UNCERTAIN"
    ).count()
    return {"updated": updated, "remaining_uncertain": remaining}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _detect_columns(file_path: str) -> list[str]:
    """Read the first row of the uploaded file and return column names."""
    try:
        import pandas as pd
        ext = Path(file_path).suffix.lower()
        if ext in (".xlsx", ".xls"):
            df = pd.read_excel(file_path, nrows=0)
        else:
            df = pd.read_csv(file_path, nrows=0)
        return list(df.columns)
    except Exception:
        return []


def _get_target_schema() -> list[dict]:
    """Our internal schema fields that source columns need to be mapped to."""
    return [
        {"field": "Business Name", "required": True,  "description": "Primary business name"},
        {"field": "Address1",      "required": True,  "description": "Street address line 1"},
        {"field": "Address2",      "required": False, "description": "Suite / unit / floor"},
        {"field": "City",          "required": False, "description": "City"},
        {"field": "State",         "required": False, "description": "State abbreviation"},
        {"field": "ZIP",           "required": False, "description": "Zip / postal code"},
        {"field": "Phone",         "required": False, "description": "Business phone"},
        {"field": "Email",         "required": False, "description": "Business email"},
        {"field": "Website",       "required": False, "description": "Website URL"},
        {"field": "Owner Name",    "required": False, "description": "Owner / contact name"},
        {"field": "Owner Phone",   "required": False, "description": "Owner phone"},
        {"field": "Owner Email",   "required": False, "description": "Owner email"},
        {"field": "SKIP",          "required": False, "description": "Do not import this column"},
    ]