"""
Pipeline Runner
===============
Plain Python — no Prefect, no legacy imports, no city_details.py.

All logic lives in backend/core/:
  step0_dedup.py    → LSH dedup + bludot concat
  step1_format.py   → pivot clusters + merge columns
  matching_orchestrator.py → candidates + LLM judge
  step5_6_output.py → final Excel output

Steps:
  0   → Dedup city sheet + concat bludot export
  1   → Reformat + merge columns
  2   → Generate match candidates + LLM judge  [GATE: human review]
  4   → Export split records (matched / additional)
  4.1 → Second-pass matching + LLM             [GATE: human review if needed]
  5+6 → Generate final output Excel files
"""

import logging
import traceback
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from ..db.session import SessionLocal
from ..db.models import City, PipelineRun, StepLog
from ..core.step0_dedup import run_step0
from ..core.step1_format import run_step1
from ..core.matching_orchestrator import generate_candidates, run_llm_judge, get_review_queue
from ..core.step5_6_output import run_step5_and_step6
from ..services.export_service import (
    export_matched_records_to_excel,
    export_additional_city_records,
    export_additional_bludot_records,
)

logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _update_run(db: Session, run: PipelineRun, status: str, step: str, error: str = None):
    run.status       = status
    run.current_step = step
    run.updated_at   = datetime.utcnow()
    if error:
        run.error_log = error
    db.commit()


def _log_step(db: Session, run: PipelineRun, step: str, status: str,
              message: str = "", stats: dict = None):
    existing = db.query(StepLog).filter_by(run_id=run.id, step=step).first()
    now = datetime.utcnow()
    if existing:
        existing.status  = status
        existing.message = message
        existing.stats   = stats or {}
        if status in ("completed", "failed", "paused"):
            existing.ended_at = now
    else:
        db.add(StepLog(
            run_id   = run.id,
            step     = step,
            status   = status,
            message  = message,
            stats    = stats or {},
            ended_at = now if status in ("completed", "failed", "paused") else None,
        ))
    run.current_step = step
    if status in ("running", "paused", "failed"):
        run.status = status
    run.updated_at = now
    db.commit()


def _results_dir(city: City) -> str:
    return str(Path(city.raw_data_path).parent / "results")


# ── Main entry points ─────────────────────────────────────────────────────────

def run_city_pipeline(city_id: int):
    """Called by FastAPI BackgroundTasks on Start Pipeline."""
    db = SessionLocal()
    try:
        city = db.get(City, city_id)
        if not city:
            logger.error(f"City {city_id} not found"); return

        run = PipelineRun(
            city_id      = city_id,
            status       = "running",
            current_step = "step0_dedup",
            started_at   = datetime.utcnow(),
            updated_at   = datetime.utcnow(),
        )
        db.add(run)
        db.commit()
        db.refresh(run)

        results_dir = _results_dir(city)

        try:
            # ── Step 0: Dedup + bludot concat ────────────────────────────────
            _log_step(db, run, "step0_dedup", "running", "Running LSH deduplication…")
            stats0 = run_step0(city, db, results_dir)
            _log_step(db, run, "step0_dedup", "completed",
                      f"{stats0['deduped_records']} records, {stats0['clusters']} clusters",
                      stats=stats0)

            # ── Gate: dedup near-miss review ──────────────────────────────────
            from ..db.models import DedupReviewPair
            uncertain_dedup = db.query(DedupReviewPair).filter_by(
                city_id=city_id, decision="UNCERTAIN"
            ).count()
            if uncertain_dedup > 0:
                _log_step(db, run, "step0_dedup_review", "paused",
                          f"{uncertain_dedup} near-miss dedup pairs need review",
                          stats={"pending_review": uncertain_dedup})
                _update_run(db, run, "paused", "step0_dedup_review")
                return

            # No uncertain dedup pairs — continue straight to step 1+
            _continue_after_dedup(city, city_id, results_dir, db, run)

        except Exception as e:
            logger.error(traceback.format_exc())
            _update_run(db, run, "failed", run.current_step, error=str(e))

    finally:
        db.close()


def resume_city_pipeline(city_id: int):
    """Called by FastAPI BackgroundTasks after human review is complete."""
    db = SessionLocal()
    try:
        city = db.get(City, city_id)
        run  = db.query(PipelineRun).filter_by(
            city_id=city_id, status="paused"
        ).order_by(PipelineRun.id.desc()).first()

        if not run:
            logger.error(f"No paused run for city {city_id}"); return

        results_dir = _results_dir(city)

        try:
            if run.current_step == "step0_dedup_review":
                _log_step(db, run, "step0_dedup_review", "completed", "Dedup review complete")
                _update_run(db, run, "running", "step1_2_format")
                _continue_after_dedup(city, city_id, results_dir, db, run)

            elif run.current_step == "step2_review":
                _log_step(db, run, "step2_review", "completed", "Human review complete")
                _update_run(db, run, "running", "step4_split")
                _post_review(city, city_id, results_dir, db, run)
            elif run.current_step == "step4_1_review":
                _log_step(db, run, "step4_1_review", "completed", "Second-pass review complete")
                _update_run(db, run, "running", "step5_final_sheets")
                _step5_6(city, city_id, results_dir, db, run)

        except Exception as e:
            logger.error(traceback.format_exc())
            _update_run(db, run, "failed", run.current_step, error=str(e))

    finally:
        db.close()


def _continue_after_dedup(city, city_id, results_dir, db, run):
    """Steps 1 → 2 → gate → 4 → 4.1 → gate → 5+6."""

    # ── Step 1: Reformat + merge ──────────────────────────────────────────
    _log_step(db, run, "step1_2_format", "running", "Reformatting and merging columns…")
    stats1 = run_step1(results_dir)
    _log_step(db, run, "step1_2_format", "completed",
              f"{stats1['output_records']} records after merge", stats=stats1)

    # ── Step 2: Matching + LLM judge ──────────────────────────────────────
    _log_step(db, run, "step2_match", "running", "Generating match candidates…")
    candidates = generate_candidates(db, city_id, match_pass=1)
    _log_step(db, run, "step2_match", "running",
              f"Running LLM judge on {len(candidates)} candidates…",
              stats={"candidates": len(candidates)})
    llm_stats = run_llm_judge(db, city_id, match_pass=1)
    _log_step(db, run, "step2_match", "completed", "Matching complete",
              stats={**llm_stats, "candidates": len(candidates)})

    # ── Gate: human review ────────────────────────────────────────────────
    uncertain = get_review_queue(db, city_id, match_pass=1)
    if uncertain:
        _log_step(db, run, "step2_review", "paused",
                  f"{len(uncertain)} pairs need human review",
                  stats={"pending_review": len(uncertain)})
        _update_run(db, run, "paused", "step2_review")
        return

    _post_review(city, city_id, results_dir, db, run)


def _post_review(city, city_id, results_dir, db, run):
    """Steps 4 → 4.1 → optional gate → 5+6."""

    # Step 4: export split Excel files
    _log_step(db, run, "step4_split", "running", "Splitting matched/additional records…")
    export_matched_records_to_excel(db, city_id)
    export_additional_city_records(db, city_id)
    export_additional_bludot_records(db, city_id)
    _log_step(db, run, "step4_split", "completed", "Records split")

    # Step 4.1: second-pass matching
    _log_step(db, run, "step4_1_extra_match", "running", "Second-pass matching…")
    candidates2 = generate_candidates(db, city_id, match_pass=2)
    if candidates2:
        llm_stats2 = run_llm_judge(db, city_id, match_pass=2)
        _log_step(db, run, "step4_1_extra_match", "completed",
                  f"{len(candidates2)} candidates processed",
                  stats={**llm_stats2, "candidates": len(candidates2)})
        uncertain2 = get_review_queue(db, city_id, match_pass=2)
        if uncertain2:
            _log_step(db, run, "step4_1_review", "paused",
                      f"{len(uncertain2)} pairs need review",
                      stats={"pending_review": len(uncertain2)})
            _update_run(db, run, "paused", "step4_1_review")
            return
    else:
        _log_step(db, run, "step4_1_extra_match", "completed",
                  "No additional candidates", stats={})

    _step5_6(city, city_id, results_dir, db, run)


def _step5_6(city, city_id, results_dir, db, run):
    _log_step(db, run, "step5_final_sheets", "running", "Generating final output sheets…")
    stats5 = run_step5_and_step6(city, db, results_dir)
    _log_step(db, run, "step5_final_sheets", "completed",
              f"Output written: {stats5.get('matched_records', 0)} matched, "
              f"{stats5.get('additional_records', 0)} additional",
              stats=stats5)
    _log_step(db, run, "done", "completed", "Pipeline complete ✓")
    _update_run(db, run, "completed", "done")