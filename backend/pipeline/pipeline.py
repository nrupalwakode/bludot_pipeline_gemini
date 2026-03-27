"""
Pipeline Runner
===============
Plain Python — no Prefect, no legacy imports, no city_details.py.

Step order:
  0     → Dedup city sheet (LSH + LLM) + concat bludot export
  GATE0 → Cluster review (soft — auto-skip if 0 uncertain dedup pairs)
  1     → Reformat + merge columns  (de_duplication_merged.xlsx)
  2     → Match candidates + LLM judge (pass 1)
  GATE1 → Human match review pass 1 (soft — auto-skip if 0 uncertain)
  3     → Split records (prepare residuals for second pass)
  4     → Second-pass match + LLM (pass 2)
  GATE2 → Human match review pass 2 (soft — auto-skip if 0 uncertain)
  5     → Generate final output Excel sheets

All GATE steps are SOFT — if nothing needs review the pipeline
continues automatically without any human interaction required.
"""

import logging
import traceback
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from ..db.session import SessionLocal
from ..db.models import City, PipelineRun, StepLog, DedupReviewPair, MatchDecision, MatchCandidate
from ..core.step0_dedup import run_step0
from ..core.step1_format import run_step1
from ..core.matching_orchestrator import generate_candidates, run_llm_judge, get_review_queue
from ..core.step5_6_output import run_step5_and_step6

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


# ── Gate helpers ──────────────────────────────────────────────────────────────

def _pending_dedup_review(db: Session, city_id: int) -> int:
    """Count uncertain dedup pairs waiting for human review."""
    return db.query(DedupReviewPair).filter_by(
        city_id=city_id, decision="UNCERTAIN"
    ).count()


def _pending_match_review(db: Session, city_id: int, match_pass: int) -> int:
    """Count uncertain match candidates waiting for human review."""
    return db.query(MatchCandidate).filter_by(
        city_id=city_id,
        match_pass=match_pass,
        final_decision=MatchDecision.NEEDS_REVIEW,
    ).count()


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
            _run_step0(city, city_id, results_dir, db, run)
        except Exception as e:
            logger.error(traceback.format_exc())
            _update_run(db, run, "failed", run.current_step, error=str(e))
    finally:
        db.close()


def resume_city_pipeline(city_id: int):
    """Called by FastAPI BackgroundTasks after human presses Resume."""
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
            step = run.current_step

            if step == "step0_dedup_review":
                _log_step(db, run, "step0_dedup_review", "completed",
                          "Cluster review complete")
                _update_run(db, run, "running", "step1_2_format")
                _run_step1(city, city_id, results_dir, db, run)

            elif step == "step2_review":
                _log_step(db, run, "step2_review", "completed",
                          "Pass 1 review complete")
                _update_run(db, run, "running", "step3_split")
                _run_step3(city, city_id, results_dir, db, run)

            elif step == "step4_1_review":
                _log_step(db, run, "step4_1_review", "completed",
                          "Pass 2 review complete")
                _update_run(db, run, "running", "step5_final_sheets")
                _run_step5(city, city_id, results_dir, db, run)

            else:
                logger.error(f"Unknown paused step: {step}")

        except Exception as e:
            logger.error(traceback.format_exc())
            _update_run(db, run, "failed", run.current_step, error=str(e))
    finally:
        db.close()


# ── Individual step runners ───────────────────────────────────────────────────

def _run_step0(city, city_id, results_dir, db, run):
    """
    Step 0 — Deduplication (LSH + LLM) + bludot concat
    Writes: manual_dedup_records.xlsx + bludot_concatenated_records.xlsx
    """
    _log_step(db, run, "step0_dedup", "running",
              "Running LSH deduplication + LLM cluster verification…")
    stats0 = run_step0(city, db, results_dir)
    _log_step(db, run, "step0_dedup", "completed",
              f"{stats0['deduped_records']} records, {stats0['clusters']} clusters",
              stats=stats0)

    # ── Gate 0: Cluster review (soft) ─────────────────────────────────────
    pending = _pending_dedup_review(db, city_id)
    if pending > 0:
        _log_step(db, run, "step0_dedup_review", "paused",
                  f"{pending} near-miss pairs need cluster review",
                  stats={"pending_review": pending})
        _update_run(db, run, "paused", "step0_dedup_review")
        logger.info(f"Step 0 gate: paused for {pending} dedup pairs")
        return

    # Nothing to review — continue automatically
    _log_step(db, run, "step0_dedup_review", "completed",
              "No uncertain dedup pairs — continuing automatically")
    _run_step1(city, city_id, results_dir, db, run)


def _run_step1(city, city_id, results_dir, db, run):
    """
    Step 1 — Reformat + merge_columns()
    Writes: de_duplication_merged.xlsx
    """
    _log_step(db, run, "step1_2_format", "running",
              "Reformatting columns and merging numbered fields…")
    stats1 = run_step1(results_dir)
    _log_step(db, run, "step1_2_format", "completed",
              f"{stats1['output_records']} records after merge",
              stats=stats1)

    _run_step2(city, city_id, results_dir, db, run)


def _run_step2(city, city_id, results_dir, db, run):
    """
    Step 2 — Generate match candidates + LLM judge (pass 1)
    Rule filter auto-decides ~80% — only ambiguous pairs go to Groq.
    """
    _log_step(db, run, "step2_match", "running",
              "Generating match candidates (pass 1)…")
    candidate_count = generate_candidates(db, city_id, match_pass=1)

    _log_step(db, run, "step2_match", "running",
              f"Running LLM judge on candidates (pass 1)…",
              stats={"candidates": candidate_count})
    llm_stats = run_llm_judge(db, city_id, match_pass=1)

    _log_step(db, run, "step2_match", "completed",
              "Pass 1 matching complete",
              stats={**llm_stats, "candidates": candidate_count})

    # ── Gate 1: Match review pass 1 (soft) ────────────────────────────────
    pending = _pending_match_review(db, city_id, match_pass=1)
    if pending > 0:
        _log_step(db, run, "step2_review", "paused",
                  f"{pending} pairs need human review (pass 1)",
                  stats={"pending_review": pending})
        _update_run(db, run, "paused", "step2_review")
        logger.info(f"Step 2 gate: paused for {pending} match pairs")
        return

    # Nothing to review — continue automatically
    _log_step(db, run, "step2_review", "completed",
              "No uncertain pairs — continuing automatically")
    _run_step3(city, city_id, results_dir, db, run)


def _run_step3(city, city_id, results_dir, db, run):
    """
    Step 3 — Split records
    Prepares residual (unmatched) records for second-pass matching.
    Actual file splitting happens in step 5 output.
    """
    _log_step(db, run, "step3_split", "running",
              "Splitting matched / unmatched records…")
    _log_step(db, run, "step3_split", "completed",
              "Records split — ready for second-pass matching")

    _run_step4(city, city_id, results_dir, db, run)


def _run_step4(city, city_id, results_dir, db, run):
    """
    Step 4 — Second-pass match + LLM (pass 2)
    Runs on records NOT matched in pass 1.
    """
    _log_step(db, run, "step4_1_extra_match", "running",
              "Second-pass matching (pass 2)…")
    candidate_count2 = generate_candidates(db, city_id, match_pass=2)

    if candidate_count2 > 0:
        llm_stats2 = run_llm_judge(db, city_id, match_pass=2)
        _log_step(db, run, "step4_1_extra_match", "completed",
                  f"{candidate_count2} candidates processed (pass 2)",
                  stats={**llm_stats2, "candidates": candidate_count2})

        # ── Gate 2: Match review pass 2 (soft) ────────────────────────────
        pending2 = _pending_match_review(db, city_id, match_pass=2)
        if pending2 > 0:
            _log_step(db, run, "step4_1_review", "paused",
                      f"{pending2} pairs need human review (pass 2)",
                      stats={"pending_review": pending2})
            _update_run(db, run, "paused", "step4_1_review")
            logger.info(f"Step 4 gate: paused for {pending2} pass-2 pairs")
            return

        # Nothing to review — continue automatically
        _log_step(db, run, "step4_1_review", "completed",
                  "No uncertain pass-2 pairs — continuing automatically")
    else:
        _log_step(db, run, "step4_1_extra_match", "completed",
                  "No additional candidates found in pass 2",
                  stats={"candidates": 0})
        _log_step(db, run, "step4_1_review", "completed",
                  "Pass 2 review skipped — no candidates")

    _run_step5(city, city_id, results_dir, db, run)


def _run_step5(city, city_id, results_dir, db, run):
    """
    Step 5 — Generate final output Excel sheets
    Reads all confirmed matches from DB (AUTO_MATCH + HUMAN_ACCEPTED, both passes).
    Writes: {city}_Business_Matched_Records.xlsx + additional records files.
    """
    _log_step(db, run, "step5_final_sheets", "running",
              "Generating final output sheets…")
    stats5 = run_step5_and_step6(city, db, results_dir)
    _log_step(db, run, "step5_final_sheets", "completed",
              f"Output written: {stats5.get('matched_records', 0)} matched, "
              f"{stats5.get('additional_city', 0)} additional city records",
              stats=stats5)

    _log_step(db, run, "done", "completed", "Pipeline complete ✓")
    _update_run(db, run, "completed", "done")
    logger.info(f"Pipeline complete for city_id={city_id}")
