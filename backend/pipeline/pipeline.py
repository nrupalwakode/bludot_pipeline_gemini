"""
Pipeline Runner — Bludot Pipeline
==================================
All steps are kept SEPARATE. Each step has its own function.
A human review gate exists after EVERY step.
All gates are SOFT — auto-continue when nothing to review.
Hard-pause gates (step1, step3, step5, step6) always stop for human verification.

Step order:
  0     → Dedup city sheet (LSH + LLM) + concat bludot
  GATE0 → Cluster review (uncertain dedup pairs — soft)
  1     → Reformat + merge numbered columns
  GATE1 → Verify de_duplication_merged.xlsx (always pauses)
  2     → Match candidates + LLM judge (pass 1)
  GATE2 → Human match review pass 1 (soft)
  3     → Split records
  GATE3 → Verify split files + add any manual fuzzy matches (always pauses)
  4     → Second-pass match + LLM (pass 2)
  GATE4 → Human match review pass 2 (soft)
  5     → Generate Business + Custom + Contact output sheets
  GATE5 → Verify final output sheets (always pauses)
  6     → Contacts dedup + append Contact sheet to final Excel
  GATE6 → Verify contacts sheet (always pauses)
"""

import logging
import traceback
from datetime import datetime
from pathlib import Path

from sqlalchemy.orm import Session

from ..db.session import SessionLocal
from ..db.models import (
    City, PipelineRun, StepLog,
    DedupReviewPair, MatchDecision, MatchCandidate
)

logger = logging.getLogger(__name__)


# ── DB helpers ────────────────────────────────────────────────────────────────

def _update_run(db, run, status, step, error=None):
    run.status       = status
    run.current_step = step
    run.updated_at   = datetime.utcnow()
    if error:
        run.error_log = error
    db.commit()


def _log_step(db, run, step, status, message="", stats=None):
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


def _results_dir(city):
    return str(Path(city.raw_data_path).parent / "results")


def _pending_dedup_review(db, city_id):
    return db.query(DedupReviewPair).filter_by(city_id=city_id, decision="UNCERTAIN").count()


def _pending_match_review(db, city_id, match_pass):
    return db.query(MatchCandidate).filter_by(
        city_id=city_id, match_pass=match_pass,
        final_decision=MatchDecision.NEEDS_REVIEW,
    ).count()


# ── Main entry points ─────────────────────────────────────────────────────────

def run_city_pipeline(city_id: int):
    """Start pipeline from Step 0."""
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

        try:
            _step0(city, city_id, _results_dir(city), db, run)
        except Exception as e:
            logger.error(traceback.format_exc())
            _update_run(db, run, "failed", run.current_step, error=str(e))
    finally:
        db.close()


def resume_city_pipeline(city_id: int):
    """Resume from wherever the pipeline is paused."""
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
            resume_map = {
                "gate0_cluster_review":       (_step1,  "Step 1 — Reformat"),
                "gate1_verify_step1":         (_step2,  "Step 2 — Match pass 1"),
                "gate2_match_review_pass1":   (_step3,  "Step 3 — Split records"),
                "gate3_verify_split":         (_step4,  "Step 4 — Match pass 2"),
                "gate4_match_review_pass2":   (_step5,  "Step 5 — Output sheets"),
                "gate5_verify_step5":         (_step6,  "Step 6 — Contacts"),
                "gate6_verify_contacts":      (None,    "Pipeline complete"),
            }

            if step not in resume_map:
                logger.error(f"Unknown paused step: {step}"); return

            next_fn, label = resume_map[step]
            _log_step(db, run, step, "completed", f"Review complete — resuming {label}")
            _update_run(db, run, "running", step)

            if next_fn is None:
                # Gate 6 — final
                _log_step(db, run, "done", "completed", "Pipeline complete ✓")
                _update_run(db, run, "completed", "done")
            else:
                next_fn(city, city_id, results_dir, db, run)

        except Exception as e:
            logger.error(traceback.format_exc())
            _update_run(db, run, "failed", run.current_step, error=str(e))
    finally:
        db.close()


# ── Individual step functions ─────────────────────────────────────────────────

def _step0(city, city_id, results_dir, db, run):
    """Step 0 — LSH dedup + LLM cluster verify + bludot concat."""
    from ..core.step0_dedup import run_step0
    _log_step(db, run, "step0_dedup", "running",
              "Step 0: LSH deduplication + LLM verification + bludot concat…")
    stats = run_step0(city, db, results_dir)
    _log_step(db, run, "step0_dedup", "completed",
              f"{stats['deduped_records']} records, {stats['clusters']} clusters — "
              f"file: results/city_data/manual_dedup_records.xlsx", stats=stats)

    # Gate 0 — soft: only pause if LLM found uncertain clusters
    pending = _pending_dedup_review(db, city_id)
    if pending > 0:
        _log_step(db, run, "gate0_cluster_review", "paused",
                  f"{pending} uncertain dedup pairs need cluster review. "
                  "Open Cluster Review in UI. "
                  "Physical file: results/city_data/manual_dedup_records.xlsx",
                  stats={"pending_review": pending})
        _update_run(db, run, "paused", "gate0_cluster_review")
        return

    _log_step(db, run, "gate0_cluster_review", "completed",
              "0 uncertain pairs — auto-continuing to Step 1")
    _step1(city, city_id, results_dir, db, run)


def _step1(city, city_id, results_dir, db, run):
    """Step 1 — Reformat + merge numbered columns."""
    from ..core.step1_format import run_step1
    _log_step(db, run, "step1_format", "running",
              "Step 1: Reformatting columns and merging numbered fields…")
    stats = run_step1(results_dir)
    _log_step(db, run, "step1_format", "completed",
              f"{stats['input_records']} → {stats['output_records']} records. "
              "File: results/city_data/de_duplication_merged.xlsx", stats=stats)

    # Gate 1 — always pause: human must verify the merged file
    _log_step(db, run, "gate1_verify_step1", "paused",
              "Step 1 complete. VERIFY before continuing: "
              "results/city_data/de_duplication_merged.xlsx — "
              "check Business Name (longest value), phone dedup, address columns. "
              "Resume when ready.",
              stats={"file": "results/city_data/de_duplication_merged.xlsx"})
    _update_run(db, run, "paused", "gate1_verify_step1")


def _step2(city, city_id, results_dir, db, run):
    """Step 2 — Match candidates + LLM judge pass 1."""
    from ..core.matching_orchestrator import generate_candidates, run_llm_judge
    _log_step(db, run, "step2_match", "running",
              "Step 2: Generating match candidates (pass 1)…")
    candidate_count = generate_candidates(db, city_id, match_pass=1)
    _log_step(db, run, "step2_match", "running",
              f"Running LLM judge on {candidate_count} candidates (pass 1)…",
              stats={"candidates": candidate_count})
    llm_stats = run_llm_judge(db, city_id, match_pass=1)
    _log_step(db, run, "step2_match", "completed",
              f"Pass 1 complete: {llm_stats.get('auto_match', 0)} auto-matched, "
              f"{llm_stats.get('auto_no_match', 0)} rejected, "
              f"{llm_stats.get('needs_review', 0)} need review",
              stats={**llm_stats, "candidates": candidate_count})

    # Gate 2 — soft: only pause if LLM returned uncertain pairs
    pending = _pending_match_review(db, city_id, match_pass=1)
    if pending > 0:
        _log_step(db, run, "gate2_match_review_pass1", "paused",
                  f"{pending} pairs need human review (pass 1). "
                  "Open Review Queue in UI.",
                  stats={"pending_review": pending})
        _update_run(db, run, "paused", "gate2_match_review_pass1")
        return

    _log_step(db, run, "gate2_match_review_pass1", "completed",
              "0 uncertain pairs — auto-continuing to Step 3")
    _step3(city, city_id, results_dir, db, run)


def _step3(city, city_id, results_dir, db, run):
    """Step 3 — Split records: matched vs unmatched."""
    from ..core.step3_split import run_step3
    _log_step(db, run, "step3_split", "running",
              "Step 3: Splitting matched and unmatched records…")
    stats = run_step3(city, city_id, db, results_dir)
    _log_step(db, run, "step3_split", "completed",
              f"{stats.get('matched', 0)} matched, "
              f"{stats.get('additional_city', 0)} additional city, "
              f"{stats.get('additional_bludot', 0)} additional bludot. "
              "Files: results/output/final_result/", stats=stats)

    # Gate 3 — always pause: human verifies split + can add manual matches
    _log_step(db, run, "gate3_verify_split", "paused",
              "Step 3 complete. VERIFY before continuing: "
              "results/output/final_result/ — "
              "check final_matched_records_for_{city}.xlsx and additional files. "
              "If you find matching businesses in additional sheets, create "
              "filter_matches/city_bludot_index.xlsx with city_index + bludot_index "
              "then resume to run Step 4.",
              stats={"files": "results/output/final_result/"})
    _update_run(db, run, "paused", "gate3_verify_split")


def _step4(city, city_id, results_dir, db, run):
    """Step 4 — Second-pass match + LLM (pass 2)."""
    from ..core.matching_orchestrator import generate_candidates, run_llm_judge
    _log_step(db, run, "step4_match_pass2", "running",
              "Step 4: Second-pass matching on unmatched records (pass 2)…")
    candidate_count2 = generate_candidates(db, city_id, match_pass=2)

    if candidate_count2 > 0:
        llm_stats2 = run_llm_judge(db, city_id, match_pass=2)
        _log_step(db, run, "step4_match_pass2", "completed",
                  f"Pass 2 complete: {candidate_count2} candidates, "
                  f"{llm_stats2.get('auto_match', 0)} auto-matched, "
                  f"{llm_stats2.get('needs_review', 0)} need review",
                  stats={**llm_stats2, "candidates": candidate_count2})

        # Gate 4 — soft: only pause if uncertain
        pending = _pending_match_review(db, city_id, match_pass=2)
        if pending > 0:
            _log_step(db, run, "gate4_match_review_pass2", "paused",
                      f"{pending} pairs need human review (pass 2). "
                      "Open Review Queue (pass 2) in UI.",
                      stats={"pending_review": pending})
            _update_run(db, run, "paused", "gate4_match_review_pass2")
            return

        _log_step(db, run, "gate4_match_review_pass2", "completed",
                  "0 uncertain pass-2 pairs — auto-continuing to Step 5")
    else:
        _log_step(db, run, "step4_match_pass2", "completed",
                  "No additional candidates found in pass 2", stats={"candidates": 0})
        _log_step(db, run, "gate4_match_review_pass2", "completed",
                  "No pass-2 candidates — skipping gate 4")

    _step5(city, city_id, results_dir, db, run)


def _step5(city, city_id, results_dir, db, run):
    """Step 5 — Generate Business + Custom + Contact output sheets."""
    from ..core.step5_output import run_step5
    _log_step(db, run, "step5_output", "running",
              "Step 5: Generating Business, Custom, and Contact output sheets…")
    stats = run_step5(city, db, results_dir)
    _log_step(db, run, "step5_output", "completed",
              f"Business sheets: {stats.get('business_matched', 0)} matched + "
              f"{stats.get('business_additional', 0)} additional. "
              f"File: results/output/final_output/{city.name}_Business_Matched_Records.xlsx",
              stats=stats)

    # Gate 5 — always pause: human verifies field mapping output
    _log_step(db, run, "gate5_verify_step5", "paused",
              "Step 5 complete. VERIFY before continuing: "
              f"results/output/final_output/{city.name}_Business_Matched_Records.xlsx — "
              "check Business_Matched_Records, Custom_Matched_Records, Contact_Matched_Records sheets. "
              "If column mapping needs changes, update city_schema.json and re-run Step 5. "
              "Resume when verified.",
              stats={"file": f"results/output/final_output/{city.name}_Business_Matched_Records.xlsx"})
    _update_run(db, run, "paused", "gate5_verify_step5")


def _step6(city, city_id, results_dir, db, run):
    """Step 6 — Contacts dedup + append Contact sheet to final Excel."""
    from ..core.step6_contacts import run_step6
    _log_step(db, run, "step6_contacts", "running",
              "Step 6: Deduplicating contacts and appending to final Excel…")
    stats = run_step6(city, db, results_dir)
    _log_step(db, run, "step6_contacts", "completed",
              f"Contacts processed: {stats.get('contact_rows', 0)} rows appended. "
              f"File: results/output/final_output/{city.name}_Business_Matched_Records.xlsx",
              stats=stats)

    # Gate 6 — always pause: human verifies final output
    _log_step(db, run, "gate6_verify_contacts", "paused",
              "Step 6 complete. VERIFY the final output: "
              f"results/output/final_output/{city.name}_Business_Matched_Records.xlsx — "
              "check Contact_Matched_Records sheet. "
              "Resume to mark pipeline as complete.",
              stats={"file": f"results/output/final_output/{city.name}_Business_Matched_Records.xlsx"})
    _update_run(db, run, "paused", "gate6_verify_contacts")
