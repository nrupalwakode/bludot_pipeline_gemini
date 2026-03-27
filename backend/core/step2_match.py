"""
Step 2 — Match Candidates + LLM Judge (Pass 1)
================================================
Calls matching_orchestrator to:
  - Generate city vs bludot candidate pairs (name-prefix blocking)
  - Run pre-LLM rule filter (auto-decides ~80% of pairs)
  - Send ambiguous pairs to Groq in ONE batched call
  - Returns stats dict

Returns:
  {candidates, auto_match, auto_no_match, needs_llm, needs_review}
"""

import logging
from sqlalchemy.orm import Session
from ..db.models import City
from .matching_orchestrator import generate_candidates, run_llm_judge

logger = logging.getLogger(__name__)


def run_step2(city: City, city_id: int, db: Session, results_dir: str) -> dict:
    """Entry point called by pipeline._step2."""
    logger.info(f"Step 2: Generating match candidates (pass 1) for city_id={city_id}")
    candidate_count = generate_candidates(db, city_id, match_pass=1)
    logger.info(f"Step 2: {candidate_count} candidates — running LLM judge…")
    llm_stats = run_llm_judge(db, city_id, match_pass=1)
    logger.info(f"Step 2 complete: {llm_stats}")
    return {**llm_stats, "candidates": candidate_count}
