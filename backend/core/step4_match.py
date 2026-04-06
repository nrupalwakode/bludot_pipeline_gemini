"""
Step 4 — Second-Pass Match + LLM (Pass 2)
==========================================
Same two-stage rule + LLM matching as Step 2 but:
  - Only runs on records NOT matched in pass 1
  - Uses match_pass=2 to keep decisions separate in DB

Returns:
  {candidates, auto_match, auto_no_match, needs_llm, needs_review}
"""

import logging
from sqlalchemy.orm import Session
from ..db.models import City
from .matching_orchestrator import generate_candidates, run_llm_judge

logger = logging.getLogger(__name__)


def run_step4(city: City, city_id: int, db: Session, results_dir: str) -> dict:
    """Entry point called by pipeline._step4."""
    logger.info(f"Step 4: Generating match candidates (pass 2) for city_id={city_id}")
    candidate_count = generate_candidates(db, city_id, match_pass=2)

    if candidate_count == 0:
        logger.info("Step 4: No pass-2 candidates found")
        return {"candidates": 0, "auto_match": 0, "auto_no_match": 0, "needs_review": 0}

    logger.info(f"Step 4: {candidate_count} candidates — running LLM judge (pass 2)…")
    llm_stats = run_llm_judge(db, city_id, match_pass=2)
    logger.info(f"Step 4 complete: {llm_stats}")
    return {**llm_stats, "candidates": candidate_count}
