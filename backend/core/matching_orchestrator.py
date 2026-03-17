"""
Matching Orchestrator
=====================
Ties all three stages together for both Step 2 (city vs bludot) and
Step 4.1 (second-pass on residuals — replaces Excel fuzzy lookup).

Stage 1 → Rule Filter   (fast, no API cost)
Stage 2 → Gemini Judge  (only for CANDIDATE pairs)
Stage 3 → Human Review  (only for UNCERTAIN pairs — via UI)

This module is called by the Prefect pipeline steps.
"""

import logging
from datetime import datetime
from sqlalchemy.orm import Session

from ..db.models import (
    City, CityRecord, BludotRecord, MatchCandidate, MatchDecision
)
from .rule_filter import apply_rule_filter
from .llm_judge import judge_candidates, CandidatePair

logger = logging.getLogger(__name__)


def generate_candidates(
    db: Session,
    city_id: int,
    match_pass: int = 1,
) -> int:
    """
    Generate (city_record, bludot_record) candidate pairs using blocking
    to avoid an O(n²) full cartesian product.

    Blocking strategy: group by first 3 chars of normalised name  OR
    same zip code (if available in raw_data).

    Returns the number of CANDIDATE pairs created.
    """
    from .rule_filter import normalize_name

    city_recs   = db.query(CityRecord).filter_by(city_id=city_id).all()
    bludot_recs = db.query(BludotRecord).filter_by(city_id=city_id).all()

    if match_pass == 2:
        # Second pass — only unmatched records
        matched_city_ids = {
            mc.city_rec_id for mc in db.query(MatchCandidate).filter(
                MatchCandidate.city_id == city_id,
                MatchCandidate.final_decision == MatchDecision.AUTO_MATCH,
            )
        }
        matched_bludot_ids = {
            mc.bludot_rec_id for mc in db.query(MatchCandidate).filter(
                MatchCandidate.city_id == city_id,
                MatchCandidate.final_decision == MatchDecision.AUTO_MATCH,
            )
        }
        city_recs   = [r for r in city_recs   if r.id not in matched_city_ids]
        bludot_recs = [r for r in bludot_recs if r.id not in matched_bludot_ids]

    logger.info(f"Generating candidates: {len(city_recs)} city × {len(bludot_recs)} bludot records")

    # Build blocking index: first 3 chars of normalised name → list of records
    def block_key(name: str) -> str:
        n = normalize_name(name or "")
        return n[:3] if len(n) >= 3 else n

    bludot_blocks: dict[str, list[BludotRecord]] = {}
    for br in bludot_recs:
        key = block_key(br.name or "")
        bludot_blocks.setdefault(key, []).append(br)
        # Also index by adjacent keys to handle 1-char prefix diff
        if len(key) >= 2:
            bludot_blocks.setdefault(key[:2], []).append(br)

    candidate_count = 0
    seen_pairs: set[tuple[int, int]] = set()

    for cr in city_recs:
        cr_key     = block_key(cr.business_name or "")
        candidates = set()

        # Collect from exact block + prefix block
        for k in [cr_key, cr_key[:2] if len(cr_key) >= 2 else cr_key]:
            for br in bludot_blocks.get(k, []):
                candidates.add(br.id)

        for br_id in candidates:
            pair_key = (cr.id, br_id)
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            br = db.get(BludotRecord, br_id)
            if not br:
                continue

            rule_result = apply_rule_filter(
                city_name     = cr.business_name or "",
                city_address  = cr.address1 or "",
                bludot_name   = br.name or "",
                bludot_address= br.address1 or "",
            )

            # Only store if not a definite no-match (saves DB space)
            if rule_result.verdict == "DEFINITE_NO_MATCH":
                continue

            mc = MatchCandidate(
                city_id        = city_id,
                city_rec_id    = cr.id,
                bludot_rec_id  = br.id,
                name_score     = rule_result.name_score,
                address_score  = rule_result.address_score,
                street_num_match = rule_result.street_num_match,
                rule_verdict   = rule_result.verdict,
                match_pass     = match_pass,
            )
            db.add(mc)
            candidate_count += 1

    db.commit()
    logger.info(f"Created {candidate_count} match candidates (pass {match_pass})")
    return candidate_count


def run_llm_judge(db: Session, city_id: int, match_pass: int = 1) -> dict:
    """
    Run Gemini on all CANDIDATE pairs for a city.
    Updates MatchCandidate rows with llm_decision and final_decision.

    Returns stats dict: {total, auto_match, auto_no_match, needs_review}
    """
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id    == city_id,
        MatchCandidate.match_pass == match_pass,
        MatchCandidate.rule_verdict == "CANDIDATE",
        MatchCandidate.llm_decision == None,  # noqa
    ).all()

    if not candidates:
        return {"total": 0, "auto_match": 0, "auto_no_match": 0, "needs_review": 0}

    # Build input for LLM judge
    pairs = []
    for mc in candidates:
        cr = db.get(CityRecord, mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue
        pairs.append(CandidatePair(
            candidate_id  = mc.id,
            city_name     = cr.business_name or "",
            city_address  = cr.address1 or "",
            bludot_name   = br.name or "",
            bludot_address= br.address1 or "",
            rule_reason   = mc.rule_verdict or "",
        ))

    logger.info(f"Sending {len(pairs)} pairs to Gemini for city_id={city_id}")
    results = judge_candidates(pairs)

    # Write decisions back to DB
    stats = {"total": len(results), "auto_match": 0, "auto_no_match": 0, "needs_review": 0}

    result_map = {r["candidate_id"]: r for r in results}

    for mc in candidates:
        res = result_map.get(mc.id)
        if not res:
            continue

        mc.llm_decision  = res["llm_decision"]
        mc.llm_reason    = res["llm_reason"]
        mc.llm_called_at = datetime.fromisoformat(res["llm_called_at"])

        if mc.llm_decision == "MATCH":
            mc.final_decision = MatchDecision.AUTO_MATCH
            stats["auto_match"] += 1
        elif mc.llm_decision == "NO_MATCH":
            mc.final_decision = MatchDecision.AUTO_NO_MATCH
            stats["auto_no_match"] += 1
        else:  # UNCERTAIN
            mc.final_decision = MatchDecision.NEEDS_REVIEW
            stats["needs_review"] += 1

    db.commit()
    logger.info(f"LLM judge results for city_id={city_id}: {stats}")
    return stats


def apply_human_decision(
    db: Session,
    candidate_id: int,
    accepted: bool,
    reviewer: str = "human",
    note: str = "",
) -> MatchCandidate:
    """
    Record a human Accept/Reject decision from the UI.
    Called by the FastAPI review endpoint.
    """
    mc = db.get(MatchCandidate, candidate_id)
    if not mc:
        raise ValueError(f"MatchCandidate {candidate_id} not found")

    mc.human_decision = MatchDecision.HUMAN_ACCEPTED if accepted else MatchDecision.HUMAN_REJECTED
    mc.final_decision = mc.human_decision
    mc.reviewed_by    = reviewer
    mc.reviewed_at    = datetime.utcnow()
    mc.review_note    = note

    db.commit()
    db.refresh(mc)
    return mc


def get_review_queue(db: Session, city_id: int, match_pass: int = 1) -> list[dict]:
    """
    Return all NEEDS_REVIEW pairs for the UI review queue.
    Includes full record details so the UI can show side-by-side.
    """
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id    == city_id,
        MatchCandidate.match_pass == match_pass,
        MatchCandidate.final_decision == MatchDecision.NEEDS_REVIEW,
    ).all()

    queue = []
    for mc in candidates:
        cr = db.get(CityRecord, mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue
        queue.append({
            "candidate_id"   : mc.id,
            "name_score"     : mc.name_score,
            "address_score"  : mc.address_score,
            "street_num_match": mc.street_num_match,
            "rule_verdict"   : mc.rule_verdict,
            "llm_decision"   : mc.llm_decision,
            "llm_reason"     : mc.llm_reason,
            "city_record"    : {
                "id"           : cr.id,
                "business_name": cr.business_name,
                "address1"     : cr.address1,
                "raw_data"     : cr.raw_data,
            },
            "bludot_record"  : {
                "id"      : br.id,
                "name"    : br.name,
                "address1": br.address1,
                "uuid"    : br.uuid,
                "raw_data": br.raw_data,
            },
        })
    return queue


def get_match_stats(db: Session, city_id: int) -> dict:
    """Summary stats for the pipeline dashboard."""
    total_city   = db.query(CityRecord).filter_by(city_id=city_id).count()
    total_bludot = db.query(BludotRecord).filter_by(city_id=city_id).count()

    auto_match    = db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.AUTO_MATCH).count()
    auto_no_match = db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.AUTO_NO_MATCH).count()
    needs_review  = db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.NEEDS_REVIEW).count()
    human_accepted= db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.HUMAN_ACCEPTED).count()
    human_rejected= db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.HUMAN_REJECTED).count()

    return {
        "total_city_records"  : total_city,
        "total_bludot_records": total_bludot,
        "auto_matched"        : auto_match,
        "auto_rejected"       : auto_no_match,
        "pending_review"      : needs_review,
        "human_accepted"      : human_accepted,
        "human_rejected"      : human_rejected,
        "total_confirmed_matches": auto_match + human_accepted,
    }
