"""
Matching Orchestrator
=====================
Ties all three stages together for both Step 2 (city vs bludot) and
Step 4.1 (second-pass on residuals).

Decision flow — in order, stop at first match:
  1. Rule filter (generate_candidates) — drops DEFINITE_NO_MATCH
  2. Pre-LLM rule pass (run_llm_judge) — auto-decides clear cases:
       AUTO_MATCH  → name ≥ 90% AND (both addr blank OR street nums match AND addr ≥ 80%)
       AUTO_REJECT → street nums both present AND different
       AUTO_REJECT → name < 50% (completely different)
  3. LLM (Groq) — only for genuinely ambiguous pairs
       = name similar but address situation unclear (one blank, no street num, etc.)
  4. Human review — only for LLM UNCERTAIN responses
"""

import re
import logging
from datetime import datetime
from sqlalchemy.orm import Session

from ..db.models import (
    City, CityRecord, BludotRecord, MatchCandidate, MatchDecision
)
from .rule_filter import apply_rule_filter
from .llm_judge import judge_candidates, CandidatePair

logger = logging.getLogger(__name__)


# ── Address helpers (self-contained, no dependency on step0) ─────────────────

def _extract_street_num(address: str) -> str:
    m = re.match(r'^\s*(\d+)', address or "")
    return m.group(1) if m else ""


def _normalize_name_simple(name: str) -> str:
    """Quick normalization for scoring — strip legal suffixes, lowercase."""
    if not name:
        return ""
    n = name.lower()
    for suffix in [" llc", " inc", " corp", " ltd", " co", " pllc"]:
        n = n.replace(suffix, "")
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', n)).strip()


def _pre_llm_decision(
    city_name: str, city_addr: str,
    bludot_name: str, bludot_addr: str,
    name_score: float,
) -> tuple[str | None, str]:
    """
    Apply pure rules before calling LLM.
    Returns (decision, reason) where decision is:
      "AUTO_MATCH"  → definitely same business
      "AUTO_REJECT" → definitely different
      None          → ambiguous, needs LLM
    """
    from rapidfuzz import fuzz

    city_blank   = not city_addr   or city_addr.strip()   in ('', '-', 'nan')
    bludot_blank = not bludot_addr or bludot_addr.strip() in ('', '-', 'nan')

    city_sn   = _extract_street_num(city_addr)
    bludot_sn = _extract_street_num(bludot_addr)

    # ── Hard reject rules (no LLM needed) ────────────────────────────────────

    # Different street numbers = different locations, period
    if city_sn and bludot_sn and city_sn != bludot_sn:
        return "AUTO_REJECT", f"Street numbers differ: {city_sn} vs {bludot_sn}"

    # Names completely different
    if name_score < 50:
        return "AUTO_REJECT", f"Name similarity too low: {name_score:.0f}%"

    # ── Hard accept rules (no LLM needed) ────────────────────────────────────

    # Both addresses blank + high name similarity → same business
    if city_blank and bludot_blank and name_score >= 90:
        return "AUTO_MATCH", f"Both addresses blank, name similarity {name_score:.0f}%"

    # Both addresses present, street numbers match, names + addresses similar
    if city_sn and bludot_sn and city_sn == bludot_sn and name_score >= 88:
        addr_sim = fuzz.token_sort_ratio(
            city_addr.lower(), bludot_addr.lower()
        )
        if addr_sim >= 75:
            return "AUTO_MATCH", f"Street nums match ({city_sn}), name {name_score:.0f}%, addr {addr_sim:.0f}%"

    # Exact name match + one address blank → auto match
    norm_city   = _normalize_name_simple(city_name)
    norm_bludot = _normalize_name_simple(bludot_name)
    if norm_city and norm_city == norm_bludot and (city_blank or bludot_blank):
        return "AUTO_MATCH", "Exact name match, one address blank"

    # ── Everything else → needs LLM ──────────────────────────────────────────
    return None, ""


# ── Main functions ────────────────────────────────────────────────────────────

def generate_candidates(
    db: Session,
    city_id: int,
    match_pass: int = 1,
) -> int:
    """
    Generate candidate pairs using name-prefix blocking.
    Returns count of candidates created.
    """
    from .rule_filter import normalize_name

    city_recs   = db.query(CityRecord).filter_by(city_id=city_id).all()
    bludot_recs = db.query(BludotRecord).filter_by(city_id=city_id).all()

    if match_pass == 2:
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

    logger.info(f"Generating candidates: {len(city_recs)} city × {len(bludot_recs)} bludot")

    def block_key(name: str) -> str:
        n = normalize_name(name or "")
        return n[:3] if len(n) >= 3 else n

    bludot_blocks: dict[str, list] = {}
    for br in bludot_recs:
        key = block_key(br.name or "")
        bludot_blocks.setdefault(key, []).append(br)
        if len(key) >= 2:
            bludot_blocks.setdefault(key[:2], []).append(br)

    candidate_count = 0
    seen_pairs: set[tuple[int, int]] = set()

    for cr in city_recs:
        cr_key    = block_key(cr.business_name or "")
        candidates = set()
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
                city_name      = cr.business_name or "",
                city_address   = cr.address1 or "",
                bludot_name    = br.name or "",
                bludot_address = br.address1 or "",
            )

            if rule_result.verdict == "DEFINITE_NO_MATCH":
                continue

            mc = MatchCandidate(
                city_id          = city_id,
                city_rec_id      = cr.id,
                bludot_rec_id    = br.id,
                name_score       = rule_result.name_score,
                address_score    = rule_result.address_score,
                street_num_match = rule_result.street_num_match,
                rule_verdict     = rule_result.verdict,
                match_pass       = match_pass,
            )
            db.add(mc)
            candidate_count += 1

    db.commit()
    logger.info(f"Created {candidate_count} candidates (pass {match_pass})")
    return candidate_count


def run_llm_judge(db: Session, city_id: int, match_pass: int = 1) -> dict:
    """
    Two-stage decision:
      Stage A: Pure rules — auto-decide clear cases (no API call)
      Stage B: LLM — only for genuinely ambiguous pairs
    """
    from rapidfuzz import fuzz

    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id    == city_id,
        MatchCandidate.match_pass == match_pass,
        MatchCandidate.rule_verdict == "CANDIDATE",
        MatchCandidate.llm_decision == None,  # noqa
    ).all()

    if not candidates:
        return {"total": 0, "auto_match": 0, "auto_no_match": 0, "needs_llm": 0, "needs_review": 0}

    stats = {"total": len(candidates), "auto_match": 0, "auto_no_match": 0,
             "needs_llm": 0, "needs_review": 0}

    # ── Stage A: rule-based pre-decision ─────────────────────────────────────
    needs_llm = []
    for mc in candidates:
        cr = db.get(CityRecord, mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue

        name_sim = fuzz.token_set_ratio(
            _normalize_name_simple(cr.business_name or ""),
            _normalize_name_simple(br.name or ""),
        )

        decision, reason = _pre_llm_decision(
            city_name    = cr.business_name or "",
            city_addr    = cr.address1 or "",
            bludot_name  = br.name or "",
            bludot_addr  = br.address1 or "",
            name_score   = name_sim,
        )

        if decision == "AUTO_MATCH":
            mc.llm_decision  = "MATCH"
            mc.llm_reason    = f"[Auto] {reason}"
            mc.final_decision = MatchDecision.AUTO_MATCH
            stats["auto_match"] += 1
        elif decision == "AUTO_REJECT":
            mc.llm_decision  = "NO_MATCH"
            mc.llm_reason    = f"[Auto] {reason}"
            mc.final_decision = MatchDecision.AUTO_NO_MATCH
            stats["auto_no_match"] += 1
        else:
            needs_llm.append((mc, cr, br))

    db.commit()
    logger.info(
        f"Pre-LLM: {stats['auto_match']} auto-matched, "
        f"{stats['auto_no_match']} auto-rejected, "
        f"{len(needs_llm)} need LLM"
    )

    # ── Stage B: LLM for ambiguous pairs only ─────────────────────────────────
    if not needs_llm:
        return stats

    stats["needs_llm"] = len(needs_llm)
    pairs = [
        CandidatePair(
            candidate_id  = mc.id,
            city_name     = cr.business_name or "",
            city_address  = cr.address1 or "",
            bludot_name   = br.name or "",
            bludot_address= br.address1 or "",
            rule_reason   = mc.rule_verdict or "",
        )
        for mc, cr, br in needs_llm
    ]

    logger.info(f"Sending {len(pairs)} ambiguous pairs to LLM in a single batch...")
    
    # Send all pairs at once, zero chunking.
    results = judge_candidates(pairs)
    result_map = {r["candidate_id"]: r for r in results if "candidate_id" in r}

    for mc, cr, br in needs_llm:
        res = result_map.get(mc.id)
        if not res:
            mc.final_decision = MatchDecision.NEEDS_REVIEW
            stats["needs_review"] += 1
            continue

        mc.llm_decision  = res["llm_decision"]
        mc.llm_reason    = res.get("llm_reason", res.get("reason", ""))
        mc.llm_called_at = datetime.fromisoformat(res["llm_called_at"]) \
                           if res.get("llm_called_at") else datetime.utcnow()

        if mc.llm_decision == "MATCH":
            mc.final_decision = MatchDecision.AUTO_MATCH
            stats["auto_match"] += 1
        elif mc.llm_decision == "NO_MATCH":
            mc.final_decision = MatchDecision.AUTO_NO_MATCH
            stats["auto_no_match"] += 1
        else:
            mc.final_decision = MatchDecision.NEEDS_REVIEW
            stats["needs_review"] += 1

    db.commit()
    logger.info(f"Final matching stats for city_id={city_id}: {stats}")
    return stats


def apply_human_decision(
    db: Session,
    candidate_id: int,
    accepted: bool,
    reviewer: str = "human",
    note: str = "",
) -> MatchCandidate:
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
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id       == city_id,
        MatchCandidate.match_pass    == match_pass,
        MatchCandidate.final_decision == MatchDecision.NEEDS_REVIEW,
    ).all()

    queue = []
    for mc in candidates:
        cr = db.get(CityRecord, mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue
        queue.append({
            "candidate_id"    : mc.id,
            "name_score"      : mc.name_score,
            "address_score"   : mc.address_score,
            "street_num_match": mc.street_num_match,
            "rule_verdict"    : mc.rule_verdict,
            "llm_decision"    : mc.llm_decision,
            "llm_reason"      : mc.llm_reason,
            "city_record"     : {
                "id"           : cr.id,
                "business_name": cr.business_name,
                "address1"     : cr.address1,
                "raw_data"     : cr.raw_data,
            },
            "bludot_record"   : {
                "id"      : br.id,
                "name"    : br.name,
                "address1": br.address1,
                "uuid"    : br.uuid,
                "raw_data": br.raw_data,
            },
        })
    return queue


def get_match_stats(db: Session, city_id: int) -> dict:
    total_city   = db.query(CityRecord).filter_by(city_id=city_id).count()
    total_bludot = db.query(BludotRecord).filter_by(city_id=city_id).count()

    auto_match    = db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.AUTO_MATCH).count()
    auto_no_match = db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.AUTO_NO_MATCH).count()
    needs_review  = db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.NEEDS_REVIEW).count()
    human_accepted= db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.HUMAN_ACCEPTED).count()
    human_rejected= db.query(MatchCandidate).filter_by(city_id=city_id, final_decision=MatchDecision.HUMAN_REJECTED).count()

    return {
        "total_city_records"     : total_city,
        "total_bludot_records"   : total_bludot,
        "auto_matched"           : auto_match,
        "auto_rejected"          : auto_no_match,
        "pending_review"         : needs_review,
        "human_accepted"         : human_accepted,
        "human_rejected"         : human_rejected,
        "total_confirmed_matches": auto_match + human_accepted,
    }