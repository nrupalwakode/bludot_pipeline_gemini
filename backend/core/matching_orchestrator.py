"""
Matching Orchestrator
=====================
Ties all three stages together for both Step 2 (city vs bludot) and
Step 4.1 (second-pass on residuals).
"""

import re
import logging
from datetime import datetime
from rapidfuzz import fuzz
from sqlalchemy.orm import Session
import usaddress
from sentence_transformers import SentenceTransformer, util

from ..db.models import (
    City, CityRecord, BludotRecord, MatchCandidate, MatchDecision
)
from .rule_filter import apply_rule_filter
from .llm_judge import judge_candidates, CandidatePair

logger = logging.getLogger(__name__)

# --- LOAD VECTOR AI GLOBALLY ---
logger.info("Loading Semantic Vector Embedding Model...")
embedder = SentenceTransformer('all-MiniLM-L6-v2')


# ── Address & String Helpers ──────────────────────────────────────────────────

def _extract_street_num(address: str) -> str:
    m = re.match(r'^\s*(\d+)', address or "")
    return m.group(1) if m else ""

def _normalize_name_simple(name: str) -> str:
    if not name:
        return ""
    n = name.lower()
    for suffix in [" llc", " inc", " corp", " ltd", " co", " pllc"]:
        n = n.replace(suffix, "")
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', n)).strip()

def normalize_address_smart(raw_address: str) -> str:
    if not raw_address or str(raw_address) == 'nan': return ""
    addr_str = str(raw_address).upper().replace('.', '').replace(',', '')
    unit_map = {'SUITE':'#', 'STE':'#', 'APARTMENT':'#', 'APT':'#', 'UNIT':'#', 'ROOM':'#', 'BLDG':'#'}
    try:
        tagged, _ = usaddress.tag(addr_str)
        parts = []
        if 'AddressNumber' in tagged: parts.append(tagged['AddressNumber'])
        if 'StreetNamePreDirectional' in tagged: parts.append(tagged['StreetNamePreDirectional'])
        if 'StreetName' in tagged: parts.append(tagged['StreetName'])
        if 'StreetNamePostType' in tagged: parts.append(tagged['StreetNamePostType'])
        if 'OccupancyType' in tagged: parts.append(unit_map.get(tagged['OccupancyType'], '#')) 
        if 'OccupancyIdentifier' in tagged: parts.append(tagged['OccupancyIdentifier'])
        return " ".join(parts)
    except:
        return re.sub(r'\s+', ' ', addr_str).strip()


# ── The Hybrid Pre-Filter ─────────────────────────────────────────────────────

def _pre_llm_decision(
    city_name: str, city_addr: str, city_dba: str,
    bludot_name: str, bludot_addr: str, bludot_dba: str,
    name_score: float,
) -> tuple[str | None, str]:
    
    city_blank   = not city_addr   or city_addr.strip()   in ('', '-', 'nan')
    bludot_blank = not bludot_addr or bludot_addr.strip() in ('', '-', 'nan')

    city_sn   = _extract_street_num(city_addr)
    bludot_sn = _extract_street_num(bludot_addr)

    norm_city_addr = normalize_address_smart(city_addr)
    norm_blu_addr = normalize_address_smart(bludot_addr)
    
    addr_score = fuzz.ratio(norm_city_addr, norm_blu_addr)

    c_dba_vs_b_name = fuzz.ratio(city_dba.lower(), bludot_name.lower()) if city_dba else 0
    c_name_vs_b_dba = fuzz.ratio(city_name.lower(), bludot_dba.lower()) if bludot_dba else 0
    both_dba_score  = fuzz.ratio(city_dba.lower(), bludot_dba.lower()) if city_dba and bludot_dba else 0
    best_name_score = max(name_score, c_dba_vs_b_name, c_name_vs_b_dba, both_dba_score)

    # ── Hard reject rules ────────────────────────────────────
    if city_sn and bludot_sn and city_sn != bludot_sn:
        return "AUTO_REJECT", f"Street numbers differ: {city_sn} vs {bludot_sn}"

    # CRITICAL FIX: Do NOT auto-reject if the address is highly similar!
    if best_name_score < 50 and addr_score < 80:
        return "AUTO_REJECT", f"Name similarity too low: {best_name_score:.0f}%"

    # ── TIER 1: Fuzzy Matching ────────

    if best_name_score >= 95:
        if addr_score >= 80 or city_blank or bludot_blank:
            return "AUTO_MATCH", f"High name similarity ({best_name_score:.0f}%)"
            
    token_base = fuzz.token_sort_ratio(city_name.lower(), bludot_name.lower())
    token_c_dba = fuzz.token_sort_ratio(city_dba.lower(), bludot_name.lower()) if city_dba else 0
    token_b_dba = fuzz.token_sort_ratio(city_name.lower(), bludot_dba.lower()) if bludot_dba else 0
    token_both = fuzz.token_sort_ratio(city_dba.lower(), bludot_dba.lower()) if city_dba and bludot_dba else 0
    best_token = max(token_base, token_c_dba, token_b_dba, token_both)

    if best_token >= 90:
        if addr_score >= 80 or city_blank or bludot_blank:
            return "AUTO_MATCH", f"Token Sort Match ({best_token:.0f}%)"

    c_display = f"{city_name.lower()} {city_dba.lower()} {norm_city_addr.lower()}"
    b_display = f"{bludot_name.lower()} {bludot_dba.lower()} {norm_blu_addr.lower()}"
    
    if not city_blank and not bludot_blank:
        combined_score = fuzz.ratio(c_display, b_display)
        if combined_score >= 88:
            return "AUTO_MATCH", f"Combined Lexical Match ({combined_score:.0f}%)"

    # ── TIER 2: Semantic Vector AI ──────────
    
    if not city_blank and not bludot_blank:
        emb1 = embedder.encode(c_display, convert_to_tensor=True)
        emb2 = embedder.encode(b_display, convert_to_tensor=True)
        semantic_score = util.cos_sim(emb1, emb2).item() * 100
        if semantic_score >= 85:
            return "AUTO_MATCH", f"Semantic Vector Match ({semantic_score:.0f}%)"

    return None, ""


# ── Main functions ────────────────────────────────────────────────────────────

def generate_candidates(
    db: Session,
    city_id: int,
    match_pass: int = 1,
) -> int:
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
        k_name = block_key(br.name or "")
        if k_name:
            bludot_blocks.setdefault(k_name, []).append(br)
            if len(k_name) >= 2:
                bludot_blocks.setdefault(k_name[:2], []).append(br)
        
        b_dba = str(br.raw_data.get('DBA Name', '')).strip() if br.raw_data else ''
        k_dba = block_key(b_dba)
        if k_dba:
            bludot_blocks.setdefault(k_dba, []).append(br)
            if len(k_dba) >= 2:
                bludot_blocks.setdefault(k_dba[:2], []).append(br)

    candidate_count = 0
    seen_pairs: set[tuple[int, int]] = set()

    for cr in city_recs:
        candidates = set()
        
        k_name = block_key(cr.business_name or "")
        if k_name:
            for k in [k_name, k_name[:2] if len(k_name) >= 2 else k_name]:
                for br in bludot_blocks.get(k, []):
                    candidates.add(br.id)

        c_dba = str(cr.raw_data.get('DBA Name', '')).strip() if cr.raw_data else ''
        k_dba = block_key(c_dba)
        if k_dba:
            for k in [k_dba, k_dba[:2] if len(k_dba) >= 2 else k_dba]:
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

            # CRITICAL FIX: THE SAFETY NET
            if rule_result.verdict == "DEFINITE_NO_MATCH":
                b_dba = str(br.raw_data.get('DBA Name', '')).strip() if br.raw_data else ''
                c_name_clean = str(cr.business_name).lower()
                b_name_clean = str(br.name).lower()
                c_dba_clean = c_dba.lower()
                b_dba_clean = b_dba.lower()

                # Use Token Sort Ratio! (Gateway Arvada Ridge vs Gateway AT Arvada Ridge LLC scores much higher this way)
                s1 = fuzz.token_sort_ratio(c_dba_clean, b_name_clean) if c_dba_clean else 0
                s2 = fuzz.token_sort_ratio(c_name_clean, b_dba_clean) if b_dba_clean else 0
                s3 = fuzz.token_sort_ratio(c_dba_clean, b_dba_clean) if c_dba_clean and b_dba_clean else 0
                best_dba_score = max(s1, s2, s3)
                
                # Check Address
                c_addr_norm = normalize_address_smart(cr.address1 or "")
                b_addr_norm = normalize_address_smart(br.address1 or "")
                addr_score = fuzz.ratio(c_addr_norm, b_addr_norm) if c_addr_norm and b_addr_norm else 0

                # NEW RULE: If DBA score is > 65 OR if the Address matches closely (85%), keep it as a candidate!
                if best_dba_score > 65 or (addr_score >= 85 and best_dba_score > 40):
                    rule_result.verdict = "CANDIDATE"
                else:
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

        c_dba = str(cr.raw_data.get('DBA Name', '')).strip() if cr.raw_data else ""
        b_dba = str(br.raw_data.get('DBA Name', '')).strip() if br.raw_data else ""

        decision, reason = _pre_llm_decision(
            city_name    = cr.business_name or "",
            city_addr    = cr.address1 or "",
            city_dba     = c_dba,
            bludot_name  = br.name or "",
            bludot_addr  = br.address1 or "",
            bludot_dba   = b_dba,
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
            needs_llm.append((mc, cr, br, c_dba, b_dba))

    db.commit()
    
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
            city_dba      = c_dba,
            bludot_dba    = b_dba,
        )
        for mc, cr, br, c_dba, b_dba in needs_llm
    ]

    results = judge_candidates(pairs)
    result_map = {r["candidate_id"]: r for r in results if "candidate_id" in r}

    for mc, cr, br, c_dba, b_dba in needs_llm:
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