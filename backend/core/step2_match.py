"""
Step 2 — Hybrid Matching (Fuzzy + LLM) & Isolated Legacy Export
==============================================================
"""

import os
import logging
import pandas as pd
from rapidfuzz import fuzz
from sqlalchemy.orm import Session

# Import your database models
from ..db.models import City, CityRecord, BludotRecord, MatchCandidate, MatchDecision, ColumnMapping
from .matching_orchestrator import generate_candidates, run_llm_judge

logger = logging.getLogger(__name__)

def run_legacy_fuzzy_match(db: Session, city_id: int) -> int:
    """Mimics the old legacy script by forcefully matching businesses."""
    logger.info("========== RUNNING LEGACY FUZZY MATCH ==========")
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id == city_id,
        MatchCandidate.final_decision == 'PENDING'
    ).all()

    fuzzy_match_count = 0
    for mc in candidates:
        city_rec = db.get(CityRecord, mc.city_rec_id)
        bludot_rec = db.get(BludotRecord, mc.bludot_rec_id)
        if not city_rec or not bludot_rec: 
            continue

        city_name = str(city_rec.business_name).lower().strip()
        bludot_name = str(bludot_rec.name).lower().strip()

        # Calculate Fuzzy Ratio exactly like the old script
        score = fuzz.ratio(city_name, bludot_name)
        
        if score >= 85:  # Legacy threshold
            mc.final_decision = MatchDecision.AUTO_MATCH
            mc.llm_reason = f"Legacy Fuzzy Match (Name Similarity: {score}%)"
            fuzzy_match_count += 1

    db.commit()
    logger.info(f"Fuzzy Match found {fuzzy_match_count} matches!")
    return fuzzy_match_count


def export_to_legacy_format(db: Session, city_id: int, city_name: str):
    logger.info(f"========== STARTING LEGACY EXPORT FOR {city_name} ==========")
    try:
        # Isolated folder so Step 5 doesn't crash!
        output_dir = os.path.join(os.getcwd(), 'cities_and_counties', city_name, 'legacy_exports')
        os.makedirs(output_dir, exist_ok=True)

        mappings = db.query(ColumnMapping).filter_by(city_id=city_id, mapping_type='business').all()
        reverse_rename = {m.target_col: m.source_col for m in mappings}
        
        # We only protect these in the MATCHED file. For leftovers, legacy added _1 and _2 to everything.
        system_cols = ['city_index', 'bludot_index', 'cluster id', 'lsh_bucket', 'norm_name', 'is_po_box', 'po_box_num', 'street_num', 'street_name', 'norm_address', 'business_address']

        successful_matches = db.query(MatchCandidate).filter(
            MatchCandidate.city_id == city_id,
            MatchCandidate.final_decision.in_([MatchDecision.AUTO_MATCH, MatchDecision.HUMAN_ACCEPTED])
        ).all()

        matched_city_ids = {mc.city_rec_id for mc in successful_matches}
        matched_bludot_ids = {mc.bludot_rec_id for mc in successful_matches}

        # ---------------------------------------------------------
        # BUILD 1: Final Matched Records
        # ---------------------------------------------------------
        matched_rows = []
        for mc in successful_matches:
            city_rec = db.get(CityRecord, mc.city_rec_id)
            bludot_rec = db.get(BludotRecord, mc.bludot_rec_id)
            if not city_rec or not bludot_rec: continue
            
            combined_row = {}
            if city_rec.raw_data:
                for k, v in city_rec.raw_data.items():
                    orig_k = reverse_rename.get(k, k) 
                    if orig_k in system_cols: combined_row[orig_k] = v
                    else: combined_row[f"{orig_k}_1"] = v 

            if bludot_rec.raw_data:
                for k, v in bludot_rec.raw_data.items():
                    if k in system_cols: combined_row[k] = v
                    else: combined_row[f"{k}_2"] = v
            
            combined_row["Match_Logic_Used"] = mc.llm_reason
            matched_rows.append(combined_row)

        # ---------------------------------------------------------
        # BUILD 2: Additional City Records (EXACT Legacy Formatting)
        # ---------------------------------------------------------
        all_city = db.query(CityRecord).filter_by(city_id=city_id).all()
        additional_city_formatted = []
        for c in all_city:
            if c.id not in matched_city_ids and c.raw_data:
                row = {}
                # Legacy artifact: 'cluster id' comes first with no suffix
                row['cluster id'] = c.raw_data.get('cluster id')
                
                # Add _1 to literally every original column to match legacy Pandas merge
                for k, v in c.raw_data.items():
                    orig_k = reverse_rename.get(k, k) 
                    row[f"{orig_k}_1"] = v 
                
                # Legacy artifact: 'city_index' comes last with no suffix
                row['city_index'] = c.raw_data.get('city_index')
                additional_city_formatted.append(row)

        # ---------------------------------------------------------
        # BUILD 3: Additional Bludot Records (EXACT Legacy Formatting)
        # ---------------------------------------------------------
        all_bludot = db.query(BludotRecord).filter_by(city_id=city_id).all()
        additional_bludot_formatted = []
        for b in all_bludot:
            if b.id not in matched_bludot_ids and b.raw_data:
                row = {}
                # Legacy artifact: 'cluster id' comes first
                row['cluster id'] = b.raw_data.get('cluster id')
                
                # Add _2 to literally every column
                for k, v in b.raw_data.items():
                    row[f"{k}_2"] = v 
                    
                # Legacy artifact: 'bludot_index' comes last
                row['bludot_index'] = b.raw_data.get('bludot_index')
                additional_bludot_formatted.append(row)

        # ---------------------------------------------------------
        # SAVE EXCEL FILES
        # ---------------------------------------------------------
        if matched_rows:
            pd.DataFrame(matched_rows).to_excel(os.path.join(output_dir, f'Final_Matched_Records_for_{city_name}.xlsx'), index=False)
        if additional_city_formatted:
            pd.DataFrame(additional_city_formatted).to_excel(os.path.join(output_dir, f'Additional_City_Records_for_{city_name}.xlsx'), index=False)
        if additional_bludot_formatted:
            pd.DataFrame(additional_bludot_formatted).to_excel(os.path.join(output_dir, f'Additional_Bludot_Records_for_{city_name}.xlsx'), index=False)

        logger.info(f"========== SUCCESS! Legacy files isolated in: {output_dir} ==========")
    except Exception as e:
        logger.error(f"========== EXPORT FAILED: {str(e)} ==========", exc_info=True)


def run_step2(city: City, city_id: int, db: Session, results_dir: str) -> dict:
    """Entry point called by pipeline._step2."""
    logger.info(f"Step 2: Generating match candidates for city_id={city_id}")
    candidate_count = generate_candidates(db, city_id, match_pass=1)
    
    fuzzy_count = run_legacy_fuzzy_match(db, city_id)
    
    logger.info(f"Step 2: Sending remaining candidates to LLM judge…")
    llm_stats = run_llm_judge(db, city_id, match_pass=1)
    
    export_to_legacy_format(db, city_id, city.name)
    
    logger.info(f"Step 2 complete! Fuzzy matched: {fuzzy_count}. LLM stats: {llm_stats}")
    return {**llm_stats, "candidates": candidate_count, "fuzzy_matches": fuzzy_count}