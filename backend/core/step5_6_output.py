"""
Step 5 & 6 — Generate Final Output Sheets
==========================================
Replaces: step4_final_matched_sheet.py + step4.1_final_matched_sheet_after_fuzzy_lookup.py

Reads matched records from DB and generates:
  results/output/final_result/additional_city_records_for_{city}.xlsx
  results/output/final_result/additional_bludot_records_for_{city}.xlsx
  results/output/final_output/{city}_Business_Matched_Records.xlsx
  results/output/final_output/Additional_Matched_Records_Of_{city}.xlsx
"""

import logging
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session

from ..db.models import (
    City, CityRecord, BludotRecord, MatchCandidate, MatchDecision
)

logger = logging.getLogger(__name__)


def _get_all_confirmed_matches(db: Session, city_id: int) -> list[dict]:
    """Return all confirmed matched pairs (AUTO + HUMAN) from both passes."""
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id == city_id,
        MatchCandidate.final_decision.in_([
            MatchDecision.AUTO_MATCH,
            MatchDecision.HUMAN_ACCEPTED,
        ])
    ).all()

    rows = []
    for mc in candidates:
        cr = db.get(CityRecord,  mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue
        rows.append({
            "city_index"   : cr.city_index,
            "bludot_index" : br.bludot_index,
            "city_name"    : cr.business_name,
            "city_address" : cr.address1,
            "bludot_name"  : br.name,
            "bludot_address": br.address1,
            "bludot_uuid"  : br.uuid,
            "match_pass"   : mc.match_pass,
            "final_decision": mc.final_decision,
            "llm_reason"   : mc.llm_reason or "",
        })
    return rows


def _separate_main_spreadsheet(matched_df, city_df, bludot_df):
    """
    From step4_final_matched_sheet.py — split out the UNMATCHED records.
    Returns (additional_city, additional_bludot) — records not in matched set.
    """
    additional_city   = city_df[~city_df['city_index'].isin(matched_df['city_index'].values)]
    additional_bludot = bludot_df[~bludot_df['bludot_index'].isin(matched_df['bludot_index'].values)]
    return additional_city.reset_index(drop=True), additional_bludot.reset_index(drop=True)


def run_step5_and_step6(city: City, db: Session, results_dir: str) -> dict:
    """
    Entry point called by pipeline.py

    Reads:  DB match candidates + city/bludot records from DB
    Writes: final output Excel files
    """
    results_path = Path(results_dir)
    city_data_dir   = results_path / 'city_data'
    bludot_data_dir = results_path / 'bludot_data'
    output_dir      = results_path / 'output'
    final_result    = output_dir / 'final_result'
    final_output    = output_dir / 'final_output'

    for d in [output_dir, final_result, final_output]:
        d.mkdir(parents=True, exist_ok=True)

    city_name = city.name

    # ── Load city and bludot records from disk (de_duplication_merged) ────────
    dedup_merged_path = city_data_dir / 'de_duplication_merged.xlsx'
    bludot_concat_path = bludot_data_dir / 'bludot_concatenated_records.xlsx'

    if not dedup_merged_path.exists():
        logger.warning(f"step5: {dedup_merged_path} not found — trying manual_dedup_records.xlsx")
        dedup_merged_path = city_data_dir / 'manual_dedup_records.xlsx'

    city_df   = pd.read_excel(str(dedup_merged_path))   if dedup_merged_path.exists()  else pd.DataFrame()
    bludot_df = pd.read_excel(str(bludot_concat_path))  if bludot_concat_path.exists() else pd.DataFrame()

    # Ensure index columns exist
    if 'city_index' not in city_df.columns and len(city_df):
        city_df['city_index'] = range(len(city_df))
    if 'bludot_index' not in bludot_df.columns and len(bludot_df):
        bludot_df['bludot_index'] = range(len(bludot_df))

    # ── Get all confirmed matches from DB ─────────────────────────────────────
    matched_rows = _get_all_confirmed_matches(db, city.id)
    logger.info(f"step5: {len(matched_rows)} confirmed matches found in DB")

    if not matched_rows:
        logger.warning("step5: No matched records found — output will be empty")
        matched_df  = pd.DataFrame(columns=['city_index', 'bludot_index'])
        add_city_df = city_df.copy()
        add_bludot_df = bludot_df.copy()
    else:
        matched_df = pd.DataFrame(matched_rows)

        # ── Reorder city records to match order of matched pairs ──────────────
        # (from step4.1 — rearrange by matched index order)
        if len(city_df):
            city_index_map   = {v: i for i, v in enumerate(matched_df['city_index'])}
            matched_city_df  = city_df[city_df['city_index'].isin(matched_df['city_index'].values)].copy()
            matched_city_df['_sort'] = matched_city_df['city_index'].map(city_index_map)
            matched_city_df  = matched_city_df.sort_values('_sort').drop('_sort', axis=1).reset_index(drop=True)
        else:
            matched_city_df = pd.DataFrame()

        if len(bludot_df):
            bludot_index_map  = {v: i for i, v in enumerate(matched_df['bludot_index'])}
            matched_bludot_df = bludot_df[bludot_df['bludot_index'].isin(matched_df['bludot_index'].values)].copy()
            matched_bludot_df['_sort'] = matched_bludot_df['bludot_index'].map(bludot_index_map)
            matched_bludot_df = matched_bludot_df.sort_values('_sort').drop('_sort', axis=1).reset_index(drop=True)
        else:
            matched_bludot_df = pd.DataFrame()

        # ── Write matched city + bludot side by side ──────────────────────────
        # (from step4.1 — merged_fuzzy_matched_city_bludot_records equivalent)
        if len(matched_city_df) and len(matched_bludot_df):
            combined = pd.concat([matched_city_df, matched_bludot_df], axis=1)

            # Add UUID, city name, bludot name, city address, bludot address as first cols
            front_cols = pd.DataFrame({
                'UUID'          : matched_df['bludot_uuid'].values,
                f'{city_name} Name'    : matched_df['city_name'].values,
                'Bludot Name'   : matched_df['bludot_name'].values,
                f'{city_name} Address' : matched_df['city_address'].values,
                'Bludot Address': matched_df['bludot_address'].values,
                'Match Pass'    : matched_df['match_pass'].values,
                'Decision'      : matched_df['final_decision'].values,
                'LLM Reason'    : matched_df['llm_reason'].values,
            })

            final_sheet = pd.concat([front_cols, combined], axis=1)
            final_sheet.to_excel(
                str(final_output / f'{city_name}_Business_Matched_Records.xlsx'),
                index=False, sheet_name='Matched_Records'
            )
            logger.info(f"step5: Wrote {len(final_sheet)} matched records → {city_name}_Business_Matched_Records.xlsx")
        else:
            matched_df.to_excel(
                str(final_output / f'{city_name}_Business_Matched_Records.xlsx'),
                index=False
            )

        # ── Separate additional (unmatched) records ───────────────────────────
        # (from step4_final_matched_sheet.py — separate_main_spreadsheet)
        add_city_df, add_bludot_df = _separate_main_spreadsheet(matched_df, city_df, bludot_df)

    # ── Write additional records ──────────────────────────────────────────────
    add_city_df.to_excel(
        str(final_result / f'additional_city_records_for_{city_name}.xlsx'),
        index=False, sheet_name='Additional_City_Records'
    )
    add_bludot_df.to_excel(
        str(final_result / f'additional_bludot_records_for_{city_name}.xlsx'),
        index=False, sheet_name='Additional_Bludot_Records'
    )

    # ── Write additional matched records file ─────────────────────────────────
    # (from step4.1 — Additional_Matched_Records equivalent)
    if len(matched_rows):
        matched_df.to_excel(
            str(final_output / f'Additional_Matched_Records_Of_{city_name}.xlsx'),
            index=False, sheet_name='All_Matches'
        )

    # ── Update city output_file_path in DB ────────────────────────────────────
    city.output_file_path = str(final_output / f'{city_name}_Business_Matched_Records.xlsx')
    db.commit()

    stats = {
        'matched_records'     : len(matched_rows),
        'additional_city'     : len(add_city_df),
        'additional_bludot'   : len(add_bludot_df) if len(bludot_df) else 0,
        'additional_records'  : len(add_city_df),
        'contact_rows'        : 0,
        'custom_fields'       : 0,
        'output_file'         : str(final_output / f'{city_name}_Business_Matched_Records.xlsx'),
    }
    logger.info(f"step5: Done — {stats}")
    return stats
