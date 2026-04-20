"""
Step 3 — Split Records
=======================
Reads all confirmed matches (AUTO_MATCH + HUMAN_ACCEPTED, pass 1) from DB.
Splits city and bludot records into:
  - final_matched_records_for_{city}.xlsx     — matched pairs side by side
  - additional_city_records_for_{city}.xlsx   — unmatched city records
  - additional_bludot_records_for_{city}.xlsx — unmatched bludot records

These files are the input for Step 5 (field mapping output).
Human verifies these files before Step 4 runs.
If additional fuzzy matches are found manually, create:
  filter_matches/city_bludot_index.xlsx  (city_index, bludot_index columns)
and resume — Step 4 will pick those up.
"""

import logging
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session

from ..db.models import City, CityRecord, BludotRecord, MatchCandidate, MatchDecision

logger = logging.getLogger(__name__)


def _get_confirmed_matches(db: Session, city_id: int, match_pass: int = 1) -> list[dict]:
    """Get all confirmed matches for a specific pass."""
    candidates = db.query(MatchCandidate).filter(
        MatchCandidate.city_id    == city_id,
        MatchCandidate.match_pass == match_pass,
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
            "city_index"    : cr.city_index,
            "bludot_index"  : br.bludot_index,
            "city_name"     : cr.business_name,
            "city_address"  : cr.address1,
            "bludot_name"   : br.name,
            "bludot_address": br.address1,
            "bludot_uuid"   : br.uuid,
        })
    return rows


def run_step3(city: City, city_id: int, db: Session, results_dir: str) -> dict:
    """Entry point called by pipeline._step3."""
    results_path    = Path(results_dir)
    city_data_dir   = results_path / "city_data"
    bludot_data_dir = results_path / "bludot_data"
    final_result    = results_path / "output" / "final_result"
    final_result.mkdir(parents=True, exist_ok=True)

    city_name = city.name

    # Load full city and bludot tables
    dedup_path   = city_data_dir / "de_duplication_merged.xlsx"
    bludot_path  = bludot_data_dir / "bludot_concatenated_records.xlsx"

    city_df   = pd.read_excel(str(dedup_path))  if dedup_path.exists()  else pd.DataFrame()
    bludot_df = pd.read_excel(str(bludot_path)) if bludot_path.exists() else pd.DataFrame()

    if "city_index"   not in city_df.columns   and len(city_df):
        city_df["city_index"]   = range(len(city_df))
    if "bludot_index" not in bludot_df.columns and len(bludot_df):
        bludot_df["bludot_index"] = range(len(bludot_df))

    # Get pass 1 confirmed matches
    matched_rows = _get_confirmed_matches(db, city_id, match_pass=1)
    logger.info(f"Step 3: {len(matched_rows)} confirmed matches (pass 1)")

    if not matched_rows:
        matched_df = pd.DataFrame(columns=["city_index", "bludot_index"])
    else:
        matched_df = pd.DataFrame(matched_rows)

    # Matched city + bludot records side by side
    if len(matched_df) and len(city_df) and len(bludot_df):
        # FIXED: Index-Locked Left Join
        # This perfectly aligns the rows 1:1 and absolutely guarantees they can never shift!
        matched_city = pd.merge(matched_df[["city_index"]], city_df, on="city_index", how="left")
        matched_bludot = pd.merge(matched_df[["bludot_index"]], bludot_df, on="bludot_index", how="left")
        
        final_matched = pd.concat([matched_city, matched_bludot], axis=1)
    else:
        final_matched = pd.DataFrame()

    # Additional (unmatched) records
    if len(matched_df):
        add_city   = city_df[~city_df["city_index"].isin(matched_df["city_index"])].reset_index(drop=True)
        add_bludot = bludot_df[~bludot_df["bludot_index"].isin(matched_df["bludot_index"])].reset_index(drop=True)
    else:
        add_city   = city_df.copy()
        add_bludot = bludot_df.copy()

    # Write all files
    if len(final_matched):
        final_matched.to_excel(
            str(final_result / f"final_matched_records_for_{city_name}.xlsx"),
            index=False)
    add_city.to_excel(
        str(final_result / f"additional_city_records_for_{city_name}.xlsx"),
        index=False)
    add_bludot.to_excel(
        str(final_result / f"additional_bludot_records_for_{city_name}.xlsx"),
        index=False)

    logger.info(f"Step 3: Wrote {len(final_matched)} matched, "
                f"{len(add_city)} additional city, {len(add_bludot)} additional bludot")

    return {
        "matched"          : len(final_matched),
        "additional_city"  : len(add_city),
        "additional_bludot": len(add_bludot),
    }