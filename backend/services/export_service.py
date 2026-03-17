"""
Export Service
==============
Converts confirmed match decisions from the DB back into the Excel files
that the existing step5 and step6 scripts expect as input.

This is the bridge between the new DB-driven pipeline and the existing
output generation logic (final_sheet_creation.py, contact_formatting.py).
"""

import os
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session

from ..db.models import (
    City, CityRecord, BludotRecord, MatchCandidate, MatchDecision, ColumnMapping
)


def export_matched_records_to_excel(db: Session, city_id: int) -> str:
    """
    Recreates the `Updated_Matched_Records.xlsx` that step4/step5 expect.
    Pulls all confirmed matches (auto_match + human_accepted) from the DB.

    Returns the path to the written file.
    """
    city = db.get(City, city_id)
    if not city:
        raise ValueError(f"City {city_id} not found")

    city_path  = Path(city.raw_data_path).parent
    output_dir = city_path / "results" / "filter_matches"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = str(output_dir / "Updated_Matched_Records.xlsx")

    # Fetch all confirmed matches
    confirmed = db.query(MatchCandidate).filter(
        MatchCandidate.city_id == city_id,
        MatchCandidate.final_decision.in_([
            MatchDecision.AUTO_MATCH,
            MatchDecision.HUMAN_ACCEPTED,
        ]),
    ).all()

    if not confirmed:
        # Write empty file so downstream steps don't crash
        pd.DataFrame().to_excel(output_path, index=False)
        return output_path

    rows = []
    for mc in confirmed:
        cr = db.get(CityRecord, mc.city_rec_id)
        br = db.get(BludotRecord, mc.bludot_rec_id)
        if not cr or not br:
            continue

        # Merge raw_data dicts from both records, prefixed to avoid collisions
        row = {}
        # City record fields
        if cr.raw_data:
            row.update(cr.raw_data)
        # Bludot record fields (will overwrite with bludot values where present)
        if br.raw_data:
            row.update({f"bludot_{k}" if k in row else k: v
                        for k, v in br.raw_data.items()})

        # Always include the index columns step4 needs
        row["city_index"]   = cr.city_index
        row["bludot_index"] = br.bludot_index
        row["UUID"]         = br.uuid
        row["match_decision"] = mc.final_decision
        row["llm_reason"]   = mc.llm_reason or ""

        rows.append(row)

    df = pd.DataFrame(rows)
    df.to_excel(output_path, sheet_name="Updated_Matched_Records", index=False)
    return output_path


def export_additional_city_records(db: Session, city_id: int) -> str:
    """Export city records that were NOT matched (additional_city_records)."""
    city = db.get(City, city_id)
    city_path  = Path(city.raw_data_path).parent
    output_dir = city_path / "results" / "output" / "final_result"
    output_dir.mkdir(parents=True, exist_ok=True)

    matched_city_ids = {
        mc.city_rec_id for mc in db.query(MatchCandidate).filter(
            MatchCandidate.city_id == city_id,
            MatchCandidate.final_decision.in_([
                MatchDecision.AUTO_MATCH, MatchDecision.HUMAN_ACCEPTED
            ]),
        )
    }

    unmatched = db.query(CityRecord).filter(
        CityRecord.city_id == city_id,
        ~CityRecord.id.in_(matched_city_ids),
    ).all()

    rows = [r.raw_data for r in unmatched if r.raw_data]
    df = pd.DataFrame(rows)

    output_path = str(output_dir / f"additional_city_records_for_{city.name}.xlsx")
    df.to_excel(output_path, index=False)
    return output_path


def export_additional_bludot_records(db: Session, city_id: int) -> str:
    """Export bludot records that were NOT matched (additional_bludot_records)."""
    city = db.get(City, city_id)
    city_path  = Path(city.raw_data_path).parent
    output_dir = city_path / "results" / "output" / "final_result"
    output_dir.mkdir(parents=True, exist_ok=True)

    matched_bludot_ids = {
        mc.bludot_rec_id for mc in db.query(MatchCandidate).filter(
            MatchCandidate.city_id == city_id,
            MatchCandidate.final_decision.in_([
                MatchDecision.AUTO_MATCH, MatchDecision.HUMAN_ACCEPTED
            ]),
        )
    }

    unmatched = db.query(BludotRecord).filter(
        BludotRecord.city_id == city_id,
        ~BludotRecord.id.in_(matched_bludot_ids),
    ).all()

    rows = [r.raw_data for r in unmatched if r.raw_data]
    df = pd.DataFrame(rows)

    output_path = str(output_dir / f"additional_bludot_records_for_{city.name}.xlsx")
    df.to_excel(output_path, index=False)
    return output_path


def get_column_mapping_dict(db: Session, city_id: int) -> dict:
    """
    Returns {source_col: target_col} mapping saved in DB.
    Used to rename columns during data ingestion.
    """
    mappings = db.query(ColumnMapping).filter_by(city_id=city_id).all()
    return {m.source_col: m.target_col for m in mappings}


def apply_column_mapping(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    """
    Rename columns in a DataFrame according to the saved mapping,
    dropping columns mapped to 'SKIP'.
    """
    rename = {src: tgt for src, tgt in mapping.items() if tgt != "SKIP"}
    drop   = [src for src, tgt in mapping.items() if tgt == "SKIP"]

    df = df.rename(columns=rename)
    df = df.drop(columns=[c for c in drop if c in df.columns], errors="ignore")
    return df
