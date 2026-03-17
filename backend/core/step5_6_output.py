"""
Steps 5 & 6 — Output Sheet Generation
======================================
Replaces: step5_business___additional_matched_records.py
          step6_contacts.py
          final_sheet_creation.py
          contact_formatting.py

Reads confirmed matches from the DB + the intermediate Excel files,
applies the user's saved column mapping, and writes:

  results/output/final_output/{CityName}_Business_Matched_Records.xlsx
    → Business_Matched_Records sheet
    → Custom_Matched_Records sheet
    → Contact_Matched_Records sheet

  results/output/final_output/Additional_Matched_Records_Of_{CityName}.xlsx
    → same 3 sheets, for additional (unmatched) records

The output Business schema is always:
  ID, Business Name, Address1, Address2, City, State, Country, Zipcode,
  Phonenumber, Website, is_business, Lat, Long, business_source
"""

import re
from datetime import datetime
from pathlib import Path

import pandas as pd
import numpy as np
from sqlalchemy.orm import Session

from ..db.models import City, ColumnMapping, MatchCandidate, MatchDecision, CityRecord, BludotRecord


# ── Fixed output schema ───────────────────────────────────────────────────────

BUSINESS_OUTPUT_COLS = [
    'ID', 'Business Name', 'Address1', 'Address2', 'City', 'State',
    'Country', 'Zipcode', 'Phonenumber', 'Website',
    'is_business', 'Lat', 'Long', 'business_source',
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _city_prefix(city_name: str) -> str:
    """e.g. 'Medford_OR_10_05_2025' → 'MED'"""
    return city_name.split('_')[0][:3].upper()


def _date_str() -> str:
    now = datetime.now()
    return f"{now.strftime('%d')}{now.strftime('%m')}{now.year}"


def _generate_ids(n: int, city_name: str, prefix_override: str = None) -> list[str]:
    prefix = prefix_override or (_city_prefix(city_name) + _date_str())
    pad = len(str(n))
    return [f"{prefix}{str(i).zfill(pad)}" for i in range(1, n + 1)]


def _load_column_mappings(db: Session, city_id: int) -> dict:
    """Returns {source_col: target_col} for mapping_type='business'."""
    rows = db.query(ColumnMapping).filter_by(city_id=city_id, mapping_type='business').all()
    return {r.source_col: r.target_col for r in rows}


def _load_contact_mappings(db: Session, city_id: int) -> list[dict]:
    """Returns list of {source_col, role, contact_type, person_col}."""
    rows = db.query(ColumnMapping).filter_by(city_id=city_id, mapping_type='contact').all()
    result = []
    for r in rows:
        meta = r.meta or {}
        result.append({
            'source_col':   r.source_col,
            'role':         meta.get('role', 'Contact'),
            'contact_type': meta.get('contact_type', 'email'),
            'person_col':   meta.get('person_col', ''),
        })
    return result


def _load_custom_mappings(db: Session, city_id: int) -> list[dict]:
    """Returns list of {source_col, target_col, bludot_custom_col}."""
    rows = db.query(ColumnMapping).filter_by(city_id=city_id, mapping_type='custom').all()
    result = []
    for r in rows:
        meta = r.meta or {}
        result.append({
            'source_col':       r.source_col,
            'target_col':       r.target_col,
            'bludot_custom_col': meta.get('bludot_custom_col', ''),
        })
    return result


def _apply_business_mapping(raw_row: dict, biz_map: dict) -> dict:
    """Apply business field mapping to a raw row dict → output row dict."""
    out = {col: '' for col in BUSINESS_OUTPUT_COLS}
    for src, tgt in biz_map.items():
        if tgt in BUSINESS_OUTPUT_COLS and src in raw_row:
            out[tgt] = raw_row.get(src, '')
    return out


# ── Step 5: Business + Custom sheets ─────────────────────────────────────────

def _build_business_df(records: list[dict], biz_map: dict, city_name: str,
                       is_additional: bool = False) -> pd.DataFrame:
    rows = [_apply_business_mapping(r, biz_map) for r in records]
    df = pd.DataFrame(rows, columns=BUSINESS_OUTPUT_COLS)
    df['is_business'] = 'TRUE'
    df['business_source'] = 'CITY' if is_additional else 'BLUDOT'
    df['ID'] = _generate_ids(len(df), city_name)
    return df


def _build_custom_df(records: list[dict], custom_map: list[dict],
                     bludot_rows: list[dict] | None, city_name: str,
                     is_additional: bool = False) -> pd.DataFrame:
    """Build custom data sheet from city columns + optional bludot custom cols."""
    if not custom_map:
        return pd.DataFrame()

    output_rows = []
    for i, city_row in enumerate(records):
        out = {}
        for m in custom_map:
            val = city_row.get(m['source_col'], '')
            # Merge with bludot custom value if mapped
            if m['bludot_custom_col'] and bludot_rows and i < len(bludot_rows):
                bval = bludot_rows[i].get(m['bludot_custom_col'], '')
                val = bval if bval and not val else val
            out[m['target_col']] = val
        output_rows.append(out)

    df = pd.DataFrame(output_rows)
    df.insert(0, 'ID', _generate_ids(len(df), city_name))
    return df


# ── Step 6: Contact sheet ─────────────────────────────────────────────────────

def _build_contact_df(records: list[dict], contact_map: list[dict],
                      city_name: str) -> pd.DataFrame:
    """
    Build contact sheet.
    Each contact_map entry = one contact column from the city sheet.
    Output columns: ID, Name, Title, Roles, Contact, Contact_type, Type
    """
    if not contact_map:
        return pd.DataFrame()

    output_rows = []
    for city_row in records:
        # Group by person_col to merge entries for the same person
        person_groups: dict[str, list] = {}
        for m in contact_map:
            val = str(city_row.get(m['source_col'], '')).strip()
            if not val:
                continue
            person_col_val = str(city_row.get(m['person_col'], '')).strip() if m['person_col'] else ''
            key = person_col_val or m['role']
            if key not in person_groups:
                person_groups[key] = {
                    'name':  person_col_val,
                    'role':  m['role'],
                    'items': [],
                }
            person_groups[key]['items'].append({
                'contact':      val,
                'contact_type': m['contact_type'],
                'type':         m['role'],
            })

        if not person_groups:
            output_rows.append({'Name': '', 'Title': '', 'Roles': '',
                                 'Contact': '', 'Contact_type': '', 'Type': ''})
            continue

        for person_key, person_data in person_groups.items():
            for item in person_data['items']:
                output_rows.append({
                    'Name':         person_data['name'],
                    'Title':        '',
                    'Roles':        person_data['role'],
                    'Contact':      item['contact'],
                    'Contact_type': item['contact_type'],
                    'Type':         item['type'],
                })

    df = pd.DataFrame(output_rows)
    if df.empty:
        return df
    df.insert(0, 'ID', _generate_ids(len(df), city_name))
    return df


# ── Public API ────────────────────────────────────────────────────────────────

def run_step5_and_step6(city: City, db: Session, output_dir: str) -> dict:
    """
    Entry point called by the pipeline.
    Generates all final output Excel files.
    """
    output_dir = Path(output_dir)
    final_result_dir = output_dir / 'output' / 'final_result'
    final_output_dir = output_dir / 'output' / 'final_output'
    final_output_dir.mkdir(parents=True, exist_ok=True)

    city_name = city.name

    # Load mappings from DB
    biz_map     = _load_column_mappings(db, city.id)
    contact_map = _load_contact_mappings(db, city.id)
    custom_map  = _load_custom_mappings(db, city.id)

    # Load the intermediate match result files
    matched_path = final_result_dir / f'final_matched_records_for_{city_name}.xlsx'
    additional_path = final_result_dir / f'additional_city_records_for_{city_name}.xlsx'
    bludot_path  = output_dir / 'bludot_data' / 'bludot_concatenated_records.xlsx'

    def load_records(path: Path) -> list[dict]:
        if not path.exists():
            return []
        df = pd.read_excel(str(path), dtype=object).fillna('')
        return df.to_dict('records')

    matched_records    = load_records(matched_path)
    additional_records = load_records(additional_path)
    bludot_records     = load_records(bludot_path) if bludot_path.exists() else []

    # ── Build sheets ──────────────────────────────────────────────────────────
    biz_matched_df    = _build_business_df(matched_records,    biz_map, city_name, is_additional=False)
    biz_additional_df = _build_business_df(additional_records, biz_map, city_name, is_additional=True)

    custom_matched_df    = _build_custom_df(matched_records,    custom_map, bludot_records, city_name)
    custom_additional_df = _build_custom_df(additional_records, custom_map, None, city_name, is_additional=True)

    contact_matched_df    = _build_contact_df(matched_records,    contact_map, city_name)
    contact_additional_df = _build_contact_df(additional_records, contact_map, city_name)

    # ── Write output Excel files ──────────────────────────────────────────────
    main_excel = final_output_dir / f'{city_name}_Business_Matched_Records.xlsx'
    with pd.ExcelWriter(str(main_excel), engine='openpyxl') as writer:
        biz_matched_df.to_excel(writer, sheet_name='Business_Matched_Records', index=False)
        if not custom_matched_df.empty:
            custom_matched_df.to_excel(writer, sheet_name='Custom_Matched_Records', index=False)
        if not contact_matched_df.empty:
            contact_matched_df.to_excel(writer, sheet_name='Contact_Matched_Records', index=False)

    add_excel = final_output_dir / f'Additional_Matched_Records_Of_{city_name}.xlsx'
    with pd.ExcelWriter(str(add_excel), engine='openpyxl') as writer:
        biz_additional_df.to_excel(writer, sheet_name='Additional_Business_Matched_Rec', index=False)
        if not custom_additional_df.empty:
            custom_additional_df.to_excel(writer, sheet_name='Additional_Custom_Matched_Rec', index=False)
        if not contact_additional_df.empty:
            contact_additional_df.to_excel(writer, sheet_name='Additional_Contact_Matched_Rec', index=False)

    return {
        'matched_records':    len(matched_records),
        'additional_records': len(additional_records),
        'contact_rows':       len(contact_matched_df),
        'custom_fields':      len(custom_map),
        'output_file':        str(main_excel),
    }