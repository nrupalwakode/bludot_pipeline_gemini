"""
Step 6 — Contacts Deduplication + Append to Final Excel
=========================================================
Adapted from: step6_contacts.py

Processes contact data from:
  - additional_city_records_for_{city}.xlsx  (additional_records sheet type)
  - final_matched_records_for_{city}.xlsx    (business_matched sheet type)

Outputs CSV files then appends as sheets to the final Excel files:
  - Contact_Matched_Records → {city}_Business_Matched_Records.xlsx
  - Additional_Contact_Matched_Records → Additional_Matched_Records_Of_{city}.xlsx

REQUIRES: src/contact_formatting.py (format_contact_data, clean_column_names)
          city_schema.json CONTACT_CONFIG section
"""

import os
import re
import csv
import glob
import logging
import datetime
import pandas as pd
from pathlib import Path
from openpyxl import load_workbook
from sqlalchemy.orm import Session

from ..db.models import City

logger = logging.getLogger(__name__)


# ── Helpers copied from step6_contacts.py ────────────────────────────────────

def _is_hardcoded_value(value: str) -> bool:
    return isinstance(value, str) and value.startswith('[') and value.endswith(']')


def _extract_hardcoded_value(value: str) -> str:
    return value[1:-1] if _is_hardcoded_value(value) else value


def _extract_date_from_city_name(city_name: str) -> str:
    try:
        parts = city_name.split('_')
        if len(parts) >= 4:
            day   = parts[-3].zfill(2)
            month = parts[-2].zfill(2)
            year  = parts[-1]
            if day.isdigit() and month.isdigit() and year.isdigit() and len(year) == 4:
                return f"{year}{month}{day}"
    except Exception:
        pass
    return datetime.datetime.now().strftime("%Y%m%d")


def _generate_id(df: pd.DataFrame, city_name: str) -> pd.Series:
    city_prefix  = city_name.split('_')[0][:3].upper()
    date_suffix  = _extract_date_from_city_name(city_name)
    total_rows   = len(df)
    padding      = len(str(total_rows))
    return pd.Series([
        f"{city_prefix}{date_suffix}{str(i).zfill(padding)}"
        for i in range(1, total_rows + 1)
    ])


def _find_dynamic_columns(columns):
    pattern = r'^(.+)_(\d+)$'
    dynamic = {}
    for col in columns:
        m = re.match(pattern, col)
        if m:
            base, suffix = m.group(1), int(m.group(2))
            dynamic.setdefault(base, {})[suffix] = col
    return dynamic


def _copy_csv_to_excel(csv_path: str, excel_path: str, sheet_name: str):
    """Append a CSV as a new sheet to an existing Excel file."""
    if not os.path.exists(csv_path):
        logger.warning(f"Step 6: CSV not found, skipping: {csv_path}")
        return
    if not os.path.exists(excel_path):
        logger.warning(f"Step 6: Excel not found, skipping: {excel_path}")
        return

    with open(csv_path, "r", encoding="utf-8") as f:
        header_line = f.readline().strip()
    original_columns = header_line.split(",")

    df = pd.read_csv(csv_path, header=0)
    df.columns = original_columns

    workbook = load_workbook(excel_path)
    if sheet_name in workbook.sheetnames:
        del workbook[sheet_name]

    ws = workbook.create_sheet(title=sheet_name)
    ws.append(original_columns)
    for row in df.itertuples(index=False, name=None):
        ws.append(list(row))

    workbook.save(excel_path)
    logger.info(f"Step 6: Appended '{sheet_name}' to {excel_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def run_step6(city: City, db: Session, results_dir: str) -> dict:
    """Entry point called by pipeline._step6."""
    try:
        from src.contact_formatting import format_contact_data, clean_column_names
    except ImportError as e:
        logger.error(f"Step 6: Cannot import src.contact_formatting: {e}")
        raise ImportError(
            "src/contact_formatting.py not found. "
            "Make sure the src/ folder is present in the backend directory."
        ) from e

    results_path = Path(results_dir)
    city_name    = city.name
    city_dir     = results_path.parent

    # Load CONTACT_CONFIG from city_schema.json
    schema_path = city_dir / "city_schema.json"
    if not schema_path.exists():
        raise FileNotFoundError(
            f"city_schema.json not found. Run step5_support_generate_city_details.py first."
        )
    import json
    with open(str(schema_path)) as f:
        schema = json.load(f)
    contact_config = schema.get("CONTACT_CONFIG", {})

    if not contact_config:
        logger.info("Step 6: No CONTACT_CONFIG in schema — skipping contacts")
        return {"contact_rows": 0}

    input_folder   = results_path / "output" / "final_result"
    output_folder  = results_path / "output" / "final_excel"
    final_folder   = results_path / "output" / "final_output"
    output_folder.mkdir(parents=True, exist_ok=True)

    # Process both sheet types (same as original step6_contacts.py)
    sheet_configs = [
        {
            "sheet_type"   : "additional_records",
            "input_file"   : str(input_folder / f"additional_city_records_for_{city_name}.xlsx"),
            "id_df_path"   : str(output_folder / "Additional_Business_Matched_Records.xlsx"),
            "output_csv"   : str(output_folder / "Additional_Contact_Matched_Records.csv"),
        },
        {
            "sheet_type"   : "business_matched",
            "input_file"   : str(input_folder / f"final_matched_records_for_{city_name}.xlsx"),
            "id_df_path"   : str(output_folder / "Business_Matched_Records.xlsx"),
            "output_csv"   : str(output_folder / "Contact_Matched_Records.csv"),
        },
    ]

    total_rows = 0

    for cfg in sheet_configs:
        sheet_type = cfg["sheet_type"]
        input_file = cfg["input_file"]
        id_df_path = cfg["id_df_path"]
        output_csv = cfg["output_csv"]

        if not os.path.exists(input_file):
            logger.warning(f"Step 6: Input file not found, skipping: {input_file}")
            continue

        logger.info(f"Step 6: Processing {sheet_type}…")

        # Read input
        df = pd.read_excel(input_file) if input_file.endswith('.xlsx') else pd.read_csv(input_file)

        # Build contact output from config (adapted from original process_contacts)
        dynamic_cols = _find_dynamic_columns(df.columns.tolist())
        contact_rows = []

        # Get ID column from Business_Matched_Records
        id_df = pd.read_excel(id_df_path) if os.path.exists(id_df_path) else pd.DataFrame()

        for i, row in df.iterrows():
            for config_key, config in contact_config.items():
                person_col   = config.get("person_col", "")
                title_col    = config.get("title_col", "")
                contact_col  = config.get("contact_col", "")
                contact_type = config.get("contact_type", "[email]")
                roles_col    = config.get("roles_col", "")
                type_val     = config.get("type", "[office]")

                # Get person name
                person_name = ""
                if person_col and person_col in df.columns:
                    person_name = str(row.get(person_col, "")).strip()
                elif person_col and person_col in dynamic_cols:
                    for sfx, col in sorted(dynamic_cols[person_col].items()):
                        v = str(row.get(col, "")).strip()
                        if v and v not in ("", "nan", "None"):
                            person_name = v; break

                if not person_name or person_name in ("nan", "None"):
                    continue

                # Get contact value
                contact_val = ""
                if contact_col and contact_col in df.columns:
                    contact_val = str(row.get(contact_col, "")).strip()
                elif contact_col and contact_col in dynamic_cols:
                    for sfx, col in sorted(dynamic_cols[contact_col].items()):
                        v = str(row.get(col, "")).strip()
                        if v and v not in ("", "nan", "None"):
                            contact_val = v; break

                title_val = ""
                if title_col and title_col in df.columns:
                    title_val = str(row.get(title_col, "")).strip()

                contact_rows.append({
                    "Name"         : person_name,
                    "Title"        : title_val,
                    "Contact"      : contact_val,
                    "Contact_type" : _extract_hardcoded_value(contact_type),
                    "Type"         : _extract_hardcoded_value(type_val),
                    "row_index"    : i,
                })

        if not contact_rows:
            logger.info(f"Step 6: No contact rows found for {sheet_type}")
            # Write empty CSV so downstream doesn't break
            pd.DataFrame(columns=["Name", "Title", "Contact", "Contact_type", "Type"]).to_csv(
                output_csv, index=False)
            continue

        contact_df = pd.DataFrame(contact_rows).drop(columns=["row_index"])

        # Apply format_contact_data (dedup + clean)
        try:
            processed_df = format_contact_data(contact_df, max_workers=None)
            processed_df = clean_column_names(processed_df)
            processed_df = processed_df.replace(to_replace=r'^BLK_\d+$', value='-', regex=True)
        except Exception as e:
            logger.warning(f"Step 6: format_contact_data failed ({e}) — using raw rows")
            processed_df = contact_df

        processed_df.to_csv(output_csv, index=False)
        total_rows += len(processed_df)
        logger.info(f"Step 6: {len(processed_df)} contact rows written to {output_csv}")

    # Append CSVs as sheets to final Excel files
    _copy_csv_to_excel(
        str(output_folder / "Additional_Contact_Matched_Records.csv"),
        str(final_folder   / f"Additional_Matched_Records_Of_{city_name}.xlsx"),
        "Additional_Contact_Matched_Rec",
    )
    _copy_csv_to_excel(
        str(output_folder / "Contact_Matched_Records.csv"),
        str(final_folder   / f"{city_name}_Business_Matched_Records.xlsx"),
        "Contact_Matched_Records",
    )

    logger.info(f"Step 6 complete: {total_rows} contact rows processed")
    return {"contact_rows": total_rows}
