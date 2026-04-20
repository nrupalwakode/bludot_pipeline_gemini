"""
Step 4.5 — Generate City Schema from DB Column Mappings
========================================================
NO LLM — reads the column mappings saved at city creation from the DB.
The user already mapped columns in the UI (NewCityPage) using the
column mapping tool. This step just converts those mappings into the
city_schema.json format that Step 5 needs.

Mapping types stored in DB:
  business  — city col → standard Bludot Business Profile field (target_col = "Business Name" etc.)
  contact   — city col → contact field (meta has role, contact_type, person_col)
  custom    — city col → bludot custom field (meta has bludot_custom_col)
  skip      — ignore this column

Output: uploads/{city}/city_schema.json
"""

import json
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session

from ..db.models import City, ColumnMapping

logger = logging.getLogger(__name__)


# Fixed Bludot Business Profile field → internal pipeline column name
BP_TO_INTERNAL = {
    "Business Name":               "Name_x",
    "Address1":                    "Address1",
    "Address2":                    "Address2",
    "City":                        "City",
    "State":                       "State",
    "Zipcode":                     "ZipCode",
    "Phonenumber":                 "PhoneNumber",
    "Website":                     "Website",
    "Lat":                         "Lat",
    "Long":                        "Long",
    "DBA Name":                    "DBA Name",
    "Business Operational Status": "Business Operational Status",
    "Country":                     "Country",
}

INTERNAL_TO_UPDATED = {
    'UUID':                        'Id',
    'Name_x':                      'Business Name',
    'Address1':                    'Address1',
    'Address2':                    'Address2',
    'City':                        'City',
    'State':                       'State',
    'ZipCode':                     'Zipcode',
    'PhoneNumber':                 'Phonenumber',
    'Website':                     'Website',
    'Valid-Business':              'is_business',
    'Lat':                         'Lat',
    'Long':                        'Long',
    'DBA Name':                    'DBA Name',
    'Business Operational Status': 'Business Operational Status',
    'Country':                     'Country', # FIXED: Added Country to prevent it from being dropped
}

PIPELINE_COLS = {'UUID', 'Valid-Business'}


def run_step4_5(city: City, db: Session, results_dir: str) -> dict:
    """
    Build city_schema.json from DB column mappings.
    If city_schema.json already exists, skip and return immediately
    (preserves any manual edits made after first generation).
    """
    results_path = Path(results_dir)
    city_dir     = results_path.parent
    schema_path  = city_dir / "city_schema.json"

    if schema_path.exists():
        logger.info("Step 4.5: city_schema.json already exists — skipping rebuild")
        with open(str(schema_path)) as f:
            schema = json.load(f)
        return {
            "schema_exists" : True,
            "mapped_bp"     : len(schema.get("BUSINESS_MATCHED_CITY_COLUMNS", [])),
            "mapped_custom" : len(schema.get("BUSINESS_CUSTOM_MATCHED_CITY_COLUMNS", [])),
            "new_fields"    : len(schema.get("NEW_FIELDS", [])),
        }

    # Load column mappings from DB
    mappings = db.query(ColumnMapping).filter_by(city_id=city.id).all()
    if not mappings:
        raise ValueError(
            f"No column mappings found for city_id={city.id}. "
            "Column mapping must be configured before running this step."
        )

    logger.info(f"Step 4.5: Building schema from {len(mappings)} column mappings in DB")

    # Separate by type
    business_maps = [m for m in mappings if m.mapping_type == "business"]
    contact_maps  = [m for m in mappings if m.mapping_type == "contact"]
    custom_maps   = [m for m in mappings if m.mapping_type == "custom"]
    # skip_maps   = [m for m in mappings if m.mapping_type == "skip"]  # ignored

    # ── Business Profile mapping ──────────────────────────────────────────────
    biz_city   = []  # city col names
    biz_bludot = []  # internal pipeline col names

    for m in business_maps:
        internal = BP_TO_INTERNAL.get(m.target_col)
        if internal:
            biz_city.append(m.source_col)
            biz_bludot.append(internal)
            logger.info(f"  Business: '{m.source_col}' → '{m.target_col}' (internal: '{internal}')")
        else:
            logger.warning(f"  Business: '{m.target_col}' not in BP_TO_INTERNAL — skipped")

    # ── COLUMNS_LIST + COLUMNS_LIST_UPDATED ───────────────────────────────────
    internal_set = set(biz_bludot)
    dyn_cols, dyn_updated = [], []
    for ic, uc in INTERNAL_TO_UPDATED.items():
        if ic in PIPELINE_COLS or ic in internal_set:
            dyn_cols.append(ic)
            dyn_updated.append(uc)

    # ── Custom Data mapping ───────────────────────────────────────────────────
    cust_city   = []
    cust_bludot = []
    new_fields  = []

    # Track all already-assigned source columns so they don't leak into NEW_FIELDS
    assigned_cols = set(biz_city)  

    for m in contact_maps:
        assigned_cols.add(m.source_col)

    for m in custom_maps:
        meta       = m.meta or {}
        bludot_col = meta.get("bludot_custom_col", "")
        if bludot_col:
            cust_city.append(m.source_col)
            cust_bludot.append(bludot_col)
            assigned_cols.add(m.source_col)
            logger.info(f"  Custom matched: '{m.source_col}' → '{bludot_col}'")
        else:
            if m.source_col not in assigned_cols:
                new_fields.append(m.source_col)
                assigned_cols.add(m.source_col)
                logger.info(f"  New field: '{m.source_col}'")

    # ── Contact Config ────────────────────────────────────────────────────────
    contact_config = {}
    idx = 1

    for m in contact_maps:
        meta         = m.meta or {}
        # FIXED: Added fallback 'or' to prevent empty string .startswith() crash
        contact_type = meta.get("contact_type") or "[email]"
        type_val     = meta.get("type") or "[office]"
        person_col   = meta.get("person_col", "")

        person_col_parts = meta.get("person_col_parts", [])
        if person_col_parts:
            person_col_value = person_col_parts 
        elif isinstance(person_col, list):
            person_col_value = person_col # Handles if frontend sends list directly
        else:
            person_col_value = person_col 

        contact_config[f"contact_{idx}"] = {
            "person_col":   person_col_value,
            "title_col":    meta.get("title_col", ""),
            "roles_col":    meta.get("roles_col", ""),
            "contact_col":  m.source_col,
            "contact_type": contact_type if contact_type.startswith("[") else f"[{contact_type}]",
            "type":         type_val if type_val.startswith("[") else f"[{type_val}]",
        }
        logger.info(f"  Contact {idx}: person='{person_col_value}', contact='{m.source_col}', type='{contact_type}'")
        idx += 1

    # ── CITY_NAME_LIST / CITY_ADDRESS_LIST / CITY_PHONE_LIST ─────────────────
    dedupe_path = results_path / "city_data" / "de_duplication_merged.xlsx"
    city_dedupe_cols = set()
    if dedupe_path.exists():
        try:
            ddf = pd.read_excel(str(dedupe_path), nrows=0)
            city_dedupe_cols = set(ddf.columns)
        except Exception as e:
            logger.warning(f"Step 4.5: Could not read dedupe file: {e}")

    def _city_col_1(source_col: str) -> str:
        """FIXED: Safely locates the exact column name in the deduplicated file."""
        if not city_dedupe_cols:
            return source_col
            
        col_1 = f"{source_col}_1"
        
        # 1. Exact matches
        if col_1 in city_dedupe_cols: return col_1
        if source_col in city_dedupe_cols: return source_col
        
        # 2. Case-insensitive matches
        for c in city_dedupe_cols:
            if c.lower() == col_1.lower(): return c
        for c in city_dedupe_cols:
            if c.lower() == source_col.lower(): return c
            
        return source_col

    name_list, addr_list, phone_list = [], [], []
    for m in business_maps:
        if m.target_col == "Business Name" or m.target_col == "DBA Name":
            name_list.append(_city_col_1(m.source_col))
        elif m.target_col == "Address1":
            addr_list.append(_city_col_1(m.source_col))
        elif m.target_col == "Phonenumber":
            phone_list.append(_city_col_1(m.source_col))

    # ── Build final schema ────────────────────────────────────────────────────
    schema = {
        "BUSINESS_MATCHED_CITY_COLUMNS":          biz_city,
        "BUSINESS_MATCHED_BLUDOT_COLUMNS":        biz_bludot,
        "COLUMNS_LIST":                           dyn_cols,
        "COLUMNS_LIST_UPDATED":                   dyn_updated,
        "BUSINESS_CUSTOM_MATCHED_CITY_COLUMNS":   cust_city,
        "BUSINESS_CUSTOM_MATCHED_BLUDOT_COLUMNS": cust_bludot,
        "NEW_FIELDS":                             new_fields,
        "CONTACT_MATCHED_CITY_COLUMNS":           [],
        "CONTACT_MATCHED_BLUDOT_COLUMNS":         [],
        "CONTACT_CONFIG":                         contact_config,
        "CITY_NAME_LIST":                         name_list,
        "CITY_ADDRESS_LIST":                      addr_list,
        "CITY_PHONE_LIST":                        phone_list,
        "BLUDOT_OR_USER":                         "bludot",
        "COUNTRY_STATE_MAPPING":                  {},
    }

    # Write schema
    schema_path.parent.mkdir(parents=True, exist_ok=True)
    with open(str(schema_path), 'w') as f:
        json.dump(schema, f, indent=4)

    logger.info(f"Step 4.5: Wrote city_schema.json → {schema_path}")
    logger.info(f"  Business mapped : {list(zip(biz_city, biz_bludot))}")
    logger.info(f"  Custom matched  : {list(zip(cust_city, cust_bludot))}")
    logger.info(f"  NEW_FIELDS      : {new_fields}")
    logger.info(f"  Contact groups  : {list(contact_config.keys())}")

    return {
        "schema_path"   : str(schema_path),
        "schema_exists" : False,
        "mapped_bp"     : len(biz_city),
        "mapped_custom" : len(cust_city),
        "new_fields"    : len(new_fields),
        "contact_groups": len(contact_config),
    }