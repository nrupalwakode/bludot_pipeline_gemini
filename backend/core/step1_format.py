"""
Step 1 — Reformat Columns + Merge
==================================
Replaces: step1.2_city_de_duplication.py + step1.3_deduplication_merge.py

Two sub-steps:
  1a. Pivot wide format — one row per cluster, numbered columns merged
      (Business Name_1, Business Name_2 → longest non-empty value)
  1b. merge_columns() — phone dedup, address/zip/website consolidation

Output: results/city_data/de_duplication_merged.xlsx  (same name as old pipeline)
"""

import re
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


# ── Phone helpers (from step1.3) ──────────────────────────────────────────────

def _normalize_phone(phone) -> str | None:
    if pd.isna(phone) or str(phone).strip() == '':
        return None
    return re.sub(r'\D', '', str(phone))


def _is_phone_duplicate(p1: str, p2: str) -> bool:
    if not p1 or not p2:
        return False
    if p1 == p2:
        return True
    longer, shorter = (p1, p2) if len(p1) >= len(p2) else (p2, p1)
    return longer.endswith(shorter)


def _norm_col(name: str) -> str:
    return re.sub(r'\s+', '', str(name).lower())


def _extract_field_and_number(col_name: str) -> tuple[str | None, int | None]:
    m = re.match(r'^(.+?)_(\d+)$', col_name)
    if m:
        return m.group(1), int(m.group(2))
    return None, None


def _find_column_groups(df: pd.DataFrame, field_variations: dict) -> dict:
    groups = {f: [] for f in field_variations}
    for col in df.columns:
        base, num = _extract_field_and_number(col)
        if base and num:
            nb = _norm_col(base)
            for ftype, variants in field_variations.items():
                if nb in variants:
                    groups[ftype].append((col, num))
                    break
    for ftype in groups:
        groups[ftype].sort(key=lambda x: x[1])
    return groups


# ── Core merge logic (from step1.3) ──────────────────────────────────────────

def merge_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge numbered columns per the original step1.3 rules:
      Business Name → longest non-empty value across _1, _2, _3 …
      Address1/2, City, State, Zipcode, Website → first non-empty
      Phonenumber → unique deduplicated numbers joined with ", "
    """
    field_variations = {
        'business_name': [_norm_col('Business Name')],
        'address1':  [_norm_col('Address1')],
        'address2':  [_norm_col('Address2'), _norm_col('Address 2'),
                      _norm_col('addr2'), _norm_col('address 2')],
        'city':      [_norm_col('City')],
        'state':     [_norm_col('State')],
        'zipcode':   [_norm_col('Zipcode'), _norm_col('Zip Code'), _norm_col('Zip'),
                      _norm_col('zip'), _norm_col('zipcode'), _norm_col('Postal Code')],
        'website':   [_norm_col('Website')],
        'phonenumber': [_norm_col('Phonenumber'), _norm_col('Phone number'),
                        _norm_col('Phone Number'), _norm_col('Business Phone Number'),
                        _norm_col('Business Phone')],
    }

    groups = _find_column_groups(df, field_variations)

    for idx in df.index:
        # Business Name → longest
        biz_cols = groups['business_name']
        if biz_cols:
            longest = ''
            for col, _ in biz_cols:
                v = df.at[idx, col]
                if pd.notna(v) and str(v).strip() and len(str(v)) > len(longest):
                    longest = str(v)
            df.at[idx, biz_cols[0][0]] = longest

        # First non-empty fields
        for ftype in ('address1', 'address2', 'city', 'state', 'zipcode', 'website'):
            cols = groups[ftype]
            if cols:
                first = ''
                for col, _ in cols:
                    v = df.at[idx, col]
                    if pd.notna(v) and str(v).strip():
                        first = str(v)
                        break
                df.at[idx, cols[0][0]] = first

        # Phonenumber → unique deduplicated
        phone_cols = groups['phonenumber']
        if phone_cols:
            unique_phones, norm_phones = [], []
            for col, _ in phone_cols:
                v = df.at[idx, col]
                norm = _normalize_phone(v)
                if norm:
                    is_dup = any(_is_phone_duplicate(norm, ep) for ep in norm_phones)
                    if not is_dup:
                        unique_phones.append(str(v).strip())
                        norm_phones.append(norm)
            df.at[idx, phone_cols[0][0]] = ', '.join(unique_phones) if unique_phones else ''

    return df


# ── Pivot / reformat step ─────────────────────────────────────────────────────

def _pivot_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """
    The dedup output has one row per record with a 'cluster id' column.
    Keep one representative row per cluster (the one with the most data),
    then rename columns to _1 suffix so merge_columns() can process them.

    If the df already has _1 columns (already pivoted), skip pivoting.
    """
    if 'cluster id' not in df.columns:
        return df

    # If already in wide format with numbered columns, skip pivot
    has_numbered = any(re.match(r'.+_\d+$', str(c)) for c in df.columns)
    if has_numbered:
        return df

    # Group by cluster — keep row with fewest NaN (most data)
    def _pick_best(group):
        return group.loc[group.isnull().sum(axis=1).idxmin()]

    merged = df.groupby('cluster id', sort=False).apply(_pick_best).reset_index(drop=True)
    return merged


# ── Public API ────────────────────────────────────────────────────────────────

def run_step1(results_dir: str) -> dict:
    """
    Entry point called by pipeline.py

    Reads:  results/city_data/manual_dedup_records.xlsx
    Writes: results/city_data/de_duplication_merged.xlsx

    Returns stats dict: {input_records, output_records}
    """
    results_path = Path(results_dir)
    city_data_dir = results_path / 'city_data'
    city_data_dir.mkdir(parents=True, exist_ok=True)

    input_file  = city_data_dir / 'manual_dedup_records.xlsx'
    output_file = city_data_dir / 'de_duplication_merged.xlsx'

    if not input_file.exists():
        logger.warning(f"step1: {input_file} not found — skipping")
        return {'input_records': 0, 'output_records': 0}

    logger.info(f"step1: Reading {input_file}")
    df = pd.read_excel(str(input_file))
    input_count = len(df)
    logger.info(f"step1: {input_count} rows, {len(df.columns)} columns")

    # 1a. Pivot clusters → one row per cluster
    df = _pivot_clusters(df)
    logger.info(f"step1: After pivot: {len(df)} rows")

    # 1b. Fill NaN, infer objects (suppress FutureWarning)
    pd.set_option('future.no_silent_downcasting', True)
    df = df.fillna('').infer_objects(copy=False)

    # 1c. Merge numbered columns
    df = merge_columns(df)
    logger.info(f"step1: After merge: {len(df)} rows")

    # 1d. Add city_index if missing (needed by matching step)
    if 'city_index' not in df.columns:
        df['city_index'] = range(len(df))

    df.to_excel(str(output_file), index=False, sheet_name='De_Duplication_Merged')
    logger.info(f"step1: Saved → {output_file}")

    return {'input_records': input_count, 'output_records': len(df)}
