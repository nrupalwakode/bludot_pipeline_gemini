"""
Step 1 — Column Reformat + Merge
=================================
Replaces: step1_2_city_de_duplication.py + step1_3_deduplication_merge.py

step1_2 reformats the deduplicated sheet so that duplicate cluster rows are
pivoted into _1, _2, _3 ... columns per field.

step1_3 merges those columns: Business Name → longest, Address1 → first non-empty,
Phone → unique normalised values joined with comma, everything else → first non-empty.

Input:  results/city_data/manual_dedup_records.xlsx
Output: results/city_data/de_duplication_merged.xlsx
"""

import re
from pathlib import Path

import pandas as pd
import numpy as np


# ── Step 1.2 — Pivot cluster rows into multi-column format ────────────────────

def pivot_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each cluster_id group, pivot all rows into a single row
    with columns Business Name_1, Business Name_2, Address1_1, Address1_2, etc.
    """
    if 'cluster id' not in df.columns:
        # No clusters — just return as-is with _1 suffix columns
        return df

    # Columns that should be pivoted (exclude metadata cols)
    skip_cols = {'cluster id', 'city_index', 'norm_name', 'norm_address',
                 'is_po_box', 'po_box_num', 'street_num', 'street_name',
                 'business_address', 'lsh_bucket'}
    data_cols = [c for c in df.columns if c not in skip_cols]

    results = []
    for cluster_id, group in df.groupby('cluster id'):
        group = group.reset_index(drop=True)
        row = {'cluster id': cluster_id}

        # Keep first city_index as the canonical one
        if 'city_index' in df.columns:
            row['city_index'] = group['city_index'].iloc[0]

        for col in data_cols:
            values = group[col].tolist()
            for i, val in enumerate(values, start=1):
                row[f'{col}_{i}'] = val

        results.append(row)

    return pd.DataFrame(results)


# ── Step 1.3 — Merge multi-column rows into single values ─────────────────────

FIELD_VARIATIONS = {
    'business_name': ['businessname', 'business name'],
    'address1':      ['address1', 'addr1', 'address 1'],
    'address2':      ['address2', 'addr2', 'address 2'],
    'city':          ['city', 'cityname'],
    'state':         ['state', 'statecode'],
    'zipcode':       ['zipcode', 'zip', 'zip code', 'postal code'],
    'website':       ['website', 'web', 'url'],
    'phonenumber':   ['phonenumber', 'phone', 'phone number', 'business phone',
                      'business phone number'],
}


def _norm_col(name: str) -> str:
    return re.sub(r'\s+', '', str(name).lower())


def _normalize_phone(phone) -> str | None:
    if pd.isna(phone) or str(phone).strip() == '':
        return None
    return re.sub(r'\D', '', str(phone))


def _phone_is_dup(p1: str, p2: str) -> bool:
    if p1 == p2:
        return True
    longer, shorter = (p1, p2) if len(p1) >= len(p2) else (p2, p1)
    return longer.endswith(shorter)


def _find_numbered_cols(df: pd.DataFrame) -> dict[str, list[tuple[str, int]]]:
    """
    Returns {field_type: [(col_name, number), ...]} for all _N suffixed columns.
    """
    groups: dict[str, list] = {k: [] for k in FIELD_VARIATIONS}
    pattern = re.compile(r'^(.+)_(\d+)$')
    for col in df.columns:
        m = pattern.match(col)
        if not m:
            continue
        base = _norm_col(m.group(1))
        num  = int(m.group(2))
        for field_type, variations in FIELD_VARIATIONS.items():
            if base in variations:
                groups[field_type].append((col, num))
                break
    for ft in groups:
        groups[ft].sort(key=lambda x: x[1])
    return groups


def merge_numbered_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collapse _1, _2 ... columns into single canonical columns.
    """
    df = df.copy()
    groups = _find_numbered_cols(df)

    # Cast all grouped columns to object to avoid dtype issues
    for cols in groups.values():
        for col, _ in cols:
            df[col] = df[col].astype(object)

    for idx in df.index:

        # Business Name — longest non-empty value
        bn_cols = groups['business_name']
        if bn_cols:
            longest = max(
                (str(df.at[idx, c]) for c, _ in bn_cols
                 if pd.notna(df.at[idx, c]) and str(df.at[idx, c]).strip()),
                key=len, default=''
            )
            df.at[idx, bn_cols[0][0]] = longest

        # First-non-empty fields
        for field in ('address1', 'address2', 'city', 'state', 'zipcode', 'website'):
            cols = groups[field]
            if not cols:
                continue
            first = next(
                (str(df.at[idx, c]) for c, _ in cols
                 if pd.notna(df.at[idx, c]) and str(df.at[idx, c]).strip()),
                ''
            )
            df.at[idx, cols[0][0]] = first

        # Phone — unique normalised numbers
        ph_cols = groups['phonenumber']
        if ph_cols:
            unique_phones, unique_norms = [], []
            for col, _ in ph_cols:
                raw = df.at[idx, col]
                norm = _normalize_phone(raw)
                if norm:
                    if not any(_phone_is_dup(norm, n) for n in unique_norms):
                        unique_phones.append(str(raw).strip())
                        unique_norms.append(norm)
            df.at[idx, ph_cols[0][0]] = ', '.join(unique_phones)

    return df


# ── Public API ────────────────────────────────────────────────────────────────

def run_step1(output_dir: str) -> dict:
    """
    Reads manual_dedup_records.xlsx, pivots clusters, merges columns,
    writes de_duplication_merged.xlsx.

    Returns stats dict.
    """
    city_data_dir = Path(output_dir) / 'city_data'
    input_path  = city_data_dir / 'manual_dedup_records.xlsx'
    output_path = city_data_dir / 'de_duplication_merged.xlsx'

    df = pd.read_excel(str(input_path), dtype=object)
    df = df.fillna('').infer_objects(copy=False)

    # Step 1.2: pivot clusters into wide format
    pivoted = pivot_clusters(df)

    # Step 1.3: merge multi-value columns
    merged = merge_numbered_columns(pivoted)

    merged.to_excel(str(output_path), index=False)

    return {
        'input_records':  len(df),
        'output_records': len(merged),
    }