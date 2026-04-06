"""
Step 0 — Deduplication + Bludot Concatenation
==============================================
Replaces: step0_manual_dedup_records_LSH.py + bludot_concat.py

Does two things:
1. Reads the city sheet, applies LSH-based deduplication, writes
   results/city_data/manual_dedup_records.xlsx

2. Reads the bludot export (Business + Custom + Contact sheets),
   merges them on UUID, writes results/bludot_data/bludot_concatenated_records.xlsx

All column mapping comes from the DB — no city_details.py needed.
"""

import datetime
import logging
import os
import re
from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd
from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy.orm import Session

from ..db.models import City, ColumnMapping

logger = logging.getLogger(__name__)


# ── Date formatting helper (from bludot_concat.py) ───────────────────────────

def date_formatting(df: pd.DataFrame) -> pd.DataFrame:
    """Convert datetime columns to MM/DD/YYYY strings."""
    df = df.replace('', np.nan)
    date_cols = [
        col for col in df.columns
        if df[col].dropna().shape[0] > 0
        and isinstance(df[col].dropna().iloc[0], (datetime.datetime,))
    ]
    df = df.fillna('')
    for col in date_cols:
        df[col] = df[col].apply(
            lambda v: v.strftime("%m/%d/%Y") if not isinstance(v, str) else v
        )
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')
    return df


# ── Bludot concatenation (from bludot_concat.py) ─────────────────────────────

def concatenate_bludot_sheets(
    bludot_path: str,
    business_sheet: str = "Business Record",
    custom_sheet: str   = "Custom Data",
    contact_sheet: str  = "Contact_Details",
) -> pd.DataFrame:
    """
    Read Business, Custom, Contact sheets from the bludot export Excel file,
    merge them on UUID, return a single concatenated DataFrame.
    """
    xl = pd.ExcelFile(bludot_path)
    available = xl.sheet_names

    # Business sheet (required)
    biz_name = next((s for s in available if business_sheet.lower() in s.lower()), None)
    if not biz_name:
        biz_name = available[0]
    business_df = date_formatting(pd.read_excel(bludot_path, sheet_name=biz_name, dtype=object))

    merged = business_df.copy()

    # Custom sheet (optional)
    cust_name = next((s for s in available if custom_sheet.lower() in s.lower()), None)
    if cust_name:
        custom_df = date_formatting(pd.read_excel(bludot_path, sheet_name=cust_name, dtype=object))
        custom_df = custom_df.fillna('')
        if 'UUID' in business_df.columns and 'Custom Data Name' in custom_df.columns:
            merged = pd.merge(merged, custom_df, left_on='UUID', right_on='Custom Data Name', how='left')
        elif 'UUID' in business_df.columns and 'UUID' in custom_df.columns:
            merged = pd.merge(merged, custom_df, on='UUID', how='left', suffixes=('', '_custom'))

    # Contact sheet (optional)
    cont_name = next((s for s in available if contact_sheet.lower() in s.lower()), None)
    if cont_name:
        contact_df = pd.read_excel(bludot_path, sheet_name=cont_name, dtype=object)
        # Fix duplicate column names that pandas renames with .1 .2 suffixes
        contact_df.columns = [c.split('.')[0] for c in contact_df.columns]
        contact_df = date_formatting(contact_df)
        contact_df = contact_df.fillna('')
        id_col = next((c for c in contact_df.columns if c == 'ID'), None)
        uuid_col = next((c for c in contact_df.columns if c == 'UUID'), None)
        if id_col and 'UUID' in merged.columns:
            merged = pd.merge(merged, contact_df, left_on='UUID', right_on='ID', how='left', suffixes=('', '_contact'))
        elif uuid_col and 'UUID' in merged.columns:
            merged = pd.merge(merged, contact_df, on='UUID', how='left', suffixes=('', '_contact'))

    merged = merged.fillna('')
    merged['bludot_index'] = range(len(merged))
    return merged


# ── City sheet preparation ────────────────────────────────────────────────────

def prepare_city_sheet(raw_path: str, db: Session, city_id: int) -> pd.DataFrame:
    """
    Read the city sheet and apply the saved column mapping so that
    the output always has Business Name, Address1, etc.
    """
    ext = Path(raw_path).suffix.lower()
    if ext in ('.xlsx', '.xls'):
        df = pd.read_excel(raw_path, dtype=object)
    else:
        df = pd.read_csv(raw_path, dtype=object)

    df = date_formatting(df)
    df = df.fillna('')

    # Apply column mapping from DB
    mappings = db.query(ColumnMapping).filter_by(city_id=city_id, mapping_type='business').all()
    rename = {m.source_col: m.target_col for m in mappings}
    df = df.rename(columns=rename)

    # Ensure required columns exist
    for col in ('Business Name', 'Address1'):
        if col not in df.columns:
            df[col] = ''

    df['city_index'] = range(len(df))
    return df


# ── LSH Deduplication (from step0_manual_dedup_records_LSH.py) ───────────────

np.random.seed(42)


class BusinessDeduplicator:

    def __init__(self, name_threshold=83, address_threshold=70, po_box_threshold=95):
        self.name_threshold    = name_threshold
        self.address_threshold = address_threshold
        self.po_box_threshold  = po_box_threshold

    # ── Normalization ─────────────────────────────────────────────────────────

    def _normalize_name(self, name: str) -> str:
        if not name or name == '-':
            return ''
        name = name.lower()
        for ent in [' llc', ' inc', ' corporation', ' corp', ' company', ' co',
                    ' ltd', ' limited', ' pllc', ' office', ' group', ' associates',
                    ' consulting', ' joint venture']:
            name = name.replace(ent, '')
        name = name.replace('&', 'and')
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'[^\w\s]', '', name)
        for pfx in ['mr ', 'mrs ', 'ms ', 'dr ', 'prof ', 'miss ']:
            if name.startswith(pfx):
                name = name[len(pfx):]
        for sfx in [' jr', ' sr', ' ii', ' iii', ' iv']:
            if name.endswith(sfx):
                name = name[:-len(sfx)]
        return name.strip()

    def _normalize_address(self, address: str) -> str:
        if not address or address == '-':
            return ''
        address = address.upper().strip()

        # Expand abbreviations to standard form
        abbrevs = {
            r'\bSTREET\b': 'ST', r'\bAVENUE\b': 'AVE', r'\bBOULEVARD\b': 'BLVD',
            r'\bDRIVE\b': 'DR', r'\bROAD\b': 'RD', r'\bLANE\b': 'LN',
            r'\bCOURT\b': 'CT', r'\bPLACE\b': 'PL', r'\bCIRCLE\b': 'CIR',
            r'\bTERRACE\b': 'TER', r'\bPARKWAY\b': 'PKWY', r'\bHIGHWAY\b': 'HWY',
            r'\bSQUARE\b': 'SQ', r'\bSUITE\b': 'STE',
            r'\bNORTH\b': 'N', r'\bSOUTH\b': 'S', r'\bEAST\b': 'E', r'\bWEST\b': 'W',
            r'\bNORTHEAST\b': 'NE', r'\bNORTHWEST\b': 'NW',
            r'\bSOUTHEAST\b': 'SE', r'\bSOUTHWEST\b': 'SW',
        }
        for pattern, repl in abbrevs.items():
            address = re.sub(pattern, repl, address)

        # Strip unit/suite/apt suffixes BEFORE punctuation cleanup
        # Handles: # 1/2, #B, STE A, APT 2B, UNIT 1, FL 3, PMB 277, etc.
        address = re.sub(
            r'(\s+#\s*\S+.*|\s+#$|\s+(?:STE|SUITE|APT|APARTMENT|UNIT|FL|FLOOR|'
            r'BLDG|BUILDING|RM|ROOM|LOT|TRLR|TRAILER|PMB|BOX|DEPT|MSC)\b.*)$',
            '', address, flags=re.IGNORECASE
        ).strip()

        # Strip trailing bare number/range unit designators e.g. '1-27'
        address = re.sub(r'\s+\d+(?:-\d+)?$', '', address).strip()

        # Remove remaining punctuation (keep hyphens inside tokens)
        address = re.sub(r'[^\w\s-]', ' ', address)
        # Collapse street number ranges to first number: 2710-3040 → 2710
        address = re.sub(r'\b(\d+)-\d+\b', r'\1', address)
        address = re.sub(r'\s+', ' ', address)
        return address.strip().lower()

    def _is_po_box(self, address: str) -> bool:
        return bool(re.search(r'p\.?\s*o\.?\s*box', address, re.IGNORECASE))

    def _extract_po_box_num(self, address: str) -> str:
        m = re.search(r'p\.?\s*o\.?\s*box\s*#?\s*(\d+)', address, re.IGNORECASE)
        return m.group(1) if m else ''

    def _extract_street_num(self, address: str) -> str:
        m = re.match(r'^\s*(\d+)', address)
        return m.group(1) if m else ''

    def _extract_street_name(self, address: str) -> str:
        """Extract base street name with all unit suffixes stripped."""
        normed = self._normalize_address(address)
        return re.sub(r'^\s*\d+\s*', '', normed).strip()

    # ── Preprocessing ─────────────────────────────────────────────────────────

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df['Business Name'] = df['Business Name'].fillna('-').astype(str)
        df['Address1']      = df['Address1'].fillna('-').astype(str)
        df['norm_name']     = df['Business Name'].apply(self._normalize_name)
        df['is_po_box']     = df['Address1'].apply(self._is_po_box)
        df['po_box_num']    = df['Address1'].apply(self._extract_po_box_num)
        df['street_num']    = df.apply(
            lambda r: self._extract_street_num(r['Address1']) if not r['is_po_box'] else '', axis=1)
        df['street_name']   = df.apply(
            lambda r: self._extract_street_name(r['Address1']) if not r['is_po_box'] else '', axis=1)
        df['norm_address']  = df['Address1'].apply(self._normalize_address)
        df['business_address'] = df['norm_name'] + '_' + df['norm_address']
        return df

    # ── LSH + TF-IDF ─────────────────────────────────────────────────────────

    def _tfidf_vectors(self, df: pd.DataFrame):
        corpus = df['business_address'].tolist()
        vectorizer = TfidfVectorizer(analyzer='char', ngram_range=(2, 3), min_df=1)
        return vectorizer.fit_transform(corpus)

    def _apply_lsh(self, n_planes: int, tfidf_matrix, df: pd.DataFrame) -> pd.DataFrame:
        n_samples, n_features = tfidf_matrix.shape
        planes = np.random.randn(n_features, n_planes)
        projections = (tfidf_matrix @ planes > 0)

        if hasattr(projections, "toarray"):
            projections = projections.toarray()

        projections = projections.astype(int)
        bucket_keys = [''.join(map(str, row)) for row in projections]
        df = df.copy()
        df['lsh_bucket'] = bucket_keys

        buckets: dict[str, list[int]] = {}
        for i, key in enumerate(bucket_keys):
            buckets.setdefault(key, []).append(i)

        G = nx.Graph()
        G.add_nodes_from(range(n_samples))

        for bucket_indices in buckets.values():
            for i in range(len(bucket_indices)):
                for j in range(i + 1, len(bucket_indices)):
                    ri = df.iloc[bucket_indices[i]]
                    rj = df.iloc[bucket_indices[j]]
                    if self.compute_similarity(ri, rj):
                        G.add_edge(bucket_indices[i], bucket_indices[j])

        cluster_id = {}
        for cluster_num, component in enumerate(nx.connected_components(G)):
            for node in component:
                cluster_id[node] = cluster_num

        df['cluster id'] = [cluster_id.get(i, i) for i in range(n_samples)]
        return df

    def compute_similarity(self, r1, r2) -> bool:
        name_sim = max(
            fuzz.ratio(r1['norm_name'], r2['norm_name']),
            fuzz.token_sort_ratio(r1['norm_name'], r2['norm_name'])
        )
        exact_name = r1['Business Name'].lower() == r2['Business Name'].lower()
        blank1 = r1['Address1'] in ('-', '')
        blank2 = r2['Address1'] in ('-', '')

        if blank1 or blank2:
            return name_sim >= 83 or exact_name

        if r1['is_po_box'] != r2['is_po_box']:
            return exact_name or name_sim >= 83

        if r1['is_po_box'] and r2['is_po_box']:
            if r1['po_box_num'] and r2['po_box_num']:
                return r1['po_box_num'] == r2['po_box_num'] and name_sim >= 83
            return name_sim >= 83

        # Both have street addresses
        sn1, sn2 = r1['street_num'], r2['street_num']
        if sn1 and sn2:
            if fuzz.ratio(sn1, sn2) < 100:
                return False
            return name_sim >= 85 and fuzz.token_sort_ratio(r1['street_name'], r2['street_name']) >= 80
        if sn1 or sn2:
            return (fuzz.token_sort_ratio(r1['street_name'], r2['street_name']) >= 80
                    and name_sim >= 85)
        return (fuzz.token_sort_ratio(r1['street_name'], r2['street_name']) >= 90
                and name_sim >= 90)

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        preprocessed = self.preprocess(df)
        tfidf = self._tfidf_vectors(preprocessed)
        return self._apply_lsh(3, tfidf, preprocessed)


# ── DB ingestion helpers ──────────────────────────────────────────────────────

def _ingest_city_records(df: pd.DataFrame, city_id: int, db: Session):
    """Load deduplicated city records into the DB (clears existing first)."""
    from ..db.models import CityRecord
    db.query(CityRecord).filter_by(city_id=city_id).delete()

    # After step1 merge the canonical columns are Business Name_1 / Address1_1
    # but at step0 they are still Business Name / Address1
    def _get(row, *keys):
        for k in keys:
            v = row.get(k, '')
            if v and str(v).strip() not in ('', '-', 'nan'):
                return str(v).strip()
        return ''

    for _, row in df.iterrows():
        row_dict = {k: ('' if str(v) in ('nan', 'NaT') else str(v))
                    for k, v in row.to_dict().items()}
        biz_name = _get(row_dict, 'Business Name', 'Business Name_1')
        address1 = _get(row_dict, 'Address1', 'Address1_1')
        cluster_id = row_dict.get('cluster id', None)

        db.add(CityRecord(
            city_id       = city_id,
            city_index    = int(row_dict.get('city_index', 0) or 0),
            business_name = biz_name,
            address1      = address1,
            cluster_id    = int(float(cluster_id)) if cluster_id not in (None, '', 'nan') else None,
            raw_data      = row_dict,
        ))
    db.commit()


def _ingest_bludot_records(df: pd.DataFrame, city_id: int, db: Session):
    """Load concatenated bludot records into the DB (clears existing first)."""
    from ..db.models import BludotRecord
    db.query(BludotRecord).filter_by(city_id=city_id).delete()

    name_col    = next((c for c in df.columns if c in ('Name', 'Business Name', 'name')), None)
    address_col = next((c for c in df.columns if c in ('Address1', 'address1', 'Address 1')), None)
    uuid_col    = next((c for c in df.columns if c in ('UUID', 'uuid')), None)

    for _, row in df.iterrows():
        row_dict = {k: ('' if str(v) in ('nan', 'NaT') else str(v))
                    for k, v in row.to_dict().items()}
        db.add(BludotRecord(
            city_id      = city_id,
            bludot_index = int(row_dict.get('bludot_index', 0) or 0),
            uuid         = row_dict.get(uuid_col, '') if uuid_col else '',
            name         = row_dict.get(name_col, '') if name_col else '',
            address1     = row_dict.get(address_col, '') if address_col else '',
            raw_data     = row_dict,
        ))
    db.commit()


def _bulk_insert(db: Session, objects: list) -> None:
    """Bulk insert without autoflush per row — much faster than individual db.add()."""
    db.bulk_save_objects(objects)
    db.commit()


# ── 4-step dedup post-processing ─────────────────────────────────────────────

def _step2_verify_clusters_with_llm(df: pd.DataFrame, city_id: int, db: Session) -> int:
    """
    Step 2: Verify intra-cluster pairs using rules + LLM for ambiguous cases.

    For each multi-record cluster, check every pair:

    AUTO-MERGE (no LLM):
      - Exact same name + exact same normalized address → definite duplicate

    AUTO-SPLIT (no LLM):
      - Same cluster but different street numbers → wrong grouping, split

    LLM DECIDES:
      - Same name + one/both addresses blank → LLM judges
      - Same name + similar address (unit suffix difference) → LLM judges

    IF LLM QUOTA EXHAUSTED → send to human review (cluster review UI)

    Returns count of pairs processed.
    """
    from rapidfuzz import fuzz
    from ..db.models import DedupReviewPair, CityRecord
    from ..core.llm_judge import judge_dedup_pairs, DedupPair, has_api_key

    if 'cluster id' not in df.columns:
        return 0

    records = df[['city_index', 'cluster id', 'Business Name', 'Address1',
                  'norm_name', 'norm_address', 'street_num']].fillna('').to_dict('records')

    cluster_groups: dict = {}
    for row in records:
        cluster_groups.setdefault(row['cluster id'], []).append(row)

    auto_merged  = 0
    auto_split   = []   # (idx_b, new_cluster) pairs to split
    llm_pairs    = []   # DedupPair objects to send to LLM
    human_pairs  = []   # DedupReviewPair rows for direct human review
    seen: set[tuple] = set()
    max_cluster = int(df['cluster id'].max()) if 'cluster id' in df.columns else 0

    for cid, group in cluster_groups.items():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                key = (min(int(a['city_index']), int(b['city_index'])),
                       max(int(a['city_index']), int(b['city_index'])))
                if key in seen:
                    continue
                seen.add(key)

                addr_a = str(a['Address1']).strip()
                addr_b = str(b['Address1']).strip()
                addr_a_blank = addr_a in ('', '-', 'nan')
                addr_b_blank = addr_b in ('', '-', 'nan')
                sn_a = str(a['street_num']).strip()
                sn_b = str(b['street_num']).strip()
                norm_a = str(a['norm_address']).strip()
                norm_b = str(b['norm_address']).strip()
                name_sim = fuzz.token_sort_ratio(a['norm_name'], b['norm_name']) / 100.0

                # ── AUTO-MERGE: exact same name + same normalized address ──────
                if norm_a and norm_b and norm_a == norm_b and name_sim >= 0.95:
                    auto_merged += 1
                    continue  # already in same cluster, nothing to do

                # ── AUTO-SPLIT: both street numbers present and different ───────
                if sn_a and sn_b and sn_a != sn_b:
                    max_cluster += 1
                    auto_split.append((int(b['city_index']), max_cluster))
                    continue

                # ── LLM: one/both addresses blank OR similar address (unit diff) ─
                addr_sim = fuzz.token_sort_ratio(norm_a, norm_b) if norm_a and norm_b else 0
                needs_llm = (
                    addr_a_blank or addr_b_blank  # blank address
                    or (addr_sim >= 70 and name_sim >= 0.85)  # similar address (unit suffix diff)
                )

                if needs_llm:
                    llm_pairs.append(DedupPair(
                        pair_id       = f"{key[0]}_{key[1]}",
                        index_a       = key[0],
                        index_b       = key[1],
                        name_a        = a['Business Name'],
                        address_a     = addr_a,
                        name_b        = b['Business Name'],
                        address_b     = addr_b,
                        similarity    = name_sim,
                        intra_cluster = True,
                    ))

    # Apply auto-splits to DB
    if auto_split:
        for idx_b, new_cid in auto_split:
            rec = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec:
                rec.cluster_id = new_cid
        db.commit()
        logger.info(f"Step 0.2: Auto-split {len(auto_split)} wrong cluster assignments")

    if not llm_pairs:
        logger.info(f"Step 0.2: {auto_merged} auto-merged, {len(auto_split)} auto-split, 0 LLM pairs")
        return auto_merged + len(auto_split)

    # Send ambiguous pairs to LLM in one batched call
    logger.info(f"Step 0.2: Sending {len(llm_pairs)} ambiguous intra-cluster pairs to LLM...")
    results = judge_dedup_pairs(llm_pairs)

    new_db_rows = []
    for res in results:
        p = next((x for x in llm_pairs if x.pair_id == res['pair_id']), None)
        if not p:
            continue

        decision = res.get('decision', 'UNCERTAIN')

        if decision == 'NOT_DUPLICATE':
            # LLM says wrong cluster — split
            max_cluster += 1
            rec = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_b).first()
            if rec:
                rec.cluster_id = max_cluster

        elif decision == 'UNCERTAIN':
            # LLM quota exhausted or unsure → human review
            new_db_rows.append(DedupReviewPair(
                city_id       = city_id,
                index_a       = p.index_a,
                index_b       = p.index_b,
                name_a        = p.name_a,
                address_a     = p.address_a,
                name_b        = p.name_b,
                address_b     = p.address_b,
                similarity    = p.similarity,
                llm_reason    = res.get('reason', 'LLM uncertain — needs human review'),
                decision      = 'UNCERTAIN',
                intra_cluster = True,
            ))
        # DUPLICATE → already in same cluster, nothing to do

    if new_db_rows:
        _bulk_insert(db, new_db_rows)

    db.commit()
    total = auto_merged + len(auto_split) + len(results)
    logger.info(f"Step 0.2 done: {auto_merged} auto-merged, {len(auto_split)} auto-split, "
                f"{len(results)} LLM judged, {len(new_db_rows)} sent to human review")
    return total


def _step3_cross_cluster_scan(df: pd.DataFrame,
                               similarity_threshold: float = 0.92) -> list[dict]:
    """
    Step 3 of dedup: find cross-cluster near-misses that are ACTUALLY
    potential duplicates.

    A pair is only flagged if:
      1. Name similarity >= threshold (0.92 — near exact)
      AND one of:
      2a. At least one address is blank (can't compare, send to review)
      2b. Address similarity >= 0.80 (addresses are also similar)

    This prevents flagging KWOK FAN at 3213 OAK CT vs 3125 PERSIMMON ST
    — same name, completely different addresses = different businesses.

    Uses np.nonzero to avoid O(n²) Python loop.
    """
    from rapidfuzz import process, fuzz
    import numpy as np

    if 'cluster id' not in df.columns or 'norm_name' not in df.columns:
        return []

    records = df[['city_index', 'cluster id', 'Business Name',
                  'Address1', 'norm_name', 'norm_address']].fillna('').reset_index(drop=True)

    names       = records['norm_name'].tolist()
    addresses   = records['norm_address'].tolist()
    cluster_ids = records['cluster id'].tolist()
    raw_addrs   = records['Address1'].tolist()

    # Vectorised name similarity — fast C implementation
    name_scores = process.cdist(names, names, scorer=fuzz.token_sort_ratio,
                                score_cutoff=int(similarity_threshold * 100))

    # np.nonzero on upper triangle — no Python O(n²) loop
    rows_idx, cols_idx = np.nonzero(np.triu(name_scores, k=1))
    cluster_arr = np.array(cluster_ids)

    near_misses = []
    for i, j in zip(rows_idx.tolist(), cols_idx.tolist()):
        # Skip same cluster — handled by step 2
        if cluster_arr[i] == cluster_arr[j]:
            continue

        addr_a = str(raw_addrs[i]).strip()
        addr_b = str(raw_addrs[j]).strip()
        addr_a_blank = addr_a in ('', '-', 'nan')
        addr_b_blank = addr_b in ('', '-', 'nan')

        if addr_a_blank or addr_b_blank:
            # One address missing — can't use address to reject, flag for review
            pass
        else:
            # Both addresses present — check address similarity
            addr_sim = fuzz.token_sort_ratio(addresses[i], addresses[j])
            if addr_sim < 80:
                # Addresses are clearly different → different locations → skip
                continue

        near_misses.append({
            'index_a'   : int(records.at[i, 'city_index']),
            'index_b'   : int(records.at[j, 'city_index']),
            'name_a'    : records.at[i, 'Business Name'],
            'address_a' : records.at[i, 'Address1'],
            'name_b'    : records.at[j, 'Business Name'],
            'address_b' : records.at[j, 'Address1'],
            'similarity': float(name_scores[i, j]) / 100.0,
        })

    return near_misses


def _step4_verify_near_misses_with_llm(near_misses: list[dict],
                                       city_id: int, db: Session) -> int:
    """
    Step 4: Verify cross-cluster near-misses using rules + LLM.

    Each near-miss already passed the step3 filter (name ≥ 92% AND
    address blank or similar). Now decide:

    AUTO-MERGE (no LLM):
      - Exact same normalized address → same business, merge clusters

    LLM DECIDES:
      - One/both addresses blank → LLM judges by name only
      - Similar address (unit suffix difference) → LLM judges

    IF LLM QUOTA EXHAUSTED → send to human review (cluster review UI)

    Returns count of pairs stored/processed.
    """
    from ..db.models import DedupReviewPair, CityRecord
    from ..core.llm_judge import judge_dedup_pairs, DedupPair
    from rapidfuzz import fuzz

    if not near_misses:
        return 0

    db.query(DedupReviewPair).filter_by(city_id=city_id, intra_cluster=False).delete()
    db.commit()

    # Build normalized address lookup from the df (use norm_address from near_miss dict)
    auto_merged = []
    llm_pairs   = []

    for m in near_misses:
        addr_a = str(m.get('address_a', '')).strip()
        addr_b = str(m.get('address_b', '')).strip()
        addr_a_blank = addr_a in ('', '-', 'nan')
        addr_b_blank = addr_b in ('', '-', 'nan')

        # Normalize for comparison
        def _norm(a):
            import re
            a = a.upper()
            a = re.sub(r'(\s+#\s*\S+.*|\s+(?:STE|APT|UNIT|FL|BLDG|PMB|SUITE)\b.*)$',
                       '', a, flags=re.IGNORECASE).strip()
            a = re.sub(r'[^\w\s]', ' ', a)
            return re.sub(r'\s+', ' ', a).strip().lower()

        norm_a = _norm(addr_a) if not addr_a_blank else ''
        norm_b = _norm(addr_b) if not addr_b_blank else ''

        # AUTO-MERGE: same normalized address (after stripping unit suffixes)
        if norm_a and norm_b and norm_a == norm_b:
            auto_merged.append((m['index_a'], m['index_b']))
            continue

        # LLM: blank addresses or similar addresses (unit diff)
        llm_pairs.append(DedupPair(
            pair_id       = f"{m['index_a']}_{m['index_b']}",
            index_a       = m['index_a'],
            index_b       = m['index_b'],
            name_a        = m['name_a'],
            address_a     = addr_a,
            name_b        = m['name_b'],
            address_b     = addr_b,
            similarity    = m['similarity'],
            intra_cluster = False,
        ))

    # Apply auto-merges
    if auto_merged:
        for idx_a, idx_b in auto_merged:
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_a).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec_a and rec_b:
                merged = min(rec_a.cluster_id or rec_a.id, rec_b.cluster_id or rec_b.id)
                rec_a.cluster_id = merged
                rec_b.cluster_id = merged
        db.commit()
        logger.info(f"Step 0.4: Auto-merged {len(auto_merged)} cross-cluster pairs")

    if not llm_pairs:
        return len(auto_merged)

    # Send to LLM in one batched call
    logger.info(f"Step 0.4: Sending {len(llm_pairs)} cross-cluster pairs to LLM...")
    results = judge_dedup_pairs(llm_pairs)

    pair_meta = {p.pair_id: p for p in llm_pairs}
    new_db_rows = []
    llm_merged  = []

    for res in results:
        p = pair_meta.get(res['pair_id'])
        if not p:
            continue

        decision = res.get('decision', 'UNCERTAIN')

        if decision == 'DUPLICATE':
            llm_merged.append((p.index_a, p.index_b))
        else:
            # UNCERTAIN or NOT_DUPLICATE → store for human review
            # (NOT_DUPLICATE also goes to review so human can confirm)
            new_db_rows.append(DedupReviewPair(
                city_id       = city_id,
                index_a       = p.index_a,
                index_b       = p.index_b,
                name_a        = p.name_a,
                address_a     = p.address_a,
                name_b        = p.name_b,
                address_b     = p.address_b,
                similarity    = p.similarity,
                llm_reason    = res.get('reason', ''),
                decision      = 'UNCERTAIN' if decision == 'UNCERTAIN' else 'NOT_DUPLICATE',
                intra_cluster = False,
            ))

    # Apply LLM-confirmed merges
    if llm_merged:
        for idx_a, idx_b in llm_merged:
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_a).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec_a and rec_b:
                merged = min(rec_a.cluster_id or rec_a.id, rec_b.cluster_id or rec_b.id)
                rec_a.cluster_id = merged
                rec_b.cluster_id = merged
        db.commit()

    if new_db_rows:
        _bulk_insert(db, new_db_rows)

    uncertain_count = sum(1 for r in new_db_rows if r.decision == 'UNCERTAIN')
    logger.info(f"Step 0.4 done: {len(auto_merged)} auto-merged, {len(llm_merged)} LLM-merged, "
                f"{uncertain_count} sent to human review")
    return len(auto_merged) + len(results)




# ── Public API ────────────────────────────────────────────────────────────────

def run_step0(city: City, db: Session, output_dir: str) -> dict:
    """
    Entry point called by the pipeline.

    4-step dedup flow:
      1. LSH clustering on city sheet
      2. LLM verifies low-confidence intra-cluster pairs (same cluster, low sim)
      3. Vectorised cross-cluster near-miss scan (rapidfuzz cdist)
      4. LLM verifies near-miss candidates from step 3

    Also concatenates bludot export and ingests both into DB.
    """
    import logging
    logger = logging.getLogger(__name__)

    output_dir = Path(output_dir)
    city_data_dir   = output_dir / 'city_data'
    bludot_data_dir = output_dir / 'bludot_data'
    city_data_dir.mkdir(parents=True, exist_ok=True)
    bludot_data_dir.mkdir(parents=True, exist_ok=True)

    # ── Step 1: LSH deduplication ─────────────────────────────────────────────
    logger.info("Step 0.1: Preparing and deduplicating city sheet...")
    city_df = prepare_city_sheet(city.raw_data_path, db, city.id)
    deduplicator = BusinessDeduplicator()
    deduped_df = deduplicator.deduplicate(city_df)
    deduped_df.to_excel(str(city_data_dir / 'manual_dedup_records.xlsx'), index=False)
    logger.info(f"Step 0.1 done: {len(deduped_df)} records, "
                f"{deduped_df['cluster id'].nunique() if 'cluster id' in deduped_df.columns else 0} clusters")

    # Bulk-ingest city records into DB (needed for matching step)
    _ingest_city_records_bulk(deduped_df, city.id, db)

    # ── Step 2: Verify intra-cluster pairs with rules + LLM ──────────────────
    logger.info("Step 0.2: Verifying cluster assignments...")
    intra_count = _step2_verify_clusters_with_llm(deduped_df, city.id, db)
    logger.info(f"Step 0.2 done: {intra_count} pairs processed")

    # ── Step 3: Vectorised cross-cluster near-miss scan ───────────────────────
    logger.info("Step 0.3: Scanning for cross-cluster near-misses...")
    near_misses = _step3_cross_cluster_scan(deduped_df, similarity_threshold=0.92)
    logger.info(f"Step 0.3 done: {len(near_misses)} near-miss candidates found")

    # ── Step 4: Verify near-misses with rules + LLM ───────────────────────────
    near_miss_count = 0
    if near_misses:
        logger.info(f"Step 0.4: Verifying {len(near_misses)} cross-cluster near-miss pairs...")
        near_miss_count = _step4_verify_near_misses_with_llm(near_misses, city.id, db)
        logger.info(f"Step 0.4 done: {near_miss_count} pairs processed")

    # ── Bludot concatenation ──────────────────────────────────────────────────
    logger.info("Step 0.5: Concatenating bludot sheets...")
    bludot_df = concatenate_bludot_sheets(city.bludot_export_path)
    bludot_df.to_excel(str(bludot_data_dir / 'bludot_concatenated_records.xlsx'), index=False)
    _ingest_bludot_records_bulk(bludot_df, city.id, db)
    logger.info(f"Step 0.5 done: {len(bludot_df)} bludot records")

    n_clusters = deduped_df['cluster id'].nunique() if 'cluster id' in deduped_df.columns else 0
    return {
        'city_records'    : len(city_df),
        'deduped_records' : len(deduped_df),
        'clusters'        : n_clusters,
        'bludot_records'  : len(bludot_df),
        'intra_pairs'     : intra_count,
        'near_miss_pairs' : near_miss_count,
    }


def _ingest_city_records_bulk(df: pd.DataFrame, city_id: int, db: Session):
    """Bulk-insert city records — much faster than one db.add() per row."""
    from ..db.models import CityRecord

    db.query(CityRecord).filter_by(city_id=city_id).delete()
    db.commit()

    def _get(row, *keys):
        for k in keys:
            v = row.get(k, '')
            if v and str(v).strip() not in ('', '-', 'nan'):
                return str(v).strip()
        return ''

    objects = []
    for _, row in df.iterrows():
        rd = {k: ('' if str(v) in ('nan', 'NaT') else str(v)) for k, v in row.to_dict().items()}
        cid = rd.get('cluster id', None)
        objects.append(CityRecord(
            city_id       = city_id,
            city_index    = int(rd.get('city_index', 0) or 0),
            business_name = _get(rd, 'Business Name', 'Business Name_1'),
            address1      = _get(rd, 'Address1', 'Address1_1'),
            cluster_id    = int(float(cid)) if cid not in (None, '', 'nan') else None,
            raw_data      = rd,
        ))
    _bulk_insert(db, objects)


def _ingest_bludot_records_bulk(df: pd.DataFrame, city_id: int, db: Session):
    """Bulk-insert bludot records."""
    from ..db.models import BludotRecord

    db.query(BludotRecord).filter_by(city_id=city_id).delete()
    db.commit()

    name_col    = next((c for c in df.columns if c in ('Name', 'Business Name', 'name')), None)
    address_col = next((c for c in df.columns if c in ('Address1', 'address1', 'Address 1')), None)
    uuid_col    = next((c for c in df.columns if c in ('UUID', 'uuid')), None)

    objects = []
    for _, row in df.iterrows():
        rd = {k: ('' if str(v) in ('nan', 'NaT') else str(v)) for k, v in row.to_dict().items()}
        objects.append(BludotRecord(
            city_id      = city_id,
            bludot_index = int(rd.get('bludot_index', 0) or 0),
            uuid         = rd.get(uuid_col, '') if uuid_col else '',
            name         = rd.get(name_col, '') if name_col else '',
            address1     = rd.get(address_col, '') if address_col else '',
            raw_data     = rd,
        ))
    _bulk_insert(db, objects)


# Keep old function renamed so it doesn't run accidentally