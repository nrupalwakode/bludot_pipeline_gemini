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
        address = address.lower()
        abbrevs = {
            r'\bstreet\b': 'st', r'\bavenue\b': 'ave', r'\bboulevard\b': 'blvd',
            r'\bdrive\b': 'dr', r'\broad\b': 'rd', r'\blane\b': 'ln',
            r'\bcourt\b': 'ct', r'\bplace\b': 'pl', r'\bcircle\b': 'cir',
            r'\bsuite\b': 'ste', r'\bnorth\b': 'n', r'\bsouth\b': 's',
            r'\beast\b': 'e', r'\bwest\b': 'w',
        }
        for pattern, repl in abbrevs.items():
            address = re.sub(pattern, repl, address)
        address = re.sub(r'[^\w\s]', ' ', address)
        address = re.sub(r'\s+', ' ', address)
        return address.strip()

    def _is_po_box(self, address: str) -> bool:
        return bool(re.search(r'p\.?\s*o\.?\s*box', address, re.IGNORECASE))

    def _extract_po_box_num(self, address: str) -> str:
        m = re.search(r'p\.?\s*o\.?\s*box\s*#?\s*(\d+)', address, re.IGNORECASE)
        return m.group(1) if m else ''

    def _extract_street_num(self, address: str) -> str:
        m = re.match(r'^\s*(\d+)', address)
        return m.group(1) if m else ''

    def _extract_street_name(self, address: str) -> str:
        return re.sub(r'^\s*\d+\s*', '', address).strip()

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

def _step2_llm_verify_clusters(df: pd.DataFrame, city_id: int, db: Session) -> list[dict]:
    """
    Step 2 of dedup: LLM verifies LOW-CONFIDENCE intra-cluster pairs.
    These are records in the SAME cluster whose similarity score was borderline.

    We compute the pairwise similarity within each cluster. Pairs that scored
    below LOW_CONFIDENCE_THRESHOLD (meaning LSH grouped them but they don't look
    obviously the same) are sent to LLM.

    Returns list of LLM results for UI display.
    """
    from rapidfuzz import fuzz
    from ..core.llm_judge import judge_dedup_pairs, DedupPair

    LOW_CONFIDENCE = 0.72   # pairs in same cluster but below this → verify

    if 'cluster id' not in df.columns:
        return []

    pairs_to_verify: list[DedupPair] = []
    records = df[['city_index', 'cluster id', 'Business Name', 'Address1',
                  'norm_name']].fillna('').to_dict('records')

    cluster_groups: dict = {}
    for row in records:
        cid = row['cluster id']
        cluster_groups.setdefault(cid, []).append(row)

    seen: set[tuple] = set()
    for cid, group in cluster_groups.items():
        if len(group) < 2:
            continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                sim = fuzz.token_sort_ratio(a['norm_name'], b['norm_name']) / 100.0
                if sim < LOW_CONFIDENCE:
                    key = (min(int(a['city_index']), int(b['city_index'])),
                           max(int(a['city_index']), int(b['city_index'])))
                    if key not in seen:
                        seen.add(key)
                        pairs_to_verify.append(DedupPair(
                            pair_id   = f"{key[0]}_{key[1]}",
                            index_a   = key[0], index_b   = key[1],
                            name_a    = a['Business Name'], address_a = a['Address1'],
                            name_b    = b['Business Name'], address_b = b['Address1'],
                            similarity= sim,
                        ))

    if not pairs_to_verify:
        return []

    return judge_dedup_pairs(pairs_to_verify)


def _step3_cross_cluster_scan(df: pd.DataFrame,
                               similarity_threshold: float = 0.85) -> list[dict]:
    """
    Step 3 of dedup: vectorised cross-cluster near-miss scan using rapidfuzz
    process.cdist — replaces the O(n²) per-block loop.

    Returns list of {index_a, index_b, name_a, address_a, name_b, address_b, similarity}
    for pairs in DIFFERENT clusters that score above threshold.
    """
    from rapidfuzz import process, fuzz
    import numpy as np

    if 'cluster id' not in df.columns or 'norm_name' not in df.columns:
        return []

    records = df[['city_index', 'cluster id', 'Business Name',
                  'Address1', 'norm_name']].fillna('').reset_index(drop=True)

    names       = records['norm_name'].tolist()
    cluster_ids = records['cluster id'].tolist()

    # rapidfuzz cdist computes all pairwise scores efficiently in C
    # Use token_sort_ratio which handles word-order differences
    scores = process.cdist(names, names, scorer=fuzz.token_sort_ratio,
                           score_cutoff=int(similarity_threshold * 100))

    near_misses = []
    n = len(names)
    for i in range(n):
        for j in range(i + 1, n):
            if scores[i, j] == 0:          # below cutoff → cdist returns 0
                continue
            if cluster_ids[i] == cluster_ids[j]:  # same cluster → already handled
                continue
            sim = scores[i, j] / 100.0
            near_misses.append({
                'index_a'   : int(records.at[i, 'city_index']),
                'index_b'   : int(records.at[j, 'city_index']),
                'name_a'    : records.at[i, 'Business Name'],
                'address_a' : records.at[i, 'Address1'],
                'name_b'    : records.at[j, 'Business Name'],
                'address_b' : records.at[j, 'Address1'],
                'similarity': sim,
            })

    return near_misses


def _step4_llm_verify_near_misses(near_misses: list[dict],
                                   city_id: int, db: Session) -> int:
    """
    Step 4 of dedup: LLM verifies the cross-cluster near-miss candidates
    found in step 3.

    Stores all results in DedupReviewPair table:
      DUPLICATE     → auto-merge clusters, no human needed
      NOT_DUPLICATE → stored but hidden from review UI
      UNCERTAIN     → shown in review UI for human decision

    Returns count of pairs stored.
    """
    from ..db.models import DedupReviewPair, CityRecord
    from ..core.llm_judge import judge_dedup_pairs, DedupPair

    if not near_misses:
        return 0

    db.query(DedupReviewPair).filter_by(city_id=city_id).delete()
    db.commit()

    pairs = [
        DedupPair(
            pair_id   = f"{m['index_a']}_{m['index_b']}",
            index_a   = m['index_a'],   index_b   = m['index_b'],
            name_a    = m['name_a'],    address_a = m['address_a'],
            name_b    = m['name_b'],    address_b = m['address_b'],
            similarity= m['similarity'],
        )
        for m in near_misses
    ]

    results = judge_dedup_pairs(pairs)

    # Build lookup for metadata
    meta = {f"{m['index_a']}_{m['index_b']}": m for m in near_misses}

    new_rows = []
    duplicate_pairs = []

    for res in results:
        m = meta.get(res['pair_id'], {})
        new_rows.append(DedupReviewPair(
            city_id    = city_id,
            index_a    = res['index_a'],
            index_b    = res['index_b'],
            name_a     = m.get('name_a', ''),
            address_a  = m.get('address_a', ''),
            name_b     = m.get('name_b', ''),
            address_b  = m.get('address_b', ''),
            similarity = m.get('similarity', 0.0),
            llm_reason = res.get('reason', ''),
            decision   = res.get('decision', 'UNCERTAIN'),
        ))
        if res.get('decision') == 'DUPLICATE':
            duplicate_pairs.append((res['index_a'], res['index_b']))

    _bulk_insert(db, new_rows)

    # Auto-merge DUPLICATE decisions
    if duplicate_pairs:
        for idx_a, idx_b in duplicate_pairs:
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_a).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec_a and rec_b:
                merged = min(rec_a.cluster_id or rec_a.id, rec_b.cluster_id or rec_b.id)
                rec_a.cluster_id = merged
                rec_b.cluster_id = merged
        db.commit()

    return len(new_rows)


def _old_run_dedup_llm_pass(df: pd.DataFrame, city_id: int, db: Session,
                        similarity_threshold: float = 0.85) -> int:
    """
    After LSH clustering, find pairs across different clusters with high
    name similarity — these are potential missed duplicates. Send to LLM,
    store UNCERTAIN ones for human review.

    Returns count of near-miss pairs found.
    """
    from ..db.models import DedupReviewPair
    from ..core.llm_judge import judge_dedup_pairs, DedupPair
    from rapidfuzz import fuzz

    # Clear existing dedup review pairs for this city
    db.query(DedupReviewPair).filter_by(city_id=city_id).delete()
    db.commit()

    if 'cluster id' not in df.columns or 'norm_name' not in df.columns:
        return 0

    # Only compare records in different clusters
    records = df[['city_index', 'cluster id', 'Business Name', 'Address1',
                  'norm_name', 'norm_address']].copy()
    records = records.fillna('')

    # Build candidate near-miss pairs using 3-char prefix blocking
    near_miss_pairs: list[DedupPair] = []
    seen: set[tuple] = set()

    rows = records.to_dict('records')
    # Index by first 3 chars of norm_name
    blocks: dict[str, list] = {}
    for row in rows:
        key = str(row['norm_name'])[:3]
        blocks.setdefault(key, []).append(row)

    for key, block_rows in blocks.items():
        for i in range(len(block_rows)):
            for j in range(i + 1, len(block_rows)):
                a, b = block_rows[i], block_rows[j]
                # Skip same cluster — already handled
                if a['cluster id'] == b['cluster id']:
                    continue
                pair_key = (min(int(a['city_index']), int(b['city_index'])),
                            max(int(a['city_index']), int(b['city_index'])))
                if pair_key in seen:
                    continue
                seen.add(pair_key)

                sim = fuzz.token_sort_ratio(a['norm_name'], b['norm_name']) / 100.0
                if sim >= similarity_threshold:
                    near_miss_pairs.append(DedupPair(
                        pair_id   = f"{pair_key[0]}_{pair_key[1]}",
                        index_a   = pair_key[0],
                        index_b   = pair_key[1],
                        name_a    = str(a['Business Name']),
                        address_a = str(a['Address1']),
                        name_b    = str(b['Business Name']),
                        address_b = str(b['Address1']),
                        similarity= sim,
                    ))

    if not near_miss_pairs:
        return 0

    # Judge with LLM
    results = judge_dedup_pairs(near_miss_pairs)

    # Store all results; UNCERTAIN ones need human review
    for res in results:
        db.add(DedupReviewPair(
            city_id    = city_id,
            index_a    = res['index_a'],
            index_b    = res['index_b'],
            name_a     = next((p.name_a for p in near_miss_pairs if p.pair_id == res['pair_id']), ''),
            address_a  = next((p.address_a for p in near_miss_pairs if p.pair_id == res['pair_id']), ''),
            name_b     = next((p.name_b for p in near_miss_pairs if p.pair_id == res['pair_id']), ''),
            address_b  = next((p.address_b for p in near_miss_pairs if p.pair_id == res['pair_id']), ''),
            similarity = next((p.similarity for p in near_miss_pairs if p.pair_id == res['pair_id']), 0.0),
            llm_reason = res.get('reason', ''),
            decision   = res.get('decision', 'UNCERTAIN'),
        ))

        # Auto-merge DUPLICATE decisions immediately
        if res.get('decision') == 'DUPLICATE':
            from ..db.models import CityRecord
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=res['index_a']).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=res['index_b']).first()
            if rec_a and rec_b:
                merged = min(rec_a.cluster_id or rec_a.id, rec_b.cluster_id or rec_b.id)
                rec_a.cluster_id = merged
                rec_b.cluster_id = merged

    db.commit()
    return len(near_miss_pairs)


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

    # ── Step 2: LLM verifies low-confidence intra-cluster pairs ──────────────
    logger.info("Step 0.2: LLM verifying low-confidence cluster assignments...")
    intra_results = _step2_llm_verify_clusters(deduped_df, city.id, db)
    logger.info(f"Step 0.2 done: verified {len(intra_results)} low-confidence pairs")

    # ── Step 3: Vectorised cross-cluster near-miss scan ───────────────────────
    logger.info("Step 0.3: Scanning for cross-cluster near-misses...")
    near_misses = _step3_cross_cluster_scan(deduped_df, similarity_threshold=0.85)
    logger.info(f"Step 0.3 done: {len(near_misses)} near-miss candidates found")

    # ── Step 4: LLM verifies near-miss candidates ─────────────────────────────
    near_miss_count = 0
    if near_misses:
        logger.info(f"Step 0.4: LLM verifying {len(near_misses)} near-miss pairs...")
        near_miss_count = _step4_llm_verify_near_misses(near_misses, city.id, db)
        logger.info(f"Step 0.4 done: {near_miss_count} pairs stored for review")

    # ── Bludot concatenation ──────────────────────────────────────────────────
    logger.info("Step 0.5: Concatenating bludot sheets...")
    bludot_df = concatenate_bludot_sheets(city.bludot_export_path)
    bludot_df.to_excel(str(bludot_data_dir / 'bludot_concatenated_records.xlsx'), index=False)
    _ingest_bludot_records_bulk(bludot_df, city.id, db)
    logger.info(f"Step 0.5 done: {len(bludot_df)} bludot records")

    n_clusters = deduped_df['cluster id'].nunique() if 'cluster id' in deduped_df.columns else 0
    return {
        'city_records'          : len(city_df),
        'deduped_records'       : len(deduped_df),
        'clusters'              : n_clusters,
        'bludot_records'        : len(bludot_df),
        'low_conf_intra_pairs'  : len(intra_results),
        'near_miss_pairs'       : near_miss_count,
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