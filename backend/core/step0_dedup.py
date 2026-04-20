"""
Step 0 — Deduplication + Bludot Concatenation
==============================================
"""

import datetime
import logging
import os
import re
import json
from pathlib import Path
from functools import lru_cache

import networkx as nx
import numpy as np
import pandas as pd
from rapidfuzz import fuzz, process
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy.orm import Session
import usaddress
from sentence_transformers import SentenceTransformer, util

from ..db.models import City, ColumnMapping, DedupReviewPair, CityRecord, BludotRecord
from ..core.llm_judge import judge_dedup_pairs, DedupPair

os.environ['HF_HUB_OFFLINE'] = '1'  # Forces the AI to load locally, ignoring the internet!

logger = logging.getLogger(__name__)

# --- SILENCE PANDAS WARNINGS ---
pd.set_option('future.no_silent_downcasting', True)

# --- LOAD AI GLOBALLY ---
logger.info("Loading Semantic Vector Embedding Model for Deduplication...")
embedder = SentenceTransformer('all-MiniLM-L6-v2')


# ── CACHED AI FUNCTIONS (50x Speed Boost) ─────────────────────────────────────

@lru_cache(maxsize=20000)
def normalize_address_smart(raw_address: str) -> str:
    if pd.isna(raw_address) or not str(raw_address).strip() or str(raw_address).strip() == '-':
        return ""

    addr_str = str(raw_address).upper().replace('.', '').replace(',', '')
    dir_map = {'NORTH':'N', 'SOUTH':'S', 'EAST':'E', 'WEST':'W', 
               'NORTHEAST':'NE', 'NORTHWEST':'NW', 'SOUTHEAST':'SE', 'SOUTHWEST':'SW'}
    type_map = {'STREET':'ST', 'AVENUE':'AVE', 'BOULEVARD':'BLVD', 'DRIVE':'DR', 
                'ROAD':'RD', 'LANE':'LN', 'COURT':'CT', 'CIRCLE':'CIR', 
                'PARKWAY':'PKWY', 'HIGHWAY':'HWY', 'SQUARE':'SQ', 'PLAZA':'PLZ'}
    unit_map = {'SUITE':'#', 'STE':'#', 'APARTMENT':'#', 'APT':'#', 
                'UNIT':'#', 'ROOM':'#', 'RM':'#', 'BUILDING':'#', 'BLDG':'#', 'SPACE':'#'}

    try:
        tagged, _ = usaddress.tag(addr_str)
        parts = []
        if 'AddressNumber' in tagged: parts.append(tagged['AddressNumber'])
        if 'StreetNamePreDirectional' in tagged: parts.append(dir_map.get(tagged['StreetNamePreDirectional'], tagged['StreetNamePreDirectional']))
        if 'StreetName' in tagged: parts.append(tagged['StreetName'])
        if 'StreetNamePostType' in tagged: parts.append(type_map.get(tagged['StreetNamePostType'], tagged['StreetNamePostType']))
        if 'StreetNamePostDirectional' in tagged: parts.append(dir_map.get(tagged['StreetNamePostDirectional'], tagged['StreetNamePostDirectional']))
        if 'OccupancyType' in tagged: parts.append(unit_map.get(tagged['OccupancyType'], '#')) 
        if 'OccupancyIdentifier' in tagged: parts.append(tagged['OccupancyIdentifier'])
        return " ".join(parts)
    except usaddress.RepeatedLabelError:
        return re.sub(r'\s+', ' ', addr_str).strip()

@lru_cache(maxsize=20000)
def get_vector_embedding(text: str):
    return embedder.encode(text, convert_to_tensor=True)


# ── DATE & FORMATTING HELPERS ─────────────────────────────────────────────────

def date_formatting(df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace('', np.nan)
    date_cols = [
        col for col in df.columns
        if df[col].dropna().shape[0] > 0 and isinstance(df[col].dropna().iloc[0], (datetime.datetime,))
    ]
    df = df.fillna('').infer_objects(copy=False)
    for col in date_cols:
        df[col] = df[col].apply(lambda v: v.strftime("%m/%d/%Y") if not isinstance(v, str) else v)
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%m/%d/%Y')
    return df

def concatenate_bludot_sheets(
    bludot_path: str, business_sheet: str = "Business Record",
    custom_sheet: str = "Custom Data", contact_sheet: str = "Contact_Details",
) -> pd.DataFrame:
    xl = pd.ExcelFile(bludot_path)
    available = xl.sheet_names

    biz_name = next((s for s in available if business_sheet.lower() in s.lower()), available[0])
    business_df = date_formatting(pd.read_excel(bludot_path, sheet_name=biz_name, dtype=object))
    merged = business_df.copy()

    cust_name = next((s for s in available if custom_sheet.lower() in s.lower()), None)
    if cust_name:
        custom_df = date_formatting(pd.read_excel(bludot_path, sheet_name=cust_name, dtype=object)).fillna('')
        if 'UUID' in business_df.columns and 'Custom Data Name' in custom_df.columns:
            merged = pd.merge(merged, custom_df, left_on='UUID', right_on='Custom Data Name', how='left')
        elif 'UUID' in business_df.columns and 'UUID' in custom_df.columns:
            merged = pd.merge(merged, custom_df, on='UUID', how='left', suffixes=('', '_custom'))

    cont_name = next((s for s in available if contact_sheet.lower() in s.lower()), None)
    if cont_name:
        contact_df = pd.read_excel(bludot_path, sheet_name=cont_name, dtype=object)
        contact_df.columns = [c.split('.')[0] for c in contact_df.columns]
        contact_df = date_formatting(contact_df).fillna('')
        id_col = next((c for c in contact_df.columns if c == 'ID'), None)
        uuid_col = next((c for c in contact_df.columns if c == 'UUID'), None)
        if id_col and 'UUID' in merged.columns:
            merged = pd.merge(merged, contact_df, left_on='UUID', right_on='ID', how='left', suffixes=('', '_contact'))
        elif uuid_col and 'UUID' in merged.columns:
            merged = pd.merge(merged, contact_df, on='UUID', how='left', suffixes=('', '_contact'))

    merged = merged.fillna('')
    merged['bludot_index'] = range(len(merged))
    return merged

# ── GENERALIZED PREP ──────────────────────────────────────────────────────────

def prepare_city_sheet(raw_path: str, db: Session, city_id: int) -> pd.DataFrame:
    ext = Path(raw_path).suffix.lower()
    df = pd.read_excel(raw_path, dtype=object) if ext in ('.xlsx', '.xls') else pd.read_csv(raw_path, dtype=object)
    df = date_formatting(df).fillna('')

    mappings = db.query(ColumnMapping).filter_by(city_id=city_id, mapping_type='business').all()
    rename = {m.source_col: m.target_col for m in mappings if m.source_col in df.columns}
    df = df.rename(columns=rename)

    # UPDATED: Actively hunt for the DBA column and separate it from the Business Name
    name_col = next((m.target_col for m in mappings if 'name' in str(m.target_col).lower() and 'dba' not in str(m.target_col).lower()), 'Business Name')
    dba_col = next((m.target_col for m in mappings if 'dba' in str(m.target_col).lower() or 'doing business' in str(m.target_col).lower()), 'DBA Name')
    addr_col = next((m.target_col for m in mappings if 'address' in str(m.target_col).lower()), 'Address1')

    if name_col in df.columns and name_col != 'Business Name':
        df['Business Name'] = df[name_col]
    elif 'Business Name' not in df.columns:
        df['Business Name'] = ''

    # Force standardization of the DBA Name so the DB always receives it consistently
    if dba_col in df.columns and dba_col != 'DBA Name':
        df['DBA Name'] = df[dba_col]
    elif 'DBA Name' not in df.columns:
        df['DBA Name'] = ''

    if addr_col in df.columns and addr_col != 'Address1':
        df['Address1'] = df[addr_col]
    elif 'Address1' not in df.columns:
        df['Address1'] = ''

    df['city_index'] = range(len(df))
    return df


# ── LSH DEDUPLICATION ─────────────────────────────────────────────────────────

np.random.seed(42)

class BusinessDeduplicator:
    def __init__(self, name_threshold=83, address_threshold=70, po_box_threshold=95):
        self.name_threshold = name_threshold
        self.address_threshold = address_threshold
        self.po_box_threshold = po_box_threshold

    def _normalize_name(self, name: str) -> str:
        if not name or name == '-': return ''
        name = name.lower()
        for ent in [' llc', ' inc', ' corporation', ' corp', ' company', ' co', ' ltd', ' limited', ' pllc', ' office', ' group', ' associates']:
            name = name.replace(ent, '')
        name = name.replace('&', 'and')
        name = re.sub(r'\s+', ' ', name)
        name = re.sub(r'[^\w\s]', '', name)
        for pfx in ['mr ', 'mrs ', 'ms ', 'dr ', 'prof ', 'miss ']:
            if name.startswith(pfx): name = name[len(pfx):]
        return name.strip()

    def _normalize_address(self, address: str) -> str:
        return normalize_address_smart(address).lower()

    def _is_po_box(self, address: str) -> bool:
        return bool(re.search(r'p\.?\s*o\.?\s*box', address, re.IGNORECASE))

    def _extract_po_box_num(self, address: str) -> str:
        m = re.search(r'p\.?\s*o\.?\s*box\s*#?\s*(\d+)', address, re.IGNORECASE)
        return m.group(1) if m else ''

    def _extract_street_num(self, address: str) -> str:
        m = re.match(r'^\s*(\d+)', address)
        return m.group(1) if m else ''

    def _extract_street_name(self, address: str) -> str:
        normed = self._normalize_address(address)
        return re.sub(r'^\s*\d+\s*', '', normed).strip()

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.loc[:, ~df.columns.duplicated()].copy()
        df['Business Name'] = df['Business Name'].fillna('-').astype(str)
        df['Address1']      = df['Address1'].fillna('-').astype(str)
        df['norm_name']     = df['Business Name'].apply(self._normalize_name)
        df['is_po_box']     = df['Address1'].apply(self._is_po_box)
        df['po_box_num']    = df['Address1'].apply(self._extract_po_box_num)
        df['street_num']    = df.apply(lambda r: self._extract_street_num(r['Address1']) if not r['is_po_box'] else '', axis=1)
        df['street_name']   = df.apply(lambda r: self._extract_street_name(r['Address1']) if not r['is_po_box'] else '', axis=1)
        df['norm_address']  = df['Address1'].apply(self._normalize_address)
        df['business_address'] = df['norm_name'] + '_' + df['norm_address']
        return df

    def _tfidf_vectors(self, df: pd.DataFrame):
        corpus = df['business_address'].tolist()
        return TfidfVectorizer(analyzer='char', ngram_range=(2, 3), min_df=1).fit_transform(corpus)

    def _apply_lsh(self, n_planes: int, tfidf_matrix, df: pd.DataFrame) -> pd.DataFrame:
        n_samples, n_features = tfidf_matrix.shape
        projections = (tfidf_matrix @ np.random.randn(n_features, n_planes) > 0)
        if hasattr(projections, "toarray"): projections = projections.toarray()
        projections = projections.astype(int)
        
        bucket_keys = [''.join(map(str, row)) for row in projections]
        df = df.copy()
        df['lsh_bucket'] = bucket_keys

        buckets = {}
        for i, key in enumerate(bucket_keys):
            buckets.setdefault(key, []).append(i)

        G = nx.Graph()
        G.add_nodes_from(range(n_samples))

        for bucket_indices in buckets.values():
            for i in range(len(bucket_indices)):
                for j in range(i + 1, len(bucket_indices)):
                    ri, rj = df.iloc[bucket_indices[i]], df.iloc[bucket_indices[j]]
                    if self.compute_similarity(ri, rj):
                        G.add_edge(bucket_indices[i], bucket_indices[j])

        cluster_id = {}
        for cluster_num, component in enumerate(nx.connected_components(G)):
            for node in component: cluster_id[node] = cluster_num
        df['cluster id'] = [cluster_id.get(i, i) for i in range(n_samples)]
        return df

    def compute_similarity(self, r1, r2) -> bool:
        name_sim = max(fuzz.ratio(r1['norm_name'], r2['norm_name']), fuzz.token_sort_ratio(r1['norm_name'], r2['norm_name']))
        exact_name = r1['Business Name'].lower() == r2['Business Name'].lower()
        blank1, blank2 = r1['Address1'] in ('-', ''), r2['Address1'] in ('-', '')

        if blank1 or blank2: return name_sim >= 83 or exact_name
        if r1['is_po_box'] != r2['is_po_box']: return exact_name or name_sim >= 83
        if r1['is_po_box'] and r2['is_po_box']:
            if r1['po_box_num'] and r2['po_box_num']: return r1['po_box_num'] == r2['po_box_num'] and name_sim >= 83
            return name_sim >= 83

        sn1, sn2 = r1['street_num'], r2['street_num']
        if sn1 and sn2:
            if fuzz.ratio(sn1, sn2) < 100: return False
            return name_sim >= 85 and fuzz.token_sort_ratio(r1['street_name'], r2['street_name']) >= 80
        if sn1 or sn2:
            return fuzz.token_sort_ratio(r1['street_name'], r2['street_name']) >= 80 and name_sim >= 85
        return fuzz.token_sort_ratio(r1['street_name'], r2['street_name']) >= 90 and name_sim >= 90

    def deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:
        preprocessed = self.preprocess(df)
        tfidf = self._tfidf_vectors(preprocessed)
        return self._apply_lsh(3, tfidf, preprocessed)


# ── DB INGESTION HELPERS (GENERALIZED) ────────────────────────────────────────

def _bulk_insert(db: Session, objects: list) -> None:
    db.bulk_save_objects(objects)
    db.commit()

def _ingest_city_records_bulk(df: pd.DataFrame, city_id: int, db: Session):
    db.query(CityRecord).filter_by(city_id=city_id).delete()
    db.commit()

    df = df.loc[:, ~df.columns.duplicated()].copy()
    objects = []
    
    for _, row in df.iterrows():
        rd = json.loads(row.to_json())
        cid = rd.get('cluster id', None)
        
        objects.append(CityRecord(
            city_id=city_id, 
            city_index=int(rd.get('city_index', 0) or 0),
            business_name=str(rd.get('Business Name', '')).strip(), 
            address1=str(rd.get('Address1', '')).strip(),
            cluster_id=int(float(cid)) if cid not in (None, '', 'nan') else None, 
            raw_data=rd,
        ))
    _bulk_insert(db, objects)


def _ingest_bludot_records_bulk(df: pd.DataFrame, city_id: int, db: Session):
    db.query(BludotRecord).filter_by(city_id=city_id).delete()
    db.commit()

    df = df.loc[:, ~df.columns.duplicated()].copy()

    # UPDATED: Dynamically find the exact column names, ensuring Name doesn't accidentally grab DBA
    uuid_col = next((c for c in df.columns if 'uuid' in str(c).lower() or 'id' == str(c).lower()), None)
    name_col = next((c for c in df.columns if ('name' in str(c).lower() or 'company' in str(c).lower()) and 'dba' not in str(c).lower()), None)
    dba_col = next((c for c in df.columns if 'dba' in str(c).lower() or 'doing business' in str(c).lower()), None)
    addr_col = next((c for c in df.columns if 'address' in str(c).lower()), None)

    objects = []
    for idx, row in df.iterrows():
        rd = json.loads(row.dropna().to_json())
        
        # Standardize the DBA name inside the JSON payload so Step 2 can query rd.get('DBA Name')
        dba_val = rd.get(dba_col, '') if dba_col else rd.get('DBA Name', '')
        rd['DBA Name'] = str(dba_val).strip()

        objects.append(BludotRecord(
            city_id=city_id, 
            bludot_index=int(idx),
            uuid=str(rd.get(uuid_col, '')) if uuid_col else '', 
            name=str(rd.get(name_col, '')) if name_col else '',
            address1=str(rd.get(addr_col, '')) if addr_col else '', 
            raw_data=rd,
        ))
    _bulk_insert(db, objects)


# ── POST-PROCESSING ──────────────────────────────────────────────────────────

def _step2_verify_clusters_with_llm(df: pd.DataFrame, city_id: int, db: Session) -> int:
    if 'cluster id' not in df.columns: return 0

    records = df[['city_index', 'cluster id', 'Business Name', 'Address1', 'norm_name', 'norm_address', 'street_num']].fillna('').to_dict('records')
    cluster_groups = {}
    for row in records: cluster_groups.setdefault(row['cluster id'], []).append(row)

    auto_merged, auto_split, llm_pairs, new_db_rows = 0, [], [], []
    seen = set()
    max_cluster = int(df['cluster id'].max()) if 'cluster id' in df.columns else 0

    for cid, group in cluster_groups.items():
        if len(group) < 2: continue
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                a, b = group[i], group[j]
                key = (min(int(a['city_index']), int(b['city_index'])), max(int(a['city_index']), int(b['city_index'])))
                if key in seen: continue
                seen.add(key)

                addr_a, addr_b = str(a['Address1']).strip(), str(b['Address1']).strip()
                sn_a, sn_b = str(a['street_num']).strip(), str(b['street_num']).strip()
                norm_a, norm_b = str(a['norm_address']).strip(), str(b['norm_address']).strip()
                name_sim = fuzz.token_sort_ratio(a['norm_name'], b['norm_name']) / 100.0

                if norm_a and norm_b and norm_a == norm_b and name_sim >= 0.95:
                    auto_merged += 1
                    continue

                if sn_a and sn_b and sn_a != sn_b:
                    max_cluster += 1
                    auto_split.append((int(b['city_index']), max_cluster))
                    continue

                combined_a = f"{a['Business Name'].lower()} {norm_a}"
                combined_b = f"{b['Business Name'].lower()} {norm_b}"
                emb_a = get_vector_embedding(combined_a)
                emb_b = get_vector_embedding(combined_b)
                if util.cos_sim(emb_a, emb_b).item() * 100 >= 88:
                    auto_merged += 1
                    continue

                needs_llm = addr_a in ('', '-', 'nan') or addr_b in ('', '-', 'nan') or (fuzz.token_sort_ratio(norm_a, norm_b) >= 70 and name_sim >= 0.85)
                if needs_llm:
                    llm_pairs.append(DedupPair(
                        pair_id=f"{key[0]}_{key[1]}", index_a=key[0], index_b=key[1], name_a=a['Business Name'], address_a=addr_a,
                        name_b=b['Business Name'], address_b=addr_b, similarity=name_sim, intra_cluster=True
                    ))

    if auto_split:
        for idx_b, new_cid in auto_split:
            rec = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec: rec.cluster_id = new_cid
        db.commit()

    if not llm_pairs: return auto_merged + len(auto_split)

    results = judge_dedup_pairs(llm_pairs)
    for res in results:
        p = next((x for x in llm_pairs if x.pair_id == res['pair_id']), None)
        if not p: continue
        decision = res.get('decision', 'UNCERTAIN')

        if decision == 'NOT_DUPLICATE':
            max_cluster += 1
            rec = db.query(CityRecord).filter_by(city_id=city_id, city_index=p.index_b).first()
            if rec: rec.cluster_id = max_cluster
        elif decision == 'UNCERTAIN':
            new_db_rows.append(DedupReviewPair(
                city_id=city_id, index_a=p.index_a, index_b=p.index_b, name_a=p.name_a, address_a=p.address_a,
                name_b=p.name_b, address_b=p.address_b, similarity=p.similarity, llm_reason=res.get('reason', ''), decision='UNCERTAIN', intra_cluster=True
            ))

    if new_db_rows: _bulk_insert(db, new_db_rows)
    db.commit()
    return auto_merged + len(auto_split) + len(results)


def _step3_cross_cluster_scan(df: pd.DataFrame, similarity_threshold: float = 0.92) -> list[dict]:
    if 'cluster id' not in df.columns or 'norm_name' not in df.columns: return []
    records = df[['city_index', 'cluster id', 'Business Name', 'Address1', 'norm_name', 'norm_address']].fillna('').reset_index(drop=True)
    names, addresses, cluster_ids, raw_addrs = records['norm_name'].tolist(), records['norm_address'].tolist(), records['cluster id'].tolist(), records['Address1'].tolist()

    name_scores = process.cdist(names, names, scorer=fuzz.token_sort_ratio, score_cutoff=int(similarity_threshold * 100))
    rows_idx, cols_idx = np.nonzero(np.triu(name_scores, k=1))
    cluster_arr = np.array(cluster_ids)
    near_misses = []

    for i, j in zip(rows_idx.tolist(), cols_idx.tolist()):
        if cluster_arr[i] == cluster_arr[j]: continue
        addr_a, addr_b = str(raw_addrs[i]).strip(), str(raw_addrs[j]).strip()
        
        if not (addr_a in ('', '-', 'nan') or addr_b in ('', '-', 'nan')):
            if fuzz.token_sort_ratio(addresses[i], addresses[j]) < 80: continue

        near_misses.append({
            'index_a': int(records.at[i, 'city_index']), 'index_b': int(records.at[j, 'city_index']),
            'name_a': records.at[i, 'Business Name'], 'address_a': records.at[i, 'Address1'],
            'name_b': records.at[j, 'Business Name'], 'address_b': records.at[j, 'Address1'],
            'similarity': float(name_scores[i, j]) / 100.0,
        })
    return near_misses


def _step4_verify_near_misses_with_llm(near_misses: list[dict], city_id: int, db: Session) -> int:
    if not near_misses: return 0
    db.query(DedupReviewPair).filter_by(city_id=city_id, intra_cluster=False).delete()
    db.commit()

    auto_merged, llm_pairs = [], []

    for m in near_misses:
        addr_a, addr_b = str(m.get('address_a', '')).strip(), str(m.get('address_b', '')).strip()
        norm_a = normalize_address_smart(addr_a) if addr_a not in ('', '-', 'nan') else ''
        norm_b = normalize_address_smart(addr_b) if addr_b not in ('', '-', 'nan') else ''

        if norm_a and norm_b and norm_a == norm_b:
            auto_merged.append((m['index_a'], m['index_b']))
            continue

        combined_a = f"{str(m['name_a']).lower()} {norm_a}"
        combined_b = f"{str(m['name_b']).lower()} {norm_b}"
        emb_a = get_vector_embedding(combined_a)
        emb_b = get_vector_embedding(combined_b)
        if util.cos_sim(emb_a, emb_b).item() * 100 >= 88:
            auto_merged.append((m['index_a'], m['index_b']))
            continue

        llm_pairs.append(DedupPair(
            pair_id=f"{m['index_a']}_{m['index_b']}", index_a=m['index_a'], index_b=m['index_b'],
            name_a=m['name_a'], address_a=addr_a, name_b=m['name_b'], address_b=addr_b, similarity=m['similarity'], intra_cluster=False
        ))

    if auto_merged:
        for idx_a, idx_b in auto_merged:
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_a).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec_a and rec_b:
                merged = min(rec_a.cluster_id or rec_a.id, rec_b.cluster_id or rec_b.id)
                rec_a.cluster_id = rec_b.cluster_id = merged
        db.commit()

    if not llm_pairs: return len(auto_merged)

    results = judge_dedup_pairs(llm_pairs)
    pair_meta = {p.pair_id: p for p in llm_pairs}
    new_db_rows, llm_merged = [], []

    for res in results:
        p = pair_meta.get(res['pair_id'])
        if not p: continue
        decision = res.get('decision', 'UNCERTAIN')

        if decision == 'DUPLICATE': llm_merged.append((p.index_a, p.index_b))
        else:
            new_db_rows.append(DedupReviewPair(
                city_id=city_id, index_a=p.index_a, index_b=p.index_b, name_a=p.name_a, address_a=p.address_a,
                name_b=p.name_b, address_b=p.address_b, similarity=p.similarity, llm_reason=res.get('reason', ''), decision='UNCERTAIN' if decision == 'UNCERTAIN' else 'NOT_DUPLICATE', intra_cluster=False
            ))

    if llm_merged:
        for idx_a, idx_b in llm_merged:
            rec_a = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_a).first()
            rec_b = db.query(CityRecord).filter_by(city_id=city_id, city_index=idx_b).first()
            if rec_a and rec_b:
                merged = min(rec_a.cluster_id or rec_a.id, rec_b.cluster_id or rec_b.id)
                rec_a.cluster_id = rec_b.cluster_id = merged
        db.commit()

    if new_db_rows: _bulk_insert(db, new_db_rows)
    return len(auto_merged) + len(results)


# ── PUBLIC API ────────────────────────────────────────────────────────────────

def run_step0(city: City, db: Session, output_dir: str) -> dict:
    output_dir = Path(output_dir)
    city_data_dir, bludot_data_dir = output_dir / 'city_data', output_dir / 'bludot_data'
    city_data_dir.mkdir(parents=True, exist_ok=True)
    bludot_data_dir.mkdir(parents=True, exist_ok=True)

    city_df = prepare_city_sheet(city.raw_data_path, db, city.id)
    deduplicator = BusinessDeduplicator()
    deduped_df = deduplicator.deduplicate(city_df)
    deduped_df.to_excel(str(city_data_dir / 'manual_dedup_records.xlsx'), index=False)
    
    _ingest_city_records_bulk(deduped_df, city.id, db)
    intra_count = _step2_verify_clusters_with_llm(deduped_df, city.id, db)
    near_misses = _step3_cross_cluster_scan(deduped_df, similarity_threshold=0.92)
    near_miss_count = _step4_verify_near_misses_with_llm(near_misses, city.id, db) if near_misses else 0

    bludot_df = concatenate_bludot_sheets(city.bludot_export_path)
    bludot_df.to_excel(str(bludot_data_dir / 'bludot_concatenated_records.xlsx'), index=False)
    _ingest_bludot_records_bulk(bludot_df, city.id, db)

    return {
        'city_records': len(city_df), 'deduped_records': len(deduped_df),
        'clusters': deduped_df['cluster id'].nunique() if 'cluster id' in deduped_df.columns else 0,
        'bludot_records': len(bludot_df), 'intra_pairs': intra_count, 'near_miss_pairs': near_miss_count,
    }