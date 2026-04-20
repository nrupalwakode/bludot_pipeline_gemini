"""
LLM Judge — Groq (High Accuracy Mode with DBA Support)
======================================================
Uses Groq's smart model (llama-3.3-70b-versatile).
Includes proactive pacing to safely stay under the 6,000 TPM free-tier limit.
"""

import json
import os
import time
import logging
from datetime import datetime
from dataclasses import dataclass, field

from groq import Groq

logger = logging.getLogger(__name__)


# ── Client setup (Round-Robin Multi-Key) ──────────────────────────────────────

_clients: list[Groq] = []
_current_key_index = 0

def _init_clients():
    global _clients
    if _clients:
        return

    raw_keys = os.getenv("GROQ_API_KEYS", "")
    keys = [k.strip() for k in raw_keys.split(",") if k.strip()]

    if not keys:
        single_key = os.getenv("GROQ_API_KEY", "").strip()
        if single_key:
            keys = [single_key]

    if not keys:
        logger.warning("No GROQ_API_KEYS or GROQ_API_KEY configured")
        return

    for k in keys:
        _clients.append(Groq(api_key=k, timeout=60.0))

def _get_client() -> Groq:
    global _current_key_index, _clients
    _init_clients()
    if not _clients:
        raise RuntimeError("No Groq API keys configured")

    client = _clients[_current_key_index]
    _current_key_index = (_current_key_index + 1) % len(_clients)
    return client

def has_api_key() -> bool:
    _init_clients()
    return len(_clients) > 0


# ── Core Groq call ────────────────────────────────────────────────────────────

def _call_llm(prompt: str, max_tokens: int = 1024, retries: int = None) -> dict | list:
    _init_clients()
    if not _clients:
        return {"decision": "UNCERTAIN", "reason": "No API key"}

    if retries is None:
        retries = len(_clients) * 3 

    last_error = None

    for attempt in range(retries):
        try:
            client = _get_client()

            response = client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=max_tokens,
                response_format={"type": "json_object"},
            )
            text = response.choices[0].message.content.strip()

            if text.startswith("```"):
                parts = text.split("```")
                if len(parts) > 1:
                    text = parts[1]
                    if text.startswith("json"):
                        text = text[4:]
            text = text.strip()

            return json.loads(text)

        except json.JSONDecodeError as e:
            last_error = e
            if attempt == 0:
                max_tokens = min(max_tokens * 2, 8192)
                continue
            break

        except Exception as e:
            last_error = e
            err_str = str(e).lower()

            if "413" in err_str or "rate_limit" in err_str or "429" in err_str:
                logger.warning(f"Groq limit hit. Swapping keys & pausing 10s... (Attempt {attempt+1}/{retries})")
                time.sleep(10)
                continue
            elif "quota" in err_str or "limit" in err_str:
                logger.warning(f"Groq quota exhausted on current key. Swapping...")
                time.sleep(5)
                continue
            else:
                logger.error(f"Groq call failed: {e}")
                break

    logger.warning("Forcing 30-second cooldown due to total key exhaustion...")
    time.sleep(30)
    return {"decision": "UNCERTAIN", "reason": f"LLM error: {last_error}"}


# ── Prompts ───────────────────────────────────────────────────────────────────

# UPDATED: Added aggressive DBA rules to force matches
BATCH_MATCH_PROMPT = """You are an expert at matching business records from two data sources.

For each numbered pair below, decide if Record A and Record B refer to the SAME physical business.

RULES (apply to every pair):
1. CRITICAL: If Record A's Primary Name matches Record B's DBA Name (or vice versa), you MUST output MATCH.
2. CRITICAL: If Record A's DBA Name matches Record B's DBA Name, you MUST output MATCH.
3. Both street numbers present AND different → NOT same business.
4. One name clearly contained in the other → likely SAME business.
5. One/both addresses blank → judge by name and DBA only.
6. Missing street number in one → do not penalise.
7. Ignore legal suffixes (LLC, Inc, Corp, Ltd).
8. Ignore spelling/abbreviation differences (St/Street, Ave/Avenue).

PAIRS:
{pairs_block}

Respond with a JSON object containing a "results" array — one object per pair in the same order:
{{"results": [
  {{"id": 1, "decision": "MATCH", "reason": "one sentence"}},
  {{"id": 2, "decision": "NO_MATCH", "reason": "one sentence"}}
]}}
decision must be exactly: MATCH, NO_MATCH, or UNCERTAIN"""

BATCH_DEDUP_PROMPT = """You are checking if business records are duplicates.

For each numbered pair, decide if Record A and Record B are the SAME business.

RULES:
1. Different street numbers → NOT duplicates.
2. Same name, one address blank → likely duplicates.
3. Abbreviations/spelling variants of same name → likely duplicates.
4. Completely different businesses → NOT duplicates.

PAIRS:
{pairs_block}

Respond with a JSON object containing a "results" array — one object per pair in the same order:
{{"results": [
  {{"id": 1, "decision": "DUPLICATE", "reason": "one sentence"}},
  {{"id": 2, "decision": "NOT_DUPLICATE", "reason": "one sentence"}}
]}}
decision must be exactly: DUPLICATE, NOT_DUPLICATE, or UNCERTAIN"""

MAPPING_PROMPT = """You are mapping columns from a city business license sheet to a standard schema.

City sheet columns with sample values:
{columns_with_samples}

Bludot custom data columns already in our database:
{bludot_custom_cols}

BUSINESS SCHEMA — map each column to one of these if it fits:
Business Name, Address1, Address2, City, State, Country, Zipcode, Phonenumber, Website, Lat, Long, DBA Name, Business Operational Status

CONTACT — person-level info (owner names, personal emails, personal phones):
mapping_type "contact" with role (Owner/Manager/Agent/Contact) and contact_type (email/phone_number/name/address)

CUSTOM — data that doesn't fit business schema:
Use bludot custom column name if it matches, else propose a clean label.

SKIP — irrelevant columns (internal IDs, sequence numbers, audit fields).

Respond with a JSON object containing a "mappings" array:
{{"mappings": [
  {{"source_col": "ACCOUNT NAME", "mapping_type": "business", "target_col": "Business Name", "meta": {{}}}},
  {{"source_col": "OWNER EMAIL", "mapping_type": "contact", "target_col": "[email]", "meta": {{"role": "Owner", "contact_type": "email", "person_col": "OWNER NAME"}}}},
  {{"source_col": "LICENSE TYPE", "mapping_type": "custom", "target_col": "License Type", "meta": {{"bludot_custom_col": "License Type"}}}},
  {{"source_col": "ROW_ID", "mapping_type": "skip", "target_col": "SKIP", "meta": {{}}}}
]}}
Every source column must appear exactly once."""


# ── Match judging ─────────────────────────────────────────────────────────────

# UPDATED: Added city_dba and bludot_dba explicitly
@dataclass
class CandidatePair:
    candidate_id: int
    city_name: str
    city_address: str
    bludot_name: str
    bludot_address: str
    rule_reason: str
    city_dba: str = field(default="")
    bludot_dba: str = field(default="")


def judge_candidates(pairs: list[CandidatePair]) -> list[dict]:
    if not pairs:
        return []

    now = datetime.utcnow().isoformat()
    if not has_api_key():
        return [{"candidate_id": p.candidate_id, "llm_decision": "UNCERTAIN", "llm_reason": "No API key", "llm_called_at": now} for p in pairs]

    CHUNK_SIZE = 20
    all_outputs = []

    for chunk_idx in range(0, len(pairs), CHUNK_SIZE):
        chunk = pairs[chunk_idx:chunk_idx + CHUNK_SIZE]
        
        lines = []
        for i, p in enumerate(chunk, 1):
            c_name = str(p.city_name or '(blank)')[:150].replace("\n", " ")
            c_dba = str(p.city_dba or '')[:100].replace("\n", " ")
            c_addr = str(p.city_address or '(blank)')[:250].replace("\n", " ")
            
            b_name = str(p.bludot_name or '(blank)')[:150].replace("\n", " ")
            b_dba = str(p.bludot_dba or '')[:100].replace("\n", " ")
            b_addr = str(p.bludot_address or '(blank)')[:250].replace("\n", " ")

            # UPDATED: Inject DBA directly into the text the AI reads
            c_display = f"{c_name}" + (f" (DBA: {c_dba})" if c_dba else "")
            b_display = f"{b_name}" + (f" (DBA: {b_dba})" if b_dba else "")

            lines.append(f"{i}. A: \"{c_display}\" / \"{c_addr}\"\n   B: \"{b_display}\" / \"{b_addr}\"")

        max_out = min(200 + len(chunk) * 40, 4096)
        prompt = BATCH_MATCH_PROMPT.format(pairs_block="\n".join(lines))

        result = _call_llm(prompt, max_tokens=max_out)

        if isinstance(result, dict) and "results" in result:
            result_list = result["results"]
        elif isinstance(result, list):
            result_list = result
        else:
            for p in chunk:
                all_outputs.append({"candidate_id" : p.candidate_id, "llm_decision" : "UNCERTAIN", "llm_reason" : "Parse error", "llm_called_at": now})
            continue

        result_by_id = {r.get("id"): r for r in result_list if isinstance(r, dict)}
        for i, p in enumerate(chunk, 1):
            r = result_by_id.get(i, {})
            all_outputs.append({
                "candidate_id" : p.candidate_id,
                "llm_decision" : r.get("decision", "UNCERTAIN"),
                "llm_reason"   : r.get("reason", ""),
                "llm_called_at": now,
            })
            
        if chunk_idx + CHUNK_SIZE < len(pairs):
            time.sleep(12)
            
    return all_outputs


def judge_single_pair(candidate_id, city_name, city_address, bludot_name, bludot_address, city_dba="", bludot_dba="") -> dict:
    pair = CandidatePair(
        candidate_id=candidate_id, 
        city_name=city_name, 
        city_address=city_address, 
        bludot_name=bludot_name, 
        bludot_address=bludot_address, 
        rule_reason="",
        city_dba=city_dba,
        bludot_dba=bludot_dba
    )
    results = judge_candidates([pair])
    return results[0] if results else {}


# ── Dedup pair judging ────────────────────────────────────────────────────────

@dataclass
class DedupPair:
    pair_id: str
    index_a: int
    index_b: int
    name_a: str
    address_a: str
    name_b: str
    address_b: str
    similarity: float
    intra_cluster: bool = field(default=False)


def judge_dedup_pairs(pairs: list[DedupPair]) -> list[dict]:
    if not pairs:
        return []

    if not has_api_key():
        return [{"pair_id": p.pair_id, "index_a": p.index_a, "index_b": p.index_b, "decision": "UNCERTAIN", "reason": "No API key"} for p in pairs]

    CHUNK_SIZE = 10
    all_outputs = []

    for chunk_idx in range(0, len(pairs), CHUNK_SIZE):
        chunk = pairs[chunk_idx:chunk_idx + CHUNK_SIZE]

        lines = []
        for i, p in enumerate(chunk, 1):
            n_a = str(p.name_a or '(blank)')[:250].replace("\n", " ")
            a_a = str(p.address_a or '(blank)')[:250].replace("\n", " ")
            n_b = str(p.name_b or '(blank)')[:250].replace("\n", " ")
            a_b = str(p.address_b or '(blank)')[:250].replace("\n", " ")

            lines.append(f"{i}. A: \"{n_a}\" / \"{a_a}\"\n   B: \"{n_b}\" / \"{a_b}\"   (similarity: {p.similarity:.0%})")

        max_out = min(200 + len(chunk) * 40, 4096)
        prompt = BATCH_DEDUP_PROMPT.format(pairs_block="\n".join(lines))

        result = _call_llm(prompt, max_tokens=max_out)

        if isinstance(result, dict) and "results" in result:
            result_list = result["results"]
        elif isinstance(result, list):
            result_list = result
        else:
            for p in chunk:
                all_outputs.append({"pair_id" : p.pair_id, "index_a" : p.index_a, "index_b" : p.index_b, "decision": "UNCERTAIN", "reason": "Parse error"})
            continue

        result_by_id = {r.get("id"): r for r in result_list if isinstance(r, dict)}
        for i, p in enumerate(chunk, 1):
            r = result_by_id.get(i, {})
            all_outputs.append({
                "pair_id" : p.pair_id,
                "index_a" : p.index_a,
                "index_b" : p.index_b,
                "decision": r.get("decision", "UNCERTAIN"),
                "reason"  : r.get("reason", ""),
            })

        if chunk_idx + CHUNK_SIZE < len(pairs):
            time.sleep(12)

    return all_outputs


# ── Column mapping suggestion ─────────────────────────────────────────────────

def suggest_column_mapping(city_columns: list[str], sample_rows: list[dict], bludot_custom_cols: list[str]) -> list[dict]:
    lines = []
    for col in city_columns:
        samples = [str(r.get(col, ''))[:100].replace("\n", " ") for r in sample_rows if str(r.get(col, '')).strip() not in ('', 'nan', 'None', 'NaT')][:5]
        sample_str = " | ".join(samples) if samples else "(all empty)"
        lines.append(f"  - {col}: {sample_str}")

    prompt = MAPPING_PROMPT.format(columns_with_samples="\n".join(lines), bludot_custom_cols=", ".join(bludot_custom_cols) if bludot_custom_cols else "(none)")

    if not has_api_key():
        return [{"source_col": c, "mapping_type": "skip", "target_col": "SKIP", "meta": {}} for c in city_columns]

    result = _call_llm(prompt, max_tokens=4096)

    if isinstance(result, dict) and "mappings" in result:
        result_list = result["mappings"]
    elif isinstance(result, list):
        result_list = result
    else:
        result_list = []

    for item in result_list:
        if not isinstance(item.get("meta"), dict): item["meta"] = {}

    mapped_cols = {r.get("source_col") for r in result_list}
    for col in city_columns:
        if col not in mapped_cols:
            result_list.append({"source_col": col, "mapping_type": "skip", "target_col": "SKIP", "meta": {}})
    return result_list