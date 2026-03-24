"""
LLM Judge — Groq
================
Uses Groq's free API (llama-3.3-70b-versatile) instead of Gemini.

Groq free tier: 14,400 requests/day, 500,000 tokens/day — much more generous.
Set GROQ_API_KEY in your .env file.

Handles three use cases:
  1. Match judging   — single batched API call for ALL pairs at once
  2. Mapping suggest — suggest column mappings from city sheet columns
  3. Dedup pair judging — batched call for dedup near-misses
"""

import json
import os
import time
import logging
from datetime import datetime
from dataclasses import dataclass, field

from groq import Groq

logger = logging.getLogger(__name__)


# ── Client setup ──────────────────────────────────────────────────────────────

_client: Groq | None = None

def _get_client() -> Groq:
    global _client
    if _client is None:
        api_key = os.getenv("GROQ_API_KEY", "")
        if not api_key:
            raise RuntimeError("No GROQ_API_KEY configured")
        _client = Groq(api_key=api_key)
    return _client

def has_api_key() -> bool:
    return bool(os.getenv("GROQ_API_KEY", "").strip())


# ── Core Groq call ────────────────────────────────────────────────────────────

def _call_llm(prompt: str, max_tokens: int = 1024, retries: int = 3) -> dict | list:
    """Call Groq with retry on rate limit. Returns parsed JSON."""
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
                text = parts[1]
                if text.startswith("json"):
                    text = text[4:]
            text = text.strip()

            return json.loads(text)

        except json.JSONDecodeError as e:
            last_error = e
            raw = locals().get("text", "")[:400]
            logger.error(f"JSON parse error (attempt {attempt+1}): {e}\nRaw: {raw!r}")
            if attempt == 0:
                max_tokens = min(max_tokens * 2, 8192)
                continue
            break

        except Exception as e:
            last_error = e
            err_str = str(e)
            if "rate_limit" in err_str.lower() or "429" in err_str:
                wait = 2 ** attempt * 5
                logger.warning(f"Groq rate limit, waiting {wait}s")
                time.sleep(wait)
            elif "quota" in err_str.lower() or "limit" in err_str.lower():
                logger.warning("Groq quota exhausted — marking as UNCERTAIN")
                return {"decision": "UNCERTAIN", "reason": "Quota exhausted"}
            else:
                logger.error(f"Groq call failed: {e}")
                break

    return {"decision": "UNCERTAIN", "reason": f"LLM error: {last_error}"}


# ── Prompts ───────────────────────────────────────────────────────────────────

BATCH_MATCH_PROMPT = """You are an expert at matching business records from two data sources.

For each numbered pair below, decide if Record A and Record B refer to the SAME physical business.

RULES (apply to every pair):
1. Both street numbers present AND different → NOT same business.
2. One name clearly contained in the other → likely SAME business.
3. One/both addresses blank → judge by name only.
4. Missing street number in one → do not penalise.
5. Ignore legal suffixes (LLC, Inc, Corp, Ltd).
6. Ignore spelling/abbreviation differences (St/Street, Ave/Avenue).
7. DBA names vs legal names are acceptable variations.

PAIRS:
{pairs_block}

Respond with a JSON object containing a "results" array — one object per pair in the same order:
{{"results": [
  {{"id": 1, "decision": "MATCH", "reason": "one sentence"}},
  {{"id": 2, "decision": "NO_MATCH", "reason": "one sentence"}}
]}}
decision must be exactly: MATCH, NO_MATCH, or UNCERTAIN (= human should review)"""

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

@dataclass
class CandidatePair:
    candidate_id: int
    city_name: str
    city_address: str
    bludot_name: str
    bludot_address: str
    rule_reason: str


def judge_candidates(pairs: list[CandidatePair]) -> list[dict]:
    """Send ALL pairs to Groq in ONE API call."""
    if not pairs:
        return []

    now = datetime.utcnow().isoformat()

    if not has_api_key():
        return [{"candidate_id": p.candidate_id, "llm_decision": "UNCERTAIN",
                 "llm_reason": "No API key", "llm_called_at": now}
                for p in pairs]

    lines = []
    for i, p in enumerate(pairs, 1):
        lines.append(
            f"{i}. A: \"{p.city_name or '(blank)'}\" / \"{p.city_address or '(blank)'}\"\n"
            f"   B: \"{p.bludot_name or '(blank)'}\" / \"{p.bludot_address or '(blank)'}\""
        )

    max_out = min(200 + len(pairs) * 40, 4096)
    prompt = BATCH_MATCH_PROMPT.format(pairs_block="\n".join(lines))

    logger.info(f"Sending {len(pairs)} pairs to Groq in 1 batched call")
    result = _call_llm(prompt, max_tokens=max_out)

    if isinstance(result, dict) and "results" in result:
        result_list = result["results"]
    elif isinstance(result, list):
        result_list = result
    else:
        logger.error(f"Unexpected Groq response: {result}")
        return [{"candidate_id": p.candidate_id, "llm_decision": "UNCERTAIN",
                 "llm_reason": "Parse error", "llm_called_at": now}
                for p in pairs]

    result_by_id = {r.get("id"): r for r in result_list if isinstance(r, dict)}
    output = []
    for i, p in enumerate(pairs, 1):
        r = result_by_id.get(i, {})
        output.append({
            "candidate_id" : p.candidate_id,
            "llm_decision" : r.get("decision", "UNCERTAIN"),
            "llm_reason"   : r.get("reason", ""),
            "llm_called_at": now,
        })
    return output


def judge_single_pair(candidate_id, city_name, city_address, bludot_name, bludot_address) -> dict:
    pair = CandidatePair(candidate_id=candidate_id, city_name=city_name,
                         city_address=city_address, bludot_name=bludot_name,
                         bludot_address=bludot_address, rule_reason="")
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
    """Send ALL dedup pairs in ONE API call."""
    if not pairs:
        return []

    if not has_api_key():
        return [{"pair_id": p.pair_id, "index_a": p.index_a, "index_b": p.index_b,
                 "decision": "UNCERTAIN", "reason": "No API key"}
                for p in pairs]

    lines = []
    for i, p in enumerate(pairs, 1):
        lines.append(
            f"{i}. A: \"{p.name_a or '(blank)'}\" / \"{p.address_a or '(blank)'}\"\n"
            f"   B: \"{p.name_b or '(blank)'}\" / \"{p.address_b or '(blank)'}\""
            f"   (similarity: {p.similarity:.0%})"
        )

    max_out = min(200 + len(pairs) * 40, 4096)
    prompt = BATCH_DEDUP_PROMPT.format(pairs_block="\n".join(lines))

    logger.info(f"Sending {len(pairs)} dedup pairs to Groq in 1 batched call")
    result = _call_llm(prompt, max_tokens=max_out)

    if isinstance(result, dict) and "results" in result:
        result_list = result["results"]
    elif isinstance(result, list):
        result_list = result
    else:
        return [{"pair_id": p.pair_id, "index_a": p.index_a, "index_b": p.index_b,
                 "decision": "UNCERTAIN", "reason": "Parse error"}
                for p in pairs]

    result_by_id = {r.get("id"): r for r in result_list if isinstance(r, dict)}
    output = []
    for i, p in enumerate(pairs, 1):
        r = result_by_id.get(i, {})
        output.append({
            "pair_id" : p.pair_id,
            "index_a" : p.index_a,
            "index_b" : p.index_b,
            "decision": r.get("decision", "UNCERTAIN"),
            "reason"  : r.get("reason", ""),
        })
    return output


# ── Column mapping suggestion ─────────────────────────────────────────────────

def suggest_column_mapping(
    city_columns: list[str],
    sample_rows: list[dict],
    bludot_custom_cols: list[str],
) -> list[dict]:
    lines = []
    for col in city_columns:
        samples = [
            str(r.get(col, '')) for r in sample_rows
            if str(r.get(col, '')).strip() not in ('', 'nan', 'None', 'NaT')
        ][:5]
        sample_str = " | ".join(samples) if samples else "(all empty)"
        lines.append(f"  - {col}: {sample_str}")

    prompt = MAPPING_PROMPT.format(
        columns_with_samples="\n".join(lines),
        bludot_custom_cols=", ".join(bludot_custom_cols) if bludot_custom_cols else "(none)",
    )

    if not has_api_key():
        return [{"source_col": c, "mapping_type": "skip", "target_col": "SKIP", "meta": {}}
                for c in city_columns]

    result = _call_llm(prompt, max_tokens=4096)

    if isinstance(result, dict) and "mappings" in result:
        result_list = result["mappings"]
    elif isinstance(result, list):
        result_list = result
    else:
        result_list = []

    for item in result_list:
        if not isinstance(item.get("meta"), dict):
            item["meta"] = {}

    mapped_cols = {r.get("source_col") for r in result_list}
    for col in city_columns:
        if col not in mapped_cols:
            result_list.append({"source_col": col, "mapping_type": "skip",
                                 "target_col": "SKIP", "meta": {}})
    return result_list
