"""
Gemini LLM Judge
================
Handles two use cases:
  1. Match judging   — MATCH / NO_MATCH / UNCERTAIN for city vs bludot pairs
  2. Mapping suggest — suggest column mappings from city sheet columns + sample data
  3. Dedup review    — flag near-miss clusters the LSH missed

Multiple API keys supported via round-robin rotation to avoid rate limits.
Set GEMINI_API_KEY as comma-separated list: key1,key2,key3
"""

import json
import os
import time
import logging
from datetime import datetime
from dataclasses import dataclass
from typing import TypedDict

import google.generativeai as genai
from langgraph.graph import StateGraph, END

logger = logging.getLogger(__name__)

# ── Multi-key round-robin ──────────────────────────────────────────────────────

_ALL_KEYS: list[str] = [
    k.strip() for k in os.getenv("GEMINI_API_KEY", "").split(",") if k.strip()
]
_key_index = 0

def _get_model() -> "genai.GenerativeModel":
    """Return a model configured with the next API key (round-robin)."""
    global _key_index
    if not _ALL_KEYS:
        raise RuntimeError("No GEMINI_API_KEY configured")
    key = _ALL_KEYS[_key_index % len(_ALL_KEYS)]
    _key_index += 1
    genai.configure(api_key=key)
    return genai.GenerativeModel("gemini-2.5-flash")

def has_api_key() -> bool:
    return len(_ALL_KEYS) > 0


# ── Prompts ───────────────────────────────────────────────────────────────────

MATCH_PROMPT = """You are an expert at matching business records from two different data sources.

Determine if Record A and Record B refer to the SAME physical business location.

Record A (City Data):
  Business Name : {city_name}
  Address       : {city_address}

Record B (Our Database):
  Business Name : {bludot_name}
  Address       : {bludot_address}

RULES:
1. Both street numbers present AND different → NOT same business.
2. One name clearly contained in the other → likely SAME business.
3. One/both addresses blank → judge by name only.
4. Missing street number in one → do not penalise.
5. Ignore legal suffixes (LLC, Inc, Corp, Ltd).
6. Ignore spelling differences, abbreviations (St/Street, Ave/Avenue).
7. DBA names vs legal names are acceptable variations.

Respond with JSON ONLY:
{{"decision":"MATCH"|"NO_MATCH"|"UNCERTAIN","reason":"one sentence","name_similarity":"high"|"medium"|"low","address_similarity":"high"|"medium"|"low"|"not_applicable"}}

UNCERTAIN = human should review."""

DEDUP_PROMPT = """Two business records were NOT clustered together by the deduplication algorithm but have high similarity ({similarity:.0%}). Should they be considered DUPLICATES of the same business?

Record A:
  Business Name : {name_a}
  Address       : {address_a}

Record B:
  Business Name : {name_b}
  Address       : {address_b}

RULES:
1. Different street numbers → NOT duplicates.
2. Same name, one address blank → likely duplicates.
3. Abbreviations/spelling variants of same name → likely duplicates.

Respond with JSON ONLY:
{{"decision":"DUPLICATE"|"NOT_DUPLICATE"|"UNCERTAIN","reason":"one sentence"}}"""

MAPPING_PROMPT = """You are mapping columns from a city business license sheet to a standard schema.

City sheet columns with sample values:
{columns_with_samples}

Bludot custom data columns (existing fields to map custom data into):
{bludot_custom_cols}

BUSINESS SCHEMA fields (map each column to one of these or SKIP or CUSTOM):
Business Name, Address1, Address2, City, State, Country, Zipcode, Phonenumber, Website, Lat, Long, DBA Name, Business Operational Status

CONTACT fields (if a column has contact info like person names/emails/phones):
Use type: "contact" with role (Owner/Manager/Agent/Contact) and contact_type (email/phone_number/address)

CUSTOM fields (any data that doesn't fit business schema):
If it matches a bludot custom column, use that name. Otherwise propose a new label.

Respond with JSON ONLY — array of mapping objects:
[
  {{"source_col":"Original Column Name","mapping_type":"business","target_col":"Business Name"}},
  {{"source_col":"Owner Email","mapping_type":"contact","target_col":"[email]","meta":{{"role":"Owner","contact_type":"email","person_col":"Owner Name"}}}},
  {{"source_col":"License Type","mapping_type":"custom","target_col":"License Type","meta":{{"bludot_custom_col":"License Type or empty if new"}}}},
  {{"source_col":"Irrelevant Col","mapping_type":"skip","target_col":"SKIP"}}
]
Return ONLY the JSON array, no explanation."""


# ── Core Gemini call with retry + key rotation ────────────────────────────────

def _call_gemini(prompt: str, retries: int = 3) -> dict | list:
    for attempt in range(retries):
        try:
            model = _get_model()
            response = model.generate_content(
                prompt,
                generation_config=genai.types.GenerationConfig(
                    temperature=0.0,
                    max_output_tokens=1024,
                ),
            )
            text = response.text.strip().replace("```json", "").replace("```", "").strip()
            return json.loads(text)
        except Exception as e:
            if "429" in str(e) or "quota" in str(e).lower():
                # Try next key immediately before waiting
                if len(_ALL_KEYS) > 1:
                    logger.warning(f"Key {(_key_index-1) % len(_ALL_KEYS)} rate limited, rotating to next key")
                    continue
                wait = 2 ** attempt * 5
                logger.warning(f"Rate limit hit, waiting {wait}s")
                time.sleep(wait)
            else:
                logger.error(f"Gemini call failed: {e}")
                return {"decision": "UNCERTAIN", "reason": f"LLM error: {e}"}
    return {"decision": "UNCERTAIN", "reason": "Max retries exceeded"}


# ── Match judging (existing) ───────────────────────────────────────────────────

@dataclass
class CandidatePair:
    candidate_id: int
    city_name: str
    city_address: str
    bludot_name: str
    bludot_address: str
    rule_reason: str


class JudgeState(TypedDict):
    pairs: list[CandidatePair]
    current_index: int
    results: list[dict]
    errors: list[str]


def _process_next_pair(state: JudgeState) -> JudgeState:
    idx  = state["current_index"]
    pair = state["pairs"][idx]
    prompt = MATCH_PROMPT.format(
        city_name      = pair.city_name     or "(blank)",
        city_address   = pair.city_address  or "(blank)",
        bludot_name    = pair.bludot_name   or "(blank)",
        bludot_address = pair.bludot_address or "(blank)",
    )
    llm_result = _call_gemini(prompt)
    time.sleep(0.35)  # stay under 15 RPM per key
    return {
        **state,
        "current_index": idx + 1,
        "results": state["results"] + [{
            "candidate_id" : pair.candidate_id,
            "llm_decision" : llm_result.get("decision", "UNCERTAIN"),
            "llm_reason"   : llm_result.get("reason", ""),
            "llm_called_at": datetime.utcnow().isoformat(),
        }],
    }


def _should_continue(state: JudgeState) -> str:
    return "process" if state["current_index"] < len(state["pairs"]) else END


_judge_graph = None

def _get_judge_graph():
    global _judge_graph
    if _judge_graph is None:
        g = StateGraph(JudgeState)
        g.add_node("process", _process_next_pair)
        g.set_entry_point("process")
        g.add_conditional_edges("process", _should_continue, {"process": "process", END: END})
        _judge_graph = g.compile()
    return _judge_graph


def judge_candidates(pairs: list[CandidatePair]) -> list[dict]:
    """Run pairs through Gemini. Returns list with llm_decision per pair."""
    if not pairs:
        return []
    if not has_api_key():
        logger.warning("No GEMINI_API_KEY — all marked UNCERTAIN")
        return [{"candidate_id": p.candidate_id, "llm_decision": "UNCERTAIN",
                 "llm_reason": "No API key", "llm_called_at": datetime.utcnow().isoformat()}
                for p in pairs]
    final = _get_judge_graph().invoke({
        "pairs": pairs, "current_index": 0, "results": [], "errors": []
    })
    return final["results"]


def judge_single_pair(candidate_id, city_name, city_address, bludot_name, bludot_address) -> dict:
    pair = CandidatePair(candidate_id=candidate_id, city_name=city_name,
                         city_address=city_address, bludot_name=bludot_name,
                         bludot_address=bludot_address, rule_reason="")
    results = judge_candidates([pair])
    return results[0] if results else {}


# ── Dedup near-miss review ────────────────────────────────────────────────────

@dataclass
class DedupPair:
    pair_id: str          # "{index_a}_{index_b}"
    index_a: int
    index_b: int
    name_a: str
    address_a: str
    name_b: str
    address_b: str
    similarity: float


def judge_dedup_pairs(pairs: list[DedupPair]) -> list[dict]:
    """
    Ask Gemini whether near-miss pairs missed by LSH are actually duplicates.
    Returns list of {pair_id, decision: DUPLICATE|NOT_DUPLICATE|UNCERTAIN, reason}
    """
    if not pairs:
        return []
    results = []
    for pair in pairs:
        if not has_api_key():
            results.append({"pair_id": pair.pair_id, "decision": "UNCERTAIN",
                            "reason": "No API key"})
            continue
        prompt = DEDUP_PROMPT.format(
            similarity=pair.similarity,
            name_a=pair.name_a or "(blank)", address_a=pair.address_a or "(blank)",
            name_b=pair.name_b or "(blank)", address_b=pair.address_b or "(blank)",
        )
        result = _call_gemini(prompt)
        time.sleep(0.35)
        results.append({
            "pair_id"  : pair.pair_id,
            "index_a"  : pair.index_a,
            "index_b"  : pair.index_b,
            "decision" : result.get("decision", "UNCERTAIN"),
            "reason"   : result.get("reason", ""),
        })
    return results


# ── Column mapping suggestion ─────────────────────────────────────────────────

def suggest_column_mapping(
    city_columns: list[str],
    sample_rows: list[dict],
    bludot_custom_cols: list[str],
) -> list[dict]:
    """
    Ask Gemini to suggest mappings for all city sheet columns.
    Returns list of mapping dicts ready to be shown in the UI.
    """
    # Build column+sample summary
    lines = []
    for col in city_columns:
        samples = [str(r.get(col, '')) for r in sample_rows if r.get(col, '')][:3]
        sample_str = " | ".join(samples) if samples else "(empty)"
        lines.append(f"  - {col}: {sample_str}")
    columns_with_samples = "\n".join(lines)
    bludot_str = ", ".join(bludot_custom_cols) if bludot_custom_cols else "(none)"

    prompt = MAPPING_PROMPT.format(
        columns_with_samples=columns_with_samples,
        bludot_custom_cols=bludot_str,
    )

    if not has_api_key():
        # Fallback: return empty mappings so UI still loads
        return [{"source_col": c, "mapping_type": "business", "target_col": "SKIP", "meta": {}}
                for c in city_columns]

    result = _call_gemini(prompt)

    # result should be a list; if Gemini returned a dict wrap it
    if isinstance(result, dict):
        result = [result]

    # Ensure every source column has a mapping (fill in SKIP for any missing)
    mapped_cols = {r.get("source_col") for r in result}
    for col in city_columns:
        if col not in mapped_cols:
            result.append({"source_col": col, "mapping_type": "skip",
                           "target_col": "SKIP", "meta": {}})
    return result