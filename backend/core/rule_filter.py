"""
Rule-Based Pre-Filter  (Stage 1 of the 3-stage matching engine)

Implements the EXACT matching rules described by the team:
  - Street numbers both present AND different  → DEFINITE_NO_MATCH
  - One name is a substring of the other AND names are somewhat similar → CANDIDATE
  - One or both addresses blank → rely on name only → CANDIDATE if name similar enough
  - Street number blank in one/both → CANDIDATE (defer to LLM)
  - Names clearly different AND addresses different → DEFINITE_NO_MATCH
  - Everything else → CANDIDATE (send to Gemini)

Returns one of: "DEFINITE_MATCH" | "DEFINITE_NO_MATCH" | "CANDIDATE"
"""

import re
from dataclasses import dataclass
from rapidfuzz import fuzz
import pickle, os


# ─── Load abbreviation dict (same pkl used by existing pipeline) ─────────────
_PKL_PATH = os.path.join(os.path.dirname(__file__), "..", "..", "src", "updated_pickle.pkl")
try:
    with open(_PKL_PATH, "rb") as f:
        _ABBREV = pickle.load(f)
except FileNotFoundError:
    _ABBREV = {}


# ─── Text normalisation (mirrors existing pipeline logic) ────────────────────

def _expand_abbrev(word: str) -> str:
    return _ABBREV.get(word, word)


def normalize_name(name: str) -> str:
    if not name:
        return ""
    name = name.lower()
    # Strip common legal suffixes
    for suffix in [" llc", " inc", " corporation", " corp", " company",
                   " co", " ltd", " limited", " pllc", " group", " associates"]:
        name = name.replace(suffix, "")
    name = name.replace("&", "and")
    name = re.sub(r"[^\w\s]", " ", name)
    name = " ".join(_expand_abbrev(w) for w in name.split())
    return name.strip()


def normalize_address(address: str) -> str:
    if not address:
        return ""
    address = address.lower()
    abbrevs = {
        "street": "st", "avenue": "ave", "boulevard": "blvd",
        "drive": "dr", "highway": "hwy", "lane": "ln",
        "parkway": "pkwy", "place": "pl", "road": "rd",
        "suite": "ste", "north": "n", "south": "s",
        "east": "e", "west": "w",
    }
    for full, short in abbrevs.items():
        address = re.sub(r"\b" + full + r"\b", short, address)
    address = re.sub(r"[^\w\s\-#]", " ", address)
    return re.sub(r"\s+", " ", address).strip()


def extract_street_number(address: str) -> str:
    """Return the leading numeric part of an address, or '' if none."""
    if not address:
        return ""
    m = re.match(r"^(\d+[\w\-]?)", address.strip())
    return m.group(1) if m else ""


def is_po_box(address: str) -> bool:
    if not address:
        return False
    return bool(re.match(r"^p\.?o\.?\s*box", address.strip().lower()))


# ─── Scoring helpers ─────────────────────────────────────────────────────────

def name_similarity(a: str, b: str) -> float:
    """Highest of ratio and token_sort_ratio, 0-100."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    return max(fuzz.ratio(na, nb), fuzz.token_sort_ratio(na, nb))


def address_similarity(a: str, b: str) -> float:
    """Street-name portion similarity after stripping street number."""
    na = normalize_address(a)
    nb = normalize_address(b)
    # Strip leading number token
    na_street = re.sub(r"^\d+[\w\-]?\s*", "", na)
    nb_street = re.sub(r"^\d+[\w\-]?\s*", "", nb)
    if not na_street or not nb_street:
        return 0.0
    return fuzz.token_sort_ratio(na_street, nb_street)


def is_substring_match(a: str, b: str) -> bool:
    """True if the shorter normalised name is a substring of the longer."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return False
    shorter, longer = (na, nb) if len(na) <= len(nb) else (nb, na)
    return shorter in longer


# ─── Main rule filter ────────────────────────────────────────────────────────

@dataclass
class RuleResult:
    verdict: str          # DEFINITE_MATCH | DEFINITE_NO_MATCH | CANDIDATE
    reason: str
    name_score: float
    address_score: float
    street_num_match: bool | None   # None = one/both blank


def apply_rule_filter(
    city_name: str,
    city_address: str,
    bludot_name: str,
    bludot_address: str,
) -> RuleResult:
    """
    Encode the team's exact business rules.
    Returns a RuleResult with the verdict and supporting scores.
    """
    city_addr_clean  = (city_address  or "").strip()
    bludot_addr_clean= (bludot_address or "").strip()

    city_addr_blank  = city_addr_clean  in ("", "-", "nan")
    bludot_addr_blank= bludot_addr_clean in ("", "-", "nan")

    name_score    = name_similarity(city_name, bludot_name)
    addr_score    = address_similarity(city_address, bludot_address)
    substr_match  = is_substring_match(city_name, bludot_name)

    city_num   = extract_street_number(city_addr_clean)
    bludot_num = extract_street_number(bludot_addr_clean)

    both_num_present = bool(city_num) and bool(bludot_num)
    one_num_blank    = (bool(city_num) != bool(bludot_num))
    both_num_blank   = not city_num and not bludot_num

    # Determine street_num_match flag for DB storage
    if both_num_present:
        street_num_match = (city_num == bludot_num)
    else:
        street_num_match = None   # indeterminate

    # ── RULE 1: Street numbers both present AND different → hard reject ───────
    if both_num_present and city_num != bludot_num:
        return RuleResult(
            verdict="DEFINITE_NO_MATCH",
            reason=f"Street numbers differ: '{city_num}' vs '{bludot_num}'",
            name_score=name_score,
            address_score=addr_score,
            street_num_match=False,
        )

    # ── RULE 2: Names completely dissimilar AND addresses clearly different ───
    if name_score < 35 and addr_score < 35 and not substr_match:
        return RuleResult(
            verdict="DEFINITE_NO_MATCH",
            reason=f"Names and addresses both dissimilar (name={name_score:.0f}, addr={addr_score:.0f})",
            name_score=name_score,
            address_score=addr_score,
            street_num_match=street_num_match,
        )

    # ── RULE 3: Both addresses blank — name-only judgment needed ─────────────
    if city_addr_blank and bludot_addr_blank:
        if name_score >= 83 or substr_match:
            return RuleResult(
                verdict="CANDIDATE",
                reason="Both addresses blank; name is similar — defer to LLM",
                name_score=name_score,
                address_score=addr_score,
                street_num_match=None,
            )
        return RuleResult(
            verdict="DEFINITE_NO_MATCH",
            reason=f"Both addresses blank and names too dissimilar (name={name_score:.0f})",
            name_score=name_score,
            address_score=addr_score,
            street_num_match=None,
        )

    # ── RULE 4: One address blank — defer to LLM if name is plausible ────────
    if city_addr_blank or bludot_addr_blank:
        if name_score >= 60 or substr_match:
            return RuleResult(
                verdict="CANDIDATE",
                reason="One address blank; name plausible — defer to LLM",
                name_score=name_score,
                address_score=addr_score,
                street_num_match=None,
            )
        return RuleResult(
            verdict="DEFINITE_NO_MATCH",
            reason=f"One address blank and names dissimilar (name={name_score:.0f})",
            name_score=name_score,
            address_score=addr_score,
            street_num_match=None,
        )

    # ── RULE 5: Street number blank in one/both — defer to LLM ───────────────
    if one_num_blank or both_num_blank:
        if name_score >= 55 or substr_match:
            return RuleResult(
                verdict="CANDIDATE",
                reason="Street number absent in one/both records — defer to LLM",
                name_score=name_score,
                address_score=addr_score,
                street_num_match=None,
            )
        return RuleResult(
            verdict="DEFINITE_NO_MATCH",
            reason=f"Street number missing and names dissimilar (name={name_score:.0f})",
            name_score=name_score,
            address_score=addr_score,
            street_num_match=None,
        )

    # ── RULE 6: Street numbers match exactly ─────────────────────────────────
    # Both present and equal — if names are close, strong candidate
    if both_num_present and city_num == bludot_num:
        if name_score >= 55 or substr_match:
            return RuleResult(
                verdict="CANDIDATE",
                reason=f"Street numbers match exactly; name similar (name={name_score:.0f})",
                name_score=name_score,
                address_score=addr_score,
                street_num_match=True,
            )
        # Same number but very different name — unusual, still send to LLM
        return RuleResult(
            verdict="CANDIDATE",
            reason=f"Street numbers match but names differ (name={name_score:.0f}) — LLM to decide",
            name_score=name_score,
            address_score=addr_score,
            street_num_match=True,
        )

    # ── Fallback: send to LLM ────────────────────────────────────────────────
    return RuleResult(
        verdict="CANDIDATE",
        reason="No hard rule fired — defer to LLM",
        name_score=name_score,
        address_score=addr_score,
        street_num_match=street_num_match,
    )
