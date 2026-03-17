"""
SQLAlchemy ORM models.
SQLite now — swap DATABASE_URL to PostgreSQL anytime, zero code changes needed.
"""

from datetime import datetime
from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime,
    ForeignKey, Text, JSON, Enum as SAEnum
)
from sqlalchemy.orm import relationship, declarative_base
import enum

Base = declarative_base()


# ─────────────────────────────────────────────
#  Enums
# ─────────────────────────────────────────────

class PipelineStatus(str, enum.Enum):
    PENDING   = "pending"
    RUNNING   = "running"
    PAUSED    = "paused"       # waiting for human input
    COMPLETED = "completed"
    FAILED    = "failed"


class MatchDecision(str, enum.Enum):
    AUTO_MATCH     = "auto_match"       # LLM said MATCH, no human needed
    AUTO_NO_MATCH  = "auto_no_match"    # Rule or LLM said NO_MATCH
    NEEDS_REVIEW   = "needs_review"     # LLM said UNCERTAIN
    HUMAN_ACCEPTED = "human_accepted"   # Reviewer accepted
    HUMAN_REJECTED = "human_rejected"   # Reviewer rejected


class PipelineStep(str, enum.Enum):
    STEP_0_DEDUP        = "step0_dedup"
    STEP_1_2_FORMAT     = "step1_2_format"
    STEP_1_3_MERGE      = "step1_3_merge"
    STEP_2_MATCH        = "step2_match"
    STEP_2_REVIEW       = "step2_review"          # Human gate
    STEP_4_SPLIT        = "step4_split"
    STEP_4_1_EXTRA      = "step4_1_extra_match"   # Second-pass matcher
    STEP_4_1_REVIEW     = "step4_1_review"        # Human gate (if needed)
    STEP_5_FINAL        = "step5_final_sheets"
    STEP_6_CONTACTS     = "step6_contacts"
    DONE                = "done"


# ─────────────────────────────────────────────
#  City / Run
# ─────────────────────────────────────────────

class City(Base):
    __tablename__ = "cities"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    name            = Column(String, nullable=False)
    city_or_county  = Column(String, default="City")
    created_at      = Column(DateTime, default=datetime.utcnow)

    # file paths stored so we can re-read them
    raw_data_path       = Column(String)
    bludot_export_path  = Column(String)

    pipeline_runs   = relationship("PipelineRun",  back_populates="city", cascade="all, delete-orphan")
    column_mappings = relationship("ColumnMapping", back_populates="city", cascade="all, delete-orphan")
    city_records    = relationship("CityRecord",    back_populates="city", cascade="all, delete-orphan")
    bludot_records  = relationship("BludotRecord",  back_populates="city", cascade="all, delete-orphan")
    match_candidates= relationship("MatchCandidate",back_populates="city", cascade="all, delete-orphan")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    city_id      = Column(Integer, ForeignKey("cities.id"), nullable=False)
    current_step = Column(SAEnum(PipelineStep), default=PipelineStep.STEP_0_DEDUP)
    status       = Column(SAEnum(PipelineStatus), default=PipelineStatus.PENDING)
    started_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    error_log    = Column(Text, nullable=True)

    city = relationship("City", back_populates="pipeline_runs")
    step_logs = relationship("StepLog", back_populates="run", cascade="all, delete-orphan")


class StepLog(Base):
    __tablename__ = "step_logs"

    id         = Column(Integer, primary_key=True, autoincrement=True)
    run_id     = Column(Integer, ForeignKey("pipeline_runs.id"), nullable=False)
    step       = Column(SAEnum(PipelineStep))
    status     = Column(SAEnum(PipelineStatus))
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at   = Column(DateTime, nullable=True)
    message    = Column(Text, nullable=True)
    stats      = Column(JSON, nullable=True)   # e.g. {"matched": 120, "unmatched": 40}

    run = relationship("PipelineRun", back_populates="step_logs")


# ─────────────────────────────────────────────
#  Column Mapping (replaces city_details.py)
# ─────────────────────────────────────────────

class ColumnMapping(Base):
    """
    Stores how the user mapped the incoming city sheet columns
    to our internal schema via the UI.
    """
    __tablename__ = "column_mappings"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    city_id     = Column(Integer, ForeignKey("cities.id"), nullable=False)
    source_col  = Column(String, nullable=False)
    target_col  = Column(String, nullable=False)

    mapping_type = Column(String, default="business")  # business | contact | custom
    meta         = Column(JSON, nullable=True)

    created_at  = Column(DateTime, default=datetime.utcnow)

    city = relationship("City", back_populates="column_mappings")


class FieldMapping(Base):
    """
    Stores the business/custom/contact field mapping configuration
    (replaces BUSINESS_MATCHED_CITY_COLUMNS etc. in city_details.py)
    """
    __tablename__ = "field_mappings"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    city_id         = Column(Integer, ForeignKey("cities.id"), nullable=False)
    mapping_type    = Column(String)   # 'business', 'custom', 'contact'
    city_col        = Column(String)
    bludot_col      = Column(String)
    is_new_field    = Column(Boolean, default=False)


# ─────────────────────────────────────────────
#  Records
# ─────────────────────────────────────────────

class CityRecord(Base):
    __tablename__ = "city_records"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    city_id       = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city_index    = Column(Integer)          # original row index from dedup merged
    business_name = Column(String)
    address1      = Column(String)
    cluster_id    = Column(Integer, nullable=True)
    raw_data      = Column(JSON)             # full row stored as JSON

    city = relationship("City", back_populates="city_records")


class BludotRecord(Base):
    __tablename__ = "bludot_records"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    city_id       = Column(Integer, ForeignKey("cities.id"), nullable=False)
    bludot_index  = Column(Integer)
    uuid          = Column(String)
    name          = Column(String)
    address1      = Column(String)
    raw_data      = Column(JSON)

    city = relationship("City", back_populates="bludot_records")


# ─────────────────────────────────────────────
#  Matching
# ─────────────────────────────────────────────

class MatchCandidate(Base):
    """
    One row = one (city_record, bludot_record) candidate pair.
    Tracks the full decision chain: rule → LLM → human.
    """
    __tablename__ = "match_candidates"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    city_id         = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city_rec_id     = Column(Integer, ForeignKey("city_records.id"), nullable=False)
    bludot_rec_id   = Column(Integer, ForeignKey("bludot_records.id"), nullable=False)

    # Scores from rapidfuzz
    name_score      = Column(Float, nullable=True)
    address_score   = Column(Float, nullable=True)
    street_num_match= Column(Boolean, nullable=True)   # True/False/None(blank)

    # Stage 1: rule-based verdict
    rule_verdict    = Column(String, nullable=True)   # DEFINITE_MATCH / DEFINITE_NO_MATCH / CANDIDATE

    # Stage 2: LLM verdict
    llm_decision    = Column(String, nullable=True)   # MATCH / NO_MATCH / UNCERTAIN
    llm_reason      = Column(Text, nullable=True)
    llm_called_at   = Column(DateTime, nullable=True)

    # Stage 3: human decision
    human_decision  = Column(SAEnum(MatchDecision), nullable=True)
    reviewed_by     = Column(String, nullable=True)
    reviewed_at     = Column(DateTime, nullable=True)
    review_note     = Column(Text, nullable=True)

    # Final resolved decision
    final_decision  = Column(SAEnum(MatchDecision), nullable=True)
    match_pass      = Column(Integer, default=1)   # 1 = step2, 2 = step4.1 second pass

    city         = relationship("City",         back_populates="match_candidates")
    city_record  = relationship("CityRecord",   foreign_keys=[city_rec_id])
    bludot_record= relationship("BludotRecord", foreign_keys=[bludot_rec_id])


# ─────────────────────────────────────────────
#  Dedup Review (near-miss pairs flagged by LLM)
# ─────────────────────────────────────────────

class DedupDecision(str, enum.Enum):
    DUPLICATE     = "DUPLICATE"
    NOT_DUPLICATE = "NOT_DUPLICATE"
    UNCERTAIN     = "UNCERTAIN"


class DedupReviewPair(Base):
    """
    Near-miss pair that LSH put in different clusters but LLM thinks
    might be duplicates. Stored for human review.
    """
    __tablename__ = "dedup_review_pairs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    city_id     = Column(Integer, ForeignKey("cities.id"), nullable=False)
    index_a     = Column(Integer)
    index_b     = Column(Integer)
    name_a      = Column(String)
    address_a   = Column(String)
    name_b      = Column(String)
    address_b   = Column(String)
    similarity  = Column(Float)
    llm_reason  = Column(Text, nullable=True)
    decision    = Column(String, default="UNCERTAIN")  # DUPLICATE|NOT_DUPLICATE|UNCERTAIN
    reviewed_by = Column(String, nullable=True)
    reviewed_at = Column(DateTime, nullable=True)
    created_at  = Column(DateTime, default=datetime.utcnow)

    city = relationship("City")