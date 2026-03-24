"""
Database Models
===============
SQLAlchemy ORM models — replaces all Excel intermediate files.

Tables:
  cities              — one row per city/county being processed
  pipeline_runs       — one row per pipeline execution
  step_logs           — one row per step per run (status + stats)
  column_mappings     — city sheet column → our schema mapping
  city_records        — deduplicated city sheet records
  bludot_records      — concatenated bludot export records
  match_candidates    — every candidate pair with full decision chain
  dedup_review_pairs  — near-miss dedup pairs flagged for review
"""

from datetime import datetime

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, JSON, String, Text,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


# ── Enums (stored as strings) ─────────────────────────────────────────────────

class PipelineStatus:
    NOT_STARTED = "not_started"
    RUNNING     = "running"
    PAUSED      = "paused"
    COMPLETED   = "completed"
    FAILED      = "failed"


class PipelineStep:
    STEP0_DEDUP         = "step0_dedup"
    STEP0_DEDUP_REVIEW  = "step0_dedup_review"
    STEP1_FORMAT        = "step1_2_format"
    STEP2_MATCH         = "step2_match"
    STEP2_REVIEW        = "step2_review"
    STEP4_SPLIT         = "step4_split"
    STEP4_1_MATCH       = "step4_1_extra_match"
    STEP4_1_REVIEW      = "step4_1_review"
    STEP5_FINAL         = "step5_final_sheets"
    DONE                = "done"


class MatchDecision:
    AUTO_MATCH     = "AUTO_MATCH"
    AUTO_REJECT    = "AUTO_REJECT"
    AUTO_NO_MATCH  = "AUTO_REJECT"    # alias used by matching_orchestrator
    NEEDS_REVIEW   = "NEEDS_REVIEW"
    HUMAN_ACCEPT   = "HUMAN_ACCEPT"
    HUMAN_ACCEPTED = "HUMAN_ACCEPT"   # alias used by matching_orchestrator
    HUMAN_REJECT   = "HUMAN_REJECT"
    HUMAN_REJECTED = "HUMAN_REJECT"   # alias used by matching_orchestrator


class DedupDecision:
    DUPLICATE     = "DUPLICATE"
    NOT_DUPLICATE = "NOT_DUPLICATE"
    UNCERTAIN     = "UNCERTAIN"


# ── Core tables ───────────────────────────────────────────────────────────────

class City(Base):
    __tablename__ = "cities"

    id                 = Column(Integer, primary_key=True, autoincrement=True)
    name               = Column(String,  nullable=False)
    city_or_county     = Column(String,  default="City")
    raw_data_path      = Column(String,  nullable=False)
    bludot_export_path = Column(String,  nullable=False)
    created_at         = Column(DateTime, default=datetime.utcnow)
    updated_at         = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    pipeline_runs   = relationship("PipelineRun",    back_populates="city", cascade="all, delete-orphan")
    column_mappings = relationship("ColumnMapping",  back_populates="city", cascade="all, delete-orphan")
    city_records    = relationship("CityRecord",     back_populates="city", cascade="all, delete-orphan")
    bludot_records  = relationship("BludotRecord",   back_populates="city", cascade="all, delete-orphan")
    match_candidates= relationship("MatchCandidate", back_populates="city", cascade="all, delete-orphan")
    dedup_pairs     = relationship("DedupReviewPair",back_populates="city", cascade="all, delete-orphan")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id           = Column(Integer,  primary_key=True, autoincrement=True)
    city_id      = Column(Integer,  ForeignKey("cities.id"), nullable=False)
    status       = Column(String,   default=PipelineStatus.NOT_STARTED)
    current_step = Column(String,   nullable=True)
    error_log    = Column(Text,     nullable=True)
    started_at   = Column(DateTime, default=datetime.utcnow)
    updated_at   = Column(DateTime, default=datetime.utcnow)

    city      = relationship("City",     back_populates="pipeline_runs")
    step_logs = relationship("StepLog",  back_populates="run", cascade="all, delete-orphan",
                             order_by="StepLog.id")


class StepLog(Base):
    __tablename__ = "step_logs"

    id         = Column(Integer,  primary_key=True, autoincrement=True)
    run_id     = Column(Integer,  ForeignKey("pipeline_runs.id"), nullable=False)
    step       = Column(String,   nullable=False)
    status     = Column(String,   default="pending")   # running | completed | failed | paused
    message    = Column(String,   default="")
    stats      = Column(JSON,     default=dict)
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at   = Column(DateTime, nullable=True)

    run = relationship("PipelineRun", back_populates="step_logs")


# ── Column mapping ────────────────────────────────────────────────────────────

class ColumnMapping(Base):
    __tablename__ = "column_mappings"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    city_id      = Column(Integer, ForeignKey("cities.id"), nullable=False)
    source_col   = Column(String,  nullable=False)
    target_col   = Column(String,  nullable=False)
    mapping_type = Column(String,  default="business")   # business | contact | custom | skip
    meta         = Column(JSON,    nullable=True)         # role, contact_type, bludot_custom_col, etc.
    created_at   = Column(DateTime, default=datetime.utcnow)

    city = relationship("City", back_populates="column_mappings")


# ── Data tables ───────────────────────────────────────────────────────────────

class CityRecord(Base):
    __tablename__ = "city_records"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    city_id       = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city_index    = Column(Integer, nullable=False)       # row index in the city sheet
    business_name = Column(String,  default="")
    address1      = Column(String,  default="")
    cluster_id    = Column(Integer, nullable=True)        # LSH cluster assignment
    raw_data      = Column(JSON,    nullable=True)        # full row as dict
    created_at    = Column(DateTime, default=datetime.utcnow)

    city = relationship("City", back_populates="city_records")


class BludotRecord(Base):
    __tablename__ = "bludot_records"

    id           = Column(Integer, primary_key=True, autoincrement=True)
    city_id      = Column(Integer, ForeignKey("cities.id"), nullable=False)
    bludot_index = Column(Integer, nullable=False)        # row index in bludot export
    uuid         = Column(String,  default="")
    name         = Column(String,  default="")
    address1     = Column(String,  default="")
    raw_data     = Column(JSON,    nullable=True)         # full row as dict
    created_at   = Column(DateTime, default=datetime.utcnow)

    city = relationship("City", back_populates="bludot_records")


# ── Matching tables ───────────────────────────────────────────────────────────

class MatchCandidate(Base):
    __tablename__ = "match_candidates"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    city_id         = Column(Integer, ForeignKey("cities.id"), nullable=False)
    city_rec_id     = Column(Integer, ForeignKey("city_records.id"),   nullable=True)
    bludot_rec_id   = Column(Integer, ForeignKey("bludot_records.id"), nullable=True)
    match_pass      = Column(Integer, default=1)          # 1 = first pass, 2 = second pass

    # Scores
    name_score       = Column(Float, default=0.0)
    address_score    = Column(Float, default=0.0)
    street_num_match = Column(String, nullable=True)   # "match" | "mismatch" | "missing" | None

    # Stage 1: rule-based filter
    rule_verdict    = Column(String, nullable=True)       # DEFINITE_MATCH | DEFINITE_NO_MATCH | CANDIDATE
    rule_reason     = Column(String, default="")

    # Stage 2: LLM judge
    llm_decision    = Column(String, nullable=True)       # MATCH | NO_MATCH | UNCERTAIN
    llm_reason      = Column(String, default="")
    llm_called_at   = Column(DateTime, nullable=True)

    # Stage 3: human review
    human_decision  = Column(String, nullable=True)       # ACCEPT | REJECT
    reviewed_by     = Column(String, nullable=True)
    reviewed_at     = Column(DateTime, nullable=True)
    review_note     = Column(String, default="")

    # Final routed decision
    final_decision  = Column(String, default=MatchDecision.NEEDS_REVIEW)

    created_at      = Column(DateTime, default=datetime.utcnow)

    city         = relationship("City",         back_populates="match_candidates")
    city_record  = relationship("CityRecord",   foreign_keys=[city_rec_id])
    bludot_record= relationship("BludotRecord", foreign_keys=[bludot_rec_id])


class DedupReviewPair(Base):
    __tablename__ = "dedup_review_pairs"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    city_id       = Column(Integer, ForeignKey("cities.id"), nullable=False)
    index_a       = Column(Integer, nullable=False)
    index_b       = Column(Integer, nullable=False)
    name_a        = Column(String,  default="")
    address_a     = Column(String,  default="")
    name_b        = Column(String,  default="")
    address_b     = Column(String,  default="")
    similarity    = Column(Float,   default=0.0)
    intra_cluster = Column(Boolean, default=False)   # True = same cluster suspicious pair
    llm_reason    = Column(String,  default="")
    decision      = Column(String,  default=DedupDecision.UNCERTAIN)
    reviewed_by   = Column(String,  nullable=True)
    reviewed_at   = Column(DateTime, nullable=True)
    created_at    = Column(DateTime, default=datetime.utcnow)

    city = relationship("City", back_populates="dedup_pairs")