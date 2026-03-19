"""
Pydantic request/response models for Smart Recon FastAPI backend.
"""
from __future__ import annotations
from typing import Any, Optional
from pydantic import BaseModel


# ── Upload ────────────────────────────────────────────────────────────────────

class UploadResponse(BaseModel):
    session_id: str
    rows_source: int
    rows_mapping: int
    rows_target: int
    rows_hierarchy: int
    validation_warnings: list[str]
    source_schema_hash: str
    mapping_schema_hash: str
    target_schema_hash: str


# ── Profile ───────────────────────────────────────────────────────────────────

class ProfileRequest(BaseModel):
    session_id: str


class ProfileResponse(BaseModel):
    session_id: str
    source: dict[str, Any]
    mapping: dict[str, Any]
    target: dict[str, Any]
    mapping_coverage: dict[str, Any]
    fingerprint_hash: str


# ── Reconcile ─────────────────────────────────────────────────────────────────

class ReconcileRequest(BaseModel):
    session_id: str


class GapInfo(BaseModel):
    our_value: float
    target_value: float
    gap: float
    gap_pct: Optional[float]
    has_gap: bool


class ReconcileResponse(BaseModel):
    session_id: str
    gap_summary: dict[str, GapInfo]
    total_gap: float
    primary_metric: str
    primary_gap: float
    dvt_rows: list[dict[str, Any]]


# ── Investigate ───────────────────────────────────────────────────────────────

class InvestigateRequest(BaseModel):
    session_id: str
    business_context: str = ""
    api_key: str = ""


class InvestigateResponse(BaseModel):
    session_id: str
    attribution: dict[str, Any]
    narrative: str
    agent_steps: list[dict[str, Any]]
    tool_results: dict[str, Any]
    primary_metric: str
    primary_gap: float
    causal_result: dict[str, Any]
    anomaly_result: dict[str, Any]
    strategy: dict[str, Any]
    confidence_gate: dict[str, Any]
    ai_used: bool
    tokens_used: int


# ── Fix Preview ───────────────────────────────────────────────────────────────

class FixPreviewRequest(BaseModel):
    session_id: str
    fix_type: str          # add_mapping | fix_hierarchy | add_elimination | add_adjustment
    fix_data: dict[str, Any]


class FixPreviewResponse(BaseModel):
    session_id: str
    fix_type: str
    fix_description: str
    comparison: dict[str, Any]
    audit_trail: list[dict[str, Any]]


# ── Export ────────────────────────────────────────────────────────────────────

class ExportRequest(BaseModel):
    session_id: str


# ── Session Status ────────────────────────────────────────────────────────────

class SessionStatus(BaseModel):
    session_id: str
    has_source: bool
    has_mapping: bool
    has_target: bool
    has_hierarchy: bool
    has_profile: bool
    has_reconciliation: bool
    has_investigation: bool
    source_system: str
    validation_warnings: list[str]
