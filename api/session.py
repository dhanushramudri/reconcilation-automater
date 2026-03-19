"""
In-memory session management for Smart Recon FastAPI backend.
Sessions are keyed by UUID and store DataFrames + computed results.
"""
from __future__ import annotations
import uuid
from typing import Optional
import pandas as pd


# Global session store — replaced per-process; not suitable for multi-worker prod
_sessions: dict[str, dict] = {}


def create_session() -> str:
    """Create a new empty session and return its ID."""
    sid = str(uuid.uuid4())
    _sessions[sid] = {
        "source": None,
        "mapping": None,
        "target": None,
        "hierarchy": None,
        "profile": None,
        "gap_summary": None,
        "our_metrics": None,
        "target_metrics": None,
        "fingerprint": None,
        "investigation": None,
        "audit_trail": [],
        "source_system": "file_upload",
        "api_key": "",
        "business_context": "",
        "validation_warnings": [],
    }
    return sid


def get_session(sid: str) -> Optional[dict]:
    """Return session dict or None if not found."""
    return _sessions.get(sid)


def update_session(sid: str, updates: dict) -> None:
    """Merge updates into an existing session."""
    if sid in _sessions:
        _sessions[sid].update(updates)


def delete_session(sid: str) -> None:
    """Remove a session."""
    _sessions.pop(sid, None)


def list_sessions() -> list[str]:
    """Return all active session IDs."""
    return list(_sessions.keys())
