"""
Central configuration for Smart Recon.
All tunable values live here — no hardcoding in business logic files.
"""
import os
from pathlib import Path

# ── AI / Model ────────────────────────────────────────────────────────────────

AI = {
    "provider": "gemini",
    "model": os.environ.get("RECON_AI_MODEL", "gemini-1.5-flash"),
    "max_tokens": int(os.environ.get("RECON_AI_MAX_TOKENS", "8192")),
    "max_agent_iterations": int(os.environ.get("RECON_MAX_ITERATIONS", "12")),
    "temperature": float(os.environ.get("RECON_AI_TEMPERATURE", "0.1")),
}

# ── Metric definitions ────────────────────────────────────────────────────────

# Canonical metric categories — inferred from mapping table's metric_category column.
# This is used as a FALLBACK ordering for display, not for classification logic.
METRIC_DISPLAY_ORDER = os.environ.get(
    "RECON_METRIC_ORDER",
    "Revenue,COGS,Gross Margin,OpEx,Operating Profit"
).split(",")

# Metric categories that represent REVENUE (positive contribution)
REVENUE_CATEGORIES = os.environ.get("RECON_REVENUE_CATS", "Revenue").split(",")

# Metric categories that represent COSTS (negative contribution, stored as negative amounts)
COST_CATEGORIES = os.environ.get("RECON_COST_CATS", "COGS,OpEx").split(",")

# Derived / computed metrics (not directly in source data)
DERIVED_METRICS = {
    "Gross Margin": ("Revenue", "COGS"),       # Revenue + COGS (COGS is negative)
    "Operating Profit": ("Gross Margin", "OpEx"),  # Gross Margin + OpEx (OpEx is negative)
}

# Aliases for metric category normalisation (lowercased key → canonical value)
METRIC_CATEGORY_ALIASES = {
    "rev": "Revenue", "revenue": "Revenue", "sales": "Revenue",
    "cogs": "COGS", "cost of goods": "COGS", "cost of sales": "COGS", "cost of revenue": "COGS",
    "opex": "OpEx", "operating expense": "OpEx", "operating expenses": "OpEx",
    "g&a": "OpEx", "sga": "OpEx", "overhead": "OpEx",
    "interco": "Intercompany", "intercompany": "Intercompany", "ic": "Intercompany",
}

# ── Data ingestion ────────────────────────────────────────────────────────────

DATA = {
    "default_currency": os.environ.get("RECON_DEFAULT_CURRENCY", "USD"),
    "default_metric_category": os.environ.get("RECON_DEFAULT_METRIC_CAT", "Revenue"),
    "gap_threshold": float(os.environ.get("RECON_GAP_THRESHOLD", "1000")),      # Min $ gap to flag
    "gap_pct_threshold": float(os.environ.get("RECON_GAP_PCT_THRESHOLD", "0.001")),  # 0.1%
    "top_n_profile": int(os.environ.get("RECON_TOP_N", "10")),                   # Top-N in profiles
}

# ── Attribution confidence scores ─────────────────────────────────────────────

CONFIDENCE = {
    "mapping_gap": float(os.environ.get("RECON_CONF_MAPPING", "0.95")),
    "intercompany": float(os.environ.get("RECON_CONF_IC", "0.85")),
    "hierarchy": float(os.environ.get("RECON_CONF_HIERARCHY", "0.80")),
    "manual_adjustment": float(os.environ.get("RECON_CONF_ADJUSTMENT", "0.65")),
    "residual": 0.0,
}

# ── App / UI ──────────────────────────────────────────────────────────────────

APP = {
    "title": os.environ.get("RECON_APP_TITLE", "Smart Recon"),
    "subtitle": os.environ.get("RECON_APP_SUBTITLE", "Automated Financial Reconciliation Engine"),
    "org_name": os.environ.get("RECON_ORG_NAME", "JMAN Group"),
    "currency_symbol": os.environ.get("RECON_CURRENCY_SYMBOL", "$"),
    "sample_data_dir": Path(__file__).parent / "sample_data",
}

# ── Chart colours (by category name) ─────────────────────────────────────────

COLORS = {
    "primary": "#1B4F72",
    "accent": "#2E86C1",
    "positive": "#27AE60",
    "negative": "#E74C3C",
    "warning": "#F39C12",
    "neutral": "#95A5A6",
    "bg": "#F8F9FA",
    # Root-cause category colours
    "Mapping Gap": "#E74C3C",
    "Hierarchy Mismatch": "#E67E22",
    "Intercompany Elimination": "#9B59B6",
    "Manual Adjustment": "#F39C12",
    "Unexplained Residual": "#95A5A6",
    # Metric colours
    "Revenue": "#2ECC71",
    "COGS": "#E74C3C",
    "Gross Margin": "#3498DB",
    "OpEx": "#E67E22",
    "Operating Profit": "#1ABC9C",
}

# Root-cause UI icons
CATEGORY_ICONS = {
    "Mapping Gap": "🗂️",
    "Hierarchy Mismatch": "🏗️",
    "Intercompany Elimination": "🔄",
    "Manual Adjustment": "✏️",
    "Unexplained Residual": "❓",
}

# Root-cause CSS classes (for Streamlit HTML)
CATEGORY_CSS = {
    "Mapping Gap": "rc-mapping",
    "Hierarchy Mismatch": "rc-hierarchy",
    "Intercompany Elimination": "rc-ic",
    "Manual Adjustment": "rc-adjustment",
    "Unexplained Residual": "rc-residual",
}

# ── Advanced analysis thresholds ──────────────────────────────────────────────

# DVT-style diff: tolerance below which a metric delta is considered a "pass"
TOLERANCE_PCT = float(os.environ.get("RECON_TOLERANCE_PCT", "0.01"))

# Whether to compute MD5 checksums for per-metric group values
CHECKSUM_ENABLED = os.environ.get("RECON_CHECKSUM_ENABLED", "true").lower() == "true"

# Causal path tracing: minimum contribution fraction to declare a single root cause
CAUSAL_THRESHOLD = float(os.environ.get("RECON_CAUSAL_THRESHOLD", "0.5"))

# IsolationForest: contamination rate and minimum row count to run
ISOLATION_FOREST_CONTAMINATION = float(os.environ.get("RECON_IF_CONTAMINATION", "0.05"))
ISOLATION_FOREST_MIN_ROWS = int(os.environ.get("RECON_IF_MIN_ROWS", "10"))

# Fuzzy account matching: minimum similarity score (0-100) to surface a suggestion
FUZZY_MATCH_THRESHOLD = int(os.environ.get("RECON_FUZZY_THRESHOLD", "85"))

# Conditional AI: only call Gemini when causal confidence is below this level
AI_CONFIDENCE_THRESHOLD = float(os.environ.get("RECON_AI_CONF_THRESHOLD", "0.5"))
