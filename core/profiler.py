"""
Layer 1 — Schema profiler + pipeline parser.

Responsibilities:
  • Parse fetched notebook cells: extract transformation steps, join keys,
    filter conditions, aggregation columns, and output schema.
  • Parse pipeline run history: extract execution timestamps, parameters,
    task order, and high-watermark values.
  • Build a structured "pipeline graph" — a DAG of what transforms what.
    This is the map the AI strategy planner (Layer 2) uses for its ranked
    investigation checklist.
  • Produce a compact data fingerprint (<5 KB) that is the ONLY thing
    ever sent to the AI.  Raw data rows are never included.

All thresholds and display limits come from config.py.
"""

from __future__ import annotations

import hashlib
import re
from typing import Optional

import pandas as pd

from config import DATA


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline graph — DAG of what transforms what (Layer 1)
# ─────────────────────────────────────────────────────────────────────────────

def parse_pipeline_graph(
    notebook_cells: Optional[list[dict]] = None,
    pipeline_run_history: Optional[list[dict]] = None,
) -> dict:
    """
    Layer 1 — Build the structured pipeline graph DAG.

    Accepts two optional inputs that come from the source connector (Layer 0):
      notebook_cells        : list of {cell_source: str, cell_type: str, ...}
                              from Databricks/Fabric Workspace API (read-only).
      pipeline_run_history  : list of {task_name, start_time, end_time,
                              parameters, status, watermark, ...}
                              from ADF/Databricks Jobs API.

    When neither is provided (e.g. demo/file-upload mode), returns a minimal
    graph constructed from the DataFrame schemas passed to build_fingerprint().

    Returns a pipeline_graph dict with:
      steps         : ordered list of transformation steps
      edges         : DAG edges {from_step -> to_step}
      run_metadata  : latest run timestamps, parameters, watermarks
      schema_summary: column names + inferred types per step
    """
    steps: list[dict] = []
    edges: list[dict] = []
    run_metadata: dict = {}

    # ── Parse notebook cells ──────────────────────────────────────────────────
    if notebook_cells:
        for i, cell in enumerate(notebook_cells):
            source = cell.get("cell_source", "")
            cell_type = cell.get("cell_type", "code")
            if cell_type != "code" or not source.strip():
                continue

            step = _extract_step_from_cell(source, step_index=i)
            steps.append(step)

            # Build DAG edge: each step depends on the previous
            if i > 0 and steps:
                edges.append({
                    "from_step": steps[-2]["step_name"] if len(steps) >= 2 else f"step_{i-1}",
                    "to_step": step["step_name"],
                    "edge_type": "sequential",
                })

    # ── Parse pipeline run history ────────────────────────────────────────────
    if pipeline_run_history:
        run_metadata = _extract_run_metadata(pipeline_run_history)

        # Enrich steps with execution timing from run history
        task_timing = {
            t.get("task_name", ""): {
                "start_time": t.get("start_time"),
                "end_time": t.get("end_time"),
                "status": t.get("status", "unknown"),
            }
            for t in pipeline_run_history
        }
        for step in steps:
            timing = task_timing.get(step["step_name"], {})
            step.update(timing)

    # ── Fallback: minimal graph when no cells/history available ───────────────
    if not steps:
        steps = [
            {
                "step_name": "source_ingest",
                "step_index": 0,
                "transform_type": "ingest",
                "join_keys": [],
                "filter_conditions": [],
                "aggregation_columns": [],
                "output_schema": [],
                "notes": "Inferred from uploaded source file — no notebook cells parsed.",
            },
            {
                "step_name": "mapping_apply",
                "step_index": 1,
                "transform_type": "lookup_join",
                "join_keys": ["account_code", "source_account_code"],
                "filter_conditions": [],
                "aggregation_columns": [],
                "output_schema": [],
                "notes": "Account code mapping step — inferred from mapping table.",
            },
            {
                "step_name": "metric_aggregation",
                "step_index": 2,
                "transform_type": "aggregate",
                "join_keys": [],
                "filter_conditions": ["is_intercompany == False"],
                "aggregation_columns": ["amount"],
                "output_schema": ["metric_category", "amount"],
                "notes": "Metric roll-up step — inferred from target results schema.",
            },
        ]
        edges = [
            {"from_step": "source_ingest",  "to_step": "mapping_apply",      "edge_type": "sequential"},
            {"from_step": "mapping_apply",   "to_step": "metric_aggregation", "edge_type": "sequential"},
        ]

    return {
        "steps": steps,
        "edges": edges,
        "step_count": len(steps),
        "run_metadata": run_metadata,
        "has_notebook_cells": bool(notebook_cells),
        "has_run_history": bool(pipeline_run_history),
    }


def _extract_step_from_cell(source: str, step_index: int) -> dict:
    """
    Parse a single notebook cell's source code to extract transformation metadata.
    Uses lightweight regex heuristics — no AST parsing required.
    """
    step_name = f"step_{step_index}"

    # Try to infer a friendly name from common patterns
    name_patterns = [
        r"#\s*(?:Step|STEP)\s*\d*[:\-–]?\s*(.+)",  # # Step 3: Filter by date
        r"def\s+(\w+)\s*\(",                         # def transform_revenue(
        r"# ==+\s*(.+?)\s*==+",                      # # ===== Revenue Join =====
        r"spark\.sql\(.*?--\s*(.+?)\n",              # -- comment inside SQL
    ]
    for pattern in name_patterns:
        m = re.search(pattern, source, re.IGNORECASE | re.MULTILINE)
        if m:
            step_name = m.group(1).strip()[:60].lower().replace(" ", "_")
            break

    # Infer transform type
    transform_type = "unknown"
    src_lower = source.lower()
    if any(kw in src_lower for kw in ["join", "merge", "lookup"]):
        transform_type = "join"
    elif any(kw in src_lower for kw in ["groupby", "group by", "agg(", "aggregate", "sum(", "count("]):
        transform_type = "aggregate"
    elif any(kw in src_lower for kw in ["filter", "where", "dropna", "drop_duplicates"]):
        transform_type = "filter"
    elif any(kw in src_lower for kw in ["read_", "load", "spark.read", "pd.read"]):
        transform_type = "ingest"
    elif any(kw in src_lower for kw in ["write", "save", "to_delta", "to_parquet", "to_excel"]):
        transform_type = "write"
    elif any(kw in src_lower for kw in ["select", "withcolumn", "rename", "cast"]):
        transform_type = "transform"

    # Extract join keys (on= / left_on= / right_on= patterns)
    join_keys: list[str] = []
    for m in re.finditer(r'(?:on|left_on|right_on)\s*=\s*[\"\'\[]([^\"\'\]]+)', source):
        join_keys.extend([k.strip().strip("\"'") for k in m.group(1).split(",")])

    # Extract filter conditions (simple WHERE / .filter() patterns)
    filter_conditions: list[str] = []
    for m in re.finditer(r'\.filter\(([^)]{3,80})\)', source):
        filter_conditions.append(m.group(1).strip()[:80])
    for m in re.finditer(r'WHERE\s+(.+?)(?:GROUP|ORDER|LIMIT|$)', source, re.IGNORECASE | re.DOTALL):
        cond = m.group(1).strip()[:120].replace("\n", " ")
        filter_conditions.append(cond)

    # Extract aggregation columns (groupby([...]).agg(...))
    aggregation_columns: list[str] = []
    for m in re.finditer(r'groupby\(\[?([^\])]+)\]?\)', source, re.IGNORECASE):
        aggregation_columns.extend([k.strip().strip("\"'") for k in m.group(1).split(",")])
    for m in re.finditer(r'GROUP BY\s+(.+?)(?:HAVING|ORDER|LIMIT|$)', source, re.IGNORECASE | re.DOTALL):
        aggregation_columns.extend([k.strip() for k in m.group(1).split(",")][:5])

    # Extract output schema (select([...]) or SELECT col1, col2 ... patterns)
    output_schema: list[str] = []
    for m in re.finditer(r'\.select\(\[?([^\])]+)\]?\)', source, re.IGNORECASE):
        output_schema.extend([k.strip().strip("\"'") for k in m.group(1).split(",")][:10])
    if not output_schema:
        for m in re.finditer(r'SELECT\s+(.+?)\s+FROM', source, re.IGNORECASE | re.DOTALL):
            cols = [c.strip() for c in m.group(1).split(",")][:10]
            output_schema.extend(cols)

    return {
        "step_name": step_name,
        "step_index": step_index,
        "transform_type": transform_type,
        "join_keys": list(dict.fromkeys(join_keys))[:8],         # dedup, cap at 8
        "filter_conditions": list(dict.fromkeys(filter_conditions))[:5],
        "aggregation_columns": list(dict.fromkeys(aggregation_columns))[:8],
        "output_schema": list(dict.fromkeys(output_schema))[:10],
        "cell_length_chars": len(source),
    }


def _extract_run_metadata(pipeline_run_history: list[dict]) -> dict:
    """
    Extract the most recent successful run's metadata from a list of pipeline run records.
    Returns timestamps, parameters, and high-watermark values only — no data rows.
    """
    if not pipeline_run_history:
        return {}

    # Find the most recent successful run
    successful = [r for r in pipeline_run_history if r.get("status", "").lower() in ("succeeded", "success", "completed")]
    latest = successful[0] if successful else pipeline_run_history[0]

    return {
        "last_successful_run_ts": latest.get("end_time") or latest.get("start_time"),
        "last_run_status": latest.get("status", "unknown"),
        "parameters": latest.get("parameters", {}),
        "high_watermark": latest.get("watermark") or latest.get("high_watermark"),
        "task_count": len(pipeline_run_history),
        "task_order": [
            r.get("task_name", f"task_{i}") for i, r in enumerate(pipeline_run_history[:10])
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Statistical profiling — compact fingerprint for AI strategy planner
# ─────────────────────────────────────────────────────────────────────────────

def profile_source(df: pd.DataFrame) -> dict:
    """
    Produce a compact statistical profile of the source DataFrame.
    This is structure + aggregate stats only — no raw rows are included.
    The output feeds the AI strategy planner (Layer 2) as part of the fingerprint.
    """
    top_n = DATA["top_n_profile"]

    profile: dict = {
        "row_count": len(df),
        "column_names": df.columns.tolist(),
        "column_dtypes": {col: str(df[col].dtype) for col in df.columns},
        "schema_hash": _df_schema_hash(df),
        "date_range": None,
        "total_amount": float(df["amount"].sum()) if "amount" in df.columns else 0,
        "unique_accounts": int(df["account_code"].nunique()) if "account_code" in df.columns else 0,
        "unique_entities": int(df["entity_code"].nunique()) if "entity_code" in df.columns else 0,
        "null_rates": {},
        "intercompany_count": 0,
        "adjustment_count": 0,
        "top_accounts_by_amount": [],
        "top_entities_by_amount": [],
        "currency_breakdown": {},
        "period_breakdown": {},
    }

    if "date" in df.columns:
        valid = df["date"].dropna()
        if not valid.empty:
            profile["date_range"] = {
                "min": str(valid.min().date()),
                "max": str(valid.max().date()),
            }

    profile["null_rates"] = {
        col: round(float(df[col].isna().mean()), 3)
        for col in df.columns
        if df[col].isna().any()
    }

    if "is_intercompany" in df.columns:
        profile["intercompany_count"] = int(df["is_intercompany"].sum())
    if "adjustment_flag" in df.columns:
        profile["adjustment_count"] = int(df["adjustment_flag"].sum())

    if "account_code" in df.columns and "amount" in df.columns:
        top_acc = df.groupby("account_code")["amount"].sum().abs().nlargest(top_n).reset_index()
        profile["top_accounts_by_amount"] = [
            {"account": str(r["account_code"]), "amount": round(float(r["amount"]), 2)}
            for _, r in top_acc.iterrows()
        ]

    if "entity_code" in df.columns and "amount" in df.columns:
        top_ent = df.groupby("entity_code")["amount"].sum().abs().nlargest(top_n).reset_index()
        profile["top_entities_by_amount"] = [
            {"entity": str(r["entity_code"]), "amount": round(float(r["amount"]), 2)}
            for _, r in top_ent.iterrows()
        ]

    if "currency" in df.columns:
        profile["currency_breakdown"] = df["currency"].value_counts().head(top_n).to_dict()
    if "period" in df.columns:
        profile["period_breakdown"] = df["period"].value_counts().head(top_n).to_dict()

    return profile


def profile_mapping(df: pd.DataFrame) -> dict:
    """
    Compact profile of the mapping/lookup table.
    Includes schema hash so the AI can flag if the mapping version has drifted.
    """
    return {
        "row_count": len(df),
        "column_names": df.columns.tolist(),
        "schema_hash": _df_schema_hash(df),
        "unique_source_accounts": (
            int(df["source_account_code"].nunique()) if "source_account_code" in df.columns else 0
        ),
        "unique_target_accounts": (
            int(df["target_account_code"].nunique()) if "target_account_code" in df.columns else 0
        ),
        "metric_categories": (
            df["metric_category"].value_counts().to_dict() if "metric_category" in df.columns else {}
        ),
        "null_rates": {
            col: round(float(df[col].isna().mean()), 3)
            for col in df.columns
            if df[col].isna().any()
        },
    }


def profile_target(df: pd.DataFrame) -> dict:
    """
    Compact profile of the target results DataFrame.
    Includes schema hash for schema version diff (Layer 4 Type 2).
    """
    profile: dict = {
        "row_count": len(df),
        "column_names": df.columns.tolist(),
        "schema_hash": _df_schema_hash(df),
        "metrics": [],
        "total_target_amount": float(df["amount"].sum()) if "amount" in df.columns else 0,
        "metric_breakdown": {},
        "period_breakdown": {},
    }
    if "metric" in df.columns:
        profile["metrics"] = df["metric"].unique().tolist()
        if "amount" in df.columns:
            profile["metric_breakdown"] = df.groupby("metric")["amount"].sum().round(2).to_dict()
    if "period" in df.columns:
        profile["period_breakdown"] = df["period"].value_counts().to_dict()
    return profile


def check_mapping_coverage(source: pd.DataFrame, mapping: pd.DataFrame) -> dict:
    """
    Pre-check: what % of source account codes have a mapping entry?
    Feeds both the Data Profile tab and the AI fingerprint.
    Only aggregate stats are returned — no raw rows.
    """
    if "account_code" not in source.columns or "source_account_code" not in mapping.columns:
        return {"coverage_pct": None, "unmapped_count": None, "unmapped_amount": None}

    mapped_codes = set(mapping["source_account_code"].dropna().astype(str))
    source = source.copy()
    source["_acc"] = source["account_code"].astype(str)
    unmapped_mask = ~source["_acc"].isin(mapped_codes)

    total = len(source)
    unmapped_count = int(unmapped_mask.sum())
    unmapped_amount = (
        float(source.loc[unmapped_mask, "amount"].sum())
        if "amount" in source.columns
        else 0.0
    )
    coverage_pct = round((1 - unmapped_count / total) * 100, 1) if total > 0 else 100.0

    top_n = DATA["top_n_profile"]
    unmapped_codes = (
        source.loc[unmapped_mask, "account_code"].value_counts().head(top_n).to_dict()
    )

    return {
        "coverage_pct": coverage_pct,
        "unmapped_count": unmapped_count,
        "unmapped_amount": round(unmapped_amount, 2),
        "total_transactions": total,
        "top_unmapped_codes": {str(k): int(v) for k, v in unmapped_codes.items()},
    }


# ─────────────────────────────────────────────────────────────────────────────
# Master fingerprint builder — the ONLY thing sent to Layer 2 (AI)
# ─────────────────────────────────────────────────────────────────────────────

def build_fingerprint(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: pd.DataFrame,
    hierarchy: Optional[pd.DataFrame] = None,
    notebook_cells: Optional[list[dict]] = None,
    pipeline_run_history: Optional[list[dict]] = None,
) -> dict:
    """
    Layer 1 — Build the complete data fingerprint.

    This is the ONLY output ever sent to the AI strategy planner (Layer 2).
    Typical size: < 5 KB.  Raw data rows are never included.

    Components:
      source          : statistical profile of source transactions
      mapping         : profile of the account code mapping table
      target          : profile of the target/expected results
      mapping_coverage: pre-check on unmapped account codes
      hierarchy       : entity rollup structure summary (if provided)
      pipeline_graph  : DAG of transformation steps (Layer 1 parser output)

    The pipeline_graph is what lets the AI act as a "human developer thinking"
    about which cells to re-execute and which mappings to check — without ever
    seeing a single data row.
    """
    pipeline_graph = parse_pipeline_graph(
        notebook_cells=notebook_cells,
        pipeline_run_history=pipeline_run_history,
    )

    fp: dict = {
        "source": profile_source(source),
        "mapping": profile_mapping(mapping),
        "target": profile_target(target),
        "mapping_coverage": check_mapping_coverage(source, mapping),
        "pipeline_graph": pipeline_graph,
    }

    if hierarchy is not None and not hierarchy.empty:
        fp["hierarchy"] = {
            "row_count": len(hierarchy),
            "unique_entities": (
                int(hierarchy["entity_code"].nunique())
                if "entity_code" in hierarchy.columns
                else 0
            ),
            "columns": hierarchy.columns.tolist(),
            "schema_hash": _df_schema_hash(hierarchy),
        }
    else:
        fp["hierarchy"] = None

    # Attach a top-level fingerprint hash so the AI can detect if anything
    # has changed since the last run (useful for incremental investigations)
    fp["fingerprint_hash"] = hashlib.md5(
        str({
            "src_hash": fp["source"]["schema_hash"],
            "map_hash": fp["mapping"]["schema_hash"],
            "tgt_hash": fp["target"]["schema_hash"],
            "coverage": fp["mapping_coverage"].get("coverage_pct"),
        }).encode()
    ).hexdigest()[:12]

    return fp


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _df_schema_hash(df: pd.DataFrame) -> str:
    """
    Lightweight fingerprint of a DataFrame's column names + dtypes.
    Used by Layer 4 Type 2 schema version diff check.
    """
    sig = ",".join(f"{c}:{df[c].dtype}" for c in sorted(df.columns))
    return hashlib.md5(sig.encode()).hexdigest()[:8]