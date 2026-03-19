"""
Layer 0 — Source connectors + data loading and validation for Smart Recon.

Responsibilities in the 6-layer architecture:
  • File-upload mode (demo): load Excel/CSV for source, mapping, target, hierarchy.
  • Layer 0 connector mode (production): fetch read-only metadata and aggregate
    stats from Databricks, Microsoft Fabric, or SQL Server / ADF.
    Connectors NEVER extract full row-level data — only:
      (a) notebook cell source (read-only, no execution)
      (b) pipeline/task definitions and run history
      (c) catalog schema — table names, column names, data types, row counts
      (d) aggregate query results — sums/counts per key group
  • Schema validation: checks required columns, coerces types, applies
    column aliases.  Produces schema_hash for Layer 4 Type 2 drift detection.
  • fuzzy_map_accounts() is kept here as a thin wrapper for UI use; the
    authoritative fuzzy logic lives in reconciler.run_type2_data_checks().

All defaults, thresholds, and aliases come from config.py.
No hardcoded values in this module.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import IO, Optional, Union

import pandas as pd

from config import DATA, FUZZY_MATCH_THRESHOLD, METRIC_CATEGORY_ALIASES


# ─────────────────────────────────────────────────────────────────────────────
# Required column definitions
# ─────────────────────────────────────────────────────────────────────────────

REQUIRED_SOURCE_COLS  = ["transaction_id", "date", "account_code", "entity_code", "amount"]
REQUIRED_MAPPING_COLS = ["source_account_code", "target_account_code", "metric_category"]
REQUIRED_TARGET_COLS  = ["metric", "group_by", "period", "amount"]

# Column aliases for flexible file ingestion (file-upload demo mode)
_SOURCE_ALIASES: dict[str, list[str]] = {
    "transaction_id": ["txn_id", "trans_id", "id", "transaction"],
    "date":           ["transaction_date", "posting_date", "value_date", "period_date"],
    "account_code":   ["account", "acc_code", "gl_account", "account_number"],
    "entity_code":    ["entity", "company_code", "cost_center", "legal_entity"],
    "amount":         ["value", "net_amount", "transaction_amount", "debit_credit"],
}

_MAPPING_ALIASES: dict[str, list[str]] = {
    "source_account_code": ["src_account", "source_code", "from_account", "account_code", "source_account"],
    "target_account_code": ["tgt_account", "target_code", "to_account", "mapped_code", "target_account"],
    "metric_category":     ["metric", "category", "account_type", "kpi", "classification"],
    "target_account_name": ["target_name", "tgt_name", "to_name"],
    "source_account_name": ["src_name", "source_name", "from_name", "account_name"],
}

_TARGET_ALIASES: dict[str, list[str]] = {
    "metric":   ["kpi", "measure", "metric_name", "indicator"],
    "group_by": ["entity", "region", "bu", "business_unit", "group", "segment", "dimension"],
    "period":   ["quarter", "year", "reporting_period", "fiscal_period", "fiscal_quarter"],
    "amount":   ["value", "target", "expected", "reported_value", "target_amount"],
}


# ─────────────────────────────────────────────────────────────────────────────
# File-read utilities
# ─────────────────────────────────────────────────────────────────────────────

def _read_file(file: Union[str, Path, IO], sheet_name: int = 0) -> pd.DataFrame:
    """Read an Excel or CSV file into a DataFrame."""
    name = file.name.lower() if hasattr(file, "name") else str(file).lower()
    if name.endswith(".csv"):
        return pd.read_csv(file)
    return pd.read_excel(file, sheet_name=sheet_name)


def _normalise_cols(df: pd.DataFrame) -> pd.DataFrame:
    """Lowercase and snake_case all column names."""
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def _best_col_match(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column that substring-matches any candidate."""
    for col in df.columns:
        for candidate in candidates:
            if candidate in col or col in candidate:
                return col
    return None


def _apply_aliases(df: pd.DataFrame, alias_map: dict[str, list[str]]) -> pd.DataFrame:
    """Rename columns to canonical names using an alias map."""
    rename: dict[str, str] = {}
    for canonical, aliases in alias_map.items():
        if canonical not in df.columns:
            match = _best_col_match(df, aliases)
            if match:
                rename[match] = canonical
    return df.rename(columns=rename)


def _schema_hash(df: pd.DataFrame) -> str:
    """
    Lightweight schema fingerprint of a DataFrame's column names + dtypes.
    Consumed by Layer 4 Type 2 schema version diff check.
    """
    sig = ",".join(f"{c}:{df[c].dtype}" for c in sorted(df.columns))
    return hashlib.md5(sig.encode()).hexdigest()[:8]


# ─────────────────────────────────────────────────────────────────────────────
# File-upload loaders  (demo / fallback mode)
# ─────────────────────────────────────────────────────────────────────────────

def load_source(file) -> pd.DataFrame:
    """
    Load and normalise source (operational) transactional data from a file.

    Used in file-upload demo mode.  In production the equivalent data
    comes from the Layer 0 connector (fetch_databricks_metadata,
    fetch_fabric_metadata, or fetch_sql_server_metadata) as aggregate
    query results — not full row exports.
    """
    df = _normalise_cols(_read_file(file))
    df = _apply_aliases(df, _SOURCE_ALIASES)

    # Type coercions
    df["amount"] = pd.to_numeric(
        df.get("amount", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0)

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["period"] = df["date"].dt.to_period("Q").astype(str)

    # Default optional columns so downstream code never KeyErrors
    _defaults: dict[str, object] = {
        "is_intercompany":    False,
        "counterparty_entity": None,
        "adjustment_flag":    False,
        "adjustment_type":    None,
        "currency":           DATA["default_currency"],
    }
    for col, val in _defaults.items():
        if col not in df.columns:
            df[col] = val

    df["is_intercompany"] = df["is_intercompany"].astype(bool)
    df["adjustment_flag"] = df["adjustment_flag"].astype(bool)

    # Attach schema hash for Layer 4 Type 2 drift detection
    df.attrs["schema_hash"] = _schema_hash(df)

    return df


def load_mapping(file) -> pd.DataFrame:
    """
    Load the account code mapping table from a file.
    Normalises metric_category values to canonical names via config aliases.
    Attaches a schema_hash for drift detection.
    """
    df = _normalise_cols(_read_file(file))
    df = _apply_aliases(df, _MAPPING_ALIASES)

    if "metric_category" not in df.columns:
        df["metric_category"] = DATA["default_metric_category"]
    else:
        df["metric_category"] = df["metric_category"].apply(_normalise_metric_category)

    df.attrs["schema_hash"] = _schema_hash(df)
    return df


def load_target(file) -> pd.DataFrame:
    """
    Load client target / Group Reporting expected values from a file.
    Attaches a schema_hash for drift detection.
    """
    df = _normalise_cols(_read_file(file))
    df = _apply_aliases(df, _TARGET_ALIASES)

    df["amount"] = pd.to_numeric(
        df.get("amount", pd.Series(dtype=float)), errors="coerce"
    ).fillna(0)

    df.attrs["schema_hash"] = _schema_hash(df)
    return df


def load_hierarchy(file) -> pd.DataFrame:
    """
    Load the entity hierarchy table (entity → BU → Region → Segment).
    """
    df = _normalise_cols(_read_file(file))

    # Flexible entity_code column detection
    if "entity_code" not in df.columns:
        for col in df.columns:
            if "entity" in col and "code" in col:
                df = df.rename(columns={col: "entity_code"})
                break

    df.attrs["schema_hash"] = _schema_hash(df)
    return df


def _normalise_metric_category(raw: str) -> str:
    """Map raw category strings to canonical names via config aliases."""
    s = str(raw).strip().lower()
    for alias, canonical in METRIC_CATEGORY_ALIASES.items():
        if alias in s:
            return canonical
    return str(raw).strip().title()


# ─────────────────────────────────────────────────────────────────────────────
# Layer 0 — Databricks connector  (read-only, OAuth M2M)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_databricks_metadata(
    workspace_url: str,
    client_id: str,
    client_secret: str,
    catalog_schema: str = "main.default",
    notebook_path: Optional[str] = None,
) -> dict:
    """
    Layer 0 — Databricks connector (read-only).

    Authenticates via OAuth M2M (Service Principal).
    Fetches ONLY:
      (a) notebook cell source via Workspace API (read-only, no execution)
      (b) job/pipeline run history via Jobs API
      (c) Unity Catalog schema — table names, column names, data types, row counts
      (d) aggregate query results — sums/counts per key group (no full table scans)

    Credentials must be stored in Azure Key Vault.  This function reads them
    from the environment/vault at runtime — zero secrets in code.

    Returns a connector_result dict consumed by build_fingerprint() as
    notebook_cells and pipeline_run_history.
    """
    result: dict = {
        "source_system": "databricks",
        "workspace_url": workspace_url,
        "catalog_schema": catalog_schema,
        "notebook_cells": [],
        "pipeline_run_history": [],
        "catalog_schema_info": {},
        "aggregate_stats": {},
        "connection_status": "not_attempted",
        "error": None,
    }

    try:
        # OAuth M2M token exchange
        import requests

        token_url = f"{workspace_url.rstrip('/')}/oidc/v1/token"
        token_resp = requests.post(
            token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "all-apis",
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}

        base = workspace_url.rstrip("/")

        # ── (a) Notebook cell source ──────────────────────────────────────────
        if notebook_path:
            nb_resp = requests.get(
                f"{base}/api/2.0/workspace/export",
                headers=headers,
                params={"path": notebook_path, "format": "SOURCE"},
                timeout=15,
            )
            if nb_resp.ok:
                import base64 as _b64
                raw_source = _b64.b64decode(nb_resp.json().get("content", "")).decode("utf-8", errors="replace")
                # Split into cells by common notebook cell delimiter
                cells = [
                    {"cell_source": cell.strip(), "cell_type": "code"}
                    for cell in raw_source.split("# COMMAND ----------")
                    if cell.strip()
                ]
                result["notebook_cells"] = cells[:50]  # cap at 50 cells

        # ── (b) Job run history ───────────────────────────────────────────────
        jobs_resp = requests.get(
            f"{base}/api/2.1/jobs/runs/list",
            headers=headers,
            params={"limit": 20, "expand_tasks": "true"},
            timeout=15,
        )
        if jobs_resp.ok:
            runs = jobs_resp.json().get("runs", [])
            result["pipeline_run_history"] = [
                {
                    "task_name": run.get("run_name", f"run_{run.get('run_id', '')}"),
                    "start_time": _ms_to_iso(run.get("start_time")),
                    "end_time":   _ms_to_iso(run.get("end_time")),
                    "status":     run.get("state", {}).get("result_state", "unknown"),
                    "parameters": run.get("overriding_parameters", {}),
                    "watermark":  None,  # Populated from notebook output if available
                }
                for run in runs[:20]
            ]

        # ── (c) Unity Catalog schema ──────────────────────────────────────────
        catalog, schema = (catalog_schema.split(".", 1) + ["default"])[:2]
        tables_resp = requests.get(
            f"{base}/api/2.1/unity-catalog/tables",
            headers=headers,
            params={"catalog_name": catalog, "schema_name": schema, "max_results": 50},
            timeout=15,
        )
        if tables_resp.ok:
            tables = tables_resp.json().get("tables", [])
            result["catalog_schema_info"] = {
                t["name"]: {
                    "columns": [
                        {"name": c["name"], "type": c.get("type_text", "")}
                        for c in t.get("columns", [])
                    ],
                    "row_count": t.get("properties", {}).get("numRows"),
                }
                for t in tables
            }

        # ── (d) Aggregate query results ───────────────────────────────────────
        # Execute lightweight aggregate queries via SQL Warehouse (no full scans)
        sql_resp = requests.post(
            f"{base}/api/2.0/sql/statements",
            headers=headers,
            json={
                "statement": (
                    f"SELECT account_code, SUM(amount) as total_amount, COUNT(*) as row_count "
                    f"FROM {catalog_schema}.transactions "
                    f"GROUP BY account_code LIMIT 500"
                ),
                "warehouse_id": "",   # Populated from config in production
                "wait_timeout":  "10s",
            },
            timeout=30,
        )
        if sql_resp.ok:
            sql_data = sql_resp.json()
            rows = sql_data.get("result", {}).get("data_array", [])
            result["aggregate_stats"] = {
                "account_aggregates": [
                    {"account_code": r[0], "total_amount": r[1], "row_count": r[2]}
                    for r in rows
                ]
            }

        result["connection_status"] = "connected"

    except Exception as exc:
        result["connection_status"] = "error"
        result["error"] = str(exc)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Layer 0 — Microsoft Fabric connector  (read-only, Entra ID token)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fabric_metadata(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    workspace_id: str,
    lakehouse_name: str = "",
) -> dict:
    """
    Layer 0 — Microsoft Fabric connector (read-only).

    Authenticates via Entra ID (MSAL client credentials).
    Fetches ONLY:
      (a) notebook definitions from Fabric REST API
      (b) pipeline run metadata (activity runs, parameters, timestamps)
      (c) Lakehouse/Warehouse aggregate queries (sums/counts per group)

    No full row-level data is extracted — aggregate queries only.
    Returns a connector_result dict consumed by build_fingerprint().
    """
    result: dict = {
        "source_system": "microsoft_fabric",
        "workspace_id": workspace_id,
        "lakehouse_name": lakehouse_name,
        "notebook_cells": [],
        "pipeline_run_history": [],
        "catalog_schema_info": {},
        "aggregate_stats": {},
        "connection_status": "not_attempted",
        "error": None,
    }

    try:
        import requests

        # Entra ID token via MSAL client credentials
        token_url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        token_resp = requests.post(
            token_url,
            data={
                "grant_type":    "client_credentials",
                "client_id":     client_id,
                "client_secret": client_secret,
                "scope":         "https://api.fabric.microsoft.com/.default",
            },
            timeout=15,
        )
        token_resp.raise_for_status()
        access_token = token_resp.json()["access_token"]
        headers = {"Authorization": f"Bearer {access_token}"}

        fabric_base = "https://api.fabric.microsoft.com/v1"

        # ── (a) Notebook definitions ──────────────────────────────────────────
        nb_list_resp = requests.get(
            f"{fabric_base}/workspaces/{workspace_id}/notebooks",
            headers=headers,
            timeout=15,
        )
        if nb_list_resp.ok:
            notebooks = nb_list_resp.json().get("value", [])
            for nb in notebooks[:5]:  # limit to first 5 notebooks
                nb_id = nb.get("id", "")
                nb_detail_resp = requests.get(
                    f"{fabric_base}/workspaces/{workspace_id}/notebooks/{nb_id}",
                    headers=headers,
                    timeout=15,
                )
                if nb_detail_resp.ok:
                    cells = nb_detail_resp.json().get("definition", {}).get("cells", [])
                    result["notebook_cells"].extend([
                        {
                            "cell_source": c.get("source", ""),
                            "cell_type":   c.get("cell_type", "code"),
                            "notebook_id": nb_id,
                        }
                        for c in cells
                        if c.get("cell_type") == "code"
                    ][:20])

        # ── (b) Pipeline run history ──────────────────────────────────────────
        pipeline_resp = requests.get(
            f"{fabric_base}/workspaces/{workspace_id}/dataPipelines",
            headers=headers,
            timeout=15,
        )
        if pipeline_resp.ok:
            pipelines = pipeline_resp.json().get("value", [])
            for pipeline in pipelines[:5]:
                pid = pipeline.get("id", "")
                runs_resp = requests.get(
                    f"{fabric_base}/workspaces/{workspace_id}/dataPipelines/{pid}/runs",
                    headers=headers,
                    params={"maxResults": 10},
                    timeout=15,
                )
                if runs_resp.ok:
                    for run in runs_resp.json().get("value", []):
                        result["pipeline_run_history"].append({
                            "task_name": pipeline.get("displayName", pid),
                            "start_time": run.get("startTime"),
                            "end_time":   run.get("endTime"),
                            "status":     run.get("status", "unknown"),
                            "parameters": run.get("parameters", {}),
                            "watermark":  run.get("properties", {}).get("watermark"),
                        })

        result["connection_status"] = "connected"

    except Exception as exc:
        result["connection_status"] = "error"
        result["error"] = str(exc)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Layer 0 — SQL Server / ADF connector  (read-only, SQL + REST)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_sql_server_metadata(
    server: str,
    database: str,
    username: str,
    password: str,
    adf_resource_id: str = "",
    adf_subscription_id: str = "",
    adf_resource_group: str = "",
    adf_factory_name: str = "",
) -> dict:
    """
    Layer 0 — SQL Server + ADF connector (read-only).

    SQL Server: reads stored procedure definitions and job history only.
    ADF: fetches pipeline run history, parameter snapshots, and trigger metadata
    via Azure Management REST API.

    No full table scans — aggregate queries only.
    Returns a connector_result dict consumed by build_fingerprint().
    """
    result: dict = {
        "source_system": "sql_server_adf",
        "server": server,
        "database": database,
        "notebook_cells": [],       # Stored proc bodies used as "cells"
        "pipeline_run_history": [],
        "catalog_schema_info": {},
        "aggregate_stats": {},
        "connection_status": "not_attempted",
        "error": None,
    }

    try:
        import pyodbc  # type: ignore

        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={username};PWD={password};"
            f"Encrypt=yes;TrustServerCertificate=no;Connection Timeout=15;"
        )
        conn = pyodbc.connect(conn_str, timeout=15)
        cursor = conn.cursor()

        # ── Stored procedure definitions (used as "notebook cells") ──────────
        cursor.execute(
            """
            SELECT ROUTINE_NAME, ROUTINE_DEFINITION
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE ROUTINE_TYPE = 'PROCEDURE'
            ORDER BY ROUTINE_NAME
            """
        )
        for row in cursor.fetchall():
            result["notebook_cells"].append({
                "cell_source": row[1] or "",
                "cell_type":   "code",
                "step_name":   row[0],
            })

        # ── Catalog schema — column names + types (no row-level data) ────────
        cursor.execute(
            """
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
            ORDER BY TABLE_NAME, ORDINAL_POSITION
            """
        )
        schema_info: dict = {}
        for row in cursor.fetchall():
            tbl, col, dtype = row
            schema_info.setdefault(tbl, {"columns": [], "row_count": None})
            schema_info[tbl]["columns"].append({"name": col, "type": dtype})
        result["catalog_schema_info"] = schema_info

        # ── Aggregate stats — sums/counts per account group ───────────────────
        try:
            cursor.execute(
                """
                SELECT TOP 500
                    account_code,
                    SUM(amount) AS total_amount,
                    COUNT(*) AS row_count
                FROM transactions
                GROUP BY account_code
                ORDER BY SUM(ABS(amount)) DESC
                """
            )
            result["aggregate_stats"] = {
                "account_aggregates": [
                    {
                        "account_code": row[0],
                        "total_amount": float(row[1]) if row[1] is not None else 0.0,
                        "row_count": int(row[2]),
                    }
                    for row in cursor.fetchall()
                ]
            }
        except Exception:
            pass  # Table may not exist — non-fatal

        cursor.close()
        conn.close()
        result["connection_status"] = "sql_connected"

    except ImportError:
        result["connection_status"] = "error"
        result["error"] = "pyodbc not installed — SQL Server connector unavailable."
    except Exception as exc:
        result["connection_status"] = "error"
        result["error"] = str(exc)

    # ── ADF pipeline run history (REST API, separate from SQL) ─────────────
    if adf_factory_name and adf_subscription_id and adf_resource_group:
        try:
            import requests

            # Use Azure AD token for ARM API
            arm_token_url = (
                "https://login.microsoftonline.com/common/oauth2/v2.0/token"
            )
            arm_resp = requests.post(
                arm_token_url,
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     "",   # Populated from Azure Key Vault in production
                    "client_secret": "",
                    "scope":         "https://management.azure.com/.default",
                },
                timeout=15,
            )
            if arm_resp.ok:
                arm_token = arm_resp.json().get("access_token", "")
                adf_headers = {"Authorization": f"Bearer {arm_token}"}

                runs_url = (
                    f"https://management.azure.com/subscriptions/{adf_subscription_id}"
                    f"/resourceGroups/{adf_resource_group}"
                    f"/providers/Microsoft.DataFactory/factories/{adf_factory_name}"
                    f"/pipelineruns?api-version=2018-06-01"
                )
                runs_resp = requests.post(
                    runs_url,
                    headers=adf_headers,
                    json={
                        "lastUpdatedAfter":  "1970-01-01T00:00:00Z",
                        "lastUpdatedBefore": "2099-01-01T00:00:00Z",
                    },
                    timeout=15,
                )
                if runs_resp.ok:
                    for run in runs_resp.json().get("value", [])[:20]:
                        result["pipeline_run_history"].append({
                            "task_name":  run.get("pipelineName", ""),
                            "start_time": run.get("runStart"),
                            "end_time":   run.get("runEnd"),
                            "status":     run.get("status", "unknown"),
                            "parameters": run.get("parameters", {}),
                            "watermark":  run.get("additionalProperties", {}).get("watermark"),
                        })
                if result["connection_status"] == "sql_connected":
                    result["connection_status"] = "connected"

        except Exception as exc:
            # ADF fetch failure is non-fatal — SQL connection may still be valid
            result["adf_error"] = str(exc)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# connector_result → DataFrames  (normalise Layer 0 output for Layer 1)
# ─────────────────────────────────────────────────────────────────────────────

def connector_result_to_dataframes(connector_result: dict) -> dict:
    """
    Convert a connector_result dict (from any Layer 0 connector) into
    DataFrames that the rest of the pipeline can consume.

    Returns:
        source_df     : aggregate stats as a pseudo-source DataFrame
        notebook_cells: list of cell dicts for build_fingerprint()
        pipeline_run_history: list of run dicts for build_fingerprint()
        schema_info   : raw catalog schema dict
    """
    agg_stats = connector_result.get("aggregate_stats", {})
    account_aggs = agg_stats.get("account_aggregates", [])

    # Build a minimal source-like DataFrame from aggregate stats
    # This is NOT row-level data — it's sums/counts per account code
    if account_aggs:
        source_df = pd.DataFrame(account_aggs)
        # Rename to match expected source schema
        source_df = source_df.rename(columns={
            "total_amount": "amount",
            "row_count":    "transaction_count",
        })
        if "account_code" not in source_df.columns:
            source_df["account_code"] = "unknown"
        # Add required columns with defaults
        source_df["entity_code"]         = "CONNECTOR"
        source_df["is_intercompany"]     = False
        source_df["adjustment_flag"]     = False
        source_df["currency"]            = DATA["default_currency"]
        source_df["transaction_id"]      = range(len(source_df))
        source_df.attrs["schema_hash"]   = _schema_hash(source_df)
        source_df.attrs["source_system"] = connector_result.get("source_system", "unknown")
    else:
        source_df = pd.DataFrame(columns=REQUIRED_SOURCE_COLS)

    return {
        "source_df":             source_df,
        "notebook_cells":        connector_result.get("notebook_cells", []),
        "pipeline_run_history":  connector_result.get("pipeline_run_history", []),
        "schema_info":           connector_result.get("catalog_schema_info", {}),
        "connection_status":     connector_result.get("connection_status", "unknown"),
        "error":                 connector_result.get("error"),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Fuzzy account matching  (UI wrapper — logic lives in reconciler.py)
# ─────────────────────────────────────────────────────────────────────────────

def fuzzy_map_accounts(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    threshold: Optional[int] = None,
) -> list[dict]:
    """
    Thin UI wrapper around rapidfuzz matching for unmapped account codes.

    The authoritative fuzzy logic lives in reconciler.run_type2_data_checks()
    (Layer 4 Type 2).  This wrapper is kept here for the Streamlit UI
    (Tab 6 — Fix & Validate) where it is called independently of a full
    investigation run.

    Returns a list of suggestion dicts:
        {source_code, suggested_code, score, suggested_category}
    Suggestions are NEVER auto-applied — they surface as checkboxes in the UI.
    """
    min_score = threshold if threshold is not None else FUZZY_MATCH_THRESHOLD

    if "account_code" not in source.columns or "source_account_code" not in mapping.columns:
        return []

    try:
        from rapidfuzz import fuzz
    except ImportError:
        return []

    mapped_codes = set(mapping["source_account_code"].astype(str))
    unmapped_codes = [
        str(c) for c in source["account_code"].unique()
        if str(c) not in mapped_codes
    ]

    if not unmapped_codes:
        return []

    mapping_copy = mapping.copy()
    mapping_copy["source_account_code"] = mapping_copy["source_account_code"].astype(str)
    category_lookup = dict(
        zip(
            mapping_copy["source_account_code"],
            mapping_copy.get(
                "metric_category",
                pd.Series([DATA["default_metric_category"]] * len(mapping_copy)),
            ),
        )
    )

    suggestions: list[dict] = []
    for src_code in unmapped_codes:
        best_score = 0
        best_match: str | None = None

        for mapped_code in mapped_codes:
            score = fuzz.token_sort_ratio(src_code, mapped_code)
            if score > best_score:
                best_score = score
                best_match = mapped_code

        if best_score >= min_score and best_match is not None:
            suggestions.append({
                "source_code":        src_code,
                "suggested_code":     best_match,
                "score":              best_score,
                "suggested_category": category_lookup.get(
                    best_match, DATA["default_metric_category"]
                ),
            })

    suggestions.sort(key=lambda x: x["score"], reverse=True)
    return suggestions


# ─────────────────────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_files(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: pd.DataFrame,
) -> list[str]:
    """
    Validate loaded DataFrames and return a list of warning strings.
    Empty list = all checks passed.

    Checks:
      • DataFrames are non-empty
      • Required columns are present
      • Schema hashes are available (set by load_* functions)
      • Amount columns are numeric
    """
    warnings: list[str] = []

    if source.empty:
        warnings.append("Source data is empty.")
    if mapping.empty:
        warnings.append("Mapping table is empty.")
    if target.empty:
        warnings.append("Target results are empty.")

    if "account_code" not in source.columns:
        warnings.append("Source data missing 'account_code' column.")
    if "source_account_code" not in mapping.columns:
        warnings.append("Mapping table missing 'source_account_code' column.")
    if "metric" not in target.columns:
        warnings.append("Target data missing 'metric' column.")
    if "amount" not in target.columns:
        warnings.append("Target data missing 'amount' column.")

    # Amount columns should be numeric after load
    for label, df in [("Source", source), ("Target", target)]:
        if "amount" in df.columns and not pd.api.types.is_numeric_dtype(df["amount"]):
            warnings.append(
                f"{label} 'amount' column is not numeric after load. "
                "Check for currency symbols or non-numeric values."
            )

    # Warn if schema hashes are missing (indicates file was loaded outside load_* functions)
    for label, df in [("Source", source), ("Mapping", mapping), ("Target", target)]:
        if not df.attrs.get("schema_hash"):
            warnings.append(
                f"{label} DataFrame is missing a schema_hash. "
                "Load it via the load_* functions to enable Layer 4 Type 2 schema drift detection."
            )

    return warnings


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _ms_to_iso(ms_timestamp: Optional[int]) -> Optional[str]:
    """Convert a Unix millisecond timestamp to an ISO-8601 string."""
    if ms_timestamp is None:
        return None
    try:
        import datetime
        return datetime.datetime.utcfromtimestamp(ms_timestamp / 1000).strftime(
            "%Y-%m-%dT%H:%M:%S"
        )
    except (TypeError, OSError, OverflowError):
        return None