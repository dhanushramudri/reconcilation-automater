"""
Core reconciliation engine — Layer 4 (Hypothesis Investigator) + Layer 5 (Output).

Layer 4 runs three typed hypothesis checks, prioritised by the AI plan from Layer 2:
  Type 1 — Code:          cell_delta / total_gap contribution scoring. Stops at 95% explained.
  Type 2 — Data:          fuzzy join coverage (rapidfuzz), unmapped ratio, schema version diff.
  Type 3 — Orchestration: pipeline run timestamp diff, parameter diff, watermark gap.

IsolationForest pre-filters noise deltas before any attribution takes place.
All checks are deterministic Python — no AI is called here.
AI (Layer 5) is only invoked if overall investigator confidence < 80%.

All thresholds and metric definitions come from config.py.
"""

from __future__ import annotations

import hashlib
from typing import Optional

import pandas as pd

from config import (
    APP,
    DATA,
    DERIVED_METRICS,
    ISOLATION_FOREST_CONTAMINATION,
    ISOLATION_FOREST_MIN_ROWS,
)

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

# Layer 4 — Type 1: stop cell-replay attribution once this fraction of total gap is explained
TYPE1_STOP_THRESHOLD = 0.95  # 95 %

# Layer 4 — Type 2: rapidfuzz similarity floor for a fuzzy mapping match
TYPE2_FUZZY_MIN_SCORE = 80  # out of 100

# Layer 5 — trigger second AI call only when confidence is below this value
CONFIDENCE_AI_TRIGGER = 0.80  # 80 %

# Layer 4 — gap tolerance: differences smaller than this are considered noise
_GAP_NOISE_FLOOR = 1.0  # $1


# ─────────────────────────────────────────────────────────────────────────────
# Mapping + hierarchy helpers (shared by all investigation types)
# ─────────────────────────────────────────────────────────────────────────────

def apply_mapping(source: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Join source transactions with mapping table.
    Produces an enriched DataFrame with is_mapped, target_account_code,
    and metric_category columns.
    """
    mapping_clean = mapping[["source_account_code", "target_account_code", "metric_category"]].copy()
    mapping_clean["source_account_code"] = mapping_clean["source_account_code"].astype(str)
    source = source.copy()
    source["account_code"] = source["account_code"].astype(str)

    merged = source.merge(
        mapping_clean,
        left_on="account_code",
        right_on="source_account_code",
        how="left",
    )
    merged["is_mapped"] = merged["target_account_code"].notna()
    return merged


def apply_hierarchy(df: pd.DataFrame, hierarchy: Optional[pd.DataFrame]) -> pd.DataFrame:
    """
    Join with the entity hierarchy table to add BU, region, and segment columns.
    No-ops gracefully when hierarchy is None or missing entity_code.
    """
    if hierarchy is None or hierarchy.empty or "entity_code" not in hierarchy.columns:
        return df

    hier_cols = [c for c in ["entity_code", "business_unit", "region", "segment"] if c in hierarchy.columns]
    hierarchy_clean = hierarchy[hier_cols].drop_duplicates("entity_code")

    for col in ["business_unit", "region", "segment"]:
        if col in df.columns:
            df = df.drop(columns=[col], errors="ignore")

    return df.merge(hierarchy_clean, on="entity_code", how="left")


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4 — IsolationForest pre-filter (noise suppression)
# ─────────────────────────────────────────────────────────────────────────────

def filter_anomalous_deltas(enriched: pd.DataFrame) -> dict:
    """
    Layer 4 — IsolationForest pre-filter.

    Flags per-account amounts that are statistical outliers *before* any
    attribution logic runs.  Anomalous accounts are surfaced in the waterfall
    but excluded from contribution scoring so noise does not dilute confidence.

    Skipped if fewer than ISOLATION_FOREST_MIN_ROWS distinct accounts exist.
    Returns a result dict consumed by the orchestrator and the Layer 5 output.
    """
    if "account_code" not in enriched.columns or "amount" not in enriched.columns:
        return {
            "status": "skipped",
            "reason": "Required columns (account_code, amount) not found",
            "anomalous_accounts": [],
            "anomalous_account_codes": set(),
            "total_accounts": 0,
            "anomalous_count": 0,
        }

    per_account = enriched.groupby("account_code")["amount"].sum().reset_index()

    if len(per_account) < ISOLATION_FOREST_MIN_ROWS:
        return {
            "status": "skipped",
            "reason": f"Too few accounts ({len(per_account)} < {ISOLATION_FOREST_MIN_ROWS})",
            "anomalous_accounts": [],
            "anomalous_account_codes": set(),
            "total_accounts": len(per_account),
            "anomalous_count": 0,
        }

    try:
        from sklearn.ensemble import IsolationForest

        X = per_account[["amount"]].values
        clf = IsolationForest(
            contamination=ISOLATION_FOREST_CONTAMINATION,
            random_state=42,
            n_estimators=100,
        )
        per_account = per_account.copy()
        per_account["anomaly_label"] = clf.fit_predict(X)  # -1 = anomaly, 1 = normal
        per_account["is_anomalous"] = per_account["anomaly_label"] == -1

        anomalous = (
            per_account[per_account["is_anomalous"]]
            .sort_values("amount", key=abs, ascending=False)
            .head(20)
        )
        anomalous_codes = set(anomalous["account_code"].astype(str).tolist())

        return {
            "status": "complete",
            "total_accounts": len(per_account),
            "anomalous_count": int(per_account["is_anomalous"].sum()),
            "anomalous_account_codes": anomalous_codes,
            "anomalous_accounts": [
                {
                    "account_code": str(row["account_code"]),
                    "amount": round(float(row["amount"]), 2),
                }
                for _, row in anomalous.iterrows()
            ],
        }

    except ImportError:
        return {
            "status": "skipped",
            "reason": "scikit-learn not installed",
            "anomalous_accounts": [],
            "anomalous_account_codes": set(),
            "total_accounts": len(per_account),
            "anomalous_count": 0,
        }
    except Exception as exc:
        return {
            "status": "error",
            "reason": str(exc),
            "anomalous_accounts": [],
            "anomalous_account_codes": set(),
            "total_accounts": len(per_account),
            "anomalous_count": 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4 — Type 1: Code (cell contribution scoring)
# ─────────────────────────────────────────────────────────────────────────────

def run_type1_code_checks(
    enriched: pd.DataFrame,
    total_gap: float,
    anomalous_codes: set,
    ai_checklist: Optional[list] = None,
) -> dict:
    """
    Layer 4 — Type 1 (Code) hypothesis checks.

    Computes a per-account-code contribution score:
        contribution = account_delta / total_gap

    Iterates in the order recommended by the AI strategy planner (ai_checklist).
    Stops once cumulative explained fraction reaches TYPE1_STOP_THRESHOLD (95%).
    Anomalous accounts (pre-filtered by IsolationForest) are excluded from
    contribution scoring but recorded separately.

    Returns:
        culprit_cells   : ranked list of {account_code, delta, contribution_pct}
        explained_pct   : fraction of total_gap attributed (0–1)
        stopped_early   : True if the 95% threshold was reached
        noise_excluded  : accounts excluded by IsolationForest pre-filter
    """
    if "account_code" not in enriched.columns or "amount" not in enriched.columns:
        return {
            "culprit_cells": [],
            "explained_pct": 0.0,
            "stopped_early": False,
            "noise_excluded": list(anomalous_codes),
            "check_type": "type1_code",
        }

    if abs(total_gap) < _GAP_NOISE_FLOOR:
        return {
            "culprit_cells": [],
            "explained_pct": 1.0,
            "stopped_early": True,
            "noise_excluded": list(anomalous_codes),
            "check_type": "type1_code",
        }

    # Aggregate delta per account, exclude anomalous noise accounts
    per_account = (
        enriched[~enriched["account_code"].astype(str).isin(anomalous_codes)]
        .groupby("account_code")["amount"]
        .sum()
        .reset_index()
        .rename(columns={"amount": "delta"})
    )
    per_account["contribution_pct"] = per_account["delta"] / total_gap
    per_account["abs_contribution"] = per_account["contribution_pct"].abs()
    per_account = per_account.sort_values("abs_contribution", ascending=False)

    # Honour AI-ranked checklist order if provided
    if ai_checklist:
        checklist_order = {code: idx for idx, code in enumerate(ai_checklist)}
        per_account["ai_rank"] = per_account["account_code"].astype(str).map(
            lambda c: checklist_order.get(c, len(ai_checklist))
        )
        per_account = per_account.sort_values(["ai_rank", "abs_contribution"], ascending=[True, False])

    # Accumulate until 95% explained
    culprit_cells = []
    cumulative = 0.0
    stopped_early = False

    for _, row in per_account.iterrows():
        entry = {
            "account_code": str(row["account_code"]),
            "delta": round(float(row["delta"]), 2),
            "contribution_pct": round(float(row["contribution_pct"]) * 100, 2),
        }
        culprit_cells.append(entry)
        cumulative += abs(float(row["contribution_pct"]))
        if cumulative >= TYPE1_STOP_THRESHOLD:
            stopped_early = True
            break

    return {
        "culprit_cells": culprit_cells,
        "explained_pct": round(min(cumulative, 1.0), 4),
        "stopped_early": stopped_early,
        "noise_excluded": list(anomalous_codes),
        "check_type": "type1_code",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4 — Type 2: Data (fuzzy mapping, unmapped ratio, schema version diff)
# ─────────────────────────────────────────────────────────────────────────────

def run_type2_data_checks(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Layer 4 — Type 2 (Data) hypothesis checks.

    Three sub-checks:
      2a. Fuzzy join coverage: rapidfuzz on mapping keys to find near-miss
          unmapped account codes (score >= TYPE2_FUZZY_MIN_SCORE).
      2b. Unmapped ratio: % of source transactions with no mapping entry.
      2c. Schema version diff: column-level comparison between source and
          target schemas to detect version drift.

    All checks are deterministic Python — no AI involved.
    Returns a structured dict consumed by Layer 5 for waterfall attribution.
    """
    result: dict = {
        "check_type": "type2_data",
        "fuzzy_matches": [],
        "unmapped_ratio": 0.0,
        "unmapped_amount": 0.0,
        "unmapped_codes": [],
        "schema_diff": {},
        "has_schema_drift": False,
        "confidence": 0.0,
    }

    # ── 2a. Fuzzy join coverage ───────────────────────────────────────────────
    if "account_code" in source.columns and "source_account_code" in mapping.columns:
        source_codes = source["account_code"].dropna().astype(str).unique().tolist()
        mapped_codes = set(mapping["source_account_code"].dropna().astype(str).tolist())
        unmapped_source = [c for c in source_codes if c not in mapped_codes]

        fuzzy_matches = []
        try:
            from rapidfuzz import process, fuzz

            for code in unmapped_source[:200]:  # cap at 200 to stay fast
                hits = process.extractOne(
                    code,
                    list(mapped_codes),
                    scorer=fuzz.token_sort_ratio,
                    score_cutoff=TYPE2_FUZZY_MIN_SCORE,
                )
                if hits:
                    match_code, score, _ = hits
                    # Look up the metric_category for the suggested mapping
                    suggested_category = ""
                    if "metric_category" in mapping.columns:
                        cat_rows = mapping[
                            mapping["source_account_code"].astype(str) == match_code
                        ]["metric_category"]
                        if not cat_rows.empty:
                            suggested_category = str(cat_rows.iloc[0])

                    fuzzy_matches.append({
                        "source_code": code,
                        "suggested_code": match_code,
                        "score": round(float(score), 1),
                        "suggested_category": suggested_category,
                    })

        except ImportError:
            # rapidfuzz not installed — skip fuzzy sub-check gracefully
            pass

        result["fuzzy_matches"] = sorted(fuzzy_matches, key=lambda x: -x["score"])

        # ── 2b. Unmapped ratio ────────────────────────────────────────────────
        total_rows = len(source)
        unmapped_rows = source[~source["account_code"].astype(str).isin(mapped_codes)]
        unmapped_count = len(unmapped_rows)
        unmapped_amount = float(unmapped_rows["amount"].sum()) if "amount" in unmapped_rows.columns else 0.0

        result["unmapped_ratio"] = round(unmapped_count / total_rows, 4) if total_rows > 0 else 0.0
        result["unmapped_amount"] = round(unmapped_amount, 2)
        result["unmapped_codes"] = [
            {"account_code": str(k), "row_count": int(v)}
            for k, v in source[~source["account_code"].astype(str).isin(mapped_codes)]
            ["account_code"].value_counts().head(20).items()
        ]

    # ── 2c. Schema version diff ───────────────────────────────────────────────
    if target is not None and not target.empty:
        source_cols = set(source.columns.tolist())
        target_cols = set(target.columns.tolist())

        missing_in_target = sorted(source_cols - target_cols)
        missing_in_source = sorted(target_cols - source_cols)
        common_cols = source_cols & target_cols

        # Check dtype drift on shared columns
        dtype_drift = {}
        for col in common_cols:
            s_dtype = str(source[col].dtype)
            t_dtype = str(target[col].dtype)
            if s_dtype != t_dtype:
                dtype_drift[col] = {"source_dtype": s_dtype, "target_dtype": t_dtype}

        schema_diff = {
            "missing_in_target": missing_in_target,
            "missing_in_source": missing_in_source,
            "dtype_drift": dtype_drift,
            "schema_hash_source": _schema_hash(source),
            "schema_hash_target": _schema_hash(target),
        }
        result["schema_diff"] = schema_diff
        result["has_schema_drift"] = bool(missing_in_target or missing_in_source or dtype_drift)

    # ── Confidence score ──────────────────────────────────────────────────────
    # Higher confidence when: low unmapped ratio, no schema drift, fuzzy hits found
    conf = 1.0
    if result["unmapped_ratio"] > 0.05:
        conf -= 0.3
    if result["has_schema_drift"]:
        conf -= 0.2
    if not result["fuzzy_matches"]:
        conf -= 0.1
    result["confidence"] = round(max(conf, 0.0), 3)

    return result


def _schema_hash(df: pd.DataFrame) -> str:
    """Lightweight fingerprint of a DataFrame's column names + dtypes."""
    sig = ",".join(f"{c}:{df[c].dtype}" for c in sorted(df.columns))
    return hashlib.md5(sig.encode()).hexdigest()[:8]


# ─────────────────────────────────────────────────────────────────────────────
# Layer 4 — Type 3: Orchestration (timestamp diff, parameter diff, watermark gap)
# ─────────────────────────────────────────────────────────────────────────────

def run_type3_orchestration_checks(
    pipeline_graph: Optional[dict] = None,
    source_run_metadata: Optional[dict] = None,
    target_run_metadata: Optional[dict] = None,
) -> dict:
    """
    Layer 4 — Type 3 (Orchestration) hypothesis checks.

    Three sub-checks:
      3a. Run timestamp diff: compare last successful run timestamps between
          source pipeline and target pipeline.  A lag > threshold suggests
          the target consumed a stale snapshot.
      3b. Parameter diff: compare parameter snapshots across runs.  Mismatched
          filter dates or aggregation keys are a common reconciliation root cause.
      3c. Watermark gap: compare high-watermark values to detect incremental
          load windows that don't align.

    All inputs are metadata dicts from the pipeline parser (Layer 1) — no raw
    data rows are examined here.

    Returns a structured dict with found discrepancies and a confidence score.
    """
    result: dict = {
        "check_type": "type3_orchestration",
        "timestamp_diff_seconds": None,
        "timestamp_gap_flag": False,
        "parameter_diffs": [],
        "watermark_gap": None,
        "watermark_gap_flag": False,
        "pipeline_steps_checked": [],
        "confidence": 0.0,
        "findings": [],
    }

    if not source_run_metadata and not target_run_metadata:
        result["confidence"] = 0.5  # no metadata available — inconclusive
        result["findings"].append({
            "type": "no_metadata",
            "detail": "No pipeline run metadata provided. Orchestration checks skipped.",
        })
        return result

    source_meta = source_run_metadata or {}
    target_meta = target_run_metadata or {}

    # ── 3a. Timestamp diff ────────────────────────────────────────────────────
    src_ts = source_meta.get("last_successful_run_ts")
    tgt_ts = target_meta.get("last_successful_run_ts")

    if src_ts and tgt_ts:
        try:
            import datetime as dt
            fmt = "%Y-%m-%dT%H:%M:%S"
            src_dt = dt.datetime.strptime(src_ts[:19], fmt)
            tgt_dt = dt.datetime.strptime(tgt_ts[:19], fmt)
            diff_seconds = abs((src_dt - tgt_dt).total_seconds())
            result["timestamp_diff_seconds"] = int(diff_seconds)

            # Flag if pipelines ran more than 1 hour apart
            if diff_seconds > 3600:
                result["timestamp_gap_flag"] = True
                result["findings"].append({
                    "type": "timestamp_gap",
                    "detail": (
                        f"Source pipeline last ran at {src_ts}, "
                        f"target at {tgt_ts} — "
                        f"{diff_seconds/3600:.1f}h gap. "
                        "Target may have consumed a stale snapshot."
                    ),
                })
        except (ValueError, TypeError):
            pass

    # ── 3b. Parameter diff ────────────────────────────────────────────────────
    src_params = source_meta.get("parameters", {})
    tgt_params = target_meta.get("parameters", {})

    if src_params or tgt_params:
        all_param_keys = set(list(src_params.keys()) + list(tgt_params.keys()))
        for key in sorted(all_param_keys):
            src_val = src_params.get(key)
            tgt_val = tgt_params.get(key)
            if src_val != tgt_val:
                diff = {
                    "parameter": key,
                    "source_value": str(src_val),
                    "target_value": str(tgt_val),
                }
                result["parameter_diffs"].append(diff)
                result["findings"].append({
                    "type": "parameter_mismatch",
                    "detail": (
                        f"Parameter '{key}' differs: source={src_val!r}, "
                        f"target={tgt_val!r}. "
                        "Mismatched filter dates or aggregation keys commonly cause gaps."
                    ),
                })

    # ── 3c. Watermark gap ─────────────────────────────────────────────────────
    src_watermark = source_meta.get("high_watermark")
    tgt_watermark = target_meta.get("high_watermark")

    if src_watermark is not None and tgt_watermark is not None:
        try:
            gap = float(src_watermark) - float(tgt_watermark)
            result["watermark_gap"] = round(gap, 4)
            if abs(gap) > 0:
                result["watermark_gap_flag"] = True
                result["findings"].append({
                    "type": "watermark_gap",
                    "detail": (
                        f"Incremental load watermark mismatch: "
                        f"source={src_watermark}, target={tgt_watermark} "
                        f"(gap={gap:+.4f}). "
                        "Records in the gap window may not have been loaded into target."
                    ),
                })
        except (ValueError, TypeError):
            pass

    # ── Pipeline steps checked (from pipeline graph, Layer 1) ─────────────────
    if pipeline_graph:
        steps = pipeline_graph.get("steps", [])
        result["pipeline_steps_checked"] = [
            s.get("step_name", f"step_{i}") for i, s in enumerate(steps[:10])
        ]

    # ── Confidence score ──────────────────────────────────────────────────────
    conf = 0.5  # base — we have metadata
    if src_ts and tgt_ts:
        conf += 0.2  # timestamps available
    if not result["timestamp_gap_flag"] and not result["parameter_diffs"] and not result["watermark_gap_flag"]:
        conf += 0.3  # orchestration is clean — high confidence nothing here
    elif result["findings"]:
        conf += 0.2  # found something concrete
    result["confidence"] = round(min(conf, 1.0), 3)

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Metric computation (feeds Layer 4 checks and Layer 5 output)
# ─────────────────────────────────────────────────────────────────────────────

def compute_our_metrics(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    hierarchy: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Apply mapping + hierarchy and compute all metrics dynamically.

    Metrics are derived from the categories present in the mapping table,
    plus any DERIVED_METRICS defined in config (e.g. Gross Margin = Revenue + COGS).
    Intercompany transactions are excluded from group-level totals.

    Returns:
        by_metric : {metric_name: total_amount}
        by_entity : DataFrame grouped by entity/metric
        enriched  : full enriched source DataFrame (used by Layer 4 checks)
    """
    enriched = apply_mapping(source, mapping)
    enriched = apply_hierarchy(enriched, hierarchy)

    non_ic_mask = (
        ~enriched["is_intercompany"]
        if "is_intercompany" in enriched.columns
        else pd.Series(True, index=enriched.index)
    )
    mapped_mask = enriched["is_mapped"]
    mapped = enriched[non_ic_mask & mapped_mask]

    by_metric: dict[str, float] = {}
    categories_in_mapping = (
        mapped["metric_category"].dropna().unique().tolist()
        if "metric_category" in mapped.columns
        else []
    )

    for cat in categories_in_mapping:
        by_metric[cat] = float(mapped[mapped["metric_category"] == cat]["amount"].sum())

    # Derived metrics (e.g. Gross Margin = Revenue + COGS, costs stored negative)
    for derived_name, (component_a, component_b) in DERIVED_METRICS.items():
        val_a = by_metric.get(component_a, 0.0)
        val_b = by_metric.get(component_b, 0.0)
        by_metric[derived_name] = val_a + val_b

    # Per-entity breakdown for Layer 5 drill-down
    group_cols = [
        c for c in ["entity_code", "business_unit", "region", "metric_category"]
        if c in mapped.columns
    ]
    if group_cols and "amount" in mapped.columns:
        by_entity = (
            mapped.groupby(group_cols)["amount"]
            .sum()
            .reset_index()
            .rename(columns={"amount": "our_amount"})
        )
    else:
        by_entity = pd.DataFrame()

    return {
        "by_metric": by_metric,
        "by_entity": by_entity,
        "enriched": enriched,
    }


def parse_target_metrics(target: pd.DataFrame) -> dict:
    """
    Parse the target results DataFrame into {metric: total_amount}.
    Prefers 'Group Total' rows to avoid double-counting regional sub-totals.
    """
    if target.empty or "metric" not in target.columns or "amount" not in target.columns:
        return {}

    if "group_by" in target.columns:
        group_total = target[
            target["group_by"].str.contains("Total|Group|All", case=False, na=False)
        ]
        if not group_total.empty:
            return group_total.groupby("metric")["amount"].sum().to_dict()

    return target.groupby("metric")["amount"].sum().to_dict()


def compute_gap_summary(our_metrics: dict, target_metrics: dict) -> dict:
    """
    Compute the headline gap for each metric.
    Used by Layer 4 to prioritise which metrics to investigate,
    and by Layer 5 to build the waterfall.
    """
    gaps: dict = {}
    all_metrics = set(list(our_metrics.keys()) + list(target_metrics.keys()))

    for metric in all_metrics:
        our_val = our_metrics.get(metric, 0.0)
        target_val = target_metrics.get(metric, 0.0)
        gap = our_val - target_val
        pct = (gap / target_val * 100) if target_val != 0 else None

        gaps[metric] = {
            "our_value": round(our_val, 2),
            "target_value": round(target_val, 2),
            "gap": round(gap, 2),
            "gap_pct": round(pct, 2) if pct is not None else None,
            "has_gap": abs(gap) > DATA["gap_threshold"],
        }

    return gaps


# ─────────────────────────────────────────────────────────────────────────────
# Layer 5 — Variance waterfall builder
# ─────────────────────────────────────────────────────────────────────────────

def build_variance_waterfall(attribution_findings: list[dict]) -> list[dict]:
    """
    Layer 5 — Build the waterfall data list from attribution findings.

    Each finding must have: label, amount, pct_of_gap, category.
    The waterfall records a running total for chart rendering.
    Attribution is drawn from Type 1/2/3 findings in priority order,
    with unexplained residual as the final bar.
    """
    waterfall = []
    running = 0.0

    for finding in attribution_findings:
        amount = finding.get("amount", 0)
        waterfall.append({
            "label": finding.get("label", "Unknown"),
            "amount": round(amount, 2),
            "pct_of_gap": round(finding.get("pct_of_gap", 0), 1),
            "category": finding.get("category", "Other"),
            "check_type": finding.get("check_type", ""),   # type1 / type2 / type3
            "running_total": round(running + amount, 2),
            "direction": "negative" if amount < 0 else "positive",
        })
        running += amount

    return waterfall


def compute_confidence_gate(
    type1_result: dict,
    type2_result: dict,
    type3_result: dict,
) -> dict:
    """
    Layer 5 — Aggregate confidence across all three check types.

    If the combined confidence is below CONFIDENCE_AI_TRIGGER (80%),
    Layer 5 will call Gemini for a narrative (~400 tokens of data summary
    + path traversed).  Above 80% the narrative is generated locally
    (zero AI tokens).

    Returns:
        overall_confidence : float 0–1
        needs_ai_narrative : bool
        confidence_by_type : per-type breakdown for the resolution table
    """
    c1 = type1_result.get("explained_pct", 0.0)
    c2 = type2_result.get("confidence", 0.0)
    c3 = type3_result.get("confidence", 0.0)

    # Weighted average: Type 1 carries most weight as it has direct delta evidence
    overall = (c1 * 0.55) + (c2 * 0.25) + (c3 * 0.20)
    overall = round(min(overall, 1.0), 4)

    return {
        "overall_confidence": overall,
        "needs_ai_narrative": overall < CONFIDENCE_AI_TRIGGER,
        "confidence_by_type": {
            "type1_code_explained_pct": round(c1, 4),
            "type2_data_confidence": round(c2, 4),
            "type3_orchestration_confidence": round(c3, 4),
        },
    }


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def format_currency(amount: float) -> str:
    """Format amount as a compact currency string using the configured symbol."""
    symbol = APP["currency_symbol"]
    if abs(amount) >= 1_000_000:
        return f"{symbol}{amount / 1_000_000:.1f}M"
    elif abs(amount) >= 1_000:
        return f"{symbol}{amount / 1_000:.1f}K"
    else:
        return f"{symbol}{amount:,.2f}"