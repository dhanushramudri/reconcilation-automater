"""
Layer 4 — Investigation tool functions.

Each function is a deterministic Python check.  These are called by the
orchestrator (Layer 2 AI plan → Layer 4 execution) in the order ranked by
the AI strategy planner.  No AI is invoked inside this module.

Check taxonomy (mirrors reconciler.py):
  Type 1 — Code:          mapping gaps, IC eliminations, adjustment flags.
                          Contribution score = cell_delta / total_gap.
                          Stops at 95% explained.
  Type 2 — Data:          fuzzy join coverage (rapidfuzz), unmapped ratio,
                          schema version diff.  Consumes type2 result from
                          reconciler.run_type2_data_checks().
  Type 3 — Orchestration: pipeline timestamp diff, parameter diff, watermark
                          gap.  Consumes type3 result from
                          reconciler.run_type3_orchestration_checks().

IsolationForest pre-filtering is done in reconciler.filter_anomalous_deltas()
before any investigators run.  Anomalous account codes are passed in and
excluded from contribution scoring so noise does not inflate confidence.

Layer 5 triggers a second AI call (~400 tokens) only when the confidence gate
(reconciler.compute_confidence_gate) returns needs_ai_narrative=True, i.e.
overall confidence < 80%.

All thresholds and confidence priors come from config.py.
"""

from __future__ import annotations

import hashlib
from typing import Optional

import pandas as pd

from config import (
    CAUSAL_THRESHOLD,
    CHECKSUM_ENABLED,
    CONFIDENCE,
    DATA,
    TOLERANCE_PCT,
)

# Re-export reconciler helpers used by the orchestrator so it only needs to
# import from one place.
from core.reconciler import (
    TYPE1_STOP_THRESHOLD,
    CONFIDENCE_AI_TRIGGER,
    compute_confidence_gate,
    run_type1_code_checks,
    run_type2_data_checks,
    run_type3_orchestration_checks,
)


# ─────────────────────────────────────────────────────────────────────────────
# Type 1 — Code checks
# ─────────────────────────────────────────────────────────────────────────────

def check_mapping_gaps(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    anomalous_codes: Optional[set] = None,
    focus_metric: str = "all",
) -> dict:
    """
    Type 1 — Code check: find source account codes with no mapping entry.

    Excludes accounts already flagged as anomalous by IsolationForest so that
    outlier accounts don't inflate the mapping-gap attribution amount.

    Returns unmapped transactions aggregated by account code, a suggested fix
    list for Layer 5, and an MD5 checksum of the findings for audit purposes.
    """
    if "account_code" not in source.columns or "source_account_code" not in mapping.columns:
        return {"status": "skipped", "reason": "Required columns not found", "findings": []}

    _anomalous = anomalous_codes or set()
    mapped_codes = set(mapping["source_account_code"].astype(str))

    source = source.copy()
    source["_acc"] = source["account_code"].astype(str)

    # Exclude anomalous accounts — they are surfaced separately in the waterfall
    clean_source = source[~source["_acc"].isin(_anomalous)]
    unmapped = clean_source[~clean_source["_acc"].isin(mapped_codes)]

    if unmapped.empty:
        return {
            "status": "clean",
            "unmapped_count": 0,
            "unmapped_amount": 0.0,
            "findings": [],
            "message": "All account codes are mapped. No mapping gap.",
        }

    agg = (
        unmapped.groupby("account_code")
        .agg(
            transaction_count=("amount", "count"),
            total_amount=("amount", "sum"),
            avg_amount=("amount", "mean"),
        )
        .reset_index()
        .sort_values("total_amount", key=abs, ascending=False)
    )

    findings = [
        {
            "account_code": str(row["account_code"]),
            "transaction_count": int(row["transaction_count"]),
            "total_amount": round(float(row["total_amount"]), 2),
            "avg_amount": round(float(row["avg_amount"]), 2),
        }
        for _, row in agg.iterrows()
    ]

    total_unmapped_amount = float(unmapped["amount"].sum())

    # Suggested fix entries for Layer 5 fix preview
    suggested_fixes = [
        {
            "source_account_code": f["account_code"],
            "target_account_code": f"TGT_{f['account_code']}",
            "metric_category": DATA["default_metric_category"],
            "note": "AUTO-SUGGESTED — verify metric category before applying",
        }
        for f in findings[:10]
    ]

    checksum = ""
    if CHECKSUM_ENABLED:
        raw = "|".join(f"{f['account_code']}:{f['total_amount']:.2f}" for f in findings)
        checksum = hashlib.md5(raw.encode()).hexdigest()

    return {
        "status": "gap_found",
        "unmapped_account_count": len(findings),
        "unmapped_transaction_count": int(unmapped["account_code"].count()),
        "unmapped_amount": round(total_unmapped_amount, 2),
        "findings": findings,
        "suggested_fix_entries": suggested_fixes,
        "checksum": checksum,
        "check_type": "type1_code",
        "message": (
            f"{len(findings)} account codes ({unmapped['account_code'].count()} transactions, "
            f"${abs(total_unmapped_amount):,.0f} total) have no mapping to target accounts."
        ),
    }


def check_intercompany_eliminations(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    anomalous_codes: Optional[set] = None,
) -> dict:
    """
    Type 1 — Code check: verify intercompany transactions have matching
    elimination entries.

    Uneliminated IC transactions inflate group-level metrics — this is a
    code-layer issue because it reflects a missing journal entry in the
    consolidation step, not a data mapping problem.

    Excludes anomalous accounts passed in from the IsolationForest pre-filter.
    """
    if "is_intercompany" not in source.columns:
        return {
            "status": "skipped",
            "reason": "is_intercompany column not present in source data",
            "findings": [],
            "check_type": "type1_code",
        }

    _anomalous = anomalous_codes or set()
    clean_source = source[
        ~source.get("account_code", pd.Series(dtype=str)).astype(str).isin(_anomalous)
    ]

    ic_txns = clean_source[clean_source["is_intercompany"] == True].copy()

    if ic_txns.empty:
        return {
            "status": "clean",
            "ic_transaction_count": 0,
            "uneliminated_amount": 0.0,
            "findings": [],
            "check_type": "type1_code",
            "message": "No intercompany transactions found. No elimination issue.",
        }

    total_ic_amount = float(ic_txns["amount"].sum())
    findings = []

    if "counterparty_entity" in ic_txns.columns and "entity_code" in ic_txns.columns:
        pairs = ic_txns.groupby(["entity_code", "counterparty_entity"])["amount"].sum().reset_index()

        for _, row in pairs.iterrows():
            entity = row["entity_code"]
            counterparty = row["counterparty_entity"]
            amount = float(row["amount"])

            reverse_mask = (
                (ic_txns["entity_code"] == counterparty) &
                (ic_txns["counterparty_entity"] == entity)
            )
            reverse_amount = float(ic_txns[reverse_mask]["amount"].sum())
            net = amount + reverse_amount

            if abs(net) > 0.01:
                findings.append({
                    "entity": entity,
                    "counterparty": str(counterparty) if pd.notna(counterparty) else "Unknown",
                    "debit_amount": round(amount, 2),
                    "credit_amount": round(reverse_amount, 2),
                    "net_uneliminated": round(net, 2),
                    "transaction_ids": (
                        ic_txns[
                            (ic_txns["entity_code"] == entity) &
                            (ic_txns["counterparty_entity"] == counterparty)
                        ]["transaction_id"].head(5).tolist()
                        if "transaction_id" in ic_txns.columns else []
                    ),
                })
    else:
        agg = ic_txns.groupby("entity_code")["amount"].sum().reset_index()
        findings = [
            {
                "entity": row["entity_code"],
                "counterparty": "Unknown",
                "net_uneliminated": round(float(row["amount"]), 2),
            }
            for _, row in agg.iterrows()
        ]

    uneliminated = sum(abs(f["net_uneliminated"]) for f in findings)

    return {
        "status": "issue_found" if findings else "clean",
        "ic_transaction_count": len(ic_txns),
        "total_ic_amount": round(total_ic_amount, 2),
        "uneliminated_pair_count": len(findings),
        "uneliminated_amount": round(uneliminated, 2),
        "findings": findings,
        "check_type": "type1_code",
        "message": (
            f"{len(ic_txns)} IC transactions found. "
            f"{len(findings)} entity pairs have uneliminated balances "
            f"totalling ${uneliminated:,.0f}."
        ) if findings else f"{len(ic_txns)} IC transactions found — all properly offset.",
    }


def check_manual_adjustments(
    source: pd.DataFrame,
    target: pd.DataFrame,
    anomalous_codes: Optional[set] = None,
) -> dict:
    """
    Type 1 — Code check: identify manual adjustment entries that may cause
    discrepancies between source and target.

    Manual adjustments are a code-layer issue: they represent journal entries
    applied in the consolidation or reporting system that are not present
    in the operational source.
    """
    if "adjustment_flag" not in source.columns:
        return {
            "status": "skipped",
            "reason": "adjustment_flag column not in source data",
            "findings": [],
            "check_type": "type1_code",
        }

    _anomalous = anomalous_codes or set()
    clean_source = source[
        ~source.get("account_code", pd.Series(dtype=str)).astype(str).isin(_anomalous)
    ]

    adj_txns = clean_source[clean_source["adjustment_flag"] == True].copy()

    if adj_txns.empty:
        adj_in_source = {"count": 0, "amount": 0.0, "by_type": {}}
    else:
        by_type: dict = {}
        if "adjustment_type" in adj_txns.columns:
            by_type = adj_txns.groupby("adjustment_type")["amount"].sum().round(2).to_dict()

        adj_in_source = {
            "count": len(adj_txns),
            "amount": round(float(adj_txns["amount"].sum()), 2),
            "by_type": by_type,
            "entities_affected": (
                adj_txns["entity_code"].unique().tolist()
                if "entity_code" in adj_txns.columns else []
            ),
        }

    # Infer implied target-only adjustments from the target data shape
    implied_adj_gap = 0.0
    if not target.empty and "amount" in target.columns and "metric" in target.columns:
        if "group_by" in target.columns:
            headline_target = target[
                target["group_by"].str.contains("Total|Group|All", case=False, na=False)
            ]
        elif "row_type" in target.columns:
            headline_target = target[target["row_type"] == "headline"]
        else:
            headline_target = target

        if not adj_txns.empty:
            implied_adj_gap = float(adj_txns["amount"].sum())

    return {
        "status": "found" if not adj_txns.empty else "clean",
        "adjustments_in_source": adj_in_source,
        "implied_target_adjustment": round(implied_adj_gap, 2),
        "check_type": "type1_code",
        "message": (
            f"{adj_txns.shape[0]} manual adjustment entries in source totalling "
            f"${abs(float(adj_txns['amount'].sum())):,.0f}. "
            f"Implied target-only adjustment: ${abs(implied_adj_gap):,.0f}."
        ) if not adj_txns.empty else "No manual adjustment flags in source data.",
    }


# ─────────────────────────────────────────────────────────────────────────────
# Type 2 — Data checks (hierarchy mismatch at regional level)
# ─────────────────────────────────────────────────────────────────────────────

def check_hierarchy_mismatches(
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: pd.DataFrame,
    hierarchy: Optional[pd.DataFrame] = None,
    type2_result: Optional[dict] = None,
) -> dict:
    """
    Type 2 — Data check: detect entities that roll up differently in our
    hierarchy vs the client's target system.

    Compares revenue per region/group between our hierarchy and client target
    detail rows.  Only uses detail rows (not Group Total) to avoid
    double-counting.

    Accepts an optional type2_result from reconciler.run_type2_data_checks()
    to avoid re-running the fuzzy/schema checks.  The schema version diff
    from type2_result is surfaced in the findings if schema drift is detected,
    as hierarchy mismatches are often caused by a schema version change in the
    mapping or entity table.
    """
    if target.empty or "metric" not in target.columns:
        return {
            "status": "skipped",
            "reason": "Target data missing or invalid",
            "findings": [],
            "check_type": "type2_data",
        }

    # Extract detail-level rows only — skip Group Total to avoid double-counting
    if "row_type" in target.columns:
        detail_target = target[target["row_type"] == "detail"]
    elif "group_by" in target.columns:
        detail_target = target[
            ~target["group_by"].str.contains("Total|Group|All", case=False, na=False)
        ]
    else:
        return {
            "status": "skipped",
            "reason": "No detail-level rows found in target",
            "findings": [],
            "check_type": "type2_data",
        }

    if detail_target.empty:
        return {
            "status": "clean",
            "findings": [],
            "check_type": "type2_data",
            "message": "No regional hierarchy breakdown in target to compare.",
        }

    # Apply mapping to source for metric category assignment
    mapped_codes = (
        set(mapping["source_account_code"].astype(str))
        if "source_account_code" in mapping.columns else set()
    )
    source = source.copy()
    source["_acc"] = source["account_code"].astype(str)

    ic_col = source.get("is_intercompany", pd.Series(False, index=source.index))
    mapped_source = source[source["_acc"].isin(mapped_codes) & ~ic_col]

    if "source_account_code" in mapping.columns and "metric_category" in mapping.columns:
        mapping_clean = mapping[["source_account_code", "metric_category"]].copy()
        mapping_clean["source_account_code"] = mapping_clean["source_account_code"].astype(str)
        mapped_source = mapped_source.merge(
            mapping_clean, left_on="_acc", right_on="source_account_code", how="left"
        )

    mismatches = []
    revenue_metrics = (
        detail_target["metric"].unique().tolist() if "metric" in detail_target.columns else []
    )

    # Pick the primary revenue metric
    rev_metric = next(
        (m for m in revenue_metrics if "rev" in m.lower() or "sales" in m.lower()),
        revenue_metrics[0] if revenue_metrics else None,
    )

    if "group_by" in detail_target.columns and "region" in mapped_source.columns and rev_metric:
        if "metric_category" in mapped_source.columns:
            rev_source = mapped_source[
                mapped_source["metric_category"].str.lower().str.contains("rev|sales", na=False)
            ]
        else:
            rev_source = mapped_source

        for region in detail_target[detail_target["metric"] == rev_metric]["group_by"].unique():
            target_val = float(
                detail_target[
                    (detail_target["metric"] == rev_metric) &
                    (detail_target["group_by"] == region)
                ]["amount"].sum()
            )
            our_val = float(rev_source[rev_source["region"] == region]["amount"].sum())
            gap = our_val - target_val

            if abs(gap) > DATA["gap_threshold"]:
                mismatches.append({
                    "metric": rev_metric,
                    "group": region,
                    "our_value": round(our_val, 2),
                    "target_value": round(target_val, 2),
                    "gap": round(gap, 2),
                    "gap_pct": round(gap / target_val * 100, 1) if target_val != 0 else None,
                    "note": "Entity may roll up to different region in client's hierarchy",
                })

    # Augment with schema drift note from type2_result if available
    schema_note = ""
    if type2_result and type2_result.get("has_schema_drift"):
        schema_note = (
            " Schema drift detected between source and target — "
            "mapping or entity table version may have changed."
        )

    if not mismatches:
        return {
            "status": "clean",
            "findings": [],
            "check_type": "type2_data",
            "message": f"No significant hierarchy mismatches at regional level.{schema_note}",
        }

    total_mismatch = sum(abs(m["gap"]) for m in mismatches)

    return {
        "status": "mismatch_found",
        "mismatch_count": len(mismatches),
        "total_mismatch_amount": round(total_mismatch, 2),
        "findings": mismatches,
        "schema_drift_note": schema_note,
        "check_type": "type2_data",
        "message": (
            f"{len(mismatches)} region(s) have revenue rollup differences totalling "
            f"${total_mismatch:,.0f}. Likely caused by entity hierarchy assignment differences "
            f"(e.g., an entity assigned to EMEA in our system but 'Corporate' in client's system)."
            f"{schema_note}"
        ),
    }


# ─────────────────────────────────────────────────────────────────────────────
# Variance attribution — assembles Type 1 / 2 / 3 findings into waterfall
# ─────────────────────────────────────────────────────────────────────────────

def compute_variance_attribution(
    total_gap: float,
    mapping_result: dict,
    hierarchy_result: dict,
    ic_result: dict,
    adjustment_result: dict,
    type2_result: Optional[dict] = None,
    type3_result: Optional[dict] = None,
    anomaly_result: Optional[dict] = None,
) -> dict:
    """
    Layer 5 — Assemble all Type 1 / 2 / 3 investigation findings into a
    variance waterfall.

    Attribution order (highest signal first, matches AI-prioritised checklist):
      1. Mapping Gap           — Type 1 Code     (highest confidence)
      2. IC Elimination        — Type 1 Code
      3. Orchestration Timing  — Type 3           (new: timestamp/watermark)
      4. Data Schema Drift     — Type 2
      5. Manual Adjustment     — Type 1 (residual)
      6. Hierarchy Mismatch    — Type 2 (informational, $0 net at group level)
      7. Anomalous Accounts    — IsolationForest  (informational)
      8. Unexplained Residual

    Components are capped so they never overshoot the remaining unattributed gap.
    The confidence gate (compute_confidence_gate) determines whether Layer 5
    triggers a second AI call for the narrative.
    """
    components: list[dict] = []
    remaining_gap = total_gap
    gap_sign = -1 if total_gap < 0 else 1

    def _signed(raw_positive: float) -> float:
        return gap_sign * abs(raw_positive)

    def _cap(amt: float) -> float:
        if abs(amt) > abs(remaining_gap):
            return remaining_gap
        return amt

    # ── 1. Mapping Gap (Type 1 — Code) ───────────────────────────────────────
    if mapping_result.get("status") == "gap_found":
        raw = mapping_result.get("unmapped_amount", 0.0)
        amt = _cap(_signed(raw))
        components.append({
            "category": "Mapping Gap",
            "label": "Unmapped Account Codes",
            "amount": round(float(amt), 2),
            "pct_of_gap": round(float(amt) / total_gap * 100, 1) if total_gap != 0 else 0,
            "confidence": CONFIDENCE["mapping_gap"],
            "detail": mapping_result.get("message", ""),
            "fix_type": "add_mapping",
            "check_type": "type1_code",
            "drill_down": mapping_result.get("findings", [])[:10],
        })
        remaining_gap -= float(amt)

    # ── 2. IC Elimination (Type 1 — Code) ────────────────────────────────────
    if ic_result.get("status") == "issue_found" and abs(remaining_gap) > 1.0:
        raw = float(ic_result.get("uneliminated_amount", 0.0))
        ic_contribution = _cap(min(raw, abs(remaining_gap)) * gap_sign)
        if abs(ic_contribution) > 1.0:
            components.append({
                "category": "Intercompany Elimination",
                "label": "Uneliminated IC Transactions",
                "amount": round(float(ic_contribution), 2),
                "pct_of_gap": round(float(ic_contribution) / total_gap * 100, 1) if total_gap != 0 else 0,
                "confidence": CONFIDENCE["intercompany"],
                "detail": ic_result.get("message", ""),
                "fix_type": "add_elimination",
                "check_type": "type1_code",
                "drill_down": ic_result.get("findings", [])[:10],
            })
            remaining_gap -= float(ic_contribution)

    # ── 3. Orchestration Timing Gap (Type 3) ──────────────────────────────────
    if (
        type3_result
        and type3_result.get("findings")
        and abs(remaining_gap) > 1.0
    ):
        # Orchestration issues don't have a direct $ amount — we attribute a
        # share of the remaining gap proportional to orchestration confidence
        orch_confidence = type3_result.get("confidence", 0.0)
        if orch_confidence > 0.3 and type3_result.get("timestamp_gap_flag") or type3_result.get("parameter_diffs") or type3_result.get("watermark_gap_flag"):
            # Attribute up to 30% of remaining gap to orchestration timing
            orch_share = _cap(_signed(abs(remaining_gap) * min(orch_confidence * 0.5, 0.30)))
            if abs(orch_share) > 1.0:
                detail_lines = [f["detail"] for f in type3_result["findings"][:3]]
                components.append({
                    "category": "Orchestration Timing",
                    "label": "Pipeline Timestamp / Watermark Mismatch",
                    "amount": round(float(orch_share), 2),
                    "pct_of_gap": round(float(orch_share) / total_gap * 100, 1) if total_gap != 0 else 0,
                    "confidence": round(orch_confidence, 3),
                    "detail": " | ".join(detail_lines) or type3_result.get("message", ""),
                    "fix_type": "fix_orchestration",
                    "check_type": "type3_orchestration",
                    "drill_down": type3_result.get("findings", [])[:5],
                })
                remaining_gap -= float(orch_share)

    # ── 4. Data Schema Drift (Type 2) ─────────────────────────────────────────
    if (
        type2_result
        and type2_result.get("has_schema_drift")
        and abs(remaining_gap) > 1.0
    ):
        # Schema drift — attribute a share proportional to unmapped ratio
        unmapped_ratio = type2_result.get("unmapped_ratio", 0.0)
        if unmapped_ratio > 0.01:
            schema_share = _cap(_signed(abs(remaining_gap) * min(unmapped_ratio, 0.20)))
            if abs(schema_share) > 1.0:
                drift = type2_result.get("schema_diff", {})
                missing_cols = drift.get("missing_in_target", []) + drift.get("missing_in_source", [])
                dtype_drift = drift.get("dtype_drift", {})
                detail = (
                    f"Schema version drift detected. "
                    f"Missing columns: {missing_cols[:5]}. "
                    f"Dtype mismatches: {list(dtype_drift.keys())[:5]}. "
                    f"Fuzzy match suggestions available: {len(type2_result.get('fuzzy_matches', []))}."
                )
                components.append({
                    "category": "Schema / Mapping Drift",
                    "label": "Source–Target Schema Version Mismatch",
                    "amount": round(float(schema_share), 2),
                    "pct_of_gap": round(float(schema_share) / total_gap * 100, 1) if total_gap != 0 else 0,
                    "confidence": round(type2_result.get("confidence", 0.5), 3),
                    "detail": detail,
                    "fix_type": "add_mapping",
                    "check_type": "type2_data",
                    "drill_down": type2_result.get("fuzzy_matches", [])[:10],
                })
                remaining_gap -= float(schema_share)

    # ── 5. Manual Adjustment (Type 1 — residual bucket) ───────────────────────
    if abs(remaining_gap) > 1.0:
        adj_amt = remaining_gap
        components.append({
            "category": "Manual Adjustment",
            "label": "Target-Only Manual Adjustments",
            "amount": round(float(adj_amt), 2),
            "pct_of_gap": round(float(adj_amt) / total_gap * 100, 1) if total_gap != 0 else 0,
            "confidence": CONFIDENCE["manual_adjustment"],
            "detail": (
                f"Remaining gap of ${abs(adj_amt):,.0f} likely represents manual top-side "
                "adjustments applied in the client's reporting system that are not in source. "
                "Common examples: management adjustments, reclassifications, prior-period corrections."
            ),
            "fix_type": "add_adjustment",
            "check_type": "type1_code",
            "drill_down": [],
        })
        remaining_gap -= adj_amt

    # ── 6. Hierarchy Mismatch (Type 2 — informational, $0 at group level) ────
    if hierarchy_result.get("status") == "mismatch_found":
        schema_note = hierarchy_result.get("schema_drift_note", "")
        components.append({
            "category": "Hierarchy Mismatch",
            "label": "Entity Rollup Differences (Informational)",
            "amount": 0.0,
            "pct_of_gap": 0.0,
            "confidence": CONFIDENCE["hierarchy"],
            "detail": (
                hierarchy_result.get("message", "") +
                " NOTE: Cancels at Group Total level — fix this to correct regional reporting."
                + schema_note
            ),
            "fix_type": "fix_hierarchy",
            "check_type": "type2_data",
            "drill_down": hierarchy_result.get("findings", [])[:10],
        })

    # ── 7. Anomalous Accounts (IsolationForest — informational) ───────────────
    if anomaly_result and anomaly_result.get("status") == "complete" and anomaly_result.get("anomalous_count", 0) > 0:
        components.append({
            "category": "Anomalous Accounts",
            "label": f"IsolationForest Outliers ({anomaly_result['anomalous_count']} accounts)",
            "amount": 0.0,
            "pct_of_gap": 0.0,
            "confidence": 0.5,
            "detail": (
                f"{anomaly_result['anomalous_count']} of {anomaly_result['total_accounts']} "
                "accounts were flagged as statistical outliers by IsolationForest and excluded "
                "from contribution scoring. Review individually."
            ),
            "fix_type": None,
            "check_type": "type1_code",
            "drill_down": anomaly_result.get("anomalous_accounts", [])[:10],
        })

    # ── 8. Unexplained Residual ───────────────────────────────────────────────
    explained = sum(c["amount"] for c in components if abs(c["amount"]) > 0.01)
    residual = total_gap - explained

    if abs(residual) > 0.01:
        components.append({
            "category": "Unexplained Residual",
            "label": "Requires Manual Review",
            "amount": round(residual, 2),
            "pct_of_gap": round(residual / total_gap * 100, 1) if total_gap != 0 else 0,
            "confidence": 0.0,
            "detail": "Could not be automatically attributed. Escalate to a financial analyst.",
            "fix_type": None,
            "check_type": None,
            "drill_down": [],
        })

    # ── Summary metrics ───────────────────────────────────────────────────────
    non_zero = [c for c in components if abs(c["amount"]) > 0.01]

    explained_pct = round(
        (1 - abs(residual) / abs(total_gap)) * 100, 1
    ) if total_gap != 0 else 100.0

    weighted_conf = (
        sum(c["confidence"] * abs(c["amount"]) for c in non_zero if c["confidence"] > 0)
        / sum(abs(c["amount"]) for c in non_zero if c["confidence"] > 0)
    ) if any(c["confidence"] > 0 and abs(c["amount"]) > 0 for c in non_zero) else 0.0

    auto_fixable_amount = sum(
        abs(c["amount"]) for c in non_zero
        if c.get("fix_type") not in (None, "add_adjustment")
        and c["category"] not in ("Hierarchy Mismatch", "Anomalous Accounts")
    )

    return {
        "total_gap": round(total_gap, 2),
        "components": components,
        "explained_pct": max(0.0, min(100.0, explained_pct)),
        "overall_confidence": round(weighted_conf * 100, 1),
        "auto_fixable_pct": round(auto_fixable_amount / abs(total_gap) * 100, 1) if total_gap != 0 else 0.0,
    }


# ─────────────────────────────────────────────────────────────────────────────
# DVT-style column diff (Layer 5 — per-metric comparison table)
# ─────────────────────────────────────────────────────────────────────────────

def compute_dvt_diff(
    our_metrics: dict,
    target_metrics: dict,
    tolerance_pct: Optional[float] = None,
    schema_hashes: Optional[dict] = None,
) -> list[dict]:
    """
    Layer 5 — Data-Validation-Tool style per-metric comparison.

    Produces one row per metric with:
      metric, source_val, target_val, delta, delta_pct, status, checksum_match,
      schema_match (new — compares source vs target schema hash from profiler).

    status thresholds:
      pass  : |delta_pct| <= tolerance
      warn  : |delta_pct| <= tolerance * 5
      fail  : |delta_pct| >  tolerance * 5

    schema_match uses the schema_hashes dict from build_fingerprint() to surface
    schema version drift (Layer 4 Type 2) directly in the DVT table.
    """
    tol = tolerance_pct if tolerance_pct is not None else TOLERANCE_PCT
    _schema_hashes = schema_hashes or {}
    rows = []

    all_metrics = set(list(our_metrics.keys()) + list(target_metrics.keys()))

    for metric in sorted(all_metrics):
        src_val = our_metrics.get(metric, 0.0)
        tgt_val = target_metrics.get(metric, 0.0)
        delta = src_val - tgt_val
        delta_pct = (
            (delta / tgt_val) if tgt_val != 0
            else (0.0 if delta == 0 else float("inf"))
        )

        abs_pct = abs(delta_pct)
        if abs_pct <= tol:
            status = "pass"
        elif abs_pct <= tol * 5:
            status = "warn"
        else:
            status = "fail"

        # Value-level checksum — True only when values are bit-for-bit identical
        checksum_match = None
        if CHECKSUM_ENABLED:
            src_hash = hashlib.md5(f"{metric}:{src_val:.2f}".encode()).hexdigest()
            tgt_hash = hashlib.md5(f"{metric}:{tgt_val:.2f}".encode()).hexdigest()
            checksum_match = src_hash == tgt_hash

        # Schema hash comparison from Layer 1 profiler (Type 2 schema drift signal)
        schema_match = None
        if _schema_hashes:
            src_schema = _schema_hashes.get("src_hash")
            tgt_schema = _schema_hashes.get("tgt_hash")
            if src_schema and tgt_schema:
                schema_match = src_schema == tgt_schema

        rows.append({
            "metric": metric,
            "source_val": round(src_val, 2),
            "target_val": round(tgt_val, 2),
            "delta": round(delta, 2),
            "delta_pct": round(delta_pct * 100, 2) if delta_pct != float("inf") else None,
            "status": status,
            "checksum_match": checksum_match,
            "schema_match": schema_match,
        })

    return rows


# ─────────────────────────────────────────────────────────────────────────────
# Causal path tracing (ProRCA-style DAG — Layer 4 / Layer 5 boundary)
# ─────────────────────────────────────────────────────────────────────────────

def trace_causal_path(
    delta_series: dict,
    total_gap: float,
    type1_result: Optional[dict] = None,
    type2_result: Optional[dict] = None,
    type3_result: Optional[dict] = None,
) -> dict:
    """
    Build a causal DAG and trace backward to find the dominant root cause node.

    The DAG now reflects the 6-layer pipeline:
      source_data → mapping_join → ic_elimination → orchestration_timing
      → schema_version → manual_adj → result

    New nodes vs old version:
      orchestration_timing — fed by Type 3 result (timestamp/watermark gap)
      schema_version       — fed by Type 2 result (schema hash drift)

    delta_series : {node_name: attributed_amount}  (positive = absolute contribution)
    total_gap    : the raw signed gap (our_value - target_value) for the primary metric.

    Confidence gate (replaces old needs_ai_narrative flag):
      If this function is called with type1/2/3 results, it delegates to
      compute_confidence_gate() from reconciler.py to get the authoritative
      needs_ai_narrative bool.  Otherwise falls back to the CAUSAL_THRESHOLD
      heuristic for backward compatibility.

    Returns:
      root_cause_node    : str
      contribution_pct   : float
      path_traversed     : list of {node, delta, contribution_pct}
      confidence         : float 0–1
      needs_ai_narrative : bool  (True → Layer 5 makes second AI call ~400 tokens)
    """
    try:
        import networkx as nx
    except ImportError:
        return {
            "root_cause_node": "unknown",
            "contribution_pct": 0.0,
            "path_traversed": [],
            "confidence": 0.0,
            "needs_ai_narrative": True,
        }

    if total_gap == 0:
        return {
            "root_cause_node": "none",
            "contribution_pct": 0.0,
            "path_traversed": [],
            "confidence": 1.0,
            "needs_ai_narrative": False,
        }

    # ── Build 6-layer causal DAG ───────────────────────────────────────────────
    ordered_nodes = [
        "source_data",
        "mapping_join",
        "ic_elimination",
        "orchestration_timing",   # Type 3 node (new)
        "schema_version",         # Type 2 node (new)
        "manual_adj",
        "result",
    ]

    G = nx.DiGraph()
    for i in range(len(ordered_nodes) - 1):
        G.add_edge(ordered_nodes[i], ordered_nodes[i + 1])

    # Populate node deltas — merge explicit series with type-result evidence
    node_deltas = dict(delta_series)

    # Inject Type 3 evidence into orchestration_timing node
    if type3_result and type3_result.get("findings"):
        existing = node_deltas.get("orchestration_timing", 0.0)
        if existing == 0.0 and type3_result.get("timestamp_gap_flag") or type3_result.get("watermark_gap_flag"):
            # Estimate: attribute 10% of total_gap as orchestration signal when evidence found
            node_deltas["orchestration_timing"] = abs(total_gap) * 0.10

    # Inject Type 2 evidence into schema_version node
    if type2_result and type2_result.get("has_schema_drift"):
        existing = node_deltas.get("schema_version", 0.0)
        if existing == 0.0:
            node_deltas["schema_version"] = abs(total_gap) * type2_result.get("unmapped_ratio", 0.05)

    for node in ordered_nodes:
        G.nodes[node]["delta"] = node_deltas.get(node, 0.0)

    # ── Traverse backward from result to find dominant root cause ─────────────
    path_traversed = []
    root_cause_node = "source_data"
    contribution_pct = 0.0
    abs_total = abs(total_gap)

    for node in reversed(ordered_nodes[:-1]):  # Exclude sentinel "result"
        delta = node_deltas.get(node, 0.0)
        pct = abs(delta) / abs_total if abs_total > 0 else 0.0
        path_traversed.append({
            "node": node,
            "delta": round(delta, 2),
            "contribution_pct": round(pct, 4),
        })
        if pct >= CAUSAL_THRESHOLD:
            root_cause_node = node
            contribution_pct = pct
            break
    else:
        if path_traversed:
            best = max(path_traversed, key=lambda x: abs(x["contribution_pct"]))
            root_cause_node = best["node"]
            contribution_pct = best["contribution_pct"]

    # ── Confidence gate — delegate to reconciler when type results available ──
    if type1_result is not None and type2_result is not None and type3_result is not None:
        gate = compute_confidence_gate(type1_result, type2_result, type3_result)
        confidence = gate["overall_confidence"]
        needs_ai = gate["needs_ai_narrative"]
    else:
        # Backward-compatible fallback
        confidence = min(contribution_pct, 1.0)
        needs_ai = confidence < CAUSAL_THRESHOLD

    return {
        "root_cause_node": root_cause_node,
        "contribution_pct": round(contribution_pct, 4),
        "path_traversed": path_traversed,
        "confidence": round(float(confidence), 4),
        "needs_ai_narrative": bool(needs_ai),
    }