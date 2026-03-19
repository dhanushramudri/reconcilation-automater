"""
Fix Preview Engine.
Applies suggested fixes to data in-memory and recomputes metrics
to show the "before/after" impact without touching production.
"""
import datetime
import pandas as pd
from typing import Optional
from core.reconciler import compute_our_metrics, compute_gap_summary, parse_target_metrics


# ─────────────────────────────────────────────────────────────────────────────
# Audit trail
# ─────────────────────────────────────────────────────────────────────────────

_audit_trail: list[dict] = []


def record_audit_step(
    step_name: str,
    method_used: str,
    delta_before: float,
    delta_after: float,
    confidence: float,
    fuzzy_suggestions_applied: Optional[list] = None,
) -> None:
    """Append a timestamped entry to the in-memory audit trail."""
    _audit_trail.append({
        "timestamp": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "step_name": step_name,
        "method_used": method_used,
        "delta_before": round(delta_before, 2),
        "delta_after": round(delta_after, 2),
        "gap_closed": round(delta_before - delta_after, 2),
        "confidence": round(confidence, 4),
        "fuzzy_suggestions_applied": fuzzy_suggestions_applied or [],
    })


def get_audit_trail() -> list[dict]:
    """Return a copy of the current audit trail."""
    return list(_audit_trail)


def clear_audit_trail() -> None:
    """Reset the audit trail (call between sessions)."""
    _audit_trail.clear()


FIX_TYPES = {
    "add_mapping": "Add missing account codes to mapping table",
    "fix_hierarchy": "Correct entity hierarchy rollup",
    "add_elimination": "Add intercompany elimination entry",
    "add_adjustment": "Add manual adjustment",
}


def apply_mapping_fix(
    mapping: pd.DataFrame,
    new_entries: list[dict],
) -> pd.DataFrame:
    """
    Add missing account code mappings.
    new_entries: list of {source_account_code, target_account_code, metric_category}
    """
    new_df = pd.DataFrame(new_entries)
    # Don't duplicate existing entries
    existing_codes = set(mapping["source_account_code"].astype(str))
    new_df = new_df[~new_df["source_account_code"].astype(str).isin(existing_codes)]
    return pd.concat([mapping, new_df], ignore_index=True)


def apply_hierarchy_fix(
    hierarchy: Optional[pd.DataFrame],
    entity_updates: list[dict],
) -> pd.DataFrame:
    """
    Fix entity hierarchy entries.
    entity_updates: list of {entity_code, field, old_value, new_value}
    """
    if hierarchy is None:
        hierarchy = pd.DataFrame()

    hier = hierarchy.copy()
    for update in entity_updates:
        entity_code = update["entity_code"]
        field = update["field"]
        new_value = update["new_value"]
        mask = hier["entity_code"] == entity_code
        if mask.any():
            hier.loc[mask, field] = new_value

    return hier


def apply_elimination_fix(
    source: pd.DataFrame,
    elimination_entries: list[dict],
) -> pd.DataFrame:
    """
    Add intercompany elimination journal entries.
    elimination_entries: list of {entity_code, account_code, amount, counterparty_entity}
    """
    elim_df = pd.DataFrame(elimination_entries)
    elim_df["is_intercompany"] = True
    elim_df["adjustment_flag"] = True
    elim_df["adjustment_type"] = "IC_Elimination"
    elim_df["is_mapped"] = True

    return pd.concat([source, elim_df], ignore_index=True)


def preview_fix(
    fix_type: str,
    fix_data: dict,
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: pd.DataFrame,
    hierarchy: Optional[pd.DataFrame] = None,
) -> dict:
    """
    Apply a fix and compute new metrics. Returns before/after comparison.
    """
    # Compute baseline (before fix)
    baseline = compute_our_metrics(source, mapping, hierarchy)
    target_metrics = parse_target_metrics(target)
    gaps_before = compute_gap_summary(baseline["by_metric"], target_metrics)

    # Apply the fix
    new_source = source.copy()
    new_mapping = mapping.copy()
    new_hierarchy = hierarchy.copy() if hierarchy is not None else None

    if fix_type == "add_mapping":
        new_mapping = apply_mapping_fix(new_mapping, fix_data.get("new_entries", []))
    elif fix_type == "fix_hierarchy":
        new_hierarchy = apply_hierarchy_fix(new_hierarchy, fix_data.get("entity_updates", []))
    elif fix_type == "add_elimination":
        new_source = apply_elimination_fix(new_source, fix_data.get("elimination_entries", []))

    # Compute after fix
    after = compute_our_metrics(new_source, new_mapping, new_hierarchy)
    gaps_after = compute_gap_summary(after["by_metric"], target_metrics)

    # Build comparison
    comparison = {}
    for metric in set(list(gaps_before.keys()) + list(gaps_after.keys())):
        before_gap = gaps_before.get(metric, {}).get("gap", 0)
        after_gap = gaps_after.get(metric, {}).get("gap", 0)
        improvement = before_gap - after_gap

        comparison[metric] = {
            "before_our_value": gaps_before.get(metric, {}).get("our_value", 0),
            "after_our_value": gaps_after.get(metric, {}).get("our_value", 0),
            "target_value": gaps_before.get(metric, {}).get("target_value", 0),
            "gap_before": round(before_gap, 2),
            "gap_after": round(after_gap, 2),
            "gap_closed": round(improvement, 2),
            "gap_closed_pct": round(
                abs(improvement / before_gap * 100) if before_gap != 0 else 0, 1
            ),
        }

    # Record primary metric delta to audit trail
    primary_metric = next(
        (m for m in ["Revenue", "Operating Profit", "Gross Margin"] if m in comparison),
        next(iter(comparison), None),
    )
    if primary_metric:
        info = comparison[primary_metric]
        record_audit_step(
            step_name=f"Fix: {fix_type}",
            method_used=FIX_TYPES.get(fix_type, fix_type),
            delta_before=abs(info["gap_before"]),
            delta_after=abs(info["gap_after"]),
            confidence=0.95 if fix_type == "add_mapping" else 0.75,
            fuzzy_suggestions_applied=fix_data.get("fuzzy_suggestions_applied", []),
        )

    return {
        "fix_type": fix_type,
        "fix_description": FIX_TYPES.get(fix_type, fix_type),
        "comparison": comparison,
        "new_mapping": new_mapping,
        "new_hierarchy": new_hierarchy,
        "new_source": new_source,
    }
