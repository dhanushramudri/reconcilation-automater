"""
ReconAgent Orchestrator — 6-Layer Agentic Pipeline.

Layer execution order:
  Layer 1 — Schema profiler (already done by build_fingerprint, passed in)
  Layer 2 — AI strategy planner: ONE Gemini call (~600 tokens) that reads the
             pipeline graph + schema summary + gap metrics and returns a RANKED
             checklist of what to investigate first.  No raw data sent.
  Layer 3 — Sandbox executor: cell-by-cell replay (stubbed here — full
             implementation lives in sandbox_executor.py).
  Layer 4 — Hypothesis investigator: Type 1 (code), Type 2 (data),
             Type 3 (orchestration) checks run in AI-ranked order.
             IsolationForest pre-filters noise BEFORE any checks run.
             Stops Type 1 attribution at 95% explained.
  Layer 5 — Output: variance waterfall + causal DAG + narrative.
             Second AI call (~400 tokens) ONLY if confidence gate < 80%.

Raw data never leaves Python.  The AI only ever sees:
  - Layer 2: pipeline graph (step names + transform types) + column names
             + gap numbers.  No rows, no amounts per row.
  - Layer 5: aggregated findings only (~1 KB).  No rows, no raw data.
"""

from __future__ import annotations

import json
import os
from typing import Callable, Optional

import pandas as pd

from config import AI, AI_CONFIDENCE_THRESHOLD, REVENUE_CATEGORIES, DERIVED_METRICS, DATA

from agents.investigators import (
    check_hierarchy_mismatches,
    check_intercompany_eliminations,
    check_manual_adjustments,
    check_mapping_gaps,
    compute_variance_attribution,
    trace_causal_path,
)
from core.reconciler import (
    CONFIDENCE_AI_TRIGGER,
    TYPE1_STOP_THRESHOLD,
    compute_confidence_gate,
    compute_our_metrics,
    filter_anomalous_deltas,
    run_type1_code_checks,
    run_type2_data_checks,
    run_type3_orchestration_checks,
)


# ─────────────────────────────────────────────────────────────────────────────
# Cross-metric consistency checker (feeds Layer 2 and Layer 5)
# ─────────────────────────────────────────────────────────────────────────────

def _detect_cross_metric_inconsistencies(gap_summary: dict) -> list[dict]:
    """
    For each derived metric in DERIVED_METRICS (from config), compute the
    expected gap from its components and compare to the actual gap.

    Example: Operating Profit = Gross Margin + OpEx (OpEx stored as negative).
    If Gross Margin gap = -2.1M and OpEx gap = 0, expected OP gap = -2.1M.
    If actual OP gap = -2.6M, the discrepancy of -0.5M is unexplained and must
    be surfaced to the AI and narrative layers.

    No hardcoding — uses DERIVED_METRICS and DATA from config.py.
    """
    inconsistencies: list[dict] = []

    for derived_name, (comp_a, comp_b) in DERIVED_METRICS.items():
        derived_info = gap_summary.get(derived_name)
        if derived_info is None:
            continue

        comp_a_info = gap_summary.get(comp_a, {})
        comp_b_info = gap_summary.get(comp_b, {})

        comp_a_gap = comp_a_info.get("gap", 0.0) if comp_a_info else 0.0
        comp_b_gap = comp_b_info.get("gap", 0.0) if comp_b_info else 0.0

        expected_gap = comp_a_gap + comp_b_gap
        actual_gap = derived_info["gap"]
        discrepancy = actual_gap - expected_gap

        if abs(discrepancy) > DATA["gap_threshold"]:
            direction = "larger" if discrepancy < 0 else "smaller"
            inconsistencies.append({
                "derived_metric": derived_name,
                "actual_gap": round(actual_gap, 2),
                "expected_gap": round(expected_gap, 2),
                "discrepancy": round(discrepancy, 2),
                "components": {
                    comp_a: round(comp_a_gap, 2),
                    comp_b: round(comp_b_gap, 2),
                },
                "note": (
                    f"{derived_name} gap (${actual_gap:+,.0f}) is ${abs(discrepancy):,.0f} "
                    f"{direction} than expected from {comp_a} (${comp_a_gap:+,.0f}) "
                    f"+ {comp_b} (${comp_b_gap:+,.0f}). "
                    f"This unexplained delta suggests a hidden adjustment, reclassification, "
                    f"or timing difference that is not captured in the component metrics."
                ),
            })

    return inconsistencies


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2 — AI strategy planner prompt builders
# ─────────────────────────────────────────────────────────────────────────────

def _build_strategy_prompt(
    fingerprint: dict,
    business_context: str,
    gap_summary: dict,
    primary_metric: str,
    primary_gap: float,
    cross_metric_inconsistencies: Optional[list] = None,
) -> str:
    """
    Build the Layer 2 strategy planner prompt (~600 tokens max).

    Sends to AI:
      - Pipeline graph: step names + transform types (from Layer 1 profiler)
      - Schema summary: column names + types (no values)
      - Business context (user-typed)
      - Observed gap metrics: numbers only, no rows

    AI returns a ranked investigation plan — which checks to run first,
    which mapping tables to scrutinise, which orchestration timestamps to compare.
    This is the "human developer thinking" step.
    """
    # Pipeline graph summary (structure only)
    pg = fingerprint.get("pipeline_graph", {})
    steps_summary = [
        {
            "step": s.get("step_name", ""),
            "type": s.get("transform_type", ""),
            "join_keys": s.get("join_keys", [])[:3],
            "filters": s.get("filter_conditions", [])[:2],
            "agg_cols": s.get("aggregation_columns", [])[:3],
        }
        for s in pg.get("steps", [])[:8]  # cap at 8 steps
    ]

    # Schema summary (column names + types, no values)
    src_schema = {
        col: dtype
        for col, dtype in fingerprint.get("source", {}).get("column_dtypes", {}).items()
    }
    tgt_schema = {
        col: dtype
        for col, dtype in fingerprint.get("target", {}).get("column_dtypes", {}).items()
    }

    # Gap metrics (numbers only)
    gap_numbers = {
        metric: {
            "our_value": info.get("our_value"),
            "target_value": info.get("target_value"),
            "gap": info.get("gap"),
            "gap_pct": info.get("gap_pct"),
        }
        for metric, info in gap_summary.items()
        if info.get("has_gap")
    }

    # Coverage alert (aggregate stats only)
    coverage = fingerprint.get("mapping_coverage", {})

    # Cross-metric inconsistency section — derived metric gaps that don't match components
    cross_metric_section = ""
    if cross_metric_inconsistencies:
        cross_metric_section = (
            f"\nCROSS-METRIC INCONSISTENCIES (derived metric gaps that don't match components):\n"
            f"{json.dumps(cross_metric_inconsistencies, indent=2)}\n"
            f"IMPORTANT: These discrepancies mean the primary metric investigation alone will NOT "
            f"explain all gaps. The ranked_checks must account for the unexplained delta in derived metrics.\n"
        )

    prompt = (
        f"You are a senior financial reconciliation analyst.\n\n"
        f"Business context: {business_context or 'Financial close reconciliation.'}\n\n"
        f"PRIMARY GAP: {primary_metric} = ${abs(primary_gap):,.0f} "
        f"({'below' if primary_gap < 0 else 'above'} target)\n\n"
        f"ALL GAPS (numbers only, no raw data):\n{json.dumps(gap_numbers, indent=2)}\n"
        f"{cross_metric_section}\n"
        f"PIPELINE GRAPH (transformation steps from Layer 1 schema profiler):\n"
        f"{json.dumps(steps_summary, indent=2)}\n\n"
        f"SOURCE SCHEMA (column names + types):\n{json.dumps(src_schema)}\n\n"
        f"TARGET SCHEMA (column names + types):\n{json.dumps(tgt_schema)}\n\n"
        f"MAPPING COVERAGE: {coverage.get('coverage_pct', 100):.1f}% "
        f"({coverage.get('unmapped_count', 0)} unmapped transactions, "
        f"${abs(coverage.get('unmapped_amount', 0)):,.0f})\n\n"
        f"Based on the pipeline graph and schema above, return a JSON object with:\n"
        f"  ranked_checks: list of check names in priority order, chosen from:\n"
        f"    ['type1_mapping_gaps', 'type1_ic_eliminations', 'type1_manual_adjustments',\n"
        f"     'type2_fuzzy_coverage', 'type2_schema_drift', 'type2_hierarchy',\n"
        f"     'type3_timestamp_diff', 'type3_parameter_diff', 'type3_watermark_gap']\n"
        f"  focus_accounts: list of account codes to prioritise (from schema, not data)\n"
        f"  hypothesis: one sentence explaining the most likely root cause\n"
        f"  reasoning: one sentence explaining why\n"
        f"  secondary_hypothesis: one sentence about what might explain the cross-metric discrepancy (if any)\n\n"
        f"Return ONLY valid JSON. No markdown, no explanation outside the JSON."
    )
    return prompt


def _build_narrative_prompt(
    business_context: str,
    causal_result: dict,
    anomaly_result: dict,
    attribution_result: dict,
    type1_result: dict,
    type2_result: dict,
    type3_result: dict,
    primary_metric: str,
    primary_gap: float,
    gap_summary: Optional[dict] = None,
    cross_metric_inconsistencies: Optional[list] = None,
) -> str:
    """
    Build the Layer 5 conditional narrative prompt (~400 tokens).

    Called ONLY when confidence gate < 80%.
    Sends aggregated findings only — no raw data rows.
    """
    top_components = [
        {
            "category": c["category"],
            "amount": c["amount"],
            "pct_of_gap": c["pct_of_gap"],
            "check_type": c.get("check_type", ""),
        }
        for c in attribution_result.get("components", [])[:4]
    ]
    path = " → ".join(p["node"] for p in causal_result.get("path_traversed", []))

    # Type 2 schema drift summary (aggregate, no values)
    schema_note = ""
    if type2_result.get("has_schema_drift"):
        drift = type2_result.get("schema_diff", {})
        schema_note = (
            f"Schema drift detected: missing cols {drift.get('missing_in_target', [])[:3]}, "
            f"dtype changes {list(drift.get('dtype_drift', {}).keys())[:3]}. "
        )

    # Type 3 orchestration summary
    orch_note = ""
    if type3_result.get("findings"):
        orch_note = " | ".join(
            f["detail"][:80] for f in type3_result["findings"][:2]
        )

    # All gapped metrics from reconciliation (not just primary)
    all_gaps_section = ""
    if gap_summary:
        gapped = {
            m: {"gap": info["gap"], "our_value": info["our_value"], "target_value": info["target_value"]}
            for m, info in gap_summary.items()
            if info.get("has_gap")
        }
        if gapped:
            all_gaps_section = f"ALL GAPPED METRICS:\n{json.dumps(gapped, indent=2)}\n\n"

    # Cross-metric inconsistency section
    cross_metric_section = ""
    if cross_metric_inconsistencies:
        cross_metric_section = (
            f"CROSS-METRIC INCONSISTENCIES (unexplained deltas in derived metrics):\n"
            f"{json.dumps(cross_metric_inconsistencies, indent=2)}\n\n"
        )

    return (
        f"Business context: {business_context or 'Financial reconciliation.'}\n\n"
        f"Primary metric: {primary_metric} | Gap: ${abs(primary_gap):,.0f} "
        f"({'below' if primary_gap < 0 else 'above'} target)\n\n"
        f"{all_gaps_section}"
        f"{cross_metric_section}"
        f"Root cause (causal DAG): "
        f"{causal_result.get('root_cause_node', 'unknown').replace('_', ' ').title()} "
        f"— {causal_result.get('contribution_pct', 0):.0%} contribution\n"
        f"Causal path: {path}\n\n"
        f"Top attribution (Type 1/2/3): {json.dumps(top_components)}\n"
        f"Unmapped ratio (Type 2): {type2_result.get('unmapped_ratio', 0):.1%}\n"
        f"Fuzzy matches found: {len(type2_result.get('fuzzy_matches', []))}\n"
        f"{schema_note}"
        f"Orchestration: {orch_note or 'No timing gaps detected.'}\n"
        f"Anomalous accounts: {anomaly_result.get('anomalous_count', 0)} flagged\n\n"
        f"Write 3-5 sentences for a finance director. "
        f"Explain the root cause for EACH gapped metric (not just the primary metric). "
        f"If cross-metric inconsistencies exist, explain what additional factor beyond the "
        f"primary root cause is causing the derived metric gap to differ from expectations. "
        f"State which layer it belongs to (code/data/orchestration) and the recommended fix. "
        f"Be specific. No jargon."
    )


# ─────────────────────────────────────────────────────────────────────────────
# Gemini API calls
# ─────────────────────────────────────────────────────────────────────────────

def _call_gemini_strategy(api_key: str, prompt: str) -> tuple[dict, int]:
    """
    Layer 2 — AI strategy planner call.
    Returns (strategy_dict, tokens_used).
    strategy_dict keys: ranked_checks, focus_accounts, hypothesis, reasoning.
    Falls back to a default strategy on any error.
    """
    default_strategy = {
        "ranked_checks": [
            "type1_mapping_gaps",
            "type1_ic_eliminations",
            "type2_fuzzy_coverage",
            "type2_schema_drift",
            "type3_timestamp_diff",
            "type1_manual_adjustments",
            "type2_hierarchy",
            "type3_parameter_diff",
            "type3_watermark_gap",
        ],
        "focus_accounts": [],
        "hypothesis": "Mapping gaps are the most likely root cause given low coverage.",
        "reasoning": "Default strategy — AI strategy planner not available.",
    }

    if not api_key:
        return default_strategy, 0

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        config = types.GenerateContentConfig(
            system_instruction=(
                "You are a senior financial reconciliation analyst. "
                "Respond ONLY with valid JSON. No markdown, no explanation."
            ),
            temperature=0.1,   # Low temperature — deterministic planning
            max_output_tokens=512,
        )
        response = client.models.generate_content(
            model=AI["model"],
            contents=[types.Content(role="user", parts=[types.Part(text=prompt)])],
            config=config,
        )

        raw = ""
        for part in response.candidates[0].content.parts:
            if hasattr(part, "text") and part.text:
                raw += part.text

        tokens_used = 0
        usage = getattr(response, "usage_metadata", None)
        if usage:
            tokens_used = getattr(usage, "total_token_count", 0) or 0

        # Parse JSON — strip any accidental markdown fences
        clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        strategy = json.loads(clean)

        # Validate expected keys — fall back to defaults for any missing key
        for key, default_val in default_strategy.items():
            if key not in strategy:
                strategy[key] = default_val

        return strategy, int(tokens_used)

    except Exception as exc:
        default_strategy["reasoning"] = f"AI strategy planner error: {exc}"
        return default_strategy, 0


def _call_gemini_narrative(api_key: str, prompt: str) -> tuple[str, int]:
    """
    Layer 5 — conditional narrative call (~400 tokens).
    Called ONLY when confidence gate < 80%.
    Returns (narrative_text, tokens_used).
    """
    if not api_key:
        return "[AI narrative unavailable — no API key configured]", 0

    try:
        from google import genai
        from google.genai import types

        client = genai.Client(api_key=api_key)
        config = types.GenerateContentConfig(
            system_instruction=(
                "You are a senior financial reconciliation analyst. "
                "Write a concise, professional narrative for a finance director."
            ),
            temperature=AI["temperature"],
            max_output_tokens=512,
        )
        response = client.models.generate_content(
            model=AI["model"],
            contents=[types.Content(role="user", parts=[types.Part(text=prompt)])],
            config=config,
        )

        narrative = ""
        for part in response.candidates[0].content.parts:
            if hasattr(part, "text") and part.text:
                narrative += part.text

        tokens_used = 0
        usage = getattr(response, "usage_metadata", None)
        if usage:
            tokens_used = getattr(usage, "total_token_count", 0) or 0

        return narrative.strip(), int(tokens_used)

    except Exception as exc:
        return f"[AI narrative unavailable: {exc}]", 0


# ─────────────────────────────────────────────────────────────────────────────
# Local rule-based narrative (0 tokens — used when confidence >= 80%)
# ─────────────────────────────────────────────────────────────────────────────

def _local_narrative(
    causal_result: dict,
    attribution_result: dict,
    type2_result: dict,
    type3_result: dict,
    primary_metric: str,
    primary_gap: float,
    gap_summary: Optional[dict] = None,
    cross_metric_inconsistencies: Optional[list] = None,
) -> str:
    """
    Generate a rule-based narrative without any AI call.
    Incorporates all gapped metrics, cross-metric inconsistencies, Type 2 and
    Type 3 findings in addition to the causal path and attribution components.
    Used when confidence gate >= 80% — 0 API tokens consumed.
    """
    root = causal_result.get("root_cause_node", "unknown").replace("_", " ").title()
    pct = causal_result.get("contribution_pct", 0)
    components = attribution_result.get("components", [])
    top = components[0] if components else {}
    explained = attribution_result.get("explained_pct", 0)
    confidence = attribution_result.get("overall_confidence", 0)

    lines: list[str] = [
        f"Root cause identified: **{root}** contributes {pct:.0%} of the "
        f"{primary_metric} gap of ${abs(primary_gap):,.0f}.",
    ]

    if top:
        lines.append(
            f"Primary attribution: **{top.get('category', 'Unknown')}** "
            f"({abs(top.get('pct_of_gap', 0)):.1f}% of gap, "
            f"${abs(top.get('amount', 0)):,.0f}) — {top.get('detail', '')[:120]}."
        )

    # Type 2 — data layer finding
    if type2_result.get("unmapped_ratio", 0) > 0.02:
        lines.append(
            f"**Type 2 (Data):** {type2_result['unmapped_ratio']:.1%} of source transactions "
            f"are unmapped. "
            + (
                f"{len(type2_result.get('fuzzy_matches', []))} fuzzy match suggestions available."
                if type2_result.get("fuzzy_matches") else ""
            )
        )

    # Type 3 — orchestration layer finding
    if type3_result.get("findings"):
        first_orch = type3_result["findings"][0]
        lines.append(
            f"**Type 3 (Orchestration):** {first_orch.get('detail', '')[:150]}."
        )

    lines.append(
        f"Overall **{explained:.1f}%** of the gap is automatically attributed "
        f"with **{confidence:.1f}%** confidence. "
        "Review the Fix & Validate tab to preview corrections before applying them."
    )

    if len(components) > 1:
        secondary = components[1]
        lines.append(
            f"Secondary factor: **{secondary.get('category', '')}** "
            f"({abs(secondary.get('pct_of_gap', 0)):.1f}% of gap)."
        )

    # Additional gapped metrics beyond the primary metric
    if gap_summary:
        secondary_gaps = [
            (m, info) for m, info in gap_summary.items()
            if info.get("has_gap") and m != primary_metric
        ]
        if secondary_gaps:
            metric_summaries = ", ".join(
                f"**{m}** (${info['gap']:+,.0f})"
                for m, info in secondary_gaps
            )
            lines.append(
                f"Additional gapped metrics: {metric_summaries}. "
                f"These are derived from the same source data — resolving the primary root cause "
                f"should reduce these gaps proportionally."
            )

    # Cross-metric inconsistencies — derived metric gaps that exceed component predictions
    if cross_metric_inconsistencies:
        for inc in cross_metric_inconsistencies:
            lines.append(
                f"⚠️ **Cross-metric inconsistency detected:** "
                f"{inc['derived_metric']} gap (${inc['actual_gap']:+,.0f}) differs from "
                f"expected (${inc['expected_gap']:+,.0f}) by ${inc['discrepancy']:+,.0f}. "
                f"{inc['note']}"
            )

    return " ".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Check dispatcher — runs checks in AI-ranked order
# ─────────────────────────────────────────────────────────────────────────────

def _run_checks_in_order(
    ranked_checks: list[str],
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: pd.DataFrame,
    hierarchy: Optional[pd.DataFrame],
    enriched: pd.DataFrame,
    anomalous_codes: set,
    pipeline_graph: dict,
    type1_result: dict,
    total_gap: float,
    on_step: Optional[Callable],
) -> tuple[dict, dict, dict, dict, dict, dict, list[dict]]:
    """
    Run Type 1 / 2 / 3 checks in the AI-ranked order from the strategy planner.

    Returns:
        mapping_result, ic_result, adj_result,
        hierarchy_result, type2_result, type3_result,
        agent_steps
    """
    # Initialise all results to safe empty dicts
    mapping_result:   dict = {"status": "not_run", "findings": [], "unmapped_amount": 0.0}
    ic_result:        dict = {"status": "not_run", "findings": [], "uneliminated_amount": 0.0}
    adj_result:       dict = {"status": "not_run", "findings": []}
    hierarchy_result: dict = {"status": "not_run", "findings": []}
    type2_result:     dict = {"check_type": "type2_data", "fuzzy_matches": [],
                              "unmapped_ratio": 0.0, "unmapped_amount": 0.0,
                              "schema_diff": {}, "has_schema_drift": False, "confidence": 0.5}
    type3_result:     dict = {"check_type": "type3_orchestration", "findings": [],
                              "timestamp_gap_flag": False, "parameter_diffs": [],
                              "watermark_gap_flag": False, "confidence": 0.5}
    agent_steps:      list[dict] = []

    # Run metadata for Type 3 (from pipeline graph)
    run_meta = pipeline_graph.get("run_metadata", {})
    source_run = run_meta if run_meta else None
    # For demo mode there is no separate target run — use None
    target_run = None

    for check_name in ranked_checks:
        # ── Type 1 — Code checks ──────────────────────────────────────────────
        if check_name == "type1_mapping_gaps" and mapping_result["status"] == "not_run":
            if on_step:
                on_step(
                    "Layer 4 — Type 1: Mapping Gaps",
                    "Scanning for unmapped account codes (anomalous codes excluded)…",
                )
            mapping_result = check_mapping_gaps(
                source, mapping, anomalous_codes=anomalous_codes
            )
            agent_steps.append({
                "step": "Type 1 — Mapping Gaps",
                "detail": mapping_result.get("message", mapping_result.get("status", "done")),
                "tool": "check_mapping_gaps",
                "check_type": "type1_code",
            })

            # Early-stop check: if Type 1 already explains >= 95%, skip remaining Type 1
            if type1_result.get("explained_pct", 0) >= TYPE1_STOP_THRESHOLD:
                if on_step:
                    on_step(
                        "Layer 4 — Type 1: 95% stop reached",
                        f"{type1_result['explained_pct']:.0%} of gap explained — "
                        "skipping remaining Type 1 checks.",
                    )
                continue

        elif check_name == "type1_ic_eliminations" and ic_result["status"] == "not_run":
            if type1_result.get("explained_pct", 0) >= TYPE1_STOP_THRESHOLD:
                continue
            if on_step:
                on_step(
                    "Layer 4 — Type 1: IC Eliminations",
                    "Checking intercompany transaction pairs…",
                )
            ic_result = check_intercompany_eliminations(
                source, mapping, anomalous_codes=anomalous_codes
            )
            agent_steps.append({
                "step": "Type 1 — IC Eliminations",
                "detail": ic_result.get("message", ic_result.get("status", "done")),
                "tool": "check_intercompany_eliminations",
                "check_type": "type1_code",
            })

        elif check_name == "type1_manual_adjustments" and adj_result["status"] == "not_run":
            if type1_result.get("explained_pct", 0) >= TYPE1_STOP_THRESHOLD:
                continue
            if on_step:
                on_step(
                    "Layer 4 — Type 1: Manual Adjustments",
                    "Identifying manual adjustment entries…",
                )
            adj_result = check_manual_adjustments(
                source, target, anomalous_codes=anomalous_codes
            )
            agent_steps.append({
                "step": "Type 1 — Manual Adjustments",
                "detail": adj_result.get("message", adj_result.get("status", "done")),
                "tool": "check_manual_adjustments",
                "check_type": "type1_code",
            })

        # ── Type 2 — Data checks ──────────────────────────────────────────────
        elif check_name in ("type2_fuzzy_coverage", "type2_schema_drift") and type2_result["unmapped_ratio"] == 0.0:
            if on_step:
                on_step(
                    "Layer 4 — Type 2: Data Checks",
                    "Fuzzy join coverage + schema version diff (rapidfuzz)…",
                )
            # Run the full Type 2 block once (covers both fuzzy and schema)
            type2_result = run_type2_data_checks(source, mapping, target)
            agent_steps.append({
                "step": "Type 2 — Data (Fuzzy + Schema)",
                "detail": (
                    f"Unmapped ratio: {type2_result.get('unmapped_ratio', 0):.1%} | "
                    f"Fuzzy matches: {len(type2_result.get('fuzzy_matches', []))} | "
                    f"Schema drift: {'yes' if type2_result.get('has_schema_drift') else 'no'}"
                ),
                "tool": "run_type2_data_checks",
                "check_type": "type2_data",
            })

        elif check_name == "type2_hierarchy" and hierarchy_result["status"] == "not_run":
            if on_step:
                on_step(
                    "Layer 4 — Type 2: Hierarchy Mismatches",
                    "Comparing entity rollup structures…",
                )
            hierarchy_result = check_hierarchy_mismatches(
                source, mapping, target, hierarchy,
                type2_result=type2_result,
            )
            agent_steps.append({
                "step": "Type 2 — Hierarchy Mismatches",
                "detail": hierarchy_result.get("message", hierarchy_result.get("status", "done")),
                "tool": "check_hierarchy_mismatches",
                "check_type": "type2_data",
            })

        # ── Type 3 — Orchestration checks ─────────────────────────────────────
        elif check_name in ("type3_timestamp_diff", "type3_parameter_diff", "type3_watermark_gap") and not type3_result.get("findings") and type3_result["confidence"] == 0.5:
            if on_step:
                on_step(
                    "Layer 4 — Type 3: Orchestration Checks",
                    "Pipeline timestamp diff + parameter diff + watermark gap…",
                )
            # Run the full Type 3 block once (covers timestamp, parameter, watermark)
            type3_result = run_type3_orchestration_checks(
                pipeline_graph=pipeline_graph,
                source_run_metadata=source_run,
                target_run_metadata=target_run,
            )
            agent_steps.append({
                "step": "Type 3 — Orchestration",
                "detail": (
                    f"Timestamp gap: {'yes' if type3_result.get('timestamp_gap_flag') else 'no'} | "
                    f"Param diffs: {len(type3_result.get('parameter_diffs', []))} | "
                    f"Watermark gap: {'yes' if type3_result.get('watermark_gap_flag') else 'no'}"
                ),
                "tool": "run_type3_orchestration_checks",
                "check_type": "type3_orchestration",
            })

    return (
        mapping_result,
        ic_result,
        adj_result,
        hierarchy_result,
        type2_result,
        type3_result,
        agent_steps,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Main orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_recon_agent(
    fingerprint: dict,
    business_context: str,
    gap_summary: dict,
    source: pd.DataFrame,
    mapping: pd.DataFrame,
    target: pd.DataFrame,
    hierarchy: Optional[pd.DataFrame] = None,
    on_step: Optional[Callable[[str, str], None]] = None,
) -> dict:
    """
    Run the SmartRecon 6-layer investigation pipeline.

    Phase 1 — Layer 2 AI strategy planner:
        ONE Gemini call with pipeline graph + schema + gap numbers.
        Returns a ranked checklist. Falls back to default order if unavailable.

    Phase 2 — Layer 4 IsolationForest pre-filter:
        Flag anomalous accounts BEFORE any checks run so noise doesn't
        inflate contribution scores.

    Phase 3 — Layer 4 Type 1/2/3 checks (AI-ranked order):
        Runs checks in the order returned by the strategy planner.
        Type 1 stops at 95% explained.

    Phase 4 — Layer 4 Causal path tracing:
        NetworkX DAG traversal with the 7-node graph including
        orchestration_timing and schema_version nodes.

    Phase 5 — Layer 5 Confidence gate + output:
        compute_confidence_gate() aggregates Type 1/2/3 confidence.
        If < 80%: second Gemini call (~400 tokens) for narrative.
        If >= 80%: local rule-based narrative — 0 tokens.

    Returns full result dict consumed by streamlit_app.py Tab 5.
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    primary_metric, primary_gap = _pick_primary_metric(gap_summary)
    pipeline_graph = fingerprint.get("pipeline_graph", {})
    total_tokens_used = 0

    # ── Compute cross-metric inconsistencies from real gap data ───────────────
    # Must happen before Phase 1 so the AI strategy prompt receives them.
    cross_metric_inconsistencies = _detect_cross_metric_inconsistencies(gap_summary)

    _step(on_step, "ReconAgent initialised",
          f"Primary gap: {primary_metric} = ${abs(primary_gap):,.0f}"
          + (f" | {len(cross_metric_inconsistencies)} cross-metric inconsistenc{'y' if len(cross_metric_inconsistencies) == 1 else 'ies'} detected"
             if cross_metric_inconsistencies else ""))

    # ── Phase 1: Layer 2 — AI Strategy Planner ────────────────────────────────

    _step(on_step, "Layer 2 — AI Strategy Planner",
          "Sending pipeline graph + schema + gap metrics to Gemini (~600 tokens)…")

    strategy_prompt = _build_strategy_prompt(
        fingerprint=fingerprint,
        business_context=business_context,
        gap_summary=gap_summary,
        primary_metric=primary_metric,
        primary_gap=primary_gap,
        cross_metric_inconsistencies=cross_metric_inconsistencies,
    )
    strategy, strategy_tokens = _call_gemini_strategy(api_key, strategy_prompt)
    total_tokens_used += strategy_tokens

    ranked_checks = strategy.get("ranked_checks", [])
    ai_hypothesis = strategy.get("hypothesis", "")
    ai_checklist_accounts = strategy.get("focus_accounts", [])

    _step(on_step, "Layer 2 — Strategy plan received",
          f"Ranked checks: {', '.join(ranked_checks[:4])}… | "
          f"Hypothesis: {ai_hypothesis[:80]}")

    # ── Phase 2: Layer 4 — IsolationForest pre-filter ─────────────────────────

    _step(on_step, "Layer 4 — IsolationForest Pre-filter",
          "Flagging anomalous per-account deltas before attribution…")

    try:
        enriched_result = compute_our_metrics(source, mapping, hierarchy)
        enriched = enriched_result["enriched"]
        anomaly_result = filter_anomalous_deltas(enriched)
    except Exception as exc:
        enriched = pd.DataFrame()
        anomaly_result = {
            "status": "error",
            "reason": str(exc),
            "anomalous_accounts": [],
            "anomalous_account_codes": set(),
            "total_accounts": 0,
            "anomalous_count": 0,
        }

    anomalous_codes: set = anomaly_result.get("anomalous_account_codes", set())

    _step(on_step, "Layer 4 — IsolationForest complete",
          f"{anomaly_result.get('anomalous_count', 0)} anomalous accounts excluded from scoring")

    # ── Type 1 contribution scores (cell delta / total gap) ───────────────────

    type1_result = run_type1_code_checks(
        enriched=enriched,
        total_gap=primary_gap,
        anomalous_codes=anomalous_codes,
        ai_checklist=ai_checklist_accounts,
    )

    # ── Phase 3: Layer 4 — Type 1 / 2 / 3 checks in AI-ranked order ──────────

    _step(on_step, "Layer 4 — Running checks (AI-ranked order)",
          f"Starting with: {ranked_checks[0] if ranked_checks else 'type1_mapping_gaps'}")

    (
        mapping_result,
        ic_result,
        adj_result,
        hierarchy_result,
        type2_result,
        type3_result,
        check_steps,
    ) = _run_checks_in_order(
        ranked_checks=ranked_checks,
        source=source,
        mapping=mapping,
        target=target,
        hierarchy=hierarchy,
        enriched=enriched,
        anomalous_codes=anomalous_codes,
        pipeline_graph=pipeline_graph,
        type1_result=type1_result,
        total_gap=primary_gap,
        on_step=on_step,
    )

    # ── Build variance waterfall ──────────────────────────────────────────────

    _step(on_step, "Layer 5 — Variance Waterfall",
          "Assembling Type 1/2/3 findings into attribution waterfall…")

    attribution_result = compute_variance_attribution(
        total_gap=primary_gap,
        mapping_result=mapping_result,
        hierarchy_result=hierarchy_result,
        ic_result=ic_result,
        adjustment_result=adj_result,
        type2_result=type2_result,
        type3_result=type3_result,
        anomaly_result=anomaly_result,
    )

    # ── Phase 4: Causal path tracing ─────────────────────────────────────────

    _step(on_step, "Layer 4/5 — Causal Path Tracing",
          "NetworkX DAG backward traversal (7-node graph)…")

    delta_series = {
        "source_data":            0.0,
        "mapping_join":           abs(mapping_result.get("unmapped_amount", 0.0)),
        "ic_elimination":         abs(ic_result.get("uneliminated_amount", 0.0)),
        "orchestration_timing":   abs(primary_gap) * 0.10 if type3_result.get("timestamp_gap_flag") else 0.0,
        "schema_version":         abs(primary_gap) * type2_result.get("unmapped_ratio", 0.0),
        "manual_adj":             abs(
            adj_result.get("implied_target_adjustment", 0.0)
            or adj_result.get("adjustments_in_source", {}).get("amount", 0.0)
        ),
    }

    causal_result = trace_causal_path(
        delta_series=delta_series,
        total_gap=primary_gap,
        type1_result=type1_result,
        type2_result=type2_result,
        type3_result=type3_result,
    )

    _step(on_step, "Layer 4/5 — Causal path complete",
          f"Root cause: "
          f"{causal_result.get('root_cause_node', '?').replace('_', ' ').title()} "
          f"({causal_result.get('contribution_pct', 0):.0%})")

    # ── Phase 5: Confidence gate ──────────────────────────────────────────────

    gate = compute_confidence_gate(type1_result, type2_result, type3_result)
    needs_ai_narrative = gate["needs_ai_narrative"] and bool(api_key)

    ai_used = False
    narrative_tokens = 0
    narrative = ""

    if needs_ai_narrative:
        _step(on_step, "Layer 5 — AI Narrative (Gemini)",
              f"Confidence {gate['overall_confidence']:.0%} < 80% — "
              "sending minimal context (~400 tokens)…")

        narrative_prompt = _build_narrative_prompt(
            business_context=business_context,
            causal_result=causal_result,
            anomaly_result=anomaly_result,
            attribution_result=attribution_result,
            type1_result=type1_result,
            type2_result=type2_result,
            type3_result=type3_result,
            primary_metric=primary_metric,
            primary_gap=primary_gap,
            gap_summary=gap_summary,
            cross_metric_inconsistencies=cross_metric_inconsistencies,
        )
        narrative, narrative_tokens = _call_gemini_narrative(api_key, narrative_prompt)
        total_tokens_used += narrative_tokens
        ai_used = True

        _step(on_step, "Layer 5 — AI Narrative complete",
              f"{narrative_tokens} tokens used")
    else:
        reason = (
            f"confidence {gate['overall_confidence']:.0%} >= 80%"
            if not gate["needs_ai_narrative"]
            else "no API key configured"
        )
        _step(on_step, "Layer 5 — Local Narrative",
              f"0 tokens — {reason}")

        narrative = _local_narrative(
            causal_result=causal_result,
            attribution_result=attribution_result,
            type2_result=type2_result,
            type3_result=type3_result,
            primary_metric=primary_metric,
            primary_gap=primary_gap,
            gap_summary=gap_summary,
            cross_metric_inconsistencies=cross_metric_inconsistencies,
        )

    # ── Assemble agent_steps list (Layer 2 + checks + causal + narrative) ─────

    agent_steps: list[dict] = [
        {
            "step": "Layer 2 — AI Strategy Planner",
            "detail": f"Hypothesis: {ai_hypothesis} | {strategy_tokens} tokens",
            "tool": "gemini_strategy_planner",
            "check_type": "",
        }
    ] + check_steps + [
        {
            "step": "Layer 4/5 — Causal Path Tracing",
            "detail": (
                f"Root cause: "
                f"{causal_result.get('root_cause_node', '').replace('_', ' ').title()} "
                f"({causal_result.get('contribution_pct', 0):.0%})"
            ),
            "tool": "trace_causal_path",
            "check_type": "",
        },
        {
            "step": "Layer 5 — Confidence Gate",
            "detail": (
                f"Overall: {gate['overall_confidence']:.0%} | "
                f"Type 1: {gate['confidence_by_type']['type1_code_explained_pct']:.0%} | "
                f"Type 2: {gate['confidence_by_type']['type2_data_confidence']:.0%} | "
                f"Type 3: {gate['confidence_by_type']['type3_orchestration_confidence']:.0%} | "
                f"AI narrative: {'yes' if ai_used else 'no'}"
            ),
            "tool": "compute_confidence_gate",
            "check_type": "",
        },
    ]

    _step(on_step, "Investigation complete",
          f"Explained {attribution_result.get('explained_pct', 0):.1f}% | "
          f"AI calls: {'2' if ai_used else '1 (strategy only)'} | "
          f"Total tokens: {total_tokens_used}")

    return {
        "attribution":                   attribution_result,
        "narrative":                     narrative,
        "agent_steps":                   agent_steps,
        "tool_results": {
            "check_mapping_gaps":              mapping_result,
            "check_hierarchy_mismatches":      hierarchy_result,
            "check_intercompany_eliminations": ic_result,
            "check_manual_adjustments":        adj_result,
            "run_type2_data_checks":           type2_result,
            "run_type3_orchestration_checks":  type3_result,
            "compute_variance_attribution":    attribution_result,
        },
        "primary_metric":                primary_metric,
        "primary_gap":                   primary_gap,
        "gap_summary":                   gap_summary,
        "cross_metric_inconsistencies":  cross_metric_inconsistencies,
        "causal_result":                 causal_result,
        "anomaly_result":                anomaly_result,
        "strategy":                      strategy,
        "confidence_gate":               gate,
        "ai_used":                       ai_used,
        "tokens_used":                   total_tokens_used,
        "strategy_tokens":               strategy_tokens,
        "narrative_tokens":              narrative_tokens,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _pick_primary_metric(gap_summary: dict) -> tuple[str, float]:
    """Select the highest-priority metric gap to investigate."""
    for metric, info in gap_summary.items():
        if any(cat.lower() in metric.lower() for cat in REVENUE_CATEGORIES) and info.get("has_gap"):
            return metric, info["gap"]
    gapped = {k: v for k, v in gap_summary.items() if v.get("has_gap")}
    if not gapped:
        first = list(gap_summary.keys())[0] if gap_summary else "Revenue"
        return first, 0.0
    best = max(gapped, key=lambda k: abs(gapped[k]["gap"]))
    return best, gapped[best]["gap"]


def _step(
    on_step: Optional[Callable[[str, str], None]],
    label: str,
    detail: str,
) -> None:
    """Fire the on_step callback if provided."""
    if on_step:
        on_step(label, detail)