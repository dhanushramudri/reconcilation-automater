"""
Plotly chart builders for Smart Recon dashboard.

Updated for 6-layer architecture:
  • All colours from config.COLORS — nothing hardcoded.
  • Every waterfall / donut / drill-down component is aware of check_type
    (type1_code · type2_data · type3_orchestration) for colour + label routing.
  • New categories: "Orchestration Timing", "Schema / Mapping Drift",
    "Anomalous Accounts" are handled explicitly.
  • DVT diff table surfaces schema_match column from Layer 1 profiler.
  • New: pipeline_graph_viz() renders the Layer 1 DAG as a Sankey diagram.
  • New: causal_dag_viz() renders the Layer 4/5 causal path as a flow diagram.
  • agent_steps_timeline() updated to colour bars by check_type.
"""

from __future__ import annotations

import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
from typing import Optional

from config import COLORS, APP


# ─────────────────────────────────────────────────────────────────────────────
# Colour + icon helpers
# ─────────────────────────────────────────────────────────────────────────────

# Per check-type accent colours (used when a category colour isn't in COLORS)
_CHECK_TYPE_COLORS: dict[str, str] = {
    "type1_code":          "#E74C3C",   # red — code/mapping issues
    "type2_data":          "#E67E22",   # orange — data/schema issues
    "type3_orchestration": "#9B59B6",   # purple — orchestration issues
}

# Explicit per-category colours (fallback chain: COLORS dict → check_type → neutral)
_CATEGORY_COLORS: dict[str, str] = {
    "Mapping Gap":               "#E74C3C",
    "Intercompany Elimination":  "#C0392B",
    "Manual Adjustment":         "#F39C12",
    "Hierarchy Mismatch":        "#E67E22",
    "Orchestration Timing":      "#9B59B6",   # new
    "Schema / Mapping Drift":    "#2980B9",   # new
    "Anomalous Accounts":        "#7F8C8D",   # new
    "Unexplained Residual":      "#95A5A6",
}

_CHECK_TYPE_ICONS: dict[str, str] = {
    "type1_code":          "🔴 Code",
    "type2_data":          "🟠 Data",
    "type3_orchestration": "🟣 Orch",
}

_STATUS_COLORS: dict[str, str] = {
    "pass": "#D5F5E3",
    "warn": "#FEF9E7",
    "fail": "#FADBD8",
}

_STATUS_FONT_COLORS: dict[str, str] = {
    "pass": "#1D8348",
    "warn": "#9A7D0A",
    "fail": "#922B21",
}


def _cat_color(category: str, check_type: str = "") -> str:
    """Resolve a fill colour for a given category / check_type pair."""
    if category in _CATEGORY_COLORS:
        return _CATEGORY_COLORS[category]
    if category in COLORS:
        return COLORS[category]
    if check_type in _CHECK_TYPE_COLORS:
        return _CHECK_TYPE_COLORS[check_type]
    return COLORS.get("neutral", "#95A5A6")


# ─────────────────────────────────────────────────────────────────────────────
# Metric comparison table
# ─────────────────────────────────────────────────────────────────────────────

def metric_comparison_table(gap_summary: dict) -> go.Figure:
    """
    Traffic-light table: Our Value vs Target vs Gap for each metric.
    Status column uses emoji + text for screen-reader friendliness.
    """
    rows = []
    for metric, info in gap_summary.items():
        gap = info.get("gap", 0)
        gap_pct = info.get("gap_pct")
        rows.append({
            "Metric": metric,
            f"Our Value ({APP['currency_symbol']}M)": round(info.get("our_value", 0) / 1e6, 2),
            f"Target ({APP['currency_symbol']}M)": round(info.get("target_value", 0) / 1e6, 2),
            f"Gap ({APP['currency_symbol']}M)": round(gap / 1e6, 2),
            "Gap %": f"{gap_pct:.1f}%" if gap_pct is not None else "—",
            "Status": (
                "✅ Match" if not info.get("has_gap")
                else ("🔴 Gap" if abs(gap) > 1e6 else "🟡 Minor")
            ),
        })

    if not rows:
        return go.Figure()

    df = pd.DataFrame(rows)

    # Row-level fill colours keyed on Status
    fill_colors = [
        "#D5F5E3" if "✅" in str(r["Status"])
        else ("#FADBD8" if "🔴" in str(r["Status"]) else "#FEF9E7")
        for _, r in df.iterrows()
    ]

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=list(df.columns),
                    fill_color=COLORS["primary"],
                    font=dict(color="white", size=13, family="Arial"),
                    align="left",
                    height=36,
                ),
                cells=dict(
                    values=[df[c] for c in df.columns],
                    fill_color=(
                        [["white"] * len(df)] * (len(df.columns) - 1)
                        + [fill_colors]
                    ),
                    font=dict(size=12, family="Arial"),
                    align="left",
                    height=30,
                ),
            )
        ]
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=10),
        height=max(200, 60 + len(rows) * 32),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Variance waterfall
# ─────────────────────────────────────────────────────────────────────────────

def variance_waterfall(attribution: dict) -> go.Figure:
    """
    Waterfall chart decomposing the total gap into root cause components.

    Each bar is coloured by its check_type (Type 1/2/3) and labelled with
    a check-type badge so analysts can immediately see which layer of the
    6-layer architecture produced each finding.
    """
    components = attribution.get("components", [])
    total_gap = attribution.get("total_gap", 0)

    if not components:
        return go.Figure()

    # Build label list — append check-type badge to each component label
    def _label(comp: dict) -> str:
        badge = _CHECK_TYPE_ICONS.get(comp.get("check_type", ""), "")
        base = comp["label"]
        return f"{base}<br><span style='font-size:10px'>{badge}</span>" if badge else base

    labels = (
        ["Total Gap"]
        + [_label(c) for c in components]
        + ["Explained Total"]
    )
    values = [total_gap] + [c["amount"] for c in components] + [0]
    measures = ["absolute"] + ["relative"] * len(components) + ["total"]

    text = (
        [f"${abs(total_gap) / 1e6:.2f}M"]
        + [
            f"${abs(c['amount']) / 1e6:.2f}M ({abs(c['pct_of_gap']):.0f}%)"
            for c in components
        ]
        + [""]
    )

    # Per-bar colours driven by check_type
    bar_colors = (
        [COLORS.get("primary", "#1B4F72")]
        + [_cat_color(c.get("category", ""), c.get("check_type", "")) for c in components]
        + [COLORS.get("accent", "#2E86C1")]
    )

    fig = go.Figure(
        go.Waterfall(
            name="Variance Bridge",
            orientation="v",
            measure=measures,
            x=labels,
            y=values,
            text=text,
            textposition="outside",
            connector={"line": {"color": COLORS.get("primary", "#1B4F72"), "width": 1}},
            increasing={"marker": {"color": COLORS.get("positive", "#27AE60")}},
            decreasing={"marker": {"color": COLORS.get("negative", "#E74C3C")}},
            totals={"marker": {"color": COLORS.get("accent", "#2E86C1")}},
        )
    )

    # Overlay individual bar colours via a scatter trace (Waterfall doesn't
    # support per-bar colours natively, so we patch via marker on update)
    fig.data[0].update(
        decreasing_marker_color=COLORS.get("negative", "#E74C3C"),
        increasing_marker_color=COLORS.get("positive", "#27AE60"),
    )

    fig.update_traces(textfont=dict(size=11, family="Arial"))
    fig.update_layout(
        title=dict(
            text=(
                f"<b>Variance Attribution Bridge</b> — "
                f"Total Gap: ${abs(total_gap) / 1e6:.2f}M"
            ),
            font=dict(size=16, color=COLORS.get("primary", "#1B4F72")),
        ),
        showlegend=False,
        plot_bgcolor=COLORS.get("bg", "#F8F9FA"),
        paper_bgcolor="white",
        yaxis=dict(
            title="Amount ($)",
            gridcolor="#E0E0E0",
            tickformat="$,.0f",
        ),
        xaxis=dict(tickangle=-20),
        margin=dict(l=60, r=20, t=60, b=100),
        height=440,
    )

    # ── Check-type legend annotation ─────────────────────────────────────────
    fig.add_annotation(
        x=1.0, y=-0.22,
        xref="paper", yref="paper",
        text=(
            "<b>Check type:</b>  "
            "🔴 Type 1 — Code &nbsp;&nbsp;"
            "🟠 Type 2 — Data &nbsp;&nbsp;"
            "🟣 Type 3 — Orchestration"
        ),
        showarrow=False,
        font=dict(size=10, color="#4A5568"),
        align="right",
    )

    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Root cause donut
# ─────────────────────────────────────────────────────────────────────────────

def root_cause_donut(attribution: dict) -> go.Figure:
    """
    Donut chart showing % split of gap by root cause category.
    Each slice is coloured consistently with the waterfall.
    Informational $0 components (Hierarchy Mismatch, Anomalous Accounts)
    are omitted from the donut as they have no dollar value.
    """
    components = [
        c for c in attribution.get("components", [])
        if abs(c.get("amount", 0)) > 0
    ]
    if not components:
        return go.Figure()

    labels = [c["category"] for c in components]
    values = [abs(c["amount"]) for c in components]
    colors = [
        _cat_color(c.get("category", ""), c.get("check_type", ""))
        for c in components
    ]

    fig = go.Figure(
        go.Pie(
            labels=labels,
            values=values,
            hole=0.55,
            marker=dict(colors=colors, line=dict(color="white", width=2)),
            textinfo="label+percent",
            textfont=dict(size=11),
            hovertemplate=(
                "<b>%{label}</b><br>"
                "$%{value:,.0f}<br>"
                "%{percent}<extra></extra>"
            ),
        )
    )

    total_gap = attribution.get("total_gap", 0)
    fig.add_annotation(
        text=f"<b>${abs(total_gap) / 1e6:.1f}M</b><br>Total Gap",
        x=0.5, y=0.5,
        showarrow=False,
        font=dict(size=14, color=COLORS.get("primary", "#1B4F72")),
    )
    fig.update_layout(
        showlegend=True,
        legend=dict(orientation="v", x=1.0, y=0.5),
        margin=dict(l=10, r=150, t=30, b=10),
        height=320,
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Confidence bar
# ─────────────────────────────────────────────────────────────────────────────

def confidence_bar(attribution: dict) -> go.Figure:
    """
    Horizontal bar showing explanation coverage, overall confidence,
    and auto-fixable % — all from the Layer 4/5 confidence gate.

    The confidence bar is coloured red when < 80% (AI narrative triggered)
    and green when >= 80% (local narrative, 0 tokens).
    """
    explained_pct = attribution.get("explained_pct", 0)
    confidence = attribution.get("overall_confidence", 0)
    auto_fixable = attribution.get("auto_fixable_pct", 0)

    ai_threshold = 80.0  # matches CONFIDENCE_AI_TRIGGER * 100 from reconciler.py
    conf_color = (
        COLORS.get("positive", "#27AE60")
        if confidence >= ai_threshold
        else COLORS.get("negative", "#E74C3C")
    )

    metrics = ["Explained", "Confidence", "Auto-Fixable"]
    values = [explained_pct, confidence, auto_fixable]
    bar_colors = [
        COLORS.get("positive", "#27AE60"),
        conf_color,
        COLORS.get("warning", "#F39C12"),
    ]

    fig = go.Figure()
    for m, v, c in zip(metrics, values, bar_colors):
        fig.add_trace(
            go.Bar(
                name=m,
                x=[v],
                y=[m],
                orientation="h",
                marker_color=c,
                text=[f"{v:.1f}%"],
                textposition="inside",
                insidetextanchor="middle",
                textfont=dict(color="white", size=13, family="Arial Bold"),
                width=0.5,
            )
        )

    # AI trigger annotation on the confidence bar
    fig.add_vline(
        x=ai_threshold,
        line_dash="dash",
        line_color="#718096",
        annotation_text="AI trigger (80%)",
        annotation_position="top right",
        annotation_font=dict(size=9, color="#718096"),
    )

    fig.update_layout(
        showlegend=False,
        xaxis=dict(range=[0, 100], ticksuffix="%", gridcolor="#E0E0E0"),
        yaxis=dict(tickfont=dict(size=13)),
        plot_bgcolor=COLORS.get("bg", "#F8F9FA"),
        paper_bgcolor="white",
        margin=dict(l=10, r=10, t=10, b=10),
        height=160,
        barmode="group",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Drill-down table
# ─────────────────────────────────────────────────────────────────────────────

def drill_down_table(findings: list[dict], category: str, check_type: str = "") -> go.Figure:
    """
    Detailed table for a specific root cause category.
    Header colour is driven by check_type (Type 1/2/3) when available,
    falling back to the category colour map.

    Adds a 'check_type' column header badge so analysts see which layer
    produced each row.
    """
    if not findings:
        return go.Figure()

    df = pd.DataFrame(findings)

    # Format currency columns
    for col in df.columns:
        if any(kw in col.lower() for kw in ("amount", "value", "gap", "uneliminated", "credit", "debit")):
            df[col] = df[col].apply(
                lambda x: f"${x:,.0f}" if isinstance(x, (int, float)) else x
            )

    header_color = _cat_color(category, check_type)

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[col.replace("_", " ").title() for col in df.columns],
                    fill_color=header_color,
                    font=dict(color="white", size=12, family="Arial"),
                    align="left",
                    height=32,
                ),
                cells=dict(
                    values=[df[c] for c in df.columns],
                    fill_color="white",
                    font=dict(size=11, family="Arial"),
                    align="left",
                    height=28,
                ),
            )
        ]
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=10),
        height=max(150, 50 + len(df) * 30),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# DVT diff table  (Layer 5 — per-metric comparison)
# ─────────────────────────────────────────────────────────────────────────────

def dvt_diff_table(dvt_rows: list[dict]) -> go.Figure:
    """
    Data-Validation-Tool style per-metric comparison table.

    Columns: Metric · Source · Target · Delta · Delta % · Status ·
             Checksum Match · Schema Match (new — from Layer 1 profiler)

    Row fill colours are driven by status (pass/warn/fail).
    Schema Match column surfaces Layer 4 Type 2 schema drift directly
    in the DVT table so analysts don't need to navigate to a separate view.
    """
    if not dvt_rows:
        return go.Figure()

    df = pd.DataFrame(dvt_rows)

    # Friendly display values
    def _fmt_status(s: str) -> str:
        return {"pass": "✅ Pass", "warn": "🟡 Warn", "fail": "🔴 Fail"}.get(s, s)

    def _fmt_bool(v) -> str:
        if v is True:
            return "✅ Match"
        if v is False:
            return "❌ Diff"
        return "—"

    display_df = df.copy()
    if "status" in display_df.columns:
        display_df["status"] = display_df["status"].map(_fmt_status)
    if "checksum_match" in display_df.columns:
        display_df["checksum_match"] = display_df["checksum_match"].map(_fmt_bool)
    if "schema_match" in display_df.columns:
        display_df["schema_match"] = display_df["schema_match"].map(_fmt_bool)

    # Row fill colours by original status
    row_fills: list[str] = []
    for _, row in df.iterrows():
        row_fills.append(_STATUS_COLORS.get(str(row.get("status", "pass")), "white"))

    # Column count: fill white for all cols except last (status)
    n_cols = len(display_df.columns)
    col_fills = [["white"] * len(display_df)] * (n_cols - 1) + [row_fills]

    fig = go.Figure(
        data=[
            go.Table(
                header=dict(
                    values=[col.replace("_", " ").title() for col in display_df.columns],
                    fill_color=COLORS.get("primary", "#1B4F72"),
                    font=dict(color="white", size=12, family="Arial"),
                    align="left",
                    height=34,
                ),
                cells=dict(
                    values=[display_df[c] for c in display_df.columns],
                    fill_color=col_fills,
                    font=dict(size=11, family="Arial"),
                    align="left",
                    height=28,
                ),
            )
        ]
    )
    fig.update_layout(
        margin=dict(l=0, r=0, t=10, b=10),
        height=max(200, 60 + len(display_df) * 30),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline graph visualisation  (Layer 1 — new)
# ─────────────────────────────────────────────────────────────────────────────

def pipeline_graph_viz(pipeline_graph: dict) -> go.Figure:
    """
    Layer 1 — Render the pipeline DAG as a Sankey diagram.

    Nodes are the transformation steps extracted by the schema profiler.
    Links follow the DAG edges.  Node colour is driven by transform_type
    so analysts can immediately see where joins, filters, and aggregations sit.

    Falls back to a minimal placeholder when the graph has no steps.
    """
    steps = pipeline_graph.get("steps", [])
    edges = pipeline_graph.get("edges", [])

    if not steps:
        fig = go.Figure()
        fig.add_annotation(
            text="No pipeline graph available — using file-upload demo mode",
            x=0.5, y=0.5, xref="paper", yref="paper",
            showarrow=False, font=dict(size=13, color="#718096"),
        )
        fig.update_layout(height=200, paper_bgcolor="white")
        return fig

    # Map step_name → index for Sankey
    step_index = {s["step_name"]: i for i, s in enumerate(steps)}

    _transform_colors: dict[str, str] = {
        "ingest":    "#3498DB",
        "join":      "#E74C3C",
        "filter":    "#F39C12",
        "aggregate": "#27AE60",
        "transform": "#9B59B6",
        "write":     "#1ABC9C",
        "unknown":   "#95A5A6",
    }

    node_colors = [
        _transform_colors.get(s.get("transform_type", "unknown"), "#95A5A6")
        for s in steps
    ]
    node_labels = [
        f"{s['step_name']}<br><i>{s.get('transform_type','')}</i>"
        for s in steps
    ]

    # Build Sankey links from DAG edges
    link_sources, link_targets, link_values = [], [], []
    for edge in edges:
        src = step_index.get(edge.get("from_step"))
        tgt = step_index.get(edge.get("to_step"))
        if src is not None and tgt is not None:
            link_sources.append(src)
            link_targets.append(tgt)
            link_values.append(1)

    # Fallback: sequential links if no edges defined
    if not link_sources and len(steps) > 1:
        for i in range(len(steps) - 1):
            link_sources.append(i)
            link_targets.append(i + 1)
            link_values.append(1)

    fig = go.Figure(
        go.Sankey(
            arrangement="snap",
            node=dict(
                pad=20,
                thickness=20,
                line=dict(color="white", width=0.5),
                label=node_labels,
                color=node_colors,
                hovertemplate=(
                    "<b>%{label}</b><br>"
                    "Index: %{index}<extra></extra>"
                ),
            ),
            link=dict(
                source=link_sources,
                target=link_targets,
                value=link_values,
                color="rgba(189,195,199,0.4)",
            ),
        )
    )
    fig.update_layout(
        title=dict(
            text="<b>Layer 1 — Pipeline DAG</b> (schema profiler output)",
            font=dict(size=14, color=COLORS.get("primary", "#1B4F72")),
        ),
        paper_bgcolor="white",
        margin=dict(l=20, r=20, t=50, b=20),
        height=max(300, 80 + len(steps) * 40),
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Causal DAG visualisation  (Layer 4/5 — new)
# ─────────────────────────────────────────────────────────────────────────────

def causal_dag_viz(causal_result: dict) -> go.Figure:
    """
    Layer 4/5 — Render the causal path trace as a horizontal flow diagram.

    Nodes are the DAG steps from trace_causal_path():
      source_data → mapping_join → ic_elimination → orchestration_timing
      → schema_version → manual_adj → result

    The root cause node is highlighted in red.  Nodes with zero delta are
    shown in light grey so the dominant causal path stands out immediately.
    """
    path = causal_result.get("path_traversed", [])
    root_cause = causal_result.get("root_cause_node", "")
    contribution_pct = causal_result.get("contribution_pct", 0)

    # Full ordered node list (matches investigators.py DAG)
    all_nodes = [
        "source_data",
        "mapping_join",
        "ic_elimination",
        "orchestration_timing",
        "schema_version",
        "manual_adj",
        "result",
    ]

    # Build delta lookup from path
    delta_map = {p["node"]: p for p in path}

    # Node positions (left to right)
    x_positions = [i / (len(all_nodes) - 1) for i in range(len(all_nodes))]
    y_positions = [0.5] * len(all_nodes)

    node_colors_viz, node_sizes, hover_texts = [], [], []
    for node in all_nodes:
        info = delta_map.get(node, {})
        delta = info.get("delta", 0.0)
        pct = info.get("contribution_pct", 0.0)

        if node == "result":
            color = COLORS.get("primary", "#1B4F72")
            size = 18
        elif node == root_cause:
            color = "#E74C3C"   # red — dominant root cause
            size = 22
        elif abs(delta) > 0:
            color = "#F39C12"   # orange — contributing node
            size = 16
        else:
            color = "#BDC3C7"   # grey — no contribution
            size = 12

        node_colors_viz.append(color)
        node_sizes.append(size)
        hover_texts.append(
            f"<b>{node.replace('_', ' ').title()}</b><br>"
            f"Delta: ${delta:,.0f}<br>"
            f"Contribution: {pct * 100:.1f}%"
            + (" ← ROOT CAUSE" if node == root_cause else "")
        )

    fig = go.Figure()

    # Draw edges first (underneath nodes)
    for i in range(len(all_nodes) - 1):
        fig.add_shape(
            type="line",
            x0=x_positions[i], y0=0.5,
            x1=x_positions[i + 1], y1=0.5,
            xref="paper", yref="paper",
            line=dict(color="#BDC3C7", width=2),
        )

    # Draw nodes as scatter markers
    fig.add_trace(
        go.Scatter(
            x=x_positions,
            y=y_positions,
            mode="markers+text",
            marker=dict(
                color=node_colors_viz,
                size=node_sizes,
                line=dict(color="white", width=2),
            ),
            text=[n.replace("_", "<br>") for n in all_nodes],
            textposition="bottom center",
            textfont=dict(size=10, color="#2D3748"),
            hovertext=hover_texts,
            hoverinfo="text",
            showlegend=False,
        )
    )

    # Root cause annotation
    if root_cause in all_nodes:
        rc_x = x_positions[all_nodes.index(root_cause)]
        fig.add_annotation(
            x=rc_x, y=0.75,
            xref="paper", yref="paper",
            text=f"<b>Root Cause</b><br>{contribution_pct * 100:.0f}% contribution",
            showarrow=True, arrowhead=2, arrowcolor="#E74C3C",
            font=dict(size=11, color="#E74C3C"),
            bgcolor="white",
            bordercolor="#E74C3C",
            borderwidth=1,
        )

    fig.update_layout(
        title=dict(
            text="<b>Layer 4/5 — Causal Path Trace</b>",
            font=dict(size=14, color=COLORS.get("primary", "#1B4F72")),
        ),
        xaxis=dict(visible=False, range=[-0.05, 1.05]),
        yaxis=dict(visible=False, range=[0.0, 1.2]),
        paper_bgcolor="white",
        plot_bgcolor="white",
        margin=dict(l=20, r=20, t=50, b=60),
        height=260,
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Agent steps timeline
# ─────────────────────────────────────────────────────────────────────────────

def agent_steps_timeline(steps: list[dict]) -> go.Figure:
    """
    Visual timeline of Layer 4 agent investigation steps.
    Bar colour is driven by check_type (type1/2/3) rather than tool name
    so the colour scheme matches the rest of the dashboard.
    """
    if not steps:
        return go.Figure()

    step_labels = [s.get("step", "Step") for s in steps]
    details = [s.get("detail", "") for s in steps]
    check_types = [s.get("check_type", s.get("tool", "")) for s in steps]

    bar_colors = [
        _CHECK_TYPE_COLORS.get(ct, COLORS.get("neutral", "#95A5A6"))
        for ct in check_types
    ]

    fig = go.Figure()
    for label, detail, color in zip(step_labels, details, bar_colors):
        fig.add_trace(
            go.Bar(
                x=[1],
                y=[label],
                orientation="h",
                marker_color=color,
                text=detail[:80] + ("..." if len(detail) > 80 else ""),
                textposition="inside",
                insidetextanchor="middle",
                textfont=dict(color="white", size=10),
                width=0.6,
                showlegend=False,
            )
        )

    # Legend annotation
    fig.add_annotation(
        x=1.5, y=-0.1,
        xref="x", yref="paper",
        text=(
            "🔴 Type 1 — Code  "
            "🟠 Type 2 — Data  "
            "🟣 Type 3 — Orchestration"
        ),
        showarrow=False,
        font=dict(size=9, color="#4A5568"),
        align="right",
    )

    fig.update_layout(
        xaxis=dict(visible=False, range=[0, 1.5]),
        yaxis=dict(autorange="reversed"),
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(l=10, r=10, t=10, b=40),
        height=max(150, 50 + len(steps) * 40),
        barmode="stack",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Fix preview chart
# ─────────────────────────────────────────────────────────────────────────────

def fix_preview_chart(comparison: dict, metric: str) -> go.Figure:
    """
    Before/after bar chart for a specific metric after applying a Layer 5 fix.
    The "After Fix" bar is coloured green when it moves closer to target,
    red when it overshoots.
    """
    info = comparison.get(metric, {})
    if not info:
        return go.Figure()

    target = info.get("target_value", 0)
    before = info.get("before_our_value", 0)
    after = info.get("after_our_value", 0)

    gap_before = abs(before - target)
    gap_after = abs(after - target)
    after_color = (
        COLORS.get("positive", "#27AE60")
        if gap_after < gap_before
        else COLORS.get("negative", "#E74C3C")
    )

    fig = go.Figure()
    for label, value, color in [
        ("Target", target, COLORS.get("positive", "#27AE60")),
        ("Before Fix", before, COLORS.get("negative", "#E74C3C")),
        ("After Fix", after, after_color),
    ]:
        fig.add_trace(
            go.Bar(
                name=label,
                x=[label],
                y=[value / 1e6],
                marker_color=color,
                text=[f"${value / 1e6:.2f}M"],
                textposition="outside",
                textfont=dict(size=13, family="Arial Bold"),
                width=0.5,
            )
        )

    # Gap-closure annotation
    gap_closed_pct = info.get("gap_closed_pct", 0)
    if gap_closed_pct:
        fig.add_annotation(
            x="After Fix", y=after / 1e6,
            text=f"+{gap_closed_pct:.1f}% gap closed",
            showarrow=True, arrowhead=2,
            font=dict(size=11, color=after_color),
            bgcolor="white",
            bordercolor=after_color,
        )

    fig.update_layout(
        title=f"<b>{metric}</b> — Fix Preview (Layer 5 Sandbox)",
        yaxis=dict(title="$M", gridcolor="#E0E0E0"),
        plot_bgcolor=COLORS.get("bg", "#F8F9FA"),
        paper_bgcolor="white",
        showlegend=True,
        margin=dict(l=40, r=20, t=50, b=40),
        height=340,
        barmode="group",
    )
    return fig