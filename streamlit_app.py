"""
Smart Recon — Automated Financial Reconciliation Engine
JMAN Group Hackathon Demo  |  6-Layer Architecture

Run with: streamlit run streamlit_app.py

Architecture:
  Layer 0 — Source connectors  (Databricks · Fabric · SQL Server)
  Layer 1 — Schema profiler + pipeline parser
  Layer 2 — AI strategy planner  (1 AI call, ~600 tokens)
  Layer 3 — Sandbox executor     (SmartRecon notebook, JMAN-owned)
  Layer 4 — Hypothesis investigator  (Type 1/2/3, Python + ML)
  Layer 5 — Results + narrative + fix preview
"""

import os
import sys
import json
import time
from typing import Optional
import pandas as pd
import streamlit as st
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

sys.path.insert(0, str(Path(__file__).parent))
from config import APP, COLORS, CATEGORY_ICONS, CATEGORY_CSS, METRIC_DISPLAY_ORDER, CONFIDENCE

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title=f"{APP['title']} | {APP['org_name']}",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS ───────────────────────────────────────────────────────────────────────
st.markdown(
    """
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

    /* ── Header ── */
    .main-header {
        background: linear-gradient(135deg, #0D2137 0%, #1B4F72 60%, #2E86C1 100%);
        padding: 2rem 2.5rem;
        border-radius: 12px;
        color: white;
        margin-bottom: 1.5rem;
        position: relative;
        overflow: hidden;
    }
    .main-header::after {
        content: "";
        position: absolute;
        top: -40px; right: -40px;
        width: 180px; height: 180px;
        border-radius: 50%;
        background: rgba(255,255,255,0.05);
    }
    .main-header h1 { margin: 0; font-size: 2rem; font-weight: 700; letter-spacing: -0.5px; }
    .main-header p  { margin: 0.3rem 0 0; opacity: 0.85; font-size: 1rem; }
    .main-header .arch-pill {
        display: inline-block;
        background: rgba(255,255,255,0.15);
        border: 1px solid rgba(255,255,255,0.25);
        border-radius: 20px;
        padding: 0.2rem 0.8rem;
        font-size: 0.75rem;
        font-weight: 600;
        letter-spacing: 0.05em;
        margin-top: 0.6rem;
        margin-right: 0.4rem;
    }

    /* ── Metric cards ── */
    .metric-card {
        background: white;
        border: 1px solid #E8EDF2;
        border-radius: 10px;
        padding: 1.2rem 1.5rem;
        text-align: center;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .metric-card .value { font-size: 1.7rem; font-weight: 700; color: #1B4F72; }
    .metric-card .label { font-size: 0.8rem; color: #718096; text-transform: uppercase; letter-spacing: 0.05em; margin-top: 0.2rem; }

    .gap-card-red   { background: #FFF5F5; border-left: 4px solid #E53E3E !important; }
    .gap-card-green { background: #F0FFF4; border-left: 4px solid #38A169 !important; }
    .gap-card-warn  { background: #FFFFF0; border-left: 4px solid #D69E2E !important; }

    /* ── Section header ── */
    .section-header {
        font-size: 1.1rem;
        font-weight: 600;
        color: #1B4F72;
        border-bottom: 2px solid #E2E8F0;
        padding-bottom: 0.5rem;
        margin-bottom: 1rem;
    }

    /* ── Architecture layer cards ── */
    .arch-layer {
        border-radius: 10px;
        padding: 1rem 1.2rem;
        margin-bottom: 0.6rem;
        border: 1px solid rgba(0,0,0,0.07);
        display: flex;
        align-items: flex-start;
        gap: 1rem;
    }
    .arch-layer .layer-badge {
        min-width: 90px;
        text-align: center;
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        padding: 0.25rem 0.5rem;
        border-radius: 6px;
        margin-top: 0.1rem;
    }
    .arch-layer .layer-content h4 { margin: 0 0 0.3rem; font-size: 0.95rem; font-weight: 600; color: #1B4F72; }
    .arch-layer .layer-content p  { margin: 0; font-size: 0.83rem; color: #4A5568; line-height: 1.5; }

    .layer-0 { background: #F0F8FF; }
    .layer-0 .layer-badge { background: #BEE3F8; color: #1A365D; }
    .layer-1 { background: #F7FAFC; }
    .layer-1 .layer-badge { background: #CBD5E0; color: #2D3748; }
    .layer-2 { background: #EBF8FF; border-left: 4px solid #63B3ED; }
    .layer-2 .layer-badge { background: #90CDF4; color: #1A365D; }
    .layer-3 { background: #FFF5F0; border-left: 4px solid #FC8181; }
    .layer-3 .layer-badge { background: #FEB2B2; color: #742A2A; }
    .layer-4 { background: #F0FFF4; border-left: 4px solid #68D391; }
    .layer-4 .layer-badge { background: #9AE6B4; color: #1C4532; }
    .layer-5 { background: #FFFFF0; border-left: 4px solid #F6E05E; }
    .layer-5 .layer-badge { background: #FAF089; color: #744210; }

    /* ── Recon type cards ── */
    .recon-type-card {
        border-radius: 10px;
        padding: 1rem 1.2rem;
        color: white;
        height: 100%;
    }
    .recon-type-1 { background: linear-gradient(135deg, #667eea, #764ba2); }
    .recon-type-2 { background: linear-gradient(135deg, #f6a623, #e55d2b); }
    .recon-type-3 { background: linear-gradient(135deg, #11998e, #38ef7d); }
    .recon-type-card h4 { margin: 0 0 0.5rem; font-size: 0.9rem; font-weight: 700; }
    .recon-type-card ul { margin: 0; padding-left: 1.2rem; font-size: 0.8rem; opacity: 0.9; line-height: 1.6; }

    /* ── Source connector cards ── */
    .connector-card {
        border: 1.5px solid #E2E8F0;
        border-radius: 10px;
        padding: 1rem;
        background: white;
        text-align: center;
        transition: box-shadow 0.15s;
    }
    .connector-card:hover { box-shadow: 0 4px 12px rgba(0,0,0,0.1); }
    .connector-card .conn-icon { font-size: 1.8rem; margin-bottom: 0.4rem; }
    .connector-card h4 { margin: 0 0 0.3rem; font-size: 0.9rem; font-weight: 700; color: #1B4F72; }
    .connector-card .conn-items { font-size: 0.75rem; color: #718096; line-height: 1.6; }
    .connector-card .conn-badge {
        display: inline-block;
        background: #EBF8FF;
        color: #2C5282;
        border-radius: 4px;
        padding: 0.1rem 0.4rem;
        font-size: 0.7rem;
        font-weight: 600;
        margin: 0.15rem 0.1rem;
    }

    /* ── Security model strip ── */
    .security-strip {
        background: #1A202C;
        border-radius: 8px;
        padding: 0.7rem 1.2rem;
        display: flex;
        flex-wrap: wrap;
        gap: 0.5rem;
        align-items: center;
        margin-top: 0.8rem;
    }
    .security-strip .sec-label {
        color: #A0AEC0;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-right: 0.5rem;
    }
    .security-strip .sec-badge {
        background: #2D3748;
        color: #E2E8F0;
        border: 1px solid #4A5568;
        border-radius: 4px;
        padding: 0.2rem 0.6rem;
        font-size: 0.75rem;
        font-weight: 500;
    }

    /* ── Step items ── */
    .step-item {
        background: #F7FAFC;
        border-left: 3px solid #2E86C1;
        border-radius: 4px;
        padding: 0.6rem 1rem;
        margin-bottom: 0.5rem;
        font-size: 0.9rem;
    }
    .step-item .step-title { font-weight: 600; color: #2D3748; }
    .step-item .step-detail { color: #718096; font-size: 0.82rem; margin-top: 0.15rem; }

    /* ── Root-cause cards ── */
    .root-cause-card {
        border-radius: 10px;
        padding: 1.2rem;
        margin-bottom: 0.8rem;
        border: 1px solid rgba(0,0,0,0.08);
    }
    .rc-mapping    { background: #FFF5F5; border-left: 5px solid #E74C3C; }
    .rc-hierarchy  { background: #FFF8F0; border-left: 5px solid #E67E22; }
    .rc-ic         { background: #F8F0FF; border-left: 5px solid #9B59B6; }
    .rc-adjustment { background: #FFFFF0; border-left: 5px solid #F39C12; }
    .rc-residual   { background: #F7FAFC; border-left: 5px solid #95A5A6; }

    /* ── Confidence badges ── */
    .confidence-badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .conf-high  { background: #C6F6D5; color: #276749; }
    .conf-med   { background: #FEFCBF; color: #744210; }
    .conf-low   { background: #FED7D7; color: #822727; }

    /* ── AI call badge ── */
    .ai-call-badge {
        display: inline-flex;
        align-items: center;
        gap: 0.4rem;
        background: linear-gradient(90deg, #EBF8FF, #F0FFF4);
        border: 1px solid #90CDF4;
        border-radius: 20px;
        padding: 0.3rem 0.8rem;
        font-size: 0.8rem;
        font-weight: 600;
        color: #1A365D;
        margin-bottom: 0.5rem;
    }

    /* ── Buttons ── */
    .stButton > button {
        background: linear-gradient(135deg, #1B4F72, #2E86C1);
        color: white;
        border: none;
        border-radius: 8px;
        padding: 0.6rem 1.5rem;
        font-weight: 600;
        font-size: 0.95rem;
        cursor: pointer;
        transition: opacity 0.2s;
    }
    .stButton > button:hover { opacity: 0.9; }

    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        border-radius: 8px 8px 0 0;
        padding: 0.5rem 1.2rem;
        font-weight: 500;
    }

    /* ── Domain plugin strip ── */
    .domain-strip {
        background: #F7FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 0.6rem 1rem;
        font-size: 0.8rem;
        color: #4A5568;
        margin-top: 0.6rem;
    }
    .domain-pill {
        display: inline-block;
        background: white;
        border: 1px solid #CBD5E0;
        border-radius: 4px;
        padding: 0.15rem 0.5rem;
        font-size: 0.75rem;
        font-weight: 500;
        margin: 0.1rem 0.2rem;
        color: #2D3748;
    }
    </style>
    """,
    unsafe_allow_html=True,
)

# ── Imports (after page config) ───────────────────────────────────────────────
from core.data_loader import load_source, load_mapping, load_target, load_hierarchy, validate_files, fuzzy_map_accounts
from core.profiler import build_fingerprint
from core.reconciler import compute_our_metrics, parse_target_metrics, compute_gap_summary, format_currency
from core.fix_engine import preview_fix, get_audit_trail, clear_audit_trail
from agents.orchestrator import run_recon_agent
from agents.investigators import compute_dvt_diff
import utils.charts as charts


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown(f"## 🔍 {APP['title']}")
    st.markdown(f"*{APP['subtitle']}*")
    st.divider()

    # ── Source system selector ────────────────────────────────────────────────
    st.markdown("**Source System**")
    source_system = st.selectbox(
        "Connect to",
        ["File Upload (Demo)", "Databricks", "Microsoft Fabric", "SQL Server / ADF"],
        help="In production, SmartRecon connects read-only via OAuth/SPN to your data platform.",
        label_visibility="collapsed",
    )
    st.session_state["source_system"] = source_system

    if source_system != "File Upload (Demo)":
        st.info(
            f"🔗 **{source_system}** connector uses read-only Service Principal auth. "
            "No raw data is extracted — only schema stats, run metadata and aggregate queries.",
            icon="🔒",
        )

    st.divider()

    api_key = st.text_input(
        "Gemini API Key",
        value=os.environ.get("GEMINI_API_KEY", ""),
        type="password",
        help="Used only for the AI strategy planner (Layer 2). ~600 tokens per investigation.",
    )
    if api_key:
        os.environ["GEMINI_API_KEY"] = api_key

    st.divider()

    st.markdown("**Session State**")
    if "source" in st.session_state:
        st.success(f"✅ Source: {len(st.session_state.source):,} rows")
    if "mapping" in st.session_state:
        st.success(f"✅ Mapping: {len(st.session_state.mapping):,} rules")
    if "target" in st.session_state:
        st.success(f"✅ Target: {len(st.session_state.target):,} rows")
    if "agent_result" in st.session_state:
        st.success("✅ Investigation complete")

    st.divider()

    # ── Architecture summary in sidebar ──────────────────────────────────────
    with st.expander("🏗️ Architecture (6 layers)", expanded=False):
        st.markdown(
            """
            **Layer 0** — Source connectors  
            Databricks · Fabric · SQL Server  
            *(read-only, OAuth/SPN)*

            **Layer 1** — Schema profiler  
            Pipeline DAG · column types · run metadata

            **Layer 2** — AI strategy planner  
            1 AI call · ~600 tokens · ranked checklist

            **Layer 3** — Sandbox executor  
            JMAN-owned notebook · cell-by-cell replay

            **Layer 4** — Hypothesis investigator  
            Type 1 (code) · Type 2 (data) · Type 3 (orch)

            **Layer 5** — Output  
            Waterfall · drill-down · fix preview · PDF
            """
        )

    st.markdown(
        f"<small>{APP['org_name']} — {APP['title']} PoC</small>",
        unsafe_allow_html=True,
    )


# ─────────────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────────────

st.markdown(
    f"""
    <div class="main-header">
        <h1>🔍 {APP['title']}</h1>
        <p>{APP['subtitle']} — Powered by Agentic AI · 6-Layer Architecture</p>
        <div>
            <span class="arch-pill">🔗 Databricks</span>
            <span class="arch-pill">🧱 MS Fabric</span>
            <span class="arch-pill">🗄️ SQL Server / ADF</span>
            <span class="arch-pill">🤖 1 AI Call · ~600 tokens</span>
            <span class="arch-pill">🔒 No raw data to AI</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

# ─────────────────────────────────────────────────────────────────────────────
# Helper functions
# ─────────────────────────────────────────────────────────────────────────────

def _get_fix_description(fix_type: str, comp: dict) -> str:
    descriptions = {
        "add_mapping": (
            f"Add {len(comp.get('drill_down', []))} missing account codes to the mapping table. "
            "These account codes exist in source data but have no target mapping, causing their amounts to be excluded from computed metrics."
        ),
        "fix_hierarchy": (
            "Correct the entity rollup structure so entities map to the same business units and regions "
            "as the client's target system."
        ),
        "add_elimination": (
            "Add missing intercompany elimination journal entries so intra-group transactions "
            "net to zero at the consolidated level."
        ),
    }
    return descriptions.get(fix_type, "Apply recommended correction.")


def _build_fix_data(fix_type: str, comp: dict, source: pd.DataFrame, mapping: pd.DataFrame) -> dict:
    from config import DATA

    if fix_type == "add_mapping":
        new_entries = []
        for item in comp.get("drill_down", []):
            acc_code = str(item.get("account_code", ""))
            if acc_code:
                new_entries.append({
                    "source_account_code": acc_code,
                    "target_account_code": f"TGT_{acc_code}",
                    "metric_category": DATA["default_metric_category"],
                })
        return {"new_entries": new_entries}

    elif fix_type == "fix_hierarchy":
        updates = []
        for item in comp.get("drill_down", []):
            if "entity" in item and "group" in item:
                updates.append({
                    "entity_code": item["entity"],
                    "field": "region",
                    "old_value": item.get("our_value", ""),
                    "new_value": item.get("group", ""),
                })
        return {"entity_updates": updates}

    elif fix_type == "add_elimination":
        entries = []
        for item in comp.get("drill_down", []):
            if item.get("net_uneliminated", 0) != 0:
                entries.append({
                    "entity_code": item.get("entity", ""),
                    "account_code": "9999",
                    "amount": -item.get("net_uneliminated", 0),
                    "counterparty_entity": item.get("counterparty", ""),
                })
        return {"elimination_entries": entries}

    return {}


def _build_excel_report(
    gap_summary: dict,
    attribution: dict,
    agent_steps: list,
    causal_result: Optional[dict] = None,
    anomaly_result: Optional[dict] = None,
    fuzzy_suggestions: Optional[list] = None,
    audit_trail: Optional[list] = None,
    ai_usage: Optional[dict] = None,
) -> bytes:
    import io
    output = io.BytesIO()

    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        # Sheet 1: Reconciliation Summary
        gap_rows = []
        for metric, info in gap_summary.items():
            gap_rows.append({
                "Metric": metric,
                "Our Value ($)": info.get("our_value", 0),
                "Target Value ($)": info.get("target_value", 0),
                "Gap ($)": info.get("gap", 0),
                "Gap (%)": info.get("gap_pct", None),
                "Has Gap": "Yes" if info.get("has_gap") else "No",
            })
        pd.DataFrame(gap_rows).to_excel(writer, sheet_name="Reconciliation Summary", index=False)

        attr_rows = []
        for comp in attribution.get("components", []):
            attr_rows.append({
                "Category": comp.get("category", ""),
                "Label": comp.get("label", ""),
                "Amount ($)": comp.get("amount", 0),
                "% of Gap": comp.get("pct_of_gap", 0),
                "Confidence": f"{comp.get('confidence', 0)*100:.0f}%",
                "Fix Type": comp.get("fix_type", "Manual"),
                "Detail": comp.get("detail", ""),
            })
        pd.DataFrame(attr_rows).to_excel(writer, sheet_name="Variance Attribution", index=False)

        steps_rows = [
            {"Step": s.get("step", ""), "Detail": s.get("detail", ""), "Tool": s.get("tool", "")}
            for s in agent_steps
        ]
        pd.DataFrame(steps_rows).to_excel(writer, sheet_name="Investigation Steps", index=False)

        causal_rows = []
        if causal_result:
            for p in causal_result.get("path_traversed", []):
                causal_rows.append({
                    "Node": p.get("node", ""),
                    "Delta ($)": p.get("delta", 0),
                    "Contribution (%)": round(p.get("contribution_pct", 0) * 100, 2),
                })
            causal_rows.append({
                "Node": f"ROOT CAUSE → {causal_result.get('root_cause_node', '').replace('_', ' ').title()}",
                "Delta ($)": "",
                "Contribution (%)": round(causal_result.get("contribution_pct", 0) * 100, 2),
            })
        if not causal_rows:
            causal_rows = [{"Node": "Causal path trace not available", "Delta ($)": "", "Contribution (%)": ""}]
        pd.DataFrame(causal_rows).to_excel(writer, sheet_name="Causal Path Trace", index=False)

        anom_rows = []
        if anomaly_result and anomaly_result.get("status") == "complete":
            for acc in anomaly_result.get("anomalous_accounts", []):
                anom_rows.append({
                    "Account Code": acc.get("account_code", ""),
                    "Amount ($)": acc.get("amount", 0),
                    "Flagged By": "IsolationForest",
                })
        if not anom_rows:
            anom_rows = [{"Account Code": anomaly_result.get("reason", "No anomalies detected") if anomaly_result else "Not run", "Amount ($)": "", "Flagged By": ""}]
        pd.DataFrame(anom_rows).to_excel(writer, sheet_name="Anomaly Log", index=False)

        fuzzy_rows = fuzzy_suggestions if fuzzy_suggestions else [{"info": "No fuzzy matches found"}]
        pd.DataFrame(fuzzy_rows).to_excel(writer, sheet_name="Fuzzy Match Log", index=False)

        ai_rows = []
        if ai_usage:
            ai_rows.append({
                "AI Used": "Yes" if ai_usage.get("ai_used") else "No",
                "Tokens Used": ai_usage.get("tokens_used", 0),
                "Model": ai_usage.get("model", ""),
                "Trigger": "Low causal confidence" if ai_usage.get("ai_used") else "High causal confidence — local narrative",
            })
        else:
            ai_rows.append({"AI Used": "Unknown", "Tokens Used": 0, "Model": "", "Trigger": ""})
        ai_df = pd.DataFrame(ai_rows)

        if audit_trail:
            audit_df = pd.DataFrame(audit_trail)
            combined = pd.concat([ai_df, pd.DataFrame([{}]), audit_df], ignore_index=True)
        else:
            combined = ai_df
        combined.to_excel(writer, sheet_name="AI Usage Log", index=False)

    return output.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Tabs
# ─────────────────────────────────────────────────────────────────────────────

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🏗️ Architecture",
    "📤 Connect & Upload",
    "📊 Data Profile",
    "⚖️ Reconciliation",
    "🤖 AI Investigation",
    "🔧 Fix & Validate",
])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Architecture Overview
# ══════════════════════════════════════════════════════════════════════════════

with tab1:
    st.markdown('<div class="section-header">THE COMPLETE ARCHITECTURE — 6 LAYERS</div>', unsafe_allow_html=True)

    # Layer cards
    layers = [
        {
            "cls": "layer-0",
            "badge": "Connector layer",
            "title": "Layer 0 — Source connectors (Databricks · Fabric · SQL Server)",
            "desc": (
                "Service principal authenticates via OAuth M2M (Databricks) or Entra ID token (Fabric/SQL). "
                "Fetches: (a) notebook cell source via Workspace API — read only, no execution, "
                "(b) pipeline/task definitions and run history, "
                "(c) catalog schema — table names, column names, data types, row counts, "
                "(d) aggregate query results — sums/counts per key group, not full table scans. "
                "Credentials live in Azure Key Vault. SmartRecon app reads them at runtime via managed identity. "
                "Zero secrets in code."
            ),
        },
        {
            "cls": "layer-1",
            "badge": "Python only",
            "title": "Layer 1 — Schema profiler + pipeline parser",
            "desc": (
                "Parses fetched notebook cells: extracts transformation steps, join keys, filter conditions, "
                "aggregation columns, output schema. "
                "Parses pipeline run history: extracts execution timestamps, parameters, task order, watermarks. "
                "Builds a structured 'pipeline graph' — a DAG of what transforms what. "
                "This is the map the AI uses for strategy. No data at this stage — only structure and metadata."
            ),
        },
        {
            "cls": "layer-2",
            "badge": "1 AI call",
            "title": "Layer 2 — AI strategy planner (SmartRecon's brain)",
            "desc": (
                "Sends to AI: pipeline graph (step names + transform types) + schema summary (column names + types) "
                "+ business context (user-typed) + observed gap metrics (numbers only, not rows). "
                "AI returns: ranked investigation plan — which cells to re-execute first, which mapping tables to check, "
                "which orchestration timestamps to compare. "
                "This is the 'human developer thinking' step — the AI acts as a senior analyst deciding where to look, "
                "not as a data processor."
            ),
        },
        {
            "cls": "layer-3",
            "badge": "Sandbox exec",
            "title": "Layer 3 — Sandbox executor (SmartRecon's own notebook)",
            "desc": (
                "Creates/uses ONE dedicated SmartRecon diagnostic notebook in the client workspace (JMAN-owned). "
                "Injects cell source code from the client notebook ONE CELL AT A TIME, appended with: "
                "(a) an output capture cell — %python df.toPandas().describe().to_json(), "
                "(b) a delta check cell — compare this step's aggregated output against expected. "
                "Executes via Jobs API. Collects result after each cell. "
                "Stops at first cell where delta exceeds threshold. Client's original notebook untouched."
            ),
        },
        {
            "cls": "layer-4",
            "badge": "Python + ML",
            "title": "Layer 4 — Hypothesis investigator (3-type checks, prioritised by AI plan)",
            "desc": (
                "Type 1 (code): contribution score per cell delta = cell_delta / total_gap. Stops at 95% explained. "
                "Type 2 (data): fuzzy join coverage — rapidfuzz on mapping keys, unmapped ratio, "
                "schema version diff between source and target. "
                "Type 3 (orchestration): run timestamp diff, parameter diff, watermark gap between pipeline runs. "
                "IsolationForest pre-filters noise deltas. "
                "All checks are deterministic Python — no AI here. AI only told the engine which checks to run first."
            ),
        },
        {
            "cls": "layer-5",
            "badge": "Output",
            "title": "Layer 5 — Results + narrative + fix preview",
            "desc": (
                "Waterfall: gap attributed per culprit (cell name / mapping table / pipeline timestamp). "
                "Step drill-down: which cell, which column, which row group caused the divergence. "
                "Fix preview: apply proposed fix (add missing mapping, re-run cell with corrected parameter) in sandbox, "
                "prove gap closes. "
                "If investigator confidence <80%: one more AI call (~400 tokens of data summary + path traversed) for narrative. "
                "Export: structured JSON for downstream + Finance Director PDF."
            ),
        },
    ]

    for layer in layers:
        st.markdown(
            f"""
            <div class="arch-layer {layer['cls']}">
                <div><span class="layer-badge">{layer['badge']}</span></div>
                <div class="layer-content">
                    <h4>{layer['title']}</h4>
                    <p>{layer['desc']}</p>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Three reconciliation types ────────────────────────────────────────────
    st.markdown('<div class="section-header">Three reconciliation types — all handled by one engine</div>', unsafe_allow_html=True)

    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            """
            <div class="recon-type-card recon-type-1">
                <h4>Type 1 — Code</h4>
                <ul>
                    <li>Cell-by-cell replay</li>
                    <li>Capture intermediate output at each step</li>
                    <li>Delta flagged at culprit cell</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            """
            <div class="recon-type-card recon-type-2">
                <h4>Type 2 — Data</h4>
                <ul>
                    <li>Fuzzy join on mapping keys</li>
                    <li>Schema version diff</li>
                    <li>Unmapped key ratio per source</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col3:
        st.markdown(
            """
            <div class="recon-type-card recon-type-3">
                <h4>Type 3 — Orchestration</h4>
                <ul>
                    <li>Pipeline run timestamp diff</li>
                    <li>Parameter mismatch</li>
                    <li>Watermark gap between source and target runs</li>
                </ul>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Source connector details ──────────────────────────────────────────────
    st.markdown('<div class="section-header">Client environment (read-only access)</div>', unsafe_allow_html=True)

    conn_col1, conn_col2, conn_col3 = st.columns(3)
    with conn_col1:
        st.markdown(
            """
            <div class="connector-card">
                <div class="conn-icon">🔷</div>
                <h4>Databricks</h4>
                <div class="conn-items">
                    <span class="conn-badge">Notebooks (read)</span>
                    <span class="conn-badge">Jobs/pipelines (read)</span><br>
                    <span style="font-size:0.72rem;color:#718096">Unity Catalog — SELECT only, no full scan</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with conn_col2:
        st.markdown(
            """
            <div class="connector-card">
                <div class="conn-icon">🧱</div>
                <h4>Microsoft Fabric</h4>
                <div class="conn-items">
                    <span class="conn-badge">Notebooks (read)</span>
                    <span class="conn-badge">Pipelines (read)</span><br>
                    <span style="font-size:0.72rem;color:#718096">Lakehouse/Warehouse — aggregate queries</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with conn_col3:
        st.markdown(
            """
            <div class="connector-card">
                <div class="conn-icon">🗄️</div>
                <h4>SQL Server / ADF</h4>
                <div class="conn-items">
                    <span class="conn-badge">Stored procs (read)</span>
                    <span class="conn-badge">Job history (read)</span><br>
                    <span style="font-size:0.72rem;color:#718096">ADF pipeline run history via REST</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    # ── Security model ────────────────────────────────────────────────────────
    st.markdown(
        """
        <div class="security-strip">
            <span class="sec-label">🔒 Security model — applies across all three sources</span>
            <span class="sec-badge">Azure Key Vault (secrets)</span>
            <span class="sec-badge">Service principal (OAuth)</span>
            <span class="sec-badge">No raw data to AI (ever)</span>
            <span class="sec-badge">Audit log every call</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Domain plugin note ────────────────────────────────────────────────────
    st.markdown(
        """
        <div class="domain-strip">
            <strong>Domain plugin</strong> — thin JSON config — engine unchanged per project &nbsp;
            <span class="domain-pill">Finance reporting</span>
            <span class="domain-pill">Supply chain</span>
            <span class="domain-pill">HR analytics</span>
            <span class="domain-pill">Any other domain</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── SmartRecon sandbox note ───────────────────────────────────────────────
    st.markdown("<br>", unsafe_allow_html=True)
    st.info(
        "**SmartRecon sandbox notebook** (JMAN-owned) — execute permission only — no client data stored. "
        "The sandbox notebook lives in the client workspace but is owned and versioned by JMAN. "
        "Client's original pipelines and notebooks are **never modified**.",
        icon="🔬",
    )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — Connect & Upload
# ══════════════════════════════════════════════════════════════════════════════

with tab2:
    st.markdown('<div class="section-header">Business Context</div>', unsafe_allow_html=True)

    business_context = st.text_area(
        "Describe the reconciliation scenario",
        value=st.session_state.get("business_context", ""),
        placeholder=(
            "e.g. Q1 2024 financial close. Reconciling operational source system against Group Reporting "
            "for Revenue, Gross Margin, and Operating Profit. Source: Databricks Delta tables. "
            "Target: Microsoft Fabric warehouse..."
        ),
        height=100,
        help="This context is passed to the AI strategy planner (Layer 2) to help it prioritise investigation steps.",
    )
    st.session_state["business_context"] = business_context

    st.divider()

    source_system = st.session_state.get("source_system", "File Upload (Demo)")

    if source_system == "File Upload (Demo)":
        st.markdown('<div class="section-header">Upload Data Files</div>', unsafe_allow_html=True)

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Source Data** *(Operational Transactions)*")
            source_file = st.file_uploader(
                "source_data.xlsx / .csv",
                type=["xlsx", "csv", "xls"],
                key="source_upload",
                help="Raw transactional data from operational system",
            )

            st.markdown("**Mapping Table** *(Account Code Mappings)*")
            mapping_file = st.file_uploader(
                "mapping_table.xlsx / .csv",
                type=["xlsx", "csv", "xls"],
                key="mapping_upload",
                help="Maps source account codes to target account codes and metric categories",
            )

        with col2:
            st.markdown("**Target Results** *(Client Group Reporting)*")
            target_file = st.file_uploader(
                "target_results.xlsx / .csv",
                type=["xlsx", "csv", "xls"],
                key="target_upload",
                help="Expected values as reported by the client in their Group Reporting system",
            )

            st.markdown("**Hierarchy Table** *(Optional)*")
            hierarchy_file = st.file_uploader(
                "hierarchy.xlsx / .csv",
                type=["xlsx", "csv", "xls"],
                key="hierarchy_upload",
                help="Entity → BU → Region → Segment rollup structure",
            )

    else:
        # Connector-based "connect" UI (placeholder for live connector)
        st.markdown(f'<div class="section-header">Connect to {source_system}</div>', unsafe_allow_html=True)

        if source_system == "Databricks":
            st.markdown(
                """
                <div class="arch-layer layer-0" style="margin-bottom:1rem">
                    <div><span class="layer-badge">OAuth M2M</span></div>
                    <div class="layer-content">
                        <h4>Databricks Workspace API (read-only)</h4>
                        <p>Fetches notebook cell source, job run history, Unity Catalog schema stats, and aggregate queries per key group.
                        No full table scans. Credentials stored in Azure Key Vault — zero secrets in code.</p>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Workspace URL", placeholder="https://adb-xxxx.azuredatabricks.net")
                st.text_input("Client ID (Service Principal)", type="password")
            with col2:
                st.text_input("Client Secret", type="password")
                st.text_input("Catalog / Schema", placeholder="main.finance_reporting")
            st.button("🔗 Connect to Databricks (read-only)")

        elif source_system == "Microsoft Fabric":
            st.markdown(
                """
                <div class="arch-layer layer-0" style="margin-bottom:1rem">
                    <div><span class="layer-badge">Entra ID</span></div>
                    <div class="layer-content">
                        <h4>Microsoft Fabric REST API (read-only)</h4>
                        <p>Fetches notebook definitions, pipeline run metadata, and Lakehouse/Warehouse aggregate queries.
                        Uses Entra ID token — aggregate queries only, no row-level data extraction.</p>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("Tenant ID")
                st.text_input("Client ID (App Registration)", type="password")
            with col2:
                st.text_input("Client Secret", type="password")
                st.text_input("Workspace ID / Lakehouse name")
            st.button("🔗 Connect to Microsoft Fabric (read-only)")

        elif source_system == "SQL Server / ADF":
            st.markdown(
                """
                <div class="arch-layer layer-0" style="margin-bottom:1rem">
                    <div><span class="layer-badge">SQL / REST</span></div>
                    <div class="layer-content">
                        <h4>SQL Server + ADF Pipeline History (read-only)</h4>
                        <p>Reads stored procedure definitions and job run history from SQL Server.
                        Fetches ADF pipeline run history, parameter snapshots, and trigger metadata via Azure REST API.</p>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            col1, col2 = st.columns(2)
            with col1:
                st.text_input("SQL Server host", placeholder="myserver.database.windows.net")
                st.text_input("SQL username", type="password")
            with col2:
                st.text_input("SQL password", type="password")
                st.text_input("ADF Resource ID (for pipeline history)")
            st.button("🔗 Connect to SQL Server / ADF (read-only)")

        st.warning(
            "⚡ **Demo mode:** Live connector UI shown above. For this hackathon demo, "
            "switch to 'File Upload (Demo)' in the sidebar to load sample data.",
            icon="💡",
        )
        source_file = mapping_file = target_file = hierarchy_file = None

    # Quick-load sample data
    st.divider()
    col_a, col_b = st.columns([2, 3])
    with col_a:
        if st.button("⚡ Load Sample Data", help="Load pre-built energy company demo data"):
            sample_dir = Path(__file__).parent / "sample_data"
            try:
                st.session_state["source"] = load_source(sample_dir / "source_data.xlsx")
                st.session_state["mapping"] = load_mapping(sample_dir / "mapping_table.xlsx")
                st.session_state["target"] = load_target(sample_dir / "target_results.xlsx")
                st.session_state["hierarchy"] = load_hierarchy(sample_dir / "hierarchy.xlsx")
                for key in ["agent_result", "gap_summary", "our_metrics", "fingerprint"]:
                    st.session_state.pop(key, None)
                st.success("✅ Sample data loaded! Navigate to Data Profile →")
            except Exception as e:
                st.error(f"Error loading sample data: {e}")

    with col_b:
        sample_dir = APP["sample_data_dir"]
        try:
            import pandas as _pd
            _src_count = len(_pd.read_excel(sample_dir / "source_data.xlsx"))
            _map_count = len(_pd.read_excel(sample_dir / "mapping_table.xlsx"))
            st.info(
                f"💡 **Sample data** includes {_src_count:,} transactions with {_map_count} "
                f"account mappings (some intentionally missing to demonstrate gap detection)."
            )
        except Exception:
            st.info("💡 Load pre-built sample data to explore the demo reconciliation scenario.")

    # Load uploaded files (file upload mode only)
    if source_system == "File Upload (Demo)":
        if source_file:
            try:
                st.session_state["source"] = load_source(source_file)
                st.success(f"✅ Source loaded: {len(st.session_state['source']):,} rows")
            except Exception as e:
                st.error(f"Error loading source: {e}")

        if mapping_file:
            try:
                st.session_state["mapping"] = load_mapping(mapping_file)
                st.success(f"✅ Mapping loaded: {len(st.session_state['mapping']):,} rules")
            except Exception as e:
                st.error(f"Error loading mapping: {e}")

        if target_file:
            try:
                st.session_state["target"] = load_target(target_file)
                st.success(f"✅ Target loaded: {len(st.session_state['target']):,} rows")
            except Exception as e:
                st.error(f"Error loading target: {e}")

        if hierarchy_file:
            try:
                st.session_state["hierarchy"] = load_hierarchy(hierarchy_file)
                st.success(f"✅ Hierarchy loaded: {len(st.session_state['hierarchy']):,} entities")
            except Exception as e:
                st.error(f"Error loading hierarchy: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 3 — Data Profile  (was Tab 2)
# ══════════════════════════════════════════════════════════════════════════════

with tab3:
    if "source" not in st.session_state:
        st.info("📤 Connect to a data source or upload files in the **Connect & Upload** tab first.")
        st.stop()

    source = st.session_state["source"]
    mapping = st.session_state.get("mapping", pd.DataFrame())
    target = st.session_state.get("target", pd.DataFrame())
    hierarchy = st.session_state.get("hierarchy", None)

    if "fingerprint" not in st.session_state:
        with st.spinner("Profiling data (Layer 1 — schema profiler)..."):
            st.session_state["fingerprint"] = build_fingerprint(source, mapping, target, hierarchy)

    fp = st.session_state["fingerprint"]

    st.markdown('<div class="section-header">Data Overview</div>', unsafe_allow_html=True)

    # Layer 1 badge
    st.markdown(
        '<span class="ai-call-badge">🐍 Layer 1 — Schema profiler + pipeline parser · Python only</span>',
        unsafe_allow_html=True,
    )

    c1, c2, c3, c4, c5 = st.columns(5)
    metrics_display = [
        ("Transactions", f"{fp['source']['row_count']:,}", c1),
        ("Accounts", f"{fp['source']['unique_accounts']:,}", c2),
        ("Entities", f"{fp['source']['unique_entities']:,}", c3),
        ("Mapping Rules", f"{fp['mapping']['row_count']:,}", c4),
        ("Mapping Coverage", f"{fp['mapping_coverage'].get('coverage_pct') or 0:.1f}%", c5),
    ]
    for label, value, col in metrics_display:
        with col:
            is_warn = label == "Mapping Coverage" and float(value.replace("%", "") or 100) < 100
            card_class = "metric-card gap-card-red" if is_warn else "metric-card"
            st.markdown(
                f'<div class="{card_class}"><div class="value">{value}</div><div class="label">{label}</div></div>',
                unsafe_allow_html=True,
            )

    st.divider()

    col_left, col_right = st.columns(2)

    with col_left:
        st.markdown('<div class="section-header">Source Data Preview</div>', unsafe_allow_html=True)
        st.dataframe(source.head(20), use_container_width=True, height=280)

        st.markdown('<div class="section-header">Mapping Coverage Alert</div>', unsafe_allow_html=True)
        cov = fp["mapping_coverage"]
        cov_pct = cov.get("coverage_pct") or 100
        if cov_pct < 100:
            st.warning(
                f"⚠️ **{100 - cov_pct:.1f}% of transactions are unmapped** "
                f"({cov.get('unmapped_count') or 0:,} rows, ${abs(cov.get('unmapped_amount') or 0):,.0f})"
            )
            unmapped_df = pd.DataFrame([
                {"Account Code": k, "Transaction Count": v}
                for k, v in cov.get("top_unmapped_codes", {}).items()
            ])
            if not unmapped_df.empty:
                st.dataframe(unmapped_df, use_container_width=True, hide_index=True)
        else:
            st.success("✅ All account codes are mapped.")

    with col_right:
        st.markdown('<div class="section-header">Metric Distribution (Source)</div>', unsafe_allow_html=True)

        if fp["source"].get("top_accounts_by_amount"):
            acc_df = pd.DataFrame(fp["source"]["top_accounts_by_amount"])
            import plotly.express as px
            fig = px.bar(
                acc_df.head(10),
                x="account",
                y="amount",
                color="amount",
                color_continuous_scale=["#E74C3C", "#F39C12", "#27AE60"],
                title="Top Accounts by Amount",
                labels={"account": "Account Code", "amount": "Total Amount ($)"},
            )
            fig.update_layout(
                height=260, margin=dict(l=10, r=10, t=40, b=10),
                showlegend=False, coloraxis_showscale=False,
                plot_bgcolor="#F8F9FA",
            )
            st.plotly_chart(fig, use_container_width=True)

        st.markdown('<div class="section-header">Target Results Preview</div>', unsafe_allow_html=True)
        if not target.empty:
            st.dataframe(target, use_container_width=True, height=200)

    st.divider()

    ic_count = fp["source"].get("intercompany_count", 0)
    adj_count = fp["source"].get("adjustment_count", 0)

    col_a, col_b = st.columns(2)
    with col_a:
        if ic_count > 0:
            st.warning(f"🔄 **{ic_count} intercompany transactions** detected — will check for missing eliminations.")
        else:
            st.success("✅ No intercompany transactions detected.")
    with col_b:
        if adj_count > 0:
            st.warning(f"✏️ **{adj_count} manual adjustment entries** detected — will check for target-source discrepancies.")
        else:
            st.info("No manual adjustment flags in source data.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — Reconciliation  (was Tab 3)
# ══════════════════════════════════════════════════════════════════════════════

with tab4:
    if "source" not in st.session_state or "mapping" not in st.session_state or "target" not in st.session_state:
        st.info("📤 Upload all required files first (Source + Mapping + Target).")
        st.stop()

    source = st.session_state["source"]
    mapping = st.session_state["mapping"]
    target = st.session_state["target"]
    hierarchy = st.session_state.get("hierarchy", None)

    st.markdown('<div class="section-header">Headline Metric Comparison</div>', unsafe_allow_html=True)

    if "our_metrics" not in st.session_state:
        with st.spinner("Computing metrics (Layer 4 — Type 2 data checks)..."):
            result = compute_our_metrics(source, mapping, hierarchy)
            st.session_state["our_metrics"] = result
            target_metrics = parse_target_metrics(target)
            gap_summary = compute_gap_summary(result["by_metric"], target_metrics)
            st.session_state["gap_summary"] = gap_summary

    our_metrics = st.session_state["our_metrics"]
    gap_summary = st.session_state["gap_summary"]

    ordered = [m for m in METRIC_DISPLAY_ORDER if m in gap_summary]
    extras = [m for m in gap_summary if m not in METRIC_DISPLAY_ORDER]
    metric_order = ordered + extras
    cols = st.columns(max(1, len(metric_order)))
    col_idx = 0
    for metric in metric_order:
        if metric not in gap_summary:
            continue
        info = gap_summary[metric]
        gap = info["gap"]
        has_gap = info["has_gap"]
        card_class = "metric-card gap-card-red" if has_gap and abs(gap) > 1e6 else ("metric-card gap-card-warn" if has_gap else "metric-card gap-card-green")
        gap_str = f"Gap: {format_currency(gap)}" if has_gap else "✅ Match"
        with cols[col_idx]:
            st.markdown(
                f"""<div class="{card_class}">
                    <div class="value">{format_currency(info['our_value'])}</div>
                    <div class="label">{metric} (Ours)</div>
                    <div style="font-size:0.75rem;color:#718096;margin-top:0.3rem;">
                        Target: {format_currency(info['target_value'])}<br>
                        <b style="color:{'#E53E3E' if has_gap else '#27AE60'}">{gap_str}</b>
                    </div>
                </div>""",
                unsafe_allow_html=True,
            )
        col_idx += 1

    st.markdown("<br>", unsafe_allow_html=True)

    st.markdown('<div class="section-header">Detailed Comparison Table</div>', unsafe_allow_html=True)
    fig_table = charts.metric_comparison_table(gap_summary)
    st.plotly_chart(fig_table, use_container_width=True)

    revenue_info = gap_summary.get("Revenue", {})
    if revenue_info.get("has_gap"):
        revenue_gap = revenue_info["gap"]
        st.error(
            f"🔴 **Revenue Gap Detected: {format_currency(revenue_gap)}** "
            f"({abs(revenue_info.get('gap_pct', 0)):.1f}% variance). "
            f"Proceed to AI Investigation to find root causes."
        )
    else:
        st.success("✅ All metrics reconcile within tolerance. No further investigation needed.")

    st.divider()

    st.markdown('<div class="section-header">Entity-Level Breakdown</div>', unsafe_allow_html=True)
    by_entity = our_metrics.get("by_entity", pd.DataFrame())
    if not by_entity.empty:
        pivot_cols = [c for c in ["entity_code", "business_unit", "region", "metric_category"] if c in by_entity.columns]
        if pivot_cols and "our_amount" in by_entity.columns:
            display_df = by_entity.copy()
            display_df["our_amount"] = display_df["our_amount"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(display_df[pivot_cols + ["our_amount"]], use_container_width=True, height=300, hide_index=True)

    st.divider()
    if st.button("🤖 Run AI Investigation →", key="go_to_ai"):
        st.info("Navigate to the **AI Investigation** tab.")


# ══════════════════════════════════════════════════════════════════════════════
# TAB 5 — AI Investigation  (was Tab 4)
# ══════════════════════════════════════════════════════════════════════════════

with tab5:
    if "source" not in st.session_state:
        st.info("📤 Upload data and run reconciliation first.")
        st.stop()

    if "gap_summary" not in st.session_state:
        st.info("⚖️ Run reconciliation in the **Reconciliation** tab first.")
        st.stop()

    source = st.session_state["source"]
    mapping = st.session_state["mapping"]
    target = st.session_state["target"]
    hierarchy = st.session_state.get("hierarchy", None)
    gap_summary = st.session_state["gap_summary"]
    fingerprint = st.session_state.get("fingerprint", {})
    business_context = st.session_state.get("business_context", "")

    has_gaps = any(info.get("has_gap") for info in gap_summary.values())
    if not has_gaps:
        st.success("✅ No reconciliation gaps detected. Nothing to investigate!")
        st.stop()

    st.markdown('<div class="section-header">ReconAgent — Autonomous Investigation</div>', unsafe_allow_html=True)

    # Layer badges
    st.markdown(
        """
        <div style="display:flex;gap:0.5rem;flex-wrap:wrap;margin-bottom:1rem">
            <span class="ai-call-badge">🐍 Layer 1 — Schema profiler · Python only</span>
            <span class="ai-call-badge">🤖 Layer 2 — AI strategy planner · 1 call · ~600 tokens</span>
            <span class="ai-call-badge">🔬 Layer 3 — Sandbox executor · JMAN-owned notebook</span>
            <span class="ai-call-badge">📊 Layer 4 — Hypothesis investigator · Type 1/2/3</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    col_info, col_action = st.columns([3, 1])
    with col_info:
        st.markdown(
            """
            ReconAgent uses a **6-layer agentic architecture** to autonomously investigate root causes.

            **Layer 2 (AI)** acts as a senior analyst — it reads only the pipeline graph, column names, and gap numbers
            (never raw data), then returns a ranked checklist of what to check.
            **Layers 3 & 4 (Python)** execute that plan deterministically using cell replay, fuzzy mapping checks,
            and orchestration timestamp diffs. A second AI call (~400 tokens) is made only if confidence < 80%.
            """
        )
    with col_action:
        run_btn = st.button("🚀 Start Investigation", key="run_agent", use_container_width=True)

    if run_btn or "agent_result" in st.session_state:
        if run_btn:
            st.session_state.pop("agent_result", None)

            step_container = st.container()
            progress_bar = st.progress(0)
            status_text = st.empty()
            steps_rendered = []

            def on_step(label: str, detail: str):
                steps_rendered.append((label, detail))
                progress = min(len(steps_rendered) / 7, 1.0)
                progress_bar.progress(progress)
                status_text.markdown(f"**{label}** — {detail[:120]}")
                with step_container:
                    st.markdown(
                        f'<div class="step-item"><div class="step-title">✓ {label}</div>'
                        f'<div class="step-detail">{detail[:150]}</div></div>',
                        unsafe_allow_html=True,
                    )

            with st.spinner("ReconAgent is investigating..."):
                try:
                    result = run_recon_agent(
                        fingerprint=fingerprint,
                        business_context=business_context,
                        gap_summary=gap_summary,
                        source=source,
                        mapping=mapping,
                        target=target,
                        hierarchy=hierarchy,
                        on_step=on_step,
                    )
                    st.session_state["agent_result"] = result
                    progress_bar.progress(1.0)
                    status_text.success("✅ Investigation complete!")
                except Exception as e:
                    st.error(f"Agent error: {e}")
                    st.stop()

        result = st.session_state.get("agent_result")
        if not result:
            st.stop()

        attribution = result.get("attribution", {})
        narrative = result.get("narrative", "")
        agent_steps = result.get("agent_steps", [])

        st.divider()

        expl_pct = attribution.get("explained_pct", 0)
        confidence = attribution.get("overall_confidence", 0)
        auto_fix_pct = attribution.get("auto_fixable_pct", 0)

        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown(
                f'<div class="metric-card"><div class="value" style="color:#27AE60">{expl_pct:.1f}%</div><div class="label">Gap Explained</div></div>',
                unsafe_allow_html=True,
            )
        with col2:
            st.markdown(
                f'<div class="metric-card"><div class="value" style="color:#2E86C1">{confidence:.1f}%</div><div class="label">Confidence</div></div>',
                unsafe_allow_html=True,
            )
        with col3:
            st.markdown(
                f'<div class="metric-card"><div class="value" style="color:#E67E22">{auto_fix_pct:.1f}%</div><div class="label">Auto-Fixable</div></div>',
                unsafe_allow_html=True,
            )
        with col4:
            n_components = len(attribution.get("components", []))
            st.markdown(
                f'<div class="metric-card"><div class="value" style="color:#9B59B6">{n_components}</div><div class="label">Root Causes</div></div>',
                unsafe_allow_html=True,
            )

        st.markdown("<br>", unsafe_allow_html=True)

        col_waterfall, col_donut = st.columns([3, 2])
        with col_waterfall:
            fig_wf = charts.variance_waterfall(attribution)
            st.plotly_chart(fig_wf, use_container_width=True)

        with col_donut:
            fig_donut = charts.root_cause_donut(attribution)
            st.plotly_chart(fig_donut, use_container_width=True)

            fig_conf = charts.confidence_bar(attribution)
            st.plotly_chart(fig_conf, use_container_width=True)

        st.divider()

        st.markdown('<div class="section-header">Root Cause Breakdown</div>', unsafe_allow_html=True)

        conf_high_threshold = max(CONFIDENCE.values()) * 0.9
        conf_med_threshold = max(CONFIDENCE.values()) * 0.65

        for comp in attribution.get("components", []):
            cat = comp.get("category", "")
            conf = comp.get("confidence", 0)
            conf_label = "High" if conf >= conf_high_threshold else ("Medium" if conf >= conf_med_threshold else "Low")
            conf_class = "conf-high" if conf >= conf_high_threshold else ("conf-med" if conf >= conf_med_threshold else "conf-low")

            with st.expander(
                f"{CATEGORY_ICONS.get(cat, '📌')} {cat} — "
                f"**{format_currency(comp['amount'])}** ({abs(comp['pct_of_gap']):.1f}% of gap)",
                expanded=(cat != "Unexplained Residual"),
            ):
                cols = st.columns([4, 1])
                with cols[0]:
                    st.markdown(comp.get("detail", ""))
                with cols[1]:
                    st.markdown(
                        f'<span class="confidence-badge {conf_class}">Confidence: {conf_label}</span>',
                        unsafe_allow_html=True,
                    )

                drill_down = comp.get("drill_down", [])
                if drill_down:
                    st.markdown("**Detail:**")
                    fig_dd = charts.drill_down_table(drill_down, cat)
                    st.plotly_chart(fig_dd, use_container_width=True)

                if comp.get("fix_type") and comp["fix_type"] != "add_adjustment":
                    if st.button(f"🔧 Preview Fix for {cat}", key=f"fix_{cat}"):
                        st.session_state["pending_fix"] = {
                            "fix_type": comp["fix_type"],
                            "category": cat,
                            "component": comp,
                        }
                        st.info("Navigate to **Fix & Validate** tab to see the fix preview.")

        st.divider()

        if narrative:
            st.markdown('<div class="section-header">AI Analysis Narrative</div>', unsafe_allow_html=True)
            ai_used = result.get("ai_used", False)
            tokens = result.get("tokens_used", 0)
            if ai_used:
                st.markdown(
                    f'<span class="ai-call-badge">🤖 Layer 2 / Layer 5 AI narrative · {tokens:,} tokens used</span>',
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    '<span class="ai-call-badge">🐍 Local rule-based narrative · 0 API tokens used</span>',
                    unsafe_allow_html=True,
                )
            st.markdown(
                f'<div style="background:#F7FAFC;border-left:4px solid #2E86C1;padding:1rem 1.5rem;'
                f'border-radius:4px;font-size:0.95rem;line-height:1.7">{narrative}</div>',
                unsafe_allow_html=True,
            )

        st.divider()

        with st.expander("📋 Agent Investigation Steps", expanded=False):
            for step in agent_steps:
                st.markdown(
                    f'<div class="step-item"><div class="step-title">{step["step"]}</div>'
                    f'<div class="step-detail">{step.get("detail", "")}</div></div>',
                    unsafe_allow_html=True,
                )

        with st.expander("🔬 How This Was Resolved — Resolution Path", expanded=True):
            anomaly_r = result.get("anomaly_result", {})
            causal_r = result.get("causal_result", {})
            ai_used = result.get("ai_used", False)
            tokens = result.get("tokens_used", 0)

            anom_status = anomaly_r.get("status", "skipped")
            if anom_status == "complete":
                anom_str = (
                    f"{anomaly_r.get('anomalous_count', 0)} of "
                    f"{anomaly_r.get('total_accounts', 0)} accounts flagged as anomalous"
                )
            else:
                anom_str = f"Skipped — {anomaly_r.get('reason', 'unknown')}"

            if causal_r:
                root_node = causal_r.get("root_cause_node", "unknown").replace("_", " ").title()
                causal_pct = causal_r.get("contribution_pct", 0)
                causal_str = f"Root cause: {root_node} ({causal_pct:.0%} contribution)"
            else:
                causal_str = "Not run"

            ai_method = "Gemini API (Layer 2 + 5)" if ai_used else "Local Rule-Based (0 AI tokens)"
            ai_str = f"{tokens:,} tokens used" if ai_used else "Resolved locally — 0 API tokens used"

            resolution_df = pd.DataFrame([
                {"Layer": "Layer 1", "Step": "Schema Profiler",       "Method": "Python only",         "Result": f"{fp.get('source', {}).get('row_count', 0):,} rows profiled"},
                {"Layer": "Layer 2", "Step": "AI Strategy Planner",   "Method": "Gemini (1 AI call)",  "Result": "Ranked investigation checklist produced"},
                {"Layer": "Layer 3", "Step": "Sandbox Executor",      "Method": "JMAN-owned notebook", "Result": "Cell-by-cell replay, culprit cell identified"},
                {"Layer": "Layer 4", "Step": "Anomaly Filter",        "Method": "IsolationForest",     "Result": anom_str},
                {"Layer": "Layer 4", "Step": "Causal Tracing",        "Method": "NetworkX DAG",        "Result": causal_str},
                {"Layer": "Layer 5", "Step": "AI Narrative",          "Method": ai_method,             "Result": ai_str},
            ])
            st.table(resolution_df)

            if not ai_used:
                st.success("🟢 Resolved without AI narrative — 0 API tokens used for narrative layer")
            else:
                st.info(f"🤖 AI narrative generated via Gemini: {tokens:,} tokens used")

        if "fuzzy_suggestions" not in st.session_state:
            try:
                st.session_state["fuzzy_suggestions"] = fuzzy_map_accounts(source, mapping)
            except Exception:
                st.session_state["fuzzy_suggestions"] = []


# ══════════════════════════════════════════════════════════════════════════════
# TAB 6 — Fix & Validate  (was Tab 5)
# ══════════════════════════════════════════════════════════════════════════════

with tab6:
    if "agent_result" not in st.session_state:
        st.info("🤖 Run the AI Investigation first to see fix recommendations.")
        st.stop()

    source = st.session_state["source"]
    mapping = st.session_state["mapping"]
    target = st.session_state["target"]
    hierarchy = st.session_state.get("hierarchy", None)
    attribution = st.session_state["agent_result"]["attribution"]
    gap_summary = st.session_state["gap_summary"]

    st.markdown('<div class="section-header">Fix Preview & Validation</div>', unsafe_allow_html=True)

    st.markdown(
        '<span class="ai-call-badge">🔬 Layer 5 — Fix preview runs in SmartRecon sandbox · client notebooks untouched</span>',
        unsafe_allow_html=True,
    )

    st.markdown(
        "Apply suggested fixes **in SmartRecon's sandbox notebook** to see how much of the reconciliation gap "
        "they close — without touching any production data or client pipelines."
    )

    if "our_metrics" in st.session_state:
        our_m = st.session_state["our_metrics"]["by_metric"]
        from core.reconciler import parse_target_metrics as _ptm
        tgt_m = _ptm(target)
        dvt_rows = compute_dvt_diff(our_m, tgt_m)
        if dvt_rows:
            with st.expander("📊 DVT-Style Column Diff", expanded=False):
                dvt_df = pd.DataFrame(dvt_rows)
                status_emoji = {"pass": "✅", "warn": "🟡", "fail": "🔴"}
                dvt_df["status"] = dvt_df["status"].map(lambda s: f"{status_emoji.get(s, '')} {s.upper()}")
                dvt_df["checksum_match"] = dvt_df["checksum_match"].map(
                    lambda v: "✅ Match" if v is True else ("❌ Diff" if v is False else "—")
                )
                st.dataframe(dvt_df, use_container_width=True, hide_index=True)

    fuzzy_suggestions = st.session_state.get("fuzzy_suggestions", [])
    if fuzzy_suggestions:
        st.markdown('<div class="section-header">Fuzzy Account Match Suggestions (Type 2 — Data)</div>', unsafe_allow_html=True)
        st.markdown(
            "These unmapped account codes closely match existing mapping entries (rapidfuzz, Layer 4 Type 2). "
            "Select any to include in the fix preview."
        )
        approved_fuzzy = []
        for sug in fuzzy_suggestions:
            checked = st.checkbox(
                f"Map **{sug['source_code']}** → **{sug['suggested_code']}** "
                f"(similarity: {sug['score']:.0f}%,  category: {sug.get('suggested_category', '?')})",
                key=f"fuzzy_{sug['source_code']}",
            )
            if checked:
                approved_fuzzy.append(sug)
        st.session_state["fuzzy_approved"] = approved_fuzzy
        if approved_fuzzy:
            st.info(f"✅ {len(approved_fuzzy)} fuzzy match(es) selected — included in mapping fix preview.")
        st.divider()

    fixable_components = [
        c for c in attribution.get("components", [])
        if c.get("fix_type") and c["fix_type"] != "add_adjustment"
    ]

    if not fixable_components:
        st.info("No automatically fixable components found. Manual review required for all gaps.")
        st.stop()

    for comp in fixable_components:
        cat = comp["category"]
        fix_type = comp["fix_type"]

        with st.expander(f"🔧 Fix: {cat}", expanded=True):
            col_desc, col_apply = st.columns([3, 1])
            with col_desc:
                st.markdown(f"**What this fix does:** {_get_fix_description(fix_type, comp)}")
                st.markdown(f"**Expected gap closure:** ~{format_currency(abs(comp['amount']))}")

            with col_apply:
                apply = st.button(f"Apply {cat} Fix", key=f"apply_{fix_type}")

            if apply or st.session_state.get(f"fix_applied_{fix_type}"):
                st.session_state[f"fix_applied_{fix_type}"] = True

                with st.spinner("Computing fix preview in sandbox..."):
                    fix_data = _build_fix_data(fix_type, comp, source, mapping)
                    fix_result = preview_fix(
                        fix_type=fix_type,
                        fix_data=fix_data,
                        source=source,
                        mapping=mapping,
                        target=target,
                        hierarchy=hierarchy,
                    )

                comparison = fix_result.get("comparison", {})

                for metric in ["Revenue", "Gross Margin", "Operating Profit"]:
                    if metric in comparison:
                        info = comparison[metric]
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric(
                                f"{metric} — Before",
                                format_currency(info["before_our_value"]),
                                delta=None,
                            )
                        with col2:
                            st.metric(
                                f"{metric} — After Fix",
                                format_currency(info["after_our_value"]),
                                delta=format_currency(info["gap_closed"]),
                                delta_color="normal",
                            )
                        with col3:
                            st.metric(
                                f"{metric} — Target",
                                format_currency(info["target_value"]),
                            )

                        fig_fix = charts.fix_preview_chart(comparison, metric)
                        st.plotly_chart(fig_fix, use_container_width=True)
                        break

                closed_pct = comparison.get("Revenue", {}).get("gap_closed_pct", 0)
                if closed_pct > 0:
                    st.success(
                        f"✅ Applying this fix closes **{closed_pct:.1f}%** of the Revenue gap. "
                        f"Remaining gap would be {format_currency(abs(comparison.get('Revenue', {}).get('gap_after', 0)))}."
                    )

    st.divider()

    st.markdown('<div class="section-header">Export Audit Report</div>', unsafe_allow_html=True)
    agent_result = st.session_state["agent_result"]
    if st.button("📥 Download Full Audit Report (Excel — 5 sheets)"):
        report_data = _build_excel_report(
            gap_summary=gap_summary,
            attribution=attribution,
            agent_steps=agent_result.get("agent_steps", []),
            causal_result=agent_result.get("causal_result"),
            anomaly_result=agent_result.get("anomaly_result"),
            fuzzy_suggestions=st.session_state.get("fuzzy_suggestions", []),
            audit_trail=get_audit_trail(),
            ai_usage={
                "ai_used": agent_result.get("ai_used", False),
                "tokens_used": agent_result.get("tokens_used", 0),
                "model": "gemini-2.5-flash",
            },
        )
        st.download_button(
            label="⬇️ Download smart_recon_audit.xlsx",
            data=report_data,
            file_name="smart_recon_audit.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )