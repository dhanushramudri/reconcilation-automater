# Smart Recon — Automated Financial Reconciliation Engine

An agentic AI system that automatically investigates why your operational source data doesn't match client Group Reporting targets. It identifies root causes, attributes the variance, and previews fixes — all without exposing raw data to any external AI.

---

## How It Works (6-Layer Pipeline)

```
Files / Connectors
      │
  Layer 0 ── Source Connectors (metadata fetch only)
      │
  Layer 1 ── Schema Profiler → fingerprint (~5 KB)
      │
  Layer 2 ── AI Strategy Planner (1 Gemini call)
      │
  Layer 3 ── Sandbox Executor (cell-by-cell replay)
      │
  Layer 4 ── Type 1 / 2 / 3 Checks + IsolationForest
      │
  Layer 5 ── Variance Attribution + Confidence Gate + Narrative
```

### Layer 0 — Source Connectors

Read-only metadata connectors for three platforms. **Never fetches raw rows — only schema and aggregates.**

| Connector | Auth | What it fetches |
|-----------|------|-----------------|
| Databricks | OAuth M2M (Service Principal) | Notebook cell source, Unity Catalog schema, `SUM/COUNT` aggregates by account |
| Microsoft Fabric | Entra ID (MSAL client credentials) | Notebook definitions, pipeline run metadata, Lakehouse aggregates |
| SQL Server / ADF | SQL + Azure Management API | Stored procedure definitions, column schema, `SUM/COUNT` aggregates, pipeline run history |

All connectors return the same shape:
```python
{
    "notebook_cells":      [...],   # cell source + transform type
    "pipeline_run_history": [...],  # task name, status, parameters, watermark
    "catalog_schema_info":  {...},  # column names + types
    "aggregate_stats":      {...},  # {account_code, total_amount, row_count}
}
```

**Demo mode:** Upload Excel/CSV files directly and the app uses those instead.

---

### Layer 1 — Schema Profiler

Parses notebook cells and pipeline run history to extract transformation metadata, then builds a **compact fingerprint (~5 KB)** — the only thing that ever gets sent to AI.

```python
fingerprint = {
    "source":            { row_count, total_amount, unique_accounts, top_accounts_by_amount, null_rates, ... },
    "mapping":           { row_count, unique_source_accounts, metric_categories, ... },
    "target":            { row_count, total_target_amount, metric_breakdown, ... },
    "mapping_coverage":  { coverage_pct, unmapped_count, unmapped_amount, ... },
    "pipeline_graph":    { steps, edges, run_metadata, ... },
    "fingerprint_hash":  "md5..."
}
```

The pipeline graph is a DAG of transform steps (join → aggregate → filter → write) with edges showing data flow. No actual values, only structure.

---

### Layer 2 — AI Strategy Planner (1 Gemini call, ~600 tokens)

Sends the fingerprint + gap metrics to Gemini 2.5 Flash. The AI acts as a **senior financial analyst deciding where to look** — it does not process data.

**Prompt contains:**
- Pipeline step names and transform types (no data values)
- Column names and types (no data)
- Observed gap metrics (numbers only: `our_value`, `target_value`, `gap`, `gap_pct`)
- User-provided business context

**AI returns:**
```json
{
    "ranked_checks":          ["type1_mapping_gaps", "type1_ic_eliminations", "type2_fuzzy_coverage", ...],
    "focus_accounts":         ["1000", "2500"],
    "hypothesis":             "Mapping gaps in revenue accounts are the root cause",
    "secondary_hypothesis":   "IC pair EUR→GBP not fully offset"
}
```

The `ranked_checks` list determines the **order** in which Layer 4 runs its deterministic checks.

---

### Layer 3 — Sandbox Executor

Creates a dedicated diagnostic notebook in the client workspace (JMAN-owned). Injects client notebook cells **one at a time**, captures aggregated output at each step, and stops at the first cell where the delta exceeds a threshold. The original client notebook is never modified.

> **Status:** Architecture is defined; full implementation is in progress.

---

### Layer 4 — Hypothesis Investigator

Three families of deterministic Python checks run in the AI-ranked order from Layer 2. Stops once 95% of the gap is explained.

#### Type 1 — Code Checks

| Check | What it does |
|-------|-------------|
| **Mapping Gaps** | Groups source transactions by `account_code`, sums amounts for codes not present in the mapping table |
| **IC Eliminations** | Pairs intercompany entities with counterparties, flags uneliminated amounts |
| **Manual Adjustments** | Identifies rows where `adjustment_flag == True`, computes implied impact on target |

Each check returns: `{count, amount, contribution_pct, confidence, suggested_fix}`.

#### Type 2 — Data Checks

| Check | What it does |
|-------|-------------|
| **Fuzzy Join Coverage** | Uses `rapidfuzz` to find near-matches (≥80 score) for unmapped account codes — catches typos and format differences |
| **Schema Version Drift** | Compares column sets and dtypes between source and target; uses `schema_hash` to detect version changes |
| **Hierarchy Mismatches** | Compares revenue per region using our hierarchy vs client target; flags entities that roll up differently |

#### Type 3 — Orchestration Checks

| Check | What it does |
|-------|-------------|
| **Timestamp Gap** | Flags if source pipeline and target pipeline ran >1 hour apart (target consumed a stale snapshot) |
| **Parameter Diff** | Compares filter dates, aggregation keys — catches cases where source filtered `date >= 2024-01-01` but target used `2024-01-15` |
| **Watermark Gap** | Detects high-watermark mismatches in incremental loads (records in gap window missing from target) |

#### IsolationForest Noise Suppression

Before attribution, an `IsolationForest` (contamination=5%) flags statistically anomalous per-account amounts. **Anomalous accounts are excluded from contribution scoring** so outlier noise doesn't distort the gap attribution. Requires at least 10 distinct accounts to run.

---

### Layer 5 — Results, Attribution & Narrative

#### Variance Waterfall

Assembles findings into an ordered attribution chain:

```
Total Gap  →  Mapping Gap  →  IC Elimination  →  Timing  →  Schema Drift  →  Manual Adj  →  Residual
              (0.95 conf)     (0.85 conf)        (varies)    (varies)         (0.65 conf)
```

Each component carries: `label`, `amount`, `pct_of_gap`, `confidence`, `fix_type`, `detail`, `drill_down[]`.

#### Causal Path Tracing (NetworkX DAG)

Builds a 7-node DAG representing the data flow:

```
source_data → mapping_join → ic_elimination → orchestration_timing
           ↘ schema_version ↙ manual_adj ↗
```

Backward traversal identifies the **root cause node** and its contribution percentage.

#### Confidence Gate

```
overall_confidence = (type1_explained% × 0.55) + (type2_confidence × 0.25) + (type3_confidence × 0.20)
```

- **≥ 80%:** Narrative generated locally with zero API tokens.
- **< 80%:** Second Gemini call (~400 tokens) for a 3–5 sentence Finance Director–level explanation.

#### Fix Preview Engine

Applies suggested fixes **in-memory only** before any production change:
- `add_mapping` — add missing account codes to the mapping table
- `fix_hierarchy` — update entity hierarchy assignments
- `add_elimination` — add intercompany elimination entries
- `add_adjustment` — add manual adjustments

Returns a before/after gap comparison so you can validate impact first.

---

## Streamlit UI (6 Tabs)

| Tab | Purpose |
|-----|---------|
| **1 — Architecture** | Visual overview of all 6 layers, check types, connectors, and security model |
| **2 — Connect & Upload** | Business context input; file upload (source, mapping, target, hierarchy) or connector credentials |
| **3 — Data Profile** | Metric comparison traffic-light table; source stats; mapping coverage %; DVT column/type diff |
| **4 — Reconciliation** | Gap summary cards; per-metric breakdown (Revenue, COGS, Gross Margin, OpEx, Operating Profit); heatmaps by entity/region |
| **5 — AI Investigation** | Live step-by-step progress; variance waterfall chart; root cause donut; causal path DAG; attribution table; AI token usage; narrative |
| **6 — Fix & Validate** | Fix suggestion cards; before/after gap preview; fuzzy match checkboxes; audit trail; Excel export |

---

## Example: Q1 Financial Close

1. **Upload** `source_data.xlsx`, `mapping_table.xlsx`, `target_results.xlsx`, `hierarchy.xlsx`.
2. **Tab 3** — Profile shows 92% mapping coverage, 8 unmapped accounts, $15M unmapped amount.
3. **Tab 4** — Revenue gap = −$5.2M, OpEx gap = +$2.1M.
4. **Tab 5** — Investigation runs:
   - AI Strategy: *"Mapping gaps + IC eliminations — check APAC region"*
   - Type 1 Mapping: $3.2M (62% of gap, confidence 0.95)
   - Type 1 IC Elim: $1.8M (35% of gap, confidence 0.85)
   - Type 2 Fuzzy: 5 near-matches found at 88% similarity
   - Causal DAG: root cause = `mapping_join` (67% contribution)
   - Confidence: 83% → local narrative, 0 tokens
   - Narrative: *"Mapping gaps in APAC revenue accounts 2100, 2150, 2160 total $3.2M..."*
5. **Tab 6** — Preview adding 3 missing mappings → gap closes to −$0.2M.
6. **Export** — Excel workbook with full findings, causal path, and audit trail.

---

## Security Model

- **No raw data to AI** — Gemini only sees aggregated metrics, schema summaries, and pipeline structure.
- **No secrets in code** — `GEMINI_API_KEY` stored in `.env`, loaded via `python-dotenv`.
- **Read-only connectors** — Service principals and Entra ID credentials have no write permissions.
- **In-memory fix preview** — No production changes without explicit user action.
- **Audit log** — Every fix recorded with timestamp, method, `gap_before`, `gap_after`, and confidence.

---

## Project Structure

```
reconcilation-automater-2/
├── config.py               # All tunable thresholds and settings
├── streamlit_app.py        # Main UI — 6 tabs
├── requirements.txt
├── .env.example
│
├── core/
│   ├── data_loader.py      # Layer 0 connectors + file upload
│   ├── profiler.py         # Layer 1 schema profiler + fingerprint builder
│   ├── reconciler.py       # Layer 4 Type 1/2/3 checks + Layer 5 waterfall
│   └── fix_engine.py       # Fix preview engine
│
├── agents/
│   ├── orchestrator.py     # Layer 2 AI planner + Layer 5 orchestration
│   └── investigators.py    # Layer 4 investigation tools
│
├── utils/
│   └── charts.py           # Plotly waterfall, donut, heatmaps
│
└── sample_data/
    ├── source_data.xlsx
    ├── mapping_table.xlsx
    ├── target_results.xlsx
    ├── hierarchy.xlsx
    └── generate_sample.py  # Synthetic data generator
```

---

## Setup

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Add your Gemini API key
cp .env.example .env
# Edit .env and set GEMINI_API_KEY=your_key_here

# 3. Run the app
streamlit run streamlit_app.py
```

For production connector mode, add the appropriate credentials to `.env` (Databricks host/token, Azure tenant/client ID/secret, SQL Server connection string).

---

## Key Configuration (`config.py`)

| Setting | Default | Effect |
|---------|---------|--------|
| `AI.model` | `gemini-2.5-flash` | Gemini model for strategy + narrative |
| `AI.temperature` | `0.1` | Low for deterministic strategy planning |
| `DATA.gap_threshold` | `$1,000` | Minimum gap to flag |
| `TOLERANCE_PCT` | `1%` | DVT pass/fail threshold |
| `FUZZY_MATCH_THRESHOLD` | `85` | Minimum rapidfuzz score for a suggested match |
| `AI_CONFIDENCE_THRESHOLD` | `0.5` | Trigger second AI call if confidence below this |
| `ISOLATION_FOREST_CONTAMINATION` | `5%` | Fraction of accounts treated as outliers |
| `TYPE1_STOP_THRESHOLD` | `95%` | Stop running checks once this much of gap is explained |

---

## API Token Budget

| Event | Model | Tokens |
|-------|-------|--------|
| Strategy planning (always) | Gemini 2.5 Flash | ~600 |
| Narrative (only if confidence < 80%) | Gemini 2.5 Flash | ~400 |
| **Max per investigation** | | **~1,000** |

The system is designed to resolve most investigations at zero or minimal AI cost — deterministic Python checks handle the majority of cases.
