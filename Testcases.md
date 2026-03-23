# SmartRecon — Test Case Validation Guide

> These test cases verify that the AI Investigation layer (ReconAgent) correctly detects,
> attributes, and narrates reconciliation gaps when input files change.
> All expected values are computed from the actual sample data files.

---

## Baseline (Before Any Test)

Load the four sample files unchanged to confirm the baseline is working before running any test.

| File | Load as |
|------|---------|
| `source_data.xlsx` | Source Data |
| `mapping_table.xlsx` | Mapping Table |
| `target_results.xlsx` | Target Results |
| `hierarchy.xlsx` | Hierarchy Table |

### Baseline Expected Results

| Metric | Our Value | Target | Gap | Has Gap |
|--------|-----------|--------|-----|---------|
| Revenue | $1,847,099,428 | $1,849,173,130 | **-$2,073,702** | ✅ Yes |
| COGS | -$738,111,958 | -$738,111,958 | $0 | No |
| Gross Margin | $1,108,987,469 | $1,111,061,172 | **-$2,073,702** | ✅ Yes |
| OpEx | -$147,163,261 | -$147,163,261 | $0 | No |
| Operating Profit | $961,824,208 | $964,397,910 | **-$2,573,702** | ✅ Yes |

### Baseline AI Investigation Expected

- **Root cause:** Mapping Gap — 3 account codes (4150, 4160, 4170), ~55 transactions, $2,073,702
- **Cross-metric inconsistency:** Operating Profit gap (-$2,573,702) is $500,000 larger than expected from Gross Margin (-$2,073,702) + OpEx ($0)
- **Gap explained:** 100%
- **Confidence:** ≥ 90%
- **Auto-fixable:** Yes (add_mapping fix type)

---

## Test Case 1 — Remove a Large Revenue Account from Mapping

### What to Change

Open `mapping_table.xlsx` and **delete the row** where `source_account_code = 1002`.

| Field | Value |
|-------|-------|
| `source_account_code` | 1002 |
| `source_account_name` | Natural Gas Sales |
| Row to delete | Row 2 (zero-indexed row 1) |

The mapping table should go from **15 rows → 14 rows** after deletion.

### Why This Test Matters

Account 1002 (Natural Gas Sales) is a large Revenue account with 45 transactions totalling
$213,376,796. Removing it from mapping creates a dramatic new gap on top of the existing
$2,073,702 baseline gap. This verifies the tool catches a newly introduced large gap that was
not in the original data.

### Expected Results After Change

| Metric | Our Value | Target | Gap | Has Gap |
|--------|-----------|--------|-----|---------|
| Revenue | ~$1,633,722,632 | $1,849,173,130 | **-$215,450,498** | ✅ Yes |
| COGS | -$738,111,958 | -$738,111,958 | $0 | No |
| Gross Margin | ~$895,610,674 | $1,111,061,172 | **-$215,450,498** | ✅ Yes |
| OpEx | -$147,163,261 | -$147,163,261 | $0 | No |
| Operating Profit | ~$748,447,413 | $964,397,910 | **-$215,950,498** | ✅ Yes |

### AI Investigation Expected

- **Unmapped accounts:** 4 (1002, 4150, 4160, 4170)
- **Root cause:** Mapping Gap — 4 account codes, ~100 transactions
- **Primary gap driver:** Account 1002 contributes ~$213M of the total gap
- **Cross-metric inconsistency:** Operating Profit gap is $500,000 larger than Gross Margin gap (the existing top-side adjustment)
- **Gap explained:** 100%
- **Confidence:** ≥ 90%
- **Layer 2 hypothesis:** Should mention mapping gaps as primary cause

### Pass Criteria

- [ ] Revenue gap shown as approximately **-$215,450,498** (within $10K rounding)
- [ ] Gross Margin gap matches Revenue gap exactly
- [ ] Operating Profit gap is Revenue gap minus $500,000 (i.e. **-$215,950,498**)
- [ ] Root cause card shows **4 unmapped account codes** (not 3)
- [ ] Account 1002 appears in the drill-down table
- [ ] Cross-metric inconsistency ($500K OP delta) still flagged

---

## Test Case 2 — Remove a COGS Account from Mapping

### What to Change

Open `mapping_table.xlsx` and **delete the row** where `source_account_code = 2001`.

| Field | Value |
|-------|-------|
| `source_account_code` | 2001 |
| `source_account_name` | Production Lifting Costs |
| Row to delete | Row 7 (the first COGS row) |

The mapping table should go from **15 rows → 14 rows** after deletion.

### Why This Test Matters

Account 2001 is a **COGS account** (negative amount, -$173,901,487). Removing it from mapping
means those costs are excluded from our computation, making COGS *less negative* (smaller in
absolute terms). This creates a **positive COGS gap** and a **positive Gross Margin gap** —
the opposite sign to the Revenue gap tests. This verifies the tool handles cost-side gaps
correctly, not just revenue-side gaps.

### Expected Results After Change

| Metric | Our Value | Target | Gap | Has Gap |
|--------|-----------|--------|-----|---------|
| Revenue | $1,847,099,428 | $1,849,173,130 | **-$2,073,702** | ✅ Yes |
| COGS | ~-$564,210,471 | -$738,111,958 | **+$173,901,487** | ✅ Yes |
| Gross Margin | ~$1,282,888,957 | $1,111,061,172 | **+$171,827,785** | ✅ Yes |
| OpEx | -$147,163,261 | -$147,163,261 | $0 | No |
| Operating Profit | ~$1,135,725,696 | $964,397,910 | **+$171,327,785** | ✅ Yes |

> Note: COGS gap is positive because our COGS is *less negative* than target (we are missing
> a cost). Gross Margin gap is also positive because Gross Margin = Revenue + COGS.

### AI Investigation Expected

- **Unmapped accounts:** 4 (2001, 4150, 4160, 4170)
- **Root cause:** Mapping Gap — account 2001 drives the COGS gap
- **Revenue gap:** Still -$2,073,702 from the original 4150/4160/4170 unmapped accounts
- **COGS gap:** +$173,901,487 (account 2001 costs excluded)
- **Gross Margin gap:** +$171,827,785 (COGS gap partially offsets Revenue gap)
- **Gap explained:** 100%
- **No cross-metric inconsistency on Operating Profit** beyond the existing $500K top-side

### Pass Criteria

- [ ] COGS gap shown as approximately **+$173,901,487** (positive — costs understated)
- [ ] Gross Margin gap shown as approximately **+$171,827,785** (positive)
- [ ] Revenue gap unchanged at **-$2,073,702**
- [ ] Root cause card shows **4 unmapped account codes** including 2001
- [ ] Account 2001 appears in the drill-down table
- [ ] Tool does NOT confuse positive gap direction — narrative should say "COGS understated"

---

## Test Case 3 — Fix the Mapping Gap (Add All 3 Unmapped Accounts)

### What to Change

Open `mapping_table.xlsx` and **add the following 3 rows** at the bottom:

| source_account_code | source_account_name | target_account_code | target_account_name | metric_category |
|---------------------|---------------------|---------------------|---------------------|-----------------|
| 4150 | Technical Service Revenue | TGT_REV_150 | Technical Service Revenue (Group) | Revenue |
| 4160 | Consultancy Service Revenue | TGT_REV_160 | Consultancy Service Revenue (Group) | Revenue |
| 4170 | Asset Management Fees | TGT_REV_170 | Asset Management Fees (Group) | Revenue |

The mapping table should go from **15 rows → 18 rows** after addition.

### Why This Test Matters

This is the "fix verification" test. Adding all 3 previously unmapped accounts closes the
Revenue and Gross Margin gaps completely. The only remaining gap should be the $500,000
Operating Profit top-side adjustment that is intentionally baked into the target data and
**cannot be resolved via mapping** — it requires a manual journal entry. This verifies that:
1. The mapping gap disappears when accounts are correctly mapped
2. The tool correctly pivots its root cause to the residual unexplained item
3. The tool does not falsely report a mapping gap when none exists

### Expected Results After Change

| Metric | Our Value | Target | Gap | Has Gap |
|--------|-----------|--------|-----|---------|
| Revenue | $1,849,173,130 | $1,849,173,130 | **$0** | No |
| COGS | -$738,111,958 | -$738,111,958 | $0 | No |
| Gross Margin | $1,111,061,172 | $1,111,061,172 | **$0** | No |
| OpEx | -$147,163,261 | -$147,163,261 | $0 | No |
| Operating Profit | $963,897,910 | $964,397,910 | **-$500,000** | ✅ Yes |

### AI Investigation Expected

- **Mapping gaps:** 0 unmapped accounts
- **Primary gap:** Operating Profit only — $500,000
- **Root cause:** Manual Adjustment — top-side adjustment in client's reporting system not present in source
- **Cross-metric inconsistency:** Operating Profit gap (-$500,000) cannot be explained by Gross Margin ($0) + OpEx ($0) — unexplained delta of $500,000
- **Gap explained:** 100%
- **Confidence:** ~65–82% (manual adjustments are inherently medium confidence)
- **Auto-fixable:** No (manual review required)
- **Layer 2 hypothesis:** Should NOT mention mapping gaps; should focus on manual adjustments or top-side entries

### Pass Criteria

- [ ] Revenue gap: **$0** — no gap shown
- [ ] Gross Margin gap: **$0** — no gap shown
- [ ] Operating Profit gap: **-$500,000** only
- [ ] Root cause card shows **Manual Adjustment** (NOT Mapping Gap)
- [ ] No mapping gap component in waterfall chart
- [ ] Cross-metric inconsistency flagged: OP gap $500K with Gross Margin $0 + OpEx $0
- [ ] Narrative states this requires manual review / journal entry investigation

---

## Quick Reference — Gap Summary Table

| Test | Revenue Gap | COGS Gap | Gross Margin Gap | OpEx Gap | OP Gap | Unmapped Accounts |
|------|-------------|----------|------------------|----------|--------|-------------------|
| Baseline | -$2,073,702 | $0 | -$2,073,702 | $0 | -$2,573,702 | 3 (4150, 4160, 4170) |
| TC1: Remove 1002 | -$215,450,498 | $0 | -$215,450,498 | $0 | -$215,950,498 | 4 (1002, 4150, 4160, 4170) |
| TC2: Remove 2001 | -$2,073,702 | +$173,901,487 | +$171,827,785 | $0 | +$171,327,785 | 4 (2001, 4150, 4160, 4170) |
| TC3: Add 4150/4160/4170 | $0 | $0 | $0 | $0 | -$500,000 | 0 |

---

## Notes for Testers

**Restarting between tests:** Always restart the Streamlit app (`Ctrl+C` then `streamlit run streamlit_app.py`) between test cases, or at minimum re-upload all files fresh. Session state from a previous test can persist and cause stale results.

**File preparation:** Keep the original sample files untouched. Make a copy of `mapping_table.xlsx` for each test case before editing.

**Tolerance:** Gap amounts may vary by ±$1 due to floating point rounding in Excel. Values within $100 of the expected amount should be considered passing.

**The $500K OP delta:** This delta is intentional and permanent — it is hardcoded into `generate_sample.py` as `op_profit += 500_000` in `generate_target_results()`. It will appear in every test case as a cross-metric inconsistency on Operating Profit. This is by design and should always be flagged.