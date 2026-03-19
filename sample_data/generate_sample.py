"""
Generate realistic sample data for a global energy company.
Includes intentional reconciliation gaps for demo purposes.

Intentional gaps:
  1. Mapping Gap      : Account codes 4150, 4160, 4170 (Service Revenue) NOT in mapping → $2.1M
  2. Hierarchy Gap    : Entity DE_HoldCo rolls under 'Corporate' in target, 'EMEA' in our system → $1.4M
  3. IC Elimination   : UK_Ops → DE_HoldCo intercompany transaction not eliminated → $0.8M
  4. Manual Adjustment: Client has $0.5M top-side adjustment in target that's not in source

Total gap: ~$4.8M (explained: $4.3M, unexplained: $0.5M)
"""

import pandas as pd
import numpy as np
from pathlib import Path
import random

np.random.seed(42)
random.seed(42)

OUTPUT_DIR = Path(__file__).parent


# ─── Master reference data ────────────────────────────────────────────────────

ENTITIES = [
    ("UK_Ops",       "UK Operations",        "Upstream",   "EMEA",    "Oil & Gas"),
    ("DE_HoldCo",    "Germany Holding Co",   "Corporate",  "EMEA",    "Corporate"),  # hierarchy gap entity
    ("US_Expl",      "US Exploration",       "Upstream",   "Americas","Oil & Gas"),
    ("SG_Refin",     "Singapore Refining",   "Downstream", "APAC",    "Refining"),
    ("NO_Offshore",  "Norway Offshore",      "Upstream",   "EMEA",    "Oil & Gas"),
    ("AE_LNG",       "UAE LNG",              "Midstream",  "APAC",    "LNG"),
    ("BR_DeepSea",   "Brazil Deep Sea",      "Upstream",   "Americas","Oil & Gas"),
    ("AU_Renew",     "Australia Renewables", "Renewables", "APAC",    "Renewables"),
]

MAPPED_ACCOUNTS = [
    # Revenue accounts
    ("1001", "Crude Oil Sales",          "TGT_REV_001", "Revenue"),
    ("1002", "Natural Gas Sales",        "TGT_REV_002", "Revenue"),
    ("1003", "LNG Export Revenue",       "TGT_REV_003", "Revenue"),
    ("1004", "Refined Products Sales",   "TGT_REV_004", "Revenue"),
    ("1005", "Power Sales - Renewables", "TGT_REV_005", "Revenue"),
    ("1010", "Pipeline Throughput Fees", "TGT_REV_010", "Revenue"),
    # COGS accounts
    ("2001", "Production Lifting Costs", "TGT_COGS_001", "COGS"),
    ("2002", "Refining Operating Costs", "TGT_COGS_002", "COGS"),
    ("2003", "LNG Liquefaction Costs",   "TGT_COGS_003", "COGS"),
    ("2010", "Royalties & Levies",       "TGT_COGS_010", "COGS"),
    # OpEx accounts
    ("3001", "G&A Expenses",             "TGT_OPEX_001", "OpEx"),
    ("3002", "Depreciation",             "TGT_OPEX_002", "OpEx"),
    ("3003", "Marketing & Distribution", "TGT_OPEX_003", "OpEx"),
    ("3004", "R&D Expenditure",          "TGT_OPEX_004", "OpEx"),
    ("3005", "HSE Compliance",           "TGT_OPEX_005", "OpEx"),
]

# UNMAPPED accounts — these cause the mapping gap
UNMAPPED_ACCOUNTS = [
    ("4150", "Technical Service Revenue"),   # $0.9M gap
    ("4160", "Consultancy Service Revenue"), # $0.7M gap
    ("4170", "Asset Management Fees"),       # $0.5M gap
]


def generate_source_data() -> pd.DataFrame:
    rows = []
    txn_id = 1000

    # ── Normal mapped transactions ──────────────────────────────────────────
    for entity_code, entity_name, bu, region, segment in ENTITIES:
        for acc_code, acc_name, _, metric_cat in MAPPED_ACCOUNTS:
            n_txns = random.randint(3, 15)
            for _ in range(n_txns):
                if metric_cat == "Revenue":
                    base = random.uniform(1_000_000, 8_000_000)
                elif metric_cat == "COGS":
                    base = -random.uniform(500_000, 4_000_000)
                else:
                    base = -random.uniform(100_000, 800_000)

                amount = base * random.uniform(0.85, 1.15)
                date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=random.randint(0, 90))

                rows.append({
                    "transaction_id": f"TXN{txn_id:05d}",
                    "date": date,
                    "account_code": acc_code,
                    "account_name": acc_name,
                    "entity_code": entity_code,
                    "entity_name": entity_name,
                    "business_unit": bu,
                    "region": region,
                    "segment": segment,
                    "amount": round(amount, 2),
                    "currency": "USD",
                    "is_intercompany": False,
                    "counterparty_entity": None,
                    "adjustment_flag": False,
                    "adjustment_type": None,
                })
                txn_id += 1

    # ── GAP 1: Unmapped account transactions ───────────────────────────────
    unmapped_amounts = {"4150": 900_000, "4160": 700_000, "4170": 500_000}
    for acc_code, acc_name in UNMAPPED_ACCOUNTS:
        entities_used = random.sample(ENTITIES, 3)
        total = unmapped_amounts[acc_code]
        per_entity = total / 3
        for entity_code, entity_name, bu, region, segment in entities_used:
            n_txns = random.randint(4, 8)
            for i in range(n_txns):
                amount = per_entity / n_txns * random.uniform(0.9, 1.1)
                date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=random.randint(0, 90))
                rows.append({
                    "transaction_id": f"TXN{txn_id:05d}",
                    "date": date,
                    "account_code": acc_code,
                    "account_name": acc_name,
                    "entity_code": entity_code,
                    "entity_name": entity_name,
                    "business_unit": bu,
                    "region": region,
                    "segment": segment,
                    "amount": round(amount, 2),
                    "currency": "USD",
                    "is_intercompany": False,
                    "counterparty_entity": None,
                    "adjustment_flag": False,
                    "adjustment_type": None,
                })
                txn_id += 1

    # ── GAP 3: Intercompany transaction (UK_Ops → DE_HoldCo) ──────────────
    # UK_Ops charges DE_HoldCo for shared services — should be eliminated at group level
    for i in range(5):
        amount = 160_000 * random.uniform(0.9, 1.1)
        date = pd.Timestamp("2024-01-01") + pd.Timedelta(days=random.randint(0, 90))
        rows.append({
            "transaction_id": f"TXN{txn_id:05d}",
            "date": date,
            "account_code": "1010",
            "account_name": "Intercompany Service Charge",
            "entity_code": "UK_Ops",
            "entity_name": "UK Operations",
            "business_unit": "Upstream",
            "region": "EMEA",
            "segment": "Oil & Gas",
            "amount": round(amount, 2),
            "currency": "USD",
            "is_intercompany": True,
            "counterparty_entity": "DE_HoldCo",
            "adjustment_flag": False,
            "adjustment_type": None,
        })
        txn_id += 1
        # DE_HoldCo side is MISSING (elimination entry not present) — this is the gap

    df = pd.DataFrame(rows)
    return df


def generate_mapping_table() -> pd.DataFrame:
    """Mapping table — intentionally excludes accounts 4150, 4160, 4170."""
    rows = []
    for acc_code, acc_name, tgt_code, metric_cat in MAPPED_ACCOUNTS:
        rows.append({
            "source_account_code": acc_code,
            "source_account_name": acc_name,
            "target_account_code": tgt_code,
            "target_account_name": acc_name + " (Group)",
            "metric_category": metric_cat,
        })
    return pd.DataFrame(rows)


def generate_hierarchy_table() -> pd.DataFrame:
    """
    Our hierarchy — DE_HoldCo is under EMEA.
    Client's target has DE_HoldCo under 'Corporate'. This is the hierarchy gap.
    """
    rows = []
    for entity_code, entity_name, bu, region, segment in ENTITIES:
        rows.append({
            "entity_code": entity_code,
            "entity_name": entity_name,
            "business_unit": bu,
            "region": region,
            "segment": segment,
            "parent_entity": None,
            "is_intercompany_entity": entity_code == "DE_HoldCo",
        })
    return pd.DataFrame(rows)


def generate_target_results(source: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Generate client target results.
    The client's system has mappings for ALL accounts including 4150, 4160, 4170,
    properly eliminates IC, and uses its own hierarchy for DE_HoldCo.

    Structure:
    - One 'Group Total' row per headline metric (used for reconciliation comparison)
    - Per-region breakdown rows (used for drill-down; NOT summed for reconciliation)
    """
    mapping_clean = mapping.set_index("source_account_code")["metric_category"].to_dict()

    # Build enriched source with client's full mappings (including unmapped accounts)
    rows_with_metric = []
    for _, row in source.iterrows():
        acc = str(row["account_code"])
        # Client has all accounts mapped; 4xxx series = Revenue in their system
        if acc in mapping_clean:
            metric_cat = mapping_clean[acc]
        elif acc.startswith("4"):
            metric_cat = "Revenue"
        else:
            metric_cat = "Other"

        rows_with_metric.append({
            "entity_code": row["entity_code"],
            "business_unit": row["business_unit"],
            "region": row["region"],
            "metric_category": metric_cat,
            "amount": row["amount"],
            "is_intercompany": row["is_intercompany"],
        })

    full_df = pd.DataFrame(rows_with_metric)

    # Client eliminates all IC at group level
    non_ic = full_df[~full_df["is_intercompany"]]

    # Compute headline totals (what client reports as Group Total)
    total_rev = float(non_ic[non_ic["metric_category"] == "Revenue"]["amount"].sum())
    total_cogs = float(non_ic[non_ic["metric_category"] == "COGS"]["amount"].sum())
    total_opex = float(non_ic[non_ic["metric_category"] == "OpEx"]["amount"].sum())
    gross_margin = total_rev + total_cogs
    op_profit = gross_margin + total_opex

    # GAP 4: Client has $500K manual top-side adjustment in Operating Profit
    op_profit += 500_000

    target_rows = []

    # ── Headline Group Total rows (these drive reconciliation comparison) ──
    for metric, amount in [
        ("Revenue", total_rev),
        ("COGS", total_cogs),
        ("Gross Margin", gross_margin),
        ("OpEx", total_opex),
        ("Operating Profit", op_profit),
    ]:
        target_rows.append({
            "metric": metric,
            "group_by": "Group Total",
            "period": "2024-Q1",
            "amount": round(amount, 2),
            "currency": "USD",
            "row_type": "headline",
        })

    # ── Regional breakdown rows for Revenue (drill-down, NOT for headline comparison) ──
    # Client puts DE_HoldCo under 'Corporate' (our system has it under EMEA — GAP 2)
    revenue = non_ic[non_ic["metric_category"] == "Revenue"]

    region_defs = {
        "EMEA": [e[0] for e in ENTITIES if e[3] == "EMEA" and e[0] != "DE_HoldCo"],
        "Americas": [e[0] for e in ENTITIES if e[3] == "Americas"],
        "APAC": [e[0] for e in ENTITIES if e[3] == "APAC"],
        "Corporate": ["DE_HoldCo"],  # Client's hierarchy puts DE_HoldCo here
    }

    for region, entity_list in region_defs.items():
        region_rev = revenue[revenue["entity_code"].isin(entity_list)]["amount"].sum()
        if abs(region_rev) > 0.01:
            target_rows.append({
                "metric": "Revenue",
                "group_by": region,
                "period": "2024-Q1",
                "amount": round(float(region_rev), 2),
                "currency": "USD",
                "row_type": "detail",
            })

    return pd.DataFrame(target_rows)


def save_all():
    """Generate and save all sample data files."""
    print("Generating source data...")
    source = generate_source_data()
    print(f"  → {len(source)} transactions, ${source['amount'].sum():,.0f} total")

    print("Generating mapping table...")
    mapping = generate_mapping_table()
    print(f"  → {len(mapping)} account mappings")

    print("Generating hierarchy table...")
    hierarchy = generate_hierarchy_table()

    print("Generating target results...")
    target = generate_target_results(source, mapping)

    # Save to Excel
    source.to_excel(OUTPUT_DIR / "source_data.xlsx", index=False)
    mapping.to_excel(OUTPUT_DIR / "mapping_table.xlsx", index=False)
    hierarchy.to_excel(OUTPUT_DIR / "hierarchy.xlsx", index=False)
    target.to_excel(OUTPUT_DIR / "target_results.xlsx", index=False)

    print("\n✅ Sample files saved:")
    print(f"   {OUTPUT_DIR}/source_data.xlsx")
    print(f"   {OUTPUT_DIR}/mapping_table.xlsx")
    print(f"   {OUTPUT_DIR}/hierarchy.xlsx")
    print(f"   {OUTPUT_DIR}/target_results.xlsx")

    # Print expected gaps
    our_rev = float(
        source[source["account_code"].astype(str).isin([r[0] for r in MAPPED_ACCOUNTS])]["amount"].sum()
    )
    target_rev = float(target[target["metric"] == "Revenue"]["amount"].sum())
    print(f"\n📊 Expected Revenue Gap: ${abs(target_rev - our_rev):,.0f}")
    print(f"   Our computed Revenue : ${our_rev:,.0f}")
    print(f"   Client target Revenue: ${target_rev:,.0f}")


if __name__ == "__main__":
    save_all()
