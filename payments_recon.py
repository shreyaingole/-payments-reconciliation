"""
payments_recon.py
Payments Reconciliation Engine — Onelab AI Fitness Assessment
Generates test data with all 4 planted gap types, reconciles, outputs CSV reports.
"""

import pandas as pd
import random
from datetime import date, timedelta

random.seed(42)

# ─────────────────────────────────────────────
# ASSUMPTIONS
# ─────────────────────────────────────────────
# 1. Platform records transaction on payment date (T+0).
# 2. Bank settles in 1–2 business days; settlements arrive as lump-sum batch rows.
# 3. Month-end cutoff = 31 Jan 2025. Anything settled after = gap type 1.
# 4. Amounts stored as float; rounding error accumulates when summed → gap type 2.
# 5. A duplicate means the platform logged the same txn twice with identical IDs → gap type 3.
# 6. Refund with no original = a credit row in settlements with no matching debit → gap type 4.
# 7. "Match" = same txn_id, same amount (within 0.01), settlement date ≤ month end.

# ─────────────────────────────────────────────
# GENERATE PLATFORM TRANSACTIONS (Jan 2025)
# ─────────────────────────────────────────────
def random_date(start, end):
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))

jan_start = date(2025, 1, 1)
jan_end   = date(2025, 1, 31)

normal_txns = []
for i in range(1, 51):
    txn_date = random_date(jan_start, date(2025, 1, 28))  # settle within Jan
    amount   = round(random.uniform(10.00, 500.00), 2)
    normal_txns.append({
        "txn_id":   f"TXN{i:04d}",
        "txn_date": txn_date,
        "amount":   amount,
        "status":   "PAID",
        "customer": f"CUST{random.randint(100,999)}",
    })

platform_df = pd.DataFrame(normal_txns)

# ── GAP 1: Late settlement (settles in Feb) ──
gap1_txn = {
    "txn_id": "TXN0051", "txn_date": date(2025, 1, 30),
    "amount": 250.00, "status": "PAID", "customer": "CUST201",
}
platform_df = pd.concat([platform_df, pd.DataFrame([gap1_txn])], ignore_index=True)

# ── GAP 2: Rounding issue ──
# Platform stores 3 decimal precision; bank rounds to 2 → sum differs
gap2_txn = {
    "txn_id": "TXN0052", "txn_date": date(2025, 1, 15),
    "amount": 99.999,   # will be rounded by bank to 100.00 → diff of 0.001 × many rows
    "status": "PAID", "customer": "CUST305",
}
platform_df = pd.concat([platform_df, pd.DataFrame([gap2_txn])], ignore_index=True)

# ── GAP 3: Duplicate entry in platform ──
dup_txn = platform_df[platform_df["txn_id"] == "TXN0010"].copy()
platform_df = pd.concat([platform_df, dup_txn], ignore_index=True)

# ── GAP 4: Refund with no original ──
# This will only appear in bank settlements (credit), no platform record
ORPHAN_REFUND_ID = "TXN9999"

# ─────────────────────────────────────────────
# GENERATE BANK SETTLEMENTS
# ─────────────────────────────────────────────
settlements = []

for _, row in platform_df.drop_duplicates(subset=["txn_id"]).iterrows():
    # Skip gap1 from settling in Jan (settles in Feb instead)
    if row["txn_id"] == "TXN0051":
        continue
    settle_date = row["txn_date"] + timedelta(days=random.randint(1, 2))
    if settle_date > jan_end:
        settle_date = jan_end  # clamp normal ones
    # Bank rounds amount to 2dp
    settled_amount = round(float(row["amount"]), 2)
    settlements.append({
        "settlement_id": f"SET{len(settlements)+1:05d}",
        "txn_id":        row["txn_id"],
        "settle_date":   settle_date,
        "settled_amount": settled_amount,
    })

# Gap 1 settles in Feb
settlements.append({
    "settlement_id": "SET99901",
    "txn_id":        "TXN0051",
    "settle_date":   date(2025, 2, 2),
    "settled_amount": 250.00,
})

# Gap 4: orphan refund (credit) — no matching platform txn
settlements.append({
    "settlement_id": "SET99902",
    "txn_id":        ORPHAN_REFUND_ID,
    "settle_date":   date(2025, 1, 20),
    "settled_amount": -150.00,   # negative = refund/credit
})

bank_df = pd.DataFrame(settlements)

# ─────────────────────────────────────────────
# RECONCILIATION ENGINE
# ─────────────────────────────────────────────
MONTH_END   = date(2025, 1, 31)
AMOUNT_TOL  = 0.01   # tolerance for rounding gaps

# --- Merge on txn_id ---
merged = platform_df.merge(bank_df, on="txn_id", how="outer", indicator=True)

gaps = []

for _, row in merged.iterrows():
    txn = row.get("txn_id", "N/A")

    # GAP 3: Duplicate in platform (same txn_id appears twice)
    dup_count = platform_df[platform_df["txn_id"] == txn].shape[0]
    if dup_count > 1 and row["_merge"] == "both":
        gaps.append({
            "txn_id":    txn,
            "gap_type":  "DUPLICATE",
            "detail":    f"Transaction appears {dup_count}x in platform records",
            "amount_platform": row.get("amount"),
            "amount_bank":     row.get("settled_amount"),
            "settle_date":     row.get("settle_date"),
        })
        continue

    # GAP 4: In bank only (orphan refund)
    if row["_merge"] == "right_only":
        gaps.append({
            "txn_id":    txn,
            "gap_type":  "ORPHAN_REFUND",
            "detail":    "Settlement exists in bank with no matching platform transaction",
            "amount_platform": None,
            "amount_bank":     row.get("settled_amount"),
            "settle_date":     row.get("settle_date"),
        })
        continue

    # GAP 1: In platform, not yet settled (or settled after month end)
    if row["_merge"] == "left_only":
        gaps.append({
            "txn_id":    txn,
            "gap_type":  "UNSETTLED",
            "detail":    "Transaction on platform has no bank settlement in Jan 2025",
            "amount_platform": row.get("amount"),
            "amount_bank":     None,
            "settle_date":     None,
        })
        continue

    # Check settle date (GAP 1 for late settlements that did arrive)
    settle_date = row.get("settle_date")
    if pd.notna(settle_date) and settle_date > MONTH_END:
        gaps.append({
            "txn_id":    txn,
            "gap_type":  "LATE_SETTLEMENT",
            "detail":    f"Settled on {settle_date} — after Jan 31 month-end cutoff",
            "amount_platform": row.get("amount"),
            "amount_bank":     row.get("settled_amount"),
            "settle_date":     settle_date,
        })
        continue

    # GAP 2: Rounding difference
    p_amt = float(row.get("amount", 0) or 0)
    b_amt = float(row.get("settled_amount", 0) or 0)
    if abs(p_amt - b_amt) > AMOUNT_TOL:
        gaps.append({
            "txn_id":    txn,
            "gap_type":  "AMOUNT_MISMATCH",
            "detail":    f"Platform={p_amt:.4f} Bank={b_amt:.2f} Diff={p_amt-b_amt:.4f}",
            "amount_platform": p_amt,
            "amount_bank":     b_amt,
            "settle_date":     settle_date,
        })

gaps_df = pd.DataFrame(gaps).drop_duplicates(subset=["txn_id", "gap_type"])

# ─────────────────────────────────────────────
# SUMMARY STATS
# ─────────────────────────────────────────────
total_platform = platform_df["amount"].sum()
total_bank     = bank_df[bank_df["settle_date"] <= MONTH_END]["settled_amount"].sum()
diff           = total_platform - total_bank

print("=" * 60)
print("RECONCILIATION SUMMARY — JAN 2025")
print("=" * 60)
print(f"Platform total (gross):  ₹{total_platform:>12.4f}")
print(f"Bank settled (Jan only): ₹{total_bank:>12.2f}")
print(f"Difference:              ₹{diff:>12.4f}")
print(f"\nGaps found: {len(gaps_df)}")
print(gaps_df[["txn_id","gap_type","detail"]].to_string(index=False))
print()

# Save outputs
platform_df.to_csv("/home/claude/platform_transactions.csv", index=False)
bank_df.to_csv("/home/claude/bank_settlements.csv", index=False)
gaps_df.to_csv("/home/claude/reconciliation_gaps.csv", index=False)

print("CSVs saved.")

# Return for use in PDF
_summary = {
    "total_platform": total_platform,
    "total_bank": total_bank,
    "diff": diff,
    "gaps_df": gaps_df,
    "platform_df": platform_df,
    "bank_df": bank_df,
}
