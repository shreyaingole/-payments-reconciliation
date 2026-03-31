# -payments-reconciliation
Payments reconciliation engine — Onelab AI Assessment

# Payments Reconciliation Engine
Onelab AI Fitness Assessment — Month-end reconciliation for a payments platform.

## Problem
A payments company's books don't balance at month end.
This engine finds why and shows where the gaps are.

## Gap Types Detected
| Gap | Description |
|-----|-------------|
| LATE_SETTLEMENT | Transaction settled after month-end cutoff |
| AMOUNT_MISMATCH | Rounding difference (3dp vs 2dp) |
| DUPLICATE | Same transaction recorded twice |
| ORPHAN_REFUND | Bank credit with no matching platform record |

## How to Run
pip install pandas
python payments_recon.py

## Output
- platform_transactions.csv
- bank_settlements.csv
- reconciliation_gaps.csv
