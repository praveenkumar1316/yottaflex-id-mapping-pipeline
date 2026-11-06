# Sample Expected Results

For `data/sample/source_records.csv` (9 data rows):
- 5 mapped (3 exact alias, 2 rule-based)
- 1 unresolved
- 1 conflict (ambiguous alias)
- 2 rejected (one missing required identifier, one duplicate source key)
- Reconciliation: 9 = 5 + 1 + 1 + 2

The run ID and timestamps in `run_report.json` are from one actual local execution and will differ on rerun.
