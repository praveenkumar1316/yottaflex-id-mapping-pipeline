# Verification Record

Fill this in with the actual output from your machine before submission.

## Test execution
Command: `python -m pytest -q`

Actual authoring-container result: `13 passed in 0.36s` (Python 3.13.5).
Run the command again in your own clean environment and retain your actual output.

## Sample run
Command:
`python -m src.cli --input data/sample/source_records.csv`

Inspect:
- `outputs/run_report.json`
- `outputs/mappings.csv`
- `outputs/unresolved.csv`
- `outputs/rejects.csv`
- SQLite tables and `current_mappings` view

## Reconciliation
For a successful run:
`input_count = mapped_count + unresolved_count + conflict_count + rejected_count`

Note: `input_count` counts parsed data rows, including quarantined records. `duplicate_count`
is a subset of `rejected_count`, not an additional category.

## Performance measurement
Run the generator with 100,000 rows, execute the pipeline, and record:
- machine/OS and Python version
- generated row count and seed
- wall-clock duration
- rows per second (`rows / elapsed_seconds`)
- observed peak memory if measured

Reference measurement from the authoring container: 100,000 rows, seed 42,
Python 3.13.5, elapsed 4.94 seconds, approximately 20,243 rows/second, peak RSS
154,500 KB as reported by `/usr/bin/time`. This includes CSV reading, matching,
SQLite writes, and report exports. Re-run on your machine and report your own result.
