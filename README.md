# Source-to-Canonical ID Mapping Pipeline

A local, reproducible data-engineering project for YottaFlex AI's candidate assignment.
It maps source-system identifiers to canonical customer IDs using exact alias matching
and deterministic rule-based matching, detects ambiguous mappings, preserves mapping
history, quarantines invalid records, and records operational metrics.

## Technology
- Python 3.11+
- SQLite (built into Python)
- pytest (tests)
- CSV for portable input/output

## Architecture
`CSV sources -> ingestion/schema checks -> data-quality quarantine -> entity resolution
-> SQLite (canonical entities, aliases, mapping history, rejects, run log) -> CSV/SQL reports`

### Matching policy
1. **Exact alias:** normalize the source identifier (trim + uppercase + remove separators)
   and look up `(source_system, normalized_identifier)` in `canonical_aliases`.
2. **Rule-based:** if no exact alias exists, independently compare normalized email and
   phone with canonical entities. A candidate is accepted only when at least two
   independent signals point to exactly one canonical entity.
3. **Conflict:** if evidence points to multiple canonical IDs, or an alias key is linked
   to multiple canonical IDs, mark `CONFLICT`; never choose arbitrarily.
4. **Unresolved:** insufficient evidence or no candidate becomes `UNRESOLVED`.
5. Names are retained for investigation but are not used alone to auto-link records.

This is intentionally conservative: false positive identity merges are more damaging
than leaving a record unresolved. Matching rules are deterministic, not probabilistic.

## Data model / grain
| Table | Grain / primary key | Purpose |
|---|---|---|
| `canonical_entities` | one row per `canonical_id` | trusted target entities |
| `canonical_aliases` | one row per `(source_system, normalized_identifier, canonical_id)` | exact source-ID crosswalk |
| `mapping_history` | one row per distinct mapping decision/event | append-only decision history |
| `source_records` | one row per `(source_system, source_record_id)` | successfully parsed source records |
| `rejects` | one row per rejected source row | traceable invalid input |
| `pipeline_runs` | one row per execution | run status and operational metrics |
| `processed_batches` | one row per successful input content hash | idempotency ledger |
| `current_mappings` | view | latest accepted mapping per source record |

### Important fields and assumptions
- `canonical_id` is a stable string key supplied by the reference/master data.
- `source_system + source_record_id` uniquely identifies a source record.
- `source_identifier` is the external identifier to resolve; source systems may format it differently.
- `source_file`, `run_id`, and ISO-8601 UTC timestamps are retained for traceability.
- Email and phone are normalized only for matching. Raw values remain in `source_records`.
- Missing required fields, malformed CSV rows, duplicate source keys and unknown schema columns are quarantined.
- An empty CSV with a valid header is a successful zero-row batch.
- Reference files are treated as trusted inputs; duplicate alias keys pointing to multiple
  canonical IDs are retained as conflicts rather than silently deduplicated.

## Quick start
```bash
# Windows PowerShell
py -3.11 -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m src.cli --input data/sample/source_records.csv

# macOS / Linux
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python -m src.cli --input data/sample/source_records.csv
```
Default database: `outputs/mapping.db`. Reference files default to
`data/reference/canonical_entities.csv` and `data/reference/canonical_aliases.csv`.
Override paths:
```bash
python -m src.cli --input data/sample/source_records.csv \
  --canonical data/reference/canonical_entities.csv \
  --aliases data/reference/canonical_aliases.csv \
  --db outputs/mapping.db
```

Generate a reproducible larger input:
```bash
python -m src.generate_data --rows 100000 --seed 42 --output data/generated/source_records_100k.csv
python -m src.cli --input data/generated/source_records_100k.csv
```

Run tests:
```bash
python -m pytest -q
```

## Outputs
- `outputs/mapping.db`: persistent SQLite database.
- `outputs/mappings.csv`: accepted mapping decisions for the latest run.
- `outputs/unresolved.csv`: unresolved and conflict decisions for the latest run.
- `outputs/rejects.csv`: quarantined invalid records for the latest run.
- `outputs/run_report.json`: run ID, status, counts, timing, and errors.

Useful SQL:
```sql
-- Accepted current mappings
SELECT * FROM current_mappings;

-- Mapping decisions by method/status
SELECT status, match_method, COUNT(*)
FROM mapping_history GROUP BY status, match_method;

-- Conflicts and unresolved records
SELECT source_system, source_record_id, status, candidate_ids, reason
FROM mapping_history
WHERE status IN ('CONFLICT', 'UNRESOLVED');

-- Rejected rows and reasons
SELECT source_file, source_row_number, reason FROM rejects;

-- Operational history
SELECT run_id, status, input_count, mapped_count, unresolved_count,
       rejected_count, error_message
FROM pipeline_runs ORDER BY started_at;
```

## Idempotency, retries, and backfills
A SHA-256 hash of the input file content plus its source-file label is stored in
`processed_batches`. Re-running the same unchanged file is recorded as `SKIPPED` and
does not duplicate source records or mapping history. A changed file is a new batch.
Each batch is committed atomically: a failed transaction is rolled back, the run is
marked `FAILED`, and retrying the same input is safe. To backfill, run a historical
file with a distinct source-file label or changed content; preserve the original files.
The database is append-oriented for mapping decisions; `current_mappings` is a view
over the latest history.

## Validation and evidence
The tests cover happy path, empty input, malformed CSV rows, missing required values,
duplicate source keys, schema drift, ambiguous candidates, conflicting alias evidence,
and idempotent reruns. The CLI prints a JSON run report and exports decision/reject CSVs.
The reconciliation report compares parsed rows with mapped, unresolved, and rejected
counts. Counts can differ from physical CSV lines because duplicate headers/records and
malformed rows are explicitly quarantined.

## Performance
The generator supports 100,000+ rows with a fixed seed. Matching uses indexed SQLite
lookups and in-memory reference indexes. The generator is streaming and does not require
loading the full generated dataset into memory. A practical next optimization for much
larger volumes is chunked/bulk inserts with `executemany`, plus profiling and indexes
on normalized email/phone and alias keys (the starter already indexes alias keys).

## Design trade-offs and limitations
- Conservative two-signal matching reduces false positives but may increase unresolved rows.
- No fuzzy name matching is performed; names alone are unsafe for identity resolution.
- Canonical reference data is loaded from CSV and upserted; production systems should
  version/reference it and validate ownership and effective dates.
- SQLite is suitable for a local take-home vertical slice, not high-concurrency production.
- The project does not call external APIs and contains synthetic data only.

## AI-use disclosure
Keep a complete, accurate record of every prompt sent to each AI tool in
`docs/ai_prompt_history.md`. In `docs/ai_usage_disclosure.md`, describe which suggestions
you accepted, changed, rejected, and personally tested. Do not claim validation you did
not perform. Review and understand all code before submission.

## GitHub submission checklist
1. Create a new **public** repository dedicated to this assignment.
2. Commit source, tests, synthetic inputs, docs, and sample outputs.
3. Use meaningful commits that reflect actual work; do not fabricate commit history.
4. Do not commit credentials, personal data, or proprietary material.
5. Push the final version before the 72-hour deadline and submit the repository URL
   from the same email address used during recruitment.
