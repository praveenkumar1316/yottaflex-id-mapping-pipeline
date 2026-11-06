# Data Dictionary

All names use `snake_case`; timestamps are ISO-8601 UTC strings.

## Source CSV: `source_records.csv`
| Column | Type | Required | Meaning |
|---|---|---:|---|
| source_system | string | yes | originating system |
| source_record_id | string | yes | stable source-side record key |
| source_identifier | string | yes | identifier to resolve |
| name | string | no | descriptive attribute; not used alone for matching |
| email | string | no | normalized for rule matching |
| phone | string | no | normalized to final 10 digits when available |
| source_updated_at | ISO-8601 string | no | source event/update time |

## Reference CSVs
`canonical_entities.csv`: `canonical_id` (unique non-null string PK),
`canonical_name` (non-null), `email`, `phone`.

`canonical_aliases.csv`: `source_system`, `source_identifier`, `canonical_id`.
The normalized tuple `(source_system, source_identifier)` may intentionally map to
multiple canonical IDs so the pipeline can expose and quarantine ambiguity.

## Derived outputs
- `status`: `MAPPED`, `UNRESOLVED`, or `CONFLICT`.
- `match_method`: `EXACT_ALIAS`, `RULE_BASED`, or `NONE`.
- `candidate_ids`: JSON array of canonical candidates.
- `reason`: human-readable decision/rejection explanation.
- `run_id`: UUID identifying one pipeline execution.
- `source_file`: input filename used for lineage.
