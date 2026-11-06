# Design and Trade-off Note

1. **Exact before rules:** explicit crosswalks are strongest evidence and are evaluated first.
2. **Two independent attributes:** rule-based mapping requires both normalized email and
   phone to identify the same canonical ID. One signal is insufficient.
3. **Conflict over guessing:** multiple candidates are surfaced as `CONFLICT`; no winner
   is selected by row order or arbitrary ranking.
4. **Append-only decisions:** mapping decisions are retained in `mapping_history`.
   The `current_mappings` view exposes latest accepted decisions.
5. **Quarantine:** malformed/missing/duplicate source rows are recorded with row number,
   raw JSON representation, reason, source file, and run ID.
6. **Atomic batch:** source rows, decisions, rejects, and processed-batch marker commit
   together. Failure rolls back the batch; the failed run remains observable.
7. **Idempotency:** identical content and filename are skipped using SHA-256.
8. **Local-first:** SQLite avoids cloud credentials and makes the project reproducible.
9. **Limitations:** no fuzzy matching, effective-dated aliases, streaming ingestion,
   distributed execution, or human review UI. These are reasonable future extensions.
