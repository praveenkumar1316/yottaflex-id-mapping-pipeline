from __future__ import annotations

import csv
import hashlib
import json
import logging
import re
import sqlite3
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

LOGGER = logging.getLogger("id_mapping")
EXPECTED_COLUMNS = {
    "source_system", "source_record_id", "source_identifier",
    "name", "email", "phone", "source_updated_at"
}
REQUIRED_COLUMNS = {"source_system", "source_record_id", "source_identifier"}
SCHEMA_SQL = """
PRAGMA foreign_keys = ON;
CREATE TABLE IF NOT EXISTS canonical_entities (
    canonical_id TEXT PRIMARY KEY,
    canonical_name TEXT NOT NULL,
    email TEXT,
    phone TEXT
);
CREATE TABLE IF NOT EXISTS canonical_aliases (
    source_system TEXT NOT NULL,
    normalized_identifier TEXT NOT NULL,
    canonical_id TEXT NOT NULL,
    PRIMARY KEY (source_system, normalized_identifier, canonical_id),
    FOREIGN KEY (canonical_id) REFERENCES canonical_entities(canonical_id)
);
CREATE INDEX IF NOT EXISTS idx_alias_key
    ON canonical_aliases(source_system, normalized_identifier);
CREATE TABLE IF NOT EXISTS source_records (
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    source_identifier TEXT NOT NULL,
    name TEXT,
    email TEXT,
    phone TEXT,
    source_updated_at TEXT,
    source_file TEXT NOT NULL,
    first_seen_run_id TEXT NOT NULL,
    PRIMARY KEY (source_system, source_record_id)
);
CREATE TABLE IF NOT EXISTS mapping_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_system TEXT NOT NULL,
    source_record_id TEXT NOT NULL,
    canonical_id TEXT,
    status TEXT NOT NULL,
    match_method TEXT NOT NULL,
    candidate_ids TEXT NOT NULL,
    reason TEXT,
    source_file TEXT NOT NULL,
    run_id TEXT NOT NULL,
    decided_at TEXT NOT NULL,
    event_key TEXT NOT NULL UNIQUE
);
CREATE INDEX IF NOT EXISTS idx_history_source
    ON mapping_history(source_system, source_record_id, history_id);
CREATE TABLE IF NOT EXISTS rejects (
    reject_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_file TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    raw_record TEXT NOT NULL,
    reason TEXT NOT NULL,
    run_id TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    source_file TEXT NOT NULL,
    input_hash TEXT NOT NULL,
    status TEXT NOT NULL,
    started_at TEXT NOT NULL,
    ended_at TEXT,
    input_count INTEGER DEFAULT 0,
    mapped_count INTEGER DEFAULT 0,
    unresolved_count INTEGER DEFAULT 0,
    conflict_count INTEGER DEFAULT 0,
    rejected_count INTEGER DEFAULT 0,
    duplicate_count INTEGER DEFAULT 0,
    error_message TEXT
);
CREATE TABLE IF NOT EXISTS processed_batches (
    batch_key TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    processed_at TEXT NOT NULL
);
DROP VIEW IF EXISTS current_mappings;
CREATE VIEW current_mappings AS
SELECT h.source_system, h.source_record_id, h.canonical_id,
       h.status, h.match_method, h.candidate_ids, h.reason,
       h.source_file, h.run_id, h.decided_at
FROM mapping_history h
JOIN (
    SELECT source_system, source_record_id, MAX(history_id) AS max_id
    FROM mapping_history GROUP BY source_system, source_record_id
) latest ON latest.max_id = h.history_id
WHERE h.status = 'MAPPED';
"""

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def normalize_identifier(value: str | None) -> str:
    return re.sub(r"[^A-Z0-9]", "", (value or "").strip().upper())

def normalize_email(value: str | None) -> str:
    return (value or "").strip().casefold()

def normalize_phone(value: str | None) -> str:
    digits = re.sub(r"\D", "", value or "")
    return digits[-10:] if len(digits) >= 10 else digits

def connect(db_path: str | Path) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    return conn

def load_reference_data(conn: sqlite3.Connection, canonical_path: str | Path,
                        aliases_path: str | Path) -> None:
    with open(canonical_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            if not row.get("canonical_id") or not row.get("canonical_name"):
                raise ValueError("Canonical reference row missing canonical_id/canonical_name")
            conn.execute(
                """INSERT INTO canonical_entities(canonical_id, canonical_name, email, phone)
                   VALUES (?, ?, ?, ?)
                   ON CONFLICT(canonical_id) DO UPDATE SET
                   canonical_name=excluded.canonical_name, email=excluded.email, phone=excluded.phone""",
                (row["canonical_id"].strip(), row["canonical_name"].strip(),
                 row.get("email", "").strip(), row.get("phone", "").strip()))
    with open(aliases_path, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            key = normalize_identifier(row.get("source_identifier"))
            if not row.get("source_system") or not key or not row.get("canonical_id"):
                raise ValueError("Alias reference row missing source_system/source_identifier/canonical_id")
            conn.execute(
                """INSERT OR IGNORE INTO canonical_aliases
                   (source_system, normalized_identifier, canonical_id) VALUES (?, ?, ?)""",
                (row["source_system"].strip(), key, row["canonical_id"].strip()))
    conn.commit()

def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()

def _read_csv(path: Path):
    with path.open(newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("CSV is missing a header row")
        extras = set(reader.fieldnames) - EXPECTED_COLUMNS
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            raise ValueError(f"Missing required columns: {sorted(missing)}")
        if extras:
            raise ValueError(f"Unexpected schema columns: {sorted(extras)}")
        for row_number, row in enumerate(reader, start=2):
            malformed = None in row or any(value is None for value in row.values())
            row = {key: (value if value is not None else "") for key, value in row.items() if key is not None}
            if malformed:
                row["_malformed"] = "CSV row has fewer or more fields than the header"
            yield row_number, row

def _reference_indexes(conn):
    alias_index = defaultdict(set)
    for r in conn.execute("SELECT source_system, normalized_identifier, canonical_id FROM canonical_aliases"):
        alias_index[(r["source_system"], r["normalized_identifier"])].add(r["canonical_id"])
    email_index, phone_index = defaultdict(set), defaultdict(set)
    for r in conn.execute("SELECT canonical_id, email, phone FROM canonical_entities"):
        if normalize_email(r["email"]):
            email_index[normalize_email(r["email"])].add(r["canonical_id"])
        if normalize_phone(r["phone"]):
            phone_index[normalize_phone(r["phone"])].add(r["canonical_id"])
    return alias_index, email_index, phone_index

def resolve_record(row, indexes):
    alias_index, email_index, phone_index = indexes
    system = row["source_system"].strip()
    identifier = normalize_identifier(row["source_identifier"])
    alias_candidates = alias_index.get((system, identifier), set())
    if len(alias_candidates) > 1:
        return None, "CONFLICT", "EXACT_ALIAS", sorted(alias_candidates), "Alias key maps to multiple canonical IDs"
    if len(alias_candidates) == 1:
        return next(iter(alias_candidates)), "MAPPED", "EXACT_ALIAS", sorted(alias_candidates), None

    email_candidates = email_index.get(normalize_email(row.get("email")), set())
    phone_candidates = phone_index.get(normalize_phone(row.get("phone")), set())
    evidence = email_candidates | phone_candidates
    if len(evidence) > 1:
        return None, "CONFLICT", "RULE_BASED", sorted(evidence), "Email/phone evidence points to different canonical IDs"
    if len(evidence) == 1:
        candidate = next(iter(evidence))
        signals = int(candidate in email_candidates) + int(candidate in phone_candidates)
        if signals >= 2:
            return candidate, "MAPPED", "RULE_BASED", [candidate], None
        return None, "UNRESOLVED", "RULE_BASED", [candidate], "Only one matching attribute; two independent signals required"
    return None, "UNRESOLVED", "NONE", [], "No exact alias or matching email/phone evidence"

def run_pipeline(input_path: str | Path, db_path: str | Path,
                 canonical_path: str | Path, aliases_path: str | Path,
                 output_dir: str | Path = "outputs") -> dict:
    input_path, db_path = Path(input_path), Path(db_path)
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    run_id, started = str(uuid.uuid4()), now_iso()
    digest = _file_hash(input_path)
    batch_key = hashlib.sha256(f"{input_path.name}:{digest}".encode()).hexdigest()
    conn = connect(db_path)
    conn.execute(
        """INSERT INTO pipeline_runs(run_id, source_file, input_hash, status, started_at)
           VALUES (?, ?, ?, 'RUNNING', ?)""",
        (run_id, input_path.name, digest, started))
    conn.commit()
    counts = dict(input_count=0, mapped_count=0, unresolved_count=0,
                  conflict_count=0, rejected_count=0, duplicate_count=0)
    try:
        load_reference_data(conn, canonical_path, aliases_path)
        if conn.execute("SELECT 1 FROM processed_batches WHERE batch_key=?", (batch_key,)).fetchone():
            conn.execute(
                """UPDATE pipeline_runs SET status='SKIPPED', ended_at=?,
                   error_message='Identical input batch already processed' WHERE run_id=?""",
                (now_iso(), run_id))
            conn.commit()
            report = {"run_id": run_id, "status": "SKIPPED", **counts,
                      "message": "Identical input batch already processed"}
            _write_report(output_dir, report, [], [], [])
            return report

        indexes = _reference_indexes(conn)
        seen_keys = set()
        mapped, unresolved, rejected = [], [], []
        # Entire batch is transactional: any unexpected failure rolls back data changes.
        conn.execute("BEGIN")
        for row_number, row in _read_csv(input_path):
            counts["input_count"] += 1
            raw = json.dumps(row, ensure_ascii=False, sort_keys=True)
            if row.get("_malformed"):
                rejected.append({"source_file": input_path.name, "source_row_number": row_number,
                                 "raw_record": raw, "reason": row["_malformed"], "run_id": run_id})
                continue
            missing = [c for c in REQUIRED_COLUMNS if not (row.get(c) or "").strip()]
            if missing:
                reason = f"Missing required field(s): {', '.join(sorted(missing))}"
                rejected.append({"source_file": input_path.name, "source_row_number": row_number,
                                 "raw_record": raw, "reason": reason, "run_id": run_id})
                continue
            key = (row["source_system"].strip(), row["source_record_id"].strip())
            if key in seen_keys:
                counts["duplicate_count"] += 1
                rejected.append({"source_file": input_path.name, "source_row_number": row_number,
                                 "raw_record": raw, "reason": "Duplicate source_system + source_record_id within batch",
                                 "run_id": run_id})
                continue
            seen_keys.add(key)
            # A key seen in an earlier successful batch is an update, not a duplicate
            # within this batch. Preserve its original lineage while updating attributes.
            canonical_id, status, method, candidates, reason = resolve_record(row, indexes)
            conn.execute(
                """INSERT INTO source_records VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(source_system, source_record_id) DO UPDATE SET
                   source_identifier=excluded.source_identifier, name=excluded.name,
                   email=excluded.email, phone=excluded.phone,
                   source_updated_at=excluded.source_updated_at, source_file=excluded.source_file""",
                (key[0], key[1], row["source_identifier"].strip(), row.get("name", "").strip(),
                 row.get("email", "").strip(), row.get("phone", "").strip(),
                 row.get("source_updated_at", "").strip(), input_path.name, run_id))
            event_key = hashlib.sha256(
                f"{key[0]}|{key[1]}|{canonical_id}|{status}|{method}|{','.join(candidates)}".encode()
            ).hexdigest()
            conn.execute(
                """INSERT OR IGNORE INTO mapping_history
                   (source_system, source_record_id, canonical_id, status, match_method,
                    candidate_ids, reason, source_file, run_id, decided_at, event_key)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (key[0], key[1], canonical_id, status, method, json.dumps(candidates),
                 reason, input_path.name, run_id, now_iso(), event_key))
            decision = {"source_system": key[0], "source_record_id": key[1],
                        "canonical_id": canonical_id or "", "status": status,
                        "match_method": method, "candidate_ids": json.dumps(candidates),
                        "reason": reason or "", "source_file": input_path.name, "run_id": run_id}
            if status == "MAPPED":
                counts["mapped_count"] += 1
                mapped.append(decision)
            else:
                unresolved.append(decision)
                counts["conflict_count" if status == "CONFLICT" else "unresolved_count"] += 1

        for item in rejected:
            conn.execute(
                """INSERT INTO rejects(source_file, source_row_number, raw_record, reason, run_id)
                   VALUES (?, ?, ?, ?, ?)""",
                (item["source_file"], item["source_row_number"], item["raw_record"], item["reason"], run_id))
        counts["rejected_count"] = len(rejected)
        conn.execute(
            """INSERT INTO processed_batches(batch_key, run_id, processed_at) VALUES (?, ?, ?)""",
            (batch_key, run_id, now_iso()))
        conn.execute(
            """UPDATE pipeline_runs SET status='SUCCESS', ended_at=?, input_count=?,
               mapped_count=?, unresolved_count=?, conflict_count=?, rejected_count=?,
               duplicate_count=? WHERE run_id=?""",
            (now_iso(), counts["input_count"], counts["mapped_count"], counts["unresolved_count"],
             counts["conflict_count"], counts["rejected_count"], counts["duplicate_count"], run_id))
        conn.commit()
        report = {"run_id": run_id, "status": "SUCCESS", **counts}
        _write_report(output_dir, report, mapped, unresolved, rejected)
        return report
    except Exception as exc:
        conn.rollback()
        conn.execute(
            "UPDATE pipeline_runs SET status='FAILED', ended_at=?, error_message=? WHERE run_id=?",
            (now_iso(), str(exc), run_id))
        conn.commit()
        LOGGER.exception("Pipeline failed; run_id=%s", run_id)
        report = {"run_id": run_id, "status": "FAILED", **counts, "error": str(exc)}
        _write_report(output_dir, report, [], [], [])
        raise
    finally:
        conn.close()

def _write_csv(path: Path, rows: list[dict], fields: list[str]):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

def _write_report(output_dir, report, mapped, unresolved, rejected):
    _write_csv(output_dir / "mappings.csv", mapped,
               ["source_system","source_record_id","canonical_id","status","match_method","candidate_ids","reason","source_file","run_id"])
    _write_csv(output_dir / "unresolved.csv", unresolved,
               ["source_system","source_record_id","canonical_id","status","match_method","candidate_ids","reason","source_file","run_id"])
    _write_csv(output_dir / "rejects.csv", rejected,
               ["source_file","source_row_number","raw_record","reason","run_id"])
    (output_dir / "run_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
