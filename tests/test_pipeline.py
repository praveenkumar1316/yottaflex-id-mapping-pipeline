import csv
import sqlite3
from pathlib import Path

import pytest

from src.pipeline import run_pipeline, connect

ROOT = Path(__file__).resolve().parents[1]
CANON = ROOT / "data/reference/canonical_entities.csv"
ALIASES = ROOT / "data/reference/canonical_aliases.csv"
FIELDS = ["source_system","source_record_id","source_identifier","name","email","phone","source_updated_at"]

def write_csv(path, rows, fields=FIELDS):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

def row(**kwargs):
    base = dict(source_system="crm", source_record_id="x1", source_identifier="CRM-001",
                name="Aarav", email="aarav@example.test", phone="9000000001",
                source_updated_at="2026-11-01T10:00:00Z")
    base.update(kwargs)
    return base

def execute(tmp_path, rows, fields=FIELDS, name="input.csv"):
    inp = tmp_path / name
    write_csv(inp, rows, fields)
    db = tmp_path / "db.sqlite"
    out = tmp_path / "out"
    report = run_pipeline(inp, db, CANON, ALIASES, out)
    return report, db, out, inp

def test_happy_path_exact_and_rule_based(tmp_path):
    rows = [
        row(source_record_id="exact", source_identifier="CRM-001"),
        row(source_record_id="rule", source_identifier="NEW-ID"),
    ]
    report, db, out, _ = execute(tmp_path, rows)
    assert report["status"] == "SUCCESS"
    assert report["mapped_count"] == 2
    with sqlite3.connect(db) as c:
        methods = {r[0] for r in c.execute("SELECT match_method FROM mapping_history")}
    assert methods == {"EXACT_ALIAS", "RULE_BASED"}

def test_empty_input(tmp_path):
    report, *_ = execute(tmp_path, [])
    assert report["status"] == "SUCCESS"
    assert report["input_count"] == 0

def test_missing_required_field_quarantined(tmp_path):
    report, db, _, _ = execute(tmp_path, [row(source_identifier="")])
    assert report["rejected_count"] == 1
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM rejects").fetchone()[0] == 1

def test_duplicate_source_key_quarantined(tmp_path):
    report, *_ = execute(tmp_path, [row(), row(source_identifier="CRM-001")])
    assert report["duplicate_count"] == 1
    assert report["rejected_count"] == 1

def test_ambiguous_alias_conflict(tmp_path):
    report, db, _, _ = execute(tmp_path, [row(source_identifier="DUP-777")])
    assert report["conflict_count"] == 1
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT status FROM mapping_history").fetchone()[0] == "CONFLICT"

def test_rule_conflict_email_phone_disagree(tmp_path):
    report, db, _, _ = execute(tmp_path, [row(source_identifier="UNKNOWN",
        email="aarav@example.test", phone="9000000002")])
    assert report["conflict_count"] == 1

def test_one_signal_is_unresolved(tmp_path):
    report, db, _, _ = execute(tmp_path, [row(source_identifier="UNKNOWN", phone="")])
    assert report["unresolved_count"] == 1

def test_schema_change_rejected_as_batch(tmp_path):
    bad_fields = FIELDS + ["unexpected_column"]
    inp = tmp_path / "schema.csv"
    write_csv(inp, [row()], bad_fields)
    with pytest.raises(ValueError, match="Unexpected schema"):
        run_pipeline(inp, tmp_path/"db.sqlite", CANON, ALIASES, tmp_path/"out")

def test_malformed_csv_row_rejected_by_csv_parser(tmp_path):
    inp = tmp_path / "malformed.csv"
    inp.write_text(",".join(FIELDS) + "\n" + "crm,broken,CRM-001,Aarav\n", encoding="utf-8")
    # DictReader fills missing cells with None; required checks quarantine the row.
    report = run_pipeline(inp, tmp_path/"db.sqlite", CANON, ALIASES, tmp_path/"out")
    assert report["rejected_count"] == 1

def test_idempotent_rerun(tmp_path):
    report1, db, out, inp = execute(tmp_path, [row()])
    report2 = run_pipeline(inp, db, CANON, ALIASES, out)
    assert report1["status"] == "SUCCESS"
    assert report2["status"] == "SKIPPED"
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT COUNT(*) FROM mapping_history").fetchone()[0] == 1

def test_phone_format_normalization_rule_match(tmp_path):
    report, db, _, _ = execute(tmp_path, [row(source_identifier="UNKNOWN",
        email=" AARAV@EXAMPLE.TEST ", phone="+91 90000 00001")])
    assert report["mapped_count"] == 1

def test_invalid_reference_key_does_not_map(tmp_path):
    report, *_ = execute(tmp_path, [row(source_identifier="NOT-IN-ALIAS",
        email="nobody@example.test", phone="")])
    assert report["unresolved_count"] == 1

def test_changed_source_record_preserves_mapping_history(tmp_path):
    inp = tmp_path / "incremental.csv"
    db = tmp_path / "history.sqlite"
    out = tmp_path / "out"
    write_csv(inp, [row(source_record_id="history-key", source_identifier="CRM-001")])
    first = run_pipeline(inp, db, CANON, ALIASES, out)
    # Same source key, later changed source event: new identifier is ambiguous.
    write_csv(inp, [row(source_record_id="history-key", source_identifier="DUP-777")])
    second = run_pipeline(inp, db, CANON, ALIASES, out)
    assert first["mapped_count"] == 1
    assert second["conflict_count"] == 1
    with sqlite3.connect(db) as c:
        decisions = c.execute(
            "SELECT status FROM mapping_history WHERE source_record_id='history-key' ORDER BY history_id"
        ).fetchall()
    assert [d[0] for d in decisions] == ["MAPPED", "CONFLICT"]

def test_failed_batch_is_logged_and_corrected_retry_succeeds(tmp_path):
    inp = tmp_path / "retry.csv"
    db = tmp_path / "retry.sqlite"
    out = tmp_path / "out"
    write_csv(inp, [row()], FIELDS + ["unexpected"])
    with pytest.raises(ValueError, match="Unexpected schema"):
        run_pipeline(inp, db, CANON, ALIASES, out)
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT status FROM pipeline_runs").fetchone()[0] == "FAILED"
    write_csv(inp, [row(source_record_id="retry-ok")], FIELDS)
    retry = run_pipeline(inp, db, CANON, ALIASES, out)
    assert retry["status"] == "SUCCESS"
    assert retry["mapped_count"] == 1

def test_late_arriving_record_keeps_source_timestamp_and_lineage(tmp_path):
    late = row(source_record_id="late-event", source_updated_at="2025-01-01T00:00:00Z")
    report, db, _, _ = execute(tmp_path, [late])
    assert report["mapped_count"] == 1
    with sqlite3.connect(db) as c:
        stored = c.execute(
            "SELECT source_updated_at, source_file FROM source_records WHERE source_record_id='late-event'"
        ).fetchone()
    assert stored == ("2025-01-01T00:00:00Z", "input.csv")
