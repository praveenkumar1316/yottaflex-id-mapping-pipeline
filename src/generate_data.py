import argparse
import csv
import random
from pathlib import Path

def generate(rows: int, seed: int, output: str):
    rng = random.Random(seed)
    path = Path(output)
    path.parent.mkdir(parents=True, exist_ok=True)
    systems = ["crm", "billing", "ecommerce"]
    reference = {
        1: ("Aarav Sharma", "aarav@example.test", "9000000001"),
        2: ("Diya Reddy", "diya@example.test", "9000000002"),
        3: ("Arjun Rao", "arjun@example.test", "9000000003"),
        4: ("Meera Iyer", "meera@example.test", "9000000004"),
        5: ("Kabir Khan", "kabir@example.test", "9000000005"),
    }
    aliases = {("crm", 1): "CRM-001", ("billing", 2): "BILL-002",
               ("ecommerce", 3): "ECOM-003", ("crm", 4): "CRM-004",
               ("billing", 5): "BILL-005"}
    with path.open("w", newline="", encoding="utf-8") as f:
        fields = ["source_system","source_record_id","source_identifier","name","email","phone","source_updated_at"]
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for i in range(rows):
            cid = (i % 5) + 1
            system = systems[i % len(systems)]
            mode = i % 10
            name, email, phone = reference[cid]
            identifier = aliases.get((system, cid), f"NEW-{i:07d}")
            if mode == 1:  # rule-based case: unknown source ID, two independent signals
                identifier = f"NEW-{i:07d}"
            elif mode == 2:  # unresolved: no usable reference evidence
                identifier, email, phone = f"UNKNOWN-{i}", f"missing{i}@example.test", ""
            writer.writerow({
                "source_system": system, "source_record_id": f"{system}-{i:08d}",
                "source_identifier": identifier, "name": f"Customer {cid}",
                "email": email, "phone": phone,
                "source_updated_at": f"2026-01-{rng.randint(1, 28):02d}T{rng.randint(0, 23):02d}:00:00Z"
            })
    return path

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--rows", type=int, default=100000)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--output", default="data/generated/source_records.csv")
    a = p.parse_args()
    print(f"Generated {generate(a.rows, a.seed, a.output)} ({a.rows} rows, seed={a.seed})")

if __name__ == "__main__":
    main()
