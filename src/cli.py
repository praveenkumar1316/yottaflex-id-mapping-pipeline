import argparse
import json
import logging
from pathlib import Path
from .pipeline import run_pipeline

def main():
    parser = argparse.ArgumentParser(description="Source-to-canonical ID mapping pipeline")
    parser.add_argument("--input", required=True, help="Source CSV path")
    parser.add_argument("--canonical", default="data/reference/canonical_entities.csv")
    parser.add_argument("--aliases", default="data/reference/canonical_aliases.csv")
    parser.add_argument("--db", default="outputs/mapping.db")
    parser.add_argument("--output-dir", default="outputs")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    report = run_pipeline(args.input, args.db, args.canonical, args.aliases, args.output_dir)
    print(json.dumps(report, indent=2))

if __name__ == "__main__":
    main()
