"""Validate a manually acquired STATS19 snapshot without changing source files."""
import argparse
import csv
import hashlib
import json
from pathlib import Path
import time
from pipeline_common import new_run_id, publish_run

FILES = {name: f"raw/{name}/{name}_last5years.csv"
         for name in ["collisions", "vehicles", "casualties"]}
SOURCE_URL = "https://www.gov.uk/government/statistical-data-sets/road-safety-open-data"


def compute_sha256(file_path):
    """Compute a SHA-256 checksum for duplicate/change detection."""
    sha256 = hashlib.sha256()

    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            sha256.update(chunk)

    return sha256.hexdigest()


def read_csv_header(file_path):
    """Read the CSV header to create a source schema snapshot."""
    with open(file_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        return next(reader)


def run(bronze, output, run_id=None):
    bronze, output = Path(bronze), Path(output)
    run_id = run_id or new_run_id()
    destination = output / "runs" / run_id
    destination.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    report = {"run_id": run_id, "status": "running", "source_url": SOURCE_URL,
              "datasets": [], "format": "CSV", "raw_files_modified": False}
    try:
        pointer = output / "latest_successful.json"
        if pointer.exists():
            previous_path = Path(json.loads(pointer.read_text())["path"]) / "quality_report.json"
        else:
            previous_path = bronze / "metadata/ingestion_metadata.json"
        previous = json.loads(previous_path.read_text()) if previous_path.exists() else {}
        hashes = {item["name"]: item.get("sha256") for item in previous.get("datasets", [])}
        for name, relative in FILES.items():
            path = bronze / relative
            if not path.is_file():
                raise FileNotFoundError(f"Missing {name} source: {path}")
            if not path.stat().st_size:
                raise ValueError(f"Empty source: {path}")
            columns = read_csv_header(path)
            if len(columns) != len(set(columns)):
                raise ValueError(f"Duplicate column names: {path}")
            required = {"collision_index", "collision_year"}
            if name != "collisions":
                required.add("vehicle_reference")
            if name == "casualties":
                required.add("casualty_reference")
            if not required.issubset(columns):
                raise ValueError(f"Missing source columns in {name}: {required - set(columns)}")
            checksum = compute_sha256(path)
            report["datasets"].append({
                "name": name, "file": relative, "size_bytes": path.stat().st_size,
                "sha256": checksum, "columns": columns, "column_count": len(columns),
                "change_status": "unchanged" if hashes.get(name) == checksum else "new_or_changed"})
            print(f"Bronze: {name} checked ({path.stat().st_size:,} bytes)", flush=True)
        guide = bronze / "reference/stats19_data_guide.xlsx"
        if not guide.is_file() or not guide.stat().st_size:
            raise ValueError(f"Missing or empty reference guide: {guide}")
        report["reference"] = {"file": "reference/stats19_data_guide.xlsx",
                               "sha256": compute_sha256(guide), "size_bytes": guide.stat().st_size}
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - started, 2)
        publish_run(output, destination, report)
    return report


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, default=root / "data/bronze/stats19")
    parser.add_argument("--output", type=Path, default=root / "data/pipeline/dev/bronze")
    args = parser.parse_args()
    result = run(args.bronze, args.output)
    print(f"Bronze status: {result['status']}; run: {result['run_id']}")
