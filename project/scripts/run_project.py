"""Run Bronze -> Silver -> Gold, stopping at the first failed quality gate."""
import argparse
import fcntl
import json
from pathlib import Path
import time

import ingest_bronze
import build_silver
import build_gold
from pipeline_common import create_spark, new_run_id, publish_run


def run(bronze, silver, gold, operations, spark=None):
    bronze, silver, gold, operations = map(Path, [bronze, silver, gold, operations])
    operations.mkdir(parents=True, exist_ok=True)
    with (operations / ".pipeline.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("Another pipeline is using these output directories") from None
        run_id = new_run_id()
        destination = operations / "runs" / run_id
        destination.mkdir(parents=True, exist_ok=False)
        started = time.monotonic()
        report = {"run_id": run_id, "status": "running", "stages": {}}
        owns_spark = spark is None
        try:
            report["stages"]["bronze"] = ingest_bronze.run(bronze, operations / "bronze", run_id)
            spark = spark or create_spark("STATS19 End-to-End")
            report["stages"]["silver"] = build_silver.run(
                bronze, silver, spark, run_id, report["stages"]["bronze"])
            if report["stages"]["silver"]["status"] != "passed":
                raise ValueError("Silver needs review; Gold was not run")
            report["stages"]["gold"] = build_gold.run(silver / "runs" / run_id, gold, spark, run_id)
            report["status"] = "passed"
        except Exception as error:
            report["status"] = "failed"
            report["error"] = str(error)
            raise
        finally:
            report["elapsed_seconds"] = round(time.monotonic() - started, 2)
            if owns_spark and spark is not None:
                spark.stop()
            publish_run(operations, destination, report)
            print(f"Pipeline: {report['status']}; report: {destination / 'quality_report.json'}", flush=True)
        return report


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--bronze", type=Path, help="Override the shared input snapshot")
    parser.add_argument("--output-root", type=Path, help="Override all outputs, e.g. for tests")
    args = parser.parse_args()
    settings = json.loads((root / "config" / f"{args.env}.json").read_text())
    bronze = args.bronze or root / settings["bronze"]
    if args.output_root:
        silver, gold, operations = [args.output_root / name for name in ["silver", "gold", "pipeline"]]
    else:
        silver, gold, operations = [root / settings[name] for name in ["silver", "gold", "pipeline"]]
    run(bronze, silver, gold, operations)
