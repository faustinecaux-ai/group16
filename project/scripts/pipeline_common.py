"""Small shared helpers for durable reports and validated publication."""
import json
from pathlib import Path
import tempfile
from datetime import datetime, timezone


def new_run_id():
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")


def write_json(path, value):
    """Replace a JSON file atomically, without exposing partially written content."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, suffix=".tmp",
                                     encoding="utf-8", delete=False) as handle:
        temporary = Path(handle.name)
        try:
            json.dump(value, handle, indent=2, allow_nan=False)
            handle.write("\n")
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    temporary.replace(path)


def publish_run(output, destination, report):
    """Store evidence before making a passed run discoverable by consumers."""
    destination = Path(destination)
    write_json(destination / "quality_report.json", report)
    if report["status"] != "passed":
        return False
    write_json(Path(output) / "latest_successful.json", {
        "run_id": report["run_id"], "path": str(destination.resolve())})
    return True


def load_validated_run(output):
    pointer = json.loads((Path(output) / "latest_successful.json").read_text())
    destination = Path(pointer["path"])
    report = json.loads((destination / "quality_report.json").read_text())
    if report["status"] != "passed" or report["run_id"] != pointer["run_id"]:
        raise ValueError("Published run and quality report do not agree")
    return destination, report


def create_spark(name):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName(name)
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.sql.session.timeZone", "Europe/London")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.ansi.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark
