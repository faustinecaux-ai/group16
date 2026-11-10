"""Measure the same collision aggregation over CSV and Parquet, without caching."""
import argparse
from contextlib import redirect_stdout
import json
from pathlib import Path
import statistics
import time

from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pipeline_common import create_spark, load_validated_run, write_json


def run(bronze, silver, output):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    silver_path, report = load_validated_run(silver)
    schema = StructType.fromJson(report["tables"]["collisions"]["input_schema"])
    spark = create_spark("STATS19 CSV and Parquet measurement")
    times = {"csv": [], "parquet": []}
    expected = None
    try:
        for iteration in range(3):
            order = ["csv", "parquet"] if iteration % 2 == 0 else ["parquet", "csv"]
            for kind in order:
                started = time.monotonic()
                if kind == "csv":
                    df = spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(
                        str(Path(bronze) / "raw/collisions/collisions_last5years.csv"))
                else:
                    df = spark.read.parquet(str(silver_path / "collisions"))
                query = df.select("collision_year", "collision_severity", "number_of_casualties").groupBy(
                    "collision_year").agg(F.count("*").alias("collisions"),
                                          F.sum("number_of_casualties").alias("casualties"),
                                          F.sum(F.col("collision_severity").isin(1, 2).cast("long")).alias("fatal_or_serious"))
                rows = sorted((r.collision_year, r.collisions, r.casualties, r.fatal_or_serious) for r in query.collect())
                times[kind].append(round(time.monotonic() - started, 4))
                if expected is None:
                    expected = rows
                if rows != expected:
                    raise ValueError("CSV and Parquet query results differ")
                if iteration == 0:
                    with (output / f"{kind}_plan.txt").open("w") as handle, redirect_stdout(handle):
                        query.explain(mode="formatted")
        result = {"silver_run_id": report["run_id"], "seconds": times,
                  "median_seconds": {k: statistics.median(v) for k, v in times.items()},
                  "results_equal": True, "yearly_totals": expected,
                  "method": "Same three-column aggregation, explicit CSV schema, three alternating trials, no Spark cache",
                  "limitations": "Local VM, warm-up and OS page cache uncontrolled; not a general scalability benchmark"}
        write_json(output / "performance.json", result)
        print(json.dumps(result, indent=2))
        return result
    finally:
        spark.stop()


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", choices=["dev", "prod"], default="dev")
    parser.add_argument("--output", type=Path, default=root / "reports")
    args = parser.parse_args()
    settings = json.loads((root / "config" / f"{args.env}.json").read_text())
    run(root / settings["bronze"], root / settings["silver"], args.output)
