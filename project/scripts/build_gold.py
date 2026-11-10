"""Produce small, reconciled analytical tables from a validated Silver run."""
import argparse
from contextlib import redirect_stdout
import csv
import json
from pathlib import Path
import time

from pyspark.sql import functions as F
from pipeline_common import create_spark, new_run_id, publish_run, load_validated_run


DIMENSIONS = {
    "monthly": ["collision_year", "month"],
    "hourly": ["hour"],
    "weather": ["weather_conditions", "weather_conditions_label"],
    "lighting": ["light_conditions", "light_conditions_label"],
    "road_type": ["road_type", "road_type_label"],
    "speed_limit": ["speed_limit_mph_clean"],
    "weekend": ["is_weekend"],
    "collision_profiles": ["light_conditions", "light_conditions_label", "road_type",
                           "road_type_label", "speed_limit_mph_clean"],
}
DIMENSIONS["collision_profiles_by_year"] = ["collision_year"] + DIMENSIONS["collision_profiles"]


def summarise(collisions, dimensions):
    """The percentage denominator is reported collisions in each group."""
    grouped = collisions.groupBy(*dimensions).agg(
        F.count("*").alias("collision_count"),
        F.sum(F.col("collision_severity").isin(1, 2).cast("long")).alias("fatal_or_serious_collision_count"),
        F.sum("number_of_casualties").alias("casualty_count"),
        F.sum("fatal_casualty_count").alias("fatal_casualty_count"),
        F.sum("serious_casualty_count").alias("serious_casualty_count"),
        F.avg("observed_vehicle_count").alias("mean_vehicles_involved"))
    return grouped.withColumn("fatal_or_serious_percentage", F.round(
        F.col("fatal_or_serious_collision_count") * 100.0 / F.col("collision_count"), 3))


def summarise_vehicle_types(collisions, vehicles):
    # A collision involving two cars must count once in the car group.
    membership = vehicles.select("collision_index", "vehicle_type", "vehicle_type_label").distinct()
    return summarise(collisions.join(membership, "collision_index", "inner"),
                     ["vehicle_type", "vehicle_type_label"])


def add_profile_reference_shares(profiles):
    """Compare severe-case shares with shares of all recorded collisions, not traffic."""
    collision_share = F.col("collision_count") * 100.0 / F.col("reference_collision_count")
    severe_share = F.when(F.col("reference_severe_collision_count") > 0,
                         F.col("fatal_or_serious_collision_count") * 100.0 /
                         F.col("reference_severe_collision_count"))
    return (profiles.withColumn("share_of_all_collisions_percentage", F.round(collision_share, 3))
            .withColumn("share_of_all_fatal_or_serious_percentage", F.round(severe_share, 3))
            .withColumn("severe_minus_all_collision_share_pp", F.round(severe_share - collision_share, 3)))


def summarise_profiles(collisions, total_severe, total_collisions):
    profiles = (summarise(collisions, DIMENSIONS["collision_profiles"])
                .withColumn("reference_collision_count", F.lit(total_collisions))
                .withColumn("reference_severe_collision_count", F.lit(total_severe)))
    return add_profile_reference_shares(profiles)


def summarise_profiles_by_year(collisions):
    reference = collisions.groupBy("collision_year").agg(
        F.count("*").alias("reference_collision_count"),
        F.sum(F.col("collision_severity").isin(1, 2).cast("long")).alias("reference_severe_collision_count"))
    profiles = summarise(collisions, DIMENSIONS["collision_profiles_by_year"])
    return add_profile_reference_shares(profiles.join(reference, "collision_year", "inner"))


def summarise_monthly_sql(collisions):
    """Use Spark SQL for the calendar summary; other summaries use DataFrames."""
    collisions.createOrReplaceTempView("validated_stats19_collisions")
    return collisions.sparkSession.sql("""
        SELECT collision_year, month,
               COUNT(*) AS collision_count,
               SUM(CASE WHEN collision_severity IN (1, 2) THEN 1 ELSE 0 END)
                   AS fatal_or_serious_collision_count,
               SUM(number_of_casualties) AS casualty_count,
               SUM(fatal_casualty_count) AS fatal_casualty_count,
               SUM(serious_casualty_count) AS serious_casualty_count,
               AVG(observed_vehicle_count) AS mean_vehicles_involved,
               ROUND(CAST(100 AS DOUBLE) *
                   SUM(CASE WHEN collision_severity IN (1, 2) THEN 1 ELSE 0 END) / COUNT(*), 3)
                   AS fatal_or_serious_percentage
        FROM validated_stats19_collisions
        GROUP BY collision_year, month
    """)


def run(silver_path, output, spark=None, run_id=None):
    silver_path, output = Path(silver_path), Path(output)
    run_id = run_id or new_run_id()
    destination = output / "runs" / run_id
    destination.mkdir(parents=True, exist_ok=False)
    started = time.monotonic()
    report = {"run_id": run_id, "status": "running", "tables": {},
              "severity_basis": "Reported severity, not severity-adjusted estimates",
              "denominator": "Reported collisions within each group; not traffic exposure",
              "vehicle_groups": "Overlapping collision memberships; do not sum across types"}
    report["analytical_question"] = (
        "Which lighting, road-type and speed-limit profiles account for the largest "
        "numbers of fatal or serious reported collisions in Great Britain, 2021–2025?")
    owns_spark = spark is None
    cached = []
    try:
        silver_report = json.loads((silver_path / "quality_report.json").read_text())
        if silver_report["status"] != "passed":
            raise ValueError("Gold requires a passed Silver quality report")
        report["silver_run_id"] = silver_report["run_id"]
        report["silver_path"] = str(silver_path.resolve())
        spark = spark or create_spark("STATS19 Gold")
        collisions = spark.read.parquet(str(silver_path / "collisions_enriched")).cache()
        cached.append(collisions)
        vehicles = spark.read.parquet(str(silver_path / "vehicles"))
        totals = collisions.agg(
            F.count("*").alias("collisions"), F.sum("number_of_casualties").alias("casualties"),
            F.sum(F.col("collision_severity").isin(1, 2).cast("long")).alias("fatal_or_serious")
        ).first().asDict()
        if totals["collisions"] != silver_report["tables"]["collisions"]["rows"]:
            raise ValueError("Silver evidence and collision table row counts differ")
        report["source_totals"] = totals
        report["source_year_totals"] = {str(row.collision_year): {
            "collisions": row.collisions, "casualties": row.casualties,
            "fatal_or_serious": row.fatal_or_serious} for row in collisions.groupBy("collision_year").agg(
                F.count("*").alias("collisions"), F.sum("number_of_casualties").alias("casualties"),
                F.sum(F.col("collision_severity").isin(1, 2).cast("long")).alias("fatal_or_serious")).collect()}
        tables = {name: summarise(collisions, dims) for name, dims in DIMENSIONS.items()}
        tables["monthly"] = summarise_monthly_sql(collisions)
        tables["collision_profiles"] = summarise_profiles(collisions, totals["fatal_or_serious"], totals["collisions"])
        tables["collision_profiles_by_year"] = summarise_profiles_by_year(collisions)
        tables["vehicle_type"] = summarise_vehicle_types(collisions, vehicles)
        for name, df in tables.items():
            dimensions = DIMENSIONS.get(name, ["vehicle_type", "vehicle_type_label"])
            rows = df.orderBy(*dimensions).limit(10001).collect()
            if not rows or len(rows) > 10000:
                raise ValueError(f"Unexpected Gold summary size: {name}")
            observed = {"collisions": sum(r.collision_count for r in rows),
                        "casualties": sum(r.casualty_count for r in rows),
                        "fatal_or_serious": sum(r.fatal_or_serious_collision_count for r in rows)}
            if name != "vehicle_type" and observed != totals:
                raise ValueError(f"Gold totals do not reconcile: {name}")
            if name == "collision_profiles_by_year":
                yearly = {}
                for row in rows:
                    counts = yearly.setdefault(str(row.collision_year),
                                               {"collisions": 0, "casualties": 0, "fatal_or_serious": 0})
                    counts["collisions"] += row.collision_count
                    counts["casualties"] += row.casualty_count
                    counts["fatal_or_serious"] += row.fatal_or_serious_collision_count
                if yearly != report["source_year_totals"]:
                    raise ValueError("Yearly collision profiles do not reconcile to yearly Silver totals")
            if any(not 0 <= r.fatal_or_serious_percentage <= 100 for r in rows):
                raise ValueError(f"Invalid percentage: {name}")
            # These are intentionally small consumer tables; one file avoids tiny partitions.
            df.coalesce(1).write.mode("errorifexists").parquet(str(destination / name))
            reloaded = spark.read.parquet(str(destination / name))
            if reloaded.count() != len(rows):
                raise ValueError(f"Gold Parquet count mismatch: {name}")
            with (destination / f"{name}.csv").open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(handle, fieldnames=df.columns)
                writer.writeheader()
                writer.writerows(row.asDict() for row in rows)
            report["tables"][name] = {"rows": len(rows), "totals": observed,
                                      "reconciled_to_silver": name != "vehicle_type"}
            if name == "collision_profiles_by_year":
                report["tables"][name]["reconciled_by_year"] = True
            print(f"Gold: {name}, {len(rows)} groups", flush=True)
        with (destination / "monthly_plan.txt").open("w") as handle, redirect_stdout(handle):
            tables["monthly"].explain(mode="formatted")
        report["status"] = "passed"
    except Exception as error:
        report["status"] = "failed"
        report["error"] = str(error)
        raise
    finally:
        report["elapsed_seconds"] = round(time.monotonic() - started, 2)
        for df in cached:
            df.unpersist()
        if owns_spark and spark is not None:
            spark.stop()
        publish_run(output, destination, report)
    return report


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver", type=Path, default=root / "data/silver/dev")
    parser.add_argument("--output", type=Path, default=root / "data/gold/dev")
    args = parser.parse_args()
    path, _ = load_validated_run(args.silver)
    result = run(path, args.output)
    print(f"Gold status: {result['status']}; run: {result['run_id']}")
