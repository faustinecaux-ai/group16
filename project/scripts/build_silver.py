"""Build STATS19 Silver without changing Bronze. Run with Spark 3.5 spark-submit."""
import argparse
import hashlib
import json
from pathlib import Path
import time
import xml.etree.ElementTree as ET
import zipfile
from contextlib import redirect_stdout

from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pipeline_common import create_spark, new_run_id, publish_run


KEYS = {
    "collisions": ["collision_index"],
    "vehicles": ["collision_index", "vehicle_reference"],
    "casualties": ["collision_index", "casualty_reference"],
}
CATEGORIES = {
    "collisions": ["collision_severity", "weather_conditions", "light_conditions",
                   "road_surface_conditions", "road_type"],
    "vehicles": ["vehicle_type"],
    "casualties": ["casualty_severity"],
}
IMPORTANT_FIELDS = {
    "collisions": ["collision_year", "date", "time", "collision_severity", "longitude",
                   "latitude", "speed_limit", "number_of_vehicles", "number_of_casualties"],
    "vehicles": ["collision_year", "vehicle_type", "age_of_driver"],
    "casualties": ["collision_year", "vehicle_reference", "casualty_severity", "age_of_casualty"],
}
AGE_FIELDS = {"vehicles": "age_of_driver", "casualties": "age_of_casualty"}
VALID_SPEED_LIMITS = [20, 30, 40, 50, 60, 70]  # Official guide: public highway limits, mph.


def read_source(spark, path):
    """Infer numeric fields but preserve identifiers and HH:mm source text."""
    schema = spark.read.option("header", True).option("inferSchema", True).option(
        "mode", "FAILFAST").csv(str(path)).schema
    for field in schema:
        if field.name in ["collision_index", "collision_ref_no", "time"]:
            field.dataType = StringType()
    return spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(str(path))


def read_code_lists(path):
    """Read official XLSX code lists using the standard library (no Excel engine)."""
    ns = {"s": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    selected = {field for fields in CATEGORIES.values() for field in fields}
    selected.add("enhanced_casualty_severity")
    selected.update(AGE_FIELDS.values())
    selected.add("speed_limit")
    with zipfile.ZipFile(path) as workbook:
        strings = ["".join(node.itertext()) for node in ET.fromstring(
            workbook.read("xl/sharedStrings.xml")).findall("s:si", ns)]
        result = {}
        rows = ET.fromstring(workbook.read("xl/worksheets/sheet1.xml"))
        for row in rows.findall(".//s:sheetData/s:row", ns):
            values = {}
            for cell in row:
                node = cell.find("s:v", ns)
                value = node.text if node is not None else ""
                if cell.get("t") == "s":
                    value = strings[int(value)]
                column = "".join(c for c in cell.get("r") if c.isalpha())
                values[column] = value
            field, code, label = (values.get(c, "") for c in ["B", "C", "D"])
            if field in selected and code.lstrip("-").isdigit() and label:
                previous = result.setdefault(field, {}).get(int(code))
                if previous is not None and previous != label:
                    raise ValueError(f"Conflicting code list: {field}/{code}")
                result[field][int(code)] = label
    return result


def audit_keys(df, keys):
    missing = F.lit(False)
    for key in keys:
        missing = missing | F.col(key).isNull() | (F.trim(F.col(key).cast("string")) == "")
    counts = df.agg(F.count("*").alias("rows"),
                    F.coalesce(F.sum(missing.cast("long")), F.lit(0)).alias("missing_keys")).first().asDict()
    counts["duplicate_key_excess"] = counts["rows"] - df.select(*keys).distinct().count()
    return counts


def decode(df, fields, mappings):
    for field in fields:
        mapping = mappings[field]
        pairs = [F.lit(value) for pair in mapping.items() for value in pair]
        df = df.withColumn(field + "_label", F.create_map(*pairs)[F.col(field)])
    return df


def audit_fields(df, fields):
    """Count missing values together in one Spark action, including blank strings."""
    return df.agg(*[
        F.sum((F.col(field).isNull() | (F.trim(F.col(field).cast("string")) == ""))
              .cast("long")).alias(field) for field in fields
    ]).first().asDict()


def prepare_age(df, field):
    """Retain source ages; negative codes must not enter numeric age analyses."""
    age = F.col(field)
    clean = field + "_clean"
    df = df.withColumn(clean, F.when(age >= 0, age))
    metrics = df.agg(
        F.sum((age == -1).cast("long")).alias("source_missing_or_out_of_range_code"),
        F.sum((age < -1).cast("long")).alias("unexpected_negative_values"),
        F.sum(age.isNull().cast("long")).alias("source_nulls"),
        F.sum(F.col(clean).isNull().cast("long")).alias("clean_nulls"),
        F.min(clean).alias("minimum_recorded_age"),
        F.max(clean).alias("maximum_recorded_age"),
    ).first().asDict()
    return df, metrics


def prepare_collisions(df):
    return (df.withColumn("date_source", F.col("date"))
            .withColumn("date", F.to_date("date", "dd/MM/yyyy"))
            .withColumn("hour", F.when(F.col("time").rlike(r"^([01][0-9]|2[0-3]):[0-5][0-9]$"),
                                       F.substring("time", 1, 2).cast("int")))
            .withColumn("month", F.month("date"))
            .withColumn("is_weekend", F.dayofweek("date").isin(1, 7))
            .withColumn("speed_limit_mph_clean", F.when(F.col("speed_limit").isin(VALID_SPEED_LIMITS),
                                                       F.col("speed_limit"))))


def enrich_collisions(collisions, vehicles, casualties):
    """Reduce each child table to collision grain before joining either one."""
    vehicle_counts = vehicles.groupBy("collision_index").agg(
        F.count("*").alias("observed_vehicle_count"),
        F.count("age_of_driver_clean").alias("known_driver_age_count"),
        F.avg("age_of_driver_clean").alias("mean_recorded_driver_age"))
    casualty_counts = casualties.groupBy("collision_index").agg(
        F.count("*").alias("observed_casualty_count"),
        *[F.sum((F.col("casualty_severity") == code).cast("long")).alias(label + "_casualty_count")
          for code, label in [(1, "fatal"), (2, "serious"), (3, "slight")]])
    return collisions.join(vehicle_counts, "collision_index", "left").join(casualty_counts, "collision_index", "left")


def run(bronze, output, spark=None, run_id=None, bronze_report=None):
    started = time.monotonic()
    bronze, output = Path(bronze), Path(output)
    run_id = run_id or new_run_id()
    destination = output / "runs" / run_id
    destination.mkdir(parents=True)
    report = {"run_id": run_id, "status": "running", "tables": {},
              "relationships": {}, "rejected_rows": 0,
              "policy": "No rows deleted; unknown source codes preserved."}
    owns_spark = spark is None
    frames = {}
    cached = []
    try:
        spark = spark or create_spark("STATS19 Silver")
        report["spark_version"] = spark.version
        if bronze_report is not None:
            if bronze_report["status"] != "passed":
                raise ValueError("Silver requires validated Bronze inputs")
            report["input_manifest"] = bronze_report
        guide = bronze / "reference/stats19_data_guide.xlsx"
        mappings = read_code_lists(guide)
        report["guide_sha256"] = hashlib.sha256(guide.read_bytes()).hexdigest()
        for name, keys in KEYS.items():
            print(f"Silver: checking {name}", flush=True)
            path = bronze / "raw" / name / f"{name}_last5years.csv"
            df = read_source(spark, path).cache()
            cached.append(df)
            metrics = audit_keys(df, keys)
            metrics["input_schema"] = df.schema.jsonValue()
            metrics["missing_important_fields"] = audit_fields(df, IMPORTANT_FIELDS[name])
            metrics["years"] = {str(r[0]): r[1] for r in df.groupBy("collision_year").count().collect()}
            report["tables"][name] = metrics
            if not metrics["rows"] or metrics["missing_keys"] or metrics["duplicate_key_excess"]:
                raise ValueError(f"{name}: missing or duplicate keys; publication blocked")
            if name in AGE_FIELDS:
                field = AGE_FIELDS[name]
                if mappings[field].get(-1) != "Data missing or out of range":
                    raise ValueError(f"Review official missing-age rule for {field}")
                df, metrics["age_quality"] = prepare_age(df, field)
            if name == "collisions":
                df = prepare_collisions(df)
                if mappings["speed_limit"].get(-1) != "Data missing or out of range":
                    raise ValueError("Review official speed-limit missing-code rule")
                metrics["speed_limit_quality"] = df.agg(
                    F.sum(F.col("speed_limit").isin(-1, 99).cast("long")).alias("coded_missing_or_unknown"),
                    F.sum((F.col("speed_limit").isNotNull() &
                           ~F.col("speed_limit").isin(VALID_SPEED_LIMITS + [-1, 99])).cast("long")).alias("unrecognised_values"),
                    F.sum(F.col("speed_limit_mph_clean").isNull().cast("long")).alias("clean_nulls")
                ).first().asDict()
                metrics["date_parse_failures"] = df.filter(F.col("date_source").isNotNull() & F.col("date").isNull()).count()
                metrics["missing_or_unusable_time"] = df.filter(F.col("hour").isNull()).count()
                metrics["time_parse_failures"] = df.filter(
                    F.col("time").isNotNull() & (F.trim("time") != "") & F.col("hour").isNull()).count()
                metrics["year_mismatches"] = df.filter(F.year("date") != F.col("collision_year")).count()
                metrics["weekday_mismatches"] = df.filter(F.dayofweek("date") != F.col("day_of_week")).count()
                metrics["out_of_range_coordinates"] = df.filter(
                    (~F.col("latitude").between(-90, 90)) | (~F.col("longitude").between(-180, 180))).count()
            df = decode(df, CATEGORIES[name], mappings)
            metrics["unmapped_categories"] = {}
            for field in CATEGORIES[name]:
                metrics["unmapped_categories"][field] = [r.asDict() for r in df.filter(
                    F.col(field + "_label").isNull()).groupBy(field).count().collect()]
            metrics["output_rows"] = df.count()
            metrics["output_schema"] = df.schema.jsonValue()
            frames[name] = df

        collisions, vehicles, casualties = [frames[n] for n in KEYS]
        for name in ["vehicles", "casualties"]:
            report["relationships"][name + "_without_collision"] = frames[name].join(
                collisions.select("collision_index"), "collision_index", "left_anti").count()
            report["relationships"][name + "_year_mismatches"] = frames[name].join(
                collisions.select("collision_index", F.col("collision_year").alias("parent_year")),
                "collision_index", "inner").filter(
                    ~F.col("collision_year").eqNullSafe(F.col("parent_year"))).count()
        report["relationships"]["casualties_without_vehicle"] = casualties.join(
            vehicles.select("collision_index", "vehicle_reference"),
            ["collision_index", "vehicle_reference"], "left_anti").count()
        enriched = enrich_collisions(collisions, vehicles, casualties)
        with (destination / "enrichment_plan.txt").open("w") as handle, redirect_stdout(handle):
            enriched.explain(mode="formatted")
        for field, source in [("observed_vehicle_count", "number_of_vehicles"),
                              ("observed_casualty_count", "number_of_casualties")]:
            report["relationships"][field + "_mismatches"] = enriched.filter(
                F.coalesce(F.col(field), F.lit(0)) != F.col(source)).count()
        report["enriched_rows"] = enriched.count()
        if report["enriched_rows"] != report["tables"]["collisions"]["rows"]:
            raise ValueError("Collision grain changed")
        # Preserve anomalies for review; do not silently publish an unreviewed run.
        needs_review = any(report["relationships"].values()) or any(
            any(m["unmapped_categories"].values()) for m in report["tables"].values())
        needs_review = needs_review or any(report["tables"]["collisions"].get(k, 0) for k in
            ["date_parse_failures", "time_parse_failures", "year_mismatches", "weekday_mismatches", "out_of_range_coordinates"])
        # Missing coordinates/ages are documented source limitations, not row rejection rules.
        needs_review = needs_review or any(
            report["tables"][name]["age_quality"]["unexpected_negative_values"] for name in AGE_FIELDS)
        needs_review = needs_review or any(
            report["tables"]["collisions"]["missing_important_fields"][field]
            for field in ["date", "collision_year", "collision_severity", "number_of_vehicles", "number_of_casualties"])
        needs_review = needs_review or report["tables"]["collisions"]["speed_limit_quality"]["unrecognised_values"]
        report["parquet_validation"] = {}
        for name, df in {**frames, "collisions_enriched": enriched}.items():
            print(f"Silver: writing {name}", flush=True)
            df.write.mode("errorifexists").parquet(str(destination / name))
            reloaded = spark.read.parquet(str(destination / name))
            schema_matches = [(f.name, f.dataType) for f in reloaded.schema] == [(f.name, f.dataType) for f in df.schema]
            count = reloaded.count()
            report["parquet_validation"][name] = {"rows": count, "schema_matches": schema_matches}
            if count != df.count() or not schema_matches:
                raise ValueError(f"Parquet validation failed: {name}")
        report["output_bytes"] = sum(p.stat().st_size for p in destination.rglob("*.parquet"))
        report["status"] = "needs_review" if needs_review else "passed"
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
        print(f"Silver status: {report['status']}; report: {destination / 'quality_report.json'}", flush=True)
    return report


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, default=root / "data/bronze/stats19")
    parser.add_argument("--output", type=Path, default=root / "data/silver")
    args = parser.parse_args()
    result = run(args.bronze.resolve(), args.output.resolve())
    raise SystemExit(0 if result["status"] == "passed" else 2)
