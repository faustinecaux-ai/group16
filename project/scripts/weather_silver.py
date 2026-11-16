"""Normalize external Bronze, join cell/UTC hour, audit coverage and disagreement."""
import hashlib
import json
import math
from pathlib import Path

from pyspark.sql import functions as F, types as T
from weather_common import cell_id, collision_clock, classify, RULE_VERSION


def load_weather(spark, root):
    root = Path(root)
    manifest = json.loads((root / "manifest.json").read_text())
    rows, keys = [], set()
    for item in manifest["responses"]:
        path = root / item["file"]
        if hashlib.sha256(path.read_bytes()).hexdigest() != item["sha256"]:
            raise ValueError(f"Weather checksum mismatch: {path}")
        payload = json.loads(path.read_text())
        hourly = payload["hourly"]
        if payload["utc_offset_seconds"] != 0:
            raise ValueError("Weather must use UTC")
        for field, unit in [("wind_speed_10m", "km/h"), ("precipitation", "mm"),
                            ("temperature_2m", "°C"), ("time", "unixtime")]:
            if payload["hourly_units"][field] != unit:
                raise ValueError(f"Unexpected weather unit: {field}")
        fields = ["weather_code", "wind_speed_10m", "temperature_2m", "precipitation"]
        if any(len(hourly[f]) != len(hourly["time"]) for f in fields):
            raise ValueError("Misaligned weather arrays")
        for i, epoch in enumerate(hourly["time"]):
            key = (item["cell_id"], epoch)
            if key in keys or epoch % 3600:
                raise ValueError("Duplicate or non-hourly weather key")
            keys.add(key)
            values = [hourly[f][i] for f in fields]
            if any(v is not None and not math.isfinite(v) for v in values):
                raise ValueError("Non-finite weather value")
            if ((values[1] is not None and values[1] < 0) or
                    (values[3] is not None and values[3] < 0)):
                raise ValueError("Negative wind or precipitation")
            rows.append((item["cell_id"], int(epoch), float(payload["latitude"]),
                         float(payload["longitude"]), values[0],
                         *[float(v) if v is not None else None for v in values[1:]], item["sha256"]))
    if not rows:
        raise ValueError("Empty weather archive")
    schema = ("weather_cell_id string, weather_hour_utc_epoch long, weather_latitude double, "
              "weather_longitude double, weather_wmo_code int, weather_wind_kmh double, "
              "weather_temperature_c double, weather_precipitation_mm double, weather_source_sha256 string")
    return spark.createDataFrame(rows, schema), manifest


def enrich_weather(collisions, weather, config):
    collisions.sparkSession.sparkContext.addPyFile(str(Path(__file__).with_name("weather_common.py")))
    clock_schema = T.StructType([T.StructField("epoch", T.LongType()),
                                T.StructField("status", T.StringType())])
    clock = F.udf(collision_clock, clock_schema)
    cell = F.udf(lambda lat, lon: cell_id(lat, lon, config["grid_degrees"])
                 if lat is not None and lon is not None and math.isfinite(lat) and math.isfinite(lon)
                 and -90 <= lat <= 90 and -180 <= lon <= 180 else None, T.StringType())
    category = F.udf(lambda code, wind: classify(code, wind, config["strong_wind_kmh"]), T.IntegerType())
    df = (collisions.withColumn("_clock", clock("date_source", "time"))
          .withColumn("collision_time_quality", F.col("_clock.status"))
          .withColumn("collision_utc_epoch", F.col("_clock.epoch"))
          .withColumn("weather_in_scope", F.coalesce(
              F.col("date").between(config["start_date"], config["end_date"]) &
              F.col("latitude").between(config["latitude_min"], config["latitude_max"]) &
              F.col("longitude").between(config["longitude_min"], config["longitude_max"]), F.lit(False)))
          .withColumn("weather_cell_id", F.when(F.col("weather_in_scope"), cell("latitude", "longitude")))
          # Nearest hour, ties towards later hour. This is retrospective enrichment.
          .withColumn("weather_hour_utc_epoch", (F.floor((F.col("collision_utc_epoch") + 1800) / 3600) * 3600).cast("long"))
          .drop("_clock"))
    if weather.groupBy("weather_cell_id", "weather_hour_utc_epoch").count().filter("count > 1").limit(1).count():
        raise ValueError("Weather join keys are not unique")
    df = df.join(weather, ["weather_cell_id", "weather_hour_utc_epoch"], "left")
    lat1, lat2 = F.radians("latitude"), F.radians("weather_latitude")
    dlat = lat2 - lat1
    dlon = F.radians(F.col("weather_longitude") - F.col("longitude"))
    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2)
    df = (df.withColumn("weather_distance_km", F.when(F.col("weather_latitude").isNotNull(),
                         12742 * F.asin(F.sqrt(F.least(F.lit(1.), a)))))
          .withColumn("weather_time_delta_minutes", F.abs(F.col("collision_utc_epoch") - F.col("weather_hour_utc_epoch")) / 60)
          .withColumn("weather_match_status", F.when(~F.col("weather_in_scope"), "outside_scope")
                      .when(F.col("collision_time_quality") != "valid", F.col("collision_time_quality"))
                      .when(F.col("weather_source_sha256").isNull(), "missing_weather")
                      .when(F.col("weather_distance_km") > config["max_distance_km"], "too_far")
                      .when(category("weather_wmo_code", "weather_wind_kmh").isNull(), "unusable_weather")
                      .otherwise("matched"))
          .withColumn("weather_conditions_reconstructed", F.when(F.col("weather_match_status") == "matched",
                       category("weather_wmo_code", "weather_wind_kmh")))
          .withColumn("weather_rule_version", F.lit(RULE_VERSION))
          .withColumn("weather_comparable", (F.col("weather_match_status") == "matched") & F.col("weather_conditions").between(1, 7))
          .withColumn("weather_agrees", F.when(F.col("weather_comparable"),
                       F.col("weather_conditions") == F.col("weather_conditions_reconstructed"))))
    return df


def audit_weather(df, manifest):
    statuses = {r.weather_match_status: r['count'] for r in df.groupBy("weather_match_status").count().collect()}
    metrics = df.agg(F.count("*").alias("rows"),
                     F.sum(F.col("weather_in_scope").cast("long")).alias("in_scope"),
                     F.sum(F.col("weather_comparable").cast("long")).alias("comparable"),
                     F.sum(F.coalesce(F.col("weather_agrees"), F.lit(False)).cast("long")).alias("agreements"),
                     F.max("weather_distance_km").alias("max_distance_km"),
                     F.max("weather_time_delta_minutes").alias("max_time_delta_minutes")).first().asDict()
    metrics.update(status_counts=statuses, rule_version=RULE_VERSION, source_manifest=manifest)
    metrics["matched"] = statuses.get("matched", 0)
    metrics["coverage_in_scope_pct"] = round(metrics["matched"] * 100 / metrics["in_scope"], 3) if metrics["in_scope"] else None
    metrics["agreement_pct"] = round(metrics["agreements"] * 100 / metrics["comparable"], 3) if metrics["comparable"] else None
    metrics["confusion_matrix"] = [r.asDict() for r in df.filter("weather_comparable").groupBy(
        "weather_conditions", "weather_conditions_reconstructed").count().orderBy(
            "weather_conditions", "weather_conditions_reconstructed").collect()]
    return metrics
