"""Small synthetic checks; run with spark-submit, without reading the raw CSVs."""
import unittest
import tempfile
import csv
import json
import shutil
from pathlib import Path

from build_silver import audit_keys, audit_fields, decode, prepare_age, prepare_collisions, enrich_collisions, read_code_lists, read_source
from build_gold import summarise_vehicle_types, summarise_profiles, summarise_profiles_by_year, run as run_gold
from run_project import run as run_project
from ingest_bronze import run as run_bronze, compute_sha256
from pipeline_common import create_spark, publish_run


def make_bronze_fixture(directory):
    """Two collisions, three vehicles, four casualties, including coded missing ages."""
    root = Path(directory)
    tables = {
        "collisions": [
            {"collision_index": "A", "collision_year": 2024, "date": "29/02/2024", "time": "23:59",
             "day_of_week": 5, "collision_severity": 2, "number_of_vehicles": 2, "number_of_casualties": 3,
             "longitude": -1.0, "latitude": 51.0, "speed_limit": 30, "road_type": 6,
             "weather_conditions": 1, "light_conditions": 4, "road_surface_conditions": 1},
            {"collision_index": "B", "collision_year": 2024, "date": "01/03/2024", "time": "12:00",
             "day_of_week": 6, "collision_severity": 3, "number_of_vehicles": 1, "number_of_casualties": 1,
             "longitude": -1.0, "latitude": 51.0, "speed_limit": -1, "road_type": 6,
             "weather_conditions": 1, "light_conditions": 1, "road_surface_conditions": 1}],
        "vehicles": [
            {"collision_index": "A", "collision_year": 2024, "vehicle_reference": 1, "vehicle_type": 9, "age_of_driver": -1},
            {"collision_index": "A", "collision_year": 2024, "vehicle_reference": 2, "vehicle_type": 9, "age_of_driver": 40},
            {"collision_index": "B", "collision_year": 2024, "vehicle_reference": 1, "vehicle_type": 1, "age_of_driver": 16}],
        "casualties": [
            {"collision_index": "A", "collision_year": 2024, "vehicle_reference": 1, "casualty_reference": 1, "casualty_severity": 2, "age_of_casualty": 20},
            {"collision_index": "A", "collision_year": 2024, "vehicle_reference": 1, "casualty_reference": 2, "casualty_severity": 3, "age_of_casualty": 0},
            {"collision_index": "A", "collision_year": 2024, "vehicle_reference": 2, "casualty_reference": 3, "casualty_severity": 3, "age_of_casualty": -1},
            {"collision_index": "B", "collision_year": 2024, "vehicle_reference": 1, "casualty_reference": 1, "casualty_severity": 3, "age_of_casualty": 16}],
    }
    for name, rows in tables.items():
        path = root / "raw" / name / f"{name}_last5years.csv"
        path.parent.mkdir(parents=True)
        with path.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
    guide = root / "reference/stats19_data_guide.xlsx"
    guide.parent.mkdir(parents=True)
    shutil.copy2(Path(__file__).resolve().parents[1] / "data/bronze/stats19/reference/stats19_data_guide.xlsx", guide)
    return root


class SilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = create_spark("STATS19 pipeline tests")
        cls.spark.conf.set("spark.sql.shuffle.partitions", "2")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_vehicle_keys_allow_multiple_vehicles_per_collision(self):
        df = self.spark.createDataFrame([("A", 1), ("A", 2)], ["collision_index", "vehicle_reference"])
        self.assertEqual(audit_keys(df, df.columns),
                         {"rows": 2, "missing_keys": 0, "duplicate_key_excess": 0})

    def test_csv_preserves_time_and_leading_zero_identifiers(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "collisions.csv"
            path.write_text("collision_index,collision_ref_no,time,collision_year\n001,0002,16:06,2025\n")
            row = read_source(self.spark, path).first()
            self.assertEqual((row.collision_index, row.collision_ref_no, row.time),
                             ("001", "0002", "16:06"))
            self.assertEqual(row.collision_year, 2025)

    def test_duplicate_and_missing_keys_are_detected(self):
        df = self.spark.createDataFrame([("A", 1), ("A", 1), (None, 2), (" ", 3)],
                                        ["collision_index", "vehicle_reference"])
        metrics = audit_keys(df, df.columns)
        self.assertEqual(metrics["duplicate_key_excess"], 1)
        self.assertEqual(metrics["missing_keys"], 2)

    def test_decoding_preserves_unknown_codes_and_rows(self):
        df = self.spark.createDataFrame([(1,), (-1,), (999,)], ["weather_conditions"])
        rows = decode(df, ["weather_conditions"],
                      {"weather_conditions": {1: "Fine", -1: "Missing"}}).collect()
        self.assertEqual([(r[0], r[1]) for r in rows], [(1, "Fine"), (-1, "Missing"), (999, None)])

    def test_official_severity_lists_remain_distinct_from_enhanced_severity(self):
        guide = Path(__file__).resolve().parents[1] / "data/bronze/stats19/reference/stats19_data_guide.xlsx"
        mappings = read_code_lists(guide)
        self.assertEqual(mappings["collision_severity"], {1: "Fatal", 2: "Serious", 3: "Slight"})
        self.assertIn(5, mappings["enhanced_casualty_severity"])
        self.assertNotIn(5, mappings["casualty_severity"])

    def test_unknown_ages_do_not_bias_an_average_or_remove_records(self):
        df = self.spark.createDataFrame([(1, -1), (2, 0), (3, 40), (4, 105), (5, None), (6, -2)],
                                        ["id", "age_of_casualty"])
        cleaned, metrics = prepare_age(df, "age_of_casualty")
        rows = {r.id: (r.age_of_casualty, r.age_of_casualty_clean) for r in cleaned.collect()}
        self.assertEqual(len(rows), 6)
        self.assertEqual(rows[1], (-1, None))
        self.assertEqual(rows[2], (0, 0))  # Babies are not missing ages.
        self.assertEqual(rows[4], (105, 105))  # No arbitrary upper-age cutoff.
        self.assertEqual(rows[6], (-2, None))
        self.assertEqual(metrics["unexpected_negative_values"], 1)
        self.assertEqual(metrics["clean_nulls"], 3)
        average = cleaned.selectExpr("avg(age_of_casualty_clean)").first()[0]
        self.assertAlmostEqual(average, 145 / 3)

    def test_missing_field_audit_counts_nulls_and_blank_strings(self):
        df = self.spark.createDataFrame([(None, None), (" ", 20), ("A", 0)], ["key", "age"])
        self.assertEqual(audit_fields(df, ["key", "age"]), {"key": 2, "age": 1})

    def test_calendar_parsing_and_unknown_speed_limits(self):
        df = self.spark.createDataFrame([
            ("29/02/2024", "23:59", 30), ("29/02/2023", "24:00", -1),
            ("01/03/2024", "00:00", 99)], ["date", "time", "speed_limit"])
        rows = prepare_collisions(df).collect()
        self.assertEqual(str(rows[0].date), "2024-02-29")
        self.assertEqual(rows[0].hour, 23)
        self.assertIsNone(rows[1].date)
        self.assertIsNone(rows[1].hour)
        self.assertIsNone(rows[1].speed_limit_mph_clean)
        self.assertEqual(rows[2].hour, 0)
        self.assertIsNone(rows[2].speed_limit_mph_clean)

    def test_two_vehicles_three_casualties_do_not_multiply_collisions(self):
        collisions = self.spark.createDataFrame([("A",)], ["collision_index"])
        vehicles = self.spark.createDataFrame([("A", 20), ("A", 40)],
                                             ["collision_index", "age_of_driver_clean"])
        casualties = self.spark.createDataFrame([("A", 1), ("A", 2), ("A", 3)],
                                               ["collision_index", "casualty_severity"])
        rows = enrich_collisions(collisions, vehicles, casualties).collect()
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0].observed_vehicle_count, rows[0].observed_casualty_count), (2, 3))
        self.assertEqual(rows[0].mean_recorded_driver_age, 30)
        self.assertEqual((rows[0].fatal_casualty_count, rows[0].serious_casualty_count, rows[0].slight_casualty_count), (1, 1, 1))

    def test_failed_publication_preserves_previous_pointer(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            self.assertTrue(publish_run(output, output / "good", {"run_id": "good", "status": "passed"}))
            previous = (output / "latest_successful.json").read_bytes()
            for status in ["failed", "needs_review"]:
                self.assertFalse(publish_run(output, output / status, {"run_id": status, "status": status}))
                self.assertEqual((output / "latest_successful.json").read_bytes(), previous)

    def test_gold_refuses_unvalidated_silver(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "quality_report.json").write_text(json.dumps({"status": "needs_review"}))
            with self.assertRaisesRegex(ValueError, "passed Silver"):
                run_gold(root, root / "gold", self.spark)
            self.assertFalse((root / "gold/latest_successful.json").exists())

    def test_vehicle_groups_deduplicate_memberships_but_may_overlap(self):
        collisions = self.spark.createDataFrame([
            ("A", 2, 3, 0, 1, 2), ("B", 3, 1, 0, 0, 1)],
            ["collision_index", "collision_severity", "number_of_casualties", "fatal_casualty_count",
             "serious_casualty_count", "observed_vehicle_count"])
        vehicles = self.spark.createDataFrame([
            ("A", 9, "Car"), ("A", 9, "Car"), ("A", 1, "Pedal cycle"), ("B", 9, "Car")],
            ["collision_index", "vehicle_type", "vehicle_type_label"])
        rows = {r.vehicle_type: r for r in summarise_vehicle_types(collisions, vehicles).collect()}
        self.assertEqual(rows[9].collision_count, 2)
        self.assertEqual(rows[9].fatal_or_serious_percentage, 50)
        self.assertEqual(rows[1].collision_count, 1)
        self.assertEqual(rows[1].fatal_or_serious_percentage, 100)
        self.assertEqual(sum(r.collision_count for r in rows.values()), 3)  # Overlap is explicit.

    def test_profile_counts_and_two_percentage_denominators(self):
        df = self.spark.createDataFrame([
            ("A", 2, 1, 0, 1, 1, 1, "Daylight", 6, "Single carriageway", 30),
            ("B", 3, 1, 0, 0, 1, 1, "Daylight", 6, "Single carriageway", 30),
            ("C", 1, 1, 1, 0, 1, 6, "Darkness - no lighting", 6, "Single carriageway", None)],
            ["collision_index", "collision_severity", "number_of_casualties", "fatal_casualty_count",
             "serious_casualty_count", "observed_vehicle_count", "light_conditions", "light_conditions_label",
             "road_type", "road_type_label", "speed_limit_mph_clean"])
        rows = {row.light_conditions: row for row in summarise_profiles(df, 2, 3).collect()}
        self.assertEqual(sum(r.collision_count for r in rows.values()), 3)
        self.assertEqual(sum(r.fatal_or_serious_collision_count for r in rows.values()), 2)
        self.assertEqual(rows[1].fatal_or_serious_percentage, 50)
        self.assertEqual(rows[6].fatal_or_serious_percentage, 100)
        self.assertEqual(rows[1].share_of_all_fatal_or_serious_percentage, 50)
        self.assertEqual(rows[6].share_of_all_fatal_or_serious_percentage, 50)
        self.assertAlmostEqual(rows[1].share_of_all_collisions_percentage, 66.667)
        self.assertAlmostEqual(rows[6].share_of_all_collisions_percentage, 33.333)
        self.assertAlmostEqual(rows[1].severe_minus_all_collision_share_pp, -16.667)
        self.assertAlmostEqual(rows[6].severe_minus_all_collision_share_pp, 16.667)
        self.assertIsNone(rows[6].speed_limit_mph_clean)  # Unknown speed is retained.

    def test_profile_overall_share_is_undefined_without_severe_collisions(self):
        df = self.spark.createDataFrame([
            ("A", 3, 1, 0, 0, 1, 1, "Daylight", 6, "Single carriageway", 30)],
            ["collision_index", "collision_severity", "number_of_casualties", "fatal_casualty_count",
             "serious_casualty_count", "observed_vehicle_count", "light_conditions", "light_conditions_label",
             "road_type", "road_type_label", "speed_limit_mph_clean"])
        row = summarise_profiles(df, 0, 1).first()
        self.assertEqual(row.fatal_or_serious_percentage, 0)
        self.assertIsNone(row.share_of_all_fatal_or_serious_percentage)
        self.assertEqual(row.share_of_all_collisions_percentage, 100)
        self.assertIsNone(row.severe_minus_all_collision_share_pp)

    def test_annual_profiles_use_each_year_as_the_reference_population(self):
        df = self.spark.createDataFrame([
            (2024, 2, 1, 0, 1, 1, 1, "Daylight", 6, "Single carriageway", 30),
            (2024, 3, 1, 0, 0, 1, 1, "Daylight", 6, "Single carriageway", 30),
            (2024, 1, 1, 1, 0, 1, 6, "Darkness - no lighting", 6, "Single carriageway", None),
            (2025, 3, 1, 0, 0, 1, 1, "Daylight", 6, "Single carriageway", 30),
            (2025, 3, 1, 0, 0, 1, 1, "Daylight", 6, "Single carriageway", 30),
            (2025, 1, 1, 1, 0, 1, 6, "Darkness - no lighting", 6, "Single carriageway", None)],
            ["collision_year", "collision_severity", "number_of_casualties", "fatal_casualty_count",
             "serious_casualty_count", "observed_vehicle_count", "light_conditions", "light_conditions_label",
             "road_type", "road_type_label", "speed_limit_mph_clean"])
        rows = {(r.collision_year, r.light_conditions): r for r in summarise_profiles_by_year(df).collect()}
        self.assertEqual(rows[2024, 6].reference_severe_collision_count, 2)
        self.assertEqual(rows[2025, 6].reference_severe_collision_count, 1)
        self.assertEqual(rows[2024, 6].share_of_all_fatal_or_serious_percentage, 50)
        self.assertEqual(rows[2025, 6].share_of_all_fatal_or_serious_percentage, 100)
        self.assertEqual(rows[2024, 6].reference_collision_count, 3)
        self.assertAlmostEqual(rows[2025, 1].severe_minus_all_collision_share_pp, -66.667)

    def test_bronze_detects_changes_and_rejects_empty_inputs(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bronze = make_bronze_fixture(root / "bronze")
            first = run_bronze(bronze, root / "outputs")
            second = run_bronze(bronze, root / "outputs")
            self.assertTrue(all(item["change_status"] == "unchanged" for item in second["datasets"]))
            path = bronze / "raw/vehicles/vehicles_last5years.csv"
            checksum = compute_sha256(path)
            self.assertEqual(checksum, first["datasets"][1]["sha256"])
            with path.open("a") as handle:
                handle.write("C,2024,1,9,35\n")
            changed = run_bronze(bronze, root / "outputs")
            self.assertEqual(changed["datasets"][1]["change_status"], "new_or_changed")
            previous = (root / "outputs/latest_successful.json").read_bytes()
            path.write_text("")
            with self.assertRaisesRegex(ValueError, "Empty source"):
                run_bronze(bronze, root / "outputs")
            self.assertEqual((root / "outputs/latest_successful.json").read_bytes(), previous)

    def test_end_to_end_totals_and_failed_update_preserves_gold(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            bronze = make_bronze_fixture(root / "bronze")
            original_hashes = {p: compute_sha256(p) for p in bronze.rglob("*.csv")}
            report = run_project(bronze, root / "silver", root / "gold", root / "operations", self.spark)
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["stages"]["gold"]["source_totals"],
                             {"collisions": 2, "casualties": 4, "fatal_or_serious": 1})
            self.assertTrue(report["stages"]["gold"]["tables"]["collision_profiles"]["reconciled_to_silver"])
            self.assertTrue(report["stages"]["gold"]["tables"]["collision_profiles_by_year"]["reconciled_by_year"])
            gold_path = root / "gold/runs" / report["run_id"]
            car = self.spark.read.parquet(str(gold_path / "vehicle_type")).filter("vehicle_type = 9").first()
            self.assertEqual(car.collision_count, 1)  # Two cars, one collision.
            self.assertEqual(car.fatal_or_serious_percentage, 100)
            monthly = self.spark.read.parquet(str(gold_path / "monthly")).orderBy("month").collect()
            self.assertEqual([(r.month, r.collision_count) for r in monthly], [(2, 1), (3, 1)])
            self.assertEqual(original_hashes, {p: compute_sha256(p) for p in original_hashes})
            pointer = (root / "gold/latest_successful.json").read_bytes()
            silver_pointer = (root / "silver/latest_successful.json").read_bytes()
            vehicle_path = bronze / "raw/vehicles/vehicles_last5years.csv"
            lines = vehicle_path.read_text().splitlines()
            vehicle_path.write_text("\n".join([lines[0], lines[1], lines[3]]) + "\n")
            with self.assertRaisesRegex(ValueError, "Silver needs review"):
                run_project(bronze, root / "silver", root / "gold", root / "operations", self.spark)
            self.assertEqual((root / "gold/latest_successful.json").read_bytes(), pointer)
            self.assertEqual((root / "silver/latest_successful.json").read_bytes(), silver_pointer)


if __name__ == "__main__":
    unittest.main()
