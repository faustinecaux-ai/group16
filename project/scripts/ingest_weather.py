"""Acquire resumable, immutable ERA5 responses for cells occupied by scoped collisions.

Network is explicit: python scripts/ingest_weather.py. Normal pipeline runs offline.
"""
import argparse
import csv
from datetime import datetime, timezone, date, timedelta
import hashlib
import json
from pathlib import Path
import time
from urllib.parse import urlencode
from urllib.request import urlopen

from pipeline_common import write_json
from weather_common import cell_id

API = "https://archive-api.open-meteo.com/v1/archive"
VARIABLES = ["weather_code", "wind_speed_10m", "temperature_2m", "precipitation"]


def acquire(bronze, output, settings):
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    config = json.loads(Path(settings).read_text())
    cells = set()
    source = Path(bronze) / "raw/collisions/collisions_last5years.csv"
    with source.open(newline="") as handle:
        for row in csv.DictReader(handle):
            try:
                day = datetime.strptime(row["date"], "%d/%m/%Y").date().isoformat()
                lat, lon = float(row["latitude"]), float(row["longitude"])
            except (ValueError, TypeError):
                continue
            if (config["start_date"] <= day <= config["end_date"] and
                    config["latitude_min"] <= lat <= config["latitude_max"] and
                    config["longitude_min"] <= lon <= config["longitude_max"]):
                cells.add(cell_id(lat, lon, config["grid_degrees"]))
    if not cells:
        raise ValueError("No collisions in configured weather scope")
    # Padding supports BST offsets and nearest-hour matching at scope boundaries.
    start = (date.fromisoformat(config["start_date"]) - timedelta(days=1)).isoformat()
    end = (date.fromisoformat(config["end_date"]) + timedelta(days=1)).isoformat()
    manifest = {"source": API, "documentation": "https://open-meteo.com/en/docs/historical-weather-api",
                "licence": "CC BY 4.0; attribute Open-Meteo and Copernicus/ECMWF ERA5",
                "model": "era5", "config": config, "responses": []}
    for cell in sorted(cells):
        y, x = map(int, cell.split("_"))
        params = dict(latitude=y * config["grid_degrees"], longitude=x * config["grid_degrees"],
                      start_date=start, end_date=end, hourly=",".join(VARIABLES), models="era5",
                      timezone="GMT", timeformat="unixtime", wind_speed_unit="kmh",
                      cell_selection="nearest")
        url = API + "?" + urlencode(params)
        token = hashlib.sha256(url.encode()).hexdigest()[:16]
        path = output / f"{cell}_{token}.json"
        receipt = path.with_suffix(".receipt.json")
        if not path.exists():
            for attempt in range(4):
                try:
                    with urlopen(url, timeout=120) as response:
                        raw = response.read()
                    payload = json.loads(raw)
                    if "hourly" not in payload:
                        raise ValueError("Missing hourly payload")
                    # Persist original bytes unchanged; receipt records actual retrieval.
                    path.write_bytes(raw)
                    write_json(receipt, {"url": url, "retrieved_at": datetime.now(timezone.utc).isoformat(),
                                         "sha256": hashlib.sha256(raw).hexdigest()})
                    break
                except Exception:
                    if attempt == 3:
                        raise
                    time.sleep(2 ** attempt * 3)
            time.sleep(2)
        record = json.loads(receipt.read_text())
        if record["sha256"] != hashlib.sha256(path.read_bytes()).hexdigest() or record["url"] != url:
            raise ValueError(f"Cached response integrity failure: {path}")
        manifest["responses"].append(dict(record, cell_id=cell, file=path.name))
        print(f"Weather Bronze: {cell} ({len(manifest['responses'])}/{len(cells)})", flush=True)
    write_json(output / "manifest.json", manifest)
    return manifest


if __name__ == "__main__":
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bronze", type=Path, default=root / "data/bronze/stats19")
    parser.add_argument("--output", type=Path, default=root / "data/bronze/weather")
    parser.add_argument("--config", type=Path, default=root / "config/weather.json")
    args = parser.parse_args()
    acquire(args.bronze, args.output, args.config)
