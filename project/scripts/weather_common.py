"""Versioned spatial, temporal and classification rules; no police weather input."""
from datetime import datetime, timezone
import math
from zoneinfo import ZoneInfo

RULE_VERSION = "era5-weather-v1"


def cell_id(latitude, longitude, step=0.25):
    # Explicit half-up rule shared by ingestion and Silver, including negatives.
    return f"{math.floor(latitude / step + .5)}_{math.floor(longitude / step + .5)}"


def collision_clock(date_source, time_source):
    """Reject ambiguous/nonexistent London wall times rather than invent an offset."""
    try:
        local = datetime.strptime(f"{date_source} {time_source}", "%d/%m/%Y %H:%M")
    except (ValueError, TypeError):
        return (None, "invalid_datetime")
    london = ZoneInfo("Europe/London")
    candidates = set()
    for fold in (0, 1):
        utc = local.replace(tzinfo=london, fold=fold).astimezone(timezone.utc)
        if utc.astimezone(london).replace(tzinfo=None) == local:
            candidates.add(int(utc.timestamp()))
    if not candidates:
        return (None, "nonexistent_local_time")
    if len(candidates) > 1:
        return (None, "ambiguous_local_time")
    return (candidates.pop(), "valid")


def classify(code, wind, threshold=40.0):
    """Project approximation of STATS19 1..7; other WMO phenomena -> 8."""
    if code is None or wind is None or not math.isfinite(wind) or wind < 0:
        return None
    strong = wind >= threshold
    if code in (0, 1, 2, 3):
        return 4 if strong else 1
    if code in (51, 53, 55, 56, 57, 61, 63, 65, 66, 67, 80, 81, 82):
        return 5 if strong else 2
    if code in (71, 73, 75, 77, 85, 86):
        return 6 if strong else 3
    if code in (45, 48):
        return 7
    if code in (95, 96, 99):
        return 8
    return None
