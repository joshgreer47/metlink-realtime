"""Static GTFS timetable: which trips run on a service date, and their stop sequences."""

import csv
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

TZ = ZoneInfo("Pacific/Auckland")
WEEKDAYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
BUS_ROUTE_TYPES = {3, 700, 712}
RAIL_ROUTE_TYPES = {2, 100}


def mode_for(route_type: int) -> str:
    if route_type in BUS_ROUTE_TYPES:
        return "bus"
    if route_type in RAIL_ROUTE_TYPES:
        return "rail"
    return "other"


def parse_gtfs_time(value: str) -> int:
    """'25:10:00' -> seconds after the service day's reference time. GTFS times may exceed 24h."""
    h, m, s = value.split(":")
    return int(h) * 3600 + int(m) * 60 + int(s)


def service_day_epoch(service_date: date) -> int:
    """GTFS times are measured from noon minus 12 hours, local time (midnight except on DST change days)."""
    noon = datetime(service_date.year, service_date.month, service_date.day, 12, tzinfo=TZ)
    return int(noon.timestamp()) - 12 * 3600


@dataclass(frozen=True)
class StopTime:
    stop_sequence: int
    stop_id: str
    offset_s: int
    is_timepoint: bool
    lat: float
    lon: float


@dataclass(frozen=True)
class Trip:
    trip_id: str
    route_id: str
    direction_id: int
    service_id: str
    mode: str
    start_time: str
    stop_times: tuple[StopTime, ...]


def _read(path: Path):
    with open(path, encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


class Timetable:
    def __init__(self, gtfs_dir: Path):
        route_types = {r["route_id"]: int(r["route_type"]) for r in _read(gtfs_dir / "routes.txt")}
        stops = {r["stop_id"]: (float(r["stop_lat"]), float(r["stop_lon"])) for r in _read(gtfs_dir / "stops.txt")}

        by_trip: dict[str, list[tuple[int, str, str, bool]]] = defaultdict(list)
        for r in _read(gtfs_dir / "stop_times.txt"):
            if r["arrival_time"] and r["stop_id"] in stops:
                by_trip[r["trip_id"]].append(
                    (int(r["stop_sequence"]), r["stop_id"], r["arrival_time"], r.get("timepoint") == "1")
                )

        self.trips_by_service: dict[str, list[Trip]] = defaultdict(list)
        for r in _read(gtfs_dir / "trips.txt"):
            rows = sorted(by_trip.get(r["trip_id"], []))
            if len(rows) < 2:
                continue
            stop_times = tuple(
                StopTime(seq, stop_id, parse_gtfs_time(t), tp, *stops[stop_id]) for seq, stop_id, t, tp in rows
            )
            self.trips_by_service[r["service_id"]].append(
                Trip(
                    trip_id=r["trip_id"],
                    route_id=r["route_id"],
                    direction_id=int(r["direction_id"] or 0),
                    service_id=r["service_id"],
                    mode=mode_for(route_types.get(r["route_id"], 3)),
                    start_time=rows[0][2],
                    stop_times=stop_times,
                )
            )

        self.calendar = list(_read(gtfs_dir / "calendar.txt")) if (gtfs_dir / "calendar.txt").exists() else []
        self.exceptions: dict[str, dict[str, int]] = defaultdict(dict)
        if (gtfs_dir / "calendar_dates.txt").exists():
            for r in _read(gtfs_dir / "calendar_dates.txt"):
                self.exceptions[r["date"]][r["service_id"]] = int(r["exception_type"])

    def services_on(self, service_date: date) -> set[str]:
        ymd = service_date.strftime("%Y%m%d")
        weekday = WEEKDAYS[service_date.weekday()]
        active = {
            c["service_id"] for c in self.calendar if c["start_date"] <= ymd <= c["end_date"] and c[weekday] == "1"
        }
        for service_id, exception_type in self.exceptions.get(ymd, {}).items():
            if exception_type == 1:
                active.add(service_id)
            elif exception_type == 2:
                active.discard(service_id)
        return active

    def trips_on(self, service_date: date) -> list[Trip]:
        return [t for s in sorted(self.services_on(service_date)) for t in self.trips_by_service.get(s, [])]


def date_range(start: date, days: int) -> list[date]:
    return [start + timedelta(days=i) for i in range(days)]
