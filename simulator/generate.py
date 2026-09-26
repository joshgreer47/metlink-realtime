"""Generates simulated GTFS-RT history from the static timetable and uploads it to a UC volume.

Output matches the collector exactly (same JSONL record shape, one file per feed per 5-minute batch, same volume
layout), gzip-compressed. Point it at a separate catalog so simulated and collected data never mix.

Usage:
    python -m simulator.generate                          # 14 days ending yesterday, upload to metlink_sim
    python -m simulator.generate --start 2026-09-12 --days 3 --no-upload
    python -m simulator.generate --upload-only            # retry uploads left in the spool
"""

import argparse
import bisect
import gzip
import json
import logging
import math
import random
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from poller.config import REPO_ROOT, load_settings
from poller.metlink_poller import fetch_feed, make_record, volume_path_for
from simulator.model import Run, assign_vehicles, draw_incidents, load_calibration, params_for, simulate_trip
from simulator.timetable import TZ, Timetable, date_range, service_day_epoch

log = logging.getLogger("simulator")

BATCH_SECONDS = 300
PRE_TRIP_S = 120  # trips appear in the feed shortly before departure
POST_TRIP_S = 180  # vehicles keep reporting, without a trip, just after finishing
REPORT_JITTER_S = 25
DEFAULT_VOLUME = "/Volumes/metlink_sim/bronze/raw"
MAX_CONSECUTIVE_UPLOAD_FAILURES = 20


def build_runs(timetable: Timetable, dates: list[date], seed: int) -> list[Run]:
    calibration = load_calibration()
    runs: list[Run] = []
    for d in dates:
        rng = random.Random(f"{seed}:{d.isoformat()}")
        trips = timetable.trips_on(d)
        incidents = draw_incidents(rng, {t.route_id for t in trips})
        day_runs = [
            simulate_trip(rng, t, d, params_for(calibration, t.mode), incidents.get(t.route_id))
            for t in sorted(trips, key=lambda t: t.trip_id)
        ]
        for run in day_runs:
            run.entity_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{run.trip.trip_id}/{d.isoformat()}"))
        runs.extend(day_runs)
        log.info("%s: %d trips, %d incidents", d, len(day_runs), len(incidents))
    assign_vehicles(runs, random.Random(seed), calibration)
    return runs


def state_at(run: Run, ts: int) -> tuple[int, float]:
    """Index of the next stop and progress (0..1) from the previous stop towards it."""
    k = bisect.bisect_right(run.actual, ts)
    if k == 0:
        return 0, 0.0
    k = min(k, len(run.actual) - 1)
    span = run.actual[k] - run.actual[k - 1]
    return k, 1.0 if span <= 0 else min(max((ts - run.actual[k - 1]) / span, 0.0), 1.0)


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    y = math.sin(math.radians(lon2 - lon1)) * math.cos(math.radians(lat2))
    x = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - math.sin(math.radians(lat1)) * math.cos(
        math.radians(lat2)
    ) * math.cos(math.radians(lon2 - lon1))
    return round((math.degrees(math.atan2(y, x)) + 360) % 360, 1)


def trip_descriptor(run: Run) -> dict:
    return {
        "trip_id": run.trip.trip_id,
        "start_time": run.trip.start_time,
        "start_date": run.service_date.strftime("%Y%m%d"),
        "schedule_relationship": "SCHEDULED",
        "route_id": run.trip.route_id,
        "direction_id": run.trip.direction_id,
    }


def trip_update_entity(run: Run, ts: int) -> dict:
    k, frac = state_at(run, ts)
    if k == 0:
        delay = run.delays[0]
    else:
        delay = round(run.delays[k - 1] + (run.delays[k] - run.delays[k - 1]) * frac)
    st = run.trip.stop_times[k]
    scheduled = service_day_epoch(run.service_date) + st.offset_s
    return {
        "id": run.entity_id,
        "trip_update": {
            "trip": trip_descriptor(run),
            "stop_time_update": [
                {
                    "stop_sequence": st.stop_sequence,
                    "arrival": {"delay": delay, "time": str(scheduled + delay)},
                    "stop_id": st.stop_id,
                    "schedule_relationship": "SCHEDULED",
                }
            ],
            "vehicle": {"id": run.vehicle_id},
            "timestamp": str(ts),
        },
    }


def vehicle_entity(run: Run, ts: int, in_service: bool) -> dict:
    stops = run.trip.stop_times
    if in_service:
        k, frac = state_at(run, ts)
        a, b = (stops[0], stops[1]) if k == 0 else (stops[k - 1], stops[k])
        f = 0.0 if k == 0 else frac
        lat, lon = a.lat + (b.lat - a.lat) * f, a.lon + (b.lon - a.lon) * f
    else:
        a, b = stops[-2], stops[-1]
        lat, lon = b.lat, b.lon
    vehicle = {
        "position": {
            "latitude": round(lat, 6),
            "longitude": round(lon, 6),
            "bearing": bearing(a.lat, a.lon, b.lat, b.lon),
        },
        "timestamp": str(ts),
        "vehicle": {"id": run.vehicle_id},
    }
    if in_service:
        vehicle = {"trip": trip_descriptor(run), **vehicle}
        if run.occupancy:
            vehicle["occupancy_status"] = run.occupancy
    return {"id": run.entity_id if in_service else f"{run.vehicle_id}-idle", "vehicle": vehicle}


def feed_message(ts: int, entities: list[dict]) -> dict:
    return {
        "header": {"gtfs_realtime_version": "2.0", "incrementality": "FULL_DATASET", "timestamp": str(ts)},
        "entity": entities,
    }


class GzipBatchWriter:
    def __init__(self, spool_dir: Path):
        self.spool_dir = spool_dir
        self.buffers: dict[str, list[str]] = {}
        self.files = 0

    def add(self, record: dict) -> None:
        self.buffers.setdefault(record["feed"], []).append(json.dumps(record, separators=(",", ":")))

    def flush(self, stamp: datetime) -> None:
        for feed, lines in self.buffers.items():
            path = self.spool_dir / feed / f"{feed}_{stamp:%Y%m%dT%H%M%SZ}.jsonl.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            partial = path.with_name(path.name + ".partial")
            with gzip.open(partial, "wt", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            partial.replace(path)
            self.files += 1
        self.buffers = {}


def generate(runs: list[Run], alerts: list[dict], poll_s: int, spool_dir: Path, seed: int) -> int:
    rng = random.Random(f"{seed}:polls")
    runs = sorted(runs, key=lambda r: r.start)
    first = (runs[0].start - PRE_TRIP_S) // poll_s * poll_s
    last = max(r.end for r in runs) + POST_TRIP_S
    writer = GzipBatchWriter(spool_dir)
    active: list[Run] = []
    nxt = 0
    bucket = first // BATCH_SECONDS

    for t in range(first, last + 1, poll_s):
        if t // BATCH_SECONDS != bucket:
            writer.flush(datetime.fromtimestamp((bucket + 1) * BATCH_SECONDS, timezone.utc))
            bucket = t // BATCH_SECONDS
        while nxt < len(runs) and runs[nxt].start - PRE_TRIP_S <= t:
            active.append(runs[nxt])
            nxt += 1
        active = [r for r in active if r.end + POST_TRIP_S >= t]

        trip_updates, vehicles = [], []
        for run in active:
            ts = t - rng.randint(0, REPORT_JITTER_S)
            if ts <= run.end:
                trip_updates.append(trip_update_entity(run, ts))
                vehicles.append(vehicle_entity(run, ts, in_service=True))
            else:
                vehicles.append(vehicle_entity(run, ts, in_service=False))

        fetched_at = datetime.fromtimestamp(t, timezone.utc)
        for feed, entities in (
            ("tripupdates", trip_updates),
            ("vehiclepositions", vehicles),
            ("servicealerts", alerts),
        ):
            writer.add(make_record(feed, fetched_at, feed_message(t - 3, entities)))
    writer.flush(datetime.fromtimestamp((bucket + 1) * BATCH_SECONDS, timezone.utc))
    return writer.files


def live_alerts() -> list[dict]:
    """Current real alerts, replayed unchanged on every simulated poll."""
    import requests

    settings = load_settings()
    if not settings.metlink_api_key:
        log.warning("METLINK_API_KEY not set; simulating without service alerts")
        return []
    return fetch_feed(requests.Session(), "servicealerts", settings.metlink_api_key).get("entity", [])


def upload_spool(spool_dir: Path, volume_root: str, workers: int) -> tuple[int, int]:
    from poller.uploader import VolumeUploader

    uploader = VolumeUploader()
    if not uploader.volume_exists(volume_root):
        raise SystemExit(
            f"Volume {volume_root} does not exist. Run src/notebooks/00_setup with the matching catalog, "
            "then rerun with --upload-only."
        )
    paths = sorted(spool_dir.glob("*/*.jsonl.gz"))

    # Skip files an earlier, interrupted run already uploaded intact.
    remote = uploader.list_files(f"{volume_root}/realtime")
    pending = []
    for path in paths:
        if remote.get(volume_path_for(path, volume_root)) == path.stat().st_size:
            path.unlink()
        else:
            pending.append(path)
    if len(pending) < len(paths):
        log.info("%d files already in the volume; uploading the remaining %d", len(paths) - len(pending), len(pending))
    paths = pending
    ok = failed = 0

    def upload(path: Path) -> None:
        uploader.upload(path, volume_path_for(path, volume_root))
        path.unlink()

    consecutive_failures = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(upload, p): p for p in paths}
        for future in as_completed(futures):
            if future.cancelled():
                continue
            try:
                future.result()
                ok += 1
                consecutive_failures = 0
            except Exception as e:
                failed += 1
                consecutive_failures += 1
                log.warning("upload failed for %s: %s", futures[future].name, e)
                if consecutive_failures >= MAX_CONSECUTIVE_UPLOAD_FAILURES:
                    log.error("stopping after %d consecutive failures; rerun with --upload-only", consecutive_failures)
                    for pending in futures:
                        pending.cancel()
            if (ok + failed) % 500 == 0:
                log.info("uploaded %d/%d", ok + failed, len(paths))
    return ok, failed


def latest_gtfs_dir() -> Path:
    versions = sorted(p for p in (REPO_ROOT / "data" / "gtfs_static").iterdir() if (p / ".complete").exists())
    if not versions:
        raise SystemExit("No static GTFS found; run `python -m poller.static_gtfs --no-upload` first")
    return versions[-1]


def main() -> None:
    today = datetime.now(TZ).date()
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--start", type=date.fromisoformat, help="first service date (default: DAYS before today)")
    parser.add_argument("--days", type=int, default=14)
    parser.add_argument("--poll-seconds", type=int, default=60)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--gtfs-dir", type=Path, help="static GTFS folder (default: latest in data/gtfs_static)")
    parser.add_argument("--spool-dir", type=Path, default=REPO_ROOT / "data" / "sim" / "spool")
    parser.add_argument("--volume-path", default=DEFAULT_VOLUME)
    parser.add_argument("--alerts", choices=("live", "none"), default="live")
    parser.add_argument("--upload-workers", type=int, default=4)
    parser.add_argument("--no-upload", action="store_true")
    parser.add_argument("--upload-only", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    if not args.upload_only:
        start = args.start or today - timedelta(days=args.days)
        dates = date_range(start, args.days)
        gtfs_dir = args.gtfs_dir or latest_gtfs_dir()
        log.info("simulating %s to %s from timetable %s", dates[0], dates[-1], gtfs_dir.name)
        runs = build_runs(Timetable(gtfs_dir), dates, args.seed)
        if not runs:
            raise SystemExit("No scheduled trips on those dates; check they fall within the timetable's calendar")
        alerts = live_alerts() if args.alerts == "live" else []
        files = generate(runs, alerts, args.poll_seconds, args.spool_dir, args.seed)
        log.info("wrote %d files to %s", files, args.spool_dir)

    if not args.no_upload:
        ok, failed = upload_spool(args.spool_dir, args.volume_path.rstrip("/"), args.upload_workers)
        log.info("uploaded %d files to %s (%d failed)", ok, args.volume_path, failed)


if __name__ == "__main__":
    main()
