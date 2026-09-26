import json
from datetime import datetime, timezone
from pathlib import Path

from google.transit import gtfs_realtime_pb2

from poller.metlink_poller import BatchWriter, make_record, parse_payload, upload_pending, volume_path_for

FETCHED_AT = datetime(2026, 9, 26, 0, 15, 0, tzinfo=timezone.utc)


def vehicle_feed_bytes(header_ts: int = 1790000000) -> bytes:
    msg = gtfs_realtime_pb2.FeedMessage()
    msg.header.gtfs_realtime_version = "2.0"
    msg.header.timestamp = header_ts
    entity = msg.entity.add(id="v1")
    entity.vehicle.trip.trip_id = "2__1__101__TZM__1"
    entity.vehicle.trip.route_id = "20"
    entity.vehicle.position.latitude = -41.2865
    entity.vehicle.position.longitude = 174.7762
    entity.vehicle.vehicle.id = "3456"
    return msg.SerializeToString()


def test_parse_protobuf_uses_spec_field_names():
    d = parse_payload(vehicle_feed_bytes(), "application/x-protobuf")
    vehicle = d["entity"][0]["vehicle"]
    assert vehicle["trip"]["trip_id"] == "2__1__101__TZM__1"
    assert abs(vehicle["position"]["latitude"] - -41.2865) < 1e-4


def test_parse_json_passthrough():
    body = json.dumps({"header": {"timestamp": 1}, "entity": []}).encode()
    assert parse_payload(body, "application/json; charset=utf-8")["header"]["timestamp"] == 1


def test_make_record_casts_header_timestamp():
    # MessageToDict renders uint64 as a string
    record = make_record("vehiclepositions", FETCHED_AT, parse_payload(vehicle_feed_bytes(123), "x-protobuf"))
    assert record["header_timestamp"] == 123
    assert record["entity_count"] == 1
    assert record["fetched_at"] == "2026-09-26T00:15:00+00:00"


def test_batch_writer_skips_unchanged_and_writes_jsonl(tmp_path: Path):
    writer = BatchWriter(tmp_path)
    feed = parse_payload(vehicle_feed_bytes(100), "x-protobuf")
    assert writer.add(make_record("vehiclepositions", FETCHED_AT, feed))
    assert not writer.add(make_record("vehiclepositions", FETCHED_AT, feed))  # same header ts
    assert writer.add(make_record("vehiclepositions", FETCHED_AT, parse_payload(vehicle_feed_bytes(130), "x")))

    [path] = writer.flush(FETCHED_AT)
    assert path.name == "vehiclepositions_20260926T001500Z.jsonl"
    lines = path.read_text().splitlines()
    assert [json.loads(line)["header_timestamp"] for line in lines] == [100, 130]
    assert writer.flush(FETCHED_AT) == []  # buffer cleared


def test_volume_path_partitions_by_date():
    p = Path("data/spool/tripupdates/tripupdates_20260926T001500Z.jsonl")
    assert volume_path_for(p, "/Volumes/metlink/bronze/raw") == (
        "/Volumes/metlink/bronze/raw/realtime/tripupdates/date=2026-09-26/tripupdates_20260926T001500Z.jsonl"
    )


def test_upload_pending_keeps_files_on_failure(tmp_path: Path):
    for name in ("a_20260926T000000Z.jsonl", "a_20260926T000500Z.jsonl"):
        (tmp_path / "a").mkdir(exist_ok=True)
        (tmp_path / "a" / name).write_text("{}\n")

    class FlakyUploader:
        calls = 0

        def upload(self, local_path, volume_file_path):
            self.calls += 1
            if self.calls == 2:
                raise RuntimeError("network down")

    assert upload_pending(tmp_path, "/Volumes/x", FlakyUploader()) == 1
    assert [p.name for p in (tmp_path / "a").iterdir()] == ["a_20260926T000500Z.jsonl"]


def test_volume_path_handles_gzip():
    p = Path("data/sim/spool/tripupdates/tripupdates_20260926T001500Z.jsonl.gz")
    assert volume_path_for(p, "/Volumes/metlink_sim/bronze/raw") == (
        "/Volumes/metlink_sim/bronze/raw/realtime/tripupdates/date=2026-09-26/tripupdates_20260926T001500Z.jsonl.gz"
    )


def test_periodic_task_runs_once_per_interval_without_overlap():
    import threading

    from poller.metlink_poller import PeriodicTask

    release, calls = threading.Event(), []
    task = PeriodicTask("t", lambda: (calls.append(1), release.wait(5)), interval_seconds=100)

    assert task.maybe_start(now=0)
    assert not task.maybe_start(now=200)  # still running: no overlap
    release.set()
    task._thread.join(5)
    assert not task.maybe_start(now=50)  # finished, but interval not elapsed
    assert task.maybe_start(now=200)
    task._thread.join(5)
    assert len(calls) == 2


def test_periodic_task_survives_errors_and_can_be_disabled():
    from poller.metlink_poller import PeriodicTask

    task = PeriodicTask("t", lambda: 1 / 0, interval_seconds=1)
    assert task.maybe_start(now=0)
    task._thread.join(5)
    assert task.maybe_start(now=2)  # an error doesn't stop future runs
    task._thread.join(5)
    assert not PeriodicTask("off", lambda: None, interval_seconds=0).maybe_start(now=0)


def test_find_official_releases():
    from poller.official_performance import find_releases

    html = """<a href="/assets/Perf/metlink-weekly-bus-performance-to-2026-03-29.csv">CSV</a>
              <a href='https://www.metlink.org.nz/assets/Perf/metlink-daily-bus-performance-to-2026-03-29.csv'>CSV</a>
              <a href="/assets/Perf/metlink-weekly-bus-performance-to-2026-03-29.xlsx">Excel</a>"""
    releases = find_releases(html, "https://www.metlink.org.nz/about-us/performance-of-our-network")
    assert releases == {
        "metlink-weekly-bus-performance-to-2026-03-29.csv": (
            "weekly",
            "https://www.metlink.org.nz/assets/Perf/metlink-weekly-bus-performance-to-2026-03-29.csv",
        ),
        "metlink-daily-bus-performance-to-2026-03-29.csv": (
            "daily",
            "https://www.metlink.org.nz/assets/Perf/metlink-daily-bus-performance-to-2026-03-29.csv",
        ),
    }
