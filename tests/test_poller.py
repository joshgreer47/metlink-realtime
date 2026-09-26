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
