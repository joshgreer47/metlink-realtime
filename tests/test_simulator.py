import gzip
import json
import random
from datetime import date, datetime, timezone
from pathlib import Path

import pytest

from poller.metlink_poller import volume_path_for
from simulator.generate import build_runs, generate, trip_update_entity, vehicle_entity
from simulator.model import (
    TIMEPOINT_EARLY_FLOOR_S,
    ModeParams,
    Run,
    assign_vehicles,
    load_calibration,
    sample_quantiles,
    simulate_trip,
)
from simulator.timetable import Timetable, service_day_epoch

THURSDAY = date(2026, 9, 24)


def write_csv(path: Path, header: str, *rows: str) -> None:
    path.write_text("\n".join([header, *rows]) + "\n", encoding="utf-8")


@pytest.fixture
def gtfs(tmp_path: Path) -> Path:
    write_csv(tmp_path / "routes.txt", "route_id,route_short_name,route_type", "10,1,3", "20,HVL,2")
    write_csv(
        tmp_path / "stops.txt",
        "stop_id,stop_name,stop_lat,stop_lon",
        "A,Alpha,-41.28,174.77",
        "B,Beta,-41.29,174.78",
        "C,Gamma,-41.30,174.79",
    )
    write_csv(
        tmp_path / "trips.txt",
        "route_id,service_id,trip_id,direction_id",
        "10,WK,t1,0",
        "10,WK,t2,0",
        "20,SAT,t3,1",
    )
    write_csv(
        tmp_path / "stop_times.txt",
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence,timepoint",
        "t1,08:00:00,08:00:00,A,1,1",
        "t1,08:05:00,08:05:00,B,2,0",
        "t1,08:10:00,08:10:00,C,3,1",
        "t2,08:30:00,08:30:00,A,1,1",
        "t2,08:35:00,08:35:00,B,2,0",
        "t2,08:40:00,08:40:00,C,3,1",
        "t3,25:10:00,25:10:00,A,1,1",
        "t3,25:20:00,25:20:00,C,2,1",
    )
    write_csv(
        tmp_path / "calendar.txt",
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,start_date,end_date",
        "WK,1,1,1,1,1,0,0,20260801,20261031",
        "SAT,0,0,0,0,0,1,0,20260801,20261031",
    )
    write_csv(tmp_path / "calendar_dates.txt", "service_id,date,exception_type", "SAT,20260924,1", "WK,20260925,2")
    return tmp_path


def flat_params(**overrides) -> ModeParams:
    values = dict(
        initial_delay_quantiles=[(0.0, 60.0), (1.0, 60.0)],
        step_mean_s=0.0,
        step_sd_s=0.0,
        hourly_mean_s={},
        mean_delay_s=60.0,
        occupancy={"MANY_SEATS_AVAILABLE": 1.0},
    )
    return ModeParams(**{**values, **overrides})


def test_services_apply_calendar_exceptions(gtfs: Path):
    tt = Timetable(gtfs)
    assert tt.services_on(THURSDAY) == {"WK", "SAT"}  # SAT added by exception
    assert tt.services_on(date(2026, 9, 25)) == set()  # WK removed by exception
    assert [t.trip_id for t in tt.trips_on(THURSDAY)] == ["t3", "t1", "t2"]


def test_timetable_parses_times_past_midnight_and_modes(gtfs: Path):
    t3 = next(t for t in Timetable(gtfs).trips_on(THURSDAY) if t.trip_id == "t3")
    assert t3.mode == "rail"
    assert t3.stop_times[0].offset_s == 25 * 3600 + 600


def test_service_day_epoch_follows_noon_minus_12h_on_dst_change():
    # NZ daylight saving starts 2026-09-27: the reference time is 23:00 the previous evening, not midnight.
    normal = service_day_epoch(date(2026, 9, 26))
    assert datetime.fromtimestamp(normal, timezone.utc) == datetime(2026, 9, 25, 12, tzinfo=timezone.utc)
    dst = service_day_epoch(date(2026, 9, 27))
    assert datetime.fromtimestamp(dst, timezone.utc) == datetime(2026, 9, 26, 11, tzinfo=timezone.utc)


def test_sample_quantiles_interpolates():
    rng = random.Random(1)
    samples = [sample_quantiles(rng, [(0.0, -100.0), (0.5, 0.0), (1.0, 100.0)]) for _ in range(2000)]
    assert min(samples) >= -100 and max(samples) <= 100
    assert abs(sum(samples) / len(samples)) < 10


def test_simulated_trip_is_monotonic_and_respects_timepoint_floor(gtfs: Path):
    t1 = next(t for t in Timetable(gtfs).trips_on(THURSDAY) if t.trip_id == "t1")
    early = flat_params(initial_delay_quantiles=[(0.0, -300.0), (1.0, -300.0)], mean_delay_s=-300.0)
    run = simulate_trip(random.Random(0), t1, THURSDAY, early, incident=None)
    assert run.actual == sorted(run.actual)
    assert run.delays[0] == TIMEPOINT_EARLY_FLOOR_S  # stop 1 is a timepoint
    assert run.delays[1] < TIMEPOINT_EARLY_FLOOR_S  # stop 2 is not


def test_vehicles_are_reused_after_layover(gtfs: Path):
    trips = [t for t in Timetable(gtfs).trips_on(THURSDAY) if t.mode == "bus"]
    runs = [simulate_trip(random.Random(0), t, THURSDAY, flat_params(), None) for t in trips]
    assign_vehicles(runs, random.Random(0), {"bus": flat_params()})
    assert runs[0].vehicle_id == runs[1].vehicle_id  # t2 starts 20 min after t1 ends


def test_prediction_converges_to_actual_delay(gtfs: Path):
    t1 = next(t for t in Timetable(gtfs).trips_on(THURSDAY) if t.trip_id == "t1")
    run = Run(t1, THURSDAY, delays=[0, 120, 240], actual=[0, 0, 0], vehicle_id="2001", entity_id="e")
    base = service_day_epoch(THURSDAY)
    run.actual = [base + st.offset_s + d for st, d in zip(t1.stop_times, run.delays, strict=True)]

    just_before_b = trip_update_entity(run, run.actual[1] - 1)["trip_update"]["stop_time_update"][0]
    assert just_before_b["stop_id"] == "B" and just_before_b["arrival"]["delay"] == 120
    halfway_to_c = trip_update_entity(run, (run.actual[1] + run.actual[2]) // 2)["trip_update"]
    assert halfway_to_c["stop_time_update"][0]["arrival"]["delay"] == 180

    vehicle = vehicle_entity(run, (run.actual[0] + run.actual[1]) // 2, in_service=True)["vehicle"]
    assert vehicle["position"]["latitude"] == pytest.approx(-41.285)
    assert vehicle["trip"]["start_date"] == "20260924"


def test_generate_writes_collector_format(gtfs: Path, tmp_path: Path, monkeypatch):
    monkeypatch.setattr("simulator.generate.load_calibration", lambda: {"bus": flat_params(), "rail": flat_params()})
    runs = build_runs(Timetable(gtfs), [THURSDAY], seed=7)
    spool = tmp_path / "spool"
    assert generate(runs, alerts=[], poll_s=60, spool_dir=spool, seed=7) > 0

    files = sorted(spool.glob("tripupdates/*.jsonl.gz"))
    record = json.loads(gzip.open(files[0], "rt", encoding="utf-8").readline())
    assert set(record) == {"feed", "fetched_at", "header_timestamp", "entity_count", "feed_message"}
    assert record["feed_message"]["header"]["incrementality"] == "FULL_DATASET"
    assert volume_path_for(files[0], "/Volumes/x").startswith("/Volumes/x/realtime/tripupdates/date=")


def test_generation_is_deterministic(gtfs: Path, monkeypatch):
    monkeypatch.setattr("simulator.generate.load_calibration", lambda: load_calibration())
    a = build_runs(Timetable(gtfs), [THURSDAY], seed=3)
    b = build_runs(Timetable(gtfs), [THURSDAY], seed=3)
    assert [(r.trip.trip_id, r.actual, r.vehicle_id) for r in a] == [
        (r.trip.trip_id, r.actual, r.vehicle_id) for r in b
    ]


def test_committed_calibration_loads():
    calibration = load_calibration()
    assert "bus" in calibration
    assert calibration["bus"].initial_delay_quantiles[0][0] == 0.0
