"""Delay model: turns a scheduled trip into a simulated run with per-stop delays.

Measured behaviour (initial delay distribution, stop-to-stop delay drift, hourly mean delay, occupancy) comes from
calibration.json, produced by `python -m simulator.calibrate`. Behaviour the observed data cannot yet support is set
by the assumptions below.
"""

import bisect
import heapq
import json
import math
import random
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

from simulator.timetable import Trip, service_day_epoch

CALIBRATION_PATH = Path(__file__).with_name("calibration.json")

# --- Assumptions (not yet measurable from collected data) ---
PEAK_HOURS = ((7, 9), (16, 18))  # weekday local hours [start, end)
PEAK_EXTRA_DELAY_S = {"bus": 90, "rail": 45, "other": 0}
PEAK_DRIFT_MULTIPLIER = 1.3
MEAN_REVERSION = 0.05  # pull per stop towards the hourly mean delay
TIMEPOINT_EARLY_FLOOR_S = -60  # drivers hold at timepoints rather than run far ahead
INCIDENT_RATE_PER_ROUTE_DAY = 0.03
INCIDENT_DURATION_S = (1800, 5400)
INCIDENT_EXTRA_DELAY_S = (300, 900)
INCIDENT_PULL = 0.25
DELAY_BOUNDS_S = (-600, 3600)
LAYOVER_S = 300
VEHICLE_ID_BASE = {"bus": 2000, "rail": 5000, "other": 7000}
MIN_STEPS_FOR_DRIFT = 500  # below this many observed steps, a measured mean drift is treated as noise


@dataclass
class ModeParams:
    initial_delay_quantiles: list[tuple[float, float]]
    step_mean_s: float
    step_sd_s: float
    hourly_mean_s: dict[str, dict[int, float]]
    mean_delay_s: float
    occupancy: dict[str, float]


def load_calibration(path: Path = CALIBRATION_PATH) -> dict[str, ModeParams]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    params = {}
    for mode, m in raw["modes"].items():
        params[mode] = ModeParams(
            initial_delay_quantiles=[(float(p), float(v)) for p, v in m["initial_delay_quantiles"]],
            step_mean_s=m["step_mean_s"] if m.get("steps", 0) >= MIN_STEPS_FOR_DRIFT else 0.0,
            step_sd_s=m["step_sd_s"],
            hourly_mean_s={dt: {int(h): v for h, v in hours.items()} for dt, hours in m["hourly_mean_s"].items()},
            mean_delay_s=m["mean_delay_s"],
            occupancy=m["occupancy"],
        )
    return params


def params_for(calibration: dict[str, ModeParams], mode: str) -> ModeParams:
    return calibration.get(mode) or calibration["bus"]


def sample_quantiles(rng: random.Random, quantiles: list[tuple[float, float]]) -> float:
    """Inverse-CDF sample from a piecewise-linear quantile function."""
    u = rng.random()
    ps = [p for p, _ in quantiles]
    i = min(max(bisect.bisect_right(ps, u), 1), len(quantiles) - 1)
    (p0, v0), (p1, v1) = quantiles[i - 1], quantiles[i]
    return v0 if p1 == p0 else v0 + (v1 - v0) * (u - p0) / (p1 - p0)


def laplace(rng: random.Random, mean: float, sd: float) -> float:
    b = sd / math.sqrt(2)
    return mean + rng.expovariate(1 / b) - rng.expovariate(1 / b) if b > 0 else mean


def day_type(d: date) -> str:
    return {5: "saturday", 6: "sunday"}.get(d.weekday(), "weekday")


def is_peak(d: date, hour: int) -> bool:
    return day_type(d) == "weekday" and any(a <= hour < b for a, b in PEAK_HOURS)


def hourly_target(p: ModeParams, d: date, hour: int) -> float:
    by_hour = p.hourly_mean_s.get(day_type(d)) or {}
    return by_hour.get(hour % 24, p.mean_delay_s)


@dataclass
class Incident:
    start_s: int
    end_s: int
    extra_s: float


def draw_incidents(rng: random.Random, route_ids: set[str]) -> dict[str, Incident]:
    incidents = {}
    for route_id in sorted(route_ids):
        if rng.random() < INCIDENT_RATE_PER_ROUTE_DAY:
            start = rng.randint(6 * 3600, 20 * 3600)
            incidents[route_id] = Incident(
                start, start + rng.randint(*INCIDENT_DURATION_S), rng.uniform(*INCIDENT_EXTRA_DELAY_S)
            )
    return incidents


@dataclass
class Run:
    """A trip as simulated: actual arrival epoch and delay at every stop."""

    trip: Trip
    service_date: date
    delays: list[int]
    actual: list[int]
    vehicle_id: str = ""
    occupancy: str | None = None
    entity_id: str = ""
    extra: dict = field(default_factory=dict)

    @property
    def start(self) -> int:
        return self.actual[0]

    @property
    def end(self) -> int:
        return self.actual[-1]


def simulate_trip(rng: random.Random, trip: Trip, service_date: date, p: ModeParams, incident: Incident | None) -> Run:
    base = service_day_epoch(service_date)
    first_hour = trip.stop_times[0].offset_s // 3600
    delay = sample_quantiles(rng, p.initial_delay_quantiles)
    if is_peak(service_date, first_hour):
        delay += PEAK_EXTRA_DELAY_S.get(trip.mode, 0)

    delays, actual = [], []
    for i, st in enumerate(trip.stop_times):
        hour = st.offset_s // 3600
        peak = is_peak(service_date, hour)
        target = hourly_target(p, service_date, hour) + (PEAK_EXTRA_DELAY_S.get(trip.mode, 0) if peak else 0)
        if i > 0:
            drift_sd = p.step_sd_s * (PEAK_DRIFT_MULTIPLIER if peak else 1.0)
            delay += laplace(rng, p.step_mean_s, drift_sd) + MEAN_REVERSION * (target - delay)
        if incident and incident.start_s <= st.offset_s < incident.end_s:
            delay += INCIDENT_PULL * (target + incident.extra_s - delay)
        if st.is_timepoint:
            delay = max(delay, TIMEPOINT_EARLY_FLOOR_S)
        delay = min(max(delay, DELAY_BOUNDS_S[0]), DELAY_BOUNDS_S[1])

        arrival = base + st.offset_s + round(delay)
        if actual and arrival < actual[-1]:
            arrival = actual[-1]
        delays.append(arrival - base - st.offset_s)
        actual.append(arrival)
    return Run(trip, service_date, delays, actual)


def assign_vehicles(runs: list[Run], rng: random.Random, calibration: dict[str, ModeParams]) -> None:
    """Chain trips onto vehicles per mode: a vehicle takes the next trip starting after its layover."""
    by_mode: dict[str, list[Run]] = {}
    for run in runs:
        by_mode.setdefault(run.trip.mode, []).append(run)
    for mode, mode_runs in sorted(by_mode.items()):
        free: list[tuple[int, str]] = []
        next_id = VEHICLE_ID_BASE.get(mode, 9000)
        occupancy = params_for(calibration, mode).occupancy
        for run in sorted(mode_runs, key=lambda r: (r.start, r.trip.trip_id)):
            if free and free[0][0] + LAYOVER_S <= run.start:
                _, vehicle_id = heapq.heappop(free)
            else:
                vehicle_id, next_id = str(next_id), next_id + 1
            run.vehicle_id = vehicle_id
            run.occupancy = sample_category(rng, occupancy)
            heapq.heappush(free, (run.end, vehicle_id))


def sample_category(rng: random.Random, weights: dict[str, float]) -> str | None:
    """Weighted choice; the key "null" means the field is absent."""
    if not weights:
        return None
    choice = rng.choices(list(weights), weights=list(weights.values()))[0]
    return None if choice == "null" else choice
