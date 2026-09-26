"""Measures simulator parameters from collected data and writes simulator/calibration.json.

Runs read-only queries on a SQL warehouse against the collected (not simulated) catalog.

Usage:
    python -m simulator.calibrate                    # catalog metlink, first available warehouse
    python -m simulator.calibrate --catalog metlink --warehouse-id <id>
"""

import argparse
import json
import logging
import re
from datetime import datetime, timezone

from simulator.model import CALIBRATION_PATH

log = logging.getLogger("calibrate")

QUANTILES = [0.0, 0.02, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 0.98, 1.0]
MIN_HOURLY_OBSERVATIONS = 50
# Starting delays come from each trip's first stop once enough departures have been sighted there. Until then, the
# first delay seen for each trip is used, which overstates it for trips first seen mid-route.
MIN_ORIGIN_DEPARTURES = 100

MODE = """CASE WHEN r.route_type IN ('3', '700', '712') THEN 'bus'
               WHEN r.route_type IN ('2', '100') THEN 'rail' ELSE 'other' END"""
ARRIVAL_MODE = """CASE WHEN mode IN ('bus', 'school bus') THEN 'bus' WHEN mode = 'rail' THEN 'rail' ELSE 'other' END"""

DAY_TYPE = """CASE dayofweek(updated_at_local) WHEN 1 THEN 'sunday' WHEN 7 THEN 'saturday' ELSE 'weekday' END"""


def queries(catalog: str) -> dict[str, str]:
    tu = f"""(SELECT u.*, {MODE} AS mode FROM {catalog}.silver.trip_updates u
              LEFT JOIN {catalog}.bronze.gtfs_routes r USING (route_id))"""
    q = ", ".join(str(x) for x in QUANTILES)
    return {
        "window": f"""SELECT min(updated_at), max(updated_at), count(DISTINCT trip_id, service_date)
                      FROM {catalog}.silver.trip_updates""",
        "initial": f"""SELECT mode, count(*), percentile_approx(d0, array({q}))
                       FROM (SELECT mode, min_by(arrival_delay_s, updated_at) d0
                             FROM {tu} GROUP BY trip_id, service_date, mode)
                       GROUP BY mode""",
        "origin": f"""SELECT {ARRIVAL_MODE} AS m, count(*), percentile_approx(arrival_delay_s, array({q}))
                      FROM {catalog}.silver.stop_arrivals WHERE is_origin AND is_final GROUP BY 1""",
        "steps": f"""WITH s AS (SELECT trip_id, service_date, mode, stop_sequence,
                                       max_by(arrival_delay_s, updated_at) d FROM {tu} GROUP BY ALL),
                     l AS (SELECT mode,
                                  d - lag(d) OVER (PARTITION BY trip_id, service_date ORDER BY stop_sequence) dd,
                                  stop_sequence - lag(stop_sequence)
                                      OVER (PARTITION BY trip_id, service_date ORDER BY stop_sequence) ds
                           FROM s)
                     SELECT mode, count(*), avg(dd), stddev(dd), avg(ds) FROM l WHERE ds > 0 GROUP BY mode""",
        "hourly": f"""SELECT mode, {DAY_TYPE}, hour(updated_at_local), count(*), avg(arrival_delay_s)
                      FROM {tu} GROUP BY ALL""",
        "overall": f"SELECT mode, avg(arrival_delay_s) FROM {tu} GROUP BY mode",
        "occupancy": f"""SELECT {MODE}, coalesce(v.occupancy_status, 'null'), count(*)
                         FROM {catalog}.silver.vehicle_positions v
                         LEFT JOIN {catalog}.bronze.gtfs_routes r USING (route_id)
                         WHERE v.trip_id IS NOT NULL GROUP BY ALL""",
    }


def run_query(client, warehouse_id: str, sql: str) -> list[list]:
    r = client.statement_execution.execute_statement(statement=sql, warehouse_id=warehouse_id, wait_timeout="50s")
    if r.status.error:
        raise RuntimeError(r.status.error.message)
    return (r.result.data_array if r.result else None) or []


def calibrate(client, warehouse_id: str, catalog: str) -> dict:
    results = {name: run_query(client, warehouse_id, sql) for name, sql in queries(catalog).items()}
    modes: dict[str, dict] = {}

    def quantiles(qs) -> list[list[float]]:
        values = json.loads(qs) if isinstance(qs, str) else qs
        return [[p, float(v)] for p, v in zip(QUANTILES, values, strict=True)]

    for mode, n, qs in results["initial"]:
        modes[mode] = {
            "trips": int(n),
            "initial_delay_source": "first seen",
            "initial_delay_quantiles": quantiles(qs),
        }
    for mode, n, qs in results["origin"]:
        if mode in modes and int(n) >= MIN_ORIGIN_DEPARTURES:
            modes[mode].update(
                initial_delay_source="origin departures",
                origin_departures=int(n),
                initial_delay_quantiles=quantiles(qs),
            )
    for mode, n, mean, sd, gap in results["steps"]:
        if mode in modes:
            gap = float(gap)
            # Observed steps span `gap` stops on average; scale to a single stop.
            modes[mode].update(
                steps=int(n), step_mean_s=round(float(mean) / gap, 2), step_sd_s=round(float(sd) / gap**0.5, 2)
            )
    for mode, mean in results["overall"]:
        if mode in modes:
            modes[mode]["mean_delay_s"] = round(float(mean), 1)
    for mode in modes:
        modes[mode]["hourly_mean_s"] = {}
    for mode, day_type, hour, n, mean in results["hourly"]:
        if mode in modes and int(n) >= MIN_HOURLY_OBSERVATIONS:
            modes[mode]["hourly_mean_s"].setdefault(day_type, {})[str(hour)] = round(float(mean), 1)
    for p in modes.values():
        p["hourly_mean_s"] = {
            dt: dict(sorted(hours.items(), key=lambda kv: int(kv[0])))
            for dt, hours in sorted(p["hourly_mean_s"].items())
        }
    occupancy: dict[str, dict[str, int]] = {}
    for mode, status, n in results["occupancy"]:
        occupancy.setdefault(mode, {})[status] = int(n)
    for mode, counts in occupancy.items():
        if mode in modes:
            total = sum(counts.values())
            modes[mode]["occupancy"] = {k: round(v / total, 4) for k, v in sorted(counts.items())}

    modes = {m: p for m, p in modes.items() if "step_sd_s" in p}
    for p in modes.values():
        p.setdefault("occupancy", {})
    (start, end, trips), *_ = results["window"]
    return {
        "source": {
            "catalog": catalog,
            "observed_from": start,
            "observed_to": end,
            "trips": int(trips),
            "calibrated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        },
        "modes": dict(sorted(modes.items())),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--catalog", default="metlink")
    parser.add_argument("--warehouse-id")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    from databricks.sdk import WorkspaceClient

    import poller.config  # noqa: F401  (loads .env)

    client = WorkspaceClient()
    warehouse_id = args.warehouse_id or next(iter(client.warehouses.list())).id
    calibration = calibrate(client, warehouse_id, args.catalog)
    if "bus" not in calibration["modes"]:
        raise SystemExit("Not enough bus data to calibrate; collect more first")
    text = json.dumps(calibration, indent=2)
    text = re.sub(r"\[\s+(-?[\d.]+),\s+(-?[\d.]+)\s+\]", r"[\1, \2]", text)  # one quantile pair per line
    CALIBRATION_PATH.write_text(text + "\n", encoding="utf-8")
    log.info(
        "wrote %s from %d trips (%s to %s)",
        CALIBRATION_PATH.name,
        calibration["source"]["trips"],
        calibration["source"]["observed_from"],
        calibration["source"]["observed_to"],
    )


if __name__ == "__main__":
    main()
