"""Settings loaded from environment variables (and .env if present)."""

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(REPO_ROOT / ".env")

REALTIME_BASE_URL = "https://api.opendata.metlink.org.nz/v1/gtfs-rt"
STATIC_GTFS_URL = "https://static.opendata.metlink.org.nz/v1/gtfs/full.zip"
FEEDS = ("tripupdates", "vehiclepositions", "servicealerts")


@dataclass(frozen=True)
class Settings:
    metlink_api_key: str
    volume_path: str
    poll_interval_seconds: int
    batch_seconds: int
    static_refresh_hours: float
    data_dir: Path


def load_settings() -> Settings:
    return Settings(
        metlink_api_key=os.environ.get("METLINK_API_KEY", ""),
        volume_path=os.environ.get("METLINK_VOLUME_PATH", "/Volumes/metlink/bronze/raw").rstrip("/"),
        poll_interval_seconds=int(os.environ.get("POLL_INTERVAL_SECONDS", "30")),
        batch_seconds=int(os.environ.get("BATCH_SECONDS", "300")),
        static_refresh_hours=float(os.environ.get("STATIC_REFRESH_HOURS", "24")),
        data_dir=REPO_ROOT / "data",
    )
