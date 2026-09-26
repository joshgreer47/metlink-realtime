"""Downloads Metlink's static GTFS zip and uploads its CSVs to the UC volume.

Files land at <volume>/gtfs_static/<version>/<name>.txt, where <version> is the zip's
Last-Modified date. Re-running is a no-op if that version was already uploaded.

Usage:
    python -m poller.static_gtfs              # download + upload
    python -m poller.static_gtfs --no-upload  # download + unzip into data/gtfs_static only
"""

import argparse
import logging
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests

from poller.config import STATIC_GTFS_URL, load_settings

log = logging.getLogger("static_gtfs")


def feed_version(last_modified: str) -> str:
    return parsedate_to_datetime(last_modified).strftime("%Y-%m-%d")


def download(dest_dir: Path) -> Path:
    with requests.get(STATIC_GTFS_URL, stream=True, timeout=60) as r:
        r.raise_for_status()
        version = feed_version(r.headers["Last-Modified"])
        version_dir = dest_dir / version
        marker = version_dir / ".complete"
        if marker.exists():
            log.info("version %s already downloaded", version)
            return version_dir
        version_dir.mkdir(parents=True, exist_ok=True)
        zip_path = version_dir / "full.zip"
        with open(zip_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(version_dir)
    zip_path.unlink()
    marker.touch()
    log.info("downloaded version %s: %s", version, ", ".join(sorted(p.name for p in version_dir.glob("*.txt"))))
    return version_dir


def upload(version_dir: Path, volume_root: str) -> None:
    from poller.uploader import VolumeUploader

    marker = version_dir / ".uploaded"
    if marker.exists():
        log.info("version %s already uploaded", version_dir.name)
        return
    uploader = VolumeUploader()
    for path in sorted(version_dir.glob("*.txt")):
        uploader.upload(path, f"{volume_root}/gtfs_static/{version_dir.name}/{path.name}")
    marker.touch()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    settings = load_settings()
    version_dir = download(settings.data_dir / "gtfs_static")
    if not args.no_upload:
        upload(version_dir, settings.volume_path)


if __name__ == "__main__":
    main()
