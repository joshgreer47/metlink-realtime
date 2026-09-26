"""Downloads Metlink's published bus performance CSVs and uploads new releases to the UC volume.

Metlink publishes cumulative daily and weekly per-route files (e.g. metlink-weekly-bus-performance-to-2026-03-29.csv)
on its "Performance of our network" page. Each release is uploaded once, to
<volume>/official/bus_performance/<daily|weekly>/<file name>.

Usage:
    python -m poller.official_performance              # download and upload new releases
    python -m poller.official_performance --no-upload  # download only, into data/official
"""

import argparse
import logging
import re
from pathlib import Path
from urllib.parse import urljoin

import requests

from poller.config import Settings, load_settings

log = logging.getLogger("official_performance")

PAGE_URL = "https://www.metlink.org.nz/about-us/performance-of-our-network"
FILE_PATTERN = re.compile(r"""["']([^"']*/(metlink-(daily|weekly)-bus-performance-to-\d{4}-\d{2}-\d{2}\.csv))["']""")


def find_releases(html: str, base_url: str = PAGE_URL) -> dict[str, tuple[str, str]]:
    """{file name: (kind, absolute URL)} for every performance CSV linked from the page."""
    return {name: (kind, urljoin(base_url, href)) for href, name, kind in FILE_PATTERN.findall(html)}


def refresh(settings: Settings, upload_enabled: bool = True) -> list[Path]:
    """Download releases not seen before and upload any not yet uploaded. Returns newly downloaded files."""
    session = requests.Session()
    page = session.get(PAGE_URL, timeout=30)
    page.raise_for_status()
    releases = find_releases(page.text)
    if not releases:
        log.warning("no performance CSVs found on %s; the page layout may have changed", PAGE_URL)
        return []

    root = settings.data_dir / "official" / "bus_performance"
    downloaded = []
    for name, (kind, url) in sorted(releases.items()):
        path = root / kind / name
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            partial = path.with_name(name + ".partial")
            with session.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(partial, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        f.write(chunk)
            partial.replace(path)
            downloaded.append(path)
            log.info("downloaded %s", name)

    if upload_enabled:
        from poller.uploader import VolumeUploader

        uploader = None
        for path in sorted(root.glob("*/*.csv")):
            marker = path.with_name(path.name + ".uploaded")
            if marker.exists():
                continue
            uploader = uploader or VolumeUploader()
            uploader.upload(path, f"{settings.volume_path}/official/bus_performance/{path.parent.name}/{path.name}")
            marker.touch()
            log.info("uploaded %s", path.name)
    return downloaded


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-upload", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    refresh(load_settings(), upload_enabled=not args.no_upload)


if __name__ == "__main__":
    main()
