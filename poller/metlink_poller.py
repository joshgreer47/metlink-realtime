"""Polls the Metlink GTFS-Realtime feeds and lands batched JSONL files in a UC volume.

Collection runs outside Databricks and pushes files into the workspace, so the platform
needs no outbound network access. Files are picked up incrementally by Auto Loader.

Each JSONL line is one poll of one feed:
    {"feed": ..., "fetched_at": ISO-8601 UTC, "header_timestamp": int,
     "entity_count": int, "feed_message": {...GTFS-RT FeedMessage as JSON...}}

Usage:
    python -m poller.metlink_poller              # poll forever, upload every BATCH_SECONDS
    python -m poller.metlink_poller --once       # one poll of each feed, then flush
    python -m poller.metlink_poller --no-upload  # keep files in data/spool only
"""

import argparse
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from google.protobuf.json_format import MessageToDict
from google.transit import gtfs_realtime_pb2

from poller.config import FEEDS, REALTIME_BASE_URL, Settings, load_settings

log = logging.getLogger("metlink_poller")


def parse_payload(content: bytes, content_type: str) -> dict:
    """Return the FeedMessage as a dict with GTFS-RT spec (snake_case) field names."""
    if "json" in content_type:
        return json.loads(content)
    message = gtfs_realtime_pb2.FeedMessage()
    message.ParseFromString(content)
    return MessageToDict(message, preserving_proto_field_name=True)


def make_record(feed: str, fetched_at: datetime, feed_message: dict) -> dict:
    header_ts = feed_message.get("header", {}).get("timestamp")
    return {
        "feed": feed,
        "fetched_at": fetched_at.isoformat(),
        "header_timestamp": int(header_ts) if header_ts is not None else None,
        "entity_count": len(feed_message.get("entity", [])),
        "feed_message": feed_message,
    }


def fetch_feed(session: requests.Session, feed: str, api_key: str) -> dict:
    response = session.get(
        f"{REALTIME_BASE_URL}/{feed}",
        headers={"x-api-key": api_key, "Accept": "application/x-protobuf"},
        timeout=20,
    )
    response.raise_for_status()
    return parse_payload(response.content, response.headers.get("Content-Type", ""))


class BatchWriter:
    """Buffers records per feed and writes one JSONL file per feed per flush."""

    def __init__(self, spool_dir: Path):
        self.spool_dir = spool_dir
        self._buffers: dict[str, list[dict]] = {}
        self._last_header_ts: dict[str, int] = {}

    def add(self, record: dict) -> bool:
        """Buffer a record. Returns False if the feed hasn't changed since the last poll."""
        feed, header_ts = record["feed"], record["header_timestamp"]
        if header_ts is not None and self._last_header_ts.get(feed) == header_ts:
            return False
        if header_ts is not None:
            self._last_header_ts[feed] = header_ts
        self._buffers.setdefault(feed, []).append(record)
        return True

    def flush(self, now: datetime) -> list[Path]:
        written = []
        stamp = now.strftime("%Y%m%dT%H%M%SZ")
        for feed, records in self._buffers.items():
            if not records:
                continue
            path = self.spool_dir / feed / f"{feed}_{stamp}.jsonl"
            path.parent.mkdir(parents=True, exist_ok=True)
            # Write to a temp name first so a crash never leaves a half-written .jsonl.
            tmp = path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                for record in records:
                    f.write(json.dumps(record, separators=(",", ":")) + "\n")
            tmp.replace(path)
            written.append(path)
            log.info("wrote %d polls -> %s", len(records), path.name)
        self._buffers = {}
        return written


def volume_path_for(spool_file: Path, volume_root: str) -> str:
    """data/spool/<feed>/<feed>_20260926T001500Z.jsonl -> <root>/realtime/<feed>/date=2026-09-26/<name>"""
    feed = spool_file.parent.name
    stamp = spool_file.stem.rsplit("_", 1)[1]
    date = f"{stamp[0:4]}-{stamp[4:6]}-{stamp[6:8]}"
    return f"{volume_root}/realtime/{feed}/date={date}/{spool_file.name}"


def upload_pending(spool_dir: Path, volume_root: str, uploader) -> int:
    """Upload every spooled file; delete each after success. Failures retry next flush."""
    uploaded = 0
    for path in sorted(spool_dir.glob("*/*.jsonl")):
        try:
            uploader.upload(path, volume_path_for(path, volume_root))
        except Exception:
            log.exception("upload failed for %s; will retry", path.name)
            break
        path.unlink()
        uploaded += 1
    return uploaded


def poll_once(session: requests.Session, settings: Settings, writer: BatchWriter) -> None:
    for feed in FEEDS:
        fetched_at = datetime.now(timezone.utc)
        try:
            feed_message = fetch_feed(session, feed, settings.metlink_api_key)
        except requests.RequestException as e:
            log.warning("%s: fetch failed: %s", feed, e)
            continue
        record = make_record(feed, fetched_at, feed_message)
        if writer.add(record):
            log.debug("%s: %d entities", feed, record["entity_count"])
        else:
            log.debug("%s: unchanged, skipped", feed)


def run(settings: Settings, once: bool, upload: bool) -> None:
    if not settings.metlink_api_key:
        raise SystemExit("METLINK_API_KEY is not set (see .env.example)")

    spool_dir = settings.data_dir / "spool"
    writer = BatchWriter(spool_dir)
    uploader = None
    if upload:
        from poller.uploader import VolumeUploader

        uploader = VolumeUploader()

    def flush() -> None:
        writer.flush(datetime.now(timezone.utc))
        if uploader:
            upload_pending(spool_dir, settings.volume_path, uploader)

    session = requests.Session()
    last_flush = time.monotonic()
    log.info(
        "polling %s every %ss, flushing every %ss (upload=%s)",
        ", ".join(FEEDS),
        settings.poll_interval_seconds,
        settings.batch_seconds,
        upload,
    )
    try:
        while True:
            started = time.monotonic()
            poll_once(session, settings, writer)
            if once:
                break
            if time.monotonic() - last_flush >= settings.batch_seconds:
                flush()
                last_flush = time.monotonic()
            time.sleep(max(0.0, settings.poll_interval_seconds - (time.monotonic() - started)))
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        flush()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="poll each feed once, flush, exit")
    parser.add_argument("--no-upload", action="store_true", help="keep files in data/spool")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    run(load_settings(), once=args.once, upload=not args.no_upload)


if __name__ == "__main__":
    main()
