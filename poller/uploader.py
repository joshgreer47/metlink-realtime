"""Uploads local files into a Unity Catalog volume via the Databricks Files API.

Auth comes from DATABRICKS_HOST / DATABRICKS_TOKEN (or a ~/.databrickscfg profile),
which the Databricks SDK picks up automatically.
"""

import logging
from pathlib import Path

log = logging.getLogger(__name__)


class VolumeUploader:
    def __init__(self):
        # Imported lazily so --no-upload runs don't need Databricks credentials.
        from databricks.sdk import WorkspaceClient

        self._client = WorkspaceClient()

    def upload(self, local_path: Path, volume_file_path: str) -> None:
        with open(local_path, "rb") as f:
            self._client.files.upload(volume_file_path, f, overwrite=True)
        log.info("uploaded %s -> %s", local_path.name, volume_file_path)
