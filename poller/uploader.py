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

    def volume_exists(self, volume_path: str) -> bool:
        from databricks.sdk.errors import NotFound

        try:
            self._client.files.get_directory_metadata(volume_path)
            return True
        except NotFound:
            return False

    def list_files(self, directory: str) -> dict[str, int]:
        """Every file under directory (recursively) mapped to its size; empty if the directory does not exist."""
        from databricks.sdk.errors import NotFound

        files: dict[str, int] = {}
        try:
            for entry in self._client.files.list_directory_contents(directory):
                if entry.is_directory:
                    files.update(self.list_files(entry.path))
                else:
                    files[entry.path] = entry.file_size
        except NotFound:
            pass
        return files

    def upload(self, local_path: Path, volume_file_path: str) -> None:
        with open(local_path, "rb") as f:
            self._client.files.upload(volume_file_path, f, overwrite=True)
        log.debug("uploaded %s -> %s", local_path.name, volume_file_path)
