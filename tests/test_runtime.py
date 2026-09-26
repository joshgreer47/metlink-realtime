from pathlib import Path

import pytest

from poller.runtime import AlreadyRunning, keep_awake, single_instance


def test_single_instance_blocks_second_holder(tmp_path: Path):
    lock = tmp_path / "poller.lock"
    with single_instance(lock):
        with pytest.raises(AlreadyRunning):
            with single_instance(lock):
                pass
    with single_instance(lock):  # released on exit
        pass


def test_keep_awake_disabled_is_noop():
    with keep_awake(False):
        pass
