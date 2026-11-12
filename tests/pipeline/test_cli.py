
import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from process_framework.pipeline.cli import EnvironmentSettings, Runner


@pytest.mark.parametrize(
    ("verbosity", "expected"),
    [
        (0, logging.WARNING),
        (1, logging.INFO),
        (2, logging.DEBUG),
        (3, logging.DEBUG),
    ],
)
def test_log_level(verbosity: int, expected: int):
    settings = EnvironmentSettings(verbosity=verbosity)

    assert settings.get_log_level() == expected


def test_environment_settings_defaults():
    settings = EnvironmentSettings.from_environment(argsv=[])

    assert settings.verbosity == 1
    assert settings.log_console is False
    assert settings.log_file is None
    assert settings.log_max_bytes == 10_485_760
    assert settings.log_backup_count == 10


def test_environment_settings_cli(tmp_path: Path):
    log_file = tmp_path / "logs" / "process.log"

    settings = EnvironmentSettings.from_environment(
        argsv=[
            "-vv",
            "--log-console",
            "--log-file", str(log_file),
            "--log-max-bytes", "1024",
            "--log-backup-count", "3",
        ]
    )

    assert settings.verbosity == 2
    assert settings.log_console is True
    assert settings.log_file == log_file
    assert settings.log_max_bytes == 1024
    assert settings.log_backup_count == 3


def test_environment_settings_overrides():
    settings = EnvironmentSettings.from_environment(
        argsv=["--log-console"],
        log_console=False,
        log_backup_count=5,
    )

    assert settings.log_console is False
    assert settings.log_backup_count == 5


def test_environment_settings_ignores_unknown_args():
    settings = EnvironmentSettings.from_environment(
        argsv=["--unknown", "value", "--log-console"],
    )

    assert settings.log_console is True
