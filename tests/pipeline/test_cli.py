
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from process_framework import PipelineDefinitionBase
from process_framework.pipeline.cli import EnvironmentSettings, Runner


# ----------------------------------------
# Fixtures
# ----------------------------------------

@pytest.fixture
def mock_definition() -> MagicMock:
    return MagicMock(spec=PipelineDefinitionBase)


@pytest.fixture
def mock_env_type() -> MagicMock:
    return MagicMock(spec=EnvironmentSettings)


@pytest.fixture
def isolated_logger() -> Iterator[logging.Logger]:
    logger = logging.getLogger("test.process_framework.cli")
    original_level = logger.level
    original_propagate = logger.propagate
    original_handlers = list(logger.handlers)

    for handler in original_handlers:
        logger.removeHandler(handler)

    logger.propagate = False

    yield logger

    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    for handler in original_handlers:
        logger.addHandler(handler)

    logger.setLevel(original_level)
    logger.propagate = original_propagate


# ----------------------------------------
# EnvironmentSettings
# ----------------------------------------

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


# ----------------------------------------
# Runner: logging configuration
# ----------------------------------------

def test_configure_console_logging(
    mock_definition: MagicMock,
    isolated_logger: logging.Logger,
    capsys: pytest.CaptureFixture[str],
):
    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
    )
    settings = EnvironmentSettings(log_console=True)

    runner.configure_logging(settings, isolated_logger)
    isolated_logger.info("Hello console")

    assert "Hello console" in capsys.readouterr().out


def test_configure_file_logging(
    mock_definition: MagicMock,
    isolated_logger: logging.Logger,
    tmp_path: Path,
):
    log_file = tmp_path / "logs" / "process.log"

    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
    )
    settings = EnvironmentSettings(
        log_file=log_file,
        log_max_bytes=1024,
        log_backup_count=3,
    )

    runner.configure_logging(settings, isolated_logger)
    isolated_logger.info("Hello file")

    assert log_file.exists()
    assert "Hello file" in log_file.read_text(encoding="utf-8")


def test_configure_logging_replaces_handlers(
    mock_definition: MagicMock,
    isolated_logger: logging.Logger,
):
    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
    )
    settings = EnvironmentSettings(log_console=True)

    runner.configure_logging(settings, isolated_logger)
    first_handler = isolated_logger.handlers[0]

    runner.configure_logging(settings, isolated_logger)

    assert len(isolated_logger.handlers) == 1
    assert isolated_logger.handlers[0] is not first_handler


# ----------------------------------------
# Runner: execution
# ----------------------------------------

def test_runner_execution(
    mock_definition: MagicMock,
    mock_env_type: MagicMock,
):
    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
        environment_settings=cast(type[EnvironmentSettings], mock_env_type),
    )
    runner.configure_logging = MagicMock()

    result = runner.run(argsv=[])

    assert result == 0

    mock_definition.from_environment.assert_called_once_with(
        argsv=[],
    )

    pipeline = (
        mock_definition.from_environment.return_value
        .instantiate_pipeline.return_value
    )

    pipeline.log_steps.assert_called_once()
    pipeline.preflight.assert_called_once()
    pipeline.do.assert_called_once()


def test_runner_argument_forwarding(
    mock_definition: MagicMock,
    mock_env_type: MagicMock,
):
    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
        environment_settings=cast(type[EnvironmentSettings], mock_env_type),
    )
    runner.configure_logging = MagicMock()

    runner.run(
        argsv=["--limit", "100"],
        custom="value",
    )

    mock_definition.from_environment.assert_called_once_with(
        argsv=["--limit", "100"],
        custom="value",
    )

    mock_env_type.from_environment.assert_called_once_with(
        ["--limit", "100"],
        custom="value",
    )


def test_runner_propagates_errors(
    mock_definition: MagicMock,
    mock_env_type: MagicMock,
):
    pipeline = (
        mock_definition.from_environment.return_value
        .instantiate_pipeline.return_value
    )
    pipeline.do.side_effect = RuntimeError("Pipeline failed")

    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
        environment_settings=cast(type[EnvironmentSettings], mock_env_type),
    )
    runner.configure_logging = MagicMock()

    with pytest.raises(RuntimeError, match="Pipeline failed"):
        runner.run(argsv=[])

    pipeline.log_steps.assert_called_once()
    pipeline.preflight.assert_called_once()
    pipeline.do.assert_called_once()


def test_runner_uses_sys_argv(
    monkeypatch: pytest.MonkeyPatch,
    mock_definition: MagicMock,
    mock_env_type: MagicMock,
):
    monkeypatch.setattr(
        sys,
        "argv",
        ["process", "--log-console", "-vv"],
    )

    runner = Runner(
        definition=cast(type[PipelineDefinitionBase], mock_definition),
        environment_settings=cast(type[EnvironmentSettings], mock_env_type),
    )
    runner.configure_logging = MagicMock()

    runner.run()

    mock_env_type.from_environment.assert_called_once_with(
        ["--log-console", "-vv"],
    )

    mock_definition.from_environment.assert_called_once_with(
        argsv=["--log-console", "-vv"],
    )
