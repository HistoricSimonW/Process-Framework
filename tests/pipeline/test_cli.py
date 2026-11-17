
import logging
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock

import pytest

from process_framework import PipelineDefinitionBase
from process_framework.pipeline.cli import EnvironmentSettings, Runner


from unittest.mock import MagicMock, call, patch

from process_framework.pipeline import Pipeline
from process_framework.pipeline.script import ScriptBase

from .fixtures import (
    PipelineDefinition,
    PipelineSettings,
    ScriptDefinition,
)


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

@pytest.fixture
def mock_runner_environment() -> MagicMock:
    return MagicMock(spec=EnvironmentSettings)


def test_runner_pipeline_execution(
    mock_runner_environment: MagicMock,
):
    definition = PipelineDefinition.from_settings(
        PipelineSettings(name="hello")
    )
    pipeline = MagicMock(spec=Pipeline)

    runner = Runner(
        definition=PipelineDefinition,
        environment_settings=cast(
            type[EnvironmentSettings],
            mock_runner_environment,
        ),
    )
    runner.configure_logging = MagicMock()

    with (
        patch.object(
            PipelineDefinition,
            "from_environment",
            return_value=definition,
        ) as construct,
        patch.object(
            definition,
            "instantiate_pipeline",
            return_value=pipeline,
        ),
    ):
        result = runner.run(argsv=[])

    assert result == 0
    construct.assert_called_once_with(argsv=[])
    pipeline.log_steps.assert_called_once_with()
    pipeline.preflight.assert_called_once_with()
    pipeline.do.assert_called_once_with()

    assert pipeline.mock_calls == [
        call.log_steps(),
        call.preflight(),
        call.do(),
    ]


def test_runner_script_execution(
    mock_runner_environment: MagicMock,
):
    script = ScriptDefinition.from_settings(
        PipelineSettings(name="hello")
    )

    runner = Runner(
        definition=ScriptDefinition,
        environment_settings=cast(
            type[EnvironmentSettings],
            mock_runner_environment,
        ),
    )
    runner.configure_logging = MagicMock()

    with (
        patch.object(
            ScriptDefinition,
            "from_environment",
            return_value=script,
        ) as construct,
        patch.object(script, "do", wraps=script.do) as execute,
    ):
        result = runner.run(argsv=[])

    assert result == 0
    construct.assert_called_once_with(argsv=[])
    execute.assert_called_once_with()
    assert script.result == "hello"


@pytest.mark.parametrize(
    "definition_type",
    [PipelineDefinition, ScriptDefinition],
)
def test_runner_argument_forwarding(
    definition_type,
    mock_runner_environment: MagicMock,
):
    runner = Runner(
        definition=definition_type,
        environment_settings=cast(
            type[EnvironmentSettings],
            mock_runner_environment,
        ),
    )
    runner.configure_logging = MagicMock()

    instance = definition_type.from_settings(
        PipelineSettings(name="hello")
    )

    with patch.object(
        definition_type,
        "from_environment",
        return_value=instance,
    ) as construct:
        runner.run(
            argsv=["--limit", "100"],
            custom="value",
        )

    construct.assert_called_once_with(
        argsv=["--limit", "100"],
        custom="value",
    )
    mock_runner_environment.from_environment.assert_called_once_with(
        ["--limit", "100"],
        custom="value",
    )


@pytest.mark.parametrize(
    "definition_type",
    [PipelineDefinition, ScriptDefinition],
)
def test_runner_uses_sys_argv(
    monkeypatch: pytest.MonkeyPatch,
    definition_type,
    mock_runner_environment: MagicMock,
):
    monkeypatch.setattr(
        sys,
        "argv",
        ["process", "--log-console", "-vv"],
    )

    runner = Runner(
        definition=definition_type,
        environment_settings=cast(
            type[EnvironmentSettings],
            mock_runner_environment,
        ),
    )
    runner.configure_logging = MagicMock()

    instance = definition_type.from_settings(
        PipelineSettings(name="hello")
    )

    with patch.object(
        definition_type,
        "from_environment",
        return_value=instance,
    ) as construct:
        runner.run()

    construct.assert_called_once_with(
        argsv=["--log-console", "-vv"],
    )
    mock_runner_environment.from_environment.assert_called_once_with(
        ["--log-console", "-vv"],
    )


@pytest.mark.parametrize(
    "definition_type",
    [PipelineDefinition, ScriptDefinition],
)
def test_runner_propagates_errors(
    definition_type,
    mock_runner_environment: MagicMock,
):
    instance = definition_type.from_settings(
        PipelineSettings(name="hello")
    )

    runner = Runner(
        definition=definition_type,
        environment_settings=cast(
            type[EnvironmentSettings],
            mock_runner_environment,
        ),
    )
    runner.configure_logging = MagicMock()

    if isinstance(instance, PipelineDefinitionBase):
        pipeline = MagicMock(spec=Pipeline)
        pipeline.do.side_effect = RuntimeError("Execution failed")
        execution_patch = patch.object(
            instance,
            "instantiate_pipeline",
            return_value=pipeline,
        )
    else:
        execution_patch = patch.object(
            instance,
            "do",
            side_effect=RuntimeError("Execution failed"),
        )

    with (
        patch.object(
            definition_type,
            "from_environment",
            return_value=instance,
        ),
        execution_patch,
        pytest.raises(RuntimeError, match="Execution failed"),
    ):
        runner.run(argsv=[])


def test_runner_rejects_unsupported_definition(
    mock_runner_environment: MagicMock,
):
    unsupported = MagicMock()
    unsupported.from_environment.return_value = object()

    runner = Runner(
        definition=cast(
            type[PipelineDefinitionBase | ScriptBase],
            unsupported,
        ),
        environment_settings=cast(
            type[EnvironmentSettings],
            mock_runner_environment,
        ),
    )
    runner.configure_logging = MagicMock()

    with pytest.raises(
        TypeError,
        match="Unsupported definition: object",
    ):
        runner.run(argsv=[])
