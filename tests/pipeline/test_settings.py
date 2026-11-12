import pytest

from .fixtures import PipelineDefinition


def test_from_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NAME", "environment-test")

    definition = PipelineDefinition.from_environment(argsv=[])

    assert definition.settings.name == "environment-test"


def test_from_environment_with_override(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("NAME", "environment-test")

    definition = PipelineDefinition.from_environment(
        argsv=[],
        name="override-test",
    )

    assert definition.settings.name == "override-test"