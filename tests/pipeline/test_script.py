
import inspect

import pytest

from process_framework.pipeline.clients import EmptyClients
from process_framework.pipeline.script import ScriptBase

from .fixtures import PipelineSettings, ScriptDefinition


def test_script_base_is_abstract():
    assert inspect.isabstract(ScriptBase)


def test_from_settings():
    settings = PipelineSettings(name="hello")

    script = ScriptDefinition.from_settings(settings)

    assert isinstance(script, ScriptDefinition)
    assert script.settings is settings
    assert isinstance(script.clients, EmptyClients)


def test_from_environment(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("NAME", "environment-test")

    script = ScriptDefinition.from_environment(argsv=[])

    assert script.settings.name == "environment-test"
    assert isinstance(script.clients, EmptyClients)


def test_from_environment_with_override(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setenv("NAME", "environment-test")

    script = ScriptDefinition.from_environment(
        argsv=[],
        name="override-test",
    )

    assert script.settings.name == "override-test"


def test_script_execution():
    script = ScriptDefinition.from_settings(
        PipelineSettings(name="hello")
    )

    assert script.result is None

    script.do()

    assert script.result == "hello"


def test_script_uses_current_settings():
    script = ScriptDefinition.from_settings(
        PipelineSettings(name="first")
    )

    script.do()
    assert script.result == "first"

    script.settings.name = "second"

    script.do()
    assert script.result == "second"


def test_script_instances_are_independent():
    first = ScriptDefinition.from_settings(
        PipelineSettings(name="first")
    )
    second = ScriptDefinition.from_settings(
        PipelineSettings(name="second")
    )

    assert first is not second
    assert first.settings is not second.settings
    assert first.clients is not second.clients

    first.do()
    second.do()

    assert first.result == "first"
    assert second.result == "second"


def test_script_propagates_step_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    from .fixtures import AssignName

    def fail(self: AssignName) -> None:
        raise RuntimeError("Step failed")

    monkeypatch.setattr(AssignName, "do", fail)

    script = ScriptDefinition.from_settings(
        PipelineSettings(name="hello")
    )

    with pytest.raises(RuntimeError, match="Step failed"):
        script.do()

    assert script.result is None
