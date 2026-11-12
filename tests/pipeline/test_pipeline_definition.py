from process_framework import Reference
from process_framework.pipeline import Pipeline
from process_framework.pipeline.clients import EmptyClients
from process_framework.steps.logging_step import LogInput

from .fixtures import (
    AssignName,
    PipelineDefinition,
    PipelineReferences,
    PipelineSettings,
)


def test_from_settings(
    settings: PipelineSettings,
    definition: PipelineDefinition,
):
    assert definition.settings is settings
    assert isinstance(definition.references, PipelineReferences)
    assert isinstance(definition.clients, EmptyClients)


def test_references_initialized(definition: PipelineDefinition):
    assert isinstance(definition.references.name, Reference)
    assert not definition.references.name.has_value()


def test_generate_steps(definition: PipelineDefinition):
    steps = definition.instantiate_steps()

    assert len(steps) == 2
    assert isinstance(steps[0], AssignName)
    assert isinstance(steps[1], LogInput)


def test_steps_share_references(definition: PipelineDefinition):
    assign, log = definition.instantiate_steps()

    assert isinstance(assign, AssignName)
    assert isinstance(log, LogInput)

    assert assign.output_ is definition.references.name
    assert log.input_ is definition.references.name


def test_instantiate_pipeline(definition: PipelineDefinition):
    pipeline = definition.instantiate_pipeline()

    assert isinstance(pipeline, Pipeline)
    assert len(pipeline.steps) == 2


def test_references_are_independent():
    first = PipelineDefinition.from_settings(
        PipelineSettings(name="first")
    )
    second = PipelineDefinition.from_settings(
        PipelineSettings(name="second")
    )

    assert first.references is not second.references
    assert first.references.name is not second.references.name