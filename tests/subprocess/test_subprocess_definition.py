
from process_framework.pipeline import Pipeline

from .fixtures import (
    Context,
    RecordStep,
    SubprocessReferences,
    make_definition,
)


def test_initialize_references():
    definition = make_definition()
    context = Context("hello", [])

    references = definition.initialize_references(context)

    assert isinstance(references, SubprocessReferences)
    assert references.value.get_value() == "hello"
    assert references.events is context.events


def test_instantiate_pipeline():
    definition = make_definition()
    context = Context("hello", [])

    pipeline = definition.instantiate_pipeline(context)

    assert isinstance(pipeline, Pipeline)
    assert len(pipeline.steps) == 1
    assert isinstance(pipeline.steps[0], RecordStep)


def test_pipeline_execution():
    definition = make_definition()
    events = []

    pipeline = definition.instantiate_pipeline(
        Context("hello", events)
    )

    pipeline.preflight()
    pipeline.do()

    assert events == [
        ("preflight", "hello"),
        ("do", "hello"),
    ]


def test_references_are_independent():
    definition = make_definition()

    first = definition.initialize_references(
        Context("first", [])
    )
    second = definition.initialize_references(
        Context("second", [])
    )

    assert first is not second
    assert first.value is not second.value
    assert first.value.get_value() == "first"
    assert second.value.get_value() == "second"


def test_multiple_pipeline_instantiations():
    definition = make_definition()
    events = []

    first = definition.instantiate_pipeline(
        Context("first", events)
    )
    second = definition.instantiate_pipeline(
        Context("second", events)
    )

    assert first is not second
    assert first.steps[0] is not second.steps[0]

    first.do()
    second.do()

    assert events == [
        ("preflight", "first"),
        ("do", "first"),
        ("preflight", "second"),
        ("do", "second"),
    ]
