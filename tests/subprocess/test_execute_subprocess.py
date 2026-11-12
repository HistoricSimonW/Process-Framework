
import pytest

from process_framework import Reference
from process_framework.composition.retries import RetryArgs
from process_framework.steps.control_flow.execute_subprocess import (
    ExecuteSubprocess,
    ExecuteSubprocessForEach,
    ExecuteSubprocessForEachBatch,
)

from .fixtures import Context, RecordStep, make_definition


def test_execute_subprocess():
    events = []

    step = ExecuteSubprocess(
        definition=make_definition(),
        context=Context("hello", events),
        retry=None,
    )

    step.do()

    assert events == [
        ("preflight", "hello"),
        ("do", "hello"),
    ]


def test_execute_subprocess_from_reference():
    events = []
    context = Reference(Context)

    step = ExecuteSubprocess(
        definition=make_definition(),
        context=context,
        retry=None,
    )

    context.set_value(Context("hello", events))
    step.do()

    assert events == [
        ("preflight", "hello"),
        ("do", "hello"),
    ]


def test_execute_subprocess_for_each():
    events = []
    items = [
        Context("first", events),
        Context("second", events),
        Context("third", events),
    ]

    step = ExecuteSubprocessForEach(
        definition=make_definition(),
        items=items,
        retry=None,
    )

    step.do()

    assert events == [
        ("preflight", "first"),
        ("do", "first"),
        ("preflight", "second"),
        ("do", "second"),
        ("preflight", "third"),
        ("do", "third"),
    ]


def test_execute_subprocess_for_each_from_reference():
    events = []
    items = Reference(list, [
        Context("first", events),
        Context("second", events),
    ])

    step = ExecuteSubprocessForEach(
        definition=make_definition(),
        items=items,
        retry=None,
    )

    step.do()

    assert events == [
        ("preflight", "first"),
        ("do", "first"),
        ("preflight", "second"),
        ("do", "second"),
    ]


def test_execute_subprocess_for_each_empty():
    events = []

    step = ExecuteSubprocessForEach(
        definition=make_definition(),
        items=[],
        retry=None,
    )

    step.do()

    assert events == []


def test_execute_subprocess_for_each_batch():
    events = []

    step = ExecuteSubprocessForEachBatch(
        definition=make_definition(),
        input_=["first", "second"],
        item_func=lambda values: (
            Context(value, events) for value in values
        ),
        retry=None,
    )

    step.do()

    assert events == [
        ("preflight", "first"),
        ("do", "first"),
        ("preflight", "second"),
        ("do", "second"),
    ]


def test_execute_subprocess_for_each_batch_from_reference():
    events = []
    input_ = Reference(list, ["first", "second"])

    step = ExecuteSubprocessForEachBatch(
        definition=make_definition(),
        input_=input_,
        item_func=lambda values: (
            Context(value.upper(), events) for value in values
        ),
        retry=None,
    )

    step.do()

    assert events == [
        ("preflight", "FIRST"),
        ("do", "FIRST"),
        ("preflight", "SECOND"),
        ("do", "SECOND"),
    ]


def test_execute_subprocess_retries(
    monkeypatch: pytest.MonkeyPatch,
):
    events = []
    attempts = 0
    original_do = RecordStep.do

    def flaky_do(self: RecordStep) -> None:
        nonlocal attempts
        attempts += 1

        if attempts == 1:
            raise RuntimeError("Temporary failure")

        original_do(self)

    monkeypatch.setattr(RecordStep, "do", flaky_do)

    step = ExecuteSubprocess(
        definition=make_definition(),
        context=Context("hello", events),
        retry=RetryArgs(
            attempts=2,
            wait_seconds=0,
        ),
    )

    step.do()

    assert attempts == 2
    assert events == [
        ("preflight", "hello"),
        ("do", "hello"),
    ]


def test_execute_subprocess_propagates_failure(
    monkeypatch: pytest.MonkeyPatch,
):
    def fail(self: RecordStep) -> None:
        raise RuntimeError("Subprocess failed")

    monkeypatch.setattr(RecordStep, "do", fail)

    step = ExecuteSubprocess(
        definition=make_definition(),
        context=Context("hello", []),
        retry=None,
    )

    with pytest.raises(
        RuntimeError,
        match="Subprocess failed",
    ):
        step.do()
