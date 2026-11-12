
import logging

import pytest

from process_framework import Reference
from process_framework.composition.core import _repr
from process_framework.steps.logging_step import LogInput, LogMessage


def test_log_message(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogMessage(message="Hello world").do()

    assert caplog.messages == ["Hello world"]


def test_log_message_level(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.DEBUG):
        LogMessage(
            message="Debug message",
            level=logging.DEBUG,
        ).do()

    assert caplog.records[0].levelno == logging.DEBUG
    assert caplog.messages == ["Debug message"]


def test_log_input_default(caplog: pytest.LogCaptureFixture):
    input_ = Reference(str, "hello")

    with caplog.at_level(logging.INFO):
        LogInput(input_=input_).do()

    assert caplog.messages == [_repr.repr(input_)]


def test_log_input_format(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogInput(
            input_="hello",
            format="Value: {}",
            args=(str.upper,),
        ).do()

    assert caplog.messages == ["Value: HELLO"]


def test_log_input_format_kwargs(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogInput(
            input_="hello",
            format="Length: {length}, value: {value}",
            kwargs={
                "length": len,
                "value": str.upper,
            },
        ).do()

    assert caplog.messages == ["Length: 5, value: HELLO"]


def test_log_input_format_mixed(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogInput(
            input_="hello",
            format="{} ({length} characters)",
            args=(str.upper,),
            kwargs={"length": len},
        ).do()

    assert caplog.messages == ["HELLO (5 characters)"]


def test_log_input_args(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogInput(
            input_="hello",
            args=(len, str.upper),
        ).do()

    assert caplog.messages == ["5, 'HELLO'"]


def test_log_input_kwargs(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogInput(
            input_="hello",
            kwargs={
                "length": len,
                "uppercase": str.upper,
            },
        ).do()

    assert caplog.messages == ["length: 5, uppercase: 'HELLO'"]


def test_log_input_args_and_kwargs(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.INFO):
        LogInput(
            input_="hello",
            args=(len, str.upper),
            kwargs={"original": lambda value: value},
        ).do()

    assert caplog.messages == [
        "5, 'HELLO', original: 'hello'"
    ]


def test_log_input_resolves_reference(caplog: pytest.LogCaptureFixture):
    input_ = Reference(str)

    step = LogInput(
        input_=input_,
        kwargs={"length": len},
    )

    input_.set_value("hello")

    with caplog.at_level(logging.INFO):
        step.do()

    assert caplog.messages == ["length: 5"]


def test_log_input_evaluates_at_execution(
    caplog: pytest.LogCaptureFixture,
):
    input_ = Reference(str, "first")

    step = LogInput(
        input_=input_,
        args=(str.upper,),
    )

    with caplog.at_level(logging.INFO):
        step.do()
        input_.set_value("second")
        step.do()

    assert caplog.messages == ["'FIRST'", "'SECOND'"]


def test_log_input_callable_evaluation_order():
    calls = []

    def record(name: str):
        def evaluate(value: str) -> str:
            calls.append(name)
            return value

        return evaluate

    LogInput(
        input_="hello",
        args=(record("first"), record("second")),
        kwargs={
            "third": record("third"),
            "fourth": record("fourth"),
        },
    ).do()

    assert calls == ["first", "second", "third", "fourth"]


def test_log_input_level(caplog: pytest.LogCaptureFixture):
    with caplog.at_level(logging.DEBUG):
        LogInput(
            input_="hello",
            level=logging.DEBUG,
        ).do()

    assert caplog.records[0].levelno == logging.DEBUG


def test_log_input_invalid_format():
    step = LogInput(
        input_="hello",
        format="{missing}",
        kwargs={"length": len},
    )

    with pytest.raises(KeyError, match="missing"):
        step.do()


def test_log_input_callable_failure():
    def fail(value: str) -> str:
        raise ValueError("Formatter failed")

    step = LogInput(
        input_="hello",
        args=(fail,),
    )

    with pytest.raises(ValueError, match="Formatter failed"):
        step.do()
